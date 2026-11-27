// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Server observability primitives: lock-free counters, latency histograms, Tonic
//! instrumentation middleware, and the unauthenticated HTTP `/healthz`, `/statz`, and
//! Prometheus `/metrics` listener.
//!
//! # Concurrency and Merge Invariants
//! Counters, sliding-window commit-rate buckets, and Prometheus latency histogram buckets
//! update via lock-free atomics (non-cumulative per-bucket counts prevent torn cumulative
//! inversions at scrape time). Percentile sampling uses a fixed 4,096-entry ring buffer and
//! active workspace tracking uses a bounded key set so memory stays O(1) under load.
//!
//! # Cross-Process and RPC Boundaries
//! [`MetricsLayer`] measures authenticated gRPC calls on the three Commit Cloud services
//! (`BackendService`, `OpStoreService`, `WorkspaceService`), excluding `/grpc.health.v1.Health/*`
//! probes, unknown paths, and `UNAUTHENTICATED` rejections (`grpc-status: 16`).
//! For server-streaming RPCs (such as `ReadFile`), recorded duration reflects time-to-headers.
//! [`bind_and_serve_metrics`] binds its TCP listener before returning so port conflicts fail
//! server startup immediately.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tower::{Layer, Service};

use crate::store::ReplicationStats;

const COMMIT_RATE_WINDOW_SECS: usize = 10;
const COMMIT_BUCKET_COUNT_MASK: u64 = (1 << 24) - 1;
const LATENCY_WINDOW_CAPACITY: usize = 4_096;
const MAX_TRACKED_WORKSPACES: usize = 100_000;
const GRPC_STATUS_UNAUTHENTICATED: &[u8] = b"16";
const RECORDED_GRPC_SERVICE_PREFIXES: [&str; 3] = [
    "/commit_cloud.backend.BackendService/",
    "/commit_cloud.op_store.OpStoreService/",
    "/commit_cloud.workspace.WorkspaceService/",
];

const HISTOGRAM_BOUNDS_SECS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const HISTOGRAM_BOUND_LABELS: [&str; 12] = [
    "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10",
];
const HISTOGRAM_BUCKET_COUNT: usize = HISTOGRAM_BOUNDS_SECS.len() + 1;

/// JSON payload returned by `GET /statz`.
///
/// Field notes:
/// - `active_workspaces`: best-effort count of distinct workspaces observed by this server
///   instance since startup (not a global database count across replicas or restarts).
/// - `cas_mirror_dropped_total`: reserved compatibility field exported alongside
///   `cloudjj_cas_mirror_dropped_total` in `/metrics`; remains `0` until async CAS mirror drop
///   tracking is wired.
/// - `db_tx_retries_total`: cumulative metadata database transaction retries across
///   `SpannerMetadataStore` and SQL-backed metadata stores; exported as
///   `cloudjj_sql_tx_retries_total` in Prometheus for dashboard/scraper compatibility.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StatzSnapshot {
    pub total_commits: i64,
    pub commits_per_second: f64,
    pub active_workspaces: i64,
    pub rpc_p50_ms: f64,
    pub rpc_p95_ms: f64,
    pub cas_mirror_dropped_total: i64,
    pub db_tx_retries_total: u64,
    pub reconcile_op_heads_total: u64,
    pub reconcile_client_fallback_total: u64,
    pub blob_cache_hits: u64,
    pub blob_cache_misses: u64,
}

#[derive(Default)]
struct LatencyRing {
    samples_ms: Vec<f64>,
    next_idx: usize,
}

impl LatencyRing {
    fn record(&mut self, ms: f64) {
        if self.samples_ms.len() < LATENCY_WINDOW_CAPACITY {
            self.samples_ms.push(ms);
        } else {
            self.samples_ms[self.next_idx] = ms;
            self.next_idx = (self.next_idx + 1) % LATENCY_WINDOW_CAPACITY;
        }
    }

    fn snapshot_samples(&self) -> Vec<f64> {
        self.samples_ms.clone()
    }
}

fn compute_percentiles(mut sorted: Vec<f64>) -> (f64, f64) {
    if sorted.is_empty() {
        return (0.0, 0.0);
    }
    sorted.sort_by(f64::total_cmp);
    (
        nearest_rank_percentile(&sorted, 0.50),
        nearest_rank_percentile(&sorted, 0.95),
    )
}

fn nearest_rank_percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[idx]
}

#[derive(Default)]
struct ActiveWorkspaceTracker {
    // Nested `repo_id -> user -> workspace_name` maps allow zero-allocation `&str` lookups on
    // steady-state `UpdateWorkspace` and `DeleteWorkspace` calls.
    by_repo: HashMap<String, HashMap<String, HashSet<String>>>,
    total: usize,
}

impl ActiveWorkspaceTracker {
    fn upsert(&mut self, repo_id: &str, user: &str, workspace_name: &str) -> i64 {
        if let Some(users) = self.by_repo.get_mut(repo_id) {
            if let Some(workspaces) = users.get_mut(user) {
                if !workspaces.contains(workspace_name) && self.total < MAX_TRACKED_WORKSPACES {
                    workspaces.insert(workspace_name.to_string());
                    self.total += 1;
                }
            } else if self.total < MAX_TRACKED_WORKSPACES {
                let mut workspaces = HashSet::new();
                workspaces.insert(workspace_name.to_string());
                users.insert(user.to_string(), workspaces);
                self.total += 1;
            }
        } else if self.total < MAX_TRACKED_WORKSPACES {
            let mut workspaces = HashSet::new();
            workspaces.insert(workspace_name.to_string());
            let mut users = HashMap::new();
            users.insert(user.to_string(), workspaces);
            self.by_repo.insert(repo_id.to_string(), users);
            self.total += 1;
        }
        self.total as i64
    }

    fn remove_workspace(&mut self, repo_id: &str, user: &str, workspace_name: &str) -> i64 {
        if let Some(users) = self.by_repo.get_mut(repo_id) {
            if let Some(workspaces) = users.get_mut(user) {
                if workspaces.remove(workspace_name) {
                    self.total = self.total.saturating_sub(1);
                }
                if workspaces.is_empty() {
                    users.remove(user);
                }
            }
            if users.is_empty() {
                self.by_repo.remove(repo_id);
            }
        }
        self.total as i64
    }

    fn remove_repo(&mut self, repo_id: &str) -> i64 {
        if let Some(removed_users) = self.by_repo.remove(repo_id) {
            let count: usize = removed_users.values().map(HashSet::len).sum();
            self.total = self.total.saturating_sub(count);
        }
        self.total as i64
    }
}

struct ServerMetricsInner {
    start_instant: Instant,
    total_commits: AtomicI64,
    // Each bucket packs `(elapsed_sec_1based << 24) | (count & 0x00FF_FFFF)` so a single
    // CAS updates both timestamp epoch and count without tearing across second boundaries.
    commit_buckets: [AtomicU64; COMMIT_RATE_WINDOW_SECS],
    active_workspaces: AtomicI64,
    workspace_tracker: Mutex<ActiveWorkspaceTracker>,
    db_tx_retries_total: Arc<AtomicU64>,
    reconcile_merged_total: AtomicU64,
    reconcile_client_fallback_total: AtomicU64,
    replication_stats: Mutex<Option<Arc<ReplicationStats>>>,
    latency_ring: Mutex<LatencyRing>,
    histogram_buckets: [AtomicU64; HISTOGRAM_BUCKET_COUNT],
    histogram_sum_micros: AtomicU64,
}

/// Shared, cheaply clonable telemetry sink attached to gRPC services, stores, and the
/// HTTP observability endpoints.
#[derive(Clone)]
pub struct ServerMetrics {
    inner: Arc<ServerMetricsInner>,
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ServerMetricsInner {
                start_instant: Instant::now(),
                total_commits: AtomicI64::new(0),
                commit_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
                active_workspaces: AtomicI64::new(0),
                workspace_tracker: Mutex::new(ActiveWorkspaceTracker::default()),
                db_tx_retries_total: Arc::new(AtomicU64::new(0)),
                reconcile_merged_total: AtomicU64::new(0),
                reconcile_client_fallback_total: AtomicU64::new(0),
                replication_stats: Mutex::new(None),
                latency_ring: Mutex::new(LatencyRing::default()),
                histogram_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
                histogram_sum_micros: AtomicU64::new(0),
            }),
        }
    }

    /// Returns a shared handle to the database transaction retry counter so storage
    /// implementations (`SpannerMetadataStore`, SQL stores) can increment it directly.
    pub fn db_tx_retries_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.inner.db_tx_retries_total)
    }

    /// Connects a `ReplicatedBlobStore`'s live `ReplicationStats` so blob cache hits and
    /// misses appear automatically in `/statz` and `/metrics`.
    pub fn attach_replication_stats(&self, stats: Arc<ReplicationStats>) {
        *self
            .inner
            .replication_stats
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(stats);
    }

    pub fn record_commit(&self) {
        self.record_commit_at(self.inner.start_instant.elapsed().as_secs());
    }

    fn record_commit_at(&self, elapsed_secs: u64) {
        self.inner.total_commits.fetch_add(1, Ordering::Relaxed);
        let stamp = elapsed_secs.saturating_add(1);
        let slot = &self.inner.commit_buckets[(stamp % (COMMIT_RATE_WINDOW_SECS as u64)) as usize];
        let mut cur = slot.load(Ordering::Relaxed);
        loop {
            let cur_stamp = cur >> 24;
            let cur_count = cur & COMMIT_BUCKET_COUNT_MASK;
            let next = if cur_stamp == stamp {
                (stamp << 24) | cur_count.saturating_add(1).min(COMMIT_BUCKET_COUNT_MASK)
            } else if cur_stamp < stamp {
                (stamp << 24) | 1
            } else {
                break;
            };
            match slot.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    fn commits_per_second_at(&self, elapsed_secs: u64) -> f64 {
        let now_stamp = elapsed_secs.saturating_add(1);
        let min_stamp = now_stamp
            .saturating_sub((COMMIT_RATE_WINDOW_SECS as u64) - 1)
            .max(1);
        let mut sum = 0u64;
        for slot in &self.inner.commit_buckets {
            let val = slot.load(Ordering::Relaxed);
            let stamp = val >> 24;
            if stamp >= min_stamp && stamp <= now_stamp {
                sum = sum.saturating_add(val & COMMIT_BUCKET_COUNT_MASK);
            }
        }
        sum as f64 / COMMIT_RATE_WINDOW_SECS as f64
    }

    pub fn record_workspace_upsert(&self, repo_id: &str, user: &str, workspace_name: &str) {
        let mut tracker = self
            .inner
            .workspace_tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let active = tracker.upsert(repo_id, user, workspace_name);
        self.inner
            .active_workspaces
            .store(active, Ordering::Relaxed);
    }

    pub fn record_workspace_deleted(&self, repo_id: &str, user: &str, workspace_name: &str) {
        let mut tracker = self
            .inner
            .workspace_tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let active = tracker.remove_workspace(repo_id, user, workspace_name);
        self.inner
            .active_workspaces
            .store(active, Ordering::Relaxed);
    }

    pub fn record_repository_deleted(&self, repo_id: &str) {
        let mut tracker = self
            .inner
            .workspace_tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let active = tracker.remove_repo(repo_id);
        self.inner
            .active_workspaces
            .store(active, Ordering::Relaxed);
    }

    pub fn record_reconcile_op_heads(&self, client_resolution_required: bool) {
        if client_resolution_required {
            self.inner
                .reconcile_client_fallback_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner
                .reconcile_merged_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_rpc_duration(&self, duration: Duration) {
        let secs = duration.as_secs_f64();
        let ms = secs * 1000.0;
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);

        let bucket_idx = HISTOGRAM_BOUNDS_SECS
            .iter()
            .position(|&bound| secs <= bound)
            .unwrap_or(HISTOGRAM_BOUNDS_SECS.len());
        self.inner.histogram_buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        let mut cur = self.inner.histogram_sum_micros.load(Ordering::Relaxed);
        while let Err(actual) = self.inner.histogram_sum_micros.compare_exchange_weak(
            cur,
            cur.saturating_add(micros),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            cur = actual;
        }

        self.inner
            .latency_ring
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .record(ms);
    }

    fn blob_cache_counts(&self) -> (u64, u64) {
        if let Some(stats) = self
            .inner
            .replication_stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            (
                stats.cache_hits.load(Ordering::Relaxed),
                stats.cache_misses.load(Ordering::Relaxed),
            )
        } else {
            (0, 0)
        }
    }

    pub fn snapshot(&self) -> StatzSnapshot {
        let elapsed_secs = self.inner.start_instant.elapsed().as_secs();
        let samples = self
            .inner
            .latency_ring
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot_samples();
        let (rpc_p50_ms, rpc_p95_ms) = compute_percentiles(samples);
        let (blob_cache_hits, blob_cache_misses) = self.blob_cache_counts();
        let reconcile_merged = self.inner.reconcile_merged_total.load(Ordering::Relaxed);
        let reconcile_fallback = self
            .inner
            .reconcile_client_fallback_total
            .load(Ordering::Relaxed);
        StatzSnapshot {
            total_commits: self.inner.total_commits.load(Ordering::Relaxed),
            commits_per_second: self.commits_per_second_at(elapsed_secs),
            active_workspaces: self.inner.active_workspaces.load(Ordering::Relaxed),
            rpc_p50_ms,
            rpc_p95_ms,
            cas_mirror_dropped_total: 0,
            db_tx_retries_total: self.inner.db_tx_retries_total.load(Ordering::Relaxed),
            reconcile_op_heads_total: reconcile_merged.saturating_add(reconcile_fallback),
            reconcile_client_fallback_total: reconcile_fallback,
            blob_cache_hits,
            blob_cache_misses,
        }
    }

    /// Renders metrics in Prometheus 0.0.4 text exposition format.
    ///
    /// Metric names retain `cloudjj_cas_mirror_dropped_total` (reserved compatibility counter,
    /// currently `0` until async CAS mirror drop tracking is wired) and
    /// `cloudjj_sql_tx_retries_total` (matching dashboard/scraper schemas for
    /// `SpannerMetadataStore` and SQL-backed metadata stores). Streaming RPC durations in
    /// `cloudjj_rpc_duration_seconds` measure time-to-response-headers.
    pub fn render_prometheus(&self) -> String {
        let total_commits = self.inner.total_commits.load(Ordering::Relaxed).max(0);
        let active_workspaces = self.inner.active_workspaces.load(Ordering::Relaxed);
        let cas_mirror_dropped = 0i64;
        let db_tx_retries = self.inner.db_tx_retries_total.load(Ordering::Relaxed);
        let reconcile_merged = self.inner.reconcile_merged_total.load(Ordering::Relaxed);
        let reconcile_fallback = self
            .inner
            .reconcile_client_fallback_total
            .load(Ordering::Relaxed);
        let (blob_hits, blob_misses) = self.blob_cache_counts();

        let sum_secs = self.inner.histogram_sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let raw_buckets: [u64; HISTOGRAM_BUCKET_COUNT] =
            std::array::from_fn(|i| self.inner.histogram_buckets[i].load(Ordering::Relaxed));

        let mut out = String::with_capacity(1536);
        let _ = writeln!(
            out,
            "# HELP cloudjj_commits_total Total number of commits written via WriteCommit."
        );
        let _ = writeln!(out, "# TYPE cloudjj_commits_total counter");
        let _ = writeln!(out, "cloudjj_commits_total {total_commits}");

        let _ = writeln!(
            out,
            "# HELP cloudjj_active_workspaces Best-effort count of active workspaces observed by this server instance since startup."
        );
        let _ = writeln!(out, "# TYPE cloudjj_active_workspaces gauge");
        let _ = writeln!(out, "cloudjj_active_workspaces {active_workspaces}");

        let _ = writeln!(
            out,
            "# HELP cloudjj_cas_mirror_dropped_total Reserved counter for dropped async CAS mirror operations (currently always 0)."
        );
        let _ = writeln!(out, "# TYPE cloudjj_cas_mirror_dropped_total counter");
        let _ = writeln!(out, "cloudjj_cas_mirror_dropped_total {cas_mirror_dropped}");

        let _ = writeln!(
            out,
            "# HELP cloudjj_rpc_duration_seconds Authenticated Commit Cloud gRPC request latency in seconds (time-to-headers for streaming RPCs; excludes health probes, unknown paths, and UNAUTHENTICATED rejections)."
        );
        let _ = writeln!(out, "# TYPE cloudjj_rpc_duration_seconds histogram");
        let mut cumulative = 0u64;
        for (idx, label) in HISTOGRAM_BOUND_LABELS.iter().enumerate() {
            cumulative = cumulative.saturating_add(raw_buckets[idx]);
            let _ = writeln!(
                out,
                "cloudjj_rpc_duration_seconds_bucket{{le=\"{label}\"}} {cumulative}"
            );
        }
        cumulative = cumulative.saturating_add(raw_buckets[HISTOGRAM_BOUNDS_SECS.len()]);
        let _ = writeln!(
            out,
            "cloudjj_rpc_duration_seconds_bucket{{le=\"+Inf\"}} {cumulative}"
        );
        let _ = writeln!(out, "cloudjj_rpc_duration_seconds_sum {sum_secs:.6}");
        let _ = writeln!(out, "cloudjj_rpc_duration_seconds_count {cumulative}");

        let _ = writeln!(
            out,
            "# HELP cloudjj_sql_tx_retries_total Total number of metadata database transaction retries across SQL-backed stores."
        );
        let _ = writeln!(out, "# TYPE cloudjj_sql_tx_retries_total counter");
        let _ = writeln!(out, "cloudjj_sql_tx_retries_total {db_tx_retries}");

        let _ = writeln!(
            out,
            "# HELP cloudjj_reconcile_op_heads_total Total number of ReconcileOpHeads outcomes."
        );
        let _ = writeln!(out, "# TYPE cloudjj_reconcile_op_heads_total counter");
        let _ = writeln!(
            out,
            "cloudjj_reconcile_op_heads_total{{outcome=\"merged\"}} {reconcile_merged}"
        );
        let _ = writeln!(
            out,
            "cloudjj_reconcile_op_heads_total{{outcome=\"client_fallback\"}} {reconcile_fallback}"
        );

        let _ = writeln!(
            out,
            "# HELP cloudjj_blob_cache_requests_total Total number of cacheable server blob cache lookups."
        );
        let _ = writeln!(out, "# TYPE cloudjj_blob_cache_requests_total counter");
        let _ = writeln!(
            out,
            "cloudjj_blob_cache_requests_total{{result=\"hit\"}} {blob_hits}"
        );
        let _ = writeln!(
            out,
            "cloudjj_blob_cache_requests_total{{result=\"miss\"}} {blob_misses}"
        );

        out
    }
}

/// Tower layer that records per-RPC durations into [`ServerMetrics`] only for the
/// authenticated Commit Cloud services (`/commit_cloud.backend.BackendService/*`,
/// `/commit_cloud.op_store.OpStoreService/*`, `/commit_cloud.workspace.WorkspaceService/*`),
/// skipping gRPC health check probes, unknown gRPC/HTTP paths that bypass per-service auth,
/// and `UNAUTHENTICATED` rejections (`grpc-status: 16`) so readiness polling or
/// unauthenticated request floods cannot skew latency percentiles. For server-streaming RPCs
/// (such as `ReadFile`), the recorded duration covers time until response headers are returned
/// rather than full stream transfer.
#[derive(Clone)]
pub struct MetricsLayer {
    metrics: ServerMetrics,
}

impl MetricsLayer {
    pub fn new(metrics: ServerMetrics) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics: ServerMetrics,
}

struct RpcDurationGuard {
    metrics: ServerMetrics,
    started: Instant,
    armed: bool,
}

impl RpcDurationGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RpcDurationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.metrics.record_rpc_duration(self.started.elapsed());
        }
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for MetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let path = req.uri().path();
        // Match Tonic's `/{service}/*rest` catch-all routes (which require a non-empty method
        // suffix) so trailing-slash roots that bypass `InterceptedService` auth are not recorded.
        let should_record = RECORDED_GRPC_SERVICE_PREFIXES.iter().any(|prefix| {
            path.strip_prefix(prefix)
                .is_some_and(|method| !method.is_empty())
        });
        let mut guard = should_record.then(|| RpcDurationGuard {
            metrics: self.metrics.clone(),
            started: Instant::now(),
            armed: true,
        });
        let fut = self.inner.call(req);
        Box::pin(async move {
            let res = fut.await;
            if let Ok(ref response) = res
                && response
                    .headers()
                    .get("grpc-status")
                    .is_some_and(|v| v.as_bytes() == GRPC_STATUS_UNAUTHENTICATED)
                && let Some(g) = guard.as_mut()
            {
                g.disarm();
            }
            res
        })
    }
}

async fn healthz_handler() -> ([(http::header::HeaderName, &'static str); 1], &'static str) {
    (
        [(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        "ok\n",
    )
}

async fn statz_handler(State(metrics): State<ServerMetrics>) -> Json<StatzSnapshot> {
    Json(metrics.snapshot())
}

async fn metrics_handler(
    State(metrics): State<ServerMetrics>,
) -> ([(http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics.render_prometheus(),
    )
}

/// Builds the unauthenticated observability `axum::Router` exposing `/healthz`, `/statz`,
/// and `/metrics`.
pub fn metrics_router(metrics: ServerMetrics) -> Router {
    Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/statz", get(statz_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(metrics)
}

/// Binds the observability HTTP listener synchronously before spawning the Axum server task,
/// failing fast on port-in-use errors and returning the bound address (supporting port `0`)
/// together with the server task handle for graceful shutdown draining.
pub async fn bind_and_serve_metrics<F>(
    addr: SocketAddr,
    metrics: ServerMetrics,
    shutdown: F,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let app = metrics_router(metrics);
    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!("Observability HTTP server exited with error: {err}");
        }
    });
    Ok((local_addr, handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sliding_window_commit_rate_expires_old_buckets() {
        let metrics = ServerMetrics::new();
        for _ in 0..5 {
            metrics.record_commit_at(0);
        }
        for _ in 0..15 {
            metrics.record_commit_at(5);
        }
        assert_eq!(metrics.snapshot().total_commits, 20);
        assert!((metrics.commits_per_second_at(5) - 2.0).abs() < 1e-9);
        // At t = 12s, bucket t=0 (12s old) has fallen outside the 10-second window [3..=12].
        assert!((metrics.commits_per_second_at(12) - 1.5).abs() < 1e-9);
        // At t = 20s, all earlier buckets have expired.
        assert_eq!(metrics.commits_per_second_at(20), 0.0);
    }

    #[test]
    fn active_workspaces_tracks_idempotent_upserts_deletes_and_repo_deletion() {
        let metrics = ServerMetrics::new();
        metrics.record_workspace_upsert("repo-a", "alice", "default");
        metrics.record_workspace_upsert("repo-a", "alice", "default");
        assert_eq!(metrics.snapshot().active_workspaces, 1);

        metrics.record_workspace_upsert("repo-a", "bob", "ws-2");
        metrics.record_workspace_upsert("repo-b", "alice", "default");
        assert_eq!(metrics.snapshot().active_workspaces, 3);

        metrics.record_workspace_deleted("repo-a", "alice", "default");
        metrics.record_workspace_deleted("repo-a", "nonexistent", "ws");
        assert_eq!(metrics.snapshot().active_workspaces, 2);

        metrics.record_repository_deleted("repo-a");
        assert_eq!(metrics.snapshot().active_workspaces, 1);

        metrics.record_repository_deleted("repo-b");
        metrics.record_repository_deleted("repo-b");
        assert_eq!(metrics.snapshot().active_workspaces, 0);
    }

    #[test]
    fn rpc_percentiles_and_prometheus_histogram_remain_consistent() {
        let metrics = ServerMetrics::new();
        for ms in 1..=100u64 {
            metrics.record_rpc_duration(Duration::from_millis(ms));
        }
        let snap = metrics.snapshot();
        assert!((snap.rpc_p50_ms - 50.0).abs() < 1e-6);
        assert!((snap.rpc_p95_ms - 95.0).abs() < 1e-6);

        let prom = metrics.render_prometheus();
        assert!(prom.contains("cloudjj_rpc_duration_seconds_count 100\n"));
        assert!(prom.contains("cloudjj_rpc_duration_seconds_bucket{le=\"+Inf\"} 100\n"));
        assert!(prom.contains("cloudjj_rpc_duration_seconds_bucket{le=\"0.05\"} 50\n"));
        assert!(prom.contains("cloudjj_rpc_duration_seconds_bucket{le=\"0.1\"} 100\n"));
    }

    #[test]
    fn rpc_duration_drop_guard_records_cancelled_calls() {
        let metrics = ServerMetrics::new();
        {
            let _guard = RpcDurationGuard {
                metrics: metrics.clone(),
                started: Instant::now(),
                armed: true,
            };
        }
        let prom = metrics.render_prometheus();
        assert!(prom.contains("cloudjj_rpc_duration_seconds_count 1\n"));
    }

    #[tokio::test]
    async fn metrics_layer_skips_unauthenticated_rejections_and_health_probes() {
        #[derive(Clone)]
        struct StubGrpcService;

        impl Service<http::Request<()>> for StubGrpcService {
            type Response = http::Response<()>;
            type Error = std::convert::Infallible;
            type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, req: http::Request<()>) -> Self::Future {
                let path = req.uri().path().to_string();
                Box::pin(async move {
                    let mut resp = http::Response::builder().status(200);
                    if path.ends_with("/Unauth") {
                        resp = resp.header("grpc-status", "16");
                    } else if path.ends_with("/NotFound") {
                        resp = resp.header("grpc-status", "5");
                    } else {
                        resp = resp.header("grpc-status", "0");
                    }
                    Ok(resp.body(()).unwrap())
                })
            }
        }

        let metrics = ServerMetrics::new();
        let mut svc = MetricsLayer::new(metrics.clone()).layer(StubGrpcService);

        for uri in [
            "/grpc.health.v1.Health/Check",
            "/",
            "/unknown.Service/Probe",
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "/commit_cloud.backend.BackendService/",
            "/commit_cloud.op_store.OpStoreService/",
            "/commit_cloud.workspace.WorkspaceService/",
            "/commit_cloud.backend.BackendService/Unauth",
            "/commit_cloud.op_store.OpStoreService/Unauth",
            "/commit_cloud.workspace.WorkspaceService/Unauth",
        ] {
            let req = http::Request::builder().uri(uri).body(()).unwrap();
            svc.call(req).await.unwrap();
        }
        assert!(
            metrics
                .render_prometheus()
                .contains("cloudjj_rpc_duration_seconds_count 0\n"),
            "health probes, unknown paths, and UNAUTHENTICATED responses must not be recorded"
        );

        for uri in [
            "/commit_cloud.backend.BackendService/Ok",
            "/commit_cloud.op_store.OpStoreService/NotFound",
            "/commit_cloud.workspace.WorkspaceService/Ok",
        ] {
            let req = http::Request::builder().uri(uri).body(()).unwrap();
            svc.call(req).await.unwrap();
        }
        assert!(
            metrics
                .render_prometheus()
                .contains("cloudjj_rpc_duration_seconds_count 3\n"),
            "authenticated Commit Cloud service responses must be recorded"
        );
    }
}
