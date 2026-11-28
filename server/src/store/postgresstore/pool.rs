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

//! PostgreSQL connection pool configuration, checkout, and retryable transaction runner.
//!
//! # Concurrency and Merge Invariants
//! Configures bounded `deadpool-postgres` wait, create, and recycle timeouts and retries
//! serialization failures (`40001`) and deadlocks (`40P01`) with exponential backoff and
//! jitter, returning pooled connections before sleeping.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use deadpool_postgres::{Object, Timeouts};
use tokio_postgres::{NoTls, Transaction};

use super::codec::{is_retryable_pg_error, map_pg_err, map_pool_err};
use super::{PgCancelTls, PostgresMetadataStore};
use crate::store::{StoreError, StoreResult};

pub(super) const POOL_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) const POOL_CREATE_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) const POOL_RECYCLE_TIMEOUT: Duration = Duration::from_secs(30);

const MAX_TX_ATTEMPTS: usize = 5;

const TX_RETRY_BASE_DELAY_MS: u64 = 10;

pub(super) const MAX_ROLLBACK_WAIT: Duration = Duration::from_secs(2);

const CANCEL_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn default_pool_timeouts() -> Timeouts {
    Timeouts {
        wait: Some(POOL_WAIT_TIMEOUT),
        create: Some(POOL_CREATE_TIMEOUT),
        recycle: Some(POOL_RECYCLE_TIMEOUT),
    }
}

/// RAII guard for a checked-out pooled PostgreSQL connection.
///
/// Because `PostgresMetadataStore` uses `RecyclingMethod::Fast` (which only checks
/// `client.is_closed()`), cancelling an in-flight query or transaction via
/// `tokio::time::timeout` or gRPC disconnect would otherwise return a dirty/busy or
/// open-transaction connection to `deadpool`. Dropping this guard without calling
/// [`EvictOnIncomplete::mark_clean`] evicts the connection with `Object::take` and, when
/// `cancel_on_drop` is armed, issues a best-effort server-side PostgreSQL `CancelRequest`.
pub(super) struct EvictOnIncomplete {
    client: Option<Object>,
    cancel_tls: PgCancelTls,
    cancel_on_drop: bool,
}

impl EvictOnIncomplete {
    pub(super) fn new(client: Object, cancel_tls: PgCancelTls) -> Self {
        Self {
            client: Some(client),
            cancel_tls,
            cancel_on_drop: false,
        }
    }

    pub(super) fn client(&self) -> &Object {
        self.client
            .as_ref()
            .expect("pooled PostgreSQL client still held")
    }

    pub(super) fn client_mut(&mut self) -> &mut Object {
        self.client
            .as_mut()
            .expect("pooled PostgreSQL client still held")
    }

    /// Checks whether the active RPC deadline has already expired before issuing a query.
    /// If expired, returns the untouched connection to the pool and fails fast;
    /// otherwise arms server-side query cancellation on drop.
    pub(super) fn arm_for_query(&mut self, op_name: &'static str) -> StoreResult<()> {
        if crate::resilience::remaining_rpc_deadline().is_some_and(|rem| rem.is_zero()) {
            self.mark_clean();
            return Err(StoreError::DeadlineExceeded(format!(
                "gRPC deadline expired before {op_name}"
            )));
        }
        self.cancel_on_drop = true;
        Ok(())
    }

    pub(super) fn arm_cancel_on_drop(&mut self) {
        self.cancel_on_drop = true;
    }

    pub(super) fn disarm_cancel_on_drop(&mut self) {
        self.cancel_on_drop = false;
    }

    /// Returns the connection to `deadpool` when dropped.
    pub(super) fn mark_clean(&mut self) {
        self.cancel_on_drop = false;
        let _ = self.client.take();
    }

    /// Marks the connection clean only when `res` succeeded or returned a server-side
    /// PostgreSQL error (`as_db_error().is_some()`), keeping transport/protocol failures
    /// armed for eviction.
    pub(super) fn mark_clean_if_healthy<T>(&mut self, res: &Result<T, tokio_postgres::Error>) {
        self.cancel_on_drop = false;
        if is_healthy_pg_result(res) {
            self.mark_clean();
        }
    }

    /// Disarms query cancellation and returns the connection to the pool iff `err` is a
    /// server-side SQL error (`as_db_error().is_some()`); transport errors stay armed for
    /// eviction.
    pub(super) fn mark_clean_if_db_error(&mut self, err: &tokio_postgres::Error) {
        self.cancel_on_drop = false;
        if err.as_db_error().is_some() {
            self.mark_clean();
        }
    }

    /// Finalizes a checked-out connection after a transaction finishes according to `outcome`.
    pub(super) fn finish_tx(&mut self, outcome: RollbackOutcome) {
        match outcome {
            RollbackOutcome::Clean => self.mark_clean(),
            RollbackOutcome::EvictOnly => self.disarm_cancel_on_drop(),
            RollbackOutcome::CancelAndEvict => {}
        }
    }
}

/// Disposition for a pooled PostgreSQL connection when exiting a transaction scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RollbackOutcome {
    /// Commit or rollback succeeded (or PostgreSQL returned a server-side SQL error while
    /// leaving the connection healthy); return to the pool.
    Clean,
    /// No query is currently in flight (deadline expired before issuing rollback, or the
    /// transaction/rollback/commit completed with a transport error); evict the connection
    /// without spawning a futile server-side `CancelRequest`.
    EvictOnly,
    /// A statement or rollback timed out in flight; evict the connection and issue a
    /// best-effort server-side `CancelRequest`.
    CancelAndEvict,
}

pub(super) fn is_healthy_pg_result<T>(res: &Result<T, tokio_postgres::Error>) -> bool {
    match res {
        Ok(_) => true,
        Err(err) => err.as_db_error().is_some(),
    }
}

impl Drop for EvictOnIncomplete {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if self.cancel_on_drop
                && let Ok(handle) = tokio::runtime::Handle::try_current()
            {
                let cancel_token = client.cancel_token();
                let cancel_tls = self.cancel_tls.clone();
                handle.spawn(async move {
                    let res = tokio::time::timeout(CANCEL_REQUEST_TIMEOUT, async move {
                        match cancel_tls {
                            PgCancelTls::NoTls => cancel_token.cancel_query(NoTls).await,
                            PgCancelTls::Rustls(tls) => cancel_token.cancel_query(tls).await,
                        }
                    })
                    .await;
                    match res {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            tracing::debug!(
                                error = %err,
                                "Best-effort PostgreSQL query cancel failed"
                            );
                        }
                        Err(_) => {
                            tracing::debug!("Best-effort PostgreSQL query cancel timed out");
                        }
                    }
                });
            }
            let _ = deadpool_postgres::Object::take(client);
        }
    }
}

/// Runs a `tokio-postgres` future bounded by [`crate::resilience::remaining_rpc_deadline`],
/// returning `Err(StoreError::DeadlineExceeded)` on timeout without masking `tokio_postgres::Error`.
pub(super) async fn run_pg_bounded<T, Fut>(
    op_name: &'static str,
    fut: Fut,
) -> StoreResult<Result<T, tokio_postgres::Error>>
where
    Fut: Future<Output = Result<T, tokio_postgres::Error>>,
{
    match crate::resilience::remaining_rpc_deadline() {
        Some(rem) if rem.is_zero() => Err(StoreError::DeadlineExceeded(format!(
            "gRPC deadline expired before {op_name}"
        ))),
        Some(rem) => match tokio::time::timeout(rem, fut).await {
            Ok(res) => Ok(res),
            Err(_) => Err(StoreError::DeadlineExceeded(format!(
                "gRPC deadline exceeded during {op_name}"
            ))),
        },
        None => Ok(fut.await),
    }
}

/// Rolls back `tx` within `min(remaining_rpc_deadline, MAX_ROLLBACK_WAIT)`. Returns
/// [`RollbackOutcome::Clean`] when rollback succeeded, [`RollbackOutcome::EvictOnly`]
/// when rollback was skipped on an already-expired deadline or completed with an error, and
/// [`RollbackOutcome::CancelAndEvict`] when rollback timed out in flight.
pub(super) async fn rollback_with_deadline(
    tx: deadpool_postgres::Transaction<'_>,
) -> RollbackOutcome {
    let budget = match crate::resilience::remaining_rpc_deadline() {
        Some(rem) if rem.is_zero() => return RollbackOutcome::EvictOnly,
        Some(rem) => rem.min(MAX_ROLLBACK_WAIT),
        None => MAX_ROLLBACK_WAIT,
    };
    match tokio::time::timeout(budget, tx.rollback()).await {
        Ok(Ok(())) => RollbackOutcome::Clean,
        Ok(Err(_)) => RollbackOutcome::EvictOnly,
        Err(_) => RollbackOutcome::CancelAndEvict,
    }
}

/// Converts a positive remaining RPC deadline into a PostgreSQL `statement_timeout`
/// value in milliseconds clamped to `1..=i32::MAX`. Sub-millisecond positive durations
/// round up to `1ms` because PostgreSQL treats `0ms` as unlimited.
pub(super) fn pg_statement_timeout_ms(remaining: Duration) -> u64 {
    let millis = remaining.as_millis();
    if millis == 0 {
        1
    } else {
        millis.min(i32::MAX as u128) as u64
    }
}

pub(super) async fn apply_tx_statement_timeout(
    tx: &Transaction<'_>,
    remaining: Duration,
) -> Result<(), tokio_postgres::Error> {
    let timeout_ms = pg_statement_timeout_ms(remaining);
    let sql = format!("SET LOCAL statement_timeout = '{timeout_ms}ms'");
    // `batch_execute` uses the simple query protocol so varying millisecond values do not
    // churn prepared statements on the pooled connection.
    tx.batch_execute(&sql).await
}

impl PostgresMetadataStore {
    pub(super) async fn get_client(&self) -> StoreResult<Object> {
        match crate::resilience::remaining_rpc_deadline() {
            Some(rem) if rem.is_zero() => Err(StoreError::DeadlineExceeded(
                "gRPC deadline expired before acquiring PostgreSQL connection".to_string(),
            )),
            Some(rem) => match tokio::time::timeout(rem, self.pool.get()).await {
                Ok(res) => res.map_err(map_pool_err),
                Err(_) => Err(StoreError::DeadlineExceeded(
                    "gRPC deadline exceeded waiting for PostgreSQL connection".to_string(),
                )),
            },
            None => self.pool.get().await.map_err(map_pool_err),
        }
    }

    pub(super) async fn checkout_guarded_client(&self) -> StoreResult<EvictOnIncomplete> {
        Ok(EvictOnIncomplete::new(
            self.get_client().await?,
            self.cancel_tls.clone(),
        ))
    }

    pub(super) async fn run_tx_with_retry<T, F>(&self, mut op: F) -> StoreResult<T>
    where
        F: for<'a, 'b> FnMut(
            &'a Transaction<'b>,
        ) -> Pin<
            Box<dyn Future<Output = Result<StoreResult<T>, tokio_postgres::Error>> + Send + 'a>,
        >,
    {
        for attempt in 0..MAX_TX_ATTEMPTS {
            if crate::resilience::remaining_rpc_deadline().is_some_and(|rem| rem.is_zero()) {
                return Err(StoreError::DeadlineExceeded(
                    "gRPC deadline expired before starting PostgreSQL transaction".to_string(),
                ));
            }
            let retry_allowed = attempt + 1 < MAX_TX_ATTEMPTS;
            let outcome: Option<StoreResult<T>> = {
                let mut conn = self.checkout_guarded_client().await?;
                let (outcome, finish) = 'tx_attempt: {
                    let tx_res = match crate::resilience::remaining_rpc_deadline() {
                        Some(rem) if rem.is_zero() => {
                            conn.mark_clean();
                            return Err(StoreError::DeadlineExceeded(
                                "gRPC deadline expired after acquiring PostgreSQL connection"
                                    .to_string(),
                            ));
                        }
                        Some(rem) => {
                            conn.arm_cancel_on_drop();
                            match tokio::time::timeout(rem, conn.client_mut().transaction()).await {
                                Ok(res) => res,
                                Err(_) => {
                                    // `conn` drops without `mark_clean()`, issuing a best-effort
                                    // server-side cancel and evicting the connection via
                                    // `Object::take` because `BEGIN` may already be queued on the wire.
                                    return Err(StoreError::DeadlineExceeded(
                                        "gRPC deadline exceeded starting PostgreSQL transaction"
                                            .to_string(),
                                    ));
                                }
                            }
                        }
                        None => {
                            conn.arm_cancel_on_drop();
                            conn.client_mut().transaction().await
                        }
                    };
                    match tx_res {
                        Ok(tx) => {
                            let op_res = match crate::resilience::remaining_rpc_deadline() {
                                Some(rem) if rem.is_zero() => {
                                    drop(tx);
                                    break 'tx_attempt (
                                        Some(Err(StoreError::DeadlineExceeded(
                                            "gRPC deadline expired before running PostgreSQL transaction"
                                                .to_string(),
                                        ))),
                                        RollbackOutcome::EvictOnly,
                                    );
                                }
                                Some(rem) => {
                                    let exec_fut = async {
                                        apply_tx_statement_timeout(&tx, rem).await?;
                                        op(&tx).await
                                    };
                                    match tokio::time::timeout(rem, exec_fut).await {
                                        Ok(res) => res,
                                        Err(_) => {
                                            drop(tx);
                                            return Err(StoreError::DeadlineExceeded(
                                                "gRPC deadline exceeded during PostgreSQL transaction"
                                                    .to_string(),
                                            ));
                                        }
                                    }
                                }
                                None => op(&tx).await,
                            };
                            match op_res {
                                Ok(Ok(val)) => {
                                    let commit_res = match crate::resilience::remaining_rpc_deadline(
                                    ) {
                                        Some(rem) if rem.is_zero() => {
                                            drop(tx);
                                            break 'tx_attempt (
                                                Some(Err(StoreError::DeadlineExceeded(
                                                    "gRPC deadline expired before committing PostgreSQL transaction"
                                                        .to_string(),
                                                ))),
                                                RollbackOutcome::EvictOnly,
                                            );
                                        }
                                        Some(rem) => {
                                            match tokio::time::timeout(rem, tx.commit()).await {
                                                Ok(res) => res,
                                                Err(_) => {
                                                    return Err(StoreError::DeadlineExceeded(
                                                        "gRPC deadline exceeded committing PostgreSQL transaction"
                                                            .to_string(),
                                                    ));
                                                }
                                            }
                                        }
                                        None => tx.commit().await,
                                    };
                                    match commit_res {
                                        Ok(()) => (Some(Ok(val)), RollbackOutcome::Clean),
                                        Err(err)
                                            if is_retryable_pg_error(&err) && retry_allowed =>
                                        {
                                            let finish = if err.as_db_error().is_some() {
                                                RollbackOutcome::Clean
                                            } else {
                                                RollbackOutcome::EvictOnly
                                            };
                                            (None, finish)
                                        }
                                        Err(err) => {
                                            let finish = if err.as_db_error().is_some() {
                                                RollbackOutcome::Clean
                                            } else {
                                                RollbackOutcome::EvictOnly
                                            };
                                            (Some(Err(map_pg_err(err))), finish)
                                        }
                                    }
                                }
                                Ok(Err(store_err)) => {
                                    let finish = rollback_with_deadline(tx).await;
                                    (Some(Err(store_err)), finish)
                                }
                                Err(err) if is_retryable_pg_error(&err) && retry_allowed => {
                                    let finish = rollback_with_deadline(tx).await;
                                    (None, finish)
                                }
                                Err(err) => {
                                    let finish = rollback_with_deadline(tx).await;
                                    (Some(Err(map_pg_err(err))), finish)
                                }
                            }
                        }
                        Err(err) if is_retryable_pg_error(&err) && retry_allowed => {
                            let finish = if err.as_db_error().is_some() {
                                RollbackOutcome::Clean
                            } else {
                                RollbackOutcome::EvictOnly
                            };
                            (None, finish)
                        }
                        Err(err) => {
                            let finish = if err.as_db_error().is_some() {
                                RollbackOutcome::Clean
                            } else {
                                RollbackOutcome::EvictOnly
                            };
                            (Some(Err(map_pg_err(err))), finish)
                        }
                    }
                };
                conn.finish_tx(finish);
                outcome
            };
            // `tx` and `client` are returned (or evicted) before sleeping on retry.
            match outcome {
                Some(result) => return result,
                None => self.record_tx_retry(attempt).await?,
            }
        }
        Err(StoreError::Unavailable(
            "PostgreSQL transaction failed after maximum retry attempts".to_string(),
        ))
    }

    async fn record_tx_retry(&self, attempt: usize) -> StoreResult<()> {
        sleep_tx_retry_backoff(self.tx_retries.as_deref(), attempt).await
    }
}

pub(super) async fn sleep_tx_retry_backoff(
    tx_retries: Option<&AtomicU64>,
    attempt: usize,
) -> StoreResult<()> {
    let base_ms = TX_RETRY_BASE_DELAY_MS.saturating_mul(1u64 << attempt.min(6));
    let jitter_ms = rand::random::<u64>() % base_ms.max(1);
    let delay = Duration::from_millis(base_ms + jitter_ms);
    if let Some(rem) = crate::resilience::remaining_rpc_deadline()
        && rem <= delay
    {
        return Err(StoreError::DeadlineExceeded(
            "gRPC deadline insufficient for PostgreSQL transaction retry backoff".to_string(),
        ));
    }
    if let Some(counter) = tx_retries {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    tokio::time::sleep(delay).await;
    Ok(())
}
