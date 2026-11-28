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

//! Persisted environment state (`.jj-cc/<env>.json`) for `cargo xtask` lifecycle commands.
//!
//! # Authority and Crash-Safety Ordering
//! `.jj-cc/` is created with mode `0700` on Unix and state files are written atomically via a
//! `0600` sibling temporary file renamed into place, preventing torn JSON reads or permissive
//! windows when bearer tokens are stored. `"state"` is rejected as an `--env` name because
//! `"default"` owns `.jj-cc/state.json`. All `*_created` ownership flags default to `false` so
//! legacy state files never delete pre-existing shared storage.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMode {
    None,
    GcloudIdentityToken,
}

/// A background process started by `xtask up` (native server, emulator, IAP tunnel, or
/// `kubectl port-forward`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedProcess {
    pub pid: u32,
    /// Text that must appear in the process command line before `down` signals `pid`, so a PID
    /// that an unrelated process reused after a crash or reboot is never killed.
    pub command_marker: String,
}

impl ManagedProcess {
    pub fn new(pid: u32, command_marker: &str) -> Self {
        Self {
            pid,
            command_marker: command_marker.to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Endpoints {
    pub primary: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub metrics: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedResources {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub containers: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub volumes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub network: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub container_runtime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub kube_manifest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub kind_cluster: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub kind_cluster_created: bool,
    /// Kubernetes context `up` applied manifests to; `down` passes it to every `kubectl` call.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub kube_context: Option<String>,
    /// Kubernetes bearer auth Secret created by `up gke` so `down` removes it.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub gke_auth_secret: Option<String>,
    /// Bare PIDs written by earlier xtask versions, before `processes` recorded a marker.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub pids: Vec<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub processes: Vec<ManagedProcess>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub data_dirs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub cloud_run_services: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub gce_instances: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub gce_firewall_rule: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub gke_cluster: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub gke_region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub spanner_instance: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub spanner_database: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub gcs_buckets: Vec<String>,
    // `down --purge` deletes shared storage only when this environment's `up` created it. The
    // flags default to false, so state files from earlier versions never delete storage.
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub gke_cluster_created: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub spanner_instance_created: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub spanner_database_created: bool,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub gcs_buckets_created: Vec<String>,
}

impl ManagedResources {
    pub fn has_created_cloud_storage(&self) -> bool {
        self.gke_cluster_created
            || self.spanner_instance_created
            || self.spanner_database_created
            || !self.gcs_buckets_created.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvState {
    pub env_name: String,
    pub mode: String,
    pub runtime: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub project_id: Option<String>,
    pub endpoints: Endpoints,
    pub auth: AuthMode,
    /// Bearer token `up` configured on the server (`JJ_CC_SERVER_AUTH_TOKEN` or GKE Secret);
    /// `status`, `smoke`, and `bench` send it.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub auth_token: Option<String>,
    /// True after `down` stopped compute while retaining created cloud storage metadata (either on
    /// non-purge `down`, or on `down --purge` when a created Spanner instance still holds another
    /// environment's database) so a later `down --purge` can still delete it.
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub compute_stopped: bool,
    pub created_unix_secs: u64,
    pub resources: ManagedResources,
}

impl EnvState {
    pub fn new(
        env_name: &str,
        mode: &str,
        runtime: &str,
        primary_url: &str,
        peer_url: Option<String>,
        auth: AuthMode,
    ) -> Self {
        let created_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            env_name: env_name.to_string(),
            mode: mode.to_string(),
            runtime: runtime.to_string(),
            project_id: None,
            endpoints: Endpoints {
                primary: primary_url.to_string(),
                peer: peer_url,
                metrics: None,
            },
            auth,
            auth_token: None,
            compute_stopped: false,
            created_unix_secs,
            resources: ManagedResources::default(),
        }
    }
}

pub fn workspace_root() -> PathBuf {
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let p = PathBuf::from(manifest_dir);
        if let Some(parent) = p.parent() {
            return parent.to_path_buf();
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

pub fn cargo_target_dir(root: &Path) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"))
}

pub fn validate_env_name(env_name: &str) -> Result<String, String> {
    if env_name == "state" {
        return Err(
            "invalid environment name 'state': reserved because 'default' owns `.jj-cc/state.json`"
                .to_string(),
        );
    }
    if env_name.is_empty()
        || !env_name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(format!(
            "invalid environment name '{env_name}': must be non-empty and contain only ASCII letters, digits, '-', or '_'"
        ));
    }
    Ok(env_name.to_string())
}

pub fn state_file_path(root: &Path, env_name: &str) -> PathBuf {
    let dir = root.join(".jj-cc");
    if env_name == "default" {
        dir.join("state.json")
    } else {
        dir.join(format!("{env_name}.json"))
    }
}

/// Ensures `dir` exists and is readable/writable only by the current user (`0700` on Unix).
pub fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub fn save_state(root: &Path, state: &EnvState) -> Result<PathBuf, BoxError> {
    validate_env_name(&state.env_name)?;
    let path = state_file_path(root, &state.env_name);
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }
    let body = serde_json::to_string_pretty(state)?;
    write_private_file(&path, format!("{body}\n").as_bytes())?;
    Ok(path)
}

/// Atomically writes `contents` via a `0600` sibling temporary file renamed into place, because
/// state files can hold a server bearer token and must never be left truncated on interruption.
fn write_private_file(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = tempfile::Builder::new()
        .prefix(".state-")
        .tempfile_in(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tmp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    tmp.write_all(contents)?;
    tmp.flush()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

pub fn load_state(root: &Path, env_name: &str) -> Result<Option<EnvState>, BoxError> {
    validate_env_name(env_name)?;
    let path = state_file_path(root, env_name);
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&path)?;
    let parsed: EnvState = serde_json::from_str(&raw)?;
    Ok(Some(parsed))
}

pub fn remove_state(root: &Path, env_name: &str) -> Result<(), BoxError> {
    validate_env_name(env_name)?;
    let path = state_file_path(root, env_name);
    if path.exists() {
        fs::remove_file(&path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_env_uses_state_json_and_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let mut st = EnvState::new(
            "default",
            "multiregion",
            "podman",
            "http://127.0.0.1:8081",
            Some("http://127.0.0.1:8082".to_string()),
            AuthMode::None,
        );
        st.resources.containers = vec!["jj-cc-default-us".to_string()];

        let saved_path = save_state(tmp.path(), &st).unwrap();
        assert_eq!(saved_path, tmp.path().join(".jj-cc").join("state.json"));

        let loaded = load_state(tmp.path(), "default").unwrap().unwrap();
        assert_eq!(loaded, st);

        remove_state(tmp.path(), "default").unwrap();
        assert!(load_state(tmp.path(), "default").unwrap().is_none());
    }

    #[test]
    fn named_env_uses_dedicated_file() {
        let tmp = tempfile::tempdir().unwrap();
        let st = EnvState::new(
            "gcp",
            "cloud-run",
            "gcloud",
            "https://jj-cc-server.a.run.app",
            None,
            AuthMode::GcloudIdentityToken,
        );
        let saved_path = save_state(tmp.path(), &st).unwrap();
        assert_eq!(saved_path, tmp.path().join(".jj-cc").join("gcp.json"));
        assert_eq!(load_state(tmp.path(), "gcp").unwrap(), Some(st));
    }

    #[test]
    fn validate_env_name_rejects_reserved_state_name() {
        let err = validate_env_name("state").unwrap_err();
        assert!(
            err.contains("default") && err.contains("state.json"),
            "unexpected error: {err}"
        );
        assert!(validate_env_name("").is_err());
        assert!(validate_env_name("../escape").is_err());
        assert_eq!(validate_env_name("default").unwrap(), "default");
        assert_eq!(validate_env_name("dev-1").unwrap(), "dev-1");
    }

    #[test]
    fn ownership_processes_and_token_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut st = EnvState::new(
            "gce",
            "gce",
            "gcloud",
            "http://203.0.113.7:8080",
            None,
            AuthMode::None,
        );
        st.auth_token = Some("secret-token".to_string());
        st.resources.processes = vec![ManagedProcess {
            pid: 4242,
            command_marker: "start-iap-tunnel jj-cc-gce-us-central1".to_string(),
        }];
        st.resources.kube_context = Some("gke_p_us-central1_jj-cc-cluster-gce".to_string());
        st.resources.gke_auth_secret = Some("jj-cc-server-auth".to_string());
        st.resources.kind_cluster_created = true;
        st.resources.spanner_instance = Some("jj-cc-dev".to_string());
        st.resources.spanner_database = Some("commit-cloud-gce".to_string());
        st.resources.spanner_database_created = true;
        st.resources.gcs_buckets = vec!["shared".to_string(), "mine".to_string()];
        st.resources.gcs_buckets_created = vec!["mine".to_string()];
        save_state(tmp.path(), &st).unwrap();
        assert_eq!(load_state(tmp.path(), "gce").unwrap(), Some(st));
    }

    #[test]
    fn legacy_state_without_ownership_fields_never_claims_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let legacy = r#"{
            "env_name": "old",
            "mode": "cloud-run",
            "runtime": "gcloud",
            "project_id": "p",
            "endpoints": { "primary": "https://jj-cc-server.a.run.app" },
            "auth": "gcloud-identity-token",
            "created_unix_secs": 1,
            "resources": {
                "pids": [123],
                "spanner_instance": "jj-cc-dev",
                "spanner_database": "commit-cloud",
                "gcs_buckets": ["p-jj-blobs-us-central1"]
            }
        }"#;
        fs::create_dir_all(tmp.path().join(".jj-cc")).unwrap();
        fs::write(state_file_path(tmp.path(), "old"), legacy).unwrap();
        let st = load_state(tmp.path(), "old").unwrap().unwrap();
        assert_eq!(st.resources.pids, vec![123]);
        assert!(st.resources.processes.is_empty());
        assert!(!st.resources.spanner_instance_created);
        assert!(!st.resources.spanner_database_created);
        assert!(!st.resources.kind_cluster_created);
        assert!(!st.compute_stopped);
        assert!(st.resources.gcs_buckets_created.is_empty());
        assert_eq!(st.auth_token, None);
    }

    #[cfg(unix)]
    #[test]
    fn state_dir_and_file_permissions_are_private_and_atomic() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join(".jj-cc");
        let path = state_file_path(tmp.path(), "default");
        fs::create_dir_all(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(&path, "{}").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();

        let st = EnvState::new("default", "gce", "gcloud", "http://x", None, AuthMode::None);
        save_state(tmp.path(), &st).unwrap();

        let dir_mode = fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(dir_mode & 0o777, 0o700);
        let file_mode = fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(file_mode & 0o777, 0o600);

        // Atomic persist leaves no sibling `.state-*` temp files behind.
        let leftover: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(".state-"))
            .collect();
        assert!(leftover.is_empty(), "leftover temp files: {leftover:?}");
    }
}
