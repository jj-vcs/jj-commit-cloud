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

//! Per-repository kill switch controlling server-side op-head reconciliation modes.
//!
//! # Concurrency and Merge Invariants
//! Stores per-repository overrides (`Bypass` and `Quarantine`) behind a shared
//! `RwLock` map while defaulting unconfigured repositories to `Normal`. Parsing
//! rejects control characters, invalid repository identifiers, and conflicting
//! mode assignments so operator configuration fails closed.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::backend::validate_identifier;

/// Per-repository circuit-breaker mode governing server-side op-head reconciliation.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum ReconcileRepoMode {
    /// Default mode: execute server-side 3-way view and tree reconciliation normally.
    #[default]
    Normal,
    /// Bypass mode: on divergent heads (`heads.len() > 1`), skip server-side `jj_lib`
    /// merging and return `client_resolution_required = true` with active
    /// `current_op_head_ids` for off-server CLI recovery. Converged repositories
    /// (`heads.len() <= 1`) still take the zero-merge single-head fast path.
    Bypass,
    /// Quarantine mode: fail closed immediately with non-retryable `FAILED_PRECONDITION`
    /// before reading or mutating repository state so automated client merge storms halt.
    /// `GetOpHeads` remains pure and unaffected for operator inspection.
    Quarantine,
}

/// Thread-safe, cheaply cloneable per-repository reconciliation kill switch.
///
/// Tracks repositories in [`ReconcileRepoMode::Bypass`] or [`ReconcileRepoMode::Quarantine`];
/// repositories absent from the map default to [`ReconcileRepoMode::Normal`].
#[derive(Clone, Debug, Default)]
pub struct ReconcileKillSwitch {
    modes: Arc<RwLock<HashMap<String, ReconcileRepoMode>>>,
}

impl ReconcileKillSwitch {
    /// Creates an empty kill switch where every repository runs in [`ReconcileRepoMode::Normal`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses and validates per-repository bypass and quarantine lists (for example from
    /// `--reconcile-bypass-repos` and `--reconcile-quarantine-repos`).
    ///
    /// Rejects ASCII control characters (including `\n`, `\r`, `\t`, `\0`) before trimming,
    /// splits comma-delimited entries, trims surrounding ASCII spaces, ignores empty entries,
    /// validates each `repo_id` via `validate_identifier`, and fails closed if any repository
    /// appears in both bypass and quarantine lists.
    pub fn try_from_repo_lists<I1, S1, I2, S2>(
        bypass_repos: I1,
        quarantine_repos: I2,
    ) -> Result<Self, String>
    where
        I1: IntoIterator<Item = S1>,
        S1: AsRef<str>,
        I2: IntoIterator<Item = S2>,
        S2: AsRef<str>,
    {
        let mut modes = HashMap::new();
        Self::insert_validated_entries(
            &mut modes,
            bypass_repos,
            ReconcileRepoMode::Bypass,
            "--reconcile-bypass-repos",
        )?;
        Self::insert_validated_entries(
            &mut modes,
            quarantine_repos,
            ReconcileRepoMode::Quarantine,
            "--reconcile-quarantine-repos",
        )?;
        Ok(Self {
            modes: Arc::new(RwLock::new(modes)),
        })
    }

    fn insert_validated_entries<I, S>(
        modes: &mut HashMap<String, ReconcileRepoMode>,
        entries: I,
        target_mode: ReconcileRepoMode,
        flag_name: &str,
    ) -> Result<(), String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for raw_entry in entries {
            let raw = raw_entry.as_ref();
            if raw.bytes().any(|b| b.is_ascii_control()) {
                return Err(format!(
                    "invalid {flag_name} entry {raw:?}: ASCII control characters are not allowed"
                ));
            }
            for part in raw.split(',') {
                let trimmed = part.trim_matches(' ');
                if trimmed.is_empty() {
                    continue;
                }
                validate_identifier(trimmed, "repo_id").map_err(|status| {
                    format!(
                        "invalid {flag_name} repo_id '{trimmed}': {}",
                        status.message()
                    )
                })?;
                if let Some(&existing) = modes.get(trimmed)
                    && existing != target_mode
                {
                    return Err(format!(
                        "repository '{trimmed}' cannot be configured in both \
                         --reconcile-bypass-repos and --reconcile-quarantine-repos"
                    ));
                }
                modes.insert(trimmed.to_string(), target_mode);
            }
        }
        Ok(())
    }

    /// Validates `repo_id` and sets its reconciliation mode. Setting
    /// [`ReconcileRepoMode::Normal`] removes the override entry so the map stays compact.
    pub fn set_mode(&self, repo_id: &str, mode: ReconcileRepoMode) -> Result<(), String> {
        validate_identifier(repo_id, "repo_id")
            .map_err(|status| format!("invalid repo_id {repo_id:?}: {}", status.message()))?;
        let mut guard = self.modes.write().unwrap_or_else(|e| e.into_inner());
        if mode == ReconcileRepoMode::Normal {
            guard.remove(repo_id);
        } else {
            guard.insert(repo_id.to_string(), mode);
        }
        Ok(())
    }

    /// Returns the active reconciliation mode for `repo_id` (defaults to
    /// [`ReconcileRepoMode::Normal`]).
    pub fn mode_for(&self, repo_id: &str) -> ReconcileRepoMode {
        self.modes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(repo_id)
            .copied()
            .unwrap_or(ReconcileRepoMode::Normal)
    }

    /// Returns `true` when no repositories are overridden from [`ReconcileRepoMode::Normal`].
    pub fn is_empty(&self) -> bool {
        self.modes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    /// Returns `(bypass_count, quarantine_count)` for observability and startup logging.
    pub fn counts(&self) -> (usize, usize) {
        let guard = self.modes.read().unwrap_or_else(|e| e.into_inner());
        let mut bypass = 0usize;
        let mut quarantine = 0usize;
        for mode in guard.values() {
            match mode {
                ReconcileRepoMode::Bypass => bypass += 1,
                ReconcileRepoMode::Quarantine => quarantine += 1,
                ReconcileRepoMode::Normal => {}
            }
        }
        (bypass, quarantine)
    }

    /// Returns a sorted snapshot of repository IDs currently in [`ReconcileRepoMode::Bypass`].
    pub fn bypass_repos(&self) -> Vec<String> {
        let guard = self.modes.read().unwrap_or_else(|e| e.into_inner());
        let mut repos: Vec<String> = guard
            .iter()
            .filter(|(_, mode)| **mode == ReconcileRepoMode::Bypass)
            .map(|(repo, _)| repo.clone())
            .collect();
        repos.sort();
        repos
    }

    /// Returns a sorted snapshot of repository IDs currently in [`ReconcileRepoMode::Quarantine`].
    pub fn quarantine_repos(&self) -> Vec<String> {
        let guard = self.modes.read().unwrap_or_else(|e| e.into_inner());
        let mut repos: Vec<String> = guard
            .iter()
            .filter(|(_, mode)| **mode == ReconcileRepoMode::Quarantine)
            .map(|(repo, _)| repo.clone())
            .collect();
        repos.sort();
        repos
    }
}
