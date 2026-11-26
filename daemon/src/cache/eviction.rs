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

//! Startup cleanup of orphaned L2 staging temporary files.
//!
//! # Authority and Crash-Safety Ordering
//! On daemon startup, `cleanup_stale_staging_files` scans `<cache_dir>/staging` for regular
//! files matching `.tmp.<pid>.<seq>` whose owning PID is provably dead (`kill(pid, 0)` returns
//! `ESRCH` on Unix) and unlinks them so crashed runs do not leak staging files.
//!
//! # Cross-Process and RPC Boundaries
//! Staging files belonging to the current process or any live PID are left untouched so
//! multiple daemons sharing `<cache_dir>` never remove each other's in-flight uploads or reads.

use std::fs;
use std::path::Path;

/// Best-effort startup sweep removing crash-orphaned `.tmp.<pid>.<seq>` staging files whose
/// owning PID is provably dead, while leaving files belonging to live daemons (which may share
/// the same `<cache_dir>`) untouched.
pub(super) fn cleanup_stale_staging_files(staging_dir: &Path) {
    #[cfg(unix)]
    {
        let Ok(entries) = fs::read_dir(staging_dir) else {
            return;
        };
        let current_pid = std::process::id();
        for entry in entries.flatten() {
            let Ok(ft) = entry.file_type() else {
                continue;
            };
            if !ft.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };
            let Some(pid) = parse_staging_temp_pid(name) else {
                continue;
            };
            if pid == current_pid || !is_pid_dead(pid) {
                continue;
            }
            let _ = fs::remove_file(entry.path());
        }
    }
    #[cfg(not(unix))]
    {
        let _ = staging_dir;
    }
}

#[cfg(unix)]
pub(super) fn parse_staging_temp_pid(name: &str) -> Option<u32> {
    let rest = name.strip_prefix(".tmp.")?;
    let (pid_str, seq_str) = rest.split_once('.')?;
    if seq_str.is_empty() || !seq_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let pid: u32 = pid_str.parse().ok()?;
    if pid == 0 {
        return None;
    }
    libc::pid_t::try_from(pid).ok().filter(|&p| p > 0)?;
    Some(pid)
}

#[cfg(unix)]
pub(super) fn is_pid_dead(pid: u32) -> bool {
    let Ok(pid_t) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid_t <= 0 {
        return false;
    }
    // SAFETY: `kill(pid, 0)` with `pid > 0` delivers no signal and only probes process existence.
    let rc = unsafe { libc::kill(pid_t, 0) };
    rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}
