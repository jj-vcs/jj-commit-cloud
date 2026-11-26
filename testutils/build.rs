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

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../server/src/");
    println!("cargo:rerun-if-changed=../server/Cargo.toml");
    println!("cargo:rerun-if-changed=../daemon/src/");
    println!("cargo:rerun-if-changed=../daemon/Cargo.toml");
    println!("cargo:rerun-if-changed=../lib/src/");
    println!("cargo:rerun-if-changed=../lib/Cargo.toml");
    println!("cargo:rerun-if-changed=../common/src/");
    println!("cargo:rerun-if-changed=../common/Cargo.toml");
    println!("cargo:rerun-if-changed=../common/proto/");

    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR should be set");
    let out_path = Path::new(&out_dir);

    // Use a separate target directory for the child Cargo invocation to avoid Cargo lock deadlocks.
    let server_target_dir = out_path.join("server_target");
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR should be set");
    let workspace_manifest = Path::new(&manifest_dir)
        .parent()
        .unwrap()
        .join("Cargo.toml");

    let mut cmd = Command::new(cargo);
    cmd.arg("build")
        .arg("--manifest-path")
        .arg(&workspace_manifest)
        .args([
            "-p",
            "jj-commit-cloud-server",
            "--bin",
            "jj-cc-server",
            "-p",
            "jj-commit-cloud-daemon",
            "--bin",
            "jj-cc-daemon",
        ]);
    if profile == "release" {
        cmd.arg("--release");
    }
    cmd.env("CARGO_TARGET_DIR", &server_target_dir);
    // `cargo clippy` runs build scripts with clippy-driver as the rustc wrapper; the nested
    // build must compile the helper binaries with plain rustc.
    for var in ["RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CLIPPY_ARGS"] {
        cmd.env_remove(var);
    }

    let status = cmd
        .status()
        .expect("Failed to execute cargo build for test helper binaries");

    if !status.success() {
        panic!("Failed to build test helper binaries");
    }

    let exe_suffix = std::env::consts::EXE_SUFFIX;
    let server_name = format!("jj-cc-server{exe_suffix}");
    let daemon_name = format!("jj-cc-daemon{exe_suffix}");

    let compiled_binary_path = server_target_dir.join(&profile).join(&server_name);
    assert!(
        compiled_binary_path.exists(),
        "Compiled jj-cc-server binary should exist at {}",
        compiled_binary_path.display()
    );

    let compiled_daemon_path = server_target_dir.join(&profile).join(&daemon_name);
    assert!(
        compiled_daemon_path.exists(),
        "Compiled jj-cc-daemon binary should exist at {}",
        compiled_daemon_path.display()
    );

    println!(
        "cargo:rustc-env=JJ_CC_SERVER_BIN={}",
        compiled_binary_path.display()
    );
    println!(
        "cargo:rustc-env=JJ_CC_DAEMON_BIN={}",
        compiled_daemon_path.display()
    );

    // Also copy into `target/<profile>/` if an ancestor matches `<profile>`
    // (handles both legacy `build/<pkg>-<hash>/out` and Cargo 1.98+ `build/<pkg>/<hash>/out`).
    if let Some(profile_dir) = out_path
        .ancestors()
        .find(|p| p.file_name().and_then(|n| n.to_str()) == Some(profile.as_str()))
    {
        for name in [&server_name, &daemon_name] {
            let dest = profile_dir.join(name);
            let _ = fs::remove_file(&dest);
            let _ = fs::copy(server_target_dir.join(&profile).join(name), dest);
        }
    }
}
