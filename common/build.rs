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
use std::process::Command;

fn ensure_protoc() -> Result<(), Box<dyn std::error::Error>> {
    if env::var_os("PROTOC").is_some() {
        return Ok(());
    }
    let system_protoc_ok = Command::new("protoc")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !system_protoc_ok {
        let vendored = protoc_bin_vendored::protoc_bin_path()?;
        env::set_var("PROTOC", vendored);
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/backend.proto");
    println!("cargo:rerun-if-changed=proto/op_store.proto");
    println!("cargo:rerun-if-changed=proto/workspace.proto");
    ensure_protoc()?;
    tonic_build::configure().compile_protos(
        &[
            "proto/backend.proto",
            "proto/op_store.proto",
            "proto/workspace.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
