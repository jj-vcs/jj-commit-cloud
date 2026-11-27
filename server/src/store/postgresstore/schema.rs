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

//! PostgreSQL schema DDL embedding and idempotent startup migration.
//!
//! # Authority and Crash-Safety Ordering
//! Serializes concurrent replica startups under `pg_advisory_xact_lock` and inspects
//! `pg_catalog` (`pg_class` relations and `projects.commit_seq`) before applying
//! `postgres_schema.sql` so rolling restarts skip redundant DDL and avoid `CREATE INDEX`
//! `SHARE` locks.

use deadpool_postgres::Object;

use super::codec::map_pg_err_with_context;
use crate::store::StoreResult;

pub(super) const POSTGRES_SCHEMA: &str = include_str!("../../../sql/postgres_schema.sql");

// Fixed 64-bit advisory lock keys for DDL migration and namespace serialization.
const SCHEMA_MIGRATION_LOCK_KEY: i64 = 0x6a6a_6363_5f73_6368; // "jjcc_sch"

const EXPECTED_SCHEMA_RELATIONS: &[&str] = &[
    "projects",
    "idx_projects_name",
    "repositories",
    "idx_repositories_project_id",
    "op_heads",
    "idx_op_heads_repo_seq",
    "commits",
    "commits_by_change_id",
    "commits_by_seq",
    "trees",
    "tree_entries",
    "symlinks",
    "blobs",
    "project_blobs",
    "views",
    "operations",
    "workspaces",
];

pub(super) async fn apply_schema(client: &mut Object) -> StoreResult<()> {
    let tx = client
        .transaction()
        .await
        .map_err(|e| map_pg_err_with_context(e, "beginning PostgreSQL schema migration tx"))?;
    tx.execute(
        "SELECT pg_advisory_xact_lock($1)",
        &[&SCHEMA_MIGRATION_LOCK_KEY],
    )
    .await
    .map_err(|e| map_pg_err_with_context(e, "acquiring PostgreSQL schema migration lock"))?;

    // Check `pg_catalog` before issuing DDL: in PostgreSQL, `CREATE INDEX IF NOT EXISTS` takes a
    // `SHARE` lock on the target table even when the index already exists, which can stall or
    // deadlock concurrent writers during rolling startups.
    let expected_count = EXPECTED_SCHEMA_RELATIONS.len() as i64;
    let already_applied: bool = tx
        .query_one(
            "SELECT
               (SELECT count(*)::BIGINT FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema() AND c.relname = ANY($1)) = $2
               AND EXISTS (
                 SELECT 1 FROM pg_attribute a
                 JOIN pg_class c ON c.oid = a.attrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = current_schema()
                   AND c.relname = 'projects'
                   AND a.attname = 'commit_seq'
                   AND NOT a.attisdropped
               )",
            &[&EXPECTED_SCHEMA_RELATIONS, &expected_count],
        )
        .await
        .map_err(|e| map_pg_err_with_context(e, "inspecting PostgreSQL schema catalog"))?
        .get(0);

    if !already_applied {
        tx.batch_execute(POSTGRES_SCHEMA)
            .await
            .map_err(|e| map_pg_err_with_context(e, "applying PostgreSQL schema DDL"))?;
    }
    tx.commit()
        .await
        .map_err(|e| map_pg_err_with_context(e, "committing PostgreSQL schema migration"))?;
    Ok(())
}
