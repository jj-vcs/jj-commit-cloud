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

//! PostgreSQL tree, symlink, and blob metadata persistence.
//!
//! # Authority and Crash-Safety Ordering
//! Stages wide trees as `trees(ready = FALSE)`, batch-inserts `tree_entries` via `UNNEST`, and
//! flips `ready = TRUE` atomically before readers observe the tree. Merges global `blobs` rows
//! with `project_blobs` links inside a retryable transaction without dropping concurrent replica
//! locations.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cc_common::backend::TreeEntry;
use prost::Message as _;
use tokio_postgres::error::SqlState;

use super::codec::{decode_blob_row, map_pg_err};
use super::pool::run_pg_bounded;
use super::{PostgresMetadataStore, TREE_ENTRY_BATCH_SIZE, UNNEST_BATCH_SIZE};
use crate::store::{BlobRecord, StoreError, StoreResult};

impl PostgresMetadataStore {
    pub(super) async fn get_tree_impl(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        let mut conn = self.checkout_guarded_client().await?;
        conn.arm_for_query("PostgreSQL read")?;
        let exists_res = run_pg_bounded(
            "PostgreSQL read",
            conn.client().query_opt(
                "SELECT 1 FROM trees WHERE project_id = $1 AND tree_id = $2 AND ready = TRUE",
                &[&project_id, &tree_id],
            ),
        )
        .await?;
        let exists = match exists_res {
            Ok(row) => {
                conn.disarm_cancel_on_drop();
                row
            }
            Err(err) => {
                conn.mark_clean_if_db_error(&err);
                return Err(map_pg_err(err));
            }
        };
        if exists.is_none() {
            conn.mark_clean();
            return Ok(None);
        }
        conn.arm_for_query("PostgreSQL read")?;
        let res = run_pg_bounded(
            "PostgreSQL read",
            conn.client().query(
                "SELECT proto_bytes FROM tree_entries
                 WHERE project_id = $1 AND tree_id = $2
                 ORDER BY name COLLATE \"C\" ASC",
                &[&project_id, &tree_id],
            ),
        )
        .await?;
        conn.mark_clean_if_healthy(&res);
        let rows = res.map_err(map_pg_err)?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let bytes: Vec<u8> = row.get(0);
            let entry = TreeEntry::decode(bytes.as_slice()).map_err(|e| {
                StoreError::Corruption(format!(
                    "failed to decode PostgreSQL TreeEntry protobuf: {e}"
                ))
            })?;
            entries.push(entry);
        }
        Ok(Some(entries))
    }

    pub(super) async fn get_tree_page_impl(
        &self,
        project_id: &str,
        tree_id: &[u8],
        after_name: Option<&str>,
        limit: usize,
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        let mut conn = self.checkout_guarded_client().await?;
        conn.arm_for_query("PostgreSQL read")?;
        let exists_res = run_pg_bounded(
            "PostgreSQL read",
            conn.client().query_opt(
                "SELECT 1 FROM trees WHERE project_id = $1 AND tree_id = $2 AND ready = TRUE",
                &[&project_id, &tree_id],
            ),
        )
        .await?;
        let exists = match exists_res {
            Ok(row) => {
                conn.disarm_cancel_on_drop();
                row
            }
            Err(err) => {
                conn.mark_clean_if_db_error(&err);
                return Err(map_pg_err(err));
            }
        };
        if exists.is_none() {
            conn.mark_clean();
            return Ok(None);
        }

        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        conn.arm_for_query("PostgreSQL read")?;
        let res = match after_name.filter(|s| !s.is_empty()) {
            Some(after) => {
                run_pg_bounded(
                    "PostgreSQL read",
                    conn.client().query(
                        "SELECT proto_bytes FROM tree_entries
                         WHERE project_id = $1 AND tree_id = $2 AND name COLLATE \"C\" > $3 COLLATE \"C\"
                         ORDER BY name COLLATE \"C\" ASC
                         LIMIT $4",
                        &[&project_id, &tree_id, &after, &limit_i64],
                    ),
                )
                .await?
            }
            None => {
                run_pg_bounded(
                    "PostgreSQL read",
                    conn.client().query(
                        "SELECT proto_bytes FROM tree_entries
                         WHERE project_id = $1 AND tree_id = $2
                         ORDER BY name COLLATE \"C\" ASC
                         LIMIT $3",
                        &[&project_id, &tree_id, &limit_i64],
                    ),
                )
                .await?
            }
        };
        conn.mark_clean_if_healthy(&res);
        let rows = res.map_err(map_pg_err)?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let bytes: Vec<u8> = row.get(0);
            let entry = TreeEntry::decode(bytes.as_slice()).map_err(|e| {
                StoreError::Corruption(format!(
                    "failed to decode PostgreSQL TreeEntry protobuf: {e}"
                ))
            })?;
            entries.push(entry);
        }
        Ok(Some(entries))
    }

    pub(super) async fn put_tree_impl(
        &self,
        project_id: &str,
        tree_id: &[u8],
        entries: &[TreeEntry],
    ) -> StoreResult<()> {
        // Fast path: trees are content-addressed and immutable once ready.
        let already_ready = {
            let mut conn = self.checkout_guarded_client().await?;
            conn.arm_for_query("PostgreSQL read")?;
            let res = run_pg_bounded(
                "PostgreSQL read",
                conn.client().query_opt(
                    "SELECT ready FROM trees WHERE project_id = $1 AND tree_id = $2",
                    &[&project_id, &tree_id],
                ),
            )
            .await?;
            conn.mark_clean_if_healthy(&res);
            let ready_row = res.map_err(map_pg_err)?;
            ready_row.is_some_and(|r| r.get::<_, bool>(0))
        };
        if already_ready {
            return Ok(());
        }

        let project_id = project_id.to_string();
        let tree_id = tree_id.to_vec();
        let encoded_entries: Arc<Vec<(String, Vec<u8>)>> = Arc::new(
            entries
                .iter()
                .map(|e| (e.name.clone(), e.encode_to_vec()))
                .collect(),
        );

        self.run_tx_with_retry(|tx| {
            let project_id = project_id.clone();
            let tree_id = tree_id.clone();
            let encoded_entries = Arc::clone(&encoded_entries);
            Box::pin(async move {
                if let Err(err) = tx
                    .execute(
                        "INSERT INTO trees (project_id, tree_id, ready)
                         VALUES ($1, $2, FALSE)
                         ON CONFLICT (project_id, tree_id) DO NOTHING",
                        &[&project_id, &tree_id],
                    )
                    .await
                {
                    if err.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) {
                        return Ok(Err(StoreError::RepoNotFound {
                            repo_id: project_id,
                            action: "writing tree".to_string(),
                        }));
                    }
                    return Err(err);
                }

                let row = tx
                    .query_one(
                        "SELECT ready FROM trees
                         WHERE project_id = $1 AND tree_id = $2 FOR UPDATE",
                        &[&project_id, &tree_id],
                    )
                    .await?;
                if row.get::<_, bool>(0) {
                    return Ok(Ok(()));
                }

                for chunk in encoded_entries.chunks(TREE_ENTRY_BATCH_SIZE) {
                    let names: Vec<&str> = chunk.iter().map(|(n, _)| n.as_str()).collect();
                    let protos: Vec<&[u8]> = chunk.iter().map(|(_, p)| p.as_slice()).collect();
                    tx.execute(
                        "INSERT INTO tree_entries (project_id, tree_id, name, proto_bytes)
                         SELECT $1, $2, n, p
                         FROM UNNEST($3::TEXT[], $4::BYTEA[]) AS t(n, p)
                         ON CONFLICT (project_id, tree_id, name) DO NOTHING",
                        &[&project_id, &tree_id, &names, &protos],
                    )
                    .await?;
                }

                tx.execute(
                    "UPDATE trees SET ready = TRUE WHERE project_id = $1 AND tree_id = $2",
                    &[&project_id, &tree_id],
                )
                .await?;
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(super) async fn find_existing_trees_impl(
        &self,
        project_id: &str,
        tree_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        if tree_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let mut conn = self.checkout_guarded_client().await?;
        let mut found = HashSet::with_capacity(tree_ids.len());
        for chunk in tree_ids.chunks(UNNEST_BATCH_SIZE) {
            conn.arm_for_query("PostgreSQL read")?;
            let res = run_pg_bounded(
                "PostgreSQL read",
                conn.client().query(
                    "SELECT tree_id FROM trees
                     WHERE project_id = $1 AND ready = TRUE AND tree_id = ANY($2)",
                    &[&project_id, &chunk],
                ),
            )
            .await?;
            let rows = match res {
                Ok(rows) => {
                    conn.disarm_cancel_on_drop();
                    rows
                }
                Err(err) => {
                    conn.mark_clean_if_db_error(&err);
                    return Err(map_pg_err(err));
                }
            };
            for row in rows {
                found.insert(row.get::<_, Vec<u8>>(0));
            }
        }
        conn.mark_clean();
        Ok(found)
    }

    pub(super) async fn get_symlink_impl(
        &self,
        project_id: &str,
        symlink_id: &[u8],
    ) -> StoreResult<Option<String>> {
        let mut conn = self.checkout_guarded_client().await?;
        conn.arm_for_query("PostgreSQL read")?;
        let res = run_pg_bounded(
            "PostgreSQL read",
            conn.client().query_opt(
                "SELECT target FROM symlinks WHERE project_id = $1 AND symlink_id = $2",
                &[&project_id, &symlink_id],
            ),
        )
        .await?;
        conn.mark_clean_if_healthy(&res);
        let row = res.map_err(map_pg_err)?;
        Ok(row.map(|r| r.get(0)))
    }

    pub(super) async fn put_symlink_impl(
        &self,
        project_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> StoreResult<()> {
        if target.contains('\0') {
            return Err(StoreError::InvalidArgument(
                "symlink target must not contain NUL bytes".to_string(),
            ));
        }
        let mut conn = self.checkout_guarded_client().await?;
        conn.arm_for_query("PostgreSQL write")?;
        let res = run_pg_bounded(
            "PostgreSQL write",
            conn.client().execute(
                "INSERT INTO symlinks (project_id, symlink_id, target)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (project_id, symlink_id) DO NOTHING",
                &[&project_id, &symlink_id, &target],
            ),
        )
        .await?;
        conn.mark_clean_if_healthy(&res);
        if let Err(err) = res {
            if err.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) {
                return Err(StoreError::RepoNotFound {
                    repo_id: project_id.to_string(),
                    action: "writing symlink".to_string(),
                });
            }
            return Err(map_pg_err(err));
        }
        Ok(())
    }

    pub(super) async fn find_existing_symlinks_impl(
        &self,
        project_id: &str,
        symlink_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        if symlink_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let mut conn = self.checkout_guarded_client().await?;
        let mut found = HashSet::with_capacity(symlink_ids.len());
        for chunk in symlink_ids.chunks(UNNEST_BATCH_SIZE) {
            conn.arm_for_query("PostgreSQL read")?;
            let res = run_pg_bounded(
                "PostgreSQL read",
                conn.client().query(
                    "SELECT symlink_id FROM symlinks
                     WHERE project_id = $1 AND symlink_id = ANY($2)",
                    &[&project_id, &chunk],
                ),
            )
            .await?;
            let rows = match res {
                Ok(rows) => {
                    conn.disarm_cancel_on_drop();
                    rows
                }
                Err(err) => {
                    conn.mark_clean_if_db_error(&err);
                    return Err(map_pg_err(err));
                }
            };
            for row in rows {
                found.insert(row.get::<_, Vec<u8>>(0));
            }
        }
        conn.mark_clean();
        Ok(found)
    }

    pub(super) async fn get_blob_record_impl(
        &self,
        project_id: &str,
        blob_id: &[u8],
    ) -> StoreResult<Option<BlobRecord>> {
        let mut conn = self.checkout_guarded_client().await?;
        conn.arm_for_query("PostgreSQL read")?;
        let res = run_pg_bounded(
            "PostgreSQL read",
            conn.client().query_opt(
                "SELECT b.blob_id, b.content_size, b.inline_content, b.primary_location, b.replicated_locations
                 FROM blobs b
                 INNER JOIN project_blobs pb ON pb.blob_id = b.blob_id
                 WHERE pb.project_id = $1 AND b.blob_id = $2",
                &[&project_id, &blob_id],
            ),
        )
        .await?;
        conn.mark_clean_if_healthy(&res);
        let row = res.map_err(map_pg_err)?;
        row.as_ref().map(decode_blob_row).transpose()
    }

    pub(super) async fn get_blob_sizes_batch_impl(
        &self,
        project_id: &str,
        blob_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        if blob_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut conn = self.checkout_guarded_client().await?;
        let mut sizes = HashMap::with_capacity(blob_ids.len());
        for chunk in blob_ids.chunks(UNNEST_BATCH_SIZE) {
            conn.arm_for_query("PostgreSQL read")?;
            let res = run_pg_bounded(
                "PostgreSQL read",
                conn.client().query(
                    "SELECT b.blob_id, b.content_size
                     FROM blobs b
                     INNER JOIN project_blobs pb ON pb.blob_id = b.blob_id
                     WHERE pb.project_id = $1 AND pb.blob_id = ANY($2)",
                    &[&project_id, &chunk],
                ),
            )
            .await?;
            let rows = match res {
                Ok(rows) => {
                    conn.disarm_cancel_on_drop();
                    rows
                }
                Err(err) => {
                    conn.mark_clean_if_db_error(&err);
                    return Err(map_pg_err(err));
                }
            };
            for row in rows {
                let bid: Vec<u8> = row.get(0);
                let sz: i64 = row.get(1);
                let Ok(size_u64) = u64::try_from(sz) else {
                    conn.mark_clean();
                    return Err(StoreError::Corruption(format!(
                        "negative PostgreSQL blob content_size {sz}"
                    )));
                };
                sizes.insert(bid, size_u64);
            }
        }
        conn.mark_clean();
        Ok(sizes)
    }

    pub(super) async fn get_global_blob_record_impl(
        &self,
        blob_id: &[u8],
    ) -> StoreResult<Option<BlobRecord>> {
        let mut conn = self.checkout_guarded_client().await?;
        conn.arm_for_query("PostgreSQL read")?;
        let res = run_pg_bounded(
            "PostgreSQL read",
            conn.client().query_opt(
                "SELECT blob_id, content_size, inline_content, primary_location, replicated_locations
                 FROM blobs WHERE blob_id = $1",
                &[&blob_id],
            ),
        )
        .await?;
        conn.mark_clean_if_healthy(&res);
        let row = res.map_err(map_pg_err)?;
        row.as_ref().map(decode_blob_row).transpose()
    }

    pub(super) async fn put_blob_record_impl(
        &self,
        project_id: &str,
        record: &BlobRecord,
    ) -> StoreResult<()> {
        let content_size_i64 = i64::try_from(record.content_size).map_err(|_| {
            StoreError::InvalidArgument(format!(
                "blob content_size {} exceeds i64::MAX",
                record.content_size
            ))
        })?;
        let project_id = project_id.to_string();
        let blob_id = record.blob_id.clone();
        let incoming_inline: Option<Vec<u8>> = record.inline_content.as_ref().map(|b| b.to_vec());
        let incoming_primary = record.primary_location.clone();
        let mut deduped_incoming = Vec::with_capacity(record.replicated_locations.len());
        for loc in &record.replicated_locations {
            if !deduped_incoming.iter().any(|l| l == loc) {
                deduped_incoming.push(loc.clone());
            }
        }

        self.run_tx_with_retry(|tx| {
            let project_id = project_id.clone();
            let blob_id = blob_id.clone();
            let incoming_inline = incoming_inline.clone();
            let incoming_primary = incoming_primary.clone();
            let deduped_incoming = deduped_incoming.clone();
            Box::pin(async move {
                let inserted = tx
                    .execute(
                        "INSERT INTO blobs
                         (blob_id, content_size, inline_content, primary_location, replicated_locations)
                         VALUES ($1, $2, $3, $4, $5)
                         ON CONFLICT (blob_id) DO NOTHING",
                        &[
                            &blob_id,
                            &content_size_i64,
                            &incoming_inline,
                            &incoming_primary,
                            &deduped_incoming,
                        ],
                    )
                    .await?;

                if inserted == 0 {
                    let row = tx
                        .query_one(
                            "SELECT inline_content, primary_location, replicated_locations
                             FROM blobs WHERE blob_id = $1 FOR NO KEY UPDATE",
                            &[&blob_id],
                        )
                        .await?;
                    let prev_inline: Option<Vec<u8>> = row.get(0);
                    let prev_primary: Option<String> = row.get(1);
                    let mut locs: Vec<String> = row.get(2);
                    let prev_locs_len = locs.len();
                    for loc in &deduped_incoming {
                        if !locs.iter().any(|existing| existing == loc) {
                            locs.push(loc.clone());
                        }
                    }
                    let inline_changed = prev_inline.is_none() && incoming_inline.is_some();
                    let primary_changed = prev_primary.is_none() && incoming_primary.is_some();
                    if inline_changed || primary_changed || locs.len() != prev_locs_len {
                        let merged_inline = prev_inline.or(incoming_inline);
                        let merged_primary = prev_primary.or(incoming_primary);
                        tx.execute(
                            "UPDATE blobs
                             SET inline_content = $1, primary_location = $2, replicated_locations = $3
                             WHERE blob_id = $4",
                            &[&merged_inline, &merged_primary, &locs, &blob_id],
                        )
                        .await?;
                    }
                }

                if let Err(err) = tx
                    .execute(
                        "INSERT INTO project_blobs (project_id, blob_id) VALUES ($1, $2)
                         ON CONFLICT (project_id, blob_id) DO NOTHING",
                        &[&project_id, &blob_id],
                    )
                    .await
                {
                    if err.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) {
                        return Ok(Err(StoreError::RepoNotFound {
                            repo_id: project_id,
                            action: "writing blob record".to_string(),
                        }));
                    }
                    return Err(err);
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(super) async fn add_blob_replica_location_impl(
        &self,
        blob_id: &[u8],
        location: &str,
    ) -> StoreResult<()> {
        let blob_id = blob_id.to_vec();
        let location = location.to_string();
        self.run_tx_with_retry(|tx| {
            let blob_id = blob_id.clone();
            let location = location.clone();
            Box::pin(async move {
                let row = tx
                    .query_opt(
                        "SELECT replicated_locations FROM blobs WHERE blob_id = $1 FOR NO KEY UPDATE",
                        &[&blob_id],
                    )
                    .await?;
                if let Some(row) = row {
                    let mut locations: Vec<String> = row.get(0);
                    if !locations.iter().any(|l| l == &location) {
                        locations.push(location);
                        tx.execute(
                            "UPDATE blobs SET replicated_locations = $1 WHERE blob_id = $2",
                            &[&locations, &blob_id],
                        )
                        .await?;
                    }
                }
                Ok(Ok(()))
            })
        })
        .await
    }
}
