// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Project-scoped commit, tree, symlink, and blob metadata persistence on Spanner.
//!
//! # Authority and Crash-Safety Ordering
//! Small trees commit parent `Trees` and interleaved `TreeEntries` rows together;
//! large trees (>4,000 entries) insert an unready parent, stream entry chunks under
//! Spanner's mutation cap, and flip `Ready = true` on the final batch.
//!
//! # Concurrency and Merge Invariants
//! `Commits` rows use insert-only write-only transactions; `put_blob_record` and
//! `add_blob_replica_location` merge replica locations in `Serializable` transactions.

use std::collections::{BTreeMap, HashMap, HashSet};

use cc_common::backend::{Commit, TreeEntry};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;

use super::codec::{
    EPOCH_RFC3339_NANOS, build_blob_upsert_mutation, build_commit_mutation,
    build_current_commit_watermark_stmt, build_list_commit_ids_since_stmt,
    build_project_blob_upsert_mutation, build_statement_with_deadline, build_symlink_mutation,
    build_tree_entry_mutation, build_tree_stage_insert_mutation, build_tree_upsert_mutation,
    collect_tree_entries, decode_blob_content_size, decode_blob_row, decode_commit_generation,
    decode_commit_proto, decode_spanner_watermark, drain_first_row, encode_spanner_watermark,
    is_already_exists, map_spanner_err,
};
use super::schema::commit_inserted_at_shard;
use super::{SpannerMetadataStore, TxAction, UNNEST_BATCH_SIZE};
use crate::store::{BlobRecord, CommitId, StoreError, StoreResult};

impl SpannerMetadataStore {
    pub(super) async fn get_commit_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<Commit>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let stmt = Statement::builder(
                "SELECT ProtoBytes FROM Commits WHERE ProjectId = @project_id AND CommitId = @commit_id",
            )
            .add_param("project_id", project_id)
            .add_param("commit_id", commit_id);
            let mut rs = self.single_use_query(stmt).await?;
            let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
                return Ok(None);
            };
            let commit = decode_commit_proto(&row)?;
            Ok(Some(commit))
        })
        .await
    }

    pub(super) async fn has_commit_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<bool> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let stmt = Statement::builder(
                "SELECT 1 FROM Commits WHERE ProjectId = @project_id AND CommitId = @commit_id LIMIT 1",
            )
            .add_param("project_id", project_id)
            .add_param("commit_id", commit_id);
            let mut rs = self.single_use_query(stmt).await?;
            Ok(rs
                .next()
                .await
                .transpose()
                .map_err(map_spanner_err)?
                .is_some())
        })
        .await
    }

    pub(super) async fn get_commit_generation_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<u64>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let stmt = Statement::builder(
                "SELECT Generation FROM Commits WHERE ProjectId = @project_id AND CommitId = @commit_id",
            )
            .add_param("project_id", project_id)
            .add_param("commit_id", commit_id);
            let mut rs = self.single_use_query(stmt).await?;
            let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
                return Ok(None);
            };
            let gen_u64 = decode_commit_generation(&row)?;
            Ok(Some(gen_u64))
        })
        .await
    }

    pub(super) async fn get_commit_with_generation_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<(Commit, u64)>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let stmt = Statement::builder(
                "SELECT ProtoBytes, Generation FROM Commits
                 WHERE ProjectId = @project_id AND CommitId = @commit_id",
            )
            .add_param("project_id", project_id)
            .add_param("commit_id", commit_id);
            let mut rs = self.single_use_query(stmt).await?;
            let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
                return Ok(None);
            };
            let commit = decode_commit_proto(&row)?;
            let gen_u64 = decode_commit_generation(&row)?;
            Ok(Some((commit, gen_u64)))
        })
        .await
    }

    pub(super) async fn get_commit_generations_impl(
        &self,
        project_id: &str,
        commit_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        if commit_ids.is_empty() {
            return Ok(HashMap::new());
        }
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let mut out = HashMap::with_capacity(commit_ids.len());
            for chunk in commit_ids.chunks(UNNEST_BATCH_SIZE) {
                let ids_vec = chunk.to_vec();
                let stmt = Statement::builder(
                    "SELECT CommitId, Generation FROM Commits
                     WHERE ProjectId = @project_id AND CommitId IN UNNEST(@ids)",
                )
                .add_param("project_id", project_id)
                .add_param("ids", ids_vec);
                let mut rs = self.single_use_query(stmt).await?;
                while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
                    let cid: Vec<u8> = row.get("CommitId").map_err(map_spanner_err)?;
                    let gen_u64 = decode_commit_generation(&row)?;
                    out.insert(cid, gen_u64);
                }
            }
            Ok(out)
        })
        .await
    }

    pub(super) async fn put_commit_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
        commit: &Commit,
        generation: u64,
    ) -> StoreResult<()> {
        let gen_i64 = i64::try_from(generation).map_err(|_| {
            StoreError::InvalidArgument(format!("commit generation {generation} exceeds i64::MAX"))
        })?;
        let mutation = build_commit_mutation(
            project_id,
            commit_id,
            commit,
            gen_i64,
            commit_inserted_at_shard(commit_id),
        );

        crate::resilience::run_with_rpc_deadline("Spanner write", async {
            match self.wo_transaction()?.build().write(vec![mutation]).await {
                Ok(_) => Ok(()),
                Err(err) if is_already_exists(&err) => Ok(()),
                Err(err) => Err(map_spanner_err(err)),
            }
        })
        .await
    }

    pub(super) async fn list_commit_ids_impl(
        &self,
        project_id: &str,
        after_commit_id: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Vec<CommitId>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            let stmt = match after_commit_id.filter(|c| !c.is_empty()) {
                Some(after) => Statement::builder(
                    "SELECT CommitId FROM Commits
                     WHERE ProjectId = @project_id AND CommitId > @after_id
                     ORDER BY CommitId ASC
                     LIMIT @limit",
                )
                .add_param("project_id", project_id)
                .add_param("after_id", after)
                .add_param("limit", limit_i64),
                None => Statement::builder(
                    "SELECT CommitId FROM Commits
                     WHERE ProjectId = @project_id
                     ORDER BY CommitId ASC
                     LIMIT @limit",
                )
                .add_param("project_id", project_id)
                .add_param("limit", limit_i64),
            };

            let mut rs = self.single_use_query(stmt).await?;
            let mut ids = Vec::new();
            while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
                let cid: Vec<u8> = row.get("CommitId").map_err(map_spanner_err)?;
                ids.push(cid);
            }
            Ok(ids)
        })
        .await
    }

    pub(super) async fn current_commit_watermark_impl(
        &self,
        project_id: &str,
    ) -> StoreResult<Vec<u8>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let stmt = build_current_commit_watermark_stmt(project_id);
            let mut rs = self.single_use_query(stmt).await?;
            if let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
                let ts_rfc3339: String = row.get("TsRfc3339").map_err(map_spanner_err)?;
                let cid: Vec<u8> = row.get("CommitId").map_err(map_spanner_err)?;
                Ok(encode_spanner_watermark(&ts_rfc3339, &cid))
            } else {
                Ok(encode_spanner_watermark(EPOCH_RFC3339_NANOS, &[]))
            }
        })
        .await
    }

    pub(super) async fn list_commit_ids_since_impl(
        &self,
        project_id: &str,
        since_token: &[u8],
        limit: usize,
    ) -> StoreResult<(Vec<CommitId>, Vec<u8>, bool)> {
        let (ts_rfc3339, after_id) = decode_spanner_watermark(since_token)?;
        if limit == 0 {
            return Ok((Vec::new(), since_token.to_vec(), false));
        }
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let fetch_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
            let stmt =
                build_list_commit_ids_since_stmt(project_id, &ts_rfc3339, &after_id, fetch_limit);

            let mut rs = self.single_use_query(stmt).await?;
            let mut items = Vec::new();
            while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
                let ts: String = row.get("TsRfc3339").map_err(map_spanner_err)?;
                let cid: Vec<u8> = row.get("CommitId").map_err(map_spanner_err)?;
                items.push((ts, cid));
            }

            let has_more = items.len() > limit;
            if has_more {
                items.truncate(limit);
            }
            let Some((last_ts, last_cid)) = items.last() else {
                return Ok((Vec::new(), since_token.to_vec(), false));
            };
            let next_tok = encode_spanner_watermark(last_ts, last_cid);
            let ids = items.into_iter().map(|(_, cid)| cid).collect();
            Ok((ids, next_tok, has_more))
        })
        .await
    }

    pub(super) async fn resolve_commit_id_prefix_impl(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<CommitId>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let stmt = Statement::builder(
                "SELECT CommitId FROM Commits
                 WHERE ProjectId = @project_id AND CommitId >= @min_id AND CommitId <= @max_id
                 ORDER BY CommitId ASC
                 LIMIT 2",
            )
            .add_param("project_id", project_id)
            .add_param("min_id", min_id)
            .add_param("max_id", max_id);

            let mut rs = self.single_use_query(stmt).await?;
            let mut matches = Vec::new();
            while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
                let cid: Vec<u8> = row.get("CommitId").map_err(map_spanner_err)?;
                matches.push(cid);
            }
            Ok(matches)
        })
        .await
    }

    pub(super) async fn resolve_change_id_prefix_impl(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Vec<u8>, Vec<CommitId>)>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let ro_tx = self
                .ro_transaction()?
                .build()
                .await
                .map_err(map_spanner_err)?;

            let ch_stmt = build_statement_with_deadline(
                Statement::builder(
                    "SELECT DISTINCT ChangeId FROM Commits@{FORCE_INDEX=CommitsByChangeId}
                     WHERE ProjectId = @project_id AND ChangeId >= @min_id AND ChangeId <= @max_id
                     ORDER BY ChangeId ASC
                     LIMIT 2",
                )
                .add_param("project_id", project_id)
                .add_param("min_id", min_id)
                .add_param("max_id", max_id),
            )?;

            let mut ch_rs = ro_tx
                .execute_query(ch_stmt)
                .await
                .map_err(map_spanner_err)?;
            let mut change_ids = Vec::new();
            while let Some(row) = ch_rs.next().await.transpose().map_err(map_spanner_err)? {
                let ch: Vec<u8> = row.get("ChangeId").map_err(map_spanner_err)?;
                change_ids.push(ch);
            }
            if change_ids.len() != 1 {
                return Ok(change_ids.into_iter().map(|ch| (ch, Vec::new())).collect());
            }

            let matched_ch = change_ids.swap_remove(0);
            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            let c_stmt = build_statement_with_deadline(
                Statement::builder(
                    "SELECT CommitId FROM Commits@{FORCE_INDEX=CommitsByChangeId}
                     WHERE ProjectId = @project_id AND ChangeId = @change_id
                     ORDER BY CommitId ASC
                     LIMIT @limit",
                )
                .add_param("project_id", project_id)
                .add_param("change_id", &matched_ch)
                .add_param("limit", limit_i64),
            )?;
            let mut c_rs = ro_tx.execute_query(c_stmt).await.map_err(map_spanner_err)?;
            let mut by_change = BTreeMap::new();
            let mut cids = Vec::new();
            while let Some(row) = c_rs.next().await.transpose().map_err(map_spanner_err)? {
                let cid: Vec<u8> = row.get("CommitId").map_err(map_spanner_err)?;
                cids.push(cid);
            }
            by_change.insert(matched_ch, cids);
            Ok(by_change.into_iter().collect())
        })
        .await
    }

    pub(super) async fn get_tree_impl(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let ro_tx = self
                .ro_transaction()?
                .build()
                .await
                .map_err(map_spanner_err)?;

            let tree_stmt = build_statement_with_deadline(
                Statement::builder(
                    "SELECT 1 FROM Trees
                     WHERE ProjectId = @project_id AND TreeId = @tree_id AND Ready = TRUE",
                )
                .add_param("project_id", project_id)
                .add_param("tree_id", tree_id),
            )?;
            let mut tree_rs = ro_tx
                .execute_query(tree_stmt)
                .await
                .map_err(map_spanner_err)?;
            if tree_rs
                .next()
                .await
                .transpose()
                .map_err(map_spanner_err)?
                .is_none()
            {
                return Ok(None);
            }

            let entries_stmt = build_statement_with_deadline(
                Statement::builder(
                    "SELECT EntryProto FROM TreeEntries
                     WHERE ProjectId = @project_id AND TreeId = @tree_id
                     ORDER BY Name ASC",
                )
                .add_param("project_id", project_id)
                .add_param("tree_id", tree_id),
            )?;
            let entries_rs = ro_tx
                .execute_query(entries_stmt)
                .await
                .map_err(map_spanner_err)?;
            Ok(Some(collect_tree_entries(entries_rs).await?))
        })
        .await
    }

    pub(super) async fn get_tree_page_impl(
        &self,
        project_id: &str,
        tree_id: &[u8],
        after_name: Option<&str>,
        limit: usize,
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let ro_tx = self
                .ro_transaction()?
                .build()
                .await
                .map_err(map_spanner_err)?;

            let tree_stmt = build_statement_with_deadline(
                Statement::builder(
                    "SELECT 1 FROM Trees
                     WHERE ProjectId = @project_id AND TreeId = @tree_id AND Ready = TRUE",
                )
                .add_param("project_id", project_id)
                .add_param("tree_id", tree_id),
            )?;
            let mut tree_rs = ro_tx
                .execute_query(tree_stmt)
                .await
                .map_err(map_spanner_err)?;
            if tree_rs
                .next()
                .await
                .transpose()
                .map_err(map_spanner_err)?
                .is_none()
            {
                return Ok(None);
            }

            let after = after_name.unwrap_or("");
            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            let entries_stmt = build_statement_with_deadline(
                Statement::builder(
                    "SELECT EntryProto FROM TreeEntries
                     WHERE ProjectId = @project_id AND TreeId = @tree_id AND Name > @after_name
                     ORDER BY Name ASC
                     LIMIT @limit",
                )
                .add_param("project_id", project_id)
                .add_param("tree_id", tree_id)
                .add_param("after_name", after)
                .add_param("limit", limit_i64),
            )?;
            let entries_rs = ro_tx
                .execute_query(entries_stmt)
                .await
                .map_err(map_spanner_err)?;
            Ok(Some(collect_tree_entries(entries_rs).await?))
        })
        .await
    }

    pub(super) async fn put_tree_impl(
        &self,
        project_id: &str,
        tree_id: &[u8],
        entries: &[TreeEntry],
    ) -> StoreResult<()> {
        crate::resilience::run_with_rpc_deadline("Spanner write", async {
            const MAX_ENTRIES_PER_BATCH: usize = 4_000;

            if entries.len() <= MAX_ENTRIES_PER_BATCH {
                let mut mutations = Vec::with_capacity(entries.len() + 1);
                // Parent row in Trees MUST precede interleaved child rows in TreeEntries.
                mutations.push(build_tree_upsert_mutation(project_id, tree_id, true));
                for entry in entries {
                    mutations.push(build_tree_entry_mutation(project_id, tree_id, entry));
                }
                self.wo_transaction()?
                    .build()
                    .write(mutations)
                    .await
                    .map_err(map_spanner_err)?;
                return Ok(());
            }

            // Large directory (>4,000 entries): stage parent with Ready=false, stream chunks under
            // Spanner's 80,000-mutation transaction cap, and flip Ready=true on the final batch.
            // Tree ids are content hashes, so a ready tree is complete; skip it rather than hiding
            // it from readers while the chunks are rewritten.
            let ready_stmt = Statement::builder(
                "SELECT 1 FROM Trees
                 WHERE ProjectId = @project_id AND TreeId = @tree_id AND Ready = TRUE",
            )
            .add_param("project_id", project_id)
            .add_param("tree_id", tree_id);
            let mut ready_rs = self.single_use_query(ready_stmt).await?;
            if ready_rs
                .next()
                .await
                .transpose()
                .map_err(map_spanner_err)?
                .is_some()
            {
                return Ok(());
            }

            // Insert-only staging never turns a row that a concurrent upload just finished back
            // to Ready=false; an existing row (ready or left by an interrupted upload) is kept.
            let stage_parent = build_tree_stage_insert_mutation(project_id, tree_id);
            match self
                .wo_transaction()?
                .build()
                .write(vec![stage_parent])
                .await
            {
                Ok(_) => {}
                Err(err) if is_already_exists(&err) => {}
                Err(err) => return Err(map_spanner_err(err)),
            }

            let chunks: Vec<&[TreeEntry]> = entries.chunks(MAX_ENTRIES_PER_BATCH).collect();
            for (idx, chunk) in chunks.iter().enumerate() {
                let is_last = idx + 1 == chunks.len();
                let mut mutations = Vec::with_capacity(chunk.len() + usize::from(is_last));
                for entry in *chunk {
                    mutations.push(build_tree_entry_mutation(project_id, tree_id, entry));
                }
                if is_last {
                    mutations.push(build_tree_upsert_mutation(project_id, tree_id, true));
                }
                self.wo_transaction()?
                    .build()
                    .write(mutations)
                    .await
                    .map_err(map_spanner_err)?;
            }
            Ok(())
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
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let mut found = HashSet::with_capacity(tree_ids.len());
            for chunk in tree_ids.chunks(UNNEST_BATCH_SIZE) {
                let ids_vec = chunk.to_vec();
                let stmt = Statement::builder(
                    "SELECT TreeId FROM Trees
                     WHERE ProjectId = @project_id AND TreeId IN UNNEST(@ids) AND Ready = TRUE",
                )
                .add_param("project_id", project_id)
                .add_param("ids", ids_vec);
                let mut rs = self.single_use_query(stmt).await?;
                while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
                    let tid: Vec<u8> = row.get("TreeId").map_err(map_spanner_err)?;
                    found.insert(tid);
                }
            }
            Ok(found)
        })
        .await
    }

    pub(super) async fn get_symlink_impl(
        &self,
        project_id: &str,
        symlink_id: &[u8],
    ) -> StoreResult<Option<String>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let stmt = Statement::builder(
                "SELECT Target FROM Symlinks WHERE ProjectId = @project_id AND SymlinkId = @symlink_id",
            )
            .add_param("project_id", project_id)
            .add_param("symlink_id", symlink_id);
            let mut rs = self.single_use_query(stmt).await?;
            let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
                return Ok(None);
            };
            let target: String = row.get("Target").map_err(map_spanner_err)?;
            Ok(Some(target))
        })
        .await
    }

    pub(super) async fn put_symlink_impl(
        &self,
        project_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> StoreResult<()> {
        crate::resilience::run_with_rpc_deadline("Spanner write", async {
            let mutation = build_symlink_mutation(project_id, symlink_id, target);
            self.wo_transaction()?
                .build()
                .write(vec![mutation])
                .await
                .map_err(map_spanner_err)?;
            Ok(())
        })
        .await
    }

    pub(super) async fn find_existing_symlinks_impl(
        &self,
        project_id: &str,
        symlink_ids: &[Vec<u8>],
    ) -> StoreResult<HashSet<Vec<u8>>> {
        if symlink_ids.is_empty() {
            return Ok(HashSet::new());
        }
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let mut found = HashSet::with_capacity(symlink_ids.len());
            for chunk in symlink_ids.chunks(UNNEST_BATCH_SIZE) {
                let ids_vec = chunk.to_vec();
                let stmt = Statement::builder(
                    "SELECT SymlinkId FROM Symlinks
                     WHERE ProjectId = @project_id AND SymlinkId IN UNNEST(@ids)",
                )
                .add_param("project_id", project_id)
                .add_param("ids", ids_vec);
                let mut rs = self.single_use_query(stmt).await?;
                while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
                    let sid: Vec<u8> = row.get("SymlinkId").map_err(map_spanner_err)?;
                    found.insert(sid);
                }
            }
            Ok(found)
        })
        .await
    }

    pub(super) async fn get_blob_record_impl(
        &self,
        project_id: &str,
        blob_id: &[u8],
    ) -> StoreResult<Option<BlobRecord>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let stmt = Statement::builder(
                "SELECT b.BlobId, b.ContentSize, b.InlineContent, b.PrimaryLocation, b.ReplicatedLocations
                 FROM Blobs b
                 INNER JOIN ProjectBlobs pb ON pb.BlobId = b.BlobId
                 WHERE pb.ProjectId = @project_id AND b.BlobId = @blob_id",
            )
            .add_param("project_id", project_id)
            .add_param("blob_id", blob_id);

            let mut rs = self.single_use_query(stmt).await?;
            let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
                return Ok(None);
            };
            decode_blob_row(&row)
        })
        .await
    }

    pub(super) async fn get_blob_sizes_batch_impl(
        &self,
        project_id: &str,
        blob_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        if blob_ids.is_empty() {
            return Ok(HashMap::new());
        }
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let mut sizes = HashMap::with_capacity(blob_ids.len());
            for chunk in blob_ids.chunks(UNNEST_BATCH_SIZE) {
                let ids_vec = chunk.to_vec();
                let stmt = Statement::builder(
                    "SELECT b.BlobId, b.ContentSize
                     FROM Blobs b
                     INNER JOIN ProjectBlobs pb ON pb.BlobId = b.BlobId
                     WHERE pb.ProjectId = @project_id AND pb.BlobId IN UNNEST(@ids)",
                )
                .add_param("project_id", project_id)
                .add_param("ids", ids_vec);
                let mut rs = self.single_use_query(stmt).await?;
                while let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? {
                    let bid: Vec<u8> = row.get("BlobId").map_err(map_spanner_err)?;
                    let size_u64 = decode_blob_content_size(&row)?;
                    sizes.insert(bid, size_u64);
                }
            }
            Ok(sizes)
        })
        .await
    }

    pub(super) async fn get_global_blob_record_impl(
        &self,
        blob_id: &[u8],
    ) -> StoreResult<Option<BlobRecord>> {
        crate::resilience::run_with_rpc_deadline("Spanner read", async {
            let stmt = Statement::builder(
                "SELECT BlobId, ContentSize, InlineContent, PrimaryLocation, ReplicatedLocations
                 FROM Blobs WHERE BlobId = @blob_id",
            )
            .add_param("blob_id", blob_id);

            let mut rs = self.single_use_query(stmt).await?;
            let Some(row) = rs.next().await.transpose().map_err(map_spanner_err)? else {
                return Ok(None);
            };
            decode_blob_row(&row)
        })
        .await
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
        let incoming_replicated = record.replicated_locations.clone();

        self.run_rw_tx(async move |tx| {
            let stmt = Statement::builder(
                "SELECT InlineContent, PrimaryLocation, ReplicatedLocations
                 FROM Blobs WHERE BlobId = @blob_id",
            )
            .add_param("blob_id", &blob_id)
            .build();
            let rs = tx.execute_query(stmt).await?;
            let (merged_inline, merged_primary, merged_replicated) =
                if let Some(row) = drain_first_row(rs).await? {
                    let prev_inline: Option<Vec<u8>> = row.get("InlineContent")?;
                    let prev_primary: Option<String> = row.get("PrimaryLocation")?;
                    let mut locs: Vec<String> = row.get("ReplicatedLocations")?;
                    for loc in &incoming_replicated {
                        if !locs.iter().any(|existing| existing == loc) {
                            locs.push(loc.clone());
                        }
                    }
                    (
                        prev_inline.or_else(|| incoming_inline.clone()),
                        prev_primary.or_else(|| incoming_primary.clone()),
                        locs,
                    )
                } else {
                    (
                        incoming_inline.clone(),
                        incoming_primary.clone(),
                        incoming_replicated.clone(),
                    )
                };

            let blob_mutation = build_blob_upsert_mutation(
                &blob_id,
                content_size_i64,
                &merged_inline,
                &merged_primary,
                &merged_replicated,
            );
            let project_blob_mutation = build_project_blob_upsert_mutation(&project_id, &blob_id);

            tx.buffer([blob_mutation, project_blob_mutation])?;
            Ok(TxAction::Commit(()))
        })
        .await
        .map_err(map_spanner_err)?
    }

    pub(super) async fn add_blob_replica_location_impl(
        &self,
        blob_id: &[u8],
        location: &str,
    ) -> StoreResult<()> {
        let blob_id = blob_id.to_vec();
        let location = location.to_string();

        self.run_rw_tx(async move |tx| {
            let stmt =
                Statement::builder("SELECT ReplicatedLocations FROM Blobs WHERE BlobId = @blob_id")
                    .add_param("blob_id", &blob_id)
                    .build();
            let rs = tx.execute_query(stmt).await?;
            if let Some(row) = drain_first_row(rs).await? {
                let mut locations: Vec<String> = row.get("ReplicatedLocations")?;
                if !locations.iter().any(|l| l == &location) {
                    locations.push(location.clone());
                    let update = Mutation::new_update_builder("Blobs")
                        .set("BlobId")
                        .to(&blob_id)
                        .set("ReplicatedLocations")
                        .to(&locations)
                        .build();
                    tx.buffer([update])?;
                    return Ok(TxAction::Commit(()));
                }
            }
            Ok(TxAction::Rollback(Ok(())))
        })
        .await
        .map_err(map_spanner_err)?
    }
}
