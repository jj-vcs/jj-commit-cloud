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

//! PostgreSQL commit storage, monotonic sequence watermarks, and ID prefix resolution.
//!
//! # Concurrency and Merge Invariants
//! Assigns per-project commit sequences under `UPDATE projects SET commit_seq = commit_seq + 1`
//! so MVCC visibility order matches `seq` order, batches generation lookups via `UNNEST`, and
//! runs change-ID prefix lookups inside a `REPEATABLE READ` read-only snapshot.

use std::collections::HashMap;

use cc_common::backend::Commit;
use prost::Message as _;
use tokio_postgres::IsolationLevel;

use super::codec::{decode_commit_watermark, encode_commit_watermark, map_pg_err};
use super::{PostgresMetadataStore, UNNEST_BATCH_SIZE};
use crate::store::{CommitId, StoreError, StoreResult};

impl PostgresMetadataStore {
    pub(super) async fn has_commit_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<bool> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT 1 FROM commits WHERE project_id = $1 AND commit_id = $2 LIMIT 1",
                &[&project_id, &commit_id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.is_some())
    }

    pub(super) async fn get_commit_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<Commit>> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT proto_bytes FROM commits WHERE project_id = $1 AND commit_id = $2",
                &[&project_id, &commit_id],
            )
            .await
            .map_err(map_pg_err)?;
        row.map(|r| {
            let bytes: Vec<u8> = r.get(0);
            Commit::decode(bytes.as_slice()).map_err(|e| {
                StoreError::Corruption(format!("failed to decode PostgreSQL Commit protobuf: {e}"))
            })
        })
        .transpose()
    }

    pub(super) async fn get_commit_with_generation_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<(Commit, u64)>> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT proto_bytes, generation FROM commits WHERE project_id = $1 AND commit_id = $2",
                &[&project_id, &commit_id],
            )
            .await
            .map_err(map_pg_err)?;
        row.map(|r| {
            let bytes: Vec<u8> = r.get(0);
            let gen_i64: i64 = r.get(1);
            let gen_u64 = u64::try_from(gen_i64).map_err(|_| {
                StoreError::Corruption(format!("negative PostgreSQL commit generation {gen_i64}"))
            })?;
            let commit = Commit::decode(bytes.as_slice()).map_err(|e| {
                StoreError::Corruption(format!("failed to decode PostgreSQL Commit protobuf: {e}"))
            })?;
            Ok((commit, gen_u64))
        })
        .transpose()
    }

    pub(super) async fn get_commit_generation_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<u64>> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT generation FROM commits WHERE project_id = $1 AND commit_id = $2",
                &[&project_id, &commit_id],
            )
            .await
            .map_err(map_pg_err)?;
        row.map(|r| {
            let gen_i64: i64 = r.get(0);
            u64::try_from(gen_i64).map_err(|_| {
                StoreError::Corruption(format!("negative PostgreSQL commit generation {gen_i64}"))
            })
        })
        .transpose()
    }

    pub(super) async fn get_commit_generations_impl(
        &self,
        project_id: &str,
        commit_ids: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, u64>> {
        if commit_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let client = self.get_client().await?;
        let mut out = HashMap::with_capacity(commit_ids.len());
        for chunk in commit_ids.chunks(UNNEST_BATCH_SIZE) {
            let rows = client
                .query(
                    "SELECT commit_id, generation FROM commits
                     WHERE project_id = $1 AND commit_id IN (SELECT UNNEST($2::BYTEA[]))",
                    &[&project_id, &chunk],
                )
                .await
                .map_err(map_pg_err)?;
            for row in rows {
                let cid: Vec<u8> = row.get(0);
                let gen_i64: i64 = row.get(1);
                let gen_u64 = u64::try_from(gen_i64).map_err(|_| {
                    StoreError::Corruption(format!(
                        "negative PostgreSQL commit generation {gen_i64}"
                    ))
                })?;
                out.insert(cid, gen_u64);
            }
        }
        Ok(out)
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
        // Fast path: commits are content-addressed and immutable, so duplicate retries avoid
        // taking the per-project sequence lock.
        {
            let client = self.get_client().await?;
            let exists = client
                .query_opt(
                    "SELECT 1 FROM commits WHERE project_id = $1 AND commit_id = $2",
                    &[&project_id, &commit_id],
                )
                .await
                .map_err(map_pg_err)?;
            if exists.is_some() {
                return Ok(());
            }
        }

        let project_id = project_id.to_string();
        let commit_id = commit_id.to_vec();
        let change_id = commit.change_id.clone();
        let proto_bytes = commit.encode_to_vec();

        // Assign a per-project monotonic sequence under `projects` row lock (`FOR NO KEY UPDATE`,
        // which does not conflict with `FOR KEY SHARE` FK checks from trees/symlinks/blobs/repos).
        // Serializing commit sequence assignment per project guarantees commit visibility order
        // strictly matches `seq` order under MVCC without stalling other projects.
        self.run_tx_with_retry(|tx| {
            let project_id = project_id.clone();
            let commit_id = commit_id.clone();
            let change_id = change_id.clone();
            let proto_bytes = proto_bytes.clone();
            Box::pin(async move {
                let seq_row = tx
                    .query_opt(
                        "UPDATE projects SET commit_seq = commit_seq + 1
                         WHERE project_id = $1
                         RETURNING commit_seq",
                        &[&project_id],
                    )
                    .await?;
                let Some(seq_row) = seq_row else {
                    return Ok(Err(StoreError::RepoNotFound {
                        repo_id: project_id,
                        action: "writing commit".to_string(),
                    }));
                };
                let seq: i64 = seq_row.get(0);
                tx.execute(
                    "INSERT INTO commits (project_id, commit_id, change_id, generation, seq, proto_bytes)
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (project_id, commit_id) DO NOTHING",
                    &[
                        &project_id,
                        &commit_id,
                        &change_id,
                        &gen_i64,
                        &seq,
                        &proto_bytes,
                    ],
                )
                .await?;
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(super) async fn list_commit_ids_impl(
        &self,
        project_id: &str,
        after_commit_id: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Vec<CommitId>> {
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        let client = self.get_client().await?;
        let rows = match after_commit_id.filter(|c| !c.is_empty()) {
            Some(after) => client
                .query(
                    "SELECT commit_id FROM commits
                     WHERE project_id = $1 AND commit_id > $2
                     ORDER BY commit_id ASC
                     LIMIT $3",
                    &[&project_id, &after, &limit_i64],
                )
                .await
                .map_err(map_pg_err)?,
            None => client
                .query(
                    "SELECT commit_id FROM commits
                     WHERE project_id = $1
                     ORDER BY commit_id ASC
                     LIMIT $2",
                    &[&project_id, &limit_i64],
                )
                .await
                .map_err(map_pg_err)?,
        };
        Ok(rows.into_iter().map(|r| r.get::<_, Vec<u8>>(0)).collect())
    }

    pub(super) async fn current_commit_watermark_impl(
        &self,
        project_id: &str,
    ) -> StoreResult<Vec<u8>> {
        let client = self.get_client().await?;
        let row = client
            .query_one(
                "SELECT COALESCE(MAX(seq), 0)::BIGINT FROM commits WHERE project_id = $1",
                &[&project_id],
            )
            .await
            .map_err(map_pg_err)?;
        let max_seq: i64 = row.get(0);
        if max_seq < 0 {
            return Err(StoreError::Corruption(format!(
                "negative PostgreSQL commit watermark sequence {max_seq}"
            )));
        }
        Ok(encode_commit_watermark(max_seq))
    }

    pub(super) async fn list_commit_ids_since_impl(
        &self,
        project_id: &str,
        since_token: &[u8],
        limit: usize,
    ) -> StoreResult<(Vec<CommitId>, Vec<u8>, bool)> {
        let since_seq = decode_commit_watermark(since_token)?;
        if limit == 0 {
            return Ok((Vec::new(), since_token.to_vec(), false));
        }
        let fetch_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
        let client = self.get_client().await?;
        let rows = client
            .query(
                "SELECT seq, commit_id FROM commits
                 WHERE project_id = $1 AND seq > $2
                 ORDER BY seq ASC, commit_id ASC
                 LIMIT $3",
                &[&project_id, &since_seq, &fetch_limit],
            )
            .await
            .map_err(map_pg_err)?;

        let mut items: Vec<(i64, CommitId)> = Vec::with_capacity(rows.len());
        for row in rows {
            let seq: i64 = row.get(0);
            if seq < 0 {
                return Err(StoreError::Corruption(format!(
                    "negative PostgreSQL commit sequence {seq}"
                )));
            }
            let cid: Vec<u8> = row.get(1);
            items.push((seq, cid));
        }
        let has_more = items.len() > limit;
        if has_more {
            items.truncate(limit);
        }
        let Some(&(last_seq, _)) = items.last() else {
            return Ok((Vec::new(), since_token.to_vec(), false));
        };
        let ids = items.into_iter().map(|(_, cid)| cid).collect();
        Ok((ids, encode_commit_watermark(last_seq), has_more))
    }

    pub(super) async fn resolve_commit_id_prefix_impl(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<CommitId>> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                "SELECT commit_id FROM commits
                 WHERE project_id = $1 AND commit_id >= $2 AND commit_id <= $3
                 ORDER BY commit_id ASC
                 LIMIT 2",
                &[&project_id, &min_id, &max_id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows.into_iter().map(|r| r.get::<_, Vec<u8>>(0)).collect())
    }

    pub(super) async fn resolve_change_id_prefix_impl(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Vec<u8>, Vec<CommitId>)>> {
        let mut client = self.get_client().await?;
        let tx = client
            .build_transaction()
            .read_only(true)
            .isolation_level(IsolationLevel::RepeatableRead)
            .start()
            .await
            .map_err(map_pg_err)?;

        let ch_rows = match tx
            .query(
                "SELECT DISTINCT change_id FROM commits
                 WHERE project_id = $1 AND change_id >= $2 AND change_id <= $3
                 ORDER BY change_id ASC
                 LIMIT 2",
                &[&project_id, &min_id, &max_id],
            )
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                let _ = tx.rollback().await;
                return Err(map_pg_err(err));
            }
        };
        let mut change_ids: Vec<Vec<u8>> = ch_rows.into_iter().map(|r| r.get(0)).collect();
        if change_ids.len() != 1 {
            tx.commit().await.map_err(map_pg_err)?;
            return Ok(change_ids.into_iter().map(|ch| (ch, Vec::new())).collect());
        }

        let matched_ch = change_ids.swap_remove(0);
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        let c_rows = match tx
            .query(
                "SELECT commit_id FROM commits
                 WHERE project_id = $1 AND change_id = $2
                 ORDER BY commit_id ASC
                 LIMIT $3",
                &[&project_id, &matched_ch, &limit_i64],
            )
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                let _ = tx.rollback().await;
                return Err(map_pg_err(err));
            }
        };
        let cids: Vec<CommitId> = c_rows.into_iter().map(|r| r.get(0)).collect();
        tx.commit().await.map_err(map_pg_err)?;
        Ok(vec![(matched_ch, cids)])
    }
}
