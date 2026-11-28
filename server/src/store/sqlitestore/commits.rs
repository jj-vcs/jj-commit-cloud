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

//! Commit storage, generation lookup, sync listing, and prefix resolution for SQLite.
//!
//! # Authority and Crash-Safety Ordering
//! Commit inserts are idempotent (`INSERT OR IGNORE`) and record monotonic row sequence
//! identifiers used as sync watermarks for incremental commit listing.
//!
//! # Concurrency and Merge Invariants
//! Read queries run against pooled WAL read-only snapshots without blocking writer
//! transactions.

use std::collections::{BTreeMap, HashMap};

use cc_common::backend::Commit;
use prost::Message;
use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::SqliteMetadataStore;
use crate::store::{CommitId, StoreError, StoreResult};

impl SqliteMetadataStore {
    pub(super) async fn get_commit_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<Commit>> {
        let project_id = project_id.to_string();
        let commit_id = commit_id.to_vec();
        self.with_read_conn(move |conn| {
            let raw: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT proto_bytes FROM commits WHERE project_id = ?1 AND commit_id = ?2",
                    params![project_id, commit_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite get_commit failed: {e}")))?;

            raw.map(|bytes| {
                Commit::decode(bytes.as_slice()).map_err(|e| {
                    StoreError::Corruption(format!("failed to decode Commit protobuf: {e}"))
                })
            })
            .transpose()
        })
        .await
    }

    pub(super) async fn has_commit_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<bool> {
        let project_id = project_id.to_string();
        let commit_id = commit_id.to_vec();
        self.with_read_conn(move |conn| {
            let exists: Option<i64> = conn
                .query_row(
                    "SELECT 1 FROM commits WHERE project_id = ?1 AND commit_id = ?2 LIMIT 1",
                    params![project_id, commit_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite has_commit failed: {e}")))?;
            Ok(exists.is_some())
        })
        .await
    }

    pub(super) async fn get_commit_generation_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<u64>> {
        let project_id = project_id.to_string();
        let commit_id = commit_id.to_vec();
        self.with_read_conn(move |conn| {
            let generation: Option<i64> = conn
                .query_row(
                    "SELECT generation FROM commits WHERE project_id = ?1 AND commit_id = ?2",
                    params![project_id, commit_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite get_commit_generation failed: {e}"))
                })?;
            generation
                .map(|g| {
                    u64::try_from(g).map_err(|_| {
                        StoreError::Corruption(format!("negative SQLite commit generation {g}"))
                    })
                })
                .transpose()
        })
        .await
    }

    pub(super) async fn get_commit_with_generation_impl(
        &self,
        project_id: &str,
        commit_id: &[u8],
    ) -> StoreResult<Option<(Commit, u64)>> {
        let project_id = project_id.to_string();
        let commit_id = commit_id.to_vec();
        self.with_read_conn(move |conn| {
            let row: Option<(Vec<u8>, i64)> = conn
                .query_row(
                    "SELECT proto_bytes, generation FROM commits
                     WHERE project_id = ?1 AND commit_id = ?2",
                    params![project_id, commit_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite get_commit_with_generation failed: {e}"))
                })?;

            row.map(|(bytes, g)| {
                let commit = Commit::decode(bytes.as_slice()).map_err(|e| {
                    StoreError::Corruption(format!("failed to decode Commit protobuf: {e}"))
                })?;
                let generation = u64::try_from(g).map_err(|_| {
                    StoreError::Corruption(format!("negative SQLite commit generation {g}"))
                })?;
                Ok((commit, generation))
            })
            .transpose()
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
        let project_id = project_id.to_string();
        let ids = commit_ids.to_vec();
        self.with_read_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(|e| {
                    StoreError::Backend(format!(
                        "SQLite begin get_commit_generations read tx failed: {e}"
                    ))
                })?;
            let mut stmt = tx
                .prepare_cached(
                    "SELECT generation FROM commits WHERE project_id = ?1 AND commit_id = ?2",
                )
                .map_err(|e| {
                    StoreError::Backend(format!(
                        "SQLite prepare get_commit_generations failed: {e}"
                    ))
                })?;
            let mut out = HashMap::with_capacity(ids.len());
            for id in ids {
                let generation: Option<i64> = stmt
                    .query_row(params![project_id, id], |r| r.get(0))
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite get_commit_generations failed: {e}"))
                    })?;
                if let Some(g) = generation {
                    let gen_u64 = u64::try_from(g).map_err(|_| {
                        StoreError::Corruption(format!("negative SQLite commit generation {g}"))
                    })?;
                    out.insert(id, gen_u64);
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
        let project_id = project_id.to_string();
        let commit_id = commit_id.to_vec();
        let change_id = commit.change_id.clone();
        let proto_bytes = commit.encode_to_vec();
        let gen_i64 = i64::try_from(generation).map_err(|_| {
            StoreError::InvalidArgument(format!("commit generation {generation} exceeds i64::MAX"))
        })?;

        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin put_commit tx failed: {e}"))
                })?;
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM commits WHERE project_id = ?1 AND commit_id = ?2",
                    params![project_id, commit_id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite check existing commit failed: {e}"))
                })?;
            if exists.is_none() {
                let next_seq: i64 = tx
                    .query_row(
                        "SELECT COALESCE(MAX(seq), 0) + 1 FROM commits WHERE project_id = ?1",
                        params![project_id],
                        |r| r.get(0),
                    )
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite allocate commit seq failed: {e}"))
                    })?;
                tx.execute(
                    "INSERT OR IGNORE INTO commits
                     (project_id, commit_id, change_id, generation, proto_bytes, seq)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        project_id,
                        commit_id,
                        change_id,
                        gen_i64,
                        proto_bytes,
                        next_seq
                    ],
                )
                .map_err(|e| StoreError::Backend(format!("SQLite put_commit failed: {e}")))?;
            }
            tx.commit()
                .map_err(|e| StoreError::Backend(format!("SQLite commit put_commit failed: {e}")))
        })
        .await
    }

    pub(super) async fn list_commit_ids_impl(
        &self,
        project_id: &str,
        after_commit_id: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<Vec<CommitId>> {
        let project_id = project_id.to_string();
        let cursor = after_commit_id
            .filter(|c| !c.is_empty())
            .map(|c| c.to_vec());
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);

        self.with_read_conn(move |conn| {
            let mut ids = Vec::new();
            if let Some(after) = cursor {
                let mut stmt = conn
                    .prepare(
                        "SELECT commit_id FROM commits
                         WHERE project_id = ?1 AND commit_id > ?2
                         ORDER BY commit_id ASC
                         LIMIT ?3",
                    )
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite prepare list_commit_ids failed: {e}"))
                    })?;
                let rows = stmt
                    .query_map(params![project_id, after, limit_i64], |r| {
                        r.get::<_, Vec<u8>>(0)
                    })
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite query list_commit_ids failed: {e}"))
                    })?;
                for r in rows {
                    ids.push(r.map_err(|e| {
                        StoreError::Backend(format!("SQLite read commit_id failed: {e}"))
                    })?);
                }
            } else {
                let mut stmt = conn
                    .prepare(
                        "SELECT commit_id FROM commits
                         WHERE project_id = ?1
                         ORDER BY commit_id ASC
                         LIMIT ?2",
                    )
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite prepare list_commit_ids failed: {e}"))
                    })?;
                let rows = stmt
                    .query_map(params![project_id, limit_i64], |r| r.get::<_, Vec<u8>>(0))
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite query list_commit_ids failed: {e}"))
                    })?;
                for r in rows {
                    ids.push(r.map_err(|e| {
                        StoreError::Backend(format!("SQLite read commit_id failed: {e}"))
                    })?);
                }
            }
            Ok(ids)
        })
        .await
    }

    pub(super) async fn current_commit_watermark_impl(
        &self,
        project_id: &str,
    ) -> StoreResult<Vec<u8>> {
        let project_id = project_id.to_string();
        self.with_read_conn(move |conn| {
            let max_seq: i64 = conn
                .query_row(
                    "SELECT COALESCE(MAX(seq), 0) FROM commits WHERE project_id = ?1",
                    params![project_id],
                    |r| r.get(0),
                )
                .map_err(|e| StoreError::Backend(format!("SQLite commit watermark failed: {e}")))?;
            Ok(max_seq.to_be_bytes().to_vec())
        })
        .await
    }

    pub(super) async fn list_commit_ids_since_impl(
        &self,
        project_id: &str,
        since_token: &[u8],
        limit: usize,
    ) -> StoreResult<(Vec<CommitId>, Vec<u8>, bool)> {
        let since_seq = if since_token.len() == 8 {
            i64::from_be_bytes(since_token.try_into().unwrap())
        } else {
            return Err(StoreError::InvalidArgument(
                "invalid SQLite commit sync token".to_string(),
            ));
        };
        if since_seq < 0 {
            return Err(StoreError::InvalidArgument(
                "invalid SQLite commit sync token: negative sequence".to_string(),
            ));
        }
        let project_id = project_id.to_string();
        let fallback_tok = since_token.to_vec();
        let fetch_limit = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);

        self.with_read_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT seq, commit_id FROM commits
                     WHERE project_id = ?1 AND seq > ?2
                     ORDER BY seq ASC
                     LIMIT ?3",
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite prepare list_commit_ids_since failed: {e}"))
                })?;
            let rows = stmt
                .query_map(params![project_id, since_seq, fetch_limit], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
                })
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite query list_commit_ids_since failed: {e}"))
                })?;
            let mut items = Vec::new();
            for r in rows {
                items.push(r.map_err(|e| {
                    StoreError::Backend(format!("SQLite read incremental commit failed: {e}"))
                })?);
            }
            let has_more = items.len() > limit;
            if has_more {
                items.truncate(limit);
            }
            let Some(&(last_seq, _)) = items.last() else {
                return Ok((Vec::new(), fallback_tok, false));
            };
            let ids = items.into_iter().map(|(_, cid)| cid).collect();
            Ok((ids, last_seq.to_be_bytes().to_vec(), has_more))
        })
        .await
    }

    pub(super) async fn resolve_commit_id_prefix_impl(
        &self,
        project_id: &str,
        min_id: &[u8],
        max_id: &[u8],
    ) -> StoreResult<Vec<CommitId>> {
        let project_id = project_id.to_string();
        let min_id = min_id.to_vec();
        let max_id = max_id.to_vec();
        self.with_read_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT commit_id FROM commits
                     WHERE project_id = ?1 AND commit_id >= ?2 AND commit_id <= ?3
                     ORDER BY commit_id ASC
                     LIMIT 2",
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite prepare commit prefix query failed: {e}"))
                })?;
            let rows = stmt
                .query_map(params![project_id, min_id, max_id], |row| {
                    row.get::<_, Vec<u8>>(0)
                })
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite execute commit prefix query failed: {e}"))
                })?;
            let mut matches = Vec::new();
            for row in rows {
                matches.push(row.map_err(|e| {
                    StoreError::Backend(format!("SQLite read commit prefix match failed: {e}"))
                })?);
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
        let project_id = project_id.to_string();
        let min_id = min_id.to_vec();
        let max_id = max_id.to_vec();
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        self.with_read_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(|e| {
                    StoreError::Backend(format!(
                        "SQLite begin resolve_change_id_prefix read tx failed: {e}"
                    ))
                })?;
            let mut stmt = tx
                .prepare_cached(
                    "SELECT DISTINCT change_id FROM commits
                     WHERE project_id = ?1 AND change_id >= ?2 AND change_id <= ?3
                     ORDER BY change_id ASC
                     LIMIT 2",
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite prepare change prefix query failed: {e}"))
                })?;
            let ch_rows = stmt
                .query_map(params![project_id, min_id, max_id], |r| {
                    r.get::<_, Vec<u8>>(0)
                })
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite execute change prefix query failed: {e}"))
                })?;
            let mut change_ids = Vec::new();
            for r in ch_rows {
                change_ids.push(r.map_err(|e| {
                    StoreError::Backend(format!("SQLite read change_id match failed: {e}"))
                })?);
            }
            if change_ids.len() != 1 {
                return Ok(change_ids.into_iter().map(|ch| (ch, Vec::new())).collect());
            }
            let matched_ch = change_ids.swap_remove(0);
            let mut c_stmt = tx
                .prepare_cached(
                    "SELECT commit_id FROM commits
                     WHERE project_id = ?1 AND change_id = ?2
                     ORDER BY commit_id ASC
                     LIMIT ?3",
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite prepare change commits query failed: {e}"))
                })?;
            let c_rows = c_stmt
                .query_map(params![project_id, matched_ch, limit_i64], |r| {
                    r.get::<_, Vec<u8>>(0)
                })
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite execute change commits query failed: {e}"))
                })?;
            let mut by_change = BTreeMap::new();
            let mut cids = Vec::new();
            for r in c_rows {
                cids.push(r.map_err(|e| {
                    StoreError::Backend(format!("SQLite read change commit_id failed: {e}"))
                })?);
            }
            by_change.insert(matched_ch, cids);
            Ok(by_change.into_iter().collect())
        })
        .await
    }
}
