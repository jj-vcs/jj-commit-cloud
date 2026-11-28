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

//! Tree, symlink, and blob metadata persistence for `SqliteMetadataStore`.
//!
//! # Authority and Crash-Safety Ordering
//! `put_tree` and `put_blob_record` write parent rows and child links inside `IMMEDIATE`
//! transactions so partial directory listings or unlinked project blobs never surface.
//!
//! # Concurrency and Merge Invariants
//! Content-addressed trees, symlinks, and blob records are insert-only per project, and
//! blob replica locations merge deduplicated location lists transactionally.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use cc_common::backend::TreeEntry;
use prost::Message;
use rusqlite::{OptionalExtension, TransactionBehavior, params};

use super::SqliteMetadataStore;
use crate::store::{BlobRecord, StoreError, StoreResult};

impl SqliteMetadataStore {
    pub(super) async fn get_tree_impl(
        &self,
        project_id: &str,
        tree_id: &[u8],
    ) -> StoreResult<Option<Vec<TreeEntry>>> {
        let project_id = project_id.to_string();
        let tree_id = tree_id.to_vec();
        self.with_read_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin get_tree read tx failed: {e}"))
                })?;
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM trees WHERE project_id = ?1 AND tree_id = ?2",
                    params![project_id, tree_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite get_tree header failed: {e}")))?;

            if exists.is_none() {
                return Ok(None);
            }

            let mut stmt = tx
                .prepare_cached(
                    "SELECT entry_proto FROM tree_entries
                     WHERE project_id = ?1 AND tree_id = ?2
                     ORDER BY name ASC",
                )
                .map_err(|e| StoreError::Backend(format!("SQLite prepare get_tree failed: {e}")))?;

            let rows = stmt
                .query_map(params![project_id, tree_id], |row| row.get::<_, Vec<u8>>(0))
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite query tree_entries failed: {e}"))
                })?;

            let mut entries = Vec::new();
            for row in rows {
                let bytes = row.map_err(|e| {
                    StoreError::Backend(format!("SQLite read tree_entry row failed: {e}"))
                })?;
                let entry = TreeEntry::decode(bytes.as_slice()).map_err(|e| {
                    StoreError::Corruption(format!("failed to decode TreeEntry protobuf: {e}"))
                })?;
                entries.push(entry);
            }
            Ok(Some(entries))
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
        let project_id = project_id.to_string();
        let tree_id = tree_id.to_vec();
        let after = after_name.unwrap_or("").to_string();
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        self.with_read_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin get_tree_page read tx failed: {e}"))
                })?;
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM trees WHERE project_id = ?1 AND tree_id = ?2",
                    params![project_id, tree_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite get_tree_page header: {e}")))?;
            if exists.is_none() {
                return Ok(None);
            }
            let mut stmt = tx
                .prepare_cached(
                    "SELECT entry_proto FROM tree_entries
                     WHERE project_id = ?1 AND tree_id = ?2 AND name > ?3
                     ORDER BY name ASC
                     LIMIT ?4",
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite prepare get_tree_page failed: {e}"))
                })?;
            let rows = stmt
                .query_map(params![project_id, tree_id, after, limit_i64], |row| {
                    row.get::<_, Vec<u8>>(0)
                })
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite query get_tree_page failed: {e}"))
                })?;
            let mut entries = Vec::new();
            for row in rows {
                let bytes = row.map_err(|e| {
                    StoreError::Backend(format!("SQLite read get_tree_page row failed: {e}"))
                })?;
                let entry = TreeEntry::decode(bytes.as_slice()).map_err(|e| {
                    StoreError::Corruption(format!("failed to decode TreeEntry protobuf: {e}"))
                })?;
                entries.push(entry);
            }
            Ok(Some(entries))
        })
        .await
    }

    pub(super) async fn put_tree_impl(
        &self,
        project_id: &str,
        tree_id: &[u8],
        entries: &[TreeEntry],
    ) -> StoreResult<()> {
        let project_id = project_id.to_string();
        let tree_id = tree_id.to_vec();
        let entries = entries.to_vec();

        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin put_tree tx failed: {e}"))
                })?;

            let inserted = tx
                .execute(
                    "INSERT OR IGNORE INTO trees (project_id, tree_id) VALUES (?1, ?2)",
                    params![project_id, tree_id],
                )
                .map_err(|e| StoreError::Backend(format!("SQLite insert tree failed: {e}")))?;

            if inserted > 0 {
                let mut stmt = tx
                    .prepare_cached(
                        "INSERT OR IGNORE INTO tree_entries (project_id, tree_id, name, entry_proto)
                         VALUES (?1, ?2, ?3, ?4)",
                    )
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite prepare tree_entry failed: {e}"))
                    })?;
                for entry in &entries {
                    let entry_proto = entry.encode_to_vec();
                    stmt.execute(params![project_id, tree_id, entry.name, entry_proto])
                        .map_err(|e| {
                            StoreError::Backend(format!("SQLite insert tree_entry failed: {e}"))
                        })?;
                }
            }

            tx.commit()
                .map_err(|e| StoreError::Backend(format!("SQLite commit put_tree failed: {e}")))
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
        let project_id = project_id.to_string();
        let ids = tree_ids.to_vec();
        self.with_read_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(|e| {
                    StoreError::Backend(format!(
                        "SQLite begin find_existing_trees read tx failed: {e}"
                    ))
                })?;
            let mut stmt = tx
                .prepare_cached("SELECT 1 FROM trees WHERE project_id = ?1 AND tree_id = ?2")
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite prepare find_existing_trees failed: {e}"))
                })?;
            let mut found = HashSet::with_capacity(ids.len());
            for id in ids {
                let exists: Option<i64> = stmt
                    .query_row(params![project_id, id], |r| r.get(0))
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite find_existing_trees failed: {e}"))
                    })?;
                if exists.is_some() {
                    found.insert(id);
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
        let project_id = project_id.to_string();
        let symlink_id = symlink_id.to_vec();
        self.with_read_conn(move |conn| {
            let raw = conn
                .query_row(
                    "SELECT target FROM symlinks WHERE project_id = ?1 AND symlink_id = ?2",
                    params![project_id, symlink_id],
                    |row| match row.get_ref(0)? {
                        rusqlite::types::ValueRef::Text(b) | rusqlite::types::ValueRef::Blob(b) => {
                            Ok(b.to_vec())
                        }
                        other => Err(rusqlite::Error::InvalidColumnType(
                            0,
                            "target".to_string(),
                            other.data_type(),
                        )),
                    },
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite get_symlink failed: {e}")))?;
            raw.map(|bytes| {
                if bytes.contains(&0) {
                    return Err(StoreError::Corruption(
                        "stored symlink target contains NUL byte".to_string(),
                    ));
                }
                String::from_utf8(bytes).map_err(|e| {
                    StoreError::Corruption(format!("stored symlink target is not valid UTF-8: {e}"))
                })
            })
            .transpose()
        })
        .await
    }

    pub(super) async fn put_symlink_impl(
        &self,
        project_id: &str,
        symlink_id: &[u8],
        target: &str,
    ) -> StoreResult<()> {
        let project_id = project_id.to_string();
        let symlink_id = symlink_id.to_vec();
        let target = target.to_string();
        self.with_write_conn(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO symlinks (project_id, symlink_id, target)
                 VALUES (?1, ?2, ?3)",
                params![project_id, symlink_id, target],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite put_symlink failed: {e}")))?;
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
        let project_id = project_id.to_string();
        let ids = symlink_ids.to_vec();
        self.with_read_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(|e| {
                    StoreError::Backend(format!(
                        "SQLite begin find_existing_symlinks read tx failed: {e}"
                    ))
                })?;
            let mut stmt = tx
                .prepare_cached("SELECT 1 FROM symlinks WHERE project_id = ?1 AND symlink_id = ?2")
                .map_err(|e| {
                    StoreError::Backend(format!(
                        "SQLite prepare find_existing_symlinks failed: {e}"
                    ))
                })?;
            let mut found = HashSet::with_capacity(ids.len());
            for id in ids {
                let exists: Option<i64> = stmt
                    .query_row(params![project_id, id], |r| r.get(0))
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite find_existing_symlinks failed: {e}"))
                    })?;
                if exists.is_some() {
                    found.insert(id);
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
        let project_id = project_id.to_string();
        let blob_id = blob_id.to_vec();
        self.with_read_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT b.blob_id, b.content_size, b.inline_content, b.primary_location, b.replicated_locations_json
                     FROM blobs b
                     INNER JOIN project_blobs pb ON pb.blob_id = b.blob_id
                     WHERE pb.project_id = ?1 AND b.blob_id = ?2",
                    params![project_id, blob_id],
                    |r| {
                        Ok((
                            r.get::<_, Vec<u8>>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, Option<Vec<u8>>>(2)?,
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, String>(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| StoreError::Backend(format!("SQLite get_blob_record failed: {e}")))?;

            row.map(|(id, size, inline, primary, repl_json)| {
                let content_size = u64::try_from(size).map_err(|_| {
                    StoreError::Corruption(format!("negative SQLite blob content_size {size}"))
                })?;
                let replicated_locations: Vec<String> = serde_json::from_str(&repl_json)
                    .map_err(|e| {
                        StoreError::Corruption(format!("invalid replicated_locations_json: {e}"))
                    })?;
                Ok(BlobRecord {
                    blob_id: id,
                    content_size,
                    inline_content: inline.map(Bytes::from),
                    primary_location: primary,
                    replicated_locations,
                })
            })
            .transpose()
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
        let project_id = project_id.to_string();
        let ids = blob_ids.to_vec();
        self.with_read_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(|e| {
                    StoreError::Backend(format!(
                        "SQLite begin get_blob_sizes_batch read tx failed: {e}"
                    ))
                })?;
            let mut stmt = tx
                .prepare_cached(
                    "SELECT b.content_size
                     FROM blobs b
                     INNER JOIN project_blobs pb ON pb.blob_id = b.blob_id
                     WHERE pb.project_id = ?1 AND pb.blob_id = ?2",
                )
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite prepare get_blob_sizes_batch failed: {e}"))
                })?;
            let mut sizes = HashMap::with_capacity(ids.len());
            for id in ids {
                let sz: Option<i64> = stmt
                    .query_row(params![project_id, id], |r| r.get(0))
                    .optional()
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite get_blob_sizes_batch failed: {e}"))
                    })?;
                if let Some(s) = sz {
                    let size_u64 = u64::try_from(s).map_err(|_| {
                        StoreError::Corruption(format!("negative SQLite blob content_size {s}"))
                    })?;
                    sizes.insert(id, size_u64);
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
        let blob_id = blob_id.to_vec();
        self.with_read_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT blob_id, content_size, inline_content, primary_location, replicated_locations_json
                     FROM blobs WHERE blob_id = ?1",
                    params![blob_id],
                    |r| {
                        Ok((
                            r.get::<_, Vec<u8>>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, Option<Vec<u8>>>(2)?,
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, String>(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite get_global_blob_record failed: {e}"))
                })?;

            row.map(|(id, size, inline, primary, repl_json)| {
                let content_size = u64::try_from(size).map_err(|_| {
                    StoreError::Corruption(format!("negative SQLite blob content_size {size}"))
                })?;
                let replicated_locations: Vec<String> = serde_json::from_str(&repl_json)
                    .map_err(|e| {
                        StoreError::Corruption(format!("invalid replicated_locations_json: {e}"))
                    })?;
                Ok(BlobRecord {
                    blob_id: id,
                    content_size,
                    inline_content: inline.map(Bytes::from),
                    primary_location: primary,
                    replicated_locations,
                })
            })
            .transpose()
        })
        .await
    }

    pub(super) async fn put_blob_record_impl(
        &self,
        project_id: &str,
        record: &BlobRecord,
    ) -> StoreResult<()> {
        let project_id = project_id.to_string();
        let record = record.clone();
        let content_size_i64 = i64::try_from(record.content_size).map_err(|_| {
            StoreError::InvalidArgument(format!(
                "blob content_size {} exceeds i64::MAX",
                record.content_size
            ))
        })?;
        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite begin put_blob_record tx failed: {e}"))
                })?;

            let existing = tx
                .query_row(
                    "SELECT inline_content, primary_location, replicated_locations_json
                     FROM blobs WHERE blob_id = ?1",
                    params![record.blob_id],
                    |r| {
                        Ok((
                            r.get::<_, Option<Vec<u8>>>(0)?,
                            r.get::<_, Option<String>>(1)?,
                            r.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite read existing blob failed: {e}"))
                })?;

            let (merged_inline, merged_primary, merged_locations) = match existing {
                Some((prev_inline, prev_primary, prev_repl_json)) => {
                    let mut locs: Vec<String> =
                        serde_json::from_str(&prev_repl_json).map_err(|e| {
                            StoreError::Corruption(format!(
                                "invalid replicated_locations_json: {e}"
                            ))
                        })?;
                    for loc in &record.replicated_locations {
                        if !locs.iter().any(|existing_loc| existing_loc == loc) {
                            locs.push(loc.clone());
                        }
                    }
                    (
                        prev_inline.or_else(|| record.inline_content.as_ref().map(|b| b.to_vec())),
                        prev_primary.or_else(|| record.primary_location.clone()),
                        locs,
                    )
                }
                None => (
                    record.inline_content.as_ref().map(|b| b.to_vec()),
                    record.primary_location.clone(),
                    record.replicated_locations.clone(),
                ),
            };
            let repl_json = serde_json::to_string(&merged_locations).map_err(|e| {
                StoreError::Corruption(format!("failed to serialize replicated_locations: {e}"))
            })?;

            tx.execute(
                "INSERT INTO blobs
                 (blob_id, content_size, inline_content, primary_location, replicated_locations_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(blob_id) DO UPDATE SET
                   inline_content = excluded.inline_content,
                   primary_location = excluded.primary_location,
                   replicated_locations_json = excluded.replicated_locations_json",
                params![
                    record.blob_id,
                    content_size_i64,
                    merged_inline.as_deref(),
                    merged_primary,
                    repl_json
                ],
            )
            .map_err(|e| StoreError::Backend(format!("SQLite upsert blobs failed: {e}")))?;

            tx.execute(
                "INSERT OR IGNORE INTO project_blobs (project_id, blob_id) VALUES (?1, ?2)",
                params![project_id, record.blob_id],
            )
            .map_err(|e| {
                StoreError::Backend(format!("SQLite insert project_blobs failed: {e}"))
            })?;

            tx.commit().map_err(|e| {
                StoreError::Backend(format!("SQLite commit put_blob_record failed: {e}"))
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
        self.with_write_conn(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| {
                    StoreError::Backend(format!(
                        "SQLite begin add_blob_replica_location tx failed: {e}"
                    ))
                })?;

            let existing_json: Option<String> = tx
                .query_row(
                    "SELECT replicated_locations_json FROM blobs WHERE blob_id = ?1",
                    params![blob_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    StoreError::Backend(format!("SQLite select blob locations failed: {e}"))
                })?;

            if let Some(raw_json) = existing_json {
                let mut locations: Vec<String> = serde_json::from_str(&raw_json).map_err(|e| {
                    StoreError::Corruption(format!("invalid replicated_locations_json: {e}"))
                })?;
                if !locations.iter().any(|l| l == &location) {
                    locations.push(location);
                    let updated_json = serde_json::to_string(&locations).map_err(|e| {
                        StoreError::Corruption(format!("failed to encode locations: {e}"))
                    })?;
                    tx.execute(
                        "UPDATE blobs SET replicated_locations_json = ?1 WHERE blob_id = ?2",
                        params![updated_json, blob_id],
                    )
                    .map_err(|e| {
                        StoreError::Backend(format!("SQLite update blob locations failed: {e}"))
                    })?;
                }
            }

            tx.commit().map_err(|e| {
                StoreError::Backend(format!(
                    "SQLite commit add_blob_replica_location failed: {e}"
                ))
            })
        })
        .await
    }
}
