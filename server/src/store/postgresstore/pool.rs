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

//! PostgreSQL connection pool configuration, checkout, and retryable transaction runner.
//!
//! # Concurrency and Merge Invariants
//! Configures bounded `deadpool-postgres` wait, create, and recycle timeouts and retries
//! serialization failures (`40001`) and deadlocks (`40P01`) with exponential backoff and
//! jitter, returning pooled connections before sleeping.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::Duration;

use deadpool_postgres::{Object, Timeouts};
use tokio_postgres::Transaction;

use super::PostgresMetadataStore;
use super::codec::{is_retryable_pg_error, map_pg_err, map_pool_err};
use crate::store::{StoreError, StoreResult};

pub(super) const POOL_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) const POOL_CREATE_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) const POOL_RECYCLE_TIMEOUT: Duration = Duration::from_secs(30);

const MAX_TX_ATTEMPTS: usize = 5;

const TX_RETRY_BASE_DELAY_MS: u64 = 10;

pub(super) fn default_pool_timeouts() -> Timeouts {
    Timeouts {
        wait: Some(POOL_WAIT_TIMEOUT),
        create: Some(POOL_CREATE_TIMEOUT),
        recycle: Some(POOL_RECYCLE_TIMEOUT),
    }
}

impl PostgresMetadataStore {
    pub(super) async fn get_client(&self) -> StoreResult<Object> {
        self.pool.get().await.map_err(map_pool_err)
    }

    pub(super) async fn run_tx_with_retry<T, F>(&self, mut op: F) -> StoreResult<T>
    where
        F: for<'a, 'b> FnMut(
            &'a Transaction<'b>,
        ) -> Pin<
            Box<dyn Future<Output = Result<StoreResult<T>, tokio_postgres::Error>> + Send + 'a>,
        >,
    {
        for attempt in 0..MAX_TX_ATTEMPTS {
            let retry_allowed = attempt + 1 < MAX_TX_ATTEMPTS;
            let outcome: Option<StoreResult<T>> = {
                let mut client = self.get_client().await?;
                match client.transaction().await {
                    Ok(tx) => match op(&tx).await {
                        Ok(Ok(val)) => match tx.commit().await {
                            Ok(()) => Some(Ok(val)),
                            Err(err) if is_retryable_pg_error(&err) && retry_allowed => None,
                            Err(err) => Some(Err(map_pg_err(err))),
                        },
                        Ok(Err(store_err)) => {
                            let _ = tx.rollback().await;
                            Some(Err(store_err))
                        }
                        Err(err) if is_retryable_pg_error(&err) && retry_allowed => {
                            let _ = tx.rollback().await;
                            None
                        }
                        Err(err) => {
                            let _ = tx.rollback().await;
                            Some(Err(map_pg_err(err)))
                        }
                    },
                    Err(err) if is_retryable_pg_error(&err) && retry_allowed => None,
                    Err(err) => Some(Err(map_pg_err(err))),
                }
            };
            // `tx` and `client` are returned to the pool before sleeping on retry.
            match outcome {
                Some(result) => return result,
                None => self.record_tx_retry(attempt).await,
            }
        }
        Err(StoreError::Unavailable(
            "PostgreSQL transaction failed after maximum retry attempts".to_string(),
        ))
    }

    async fn record_tx_retry(&self, attempt: usize) {
        if let Some(counter) = &self.tx_retries {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        let base_ms = TX_RETRY_BASE_DELAY_MS.saturating_mul(1u64 << attempt.min(6));
        let jitter_ms = rand::random::<u64>() % base_ms.max(1);
        tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
    }
}
