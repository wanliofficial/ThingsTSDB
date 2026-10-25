// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to
// you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The `metrics` table, mapping metric names to their ids:
//!
//! | metric_name (pk) | metric_id |
//! |------------------|-----------|
//! | binary           | uint64    |

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use arrow::{
    array::{BinaryArray, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use columnar_storage::{
    storage::{ColumnarStorageRef, ScanRequest, WriteRequest},
    types::{TimeRange, Timestamp},
};
use common::now;
use futures::StreamExt;
use tracing::debug;

use crate::{hash, types::Sample, Result};

pub const METRIC_NAME_COLUMN: &str = "metric_name";
pub const METRIC_ID_COLUMN: &str = "metric_id";

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(METRIC_NAME_COLUMN, DataType::Binary, false),
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
    ]))
}

pub struct MetricManager {
    storage: ColumnarStorageRef,
    /// Metric ids that have been persisted in the storage, used to avoid
    /// writing duplicated metadata rows for every write request.
    known_ids: Mutex<HashSet<u64>>,
}

impl MetricManager {
    pub fn new(storage: ColumnarStorageRef) -> Self {
        Self {
            storage,
            known_ids: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn storage(&self) -> &ColumnarStorageRef {
        &self.storage
    }

    #[cfg(test)]
    pub(crate) fn num_known_ids(&self) -> usize {
        self.known_ids.lock().unwrap().len()
    }

    /// Load all metric ids from storage, so already-known metrics won't be
    /// written again after restart.
    pub async fn load(&self) -> Result<()> {
        let mut stream = self
            .storage
            .scan(ScanRequest {
                range: full_range(),
                predicate: Vec::new(),
                projections: None,
            })
            .await?;

        // Collect first, then take the lock, to avoid holding it across
        // await points.
        let mut scanned_ids = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.context("scan metrics table")?;
            let id_idx = self
                .storage
                .schema()
                .index_of(METRIC_ID_COLUMN)
                .context("find metric_id column")?;
            let id_col = batch
                .column(id_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("metric_id column must be UInt64Array");
            scanned_ids.extend(id_col.iter().flatten());
        }
        let mut ids = self.known_ids.lock().unwrap();
        ids.extend(scanned_ids);
        debug!(num_metrics = ids.len(), "MetricManager loaded");
        Ok(())
    }

    /// Populate metric ids from names.
    /// If a name does not exist, it will be created on demand.
    pub async fn populate_metric_ids(&self, samples: &mut [Sample]) -> Result<()> {
        let mut new_names = Vec::new();
        {
            let mut known_ids = self.known_ids.lock().unwrap();
            for sample in samples.iter_mut() {
                let id = hash(&sample.name);
                sample.name_id = Some(id.into());
                if known_ids.insert(id) {
                    new_names.push((sample.name.clone(), id));
                }
            }
        }
        if new_names.is_empty() {
            return Ok(());
        }

        debug!(num_new_names = new_names.len(), "Persist new metric names");
        let names =
            BinaryArray::from_iter_values(new_names.iter().map(|(name, _)| name.as_slice()));
        let ids = UInt64Array::from_iter_values(new_names.iter().map(|(_, id)| *id));
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(names), Arc::new(ids)])
            .context("build metrics record batch")?;
        let now = now();
        self.storage
            .write(WriteRequest {
                batch,
                // Metadata tables have no business time, use the write time so
                // they are always in the latest segment.
                time_range: single_millis(now),
                enable_check: false,
            })
            .await
    }
}

/// The full time range, used when scanning metadata tables.
pub(crate) fn full_range() -> TimeRange {
    TimeRange::new(Timestamp::MIN, Timestamp::MAX)
}

/// A one-millisecond-wide time range, used by metadata tables whose rows have
/// no business timestamps.
pub(crate) fn single_millis(ts: i64) -> TimeRange {
    TimeRange::new(Timestamp(ts), Timestamp(ts + 1))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use columnar_storage::{
        config::StorageConfig,
        storage::{ObjectBasedStorage, StorageRuntimes},
        types::ObjectStoreRef,
    };
    use object_store::local::LocalFileSystem;

    use super::*;

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_populate_metric_ids() {
        let root_dir = temp_dir::TempDir::new().unwrap();
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let runtimes = StorageRuntimes::new(runtime.clone(), runtime);
        let storage: ColumnarStorageRef = Arc::new(
            ObjectBasedStorage::try_new(
                root_dir.path().to_string_lossy().to_string(),
                Duration::from_hours(12),
                store,
                schema(),
                1,
                StorageConfig::default(),
                runtimes,
            )
            .await
            .unwrap(),
        );

        let manager = MetricManager::new(storage.clone());
        manager.load().await.unwrap();

        let mut samples = vec![
            Sample {
                name: b"http_requests".to_vec(),
                labels: Vec::new(),
                timestamp: 1,
                value: 1.0,
                name_id: None,
                series_id: None,
            },
            Sample {
                name: b"http_errors".to_vec(),
                labels: Vec::new(),
                timestamp: 1,
                value: 1.0,
                name_id: None,
                series_id: None,
            },
        ];
        manager.populate_metric_ids(&mut samples).await.unwrap();
        assert_eq!(samples[0].name_id, Some(hash(b"http_requests").into()));
        assert_eq!(samples[1].name_id, Some(hash(b"http_errors").into()));

        // All ids are persisted and can be loaded after "restart".
        let reloaded = MetricManager::new(storage);
        reloaded.load().await.unwrap();
        let known = reloaded.known_ids.lock().unwrap();
        assert!(known.contains(&hash(b"http_requests")));
        assert!(known.contains(&hash(b"http_errors")));
        assert_eq!(known.len(), 2);
    }
}
