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

//! Index for series, composed of two tables:
//!
//! `series`, the mapping from series key to series id:
//!
//! | metric_id (pk) | tsid (pk) | series_key |
//! |----------------|-----------|------------|
//! | uint64         | uint64    | binary     |
//!
//! `index`, the inverted index from label pair to series ids:
//!
//! | metric_id (pk) | tag_key (pk) | tag_value (pk) | tsid (pk) | registered_at |
//! |----------------|--------------|----------------|-----------|----------------|
//! | uint64         | binary       | binary         | uint64    | int64          |
//!
//! `tsid` must be part of primary keys, otherwise multiple series sharing one
//! label pair (e.g. `job=api`) would overwrite each other during scan and
//! compaction.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use arrow::{
    array::{BinaryArray, Int64Array, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use columnar_storage::storage::{ColumnarStorageRef, ScanRequest, WriteRequest};
use common::now;
use futures::StreamExt;
use tracing::debug;

use crate::{
    metric::{full_range, single_millis},
    types::Sample,
    Result,
};

pub const METRIC_ID_COLUMN: &str = "metric_id";
pub const TSID_COLUMN: &str = "tsid";
pub const SERIES_KEY_COLUMN: &str = "series_key";
pub const TAG_KEY_COLUMN: &str = "tag_key";
pub const TAG_VALUE_COLUMN: &str = "tag_value";
pub const REGISTERED_AT_COLUMN: &str = "registered_at";

pub fn series_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new(TSID_COLUMN, DataType::UInt64, false),
        Field::new(SERIES_KEY_COLUMN, DataType::Binary, false),
    ]))
}

pub fn index_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new(TAG_KEY_COLUMN, DataType::Binary, false),
        Field::new(TAG_VALUE_COLUMN, DataType::Binary, false),
        Field::new(TSID_COLUMN, DataType::UInt64, false),
        // The only value column, recording when this series was registered.
        Field::new(REGISTERED_AT_COLUMN, DataType::Int64, false),
    ]))
}

pub struct IndexManager {
    series_storage: ColumnarStorageRef,
    index_storage: ColumnarStorageRef,
    /// Series ids that have been persisted in the storage, used to avoid
    /// writing duplicated index rows for every write request.
    known_series: Mutex<HashSet<u64>>,
}

impl IndexManager {
    pub fn new(series_storage: ColumnarStorageRef, index_storage: ColumnarStorageRef) -> Self {
        Self {
            series_storage,
            index_storage,
            known_series: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn series_storage(&self) -> &ColumnarStorageRef {
        &self.series_storage
    }

    pub(crate) fn index_storage(&self) -> &ColumnarStorageRef {
        &self.index_storage
    }

    #[cfg(test)]
    pub(crate) fn num_known_series(&self) -> usize {
        self.known_series.lock().unwrap().len()
    }

    /// Load all series ids from storage, so already-known series won't be
    /// indexed again after restart.
    pub async fn load(&self) -> Result<()> {
        let mut stream = self
            .series_storage
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
            let batch = batch.context("scan series table")?;
            let id_idx = self
                .series_storage
                .schema()
                .index_of(TSID_COLUMN)
                .context("find tsid column")?;
            let id_col = batch
                .column(id_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("tsid column must be UInt64Array");
            scanned_ids.extend(id_col.iter().flatten());
        }
        let mut series = self.known_series.lock().unwrap();
        series.extend(scanned_ids);
        debug!(num_series = series.len(), "IndexManager loaded");
        Ok(())
    }

    /// Populate series ids from labels.
    /// It will also build inverted index for labels.
    pub async fn populate_series_ids(&self, samples: &mut [Sample]) -> Result<()> {
        struct NewSeries {
            metric_id: u64,
            tsid: u64,
            series_key: Vec<u8>,
            labels: Vec<(Vec<u8>, Vec<u8>)>,
        }

        let mut new_series = Vec::new();
        {
            let mut known_series = self.known_series.lock().unwrap();
            for sample in samples.iter_mut() {
                let series_id = sample.series_id();
                sample.series_id = Some(series_id);
                if known_series.insert(series_id.0) {
                    // name_id is populated by MetricManager beforehand.
                    let metric_id = sample
                        .name_id
                        .expect("name_id must be populated before indexing")
                        .0;
                    let labels = sample
                        .labels
                        .iter()
                        .map(|l| (l.name.clone(), l.value.clone()))
                        .collect();
                    new_series.push(NewSeries {
                        metric_id,
                        tsid: series_id.0,
                        series_key: sample.series_key(),
                        labels,
                    });
                }
            }
        }
        if new_series.is_empty() {
            return Ok(());
        }

        debug!(num_new_series = new_series.len(), "Persist new series");
        let now = now();
        {
            let metric_ids = UInt64Array::from_iter_values(new_series.iter().map(|s| s.metric_id));
            let tsids = UInt64Array::from_iter_values(new_series.iter().map(|s| s.tsid));
            let series_keys =
                BinaryArray::from_iter_values(new_series.iter().map(|s| s.series_key.as_slice()));
            let batch = RecordBatch::try_new(
                series_schema(),
                vec![Arc::new(metric_ids), Arc::new(tsids), Arc::new(series_keys)],
            )
            .context("build series record batch")?;
            self.series_storage
                .write(WriteRequest {
                    batch,
                    time_range: single_millis(now),
                    enable_check: false,
                })
                .await?;
        }

        {
            let mut metric_ids = Vec::new();
            let mut tag_keys = Vec::new();
            let mut tag_values = Vec::new();
            let mut tsids = Vec::new();
            for series in &new_series {
                for (key, value) in &series.labels {
                    metric_ids.push(series.metric_id);
                    tag_keys.push(key.as_slice());
                    tag_values.push(value.as_slice());
                    tsids.push(series.tsid);
                }
            }
            let num_rows = tsids.len();
            let batch = RecordBatch::try_new(
                index_schema(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(metric_ids)),
                    Arc::new(BinaryArray::from_iter_values(tag_keys)),
                    Arc::new(BinaryArray::from_iter_values(tag_values)),
                    Arc::new(UInt64Array::from_iter_values(tsids)),
                    Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                        now, num_rows,
                    ))),
                ],
            )
            .context("build index record batch")?;
            self.index_storage
                .write(WriteRequest {
                    batch,
                    time_range: single_millis(now),
                    enable_check: false,
                })
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::types::Label;

    fn sample(name: &str, code: &str, job: &str) -> super::Sample {
        super::Sample {
            name: name.as_bytes().to_vec(),
            labels: vec![
                Label {
                    name: b"code".to_vec(),
                    value: code.as_bytes().to_vec(),
                },
                Label {
                    name: b"job".to_vec(),
                    value: job.as_bytes().to_vec(),
                },
            ],
            timestamp: 1,
            value: 1.0,
            name_id: Some(crate::hash(name.as_bytes()).into()),
            series_id: None,
        }
    }

    #[test]
    fn test_populate_series_ids_in_memory() {
        // Only verify the id allocation logic here, storage interactions are
        // covered by engine tests.
        let known: std::sync::Mutex<HashSet<u64>> = std::sync::Mutex::new(HashSet::new());
        let mut samples = [
            sample("http_requests", "200", "proxy"),
            sample("http_requests", "200", "proxy"),
            sample("http_requests", "404", "proxy"),
        ];
        {
            let mut known_series = known.lock().unwrap();
            for s in samples.iter_mut() {
                let series_id = s.series_id();
                s.series_id = Some(series_id);
                known_series.insert(series_id.0);
            }
        }
        assert_eq!(samples[0].series_id, samples[1].series_id);
        assert_ne!(samples[0].series_id, samples[2].series_id);
        assert_eq!(known.lock().unwrap().len(), 2);
    }
}
