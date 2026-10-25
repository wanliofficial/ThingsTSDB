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

//! The `data` table, storing all sample values:
//!
//! | metric_id (pk) | tsid (pk) | timestamp (pk) | value |
//! |----------------|-----------|----------------|-------|
//! | uint64         | uint64    | int64          | double|
//!
//! Timestamp is part of the primary keys, so multiple samples of one series
//! can be stored, and re-written samples with the same timestamp dedup to the
//! latest value during scan and compaction.

use std::sync::Arc;

use anyhow::Context;
use arrow::{
    array::{Float64Array, Int64Array, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use columnar_storage::{
    storage::{ColumnarStorageRef, WriteRequest},
    types::{TimeRange, Timestamp},
};

use crate::{types::Sample, Result};

pub const METRIC_ID_COLUMN: &str = "metric_id";
pub const TSID_COLUMN: &str = "tsid";
pub const TIMESTAMP_COLUMN: &str = "timestamp";
pub const VALUE_COLUMN: &str = "value";

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new(TSID_COLUMN, DataType::UInt64, false),
        Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
        Field::new(VALUE_COLUMN, DataType::Float64, false),
    ]))
}

pub struct SampleManager {
    storage: ColumnarStorageRef,
}

impl SampleManager {
    pub fn new(storage: ColumnarStorageRef) -> Self {
        Self { storage }
    }

    pub(crate) fn storage(&self) -> &ColumnarStorageRef {
        &self.storage
    }

    pub async fn persist(&self, samples: Vec<Sample>) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut metric_ids = Vec::with_capacity(samples.len());
        let mut tsids = Vec::with_capacity(samples.len());
        let mut timestamps = Vec::with_capacity(samples.len());
        let mut values = Vec::with_capacity(samples.len());
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for sample in &samples {
            // name_id and series_id are populated by managers beforehand.
            let metric_id = sample
                .name_id
                .expect("name_id must be populated before persisting")
                .0;
            let tsid = sample
                .series_id
                .expect("series_id must be populated before persisting")
                .0;
            metric_ids.push(metric_id);
            tsids.push(tsid);
            timestamps.push(sample.timestamp);
            values.push(sample.value);
            min_ts = min_ts.min(sample.timestamp);
            max_ts = max_ts.max(sample.timestamp);
        }

        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(UInt64Array::from_iter_values(metric_ids)),
                Arc::new(UInt64Array::from_iter_values(tsids)),
                Arc::new(Int64Array::from_iter_values(timestamps)),
                Arc::new(Float64Array::from_iter_values(values)),
            ],
        )
        .context("build data record batch")?;
        let time_range = TimeRange::new(Timestamp(min_ts), Timestamp(max_ts + 1));
        self.storage
            .write(WriteRequest {
                batch,
                time_range,
                // One batch may cross segment boundaries.
                enable_check: false,
            })
            .await
    }
}
