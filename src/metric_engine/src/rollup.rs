// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
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

//! Write-time rollups (roadmap Phase 3): 1-minute and 1-hour aggregate tables.
//!
//! A downsampled query over a wide range otherwise re-reads every raw sample.
//! The write path therefore folds each sample into a minute bucket and an hour
//! bucket, keeping the aggregates a downsampled query needs (`count/sum/min/
//! max/first/last`), so a range query over months reads a few rows per bucket
//! instead of every sample.
//!
//! Correctness rules, all of which the tests pin down:
//!
//! - **Same LWW semantics as the data table.** A rewritten sample replaces the
//!   older one for the same `(series, timestamp)`, so a bucket's aggregate
//!   equals the aggregate of what a raw scan would return.
//! - **Only closed buckets are published.** A bucket is flushed once
//!   `bucket_end + interval <= now`, so new samples keep landing in an open
//!   bucket that is still in memory.
//! - **A published bucket is never read twice.** The watermark `wm` divides
//!   time: buckets with `start < wm` come from the table, `[wm, now)` from
//!   memory and raw data. A failed flush leaves `wm` untouched and puts the
//!   buckets back, so the split stays exact.
//! - **A late write can invalidate a published bucket.** Writing a sample below
//!   `wm` marks that bucket dirty; the next flush re-reads the raw samples of
//!   just that bucket and republishes it (the table keeps the newer row). Until
//!   then the watermark is lowered to the dirty bucket, so queries fall back to
//!   raw data there instead of trusting a stale aggregate.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use anyhow::Context;
use arrow::{
    array::{Float64Array, Int64Array, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use columnar_storage::{
    config::StorageConfig,
    storage::{ColumnarStorageRef, ObjectBasedStorage, ScanRequest, StorageRuntimes, WriteRequest},
    types::{ObjectStoreRef, TimeRange, Timestamp},
};
use datafusion::{
    logical_expr::{col, lit, Expr},
    scalar::ScalarValue,
};
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore, PutPayload};

use crate::{data, query::Aggregation, types::Sample, Result};

/// The finest rollup resolution: one minute.
pub const MINUTE_MS: i64 = 60_000;
/// The coarsest rollup resolution: one hour.
pub const HOUR_MS: i64 = 3_600_000;

/// Primary keys of the rollup table: metric, series and bucket start.
const NUM_PRIMARY_KEYS: usize = 3;

pub const BUCKET_COLUMN: &str = "timestamp";
pub const COUNT_COLUMN: &str = "count";
pub const SUM_COLUMN: &str = "sum";
pub const MIN_COLUMN: &str = "min";
pub const MAX_COLUMN: &str = "max";
pub const FIRST_TS_COLUMN: &str = "first_timestamp";
pub const FIRST_COLUMN: &str = "first";
pub const LAST_TS_COLUMN: &str = "last_timestamp";
pub const LAST_COLUMN: &str = "last";

/// Index of `name` in `schema`, as an engine error.
fn column_index(schema: &SchemaRef, name: &str) -> Result<usize> {
    schema
        .index_of(name)
        .map_err(|e| anyhow::anyhow!("column {name} in rollup schema: {e}").into())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(data::METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new(data::TSID_COLUMN, DataType::UInt64, false),
        Field::new(BUCKET_COLUMN, DataType::Int64, false),
        Field::new(COUNT_COLUMN, DataType::UInt64, false),
        Field::new(SUM_COLUMN, DataType::Float64, false),
        Field::new(MIN_COLUMN, DataType::Float64, false),
        Field::new(MAX_COLUMN, DataType::Float64, false),
        Field::new(FIRST_TS_COLUMN, DataType::Int64, false),
        Field::new(FIRST_COLUMN, DataType::Float64, false),
        Field::new(LAST_TS_COLUMN, DataType::Int64, false),
        Field::new(LAST_COLUMN, DataType::Float64, false),
    ]))
}

/// Rounds `ts` down to the start of its bucket.
pub fn align_down(ts: i64, interval_ms: i64) -> i64 {
    ts.div_euclid(interval_ms) * interval_ms
}

/// Rounds `ts` up to the start of the next bucket, or `ts` when aligned.
pub fn align_up(ts: i64, interval_ms: i64) -> i64 {
    align_down(ts, interval_ms).saturating_add(if ts.rem_euclid(interval_ms) == 0 {
        0
    } else {
        interval_ms
    })
}

/// Aggregates of one bucket: everything a downsampled query can ask for.
///
/// `first`/`last` carry their timestamps because a coarser step re-buckets
/// finer rollups, and the earliest/latest sample of the coarse bucket is not
/// necessarily the one of the first/last fine bucket.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Rollup {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first_ts: i64,
    pub first: f64,
    pub last_ts: i64,
    pub last: f64,
}

impl Rollup {
    /// The aggregate of one sample.
    pub fn sample(timestamp: i64, value: f64) -> Self {
        Self {
            count: 1,
            sum: value,
            min: value,
            max: value,
            first_ts: timestamp,
            first: value,
            last_ts: timestamp,
            last: value,
        }
    }

    /// Folds one sample in.
    ///
    /// Callers pass at most one sample per timestamp: the buffer already
    /// applies last-write-wins before aggregating, and the data scan dedups by
    /// `__seq__`, so a shared timestamp here would be a new sample, not a
    /// rewrite.
    pub fn add_sample(&mut self, timestamp: i64, value: f64) {
        if self.count == 0 {
            *self = Self::sample(timestamp, value);
            return;
        }
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        if timestamp <= self.first_ts {
            self.first_ts = timestamp;
            self.first = value;
        }
        if timestamp >= self.last_ts {
            self.last_ts = timestamp;
            self.last = value;
        }
    }

    /// Merges another aggregate of a disjoint (or finer) bucket.
    pub fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = *other;
            return;
        }
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        if other.first_ts < self.first_ts {
            self.first_ts = other.first_ts;
            self.first = other.first;
        }
        if other.last_ts >= self.last_ts {
            self.last_ts = other.last_ts;
            self.last = other.last;
        }
    }

    /// The value this aggregate reports for `agg`.
    pub fn value(&self, agg: Aggregation) -> f64 {
        match agg {
            Aggregation::Avg => self.sum / self.count as f64,
            Aggregation::Min => self.min,
            Aggregation::Max => self.max,
            Aggregation::Sum => self.sum,
            Aggregation::Count => self.count as f64,
            Aggregation::Last => self.last,
        }
    }
}

/// One row to publish: the aggregate of `bucket` for one series.
struct Row {
    metric_id: u64,
    tsid: u64,
    bucket: i64,
    agg: Rollup,
}

/// Bucket key: `(metric_id, tsid, bucket_start)`.
type Key = (u64, u64, i64);

/// One buffered bucket: timestamp -> `(write sequence, value)`, so the newest
/// write wins on a shared timestamp and the map stays sorted by time.
type BucketSamples = BTreeMap<i64, (u64, f64)>;

/// The candidate tsids and expected buckets of one metric, as collected for a
/// dirty-bucket rebuild.
type DirtyIndex = HashMap<u64, (HashSet<u64>, HashSet<(u64, i64)>)>;

/// One resolution's open buckets plus the published table.
pub struct RollupTable {
    interval_ms: i64,
    storage: ColumnarStorageRef,
    /// The raw data table, needed to rebuild a bucket invalidated by a late
    /// write.
    data: ColumnarStorageRef,
    store: ObjectStoreRef,
    state_path: ObjectPath,
    /// Buckets that are not published yet: samples keyed by timestamp with the
    /// sequence that decides last-write-wins.
    buffer: Mutex<HashMap<Key, BucketSamples>>,
    /// Buckets a late write invalidated; rebuilt on the next flush.
    dirty: Mutex<HashSet<Key>>,
    seq: AtomicU64,
    /// Published coverage: every sample with `timestamp < watermark` is
    /// aggregated by the table.
    watermark: AtomicI64,
}

impl RollupTable {
    pub fn interval_ms(&self) -> i64 {
        self.interval_ms
    }

    /// Replaces a bucket in a rollup: samples at or after the watermark are
    /// buffered, older ones mark their bucket for a rebuild.
    ///
    /// A late sample is not buffered: its bucket may already have published
    /// rows, and a partial aggregate written later would overwrite the rebuilt
    /// one. Raw data is authoritative until the rebuild runs.
    pub fn record(&self, samples: &[Sample]) {
        let mut buffer = self.buffer.lock().unwrap();
        let watermark = self.watermark.load(Ordering::Relaxed);
        let mut dirty = None;
        for sample in samples {
            let (Some(metric_id), Some(tsid)) = (sample.name_id, sample.series_id) else {
                continue;
            };
            let bucket = align_down(sample.timestamp, self.interval_ms);
            if sample.timestamp < watermark {
                dirty
                    .get_or_insert_with(|| self.dirty.lock().unwrap())
                    .insert((metric_id.0, tsid.0, bucket));
                continue;
            }
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            let entry = buffer.entry((metric_id.0, tsid.0, bucket)).or_default();
            match entry.get_mut(&sample.timestamp) {
                // Last write wins, exactly like the data table's `__seq__`.
                Some(slot) => {
                    if seq >= slot.0 {
                        *slot = (seq, sample.value);
                    }
                }
                None => {
                    entry.insert(sample.timestamp, (seq, sample.value));
                }
            }
        }
    }

    /// The coverage watermark a query may trust: the published one, lowered to
    /// the oldest bucket a late write invalidated.
    pub fn coverage(&self) -> i64 {
        let watermark = self.watermark.load(Ordering::Relaxed);
        let dirty = self.dirty.lock().unwrap();
        if let Some(oldest) = dirty.iter().map(|key| key.2).min() {
            watermark.min(oldest)
        } else {
            watermark
        }
    }

    /// The persisted coverage, used to compare with [`RollupTable::coverage`].
    pub fn published_watermark(&self) -> i64 {
        self.watermark.load(Ordering::Relaxed)
    }

    /// Recomposes the buckets a late write invalidated.
    ///
    /// Separate from [`RollupTable::flush_closed`] because it reads the raw
    /// data table, which is far more expensive than publishing what is already
    /// in memory: the write path only publishes, and this runs on flush and
    /// compaction.
    pub async fn flush_dirty(&self) -> Result<()> {
        let dirty: Vec<Key> = self.dirty.lock().unwrap().iter().copied().collect();
        if dirty.is_empty() {
            return Ok(());
        }
        let rows = self.rebuild_dirty(&dirty).await?;
        self.write_rows(rows).await?;
        let mut guard = self.dirty.lock().unwrap();
        for key in dirty {
            guard.remove(&key);
        }
        Ok(())
    }

    /// Publishes every bucket closed before `now - interval`.
    ///
    /// Buckets are written before they are dropped from memory, so a
    /// concurrent query sees each bucket in at least one place.
    pub async fn flush_closed(&self, now_ms: i64) -> Result<()> {
        let target = align_down(now_ms.saturating_sub(self.interval_ms), self.interval_ms);

        if target <= self.watermark.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Drain the closed buckets, then publish.
        let ready: Vec<(Key, BucketSamples)> = {
            let mut buffer = self.buffer.lock().unwrap();
            let keys: Vec<Key> = buffer
                .keys()
                .filter(|key| key.2 + self.interval_ms <= target)
                .copied()
                .collect();
            keys.into_iter()
                .filter_map(|key| buffer.remove(&key).map(|samples| (key, samples)))
                .collect()
        };
        if ready.is_empty() {
            self.publish(target).await?;
            return Ok(());
        }
        let rows = ready
            .iter()
            .map(|((metric_id, tsid, bucket), samples)| Row {
                metric_id: *metric_id,
                tsid: *tsid,
                bucket: *bucket,
                agg: aggregate(samples),
            })
            .collect();
        if let Err(e) = self.write_rows(rows).await {
            // Put the buckets back so the next flush retries them.
            let mut buffer = self.buffer.lock().unwrap();
            for (key, samples) in ready {
                buffer.insert(key, samples);
            }
            return Err(e);
        }
        self.publish(target).await
    }

    /// Advances and persists the watermark.
    async fn publish(&self, watermark: i64) -> Result<()> {
        self.store
            .put(
                &self.state_path,
                PutPayload::from(watermark.to_string().into_bytes()),
            )
            .await
            .context("persist rollup watermark")?;
        self.watermark.store(watermark, Ordering::Relaxed);
        Ok(())
    }

    /// Re-reads the raw samples of the dirty buckets and recomputes their
    /// aggregates.
    ///
    /// One scan per metric, not per bucket: a fleet-wide late write marks
    /// thousands of buckets at once, and a scan each would read the same files
    /// thousands of times.
    async fn rebuild_dirty(&self, keys: &[Key]) -> Result<Vec<Row>> {
        // metric -> (candidate tsids, expected buckets)
        let mut by_metric: DirtyIndex = HashMap::new();
        for &(metric_id, tsid, bucket) in keys {
            let (tsids, buckets) = by_metric.entry(metric_id).or_default();
            tsids.insert(tsid);
            buckets.insert((tsid, bucket));
        }

        let mut rows = Vec::with_capacity(keys.len());
        for (metric_id, (tsids, expected)) in by_metric {
            let start = expected
                .iter()
                .map(|(_, bucket)| *bucket)
                .min()
                .expect("a metric has at least one dirty bucket");
            let end = expected
                .iter()
                .map(|(_, bucket)| *bucket + self.interval_ms)
                .max()
                .expect("a metric has at least one dirty bucket");
            let mut predicates = vec![
                col(data::METRIC_ID_COLUMN).eq(lit(ScalarValue::UInt64(Some(metric_id)))),
                col(data::TIMESTAMP_COLUMN)
                    .gt_eq(lit(ScalarValue::Int64(Some(start))))
                    .and(col(data::TIMESTAMP_COLUMN).lt(lit(ScalarValue::Int64(Some(end))))),
            ];
            if tsids.len() <= crate::query::IN_LIST_LIMIT {
                let values = tsids
                    .iter()
                    .map(|tsid| Expr::Literal(ScalarValue::UInt64(Some(*tsid))))
                    .collect::<Vec<_>>();
                predicates.push(col(data::TSID_COLUMN).in_list(values, false));
            }

            let mut aggs: HashMap<(u64, i64), Rollup> = HashMap::new();
            let mut stream = self
                .scan_data_with_retry(predicates, "scan data table for rollup rebuild")
                .await?;
            let schema = self.data.schema();
            let tsid_idx = column_index(schema, data::TSID_COLUMN)?;
            let ts_idx = column_index(schema, data::TIMESTAMP_COLUMN)?;
            let value_idx = column_index(schema, data::VALUE_COLUMN)?;
            while let Some(batch) = stream.next().await {
                let batch = batch.context("scan data table for rollup rebuild")?;
                let batch_tsids = batch
                    .column(tsid_idx)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .context("tsid column must be UInt64Array")?;
                let timestamps = batch
                    .column(ts_idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("timestamp column must be Int64Array")?;
                let values = batch
                    .column(value_idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .context("value column must be Float64Array")?;
                for i in 0..batch.num_rows() {
                    let tsid = batch_tsids.value(i);
                    let timestamp = timestamps.value(i);
                    if !tsids.contains(&tsid) {
                        continue;
                    }
                    let bucket = align_down(timestamp, self.interval_ms);
                    if !expected.contains(&(tsid, bucket)) {
                        continue;
                    }
                    // The scan already deduplicated by `__seq__`, so every
                    // timestamp here is a distinct sample.
                    aggs.entry((tsid, bucket))
                        .or_default()
                        .add_sample(timestamp, values.value(i));
                }
            }
            for ((tsid, bucket), agg) in aggs {
                rows.push(Row {
                    metric_id,
                    tsid,
                    bucket,
                    agg,
                });
            }
        }
        Ok(rows)
    }

    /// Scans the data table, retrying when a compaction removed a file between
    /// the manifest lookup and the read.
    ///
    /// A compaction commits its new file before it deletes the old ones, so a
    /// reader that resolved the file list just before the commit can be handed
    /// a file that is already gone. The manifest has moved on by the time the
    /// error surfaces, so re-resolving the plan is the fix.
    async fn scan_data_with_retry(
        &self,
        predicates: Vec<Expr>,
        what: &str,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        const MAX_ATTEMPTS: usize = 4;
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self
                .data
                .scan(ScanRequest {
                    range: crate::metric::full_range(),
                    predicate: predicates.clone(),
                    projections: None,
                })
                .await
            {
                Ok(stream) => return Ok(stream),
                Err(e) if attempt < MAX_ATTEMPTS && is_transient_read_error(&e) => {
                    tracing::debug!(attempt, err = ?e, "{what}: file vanished, retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("{what}: {e}").into());
                }
            }
        }
    }

    /// Appends the rows to the rollup table.
    async fn write_rows(&self, mut rows: Vec<Row>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        rows.sort_by_key(|row| (row.metric_id, row.tsid, row.bucket));
        let mut metric_ids = Vec::with_capacity(rows.len());
        let mut tsids = Vec::with_capacity(rows.len());
        let mut buckets = Vec::with_capacity(rows.len());
        let mut counts = Vec::with_capacity(rows.len());
        let mut sums = Vec::with_capacity(rows.len());
        let mut mins = Vec::with_capacity(rows.len());
        let mut maxes = Vec::with_capacity(rows.len());
        let mut first_ts = Vec::with_capacity(rows.len());
        let mut firsts = Vec::with_capacity(rows.len());
        let mut last_ts = Vec::with_capacity(rows.len());
        let mut lasts = Vec::with_capacity(rows.len());
        for row in &rows {
            metric_ids.push(row.metric_id);
            tsids.push(row.tsid);
            buckets.push(row.bucket);
            counts.push(row.agg.count);
            sums.push(row.agg.sum);
            mins.push(row.agg.min);
            maxes.push(row.agg.max);
            first_ts.push(row.agg.first_ts);
            firsts.push(row.agg.first);
            last_ts.push(row.agg.last_ts);
            lasts.push(row.agg.last);
        }
        let first_bucket = *buckets.first().expect("rows is non-empty");
        let last_bucket = *buckets.last().expect("rows is non-empty");
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(UInt64Array::from_iter_values(metric_ids)),
                Arc::new(UInt64Array::from_iter_values(tsids)),
                Arc::new(Int64Array::from_iter_values(buckets)),
                Arc::new(UInt64Array::from_iter_values(counts)),
                Arc::new(Float64Array::from_iter_values(sums)),
                Arc::new(Float64Array::from_iter_values(mins)),
                Arc::new(Float64Array::from_iter_values(maxes)),
                Arc::new(Int64Array::from_iter_values(first_ts)),
                Arc::new(Float64Array::from_iter_values(firsts)),
                Arc::new(Int64Array::from_iter_values(last_ts)),
                Arc::new(Float64Array::from_iter_values(lasts)),
            ],
        )
        .context("build rollup record batch")?;
        let time_range = TimeRange::new(
            Timestamp(first_bucket),
            Timestamp(last_bucket + self.interval_ms),
        );
        self.storage
            .write(WriteRequest {
                batch,
                time_range,
                enable_check: false,
            })
            .await
    }

    /// Published buckets of `[start, end)` for the candidate series.
    pub async fn read(
        &self,
        metric_id: u64,
        candidates: &HashSet<u64>,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<u64, Vec<(i64, Rollup)>>> {
        let mut out: HashMap<u64, Vec<(i64, Rollup)>> = HashMap::new();
        if end_ms <= start_ms || candidates.is_empty() {
            return Ok(out);
        }
        let mut predicates = vec![
            col(data::METRIC_ID_COLUMN).eq(lit(ScalarValue::UInt64(Some(metric_id)))),
            col(BUCKET_COLUMN)
                .gt_eq(lit(ScalarValue::Int64(Some(start_ms))))
                .and(col(BUCKET_COLUMN).lt(lit(ScalarValue::Int64(Some(end_ms))))),
        ];
        // Same trade-off as the data scan: an IN list pays off only while the
        // candidate set is small; above that the batches are filtered here.
        let pushdown = candidates.len() <= crate::query::IN_LIST_LIMIT;
        if pushdown {
            let values = candidates
                .iter()
                .map(|tsid| Expr::Literal(ScalarValue::UInt64(Some(*tsid))))
                .collect::<Vec<_>>();
            predicates.push(col(data::TSID_COLUMN).in_list(values, false));
        }
        let mut stream = self
            .storage
            .scan(ScanRequest {
                range: crate::metric::full_range(),
                predicate: predicates,
                projections: None,
            })
            .await?;
        let schema = self.storage.schema();
        let tsid_idx = column_index(schema, data::TSID_COLUMN)?;
        let bucket_idx = column_index(schema, BUCKET_COLUMN)?;
        let count_idx = column_index(schema, COUNT_COLUMN)?;
        let sum_idx = column_index(schema, SUM_COLUMN)?;
        let min_idx = column_index(schema, MIN_COLUMN)?;
        let max_idx = column_index(schema, MAX_COLUMN)?;
        let first_ts_idx = column_index(schema, FIRST_TS_COLUMN)?;
        let first_idx = column_index(schema, FIRST_COLUMN)?;
        let last_ts_idx = column_index(schema, LAST_TS_COLUMN)?;
        let last_idx = column_index(schema, LAST_COLUMN)?;
        while let Some(batch) = stream.next().await {
            let batch = batch.context("scan rollup table")?;
            let tsids = batch
                .column(tsid_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("tsid column must be UInt64Array")?;
            let buckets = batch
                .column(bucket_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("bucket column must be Int64Array")?;
            let counts = batch
                .column(count_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("count column must be UInt64Array")?;
            let sums = batch
                .column(sum_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("sum column must be Float64Array")?;
            let mins = batch
                .column(min_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("min column must be Float64Array")?;
            let maxes = batch
                .column(max_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("max column must be Float64Array")?;
            let first_ts = batch
                .column(first_ts_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("first timestamp column must be Int64Array")?;
            let firsts = batch
                .column(first_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("first column must be Float64Array")?;
            let last_ts = batch
                .column(last_ts_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("last timestamp column must be Int64Array")?;
            let lasts = batch
                .column(last_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("last column must be Float64Array")?;
            for i in 0..batch.num_rows() {
                let tsid = tsids.value(i);
                if !pushdown && !candidates.contains(&tsid) {
                    continue;
                }
                out.entry(tsid).or_default().push((
                    buckets.value(i),
                    Rollup {
                        count: counts.value(i),
                        sum: sums.value(i),
                        min: mins.value(i),
                        max: maxes.value(i),
                        first_ts: first_ts.value(i),
                        first: firsts.value(i),
                        last_ts: last_ts.value(i),
                        last: lasts.value(i),
                    },
                ));
            }
        }
        Ok(out)
    }

    /// The table itself, for flush/compact.
    pub fn storage(&self) -> &ColumnarStorageRef {
        &self.storage
    }
}

/// Whether an engine error is a data file that disappeared mid-read.
///
/// Compaction and TTL expiry delete objects after their manifest entry is
/// gone, so a scan that resolved the file list just before that commit opens a
/// path that no longer exists. The manifest has moved on by the time the error
/// surfaces, so the fix is to re-resolve and retry rather than to fail the
/// query.
fn is_transient_read_error(err: &common::Error) -> bool {
    let text = err.to_string();
    text.contains("No such file or directory")
        || text.contains("not found")
        || text.contains("NotFound")
}

/// Folds the buffered samples of one bucket into an aggregate.
fn aggregate(samples: &BucketSamples) -> Rollup {
    let mut agg = Rollup {
        count: 0,
        sum: 0.0,
        min: f64::INFINITY,
        max: f64::NEG_INFINITY,
        first_ts: 0,
        first: 0.0,
        last_ts: 0,
        last: 0.0,
    };
    for (timestamp, (_, value)) in samples {
        agg.add_sample(*timestamp, *value);
    }
    agg
}

/// The 1m and 1h rollup tables of one engine.
pub struct RollupManager {
    minute: RollupTable,
    hour: RollupTable,
    /// Last time [`RollupManager::maybe_flush`] published, so the write path
    /// does not pay a flush per batch.
    last_flush: AtomicI64,
}

impl RollupManager {
    /// Opens (creating if needed) both rollup tables under `{root}/rollups`.
    pub async fn open(
        root: &str,
        store: ObjectStoreRef,
        data: ColumnarStorageRef,
        segment_duration: std::time::Duration,
        storage_opts: StorageConfig,
        runtimes: StorageRuntimes,
    ) -> Result<Self> {
        let minute = Self::open_table(
            root,
            "1m",
            MINUTE_MS,
            store.clone(),
            Arc::clone(&data),
            segment_duration,
            storage_opts.clone(),
            runtimes.clone(),
        )
        .await?;
        let hour = Self::open_table(
            root,
            "1h",
            HOUR_MS,
            store,
            data,
            segment_duration,
            storage_opts,
            runtimes,
        )
        .await?;
        Ok(Self {
            minute,
            hour,
            last_flush: AtomicI64::new(0),
        })
    }

    /// Publishes closed buckets at most once per minute, called from the write
    /// path so a long-running server does not accumulate buffers forever.
    ///
    /// Errors are logged, not returned: a failed rollup flush must not fail an
    /// ingest whose sample is already durable, and the next attempt retries
    /// from the buffers.
    pub async fn maybe_flush(&self, now_ms: i64) {
        let last = self.last_flush.load(Ordering::Relaxed);
        if now_ms - last < MINUTE_MS {
            return;
        }
        if self
            .last_flush
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        if let Err(e) = self.flush_closed(now_ms).await {
            tracing::warn!(err = ?e, "publish closed rollup buckets failed");
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_table(
        root: &str,
        name: &str,
        interval_ms: i64,
        store: ObjectStoreRef,
        data: ColumnarStorageRef,
        segment_duration: std::time::Duration,
        storage_opts: StorageConfig,
        runtimes: StorageRuntimes,
    ) -> Result<RollupTable> {
        let storage: ColumnarStorageRef = Arc::new(
            ObjectBasedStorage::try_new(
                format!("{root}/rollups/{name}"),
                segment_duration,
                store.clone(),
                schema(),
                NUM_PRIMARY_KEYS,
                storage_opts,
                runtimes,
            )
            .await?,
        );
        let state_path = ObjectPath::from(format!("{root}/rollup_state/{name}"));
        let table = RollupTable {
            interval_ms,
            storage,
            data,
            store,
            state_path,
            buffer: Mutex::new(HashMap::new()),
            dirty: Mutex::new(HashSet::new()),
            seq: AtomicU64::new(0),
            watermark: AtomicI64::new(0),
        };
        table.load_watermark().await?;
        Ok(table)
    }

    /// Records samples in both resolutions.
    pub fn record(&self, samples: &[Sample]) {
        if samples.is_empty() {
            return;
        }
        self.minute.record(samples);
        self.hour.record(samples);
    }

    /// Publishes the buckets both tables closed before `now`.
    ///
    /// Does not rebuild dirty buckets; see [`RollupManager::flush_dirty`].
    pub async fn flush_closed(&self, now_ms: i64) -> Result<()> {
        self.minute.flush_closed(now_ms).await?;
        self.hour.flush_closed(now_ms).await
    }

    /// Rebuilds the buckets a late write invalidated, in both tables.
    pub async fn flush_dirty(&self) -> Result<()> {
        self.minute.flush_dirty().await?;
        self.hour.flush_dirty().await
    }

    /// The finest resolution that divides `step_ms`, if one does.
    pub fn for_step(&self, step_ms: i64) -> Option<&RollupTable> {
        if step_ms <= 0 {
            return None;
        }
        if step_ms % HOUR_MS == 0 {
            Some(&self.hour)
        } else if step_ms % MINUTE_MS == 0 {
            Some(&self.minute)
        } else {
            None
        }
    }

    pub fn minute(&self) -> &RollupTable {
        &self.minute
    }

    pub fn hour(&self) -> &RollupTable {
        &self.hour
    }

    /// Both tables, for `flush_all` / `compact`.
    pub fn tables(&self) -> [&RollupTable; 2] {
        [&self.minute, &self.hour]
    }
}

impl RollupTable {
    async fn load_watermark(&self) -> Result<()> {
        match self.store.get(&self.state_path).await {
            Ok(result) => {
                let bytes = result.bytes().await.context("read rollup watermark")?;
                let text = std::str::from_utf8(&bytes).context("decode rollup watermark")?;
                // A torn state file degrades to "nothing published", which
                // makes queries read raw data instead of trusting bad rows.
                let watermark = text.trim().parse().unwrap_or(0);
                self.watermark.store(watermark, Ordering::Relaxed);
            }
            Err(object_store::Error::NotFound { .. }) => {
                self.watermark.store(0, Ordering::Relaxed);
            }
            Err(e) => return Err(anyhow::anyhow!("read rollup watermark: {e}").into()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rollup_aggregates_match_samples() {
        let mut agg = Rollup {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first_ts: 0,
            first: 0.0,
            last_ts: 0,
            last: 0.0,
        };
        for (i, value) in [4.0, 1.0, 9.0, 2.0].iter().enumerate() {
            agg.add_sample(i as i64 * 1_000, *value);
        }
        assert_eq!(agg.count, 4);
        assert_eq!(agg.sum, 16.0);
        assert_eq!(agg.min, 1.0);
        assert_eq!(agg.max, 9.0);
        assert_eq!(agg.first, 4.0);
        assert_eq!(agg.last, 2.0);
        assert_eq!(agg.value(Aggregation::Avg), 4.0);
        assert_eq!(agg.value(Aggregation::Count), 4.0);
    }

    #[test]
    fn test_rollup_first_last_follow_timestamps() {
        let mut agg = Rollup::sample(30_000, 5.0);
        agg.add_sample(10_000, 7.0);
        agg.add_sample(50_000, 6.0);
        assert_eq!(agg.count, 3);
        assert_eq!(agg.first_ts, 10_000);
        assert_eq!(agg.first, 7.0);
        assert_eq!(agg.last_ts, 50_000);
        assert_eq!(agg.last, 6.0);
        assert_eq!(agg.sum, 18.0);
    }

    #[test]
    fn test_rollup_merge_keeps_extremal_timestamps() {
        let mut hour = Rollup::sample(0, 1.0);
        hour.add_sample(30_000, 5.0);
        let mut next = Rollup::sample(60_000, 3.0);
        next.add_sample(90_000, 2.0);
        hour.merge(&next);
        assert_eq!(hour.count, 4);
        assert_eq!(hour.first_ts, 0);
        assert_eq!(hour.last_ts, 90_000);
        assert_eq!(hour.value(Aggregation::Avg), 2.75);
        assert_eq!(hour.value(Aggregation::Min), 1.0);
        assert_eq!(hour.value(Aggregation::Max), 5.0);
        assert_eq!(hour.value(Aggregation::Sum), 11.0);
        assert_eq!(hour.value(Aggregation::Last), 2.0);
    }

    #[test]
    fn test_align_down_handles_negative_timestamps() {
        assert_eq!(align_down(61_000, MINUTE_MS), 60_000);
        assert_eq!(align_down(60_000, MINUTE_MS), 60_000);
        assert_eq!(align_down(-1, MINUTE_MS), -60_000);
    }
}
