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

//! [`MetricEngine`] organizes all tables of the metric engine under one root
//! path:
//!
//! ```plaintext
//! {root_path}/metrics  # metric name -> metric id
//! {root_path}/series   # series key -> series id
//! {root_path}/index    # label pair -> series ids (inverted index)
//! {root_path}/data     # samples, keyed by (metric_id, tsid, timestamp)
//! ```

use std::{sync::Arc, time::Duration};

use columnar_storage::{
    config::StorageConfig,
    storage::{ColumnarStorageRef, CompactRequest, ObjectBasedStorage, StorageRuntimes},
    types::ObjectStoreRef,
};
use object_store::ObjectStore;

use crate::{
    data::SampleManager,
    index::IndexManager,
    indexdb::{IndexDbManager, IndexMode, IndexQueryStats},
    last_cache::LastCache,
    metric::MetricManager,
    retention::RetentionPolicy,
    rollup::RollupManager,
    types::Sample,
    Result,
};

pub type MetricEngineRef = Arc<MetricEngine>;

async fn open_table_storage(
    path: String,
    segment_duration: Duration,
    store: ObjectStoreRef,
    schema: arrow::datatypes::SchemaRef,
    num_primary_keys: usize,
    storage_opts: StorageConfig,
    runtimes: StorageRuntimes,
) -> Result<ObjectBasedStorage> {
    ObjectBasedStorage::try_new(
        path,
        segment_duration,
        store,
        schema,
        num_primary_keys,
        storage_opts,
        runtimes,
    )
    .await
}

pub struct MetricEngine {
    pub(crate) metric: Arc<MetricManager>,
    pub(crate) index: Arc<IndexManager>,
    pub(crate) data: Arc<SampleManager>,
    /// Month-partitioned mergeset index (ADR-0001 M5). Always written, so a
    /// deployment can switch [`IndexMode`] without a backfill.
    pub(crate) indexdb: Option<Arc<IndexDbManager>>,
    pub(crate) index_mode: IndexMode,
    /// Newest sample per series, mirrored by the write path so instant queries
    /// avoid scanning the data table (roadmap Phase 3).
    pub(crate) last_cache: Arc<LastCache>,
    /// Per-metric read-time retention windows (roadmap Phase 4). Empty by
    /// default; set with [`MetricEngine::with_retention`].
    pub(crate) retention: Arc<RetentionPolicy>,
    /// Write-time 1m/1h rollup tables (roadmap Phase 3). Downsampled queries
    /// read them once their step is a multiple of a resolution.
    pub(crate) rollups: RollupManager,
    /// The engine root prefix, retained so [`MetricEngine::backup`] can copy
    /// the whole tree.
    pub(crate) root_path: String,
    /// The object store backing the four columnar tables.
    pub(crate) store: ObjectStoreRef,
}

impl MetricEngine {
    pub async fn try_open(
        root_path: &str,
        segment_duration: Duration,
        store: ObjectStoreRef,
        storage_opts: StorageConfig,
        runtimes: StorageRuntimes,
    ) -> Result<Self> {
        Self::try_open_with_index_mode(
            root_path,
            segment_duration,
            store,
            storage_opts,
            runtimes,
            IndexMode::default(),
        )
        .await
    }

    /// Opens the engine with an explicit index mode (ADR-0001 §12.5).
    pub async fn try_open_with_index_mode(
        root_path: &str,
        segment_duration: Duration,
        store: ObjectStoreRef,
        storage_opts: StorageConfig,
        runtimes: StorageRuntimes,
        index_mode: IndexMode,
    ) -> Result<Self> {
        // Metadata tables are not TTL-evicted: their rows use write-time
        // timestamps and must survive data expiry.
        //
        // They also stay on Parquet: TSF v1 only models the sample schema
        // (`metric_id, tsid, timestamp, value`), and `StorageConfig::format`
        // applies to the data table (ADR-0002 §12.1).
        let mut metadata_opts = storage_opts.clone();
        metadata_opts.scheduler.ttl = None;
        metadata_opts.format = columnar_storage::config::DataFileFormat::Parquet;
        // Retain a handle for backup/restore after the tables take ownership.
        let backup_store = store.clone();
        let metric_storage: ColumnarStorageRef = Arc::new(
            open_table_storage(
                format!("{root_path}/metrics"),
                segment_duration,
                store.clone(),
                crate::metric::schema(),
                1,
                metadata_opts.clone(),
                runtimes.clone(),
            )
            .await?,
        );
        let series_storage: ColumnarStorageRef = Arc::new(
            open_table_storage(
                format!("{root_path}/series"),
                segment_duration,
                store.clone(),
                crate::index::series_schema(),
                2,
                metadata_opts.clone(),
                runtimes.clone(),
            )
            .await?,
        );
        let index_storage: ColumnarStorageRef = Arc::new(
            open_table_storage(
                format!("{root_path}/index"),
                segment_duration,
                store.clone(),
                crate::index::index_schema(),
                4,
                metadata_opts,
                runtimes.clone(),
            )
            .await?,
        );
        let data_storage: ColumnarStorageRef = Arc::new(
            open_table_storage(
                format!("{root_path}/data"),
                segment_duration,
                store.clone(),
                crate::data::schema(),
                3,
                storage_opts.clone(),
                runtimes.clone(),
            )
            .await?,
        );

        let metric = Arc::new(MetricManager::new(metric_storage));
        let index = Arc::new(IndexManager::new(series_storage, index_storage));
        // Write-time rollups read the raw data table to rebuild a bucket a
        // late write invalidated, so they share the same storage handle.
        let rollups = RollupManager::open(
            root_path,
            store.clone(),
            Arc::clone(&data_storage),
            segment_duration,
            storage_opts,
            runtimes,
        )
        .await?;
        let data = Arc::new(SampleManager::new(data_storage));
        let indexdb = Arc::new(
            IndexDbManager::open(root_path, Default::default())
                .map_err(|e| anyhow::anyhow!("open indexdb: {e}"))?,
        );
        let engine = Self {
            metric,
            index,
            data,
            indexdb: Some(indexdb),
            index_mode,
            last_cache: Arc::new(LastCache::with_default_capacity()),
            retention: Arc::new(RetentionPolicy::new()),
            rollups,
            root_path: root_path.to_string(),
            store: backup_store,
        };
        // Warm up caches so known metrics and series won't be re-persisted.
        engine.metric.load().await?;
        engine.index.load().await?;
        Ok(engine)
    }

    /// Write samples, the full pipeline is:
    /// 1. allocate metric ids for metric names;
    /// 2. allocate series ids for series keys and build the inverted index;
    /// 3. register the series days in the month-partitioned indexDB;
    /// 4. persist all samples.
    pub async fn write(&self, mut samples: Vec<Sample>) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        self.metric.populate_metric_ids(&mut samples).await?;
        self.index.populate_series_ids(&mut samples).await?;
        // Dual-write the mergeset index: it stays in sync while the legacy path
        // is still authoritative, so switching modes never needs a backfill.
        if let Some(indexdb) = &self.indexdb {
            let indexdb = Arc::clone(indexdb);
            let batch = samples.clone();
            tokio::task::spawn_blocking(move || indexdb.register_samples(&batch))
                .await
                .map_err(|e| anyhow::anyhow!("indexdb write task: {e}"))??;
        }
        // Data is about to become durable; capture the samples first because
        // `persist` consumes the vector.
        let last = self.last_cache.clone();
        let mirrored: Vec<Sample> = samples
            .iter()
            .filter(|s| s.series_id.is_some())
            .cloned()
            .collect();
        let result = self.data.persist(samples).await;
        if result.is_ok() {
            // The rollups mirror the durable data, so a bucket's aggregate is
            // never ahead of what a raw scan would return.
            self.rollups.record(&mirrored);
            last.update(&mirrored);
            // Publish closed buckets periodically so the buffers stay bounded.
            self.rollups.maybe_flush(common::now()).await;
        }
        result
    }

    /// The index mode this engine was opened with.
    pub fn index_mode(&self) -> IndexMode {
        self.index_mode
    }

    /// The month-partitioned indexDB, when enabled.
    pub fn indexdb(&self) -> Option<&Arc<IndexDbManager>> {
        self.indexdb.as_ref()
    }

    /// Query counters of the new index path (ADR-0001 §12.5).
    pub fn index_stats(&self) -> Option<&IndexQueryStats> {
        self.indexdb.as_ref().map(|db| db.stats())
    }

    /// The per-series latest-value cache.
    pub fn last_cache(&self) -> &LastCache {
        &self.last_cache
    }

    /// Attaches a per-metric retention policy and returns the engine.
    ///
    /// Queries for a configured metric only expose samples inside its retention
    /// window (see [`RetentionPolicy`]). Consuming `self` keeps the borrow
    /// checker happy without a lock on the hot query path.
    pub fn with_retention(mut self, policy: RetentionPolicy) -> Self {
        self.retention = Arc::new(policy);
        self
    }

    /// The active per-metric retention policy.
    pub fn retention(&self) -> &RetentionPolicy {
        &self.retention
    }

    /// The write-time 1m/1h rollup tables.
    pub fn rollups(&self) -> &RollupManager {
        &self.rollups
    }

    /// Trigger compaction of all tables.
    pub async fn compact(&self) -> Result<()> {
        let storages = [
            (self.metric.storage(), "metrics"),
            (self.index.series_storage(), "series"),
            (self.index.index_storage(), "index"),
            (self.data.storage(), "data"),
            (self.rollups.minute().storage(), "rollup-1m"),
            (self.rollups.hour().storage(), "rollup-1h"),
        ];
        for (storage, name) in storages {
            if let Err(e) = storage.compact(CompactRequest::default()).await {
                // Continue with other tables, one failure should not block the
                // rest.
                tracing::warn!(name, err = ?e, "Compact table failed");
            }
        }
        Ok(())
    }

    /// Flushes every table and the mergeset index to a durable point.
    ///
    /// After this returns, every buffered sample is an SST committed by the
    /// manifest and every pending index row is in an indexdb part, so the
    /// on-disk state is a consistent snapshot for
    /// [`MetricEngine::backup`].
    pub async fn flush_all(&self) -> Result<()> {
        // Rebuild the buckets a late write invalidated, then publish the ones
        // that closed before now, so the flush below also makes them durable.
        // Both read/write the raw data table, so they stay off the write path.
        self.rollups.flush_dirty().await?;
        self.rollups.flush_closed(common::now()).await?;
        let storages = [
            (self.metric.storage(), "metrics"),
            (self.index.series_storage(), "series"),
            (self.index.index_storage(), "index"),
            (self.data.storage(), "data"),
            (self.rollups.minute().storage(), "rollup-1m"),
            (self.rollups.hour().storage(), "rollup-1h"),
        ];
        for (storage, name) in storages {
            storage
                .flush()
                .await
                .map_err(|e| anyhow::anyhow!("flush {name} table: {e}"))?;
        }
        if let Some(indexdb) = &self.indexdb {
            indexdb
                .flush()
                .map_err(|e| anyhow::anyhow!("flush indexdb: {e}"))?;
        }
        Ok(())
    }

    /// Copies the durable engine state to `dest_root` under the same object
    /// store (roadmap Phase 4 backup).
    ///
    /// The engine is flushed first, then every object under the root — the
    /// columnar tables, the mergeset index and (for the local store) the WAL —
    /// is copied to `dest_root`, preserving the relative layout. A fresh
    /// `MetricEngine` opened at `dest_root` answers the same queries. Run this
    /// against a quiescent engine; the copy is not atomic with concurrent
    /// writes or compaction.
    pub async fn backup(&self, dest_root: &str) -> Result<crate::backup::BackupStats> {
        self.flush_all().await?;
        crate::backup::copy_prefix(
            self.store.as_ref(),
            &self.root_path,
            self.store.as_ref(),
            dest_root,
        )
        .await
    }

    /// Restores engine state from `src_root` into `dest_root` (roadmap Phase 4
    /// backup). A `MetricEngine` opened at `dest_root` afterwards sees the
    /// backed-up data.
    pub async fn restore_from(
        store: &dyn ObjectStore,
        src_root: &str,
        dest_root: &str,
    ) -> Result<crate::backup::BackupStats> {
        crate::backup::copy_prefix(store, src_root, store, dest_root).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, UInt64Array};
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::types::Label;

    /// Scan the whole time range of a storage and collect all record batches.
    async fn scan_all(storage: &ColumnarStorageRef) -> Vec<RecordBatch> {
        use columnar_storage::storage::ScanRequest;
        use futures::TryStreamExt;

        use crate::metric::full_range;

        let stream = storage
            .scan(ScanRequest {
                range: full_range(),
                predicate: Vec::new(),
                projections: None,
            })
            .await
            .unwrap();
        stream.try_collect().await.unwrap()
    }

    async fn open_engine(root: &temp_dir::TempDir) -> MetricEngine {
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let runtimes = StorageRuntimes::new(rt.clone(), rt);
        MetricEngine::try_open(
            &root.path().to_string_lossy(),
            Duration::from_hours(2),
            store,
            StorageConfig::default(),
            runtimes,
        )
        .await
        .unwrap()
    }

    fn make_sample(name: &str, code: Option<&str>, job: &str, ts: i64, value: f64) -> Sample {
        let mut labels = Vec::new();
        if let Some(code) = code {
            labels.push(Label {
                name: b"code".to_vec(),
                value: code.as_bytes().to_vec(),
            });
        }
        labels.push(Label {
            name: b"job".to_vec(),
            value: job.as_bytes().to_vec(),
        });
        Sample {
            name: name.as_bytes().to_vec(),
            labels,
            timestamp: ts,
            value,
            name_id: None,
            series_id: None,
        }
    }

    async fn count_rows(storage: &ColumnarStorageRef) -> usize {
        let mut total = 0;
        for batch in scan_all(storage).await {
            total += batch.num_rows();
        }
        total
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_engine_write_and_scan() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine(&root).await;

        // m1: A = {code=200, job=api}, B = {code=404, job=api}; m2: C =
        // {job=db}
        engine
            .write(vec![
                make_sample("m1", Some("200"), "api", 1, 1.0),
                make_sample("m1", Some("200"), "api", 2, 2.0),
                make_sample("m1", Some("404"), "api", 1, 10.0),
                make_sample("m2", None, "db", 5, 42.0),
            ])
            .await
            .unwrap();

        // Re-write A@2 with a new value, and append A@4.
        engine
            .write(vec![
                make_sample("m1", Some("200"), "api", 2, 22.0),
                make_sample("m1", Some("200"), "api", 4, 4.0),
            ])
            .await
            .unwrap();

        // Verify data table: 6 unique (metric_id, tsid, timestamp) rows, and
        // A@2 is overwritten by the newer value.
        let data: HashMap<(_, _, _), f64> = {
            let mut m = HashMap::new();
            for batch in scan_all(engine.data.storage()).await {
                let metric_ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let tsids = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let timestamps = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let values = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    m.insert(
                        (metric_ids.value(i), tsids.value(i), timestamps.value(i)),
                        values.value(i),
                    );
                }
            }
            m
        };
        assert_eq!(data.len(), 5);

        let tsid = |name: &str, code: Option<&str>, job: &str| {
            make_sample(name, code, job, 0, 0.0).series_id().0
        };
        let metric_id = |name: &str| crate::hash(name.as_bytes());
        let a = (metric_id("m1"), tsid("m1", Some("200"), "api"));
        let b = (metric_id("m1"), tsid("m1", Some("404"), "api"));
        let c = (metric_id("m2"), tsid("m2", None, "db"));
        assert_eq!(data.get(&(a.0, a.1, 1)), Some(&1.0));
        assert_eq!(data.get(&(a.0, a.1, 2)), Some(&22.0));
        assert_eq!(data.get(&(a.0, a.1, 4)), Some(&4.0));
        assert_eq!(data.get(&(b.0, b.1, 1)), Some(&10.0));
        assert_eq!(data.get(&(c.0, c.1, 5)), Some(&42.0));

        // Verify metadata tables: 2 metrics, 3 series, 5 inverted index rows.
        assert_eq!(count_rows(engine.metric.storage()).await, 2);
        assert_eq!(count_rows(engine.index.series_storage()).await, 3);
        assert_eq!(count_rows(engine.index.index_storage()).await, 5);

        // Reopen the engine on the same directory, caches should be warmed up
        // so re-writing known series won't append new metadata rows.
        let engine = open_engine(&root).await;
        assert_eq!(engine.metric.num_known_ids(), 2);
        assert_eq!(engine.index.num_known_series(), 3);
        engine
            .write(vec![make_sample("m1", Some("200"), "api", 6, 6.0)])
            .await
            .unwrap();
        assert_eq!(count_rows(engine.metric.storage()).await, 2);
        assert_eq!(count_rows(engine.index.series_storage()).await, 3);
        assert_eq!(count_rows(engine.index.index_storage()).await, 5);
    }

    /// Concurrent load test (roadmap Phase 4, load-jitter): writers append
    /// while readers query, and every query stays correct. Asserts the
    /// invariants (no query errors, resolved series is a subset of the written
    /// set, the full set is visible once writes drain) and reports the
    /// read-latency spread as jitter evidence.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_concurrent_writes_and_queries() {
        use crate::query::SearchRequest;

        let root = temp_dir::TempDir::new().unwrap();
        let engine = Arc::new(open_engine(&root).await);

        let series_count = 16usize;
        let per_series = 64i64;
        let writers = 4usize;
        let readers = 4usize;
        let iters = 40usize;

        // Writers append concurrently; each round every writer contributes a
        // new timestamp to every series, so readers see a moving data frontier.
        let mut write_tasks = Vec::new();
        for w in 0..writers {
            let engine = Arc::clone(&engine);
            write_tasks.push(tokio::spawn(async move {
                for round in 0..per_series {
                    let mut batch = Vec::with_capacity(series_count);
                    for s in 0..series_count {
                        let code = format!("c{}", s % 4);
                        let job = format!("j{}", s / 4);
                        batch.push(make_sample(
                            "load",
                            Some(&code),
                            &job,
                            round * 1000 + w as i64,
                            s as f64 + round as f64,
                        ));
                    }
                    engine.write(batch).await.unwrap();
                }
            }));
        }

        // Readers run instant and range queries while writes are in flight.
        let req = SearchRequest {
            metric_name: b"load".to_vec(),
            matchers: Vec::new(),
            start_ms: 0,
            end_ms: i64::MAX,
            limit: 0,
        };
        let mut read_tasks = Vec::new();
        for _ in 0..readers {
            let engine = Arc::clone(&engine);
            let req = SearchRequest { ..req.clone() };
            read_tasks.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(iters);
                let mut peak_series = 0usize;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    let latest = engine.last_values(&req).await.unwrap();
                    latencies.push(start.elapsed().as_micros() as u64);
                    peak_series = peak_series.max(latest.len());
                }
                (latencies, peak_series)
            }));
        }

        for task in write_tasks {
            task.await.unwrap();
        }
        let mut latencies = Vec::new();
        let mut peak_series = 0usize;
        for task in read_tasks {
            let (mut lat, peak) = task.await.unwrap();
            latencies.append(&mut lat);
            peak_series = peak_series.max(peak);
        }

        // No query resolved more series than were written.
        assert!(
            peak_series <= series_count,
            "resolved {peak_series} series, only {series_count} exist"
        );

        // Once all writers drain, the full set is visible and consistent.
        let final_latest = engine.last_values(&req).await.unwrap();
        assert_eq!(final_latest.len(), series_count);

        // Jitter evidence: report the read-latency spread across all readers.
        latencies.sort_unstable();
        let n = latencies.len();
        let p50 = latencies[n / 2];
        let p99 = latencies[(n * 99) / 100];
        let max = latencies[n - 1];
        eprintln!("concurrent load jitter: {n} reads, p50={p50}us p99={p99}us max={max}us");
        // A generous ceiling so the test never flakes on a busy machine, while
        // a pathological stall (deadlock, contention regression) would blow
        // past it.
        assert!(max < 5_000_000, "query under concurrent load took {max}us");
    }

    // --- write-time rollups (roadmap Phase 3) -----------------------------

    use crate::query::{downsample, Aggregation, SearchRequest, SeriesSamples};

    /// A multiple of both an hour and a minute, so every rollup bucket in the
    /// tests is aligned.
    const BASE: i64 = 1_699_999_200_000;

    fn query(name: &str, start: i64, end: i64) -> SearchRequest {
        SearchRequest {
            metric_name: name.as_bytes().to_vec(),
            matchers: Vec::new(),
            start_ms: start,
            end_ms: end,
            limit: 0,
        }
    }

    /// Ten minutes of two series, sampled every 10 s and 15 s.
    async fn write_history(engine: &MetricEngine) {
        let mut samples = Vec::new();
        for i in 0..60i64 {
            samples.push(make_sample(
                "cpu",
                Some("200"),
                "api",
                BASE + i * 10_000,
                (i as f64 * 7.0) % 13.0 - 3.0,
            ));
        }
        for i in 0..40i64 {
            samples.push(make_sample(
                "cpu",
                Some("404"),
                "api",
                BASE + i * 15_000,
                100.0 - i as f64,
            ));
        }
        engine.write(samples).await.unwrap();
    }

    /// The reference answer: raw samples reduced by [`downsample`].
    async fn raw_downsampled(
        engine: &MetricEngine,
        req: &SearchRequest,
        step_ms: i64,
        agg: Aggregation,
    ) -> Vec<SeriesSamples> {
        let mut series = engine.search(req).await.unwrap();
        for series in &mut series {
            series.samples = downsample(&series.samples, step_ms, agg);
        }
        series
    }

    fn assert_same_series(got: &[SeriesSamples], want: &[SeriesSamples], what: &str) {
        assert_eq!(got.len(), want.len(), "{what}: series count");
        for (got, want) in got.iter().zip(want) {
            assert_eq!(got.metric_name, want.metric_name, "{what}: name");
            assert_eq!(got.labels.len(), want.labels.len(), "{what}: labels");
            assert_eq!(
                got.samples.len(),
                want.samples.len(),
                "{what}: point count of {:?}",
                String::from_utf8_lossy(&got.metric_name)
            );
            for (got, want) in got.samples.iter().zip(&want.samples) {
                assert_eq!(got.timestamp, want.timestamp, "{what}: timestamp");
                assert!(
                    (got.value - want.value).abs() <= 1e-9 * want.value.abs().max(1.0),
                    "{what}: value {} vs {}",
                    got.value,
                    want.value
                );
            }
        }
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_rollup_matches_raw_downsample() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine(&root).await;
        write_history(&engine).await;
        // Publishing is what moves buckets from memory into the table.
        engine.flush_all().await.unwrap();

        let aggs = [
            Aggregation::Avg,
            Aggregation::Min,
            Aggregation::Max,
            Aggregation::Sum,
            Aggregation::Count,
            Aggregation::Last,
        ];
        // 1m reads the minute table, 5m and 1h exercise re-aggregation (5m
        // folds minute buckets, 1h folds hour buckets).
        for step in [60_000i64, 300_000, 3_600_000] {
            let table = engine.rollups().for_step(step).expect("step is covered");
            assert!(
                table.coverage() > BASE,
                "step {step} should have published buckets"
            );
            // An aligned range and one that starts mid-bucket, so the leading
            // partial bucket is served from raw data.
            for (start, end) in [
                (BASE, BASE + 600_000),
                (BASE + 30_000, BASE + 600_000),
                (BASE + 12_345, BASE + 480_000),
            ] {
                let req = query("cpu", start, end);
                for agg in aggs {
                    let want = raw_downsampled(&engine, &req, step, agg).await;
                    let got = engine.search_downsampled(&req, step, agg).await.unwrap();
                    assert_same_series(
                        &got,
                        &want,
                        &format!("step {step} agg {agg:?} range {start}..{end}"),
                    );
                }
            }
        }
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_rollup_matches_raw_after_open_and_late_writes() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine(&root).await;
        write_history(&engine).await;
        engine.flush_all().await.unwrap();

        // A sample newer than everything published: it lands in an open bucket
        // and must be served from raw data.
        engine
            .write(vec![make_sample(
                "cpu",
                Some("200"),
                "api",
                BASE + 9 * 60_000 + 30_000,
                1_000.0,
            )])
            .await
            .unwrap();
        let req = query("cpu", BASE - 60_000, BASE + 600_000);
        for step in [60_000i64, 300_000] {
            let want = raw_downsampled(&engine, &req, step, Aggregation::Avg).await;
            let got = engine
                .search_downsampled(&req, step, Aggregation::Avg)
                .await
                .unwrap();
            assert_same_series(&got, &want, &format!("open tail, step {step}"));
        }

        // A rewrite of an already published sample marks its bucket dirty.
        engine
            .write(vec![make_sample(
                "cpu",
                Some("200"),
                "api",
                BASE + 5_000,
                500.0,
            )])
            .await
            .unwrap();
        let minute = engine.rollups().minute();
        assert!(
            minute.coverage() <= BASE,
            "a late write must lower coverage to the dirty bucket"
        );
        let want = raw_downsampled(&engine, &req, 60_000, Aggregation::Max).await;
        let got = engine
            .search_downsampled(&req, 60_000, Aggregation::Max)
            .await
            .unwrap();
        assert_same_series(&got, &want, "dirty bucket falls back to raw data");

        // The next flush rebuilds the dirty bucket from raw data.
        engine.flush_all().await.unwrap();
        assert!(engine.rollups().minute().coverage() > BASE + 60_000);
        for agg in [Aggregation::Max, Aggregation::Sum, Aggregation::Count] {
            let want = raw_downsampled(&engine, &req, 60_000, agg).await;
            let got = engine.search_downsampled(&req, 60_000, agg).await.unwrap();
            assert_same_series(&got, &want, &format!("rebuilt bucket, agg {agg:?}"));
        }
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_rollup_serves_open_bucket_from_raw_data() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine(&root).await;
        write_history(&engine).await;
        engine.flush_all().await.unwrap();

        // A sample in the current minute: its bucket is still open, so the
        // query must reach it through raw data rather than the table.
        let now = common::now();
        engine
            .write(vec![make_sample(
                "cpu",
                Some("200"),
                "api",
                now - 5_000,
                1_234.0,
            )])
            .await
            .unwrap();
        let minute = engine.rollups().minute();
        assert!(
            minute.coverage() <= now - 5_000,
            "the current minute must stay unpublished"
        );

        let req = query("cpu", BASE, now + 60_000);
        for agg in [Aggregation::Avg, Aggregation::Max, Aggregation::Last] {
            let want = raw_downsampled(&engine, &req, 60_000, agg).await;
            let got = engine.search_downsampled(&req, 60_000, agg).await.unwrap();
            assert_same_series(&got, &want, &format!("open bucket, agg {agg:?}"));
        }
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_rollup_falls_back_for_unaligned_step_and_survives_restart() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine(&root).await;
        write_history(&engine).await;
        engine.flush_all().await.unwrap();

        // 90 s is not a multiple of a maintained resolution: raw downsample.
        let req = query("cpu", BASE, BASE + 600_000);
        assert!(engine.rollups().for_step(90_000).is_none());
        let want = raw_downsampled(&engine, &req, 90_000, Aggregation::Avg).await;
        let got = engine
            .search_downsampled(&req, 90_000, Aggregation::Avg)
            .await
            .unwrap();
        assert_same_series(&got, &want, "unaligned step");

        // Reopening loads the persisted watermark, so the table is still
        // trusted and answers the same as raw data.
        drop(engine);
        let engine = open_engine(&root).await;
        assert!(engine.rollups().minute().coverage() > BASE);
        for step in [60_000i64, 3_600_000] {
            for agg in [Aggregation::Avg, Aggregation::Min, Aggregation::Last] {
                let want = raw_downsampled(&engine, &req, step, agg).await;
                let got = engine.search_downsampled(&req, step, agg).await.unwrap();
                assert_same_series(&got, &want, &format!("restart step {step} agg {agg:?}"));
            }
        }
    }
}
