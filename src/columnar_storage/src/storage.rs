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

use std::{sync::Arc, time::Duration, vec};

use anyhow::Context;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    self,
    execution::SendableRecordBatchStream,
    logical_expr::Expr,
    physical_plan::{execute_stream, union::UnionExec, EmptyRecordBatchStream},
    prelude::SessionContext,
};
use itertools::Itertools;
use parquet::{
    file::properties::WriterProperties, format::SortingColumn, schema::types::ColumnPath,
};
use tokio::{runtime::Runtime, sync::mpsc};

use crate::{
    buffer::{spawn_flush_loop, FlushState},
    compaction::CompactionScheduler,
    config::{StorageConfig, WriteConfig},
    manifest::{Manifest, ManifestRef},
    read::ParquetReader,
    sst::SstPathGenerator,
    types::{ObjectStoreRef, StorageSchema, TimeRange},
    wal::Wal,
    Result,
};

pub struct WriteRequest {
    pub batch: RecordBatch,
    pub time_range: TimeRange,
    // Check data is valid if it's true.
    pub enable_check: bool,
}

pub struct ScanRequest {
    pub range: TimeRange,
    pub predicate: Vec<Expr>,
    /// `None` means all columns.
    pub projections: Option<Vec<usize>>,
}

#[derive(Default)]
pub struct CompactRequest {}

/// Time-aware merge storage interface.
#[async_trait]
pub trait ColumnarStorage {
    fn schema(&self) -> &SchemaRef;

    async fn write(&self, req: WriteRequest) -> Result<()>;

    /// Implementation shoule ensure that the returned stream is sorted by time,
    /// from old to latest.
    async fn scan(&self, req: ScanRequest) -> Result<SendableRecordBatchStream>;

    async fn compact(&self, req: CompactRequest) -> Result<()>;
}

pub type ColumnarStorageRef = Arc<dyn ColumnarStorage + Send + Sync>;

#[derive(Clone)]
pub struct StorageRuntimes {
    manifest_compact_runtime: Arc<Runtime>,
    sst_compact_runtime: Arc<Runtime>,
}

impl StorageRuntimes {
    pub fn new(manifest_compact_runtime: Arc<Runtime>, sst_compact_runtime: Arc<Runtime>) -> Self {
        Self {
            manifest_compact_runtime,
            sst_compact_runtime,
        }
    }
}

/// `ObjectBasedStorage` implements `ColumnarStorage` utilizing cloud object
/// storage (e.g., S3), dividing data into distinct segments based on time
/// ranges(often referred to as `segment_duration`) .
///
/// Compaction is facilitated through the merging of SST files within a single
/// segment, making it simple to support expiration.
#[allow(dead_code)]
pub struct ObjectBasedStorage {
    segment_duration: Duration,
    path: String,
    store: ObjectStoreRef,
    schema: StorageSchema,
    manifest: ManifestRef,
    runtimes: StorageRuntimes,
    parquet_reader: Arc<ParquetReader>,
    write_props: WriterProperties,
    sst_path_gen: Arc<SstPathGenerator>,
    compact_scheduler: CompactionScheduler,
    flush_state: Arc<FlushState>,
    flush_tx: mpsc::Sender<()>,
}

/// It will organize the data in the following way:
/// ```plaintext
/// {root_path}/manifest/snapshot
/// {root_path}/manifest/timestamp1
/// {root_path}/manifest/timestamp2
/// {root_path}/manifest/...
/// {root_path}/data/timestamp_a.sst
/// {root_path}/data/timestamp_b.sst
/// {root_path}/data/...
/// ```
/// `root_path` is composed of `path` and `segment_duration`.
impl ObjectBasedStorage {
    pub async fn try_new(
        path: String,
        segment_duration: Duration,
        store: ObjectStoreRef,
        arrow_schema: SchemaRef,
        num_primary_keys: usize,
        storage_opts: StorageConfig,
        runtimes: StorageRuntimes,
    ) -> Result<Self> {
        let schema =
            StorageSchema::try_new(arrow_schema, num_primary_keys, storage_opts.update_mode)?;
        let manifest = Manifest::try_new(
            path.clone(),
            store.clone(),
            runtimes.manifest_compact_runtime.clone(),
            storage_opts.manifest,
        )
        .await?;
        let manifest = Arc::new(manifest);
        let write_props = Self::build_write_props(storage_opts.write, num_primary_keys);
        let format = storage_opts.format.clone().into();
        let sst_path_gen = Arc::new(SstPathGenerator::new(path.clone()));
        let parquet_reader = Arc::new(ParquetReader::new(
            store.clone(),
            schema.clone(),
            sst_path_gen.clone(),
        ));
        let compact_scheduler = CompactionScheduler::new(
            runtimes.sst_compact_runtime.clone(),
            manifest.clone(),
            store.clone(),
            schema.clone(),
            segment_duration,
            sst_path_gen.clone(),
            parquet_reader.clone(),
            storage_opts.scheduler,
            write_props.clone(),
            format,
        );

        let wal = if storage_opts.wal.enabled {
            Some(Wal::new(&path, &storage_opts.wal)?)
        } else {
            None
        };
        let flush_state = Arc::new(FlushState::new(
            schema.clone(),
            store.clone(),
            write_props.clone(),
            sst_path_gen.clone(),
            manifest.clone(),
            segment_duration,
            format,
            &storage_opts.buffer,
            wal,
        ));
        flush_state.recover_from_wal().await?;
        let (flush_tx, flush_rx) = mpsc::channel(1);
        if !flush_state.buffer_is_empty() {
            let _ = flush_tx.try_send(());
        }
        spawn_flush_loop(
            runtimes.sst_compact_runtime.clone(),
            &flush_state,
            flush_rx,
            storage_opts.buffer.flush_interval.0,
        );

        Ok(Self {
            path,
            schema,
            segment_duration,
            store,
            manifest,
            parquet_reader,
            runtimes,
            write_props,
            sst_path_gen,
            compact_scheduler,
            flush_state,
            flush_tx,
        })
    }

    fn build_write_props(write_options: WriteConfig, num_primary_key: usize) -> WriterProperties {
        let sorting_columns = write_options.enable_sorting_columns.then(|| {
            (0..num_primary_key)
                .map(|i| {
                    SortingColumn::new(i as i32, false /* desc */, true /* nulls_first */)
                })
                .collect::<Vec<_>>()
        });

        let mut builder = WriterProperties::builder()
            .set_max_row_group_size(write_options.max_row_group_size)
            .set_write_batch_size(write_options.write_bacth_size)
            .set_sorting_columns(sorting_columns)
            .set_dictionary_enabled(write_options.enable_dict)
            .set_bloom_filter_enabled(write_options.enable_bloom_filter)
            .set_encoding(write_options.encoding.into())
            .set_compression(write_options.compression.into());

        if write_options.column_options.is_none() {
            return builder.build();
        }

        for (col_name, col_opt) in write_options.column_options.unwrap() {
            let col_path = ColumnPath::new(vec![col_name.to_string()]);
            if let Some(enable_dict) = col_opt.enable_dict {
                builder = builder.set_column_dictionary_enabled(col_path.clone(), enable_dict);
            }
            if let Some(enable_bloom_filter) = col_opt.enable_bloom_filter {
                builder =
                    builder.set_column_bloom_filter_enabled(col_path.clone(), enable_bloom_filter);
            }
            if let Some(encoding) = col_opt.encoding {
                builder = builder.set_column_encoding(col_path.clone(), encoding.into());
            }
            if let Some(compression) = col_opt.compression {
                builder = builder.set_column_compression(col_path, compression.into());
            }
        }

        builder.build()
    }
}

#[async_trait]
impl ColumnarStorage for ObjectBasedStorage {
    fn schema(&self) -> &SchemaRef {
        &self.schema.arrow_schema
    }

    async fn write(&self, req: WriteRequest) -> Result<()> {
        let trigger = self.flush_state.write(req).await?;
        if trigger {
            // A full channel means a flush is already pending, so the newest
            // write is still covered by it.
            let _ = self.flush_tx.try_send(());
        }
        Ok(())
    }

    async fn scan(&self, mut req: ScanRequest) -> Result<SendableRecordBatchStream> {
        // Flush buffered writes so scans can see them.
        self.flush_state.flush().await?;
        let total_ssts = self.manifest.find_ssts(&req.range).await;
        if total_ssts.is_empty() {
            return Ok(Box::pin(EmptyRecordBatchStream::new(
                self.schema.arrow_schema.clone(),
            )));
        }

        let ssts_by_segment = total_ssts.into_iter().group_by(|file| {
            file.meta().time_range.start.0 / self.segment_duration.as_millis() as i64
        });

        let mut plan_for_all_segments = Vec::new();
        self.schema.fill_required_projections(&mut req.projections);
        for (_, ssts) in ssts_by_segment.sorted_by(|a, b| a.0.cmp(&b.0)) {
            let plan = self.parquet_reader.build_df_plan(
                ssts,
                req.projections.clone(),
                req.predicate.clone(),
                false, // keep_builtin
            )?;

            plan_for_all_segments.push(plan);
        }

        let ctx = SessionContext::default();
        if plan_for_all_segments.len() == 1 {
            let res = execute_stream(plan_for_all_segments.remove(0), ctx.task_ctx())
                .context("execute stream")?;
            return Ok(res);
        }

        let union_exec = Arc::new(UnionExec::new(plan_for_all_segments));
        let res = execute_stream(union_exec, ctx.task_ctx()).context("execute stream")?;
        return Ok(res);
    }

    async fn compact(&self, _req: CompactRequest) -> Result<()> {
        self.flush_state.flush().await?;
        self.compact_scheduler.trigger_compaction()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{col, lit};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use test_log::test;

    use super::*;
    use crate::{arrow_schema, record_batch, test_util::check_stream, types::Timestamp};

    fn build_runtimes() -> StorageRuntimes {
        let rt = Arc::new(Runtime::new().unwrap());
        StorageRuntimes::new(rt.clone(), rt)
    }

    #[test(test)]
    fn test_storage_write_and_scan() {
        let schema = arrow_schema!(("pk1", UInt8), ("pk2", UInt8), ("value", Int64));
        let root_dir = temp_dir::TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let runtimes = build_runtimes();
        runtimes.sst_compact_runtime.clone().block_on(async move {
            let storage = ObjectBasedStorage::try_new(
                root_dir.path().to_string_lossy().to_string(),
                Duration::from_hours(2),
                store,
                schema.clone(),
                2, // num_primary_keys
                StorageConfig::default(),
                runtimes,
            )
            .await
            .unwrap();

            let batch = record_batch!(
                ("pk1", UInt8, vec![11, 11, 9, 10, 5]),
                ("pk2", UInt8, vec![100, 100, 1, 2, 3]),
                ("value", Int64, vec![2, 7, 4, 6, 1])
            )
            .unwrap();
            storage
                .write(WriteRequest {
                    batch,
                    time_range: (1..10).into(),
                    enable_check: true,
                })
                .await
                .unwrap();

            let batch = record_batch!(
                ("pk1", UInt8, vec![11, 11, 9, 10]),
                ("pk2", UInt8, vec![100, 99, 1, 2]),
                ("value", Int64, vec![22, 77, 44, 66])
            )
            .unwrap();
            storage
                .write(WriteRequest {
                    batch,
                    time_range: (10..20).into(),
                    enable_check: true,
                })
                .await
                .unwrap();

            let result_stream = storage
                .scan(ScanRequest {
                    range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                    predicate: vec![],
                    projections: None,
                })
                .await
                .unwrap();
            let expected_batch = [
                record_batch!(
                    ("pk1", UInt8, vec![5, 9, 10, 11]),
                    ("pk2", UInt8, vec![3, 1, 2, 99]),
                    ("value", Int64, vec![1, 44, 66, 77])
                )
                .unwrap(),
                record_batch!(
                    ("pk1", UInt8, vec![11]),
                    ("pk2", UInt8, vec![100]),
                    ("value", Int64, vec![22])
                )
                .unwrap(),
            ];

            check_stream(result_stream, expected_batch).await;

            // test with predicate
            let expr = col("pk1").eq(lit(11_u8));
            let result_stream = storage
                .scan(ScanRequest {
                    range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                    predicate: vec![expr],
                    projections: None,
                })
                .await
                .unwrap();
            let expected_batch = [
                record_batch!(
                    ("pk1", UInt8, vec![11]),
                    ("pk2", UInt8, vec![99]),
                    ("value", Int64, vec![77])
                )
                .unwrap(),
                record_batch!(
                    ("pk1", UInt8, vec![11]),
                    ("pk2", UInt8, vec![100]),
                    ("value", Int64, vec![22])
                )
                .unwrap(),
            ];
            check_stream(result_stream, expected_batch).await;
        });
    }

    fn config_without_periodic_flush() -> StorageConfig {
        let mut config = StorageConfig::default();
        config.buffer.flush_interval = common::ReadableDuration::hours(1);
        config
    }

    /// Merge stream output may be split into arbitrary batches, so compare the
    /// concatenation of all rows.
    async fn collect_batch(mut stream: SendableRecordBatchStream) -> RecordBatch {
        let schema = stream.schema();
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        arrow::compute::concat_batches(&schema, &batches).unwrap()
    }

    #[test(test)]
    fn test_storage_flush_into_single_sst() {
        let schema = arrow_schema!(("pk", UInt8), ("value", Int64));
        let root_dir = temp_dir::TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let runtimes = build_runtimes();
        runtimes.sst_compact_runtime.clone().block_on(async move {
            let storage = ObjectBasedStorage::try_new(
                root_dir.path().to_string_lossy().to_string(),
                Duration::from_hours(2),
                store,
                schema.clone(),
                1,
                config_without_periodic_flush(),
                runtimes,
            )
            .await
            .unwrap();

            // Writes sharing a primary key are buffered, and the last one wins.
            for (i, value) in [10, 20, 30].into_iter().enumerate() {
                let batch = record_batch!(
                    ("pk", UInt8, vec![1, 2]),
                    ("value", Int64, vec![value, value + 1])
                )
                .unwrap();
                storage
                    .write(WriteRequest {
                        batch,
                        time_range: ((i as i64 * 10)..(i as i64 * 10 + 10)).into(),
                        enable_check: true,
                    })
                    .await
                    .unwrap();
            }
            assert!(storage.manifest.all_ssts().await.is_empty());

            let result_stream = storage
                .scan(ScanRequest {
                    range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                    predicate: vec![],
                    projections: None,
                })
                .await
                .unwrap();
            let actual = collect_batch(result_stream).await;
            let expected =
                record_batch!(("pk", UInt8, vec![1, 2]), ("value", Int64, vec![30, 31])).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(storage.manifest.all_ssts().await.len(), 1);
        });
    }

    /// End-to-end TSF path: flush writes a `.tsf` object under the day
    /// partition directory, the manifest records `format = Tsf`, and a scan
    /// reads the file back through `TsfExec` (ADR-0002 §7.2, §8.1).
    #[test(test)]
    fn test_storage_tsf_flush_and_scan() {
        use object_store::path::Path;
        use storage_kernel::tsf::partition::partition_label;

        use crate::sst::FileFormat;

        let schema = arrow_schema!(
            ("metric_id", UInt64),
            ("tsid", UInt64),
            ("timestamp", Int64),
            ("value", Float64)
        );
        let root_dir = temp_dir::TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let runtimes = build_runtimes();
        runtimes.sst_compact_runtime.clone().block_on(async move {
            let mut config = config_without_periodic_flush();
            config.format = crate::config::DataFileFormat::Tsf;
            let storage = ObjectBasedStorage::try_new(
                root_dir.path().to_string_lossy().to_string(),
                Duration::from_hours(2),
                store,
                schema.clone(),
                3, // metric_id, tsid, timestamp
                config,
                runtimes,
            )
            .await
            .unwrap();

            // 2023-11-14T22:13:20Z, comfortably inside one UTC day and one
            // 2-hour segment.
            let t0 = 1_700_000_000_000i64;
            let batch = record_batch!(
                ("metric_id", UInt64, vec![1, 1, 1]),
                ("tsid", UInt64, vec![10, 10, 11]),
                ("timestamp", Int64, vec![t0, t0 + 1, t0 + 2]),
                ("value", Float64, vec![1.5, 2.5, 3.5])
            )
            .unwrap();
            storage
                .write(WriteRequest {
                    batch,
                    time_range: (t0..t0 + 10).into(),
                    enable_check: true,
                })
                .await
                .unwrap();

            let result_stream = storage
                .scan(ScanRequest {
                    range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                    predicate: vec![],
                    projections: None,
                })
                .await
                .unwrap();
            let actual = collect_batch(result_stream).await;
            let expected = record_batch!(
                ("metric_id", UInt64, vec![1, 1, 1]),
                ("tsid", UInt64, vec![10, 10, 11]),
                ("timestamp", Int64, vec![t0, t0 + 1, t0 + 2]),
                ("value", Float64, vec![1.5, 2.5, 3.5])
            )
            .unwrap();
            assert_eq!(actual, expected);

            // The manifest must describe exactly one TSF file whose object
            // lives under the day partition directory.
            let ssts = storage.manifest.all_ssts().await;
            assert_eq!(ssts.len(), 1);
            let sst = &ssts[0];
            assert_eq!(sst.meta().format, FileFormat::Tsf);
            assert_eq!(sst.meta().num_rows, 3);
            assert!(sst.meta().min_sequence > 0);

            let path = storage.sst_path_gen.generate_for(sst.id(), sst.meta());
            assert!(
                path.ends_with(".tsf"),
                "TSF object path should end with .tsf, got {path}"
            );
            assert!(
                path.contains(&format!("/data/{}/", partition_label(t0))),
                "TSF object should live under the day partition, got {path}"
            );
            storage
                .store
                .get(&Path::from(path))
                .await
                .expect("TSF object must exist");
        });
    }

    /// Migration dual-read: an old Parquet SST and a new TSF SST in the same
    /// root must both be read and merged (ADR-0002 §7.2, §10).
    #[test(test)]
    fn test_storage_dual_read_parquet_and_tsf() {
        use crate::config::DataFileFormat;

        let schema = arrow_schema!(
            ("metric_id", UInt64),
            ("tsid", UInt64),
            ("timestamp", Int64),
            ("value", Float64)
        );
        let root_dir = temp_dir::TempDir::new().unwrap();
        let path = root_dir.path().to_string_lossy().to_string();
        let t0 = 1_700_000_000_000i64;

        // Phase 1: write a Parquet SST with the legacy (default) format.
        {
            let rt = Arc::new(Runtime::new().unwrap());
            let runtimes = StorageRuntimes::new(rt.clone(), rt.clone());
            let path = path.clone();
            let schema = schema.clone();
            rt.block_on(async move {
                let storage = ObjectBasedStorage::try_new(
                    path.clone(),
                    Duration::from_hours(2),
                    Arc::new(LocalFileSystem::new()),
                    schema.clone(),
                    3,
                    config_without_periodic_flush(),
                    runtimes,
                )
                .await
                .unwrap();
                let batch = record_batch!(
                    ("metric_id", UInt64, vec![1]),
                    ("tsid", UInt64, vec![10]),
                    ("timestamp", Int64, vec![t0]),
                    ("value", Float64, vec![1.5])
                )
                .unwrap();
                storage
                    .write(WriteRequest {
                        batch,
                        time_range: (t0..t0 + 1).into(),
                        enable_check: true,
                    })
                    .await
                    .unwrap();
                storage.flush_state.flush().await.unwrap();
            });
        }

        // Phase 2: reopen the same root with TSF enabled and write another row.
        // The scan must union both formats and merge on the primary keys.
        {
            let rt = Arc::new(Runtime::new().unwrap());
            let runtimes = StorageRuntimes::new(rt.clone(), rt.clone());
            rt.block_on(async move {
                let mut config = config_without_periodic_flush();
                config.format = DataFileFormat::Tsf;
                let storage = ObjectBasedStorage::try_new(
                    path,
                    Duration::from_hours(2),
                    Arc::new(LocalFileSystem::new()),
                    schema,
                    3,
                    config,
                    runtimes,
                )
                .await
                .unwrap();
                // Same (metric_id, tsid, timestamp) key with a newer value must
                // win the LWW merge across formats.
                let batch = record_batch!(
                    ("metric_id", UInt64, vec![1, 1]),
                    ("tsid", UInt64, vec![10, 11]),
                    ("timestamp", Int64, vec![t0, t0 + 1]),
                    ("value", Float64, vec![9.5, 2.5])
                )
                .unwrap();
                storage
                    .write(WriteRequest {
                        batch,
                        time_range: (t0..t0 + 2).into(),
                        enable_check: true,
                    })
                    .await
                    .unwrap();

                let formats: Vec<_> = {
                    storage.flush_state.flush().await.unwrap();
                    storage
                        .manifest
                        .all_ssts()
                        .await
                        .iter()
                        .map(|f| f.meta().format)
                        .collect()
                };
                assert!(formats.contains(&crate::sst::FileFormat::Parquet));
                assert!(formats.contains(&crate::sst::FileFormat::Tsf));

                let result_stream = storage
                    .scan(ScanRequest {
                        range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                        predicate: vec![],
                        projections: None,
                    })
                    .await
                    .unwrap();
                let actual = collect_batch(result_stream).await;
                let expected = record_batch!(
                    ("metric_id", UInt64, vec![1, 1]),
                    ("tsid", UInt64, vec![10, 11]),
                    ("timestamp", Int64, vec![t0, t0 + 1]),
                    ("value", Float64, vec![9.5, 2.5])
                )
                .unwrap();
                assert_eq!(actual, expected);
            });
        }
    }

    #[test(test)]
    fn test_storage_wal_recovery() {
        let schema = arrow_schema!(("pk", UInt8), ("value", Int64));
        let root_dir = temp_dir::TempDir::new().unwrap();
        let path = root_dir.path().to_string_lossy().to_string();
        let rt = Arc::new(Runtime::new().unwrap());
        let config = config_without_periodic_flush();

        // Write without flushing and drop the storage, as if the process
        // crashed before the buffer was persisted.
        {
            let config = config.clone();
            let path = path.clone();
            let schema = schema.clone();
            let runtimes = StorageRuntimes::new(rt.clone(), rt.clone());
            rt.block_on(async move {
                let storage = ObjectBasedStorage::try_new(
                    path,
                    Duration::from_hours(2),
                    Arc::new(LocalFileSystem::new()),
                    schema,
                    1,
                    config,
                    runtimes,
                )
                .await
                .unwrap();
                let batch =
                    record_batch!(("pk", UInt8, vec![1, 2]), ("value", Int64, vec![10, 20]))
                        .unwrap();
                storage
                    .write(WriteRequest {
                        batch,
                        time_range: (0..10).into(),
                        enable_check: true,
                    })
                    .await
                    .unwrap();
            });
        }

        // Reopening the storage replays the WAL, so the write survives.
        let runtimes = StorageRuntimes::new(rt.clone(), rt.clone());
        rt.block_on(async move {
            let storage = ObjectBasedStorage::try_new(
                path,
                Duration::from_hours(2),
                Arc::new(LocalFileSystem::new()),
                schema,
                1,
                config,
                runtimes,
            )
            .await
            .unwrap();
            let result_stream = storage
                .scan(ScanRequest {
                    range: TimeRange::new(Timestamp(0), Timestamp::MAX),
                    predicate: vec![],
                    projections: None,
                })
                .await
                .unwrap();
            let actual = collect_batch(result_stream).await;
            let expected =
                record_batch!(("pk", UInt8, vec![1, 2]), ("value", Int64, vec![10, 20])).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(storage.manifest.all_ssts().await.len(), 1);
        });
    }

    #[test]
    fn test_storage_sort_batch() {
        let schema = arrow_schema!(("a", UInt8), ("b", UInt8), ("c", UInt8), ("c", UInt8));
        let root_dir = temp_dir::TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new());
        let runtimes = build_runtimes();
        runtimes.sst_compact_runtime.clone().block_on(async move {
            let storage = ObjectBasedStorage::try_new(
                root_dir.path().to_string_lossy().to_string(),
                Duration::from_hours(2),
                store,
                schema.clone(),
                1,
                StorageConfig::default(),
                runtimes,
            )
            .await
            .unwrap();
            let batch = record_batch!(
                ("a", UInt8, vec![2, 1, 3, 4, 8, 6, 5, 7]),
                ("b", UInt8, vec![1, 3, 4, 8, 2, 6, 5, 7]),
                ("c", UInt8, vec![8, 6, 2, 4, 3, 1, 5, 7]),
                ("d", UInt8, vec![2, 7, 4, 6, 1, 3, 5, 8])
            )
            .unwrap();

            let mut sorted_batches = crate::buffer::sort_batch(&storage.schema, batch, false)
                .await
                .unwrap();
            let expected_bacth = record_batch!(
                ("a", UInt8, vec![1, 2, 3, 4, 5, 6, 7, 8]),
                ("b", UInt8, vec![3, 1, 4, 8, 5, 6, 7, 2]),
                ("c", UInt8, vec![6, 8, 2, 4, 5, 1, 7, 3]),
                ("d", UInt8, vec![7, 2, 4, 6, 5, 3, 8, 1])
            )
            .unwrap();
            let mut offset = 0;
            while let Some(sorted_batch) = sorted_batches.next().await {
                let sorted_batch = sorted_batch.unwrap();
                let length = sorted_batch.num_rows();
                let batch = expected_bacth.slice(offset, length);
                assert_eq!(sorted_batch, batch);
                offset += length;
            }
        });
    }
}
