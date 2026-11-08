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

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use anyhow::Context;
use async_scoped::TokioScope;
use datafusion::{
    execution::TaskContext,
    physical_plan::{execute_stream, SendableRecordBatchStream},
};
use futures::StreamExt;
use object_store::path::Path;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::WriterProperties,
};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, trace};

use crate::{
    compaction::Task,
    ensure,
    manifest::{ManifestRef, ManifestUpdate},
    metrics::{CompactionVolume, StorageMetrics},
    read::ParquetReader,
    sst::{FileFormat, FileId, FileMeta, SstFile, SstPathGenerator},
    types::{ObjectStoreRef, RuntimeRef, StorageSchema},
    Result,
};

#[derive(Clone)]
pub struct Executor {
    inner: Arc<Inner>,
}

/// Minimum `__seq__` across the inputs of a compaction task. The output of a
/// compaction must not narrow the sequence span of its inputs (ADR-0002 §6.2).
fn min_sequence_of(task: &Task) -> u64 {
    task.inputs
        .iter()
        .map(|f| f.meta().min_sequence)
        .min()
        .unwrap_or(0)
}

struct Inner {
    runtime: RuntimeRef,
    store: ObjectStoreRef,
    schema: StorageSchema,
    manifest: ManifestRef,
    sst_path_gen: Arc<SstPathGenerator>,
    parquet_reader: Arc<ParquetReader>,
    write_props: WriterProperties,
    inused_memory: AtomicU64,
    mem_limit: u64,
    trigger_tx: Sender<()>,
    format: FileFormat,
    metrics: Arc<StorageMetrics>,
}

impl Executor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: RuntimeRef,
        store: ObjectStoreRef,
        schema: StorageSchema,
        manifest: ManifestRef,
        sst_path_gen: Arc<SstPathGenerator>,
        parquet_reader: Arc<ParquetReader>,
        write_props: WriterProperties,
        mem_limit: u64,
        trigger_tx: Sender<()>,
        format: FileFormat,
        metrics: Arc<StorageMetrics>,
    ) -> Self {
        let inner = Inner {
            runtime,
            store,
            schema,
            manifest,
            sst_path_gen,
            parquet_reader,
            write_props,
            mem_limit,
            inused_memory: AtomicU64::new(0),
            trigger_tx,
            format,
            metrics,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    fn pre_check(&self, task: &Task) -> Result<()> {
        ensure!(
            !task.inputs.is_empty() || !task.expireds.is_empty(),
            "compaction task has no input and no expired file"
        );
        for f in &task.inputs {
            assert!(f.is_compaction());
        }
        for f in &task.expireds {
            assert!(f.is_compaction());
        }

        let task_size = task.input_size();
        let inused = self.inner.inused_memory.load(Ordering::Relaxed);
        let mem_limit = self.inner.mem_limit;
        ensure!(
            inused + task_size <= mem_limit,
            "Compaction memory usage too high, inused:{inused}, task_size:{task_size}, limit:{mem_limit}"
        );

        self.inner
            .inused_memory
            .fetch_add(task.input_size(), Ordering::Relaxed);
        Ok(())
    }

    pub fn on_success(&self, task: &Task) {
        let task_size = task.input_size();
        self.inner
            .inused_memory
            .fetch_sub(task_size, Ordering::Relaxed);
    }

    pub fn on_failure(&self, task: &Task) {
        let task_size = task.input_size();
        self.inner
            .inused_memory
            .fetch_sub(task_size, Ordering::Relaxed);

        // When task execution fails, unmark sst so they can be
        // reschduled.
        for sst in &task.inputs {
            sst.unmark_compaction();
        }
        for sst in &task.expireds {
            sst.unmark_compaction();
        }
    }

    pub fn submit(&self, task: Task) {
        let runnable = Runnable {
            executor: self.clone(),
            task,
        };
        runnable.spawn()
    }

    fn trigger_more_task(&self) {
        if let Err(e) = self.inner.trigger_tx.try_send(()) {
            debug!("Send pick task trigger signal failed, err{e:?}");
        }
    }

    // Merge input sst files into one new sst file and delete the expired sst
    // files. Tasks with no input files are pure expiration tasks.
    pub async fn do_compaction(&self, task: &Task) -> Result<()> {
        self.pre_check(task)?;
        self.trigger_more_task();

        debug!(
            input_len = task.inputs.len(),
            expired_len = task.expireds.len(),
            "Start do compaction"
        );

        let to_adds = if task.inputs.is_empty() {
            Vec::new()
        } else {
            self.merge_inputs(task).await?
        };
        let removed_files = task
            .expireds
            .iter()
            .chain(task.inputs.iter())
            .cloned()
            .collect::<Vec<_>>();
        let to_deletes = removed_files.iter().map(|f| f.id()).collect::<Vec<_>>();
        // First add new sst to manifest, then delete expired/old sst
        let output_bytes = to_adds.iter().map(|f| f.size() as u64).sum();
        let output_files = to_adds.len() as u64;
        let output_rows = to_adds.iter().map(|f| f.meta().num_rows as u64).sum();
        self.inner
            .manifest
            .update(ManifestUpdate::new(to_adds, to_deletes.clone()))
            .await?;

        // The commit is the point of no return: count the task once it is
        // durable, so a retry after a failure is not double counted. A pure
        // expiration task merges nothing and is reported through the expired
        // counters only.
        if !task.inputs.is_empty() {
            self.inner.metrics.record_compaction(CompactionVolume {
                input_files: task.inputs.len() as u64,
                input_bytes: task.input_size(),
                input_rows: task.inputs.iter().map(|f| f.meta().num_rows as u64).sum(),
                output_files,
                output_bytes,
                output_rows,
            });
        }
        if !task.expireds.is_empty() {
            let expired_bytes = task.expireds.iter().map(|f| f.size() as u64).sum();
            self.inner
                .metrics
                .record_expired(task.expireds.len() as u64, expired_bytes);
        }

        // From now on, no error should be returned!
        // Because we have already updated manifest.
        let paths = removed_files
            .iter()
            .map(|f| Path::from(self.inner.sst_path_gen.generate_for(f.id(), f.meta())))
            .collect::<Vec<_>>();
        self.delete_paths(paths);
        Ok(())
    }

    /// Merge all input sst files into one new sst file, return it as the only
    /// file to add.
    ///
    /// TSF output may need several files: a compaction task groups files by
    /// time segment, and a segment can straddle a UTC day boundary while a TSF
    /// file must belong to exactly one day partition (ADR-0002 §8.1).
    async fn merge_inputs(&self, task: &Task) -> Result<Vec<SstFile>> {
        let mut time_range = task.inputs[0].meta().time_range.clone();
        for f in &task.inputs[1..] {
            time_range.merge(&f.meta().time_range);
        }
        let plan = self.inner.parquet_reader.build_df_plan(
            task.inputs.clone(),
            None,       // projection
            Vec::new(), // predicate
            true,       // keep_builtin
        )?;
        let mut stream = execute_stream(plan, Arc::new(TaskContext::default()))
            .context("execute datafusion plan")?;

        match self.inner.format {
            FileFormat::Parquet => {
                let file_id = SstFile::allocate_id();
                let (object_size, num_rows, min_sequence) = self
                    .write_parquet_output(file_id, &mut stream, task)
                    .await?;
                let file_meta = FileMeta {
                    max_sequence: file_id,
                    min_sequence,
                    num_rows: num_rows as u32,
                    size: object_size as u32,
                    time_range,
                    format: FileFormat::Parquet,
                };
                debug!(file_meta = ?file_meta, "Compact output new sst");
                Ok(vec![SstFile::new(file_id, file_meta)])
            }
            FileFormat::Tsf => {
                let files = crate::tsf::write::write_batches_by_day(
                    &self.inner.store,
                    &self.inner.sst_path_gen,
                    &self.inner.schema,
                    &mut stream,
                    &time_range,
                )
                .await?;
                // ADR-0002 §6.2: compaction must not narrow the sequence span
                // of its inputs, and LWW dedup at encode time can raise the
                // written minimum.
                let min_sequence = min_sequence_of(task);
                let max_sequence = task
                    .inputs
                    .iter()
                    .map(|f| f.meta().max_sequence)
                    .max()
                    .unwrap_or(0);
                let files = files
                    .into_iter()
                    .map(|f| {
                        let meta = FileMeta {
                            min_sequence: f.meta().min_sequence.min(min_sequence),
                            max_sequence: f.meta().max_sequence.max(max_sequence),
                            ..f.meta().clone()
                        };
                        debug!(file_meta = ?meta, "Compact output new sst");
                        SstFile::new(f.id(), meta)
                    })
                    .collect::<Vec<_>>();
                Ok(files)
            }
        }
    }

    /// Streams the merged rows into a single Parquet object.
    async fn write_parquet_output(
        &self,
        file_id: FileId,
        stream: &mut SendableRecordBatchStream,
        task: &Task,
    ) -> Result<(u64, usize, u64)> {
        let file_path = Path::from(self.inner.sst_path_gen.generate(file_id));
        let object_store_writer =
            ParquetObjectWriter::new(self.inner.store.clone(), file_path.clone());
        let mut writer = AsyncArrowWriter::try_new(
            object_store_writer,
            self.inner.schema.arrow_schema.clone(),
            Some(self.inner.write_props.clone()),
        )
        .context("create arrow writer")?;
        let mut num_rows = 0;
        // TODO: support multi-part write
        while let Some(batch) = stream.next().await {
            let batch = batch.context("execute plan")?;
            num_rows += batch.num_rows();
            writer.write(&batch).await.context("write batch")?;
        }
        writer.close().await.context("close writer")?;
        let object_meta = self
            .inner
            .store
            .head(&file_path)
            .await
            .context("get object meta")?;
        Ok((object_meta.size as u64, num_rows, min_sequence_of(task)))
    }

    fn delete_paths(&self, paths: Vec<Path>) {
        let (_, results) = TokioScope::scope_and_block(|scope| {
            for path in paths {
                trace!(%path, "Delete sst file");
                let path = path.clone();
                scope.spawn(async move {
                    self.inner
                        .store
                        .delete(&path)
                        .await
                        .with_context(|| format!("failed to delete file, path:{path}"))
                });
            }
        });
        for res in results {
            match res {
                Err(e) => {
                    error!("Failed to join delete task, err:{e}")
                }
                Ok(v) => {
                    if let Err(e) = v {
                        error!("Failed to delete sst, err:{e}")
                    }
                }
            }
        }
    }
}

pub struct Runnable {
    executor: Executor,
    task: Task,
}

impl Runnable {
    fn spawn(self) {
        let rt = self.executor.inner.runtime.clone();
        rt.spawn(async move {
            if let Err(e) = self.executor.do_compaction(&self.task).await {
                error!("Do compaction failed, err:{e:?}");
                self.executor.on_failure(&self.task);
            } else {
                self.executor.on_success(&self.task);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Float64Array, Int64Array, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use object_store::local::LocalFileSystem;
    use parquet::file::properties::WriterProperties;
    use storage_kernel::tsf::partition::{partition_label, DAY_MS};

    use super::*;
    use crate::{
        config::{ManifestConfig, UpdateMode},
        manifest::{Manifest, ManifestRef},
        read::ParquetReader,
        types::StorageSchema,
    };

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("metric_id", DataType::UInt64, false),
            Field::new("tsid", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    fn batch(rows: &[(u64, u64, i64, f64)]) -> RecordBatch {
        RecordBatch::try_new(
            sample_schema(),
            vec![
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.3))),
            ],
        )
        .unwrap()
    }

    /// A compaction task groups files by time segment, and a segment can
    /// straddle UTC midnight. The TSF output must still be split per day, and
    /// the sequence span must not narrow (ADR-0002 §6.2, §8.1).
    #[test_log::test(test)]
    fn test_merge_inputs_splits_tsf_output_by_day() {
        let root = temp_dir::TempDir::new().unwrap();
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let runtime: RuntimeRef = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let schema = StorageSchema::try_new(sample_schema(), 3, UpdateMode::Overwrite).unwrap();
        let path_gen = Arc::new(SstPathGenerator::new(
            root.path().to_string_lossy().to_string(),
        ));
        let reader = Arc::new(ParquetReader::new(
            store.clone(),
            schema.clone(),
            path_gen.clone(),
        ));
        let manifest: ManifestRef = Arc::new(
            runtime
                .block_on(async {
                    Manifest::try_new(
                        root.path().to_string_lossy().to_string(),
                        store.clone(),
                        runtime.clone(),
                        ManifestConfig::default(),
                    )
                    .await
                })
                .unwrap(),
        );

        // 20:00 and 00:30 UTC of the next day: one 5-hour segment, two days.
        let day = 20_000 * DAY_MS;
        let t0 = day + 20 * 3_600_000;
        let t1 = day + DAY_MS + 30 * 60_000;

        runtime.block_on(async {
            let mut inputs = Vec::new();
            for (id_seq, ts, tsid) in [(100u64, t0, 10u64), (200, t1, 11)] {
                let id = SstFile::allocate_id();
                let path = object_store::path::Path::from(path_gen.generate(id));
                let batch = schema
                    .fill_builtin_columns_with_start_seq(batch(&[(1, tsid, ts, 1.5)]), id_seq)
                    .unwrap();
                let mut writer = AsyncArrowWriter::try_new(
                    ParquetObjectWriter::new(store.clone(), path.clone()),
                    schema.arrow_schema.clone(),
                    Some(WriterProperties::builder().build()),
                )
                .unwrap();
                writer.write(&batch).await.unwrap();
                writer.close().await.unwrap();
                let size = store.head(&path).await.unwrap().size;

                let file = SstFile::new(
                    id,
                    FileMeta {
                        max_sequence: id_seq + 10,
                        min_sequence: id_seq,
                        num_rows: 1,
                        size: size as u32,
                        time_range: (ts..ts + 1).into(),
                        format: FileFormat::Parquet,
                    },
                );
                manifest.add_file(id, file.meta().clone()).await.unwrap();
                file.mark_compaction();
                inputs.push(file);
            }

            let (trigger_tx, _trigger_rx) = tokio::sync::mpsc::channel(1);
            let executor = Executor::new(
                runtime.clone(),
                store.clone(),
                schema.clone(),
                manifest.clone(),
                path_gen.clone(),
                reader,
                WriterProperties::builder().build(),
                1 << 30,
                trigger_tx,
                FileFormat::Tsf,
                Arc::new(StorageMetrics::default()),
            );

            let task = Task {
                inputs,
                expireds: Vec::new(),
            };
            let outputs = executor.merge_inputs(&task).await.unwrap();

            assert_eq!(outputs.len(), 2, "one TSF file per UTC day");
            let mut total_rows = 0;
            for file in &outputs {
                assert_eq!(file.meta().format, FileFormat::Tsf);
                // Compaction must not narrow the input sequence span.
                assert_eq!(file.meta().min_sequence, 100);
                assert_eq!(file.meta().max_sequence, 210);
                total_rows += file.meta().num_rows;

                let path = path_gen.generate_for(file.id(), file.meta());
                assert!(path.ends_with(".tsf"), "unexpected path {path}");
                assert!(
                    path.contains(&format!(
                        "/data/{}/",
                        partition_label(*file.meta().time_range.start)
                    )),
                    "TSF output not in its day partition: {path}"
                );
                store
                    .get(&object_store::path::Path::from(path))
                    .await
                    .expect("TSF object must exist");
            }
            assert_eq!(total_rows, 2);

            let mut starts = outputs
                .iter()
                .map(|f| *f.meta().time_range.start)
                .collect::<Vec<_>>();
            starts.sort_unstable();
            assert_eq!(starts, vec![t0, t1]);
        });
    }
}
