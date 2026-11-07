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
use bytes::Bytes;
use datafusion::{execution::TaskContext, physical_plan::execute_stream};
use futures::StreamExt;
use object_store::{path::Path, PutPayload};
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
    read::ParquetReader,
    sst::{FileFormat, FileMeta, SstFile, SstPathGenerator},
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
        self.inner
            .manifest
            .update(ManifestUpdate::new(to_adds, to_deletes.clone()))
            .await?;

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

        let file_id = SstFile::allocate_id();
        let (object_size, num_rows, min_sequence) = match self.inner.format {
            FileFormat::Parquet => {
                let file_path = self.inner.sst_path_gen.generate(file_id);
                let file_path = Path::from(file_path);
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
                (object_meta.size as u64, num_rows, min_sequence_of(task))
            }
            FileFormat::Tsf => {
                use storage_kernel::tsf::{
                    file::{encode_file, WriterOptions},
                    partition::{partition_end, partition_start},
                };
                let start = partition_start(*time_range.start);
                let end = partition_end(*time_range.start);
                crate::ensure!(
                    *time_range.end <= end,
                    "TSF compaction output spans more than one day: {:?}",
                    time_range
                );
                let mut rows = Vec::new();
                let mut num_rows = 0;
                while let Some(batch) = stream.next().await {
                    let batch = batch.context("execute plan")?;
                    num_rows += batch.num_rows();
                    rows.extend(
                        crate::tsf::write::batch_to_rows(&batch, &self.inner.schema)
                            .map_err(|e| anyhow::anyhow!("convert batch to TSF rows: {e}"))?,
                    );
                }
                let (bytes, _stats) = encode_file(rows, (start, end), &WriterOptions::default())
                    .map_err(|e| anyhow::anyhow!("encode TSF file: {e}"))?;
                let size = bytes.len() as u64;
                let path = Path::from(self.inner.sst_path_gen.generate_tsf(file_id, start));
                self.inner
                    .store
                    .put(&path, PutPayload::from_bytes(Bytes::from(bytes)))
                    .await
                    .context("write TSF object")?;
                (size, num_rows, min_sequence_of(task))
            }
        };
        let file_meta = FileMeta {
            max_sequence: file_id,
            min_sequence,
            num_rows: num_rows as u32,
            size: object_size as u32,
            time_range: time_range.clone(),
            format: self.inner.format,
        };
        debug!(file_meta = ?file_meta, "Compact output new sst");
        Ok(vec![SstFile::new(file_id, file_meta)])
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
