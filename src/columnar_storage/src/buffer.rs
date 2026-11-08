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

//! Buffered write path and its flush pipeline.
//!
//! Writes are accumulated in memory and the WAL, then flushed into one sorted
//! SST file once a size threshold is reached, periodically, or before a scan.
//! Every row is assigned a monotonically increasing sequence, so the existing
//! merge logic still resolves duplicate primary keys last-write-wins.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use arrow::{
    array::{Array, RecordBatch},
    compute::concat_batches,
};
use datafusion::{
    common::DFSchema,
    execution::{context::ExecutionProps, SendableRecordBatchStream},
    logical_expr::ident,
    physical_expr::LexOrdering,
    physical_plan::{execute_stream, memory::MemoryExec, sorts::sort::SortExec},
    physical_planner::create_physical_sort_exprs,
    prelude::SessionContext,
};
use futures::StreamExt;
use object_store::path::Path;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::WriterProperties,
};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error};

use crate::{
    config::BufferConfig,
    manifest::{ManifestRef, ManifestUpdate},
    metrics::{FlushStages, StorageMetrics},
    sst::{FileFormat, FileId, FileMeta, SstFile, SstPathGenerator},
    storage::WriteRequest,
    types::{ObjectStoreRef, RuntimeRef, StorageSchema, TimeRange, SEQ_COLUMN_NAME},
    wal::{Wal, WalFrame},
    Result,
};

/// One write request waiting to be flushed.
pub(crate) struct BufferedFrame {
    /// Sequence of the first row in the batch.
    seq: u64,
    time_range: TimeRange,
    batch: RecordBatch,
}

impl BufferedFrame {
    fn memory_size(&self) -> usize {
        self.batch.get_array_memory_size()
    }
}

struct BufferState {
    frames: Vec<BufferedFrame>,
    /// WAL files covering the buffered frames, deleted after a flush succeeds.
    pending_wals: Vec<PathBuf>,
    rows: usize,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
}

impl BufferState {
    fn new(config: &BufferConfig) -> Self {
        Self {
            frames: Vec::new(),
            pending_wals: Vec::new(),
            rows: 0,
            bytes: 0,
            max_rows: config.max_rows,
            max_bytes: config.max_bytes.as_byte() as usize,
        }
    }

    /// Push a frame, returning true when the buffer should be flushed.
    fn push(&mut self, frame: BufferedFrame) -> bool {
        self.rows += frame.batch.num_rows();
        self.bytes += frame.memory_size();
        self.frames.push(frame);
        self.rows >= self.max_rows || self.bytes >= self.max_bytes
    }

    fn drain(&mut self) -> (Vec<BufferedFrame>, Vec<PathBuf>) {
        self.rows = 0;
        self.bytes = 0;
        (
            std::mem::take(&mut self.frames),
            std::mem::take(&mut self.pending_wals),
        )
    }

    /// Put frames back after a failed flush.
    fn reinsert(&mut self, frames: Vec<BufferedFrame>, wal_paths: Vec<PathBuf>) {
        self.rows += frames.iter().map(|f| f.batch.num_rows()).sum::<usize>();
        self.bytes += frames.iter().map(BufferedFrame::memory_size).sum::<usize>();
        let mut merged = frames;
        merged.append(&mut self.frames);
        self.frames = merged;
        self.pending_wals.extend(wal_paths);
    }

    fn add_pending_wals(&mut self, wal_paths: Vec<PathBuf>) {
        self.pending_wals.extend(wal_paths);
    }
}

/// Shared state of the buffered write path. Writes append to the WAL and the
/// buffer under `write_lock`, flushes rotate the WAL and drain the buffer under
/// the same lock, so a frame can never be lost between the two.
pub(crate) struct FlushState {
    schema: StorageSchema,
    store: ObjectStoreRef,
    write_props: WriterProperties,
    sst_path_gen: Arc<SstPathGenerator>,
    manifest: ManifestRef,
    segment_duration: Duration,
    format: FileFormat,
    buffer: StdMutex<BufferState>,
    wal: Option<Wal>,
    write_lock: Mutex<()>,
    flush_lock: Mutex<()>,
    metrics: Arc<StorageMetrics>,
}

impl FlushState {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: StorageSchema,
        store: ObjectStoreRef,
        write_props: WriterProperties,
        sst_path_gen: Arc<SstPathGenerator>,
        manifest: ManifestRef,
        segment_duration: Duration,
        format: FileFormat,
        buffer_config: &BufferConfig,
        wal: Option<Wal>,
        metrics: Arc<StorageMetrics>,
    ) -> Self {
        Self {
            schema,
            store,
            write_props,
            sst_path_gen,
            manifest,
            segment_duration,
            format,
            buffer: StdMutex::new(BufferState::new(buffer_config)),
            wal,
            write_lock: Mutex::new(()),
            flush_lock: Mutex::new(()),
            metrics,
        }
    }

    /// Buffer one write request. Returns true if a flush should be triggered.
    pub(crate) async fn write(&self, req: WriteRequest) -> Result<bool> {
        let num_rows = req.batch.num_rows();
        if num_rows == 0 {
            return Ok(false);
        }
        if req.enable_check {
            let segment_duration = self.segment_duration.as_millis() as i64;
            crate::ensure!(
                req.time_range.start.0 / segment_duration
                    == (req.time_range.end.0 - 1) / segment_duration,
                "time range can't cross segment, value:{:?}",
                &req.time_range
            );
        }

        let frame = BufferedFrame {
            seq: SstFile::allocate_ids(num_rows as u64),
            time_range: req.time_range,
            batch: req.batch,
        };

        // Appending to the WAL and pushing to the buffer must be atomic with
        // respect to `flush`, otherwise a flush could persist and delete a WAL
        // file that no longer has its frames in the buffer.
        let _guard = self.write_lock.lock().await;
        if let Some(wal) = &self.wal {
            wal.append(&WalFrame {
                seq: frame.seq,
                time_range: frame.time_range.clone(),
                batch: frame.batch.clone(),
            })
            .await?;
        }
        let trigger = self.buffer.lock().unwrap().push(frame);
        Ok(trigger)
    }

    /// Replay WAL files left by a previous run into the buffer, so they are
    /// persisted by the next flush.
    pub(crate) async fn recover_from_wal(&self) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        let (frames, wal_files) = wal.load().await?;
        if frames.is_empty() && wal_files.is_empty() {
            return Ok(());
        }
        debug!(
            frames = frames.len(),
            wal_files = wal_files.len(),
            "Recovered wal"
        );
        self.recover(frames, wal_files);
        Ok(())
    }

    /// Recover frames replayed from the WAL, so they are persisted by a later
    /// flush.
    fn recover(&self, frames: Vec<WalFrame>, wal_files: Vec<PathBuf>) {
        let mut buffer = self.buffer.lock().unwrap();
        for frame in frames {
            buffer.push(BufferedFrame {
                seq: frame.seq,
                time_range: frame.time_range,
                batch: frame.batch,
            });
        }
        buffer.add_pending_wals(wal_files);
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        let _flush_guard = self.flush_lock.lock().await;
        let (frames, wal_paths) = {
            let _write_guard = self.write_lock.lock().await;
            let rotated = match &self.wal {
                Some(wal) => wal.rotate().await?,
                None => None,
            };
            let mut buffer = self.buffer.lock().unwrap();
            let (frames, mut wal_paths) = buffer.drain();
            if let Some(path) = rotated {
                wal_paths.push(path);
            }
            if frames.is_empty() {
                // Keep the WAL files: they have no buffered frames to be
                // persisted yet.
                buffer.add_pending_wals(wal_paths);
                return Ok(());
            }
            (frames, wal_paths)
        };

        let rows = frames.iter().map(|f| f.batch.num_rows() as u64).sum();
        let started = Instant::now();
        match self.persist_frames(&frames).await {
            Ok(mut stages) => {
                let wal_started = Instant::now();
                if let Some(wal) = &self.wal {
                    for path in &wal_paths {
                        wal.remove(path).await;
                    }
                }
                stages.wal = wal_started.elapsed();
                self.metrics.record_flush(rows, started.elapsed(), stages);
                Ok(())
            }
            Err(e) => {
                // The frames are still in the WAL, retry them on the next
                // flush.
                self.buffer.lock().unwrap().reinsert(frames, wal_paths);
                Err(e)
            }
        }
    }

    /// Persist all frames, split into one sorted SST file per segment. Keeping
    /// segments apart is required by the file-granularity TTL, which can only
    /// evict a whole SST.
    async fn persist_frames(&self, frames: &[BufferedFrame]) -> Result<FlushStages> {
        let segment_duration = self.segment_duration.as_millis() as i64;
        let mut frames_by_segment: BTreeMap<i64, Vec<&BufferedFrame>> = BTreeMap::new();
        for frame in frames {
            let segment = frame.time_range.start.0 / segment_duration;
            frames_by_segment.entry(segment).or_default().push(frame);
        }
        let mut stages = FlushStages::default();
        for (segment, frames) in frames_by_segment {
            let timing = self.persist_segment(segment, &frames).await?;
            stages.sort += timing.sort;
            stages.write += timing.write;
        }
        Ok(stages)
    }

    /// Merge the frames of one segment into one sorted SST file.
    async fn persist_segment(
        &self,
        segment: i64,
        frames: &[&BufferedFrame],
    ) -> Result<FlushStages> {
        let mut time_range = frames[0].time_range.clone();
        let mut max_seq = frames[0].seq;
        let mut batches = Vec::with_capacity(frames.len());
        for frame in frames {
            time_range.merge(&frame.time_range);
            max_seq = max_seq.max(frame.seq + frame.batch.num_rows() as u64 - 1);
            batches.push(
                self.schema
                    .fill_builtin_columns_with_start_seq(frame.batch.clone(), frame.seq)?,
            );
        }
        let combined = concat_batches(&self.schema.arrow_schema, &batches)
            .context("concat buffered batches")?;
        let num_rows = combined.num_rows();
        let sort_started = Instant::now();
        let mut sorted = sort_batch(&self.schema, combined, true).await?;
        let sort = sort_started.elapsed();

        let write_started = Instant::now();
        match self.format {
            FileFormat::Parquet => {
                let file_id = SstFile::allocate_id();
                let (object_size, written_rows, min_seq) =
                    self.write_parquet(file_id, &mut sorted).await?;
                let file_meta = FileMeta {
                    max_sequence: max_seq,
                    min_sequence: min_seq,
                    num_rows: written_rows as u32,
                    size: object_size as u32,
                    time_range,
                    format: FileFormat::Parquet,
                };
                self.manifest.add_file(file_id, file_meta).await?;
            }
            FileFormat::Tsf => {
                // A segment can straddle UTC midnight, so this may yield more
                // than one file. Commit them in a single manifest update: a
                // half-applied flush would be retried from the WAL and could
                // duplicate rows.
                let files = crate::tsf::write::write_batches_by_day(
                    &self.store,
                    &self.sst_path_gen,
                    &self.schema,
                    &mut sorted,
                    &time_range,
                )
                .await?;
                let to_adds = files
                    .into_iter()
                    .inspect(|f| {
                        debug!(file_meta = ?f.meta(), segment, "Flushed TSF segment");
                    })
                    .collect::<Vec<_>>();
                self.manifest
                    .update(ManifestUpdate::new(to_adds, Vec::new()))
                    .await?;
            }
        }
        debug!(
            segment,
            frames = frames.len(),
            num_rows,
            "Flushed write buffer"
        );
        Ok(FlushStages {
            sort,
            write: write_started.elapsed(),
            wal: Duration::ZERO,
        })
    }

    /// Streams the sorted batches into a Parquet object. Returns
    /// `(size_bytes, rows, min_sequence)`.
    async fn write_parquet(
        &self,
        file_id: FileId,
        sorted: &mut SendableRecordBatchStream,
    ) -> Result<(u64, usize, u64)> {
        let file_path = Path::from(self.sst_path_gen.generate(file_id));
        let object_writer = ParquetObjectWriter::new(self.store.clone(), file_path.clone());
        let mut writer = AsyncArrowWriter::try_new(
            object_writer,
            self.schema.arrow_schema.clone(),
            Some(self.write_props.clone()),
        )
        .context("create arrow writer")?;
        let mut written_rows = 0;
        let mut min_seq = u64::MAX;
        while let Some(batch) = sorted.next().await {
            let batch = batch.context("get sorted batch")?;
            written_rows += batch.num_rows();
            min_seq = min_seq.min(min_seq_of_batch(&batch, self.schema.seq_idx)?);
            writer.write(&batch).await.context("write arrow batch")?;
        }
        writer.close().await.context("close arrow writer")?;
        let object_meta = self
            .store
            .head(&file_path)
            .await
            .context("get object meta")?;
        Ok((
            object_meta.size as u64,
            written_rows,
            if written_rows == 0 { 0 } else { min_seq },
        ))
    }

    pub(crate) fn buffer_is_empty(&self) -> bool {
        self.buffer.lock().unwrap().frames.is_empty()
    }
}

/// Spawn the background flush loop. It runs until the storage is dropped (the
/// trigger channel closes) or the state is gone.
pub(crate) fn spawn_flush_loop(
    runtime: RuntimeRef,
    state: &Arc<FlushState>,
    mut rx: mpsc::Receiver<()>,
    interval: Duration,
) {
    let interval = interval.max(Duration::from_millis(1));
    let weak = Arc::downgrade(state);
    runtime.spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                msg = rx.recv() => {
                    if msg.is_none() {
                        break;
                    }
                }
            }
            let Some(state) = weak.upgrade() else {
                break;
            };
            if let Err(e) = state.flush().await {
                error!(err = ?e, "Flush write buffer failed");
            }
        }
    });
}

/// Sort a batch by primary keys, and optionally by the sequence column, which
/// is required by the merge operator.
pub(crate) async fn sort_batch(
    schema: &StorageSchema,
    batch: RecordBatch,
    sort_seq: bool,
) -> Result<SendableRecordBatchStream> {
    let ctx = SessionContext::default();
    let batch_schema = batch.schema();
    let df_schema = DFSchema::try_from(schema.arrow_schema.clone()).context("build DFSchema")?;
    let sort_exprs = build_sort_exprs(schema, &df_schema, sort_seq)?;
    let batch_plan =
        MemoryExec::try_new(&[vec![batch]], batch_schema, None).context("build batch plan")?;
    let physical_plan = Arc::new(SortExec::new(sort_exprs, Arc::new(batch_plan)));

    Ok(execute_stream(physical_plan, ctx.task_ctx()).context("execute sort physical plan")?)
}

/// Minimum `__seq__` in a batch; assumes a non-null sequence column.
fn min_seq_of_batch(batch: &RecordBatch, seq_idx: usize) -> Result<u64> {
    use arrow::array::UInt64Array;
    let column = batch
        .column(seq_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("sequence column is not UInt64")?;
    let mut min = u64::MAX;
    for i in 0..column.len() {
        if !column.is_null(i) {
            min = min.min(column.value(i));
        }
    }
    Ok(min)
}

fn build_sort_exprs(
    schema: &StorageSchema,
    df_schema: &DFSchema,
    sort_seq: bool,
) -> Result<LexOrdering> {
    let mut sort_exprs = (0..schema.num_primary_keys)
        .map(|i| {
            ident(schema.arrow_schema.field(i).name())
                .sort(true /* asc */, true /* nulls_first */)
        })
        .collect::<Vec<_>>();
    if sort_seq {
        sort_exprs.push(ident(SEQ_COLUMN_NAME).sort(true, true));
    }
    let sort_exprs = create_physical_sort_exprs(&sort_exprs, df_schema, &ExecutionProps::default())
        .context("create physical sort exprs")?;

    Ok(sort_exprs)
}
