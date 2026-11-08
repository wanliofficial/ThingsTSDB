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

//! DataFusion execution plan for TSF v1 files.
//!
//! ADR-0002 §7.2 requires the query path to read both Parquet and TSF files
//! during migration. The Parquet side is a `ParquetExec`; this is its TSF
//! counterpart. It reads each file through the object store and emits batches
//! in the same Arrow schema the Parquet path produces, so the LWW merge plan
//! above it is format-agnostic.

use std::{any::Any, fmt, sync::Arc};

use arrow::{
    array::RecordBatch,
    compute::concat_batches,
    datatypes::{Schema, SchemaRef},
};
use datafusion::{
    common::internal_err,
    error::{DataFusionError, Result as DfResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, LexOrdering},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties,
    },
};
use futures::{stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use storage_kernel::tsf::{
    file::{TsfReader, FOOTER_SIZE},
    ColumnMask,
};

use crate::{
    metrics::StorageMetrics,
    sst::FileFormat,
    tsf::scan::{decoded_columns_to_batch, tsf_schema},
    types::{ObjectStoreRef, StorageSchema, TimeRange},
};

/// The part of a file that a scan actually touches.
fn file_scan_range(scan: &TimeRange, file: &TimeRange) -> storage_kernel::tsf::file::TimeRange {
    let start = scan.start.0.max(file.start.0);
    let end = scan.end.0.min(file.end.0);
    storage_kernel::tsf::file::TimeRange::new(start, end.max(start))
}

/// Every metric in `reader`, paired with the scan range to prune against.
fn metric_queries(
    reader: &TsfReader,
    range: storage_kernel::tsf::file::TimeRange,
) -> Vec<(u64, storage_kernel::tsf::file::TimeRange)> {
    reader
        .metric_ids()
        .into_iter()
        .map(|id| (id, range))
        .collect()
}

/// Largest gap a single request may read over to avoid a second request.
const COALESCE_GAP_BYTES: u64 = 8 * 1024;

/// Fixed allowance for coalescing waste, in addition to the fraction below.
const COALESCE_WASTE_FLOOR: u64 = 8 * 1024;

/// Maximum number of range requests in flight for one file.
const FETCH_CONCURRENCY: usize = 8;

/// Merges ranges into fetch groups.
///
/// A gap is only read over when it is small ([`COALESCE_GAP_BYTES`]) *and* the
/// total waste stays inside a budget proportional to the bytes actually wanted:
/// on a local disk over-reading is free-ish, on S3 it is not, and the count of
/// requests matters more than a few kilobytes either way.
fn coalesce_ranges(ranges: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let merged = merge_ranges(ranges);
    let planned: u64 = merged.iter().map(|(start, end)| end - start).sum();
    let budget = planned / 4 + COALESCE_WASTE_FLOOR;
    let mut wasted = 0;
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(merged.len());
    for (start, end) in merged {
        if let Some(last) = out.last_mut() {
            let gap = start - last.1;
            if gap <= COALESCE_GAP_BYTES && wasted + gap <= budget {
                wasted += gap;
                last.1 = end;
                continue;
            }
        }
        out.push((start, end));
    }
    out
}

/// Adds `data` at `offset` to `segments`, keeping the list sorted and
/// non-overlapping.
///
/// Fetched ranges can overlap what is already held: coalescing reads over gaps
/// and the caller retries a level when new metadata lands inside an existing
/// segment. The file is immutable, so overlapping bytes are identical and the
/// copy already held wins.
fn absorb_segment(segments: &mut Vec<(u64, Vec<u8>)>, offset: u64, data: Vec<u8>) {
    let end = offset + data.len() as u64;
    for (start, piece) in missing_ranges(segments, &[(offset, end)]) {
        let from = (start - offset) as usize;
        let to = (piece - offset) as usize;
        segments.push((start, data[from..to].to_vec()));
    }
    segments.sort_unstable_by_key(|(start, _)| *start);
    // Join neighbours so a contiguous run is one segment again.
    let mut merged: Vec<(u64, Vec<u8>)> = Vec::with_capacity(segments.len());
    for (start, data) in segments.drain(..) {
        match merged.last_mut() {
            Some((last_start, last_data)) if *last_start + last_data.len() as u64 == start => {
                last_data.extend_from_slice(&data);
            }
            _ => merged.push((start, data)),
        }
    }
    *segments = merged;
}

/// Subtracts the byte ranges already held in `segments` from `ranges`.
fn missing_ranges(segments: &[(u64, Vec<u8>)], ranges: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let held = merge_ranges(
        &segments
            .iter()
            .map(|(offset, data)| (*offset, *offset + data.len() as u64))
            .collect::<Vec<_>>(),
    );
    let mut out = Vec::new();
    for (start, end) in merge_ranges(ranges) {
        let mut cursor = start;
        for &(held_start, held_end) in &held {
            if held_end <= cursor {
                continue;
            }
            if held_start >= end {
                break;
            }
            if held_start > cursor {
                out.push((cursor, held_start));
            }
            cursor = cursor.max(held_end);
            if cursor >= end {
                break;
            }
        }
        if cursor < end {
            out.push((cursor, end));
        }
    }
    out
}

/// Sorts ranges and merges the ones that overlap or touch, so a byte is never
/// fetched twice.
fn merge_ranges(ranges: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut sorted = ranges
        .iter()
        .copied()
        .filter(|(start, end)| start < end)
        .collect::<Vec<_>>();
    sorted.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(sorted.len());
    for (start, end) in sorted {
        match merged.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// Files at or below this size are read whole: the four-plus ranged fetches a
/// planned read needs would cost more round trips than the bytes they save.
const WHOLE_FILE_THRESHOLD: u64 = 512 * 1024;

/// Hard cap on metadata planning rounds, so a malformed file cannot loop.
const MAX_PLAN_ROUNDS: usize = 8;

/// One TSF file to read: its object path and metadata time range.
#[derive(Debug, Clone)]
pub struct TsfFile {
    pub path: Path,
    pub time_range: TimeRange,
}

/// Reads a set of TSF files and projects them to the storage schema.
pub struct TsfExec {
    store: ObjectStoreRef,
    schema: StorageSchema,
    files: Vec<TsfFile>,
    range: TimeRange,
    projection: Option<Vec<usize>>,
    output_schema: SchemaRef,
    /// Files at or below this size are read whole; see
    /// [`WHOLE_FILE_THRESHOLD`]. Tests set it to zero to force ranged reads.
    min_ranged_bytes: u64,
    /// Per-format read counters (ADR-0002 §13), when the caller attached them.
    metrics: Option<Arc<StorageMetrics>>,
    cache: PlanProperties,
}

impl fmt::Debug for TsfExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TsfExec")
            .field("files", &self.files.len())
            .field("range", &self.range)
            .field("projection", &self.projection)
            .finish()
    }
}

impl TsfExec {
    /// Builds a TSF scan over `files`.
    ///
    /// The TSF schema is fixed to `(metric_id, tsid, timestamp, value, __seq__,
    /// __reserved__)`, so the storage schema must match it field-for-field for
    /// index-based projection to be valid. The flush path already rejects other
    /// schemas; this is the query-side check.
    pub fn try_new(
        store: ObjectStoreRef,
        schema: StorageSchema,
        files: Vec<TsfFile>,
        range: TimeRange,
        projection: Option<Vec<usize>>,
        ordering: LexOrdering,
    ) -> DfResult<Self> {
        ensure_schema_matches(schema.arrow_schema.clone())?;
        let output_schema = match &projection {
            Some(indices) => {
                let fields = indices
                    .iter()
                    .map(|i| schema.arrow_schema.field(*i).clone())
                    .collect::<Vec<_>>();
                Arc::new(Schema::new_with_metadata(
                    fields,
                    schema.arrow_schema.metadata().clone(),
                ))
            }
            None => schema.arrow_schema.clone(),
        };
        let mut eq_properties = EquivalenceProperties::new(output_schema.clone());
        eq_properties.add_new_orderings(vec![ordering]);
        let cache = PlanProperties::new(
            eq_properties,
            datafusion::physical_plan::Partitioning::UnknownPartitioning(files.len().max(1)),
            datafusion::physical_plan::ExecutionMode::Bounded,
        );
        Ok(Self {
            store,
            schema,
            files,
            range,
            projection,
            output_schema,
            min_ranged_bytes: WHOLE_FILE_THRESHOLD,
            metrics: None,
            cache,
        })
    }

    /// Attaches per-format read counters. Every byte fetched for a TSF file is
    /// added to the TSF read counter.
    pub fn with_metrics(mut self, metrics: Arc<StorageMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Forces ranged reads for files larger than `bytes`.
    pub fn with_min_ranged_bytes(mut self, bytes: u64) -> Self {
        self.min_ranged_bytes = bytes;
        self
    }

    /// Reads one file as a single sorted stream.
    ///
    /// Only the bytes the query needs are fetched (ADR-0002 §7.2): the footer,
    /// then the file index, then the metadata and pages whose time bounds
    /// overlap the scan range. A whole-file fetch is used for small files,
    /// where the extra round trips would cost more than the bytes they save.
    async fn read_file(&self, file: &TsfFile) -> DfResult<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        if !file.time_range.overlaps(&self.range) {
            return Ok(batches);
        }
        // Decode only the intersection of the scan range and this file's
        // range. File-level pruning against the scan range already happened in
        // `Manifest::find_ssts`, but a selected file almost always extends
        // past the scan window, and every page outside it would be decoded
        // for nothing.
        let kernel_range = file_scan_range(&self.range, &file.time_range);
        let reader = self.open_ranged(file).await?;
        let metrics = reader
            .metric_ids()
            .into_iter()
            .map(|id| (id, kernel_range))
            .collect::<Vec<_>>();
        let mut ctx = storage_kernel::tsf::file::ReadContext::new();
        for (metric_id, range) in metrics {
            let columns = reader
                .read_metric_columns(metric_id, range, ColumnMask::ALL, &mut ctx)
                .map_err(DataFusionError::Execution)?;
            for decoded in columns {
                let batch =
                    decoded_columns_to_batch(decoded).map_err(DataFusionError::Execution)?;
                batches.push(self.project(batch)?);
            }
        }
        Ok(batches)
    }

    /// Opens `file` with the minimum set of byte ranges needed for the scan.
    async fn open_ranged(&self, file: &TsfFile) -> DfResult<TsfReader> {
        let path = &file.path;
        let meta = self
            .store
            .head(path)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let file_len = meta.size as u64;
        if file_len <= self.min_ranged_bytes {
            let bytes = self
                .store
                .get(path)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .bytes()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            self.record_bytes_read(bytes.len() as u64);
            return TsfReader::from_bytes(bytes.to_vec())
                .map_err(|e| DataFusionError::Execution(format!("open TSF file {path}: {e}")));
        }

        // Round 1: the footer, which names the index locations.
        let footer_start = file_len - FOOTER_SIZE as u64;
        let mut segments = self.fetch(path, &[(footer_start, file_len)]).await?;
        let layout = TsfReader::footer_layout(&segments[0].1, file_len)
            .map_err(|e| DataFusionError::Execution(format!("read TSF footer {path}: {e}")))?;

        // Round 2: header + bloom + file index. Every later round asks the
        // kernel for the ranges it is still missing, so each byte is fetched
        // exactly once.
        let mut rounds = 0;
        let mut reader = self
            .extend_reader(path, &mut segments, &layout.index_ranges(file_len))
            .await?;
        let kernel_range = file_scan_range(&self.range, &file.time_range);
        loop {
            let metrics = metric_queries(&reader, kernel_range);
            let Some(more) = reader.next_scan_ranges(&metrics) else {
                break;
            };
            rounds += 1;
            if rounds > MAX_PLAN_ROUNDS {
                return Err(DataFusionError::Execution(format!(
                    "TSF read plan for {path} did not converge after {MAX_PLAN_ROUNDS} rounds"
                )));
            }
            reader = self.extend_reader(path, &mut segments, &more).await?;
        }

        // Final level: the pages themselves.
        let pages = reader.page_ranges_for_metrics(&metric_queries(&reader, kernel_range));
        if pages.is_empty() {
            return Ok(reader);
        }
        self.extend_reader(path, &mut segments, &pages).await
    }

    /// Fetches whatever part of `ranges` is still missing and rebuilds a reader
    /// over everything gathered so far.
    async fn extend_reader(
        &self,
        path: &Path,
        segments: &mut Vec<(u64, Vec<u8>)>,
        ranges: &[(u64, u64)],
    ) -> DfResult<TsfReader> {
        let missing = missing_ranges(segments, ranges);
        if !missing.is_empty() {
            for (offset, data) in self.fetch(path, &missing).await? {
                absorb_segment(segments, offset, data);
            }
        }
        TsfReader::from_segments(segments.clone())
            .map_err(|e| DataFusionError::Execution(format!("open TSF file {path}: {e}")))
    }

    /// Fetches byte ranges from one object.
    ///
    /// Ranges separated by less than [`COALESCE_GAP_BYTES`] are fetched
    /// together: one request that reads a small gap is cheaper than two
    /// requests. `ObjectStore::get_ranges` cannot be used here because its
    /// default coalescing window is 1 MiB, which would turn a planned read into
    /// a whole-file download.
    async fn fetch(&self, path: &Path, ranges: &[(u64, u64)]) -> DfResult<Vec<(u64, Vec<u8>)>> {
        let groups = coalesce_ranges(ranges);
        let fetched = futures::stream::iter(groups)
            .map(|(start, end)| async move {
                let bytes = self
                    .store
                    .get_range(path, start as usize..end as usize)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok::<_, DataFusionError>((start, bytes.to_vec()))
            })
            .buffered(FETCH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let bytes = fetched.iter().map(|(_, data)| data.len() as u64).sum();
        self.record_bytes_read(bytes);
        Ok(fetched)
    }

    /// Adds bytes actually fetched from the object store to the TSF read
    /// counter. Both the whole-object path and the ranged path call it; a
    /// `head` carries no bytes and is never counted.
    fn record_bytes_read(&self, bytes: u64) {
        if bytes > 0 {
            if let Some(metrics) = &self.metrics {
                metrics.for_format(FileFormat::Tsf).record_bytes_read(bytes);
            }
        }
    }

    fn project(&self, batch: RecordBatch) -> DfResult<RecordBatch> {
        match &self.projection {
            Some(indices) => batch
                .project(indices)
                .map_err(|e| DataFusionError::ArrowError(e, None)),
            None => Ok(batch),
        }
    }
}

impl DisplayAs for TsfExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TsfExec: [files: {}, range: {:?}, projection: {:?}]",
            self.files.len(),
            self.range,
            self.projection
        )
    }
}

impl ExecutionPlan for TsfExec {
    fn name(&self) -> &'static str {
        "TsfExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition >= self.files.len() {
            return internal_err!("TsfExec invalid partition {partition}");
        }
        let schema = self.output_schema.clone();
        let this = TsfExec {
            store: self.store.clone(),
            schema: self.schema.clone(),
            files: self.files.clone(),
            range: self.range.clone(),
            projection: self.projection.clone(),
            output_schema: self.output_schema.clone(),
            min_ranged_bytes: self.min_ranged_bytes,
            metrics: self.metrics.clone(),
            cache: self.cache.clone(),
        };
        // One partition per file keeps every emitted stream sorted, which is
        // what `SortPreservingMergeExec` above expects.
        let file = self.files[partition].clone();
        let future = async move {
            let batches = this.read_file(&file).await?;
            Ok::<_, DataFusionError>(stream::iter(
                batches
                    .into_iter()
                    .map(Ok::<_, DataFusionError>)
                    .collect::<Vec<_>>(),
            ))
        };
        let stream = futures::stream::once(future).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn ensure_schema_matches(arrow_schema: SchemaRef) -> DfResult<()> {
    let expected = tsf_schema();
    if arrow_schema.fields().len() != expected.fields().len()
        || arrow_schema
            .fields()
            .iter()
            .zip(expected.fields())
            .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
    {
        return Err(DataFusionError::Plan(format!(
            "TSF reader requires the sample schema {expected:?}, got {arrow_schema:?}"
        )));
    }
    Ok(())
}

/// Concatenates projected batches, used by tests and small reads.
#[allow(dead_code)]
pub fn concat_or_empty(schema: &SchemaRef, batches: &[RecordBatch]) -> DfResult<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    concat_batches(schema, batches).map_err(|e| DataFusionError::ArrowError(e, None))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use futures::StreamExt;
    use object_store::{
        local::LocalFileSystem, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };
    use storage_kernel::tsf::{
        file::{encode_file, WriterOptions},
        Row,
    };

    use super::*;
    use crate::{
        arrow_schema,
        config::UpdateMode,
        types::{StorageSchema, Timestamp},
    };

    /// Wraps a store and records how many bytes the plan actually fetched.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        bytes: AtomicU64,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let head = options.head;
            let result = self.inner.get_opts(location, options).await?;
            if !head {
                self.bytes
                    .fetch_add(result.range.len() as u64, Ordering::Relaxed);
            }
            Ok(result)
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn sample_schema() -> StorageSchema {
        StorageSchema::try_new(
            arrow_schema!(
                ("metric_id", UInt64),
                ("tsid", UInt64),
                ("timestamp", Int64),
                ("value", Float64)
            ),
            3,
            UpdateMode::Overwrite,
        )
        .unwrap()
    }

    /// 300 series x 512 samples: enough pages that pruning a one-page window
    /// is measurable against the file size.
    fn sample_rows() -> (Vec<Row>, i64) {
        let base = 1_700_000_000_000i64;
        let mut rows = Vec::new();
        for series in 0..300u64 {
            for i in 0..512i64 {
                rows.push(Row {
                    metric_id: 1,
                    series_id: series,
                    timestamp: base + i * 1_000,
                    value: series as f64 + i as f64,
                    sequence: 0,
                    reserved: 0,
                });
            }
        }
        (rows, base)
    }

    /// A narrow time window must read only the pages that overlap it, and must
    /// still return exactly the rows a whole-file read returns.
    #[test]
    fn test_ranged_read_fetches_only_touched_pages() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let root_dir = temp_dir::TempDir::new().unwrap();
            let path = format!("{}/data/20231114/1.tsf", root_dir.path().to_string_lossy());
            let (rows, base) = sample_rows();
            let expected_rows = rows.len();
            let (bytes, _) = encode_file(
                rows,
                (base, base + 512_000),
                &WriterOptions { page_rows: 64 },
            )
            .unwrap();
            let file_len = bytes.len() as u64;

            let inner: ObjectStoreRef = Arc::new(LocalFileSystem::new());
            inner
                .put(&Path::from(path.clone()), bytes.into())
                .await
                .unwrap();
            let counting = Arc::new(CountingStore {
                inner,
                bytes: AtomicU64::new(0),
            });
            let store: ObjectStoreRef = counting.clone();

            let file = TsfFile {
                path: Path::from(path.clone()),
                time_range: TimeRange::new(Timestamp(base), Timestamp(base + 512_000)),
            };
            let make_exec = |range: TimeRange| {
                TsfExec::try_new(
                    store.clone(),
                    sample_schema(),
                    vec![file.clone()],
                    range,
                    None,
                    LexOrdering::default(),
                )
                .unwrap()
                // Force the ranged path even though the file is small.
                .with_min_ranged_bytes(0)
            };

            // One sample wide.
            let narrow = TimeRange::new(
                Timestamp(base + 100 * 1_000),
                Timestamp(base + 100 * 1_000 + 1),
            );
            let plan = Arc::new(make_exec(narrow)) as Arc<dyn ExecutionPlan>;
            let stream = plan
                .execute(0, datafusion::execution::TaskContext::default().into())
                .unwrap();
            let batches = collect_stream(stream).await;
            assert_eq!(count_rows(&batches), 300, "one sample per series");
            let narrow_bytes = counting.bytes.load(Ordering::Relaxed);
            assert!(
                narrow_bytes * 2 < file_len,
                "narrow read fetched {narrow_bytes} of {file_len} bytes"
            );

            // Whole range: every row comes back.
            let all = TimeRange::new(Timestamp(base), Timestamp(base + 512_000));
            let plan = Arc::new(make_exec(all)) as Arc<dyn ExecutionPlan>;
            let stream = plan
                .execute(0, datafusion::execution::TaskContext::default().into())
                .unwrap();
            let batches = collect_stream(stream).await;
            assert_eq!(count_rows(&batches), expected_rows);
        });
    }

    async fn collect_stream(mut stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            out.push(batch.unwrap());
        }
        out
    }

    fn count_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }
}
