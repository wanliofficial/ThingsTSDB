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
    common::{internal_err, ScalarValue},
    error::{DataFusionError, Result as DfResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, Operator},
    physical_expr::{EquivalenceProperties, LexOrdering},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties,
    },
};
use futures::{stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use storage_kernel::tsf::{
    file::{SeriesFilter, TsfReader, FOOTER_SIZE},
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

/// ID constraints a scan can push into the file, extracted from its
/// predicates.
///
/// The TSF file groups rows by metric and, inside a group, by series. The query
/// above the scan still applies the full predicate; this only narrows what is
/// fetched and decoded. Every set is therefore allowed to be a *superset* of
/// the rows the predicate accepts: a constraint the extractor does not
/// understand is dropped, never guessed at.
#[derive(Debug, Clone, Default)]
pub struct TsfPushdown {
    /// `metric_id = lit` / `metric_id IN (...)`, sorted and deduplicated.
    /// `None` means every metric; `Some(empty)` means no metric.
    metric_ids: Option<Vec<u64>>,
    /// The same for `tsid`. `None` means every series of the metric.
    series_ids: Option<SeriesFilter>,
}

impl TsfPushdown {
    /// Extracts the ID constraints from a conjunctive predicate list.
    ///
    /// Only equality and non-negated `IN` lists are understood, combined with
    /// `AND`. Everything else — ranges, `OR`, `NOT IN`, a cast on the column —
    /// is left to the filter above the scan.
    pub fn from_predicates(predicates: &[Expr]) -> Self {
        Self {
            metric_ids: id_constraints(predicates, METRIC_COLUMN),
            series_ids: series_constraints(predicates, TSID_COLUMN),
        }
    }

    pub fn metric_ids(&self) -> Option<&[u64]> {
        self.metric_ids.as_deref()
    }

    pub fn series_ids(&self) -> Option<&SeriesFilter> {
        self.series_ids.as_ref()
    }

    /// True when no predicate narrowed the scan.
    pub fn is_empty(&self) -> bool {
        self.metric_ids.is_none() && self.series_ids.is_none()
    }
}

/// The TSF primary-key column names, fixed by [`tsf_schema`].
const METRIC_COLUMN: &str = "metric_id";
const TSID_COLUMN: &str = "tsid";

/// Intersects the ID constraints on `column` across all predicates.
fn id_constraints(predicates: &[Expr], column: &str) -> Option<Vec<u64>> {
    let mut acc: Option<Vec<u64>> = None;
    for predicate in predicates {
        let Some(mut values) = id_values(predicate, column) else {
            continue;
        };
        values.sort_unstable();
        values.dedup();
        acc = Some(match acc {
            Some(previous) => previous
                .into_iter()
                .filter(|id| values.binary_search(id).is_ok())
                .collect(),
            None => values,
        });
    }
    acc
}

fn series_constraints(predicates: &[Expr], column: &str) -> Option<SeriesFilter> {
    id_constraints(predicates, column).map(SeriesFilter::new)
}

/// IDs an expression restricts `column` to, or `None` when it does not
/// restrict it in a way this extractor understands.
fn id_values(expr: &Expr, column: &str) -> Option<Vec<u64>> {
    match expr {
        Expr::Alias(alias) => id_values(&alias.expr, column),
        Expr::Cast(cast) => id_values(&cast.expr, column),
        // `a AND b` is two constraints on the same scan, so it is an
        // intersection. A side that restricts nothing simply drops out.
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left = id_values(&binary.left, column);
            let right = id_values(&binary.right, column);
            match (left, right) {
                (Some(left), Some(right)) => Some(
                    left.into_iter()
                        .filter(|id| right.contains(id))
                        .collect::<Vec<_>>(),
                ),
                (Some(left), None) => Some(left),
                (None, Some(right)) => Some(right),
                (None, None) => None,
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            if is_column(&binary.left, column) {
                id_literal(&binary.right).map(|id| vec![id])
            } else if is_column(&binary.right, column) {
                id_literal(&binary.left).map(|id| vec![id])
            } else {
                None
            }
        }
        Expr::InList(list) if !list.negated => {
            if !is_column(&list.expr, column) {
                return None;
            }
            list.list.iter().map(id_literal).collect()
        }
        _ => None,
    }
}

fn is_column(expr: &Expr, name: &str) -> bool {
    matches!(expr, Expr::Column(column) if column.name == name)
}

fn id_literal(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Literal(scalar) => match scalar {
            ScalarValue::UInt64(Some(v)) => Some(*v),
            ScalarValue::UInt32(Some(v)) => Some(*v as u64),
            ScalarValue::UInt16(Some(v)) => Some(*v as u64),
            ScalarValue::UInt8(Some(v)) => Some(*v as u64),
            ScalarValue::Int64(Some(v)) => u64::try_from(*v).ok(),
            ScalarValue::Int32(Some(v)) => u64::try_from(*v).ok(),
            _ => None,
        },
        Expr::Cast(cast) => id_literal(&cast.expr),
        _ => None,
    }
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
    /// ID constraints pushed down from the predicates; see [`TsfPushdown`].
    pushdown: Arc<TsfPushdown>,
    cache: PlanProperties,
}

impl fmt::Debug for TsfExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TsfExec")
            .field("files", &self.files.len())
            .field("range", &self.range)
            .field("projection", &self.projection)
            .field("pushdown", &self.pushdown)
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
            pushdown: Arc::new(TsfPushdown::default()),
            cache,
        })
    }

    /// Attaches per-format read counters. Every byte fetched for a TSF file is
    /// added to the TSF read counter.
    pub fn with_metrics(mut self, metrics: Arc<StorageMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Pushes ID constraints into the scan: metrics and series that the
    /// predicates exclude are never fetched or decoded.
    pub fn with_pushdown(mut self, pushdown: TsfPushdown) -> Self {
        self.pushdown = Arc::new(pushdown);
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
        let mut ctx = storage_kernel::tsf::file::ReadContext::new();
        for (metric_id, range) in self.metric_queries(&reader, kernel_range) {
            match self.pushdown.series_ids() {
                // One series at a time: `read_series_columns` uses the bloom
                // filter and the series index to jump straight to that chunk.
                Some(filter) => {
                    for &series_id in filter.ids() {
                        let decoded = reader
                            .read_series_columns(
                                metric_id,
                                series_id,
                                range,
                                ColumnMask::ALL,
                                &mut ctx,
                            )
                            .map_err(DataFusionError::Execution)?;
                        if decoded.is_empty() {
                            continue;
                        }
                        let batch = decoded_columns_to_batch(decoded)
                            .map_err(DataFusionError::Execution)?;
                        batches.push(self.project(batch)?);
                    }
                }
                None => {
                    let columns = reader
                        .read_metric_columns(metric_id, range, ColumnMask::ALL, &mut ctx)
                        .map_err(DataFusionError::Execution)?;
                    for decoded in columns {
                        let batch = decoded_columns_to_batch(decoded)
                            .map_err(DataFusionError::Execution)?;
                        batches.push(self.project(batch)?);
                    }
                }
            }
        }
        Ok(batches)
    }

    /// Every metric the scan may read in `reader`, paired with the range to
    /// prune against. Metrics the predicates exclude are dropped here, before
    /// any of their metadata is fetched.
    fn metric_queries(
        &self,
        reader: &TsfReader,
        range: storage_kernel::tsf::file::TimeRange,
    ) -> Vec<(u64, storage_kernel::tsf::file::TimeRange)> {
        reader
            .metric_ids()
            .into_iter()
            .filter(|id| match self.pushdown.metric_ids() {
                Some(wanted) => wanted.binary_search(id).is_ok(),
                None => true,
            })
            .map(|id| (id, range))
            .collect()
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
            let metrics = self.metric_queries(&reader, kernel_range);
            let Some(more) = reader.next_scan_ranges(&metrics, self.pushdown.series_ids()) else {
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
        let pages = reader.page_ranges_for_metrics(
            &self.metric_queries(&reader, kernel_range),
            self.pushdown.series_ids(),
        );
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
            pushdown: self.pushdown.clone(),
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

    /// Equality and `IN` constraints are extracted; everything else is left to
    /// the filter above the scan.
    #[test]
    fn test_pushdown_extracts_id_constraints() {
        use datafusion::prelude::{col, lit};

        let none = TsfPushdown::from_predicates(&[]);
        assert!(none.is_empty());

        let eq = TsfPushdown::from_predicates(&[col("tsid").eq(lit(7u64))]);
        assert_eq!(eq.series_ids().unwrap().ids(), &[7]);
        assert!(eq.metric_ids().is_none());

        // Operand order does not matter.
        let reversed = TsfPushdown::from_predicates(&[lit(7u64).eq(col("tsid"))]);
        assert_eq!(reversed.series_ids().unwrap().ids(), &[7]);

        let list = TsfPushdown::from_predicates(&[
            col("tsid").in_list(vec![lit(9u64), lit(3u64), lit(3u64)], false)
        ]);
        assert_eq!(list.series_ids().unwrap().ids(), &[3, 9]);

        // Two constraints on one column intersect.
        let both = TsfPushdown::from_predicates(&[
            col("metric_id").eq(lit(1u64)),
            col("tsid").in_list(vec![lit(1u64), lit(2u64), lit(3u64)], false),
            col("tsid").gt_eq(lit(2u64)),
        ]);
        assert_eq!(both.metric_ids().unwrap(), &[1]);
        assert_eq!(both.series_ids().unwrap().ids(), &[1, 2, 3]);

        // Unsupported forms must not narrow anything.
        for predicate in [
            col("tsid").gt(lit(5u64)),
            col("tsid").in_list(vec![lit(1u64)], true), // NOT IN
            col("tsid").not_eq(lit(5u64)),
            col("value").eq(lit(5u64)),
        ] {
            assert!(
                TsfPushdown::from_predicates(std::slice::from_ref(&predicate)).is_empty(),
                "unexpected pushdown for {predicate}"
            );
        }

        // An empty IN list matches nothing, which is different from no filter.
        let empty = TsfPushdown::from_predicates(&[col("tsid").in_list(Vec::new(), false)]);
        assert!(empty.series_ids().unwrap().is_empty());
    }

    /// A metric and series filter must cut the rows read, not just the rows
    /// returned, and must never drop a row the predicate accepts.
    #[test]
    fn test_pushdown_filters_metrics_and_series() {
        use datafusion::prelude::{col, lit};

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let root_dir = temp_dir::TempDir::new().unwrap();
            let path = format!("{}/data/20231114/1.tsf", root_dir.path().to_string_lossy());
            let base = 1_700_000_000_000i64;
            let mut rows = Vec::new();
            for metric in 1..=2u64 {
                for series in 0..10u64 {
                    for i in 0..128i64 {
                        rows.push(Row {
                            metric_id: metric,
                            series_id: series,
                            timestamp: base + i * 1_000,
                            value: metric as f64 * 1_000.0 + series as f64 + i as f64,
                            sequence: 0,
                            reserved: 0,
                        });
                    }
                }
            }
            let (bytes, _) = encode_file(
                rows,
                (base, base + 128_000),
                &WriterOptions { page_rows: 32 },
            )
            .unwrap();

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
                time_range: TimeRange::new(Timestamp(base), Timestamp(base + 128_000)),
            };
            let range = TimeRange::new(Timestamp(base), Timestamp(base + 128_000));

            let run = |predicates: Vec<Expr>, expected: usize| {
                let store = store.clone();
                let file = file.clone();
                let range = range.clone();
                let counting = counting.clone();
                async move {
                    let before = counting.bytes.load(Ordering::Relaxed);
                    let exec = TsfExec::try_new(
                        store,
                        sample_schema(),
                        vec![file],
                        range,
                        None,
                        LexOrdering::default(),
                    )
                    .unwrap()
                    .with_pushdown(TsfPushdown::from_predicates(&predicates))
                    // Force the ranged path even though the file is small.
                    .with_min_ranged_bytes(0);
                    let plan = Arc::new(exec) as Arc<dyn ExecutionPlan>;
                    let stream = plan
                        .execute(0, datafusion::execution::TaskContext::default().into())
                        .unwrap();
                    let batches = collect_stream(stream).await;
                    assert_eq!(count_rows(&batches), expected);
                    counting.bytes.load(Ordering::Relaxed) - before
                }
            };

            // Everything: 2 metrics x 10 series x 128 samples.
            let all_bytes = run(Vec::new(), 2 * 10 * 128).await;

            // One metric: half the rows.
            let metric_bytes = run(vec![col("metric_id").eq(lit(1u64))], 10 * 128).await;

            // Two series of one metric: 256 of 2,560 rows.
            let narrow_bytes = run(
                vec![
                    col("metric_id").eq(lit(2u64)),
                    col("tsid").in_list(vec![lit(3u64), lit(4u64)], false),
                ],
                2 * 128,
            )
            .await;

            assert!(
                narrow_bytes * 4 < all_bytes,
                "series-filtered read fetched {narrow_bytes} of {all_bytes} bytes"
            );
            assert!(
                metric_bytes < all_bytes,
                "metric-filtered read fetched {metric_bytes} of {all_bytes} bytes"
            );

            // A filter for a series the file does not hold returns nothing and
            // reads almost nothing.
            run(vec![col("tsid").in_list(vec![lit(999u64)], false)], 0).await;
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
