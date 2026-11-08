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

use std::{any::Any, pin::Pin, sync::Arc, task::Poll};

use anyhow::Context;
use arrow::{
    array::{Array, AsArray, BooleanArray, RecordBatch},
    compute::{and_kleene, concat_batches, kernels::cmp::eq as eq_kernel},
    datatypes::{
        GenericBinaryType, Int32Type, Int64Type, Int8Type, Schema, SchemaRef, UInt32Type,
        UInt64Type, UInt8Type,
    },
};
use datafusion::{
    common::{internal_err, DFSchema},
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileScanConfig, ParquetExec, ParquetFileReaderFactory},
    },
    error::{DataFusionError, Result as DfResult},
    execution::{
        context::ExecutionProps, object_store::ObjectStoreUrl, RecordBatchStream,
        SendableRecordBatchStream, TaskContext,
    },
    logical_expr::utils::conjunction,
    parquet::arrow::async_reader::AsyncFileReader,
    physical_expr::{create_physical_expr, EquivalenceProperties, LexOrdering},
    physical_plan::{
        filter::FilterExec, metrics::ExecutionPlanMetricsSet,
        sorts::sort_preserving_merge::SortPreservingMergeExec, DisplayAs, Distribution,
        ExecutionPlan, PlanProperties,
    },
    physical_planner::create_physical_sort_exprs,
    prelude::{ident, Expr},
};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use parquet::arrow::async_reader::ParquetObjectReader;

use crate::{
    compare_primitive_columns,
    config::UpdateMode,
    operator::{BytesMergeOperator, LastValueOperator, MergeOperator, MergeOperatorRef},
    sst::{FileFormat, SstFile, SstPathGenerator},
    types::{
        ObjectStoreRef, StorageSchema, BUILTIN_COLUMN_NUM, RESERVED_COLUMN_NAME, SEQ_COLUMN_NAME,
    },
    Result,
};

#[derive(Debug, Clone)]
pub struct DefaultParquetFileReaderFactory {
    object_store: ObjectStoreRef,
}

/// Returns a AsyncFileReader factory
impl DefaultParquetFileReaderFactory {
    pub fn new(object_store: ObjectStoreRef) -> Self {
        Self { object_store }
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DfResult<Box<dyn AsyncFileReader + Send>> {
        let object_store = self.object_store.clone();
        let mut reader = ParquetObjectReader::new(object_store, file_meta.object_meta);
        if let Some(size) = metadata_size_hint {
            reader = reader.with_footer_size_hint(size);
        }
        Ok(Box::new(reader))
    }
}

/// Execution plan for merge RecordBatch values, like Merge Operator in RocksDB.
///
/// Input record batches are sorted by the primary key columns and seq
/// column.
#[derive(Debug)]
pub(crate) struct MergeExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// (0..num_primary_keys) are primary key columns
    num_primary_keys: usize,
    /// Operator to merge values when primary keys are the same
    value_operator: Arc<dyn MergeOperator>,
    /// Whether to keep the builtin columns in the output
    keep_builtin: bool,
    /// Output schema and partitioning, computed once.
    ///
    /// The output schema is *not* the input schema when `keep_builtin` is
    /// false: the merge drops the trailing builtin columns. Reporting the input
    /// schema here makes a plan built on top of this one (a `UnionExec` for a
    /// multi-segment scan) advertise columns its batches do not have.
    cache: PlanProperties,
}

impl MergeExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        num_primary_keys: usize,
        value_operator: Arc<dyn MergeOperator>,
        keep_builtin: bool,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(merge_output_schema(input.schema(), keep_builtin)),
            input.properties().output_partitioning().clone(),
            input.properties().execution_mode,
        );
        Self {
            input,
            num_primary_keys,
            value_operator,
            keep_builtin,
            cache,
        }
    }
}

/// Schema a merged stream emits: the input schema without the builtin columns
/// when the caller does not keep them. Builtin columns are always at the end.
fn merge_output_schema(input: SchemaRef, keep_builtin: bool) -> SchemaRef {
    if keep_builtin {
        return input;
    }
    let fields = input
        .fields()
        .into_iter()
        .take(input.fields().len() - BUILTIN_COLUMN_NUM)
        .cloned()
        .collect_vec();
    Arc::new(Schema::new_with_metadata(fields, input.metadata().clone()))
}
impl DisplayAs for MergeExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "MergeExec: [primary_keys: {}, keep_builtin: {}]",
            self.num_primary_keys, self.keep_builtin
        )?;
        Ok(())
    }
}

impl ExecutionPlan for MergeExec {
    fn name(&self) -> &str {
        "MergeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MergeExec::new(
            Arc::clone(&children[0]),
            self.num_primary_keys,
            self.value_operator.clone(),
            self.keep_builtin,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!("MergeExec invalid partition {partition}");
        }

        Ok(Box::pin(MergeStream::new(
            self.input.execute(partition, context)?,
            self.num_primary_keys,
            self.value_operator.clone(),
            self.keep_builtin,
        )))
    }
}

struct MergeStream {
    stream: SendableRecordBatchStream,
    num_primary_keys: usize,
    value_operator: MergeOperatorRef,
    keep_builtin: bool,

    pending_batch: Option<RecordBatch>,
    arrow_schema: SchemaRef,
}

impl MergeStream {
    fn new(
        stream: SendableRecordBatchStream,
        num_primary_keys: usize,
        value_operator: MergeOperatorRef,
        keep_builtin: bool,
    ) -> Self {
        let arrow_schema = if keep_builtin {
            let schema = stream.schema();
            let found_seq = schema.fields().iter().any(|f| f.name() == SEQ_COLUMN_NAME);
            assert!(found_seq, "Sequence column not found");
            let found_reserved = schema
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_COLUMN_NAME);
            assert!(found_reserved, "Reserved column not found");
            schema
        } else {
            merge_output_schema(stream.schema(), false)
        };
        Self {
            stream,
            num_primary_keys,
            value_operator,
            keep_builtin,
            pending_batch: None,
            arrow_schema,
        }
    }

    fn maybe_remove_builtin_columns(&self, batch: &mut RecordBatch) {
        if self.keep_builtin {
            return;
        }

        // builtin columns are always at the end.
        for _ in 0..BUILTIN_COLUMN_NUM {
            batch.remove_column(batch.num_columns() - 1);
        }
    }

    fn primary_key_eq(
        &self,
        lhs: &RecordBatch,
        lhs_idx: usize,
        rhs: &RecordBatch,
        rhs_idx: usize,
    ) -> bool {
        for k in 0..self.num_primary_keys {
            let lhs_col = lhs.column(k);
            let rhs_col = rhs.column(k);

            compare_primitive_columns!(
                lhs_col, rhs_col, lhs_idx, rhs_idx, // TODO: Add more types here
                UInt8Type, Int8Type, UInt32Type, Int32Type, UInt64Type, Int64Type
            );

            if let Some(lhs_col) = lhs_col.as_bytes_opt::<GenericBinaryType<i32>>() {
                let rhs_col = rhs_col.as_bytes::<GenericBinaryType<i32>>();
                if !rhs_col.value(rhs_idx).eq(lhs_col.value(lhs_idx)) {
                    return false;
                }
            }
        }

        true
    }

    /// Whether every row in `batch` has a primary key distinct from its
    /// neighbours and from the pending row, so the batch can be emitted without
    /// a merge-operator call per row.
    ///
    /// The general path slices one `RecordBatch` per primary-key group and
    /// calls the merge operator once per group. A time-series scan has a unique
    /// timestamp per row, so that is one slice, one dynamic-key comparison and
    /// one operator call per row, which dominates the scan (see the T5
    /// benchmark). Checking for adjacent duplicates column-wise is two vector
    /// comparisons per primary key, and in the common case it proves the fast
    /// path is safe for the whole batch.
    fn can_emit_in_bulk(&self, batch: &RecordBatch) -> bool {
        if let Some(pending) = &self.pending_batch {
            let first = batch.slice(0, 1);
            if self.primary_key_eq(pending, pending.num_rows() - 1, &first, 0) {
                return false;
            }
        }
        !self.has_adjacent_duplicate_keys(batch)
    }

    /// True if two adjacent rows share a primary key, or if the primary-key
    /// types are not supported by the comparison kernel (in which case the
    /// caller must take the general path).
    fn has_adjacent_duplicate_keys(&self, batch: &RecordBatch) -> bool {
        let rows = batch.num_rows();
        if rows < 2 {
            return false;
        }
        let mut same: Option<BooleanArray> = None;
        for idx in 0..self.num_primary_keys {
            let column = batch.column(idx);
            let lhs = column.slice(0, rows - 1);
            let rhs = column.slice(1, rows - 1);
            let lhs: &dyn Array = &lhs;
            let rhs: &dyn Array = &rhs;
            let adjacent = match eq_kernel(&lhs, &rhs) {
                Ok(adjacent) => adjacent,
                Err(_) => return true,
            };
            same = Some(match same {
                Some(acc) => match and_kleene(&acc, &adjacent) {
                    Ok(mask) => mask,
                    Err(_) => return true,
                },
                None => adjacent,
            });
        }
        same.is_some_and(|mask| mask.true_count() > 0)
    }

    fn merge_batch(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        // Fast path: no two rows share a primary key, so every group is a
        // singleton and the merge operator is the identity. Emit the whole
        // batch at once and keep only its last row as pending, since the next
        // batch may continue that key.
        if self.can_emit_in_bulk(&batch) {
            let rows = batch.num_rows();
            let mut output = Vec::with_capacity(2);
            if let Some(pending) = self.pending_batch.take() {
                output.push(pending);
            }
            self.pending_batch = Some(batch.slice(rows - 1, 1));
            if rows > 1 {
                output.push(batch.slice(0, rows - 1));
            }
            if output.is_empty() {
                return Ok(None);
            }
            let mut output = if output.len() == 1 {
                output.pop().unwrap()
            } else {
                concat_batches(&batch.schema(), &output).context("concat batch")?
            };
            self.maybe_remove_builtin_columns(&mut output);
            return Ok(Some(output));
        }

        // Group rows with the same primary keys
        let mut groupby_pk_batches = Vec::new();
        let mut start_idx = 0;
        while start_idx < batch.num_rows() {
            let mut end_idx = start_idx + 1;
            while end_idx < batch.num_rows()
                && self.primary_key_eq(&batch, start_idx, &batch, end_idx)
            {
                end_idx += 1;
            }
            groupby_pk_batches.push(batch.slice(start_idx, end_idx - start_idx));
            start_idx = end_idx;
        }

        let rows_with_same_primary_keys = &groupby_pk_batches[0];
        let mut output_batches = Vec::new();
        if let Some(pending) = self.pending_batch.take() {
            if self.primary_key_eq(
                &pending,
                pending.num_rows() - 1,
                rows_with_same_primary_keys,
                0,
            ) {
                groupby_pk_batches[0] = concat_batches(
                    &self.stream.schema(),
                    [&pending, rows_with_same_primary_keys],
                )
                .context("concat batch")?;
            } else {
                output_batches.push(self.value_operator.merge(pending)?);
            }
        }

        // last batch may have overlapping rows with the next batch, so keep
        // them in pending_batch
        self.pending_batch = groupby_pk_batches.pop();

        for batch in groupby_pk_batches {
            output_batches.push(self.value_operator.merge(batch)?);
        }
        if output_batches.is_empty() {
            return Ok(None);
        }

        let mut output_batches =
            concat_batches(&self.stream.schema(), output_batches.iter()).context("concat batch")?;

        self.maybe_remove_builtin_columns(&mut output_batches);
        Ok(Some(output_batches))
    }
}

impl Stream for MergeStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut std::task::Context,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.stream.poll_next_unpin(ctx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    let value = if let Some(mut pending) = self.pending_batch.take() {
                        self.maybe_remove_builtin_columns(&mut pending);
                        let res = self
                            .value_operator
                            .merge(pending)
                            .map_err(|e| DataFusionError::External(Box::new(e)));
                        Some(res)
                    } else {
                        None
                    };
                    return Poll::Ready(value);
                }
                Poll::Ready(Some(v)) => match v {
                    Ok(v) => match self.merge_batch(v) {
                        Ok(v) => {
                            if let Some(v) = v {
                                return Poll::Ready(Some(Ok(v)));
                            }
                        }
                        Err(e) => {
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))))
                        }
                    },
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
            }
        }
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }
}

pub struct ParquetReader {
    store: ObjectStoreRef,
    schema: StorageSchema,
    sst_path_gen: Arc<SstPathGenerator>,
    /// Per-format read counters (ADR-0002 §13). Only the query reader has one:
    /// compaction and rewrite build their own readers, whose reads are write
    /// amplification rather than query traffic.
    metrics: Option<Arc<crate::metrics::StorageMetrics>>,
}

impl ParquetReader {
    pub fn new(
        store: ObjectStoreRef,
        schema: StorageSchema,
        sst_path_gen: Arc<SstPathGenerator>,
    ) -> Self {
        Self {
            store,
            schema,
            sst_path_gen,
            metrics: None,
        }
    }

    /// Attaches per-format read counters to the plans this reader builds.
    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::StorageMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn build_sort_exprs(&self, df_schema: &DFSchema, sort_seq: bool) -> Result<LexOrdering> {
        let mut sort_exprs = (0..self.schema.num_primary_keys)
            .map(|i| {
                ident(self.schema.arrow_schema.field(i).name())
                    .sort(true /* asc */, true /* nulls_first */)
            })
            .collect::<Vec<_>>();
        if sort_seq {
            sort_exprs.push(ident(SEQ_COLUMN_NAME).sort(true, true));
        }
        let sort_exprs =
            create_physical_sort_exprs(&sort_exprs, df_schema, &ExecutionProps::default())
                .context("create physical sort exprs")?;

        Ok(sort_exprs)
    }

    pub fn build_df_plan(
        &self,
        ssts: Vec<SstFile>,
        projection: Option<Vec<usize>>,
        predicates: Vec<Expr>,
        keep_builtin: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // ADR-0002 §7.2: during migration both formats are selected and merged.
        let (parquet_ssts, tsf_ssts): (Vec<SstFile>, Vec<SstFile>) =
            ssts.into_iter().partition(|f| !f.meta().is_tsf());

        // The same predicates are applied above the scan, but the ID ones
        // decide what the TSF plan fetches: without this a single-series query
        // decodes every series of the metric.
        let pushdown = crate::tsf::exec::TsfPushdown::from_predicates(&predicates);

        let df_schema =
            DFSchema::try_from(self.schema.arrow_schema.clone()).context("build DFSchema")?;
        let sort_exprs = self.build_sort_exprs(&df_schema, true /* sort_seq */)?;

        // One read per selected file, per format.
        if let Some(metrics) = &self.metrics {
            metrics
                .for_format(FileFormat::Parquet)
                .record_file_read_many(parquet_ssts.len() as u64);
            metrics
                .for_format(FileFormat::Tsf)
                .record_file_read_many(tsf_ssts.len() as u64);
        }

        // Build one physical plan per format and merge under the sort.
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        if !parquet_ssts.is_empty() {
            plans.push(self.build_parquet_plan(
                parquet_ssts,
                projection.clone(),
                predicates.clone(),
                sort_exprs.clone(),
            )?);
        }
        if !tsf_ssts.is_empty() {
            let files = tsf_ssts
                .into_iter()
                .map(|f| crate::tsf::TsfFile {
                    path: object_store::path::Path::from(
                        self.sst_path_gen.generate_for(f.id(), f.meta()),
                    ),
                    time_range: f.meta().time_range.clone(),
                })
                .collect::<Vec<_>>();
            let range = files
                .iter()
                .fold(None::<crate::types::TimeRange>, |acc, f| match acc {
                    None => Some(f.time_range.clone()),
                    Some(mut acc) => {
                        acc.merge(&f.time_range);
                        Some(acc)
                    }
                })
                .unwrap_or_else(|| {
                    crate::types::TimeRange::new(
                        crate::types::Timestamp::MIN,
                        crate::types::Timestamp::MAX,
                    )
                });
            plans.push(Arc::new(
                crate::tsf::TsfExec::try_new(
                    self.store.clone(),
                    self.schema.clone(),
                    files,
                    range,
                    projection.clone(),
                    sort_exprs.clone(),
                )
                .map(|exec| exec.with_pushdown(pushdown))
                .map(|exec| match &self.metrics {
                    Some(metrics) => exec.with_metrics(metrics.clone()),
                    None => exec,
                })
                .map_err(|e| anyhow::anyhow!("build TSF plan: {e}"))?,
            ));
        }

        let base_plan = match plans.len() {
            0 => {
                return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                    self.schema.arrow_schema.clone(),
                )))
            }
            1 => plans.remove(0),
            _ => Arc::new(datafusion::physical_plan::union::UnionExec::new(plans)),
        };

        let base_plan: Arc<dyn ExecutionPlan> = match conjunction(predicates) {
            Some(expr) => {
                let filters = create_physical_expr(&expr, &df_schema, &ExecutionProps::new())
                    .context("create physical expr")?;
                let filter_exec =
                    FilterExec::try_new(filters, base_plan).context("create filter exec")?;
                Arc::new(filter_exec)
            }
            None => base_plan,
        };

        // TODO: fetch using multiple threads since read from parquet will incur
        // CPU when convert between arrow and parquet.
        let sort_exec =
            SortPreservingMergeExec::new(sort_exprs, base_plan).with_round_robin_repartition(true);

        let merge_exec = MergeExec::new(
            Arc::new(sort_exec),
            self.schema.num_primary_keys,
            match self.schema.update_mode {
                UpdateMode::Overwrite => Arc::new(LastValueOperator),
                UpdateMode::Append => {
                    Arc::new(BytesMergeOperator::new(self.schema.value_idxes.clone()))
                }
            },
            keep_builtin,
        );
        Ok(Arc::new(merge_exec))
    }

    fn build_parquet_plan(
        &self,
        ssts: Vec<SstFile>,
        projection: Option<Vec<usize>>,
        predicates: Vec<Expr>,
        sort_exprs: LexOrdering,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // we won't use url for selecting object_store.
        let dummy_url = ObjectStoreUrl::parse("empty://").unwrap();
        let df_schema =
            DFSchema::try_from(self.schema.arrow_schema.clone()).context("build DFSchema")?;

        let file_groups = ssts
            .into_iter()
            .map(|f| {
                vec![PartitionedFile::new(
                    self.sst_path_gen.generate(f.id()),
                    f.meta().size as u64,
                )]
            })
            .collect::<Vec<_>>();
        let scan_config = FileScanConfig::new(dummy_url, self.schema.arrow_schema.clone())
            .with_output_ordering(vec![sort_exprs.clone(); file_groups.len()])
            .with_file_groups(file_groups)
            .with_projection(projection);

        // Count Parquet reads at the object store: `ParquetExec` owns its
        // reader and reports nothing upward.
        let read_store = match &self.metrics {
            Some(metrics) => Arc::new(crate::metrics::CountingStore::new(
                self.store.clone(),
                metrics.clone(),
                FileFormat::Parquet,
            )) as ObjectStoreRef,
            None => self.store.clone(),
        };
        let mut builder = ParquetExec::builder(scan_config).with_parquet_file_reader_factory(
            Arc::new(DefaultParquetFileReaderFactory::new(read_store)),
        );
        match conjunction(predicates) {
            Some(expr) => {
                let filters = create_physical_expr(&expr, &df_schema, &ExecutionProps::new())
                    .context("create physical expr")?;
                builder = builder.with_predicate(filters);
                Ok(Arc::new(builder.build()))
            }
            None => Ok(Arc::new(builder.build())),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{col, lit};
    use object_store::local::LocalFileSystem;
    use test_log::test;

    use super::*;
    use crate::{
        arrow_schema,
        operator::{BytesMergeOperator, LastValueOperator, MergeOperatorRef},
        record_batch,
        sst::{FileFormat, FileMeta},
        test_util::{check_stream, make_sendable_record_batches},
    };

    #[test(tokio::test)]
    async fn test_merge_stream() {
        let expected = [
            record_batch!(
                ("pk1", UInt8, vec![11, 12]),
                ("value", Binary, vec![b"2", b"4"])
            )
            .unwrap(),
            record_batch!(("pk1", UInt8, vec![13]), ("value", Binary, vec![b"8"])).unwrap(),
            record_batch!(("pk1", UInt8, vec![14]), ("value", Binary, vec![b"9"])).unwrap(),
        ];

        test_merge_stream_inner(Arc::new(LastValueOperator), expected).await;

        let expected = [
            record_batch!(
                ("pk1", UInt8, vec![11, 12]),
                ("value", Binary, vec![b"12", b"34"])
            )
            .unwrap(),
            record_batch!(("pk1", UInt8, vec![13]), ("value", Binary, vec![b"5678"])).unwrap(),
            record_batch!(("pk1", UInt8, vec![14]), ("value", Binary, vec![b"9"])).unwrap(),
        ];

        test_merge_stream_inner(Arc::new(BytesMergeOperator::new(vec![1])), expected).await;
    }

    /// A batch whose rows all have distinct primary keys is emitted without a
    /// merge-operator call per row: the T5 benchmark showed that path
    /// dominating a full scan.
    #[test(tokio::test)]
    async fn test_merge_stream_bulk_path_for_distinct_keys() {
        let stream = make_sendable_record_batches([
            record_batch!(
                ("pk1", UInt8, vec![11, 12]),
                ("value", Binary, vec![b"1", b"2"]),
                (SEQ_COLUMN_NAME, UInt8, vec![1, 2]),
                (RESERVED_COLUMN_NAME, UInt8, vec![None; 2])
            )
            .unwrap(),
            record_batch!(
                ("pk1", UInt8, vec![13, 14]),
                ("value", Binary, vec![b"3", b"4"]),
                (SEQ_COLUMN_NAME, UInt8, vec![3, 4]),
                (RESERVED_COLUMN_NAME, UInt8, vec![None; 2])
            )
            .unwrap(),
        ]);
        let expected = record_batch!(
            ("pk1", UInt8, vec![11, 12, 13, 14]),
            ("value", Binary, vec![b"1", b"2", b"3", b"4"])
        )
        .unwrap();

        let mut stream = Box::pin(MergeStream::new(
            stream,
            1,
            Arc::new(LastValueOperator),
            false, // keep_builtin
        ));
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        let actual = concat_batches(&expected.schema(), &batches).unwrap();
        assert_eq!(actual, expected);
    }

    /// The bulk path must still merge a key that continues from the pending row
    /// of the previous batch.
    #[test(tokio::test)]
    async fn test_merge_stream_bulk_path_merges_across_batches() {
        let stream = make_sendable_record_batches([
            record_batch!(
                ("pk1", UInt8, vec![11, 12]),
                ("value", Binary, vec![b"1", b"2"]),
                (SEQ_COLUMN_NAME, UInt8, vec![1, 2]),
                (RESERVED_COLUMN_NAME, UInt8, vec![None; 2])
            )
            .unwrap(),
            record_batch!(
                ("pk1", UInt8, vec![12, 13]),
                ("value", Binary, vec![b"3", b"4"]),
                (SEQ_COLUMN_NAME, UInt8, vec![3, 4]),
                (RESERVED_COLUMN_NAME, UInt8, vec![None; 2])
            )
            .unwrap(),
        ]);
        // Last value wins for the shared key: 12 -> "3".
        let expected = record_batch!(
            ("pk1", UInt8, vec![11, 12, 13]),
            ("value", Binary, vec![b"1", b"3", b"4"])
        )
        .unwrap();

        let mut stream = Box::pin(MergeStream::new(
            stream,
            1,
            Arc::new(LastValueOperator),
            false, // keep_builtin
        ));
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        let actual = concat_batches(&expected.schema(), &batches).unwrap();
        assert_eq!(actual, expected);
    }

    async fn test_merge_stream_inner<I>(merge_op: MergeOperatorRef, expected: I)
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        let stream = make_sendable_record_batches([
            record_batch!(
                ("pk1", UInt8, vec![11, 11, 12, 12, 13]),
                ("value", Binary, vec![b"1", b"2", b"3", b"4", b"5"]),
                (SEQ_COLUMN_NAME, UInt8, vec![1, 2, 3, 4, 5]),
                (RESERVED_COLUMN_NAME, UInt8, vec![None; 5])
            )
            .unwrap(),
            record_batch!(
                ("pk1", UInt8, vec![13, 13]),
                ("value", Binary, vec![b"6", b"7"]),
                (SEQ_COLUMN_NAME, UInt8, vec![6, 7]),
                (RESERVED_COLUMN_NAME, UInt8, vec![None; 2])
            )
            .unwrap(),
            record_batch!(
                ("pk1", UInt8, vec![13, 14]),
                ("value", Binary, vec![b"8", b"9"]),
                (SEQ_COLUMN_NAME, UInt8, vec![8, 9]),
                (RESERVED_COLUMN_NAME, UInt8, vec![None; 2])
            )
            .unwrap(),
        ]);

        let stream = MergeStream::new(
            stream, 1,        // num_primary_keys
            merge_op, // merge_operator
            false,    // keep_builtin
        );
        check_stream(Box::pin(stream), expected).await;
    }

    #[tokio::test]
    async fn test_build_scan_plan() {
        let schema = arrow_schema!(("pk1", UInt8), ("value", UInt8));
        let store = Arc::new(LocalFileSystem::new());
        let reader = ParquetReader::new(
            store,
            StorageSchema::try_new(schema, 1, UpdateMode::Overwrite).unwrap(),
            Arc::new(SstPathGenerator::new("mock".to_string())),
        );

        let expr = col("pk1").eq(lit(0_u8));
        let plan = reader
            .build_df_plan(
                (100..103)
                    .map(|id| {
                        SstFile::new(
                            id,
                            FileMeta {
                                max_sequence: id,
                                min_sequence: 0,
                                num_rows: 1,
                                size: 1,
                                time_range: (1..10).into(),
                                format: FileFormat::Parquet,
                            },
                        )
                    })
                    .collect(),
                None,
                vec![expr],
                false, // keep_builtin
            )
            .unwrap();
        let display_plan =
            datafusion::physical_plan::display::DisplayableExecutionPlan::new(plan.as_ref())
                .indent(true);
        assert_eq!(
            r#"MergeExec: [primary_keys: 1, keep_builtin: false]
  SortPreservingMergeExec: [pk1@0 ASC, __seq__@2 ASC]
    FilterExec: pk1@0 = 0
      ParquetExec: file_groups={3 groups: [[mock/data/100.sst], [mock/data/101.sst], [mock/data/102.sst]]}, projection=[pk1, value, __seq__, __reserved__], output_orderings=[[pk1@0 ASC, __seq__@2 ASC], [pk1@0 ASC, __seq__@2 ASC], [pk1@0 ASC, __seq__@2 ASC]], predicate=pk1@0 = 0, pruning_predicate=CASE WHEN pk1_null_count@2 = pk1_row_count@3 THEN false ELSE pk1_min@0 <= 0 AND 0 <= pk1_max@1 END, required_guarantees=[pk1 in (0)]
"#,
            format!("{display_plan}")
        );
    }
}
