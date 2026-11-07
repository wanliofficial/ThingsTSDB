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
use futures::{stream, TryStreamExt};
use object_store::path::Path;
use storage_kernel::tsf::{file::TsfReader, ColumnMask};

use crate::{
    tsf::scan::{decoded_columns_to_batch, tsf_schema},
    types::{ObjectStoreRef, StorageSchema, TimeRange},
};

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
            cache,
        })
    }

    /// Reads one file as a single sorted stream.
    async fn read_file(&self, file: &TsfFile) -> DfResult<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        if !file.time_range.overlaps(&self.range) {
            return Ok(batches);
        }
        // Prune with this file's own range: each file declares the range of
        // its contents, so a multi-file plan does not decode pages outside the
        // file. File-level pruning against the scan range already happened in
        // `Manifest::find_ssts`.
        let kernel_range = crate::tsf::scan::kernel_time_range(&file.time_range);
        let bytes = self
            .store
            .get(&file.path)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .bytes()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let reader = TsfReader::from_bytes(bytes.to_vec())
            .map_err(|e| DataFusionError::Execution(format!("open TSF file {}: {e}", file.path)))?;
        let mut ctx = storage_kernel::tsf::file::ReadContext::new();
        for metric_id in reader.metric_ids() {
            let columns = reader
                .read_metric_columns(metric_id, kernel_range, ColumnMask::ALL, &mut ctx)
                .map_err(DataFusionError::Execution)?;
            for decoded in columns {
                let batch =
                    decoded_columns_to_batch(decoded).map_err(DataFusionError::Execution)?;
                batches.push(self.project(batch)?);
            }
        }
        Ok(batches)
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
