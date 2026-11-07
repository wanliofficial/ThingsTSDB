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

//! Parquet-to-TSF rewrite, the last step of the ADR-0002 §10 migration.
//!
//! Once a table is switched to `format = "tsf"`, new flushes write TSF but the
//! Parquet SSTs written before the switch stay in the manifest. This module
//! rewrites those files in place: read a set of Parquet files, write the merged
//! rows as TSF, then swap the manifest entries.
//!
//! The rewrite is deliberately conservative:
//!
//! - it only handles files that fit in one UTC day, because a TSF file belongs
//!   to exactly one partition directory;
//! - it commits the new file before deleting the old ones, so a crash leaves
//!   duplicates (harmless under LWW) rather than data loss;
//! - it never touches files that are already TSF.
//!
//! Running it repeatedly is safe: each pass converges because rewritten files
//! are skipped.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Context;
use datafusion::{execution::TaskContext, physical_plan::execute_stream};
use storage_kernel::tsf::partition::partition_index;
use tracing::{debug, info};

use crate::{
    ensure,
    manifest::{ManifestRef, ManifestUpdate},
    read::ParquetReader,
    sst::{FileFormat, FileMeta, SstFile, SstPathGenerator},
    types::{ObjectStoreRef, StorageSchema},
    Result,
};

/// What one rewrite pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RewriteReport {
    /// Parquet files read and replaced by TSF files.
    pub rewritten_files: usize,
    /// Parquet files left alone because they span more than one UTC day.
    pub skipped_multi_day: usize,
    /// TSF files produced.
    pub output_files: usize,
    /// Rows written across all output files.
    pub output_rows: u64,
}

impl RewriteReport {
    /// Whether the pass made progress. A pass that rewrites nothing will not
    /// make progress on a later run either, so callers can stop looping.
    pub fn made_progress(&self) -> bool {
        self.rewritten_files > 0
    }
}

/// Rewrites every Parquet SST in `ssts` to TSF, one output file per UTC day.
///
/// The caller decides which files to rewrite (for example, only files inside
/// the retention window). `ssts` may contain TSF files; they are ignored.
pub async fn rewrite_to_tsf(
    store: &ObjectStoreRef,
    schema: &StorageSchema,
    manifest: &ManifestRef,
    reader: &Arc<ParquetReader>,
    path_gen: &Arc<SstPathGenerator>,
    ssts: Vec<SstFile>,
) -> Result<RewriteReport> {
    let mut report = RewriteReport::default();

    // Group Parquet files by the day of their start timestamp. A file whose
    // range crosses a day boundary is skipped: splitting it would need page
    // level work, which is a follow-up.
    let mut by_day: BTreeMap<i64, Vec<SstFile>> = BTreeMap::new();
    for sst in ssts {
        if sst.meta().format != FileFormat::Parquet {
            continue;
        }
        let day_start = partition_index(*sst.meta().time_range.start);
        let day_end = partition_index(*sst.meta().time_range.end - 1);
        if day_start != day_end {
            report.skipped_multi_day += 1;
            debug!(
                id = sst.id(),
                range = ?sst.meta().time_range,
                "Skip multi-day Parquet file in TSF rewrite"
            );
            continue;
        }
        by_day.entry(day_start).or_default().push(sst);
    }

    for (day, inputs) in by_day {
        let mut time_range = inputs[0].meta().time_range.clone();
        let mut min_sequence = inputs[0].meta().min_sequence;
        for f in &inputs[1..] {
            time_range.merge(&f.meta().time_range);
            min_sequence = min_sequence.min(f.meta().min_sequence);
        }

        let plan = reader.build_df_plan(
            inputs.clone(),
            None,       // projection
            Vec::new(), // predicate
            true,       // keep_builtin: the sequence column must survive
        )?;
        let mut stream = execute_stream(plan, Arc::new(TaskContext::default()))
            .context("execute rewrite plan")?;

        let file_id = SstFile::allocate_id();
        let (size, num_rows, written_min_sequence) = crate::tsf::write::write_stream(
            store,
            path_gen,
            schema,
            file_id,
            &mut stream,
            &time_range,
        )
        .await
        .with_context(|| format!("rewrite Parquet files for day {day}"))?;

        let file_meta = FileMeta {
            max_sequence: file_id,
            // The encoded file is authoritative: LWW dedup at encode time can
            // drop rows, so the header's minimum wins over the input minimum.
            min_sequence: written_min_sequence.min(min_sequence),
            num_rows: num_rows as u32,
            size: size as u32,
            time_range,
            format: FileFormat::Tsf,
        };
        ensure!(
            num_rows > 0,
            "TSF rewrite produced an empty file for day {day}"
        );

        let to_deletes = inputs.iter().map(|f| f.id()).collect::<Vec<_>>();
        // Commit the new file first: a crash here leaves both copies, which
        // LWW resolves, instead of losing data.
        manifest
            .update(ManifestUpdate::new(
                vec![SstFile::new(file_id, file_meta)],
                to_deletes.clone(),
            ))
            .await?;

        info!(
            day,
            inputs = inputs.len(),
            num_rows,
            "Rewrote Parquet files to TSF"
        );
        report.rewritten_files += inputs.len();
        report.output_files += 1;
        report.output_rows += num_rows as u64;
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Float64Array, Int64Array, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use datafusion::{execution::TaskContext, physical_plan::execute_stream};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use parquet::{
        arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
        file::properties::WriterProperties,
    };
    use storage_kernel::tsf::partition::DAY_MS;

    use super::*;
    use crate::{
        config::ManifestConfig,
        manifest::Manifest,
        read::ParquetReader,
        types::{StorageSchema, TimeRange},
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

    /// Writes a Parquet SST the way the flush path does, then registers it.
    async fn add_parquet_sst(
        store: &ObjectStoreRef,
        manifest: &ManifestRef,
        path_gen: &SstPathGenerator,
        schema: &StorageSchema,
        rows: &[(u64, u64, i64, f64)],
        time_range: TimeRange,
    ) -> SstFile {
        let id = SstFile::allocate_id();
        let path = object_store::path::Path::from(path_gen.generate(id));
        let writer = ParquetObjectWriter::new(store.clone(), path.clone());
        let mut writer = AsyncArrowWriter::try_new(
            writer,
            schema.arrow_schema.clone(),
            Some(WriterProperties::builder().build()),
        )
        .unwrap();
        let batch = schema
            .fill_builtin_columns_with_start_seq(batch(rows), 100)
            .unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();
        let size = store.head(&path).await.unwrap().size;

        let file = SstFile::new(
            id,
            FileMeta {
                max_sequence: 100 + rows.len() as u64 - 1,
                min_sequence: 100,
                num_rows: rows.len() as u32,
                size: size as u32,
                time_range,
                format: FileFormat::Parquet,
            },
        );
        manifest.add_file(id, file.meta().clone()).await.unwrap();
        file
    }

    #[test_log::test(test)]
    fn test_rewrite_parquet_files_to_tsf() {
        let root = temp_dir::TempDir::new().unwrap();
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let manifest = Arc::new(
            rt.block_on(async {
                Manifest::try_new(
                    root.path().to_string_lossy().to_string(),
                    store.clone(),
                    rt.clone(),
                    ManifestConfig::default(),
                )
                .await
            })
            .unwrap(),
        );
        let schema =
            StorageSchema::try_new(sample_schema(), 3, crate::config::UpdateMode::Overwrite)
                .unwrap();
        let path_gen = Arc::new(SstPathGenerator::new(
            root.path().to_string_lossy().to_string(),
        ));
        let reader = Arc::new(ParquetReader::new(
            store.clone(),
            schema.clone(),
            path_gen.clone(),
        ));

        let day = 20_000 * DAY_MS;

        rt.block_on(async {
            add_parquet_sst(
                &store,
                &manifest,
                &path_gen,
                &schema,
                &[(1, 10, day, 1.5), (1, 11, day + 1, 2.5)],
                (day..day + 2).into(),
            )
            .await;

            let report = rewrite_to_tsf(
                &store,
                &schema,
                &manifest,
                &reader,
                &path_gen,
                manifest.all_ssts().await,
            )
            .await
            .unwrap();
            assert_eq!(report.rewritten_files, 1);
            assert_eq!(report.output_files, 1);
            assert_eq!(report.output_rows, 2);
            assert_eq!(report.skipped_multi_day, 0);
            assert!(report.made_progress());

            let ssts = manifest.all_ssts().await;
            assert_eq!(ssts.len(), 1);
            assert_eq!(ssts[0].meta().format, FileFormat::Tsf);
            let path = path_gen.generate_for(ssts[0].id(), ssts[0].meta());
            assert!(path.ends_with(".tsf"), "unexpected path {path}");
            store
                .get(&object_store::path::Path::from(path))
                .await
                .expect("rewritten TSF object must exist");

            // A second pass has nothing left to do.
            let report = rewrite_to_tsf(
                &store,
                &schema,
                &manifest,
                &reader,
                &path_gen,
                manifest.all_ssts().await,
            )
            .await
            .unwrap();
            assert_eq!(report, RewriteReport::default());
            assert!(!report.made_progress());

            // The rewritten file still reads back through the shared plan.
            let plan = reader
                .build_df_plan(manifest.all_ssts().await, None, Vec::new(), true)
                .unwrap();
            let mut stream = execute_stream(plan, Arc::new(TaskContext::default())).unwrap();
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch.unwrap().num_rows();
            }
            assert_eq!(rows, 2);
        });
    }

    /// A Parquet file whose range crosses a UTC day boundary cannot become one
    /// TSF file, so the rewrite must leave it alone and report it.
    #[test_log::test(test)]
    fn test_rewrite_skips_multi_day_parquet() {
        let root = temp_dir::TempDir::new().unwrap();
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let manifest = Arc::new(
            rt.block_on(async {
                Manifest::try_new(
                    root.path().to_string_lossy().to_string(),
                    store.clone(),
                    rt.clone(),
                    ManifestConfig::default(),
                )
                .await
            })
            .unwrap(),
        );
        let schema =
            StorageSchema::try_new(sample_schema(), 3, crate::config::UpdateMode::Overwrite)
                .unwrap();
        let path_gen = Arc::new(SstPathGenerator::new(
            root.path().to_string_lossy().to_string(),
        ));
        let reader = Arc::new(ParquetReader::new(
            store.clone(),
            schema.clone(),
            path_gen.clone(),
        ));

        let day = 20_000 * DAY_MS;
        rt.block_on(async {
            add_parquet_sst(
                &store,
                &manifest,
                &path_gen,
                &schema,
                &[(1, 10, day, 1.5), (1, 10, day + DAY_MS, 2.5)],
                (day..day + DAY_MS + 1).into(),
            )
            .await;

            let report = rewrite_to_tsf(
                &store,
                &schema,
                &manifest,
                &reader,
                &path_gen,
                manifest.all_ssts().await,
            )
            .await
            .unwrap();
            assert_eq!(report.skipped_multi_day, 1);
            assert_eq!(report.rewritten_files, 0);
            assert!(!report.made_progress());

            // The original file is untouched.
            let ssts = manifest.all_ssts().await;
            assert_eq!(ssts.len(), 1);
            assert_eq!(ssts[0].meta().format, FileFormat::Parquet);
        });
    }
}
