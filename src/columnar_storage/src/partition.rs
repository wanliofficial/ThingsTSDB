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

//! Day-partition discovery, TTL drops and orphan GC (ADR-0002 §8.3, §12 T3).
//!
//! The manifest is the source of truth for live files, so this module never
//! needs to list `data/` to answer a query. It lists only to maintain the
//! storage: discovering which day partitions exist, dropping expired ones, and
//! deleting objects that no manifest entry references.
//!
//! Ordering rule for every destructive operation: commit the manifest first,
//! delete objects afterwards. A crash in between leaves garbage, which the next
//! GC pass removes; the reverse order would leave queries reading files the
//! manifest still points at.

use std::{collections::HashSet, sync::Arc};

use object_store::path::Path;
use storage_kernel::tsf::partition::{parse_partition_label, partition_label};
use tracing::{debug, warn};

use crate::{
    manifest::ManifestRef,
    sst::{FileId, SstPathGenerator},
    types::ObjectStoreRef,
    Result,
};

/// A day partition that exists on storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    /// UTC-day index (`timestamp_ms.div_euclid(DAY_MS)`).
    pub index: i64,
    /// Directory label, `YYYYMMDD`.
    pub label: String,
    /// Number of objects under the directory, including orphans.
    pub objects: usize,
    /// Total size of those objects, in bytes.
    pub bytes: u64,
}

/// What one orphan sweep removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Objects deleted because no manifest entry referenced them.
    pub orphan_files: usize,
    /// Bytes deleted.
    pub orphan_bytes: u64,
    /// Objects left alone because the manifest still references them.
    pub retained_files: usize,
    /// Delete attempts that failed; they are retried by the next pass.
    pub failed_deletes: usize,
}

impl GcReport {
    pub fn made_progress(&self) -> bool {
        self.orphan_files > 0
    }
}

#[derive(Clone)]
pub struct PartitionManager {
    store: ObjectStoreRef,
    manifest: ManifestRef,
    path_gen: Arc<SstPathGenerator>,
}

impl PartitionManager {
    pub fn new(
        store: ObjectStoreRef,
        manifest: ManifestRef,
        path_gen: Arc<SstPathGenerator>,
    ) -> Self {
        Self {
            store,
            manifest,
            path_gen,
        }
    }

    /// Lists the day partitions under `data/`.
    ///
    /// Parquet files live directly in `data/`, so a listing there is a mix of
    /// day directories and legacy objects. Anything that is not a valid
    /// `YYYYMMDD` segment is ignored: it cannot be a partition, and deleting it
    /// here would be surprising.
    pub async fn list_partitions(&self) -> Result<Vec<Partition>> {
        use futures::TryStreamExt;

        let root = Path::from(format!("{}/data", self.path_gen.prefix()));
        let listing = self
            .store
            .list_with_delimiter(Some(&root))
            .await
            .map_err(|e| anyhow::anyhow!("list {root}: {e}"))?;

        let mut partitions = Vec::new();
        for prefix in listing.common_prefixes {
            let Some(label) = prefix.parts().last().map(|p| p.as_ref().to_string()) else {
                continue;
            };
            let Some(index) = parse_partition_label(&label) else {
                debug!(label, "Skip non-partition directory under data/");
                continue;
            };
            let mut objects = 0;
            let mut bytes = 0;
            let mut stream = self.store.list(Some(&prefix));
            while let Some(meta) = stream
                .try_next()
                .await
                .map_err(|e| anyhow::anyhow!("list partition {label}: {e}"))?
            {
                objects += 1;
                bytes += meta.size as u64;
            }
            // A directory whose objects are all gone is not a partition any
            // more; deleting objects does not remove the directory itself on a
            // local filesystem, and reporting it would make a TTL drop look
            // like it did nothing.
            if objects == 0 {
                continue;
            }
            partitions.push(Partition {
                index,
                label,
                objects,
                bytes,
            });
        }
        partitions.sort_unstable_by_key(|p| p.index);
        Ok(partitions)
    }

    /// Drops every partition older than `ttl_ms`, returning the files removed
    /// from the manifest.
    ///
    /// Manifest entries are committed first; only then are objects deleted. If
    /// the object delete fails the manifest no longer references the files, so
    /// a later [`Self::gc_orphans`] finishes the job.
    pub async fn drop_expired(&self, now_ms: i64, ttl_ms: i64) -> Result<Vec<FileId>> {
        let partitions = self.list_partitions().await?;
        let indexes = partitions.iter().map(|p| p.index).collect::<Vec<_>>();
        let removed = self
            .manifest
            .drop_expired_partitions(&indexes, now_ms, ttl_ms)
            .await?;
        if removed.is_empty() {
            return Ok(removed);
        }
        let expired = partitions
            .into_iter()
            .filter(|p| {
                storage_kernel::tsf::partition::plan_ttl_drop([p.index], now_ms, ttl_ms)
                    .contains(&p.index)
            })
            .collect::<Vec<_>>();
        self.delete_partition_objects(&expired).await;
        Ok(removed)
    }

    /// Deletes every object under the given partitions, best effort.
    ///
    /// Called after the manifest commit that dropped them, so failures are
    /// logged and left for GC rather than returned.
    async fn delete_partition_objects(&self, partitions: &[Partition]) {
        for partition in partitions {
            let root = Path::from(format!(
                "{}/data/{}",
                self.path_gen.prefix(),
                partition.label
            ));
            let mut stream = self.store.list(Some(&root));
            use futures::TryStreamExt;
            let mut deleted = 0;
            while let Ok(Some(meta)) = stream.try_next().await {
                match self.store.delete(&meta.location).await {
                    Ok(()) => deleted += 1,
                    Err(e) => warn!(
                        path = %meta.location,
                        error = %e,
                        "Delete expired partition object failed; GC will retry"
                    ),
                }
            }
            debug!(
                label = partition.label,
                deleted, "Deleted expired partition"
            );
            // Removing the now-empty directory is local-FS specific and not
            // required by the layout, so it is left alone.
        }
    }

    /// Deletes objects under `data/` that no manifest entry references.
    ///
    /// This is the only pass that walks the whole data directory, so it is
    /// meant for maintenance, not the query path. It never deletes a file that
    /// the manifest lists, including files a compaction has just written but
    /// not yet committed (they are simply not listed yet, so a *very* unlucky
    /// interleaving could delete them: run GC when writes are quiet, which the
    /// operator-facing API documents).
    pub async fn gc_orphans(&self) -> Result<GcReport> {
        use futures::TryStreamExt;

        // Normalise both sides through `Path`: a local store reports locations
        // without the leading slash that a filesystem root adds to the string
        // the path generator produced.
        let live = self
            .manifest
            .all_ssts()
            .await
            .into_iter()
            .map(|f| Path::from(self.path_gen.generate_for(f.id(), f.meta())).to_string())
            .collect::<HashSet<String>>();

        let root = Path::from(format!("{}/data", self.path_gen.prefix()));
        let mut stream = self.store.list(Some(&root));
        let mut report = GcReport::default();
        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(|e| anyhow::anyhow!("list {root}: {e}"))?
        {
            let location = meta.location.to_string();
            if live.contains(&location) {
                report.retained_files += 1;
                continue;
            }
            match self.store.delete(&meta.location).await {
                Ok(()) => {
                    report.orphan_files += 1;
                    report.orphan_bytes += meta.size as u64;
                }
                Err(e) => {
                    report.failed_deletes += 1;
                    warn!(path = %meta.location, error = %e, "Delete orphan object failed");
                }
            }
        }
        Ok(report)
    }
}

/// Formats a partition index as a directory label, for logs and tests.
pub fn label_of(index: i64) -> String {
    partition_label(index * storage_kernel::tsf::partition::DAY_MS)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use storage_kernel::tsf::partition::DAY_MS;

    use super::*;
    use crate::{
        config::ManifestConfig,
        manifest::Manifest,
        sst::{FileFormat, FileMeta, SstFile},
        types::TimeRange,
    };

    fn file_meta(day: i64, size: u32) -> FileMeta {
        let start = day * DAY_MS + 1_000;
        FileMeta {
            max_sequence: day as u64,
            min_sequence: 0,
            num_rows: 1,
            size,
            time_range: TimeRange::new(start.into(), (start + 1_000).into()),
            format: FileFormat::Tsf,
        }
    }

    struct Fixture {
        _root: temp_dir::TempDir,
        runtime: Arc<tokio::runtime::Runtime>,
        store: ObjectStoreRef,
        manifest: ManifestRef,
        path_gen: Arc<SstPathGenerator>,
    }

    fn fixture() -> Fixture {
        let root = temp_dir::TempDir::new().unwrap();
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let root_dir = root.path().to_string_lossy().to_string();
        let manifest = runtime
            .block_on(Manifest::try_new(
                root_dir.clone(),
                store.clone(),
                runtime.clone(),
                ManifestConfig::default(),
            ))
            .unwrap();
        Fixture {
            _root: root,
            runtime,
            store,
            manifest: Arc::new(manifest),
            path_gen: Arc::new(SstPathGenerator::new(root_dir)),
        }
    }

    /// Writes a TSF object and registers it in the manifest.
    async fn add_tsf_file(fx: &Fixture, id: FileId, day: i64, bytes: &[u8]) {
        let meta = file_meta(day, bytes.len() as u32);
        let path = fx.path_gen.generate_for(id, &meta);
        fx.store
            .put(&Path::from(path), bytes.to_vec().into())
            .await
            .unwrap();
        fx.manifest.add_file(id, meta).await.unwrap();
    }

    #[test]
    fn test_list_partitions_groups_by_day() {
        let fx = fixture();
        fx.runtime.block_on(async {
            add_tsf_file(&fx, 1, 100, &[0u8; 10]).await;
            add_tsf_file(&fx, 2, 100, &[0u8; 20]).await;
            add_tsf_file(&fx, 3, 101, &[0u8; 5]).await;

            // A legacy Parquet object sits directly in data/ and must not be
            // reported as a partition.
            let legacy = fx.path_gen.generate(4);
            fx.store
                .put(&Path::from(legacy), vec![0u8; 3].into())
                .await
                .unwrap();

            let partitions =
                PartitionManager::new(fx.store.clone(), fx.manifest.clone(), fx.path_gen.clone())
                    .list_partitions()
                    .await
                    .unwrap();

            assert_eq!(partitions.len(), 2);
            assert_eq!(partitions[0].index, 100);
            assert_eq!(partitions[0].label, label_of(100));
            assert_eq!(partitions[0].objects, 2);
            assert_eq!(partitions[0].bytes, 30);
            assert_eq!(partitions[1].index, 101);
            assert_eq!(partitions[1].objects, 1);
        });
    }

    #[test]
    fn test_drop_expired_removes_manifest_first_then_objects() {
        let fx = fixture();
        fx.runtime.block_on(async {
            add_tsf_file(&fx, 1, 100, &[1u8; 10]).await;
            add_tsf_file(&fx, 2, 101, &[2u8; 10]).await;
            add_tsf_file(&fx, 3, 300, &[3u8; 10]).await;
            let manager =
                PartitionManager::new(fx.store.clone(), fx.manifest.clone(), fx.path_gen.clone());

            // Keep the last 50 days; day 300 is "now".
            let now = 300 * DAY_MS + 5_000;
            let removed = manager.drop_expired(now, 50 * DAY_MS).await.unwrap();
            assert_eq!(removed.len(), 2);

            let live = fx.manifest.all_ssts().await;
            assert_eq!(live.len(), 1);
            assert_eq!(live[0].id(), 3);

            // The dropped partitions are gone from storage too.
            let partitions = manager.list_partitions().await.unwrap();
            assert_eq!(partitions.len(), 1);
            assert_eq!(partitions[0].index, 300);
        });
    }

    #[test]
    fn test_gc_orphans_keeps_live_files_and_bytes() {
        let fx = fixture();
        fx.runtime.block_on(async {
            add_tsf_file(&fx, 1, 100, &[0u8; 10]).await;
            add_tsf_file(&fx, 2, 100, &[0u8; 20]).await;

            // An object in a live partition that the manifest does not list:
            // written by a compaction that crashed before committing.
            let orphan = fx.path_gen.generate_tsf(99, 100 * DAY_MS + 1_000);
            fx.store
                .put(&Path::from(orphan.clone()), vec![0u8; 7].into())
                .await
                .unwrap();
            // An orphan in a partition that has no live files at all.
            let stale = fx.path_gen.generate_tsf(98, 50 * DAY_MS + 1_000);
            fx.store
                .put(&Path::from(stale.clone()), vec![0u8; 3].into())
                .await
                .unwrap();

            let manager =
                PartitionManager::new(fx.store.clone(), fx.manifest.clone(), fx.path_gen.clone());
            let report = manager.gc_orphans().await.unwrap();
            assert_eq!(report.orphan_files, 2);
            assert_eq!(report.orphan_bytes, 10);
            assert_eq!(report.retained_files, 2);
            assert_eq!(report.failed_deletes, 0);
            assert!(report.made_progress());

            // Idempotent.
            let again = manager.gc_orphans().await.unwrap();
            assert_eq!(
                again,
                GcReport {
                    retained_files: 2,
                    ..Default::default()
                }
            );

            // Live objects survived.
            for id in [1u64, 2] {
                let path = fx.path_gen.generate_for(id, &file_meta(100, 10));
                assert!(fx.store.get(&Path::from(path)).await.is_ok());
            }
            assert!(fx.store.get(&Path::from(orphan)).await.is_err());
        });
    }

    #[test]
    fn test_gc_never_deletes_uncommitted_parquet_of_live_file() {
        // A Parquet file listed in the manifest must survive GC even though it
        // lives in data/ next to the day directories.
        let fx = fixture();
        fx.runtime.block_on(async {
            let meta = FileMeta {
                format: FileFormat::Parquet,
                ..file_meta(100, 5)
            };
            let path = fx.path_gen.generate_for(7, &meta);
            fx.store
                .put(&Path::from(path.clone()), vec![0u8; 5].into())
                .await
                .unwrap();
            fx.manifest.add_file(7, meta).await.unwrap();

            let report =
                PartitionManager::new(fx.store.clone(), fx.manifest.clone(), fx.path_gen.clone())
                    .gc_orphans()
                    .await
                    .unwrap();
            assert_eq!(report.orphan_files, 0);
            assert_eq!(report.retained_files, 1);
            assert!(fx.store.get(&Path::from(path)).await.is_ok());
        });
    }

    /// Orphan GC must also collect files from a partition directory that the
    /// TTL drop already emptied.
    #[test]
    fn test_gc_cleans_empty_partition_directories() {
        let fx = fixture();
        fx.runtime.block_on(async {
            let id = SstFile::allocate_id();
            let stale = fx.path_gen.generate_tsf(id, 42 * DAY_MS + 1_000);
            fx.store
                .put(&Path::from(stale.clone()), vec![0u8; 4].into())
                .await
                .unwrap();

            let manager =
                PartitionManager::new(fx.store.clone(), fx.manifest.clone(), fx.path_gen.clone());
            let report = manager.gc_orphans().await.unwrap();
            assert_eq!(report.orphan_files, 1);
            assert!(fx.store.get(&Path::from(stale)).await.is_err());
            assert!(manager.list_partitions().await.unwrap().is_empty());
        });
    }
}
