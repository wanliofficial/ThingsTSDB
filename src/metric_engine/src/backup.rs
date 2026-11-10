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

//! Logical backup and restore of a metric engine (roadmap Phase 4).
//!
//! A backup copies every object under the engine root — the four columnar
//! tables (`metrics`, `series`, `index`, `data`) plus the month-partitioned
//! mergeset index (`indexdb`) — to a destination prefix in the same object
//! store. Restore is the reverse copy, after which a fresh `MetricEngine`
//! opened at the destination answers the same queries.
//!
//! The engine flushes first ([`crate::MetricEngine::flush_all`]), so the copy
//! captures a durable point: every buffered sample is an SST committed by the
//! manifest and every pending index row is in an indexdb part. **The copy is
//! not atomic with respect to concurrent writes or background compaction**: a
//! file deleted between `list` and `get` makes the copy fail rather than
//! silently producing a backup that references a missing SST. Run a backup
//! against a quiescent engine, or tolerate a retry.
//!
//! For the local object store the store *is* the filesystem, so the same copy
//! covers the on-disk WAL and the mergeset parts. A backup taken into a
//! different bucket (S3/OSS) copies the object-store tables; the mergeset
//! index is local and is rebuilt by re-writing if it is absent.

use std::path::Path as FsPath;

use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore};

use crate::Result;

/// What a backup or restore copied.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackupStats {
    /// Number of objects copied.
    pub objects: u64,
    /// Total bytes copied.
    pub bytes: u64,
}

/// Copies every object under `src_root` in `src` to `dest_root` in `dest`,
/// preserving the path below the root.
///
/// The engine root is a prefix, so `{src_root}/data/1.sst` lands at
/// `{dest_root}/data/1.sst`. An object that disappears between listing and
/// reading aborts the copy with an error (a half-backed backup is worse than a
/// failed one).
pub async fn copy_prefix(
    src: &dyn ObjectStore,
    src_root: &str,
    dest: &dyn ObjectStore,
    dest_root: &str,
) -> Result<BackupStats> {
    let src_root = Path::from(src_root);
    let dest_root = Path::from(dest_root);
    if src_root == dest_root {
        return Err(anyhow::anyhow!(
            "backup source and destination are the same prefix: {src_root}"
        )
        .into());
    }

    let mut stats = BackupStats::default();
    let mut stream = src.list(Some(&src_root));
    while let Some(meta) = stream
        .try_next()
        .await
        .map_err(|e| anyhow::anyhow!("list backup source {src_root}: {e}"))?
    {
        // Rebuild the destination path from the parts that follow the root,
        // so a nested table directory keeps its structure.
        let rel: Vec<String> = meta
            .location
            .prefix_match(&src_root)
            .ok_or_else(|| {
                anyhow::anyhow!("listed object {} is outside {src_root}", meta.location)
            })?
            .map(|part| part.as_ref().to_string())
            .collect();
        let mut dest_path = dest_root.clone();
        for part in &rel {
            dest_path = dest_path.child(part.as_str());
        }

        let bytes = src
            .get(&meta.location)
            .await
            .map_err(|e| anyhow::anyhow!("read {}: {e}", meta.location))?
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("read body of {}: {e}", meta.location))?;
        dest.put(&dest_path, bytes.into())
            .await
            .map_err(|e| anyhow::anyhow!("write {dest_path}: {e}"))?;

        stats.objects += 1;
        stats.bytes += meta.size as u64;
    }
    Ok(stats)
}

/// Recursively copies a local directory tree, used for the mergeset index and
/// the WAL when a backup crosses object stores. Mirrors directories, so an
/// empty table directory survives the round trip.
pub fn copy_dir_all(src: &FsPath, dest: &FsPath) -> Result<BackupStats> {
    let mut stats = BackupStats::default();
    copy_dir_into(src, dest, &mut stats)?;
    Ok(stats)
}

fn copy_dir_into(src: &FsPath, dest: &FsPath, stats: &mut BackupStats) -> Result<()> {
    if !src.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(dest)
        .map_err(|e| anyhow::anyhow!("create backup dir {dest:?}: {e}"))?;
    for entry in
        std::fs::read_dir(src).map_err(|e| anyhow::anyhow!("read backup dir {src:?}: {e}"))?
    {
        let entry = entry.map_err(|e| anyhow::anyhow!("read dir entry in {src:?}: {e}"))?;
        let file_type = entry
            .file_type()
            .map_err(|e| anyhow::anyhow!("stat {:?}: {e}", entry.path()))?;
        let target = dest.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_into(&entry.path(), &target, stats)?;
        } else if file_type.is_file() {
            let copied = std::fs::copy(entry.path(), &target)
                .map_err(|e| anyhow::anyhow!("copy {:?} -> {target:?}: {e}", entry.path()))?;
            stats.objects += 1;
            stats.bytes += copied;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{local::LocalFileSystem, ObjectStore};

    use super::*;

    fn local() -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_prefix_preserves_layout() {
        let src_root = temp_dir::TempDir::new().unwrap();
        let dest_root = temp_dir::TempDir::new().unwrap();
        let store = local();
        let src = src_root.path().to_string_lossy().to_string();
        let dest = dest_root.path().to_string_lossy().to_string();

        store
            .put(
                &Path::from(format!("{src}/data/1.sst")),
                vec![1u8, 2, 3].into(),
            )
            .await
            .unwrap();
        store
            .put(
                &Path::from(format!("{src}/manifest/snapshot")),
                vec![9u8].into(),
            )
            .await
            .unwrap();

        let stats = copy_prefix(store.as_ref(), &src, store.as_ref(), &dest)
            .await
            .unwrap();
        assert_eq!(stats.objects, 2);
        assert_eq!(stats.bytes, 4);

        // The copied bytes and relative layout match the source.
        let got = store
            .get(&Path::from(format!("{dest}/data/1.sst")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(got.as_ref(), &[1u8, 2, 3]);
        let snapshot = store
            .get(&Path::from(format!("{dest}/manifest/snapshot")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(snapshot.as_ref(), &[9u8]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_prefix_rejects_same_source_and_dest() {
        let store = local();
        let root = temp_dir::TempDir::new().unwrap();
        let root = root.path().to_string_lossy().to_string();
        let err = copy_prefix(store.as_ref(), &root, store.as_ref(), &root)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("same prefix"), "{err}");
    }

    #[test]
    fn copy_dir_all_copies_nested_files() {
        let src = temp_dir::TempDir::new().unwrap();
        let dest = temp_dir::TempDir::new().unwrap();
        std::fs::create_dir_all(src.path().join("indexdb/2026_11")).unwrap();
        std::fs::write(src.path().join("indexdb/2026_11/part-1"), b"abc").unwrap();
        std::fs::write(src.path().join("indexdb/top"), b"xy").unwrap();

        let stats = copy_dir_all(src.path(), dest.path()).unwrap();
        assert_eq!(stats.objects, 2);
        assert_eq!(stats.bytes, 5);
        assert_eq!(
            std::fs::read(dest.path().join("indexdb/2026_11/part-1")).unwrap(),
            b"abc"
        );
        assert_eq!(
            std::fs::read(dest.path().join("indexdb/top")).unwrap(),
            b"xy"
        );
    }
}
