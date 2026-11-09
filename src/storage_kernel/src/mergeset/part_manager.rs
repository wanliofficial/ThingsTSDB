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
// specific language governing permissions and
// limitations under the License.

//! Background part manager (ADR-0001 §3.6).
//!
//! Drives the two steady-state loops of an indexDB:
//!
//! - **Flush loop** (`DATA_FLUSH_INTERVAL`): pending items become a searchable
//!   in-memory part.
//! - **Merge loop**: the [`merge_policy`] selects a window of file parts and
//!   merges them into one, removing the sources on success.
//!
//! Both loops are bounded by a stop channel; an interrupted merge leaves its
//! sources untouched. The intervals are injected so the acceptance tests
//! (crash/restart, write amplification) can drive the loops deterministically
//! without sleeping for the real 1s/5s.

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

use crate::mergeset::{
    merge_policy::{select_parts_to_merge, MergeCandidate, Selection},
    part::{Part, PartWriter},
    table::Table,
    table_search::TableSearch,
};

/// Tunables for the part manager. The ADR fixes the defaults (§3.6).
#[derive(Debug, Clone)]
pub struct ManagerConfig {
    /// `DATA_FLUSH_INTERVAL`: how often pending items become a file part.
    pub flush_interval: std::time::Duration,
    /// How often the merge loop considers a merge.
    pub merge_interval: std::time::Duration,
    /// zstd level for merged output when the source item count is small.
    pub small_part_compress_level: i32,
    /// The item count above which a part bypasses the page cache.
    pub cached_part_threshold: u64,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            flush_interval: std::time::Duration::from_secs(5),
            merge_interval: std::time::Duration::from_secs(1),
            small_part_compress_level: -5,
            cached_part_threshold: 1_000_000,
        }
    }
}

/// Outcome of one flush cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Number of items flushed into the new part.
    pub items_flushed: usize,
    /// Number of file parts after the flush.
    pub file_parts: usize,
    /// Whether a part was actually written.
    pub flushed: bool,
}

/// Outcome of one merge cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeOutcome {
    /// The part ids that were merged, when a merge ran.
    pub merged_ids: Vec<usize>,
    /// Bytes written to the output part.
    pub output_bytes: u64,
    /// Whether a merge ran.
    pub merged: bool,
}

/// Drives the flush and merge loops for one indexDB.
pub struct PartManager {
    table: Arc<Mutex<Table>>,
    dir: PathBuf,
    config: ManagerConfig,
    next_part_id: Arc<Mutex<u64>>,
    stop: Arc<AtomicBool>,
    file_parts: Arc<Mutex<Vec<Part>>>,
}

impl PartManager {
    pub fn new(table: Arc<Mutex<Table>>, dir: impl AsRef<Path>, config: ManagerConfig) -> Self {
        Self {
            table,
            dir: dir.as_ref().to_path_buf(),
            config,
            next_part_id: Arc::new(Mutex::new(0)),
            stop: Arc::new(AtomicBool::new(false)),
            file_parts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Allocates the next zero-padded part directory name.
    fn alloc_part_path(&self, tag: &str) -> PathBuf {
        let mut id = self.next_part_id.lock().unwrap_or_else(|e| e.into_inner());
        *id += 1;
        self.dir.join(format!("{tag}-{:016X}", *id))
    }

    /// Registers a freshly written file part with the manager.
    pub fn register_part(&self, part: Part) {
        self.file_parts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(part);
    }

    /// Rebuilds the file-part set from `parts.json` after a restart.
    ///
    /// Every part listed is reopened and validated; a missing or corrupt part
    /// directory is skipped (its `metadata.json` makes an unfinished part
    /// unreadable, so the merge that was interrupted left no orphan rows). The
    /// parts are then re-registered in first-item order.
    pub fn recover_from_disk(&self) -> Result<usize, String> {
        let names = crate::mergeset::part::read_parts_json(&self.dir)?;
        let mut recovered = 0;
        for name in names {
            let path = self.dir.join(&name);
            match Part::open(&path) {
                Ok(part) => {
                    self.register_part(part);
                    recovered += 1;
                }
                Err(_) => {
                    // An unfinished or corrupt part from an interrupted merge
                    // is not loaded; it is not referenced by any data row.
                }
            }
        }
        Ok(recovered)
    }

    /// Returns the current file parts, sorted by first item.
    pub fn file_parts(&self) -> Vec<Part> {
        let mut parts = self
            .file_parts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        parts.sort_by(|a, b| a.first_item().cmp(b.first_item()));
        parts
    }

    /// Runs one flush cycle: writes pending items to a file part.
    pub fn flush_once(&self) -> Result<FlushOutcome, String> {
        let pending = self
            .table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take_pending();
        if pending.is_empty() {
            return Ok(FlushOutcome {
                items_flushed: 0,
                file_parts: self
                    .file_parts
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .len(),
                flushed: false,
            });
        }
        let level = if (pending.len() as u64) < self.config.cached_part_threshold {
            self.config.small_part_compress_level
        } else {
            3
        };
        let out = self.alloc_part_path("part");
        let part = {
            let mut writer = PartWriter::new(level);
            for item in &pending {
                writer.add(item)?;
            }
            writer.finish(&out)?
        };
        let count = pending.len();
        self.register_part(part);
        {
            let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
            table.clear_pending();
        }
        Ok(FlushOutcome {
            items_flushed: count,
            file_parts: self
                .file_parts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            flushed: true,
        })
    }

    /// Runs one merge cycle: selects and merges a window of file parts.
    pub fn merge_once(&self) -> Result<MergeOutcome, String> {
        let parts = self.file_parts();
        if parts.len() < 8 {
            return Ok(MergeOutcome {
                merged_ids: Vec::new(),
                output_bytes: 0,
                merged: false,
            });
        }
        let candidates: Vec<MergeCandidate> = parts
            .iter()
            .map(|p| MergeCandidate {
                id: part_identity(p),
                size: p.size,
            })
            .collect();
        // Candidates must be sorted by ascending size for the policy.
        let mut sorted_idx: Vec<usize> = (0..candidates.len()).collect();
        sorted_idx.sort_by_key(|&i| candidates[i].size);
        let sorted: Vec<MergeCandidate> = sorted_idx.iter().map(|&i| candidates[i]).collect();
        let Selection::Merge(window) = select_parts_to_merge(&sorted) else {
            return Ok(MergeOutcome {
                merged_ids: Vec::new(),
                output_bytes: 0,
                merged: false,
            });
        };
        // Map selected ids back to their parts.
        let selected: Vec<Part> = window
            .iter()
            .filter_map(|c| parts.iter().find(|p| part_identity(p) == c.id).cloned())
            .collect();
        if selected.len() < 2 {
            return Ok(MergeOutcome {
                merged_ids: Vec::new(),
                output_bytes: 0,
                merged: false,
            });
        }
        let out = self.alloc_part_path("merged");
        let merged = merge_parts(&selected, &out, self.config.small_part_compress_level)?;
        let output_bytes = merged.size;
        let merged_ids: Vec<usize> = selected.iter().map(part_identity).collect();
        // Remove the sources only after the output is fully written.
        {
            let mut file_parts = self.file_parts.lock().unwrap_or_else(|e| e.into_inner());
            file_parts.retain(|p| !selected.iter().any(|s| s.path() == p.path()));
            file_parts.push(merged);
        }
        Ok(MergeOutcome {
            merged_ids,
            output_bytes,
            merged: true,
        })
    }

    /// Signals the loops to stop after their current cycle.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Spawns the flush and merge loops on background threads. Returns the
    /// join handles; joining a loop ends when `stop()` is called.
    ///
    /// Takes `&Arc<Self>` so the loop closures hold a real shared handle to the
    /// manager rather than a stack reconstruction.
    pub fn spawn(self: &Arc<Self>) -> Vec<JoinHandle<()>> {
        let flush = spawn_loop(
            "indexdb-flush",
            self.config.flush_interval,
            self.stop.clone(),
            {
                let this = Arc::clone(self);
                move || this.flush_once().map(|_| ())
            },
        );
        let merge = spawn_loop(
            "indexdb-merge",
            self.config.merge_interval,
            self.stop.clone(),
            {
                let this = Arc::clone(self);
                move || this.merge_once().map(|_| ())
            },
        );
        vec![flush, merge]
    }
}

/// A cheap stable identity for a part: its on-disk path string hash.
fn part_identity(part: &Part) -> usize {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };
    let mut hasher = DefaultHasher::new();
    part.path().hash(&mut hasher);
    hasher.finish() as usize
}

/// Merges the given parts into one, streaming items through a k-way search.
fn merge_parts(sources: &[Part], out: &Path, compress_level: i32) -> Result<Part, String> {
    let mut writer = PartWriter::new(compress_level);
    let mut search = TableSearch::new(sources, None)?;
    while let Some(item) = search.next_item()? {
        writer.add(&item)?;
    }
    writer.finish(out)
}

/// Runs `work` every `interval` until `stop` is set. `work` returns an error
/// string on failure, which is dropped here (the loops are best-effort; a
/// failed cycle does not abort the index).
fn spawn_loop<F>(
    name: &str,
    interval: std::time::Duration,
    stop: Arc<AtomicBool>,
    work: F,
) -> JoinHandle<()>
where
    F: Fn() -> Result<(), String> + Send + 'static,
{
    let name = name.to_string();
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Err(err) = work() {
                    eprintln!("part-manager loop error: {err}");
                }
                // Sleep in small slices so stop is responsive.
                let mut slept = std::time::Duration::ZERO;
                while slept < interval && !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    slept += std::time::Duration::from_millis(10);
                }
            }
        })
        .expect("spawn part-manager loop")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
            let path = std::env::temp_dir().join(format!("mergeset-pm-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn make_items(n: usize) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("metric{{id={i:08}}}").into_bytes())
            .collect()
    }

    #[test]
    fn test_flush_moves_pending_to_file_part() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        {
            let mut t = table.lock().unwrap();
            for item in make_items(1_000) {
                t.add(&item);
            }
        }
        let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
        let outcome = pm.flush_once().unwrap();
        assert!(outcome.flushed);
        assert_eq!(outcome.items_flushed, 1_000);
        assert_eq!(outcome.file_parts, 1);
        // The table no longer holds the pending items.
        assert_eq!(table.lock().unwrap().pending_len(), 0);
        // A reopen sees the same items.
        let parts = pm.file_parts();
        let mut search = TableSearch::new(&parts, None).unwrap();
        let mut count = 0;
        while search.next_item().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1_000);
    }

    #[test]
    fn test_flush_noop_when_empty() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table, &dir.0, ManagerConfig::default());
        let outcome = pm.flush_once().unwrap();
        assert!(!outcome.flushed);
    }

    #[test]
    fn test_merge_combines_parts_and_removes_sources() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        // Seed nine file parts directly so the merge policy has a window.
        let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
        let total = 9 * 500;
        for part_no in 0..9u32 {
            let mut writer = PartWriter::new(1);
            for i in 0..500u32 {
                let id = part_no * 500 + i;
                writer
                    .add(format!("metric{{id={id:08}}}").as_bytes())
                    .unwrap();
            }
            let path = dir.0.join(format!("seed-{part_no}"));
            let part = writer.finish(path).unwrap();
            pm.register_part(part);
        }
        assert_eq!(pm.file_parts().len(), 9);
        let outcome = pm.merge_once().unwrap();
        assert!(outcome.merged, "merge policy should select a window");
        // Sources are removed, one output added.
        assert_eq!(pm.file_parts().len(), 9 - outcome.merged_ids.len() + 1);
        // All items survive the merge.
        let parts = pm.file_parts();
        let mut search = TableSearch::new(&parts, None).unwrap();
        let mut count = 0;
        while search.next_item().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, total);
    }

    #[test]
    fn test_merge_needs_enough_parts() {
        let dir = TempDir::new();
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table, &dir.0, ManagerConfig::default());
        // Only a few parts: below the merge window minimum.
        let outcome = pm.merge_once().unwrap();
        assert!(!outcome.merged);
    }

    #[test]
    fn test_crash_restart_recovers_written_parts() {
        let dir = TempDir::new();
        // First "process": write two parts and persist parts.json.
        {
            let table = Arc::new(Mutex::new(Table::new()));
            let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
            for item in make_items(500) {
                table.lock().unwrap().add(&item);
            }
            pm.flush_once().unwrap();
            for item in make_items(500).into_iter().map(|mut v| {
                v.extend_from_slice(b"-2");
                v
            }) {
                table.lock().unwrap().add(&item);
            }
            pm.flush_once().unwrap();
            // Persist the part list as the shutdown path would.
            let paths: Vec<PathBuf> = pm
                .file_parts()
                .iter()
                .map(|p| p.path().to_path_buf())
                .collect();
            let refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
            crate::mergeset::part::write_parts_json(&dir.0, &refs).unwrap();
            assert_eq!(pm.file_parts().len(), 2);
        }
        // Second "process": a fresh manager recovers from parts.json.
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table, &dir.0, ManagerConfig::default());
        let recovered = pm.recover_from_disk().unwrap();
        assert_eq!(recovered, 2);
        let parts = pm.file_parts();
        assert_eq!(parts.len(), 2);
        // The recovered parts answer the full item set.
        let mut search = TableSearch::new(&parts, None).unwrap();
        let mut count = 0;
        while search.next_item().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1_000);
    }

    #[test]
    fn test_recovery_skips_unfinished_part() {
        let dir = TempDir::new();
        // Write a part, then truncate its metadata so it cannot be opened.
        let table = Arc::new(Mutex::new(Table::new()));
        let pm = PartManager::new(table.clone(), &dir.0, ManagerConfig::default());
        for item in make_items(100) {
            table.lock().unwrap().add(&item);
        }
        pm.flush_once().unwrap();
        let parts = pm.file_parts();
        let bad = parts[0].path().to_path_buf();
        let _ = std::fs::remove_file(bad.join("metadata.json"));
        let refs: Vec<&Path> = vec![parts[0].path()];
        crate::mergeset::part::write_parts_json(&dir.0, &refs).unwrap();

        let pm2 = PartManager::new(
            Arc::new(Mutex::new(Table::new())),
            &dir.0,
            ManagerConfig::default(),
        );
        // The corrupt part is skipped, not loaded.
        assert_eq!(pm2.recover_from_disk().unwrap(), 0);
        assert!(pm2.file_parts().is_empty());
    }
}
