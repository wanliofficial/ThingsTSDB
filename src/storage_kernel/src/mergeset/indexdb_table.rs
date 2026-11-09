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

//! One indexDB: the mergeset table holding the namespace rows of a month
//! partition, plus the query surface the metric engine needs (ADR-0001 §4, §7).
//!
//! ```text
//! {root}/indexdb/{YYYY_MM}/parts.json     mergeset part list
//! {root}/indexdb/{YYYY_MM}/wal/           index WAL segments
//! ```
//!
//! Writes go through [`PartManager::write_index_items`], so they are durable in
//! the index WAL before they become searchable (ADR-0001 §6.1). Reads walk the
//! namespace prefixes of §4 and never touch a namespace they do not need.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::mergeset::{
    indexdb::{
        self, canonical_name, date_metric_name_row, date_series_prefix, date_series_rows,
        date_tag_posting_row, decode_canonical_name, decode_date_series_row,
        decode_series_id_name_row, group_posting_row_prefix, metric_name_row, posting_row_prefix,
        series_id_name_row, series_id_row, tag_key_prefix, Tsid,
    },
    part_manager::{ManagerConfig, MergeStats, PartManager},
    series_id_set::SeriesIdSet,
    table::Table,
    tag_filter::{FilterCache, IndexResolver, QueryPlan, RowSource},
    timeutil,
    wal::WalWriter,
};

/// Drop-in configuration for one month partition.
#[derive(Debug, Clone)]
pub struct IndexDbConfig {
    /// Flush/merge tuning and WAL durability.
    pub manager: ManagerConfig,
    /// Start the background flush and merge loops on open.
    pub spawn_workers: bool,
}

impl Default for IndexDbConfig {
    fn default() -> Self {
        Self {
            manager: ManagerConfig::default(),
            spawn_workers: true,
        }
    }
}

/// A month-partitioned indexDB over a mergeset [`Table`].
pub struct IndexDb {
    dir: PathBuf,
    partition: u16,
    table: Arc<Mutex<Table>>,
    manager: Arc<PartManager>,
    cache: FilterCache,
}

impl Drop for IndexDb {
    fn drop(&mut self) {
        self.manager.stop();
    }
}

impl IndexDb {
    /// Opens (creating if needed) the indexDB of `partition` at `dir`.
    ///
    /// Recovery order is the one fixed by ADR-0001 §6.4: the persisted parts
    /// first, then the index WAL frames they do not cover. Replayed frames land
    /// in the pending buffer and are flushed after `flush_interval`.
    pub fn open(
        dir: impl AsRef<Path>,
        partition: u16,
        config: IndexDbConfig,
    ) -> Result<Self, String> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| format!("cannot create {dir:?}: {e}"))?;
        let table = Arc::new(Mutex::new(Table::new()));
        let wal_dir = dir.join("wal");
        let wal = WalWriter::open(
            &wal_dir,
            u64::from(partition),
            config.manager.wal_sync_on_write,
        )?;
        let manager = Arc::new(
            PartManager::new(Arc::clone(&table), &dir, config.manager)
                .with_wal(wal, wal_dir, u64::from(partition))
                .with_row_merger(Arc::new(indexdb::collapse_posting_items)),
        );
        manager.recover_from_disk()?;
        manager.replay_wal()?;
        if config.spawn_workers {
            // The loops hold their own `Arc`, so the handles themselves are
            // fire-and-forget; `stop()` ends them.
            std::mem::forget(manager.spawn());
        }
        Ok(Self {
            dir,
            partition,
            table,
            manager,
            cache: FilterCache::default(),
        })
    }

    /// The month partition this indexDB serves (`months since 1970-01`).
    pub fn partition(&self) -> u16 {
        self.partition
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Shared write-amplification and WAL counters.
    pub fn stats(&self) -> Arc<MergeStats> {
        self.manager.stats()
    }

    /// The posting-set cache shared by every query against this partition.
    pub fn cache(&self) -> &FilterCache {
        &self.cache
    }

    /// Registers one series and its data days (ADR-0001 §4, every namespace).
    ///
    /// Rows written:
    ///
    /// - ns0/n077 name lookup (`always`, §4 note), so ingestion can find the
    ///   TSID without a per-day scan;
    /// - ns2 `seriesID -> TSID` and ns3 `seriesID -> canonical name`;
    /// - ns1 global postings, one row per tag pair plus the metric-group row;
    /// - ns5/ns6/ns7 per-day rows for every day in `days`.
    ///
    /// Returns the number of rows handed to the part manager.
    pub fn write_series(
        &self,
        series_id: u64,
        group: &[u8],
        tags: &[(&[u8], &[u8])],
        days: &[u64],
    ) -> Result<usize, String> {
        let mut items = Vec::new();
        let tsid = Tsid::build(group, tags).with_series_id(series_id);
        let name = canonical_name(group, tags);
        items.push(metric_name_row(&name, &tsid));
        items.push(series_id_row(series_id, &tsid));
        items.push(series_id_name_row(series_id, &name));
        // Group row: `tagKey=""`, `tagValue=group`.
        items.push(indexdb::tag_posting_row(b"", group, &[series_id]));
        for (key, value) in tags {
            let composite = indexdb::composite_key(group, key);
            items.push(indexdb::tag_posting_row(key, value, &[series_id]));
            items.push(indexdb::tag_posting_row(&composite, value, &[series_id]));
        }
        for &day in days {
            items.push(date_metric_name_row(day, &name, &tsid));
            items.extend(date_series_rows(day, &[series_id]));
            items.push(date_tag_posting_row(day, b"", group, &[series_id]));
            for (key, value) in tags {
                items.push(date_tag_posting_row(day, key, value, &[series_id]));
            }
        }
        let count = items.len();
        self.manager.write_index_items(items)?;
        // The rows are searchable right away (pending buffer included), so the
        // resolved sets of the previous generation are now incomplete.
        self.cache.invalidate();
        Ok(count)
    }

    /// Makes the pending rows searchable immediately (tests, read-your-writes).
    pub fn flush(&self) -> Result<usize, String> {
        Ok(self.manager.flush_once()?.items_flushed)
    }

    /// Runs one merge cycle and reports whether a merge happened.
    ///
    /// Background loops do this on their own; tests and benchmarks that disable
    /// the workers call it directly to reach the merged steady state.
    pub fn merge_once(&self) -> Result<bool, String> {
        Ok(self.manager.merge_once()?.merged)
    }

    /// Resolves a query plan to the matching series IDs (ADR-0001 §5, §7).
    ///
    /// `days` restricts the per-day index; an empty slice means "no time
    /// constraint" and skips the ns5 intersection.
    pub fn search_series_ids(&self, plan: &QueryPlan, days: &[u64]) -> Result<SeriesIdSet, String> {
        let table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        IndexResolver::new(&TableRows(&table))
            .with_cache(&self.cache)
            .resolve(plan, days)
    }

    /// Alias matching the ADR's `indexdb.search_tsids` name.
    pub fn search_tsids(&self, plan: &QueryPlan, days: &[u64]) -> Result<SeriesIdSet, String> {
        self.search_series_ids(plan, days)
    }

    /// Returns the canonical names of `ids` (ns3), used for label
    /// reconstruction (ADR-0001 §7).
    ///
    /// A small result set is fetched with one seek per series; a large one
    /// switches to a single ns3 range scan, because 10M seeks cost more than
    /// one sequential pass over the namespace.
    pub fn series_names(&self, ids: &[u64]) -> Result<HashMap<u64, indexdb::SeriesName>, String> {
        const SEEK_LIMIT: usize = 64;
        let table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = HashMap::with_capacity(ids.len());
        if ids.len() <= SEEK_LIMIT {
            for &id in ids {
                for row in table.search_prefix(&indexdb::series_id_prefix(id))? {
                    if let Some((found, name)) = decode_series_id_name_row(&row) {
                        if found == id {
                            out.insert(id, decode_name(&name));
                        }
                    }
                }
            }
            return Ok(out);
        }
        let mut want: Vec<u64> = ids.to_vec();
        want.sort_unstable();
        want.dedup();
        let start = [indexdb::ns::METRIC_NAME_BY_SERIES_ID];
        let end = [indexdb::ns::METRIC_NAME_BY_SERIES_ID + 1];
        for row in table.search_range(&start, &end)? {
            let Some((id, name)) = decode_series_id_name_row(&row) else {
                continue;
            };
            if want.binary_search(&id).is_ok() {
                out.insert(id, decode_name(&name));
            }
        }
        Ok(out)
    }

    /// Looks a series up by its identity (ns0), returning the encoded series
    /// ID.
    ///
    /// Ingestion uses this to avoid allocating a second ID for a series that
    /// already exists in the partition.
    pub fn lookup_series_id(
        &self,
        group: &[u8],
        tags: &[(&[u8], &[u8])],
    ) -> Result<Option<u64>, String> {
        // The ns0 row is `[ns] + canonical name + 8B TSID length + TSID(24B)`.
        // Seeking the full canonical name matches exactly this tag set.
        let mut prefix = vec![indexdb::ns::METRIC_NAME];
        prefix.extend_from_slice(&canonical_name(group, tags));
        let table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        for row in table.search_prefix(&prefix)? {
            let rest = &row[prefix.len()..];
            // `append_name_tsid` writes the 8-byte TSID length after the name,
            // so the remaining bytes are 8 + 24 on a well-formed row.
            if rest.len() == 8 + indexdb::TSID_LEN {
                let mut tsid = [0u8; indexdb::TSID_LEN];
                tsid.copy_from_slice(&rest[8..]);
                if let Ok(tsid) = Tsid::unmarshal(&tsid) {
                    return Ok(Some(tsid.series_id()));
                }
            }
        }
        Ok(None)
    }

    /// Number of pending (not yet flushed) rows.
    pub fn pending_len(&self) -> usize {
        self.table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending_len()
    }
}

/// Row lookups over a live [`Table`].
struct TableRows<'a>(&'a Table);

impl RowSource for TableRows<'_> {
    fn search_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.0.search_prefix(prefix)
    }

    fn search_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.0.search_range(start, end)
    }
}

fn decode_name(name: &[u8]) -> indexdb::SeriesName {
    let (group, mut tags) = decode_canonical_name(name).unwrap_or_default();
    // The canonical encoding already sorts, but normalise anyway so callers get
    // a stable label order regardless of how the name was produced.
    tags.sort();
    (group, tags)
}

/// The month partition and directory of a timestamp under `root`.
pub fn month_dir(root: &Path, ms: i64) -> (u16, PathBuf) {
    let month = timeutil::month_of_millis(ms);
    (month, root.join(timeutil::month_label(month)))
}

/// The `[prefix][value]` posting row prefix of a tag pair, re-exported so
/// callers do not have to reach into `indexdb`.
pub fn tag_posting_prefix(key: &[u8], value: &[u8]) -> Vec<u8> {
    posting_row_prefix(key, value)
}

/// The metric-group posting row prefix used for a name-only query.
pub fn group_posting_prefix(group: &[u8]) -> Vec<u8> {
    group_posting_row_prefix(group)
}

/// The ns1 prefix of one tag key, used by prefix/regex filters.
pub fn tag_prefix(key: &[u8]) -> Vec<u8> {
    tag_key_prefix(key)
}

/// The ns5 prefix of one day.
pub fn day_series_prefix(day: u64) -> Vec<u8> {
    date_series_prefix(day)
}

/// Decodes a ns5 row, exposed for callers building their own day sets.
pub fn decode_day_row(row: &[u8]) -> Option<(u64, u64)> {
    decode_date_series_row(row)
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;
    use crate::mergeset::{
        indexdb::series_id_row,
        tag_filter::{simplify_regex, RegexSimplification, TagMatcher},
    };

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("storage-kernel-indexdb-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn db() -> (TempDir, IndexDb) {
        let root = TempDir::new();
        let cfg = IndexDbConfig {
            spawn_workers: false,
            ..Default::default()
        };
        let db = IndexDb::open(root.path(), 655, cfg).unwrap();
        (root, db)
    }

    fn plan(matchers: &[TagMatcher]) -> QueryPlan {
        QueryPlan::from_matchers(matchers)
    }

    fn eq(key: &str, value: &str) -> TagMatcher {
        TagMatcher::Equal {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_write_then_resolve_eq_neq() {
        let (_root, db) = db();
        // s1 = m{code=200,job=api}, s2 = m{code=404,job=api},
        // s3 = m{code=200,job=db}
        db.write_series(1, b"m", &[(b"code", b"200"), (b"job", b"api")], &[100, 101])
            .unwrap();
        db.write_series(2, b"m", &[(b"code", b"404"), (b"job", b"api")], &[100])
            .unwrap();
        db.write_series(3, b"m", &[(b"code", b"200"), (b"job", b"db")], &[101])
            .unwrap();

        assert_eq!(
            db.search_series_ids(&plan(&[eq("code", "200")]), &[100, 101])
                .unwrap()
                .ids(),
            &[1, 3]
        );
        // Day pruning: s3 only has data on day 101.
        assert_eq!(
            db.search_series_ids(&plan(&[eq("code", "200")]), &[100])
                .unwrap()
                .ids(),
            &[1]
        );
        // Intersection.
        assert_eq!(
            db.search_series_ids(&plan(&[eq("code", "200"), eq("job", "db")]), &[100, 101])
                .unwrap()
                .ids(),
            &[3]
        );
        // Negative filter subtracts from the group.
        assert_eq!(
            db.search_series_ids(
                &plan(&[TagMatcher::NotEqual {
                    key: b"code".to_vec(),
                    value: b"200".to_vec(),
                }]),
                &[100, 101]
            )
            .unwrap()
            .ids(),
            &[2]
        );
        // Name-only query uses the metric-group posting row.
        assert_eq!(
            db.search_series_ids(&plan(&[]), &[100, 101]).unwrap().ids(),
            &[1, 2, 3]
        );
        // Unknown tag value matches nothing.
        assert!(db
            .search_series_ids(&plan(&[eq("code", "500")]), &[100, 101])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_regex_and_prefix_filters() {
        let (_root, db) = db();
        db.write_series(1, b"m", &[(b"job", b"api")], &[10])
            .unwrap();
        db.write_series(2, b"m", &[(b"job", b"api2")], &[10])
            .unwrap();
        db.write_series(3, b"m", &[(b"job", b"web")], &[10])
            .unwrap();

        // `api.*` simplifies to a prefix scan.
        assert_eq!(
            simplify_regex(b"api.*"),
            RegexSimplification::Prefix(b"api".to_vec())
        );
        let re = TagMatcher::Re {
            key: b"job".to_vec(),
            regex: b"api.*".to_vec(),
        };
        assert_eq!(
            db.search_series_ids(&plan(&[re]), &[10]).unwrap().ids(),
            &[1, 2]
        );
        // An alternation unions its exact branches.
        let re = TagMatcher::Re {
            key: b"job".to_vec(),
            regex: b"web|api".to_vec(),
        };
        assert_eq!(
            db.search_series_ids(&plan(&[re]), &[10]).unwrap().ids(),
            &[1, 3]
        );
        // A general regex falls back to the engine over the tag-key rows.
        let re = TagMatcher::Re {
            key: b"job".to_vec(),
            regex: b"a.i[0-9]".to_vec(),
        };
        assert_eq!(
            db.search_series_ids(&plan(&[re]), &[10]).unwrap().ids(),
            &[2]
        );
        // `!~` subtracts the matching values.
        let nre = TagMatcher::Nre {
            key: b"job".to_vec(),
            regex: b"api.*".to_vec(),
        };
        assert_eq!(
            db.search_series_ids(&plan(&[nre]), &[10]).unwrap().ids(),
            &[3]
        );
    }

    #[test]
    fn test_series_names_and_lookup() {
        let (_root, db) = db();
        db.write_series(7, b"m", &[(b"job", b"api"), (b"zone", b"a")], &[10])
            .unwrap();
        let names = db.series_names(&[7]).unwrap();
        let (group, tags) = names.get(&7).expect("series name");
        assert_eq!(group, b"m");
        assert_eq!(
            tags,
            &vec![
                (b"job".to_vec(), b"api".to_vec()),
                (b"zone".to_vec(), b"a".to_vec())
            ]
        );
        assert_eq!(
            db.lookup_series_id(b"m", &[(b"job", b"api"), (b"zone", b"a")])
                .unwrap(),
            Some(7)
        );
        assert_eq!(db.lookup_series_id(b"m", &[(b"job", b"db")]).unwrap(), None);
        // A large request switches to the ns3 range scan and returns the same
        // map.
        let many: Vec<u64> = (0..100).chain(std::iter::once(7)).collect();
        assert_eq!(db.series_names(&many).unwrap().len(), 1);
    }

    #[test]
    fn test_reopen_recovers_rows_from_parts_and_wal() {
        let root = TempDir::new();
        let cfg = || IndexDbConfig {
            spawn_workers: false,
            ..Default::default()
        };
        {
            let db = IndexDb::open(root.path(), 655, cfg()).unwrap();
            // Series 1 becomes a part; series 2 only lives in the WAL when the
            // process stops.
            db.write_series(1, b"m", &[(b"job", b"api")], &[10])
                .unwrap();
            let pending = db.pending_len();
            assert!(pending > 0);
            assert_eq!(db.flush().unwrap(), pending);
            assert_eq!(db.pending_len(), 0);
            db.write_series(2, b"m", &[(b"job", b"api")], &[11])
                .unwrap();
            assert!(db.pending_len() > 0);
        }
        let db = IndexDb::open(root.path(), 655, cfg()).unwrap();
        assert_eq!(
            db.search_series_ids(&plan(&[eq("job", "api")]), &[10, 11])
                .unwrap()
                .ids(),
            &[1, 2]
        );
    }

    #[test]
    fn test_flush_then_merge_keeps_uniqueness() {
        let (_root, db) = db();
        for id in 1..=20u64 {
            db.write_series(
                id,
                b"m",
                &[
                    (b"job", b"api"),
                    (b"code", if id % 2 == 0 { b"200" } else { b"500" }),
                ],
                &[10],
            )
            .unwrap();
        }
        db.flush().unwrap();
        let all = db
            .search_series_ids(&plan(&[eq("job", "api")]), &[10])
            .unwrap();
        assert_eq!(all.len(), 20);
        // Re-writing the same series must not duplicate it: the mergeset dedups
        // identical rows, and the posting rows carry fixed-width IDs.
        db.write_series(1, b"m", &[(b"job", b"api"), (b"code", b"500")], &[10])
            .unwrap();
        db.flush().unwrap();
        let all = db
            .search_series_ids(&plan(&[eq("job", "api")]), &[10])
            .unwrap();
        assert_eq!(all.len(), 20);
        // ns2 rows stay one per series.
        let _ = series_id_row;
    }

    #[test]
    fn test_cache_invalidated_on_write() {
        let (_root, db) = db();
        db.write_series(1, b"m", &[(b"job", b"api")], &[10])
            .unwrap();
        let q = plan(&[eq("job", "api")]);
        assert_eq!(db.search_series_ids(&q, &[10]).unwrap().ids(), &[1]);
        let hits = db.cache().hits();
        // The second read is served from the cache.
        assert_eq!(db.search_series_ids(&q, &[10]).unwrap().ids(), &[1]);
        assert!(db.cache().hits() > hits);
        // A new series bumps the generation, so the stale sets are not reused
        // and the query sees the new ID.
        db.write_series(2, b"m", &[(b"job", b"api")], &[10])
            .unwrap();
        assert_eq!(db.search_series_ids(&q, &[10]).unwrap().ids(), &[1, 2]);
    }
}
