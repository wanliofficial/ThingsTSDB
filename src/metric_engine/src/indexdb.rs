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

//! Month-partitioned indexDB manager for the metric engine (ADR-0001 §7).
//!
//! One `IndexDb` exists per calendar month under `{root}/indexdb/{YYYY_MM}`
//! (ADR-0001 §4). This module:
//!
//! - routes a write to every month a sample's day belongs to, skipping series
//!   days that are already registered;
//! - resolves a query to series IDs by walking every month that overlaps the
//!   time range and unioning the per-month answers;
//! - reconstructs labels from namespace 3, so the data scan needs only the
//!   series IDs.
//!
//! The manager is synchronous on purpose: the mergeset is a local file-backed
//! structure, and the metric engine calls it from a blocking task.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use storage_kernel::mergeset::{
    indexdb::SeriesName,
    indexdb_table::{IndexDb, IndexDbConfig},
    tag_filter::{QueryPlan, TagMatcher},
    timeutil,
};

use crate::{types::Sample, Result};

/// Which index answers a query (ADR-0001 §7, §12.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexMode {
    /// Only the legacy inverted-index tables.
    Legacy,
    /// Only the month-partitioned indexDB.
    New,
    /// Both; results are unioned. Used while a deployment migrates.
    #[default]
    DualRead,
}

/// Counters for the new query path, exposed so operators can watch the
/// migration before switching [`IndexMode::Legacy`] off.
#[derive(Debug, Default)]
pub struct IndexQueryStats {
    queries: AtomicU64,
    matched_series: AtomicU64,
    index_micros: AtomicU64,
    writes: AtomicU64,
    legacy_union_skips: AtomicU64,
}

impl IndexQueryStats {
    pub fn queries(&self) -> u64 {
        self.queries.load(Ordering::Relaxed)
    }

    pub fn matched_series(&self) -> u64 {
        self.matched_series.load(Ordering::Relaxed)
    }

    pub fn index_micros(&self) -> u64 {
        self.index_micros.load(Ordering::Relaxed)
    }

    pub fn writes(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }

    /// Queries whose legacy union was skipped because the new index already
    /// covered every legacy series (migration progress signal).
    pub fn legacy_union_skips(&self) -> u64 {
        self.legacy_union_skips.load(Ordering::Relaxed)
    }

    pub(crate) fn record_legacy_union_skip(&self) {
        self.legacy_union_skips.fetch_add(1, Ordering::Relaxed);
    }
}

/// Number of registered `(series, day)` pairs kept in memory before the cache
/// is cleared. A cleared cache only costs a duplicate write: identical rows
/// collapse in the mergeset, so the cache is purely an optimization.
const REGISTERED_CACHE_LIMIT: usize = 4_000_000;

pub struct IndexDbManager {
    root: PathBuf,
    config: IndexDbConfig,
    /// Month -> opened indexDB. Opening a month is lazy and cached.
    open: Mutex<HashMap<u16, std::sync::Arc<IndexDb>>>,
    /// Months that have an on-disk partition. A query only opens these, so a
    /// wide time range like `[0, now)` does not create hundreds of empty
    /// indexDBs.
    months: Mutex<BTreeSet<u16>>,
    /// `(series_id, day)` pairs already written to the index.
    registered: Mutex<HashSet<(u64, u64)>>,
    stats: IndexQueryStats,
}

impl IndexDbManager {
    /// Opens (or creates) the manager rooted at `{root}/indexdb`.
    pub fn open(root: impl AsRef<Path>, config: IndexDbConfig) -> Result<Self> {
        let root = root.as_ref().join("indexdb");
        std::fs::create_dir_all(&root)
            .map_err(|e| anyhow::anyhow!("cannot create indexdb root {root:?}: {e}"))?;
        let months = Self::scan_months(&root);
        Ok(Self {
            root,
            config,
            open: Mutex::new(HashMap::new()),
            months: Mutex::new(months),
            registered: Mutex::new(HashSet::new()),
            stats: IndexQueryStats::default(),
        })
    }

    /// The range the index is queried with: at least one day, and an open lower
    /// bound (`<= 0`) mapped to the first day of the oldest on-disk partition.
    ///
    /// The per-day pruning intersects with the union of every day in the range,
    /// so a range that starts at the epoch drags the first day's set into every
    /// query. Day 0 holds no samples, and the per-day rows only exist under
    /// partitions that were actually written.
    fn query_bounds(&self, start_ms: i64, end_ms: i64) -> (i64, i64) {
        const DAY_MS: i64 = 86_400_000;
        let start = if start_ms > 0 {
            start_ms
        } else {
            let first_month = *self
                .months
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .next()
                .unwrap_or(&0);
            timeutil::month_first_day(first_month) as i64 * DAY_MS
        };
        let end = if end_ms > start {
            end_ms
        } else {
            start + DAY_MS
        };
        (start, end)
    }

    /// Reads the `{YYYY_MM}` partition directories under `root`.
    fn scan_months(root: &Path) -> BTreeSet<u16> {
        let Ok(entries) = std::fs::read_dir(root) else {
            return BTreeSet::new();
        };
        let mut months = BTreeSet::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some((year, month)) = name.split_once('_') else {
                continue;
            };
            let (Ok(year), Ok(month)) = (year.parse::<i32>(), month.parse::<u32>()) else {
                continue;
            };
            if (1..=12).contains(&month) {
                months.insert(month_number(year, month));
            }
        }
        months
    }

    /// The `{root}/indexdb` directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn stats(&self) -> &IndexQueryStats {
        &self.stats
    }

    /// Opens (and caches) the indexDB of one month.
    fn db(&self, month: u16) -> Result<std::sync::Arc<IndexDb>> {
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(db) = open.get(&month) {
            return Ok(std::sync::Arc::clone(db));
        }
        let dir = self.root.join(timeutil::month_label(month));
        let db = IndexDb::open(&dir, month, self.config.clone())
            .map_err(|e| anyhow::anyhow!("open indexdb {dir:?}: {e}"))?;
        let db = std::sync::Arc::new(db);
        open.insert(month, std::sync::Arc::clone(&db));
        self.months
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(month);
        Ok(db)
    }

    /// Registers every series of `samples` for the days they carry data on.
    ///
    /// This is a no-op for series days already registered in this process.
    pub fn register_samples(&self, samples: &[Sample]) -> Result<()> {
        // series_key -> (group, tags, series id, days)
        struct Entry {
            group: Vec<u8>,
            tags: Vec<(Vec<u8>, Vec<u8>)>,
            series_id: u64,
            days: Vec<u64>,
        }
        let mut by_series: HashMap<Vec<u8>, Entry> = HashMap::new();
        for sample in samples {
            let series_id = sample.series_id().0;
            let key = sample.series_key();
            let entry = by_series.entry(key).or_insert_with(|| Entry {
                group: sample.name.clone(),
                tags: sample
                    .labels
                    .iter()
                    .map(|l| (l.name.clone(), l.value.clone()))
                    .collect(),
                series_id,
                days: Vec::new(),
            });
            entry.days.push(timeutil::day_of_millis(sample.timestamp));
        }
        if by_series.is_empty() {
            return Ok(());
        }

        // Group the writes by month so each month appends one batched WAL
        // frame per few thousand series instead of one fsync per series.
        struct MonthWrite {
            series_id: u64,
            group: Vec<u8>,
            tags: Vec<(Vec<u8>, Vec<u8>)>,
            days: Vec<u64>,
        }
        let mut by_month: HashMap<u16, Vec<MonthWrite>> = HashMap::new();
        for entry in by_series.values_mut() {
            entry.days.sort_unstable();
            entry.days.dedup();
            let mut days_by_month: HashMap<u16, Vec<u64>> = HashMap::new();
            for &day in &entry.days {
                days_by_month
                    .entry(timeutil::month_of_day(day))
                    .or_default()
                    .push(day);
            }
            for (month, days) in days_by_month {
                let days = self.filter_unregistered(entry.series_id, days);
                if days.is_empty() {
                    continue;
                }
                by_month.entry(month).or_default().push(MonthWrite {
                    series_id: entry.series_id,
                    group: entry.group.clone(),
                    tags: entry.tags.clone(),
                    days,
                });
            }
        }

        let mut written = 0usize;
        for (month, writes) in by_month {
            let db = self.db(month)?;
            // Borrow the owned tag vectors once and hand the batch a slice of
            // borrowed tuples (the batch API stays allocation-free).
            let tag_refs: Vec<Vec<(&[u8], &[u8])>> = writes
                .iter()
                .map(|w| {
                    w.tags
                        .iter()
                        .map(|(k, v)| (k.as_slice(), v.as_slice()))
                        .collect()
                })
                .collect();
            let batch: Vec<(u64, &[u8], &[(&[u8], &[u8])], &[u64])> = writes
                .iter()
                .zip(tag_refs.iter())
                .map(|(w, tags)| {
                    (
                        w.series_id,
                        w.group.as_slice(),
                        tags.as_slice(),
                        w.days.as_slice(),
                    )
                })
                .collect();
            db.write_series_batch(&batch)
                .map_err(|e| anyhow::anyhow!("write indexdb month {month}: {e}"))?;
            for w in &writes {
                self.mark_registered(w.series_id, &w.days);
                written += w.days.len();
            }
        }
        if written > 0 {
            self.stats.writes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn filter_unregistered(&self, series_id: u64, days: Vec<u64>) -> Vec<u64> {
        let registered = self.registered.lock().unwrap_or_else(|e| e.into_inner());
        days.into_iter()
            .filter(|day| !registered.contains(&(series_id, *day)))
            .collect()
    }

    fn mark_registered(&self, series_id: u64, days: &[u64]) {
        let mut registered = self.registered.lock().unwrap_or_else(|e| e.into_inner());
        if registered.len() + days.len() > REGISTERED_CACHE_LIMIT {
            registered.clear();
        }
        registered.extend(days.iter().map(|day| (series_id, *day)));
    }

    /// Resolves a query to `series id -> (group, sorted tags)`.
    ///
    /// The range is `[start_ms, end_ms)`; a whole month is pruned when the
    /// range does not overlap it, and within a month the per-day rows prune
    /// series without data on the requested days (ADR-0001 §5).
    pub fn search(
        &self,
        group: &[u8],
        matchers: &[TagMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<u64, SeriesName>> {
        let started = std::time::Instant::now();
        // A range must contain at least one day for the per-day pruning to
        // work; an empty range still resolves the group postings.
        // Day granularity: without a lower bound the range must not start at
        // the epoch, or the first day's set (often the whole series space for
        // a dense metric) is scanned and unioned into every query.
        let (start_ms, end_ms) = self.query_bounds(start_ms, end_ms);
        let first_month = timeutil::month_of_millis(start_ms);
        let last_month = timeutil::month_of_millis(end_ms - 1);
        let mut plan_matchers = Vec::with_capacity(matchers.len() + 1);
        plan_matchers.push(TagMatcher::Equal {
            key: b"__name__".to_vec(),
            value: group.to_vec(),
        });
        plan_matchers.extend_from_slice(matchers);
        let plan = QueryPlan::from_matchers(&plan_matchers);

        let mut out = HashMap::new();
        let mut matched = 0u64;
        let candidates: Vec<u16> = {
            let months = self.months.lock().unwrap_or_else(|e| e.into_inner());
            months.range(first_month..=last_month).copied().collect()
        };
        for month in candidates {
            let days = timeutil::month_days_in_range(month, start_ms, end_ms);
            if days.is_empty() {
                continue;
            }
            let db = self.db(month)?;
            let ids = db
                .search_series_ids_arc(&plan, &days)
                .map_err(|e| anyhow::anyhow!("search indexdb month {month}: {e}"))?;
            if ids.is_empty() {
                continue;
            }
            matched += ids.len() as u64;
            let names = db
                .series_names(ids.ids())
                .map_err(|e| anyhow::anyhow!("read indexdb names for month {month}: {e}"))?;
            for (id, name) in names {
                out.entry(id).or_insert(name);
            }
        }
        self.stats.queries.fetch_add(1, Ordering::Relaxed);
        self.stats
            .matched_series
            .fetch_add(matched, Ordering::Relaxed);
        self.stats
            .index_micros
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        Ok(out)
    }

    /// Every series id of `group` that has a day row in `[start_ms, end_ms)`.
    ///
    /// This is the metric-group set the new index would return for an empty
    /// matcher list. `DualRead` uses it to decide whether the legacy inverted
    /// index can be skipped for a metric during migration.
    pub fn group_series_ids(&self, group: &[u8], start_ms: i64, end_ms: i64) -> Result<Vec<u64>> {
        let (start_ms, end_ms) = self.query_bounds(start_ms, end_ms);
        let first_month = timeutil::month_of_millis(start_ms);
        let last_month = timeutil::month_of_millis(end_ms - 1);
        let plan = QueryPlan::from_matchers(&[TagMatcher::Equal {
            key: b"__name__".to_vec(),
            value: group.to_vec(),
        }]);
        let candidates: Vec<u16> = {
            let months = self.months.lock().unwrap_or_else(|e| e.into_inner());
            months.range(first_month..=last_month).copied().collect()
        };
        let mut ids = Vec::new();
        for month in candidates {
            let days = timeutil::month_days_in_range(month, start_ms, end_ms);
            if days.is_empty() {
                continue;
            }
            let db = self.db(month)?;
            let set = db
                .search_series_ids_arc(&plan, &days)
                .map_err(|e| anyhow::anyhow!("search indexdb month {month}: {e}"))?;
            ids.extend_from_slice(set.ids());
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    /// Resolves a query to the matching series IDs only, without labels.
    ///
    /// Used by parity tests and callers that already have the labels.
    pub fn search_ids(
        &self,
        group: &[u8],
        matchers: &[TagMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<u64>> {
        let found = self.search(group, matchers, start_ms, end_ms)?;
        let mut ids: Vec<u64> = found.keys().copied().collect();
        ids.sort_unstable();
        Ok(ids)
    }

    /// Makes every pending row of every open month searchable. Tests use this
    /// to observe writes without waiting for the flush interval.
    pub fn flush(&self) -> Result<()> {
        let open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        for (month, db) in open.iter() {
            db.flush()
                .map_err(|e| anyhow::anyhow!("flush indexdb month {month}: {e}"))?;
        }
        Ok(())
    }
}

/// The number of months since 1970-01 for a civil year/month.
fn month_number(year: i32, month: u32) -> u16 {
    let n = (year - 1970) * 12 + month as i32 - 1;
    n.clamp(0, u16::MAX as i32) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &str, labels: &[(&str, &str)], ts: i64) -> Sample {
        Sample {
            name: name.as_bytes().to_vec(),
            labels: labels
                .iter()
                .map(|(k, v)| crate::types::Label {
                    name: k.as_bytes().to_vec(),
                    value: v.as_bytes().to_vec(),
                })
                .collect(),
            timestamp: ts,
            value: 1.0,
            name_id: None,
            series_id: None,
        }
    }

    fn manager() -> (temp_dir::TempDir, IndexDbManager) {
        let root = temp_dir::TempDir::new().unwrap();
        let mgr = IndexDbManager::open(root.path(), Default::default()).unwrap();
        (root, mgr)
    }

    #[test]
    fn test_register_and_search_across_days() {
        let (_root, mgr) = manager();
        // 2023-11-14T22:13:20Z, and the next day.
        let day_one = 1_700_000_000_000i64;
        let day_two = day_one + 86_400_000;
        mgr.register_samples(&[
            sample("http_requests", &[("job", "api"), ("code", "200")], day_one),
            sample("http_requests", &[("job", "api"), ("code", "200")], day_two),
            sample("http_requests", &[("job", "web"), ("code", "200")], day_one),
            sample("grpc_requests", &[("job", "api"), ("code", "200")], day_one),
        ])
        .unwrap();

        let tags = [
            TagMatcher::Equal {
                key: b"job".to_vec(),
                value: b"api".to_vec(),
            },
            TagMatcher::Equal {
                key: b"code".to_vec(),
                value: b"200".to_vec(),
            },
        ];
        // Only the http_requests series matches both the group and the tags,
        // across both days.
        let found = mgr
            .search(b"http_requests", &tags, day_one - 1, day_two + 1)
            .unwrap();
        assert_eq!(found.len(), 1);
        let (group, series_tags) = found.values().next().unwrap();
        assert_eq!(group, b"http_requests");
        assert_eq!(
            series_tags,
            &vec![
                (b"code".to_vec(), b"200".to_vec()),
                (b"job".to_vec(), b"api".to_vec())
            ]
        );

        // Day pruning: the day after the last data returns nothing.
        let day_three = day_two + 86_400_000;
        let found = mgr
            .search(b"http_requests", &tags, day_three, day_three + 1)
            .unwrap();
        assert!(found.is_empty(), "no data on day three");
        // The web job does exist on day one.
        let found = mgr
            .search(
                b"http_requests",
                &[TagMatcher::Equal {
                    key: b"job".to_vec(),
                    value: b"web".to_vec(),
                }],
                day_one,
                day_one + 1,
            )
            .unwrap();
        assert_eq!(found.len(), 1);

        // A wide range must not open empty months: only 2023-11 exists.
        let wide = mgr.search(b"http_requests", &[], 0, day_two + 1).unwrap();
        assert_eq!(wide.len(), 2, "both http_requests series");
    }

    #[test]
    fn test_regex_and_negative_matchers() {
        let (_root, mgr) = manager();
        let ts = 1_700_000_000_000i64;
        mgr.register_samples(&[
            sample("m", &[("job", "api")], ts),
            sample("m", &[("job", "api2")], ts),
            sample("m", &[("job", "web")], ts),
        ])
        .unwrap();

        let ids = mgr
            .search_ids(
                b"m",
                &[TagMatcher::Re {
                    key: b"job".to_vec(),
                    regex: b"api.*".to_vec(),
                }],
                ts,
                ts + 1,
            )
            .unwrap();
        assert_eq!(ids.len(), 2);
        let ids = mgr
            .search_ids(
                b"m",
                &[TagMatcher::Nre {
                    key: b"job".to_vec(),
                    regex: b"api.*".to_vec(),
                }],
                ts,
                ts + 1,
            )
            .unwrap();
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn test_register_skips_already_registered_days() {
        let (_root, mgr) = manager();
        let ts = 1_700_000_000_000i64;
        let s = sample("m", &[("job", "api")], ts);
        mgr.register_samples(std::slice::from_ref(&s)).unwrap();
        mgr.flush().unwrap();
        let before = mgr.stats().writes();
        // Re-registering the same series day writes nothing.
        mgr.register_samples(std::slice::from_ref(&s)).unwrap();
        assert_eq!(mgr.stats().writes(), before);
    }
}
