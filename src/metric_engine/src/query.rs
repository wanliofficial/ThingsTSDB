// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to
// you under the Apache License, Version 2.0 (the
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

//! Query path, the counterpart of the write path:
//!
//! 1. resolve label matchers to a series id set via the month-partitioned
//!    mergeset index (ADR-0001 §7), optionally unioned with the legacy inverted
//!    `index` table while a deployment migrates;
//! 2. fetch the labels of the matched series;
//! 3. scan the `data` table by (metric_id, tsid, timestamp range);
//! 4. group samples back into series.
//!
//! The cost is proportional to the matched series and the requested time
//! range, not to the total data volume.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::Context;
use arrow::array::{Array, Float64Array, Int64Array, UInt64Array};
use columnar_storage::storage::ScanRequest;
use datafusion::{
    logical_expr::{col, lit, Expr},
    scalar::ScalarValue,
};
use futures::StreamExt;
use storage_kernel::mergeset::tag_filter::TagMatcher;

use crate::{
    data, index,
    indexdb::IndexMode,
    metric::full_range,
    types::{build_series_key, decode_series_key, Label},
    MetricEngine, Result,
};

/// The current wall-clock time in milliseconds, used as the reference for
/// per-metric retention windows.
fn now_ms() -> i64 {
    common::now()
}

/// The label matcher types Prometheus remote read and PromQL use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatcherType {
    /// `=`
    Eq,
    /// `!=`
    Neq,
    /// `=~`
    Re,
    /// `!~`
    Nre,
}

#[derive(Debug, Clone)]
pub struct LabelMatcher {
    pub matcher_type: MatcherType,
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub metric_name: Vec<u8>,
    /// Label matchers excluding the metric name itself.
    pub matchers: Vec<LabelMatcher>,
    /// Milliseconds, samples in `[start_ms, end_ms)` are returned.
    pub start_ms: i64,
    pub end_ms: i64,
    /// Maximum number of series to materialize, `0` for no limit.
    ///
    /// The series are resolved and matched first (that is index work), then
    /// only this many are scanned and returned. A dashboard panel renders a
    /// bounded number of series, and scanning samples for the other 100k
    /// matched series is most of a wide query's cost.
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SamplePoint {
    pub timestamp: i64,
    pub value: f64,
}

/// One matched series with its samples, sorted by timestamp.
#[derive(Debug, Clone)]
pub struct SeriesSamples {
    pub metric_name: Vec<u8>,
    pub labels: Vec<Label>,
    pub samples: Vec<SamplePoint>,
}

/// One matched series' newest sample inside the requested range.
#[derive(Debug, Clone)]
pub struct SeriesLatest {
    pub metric_name: Vec<u8>,
    pub labels: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
}

/// Aggregation applied to every downsampling bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    Avg,
    Min,
    Max,
    Sum,
    Count,
    Last,
}

/// Above this series count, tsids are filtered in the engine instead of being
/// pushed down as an IN list.
///
/// The predicate is planned once and then evaluated per row, so a large list
/// costs far more than the row-wise hash lookup it replaces: at 200k series a
/// two-tag filter that matched 154k series took 1.4 s with the pushdown and
/// 44 ms without it. Small candidate sets still benefit, because the scan can
/// then skip rows of other series and the filter is cheap to evaluate.
const IN_LIST_LIMIT: usize = 1_000;

impl MetricEngine {
    pub async fn search(&self, req: &SearchRequest) -> Result<Vec<SeriesSamples>> {
        let metric_id = crate::hash(&req.metric_name);
        // Per-metric retention raises the lower bound of the range to the
        // retention floor before any index or data work, so aged-out samples
        // are invisible without a separate scan.
        let (start_ms, end_ms) =
            self.retention
                .clamp(&req.metric_name, req.start_ms, req.end_ms, now_ms());
        // Step 1: resolve matchers to series id -> labels.
        let series = self.resolve_series(req, start_ms, end_ms).await?;

        // Step 2: scan data. A `limit` narrows the candidate set to a stable
        // subset (sorted by series id) so the same query returns the same
        // series instead of whichever rows the scan happens to reach first.
        let series = self.limit_series(series, req.limit);
        let candidates: HashSet<u64> = series.keys().copied().collect();
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let samples_by_tsid = self
            .scan_samples(metric_id, &candidates, start_ms, end_ms)
            .await?;

        // Step 3: assemble results, ordered by canonical series key for
        // determinism.
        let mut matched: Vec<(Vec<u8>, SeriesSamples)> = samples_by_tsid
            .into_iter()
            .filter_map(|(tsid, mut samples)| {
                let (name, labels) = series.get(&tsid)?;
                samples.sort_by_key(|s| s.timestamp);
                let key = build_series_key(name, labels);
                Some((
                    key,
                    SeriesSamples {
                        metric_name: name.clone(),
                        labels: labels.clone(),
                        samples,
                    },
                ))
            })
            .collect();
        matched.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(matched.into_iter().map(|(_, series)| series).collect())
    }

    /// Resolves matchers to `series id -> (name, labels)`.
    ///
    /// The new index is authoritative once fully migrated; the legacy tables
    /// are still read in `DualRead` so data written before the switch stays
    /// visible.
    /// Narrows `series` to a stable subset of at most `limit` entries.
    ///
    /// Applied before the data scan so a bounded query does not read samples
    /// for series it will not return; ids are sorted so repeated queries return
    /// the same series.
    fn limit_series(
        &self,
        series: HashMap<u64, (Vec<u8>, Vec<Label>)>,
        limit: usize,
    ) -> HashMap<u64, (Vec<u8>, Vec<Label>)> {
        if limit == 0 || series.len() <= limit {
            return series;
        }
        let mut ids: Vec<u64> = series.keys().copied().collect();
        ids.sort_unstable();
        ids.truncate(limit);
        let keep: HashSet<u64> = ids.into_iter().collect();
        series
            .into_iter()
            .filter(|(id, _)| keep.contains(id))
            .collect()
    }

    async fn resolve_series(
        &self,
        req: &SearchRequest,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<u64, (Vec<u8>, Vec<Label>)>> {
        let metric_id = crate::hash(&req.metric_name);
        let mut series: HashMap<u64, (Vec<u8>, Vec<Label>)> = HashMap::new();
        let use_new = self.indexdb.is_some() && self.index_mode != IndexMode::Legacy;
        let use_legacy = self.index_mode != IndexMode::New;

        if use_new {
            let indexdb = self.indexdb.as_ref().expect("checked above");
            let found = indexdb.search(
                &req.metric_name,
                &tag_matchers(&req.matchers),
                start_ms,
                end_ms,
            )?;
            for (tsid, (name, tags)) in found {
                let labels = tags
                    .into_iter()
                    .map(|(name, value)| Label { name, value })
                    .collect();
                series.insert(tsid, (name, labels));
            }
        }

        if use_legacy {
            let series_keys = self.fetch_series_keys(metric_id).await?;
            // During migration the new index eventually holds every legacy
            // series; once it covers the queried range for this metric, the
            // union cannot add anything and is skipped. The check is cheap
            // (cached metric-day set), while the legacy resolve is either a
            // full series-key pass for regex or an index-table scan for eq.
            let covered =
                use_new && self.legacy_covered(&req.metric_name, &series_keys, start_ms, end_ms);
            if !covered {
                let candidates = self
                    .resolve_legacy(metric_id, &req.matchers, series_keys.as_ref())
                    .await?;
                for tsid in candidates {
                    if series.contains_key(&tsid) {
                        continue;
                    }
                    let Some(key) = series_keys.get(&tsid) else {
                        continue;
                    };
                    let (name, labels) = decode_series_key(key).context("decode series key")?;
                    series.insert(tsid, (name, labels));
                }
            } else if let Some(indexdb) = self.indexdb.as_ref() {
                indexdb.stats().record_legacy_union_skip();
            }
        }

        Ok(series)
    }

    /// Newest sample per matched series inside `[start_ms, end_ms)`.
    ///
    /// Served from the per-series last-value cache the write path maintains. A
    /// series whose cached sample falls outside the range (or a cache miss
    /// after restart) is resolved with a range scan and promoted back into the
    /// cache.
    pub async fn last_values(&self, req: &SearchRequest) -> Result<Vec<SeriesLatest>> {
        let (start_ms, end_ms) =
            self.retention
                .clamp(&req.metric_name, req.start_ms, req.end_ms, now_ms());
        let series =
            self.limit_series(self.resolve_series(req, start_ms, end_ms).await?, req.limit);
        if series.is_empty() {
            return Ok(Vec::new());
        }
        let end_ms = if end_ms > start_ms {
            end_ms
        } else {
            start_ms + 1
        };

        let mut latest: HashMap<u64, crate::last_cache::LastValue> = HashMap::new();
        let mut missing: HashSet<u64> = HashSet::new();
        for &tsid in series.keys() {
            match self.last_cache.get(tsid) {
                Some(value) if value.timestamp >= start_ms && value.timestamp < end_ms => {
                    latest.insert(tsid, value);
                }
                _ => {
                    missing.insert(tsid);
                }
            }
        }

        if !missing.is_empty() {
            let metric_id = crate::hash(&req.metric_name);
            let scanned = self
                .scan_samples(metric_id, &missing, start_ms, end_ms)
                .await?;
            for (tsid, points) in scanned {
                let Some(last) = points.into_iter().max_by_key(|p| p.timestamp) else {
                    continue;
                };
                self.last_cache.observe(tsid, last.timestamp, last.value);
                latest.insert(
                    tsid,
                    crate::last_cache::LastValue {
                        timestamp: last.timestamp,
                        value: last.value,
                    },
                );
            }
        }

        let mut matched: Vec<(Vec<u8>, SeriesLatest)> = latest
            .into_iter()
            .filter_map(|(tsid, value)| {
                let (name, labels) = series.get(&tsid)?;
                let key = build_series_key(name, labels);
                Some((
                    key,
                    SeriesLatest {
                        metric_name: name.clone(),
                        labels: labels.clone(),
                        timestamp: value.timestamp,
                        value: value.value,
                    },
                ))
            })
            .collect();
        matched.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(matched.into_iter().map(|(_, series)| series).collect())
    }

    /// Buckets every series' samples into fixed `step_ms` windows and reduces
    /// each window with `agg` (the range-query / rollup read shape).
    ///
    /// Buckets are aligned to the Unix epoch and labelled with their start
    /// timestamp; empty windows produce no point.
    pub async fn search_downsampled(
        &self,
        req: &SearchRequest,
        step_ms: i64,
        agg: Aggregation,
    ) -> Result<Vec<SeriesSamples>> {
        if step_ms <= 0 {
            return Err(anyhow::anyhow!("step_ms must be positive, got {step_ms}").into());
        }
        let mut series = self.search(req).await?;
        for series in &mut series {
            series.samples = downsample(&series.samples, step_ms, agg);
        }
        Ok(series)
    }

    /// Whether the new index already holds every legacy series of the metric
    /// for the queried range, so the legacy union cannot add a series.
    ///
    /// A series that carries data in the range was registered for its days by
    /// the write path, so membership in the group's day set is the coverage
    /// test. Legacy-only series (data written before the index was enabled, or
    /// series without data in the range) make the check fail and keep the
    /// union, which is conservative rather than wrong.
    fn legacy_covered(
        &self,
        metric_name: &[u8],
        legacy: &HashMap<u64, Vec<u8>>,
        start_ms: i64,
        end_ms: i64,
    ) -> bool {
        if legacy.is_empty() {
            return true;
        }
        let Some(indexdb) = self.indexdb.as_ref() else {
            return false;
        };
        let Ok(ids) = indexdb.group_series_ids(metric_name, start_ms, end_ms) else {
            return false;
        };
        !ids.is_empty()
            && legacy.len() <= ids.len()
            && legacy.keys().all(|id| ids.binary_search(id).is_ok())
    }

    /// Resolves matchers through the legacy inverted index. Regex matchers are
    /// evaluated against the series labels, because the legacy index only
    /// stores exact tag pairs.
    async fn resolve_legacy(
        &self,
        metric_id: u64,
        matchers: &[LabelMatcher],
        series_keys: &HashMap<u64, Vec<u8>>,
    ) -> Result<HashSet<u64>> {
        let needs_scan = matchers
            .iter()
            .any(|m| matches!(m.matcher_type, MatcherType::Re | MatcherType::Nre));
        if needs_scan {
            let compiled = compile_matchers(matchers)?;
            let mut out = HashSet::new();
            for (tsid, key) in series_keys {
                let (_, labels) = decode_series_key(key).context("decode series key")?;
                if compiled.iter().all(|m| m.matches(&labels)) {
                    out.insert(*tsid);
                }
            }
            return Ok(out);
        }

        let mut candidates: Option<HashSet<u64>> = None;
        let mut neq_matchers = Vec::new();
        for matcher in matchers {
            match matcher.matcher_type {
                MatcherType::Eq => {
                    let matched = self.lookup_index(metric_id, matcher).await?;
                    candidates = Some(match candidates {
                        None => matched,
                        Some(current) => current.intersection(&matched).copied().collect(),
                    });
                }
                MatcherType::Neq => neq_matchers.push(matcher),
                MatcherType::Re | MatcherType::Nre => unreachable!("handled above"),
            }
        }
        // NEQ matchers need the full series set to subtract from.
        if !neq_matchers.is_empty() && candidates.is_none() {
            candidates = Some(series_keys.keys().copied().collect());
        }
        for matcher in &neq_matchers {
            let matched = self.lookup_index(metric_id, matcher).await?;
            if let Some(current) = candidates.as_mut() {
                for tsid in matched {
                    current.remove(&tsid);
                }
            }
        }
        Ok(candidates.unwrap_or_else(|| series_keys.keys().copied().collect()))
    }

    /// Look up series ids matching one label pair from the inverted index.
    async fn lookup_index(&self, metric_id: u64, matcher: &LabelMatcher) -> Result<HashSet<u64>> {
        let predicates = vec![
            col(index::METRIC_ID_COLUMN).eq(lit(ScalarValue::UInt64(Some(metric_id)))),
            col(index::TAG_KEY_COLUMN).eq(lit(ScalarValue::Binary(Some(matcher.name.clone())))),
            col(index::TAG_VALUE_COLUMN).eq(lit(ScalarValue::Binary(Some(matcher.value.clone())))),
        ];
        let stream = self
            .index
            .index_storage()
            .scan(ScanRequest {
                range: full_range(),
                predicate: predicates,
                projections: None,
            })
            .await?;
        collect_tsids(stream, index::TSID_COLUMN, self.index.index_storage()).await
    }

    /// Series id -> series key of one metric, cached until new series arrive.
    async fn fetch_series_keys(&self, metric_id: u64) -> Result<Arc<HashMap<u64, Vec<u8>>>> {
        self.index.series_keys(metric_id).await
    }

    /// Scan samples of the candidate series within the time range.
    async fn scan_samples(
        &self,
        metric_id: u64,
        candidates: &HashSet<u64>,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<u64, Vec<SamplePoint>>> {
        self.scan_samples_limited(metric_id, candidates, start_ms, end_ms, 0)
            .await
    }

    /// Like [`MetricEngine::scan_samples`] but returns at most `limit` series.
    async fn scan_samples_limited(
        &self,
        metric_id: u64,
        candidates: &HashSet<u64>,
        start_ms: i64,
        end_ms: i64,
        limit: usize,
    ) -> Result<HashMap<u64, Vec<SamplePoint>>> {
        let storage = self.data.storage();
        let mut predicates = vec![
            col(data::METRIC_ID_COLUMN).eq(lit(ScalarValue::UInt64(Some(metric_id)))),
            col(data::TIMESTAMP_COLUMN)
                .gt_eq(lit(ScalarValue::Int64(Some(start_ms))))
                .and(col(data::TIMESTAMP_COLUMN).lt(lit(ScalarValue::Int64(Some(end_ms))))),
        ];
        // Push a candidate set down as an IN list only when it is small
        // enough to pay for; above that the scan stays wide and the batches
        // are filtered here. (`limit` narrows `candidates` before the scan.)
        let pushdown = !candidates.is_empty() && candidates.len() <= IN_LIST_LIMIT;
        if pushdown {
            let values = candidates
                .iter()
                .map(|tsid| Expr::Literal(ScalarValue::UInt64(Some(*tsid))))
                .collect::<Vec<_>>();
            predicates.push(col(data::TSID_COLUMN).in_list(values, false));
        }

        let mut stream = storage
            .scan(ScanRequest {
                range: full_range(),
                predicate: predicates,
                projections: None,
            })
            .await?;

        let mut result = HashMap::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.context("scan data table")?;
            let schema = storage.schema();
            let tsid_idx = schema.index_of(data::TSID_COLUMN).context("find column")?;
            let ts_idx = schema
                .index_of(data::TIMESTAMP_COLUMN)
                .context("find column")?;
            let value_idx = schema.index_of(data::VALUE_COLUMN).context("find column")?;
            let tsids = batch
                .column(tsid_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("tsid column must be UInt64Array");
            let timestamps = batch
                .column(ts_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("timestamp column must be Int64Array");
            let values = batch
                .column(value_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("value column must be Float64Array");
            for i in 0..batch.num_rows() {
                let tsid = tsids.value(i);
                // When the in-list pushdown was skipped, filter here.
                if !pushdown && !candidates.contains(&tsid) {
                    continue;
                }
                if limit > 0 && !result.contains_key(&tsid) && result.len() >= limit {
                    continue;
                }
                result
                    .entry(tsid)
                    .or_insert_with(Vec::new)
                    .push(SamplePoint {
                        timestamp: timestamps.value(i),
                        value: values.value(i),
                    });
            }
        }
        Ok(result)
    }
}

/// Reduces sorted samples into `step_ms` buckets aligned to the epoch.
pub fn downsample(samples: &[SamplePoint], step_ms: i64, agg: Aggregation) -> Vec<SamplePoint> {
    let mut out = Vec::new();
    let mut index = 0;
    while index < samples.len() {
        let bucket = samples[index].timestamp.div_euclid(step_ms) * step_ms;
        let mut count = 0u64;
        let mut sum = 0.0f64;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        let mut last = 0.0f64;
        while index < samples.len()
            && samples[index].timestamp.div_euclid(step_ms) * step_ms == bucket
        {
            let value = samples[index].value;
            count += 1;
            sum += value;
            min = min.min(value);
            max = max.max(value);
            last = value;
            index += 1;
        }
        let value = match agg {
            Aggregation::Avg => sum / count as f64,
            Aggregation::Min => min,
            Aggregation::Max => max,
            Aggregation::Sum => sum,
            Aggregation::Count => count as f64,
            Aggregation::Last => last,
        };
        out.push(SamplePoint {
            timestamp: bucket,
            value,
        });
    }
    out
}

/// Maps the engine matchers to the planner's canonical matchers.
fn tag_matchers(matchers: &[LabelMatcher]) -> Vec<TagMatcher> {
    matchers
        .iter()
        .map(|m| match m.matcher_type {
            MatcherType::Eq => TagMatcher::Equal {
                key: m.name.clone(),
                value: m.value.clone(),
            },
            MatcherType::Neq => TagMatcher::NotEqual {
                key: m.name.clone(),
                value: m.value.clone(),
            },
            MatcherType::Re => TagMatcher::Re {
                key: m.name.clone(),
                regex: m.value.clone(),
            },
            MatcherType::Nre => TagMatcher::Nre {
                key: m.name.clone(),
                regex: m.value.clone(),
            },
        })
        .collect()
}

/// A matcher compiled for the legacy path's in-memory series filter.
enum LegacyMatcher {
    Eq(Vec<u8>, Vec<u8>),
    Neq(Vec<u8>, Vec<u8>),
    Re(Vec<u8>, regex::bytes::Regex),
    Nre(Vec<u8>, regex::bytes::Regex),
}

impl LegacyMatcher {
    /// Evaluates the matcher against a series' labels.
    ///
    /// Prometheus semantics: a negative matcher also matches series that do
    /// not carry the label at all.
    fn matches(&self, labels: &[Label]) -> bool {
        match self {
            LegacyMatcher::Eq(name, want) => {
                labels.iter().any(|l| &l.name == name && &l.value == want)
            }
            LegacyMatcher::Neq(name, want) => {
                !labels.iter().any(|l| &l.name == name && &l.value == want)
            }
            LegacyMatcher::Re(name, re) => labels
                .iter()
                .any(|l| &l.name == name && re.is_match(&l.value)),
            LegacyMatcher::Nre(name, re) => !labels
                .iter()
                .any(|l| &l.name == name && re.is_match(&l.value)),
        }
    }
}

fn compile_matchers(matchers: &[LabelMatcher]) -> Result<Vec<LegacyMatcher>> {
    let mut out = Vec::with_capacity(matchers.len());
    for m in matchers {
        out.push(match m.matcher_type {
            MatcherType::Eq => LegacyMatcher::Eq(m.name.clone(), m.value.clone()),
            MatcherType::Neq => LegacyMatcher::Neq(m.name.clone(), m.value.clone()),
            MatcherType::Re | MatcherType::Nre => {
                let pattern = std::str::from_utf8(&m.value)
                    .with_context(|| format!("matcher on {:?} is not UTF-8", m.name))?;
                // Prometheus regexes are fully anchored.
                let re = regex::bytes::Regex::new(&format!("^(?:{pattern})$"))
                    .with_context(|| format!("invalid regex {pattern:?}"))?;
                if m.matcher_type == MatcherType::Re {
                    LegacyMatcher::Re(m.name.clone(), re)
                } else {
                    LegacyMatcher::Nre(m.name.clone(), re)
                }
            }
        });
    }
    Ok(out)
}

async fn collect_tsids(
    mut stream: datafusion::execution::SendableRecordBatchStream,
    tsid_column: &str,
    storage: &columnar_storage::storage::ColumnarStorageRef,
) -> Result<HashSet<u64>> {
    let mut result = HashSet::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.context("scan index table")?;
        let tsid_idx = storage
            .schema()
            .index_of(tsid_column)
            .context("find column")?;
        let tsids = batch
            .column(tsid_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("tsid column must be UInt64Array");
        result.extend(tsids.iter().flatten());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use columnar_storage::{
        config::StorageConfig, storage::StorageRuntimes, types::ObjectStoreRef,
    };
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::types::Label;

    async fn open_engine_with_opts(root: &temp_dir::TempDir, opts: StorageConfig) -> MetricEngine {
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let runtimes = StorageRuntimes::new(rt.clone(), rt);
        MetricEngine::try_open(
            &root.path().to_string_lossy(),
            Duration::from_hours(2),
            store,
            opts,
            runtimes,
        )
        .await
        .unwrap()
    }

    async fn open_engine(root: &temp_dir::TempDir) -> MetricEngine {
        open_engine_with_opts(root, StorageConfig::default()).await
    }

    /// Opens an engine with an explicit index mode, so tests can compare the
    /// legacy and mergeset paths on identical data.
    async fn open_engine_with_mode(
        root: &temp_dir::TempDir,
        index_mode: IndexMode,
    ) -> MetricEngine {
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let runtimes = StorageRuntimes::new(rt.clone(), rt);
        MetricEngine::try_open_with_index_mode(
            &root.path().to_string_lossy(),
            Duration::from_hours(2),
            store,
            StorageConfig::default(),
            runtimes,
            index_mode,
        )
        .await
        .unwrap()
    }

    fn make_sample(name: &str, code: &str, job: &str, ts: i64, value: f64) -> crate::Sample {
        crate::Sample {
            name: name.as_bytes().to_vec(),
            labels: vec![
                Label {
                    name: b"code".to_vec(),
                    value: code.as_bytes().to_vec(),
                },
                Label {
                    name: b"job".to_vec(),
                    value: job.as_bytes().to_vec(),
                },
            ],
            timestamp: ts,
            value,
            name_id: None,
            series_id: None,
        }
    }

    fn matcher(name: &str, value: &str, matcher_type: MatcherType) -> LabelMatcher {
        LabelMatcher {
            matcher_type,
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn search_req(name: &str, matchers: Vec<LabelMatcher>, start: i64, end: i64) -> SearchRequest {
        SearchRequest {
            metric_name: name.as_bytes().to_vec(),
            matchers,
            start_ms: start,
            end_ms: end,
            limit: 0,
        }
    }

    async fn prepare_data(engine: &MetricEngine) {
        // m1: A = {code=200, job=api}, B = {code=404, job=api}
        // m2: C = {code=200, job=db}
        engine
            .write(vec![
                make_sample("m1", "200", "api", 10, 1.0),
                make_sample("m1", "200", "api", 20, 2.0),
                make_sample("m1", "200", "api", 30, 3.0),
                make_sample("m1", "404", "api", 10, 10.0),
                make_sample("m2", "200", "db", 10, 42.0),
            ])
            .await
            .unwrap();
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_search_by_eq_matchers() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine(&root).await;
        prepare_data(&engine).await;

        // Single matcher.
        let results = engine
            .search(&search_req(
                "m1",
                vec![matcher("code", "200", MatcherType::Eq)],
                0,
                100,
            ))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name, b"m1".to_vec());
        assert_eq!(results[0].samples.len(), 3);
        assert_eq!(results[0].samples[0].timestamp, 10);
        assert_eq!(results[0].samples[2].value, 3.0);

        // Intersected matchers: code=200 AND job=db only matches C in m2.
        let results = engine
            .search(&search_req(
                "m2",
                vec![
                    matcher("code", "200", MatcherType::Eq),
                    matcher("job", "db", MatcherType::Eq),
                ],
                0,
                100,
            ))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].samples[0].value, 42.0);

        // No match.
        let results = engine
            .search(&search_req(
                "m1",
                vec![matcher("code", "500", MatcherType::Eq)],
                0,
                100,
            ))
            .await
            .unwrap();
        assert!(results.is_empty());

        // Unknown metric.
        let results = engine
            .search(&search_req("m404", vec![], 0, 100))
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_search_by_neq_matcher_and_time_range() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine(&root).await;
        prepare_data(&engine).await;

        // code!=200 in m1 leaves B only.
        let results = engine
            .search(&search_req(
                "m1",
                vec![matcher("code", "200", MatcherType::Neq)],
                0,
                100,
            ))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].samples[0].value, 10.0);

        // No matchers returns all series of the metric.
        let results = engine
            .search(&search_req("m1", vec![], 0, 100))
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        // Time range clipping: [20, 30) keeps samples at 20 only.
        let results = engine
            .search(&search_req(
                "m1",
                vec![matcher("code", "200", MatcherType::Eq)],
                20,
                30,
            ))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].samples.len(), 1);
        assert_eq!(results[0].samples[0].timestamp, 20);
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_series_key_cache_sees_new_series() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine(&root).await;
        engine
            .write(vec![make_sample("m1", "200", "api", 10, 1.0)])
            .await
            .unwrap();
        // Warm the per-metric series-key cache used by the legacy half of
        // DualRead.
        let first = engine
            .search(&search_req("m1", vec![], 0, 100))
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        engine
            .write(vec![make_sample("m1", "404", "api", 10, 2.0)])
            .await
            .unwrap();
        // A new series must invalidate the cache, or it would stay invisible
        // on the legacy path.
        let second = engine
            .search(&search_req("m1", vec![], 0, 100))
            .await
            .unwrap();
        assert_eq!(second.len(), 2);
    }

    #[test]
    fn test_downsample_buckets_and_aggregations() {
        let samples: Vec<SamplePoint> = (0..6)
            .map(|i| SamplePoint {
                timestamp: i * 10,
                value: i as f64,
            })
            .collect();
        let at = |agg| downsample(&samples, 30, agg);

        assert_eq!(
            at(Aggregation::Avg),
            vec![
                SamplePoint {
                    timestamp: 0,
                    value: 1.0
                },
                SamplePoint {
                    timestamp: 30,
                    value: 4.0
                },
            ]
        );
        assert_eq!(at(Aggregation::Sum)[0].value, 3.0);
        assert_eq!(at(Aggregation::Min)[1].value, 3.0);
        assert_eq!(at(Aggregation::Max)[0].value, 2.0);
        assert_eq!(at(Aggregation::Count)[1].value, 3.0);
        assert_eq!(at(Aggregation::Last)[0].value, 2.0);
        assert_eq!(at(Aggregation::Last)[1].timestamp, 30);

        // Buckets are aligned to the epoch, not to the first sample.
        let offset = vec![SamplePoint {
            timestamp: 35,
            value: 7.0,
        }];
        assert_eq!(
            downsample(&offset, 30, Aggregation::Last),
            vec![SamplePoint {
                timestamp: 30,
                value: 7.0
            }]
        );

        assert!(downsample(&[], 30, Aggregation::Avg).is_empty());
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_search_downsampled_by_series() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine_with_mode(&root, IndexMode::DualRead).await;
        prepare_data(&engine).await;

        // A has ts 10, 20, 30 (values 1, 2, 3); a 20 ms step makes two buckets.
        let series = engine
            .search_downsampled(
                &search_req("m1", vec![matcher("code", "200", MatcherType::Eq)], 0, 100),
                20,
                Aggregation::Max,
            )
            .await
            .unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(
            series[0].samples,
            vec![
                SamplePoint {
                    timestamp: 0,
                    value: 1.0
                },
                SamplePoint {
                    timestamp: 20,
                    value: 3.0
                },
            ]
        );

        // A non-positive step is rejected.
        let err = engine
            .search_downsampled(&search_req("m1", vec![], 0, 100), 0, Aggregation::Avg)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("step_ms"));
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_search_limit_bounds_scanned_series() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine_with_mode(&root, IndexMode::DualRead).await;
        prepare_data(&engine).await;

        // m1 has two series (A with 3 samples and a 404/job=api one); a limit
        // of 1 returns exactly one of them, deterministically.
        let mut req = search_req("m1", vec![], 0, 100);
        req.limit = 1;
        let first = engine.search(&req).await.unwrap();
        assert_eq!(first.len(), 1);
        let again = engine.search(&req).await.unwrap();
        assert_eq!(render(&first), render(&again), "limited results are stable");

        // The limit is applied after matching, so a filter still wins.
        let mut filtered = search_req("m1", vec![matcher("code", "200", MatcherType::Eq)], 0, 100);
        filtered.limit = 5;
        let hits = engine.search(&filtered).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].samples.len(), 3);

        // Zero means no limit.
        let all = engine
            .search(&search_req("m1", vec![], 0, 100))
            .await
            .unwrap();
        assert_eq!(all.len(), 2);

        // Last values and downsampled reads honour the limit too.
        let mut last = search_req("m1", vec![], 0, 100);
        last.limit = 1;
        assert_eq!(engine.last_values(&last).await.unwrap().len(), 1);
        assert_eq!(
            engine
                .search_downsampled(&last, 20, Aggregation::Avg)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_last_values_from_cache_and_fallback() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine_with_mode(&root, IndexMode::DualRead).await;
        prepare_data(&engine).await;

        let req = |start, end| {
            search_req(
                "m1",
                vec![matcher("code", "200", MatcherType::Eq)],
                start,
                end,
            )
        };

        // The write path filled the cache: newest sample of A is ts=30.
        let latest = engine.last_values(&req(0, 100)).await.unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].timestamp, 30);
        assert_eq!(latest[0].value, 3.0);
        assert_eq!(latest[0].labels.len(), 2);

        // An out-of-order older sample must not replace the cached newest.
        engine
            .write(vec![make_sample("m1", "200", "api", 5, 99.0)])
            .await
            .unwrap();
        let latest = engine.last_values(&req(0, 100)).await.unwrap();
        assert_eq!(latest[0].timestamp, 30);
        assert_eq!(latest[0].value, 3.0);

        // A cache miss (restart) falls back to the data scan and refills.
        engine.last_cache().clear();
        assert!(engine.last_cache().is_empty());
        let latest = engine.last_values(&req(0, 100)).await.unwrap();
        assert_eq!(latest[0].timestamp, 30);
        assert_eq!(latest[0].value, 3.0);
        assert!(!engine.last_cache().is_empty());

        // A range that excludes the cached newest falls back to the newest
        // sample inside the range.
        let latest = engine.last_values(&req(0, 25)).await.unwrap();
        assert_eq!(latest[0].timestamp, 20);
        assert_eq!(latest[0].value, 2.0);

        // No match and an unknown metric return an empty result.
        let none = engine
            .last_values(&search_req(
                "m1",
                vec![matcher("code", "500", MatcherType::Eq)],
                0,
                100,
            ))
            .await
            .unwrap();
        assert!(none.is_empty());
        let unknown = engine
            .last_values(&search_req("m404", vec![], 0, 100))
            .await
            .unwrap();
        assert!(unknown.is_empty());
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_legacy_union_skipped_for_covered_metric() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine_with_mode(&root, IndexMode::DualRead).await;
        prepare_data(&engine).await;
        engine.indexdb().unwrap().flush().unwrap();

        let req = search_req("m1", vec![matcher("job", "a.*", MatcherType::Re)], 0, 100);
        let first = engine.search(&req).await.unwrap();
        assert_eq!(first.len(), 2);
        // The new index holds every legacy series of m1 on the queried days,
        // so the regex union is skipped instead of decoding every key.
        assert_eq!(engine.index_stats().unwrap().legacy_union_skips(), 1);

        let second = engine.search(&req).await.unwrap();
        assert_eq!(render(&first), render(&second));
        assert_eq!(engine.index_stats().unwrap().legacy_union_skips(), 2);
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_legacy_coverage_requires_indexed_days() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine_with_mode(&root, IndexMode::DualRead).await;
        prepare_data(&engine).await;
        engine.indexdb().unwrap().flush().unwrap();

        let keys = engine.index.series_keys(crate::hash(b"m1")).await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(engine.legacy_covered(b"m1", &keys, 0, 100));
        // A range with no registered day rows is not covered, so a
        // pre-migration legacy series would still be found through the union.
        let far = 100_000_000_000;
        assert!(!engine.legacy_covered(b"m1", &keys, far, far + 100));
        let results = engine
            .search(&search_req(
                "m1",
                vec![matcher("job", "a.*", MatcherType::Re)],
                far,
                far + 100,
            ))
            .await
            .unwrap();
        assert!(results.is_empty());
        assert_eq!(engine.index_stats().unwrap().legacy_union_skips(), 0);
    }

    /// The HTTP API defaults an open upper bound to `i64::MAX`; the index must
    /// clamp the range to the on-disk partitions instead of overflowing
    /// `month_of_millis` or opening hundreds of empty future months. A bounded
    /// range over the written data still resolves.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_search_clamps_open_and_future_bounds() {
        let root = temp_dir::TempDir::new().unwrap();
        let engine = open_engine_with_mode(&root, IndexMode::DualRead).await;
        prepare_data(&engine).await;
        engine.indexdb().unwrap().flush().unwrap();

        // `i64::MAX` end (the server default) and a future start must not panic
        // and must return the same series as the explicit data range.
        let open = engine
            .search(&search_req("m1", vec![], 0, i64::MAX))
            .await
            .unwrap();
        let explicit = engine
            .search(&search_req("m1", vec![], 0, 100))
            .await
            .unwrap();
        assert_eq!(open.len(), explicit.len());
        assert_eq!(open.len(), 2);

        // A range entirely in the future (past every partition) matches
        // nothing.
        let future = engine
            .search(&search_req("m1", vec![], 100_000_000_000, 100_000_100_000))
            .await
            .unwrap();
        assert!(future.is_empty());

        // `last_values` shares the same resolver and must not overflow either.
        let last = engine
            .last_values(&search_req("m1", vec![], 0, i64::MAX))
            .await
            .unwrap();
        assert_eq!(last.len(), 2);
    }

    /// Per-metric retention (roadmap Phase 4): a metric with a retention window
    /// only exposes samples inside `[now - retention, now]` at query time, so
    /// samples older than the window are invisible while recent ones survive —
    /// even when they share a segment with a longer-lived metric.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_per_metric_retention_hides_aged_samples() {
        let root = temp_dir::TempDir::new().unwrap();
        // 10-minute retention for m1, none for m2. Timestamps are relative to
        // real wall-clock so `now_ms()` inside the query matches the data.
        let now = common::now();
        let hour_ms = 3_600_000i64;
        let recent = now; // inside the window
                          // 30 min old: outside the 10-min window, but well
                          // inside the 2h segment and current day partition, so
                          // both samples are stored identically and only the
                          // read-time clamp differs.
        let aged = now - 30 * 60 * 1000;
        let policy = crate::RetentionPolicy::from_ms([(b"m1".to_vec(), 10 * 60 * 1000)]);
        let engine = open_engine_with_mode(&root, IndexMode::DualRead)
            .await
            .with_retention(policy);

        // m1 has one aged sample (code=500) and one recent sample (code=200);
        // m2 has only recent samples and no retention configured.
        engine
            .write(vec![
                make_sample("m1", "500", "api", aged, 1.0),
                make_sample("m1", "200", "api", recent, 2.0),
                make_sample("m2", "200", "db", recent, 42.0),
            ])
            .await
            .unwrap();
        engine.indexdb().unwrap().flush().unwrap();

        // m1: the aged code=500 sample is gone, the recent code=200 survives.
        let m1 = engine
            .search(&search_req("m1", vec![], 0, i64::MAX))
            .await
            .unwrap();
        assert_eq!(m1.len(), 1);
        assert_eq!(m1[0].samples.len(), 1);
        assert_eq!(m1[0].samples[0].timestamp, recent);
        assert_eq!(m1[0].samples[0].value, 2.0);
        // The aged sample is not hiding behind a matcher either.
        let aged_hit = engine
            .search(&search_req(
                "m1",
                vec![matcher("code", "500", MatcherType::Eq)],
                0,
                i64::MAX,
            ))
            .await
            .unwrap();
        assert!(aged_hit.is_empty(), "aged sample must be invisible");

        // m2: no retention configured, so its recent sample is unaffected.
        let m2 = engine
            .search(&search_req("m2", vec![], 0, i64::MAX))
            .await
            .unwrap();
        assert_eq!(m2.len(), 1);
        assert_eq!(m2[0].samples[0].timestamp, recent);

        // last_values honours the same window: m1's most recent sample is the
        // surviving code=200 one; the aged code=500 is never returned.
        let last = engine
            .last_values(&search_req("m1", vec![], 0, i64::MAX))
            .await
            .unwrap();
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].timestamp, recent);
        assert_eq!(last[0].value, 2.0);

        // A window that reaches back past the aged sample (1h retention) keeps
        // both, proving the read-time clamp — not the stored data — is what hid
        // the aged sample under the 10-minute policy. Written into a fresh
        // engine so the two samples are guaranteed present.
        let wide_root = temp_dir::TempDir::new().unwrap();
        let wide = open_engine_with_mode(&wide_root, IndexMode::DualRead)
            .await
            .with_retention(crate::RetentionPolicy::from_ms([(b"m1".to_vec(), hour_ms)]));
        wide.write(vec![
            make_sample("m1", "500", "api", aged, 1.0),
            make_sample("m1", "200", "api", recent, 2.0),
        ])
        .await
        .unwrap();
        let m1_wide = wide
            .search(&search_req("m1", vec![], 0, i64::MAX))
            .await
            .unwrap();
        let total: usize = m1_wide.iter().map(|s| s.samples.len()).sum();
        assert_eq!(total, 2, "wider window keeps both series' samples");
    }

    /// The mergeset index and the legacy inverted index must resolve the same
    /// series for every matcher type, across the time range and label
    /// reconstruction. This is the migration gate in ADR-0001 §12.5.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_index_modes_agree_on_search_results() {
        let legacy_root = temp_dir::TempDir::new().unwrap();
        let new_root = temp_dir::TempDir::new().unwrap();
        let legacy = open_engine_with_mode(&legacy_root, IndexMode::Legacy).await;
        let new = open_engine_with_mode(&new_root, IndexMode::New).await;
        prepare_data(&legacy).await;
        prepare_data(&new).await;

        // Flush both indexDBs so the comparison covers parts as well as the
        // in-memory pending buffer. The legacy engine writes the mergeset too.
        legacy.indexdb().unwrap().flush().unwrap();
        new.indexdb().unwrap().flush().unwrap();

        let cases: Vec<(&str, Vec<LabelMatcher>, i64, i64)> = vec![
            ("m1", vec![], 0, 100),
            ("m1", vec![matcher("code", "200", MatcherType::Eq)], 0, 100),
            ("m1", vec![matcher("code", "200", MatcherType::Neq)], 0, 100),
            ("m1", vec![matcher("job", "a.*", MatcherType::Re)], 0, 100),
            ("m1", vec![matcher("job", "a.*", MatcherType::Nre)], 0, 100),
            ("m1", vec![matcher("code", "2..", MatcherType::Re)], 20, 30),
            (
                "m1",
                vec![
                    matcher("code", "200", MatcherType::Eq),
                    matcher("job", "api", MatcherType::Eq),
                ],
                0,
                100,
            ),
            ("m2", vec![matcher("job", "db", MatcherType::Eq)], 0, 100),
            ("m404", vec![], 0, 100),
        ];

        for (name, matchers, start, end) in cases {
            let left = legacy
                .search(&search_req(name, matchers.clone(), start, end))
                .await
                .unwrap();
            let right = new
                .search(&search_req(name, matchers.clone(), start, end))
                .await
                .unwrap();
            assert_eq!(
                render(&left),
                render(&right),
                "index modes disagree for {name} {matchers:?} [{start}, {end})"
            );
        }
    }

    /// A stable textual form of search results, for equality assertions.
    fn render(results: &[SeriesSamples]) -> Vec<String> {
        results
            .iter()
            .map(|series| {
                let labels: Vec<String> = series
                    .labels
                    .iter()
                    .map(|l| {
                        format!(
                            "{}={}",
                            String::from_utf8_lossy(&l.name),
                            String::from_utf8_lossy(&l.value)
                        )
                    })
                    .collect();
                let samples: Vec<String> = series
                    .samples
                    .iter()
                    .map(|s| format!("{}@{}", s.timestamp, s.value))
                    .collect();
                format!(
                    "{}{{{}}} {}",
                    String::from_utf8_lossy(&series.metric_name),
                    labels.join(","),
                    samples.join(",")
                )
            })
            .collect()
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_ttl_evicts_only_expired_data() {
        let root = temp_dir::TempDir::new().unwrap();
        let mut opts = StorageConfig::default();
        opts.scheduler.ttl = Some(common::ReadableDuration::hours(1));
        let engine = open_engine_with_opts(&root, opts).await;

        let now = common::now();
        let ten_hours_ago = now - 10 * 3600 * 1000;
        // Old samples of A and new samples of B, written separately so they
        // land in different sst files (a file is expired only as a whole).
        engine
            .write(vec![make_sample("m1", "200", "api", ten_hours_ago, 1.0)])
            .await
            .unwrap();
        engine
            .write(vec![make_sample("m1", "404", "api", now, 2.0)])
            .await
            .unwrap();

        engine.compact().await.unwrap();
        // The compaction executor runs asynchronously, poll until the old
        // sample is evicted.
        let mut evicted = false;
        for _ in 0..100 {
            let results = engine
                .search(&search_req(
                    "m1",
                    vec![matcher("code", "200", MatcherType::Eq)],
                    0,
                    now,
                ))
                .await
                .unwrap();
            if results.is_empty() {
                evicted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(evicted, "old data should be evicted by ttl");

        // Recent data survives (end is exclusive, the sample sits at `now`).
        let results = engine
            .search(&search_req(
                "m1",
                vec![matcher("code", "404", MatcherType::Eq)],
                0,
                now + 1,
            ))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);

        // Metadata tables are not TTL-evicted, both series can still be
        // resolved by matchers.
        let results = engine
            .search(&search_req("m1", vec![], 0, now + 1))
            .await
            .unwrap();
        assert_eq!(results.len(), 1, "only the series with samples in range");
        let all = engine
            .search(&search_req("m1", vec![], ten_hours_ago, now + 1))
            .await
            .unwrap();
        assert_eq!(all.len(), 1, "old samples are gone");
    }

    /// The metric engine's data table is exactly the TSF sample schema, so it
    /// must work unchanged when `format = tsf`: write, flush and query both go
    /// through the TSF path (ADR-0002 §7.2).
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_tsf_format_serves_search() {
        use columnar_storage::config::DataFileFormat;

        let root = temp_dir::TempDir::new().unwrap();
        let opts = StorageConfig {
            format: DataFileFormat::Tsf,
            ..Default::default()
        };
        let engine = open_engine_with_opts(&root, opts).await;

        prepare_data(&engine).await;

        let results = engine
            .search(&search_req(
                "m1",
                vec![matcher("code", "200", MatcherType::Eq)],
                0,
                100,
            ))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].samples.len(), 3);
        assert_eq!(results[0].samples[2].value, 3.0);

        // Values written twice to the same key must still collapse to one row.
        engine
            .write(vec![make_sample("m1", "200", "api", 20, 99.0)])
            .await
            .unwrap();
        let results = engine
            .search(&search_req(
                "m1",
                vec![matcher("code", "200", MatcherType::Eq)],
                0,
                100,
            ))
            .await
            .unwrap();
        assert_eq!(results[0].samples.len(), 3);
        let updated = results[0]
            .samples
            .iter()
            .find(|s| s.timestamp == 20)
            .expect("sample at ts=20");
        assert_eq!(updated.value, 99.0);
    }
}
