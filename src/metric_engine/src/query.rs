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
//! 1. resolve label matchers to a series id set via the inverted `index` table
//!    (EQ) and subtraction (NEQ);
//! 2. fetch series keys of the matched series from the `series` table;
//! 3. scan the `data` table by (metric_id, tsid, timestamp range);
//! 4. group samples back into series.
//!
//! The cost is proportional to the matched series and the requested time
//! range, not to the total data volume.

use std::collections::{HashMap, HashSet};

use anyhow::Context;
use arrow::array::{Array, BinaryArray, Float64Array, Int64Array, UInt64Array};
use columnar_storage::storage::ScanRequest;
use datafusion::{
    logical_expr::{col, lit, Expr},
    scalar::ScalarValue,
};
use futures::StreamExt;

use crate::{
    data, index,
    metric::full_range,
    types::{decode_series_key, Label},
    MetricEngine, Result,
};

/// Regex matchers (RE/NRE in Prometheus) are not supported yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatcherType {
    Eq,
    Neq,
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
}

#[derive(Debug, Clone)]
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

/// Above this series count, tsids are filtered in the engine instead of being
/// pushed down as an IN list, to keep the predicate size reasonable.
const IN_LIST_LIMIT: usize = 10_000;

impl MetricEngine {
    pub async fn search(&self, req: &SearchRequest) -> Result<Vec<SeriesSamples>> {
        let metric_id = crate::hash(&req.metric_name);

        // Step 1: resolve matchers to a candidate series id set.
        // `None` means "all series of this metric" so far.
        let mut candidates: Option<HashSet<u64>> = None;
        let mut eq_matchers = Vec::new();
        let mut neq_matchers = Vec::new();
        for matcher in &req.matchers {
            match matcher.matcher_type {
                MatcherType::Eq => eq_matchers.push(matcher),
                MatcherType::Neq => neq_matchers.push(matcher),
            }
        }
        for matcher in &eq_matchers {
            let matched = self.lookup_index(metric_id, matcher).await?;
            candidates = Some(match candidates {
                None => matched,
                Some(current) => current.intersection(&matched).copied().collect(),
            });
        }
        // NEQ matchers need the full series set to subtract from.
        if !neq_matchers.is_empty() && candidates.is_none() {
            let all = self.fetch_all_tsids(metric_id).await?;
            candidates = Some(all);
        }
        for matcher in &neq_matchers {
            let matched = self.lookup_index(metric_id, matcher).await?;
            if let Some(current) = candidates.as_mut() {
                for tsid in matched {
                    current.remove(&tsid);
                }
            }
        }

        // Step 2: fetch series keys for label reconstruction.
        let series_keys = self.fetch_series_keys(metric_id).await?;
        let candidates = match candidates {
            Some(set) => set,
            None => series_keys.keys().copied().collect(),
        };

        // Step 3: scan data.
        let samples_by_tsid = self
            .scan_samples(metric_id, &candidates, req.start_ms, req.end_ms)
            .await?;

        // Step 4: assemble results, ordered by series key for determinism.
        let mut matched: Vec<(Vec<u8>, u64, Vec<SamplePoint>)> = samples_by_tsid
            .into_iter()
            .filter_map(|(tsid, mut samples)| {
                let series_key = series_keys.get(&tsid)?;
                samples.sort_by_key(|s| s.timestamp);
                Some((series_key.clone(), tsid, samples))
            })
            .collect();
        matched.sort_by(|a, b| a.0.cmp(&b.0));

        let mut results = Vec::with_capacity(matched.len());
        for (series_key, _, samples) in matched {
            let (name, labels) = decode_series_key(&series_key).context("decode series key")?;
            results.push(SeriesSamples {
                metric_name: name,
                labels,
                samples,
            });
        }
        Ok(results)
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

    /// All series ids of one metric, from the series table.
    async fn fetch_all_tsids(&self, metric_id: u64) -> Result<HashSet<u64>> {
        let stream = self
            .index
            .series_storage()
            .scan(ScanRequest {
                range: full_range(),
                predicate: vec![
                    col(index::METRIC_ID_COLUMN).eq(lit(ScalarValue::UInt64(Some(metric_id))))
                ],
                projections: None,
            })
            .await?;
        collect_tsids(stream, index::TSID_COLUMN, self.index.series_storage()).await
    }

    /// Series id -> series key of one metric.
    async fn fetch_series_keys(&self, metric_id: u64) -> Result<HashMap<u64, Vec<u8>>> {
        let storage = self.index.series_storage();
        let mut stream = storage
            .scan(ScanRequest {
                range: full_range(),
                predicate: vec![
                    col(index::METRIC_ID_COLUMN).eq(lit(ScalarValue::UInt64(Some(metric_id))))
                ],
                projections: None,
            })
            .await?;

        let mut result = HashMap::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.context("scan series table")?;
            let schema = storage.schema();
            let tsid_idx = schema.index_of(index::TSID_COLUMN).context("find column")?;
            let key_idx = schema
                .index_of(index::SERIES_KEY_COLUMN)
                .context("find column")?;
            let tsids = batch
                .column(tsid_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("tsid column must be UInt64Array");
            let keys = batch
                .column(key_idx)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("series_key column must be BinaryArray");
            for i in 0..batch.num_rows() {
                result.insert(tsids.value(i), keys.value(i).to_vec());
            }
        }
        Ok(result)
    }

    /// Scan samples of the candidate series within the time range.
    async fn scan_samples(
        &self,
        metric_id: u64,
        candidates: &HashSet<u64>,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<u64, Vec<SamplePoint>>> {
        let storage = self.data.storage();
        let mut predicates = vec![
            col(data::METRIC_ID_COLUMN).eq(lit(ScalarValue::UInt64(Some(metric_id)))),
            col(data::TIMESTAMP_COLUMN)
                .gt_eq(lit(ScalarValue::Int64(Some(start_ms))))
                .and(col(data::TIMESTAMP_COLUMN).lt(lit(ScalarValue::Int64(Some(end_ms))))),
        ];
        if candidates.len() <= IN_LIST_LIMIT {
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
                if candidates.len() > IN_LIST_LIMIT && !candidates.contains(&tsid) {
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
        config::StorageConfig,
        storage::{ObjectBasedStorage, StorageRuntimes},
        types::ObjectStoreRef,
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
}
