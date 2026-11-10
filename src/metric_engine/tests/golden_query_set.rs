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

//! Golden query set (roadmap Phase 3 acceptance: "黄金查询集进 CI").
//!
//! A fixed deployment — three metrics, eight series, a day of deterministic
//! samples — is loaded, then a fixed list of queries covering every matcher
//! type, range and instant reads, limits and downsampled steps is answered and
//! compared against goldens stored in
//! `tests/golden/query_set.json`.
//!
//! The goldens are a regression fence, not a specification: they were produced
//! by the current engine and reviewed once, so any behaviour change shows up as
//! a diff instead of silently shifting dashboard output. Set
//! `THINGS_TSDB_UPDATE_GOLDEN=1` to rewrite the file after an intentional
//! change.
//!
//! Everything is deterministic: timestamps come from a fixed base, values from
//! a closed-form function, series order from the canonical series key, and the
//! snapshot normalizes floats to a fixed precision so a last-bit difference in
//! summation does not flap the test.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use columnar_storage::{config::StorageConfig, storage::StorageRuntimes, types::ObjectStoreRef};
use metric_engine::{
    Aggregation, Label, LabelMatcher, MatcherType, MetricEngine, Sample, SearchRequest,
};
use object_store::local::LocalFileSystem;
use serde_json::{json, Value};

/// 2023-11-14T22:13:20Z: hour-aligned, so every rollup bucket in the fixtures
/// is aligned and the golden does not depend on the wall clock.
const BASE: i64 = 1_699_999_200_000;
const MINUTE: i64 = 60_000;
const HOUR: i64 = 3_600_000;

/// One series of the fixture deployment.
struct SeriesSpec {
    metric: &'static str,
    job: &'static str,
    code: &'static str,
    instance: &'static str,
    /// Sample period in milliseconds.
    period: i64,
    /// Number of samples, starting at [`BASE`].
    count: i64,
}

const SERIES: [SeriesSpec; 8] = [
    SeriesSpec {
        metric: "http_requests",
        job: "api",
        code: "200",
        instance: "host-1",
        period: 15_000,
        count: 480,
    },
    SeriesSpec {
        metric: "http_requests",
        job: "api",
        code: "500",
        instance: "host-1",
        period: 15_000,
        count: 480,
    },
    SeriesSpec {
        metric: "http_requests",
        job: "api",
        code: "200",
        instance: "host-2",
        period: 30_000,
        count: 240,
    },
    SeriesSpec {
        metric: "http_requests",
        job: "db",
        code: "200",
        instance: "host-2",
        period: 60_000,
        count: 120,
    },
    SeriesSpec {
        metric: "http_requests",
        job: "db",
        code: "503",
        instance: "host-3",
        period: 30_000,
        count: 240,
    },
    SeriesSpec {
        metric: "cpu_seconds",
        job: "api",
        code: "200",
        instance: "host-1",
        period: 20_000,
        count: 360,
    },
    SeriesSpec {
        metric: "cpu_seconds",
        job: "db",
        code: "200",
        instance: "host-3",
        period: 20_000,
        count: 360,
    },
    SeriesSpec {
        metric: "memory_bytes",
        job: "api",
        code: "200",
        instance: "host-2",
        period: 45_000,
        count: 160,
    },
];

/// A deterministic value in a plausible range: a slow trend plus a fast wobble.
fn value_of(spec: &SeriesSpec, i: i64) -> f64 {
    let seed = spec.job.len() as f64 * 17.0 + spec.code.len() as f64 * 31.0;
    let trend = (i as f64) * 0.25;
    let wobble = ((i as f64 + seed) * 0.7).sin() * 9.0;
    trend + wobble + seed
}

fn samples() -> Vec<Sample> {
    let mut samples = Vec::new();
    for spec in &SERIES {
        for i in 0..spec.count {
            samples.push(Sample {
                name: spec.metric.as_bytes().to_vec(),
                labels: vec![
                    Label {
                        name: b"code".to_vec(),
                        value: spec.code.as_bytes().to_vec(),
                    },
                    Label {
                        name: b"instance".to_vec(),
                        value: spec.instance.as_bytes().to_vec(),
                    },
                    Label {
                        name: b"job".to_vec(),
                        value: spec.job.as_bytes().to_vec(),
                    },
                ],
                timestamp: BASE + i * spec.period,
                value: value_of(spec, i),
                name_id: None,
                series_id: None,
            });
        }
    }
    samples
}

fn matcher(matcher_type: MatcherType, name: &str, value: &str) -> LabelMatcher {
    LabelMatcher {
        matcher_type,
        name: name.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

/// One query of the golden set.
struct Query {
    name: &'static str,
    metric: &'static str,
    matchers: Vec<LabelMatcher>,
    start: i64,
    end: i64,
    limit: usize,
    /// `Some` for a downsampled range read, `None` for a plain range read.
    step: Option<(i64, Aggregation)>,
}

fn queries() -> Vec<Query> {
    let q = |name, metric, matchers, start, end, limit, step| Query {
        name,
        metric,
        matchers,
        start,
        end,
        limit,
        step,
    };
    use Aggregation::*;
    use MatcherType::*;
    vec![
        // Selector shapes: name only, one eq tag, two eq tags, a regex, a
        // negated matcher, and every metric's full series set.
        q(
            "name_only",
            "http_requests",
            vec![],
            BASE,
            BASE + HOUR,
            0,
            None,
        ),
        q(
            "eq_job",
            "http_requests",
            vec![matcher(Eq, "job", "api")],
            BASE,
            BASE + HOUR,
            0,
            None,
        ),
        q(
            "eq_job_and_code",
            "http_requests",
            vec![matcher(Eq, "job", "api"), matcher(Eq, "code", "200")],
            BASE,
            BASE + HOUR,
            0,
            None,
        ),
        q(
            "re_instance",
            "http_requests",
            vec![matcher(Re, "instance", "host-[12]")],
            BASE,
            BASE + HOUR,
            0,
            None,
        ),
        q(
            "neq_code",
            "http_requests",
            vec![matcher(Neq, "code", "200")],
            BASE,
            BASE + HOUR,
            0,
            None,
        ),
        q(
            "nre_job",
            "cpu_seconds",
            vec![matcher(Nre, "job", "api")],
            BASE,
            BASE + HOUR,
            0,
            None,
        ),
        q(
            "single_series",
            "memory_bytes",
            vec![],
            BASE,
            BASE + HOUR,
            0,
            None,
        ),
        // Ranges: a sub-minute window, a mid-bucket start, and an empty range
        // past the data.
        q(
            "window_30s",
            "http_requests",
            vec![matcher(Eq, "job", "api")],
            BASE + 10 * MINUTE,
            BASE + 10 * MINUTE + 30_000,
            0,
            None,
        ),
        q(
            "mid_bucket_start",
            "http_requests",
            vec![matcher(Eq, "instance", "host-1")],
            BASE + 5 * MINUTE + 7_000,
            BASE + 40 * MINUTE,
            0,
            None,
        ),
        q(
            "past_the_data",
            "http_requests",
            vec![],
            BASE + 30 * HOUR,
            BASE + 31 * HOUR,
            0,
            None,
        ),
        // Limit: the first series by series id, not by scan order.
        q(
            "limit_1",
            "http_requests",
            vec![],
            BASE,
            BASE + HOUR,
            1,
            None,
        ),
        // Downsampled reads: every aggregation at 1m, plus minute and hour
        // re-aggregation at 5m and 1h, and an unaligned 90 s step that falls
        // back to raw data.
        q(
            "step_1m_avg",
            "http_requests",
            vec![matcher(Eq, "job", "api")],
            BASE,
            BASE + HOUR,
            0,
            Some((MINUTE, Avg)),
        ),
        q(
            "step_1m_min",
            "http_requests",
            vec![matcher(Eq, "code", "200")],
            BASE,
            BASE + HOUR,
            0,
            Some((MINUTE, Min)),
        ),
        q(
            "step_1m_max",
            "http_requests",
            vec![matcher(Eq, "code", "200")],
            BASE,
            BASE + HOUR,
            0,
            Some((MINUTE, Max)),
        ),
        q(
            "step_1m_sum",
            "http_requests",
            vec![matcher(Eq, "job", "api")],
            BASE,
            BASE + HOUR,
            0,
            Some((MINUTE, Sum)),
        ),
        q(
            "step_1m_count",
            "http_requests",
            vec![matcher(Eq, "job", "api")],
            BASE,
            BASE + HOUR,
            0,
            Some((MINUTE, Count)),
        ),
        q(
            "step_1m_last",
            "http_requests",
            vec![matcher(Eq, "job", "api")],
            BASE,
            BASE + HOUR,
            0,
            Some((MINUTE, Last)),
        ),
        q(
            "step_5m_avg",
            "http_requests",
            vec![],
            BASE,
            BASE + HOUR,
            0,
            Some((5 * MINUTE, Avg)),
        ),
        q(
            "step_1h_avg",
            "http_requests",
            vec![],
            BASE,
            BASE + 2 * HOUR,
            0,
            Some((HOUR, Avg)),
        ),
        q(
            "step_90s_avg",
            "http_requests",
            vec![matcher(Eq, "job", "api")],
            BASE,
            BASE + HOUR,
            0,
            Some((90_000, Avg)),
        ),
    ]
}

/// Rounds a float for the golden comparison, so last-bit summation differences
/// do not flap the test.
fn round(value: f64) -> f64 {
    (value * 1e6).round() / 1e6
}

fn labels_of(labels: &[Label]) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|label| {
            (
                String::from_utf8_lossy(&label.name).into_owned(),
                String::from_utf8_lossy(&label.value).into_owned(),
            )
        })
        .collect()
}

async fn open_engine(root: &Path) -> MetricEngine {
    let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let runtimes = StorageRuntimes::new(Arc::clone(&rt), rt);
    MetricEngine::try_open(
        &root.to_string_lossy(),
        Duration::from_hours(2),
        store,
        StorageConfig::default(),
        runtimes,
    )
    .await
    .expect("open engine")
}

fn request(query: &Query) -> SearchRequest {
    SearchRequest {
        metric_name: query.metric.as_bytes().to_vec(),
        matchers: query.matchers.clone(),
        start_ms: query.start,
        end_ms: query.end,
        limit: query.limit,
    }
}

/// A digest of a point list: it covers every point while keeping the golden
/// file small enough to review.
///
/// The checksum is order-sensitive over the rounded values, so a single
/// changed sample fails the comparison.
fn digest(points: &[(i64, f64)]) -> Value {
    let mut checksum: u64 = 0xcbf29ce484222325;
    for (timestamp, value) in points {
        checksum ^= *timestamp as u64;
        checksum = checksum.wrapping_mul(0x100000001b3);
        checksum ^= round(*value).to_bits();
        checksum = checksum.wrapping_mul(0x100000001b3);
    }
    let points_json: Vec<Value> = points.iter().map(|(t, v)| json!([t, round(*v)])).collect();
    json!({
        "count": points.len(),
        "checksum": checksum,
        "first": points_json.first().cloned(),
        "last": points_json.last().cloned(),
    })
}

/// Answers one query as JSON: series sorted by labels, points by timestamp.
///
/// A downsampled read keeps its points verbatim (a few dozen per series); a raw
/// read is summarised by [`digest`], because a day of raw samples per series
/// would bloat the golden file without making a diff more readable.
async fn answer(engine: &MetricEngine, query: &Query) -> Value {
    let req = request(query);
    let samples = match query.step {
        Some((step, agg)) => engine.search_downsampled(&req, step, agg).await,
        None => engine.search(&req).await,
    }
    .unwrap_or_else(|e| panic!("query {} failed: {e}", query.name));

    let summarise = query.step.is_none();
    let series: Vec<Value> = samples
        .iter()
        .map(|series| {
            let points: Vec<(i64, f64)> = series
                .samples
                .iter()
                .map(|point| (point.timestamp, round(point.value)))
                .collect();
            let mut value = json!({
                "metric": String::from_utf8_lossy(&series.metric_name),
                "labels": labels_of(&series.labels),
            });
            if summarise {
                value["points"] = digest(&points);
            } else {
                value["points"] = Value::Array(
                    points
                        .iter()
                        .map(|(t, v)| json!([t, v]))
                        .collect::<Vec<_>>(),
                );
            }
            value
        })
        .collect();
    json!({ "series": series })
}

fn golden_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/query_set.json")
}

fn load_goldens() -> Value {
    let text = std::fs::read_to_string(golden_path())
        .expect("read tests/golden/query_set.json (run with THINGS_TSDB_UPDATE_GOLDEN=1)");
    serde_json::from_str(&text).expect("parse golden file")
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_golden_query_set() {
    let root = temp_dir::TempDir::new().unwrap();
    let engine = open_engine(root.path()).await;
    engine.write(samples()).await.unwrap();
    // Publishing the rollup buckets is what makes the downsampled queries read
    // the tables instead of raw data.
    engine.flush_all().await.unwrap();

    let mut actual = serde_json::Map::new();
    actual.insert(
        "_comment".to_string(),
        json!(
            "golden answers produced by src/metric_engine/tests/golden_query_set.rs; \
               regenerate with THINGS_TSDB_UPDATE_GOLDEN=1"
        ),
    );
    for query in queries() {
        // Downsampled reads must match the raw path; assert that here so a
        // golden diff also tells us which reader diverged.
        if let Some((step, agg)) = query.step {
            let raw = engine
                .search(&request(&query))
                .await
                .unwrap_or_else(|e| panic!("raw query {} failed: {e}", query.name));
            let expected: Vec<Vec<(i64, f64)>> = raw
                .iter()
                .map(|series| {
                    metric_engine::downsample(&series.samples, step, agg)
                        .into_iter()
                        .map(|point| (point.timestamp, round(point.value)))
                        .collect()
                })
                .collect();
            let got = engine
                .search_downsampled(&request(&query), step, agg)
                .await
                .unwrap_or_else(|e| panic!("rollup query {} failed: {e}", query.name));
            let actual_points: Vec<Vec<(i64, f64)>> = got
                .iter()
                .map(|series| {
                    series
                        .samples
                        .iter()
                        .map(|point| (point.timestamp, round(point.value)))
                        .collect()
                })
                .collect();
            assert_eq!(
                actual_points, expected,
                "query {} disagrees with the raw downsample",
                query.name
            );
        }
        actual.insert(query.name.to_string(), answer(&engine, &query).await);
    }
    let actual = Value::Object(actual);

    if std::env::var("THINGS_TSDB_UPDATE_GOLDEN").is_ok() {
        let path = golden_path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
        eprintln!("rewrote golden file {}", path.display());
        return;
    }

    let golden = load_goldens();
    let mut diffs = Vec::new();
    for query in queries() {
        let want = &golden[query.name];
        let got = &actual[query.name];
        if want != got {
            diffs.push(format!(
                "{}\n  want: {}\n  got:  {}",
                query.name,
                summarise(want),
                summarise(got)
            ));
        }
    }
    assert!(
        diffs.is_empty(),
        "{} of {} golden queries changed:\n{}\n\nIf the change is intended, \
         rerun with THINGS_TSDB_UPDATE_GOLDEN=1 and review the diff.",
        diffs.len(),
        queries().len(),
        diffs.join("\n")
    );
}

/// One-line shape of an answer, long enough to debug a diff without dumping a
/// day of samples.
fn summarise(value: &Value) -> String {
    let Some(series) = value["series"].as_array() else {
        return "<missing>".to_string();
    };
    let mut parts = Vec::with_capacity(series.len());
    for series in series {
        let labels = series["labels"]
            .as_object()
            .map(|labels| {
                labels
                    .iter()
                    .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("?")))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let points = &series["points"];
        let count = points
            .as_array()
            .map_or_else(|| points["count"].as_u64().unwrap_or(0), |p| p.len() as u64);
        parts.push(format!("[{labels}] {count} points"));
    }
    format!("{} series: {}", series.len(), parts.join(" | "))
}

/// The golden set covers every matcher and every aggregation, so a new
/// selector shape cannot silently ship untested.
#[test]
fn test_golden_set_covers_the_query_surface() {
    let all = queries();
    for matcher_type in [
        MatcherType::Eq,
        MatcherType::Neq,
        MatcherType::Re,
        MatcherType::Nre,
    ] {
        assert!(
            all.iter().any(|query| query
                .matchers
                .iter()
                .any(|m| m.matcher_type == matcher_type)),
            "no golden query uses {matcher_type:?}"
        );
    }
    for agg in [
        Aggregation::Avg,
        Aggregation::Min,
        Aggregation::Max,
        Aggregation::Sum,
        Aggregation::Count,
        Aggregation::Last,
    ] {
        assert!(
            all.iter()
                .any(|query| matches!(query.step, Some((_, a)) if a == agg)),
            "no golden query uses {agg:?}"
        );
    }
    assert!(
        all.iter().any(|query| query.limit > 0),
        "no golden query exercises `limit`"
    );
    let names: std::collections::HashSet<_> = all.iter().map(|query| query.name).collect();
    assert_eq!(names.len(), all.len(), "duplicate golden query name");
}
