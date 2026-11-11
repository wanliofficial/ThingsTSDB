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

mod config;
mod metrics;
mod prometheus;
mod promql;

use std::{
    fs,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use actix_web::{
    get, post,
    web::{self, Bytes, Data},
    App, HttpResponse, HttpServer, Responder,
};
use bytes::Bytes as BytesBuf;
use clap::Parser;
use columnar_storage::types::ObjectStoreRef;
use common::now;
use metric_engine::{
    types::Label, Aggregation, LabelMatcher, MatcherType, MetricEngine, MetricEngineRef, Sample,
    SearchRequest,
};
use object_store::local::LocalFileSystem;
use pb_types::prometheus::remote_read::{
    label_matcher::Type as PbMatcherType, Query as PbQuery, ReadRequest, ReadResponse,
};
use prost::Message;
use remote_write::{pooled_parser::PooledParser, pooled_types::WriteRequest as RemoteWriteRequest};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::{
    config::{Config, ObjectStorageConfig},
    metrics::{QueryKind, ServerMetrics},
};

#[derive(Parser, Debug)]
#[command(version, about, long_about)]
struct Args {
    /// Config file path
    #[arg(short, long)]
    config: String,
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[get("/toggle")]
async fn toggle(data: web::Data<AppState>) -> impl Responder {
    let prev = data.keep_writing.fetch_not(Ordering::Relaxed);
    if prev {
        HttpResponse::Ok().body("Stop!")
    } else {
        HttpResponse::Ok().body("Start write again!")
    }
}

#[get("/compact")]
async fn compact(data: web::Data<AppState>) -> impl Responder {
    if let Err(e) = data.backend.compact().await {
        error!("compact failed, err:{e}");
    }
    HttpResponse::Ok().body("Task submit!")
}

/// Ingest endpoint for Prometheus remote write (v1): the body is a
/// snappy-compressed protobuf `WriteRequest`.
#[post("/api/v1/write")]
async fn prometheus_write(body: Bytes, data: web::Data<AppState>) -> impl Responder {
    let raw = match snap::raw::Decoder::new().decompress_vec(&body) {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest().body(format!("decompress failed: {e}"));
        }
    };
    let request = match PooledParser.decode(BytesBuf::from(raw)) {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest().body(format!("decode failed: {e}"));
        }
    };

    let samples = convert_request(&request);
    if samples.is_empty() {
        return HttpResponse::NoContent().finish();
    }
    let num_series = request.timeseries.len();
    drop(request);

    match data.backend.write(samples).await {
        Ok(()) => {
            info!(num_series, "Remote write handled");
            HttpResponse::NoContent().finish()
        }
        Err(e) => {
            error!("write failed, err:{e}");
            HttpResponse::InternalServerError().body(format!("write failed: {e}"))
        }
    }
}

/// Query endpoint for Prometheus remote read (v1): the body is a
/// snappy-compressed protobuf `ReadRequest`, and so is the response.
#[post("/api/v1/read")]
async fn prometheus_read(body: Bytes, data: web::Data<AppState>) -> impl Responder {
    let raw = match snap::raw::Decoder::new().decompress_vec(&body) {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest().body(format!("decompress failed: {e}"));
        }
    };
    let request = match ReadRequest::decode(raw.as_ref()) {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest().body(format!("decode failed: {e}"));
        }
    };
    if request.response_type
        == pb_types::prometheus::remote_read::read_request::ResponseType::StreamedXorChunks as i32
    {
        return HttpResponse::NotImplemented().body("streamed xor chunks not supported");
    }

    let mut results = Vec::with_capacity(request.queries.len());
    for query in &request.queries {
        match run_read_query(&data.backend, query).await {
            Ok(timeseries) => {
                results.push(pb_types::prometheus::remote_read::QueryResult { timeseries })
            }
            Err(e) => {
                return HttpResponse::BadRequest().body(format!("query failed: {e}"));
            }
        }
    }
    let response = ReadResponse { results };
    let mut buf = Vec::with_capacity(response.encoded_len());
    response.encode(&mut buf).expect("encode read response");
    let compressed = match snap::raw::Encoder::new().compress_vec(&buf) {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!("compress failed: {e}"));
        }
    };
    info!(num_queries = request.queries.len(), "Remote read handled");
    HttpResponse::Ok()
        .content_type("application/x-protobuf")
        .insert_header(("Content-Encoding", "snappy"))
        .body(compressed)
}

/// Run one remote read query: matchers are resolved to series, then samples
/// within `[start_timestamp_ms, end_timestamp_ms]` (inclusive, Prometheus
/// semantics) are returned.
async fn run_read_query(
    backend: &Backend,
    query: &PbQuery,
) -> metric_engine::Result<Vec<pb_types::prometheus::remote_write::TimeSeries>> {
    let mut metric_name: Option<String> = None;
    let mut matchers = Vec::new();
    for matcher in &query.matchers {
        let matcher_type = match matcher.r#type() {
            PbMatcherType::Eq => MatcherType::Eq,
            PbMatcherType::Neq => MatcherType::Neq,
            PbMatcherType::Re => MatcherType::Re,
            PbMatcherType::Nre => MatcherType::Nre,
        };
        if matcher.name == "__name__" {
            if matcher_type != MatcherType::Eq {
                return Err(anyhow::anyhow!("__name__ matcher must be EQ").into());
            }
            metric_name = Some(matcher.value.clone());
        } else {
            matchers.push(LabelMatcher {
                matcher_type,
                name: matcher.name.as_bytes().to_vec(),
                value: matcher.value.as_bytes().to_vec(),
            });
        }
    }
    let metric_name = metric_name.ok_or_else(|| anyhow::anyhow!("__name__ matcher is required"))?;

    let request = SearchRequest {
        metric_name: metric_name.into_bytes(),
        matchers,
        // Prometheus range is inclusive on both ends, the engine takes
        // a half-open range.
        start_ms: query.start_timestamp_ms,
        end_ms: query.end_timestamp_ms + 1,
        limit: 0,
    };
    let series = backend.search(&request).await?;
    let timeseries = series
        .into_iter()
        .map(|s| pb_types::prometheus::remote_write::TimeSeries {
            labels: series_labels(&s.metric_name, &s.labels),
            samples: s
                .samples
                .into_iter()
                .map(|p| pb_types::prometheus::remote_write::Sample {
                    value: p.value,
                    timestamp: p.timestamp,
                })
                .collect(),
            exemplars: Vec::new(),
        })
        .collect();
    Ok(timeseries)
}

/// Rebuild the label set of one series, with `__name__` first.
fn series_labels(
    metric_name: &[u8],
    labels: &[Label],
) -> Vec<pb_types::prometheus::remote_write::Label> {
    let mut result = vec![pb_types::prometheus::remote_write::Label {
        name: "__name__".to_string(),
        value: String::from_utf8_lossy(metric_name).into_owned(),
    }];
    for label in labels {
        result.push(pb_types::prometheus::remote_write::Label {
            name: String::from_utf8_lossy(&label.name).into_owned(),
            value: String::from_utf8_lossy(&label.value).into_owned(),
        });
    }
    result
}

/// Human-friendly JSON query endpoint for debugging and ad-hoc checks, e.g.
/// `/api/v1/query?metric=bench_cpu_usage&start=0&end=9999999999999&filter=job:
/// api,code:200`.
#[get("/api/v1/query")]
async fn query_json(
    data: web::Data<AppState>,
    params: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let request = match parse_search_request(&params) {
        Ok(request) => request,
        Err(message) => {
            data.metrics.record_rejected(QueryKind::RANGE);
            return HttpResponse::BadRequest().body(message);
        }
    };
    let started = Instant::now();
    match data.backend.search(&request).await {
        Ok(series) => {
            let samples = series.iter().map(|s| s.samples.len() as u64).sum::<u64>();
            data.metrics.observe(
                QueryKind::RANGE,
                started.elapsed(),
                series.len() as u64,
                samples,
                &format!(
                    "range query metric={} matchers={} series={}",
                    String::from_utf8_lossy(&request.metric_name),
                    request.matchers.len(),
                    series.len()
                ),
            );
            let series_json: Vec<serde_json::Value> = series
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "metric": String::from_utf8_lossy(&s.metric_name),
                        "labels": serde_json::Map::from_iter(s.labels.iter().map(|l| (
                            String::from_utf8_lossy(&l.name).into_owned(),
                            serde_json::Value::String(String::from_utf8_lossy(&l.value).into_owned()),
                        ))),
                        "samples": s.samples.iter().map(|p| (
                            p.timestamp, p.value
                        )).collect::<Vec<_>>(),
                    })
                })
                .collect();
            HttpResponse::Ok().json(serde_json::json!({ "series": series_json }))
        }
        Err(e) => {
            data.metrics.record_failed(QueryKind::RANGE);
            HttpResponse::InternalServerError().body(format!("query failed: {e}"))
        }
    }
}

/// Latest value of every matched series, e.g.
/// `/api/v1/last?metric=bench_cpu_usage&filter=job:api`. Served from the
/// per-series last-value cache; a cold cache falls back to a data scan.
#[get("/api/v1/last")]
async fn last_json(
    data: web::Data<AppState>,
    params: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let request = match parse_search_request(&params) {
        Ok(request) => request,
        Err(message) => {
            data.metrics.record_rejected(QueryKind::LAST);
            return HttpResponse::BadRequest().body(message);
        }
    };
    let started = Instant::now();
    match data.backend.last_values(&request).await {
        Ok(series) => {
            data.metrics.observe(
                QueryKind::LAST,
                started.elapsed(),
                series.len() as u64,
                series.len() as u64,
                &format!(
                    "last query metric={} matchers={} series={}",
                    String::from_utf8_lossy(&request.metric_name),
                    request.matchers.len(),
                    series.len()
                ),
            );
            let series_json: Vec<serde_json::Value> = series
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "metric": String::from_utf8_lossy(&s.metric_name),
                        "labels": serde_json::Map::from_iter(s.labels.iter().map(|l| (
                            String::from_utf8_lossy(&l.name).into_owned(),
                            serde_json::Value::String(String::from_utf8_lossy(&l.value).into_owned()),
                        ))),
                        "timestamp": s.timestamp,
                        "value": s.value,
                    })
                })
                .collect();
            HttpResponse::Ok().json(serde_json::json!({ "series": series_json }))
        }
        Err(e) => {
            data.metrics.record_failed(QueryKind::LAST);
            HttpResponse::InternalServerError().body(format!("last query failed: {e}"))
        }
    }
}

/// Downsampled range query, e.g.
/// `/api/v1/query_range?metric=http_requests&step=60000&agg=avg`.
///
/// `step` is in milliseconds and `agg` is one of `avg`, `min`, `max`, `sum`,
/// `count`, `last` (default `avg`).
#[get("/api/v1/query_range")]
async fn query_range_json(
    data: web::Data<AppState>,
    params: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let request = match parse_search_request(&params) {
        Ok(request) => request,
        Err(message) => {
            data.metrics.record_rejected(QueryKind::RANGE_STEP);
            return HttpResponse::BadRequest().body(message);
        }
    };
    let step: i64 = match params.get("step").map(|v| v.parse()).transpose() {
        Ok(Some(step)) if step > 0 => step,
        _ => {
            data.metrics.record_rejected(QueryKind::RANGE_STEP);
            return HttpResponse::BadRequest().body("step must be a positive integer (ms)");
        }
    };
    let agg = match params.get("agg").map(String::as_str).unwrap_or("avg") {
        "avg" => Aggregation::Avg,
        "min" => Aggregation::Min,
        "max" => Aggregation::Max,
        "sum" => Aggregation::Sum,
        "count" => Aggregation::Count,
        "last" => Aggregation::Last,
        other => {
            data.metrics.record_rejected(QueryKind::RANGE_STEP);
            return HttpResponse::BadRequest().body(format!("unknown aggregation {other:?}"));
        }
    };
    let started = Instant::now();
    match data.backend.search_downsampled(&request, step, agg).await {
        Ok(series) => {
            let samples = series.iter().map(|s| s.samples.len() as u64).sum::<u64>();
            data.metrics.observe(
                QueryKind::RANGE_STEP,
                started.elapsed(),
                series.len() as u64,
                samples,
                &format!(
                    "range_step query metric={} step={}ms series={}",
                    String::from_utf8_lossy(&request.metric_name),
                    step,
                    series.len()
                ),
            );
            let series_json: Vec<serde_json::Value> = series
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "metric": String::from_utf8_lossy(&s.metric_name),
                        "labels": serde_json::Map::from_iter(s.labels.iter().map(|l| (
                            String::from_utf8_lossy(&l.name).into_owned(),
                            serde_json::Value::String(String::from_utf8_lossy(&l.value).into_owned()),
                        ))),
                        "samples": s.samples.iter().map(|p| (
                            p.timestamp, p.value
                        )).collect::<Vec<_>>(),
                    })
                })
                .collect();
            HttpResponse::Ok().json(serde_json::json!({ "series": series_json }))
        }
        Err(e) => {
            data.metrics.record_failed(QueryKind::RANGE_STEP);
            HttpResponse::InternalServerError().body(format!("range query failed: {e}"))
        }
    }
}

/// Query observability: counters, a latency histogram and quantiles for the
/// query endpoints, plus the slow-query log. Returns Prometheus text exposition
/// (roadmap Phase 4, observability).
#[get("/api/v1/stats")]
async fn stats_json(data: web::Data<AppState>) -> impl Responder {
    let mut body = String::from("# HoraeDB query metrics\n");
    for kind in 0..QueryKind::COUNT {
        if let Some(q) = data.metrics.latency_quantiles(kind) {
            body.push_str(&format!(
                "# kind={} count={} avg_us={} p50_us={} p99_us={}\n",
                QueryKind::name(kind),
                q.count,
                q.avg_us,
                q.p50_us,
                q.p99_us
            ));
        }
    }
    body.push('\n');
    body.push_str(&data.metrics.render_prometheus());
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .body(body)
}

/// The static topology this process is serving, plus the placement it derives
/// from it. `/api/v1/cluster` is the operator's view of where every shard
/// lives (roadmap Phase 4: sharding, replication and rebalancing).
#[get("/api/v1/cluster")]
async fn cluster_json(data: web::Data<AppState>) -> impl Responder {
    let Some(map) = &data.cluster_map else {
        return HttpResponse::Ok().json(serde_json::json!({
            "mode": "single",
            "root": data.root,
        }));
    };
    let shards: Vec<serde_json::Value> = (0..map.shard_count())
        .map(|shard| {
            serde_json::json!({
                "shard": shard,
                "owners": map.owners(shard),
            })
        })
        .collect();
    HttpResponse::Ok().json(serde_json::json!({
        "mode": "cluster",
        "root": data.root,
        "nodes": map.nodes(),
        "shards": map.shard_count(),
        "replicas": map.replicas(),
        "placement": shards,
    }))
}

/// Repairs the cluster: flushes every replica to a durable point, then copies
/// any shard prefix a replica is missing from one that has it.
///
/// `POST /api/v1/rebalance` with no parameters refills the *current* topology,
/// which is what a node that lost a directory or fell behind needs. Pass
/// `nodes=a,b,c` (optionally `shards=`, `replicas=`) to plan against a new one:
/// the copies are made, but serving the new placement needs a restart, since
/// the open engines are fixed at startup. `plan=1` only reports what would be
/// copied.
#[post("/api/v1/rebalance")]
async fn rebalance_endpoint(
    data: web::Data<AppState>,
    params: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let Some(current) = &data.cluster_map else {
        return HttpResponse::BadRequest().body("this server does not run a cluster");
    };
    let plan_only = matches!(
        params.get("plan").map(String::as_str),
        Some("1") | Some("true")
    );

    let target = match params.get("nodes") {
        Some(nodes) => {
            let config = metric_engine::ClusterConfig::new(
                nodes
                    .split(',')
                    .map(str::trim)
                    .filter(|n| !n.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
                params
                    .get("shards")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| current.shard_count()),
                params
                    .get("replicas")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| current.replicas()),
            );
            match metric_engine::ShardMap::new(config) {
                Ok(map) => map,
                Err(e) => return HttpResponse::BadRequest().body(format!("bad topology: {e}")),
            }
        }
        None => current.clone(),
    };

    // A copy has to see a stable object set, so publish every buffered sample
    // first: a part that is still only in memory would not be copied.
    if !plan_only {
        if let Err(e) = data.backend.flush_all().await {
            error!("rebalance flush failed, err:{e}");
            return HttpResponse::InternalServerError()
                .body(format!("flush before rebalance failed: {e}"));
        }
    }

    if plan_only {
        // `Migration` is a plain data struct in the engine crate, which does
        // not depend on serde, so build the JSON here.
        let migrations: Vec<serde_json::Value> = target
            .migrations(current)
            .into_iter()
            .map(|m| {
                serde_json::json!({
                    "shard": m.shard,
                    "added": m.added,
                    "removed": m.removed,
                })
            })
            .collect();
        return HttpResponse::Ok().json(serde_json::json!({
            "shards_to_copy": migrations.len(),
            "plan": migrations,
            "restart_required": target != *current,
        }));
    }

    let report =
        match metric_engine::rebalance_from_disk(data.store.as_ref(), &data.root, &target).await {
            Ok(report) => report,
            Err(e) => {
                error!("rebalance failed, err:{e}");
                return HttpResponse::InternalServerError().body(format!("rebalance failed: {e}"));
            }
        };
    info!(
        shards = report.shards_moved,
        objects = report.objects,
        bytes = report.bytes,
        "Rebalance finished"
    );
    HttpResponse::Ok().json(serde_json::json!({
        "shards_copied": report.shards_moved,
        "objects": report.objects,
        "bytes": report.bytes,
        "restart_required": target != *current,
    }))
}

/// Parses `metric`, `start`, `end` and an exact-match `filter` list such as
/// `job:api,code:200`.
fn parse_search_request(
    params: &std::collections::HashMap<String, String>,
) -> std::result::Result<SearchRequest, String> {
    let Some(metric) = params.get("metric") else {
        return Err("metric is required".to_string());
    };
    let start: i64 = params
        .get("start")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let end: i64 = params
        .get("end")
        .and_then(|v| v.parse().ok())
        .unwrap_or(i64::MAX);
    let matchers = params
        .get("filter")
        .map(|filter| {
            filter
                .split(',')
                .filter_map(|pair| pair.split_once(':'))
                .map(|(name, value)| LabelMatcher {
                    matcher_type: MatcherType::Eq,
                    name: name.as_bytes().to_vec(),
                    value: value.as_bytes().to_vec(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let limit: usize = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    Ok(SearchRequest {
        metric_name: metric.as_bytes().to_vec(),
        matchers,
        start_ms: start,
        end_ms: end,
        limit,
    })
}

/// Convert the remote write request to engine samples: the metric name is the
/// `__name__` label, other labels identify the series.
fn convert_request(request: &RemoteWriteRequest) -> Vec<Sample> {
    let mut samples = Vec::new();
    for timeseries in request.timeseries.iter() {
        let Some(metric_name) = timeseries
            .labels
            .iter()
            .find(|l| l.name.as_ref() == b"__name__")
            .map(|l| l.value.clone())
        else {
            warn!("Skip timeseries without __name__ label");
            continue;
        };
        let labels: Vec<Label> = timeseries
            .labels
            .iter()
            .filter(|l| l.name.as_ref() != b"__name__")
            .map(|l| Label {
                name: l.name.to_vec(),
                value: l.value.to_vec(),
            })
            .collect();
        for sample in timeseries.samples.iter() {
            samples.push(Sample {
                name: metric_name.to_vec(),
                labels: labels.clone(),
                timestamp: sample.timestamp,
                value: sample.value,
                name_id: None,
                series_id: None,
            });
        }
    }
    samples
}

/// The query/write surface the HTTP handlers talk to.
///
/// A deployment is either one engine (`[metric_engine.cluster]` absent) or a
/// static cluster of `(node, shard)` engines. The handlers only ever see this
/// enum, so the endpoints do not care which one is behind them.
#[derive(Clone)]
pub(crate) enum Backend {
    Single(MetricEngineRef),
    Cluster(Arc<metric_engine::Cluster>),
}

impl Backend {
    async fn write(&self, samples: Vec<Sample>) -> metric_engine::Result<()> {
        match self {
            Backend::Single(engine) => engine.write(samples).await,
            Backend::Cluster(cluster) => {
                let report = cluster.write(samples).await?;
                if report.failed > 0 {
                    warn!(
                        samples = report.samples,
                        shards = report.shards,
                        writes = report.writes,
                        failed = report.failed,
                        "replicated write degraded: a replica did not acknowledge"
                    );
                }
                Ok(())
            }
        }
    }

    async fn search(
        &self,
        request: &SearchRequest,
    ) -> metric_engine::Result<Vec<metric_engine::SeriesSamples>> {
        match self {
            Backend::Single(engine) => engine.search(request).await,
            Backend::Cluster(cluster) => cluster.search(request).await,
        }
    }

    async fn last_values(
        &self,
        request: &SearchRequest,
    ) -> metric_engine::Result<Vec<metric_engine::SeriesLatest>> {
        match self {
            Backend::Single(engine) => engine.last_values(request).await,
            Backend::Cluster(cluster) => cluster.last_values(request).await,
        }
    }

    async fn search_downsampled(
        &self,
        request: &SearchRequest,
        step_ms: i64,
        agg: Aggregation,
    ) -> metric_engine::Result<Vec<metric_engine::SeriesSamples>> {
        match self {
            Backend::Single(engine) => engine.search_downsampled(request, step_ms, agg).await,
            Backend::Cluster(cluster) => cluster.search_downsampled(request, step_ms, agg).await,
        }
    }

    async fn compact(&self) -> metric_engine::Result<()> {
        match self {
            Backend::Single(engine) => engine.compact().await,
            Backend::Cluster(cluster) => cluster.compact().await,
        }
    }

    async fn flush_all(&self) -> metric_engine::Result<()> {
        match self {
            Backend::Single(engine) => engine.flush_all().await,
            Backend::Cluster(cluster) => cluster.flush_all().await,
        }
    }
}

pub(crate) struct AppState {
    pub(crate) backend: Backend,
    /// The static topology, when the server runs as a cluster. Kept so the
    /// rebalance endpoint can recompute placement.
    pub(crate) cluster_map: Option<metric_engine::ShardMap>,
    /// Object store and engine root, needed by the rebalance endpoint.
    pub(crate) store: ObjectStoreRef,
    pub(crate) root: String,
    pub(crate) keep_writing: Arc<AtomicBool>,
    pub(crate) metrics: ServerMetrics,
}

pub fn main() {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_env_filter(EnvFilter::from_default_env())
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .init();

    let args = Args::parse();
    let config_body = fs::read_to_string(args.config).expect("read config file failed");
    let config: Config = toml::from_str(&config_body).unwrap();
    info!("Config loaded: \n{:#?}", config);

    let port = config.port;
    let rt = build_multi_runtime("main", 1);
    let manifest_compact_runtime = build_multi_runtime(
        "manifest-compact",
        config.metric_engine.threads.manifest_thread_num,
    );
    let sst_compact_runtime =
        build_multi_runtime("sst-compact", config.metric_engine.threads.sst_thread_num);
    let runtimes = columnar_storage::storage::StorageRuntimes::new(
        manifest_compact_runtime,
        sst_compact_runtime,
    );
    let object_store_config = match config.metric_engine.storage.object_store {
        ObjectStorageConfig::Local(v) => v,
        ObjectStorageConfig::S3Like(_) => panic!("S3 not support yet"),
    };
    let storage_opts = config.metric_engine.storage.time_merge_storage;
    let segment_duration = config.metric_engine.segment_duration.0;
    let index_mode = config.metric_engine.index_mode.into();
    let retention: metric_engine::RetentionPolicy = (&config.metric_engine.retention).into();

    let write_worker_num = config.test.write_worker_num;
    let write_interval = config.test.write_interval.0;
    let enable_write = config.test.enable_write;
    let write_rt = build_multi_runtime("write", write_worker_num);
    let keep_writing = Arc::new(AtomicBool::new(true));
    let cluster_config = config.metric_engine.cluster.clone();
    let data_dir = object_store_config.data_dir.clone();
    let _ = rt.block_on(async move {
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        // A configured cluster replaces the single engine: every `(node,
        // shard)` pair is an engine rooted at `{data_dir}/{node}/shard-{n}`,
        // and the handlers go through `Backend`.
        let (backend, cluster_map) = match cluster_config {
            Some(ref cluster) => {
                let map = metric_engine::ShardMap::new(cluster.into()).unwrap();
                let opened = metric_engine::Cluster::open_with_retention(
                    &data_dir,
                    store.clone(),
                    storage_opts,
                    runtimes,
                    map.clone(),
                    retention,
                )
                .await
                .unwrap();
                info!(
                    nodes = map.nodes().len(),
                    shards = map.shard_count(),
                    replicas = map.replicas(),
                    "Cluster opened"
                );
                (Backend::Cluster(Arc::new(opened)), Some(map))
            }
            None => {
                let engine = Arc::new(
                    MetricEngine::try_open_with_index_mode(
                        &data_dir,
                        segment_duration,
                        store.clone(),
                        storage_opts,
                        runtimes,
                        index_mode,
                    )
                    .await
                    .unwrap()
                    .with_retention(retention),
                );
                (Backend::Single(engine), None)
            }
        };

        if enable_write {
            bench_write(
                backend.clone(),
                write_rt.clone(),
                write_worker_num,
                write_interval,
                keep_writing.clone(),
            );
        }

        let app_state = Data::new(AppState {
            backend,
            cluster_map,
            store,
            root: data_dir,
            keep_writing,
            metrics: ServerMetrics::default(),
        });
        info!(port, "Start HoraeDB http server...");
        HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                // Remote write requests can be several megabytes, allow a
                // larger body than the actix default.
                .app_data(web::PayloadConfig::default().limit(256 * 1024 * 1024))
                .service(hello)
                .service(compact)
                .service(toggle)
                .service(prometheus_write)
                .service(prometheus_read)
                .service(query_json)
                .service(last_json)
                .service(query_range_json)
                .service(stats_json)
                .service(cluster_json)
                .service(rebalance_endpoint)
                // Prometheus HTTP API, so Grafana's Prometheus data source can
                // read this server directly (roadmap Phase 3).
                .service(prometheus::query_endpoint)
                .service(prometheus::query_range_endpoint)
                .service(prometheus::series_endpoint)
                .service(prometheus::labels_endpoint)
                .service(prometheus::label_values_endpoint)
        })
        .workers(4)
        .bind(("127.0.0.1", port))
        .expect("Server bind failed")
        .run()
        .await
    });
}

fn build_multi_runtime(name: &str, workers: usize) -> columnar_storage::types::RuntimeRef {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name(name)
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    Arc::new(rt)
}

/// Write synthetic samples through the backend, for benchmark and demo.
fn bench_write(
    backend: Backend,
    rt: columnar_storage::types::RuntimeRef,
    workers: usize,
    interval: std::time::Duration,
    keep_writing: Arc<AtomicBool>,
) {
    const METRIC_NAMES: [&[u8]; 3] = [b"bench_cpu_usage", b"bench_mem_usage", b"bench_qps"];
    const INSTANCES: [&[u8]; 4] = [b"10.0.0.1", b"10.0.0.2", b"10.0.0.3", b"10.0.0.4"];

    for worker_id in 0..workers {
        let backend = backend.clone();
        let keep_writing = keep_writing.clone();
        rt.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if !keep_writing.load(Ordering::Relaxed) {
                    continue;
                }

                let timestamp = now();
                let mut samples = Vec::new();
                for metric in METRIC_NAMES {
                    for instance in INSTANCES {
                        samples.push(Sample {
                            name: metric.to_vec(),
                            labels: vec![Label {
                                name: b"instance".to_vec(),
                                value: instance.to_vec(),
                            }],
                            timestamp,
                            value: rand::random::<f64>(),
                            name_id: None,
                            series_id: None,
                        });
                    }
                }
                // Series count: one worker writes all series, extra workers
                // only add load with the same series.
                if worker_id > 0 {
                    // Keep batches from different workers distinguishable.
                    for sample in samples.iter_mut() {
                        sample.timestamp += worker_id as i64;
                    }
                }
                if let Err(e) = backend.write(samples).await {
                    error!("bench write failed, err:{}", e);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use columnar_storage::{
        config::StorageConfig, storage::StorageRuntimes, types::ObjectStoreRef,
    };
    use metric_engine::{IndexMode, Label};
    use object_store::local::LocalFileSystem;
    use serde_json::Value;

    use super::*;

    /// Opens a single-engine backend over a scratch directory.
    async fn backend(root: &str) -> Backend {
        let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let runtimes = StorageRuntimes::new(rt.clone(), rt);
        let store: ObjectStoreRef = Arc::new(LocalFileSystem::new());
        let engine = MetricEngine::try_open_with_index_mode(
            root,
            std::time::Duration::from_secs(8 * 3600),
            store,
            StorageConfig::default(),
            runtimes,
            IndexMode::New,
        )
        .await
        .unwrap();
        Backend::Single(Arc::new(engine))
    }

    fn state(backend: Backend, root: &str) -> AppState {
        AppState {
            backend,
            cluster_map: None,
            store: Arc::new(LocalFileSystem::new()),
            root: root.to_string(),
            keep_writing: Arc::new(AtomicBool::new(false)),
            metrics: ServerMetrics::default(),
        }
    }

    /// Percent-encodes a query-string value (actix needs the URI already
    /// escaped, since a selector contains `{`, `"` and `,`).
    fn encode(raw: &str) -> String {
        let mut out = String::new();
        for byte in raw.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(byte as char)
                }
                _ => out.push_str(&format!("%{byte:02X}")),
            }
        }
        out
    }

    fn qs(pairs: &[(&str, &str)]) -> String {
        pairs
            .iter()
            .map(|(k, v)| format!("{k}={}", encode(v)))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// The Prometheus endpoints must answer real engine data, not just parse a
    /// query: this writes two series and checks the instant, range, series and
    /// label answers Grafana would consume. Requests go through the actix
    /// router, so path and query-parameter wiring is covered too.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_prometheus_endpoints_serve_engine_data() {
        let dir = temp_dir::TempDir::new().unwrap();
        let root = dir.path().to_string_lossy().into_owned();
        let backend = backend(&root).await;
        let now = common::now();
        let samples: Vec<Sample> = ["api", "db"]
            .iter()
            .enumerate()
            .flat_map(|(i, job)| {
                (0..4).map(move |t| Sample {
                    name: b"http_requests".to_vec(),
                    labels: vec![Label {
                        name: b"job".to_vec(),
                        value: job.as_bytes().to_vec(),
                    }],
                    timestamp: now - (4 - t) * 10_000,
                    value: (i * 10 + t as usize) as f64,
                    name_id: None,
                    series_id: None,
                })
            })
            .collect();
        backend.write(samples).await.unwrap();
        backend.flush_all().await.unwrap();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(Data::new(state(backend, &root)))
                .service(prometheus::query_endpoint)
                .service(prometheus::query_range_endpoint)
                .service(prometheus::series_endpoint)
                .service(prometheus::labels_endpoint)
                .service(prometheus::label_values_endpoint),
        )
        .await;

        let now_s = now as f64 / 1_000.0;
        let start_s = (now - 60_000) as f64 / 1_000.0;
        let time = now_s.to_string();
        let start = start_s.to_string();

        // Instant query: one series, `vector` shape, value as a string.
        let body: Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/query?{}",
                    qs(&[("query", r#"http_requests{job="api"}"#), ("time", &time),])
                ))
                .to_request(),
        )
        .await;
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "vector");
        assert_eq!(body["data"]["result"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["data"]["result"][0]["metric"]["__name__"],
            "http_requests"
        );
        assert_eq!(body["data"]["result"][0]["metric"]["job"], "api");

        // Range query: every series, `matrix` shape, points never past `end`.
        let body: Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/query_range?{}",
                    qs(&[
                        ("query", "http_requests"),
                        ("start", &start),
                        ("end", &time),
                        ("step", "10"),
                    ])
                ))
                .to_request(),
        )
        .await;
        assert_eq!(body["data"]["resultType"], "matrix");
        let result = body["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 2);
        for series in result {
            for point in series["values"].as_array().unwrap() {
                assert!(point[0].as_f64().unwrap() <= now_s);
            }
        }

        // Label discovery, as a Grafana template variable would use it.
        let body: Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/label/job/values?{}",
                    qs(&[("match[]", "http_requests")])
                ))
                .to_request(),
        )
        .await;
        assert_eq!(body["data"], serde_json::json!(["api", "db"]));

        let body: Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/series?{}",
                    qs(&[("match[]", r#"http_requests{job!="db"}"#)])
                ))
                .to_request(),
        )
        .await;
        assert_eq!(body["data"].as_array().unwrap().len(), 1);
        assert_eq!(body["data"][0]["job"], "api");

        let body: Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/labels?{}",
                    qs(&[("match[]", "http_requests")])
                ))
                .to_request(),
        )
        .await;
        assert_eq!(body["data"], serde_json::json!(["__name__", "job"]));

        // A PromQL aggregation over the same data: `sum` drops the labels and
        // keeps only `__name__` for a bare selector.
        let body: Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/query_range?{}",
                    qs(&[
                        ("query", "sum(http_requests)"),
                        ("start", &start),
                        ("end", &time),
                        ("step", "10"),
                    ])
                ))
                .to_request(),
        )
        .await;
        assert_eq!(body["data"]["resultType"], "matrix");
        let result = body["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 1, "an aggregation collapses to one series");
        assert_eq!(result[0]["metric"], serde_json::json!({}));

        // An unsupported construct is a `bad_data` error naming the gap, not a
        // wrong answer.
        let response = actix_web::test::call_service(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/query?{}",
                    qs(&[("query", "http_requests[5m:1m]")])
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
        let body: Value = actix_web::test::read_body_json(response).await;
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "bad_data");
        assert!(body["error"].as_str().unwrap().contains("subqueries"));

        // `rate` over a range selector, and a `by` aggregation: these are the
        // two shapes most dashboard panels use.
        let body: Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/query_range?{}",
                    qs(&[
                        ("query", r#"sum by (job) (http_requests)"#),
                        ("start", &start),
                        ("end", &time),
                        ("step", "10"),
                    ])
                ))
                .to_request(),
        )
        .await;
        let result = body["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 2, "one series per job");
        let mut jobs: Vec<&str> = result
            .iter()
            .map(|series| series["metric"]["job"].as_str().unwrap())
            .collect();
        jobs.sort_unstable();
        assert_eq!(jobs, vec!["api", "db"]);

        // `rate` needs a range vector, so this exercises the range-selector
        // path against stored samples.
        let body: Value = actix_web::test::call_and_read_body_json(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/query?{}",
                    qs(&[("query", "rate(http_requests[60s])"), ("time", &time)])
                ))
                .to_request(),
        )
        .await;
        assert_eq!(body["status"], "success");
        let result = body["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 2);
        for series in result {
            // Values are 0,1,2,3 over 10 s steps, so the rate is 0.1/s.
            let value: f64 = series["value"][1].as_str().unwrap().parse().unwrap();
            assert!((value - 0.1).abs() < 1e-9, "got {value}");
        }

        // `rate` needs a range vector; a bare selector is rejected.
        let response = actix_web::test::call_service(
            &app,
            actix_web::test::TestRequest::get()
                .uri(&format!(
                    "/prom/api/v1/query?{}",
                    qs(&[("query", "rate(http_requests)")])
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
}
