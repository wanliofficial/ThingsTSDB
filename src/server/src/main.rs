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

use std::{
    fs,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use actix_web::{
    get, post,
    web::{self, Bytes, Data},
    App, HttpResponse, HttpServer, Responder,
};
use bytes::Bytes as BytesBuf;
use clap::Parser;
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

use crate::config::{Config, ObjectStorageConfig};

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
    if let Err(e) = data.engine.compact().await {
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

    match data.engine.write(samples).await {
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
        match run_read_query(&data.engine, query).await {
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
    engine: &MetricEngineRef,
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
    let series = engine.search(&request).await?;
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
        Err(message) => return HttpResponse::BadRequest().body(message),
    };
    match data.engine.search(&request).await {
        Ok(series) => {
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
        Err(e) => HttpResponse::InternalServerError().body(format!("query failed: {e}")),
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
        Err(message) => return HttpResponse::BadRequest().body(message),
    };
    match data.engine.last_values(&request).await {
        Ok(series) => {
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
        Err(e) => HttpResponse::InternalServerError().body(format!("last query failed: {e}")),
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
        Err(message) => return HttpResponse::BadRequest().body(message),
    };
    let step: i64 = match params.get("step").map(|v| v.parse()).transpose() {
        Ok(Some(step)) if step > 0 => step,
        _ => return HttpResponse::BadRequest().body("step must be a positive integer (ms)"),
    };
    let agg = match params.get("agg").map(String::as_str).unwrap_or("avg") {
        "avg" => Aggregation::Avg,
        "min" => Aggregation::Min,
        "max" => Aggregation::Max,
        "sum" => Aggregation::Sum,
        "count" => Aggregation::Count,
        "last" => Aggregation::Last,
        other => return HttpResponse::BadRequest().body(format!("unknown aggregation {other:?}")),
    };
    match data.engine.search_downsampled(&request, step, agg).await {
        Ok(series) => {
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
        Err(e) => HttpResponse::InternalServerError().body(format!("range query failed: {e}")),
    }
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

struct AppState {
    engine: MetricEngineRef,
    keep_writing: Arc<AtomicBool>,
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

    let write_worker_num = config.test.write_worker_num;
    let write_interval = config.test.write_interval.0;
    let enable_write = config.test.enable_write;
    let write_rt = build_multi_runtime("write", write_worker_num);
    let keep_writing = Arc::new(AtomicBool::new(true));
    let _ = rt.block_on(async move {
        let store = Arc::new(LocalFileSystem::new());
        let engine = Arc::new(
            MetricEngine::try_open_with_index_mode(
                &object_store_config.data_dir,
                segment_duration,
                store,
                storage_opts,
                runtimes,
                index_mode,
            )
            .await
            .unwrap(),
        );

        if enable_write {
            bench_write(
                engine.clone(),
                write_rt.clone(),
                write_worker_num,
                write_interval,
                keep_writing.clone(),
            );
        }

        let app_state = Data::new(AppState {
            engine,
            keep_writing,
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

/// Write synthetic samples through the engine, for benchmark and demo.
fn bench_write(
    engine: MetricEngineRef,
    rt: columnar_storage::types::RuntimeRef,
    workers: usize,
    interval: std::time::Duration,
    keep_writing: Arc<AtomicBool>,
) {
    const METRIC_NAMES: [&[u8]; 3] = [b"bench_cpu_usage", b"bench_mem_usage", b"bench_qps"];
    const INSTANCES: [&[u8]; 4] = [b"10.0.0.1", b"10.0.0.2", b"10.0.0.3", b"10.0.0.4"];

    for worker_id in 0..workers {
        let engine = engine.clone();
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
                if let Err(e) = engine.write(samples).await {
                    error!("bench write failed, err:{}", e);
                }
            }
        });
    }
}
