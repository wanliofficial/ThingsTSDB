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

//! Prometheus HTTP API compatibility (roadmap Phase 3: Grafana 对接).
//!
//! Grafana's Prometheus data source speaks the Prometheus HTTP API, so the
//! server answers the same paths with the same JSON envelope:
//!
//! ```text
//! GET /prom/api/v1/query?query=http_requests{job="api"}&time=1700000000
//! GET /prom/api/v1/query_range?query=...&start=..&end=..&step=30
//! GET /prom/api/v1/series?match[]=http_requests{job="api"}
//! GET /prom/api/v1/labels
//! GET /prom/api/v1/label/job/values
//! ```
//!
//! The existing `/api/v1/*` endpoints keep their own (engine-shaped) answers;
//! this module mounts the Grafana-facing surface under its own prefix so a
//! dashboard can be pointed at `/prom` without changing the native API.
//!
//! Only a *selector* subset of PromQL is accepted — `metric{label="v"}`,
//! `{__name__="metric"}`, and the four matcher operators. Aggregations,
//! functions, binary operators and subqueries are rejected with a clear
//! `bad_data` error instead of returning something that silently disagrees
//! with what the dashboard asked for.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use actix_web::{
    get,
    web::{self, Data},
    HttpResponse,
};
use metric_engine::{Label, LabelMatcher, MatcherType, SearchRequest, SeriesSamples};
use serde_json::{json, Value};

use crate::AppState;

/// Prometheus timestamps are seconds; the engine works in milliseconds.
const MS_PER_SEC: f64 = 1_000.0;

/// A Prometheus error body, as the data source expects it.
fn error(error_type: &str, message: impl Into<String>) -> HttpResponse {
    HttpResponse::BadRequest().json(json!({
        "status": "error",
        "errorType": error_type,
        "error": message.into(),
    }))
}

fn success(data: Value) -> HttpResponse {
    HttpResponse::Ok().json(json!({
        "status": "success",
        "data": data,
    }))
}

/// A parsed Prometheus selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selector {
    /// Metric name, empty when the query only used `{__name__="..."}`.
    pub metric: String,
    /// Matchers applied to labels (never `__name__`).
    pub matchers: Vec<(MatcherType, String, String)>,
}

impl Selector {
    fn to_request(&self, start_ms: i64, end_ms: i64, limit: usize) -> SearchRequest {
        SearchRequest {
            metric_name: self.metric.as_bytes().to_vec(),
            matchers: self
                .matchers
                .iter()
                .map(|(kind, name, value)| LabelMatcher {
                    matcher_type: *kind,
                    name: name.as_bytes().to_vec(),
                    value: value.as_bytes().to_vec(),
                })
                .collect(),
            start_ms,
            end_ms,
            limit,
        }
    }
}

/// Parses a PromQL selector.
///
/// Grammar accepted: `name` | `name{matchers}` | `{matchers}`, where each
/// matcher is `label=("..."|ident) | label!=... | label=~... | label!~...`.
/// `__name__` inside the braces is lifted into the metric name, matching how
/// Prometheus treats it.
pub fn parse_selector(query: &str) -> std::result::Result<Selector, String> {
    let query = query.trim();
    if query.is_empty() {
        return Err("empty query".to_string());
    }
    let (head, body) = match query.find('{') {
        Some(open) => {
            let head = query[..open].trim();
            if !query.ends_with('}') {
                return Err(format!("unterminated matcher list in {query:?}"));
            }
            let body = &query[open + 1..query.len() - 1];
            if body.contains(['{', '}']) {
                return Err(format!("nested braces are not a selector: {query:?}"));
            }
            (head, Some(body))
        }
        None => (query, None),
    };

    if head.is_empty() && body.is_none() {
        return Err("empty selector".to_string());
    }
    if !head.is_empty() && !is_metric_name(head) {
        return Err(format!(
            "unsupported expression {head:?}: this endpoint accepts a plain \
             selector such as `metric{{label=\"value\"}}` only, not functions, \
             aggregations or binary operators"
        ));
    }

    let mut selector = Selector {
        metric: head.to_string(),
        matchers: Vec::new(),
    };
    if let Some(body) = body {
        for matcher in split_matchers(body)? {
            let (kind, name, value) = parse_matcher(&matcher)?;
            if name == "__name__" {
                if kind != MatcherType::Eq {
                    return Err(format!(
                        "only __name__=\"metric\" is supported, got {matcher:?}"
                    ));
                }
                if !selector.metric.is_empty() && selector.metric != value {
                    return Err(format!(
                        "metric name {selector:?} conflicts with __name__={value:?}"
                    ));
                }
                selector.metric = value;
            } else {
                selector.matchers.push((kind, name, value));
            }
        }
    }
    if selector.metric.is_empty() {
        return Err("selector needs a metric name".to_string());
    }
    Ok(selector)
}

fn is_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

/// Splits a matcher list on commas that are not inside a quoted string.
fn split_matchers(body: &str) -> std::result::Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for c in body.chars() {
        if let Some(q) = quote {
            current.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '"' | '\'' => {
                quote = Some(c);
                current.push(c);
            }
            ',' => {
                if !current.trim().is_empty() {
                    out.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if quote.is_some() {
        return Err("unterminated string literal".to_string());
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }
    Ok(out)
}

/// Parses one `label<op>value` matcher.
fn parse_matcher(text: &str) -> std::result::Result<(MatcherType, String, String), String> {
    const OPS: [(&str, MatcherType); 4] = [
        ("!~", MatcherType::Nre),
        ("!=", MatcherType::Neq),
        ("=~", MatcherType::Re),
        ("=", MatcherType::Eq),
    ];
    // Longest operator first so `!=` is not read as `!`.
    let (pos, kind) = OPS
        .iter()
        .filter_map(|(op, kind)| text.find(op).map(|pos| (pos, *op, *kind)))
        .min_by_key(|(pos, op, _)| (*pos, usize::MAX - op.len()))
        .map(|(pos, _, kind)| (pos, kind))
        .ok_or_else(|| format!("matcher {text:?} has no =, !=, =~ or !~ operator"))?;
    let op_len = OPS
        .iter()
        .find(|(_, k)| *k == kind)
        .map(|(op, _)| op.len())
        .expect("op found above");
    let name = text[..pos].trim();
    if !is_metric_name(name) {
        return Err(format!("invalid label name {name:?}"));
    }
    let raw = text[pos + op_len..].trim();
    let value = unquote(raw)?;
    Ok((kind, name.to_string(), value))
}

/// Strips surrounding quotes and resolves the escapes PromQL allows.
fn unquote(raw: &str) -> std::result::Result<String, String> {
    let bytes = raw.as_bytes();
    if bytes.len() >= 2
        && ((bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\''))
    {
        let inner = &raw[1..raw.len() - 1];
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c != '\\' {
                out.push(c);
                continue;
            }
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('\\') => out.push('\\'),
                Some('"') => out.push('"'),
                Some('\'') => out.push('\''),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => return Err("trailing backslash in string".to_string()),
            }
        }
        return Ok(out);
    }
    if raw.is_empty() {
        return Err("missing matcher value".to_string());
    }
    if !is_metric_name(raw) {
        return Err(format!("invalid matcher value {raw:?}"));
    }
    Ok(raw.to_string())
}

/// Parses a Prometheus timestamp (seconds, possibly fractional) to
/// milliseconds.
fn ts_ms(raw: &str, what: &str) -> std::result::Result<i64, String> {
    let seconds: f64 = raw
        .parse()
        .map_err(|_| format!("{what} must be a Unix timestamp in seconds, got {raw:?}"))?;
    if !seconds.is_finite() {
        return Err(format!("{what} must be finite, got {raw:?}"));
    }
    Ok((seconds * MS_PER_SEC).round() as i64)
}

/// Prometheus renders sample values as strings to keep full f64 precision.
fn format_value(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Inf".to_string()
        } else {
            "+Inf".to_string()
        };
    }
    let mut text = format!("{value}");
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        text.push_str(".0");
    }
    text
}

/// The `metric` object of a Prometheus result: `__name__` plus every label.
fn label_map(metric_name: &[u8], labels: &[Label]) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    map.insert(
        "__name__".to_string(),
        Value::String(String::from_utf8_lossy(metric_name).into_owned()),
    );
    for label in labels {
        map.insert(
            String::from_utf8_lossy(&label.name).into_owned(),
            Value::String(String::from_utf8_lossy(&label.value).into_owned()),
        );
    }
    map
}

/// Epoch seconds as the JSON number Prometheus uses (fractional is allowed).
fn seconds_json(ms: i64) -> Value {
    json!(ms as f64 / MS_PER_SEC)
}

/// One `matrix` (or `vector`) entry from a series' samples.
fn matrix_entry(series: &SeriesSamples) -> Value {
    json!({
        "metric": label_map(&series.metric_name, &series.labels),
        "values": series
            .samples
            .iter()
            .map(|p| json!([seconds_json(p.timestamp), format_value(p.value)]))
            .collect::<Vec<_>>(),
    })
}

fn vector_entry(series: &SeriesSamples) -> Value {
    let Some(last) = series.samples.last() else {
        return Value::Null;
    };
    json!({
        "metric": label_map(&series.metric_name, &series.labels),
        "value": [seconds_json(last.timestamp), format_value(last.value)],
    })
}

/// Drops points after the requested `end`.
///
/// Downsampling aligns buckets to epoch multiples and reports the bucket's
/// timestamp, so the bucket containing `end` can be stamped just past it (a
/// 15 s step ends up at `end + <15 s`). Prometheus never returns a point later
/// than `end`, and a dashboard would draw a step outside the panel range, so
/// the tail is trimmed here.
fn clip_to_window(mut series: Vec<SeriesSamples>, end_ms: i64) -> Vec<SeriesSamples> {
    for series in series.iter_mut() {
        series.samples.retain(|p| p.timestamp <= end_ms);
    }
    series.retain(|s| !s.samples.is_empty());
    series
}

fn matrix(series: &[SeriesSamples]) -> HttpResponse {
    success(json!({
        "resultType": "matrix",
        "result": series.iter().map(matrix_entry).collect::<Vec<_>>(),
    }))
}

fn vector(series: &[SeriesSamples]) -> HttpResponse {
    success(json!({
        "resultType": "vector",
        "result": series
            .iter()
            .filter(|s| !s.samples.is_empty())
            .map(vector_entry)
            .collect::<Vec<_>>(),
    }))
}

/// `GET /prom/api/v1/query_range?query=&start=&end=&step=`
///
/// `step` is in seconds. Every result point is the last sample inside its
/// `step`-aligned bucket, which is what a dashboard panel draws.
#[get("/prom/api/v1/query_range")]
pub async fn query_range_endpoint(
    data: Data<AppState>,
    params: web::Query<HashMap<String, String>>,
) -> HttpResponse {
    let Some(expr) = params.get("query") else {
        return error("bad_data", "query is required");
    };
    let selector = match parse_selector(expr) {
        Ok(selector) => selector,
        Err(message) => return error("bad_data", message),
    };
    let start_ms = match params.get("start").map(|v| ts_ms(v, "start")).transpose() {
        Ok(Some(v)) => v,
        Ok(None) => return error("bad_data", "start is required"),
        Err(message) => return error("bad_data", message),
    };
    let end_ms = match params.get("end").map(|v| ts_ms(v, "end")).transpose() {
        Ok(Some(v)) => v,
        Ok(None) => return error("bad_data", "end is required"),
        Err(message) => return error("bad_data", message),
    };
    if end_ms < start_ms {
        return error("bad_data", "end must not be before start");
    }
    let step_ms = match params.get("step").map(|v| ts_ms(v, "step")).transpose() {
        Ok(Some(v)) if v > 0 => v,
        Ok(Some(_)) => return error("bad_data", "step must be positive"),
        Ok(None) => return error("bad_data", "step is required"),
        Err(message) => return error("bad_data", message),
    };

    match data
        .backend
        .search_downsampled(
            &selector.to_request(start_ms, end_ms + 1, 0),
            step_ms,
            metric_engine::Aggregation::Last,
        )
        .await
    {
        Ok(series) => matrix(&clip_to_window(series, end_ms)),
        Err(e) => error("execution", format!("query failed: {e}")),
    }
}

/// `GET /prom/api/v1/query?query=&time=`
///
/// Returns the newest sample at or before `time`. Prometheus defaults the
/// lookback window to five minutes; a series silent for longer has no value.
#[get("/prom/api/v1/query")]
pub async fn query_endpoint(
    data: Data<AppState>,
    params: web::Query<HashMap<String, String>>,
) -> HttpResponse {
    const LOOKBACK_MS: i64 = 5 * 60 * 1_000;

    let Some(expr) = params.get("query") else {
        return error("bad_data", "query is required");
    };
    let selector = match parse_selector(expr) {
        Ok(selector) => selector,
        Err(message) => return error("bad_data", message),
    };
    let now_ms = match params.get("time").map(|v| ts_ms(v, "time")).transpose() {
        Ok(Some(v)) => v,
        Ok(None) => common::now(),
        Err(message) => return error("bad_data", message),
    };

    let request = selector.to_request(now_ms - LOOKBACK_MS, now_ms + 1, 0);
    match data.backend.search(&request).await {
        Ok(series) => vector(&series),
        Err(e) => error("execution", format!("query failed: {e}")),
    }
}

/// `GET /prom/api/v1/series?match[]=...&start=&end=`
///
/// Returns the label sets of the matching series. Series are resolved through
/// the index, so a window with no samples still lists the series that exist.
#[get("/prom/api/v1/series")]
pub async fn series_endpoint(
    data: Data<AppState>,
    params: web::Query<HashMap<String, String>>,
) -> HttpResponse {
    let selector = match selector_param(&params) {
        Ok(selector) => selector,
        Err(response) => return response,
    };
    match collect_label_sets(&data, &params, selector).await {
        Ok(sets) => success(Value::Array(
            sets.into_iter()
                .map(|set| {
                    Value::Object(
                        set.into_iter()
                            .map(|(k, v)| (k, Value::String(v)))
                            .collect(),
                    )
                })
                .collect(),
        )),
        Err(response) => response,
    }
}

/// `GET /prom/api/v1/labels` — every label name seen by a selector.
#[get("/prom/api/v1/labels")]
pub async fn labels_endpoint(
    data: Data<AppState>,
    params: web::Query<HashMap<String, String>>,
) -> HttpResponse {
    let selector = match selector_param(&params) {
        Ok(selector) => selector,
        Err(response) => return response,
    };
    match collect_label_sets(&data, &params, selector).await {
        Ok(sets) => {
            let names: BTreeSet<String> = sets.into_iter().flat_map(|s| s.into_keys()).collect();
            success(json!(names.into_iter().collect::<Vec<_>>()))
        }
        Err(response) => response,
    }
}

/// `GET /prom/api/v1/label/<name>/values` — the distinct values of one label.
#[get("/prom/api/v1/label/{name}/values")]
pub async fn label_values_endpoint(
    data: Data<AppState>,
    path: web::Path<String>,
    params: web::Query<HashMap<String, String>>,
) -> HttpResponse {
    let name = path.into_inner();
    let selector = match selector_param(&params) {
        Ok(selector) => selector,
        Err(response) => return response,
    };
    match collect_label_sets(&data, &params, selector).await {
        Ok(sets) => {
            let values: BTreeSet<String> = sets
                .into_iter()
                .filter_map(|mut s| s.remove(&name))
                .collect();
            success(json!(values.into_iter().collect::<Vec<_>>()))
        }
        Err(response) => response,
    }
}

/// The selector from `match[]` (or the `match` alias).
fn selector_param(params: &HashMap<String, String>) -> std::result::Result<Selector, HttpResponse> {
    match params.get("match[]").or_else(|| params.get("match")) {
        Some(selector) => parse_selector(selector).map_err(|message| error("bad_data", message)),
        None => Err(error("bad_data", "match[] is required")),
    }
}

/// Label sets of the series a selector matches inside the requested window.
///
/// This answers `/series`, `/labels` and `/label/<name>/values`, all of which
/// only need the label sets. It asks for each series' newest sample instead of
/// its full history: a template variable should not read every point of every
/// matched series, and the last-value cache answers most of it without a scan.
async fn collect_label_sets(
    data: &AppState,
    params: &HashMap<String, String>,
    selector: Selector,
) -> std::result::Result<Vec<BTreeMap<String, String>>, HttpResponse> {
    let start_ms = match params.get("start").map(|v| ts_ms(v, "start")).transpose() {
        Ok(Some(v)) => v,
        Ok(None) => i64::MIN,
        Err(message) => return Err(error("bad_data", message)),
    };
    let end_ms = match params.get("end").map(|v| ts_ms(v, "end")).transpose() {
        Ok(Some(v)) => v,
        Ok(None) => i64::MAX,
        Err(message) => return Err(error("bad_data", message)),
    };
    match data
        .backend
        .last_values(&selector.to_request(start_ms, end_ms, 0))
        .await
    {
        Ok(series) => Ok(series
            .iter()
            .map(|s| {
                label_map(&s.metric_name, &s.labels)
                    .into_iter()
                    .filter_map(|(k, v)| v.as_str().map(|v| (k, v.to_string())))
                    .collect()
            })
            .collect()),
        Err(e) => Err(error("execution", format!("label lookup failed: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selector(query: &str) -> Selector {
        parse_selector(query).unwrap()
    }

    #[test]
    fn test_bare_metric_name() {
        assert_eq!(
            selector("http_requests"),
            Selector {
                metric: "http_requests".into(),
                matchers: vec![],
            }
        );
    }

    #[test]
    fn test_all_four_matcher_operators() {
        let s = selector(r#"m{a="1",b!="2",c=~"x.*",d!~"y|z"}"#);
        assert_eq!(s.metric, "m");
        assert_eq!(
            s.matchers,
            vec![
                (MatcherType::Eq, "a".into(), "1".into()),
                (MatcherType::Neq, "b".into(), "2".into()),
                (MatcherType::Re, "c".into(), "x.*".into()),
                (MatcherType::Nre, "d".into(), "y|z".into()),
            ]
        );
    }

    #[test]
    fn test_name_label_is_lifted_into_the_metric() {
        let s = selector(r#"{__name__="http_requests",job="api"}"#);
        assert_eq!(s.metric, "http_requests");
        assert_eq!(
            s.matchers,
            vec![(MatcherType::Eq, "job".into(), "api".into())]
        );
    }

    #[test]
    fn test_escapes_inside_strings() {
        let s = selector(r#"m{path="/a\",b"}"#);
        assert_eq!(s.matchers[0].2, "/a\",b");
    }

    #[test]
    fn test_unsupported_expressions_are_rejected() {
        for query in [
            "",
            "sum(http_requests)",
            "rate(http_requests[5m])",
            "a + b",
            "m{",
            r#"m{a=~}"#,
            r#"m{__name__=~"a.*"}"#,
            r#"m{__name__="other"}"#,
            "m{},",
        ] {
            assert!(
                parse_selector(query).is_err(),
                "{query:?} should be rejected"
            );
        }
    }

    #[test]
    fn test_values_are_rendered_like_prometheus() {
        assert_eq!(format_value(1.0), "1.0");
        assert_eq!(format_value(1.5), "1.5");
        assert_eq!(format_value(-2.0), "-2.0");
        assert_eq!(format_value(f64::NAN), "NaN");
        assert_eq!(format_value(f64::INFINITY), "+Inf");
        assert_eq!(format_value(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn test_timestamps_convert_seconds_to_millis() {
        assert_eq!(ts_ms("1700000000", "t").unwrap(), 1_700_000_000_000);
        assert_eq!(ts_ms("1700000000.5", "t").unwrap(), 1_700_000_000_500);
        assert!(ts_ms("abc", "t").is_err());
    }

    #[test]
    fn test_points_after_end_are_clipped() {
        // A step-aligned bucket can be stamped past `end`; Prometheus never
        // returns such a point, so it must not leak into the matrix.
        let series = SeriesSamples {
            metric_name: b"m".to_vec(),
            labels: Vec::new(),
            samples: vec![
                metric_engine::SamplePoint {
                    timestamp: 1_000,
                    value: 1.0,
                },
                metric_engine::SamplePoint {
                    timestamp: 2_000,
                    value: 2.0,
                },
                metric_engine::SamplePoint {
                    timestamp: 3_000,
                    value: 3.0,
                },
            ],
        };
        let clipped = clip_to_window(vec![series], 2_000);
        assert_eq!(clipped.len(), 1);
        assert_eq!(clipped[0].samples.len(), 2);
        assert_eq!(clipped[0].samples[1].timestamp, 2_000);

        // A series with nothing before `end` disappears rather than rendering
        // as an empty entry.
        let mut empty = clipped[0].clone();
        empty.samples.clear();
        assert!(clip_to_window(vec![empty], 2_000).is_empty());
    }

    #[test]
    fn test_matrix_and_vector_shapes() {
        let series = SeriesSamples {
            metric_name: b"http_requests".to_vec(),
            labels: vec![Label {
                name: b"job".to_vec(),
                value: b"api".to_vec(),
            }],
            samples: vec![
                metric_engine::SamplePoint {
                    timestamp: 1_700_000_000_000,
                    value: 3.0,
                },
                metric_engine::SamplePoint {
                    timestamp: 1_700_000_015_000,
                    value: 4.5,
                },
            ],
        };
        let entry = matrix_entry(&series);
        assert_eq!(entry["metric"]["__name__"], "http_requests");
        assert_eq!(entry["metric"]["job"], "api");
        assert_eq!(entry["values"][0][0], 1_700_000_000.0);
        assert_eq!(entry["values"][0][1], "3.0");

        let entry = vector_entry(&series);
        assert_eq!(entry["value"][0], 1_700_000_015.0);
        assert_eq!(entry["value"][1], "4.5");
    }
}
