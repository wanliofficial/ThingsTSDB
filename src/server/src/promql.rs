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

//! PromQL subset for the Grafana-facing API (roadmap Phase 3「查询语言」).
//!
//! Scope, and where it stops:
//!
//! * instant selectors, with `offset` and `@`;
//! * range selectors `[5m]` as function arguments;
//! * subqueries `expr[5m:1m]` (and `[5m:]`, which uses the default one-minute
//!   step) as function arguments, including nested ones;
//! * range functions — `rate`, `irate`, `increase`, `delta`, `idelta`,
//!   `changes`, `resets`, `deriv`, `*_over_time`;
//! * instant functions — math (`abs`, `ceil`, `clamp`, `clamp_min`, ...),
//!   `scalar`, the date functions (`year`, `month`, `day_of_month`, ...),
//!   `timestamp`, `sort`, `sort_desc`, `absent`, `histogram_quantile`,
//!   `histogram_fraction`, `histogram_quantiles`, `max_of` / `min_of`,
//!   `label_replace` / `label_join`, the query-context scalars `start()`,
//!   `end()`, `range()`, `step()`, and the generators `time`, `pi`, `vector`,
//!   plus MetricsQL's `default`;
//! * aggregations — `sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`,
//!   `stdvar`, `topk`, `bottomk`, `quantile`, with `by` / `without`;
//! * binary operators — arithmetic and comparison (with `bool`) between two
//!   scalars or a vector and a scalar, vector matching between two vectors
//!   (`on` / `ignoring`, `group_left` / `group_right`), and the set operators
//!   `and` / `or` / `unless`;
//! * a MetricsQL subset on top of PromQL — `aggr_over_time(<agg>, v[range])`,
//!   `absent_over_time`, `tmin_over_time` / `tmax_over_time`,
//!   `predict_linear(v[range], t)`, the `*_over_time` family, the label helpers
//!   `label_set` / `label_del` / `label_keep` / `label_copy` / `label_move` /
//!   `label_value` / `alias`, and the classic-bucket summary functions
//!   `histogram_avg` / `histogram_stddev` / `histogram_stdvar`.
//!
//! The parameterised range functions that put the range vector first —
//! `predict_linear`, `double_exponential_smoothing` and `integral` — are
//! dispatched by name in the parser, since `quantile_over_time(0.9, v[range])`
//! is the only one whose scalar comes first.
//!
//! Prometheus' `present_over_time` and `mad_over_time` are part of the range
//! function set above.
//!
//! MetricsQL is what VictoriaMetrics' Grafana datasource sends, so these are
//! accepted alongside PromQL; they reuse the same evaluator and are refused
//! with the same care (see below). The `rollup_*` family is intentionally
//! absent: it is a VictoriaMetrics-only interface that assumes its own rollup
//! tables, and this server's rollup path is still raw-sample based.
//!
//! Vector matching follows Prometheus' `VectorBinop` / `resultMetric`: the
//! right side is the "one" side (a `group_right` query is evaluated as the
//! mirrored `group_left`), the match key is the `on` labels or every label but
//! `__name__` and the `ignoring` ones, a duplicate key on the "one" side is an
//! error naming both series, and a one-to-one match that pairs one series twice
//! is an error telling the caller to add `group_left`/`group_right`. Arithmetic
//! and `bool` comparisons drop `__name__`; a filtering comparison keeps it.
//!
//! Subqueries follow Prometheus' `subqueryEvaluationTimes`: the step grid is
//! anchored at absolute multiples of the step, the first point is strictly
//! after `at - offset - range`, and points continue up to `at - offset`. Each
//! point is an ordinary instant evaluation, and the resulting samples carry the
//! grid timestamp — which is why `rate(m[20s:5s])` extrapolates over the grid
//! span and not over the raw sample times.
//!
//! `label_replace` / `label_join` rewrite one label per series and leave
//! values and timestamps alone, so they ride on the ordinary per-step
//! evaluation. `label_replace` anchors its pattern the way Prometheus does
//! (`^(?s:...)$`) and expands `$1` / `${name}` / `$$` exactly like Go's
//! `ExpandString`; a series whose source value does not match passes through
//! unchanged, and an empty result deletes the destination label.
//!
//! MetricsQL's label helpers follow `transform.go` too: `label_set` applies
//! its name/value pairs in order and an empty value deletes the label
//! (`alias` is `label_set` on `__name__`); `label_del` and `label_keep` treat
//! `__name__` like any other name, so `label_keep(v, "job")` drops the metric
//! name — VM's `RemoveTagsOn` resets the metric group unless `__name__` is
//! kept; `label_copy` skips a missing or empty source instead of deleting the
//! destination, and `label_move` then deletes the source; `label_value`
//! replaces every non-NaN value with the named label's text parsed as a
//! float (NaN when it is not a number) and drops the metric name.
//!
//! `histogram_avg` / `histogram_stddev` / `histogram_stdvar` are
//! VictoriaMetrics' classic-`le` versions, not Prometheus' native-histogram
//! ones: every bucket is taken as uniform between the previous bound and its
//! own, so it contributes its midpoint weighted by the increase in the
//! cumulative count, the first interval starts at zero, and the `+Inf` bucket
//! is skipped because it has no midpoint.
//!
//! Evaluation asks the engine for each selector's raw samples over the union
//! window once, then evaluates the expression per step. One scan per selector
//! instead of one query per step, and the per-step selection follows
//! Prometheus' rules (last sample within the 5-minute lookback, range vectors
//! half-open on the left). It does bypass the rollup tables that
//! `search_downsampled` uses, so a wide range query reads raw points; routing
//! rollup-aware evaluation is a separate piece of work, noted in the roadmap.

use std::collections::{BTreeMap, HashMap, HashSet};

use metric_engine::{MatcherType, SamplePoint, SearchRequest, SeriesSamples};

use crate::{
    prometheus::{format_value, seconds_json, Selector},
    Backend,
};

/// Prometheus' default lookback for an instant selector.
pub const LOOKBACK_MS: i64 = 5 * 60 * 1_000;

/// Prometheus refuses more than 11k points per time series in a range query.
pub const MAX_STEPS: i64 = 11_000;

/// The step a subquery uses when its `:` is not followed by one. Prometheus
/// falls back to the configured global evaluation interval, which is one
/// minute by default.
pub const DEFAULT_SUBQUERY_STEP_MS: i64 = 60_000;

/// The step a subquery actually evaluates at (`[5m:]` means the default).
fn subquery_step(step_ms: i64) -> i64 {
    if step_ms > 0 {
        step_ms
    } else {
        DEFAULT_SUBQUERY_STEP_MS
    }
}

/// A compiled regular expression from `label_replace`.
///
/// `Expr` is compared in tests only, and a compiled regex has no useful
/// equality of its own, so this compares the pattern source: two spellings of
/// the same regex are the same expression.
#[derive(Debug, Clone)]
pub struct Pattern(regex::Regex);

impl PartialEq for Pattern {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

/// `[A-Za-z_][A-Za-z0-9_]*`, the label names Prometheus accepts for the
/// labels `label_replace` / `label_join` write.
fn validate_label_name(name: &str, function: &str) -> Result<()> {
    let mut chars = name.chars();
    let valid = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        bad(format!(
            "invalid label name {name:?} in {function}: label names are \
             [A-Za-z_][A-Za-z0-9_]*"
        ))
    }
}

/// Go's `regexp.ExpandString`: `$1` and `${name}` insert a capture, `$$` is a
/// literal `$`, an out-of-range or unmatched capture inserts nothing, and a
/// `$` that does not start a name is copied as-is. `label_replace` expands its
/// replacement with exactly this function, so the same spellings work here.
fn expand_replacement(template: &str, captures: &regex::Captures<'_>) -> String {
    let bytes = template.as_bytes();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;
    while i < bytes.len() {
        let rest = &template[i..];
        let Some(dollar) = rest.find('$') else {
            out.push_str(rest);
            break;
        };
        out.push_str(&rest[..dollar]);
        let after = i + dollar + 1;
        if bytes.get(after) == Some(&b'$') {
            out.push('$');
            i = after + 1;
            continue;
        }
        let (name, next) = if bytes.get(after) == Some(&b'{') {
            // `${name}` — the brace must close. An empty `${}` names no group
            // and therefore expands to nothing, as in Go.
            let body = &template[after + 1..];
            match body.find('}') {
                Some(end) if body[..end].bytes().all(is_name_byte) => {
                    (body[..end].to_string(), after + 1 + end + 1)
                }
                _ => {
                    out.push('$');
                    i = after;
                    continue;
                }
            }
        } else {
            // `$name` — the name is the longest run of name bytes, so `$1x`
            // means `${1x}` and not `${1}x`.
            let start = after;
            let mut end = after;
            while end < bytes.len() && is_name_byte(bytes[end]) {
                end += 1;
            }
            if end == start {
                out.push('$');
                i = start;
                continue;
            }
            (template[start..end].to_string(), end)
        };
        // Digits name a capture by index (`$0` is the whole match), anything
        // else by name; a name the regex does not declare expands to nothing.
        let capture = match name.parse::<usize>() {
            Ok(index) => captures.get(index),
            Err(_) => captures.name(&name),
        };
        if let Some(capture) = capture {
            out.push_str(capture.as_str());
        }
        i = next;
    }
    out
}

fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// A query error: `Bad` is the caller's fault (`bad_data`), `Exec` is ours
/// (`execution`).
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Bad(String),
    Exec(String),
}

impl Error {
    pub fn error_type(&self) -> &'static str {
        match self {
            Error::Bad(_) => "bad_data",
            Error::Exec(_) => "execution",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Error::Bad(message) | Error::Exec(message) => message,
        }
    }
}

impl From<common::Error> for Error {
    fn from(e: common::Error) -> Self {
        Error::Exec(format!("query failed: {e}"))
    }
}

type Result<T> = std::result::Result<T, Error>;

fn bad<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::Bad(message.into()))
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(f64),
    /// A quoted string literal, with escapes already resolved.
    Str(String),
    /// A duration literal, already converted to milliseconds.
    Duration(i64),
    LBrace,
    RBrace,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    /// One of `+ - * / % ^ == != > < >= <= = =~ !~`.
    Op(&'static str),
}

impl Token {
    fn describe(&self) -> String {
        match self {
            Token::Ident(name) => format!("identifier {name:?}"),
            Token::Number(value) => format!("number {value}"),
            Token::Str(value) => format!("string {value:?}"),
            Token::Duration(ms) => format!("duration {ms}ms"),
            Token::Op(op) => format!("{op:?}"),
            other => format!("{other:?}"),
        }
    }
}

/// Splits an expression into tokens.
///
/// Durations (`15s`, `5m`, `1h30m` is *not* supported — Prometheus' own
/// grammar takes a single unit here) are lexed as one token because a bare
/// `5` followed by `m` would otherwise read as a selectable metric name.
fn tokenize(input: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c.is_ascii_digit() || (c == '.' && chars.get(i + 1).is_some_and(|n| n.is_ascii_digit()))
        {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            // `1e9` is a number, not a duration: only consume an exponent when
            // a digit follows the sign.
            if i < chars.len() && (chars[i] == 'e' || chars[i] == 'E') {
                let mut j = i + 1;
                if j < chars.len() && (chars[j] == '+' || chars[j] == '-') {
                    j += 1;
                }
                if j < chars.len() && chars[j].is_ascii_digit() {
                    while j < chars.len() && chars[j].is_ascii_digit() {
                        j += 1;
                    }
                    i = j;
                }
            }
            let text: String = chars[start..i].iter().collect();
            let number: f64 = text
                .parse()
                .map_err(|_| Error::Bad(format!("invalid number {text:?}")))?;
            // A unit right after the number makes it a duration; `1h30m`
            // chains further number/unit pairs onto the same token.
            if let Some(unit) = chars.get(i).and_then(|c| duration_unit(*c)) {
                if text.contains('.') || text.contains('e') || text.contains('E') {
                    return bad(format!(
                        "duration {text}{:?} must be a whole number of a unit",
                        chars[i]
                    ));
                }
                let mut total = number as i64 * unit;
                i += 1;
                loop {
                    let start = i;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    if i == start {
                        break;
                    }
                    let Some(next_unit) = chars.get(i).and_then(|c| duration_unit(*c)) else {
                        break;
                    };
                    let amount: i64 = chars[start..i]
                        .iter()
                        .collect::<String>()
                        .parse()
                        .map_err(|_| Error::Bad("invalid duration".to_string()))?;
                    total += amount * next_unit;
                    i += 1;
                }
                tokens.push(Token::Duration(total));
                continue;
            }
            tokens.push(Token::Number(number));
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == ':')
            {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            // `Inf` and `NaN` are float literals, case-insensitively, exactly
            // as Prometheus' lexer treats them; `-Inf` is then a unary minus
            // on that literal. A longer name such as `Infinity` stays an
            // identifier, and a metric actually called `inf` would have to be
            // quoted (which this subset does not support yet).
            if word.eq_ignore_ascii_case("inf") {
                tokens.push(Token::Number(f64::INFINITY));
                continue;
            }
            if word.eq_ignore_ascii_case("nan") {
                tokens.push(Token::Number(f64::NAN));
                continue;
            }
            tokens.push(Token::Ident(word));
            continue;
        }
        let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
        let op = match two.as_str() {
            "==" => Some("=="),
            "!=" => Some("!="),
            ">=" => Some(">="),
            "<=" => Some("<="),
            "=~" => Some("=~"),
            "!~" => Some("!~"),
            _ => None,
        };
        if let Some(op) = op {
            tokens.push(Token::Op(op));
            i += 2;
            continue;
        }
        let op = match c {
            '@' => "@",
            // `:` is legal inside a metric name but also separates a subquery's
            // range from its step, so it is only a token when it starts one.
            ':' => ":",
            '+' => "+",
            '-' => "-",
            '*' => "*",
            '/' => "/",
            '%' => "%",
            '^' => "^",
            '>' => ">",
            '<' => "<",
            '=' => "=",
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
                continue;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
                continue;
            }
            '{' => {
                tokens.push(Token::LBrace);
                i += 1;
                continue;
            }
            '}' => {
                tokens.push(Token::RBrace);
                i += 1;
                continue;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
                continue;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
                continue;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
                continue;
            }
            '"' => {
                i += 1;
                let mut value = String::new();
                loop {
                    let Some(&next) = chars.get(i) else {
                        return bad("unterminated string literal");
                    };
                    i += 1;
                    match next {
                        '"' => break,
                        '\\' => {
                            let Some(&escaped) = chars.get(i) else {
                                return bad("unterminated escape in string literal");
                            };
                            i += 1;
                            match escaped {
                                'n' => value.push('\n'),
                                't' => value.push('\t'),
                                'r' => value.push('\r'),
                                '\\' => value.push('\\'),
                                '"' => value.push('"'),
                                // An unknown escape keeps its backslash: a
                                // regex like `\d+` must reach the engine as
                                // `\d+`, not silently become `d+`. This matches
                                // `prometheus::unquote`, which the /series
                                // endpoints share a parser lineage with.
                                other => {
                                    value.push('\\');
                                    value.push(other);
                                }
                            }
                        }
                        other => value.push(other),
                    }
                }
                tokens.push(Token::Str(value));
                continue;
            }
            _ => return bad(format!("unexpected character {c:?} in query")),
        };
        tokens.push(Token::Op(op));
        i += 1;
    }
    Ok(tokens)
}

fn duration_unit(c: char) -> Option<i64> {
    match c {
        'y' => Some(365 * 24 * 3600 * 1_000),
        'w' => Some(7 * 24 * 3600 * 1_000),
        'd' => Some(24 * 3600 * 1_000),
        'h' => Some(3600 * 1_000),
        'm' => Some(60 * 1_000),
        's' => Some(1_000),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

/// The `@` modifier on a selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtModifier {
    /// `@ <seconds>` — a fixed instant, in milliseconds.
    At(i64),
    /// `@ start()` — the query's own range start.
    Start,
    /// `@ end()` — the query's own range end.
    End,
}

impl AtModifier {
    fn resolve(self, query_start_ms: i64, query_end_ms: i64) -> i64 {
        match self {
            AtModifier::At(ms) => ms,
            AtModifier::Start => query_start_ms,
            AtModifier::End => query_end_ms,
        }
    }
}

/// How an aggregation groups its input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grouping {
    By(Vec<String>),
    Without(Vec<String>),
}

/// How many samples on each side of a binary operation may share one match
/// key, following Prometheus' `VectorMatchCardinality`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Cardinality {
    /// The unmodified case: one series per match key on either side.
    #[default]
    OneToOne,
    ManyToOne,
    OneToMany,
    ManyToMany,
}

/// How the two sides of a binary operation are matched, i.e. the
/// `on(...)` / `ignoring(...)` / `group_left(...)` / `group_right(...)`
/// modifiers after the operator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorMatching {
    /// `on(...)` keeps only `matching_labels`; `ignoring(...)` drops them.
    pub on: bool,
    /// The labels named by `on(...)` or `ignoring(...)`.
    pub matching_labels: Vec<String>,
    pub card: Cardinality,
    /// The `group_left(...)` / `group_right(...)` labels, which are copied
    /// from the "one" side of the match.
    pub include: Vec<String>,
}

impl VectorMatching {
    /// True when no modifier was given, i.e. the default "match on every
    /// label except `__name__`" rule.
    #[cfg(test)]
    fn is_default(&self) -> bool {
        !self.on
            && self.matching_labels.is_empty()
            && self.card == Cardinality::OneToOne
            && self.include.is_empty()
    }
}

/// A label set, as found in `Point::labels`.
type LabelSet = BTreeMap<String, String>;

/// The match key of a label set: the label pairs that decide whether two
/// series on the two sides of a binary operation are the same series.
type MatchKey = Vec<(String, String)>;

/// The match key of a label set under `matching`.
///
/// `on(...)` keeps exactly the named labels (present ones only), everything
/// else keeps every label but `__name__` and the ignored ones — which is what
/// Prometheus' signature function does, so two series that a query treats as
/// "the same series" produce the same key.
fn matching_signature(labels: &LabelSet, matching: &VectorMatching) -> MatchKey {
    labels
        .iter()
        .filter(|(name, _)| {
            if matching.on {
                matching
                    .matching_labels
                    .iter()
                    .any(|label| label == name.as_str())
            } else {
                name.as_str() != "__name__"
                    && !matching
                        .matching_labels
                        .iter()
                        .any(|label| label == name.as_str())
            }
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    /// `atan2` — the trigonometric binary operator (precedence of `*`).
    Atan2,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
    Unless,
}

impl BinaryOp {
    fn name(self) -> &'static str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
            BinaryOp::Pow => "^",
            BinaryOp::Atan2 => "atan2",
            BinaryOp::Eq => "==",
            BinaryOp::Neq => "!=",
            BinaryOp::Gt => ">",
            BinaryOp::Lt => "<",
            BinaryOp::Gte => ">=",
            BinaryOp::Lte => "<=",
            BinaryOp::And => "and",
            BinaryOp::Or => "or",
            BinaryOp::Unless => "unless",
        }
    }

    fn is_comparison(self) -> bool {
        matches!(
            self,
            BinaryOp::Eq
                | BinaryOp::Neq
                | BinaryOp::Gt
                | BinaryOp::Lt
                | BinaryOp::Gte
                | BinaryOp::Lte
        )
    }

    fn is_set(self) -> bool {
        matches!(self, BinaryOp::And | BinaryOp::Or | BinaryOp::Unless)
    }

    fn apply(self, lhs: f64, rhs: f64) -> f64 {
        match self {
            BinaryOp::Add => lhs + rhs,
            BinaryOp::Sub => lhs - rhs,
            BinaryOp::Mul => lhs * rhs,
            BinaryOp::Div => lhs / rhs,
            BinaryOp::Mod => lhs % rhs,
            BinaryOp::Pow => lhs.powf(rhs),
            BinaryOp::Atan2 => lhs.atan2(rhs),
            BinaryOp::Eq => f64::from(lhs == rhs),
            BinaryOp::Neq => f64::from(lhs != rhs),
            BinaryOp::Gt => f64::from(lhs > rhs),
            BinaryOp::Lt => f64::from(lhs < rhs),
            BinaryOp::Gte => f64::from(lhs >= rhs),
            BinaryOp::Lte => f64::from(lhs <= rhs),
            // Set operators never reach `apply`.
            BinaryOp::And | BinaryOp::Or | BinaryOp::Unless => f64::NAN,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Group,
    Stddev,
    Stdvar,
    Topk,
    Bottomk,
    Quantile,
    /// `count_values("label", v)` — one output series per distinct value.
    CountValues,
}

impl AggOp {
    pub fn name(self) -> &'static str {
        match self {
            AggOp::Sum => "sum",
            AggOp::Avg => "avg",
            AggOp::Min => "min",
            AggOp::Max => "max",
            AggOp::Count => "count",
            AggOp::Group => "group",
            AggOp::Stddev => "stddev",
            AggOp::Stdvar => "stdvar",
            AggOp::Topk => "topk",
            AggOp::Bottomk => "bottomk",
            AggOp::Quantile => "quantile",
            AggOp::CountValues => "count_values",
        }
    }

    fn parse(name: &str) -> Option<AggOp> {
        Some(match name {
            "sum" => AggOp::Sum,
            "avg" => AggOp::Avg,
            "min" => AggOp::Min,
            "max" => AggOp::Max,
            "count" => AggOp::Count,
            "group" => AggOp::Group,
            "stddev" => AggOp::Stddev,
            "stdvar" => AggOp::Stdvar,
            "topk" => AggOp::Topk,
            "bottomk" => AggOp::Bottomk,
            "quantile" => AggOp::Quantile,
            "count_values" => AggOp::CountValues,
            _ => return None,
        })
    }

    /// `topk` / `bottomk` / `quantile` take a numeric parameter before the
    /// vector, `count_values` a label name.
    fn takes_param(self) -> bool {
        matches!(
            self,
            AggOp::Topk | AggOp::Bottomk | AggOp::Quantile | AggOp::CountValues
        )
    }

    /// True when the parameter is a quoted label name rather than a scalar.
    fn takes_label_param(self) -> bool {
        self == AggOp::CountValues
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The instant functions.
pub enum RangeFunc {
    Rate,
    Irate,
    Increase,
    Delta,
    Idelta,
    Changes,
    Resets,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    SumOverTime,
    CountOverTime,
    LastOverTime,
    StddevOverTime,
    StdvarOverTime,
    QuantileOverTime,
    /// `absent_over_time(v[range])`.
    AbsentOverTime,
    /// `aggr_over_time(<agg>, v[range])`.
    AggrOverTime(AggOp),
    /// `tmin_over_time` / `tmax_over_time`.
    TMinOverTime,
    TMaxOverTime,
    /// `present_over_time(v[range])` — 1 for any series with a sample.
    PresentOverTime,
    /// `mad_over_time(v[range])` — median absolute deviation.
    MadOverTime,
    /// `predict_linear(v[range], t)`.
    PredictLinear,
    /// `deriv(v[range])` — the least-squares slope, in per-second units.
    Deriv,
    /// `first_over_time(v[range])` — the oldest sample's value.
    FirstOverTime,
    /// `ts_of_first_over_time(v[range])` — the oldest sample's timestamp.
    TsOfFirstOverTime,
    /// `ts_of_last_over_time(v[range])` — the newest sample's timestamp.
    TsOfLastOverTime,
    /// `ts_of_max_over_time(v[range])` — the *last* maximal sample's
    /// timestamp (Prometheus compares with `>=`).
    TsOfMaxOverTime,
    /// `ts_of_min_over_time(v[range])` — the *last* minimal sample's
    /// timestamp (Prometheus compares with `<=`).
    TsOfMinOverTime,
    /// `double_exponential_smoothing(v[range], sf, tf)` — Holt's linear
    /// smoothing (Prometheus V2 called it `holt_winters`).
    DoubleExpSmoothing,
    /// `integral(v[range], strategy=2)` — the integral over time in seconds.
    Integral,
    /// MetricsQL `range_over_time(v[range])` — `max - min`.
    RangeOverTime,
    /// MetricsQL `median_over_time(v[range])` — `quantile(0.5)`.
    MedianOverTime,
    /// MetricsQL `sum2_over_time(v[range])` — the sum of the squares.
    Sum2OverTime,
    /// MetricsQL `geomean_over_time(v[range])` — `prod ^ (1/n)`.
    GeomeanOverTime,
    /// MetricsQL `distinct_over_time(v[range])` — how many distinct values.
    DistinctOverTime,
    /// MetricsQL `mode_over_time(v[range])` — the most frequent value.
    ModeOverTime,
    /// MetricsQL `increases_over_time(v[range])`.
    IncreasesOverTime,
    /// MetricsQL `decreases_over_time(v[range])` — the same rule as `resets`.
    DecreasesOverTime,
    /// MetricsQL `tfirst_over_time(v[range])`, an alias of
    /// `ts_of_first_over_time`.
    TFirstOverTime,
    /// MetricsQL `tlast_over_time(v[range])`, an alias of
    /// `ts_of_last_over_time`.
    TLastOverTime,
}

impl RangeFunc {
    fn parse(name: &str) -> Option<RangeFunc> {
        Some(match name {
            "rate" => RangeFunc::Rate,
            "irate" => RangeFunc::Irate,
            "increase" => RangeFunc::Increase,
            "delta" => RangeFunc::Delta,
            "idelta" => RangeFunc::Idelta,
            "changes" => RangeFunc::Changes,
            "resets" => RangeFunc::Resets,
            "avg_over_time" => RangeFunc::AvgOverTime,
            "min_over_time" => RangeFunc::MinOverTime,
            "max_over_time" => RangeFunc::MaxOverTime,
            "sum_over_time" => RangeFunc::SumOverTime,
            "count_over_time" => RangeFunc::CountOverTime,
            "last_over_time" => RangeFunc::LastOverTime,
            "stddev_over_time" => RangeFunc::StddevOverTime,
            "stdvar_over_time" => RangeFunc::StdvarOverTime,
            "quantile_over_time" => RangeFunc::QuantileOverTime,
            "absent_over_time" => RangeFunc::AbsentOverTime,
            "tmin_over_time" => RangeFunc::TMinOverTime,
            "tmax_over_time" => RangeFunc::TMaxOverTime,
            "present_over_time" => RangeFunc::PresentOverTime,
            "mad_over_time" => RangeFunc::MadOverTime,
            "deriv" => RangeFunc::Deriv,
            "first_over_time" => RangeFunc::FirstOverTime,
            "ts_of_first_over_time" => RangeFunc::TsOfFirstOverTime,
            "ts_of_last_over_time" => RangeFunc::TsOfLastOverTime,
            "ts_of_max_over_time" => RangeFunc::TsOfMaxOverTime,
            "ts_of_min_over_time" => RangeFunc::TsOfMinOverTime,
            // MetricsQL aliases for the two above.
            "tfirst_over_time" => RangeFunc::TFirstOverTime,
            "tlast_over_time" => RangeFunc::TLastOverTime,
            // MetricsQL rollups that are plain functions of the window.
            "range_over_time" => RangeFunc::RangeOverTime,
            "median_over_time" => RangeFunc::MedianOverTime,
            "sum2_over_time" => RangeFunc::Sum2OverTime,
            "geomean_over_time" => RangeFunc::GeomeanOverTime,
            "distinct_over_time" => RangeFunc::DistinctOverTime,
            "mode_over_time" => RangeFunc::ModeOverTime,
            "increases_over_time" => RangeFunc::IncreasesOverTime,
            "decreases_over_time" => RangeFunc::DecreasesOverTime,
            // `double_exponential_smoothing` and `predict_linear` take their
            // range vector first, so they are dispatched by name in `call`
            // rather than through `parse`.
            _ => return None,
        })
    }

    fn takes_param(self) -> bool {
        matches!(self, RangeFunc::QuantileOverTime | RangeFunc::PredictLinear)
    }

    /// The MetricsQL-only rollups, whose input VM cleans of NaNs first
    /// (`removeNanValues` runs before every rollup function in `eval.go`).
    fn is_metricsql_rollup(self) -> bool {
        matches!(
            self,
            RangeFunc::RangeOverTime
                | RangeFunc::MedianOverTime
                | RangeFunc::Sum2OverTime
                | RangeFunc::GeomeanOverTime
                | RangeFunc::DistinctOverTime
                | RangeFunc::ModeOverTime
                | RangeFunc::IncreasesOverTime
                | RangeFunc::DecreasesOverTime
                | RangeFunc::TFirstOverTime
                | RangeFunc::TLastOverTime
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MathFunc {
    Abs,
    Ceil,
    Floor,
    Sqrt,
    Exp,
    Ln,
    Log2,
    Log10,
    Sgn,
    Round,
    ClampMin,
    ClampMax,
    /// `default(v, x)`.
    Default,
    /// `histogram_quantile(q, v)` over classic `le` buckets.
    HistogramQuantile,
    Timestamp,
    // Trigonometric, hyperbolic and degree/radian helpers. All of them are
    // Prometheus' `simpleFloatFunc`: map the value, drop `__name__`, and let
    // the IEEE result (NaN, ±Inf) through unchanged.
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    /// `deg(v)` — radians to degrees.
    Deg,
    /// `rad(v)` — degrees to radians.
    Rad,
}

impl MathFunc {
    fn parse(name: &str) -> Option<MathFunc> {
        Some(match name {
            "abs" => MathFunc::Abs,
            "ceil" => MathFunc::Ceil,
            "floor" => MathFunc::Floor,
            "sqrt" => MathFunc::Sqrt,
            "exp" => MathFunc::Exp,
            "ln" => MathFunc::Ln,
            "log2" => MathFunc::Log2,
            "log10" => MathFunc::Log10,
            "sgn" => MathFunc::Sgn,
            "round" => MathFunc::Round,
            "clamp_min" => MathFunc::ClampMin,
            "clamp_max" => MathFunc::ClampMax,
            // MetricsQL: `default(v, x)` fills an empty result with `x`.
            "default" => MathFunc::Default,
            "histogram_quantile" => MathFunc::HistogramQuantile,
            "timestamp" => MathFunc::Timestamp,
            "sin" => MathFunc::Sin,
            "cos" => MathFunc::Cos,
            "tan" => MathFunc::Tan,
            "asin" => MathFunc::Asin,
            "acos" => MathFunc::Acos,
            "atan" => MathFunc::Atan,
            "sinh" => MathFunc::Sinh,
            "cosh" => MathFunc::Cosh,
            "tanh" => MathFunc::Tanh,
            "asinh" => MathFunc::Asinh,
            "acosh" => MathFunc::Acosh,
            "atanh" => MathFunc::Atanh,
            "deg" => MathFunc::Deg,
            "rad" => MathFunc::Rad,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// The `*_of_*` / component date functions.
///
/// Prometheus implements them all through `dateWrapper`: an argument is
/// optional, and with one each sample's **value** is read as Unix seconds
/// (the sample's timestamp is irrelevant).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateFunc {
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
    Month,
    Year,
}

impl DateFunc {
    fn parse(name: &str) -> Option<DateFunc> {
        Some(match name {
            "day_of_month" => DateFunc::DayOfMonth,
            "day_of_week" => DateFunc::DayOfWeek,
            "day_of_year" => DateFunc::DayOfYear,
            "days_in_month" => DateFunc::DaysInMonth,
            "hour" => DateFunc::Hour,
            "minute" => DateFunc::Minute,
            "month" => DateFunc::Month,
            "year" => DateFunc::Year,
            _ => return None,
        })
    }

    /// The component of `unix_secs`, in UTC, exactly as `dateWrapper` reads it.
    /// `days_in_month` counts by asking for the 32nd day of the month, which
    /// Go normalises back into the last day of that month.
    fn of(self, unix_secs: f64) -> f64 {
        let seconds = unix_secs.floor();
        let date = civil_from_unix_seconds(seconds);
        match self {
            DateFunc::DayOfMonth => f64::from(date.day),
            DateFunc::DayOfWeek => weekday_from_days(date.days) as f64,
            DateFunc::DayOfYear => (date.days - days_from_civil(date.year, 1, 1) + 1) as f64,
            DateFunc::DaysInMonth => f64::from(days_in_month(date.year, date.month)),
            DateFunc::Hour => f64::from(date.hour),
            DateFunc::Minute => f64::from(date.minute),
            DateFunc::Month => f64::from(date.month),
            DateFunc::Year => f64::from(date.year),
        }
    }
}

/// A UTC calendar instant, with the day count since the Unix epoch kept so
/// `day_of_year` and `day_of_week` do not need a second conversion.
struct CivilDate {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    /// Days since 1970-01-01.
    days: i64,
}

/// Converts Unix seconds to a UTC calendar date.
///
/// Howard Hinnant's `civil_from_days`: shift the epoch to 0000-03-01 so leap
/// days land at the end of the 400-year cycle, then undo the shift. Integer
/// arithmetic only, so no dependency and no floating-point rounding.
fn civil_from_unix_seconds(unix_secs: f64) -> CivilDate {
    // A NaN or infinite sample has no date; Go's `int64` conversion is
    // unspecified there, so treat it as the epoch. Finite values are clamped
    // into a range whose day count is far from overflowing the integer
    // arithmetic below (about ±31 million years).
    const MAX_UNIX_SECS: f64 = 1.0e15;
    let total_seconds = if unix_secs.is_finite() {
        unix_secs.clamp(-MAX_UNIX_SECS, MAX_UNIX_SECS)
    } else {
        0.0
    };
    let days = (total_seconds / 86_400.0).floor() as i64;
    // `rem_euclid` keeps the time of day in [0, 86400) even for the negative
    // extremes, where `total - days * 86400` would not.
    let secs_of_day = total_seconds.rem_euclid(86_400.0) as i64;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = (year + i64::from(month <= 2)) as i32;

    CivilDate {
        year,
        month,
        day,
        hour: (secs_of_day / 3_600) as u32,
        minute: ((secs_of_day % 3_600) / 60) as u32,
        days,
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = i64::from(if month > 2 { month - 3 } else { month + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// 1970-01-01 was a Thursday (4).
fn weekday_from_days(days: i64) -> i64 {
    (days + 4).rem_euclid(7)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => 0,
    }
}

/// The MetricsQL label-rewriting functions
/// (`app/vmselect/promql/transform.go`).
///
/// Every variant rewrites the labels of each series and leaves its value and
/// timestamp alone, except `Value`, which replaces the value with one parsed
/// from a label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelOp {
    /// `label_set(v, "a", "1", ...)` — name/value pairs; an empty value
    /// deletes the label. `alias(v, "name")` is the same call with
    /// `__name__` as the destination (MetricsQL rewrites it that way).
    Set(Vec<(String, String)>),
    /// `label_del(v, "a", ...)` — drops the named labels, `__name__`
    /// included when it is named.
    Del(Vec<String>),
    /// `label_keep(v, "a", ...)` — drops everything else, `__name__`
    /// included unless it is named.
    Keep(Vec<String>),
    /// `label_copy(v, "src", "dst", ...)` — copies the source value to the
    /// destination and leaves the source alone.
    Copy(Vec<(String, String)>),
    /// `label_move(v, "src", "dst", ...)` — a copy that then deletes the
    /// source, unless source and destination are the same label.
    Move(Vec<(String, String)>),
    /// `label_value(v, "label")` — replaces each non-NaN value with the
    /// label's text parsed as a float (NaN when it does not parse) and
    /// drops `__name__`.
    Value(String),
}

impl LabelOp {
    pub fn name(&self) -> &'static str {
        match self {
            LabelOp::Set(_) => "label_set",
            LabelOp::Del(_) => "label_del",
            LabelOp::Keep(_) => "label_keep",
            LabelOp::Copy(_) => "label_copy",
            LabelOp::Move(_) => "label_move",
            LabelOp::Value(_) => "label_value",
        }
    }
}

/// The MetricsQL summary functions over classic `le` buckets
/// (`avgForLeTimeseries` / `stdvarForLeTimeseries` in `transform.go`).
///
/// VictoriaMetrics treats each bucket as uniform between its bound and the
/// previous one and weights the bucket's midpoint by how many observations it
/// holds; the total gives an approximate mean or variance. Prometheus' own
/// functions with these names only read native histograms, so this is
/// deliberately VM's classic-bucket semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramStatOp {
    Avg,
    Stddev,
    Stdvar,
}

impl HistogramStatOp {
    pub fn name(self) -> &'static str {
        match self {
            HistogramStatOp::Avg => "histogram_avg",
            HistogramStatOp::Stddev => "histogram_stddev",
            HistogramStatOp::Stdvar => "histogram_stdvar",
        }
    }

    fn parse(name: &str) -> Option<HistogramStatOp> {
        Some(match name {
            "histogram_avg" => HistogramStatOp::Avg,
            "histogram_stddev" => HistogramStatOp::Stddev,
            "histogram_stdvar" => HistogramStatOp::Stdvar,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Number(f64),
    /// A bare string literal. Only `count_values` accepts one, as the name of
    /// the label it writes the distinct values into.
    Str(String),
    /// A selector, with an optional `offset` (milliseconds) and `@` (ms).
    Selector {
        selector: Selector,
        offset_ms: i64,
        at: Option<AtModifier>,
    },
    /// `metric[5m]` — only valid as a range-function argument.
    Range {
        selector: Selector,
        range_ms: i64,
        offset_ms: i64,
        at: Option<AtModifier>,
    },
    /// `expr[5m:1m]` — a range vector built by evaluating `expr` every
    /// `step_ms` inside `(at - offset - range, at - offset]`. Only valid as a
    /// range-function argument, like `Range`.
    Subquery {
        expr: Box<Expr>,
        range_ms: i64,
        step_ms: i64,
        offset_ms: i64,
        at: Option<AtModifier>,
    },
    Unary {
        negate: bool,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
        /// `bool` makes a comparison return 0/1 instead of filtering.
        ret_bool: bool,
        /// The `on` / `ignoring` / `group_left` / `group_right` modifiers.
        /// `VectorMatching::default()` is one-to-one on all labels but
        /// `__name__`, which is what an unmodified operator does.
        matching: VectorMatching,
    },
    Aggregation {
        op: AggOp,
        expr: Box<Expr>,
        param: Option<Box<Expr>>,
        grouping: Option<Grouping>,
    },
    /// A function over a range vector. `expr` is an `Expr::Range` or an
    /// `Expr::Subquery`; the range is carried by that node so `rate(a[5m])` and
    /// `rate(a[5m:1m])` share one path.
    RangeFunc {
        func: RangeFunc,
        expr: Box<Expr>,
        param: Option<Box<Expr>>,
        /// The second scalar argument. Only `double_exponential_smoothing`
        /// takes two (`sf` and `tf`); every other range function has one or
        /// none.
        param2: Option<Box<Expr>>,
    },
    MathFunc {
        func: MathFunc,
        param: Option<Box<Expr>>,
        expr: Box<Expr>,
    },
    /// `label_replace(v, dst, replacement, src, regex)` — rewrites one label of
    /// each series, leaving series whose `src` value does not match untouched.
    LabelReplace {
        expr: Box<Expr>,
        dst: String,
        /// The `$1` / `${name}` template expanded with the regex's captures.
        replacement: String,
        src: String,
        pattern: Pattern,
    },
    /// `label_join(v, dst, separator, src...)` — joins several labels into one.
    LabelJoin {
        expr: Box<Expr>,
        dst: String,
        separator: String,
        srcs: Vec<String>,
    },
    /// MetricsQL's label helpers: `label_set`, `label_del`, `label_keep`,
    /// `label_copy`, `label_move`, `label_value` and `alias`.
    LabelOp {
        op: LabelOp,
        expr: Box<Expr>,
    },
    Sort {
        dir: SortDir,
        expr: Box<Expr>,
    },
    /// `sort_by_label(v, "a", "b", ...)` — natural-order sort on the named
    /// labels, with the full labelset as a tiebreak.
    SortByLabel {
        desc: bool,
        labels: Vec<String>,
        expr: Box<Expr>,
    },
    Absent(Box<Expr>),
    /// `vector(scalar)` lifts a scalar into a one-sample vector.
    Vector(Box<Expr>),
    /// `scalar(v)` — the sole value of a one-sample vector, NaN otherwise.
    Scalar(Box<Expr>),
    /// `clamp(v, min, max)` — both bounds are scalars, and `max < min` yields
    /// an empty result rather than the input.
    Clamp {
        expr: Box<Expr>,
        min: Box<Expr>,
        max: Box<Expr>,
    },
    /// `histogram_fraction(lower, upper, v)` — both bounds are scalars, `v` a
    /// vector of classic `le` buckets.
    HistogramFraction {
        lower: Box<Expr>,
        upper: Box<Expr>,
        expr: Box<Expr>,
    },
    /// `histogram_quantiles(v, "label", q1, q2, ...)` — one series per
    /// quantile per classic histogram, with the quantile written into the
    /// given label.
    HistogramQuantiles {
        expr: Box<Expr>,
        label: String,
        quantiles: Vec<Expr>,
    },
    /// MetricsQL's `histogram_avg` / `histogram_stddev` / `histogram_stdvar`
    /// over classic `le` buckets.
    HistogramStat {
        op: HistogramStatOp,
        expr: Box<Expr>,
    },
    /// `max_of(a, b)` / `min_of(a, b)` — two scalars, one scalar result
    /// (Prometheus' duration-expression helpers, also usable on plain
    /// numbers).
    Extremum {
        max: bool,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    /// `day_of_month(v)` and friends. With an argument each sample's value is
    /// read as Unix seconds; with none the evaluation instant is used.
    DateFunc {
        func: DateFunc,
        expr: Option<Box<Expr>>,
    },
    /// `time()` — the evaluation instant in seconds.
    Time,
    /// `start()` / `end()` / `range()` / `step()` — the query's own time range
    /// and step, as seconds. Prometheus folds them into number literals before
    /// evaluation (`foldQueryContextFunctions`); an instant query has no step,
    /// so `step()` is 0 there.
    QueryContext(QueryContext),
}

/// The query-context scalars Prometheus folds into number literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryContext {
    Start,
    End,
    Range,
    Step,
}

impl Expr {
    /// Collects every selector in the expression, so the engine can be asked
    /// for each one's samples once per request.
    pub fn selectors(&self, out: &mut Vec<Selector>) {
        match self {
            Expr::Number(_) | Expr::Str(_) | Expr::Time | Expr::QueryContext(_) => {}
            Expr::Selector { selector, .. } | Expr::Range { selector, .. } => {
                out.push(selector.clone())
            }
            Expr::Subquery { expr, .. } => expr.selectors(out),
            Expr::Unary { expr, .. } => expr.selectors(out),
            Expr::Binary { lhs, rhs, .. } => {
                lhs.selectors(out);
                rhs.selectors(out);
            }
            Expr::Aggregation { expr, param, .. } => {
                expr.selectors(out);
                if let Some(param) = param {
                    param.selectors(out);
                }
            }
            Expr::RangeFunc {
                expr,
                param,
                param2,
                ..
            } => {
                expr.selectors(out);
                if let Some(param) = param {
                    param.selectors(out);
                }
                if let Some(param2) = param2 {
                    param2.selectors(out);
                }
            }
            Expr::MathFunc { param, expr, .. } => {
                if let Some(param) = param {
                    param.selectors(out);
                }
                expr.selectors(out);
            }
            Expr::Sort { expr, .. }
            | Expr::SortByLabel { expr, .. }
            | Expr::Absent(expr)
            | Expr::Vector(expr) => expr.selectors(out),
            Expr::Scalar(expr) => expr.selectors(out),
            Expr::Clamp { expr, min, max } => {
                expr.selectors(out);
                min.selectors(out);
                max.selectors(out);
            }
            Expr::HistogramFraction { lower, upper, expr } => {
                lower.selectors(out);
                upper.selectors(out);
                expr.selectors(out);
            }
            Expr::HistogramQuantiles {
                expr, quantiles, ..
            } => {
                expr.selectors(out);
                for quantile in quantiles {
                    quantile.selectors(out);
                }
            }
            Expr::HistogramStat { expr, .. } => expr.selectors(out),
            Expr::Extremum { lhs, rhs, .. } => {
                lhs.selectors(out);
                rhs.selectors(out);
            }
            Expr::DateFunc { expr, .. } => {
                if let Some(expr) = expr {
                    expr.selectors(out);
                }
            }
            Expr::LabelReplace { expr, .. }
            | Expr::LabelJoin { expr, .. }
            | Expr::LabelOp { expr, .. } => expr.selectors(out),
        }
    }

    /// The absolute time window the engine must be asked for, given the
    /// query's own range.
    ///
    /// An instant selector needs `(e - lookback, e]`; a range selector needs
    /// `(e - range, e]`, where `e` is the pinned `@` (or `now - offset`). With
    /// no `@`, `e` runs over the whole query range, so the window is widened
    /// by the query's own span. Returned half-open: `[start, end)`.
    pub fn scan_window(&self, query_start_ms: i64, query_end_ms: i64) -> (i64, i64) {
        let mut start = query_start_ms.saturating_sub(LOOKBACK_MS);
        let mut end = query_end_ms.saturating_add(1);
        self.widen_window(query_start_ms, query_end_ms, &mut start, &mut end);
        (start, end)
    }

    fn widen_window(&self, query_start_ms: i64, query_end_ms: i64, start: &mut i64, end: &mut i64) {
        match self {
            Expr::Number(_) | Expr::Str(_) | Expr::Time | Expr::QueryContext(_) => {}
            Expr::Selector {
                offset_ms,
                at,
                selector: _,
            }
            | Expr::Range {
                offset_ms,
                at,
                range_ms: _,
                selector: _,
            } => {
                let span = match self {
                    Expr::Range { range_ms, .. } => *range_ms,
                    _ => 0,
                };
                // Without `@` the instant varies across the query range.
                let (lowest, highest) = match at {
                    Some(modifier) => {
                        let at = modifier.resolve(query_start_ms, query_end_ms);
                        (at, at)
                    }
                    None => (query_start_ms, query_end_ms),
                };
                *start = (*start).min(lowest - offset_ms - span - LOOKBACK_MS);
                *end = (*end).max(highest - offset_ms + 1);
            }
            Expr::Subquery {
                expr,
                range_ms,
                step_ms,
                offset_ms,
                at,
            } => {
                // The inner expression is evaluated on the subquery's step
                // grid, which starts after `lowest - offset - range`; widen by
                // one step to be sure every grid point is covered even when
                // the alignment rounds down.
                let step = subquery_step(*step_ms);
                let (lowest, highest) = match at {
                    Some(modifier) => {
                        let at = modifier.resolve(query_start_ms, query_end_ms);
                        (at, at)
                    }
                    None => (query_start_ms, query_end_ms),
                };
                let inner_start = lowest - offset_ms - range_ms - step;
                let inner_end = highest - offset_ms;
                expr.widen_window(inner_start, inner_end, start, end);
                *start = (*start).min(inner_start - LOOKBACK_MS);
                *end = (*end).max(inner_end + 1);
            }
            Expr::Unary { expr, .. } => expr.widen_window(query_start_ms, query_end_ms, start, end),
            Expr::Binary { lhs, rhs, .. } => {
                lhs.widen_window(query_start_ms, query_end_ms, start, end);
                rhs.widen_window(query_start_ms, query_end_ms, start, end);
            }
            Expr::Aggregation { expr, param, .. } => {
                expr.widen_window(query_start_ms, query_end_ms, start, end);
                if let Some(param) = param {
                    param.widen_window(query_start_ms, query_end_ms, start, end);
                }
            }
            Expr::RangeFunc {
                expr,
                param,
                param2,
                ..
            } => {
                expr.widen_window(query_start_ms, query_end_ms, start, end);
                if let Some(param) = param {
                    param.widen_window(query_start_ms, query_end_ms, start, end);
                }
                if let Some(param2) = param2 {
                    param2.widen_window(query_start_ms, query_end_ms, start, end);
                }
            }
            Expr::MathFunc { param, expr, .. } => {
                if let Some(param) = param {
                    param.widen_window(query_start_ms, query_end_ms, start, end);
                }
                expr.widen_window(query_start_ms, query_end_ms, start, end);
            }
            Expr::Sort { expr, .. }
            | Expr::SortByLabel { expr, .. }
            | Expr::Absent(expr)
            | Expr::Vector(expr) => expr.widen_window(query_start_ms, query_end_ms, start, end),
            Expr::Scalar(expr) => expr.widen_window(query_start_ms, query_end_ms, start, end),
            Expr::Clamp { expr, min, max } => {
                expr.widen_window(query_start_ms, query_end_ms, start, end);
                min.widen_window(query_start_ms, query_end_ms, start, end);
                max.widen_window(query_start_ms, query_end_ms, start, end);
            }
            Expr::HistogramFraction { lower, upper, expr } => {
                lower.widen_window(query_start_ms, query_end_ms, start, end);
                upper.widen_window(query_start_ms, query_end_ms, start, end);
                expr.widen_window(query_start_ms, query_end_ms, start, end);
            }
            Expr::HistogramQuantiles {
                expr, quantiles, ..
            } => {
                expr.widen_window(query_start_ms, query_end_ms, start, end);
                for quantile in quantiles {
                    quantile.widen_window(query_start_ms, query_end_ms, start, end);
                }
            }
            Expr::HistogramStat { expr, .. } => {
                expr.widen_window(query_start_ms, query_end_ms, start, end)
            }
            Expr::Extremum { lhs, rhs, .. } => {
                lhs.widen_window(query_start_ms, query_end_ms, start, end);
                rhs.widen_window(query_start_ms, query_end_ms, start, end);
            }
            Expr::DateFunc { expr, .. } => {
                if let Some(expr) = expr {
                    expr.widen_window(query_start_ms, query_end_ms, start, end);
                }
            }
            Expr::LabelReplace { expr, .. }
            | Expr::LabelJoin { expr, .. }
            | Expr::LabelOp { expr, .. } => {
                expr.widen_window(query_start_ms, query_end_ms, start, end)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parses a PromQL expression.
pub fn parse(input: &str) -> Result<Expr> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return bad("empty query");
    }
    let mut parser = Parser { tokens, pos: 0 };
    let expr = parser.expression(0)?;
    if parser.pos < parser.tokens.len() {
        return bad(format!(
            "unexpected {} at the end of the query",
            parser.tokens[parser.pos].describe()
        ));
    }
    Ok(expr)
}

/// Binding power, tightest last. Comparisons sit below arithmetic and set
/// operators below them, matching Prometheus' precedence.
fn precedence(op: &str) -> Option<(u8, BinaryOp)> {
    Some(match op {
        "or" => (1, BinaryOp::Or),
        "and" => (2, BinaryOp::And),
        "unless" => (2, BinaryOp::Unless),
        "==" => (3, BinaryOp::Eq),
        "!=" => (3, BinaryOp::Neq),
        ">" => (3, BinaryOp::Gt),
        "<" => (3, BinaryOp::Lt),
        ">=" => (3, BinaryOp::Gte),
        "<=" => (3, BinaryOp::Lte),
        "+" => (4, BinaryOp::Add),
        "-" => (4, BinaryOp::Sub),
        "*" => (5, BinaryOp::Mul),
        "/" => (5, BinaryOp::Div),
        "%" => (5, BinaryOp::Mod),
        "atan2" => (5, BinaryOp::Atan2),
        "^" => (6, BinaryOp::Pow),
        _ => return None,
    })
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn eat(&mut self, token: &Token) -> bool {
        if self.peek() == Some(token) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, token: Token) -> Result<()> {
        if self.eat(&token) {
            return Ok(());
        }
        let found = match self.peek() {
            Some(found) => found.describe(),
            None => "end of query".to_string(),
        };
        bad(format!("expected {} but found {found}", token.describe()))
    }

    fn ident(&mut self) -> Option<String> {
        match self.peek() {
            Some(Token::Ident(name)) => {
                let name = name.clone();
                self.pos += 1;
                Some(name)
            }
            _ => None,
        }
    }

    /// Precedence-climbing over binary operators.
    fn expression(&mut self, min_bp: u8) -> Result<Expr> {
        let mut lhs = self.unary()?;
        loop {
            let Some(next) = self.peek().cloned() else {
                break;
            };
            let (text, is_ident) = match &next {
                Token::Op(op) => (*op, false),
                Token::Ident(name) => (name.as_str(), true),
                _ => break,
            };
            let Some((bp, op)) = precedence(text) else {
                break;
            };
            if bp < min_bp {
                break;
            }
            self.pos += 1;
            let ret_bool = if op.is_comparison() && self.peek_ident_is("bool") {
                self.pos += 1;
                true
            } else {
                false
            };
            if is_ident && !op.is_set() && op != BinaryOp::Atan2 {
                unreachable!("only set operators and atan2 are spelled with words");
            }
            let matching = self.bin_modifier(op)?;
            let rhs = self.expression(bp + 1)?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                ret_bool,
                matching,
            };
        }
        Ok(lhs)
    }

    /// Parses the modifiers after a binary operator: an optional
    /// `on(...)` / `ignoring(...)` and an optional
    /// `group_left(...)` / `group_right(...)`.
    fn bin_modifier(&mut self, op: BinaryOp) -> Result<VectorMatching> {
        let mut matching = VectorMatching::default();
        if self.peek_ident_is("on") || self.peek_ident_is("ignoring") {
            matching.on = self.peek_ident_is("on");
            self.pos += 1;
            matching.matching_labels = self.label_list()?;
        }
        let card = if self.peek_ident_is("group_left") {
            self.pos += 1;
            Cardinality::ManyToOne
        } else if self.peek_ident_is("group_right") {
            self.pos += 1;
            Cardinality::OneToMany
        } else {
            Cardinality::OneToOne
        };
        if card != Cardinality::OneToOne {
            matching.card = card;
            // The trailing label list is optional.
            if matches!(self.peek(), Some(Token::LParen)) {
                matching.include = self.label_list()?;
            }
        }
        if op.is_set() {
            if matching.card != Cardinality::OneToOne {
                return bad(format!("no grouping allowed for `{}` operation", op.name()));
            }
            // Set operators are implicitly many-to-many: several series may
            // share a match key on either side.
            matching.card = Cardinality::ManyToMany;
        }
        if matching.on {
            for name in &matching.include {
                if matching.matching_labels.contains(name) {
                    return bad(format!(
                        "label {name:?} must not occur in ON and GROUP clause at once"
                    ));
                }
            }
        }
        // Prometheus' `fill(...)` / `fill_left(...)` / `fill_right(...)`
        // modifiers are not implemented; name them rather than leaving the
        // caller guessing.
        for unsupported in ["fill", "fill_left", "fill_right"] {
            if self.peek_ident_is(unsupported) {
                return bad(format!("`{unsupported}` vector matching is not supported"));
            }
        }
        Ok(matching)
    }

    /// Parses `(a, b, c)`, allowing the trailing comma Prometheus accepts.
    fn label_list(&mut self) -> Result<Vec<String>> {
        self.expect(Token::LParen)?;
        let mut labels = Vec::new();
        loop {
            if !self.eat(&Token::Comma) {
                match self.ident() {
                    Some(name) => labels.push(name),
                    None => break,
                }
                continue;
            }
            if matches!(self.peek(), Some(Token::RParen)) {
                break;
            }
        }
        self.expect(Token::RParen)?;
        Ok(labels)
    }

    fn peek_ident_is(&self, name: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(found)) if found == name)
    }

    fn unary(&mut self) -> Result<Expr> {
        match self.peek() {
            Some(Token::Op(op @ ("+" | "-"))) => {
                let negate = *op == "-";
                self.pos += 1;
                let expr = self.expression(7)?;
                Ok(Expr::Unary {
                    negate,
                    expr: Box::new(expr),
                })
            }
            _ => self.power(),
        }
    }

    fn power(&mut self) -> Result<Expr> {
        let lhs = self.postfix()?;
        if match self.peek() {
            Some(Token::Op("^")) => true,
            _ => false,
        } {
            self.pos += 1;
            let matching = self.bin_modifier(BinaryOp::Pow)?;
            // Right associative, and the exponent may be a bare scalar.
            let rhs = self.expression(6)?;
            return Ok(Expr::Binary {
                op: BinaryOp::Pow,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                ret_bool: false,
                matching,
            });
        }
        Ok(lhs)
    }

    /// Applies `[range]`, `[range:step]`, `offset d` and `@ t` to a primary
    /// expression.
    fn postfix(&mut self) -> Result<Expr> {
        let expr = self.primary()?;
        let mut offset_ms: i64 = 0;
        let mut at: Option<AtModifier> = None;
        // `(range_ms, step_ms)`: a `None` step is `metric[5m]` (raw samples), a
        // `Some` one is `expr[5m:1m]` (a subquery; 0 means the default step).
        let mut range: Option<(i64, Option<i64>)> = None;
        loop {
            match self.peek() {
                Some(Token::LBracket) => {
                    self.pos += 1;
                    let range_ms = self.duration("inside [ ]")?;
                    let step = if self.eat(&Token::Op(":")) {
                        // `[5m:]` leaves the step out and uses the default
                        // evaluation interval, like Prometheus.
                        if matches!(self.peek(), Some(Token::RBracket)) {
                            Some(0)
                        } else {
                            let step = self.duration("after ':' in a subquery")?;
                            if step <= 0 {
                                return bad("a subquery step must be positive");
                            }
                            Some(step)
                        }
                    } else {
                        None
                    };
                    self.expect(Token::RBracket)?;
                    if range.is_some() {
                        return bad("a range selector may only be applied once; put the inner \
                             expression in parentheses for a subquery");
                    }
                    if range_ms <= 0 {
                        return bad("a range must be positive");
                    }
                    range = Some((range_ms, step));
                }
                Some(Token::Ident(word)) if word == "offset" => {
                    self.pos += 1;
                    let shift = self.duration("after 'offset'")?;
                    offset_ms = offset_ms
                        .checked_add(shift)
                        .ok_or_else(|| Error::Bad("offset overflows".to_string()))?;
                }
                Some(Token::Op("@")) => {
                    self.pos += 1;
                    let modifier = match self.next() {
                        Some(Token::Number(value)) if value.is_finite() => {
                            AtModifier::At((value * 1_000.0) as i64)
                        }
                        Some(Token::Ident(word)) if word == "start" => {
                            self.expect(Token::LParen)?;
                            self.expect(Token::RParen)?;
                            AtModifier::Start
                        }
                        Some(Token::Ident(word)) if word == "end" => {
                            self.expect(Token::LParen)?;
                            self.expect(Token::RParen)?;
                            AtModifier::End
                        }
                        other => {
                            return bad(format!(
                                "expected a timestamp after '@' but found {}",
                                describe(other)
                            ))
                        }
                    };
                    if at.is_some() {
                        return bad("the `@` modifier may only be set once");
                    }
                    at = Some(modifier);
                }
                _ => break,
            }
        }
        match (expr, range) {
            (
                Expr::Selector {
                    selector,
                    offset_ms: inner_offset,
                    at: inner_at,
                },
                None,
            ) => {
                let offset_ms = inner_offset
                    .checked_add(offset_ms)
                    .ok_or_else(|| Error::Bad("offset overflows".to_string()))?;
                Ok(Expr::Selector {
                    selector,
                    offset_ms,
                    at: at.or(inner_at),
                })
            }
            (
                Expr::Selector {
                    selector,
                    offset_ms: inner_offset,
                    at: inner_at,
                },
                Some((range_ms, None)),
            ) => {
                let offset_ms = inner_offset
                    .checked_add(offset_ms)
                    .ok_or_else(|| Error::Bad("offset overflows".to_string()))?;
                Ok(Expr::Range {
                    selector,
                    range_ms,
                    offset_ms,
                    at: at.or(inner_at),
                })
            }
            (inner, Some((range_ms, Some(step_ms)))) => {
                // A subquery needs an instant vector inside it; a scalar or a
                // range vector has no per-step value to sample.
                if matches!(inner, Expr::Number(_) | Expr::Time | Expr::QueryContext(_)) {
                    return bad("subquery is only allowed on instant vector, got scalar instead");
                }
                if matches!(inner, Expr::Range { .. } | Expr::Subquery { .. }) {
                    return bad(
                        "subquery is only allowed on instant vector, got range vector instead",
                    );
                }
                Ok(Expr::Subquery {
                    expr: Box::new(inner),
                    range_ms,
                    step_ms,
                    offset_ms,
                    at,
                })
            }
            (other, Some((_, None))) => bad(format!(
                "a range selector needs a plain metric name, got {}",
                render(&other)
            )),
            (other, None) => {
                if offset_ms != 0 || at.is_some() {
                    // `(expr) offset 5m` is legal PromQL only for selectors
                    // and subqueries; refuse rather than ignore the shift.
                    return bad(format!(
                        "`offset` and `@` are only supported on a selector or a subquery, \
                         not on this expression ({})",
                        render(&other)
                    ));
                }
                Ok(other)
            }
        }
    }

    /// A duration argument: a unit-suffixed duration, or a bare number of
    /// seconds as Prometheus accepts inside `[ ]` and after `offset`.
    fn duration(&mut self, context: &str) -> Result<i64> {
        match self.next() {
            Some(Token::Duration(ms)) => Ok(ms),
            Some(Token::Number(seconds)) if seconds >= 0.0 && seconds.is_finite() => {
                Ok((seconds * 1_000.0) as i64)
            }
            other => bad(format!(
                "expected a duration {context} but found {}",
                describe(other)
            )),
        }
    }

    fn primary(&mut self) -> Result<Expr> {
        match self.next() {
            Some(Token::Number(value)) => Ok(Expr::Number(value)),
            Some(Token::Duration(ms)) => Ok(Expr::Number(ms as f64)),
            Some(Token::LParen) => {
                let inner = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(inner)
            }
            Some(Token::LBrace) => {
                let selector = self.matchers(None)?;
                Ok(Expr::Selector {
                    selector,
                    offset_ms: 0,
                    at: None,
                })
            }
            Some(Token::Ident(name)) => {
                if matches!(self.peek(), Some(Token::LParen)) {
                    self.call(&name)
                } else if matches!(self.peek(), Some(Token::LBrace)) {
                    self.pos += 1;
                    let selector = self.matchers(Some(name))?;
                    Ok(Expr::Selector {
                        selector,
                        offset_ms: 0,
                        at: None,
                    })
                } else if let Some(op) = AggOp::parse(&name) {
                    // `sum by (job) (...)` / `sum without (...) (...)`
                    let grouping = self.grouping()?;
                    let Some(grouping) = grouping else {
                        return bad(format!("expected a grouping after {name}"));
                    };
                    self.expect(Token::LParen)?;
                    // `topk` / `bottomk` / `quantile` carry a parameter first.
                    let param = if op.takes_param() {
                        let param = self.aggregation_param(op)?;
                        self.expect(Token::Comma)?;
                        Some(Box::new(param))
                    } else {
                        None
                    };
                    let expr = self.expression(0)?;
                    self.expect(Token::RParen)?;
                    Ok(Expr::Aggregation {
                        op,
                        expr: Box::new(expr),
                        param,
                        grouping: Some(grouping),
                    })
                } else {
                    Ok(Expr::Selector {
                        selector: Selector {
                            metric: name,
                            matchers: Vec::new(),
                        },
                        offset_ms: 0,
                        at: None,
                    })
                }
            }
            other => bad(format!(
                "expected a metric name, number or '(' but found {}",
                describe(other)
            )),
        }
    }

    /// Parses the parameter that precedes an aggregation's vector argument.
    ///
    /// `topk` / `bottomk` / `quantile` take a scalar expression, while
    /// `count_values` takes a quoted label name — the label the distinct sample
    /// values are written into.
    fn aggregation_param(&mut self, op: AggOp) -> Result<Expr> {
        if op.takes_label_param() {
            let label = self.string_argument(op.name())?;
            validate_label_name(&label, op.name())?;
            return Ok(Expr::Str(label));
        }
        self.expression(0)
    }

    /// Parses `by (a, b)` / `without (a)` if present.
    fn grouping(&mut self) -> Result<Option<Grouping>> {
        let without = match self.peek() {
            Some(Token::Ident(word)) if word == "by" => false,
            Some(Token::Ident(word)) if word == "without" => true,
            _ => return Ok(None),
        };
        self.pos += 1;
        self.expect(Token::LParen)?;
        let mut labels = Vec::new();
        loop {
            if !self.eat(&Token::Comma) {
                match self.ident() {
                    Some(name) => labels.push(name),
                    None => break,
                }
                continue;
            }
            if matches!(self.peek(), Some(Token::RParen)) {
                break;
            }
        }
        self.expect(Token::RParen)?;
        Ok(Some(if without {
            Grouping::Without(labels)
        } else {
            Grouping::By(labels)
        }))
    }

    /// Parses `name(args...)`, dispatching on the name.
    fn call(&mut self, name: &str) -> Result<Expr> {
        match name {
            // `label_replace` and `label_join` rewrite labels without touching
            // values or timestamps, so they work per step like any other
            // instant function.
            "label_replace" => {
                self.expect(Token::LParen)?;
                let expr = self.expression(0)?;
                self.expect(Token::Comma)?;
                let dst = self.string_argument(name)?;
                self.expect(Token::Comma)?;
                let replacement = self.string_argument(name)?;
                self.expect(Token::Comma)?;
                let src = self.string_argument(name)?;
                self.expect(Token::Comma)?;
                let pattern = self.string_argument(name)?;
                self.expect(Token::RParen)?;
                validate_label_name(&dst, name)?;
                // Prometheus anchors the pattern with dot-all, so `(.*)` covers
                // a newline and `a` matches only the whole value.
                let regex = regex::Regex::new(&format!("^(?s:{pattern})$")).map_err(|e| {
                    Error::Bad(format!(
                        "invalid regular expression in label_replace(): {pattern} ({e})"
                    ))
                })?;
                return Ok(Expr::LabelReplace {
                    expr: Box::new(expr),
                    dst,
                    replacement,
                    src,
                    pattern: Pattern(regex),
                });
            }
            "label_join" => {
                self.expect(Token::LParen)?;
                let expr = self.expression(0)?;
                self.expect(Token::Comma)?;
                let dst = self.string_argument(name)?;
                self.expect(Token::Comma)?;
                let separator = self.string_argument(name)?;
                let mut srcs = Vec::new();
                while self.eat(&Token::Comma) {
                    let src = self.string_argument(name)?;
                    validate_label_name(&src, name)?;
                    srcs.push(src);
                }
                self.expect(Token::RParen)?;
                if srcs.is_empty() {
                    return bad("label_join needs at least one source label");
                }
                validate_label_name(&dst, name)?;
                return Ok(Expr::LabelJoin {
                    expr: Box::new(expr),
                    dst,
                    separator,
                    srcs,
                });
            }
            // MetricsQL's label helpers rewrite labels per series without
            // touching the values, so they are per-step instant functions like
            // `label_replace`.
            "label_set" | "label_del" | "label_keep" | "label_copy" | "label_move"
            | "label_value" | "alias" => return self.label_op(name),
            // `aggr_over_time(avg, v[range])` names an aggregation where the
            // other range functions take a vector, so it is dispatched first.
            "aggr_over_time" => {
                self.expect(Token::LParen)?;
                let op = match self.next() {
                    Some(Token::Ident(word)) => AggOp::parse(&word).ok_or_else(|| {
                        Error::Bad(format!(
                            "`{word}` is not an aggregation; aggr_over_time accepts sum, \
                             avg, min, max, count, stddev, stdvar"
                        ))
                    })?,
                    Some(Token::Str(word)) => AggOp::parse(&word).ok_or_else(|| {
                        Error::Bad(format!(
                            "`{word}` is not an aggregation; aggr_over_time accepts sum, \
                             avg, min, max, count, stddev, stdvar"
                        ))
                    })?,
                    other => {
                        return bad(format!(
                            "expected an aggregation name but found {}",
                            describe(other)
                        ))
                    }
                };
                if op.takes_param() {
                    return bad(format!(
                        "`{}` takes a parameter and is not an aggregation over_time accepts; \
                         use quantile_over_time for a quantile",
                        op.name()
                    ));
                }
                self.expect(Token::Comma)?;
                let inner = self.range_argument("aggr_over_time")?;
                self.expect(Token::RParen)?;
                return Ok(Expr::RangeFunc {
                    func: RangeFunc::AggrOverTime(op),
                    expr: Box::new(inner),
                    param: None,
                    param2: None,
                });
            }
            // `predict_linear(v[range], t)` and
            // `double_exponential_smoothing(v[range], sf, tf)` put the range
            // vector first, the reverse of `quantile_over_time(0.9, v[range])`,
            // so they are dispatched separately too.
            "predict_linear" | "double_exponential_smoothing" => {
                self.expect(Token::LParen)?;
                let inner = self.range_argument(name)?;
                self.expect(Token::Comma)?;
                let first = self.expression(0)?;
                let second = if name == "double_exponential_smoothing" {
                    self.expect(Token::Comma)?;
                    Some(Box::new(self.expression(0)?))
                } else {
                    None
                };
                self.expect(Token::RParen)?;
                let func = if name == "predict_linear" {
                    RangeFunc::PredictLinear
                } else {
                    RangeFunc::DoubleExpSmoothing
                };
                return Ok(Expr::RangeFunc {
                    func,
                    expr: Box::new(inner),
                    param: Some(Box::new(first)),
                    param2: second,
                });
            }
            // `integral(v[range], strategy=2)` also puts the range vector
            // first, but its parameter is optional (the trapezoidal rule is the
            // default), so it gets its own arm.
            "integral" => {
                self.expect(Token::LParen)?;
                let inner = self.range_argument(name)?;
                let param = if self.eat(&Token::Comma) {
                    Some(Box::new(self.expression(0)?))
                } else {
                    None
                };
                self.expect(Token::RParen)?;
                return Ok(Expr::RangeFunc {
                    func: RangeFunc::Integral,
                    expr: Box::new(inner),
                    param,
                    param2: None,
                });
            }
            _ => {}
        }
        self.expect(Token::LParen)?;
        // Aggregations may carry a parameter first (`topk(5, expr)`).
        if let Some(op) = AggOp::parse(name) {
            if op.takes_param() {
                let param = self.aggregation_param(op)?;
                self.expect(Token::Comma)?;
                let expr = self.expression(0)?;
                self.expect(Token::RParen)?;
                let grouping = self.grouping()?;
                return Ok(Expr::Aggregation {
                    op,
                    expr: Box::new(expr),
                    param: Some(Box::new(param)),
                    grouping,
                });
            }
            let expr = self.expression(0)?;
            self.expect(Token::RParen)?;
            let grouping = self.grouping()?;
            return Ok(Expr::Aggregation {
                op,
                expr: Box::new(expr),
                param: None,
                grouping,
            });
        }
        if let Some(func) = RangeFunc::parse(name) {
            if func.takes_param() {
                let param = self.expression(0)?;
                self.expect(Token::Comma)?;
                let inner = self.range_argument(name)?;
                self.expect(Token::RParen)?;
                return Ok(Expr::RangeFunc {
                    func,
                    expr: Box::new(inner),
                    param: Some(Box::new(param)),
                    param2: None,
                });
            }
            let inner = self.range_argument(name)?;
            self.expect(Token::RParen)?;
            return Ok(Expr::RangeFunc {
                func,
                expr: Box::new(inner),
                param: None,
                param2: None,
            });
        }
        if let Some(func) = MathFunc::parse(name) {
            match func {
                MathFunc::ClampMin | MathFunc::ClampMax | MathFunc::Default => {
                    let expr = self.expression(0)?;
                    self.expect(Token::Comma)?;
                    let param = self.expression(0)?;
                    self.expect(Token::RParen)?;
                    return Ok(Expr::MathFunc {
                        func,
                        param: Some(Box::new(param)),
                        expr: Box::new(expr),
                    });
                }
                // `histogram_quantile(q, v)` also takes the quantile first,
                // but it needs the whole vector to group the buckets.
                MathFunc::HistogramQuantile => {
                    let param = self.expression(0)?;
                    self.expect(Token::Comma)?;
                    let expr = self.expression(0)?;
                    self.expect(Token::RParen)?;
                    return Ok(Expr::MathFunc {
                        func,
                        param: Some(Box::new(param)),
                        expr: Box::new(expr),
                    });
                }
                MathFunc::Round => {
                    let expr = self.expression(0)?;
                    let param = if self.eat(&Token::Comma) {
                        let param = self.expression(0)?;
                        Some(Box::new(param))
                    } else {
                        None
                    };
                    self.expect(Token::RParen)?;
                    return Ok(Expr::MathFunc {
                        func,
                        param,
                        expr: Box::new(expr),
                    });
                }
                _ => {
                    let expr = self.expression(0)?;
                    self.expect(Token::RParen)?;
                    return Ok(Expr::MathFunc {
                        func,
                        param: None,
                        expr: Box::new(expr),
                    });
                }
            }
        }
        match name {
            "max_of" | "min_of" => {
                let lhs = self.expression(0)?;
                self.expect(Token::Comma)?;
                let rhs = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(Expr::Extremum {
                    max: name == "max_of",
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            "histogram_fraction" => {
                let lower = self.expression(0)?;
                self.expect(Token::Comma)?;
                let upper = self.expression(0)?;
                self.expect(Token::Comma)?;
                let expr = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(Expr::HistogramFraction {
                    lower: Box::new(lower),
                    upper: Box::new(upper),
                    expr: Box::new(expr),
                })
            }
            "histogram_quantiles" => {
                let expr = self.expression(0)?;
                self.expect(Token::Comma)?;
                let label = self.string_argument(name)?;
                validate_label_name(&label, name)?;
                let mut quantiles = Vec::new();
                while self.eat(&Token::Comma) {
                    quantiles.push(self.expression(0)?);
                }
                self.expect(Token::RParen)?;
                // Prometheus takes `vector, string` plus at least one variadic
                // scalar and allows nine more (`ArgTypes` has four entries,
                // `Variadic` is 9), so one to ten quantiles.
                if quantiles.is_empty() {
                    return bad(
                        "histogram_quantiles needs at least one quantile after the label name",
                    );
                }
                if quantiles.len() > 10 {
                    return bad(format!(
                        "histogram_quantiles takes at most ten quantiles, got {}",
                        quantiles.len()
                    ));
                }
                Ok(Expr::HistogramQuantiles {
                    expr: Box::new(expr),
                    label,
                    quantiles,
                })
            }
            "clamp" => {
                let expr = self.expression(0)?;
                self.expect(Token::Comma)?;
                let min = self.expression(0)?;
                self.expect(Token::Comma)?;
                let max = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(Expr::Clamp {
                    expr: Box::new(expr),
                    min: Box::new(min),
                    max: Box::new(max),
                })
            }
            // VictoriaMetrics' summary functions over classic `le` buckets.
            "histogram_avg" | "histogram_stddev" | "histogram_stdvar" => {
                let op = HistogramStatOp::parse(name).expect("name matched above");
                let expr = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(Expr::HistogramStat {
                    op,
                    expr: Box::new(expr),
                })
            }
            "scalar" => {
                let expr = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(Expr::Scalar(Box::new(expr)))
            }
            "sort" | "sort_desc" => {
                let expr = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(Expr::Sort {
                    dir: if name == "sort" {
                        SortDir::Asc
                    } else {
                        SortDir::Desc
                    },
                    expr: Box::new(expr),
                })
            }
            "sort_by_label" | "sort_by_label_desc" => {
                let expr = self.expression(0)?;
                let mut labels = Vec::new();
                while self.eat(&Token::Comma) {
                    let label = self.string_argument(name)?;
                    validate_label_name(&label, name)?;
                    labels.push(label);
                }
                self.expect(Token::RParen)?;
                if labels.is_empty() {
                    return bad(format!("{name} needs at least one label name"));
                }
                Ok(Expr::SortByLabel {
                    desc: name == "sort_by_label_desc",
                    labels,
                    expr: Box::new(expr),
                })
            }
            "absent" => {
                let expr = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(Expr::Absent(Box::new(expr)))
            }
            "vector" => {
                let expr = self.expression(0)?;
                self.expect(Token::RParen)?;
                Ok(Expr::Vector(Box::new(expr)))
            }
            "time" => {
                self.expect(Token::RParen)?;
                Ok(Expr::Time)
            }
            "start" | "end" | "range" | "step" => {
                self.expect(Token::RParen)?;
                let context = match name {
                    "start" => QueryContext::Start,
                    "end" => QueryContext::End,
                    "range" => QueryContext::Range,
                    _ => QueryContext::Step,
                };
                Ok(Expr::QueryContext(context))
            }
            "pi" => {
                self.expect(Token::RParen)?;
                Ok(Expr::Number(std::f64::consts::PI))
            }
            other if DateFunc::parse(other).is_some() => {
                let func = DateFunc::parse(other).expect("checked above");
                // The argument is optional: `year()` reads the evaluation
                // instant, `year(v)` each sample's value as Unix seconds.
                let expr = if self.eat(&Token::RParen) {
                    None
                } else {
                    let expr = self.expression(0)?;
                    self.expect(Token::RParen)?;
                    Some(Box::new(expr))
                };
                Ok(Expr::DateFunc { func, expr })
            }
            other => bad(format!(
                "unknown function {other:?}; supported range functions are rate, irate, \
                 increase, delta, idelta, deriv, changes, resets, first_over_time, \
                 ts_of_first_over_time, ts_of_last_over_time, ts_of_max_over_time, \
                 ts_of_min_over_time, predict_linear, double_exponential_smoothing, \
                 integral and *_over_time, the MetricsQL rollups range_over_time, \
                 median_over_time, sum2_over_time, geomean_over_time, distinct_over_time, \
                 mode_over_time, increases_over_time, decreases_over_time, \
                 tfirst_over_time, tlast_over_time, the math \
                 functions abs, ceil, floor, sqrt, exp, ln, log2, log10, sgn, round, \
                 clamp, clamp_min, clamp_max, scalar, timestamp, the trigonometric and \
                 hyperbolic functions sin, cos, tan, asin, acos, atan, sinh, cosh, tanh, \
                 asinh, acosh, atanh, deg, rad, the date functions day_of_month, day_of_week, \
                 day_of_year, days_in_month, hour, minute, month, year, sort_by_label and \
                 sort_by_label_desc, histogram_quantiles, histogram_fraction, histogram_avg, \
                 histogram_stddev, histogram_stdvar, max_of, min_of, \
                 the query-context \
                 functions start, end, range, step, and the label functions label_replace, \
                 label_join, label_set, label_del, label_keep, label_copy, label_move, \
                 label_value and alias"
            )),
        }
    }

    /// Parses one of MetricsQL's label helpers: `label_set`, `label_del`,
    /// `label_keep`, `label_copy`, `label_move`, `label_value` and `alias`.
    ///
    /// The label names are taken as written: VictoriaMetrics stores tags as
    /// arbitrary byte strings, so `label_set(v, "foo.bar", "1")` is legal
    /// there and is accepted here too — unlike `label_replace`, which follows
    /// Prometheus and validates its destination.
    fn label_op(&mut self, name: &str) -> Result<Expr> {
        self.expect(Token::LParen)?;
        let expr = self.expression(0)?;
        let op = match name {
            // MetricsQL rewrites `alias(q, "name")` into
            // `label_set(q, "__name__", "name")`.
            "alias" => {
                self.expect(Token::Comma)?;
                let value = self.string_argument(name)?;
                self.expect(Token::RParen)?;
                LabelOp::Set(vec![("__name__".to_string(), value)])
            }
            "label_value" => {
                self.expect(Token::Comma)?;
                let label = self.string_argument(name)?;
                self.expect(Token::RParen)?;
                LabelOp::Value(label)
            }
            "label_del" | "label_keep" => {
                let labels = self.string_list(name)?;
                if name == "label_del" {
                    LabelOp::Del(labels)
                } else {
                    LabelOp::Keep(labels)
                }
            }
            // `label_set`, `label_copy` and `label_move` take name/value
            // pairs; VM rejects an odd number of strings.
            _ => {
                let strings = self.string_list(name)?;
                if strings.len() % 2 != 0 {
                    return bad(format!(
                        "the number of string args to {name} must be even; got {}",
                        strings.len()
                    ));
                }
                let mut pairs = Vec::with_capacity(strings.len() / 2);
                let mut strings = strings.into_iter();
                while let (Some(dst), Some(value)) = (strings.next(), strings.next()) {
                    pairs.push((dst, value));
                }
                match name {
                    "label_set" => LabelOp::Set(pairs),
                    "label_copy" => LabelOp::Copy(pairs),
                    _ => LabelOp::Move(pairs),
                }
            }
        };
        Ok(Expr::LabelOp {
            op,
            expr: Box::new(expr),
        })
    }

    /// Parses `, "a", "b", ...)` and the closing parenthesis.
    fn string_list(&mut self, name: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        while self.eat(&Token::Comma) {
            out.push(self.string_argument(name)?);
        }
        self.expect(Token::RParen)?;
        Ok(out)
    }

    /// Parses a quoted string argument, the only form `label_replace` and
    /// `label_join` accept for label names, separators and patterns.
    fn string_argument(&mut self, name: &str) -> Result<String> {
        match self.next() {
            Some(Token::Str(value)) => Ok(value),
            other => bad(format!(
                "{name}(...) needs a quoted string here, found {}",
                describe(other)
            )),
        }
    }

    /// Parses a range-vector argument: `metric[5m]` or `expr[5m:1m]`.
    fn range_argument(&mut self, name: &str) -> Result<Expr> {
        // Only a unary-level argument: `rate(a[5m]) * 2` must stop at the
        // closing parenthesis, not swallow the `* 2` into the argument (which
        // would then fail the range-vector check with a confusing message).
        let inner = self.expression(7)?;
        match inner {
            Expr::Range { .. } | Expr::Subquery { .. } => Ok(inner),
            other => bad(format!(
                "{name}(...) needs a range vector such as `metric[5m]`, got {}",
                render(&other)
            )),
        }
    }

    /// Parses the matcher list after `{`, with an optional metric name already
    /// consumed.
    fn matchers(&mut self, metric: Option<String>) -> Result<Selector> {
        let mut selector = Selector {
            metric: metric.unwrap_or_default(),
            matchers: Vec::new(),
        };
        loop {
            if self.eat(&Token::RBrace) {
                break;
            }
            if self.eat(&Token::Comma) {
                continue;
            }
            let Some(Token::Ident(name)) = self.next() else {
                return bad(format!(
                    "expected a label name but found {}",
                    describe(self.tokens.get(self.pos - 1).cloned())
                ));
            };
            let op = match self.next() {
                Some(Token::Op("=")) => MatcherType::Eq,
                Some(Token::Op("!=")) => MatcherType::Neq,
                Some(Token::Op("=~")) => MatcherType::Re,
                Some(Token::Op("!~")) => MatcherType::Nre,
                other => {
                    return bad(format!(
                        "expected =, !=, =~ or !~ after label {name:?} but found {}",
                        describe(other)
                    ))
                }
            };
            let value = match self.next() {
                Some(Token::Str(value)) | Some(Token::Ident(value)) => value,
                other => {
                    return bad(format!(
                        "expected a quoted label value after {name}{} but found {}",
                        op_name(op),
                        describe(other)
                    ))
                }
            };
            if name == "__name__" {
                if op != MatcherType::Eq {
                    return bad("only __name__=\"metric\" is supported, not a regex or negation");
                }
                if !selector.metric.is_empty() && selector.metric != value {
                    return bad(format!(
                        "metric name {:?} conflicts with __name__={value:?}",
                        selector.metric
                    ));
                }
                selector.metric = value;
            } else {
                selector.matchers.push((op, name, value));
            }
        }
        if selector.metric.is_empty() {
            return bad("a selector needs a metric name");
        }
        Ok(selector)
    }
}

fn op_name(op: MatcherType) -> &'static str {
    match op {
        MatcherType::Eq => "=",
        MatcherType::Neq => "!=",
        MatcherType::Re => "=~",
        MatcherType::Nre => "!~",
    }
}

fn describe(token: Option<Token>) -> String {
    match token {
        Some(token) => token.describe(),
        None => "end of query".to_string(),
    }
}

/// Best-effort rendering of an expression for error messages.
fn render(expr: &Expr) -> String {
    match expr {
        Expr::Number(value) => format_value(*value),
        Expr::Str(value) => format!("{value:?}"),
        Expr::Selector { selector, .. } => selector.metric.clone(),
        Expr::Range { selector, .. } => format!("{}[...]", selector.metric),
        Expr::Subquery { expr, .. } => format!("{}[...:...]", render(expr)),
        _ => "expression".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// One evaluated sample: its label set and value.
#[derive(Debug, Clone)]
pub struct Point {
    /// Labels of the series. `__name__` is present for a raw selector result
    /// and dropped as soon as an operation derives a new value.
    pub labels: LabelSet,
    pub value: f64,
    /// The sample's own timestamp, when it came from a stored point. Derived
    /// values inherit it from their input so `timestamp()` can report it.
    pub sample_ts: Option<i64>,
}

/// An instant vector.
#[derive(Debug, Clone, Default)]
pub struct Vector(pub Vec<Point>);

impl Vector {
    /// Appends a derived sample.
    ///
    /// NaN is kept, not dropped: `a / b` on a zero denominator, `sqrt(-1)` and
    /// `quantile_over_time(NaN, ...)` all produce a NaN sample in Prometheus,
    /// and the JSON layer already renders it as `"NaN"`. Dropping it would
    /// silently remove a series from the answer.
    fn push(&mut self, labels: LabelSet, value: f64) {
        self.0.push(Point {
            labels,
            value,
            sample_ts: None,
        });
    }

    fn push_sample(&mut self, labels: LabelSet, value: f64, sample_ts: i64) {
        self.0.push(Point {
            labels,
            value,
            sample_ts: Some(sample_ts),
        });
    }

    /// Appends a sample unchanged, labels and timestamp included (the set
    /// operators pass their input through).
    fn push_point(&mut self, point: &Point) {
        self.0.push(point.clone());
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// One series' samples inside a range, ascending by timestamp.
#[derive(Debug, Clone, Default)]
pub struct RangeSeries {
    pub labels: LabelSet,
    pub samples: Vec<SamplePoint>,
}

/// A range vector.
#[derive(Debug, Clone, Default)]
pub struct Matrix(pub Vec<RangeSeries>);

/// The evaluated value of an expression at one instant, or over a range.
#[derive(Debug, Clone, Default)]
pub struct Instant {
    pub vector: Vector,
    pub scalar: Option<f64>,
}

impl Instant {
    fn scalar(value: f64) -> Self {
        Instant {
            vector: Vector::default(),
            scalar: Some(value),
        }
    }
}

/// Samples fetched from the engine, keyed by a selector's identity.
///
/// Keyed by metric + matchers rather than by the `Selector` struct so two
/// spellings of the same series (`m{a="1"}` and `{__name__="m",a="1"}`) share
/// one fetch.
fn selector_key(selector: &Selector) -> String {
    let mut matchers: Vec<String> = selector
        .matchers
        .iter()
        .map(|(kind, name, value)| format!("{}{name}={value}", op_name(*kind)))
        .collect();
    matchers.sort();
    format!("{}{{{}}}", selector.metric, matchers.join(","))
}

/// Fetches every selector's raw samples once, over a window wide enough for
/// every `offset`, `@` and range in the expression.
async fn fetch(
    backend: &Backend,
    expr: &Expr,
    start_ms: i64,
    end_ms: i64,
) -> Result<HashMap<String, Vec<SeriesSamples>>> {
    let mut selectors = Vec::new();
    expr.selectors(&mut selectors);

    let (scan_start, scan_end) = expr.scan_window(start_ms, end_ms);

    let mut fetched: HashMap<String, Vec<SeriesSamples>> = HashMap::new();
    for selector in selectors {
        let key = selector_key(&selector);
        if fetched.contains_key(&key) {
            continue;
        }
        let request = SearchRequest {
            metric_name: selector.metric.as_bytes().to_vec(),
            matchers: selector
                .matchers
                .iter()
                .map(|(kind, name, value)| metric_engine::LabelMatcher {
                    matcher_type: *kind,
                    name: name.as_bytes().to_vec(),
                    value: value.as_bytes().to_vec(),
                })
                .collect(),
            start_ms: scan_start,
            end_ms: scan_end,
            limit: 0,
        };
        fetched.insert(key, backend.search(&request).await?);
    }
    Ok(fetched)
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// Evaluates an expression at a single instant.
struct Evaluator<'a> {
    fetched: &'a HashMap<String, Vec<SeriesSamples>>,
    /// The query's own time range, so `@ start()` / `@ end()` resolve.
    query_start_ms: i64,
    query_end_ms: i64,
    /// The range query's step, so `step()` can answer it; 0 for an instant
    /// query, matching Prometheus' folding of `step()` to 0 there.
    step_ms: i64,
}

impl<'a> Evaluator<'a> {
    fn eval(&self, expr: &Expr, at_ms: i64) -> Result<Instant> {
        match expr {
            Expr::Number(value) => Ok(Instant::scalar(*value)),
            Expr::Str(_) => {
                bad("a string literal is only valid as the label argument of count_values")
            }
            Expr::Time => Ok(Instant::scalar(at_ms as f64 / 1_000.0)),
            Expr::QueryContext(context) => {
                // `step()` is only defined for a range query; Prometheus'
                // `foldQueryContextFunctions` leaves it at 0 for an instant
                // query, which is what an equal start/end means here.
                let value = match context {
                    QueryContext::Start => self.query_start_ms,
                    QueryContext::End => self.query_end_ms,
                    QueryContext::Range => self.query_end_ms - self.query_start_ms,
                    QueryContext::Step => self.step_ms,
                };
                Ok(Instant::scalar(value as f64 / 1_000.0))
            }
            Expr::Vector(inner) => {
                let value = self.scalar_of(inner, at_ms)?;
                let mut vector = Vector::default();
                vector.push(BTreeMap::new(), value);
                Ok(Instant {
                    vector,
                    scalar: None,
                })
            }
            Expr::Selector {
                selector,
                offset_ms,
                at: pinned,
            } => {
                let at = self.resolve_at(*pinned, at_ms) - offset_ms;
                Ok(Instant {
                    vector: self.select_instant(selector, at),
                    scalar: None,
                })
            }
            Expr::Range { .. } => bad("a range vector is only valid as a function argument"),
            Expr::Subquery { .. } => bad("a subquery is only valid as a function argument"),
            Expr::Unary { negate, expr } => {
                if *negate {
                    let mut instant = self.eval(expr, at_ms)?;
                    if let Some(scalar) = instant.scalar.as_mut() {
                        *scalar = -*scalar;
                    }
                    for point in instant.vector.0.iter_mut() {
                        point.value = -point.value;
                    }
                    Ok(instant)
                } else {
                    self.eval(expr, at_ms)
                }
            }
            Expr::Binary {
                op,
                lhs,
                rhs,
                ret_bool,
                matching,
            } => self.binary(*op, lhs, rhs, *ret_bool, matching, at_ms),
            Expr::Aggregation {
                op,
                expr,
                param,
                grouping,
            } => self.aggregate(*op, expr, param.as_deref(), grouping.as_ref(), at_ms),
            Expr::RangeFunc {
                func,
                expr,
                param,
                param2,
            } => self.range_func(*func, expr, param.as_deref(), param2.as_deref(), at_ms),
            Expr::MathFunc { func, param, expr } => {
                self.math_func(*func, expr, param.as_deref(), at_ms)
            }
            Expr::Sort { dir, expr } => {
                let mut instant = self.eval(expr, at_ms)?;
                match dir {
                    SortDir::Asc => {
                        instant.vector.0.sort_by(|a, b| a.value.total_cmp(&b.value));
                    }
                    SortDir::Desc => {
                        instant.vector.0.sort_by(|a, b| b.value.total_cmp(&a.value));
                    }
                }
                Ok(instant)
            }
            Expr::SortByLabel { desc, labels, expr } => {
                let mut instant = self.eval(expr, at_ms)?;
                instant.vector.0.sort_by(|a, b| {
                    let order = sort_by_label_order(&a.labels, &b.labels, labels);
                    if *desc {
                        order.reverse()
                    } else {
                        order
                    }
                });
                Ok(instant)
            }
            Expr::Absent(expr) => {
                let instant = self.eval(expr, at_ms)?;
                let empty = instant.scalar.is_none() && instant.vector.is_empty();
                let mut vector = Vector::default();
                if empty {
                    vector.push(BTreeMap::new(), 1.0);
                }
                Ok(Instant {
                    vector,
                    scalar: None,
                })
            }
            Expr::Scalar(expr) => {
                let instant = self.eval(expr, at_ms)?;
                // Prometheus returns NaN unless exactly one float sample is
                // present; a scalar argument is already such a sample.
                let value = match (instant.scalar, instant.vector.0.as_slice()) {
                    (Some(value), _) => value,
                    (None, [point]) => point.value,
                    (None, _) => f64::NAN,
                };
                Ok(Instant::scalar(value))
            }
            Expr::Clamp { expr, min, max } => {
                let instant = self.eval(expr, at_ms)?;
                let min = self.scalar_of(min, at_ms)?;
                let max = self.scalar_of(max, at_ms)?;
                // `max < min` is an empty result, not the untouched input. A
                // NaN bound is not less than anything, so — as in Go — it
                // proceeds and turns every sample into NaN.
                let inverted = max < min;
                let mut vector = Vector::default();
                if !inverted {
                    let points = match instant.scalar {
                        Some(value) => {
                            let mut one = Vector::default();
                            one.push(BTreeMap::new(), value);
                            one
                        }
                        None => instant.vector,
                    };
                    for point in points.0 {
                        let value = go_max(min, go_min(point.value, max));
                        let mut labels = point.labels;
                        labels.remove("__name__");
                        vector.push(labels, value);
                    }
                }
                Ok(Instant {
                    vector,
                    scalar: None,
                })
            }
            Expr::HistogramFraction { lower, upper, expr } => {
                let instant = self.eval(expr, at_ms)?;
                let lower = self.scalar_of(lower, at_ms)?;
                let upper = self.scalar_of(upper, at_ms)?;
                if instant.scalar.is_some() {
                    return bad(
                        "histogram_fraction needs an instant vector of `le` buckets, not a scalar",
                    );
                }
                Ok(Instant {
                    vector: histogram_fraction(lower, upper, &instant.vector)?,
                    scalar: None,
                })
            }
            Expr::HistogramQuantiles {
                expr,
                label,
                quantiles,
            } => {
                let instant = self.eval(expr, at_ms)?;
                if instant.scalar.is_some() {
                    return bad(
                        "histogram_quantiles needs an instant vector of `le` buckets, \
                         not a scalar",
                    );
                }
                let mut values = Vec::with_capacity(quantiles.len());
                for quantile in quantiles {
                    values.push(self.scalar_of(quantile, at_ms)?);
                }
                Ok(Instant {
                    vector: histogram_quantiles(&values, label, &instant.vector)?,
                    scalar: None,
                })
            }
            Expr::HistogramStat { op, expr } => {
                let instant = self.eval(expr, at_ms)?;
                if instant.scalar.is_some() {
                    return bad(format!(
                        "{} needs an instant vector of `le` buckets, not a scalar",
                        op.name()
                    ));
                }
                Ok(Instant {
                    vector: histogram_stat(*op, &instant.vector)?,
                    scalar: None,
                })
            }
            Expr::Extremum { max, lhs, rhs } => {
                let lhs = self.scalar_of(lhs, at_ms)?;
                let rhs = self.scalar_of(rhs, at_ms)?;
                // Prometheus' `funcMaxOf` / `funcMinOf` match their respective
                // aggregators: NaN only survives when both sides are NaN.
                let value = if *max {
                    if lhs < rhs || lhs.is_nan() {
                        rhs
                    } else {
                        lhs
                    }
                } else if lhs > rhs || lhs.is_nan() {
                    rhs
                } else {
                    lhs
                };
                Ok(Instant::scalar(value))
            }
            Expr::DateFunc { func, expr } => {
                let mut vector = Vector::default();
                let Some(expr) = expr else {
                    // No argument: one label-less sample from the evaluation
                    // instant (Prometheus' `time.UnixMilli(enh.Ts)`).
                    vector.push(BTreeMap::new(), func.of(at_ms as f64 / 1_000.0));
                    return Ok(Instant {
                        vector,
                        scalar: None,
                    });
                };
                let instant = self.eval(expr, at_ms)?;
                // The argument's *value* is the timestamp; the sample's own
                // timestamp plays no part. Prometheus drops `__name__`.
                let points = match instant.scalar {
                    Some(value) => {
                        let mut one = Vector::default();
                        one.push(BTreeMap::new(), value);
                        one
                    }
                    None => instant.vector,
                };
                for point in points.0 {
                    let mut labels = point.labels;
                    labels.remove("__name__");
                    vector.push(labels, func.of(point.value));
                }
                Ok(Instant {
                    vector,
                    scalar: None,
                })
            }
            Expr::LabelReplace {
                expr,
                dst,
                replacement,
                src,
                pattern,
            } => {
                let instant = self.eval(expr, at_ms)?;
                if instant.scalar.is_some() {
                    return bad("label_replace expects an instant vector, got a scalar");
                }
                let mut vector = Vector::default();
                for mut point in instant.vector.0 {
                    let source = point.labels.get(src).map(String::as_str).unwrap_or("");
                    // Like Prometheus, a series whose source value does not
                    // match the pattern passes through unchanged, and an empty
                    // expansion deletes the destination label.
                    if let Some(captures) = pattern.0.captures(source) {
                        let value = expand_replacement(replacement, &captures);
                        if value.is_empty() {
                            point.labels.remove(dst);
                        } else {
                            point.labels.insert(dst.clone(), value);
                        }
                    }
                    vector.push_point(&point);
                }
                Ok(Instant {
                    vector,
                    scalar: None,
                })
            }
            Expr::LabelJoin {
                expr,
                dst,
                separator,
                srcs,
            } => {
                let instant = self.eval(expr, at_ms)?;
                if instant.scalar.is_some() {
                    return bad("label_join expects an instant vector, got a scalar");
                }
                let mut vector = Vector::default();
                for mut point in instant.vector.0 {
                    // Missing labels join as empty strings, so the separator
                    // still appears — `label_join(m, "x", "-", "a", "b")` on a
                    // series with only `a="1"` gives `x="1-"`.
                    let joined = srcs
                        .iter()
                        .map(|src| point.labels.get(src).map(String::as_str).unwrap_or(""))
                        .collect::<Vec<_>>()
                        .join(separator);
                    if joined.is_empty() {
                        point.labels.remove(dst);
                    } else {
                        point.labels.insert(dst.clone(), joined);
                    }
                    vector.push_point(&point);
                }
                Ok(Instant {
                    vector,
                    scalar: None,
                })
            }
            Expr::LabelOp { op, expr } => {
                let instant = self.eval(expr, at_ms)?;
                if instant.scalar.is_some() {
                    return bad(format!(
                        "{} expects an instant vector, got a scalar",
                        op.name()
                    ));
                }
                let mut vector = Vector::default();
                for mut point in instant.vector.0 {
                    match op {
                        // The pairs are applied in order, so a repeated label
                        // keeps the last value (VM writes them in sequence
                        // too).
                        LabelOp::Set(pairs) => {
                            for (dst, value) in pairs {
                                if value.is_empty() {
                                    point.labels.remove(dst);
                                } else {
                                    point.labels.insert(dst.clone(), value.clone());
                                }
                            }
                        }
                        LabelOp::Del(labels) => {
                            for label in labels {
                                point.labels.remove(label);
                            }
                        }
                        // `label_keep` names `__name__` like any other label:
                        // it survives only when listed (VM's `RemoveTagsOn`
                        // resets the metric group unless `__name__` is kept).
                        LabelOp::Keep(labels) => {
                            point.labels.retain(|name, _| labels.contains(name));
                        }
                        LabelOp::Copy(pairs) => label_copy(&mut point.labels, pairs, false),
                        LabelOp::Move(pairs) => label_copy(&mut point.labels, pairs, true),
                        LabelOp::Value(label) => {
                            // VM replaces the value with the label's text and
                            // drops the metric name; every other label stays.
                            point.labels.remove("__name__");
                            let parsed = point
                                .labels
                                .get(label)
                                .and_then(|value| value.parse::<f64>().ok())
                                .unwrap_or(f64::NAN);
                            // NaN values are gaps in MetricsQL and stay NaN.
                            if !point.value.is_nan() {
                                point.value = parsed;
                            }
                        }
                    }
                    vector.push_point(&point);
                }
                Ok(Instant {
                    vector,
                    scalar: None,
                })
            }
        }
    }

    fn resolve_at(&self, pinned: Option<AtModifier>, at_ms: i64) -> i64 {
        match pinned {
            Some(modifier) => modifier.resolve(self.query_start_ms, self.query_end_ms),
            None => at_ms,
        }
    }

    /// A scalar expression, or the sole value of a one-series vector.
    fn scalar_of(&self, expr: &Expr, at_ms: i64) -> Result<f64> {
        let instant = self.eval(expr, at_ms)?;
        if let Some(scalar) = instant.scalar {
            return Ok(scalar);
        }
        match instant.vector.0.as_slice() {
            [point] => Ok(point.value),
            [] => Ok(f64::NAN),
            many => bad(format!(
                "expected a scalar but the expression produced {} series",
                many.len()
            )),
        }
    }

    /// The newest sample at or before `at`, within the lookback window.
    fn select_instant(&self, selector: &Selector, at: i64) -> Vector {
        let mut vector = Vector::default();
        let Some(series) = self.fetched.get(&selector_key(selector)) else {
            return vector;
        };
        for series in series {
            let Some(sample) = series
                .samples
                .iter()
                .rev()
                .find(|sample| sample.timestamp <= at)
            else {
                continue;
            };
            if at - sample.timestamp > LOOKBACK_MS {
                continue;
            }
            let mut labels = labels_of(series);
            // Prometheus drops `__name__` from derived labels but keeps it on a
            // plain selector result, which is what a dashboard shows.
            labels
                .entry("__name__".to_string())
                .or_insert_with(|| String::from_utf8_lossy(&series.metric_name).into_owned());
            vector.push_sample(labels, sample.value, sample.timestamp);
        }
        vector
    }

    /// The samples of every matched series inside `(at - range, at]`.
    fn select_range(&self, selector: &Selector, at: i64, range_ms: i64) -> Matrix {
        let mut matrix = Matrix::default();
        let Some(series) = self.fetched.get(&selector_key(selector)) else {
            return matrix;
        };
        let start = at - range_ms;
        for series in series {
            let samples: Vec<SamplePoint> = series
                .samples
                .iter()
                .filter(|sample| sample.timestamp > start && sample.timestamp <= at)
                .cloned()
                .collect();
            if samples.is_empty() {
                continue;
            }
            // `__name__` travels with the labels so a range function can decide
            // whether to keep it (`first_over_time` / `last_over_time` do,
            // because they act like `offset`). Every other function drops it,
            // so the choice is made there rather than lost here.
            let mut labels = labels_of(series);
            labels
                .entry("__name__".to_string())
                .or_insert_with(|| String::from_utf8_lossy(&series.metric_name).into_owned());
            matrix.0.push(RangeSeries { labels, samples });
        }
        matrix
    }

    /// The samples of a range-vector argument: a raw range selector
    /// (`metric[5m]`) or a subquery (`expr[5m:1m]`).
    fn matrix_of(&self, arg: &Expr, at_ms: i64) -> Result<Matrix> {
        match arg {
            Expr::Range {
                selector,
                range_ms,
                offset_ms,
                at: pinned,
            } => {
                let at = self.resolve_at(*pinned, at_ms) - offset_ms;
                Ok(self.select_range(selector, at, *range_ms))
            }
            Expr::Subquery {
                expr,
                range_ms,
                step_ms,
                offset_ms,
                at: pinned,
            } => self.subquery_matrix(expr, *range_ms, *step_ms, *offset_ms, *pinned, at_ms),
            other => bad(format!(
                "expected a range vector such as `metric[5m]`, got {}",
                render(other)
            )),
        }
    }

    /// Evaluates a subquery `expr[range:step]` on its own step grid and folds
    /// the per-step vectors into one matrix, following Prometheus'
    /// `subqueryEvaluationTimes`: the grid is anchored at absolute multiples of
    /// the step, starts strictly after `end - range`, and runs up to the
    /// evaluation instant (or up to the `@` instant when one is pinned).
    ///
    /// Each grid point is an ordinary instant evaluation, so the inner
    /// selectors use the same lookback as anywhere else. The matrix samples
    /// carry the *grid* timestamp, not the timestamp of the sample they were
    /// derived from, which is what makes `rate()` over a subquery use the
    /// requested step.
    fn subquery_matrix(
        &self,
        expr: &Expr,
        range_ms: i64,
        step_ms: i64,
        offset_ms: i64,
        pinned: Option<AtModifier>,
        at_ms: i64,
    ) -> Result<Matrix> {
        let step = subquery_step(step_ms);
        let end = self.resolve_at(pinned, at_ms) - offset_ms;
        let from = end - range_ms;
        // Rust and Go both truncate integer division toward zero, so this is
        // the same grid Prometheus computes.
        let mut ts = step * (from / step);
        if ts <= from {
            ts += step;
        }
        let mut rows: BTreeMap<LabelSet, Vec<SamplePoint>> = BTreeMap::new();
        while ts <= end {
            let instant = self.eval(expr, ts)?;
            if instant.scalar.is_some() {
                return bad("subquery is only allowed on instant vector, got scalar instead");
            }
            for point in instant.vector.0 {
                rows.entry(point.labels).or_default().push(SamplePoint {
                    timestamp: ts,
                    value: point.value,
                });
            }
            ts += step;
        }
        Ok(Matrix(
            rows.into_iter()
                .map(|(labels, samples)| RangeSeries { labels, samples })
                .collect(),
        ))
    }

    fn binary(
        &self,
        op: BinaryOp,
        lhs: &Expr,
        rhs: &Expr,
        ret_bool: bool,
        matching: &VectorMatching,
        at_ms: i64,
    ) -> Result<Instant> {
        let left = self.eval(lhs, at_ms)?;
        let right = self.eval(rhs, at_ms)?;
        // Prometheus only honours vector matching between two instant vectors;
        // with a scalar on either side the modifiers are meaningless, and
        // silently ignoring them would answer a different query.
        let both_vectors = left.scalar.is_none() && right.scalar.is_none();
        if !both_vectors {
            if !matching.matching_labels.is_empty() {
                return bad("vector matching only allowed between instant vectors");
            }
            if !matching.include.is_empty() || matching.card != Cardinality::OneToOne {
                return bad("grouping labels only allowed between instant vectors");
            }
            if op.is_set() {
                return bad(format!(
                    "set operator `{}` not allowed in binary scalar expression",
                    op.name()
                ));
            }
        }
        match (left.scalar, right.scalar) {
            (Some(a), Some(b)) => {
                let value = op.apply(a, b);
                if op.is_comparison() && !ret_bool {
                    let value = if value == 1.0 { a } else { f64::NAN };
                    return Ok(Instant::scalar(value));
                }
                Ok(Instant::scalar(value))
            }
            (Some(a), None) => Ok(Instant {
                vector: self.binary_with_scalar(op, &right.vector, a, false, ret_bool),
                scalar: None,
            }),
            (None, Some(b)) => Ok(Instant {
                vector: self.binary_with_scalar(op, &left.vector, b, true, ret_bool),
                scalar: None,
            }),
            (None, None) => Ok(Instant {
                vector: binary_vectors(op, &left.vector, &right.vector, ret_bool, matching)?,
                scalar: None,
            }),
        }
    }

    /// Applies a scalar to every sample of a vector. `scalar_on_left` keeps
    /// the operand order (`5 - v` is not `v - 5`).
    fn binary_with_scalar(
        &self,
        op: BinaryOp,
        vector: &Vector,
        scalar: f64,
        scalar_on_left: bool,
        ret_bool: bool,
    ) -> Vector {
        let mut out = Vector::default();
        for point in &vector.0 {
            let (a, b) = if scalar_on_left {
                (scalar, point.value)
            } else {
                (point.value, scalar)
            };
            let value = op.apply(a, b);
            if op.is_comparison() && !ret_bool {
                if value == 1.0 {
                    out.push(point.labels.clone(), point.value);
                }
                continue;
            }
            let mut labels = point.labels.clone();
            // Arithmetic results are no longer the raw metric.
            if !op.is_comparison() {
                labels.remove("__name__");
            }
            out.push(labels, value);
        }
        out
    }

    fn aggregate(
        &self,
        op: AggOp,
        expr: &Expr,
        param: Option<&Expr>,
        grouping: Option<&Grouping>,
        at_ms: i64,
    ) -> Result<Instant> {
        let instant = self.eval(expr, at_ms)?;
        let values = if let Some(scalar) = instant.scalar {
            // Prometheus treats a scalar as a single unlabelled sample.
            let mut vector = Vector::default();
            vector.push(BTreeMap::new(), scalar);
            vector
        } else {
            instant.vector
        };

        if op == AggOp::CountValues {
            let Some(Expr::Str(label)) = param else {
                return bad("count_values needs a quoted label name");
            };
            return Ok(Instant {
                vector: count_values(&values, label, grouping),
                scalar: None,
            });
        }
        if op.takes_param() {
            let param = match param {
                Some(param) => self.scalar_of(param, at_ms)?,
                None => return bad(format!("{} needs a parameter", op.name())),
            };
            if op == AggOp::Quantile {
                return Ok(Instant {
                    vector: quantile_agg(&values, param, grouping),
                    scalar: None,
                });
            }
            return Ok(Instant {
                vector: topk(op, &values, param as usize, grouping),
                scalar: None,
            });
        }
        if param.is_some() {
            return bad(format!("{} does not take a parameter", op.name()));
        }

        let mut groups: BTreeMap<Vec<(String, String)>, Vec<f64>> = BTreeMap::new();
        for point in &values.0 {
            groups
                .entry(group_key(&point.labels, grouping))
                .or_default()
                .push(point.value);
        }
        let mut vector = Vector::default();
        for (key, group) in groups {
            let value = aggregate_values(op, &group);
            let labels: BTreeMap<String, String> = key.into_iter().collect();
            vector.push(labels, value);
        }
        Ok(Instant {
            vector,
            scalar: None,
        })
    }

    fn range_func(
        &self,
        func: RangeFunc,
        expr: &Expr,
        param: Option<&Expr>,
        param2: Option<&Expr>,
        at_ms: i64,
    ) -> Result<Instant> {
        let matrix = self.matrix_of(expr, at_ms)?;
        // The range of the range vector: `rate(a[5m])` and `rate(a[5m:1m])`
        // both use 5m for their extrapolation and for `count`-style averages.
        let range_ms = match expr {
            Expr::Range { range_ms, .. } | Expr::Subquery { range_ms, .. } => *range_ms,
            _ => 0,
        };
        let param = match param {
            Some(param) => Some(self.scalar_of(param, at_ms)?),
            None => None,
        };
        let param2 = match param2 {
            Some(param2) => Some(self.scalar_of(param2, at_ms)?),
            None => None,
        };
        if func == RangeFunc::QuantileOverTime && param.is_none() {
            return bad("quantile_over_time needs a quantile parameter");
        }
        if let RangeFunc::AggrOverTime(op) = func {
            if matches!(
                op,
                AggOp::Topk | AggOp::Bottomk | AggOp::Group | AggOp::Quantile | AggOp::CountValues
            ) {
                return bad(format!(
                    "`{}` is not an aggregation over_time accepts",
                    op.name()
                ));
            }
        }
        if func == RangeFunc::PredictLinear && param.is_none() {
            return bad("predict_linear needs a horizon in seconds");
        }
        if func == RangeFunc::DoubleExpSmoothing {
            // Prometheus panics on these; a 400 is the honest equivalent. The
            // bounds are written as two comparisons so a NaN factor passes
            // through (as it does in Go, where `NaN <= 0` and `NaN >= 1` are
            // both false) and simply yields NaN results.
            let (Some(sf), Some(tf)) = (param, param2) else {
                return bad(
                    "double_exponential_smoothing needs a smoothing factor and a trend factor",
                );
            };
            if sf <= 0.0 || sf >= 1.0 {
                return bad(format!(
                    "invalid smoothing factor: expected 0 < sf < 1, got {}",
                    format_float_g(sf)
                ));
            }
            if tf <= 0.0 || tf >= 1.0 {
                return bad(format!(
                    "invalid trend factor: expected 0 < tf < 1, got {}",
                    format_float_g(tf)
                ));
            }
        }
        let mut vector = Vector::default();
        // `absent_over_time` is the one range function whose answer is "1" when
        // nothing matched: Prometheus returns a single label-less 1 for a
        // selector with no samples in the range, and nothing otherwise.
        if func == RangeFunc::AbsentOverTime {
            if matrix.0.is_empty() {
                vector.push(BTreeMap::new(), 1.0);
            }
            return Ok(Instant {
                vector,
                scalar: None,
            });
        }
        for series in &matrix.0 {
            if let Some(value) = apply_range_func(func, &series.samples, range_ms, param, param2) {
                let mut labels = series.labels.clone();
                // `first_over_time` and `last_over_time` act like `offset`, so
                // Prometheus keeps the metric name for them; every other range
                // function describes a new quantity and drops it.
                if !matches!(func, RangeFunc::FirstOverTime | RangeFunc::LastOverTime) {
                    labels.remove("__name__");
                }
                vector.push(labels, value);
            }
        }
        Ok(Instant {
            vector,
            scalar: None,
        })
    }

    fn math_func(
        &self,
        func: MathFunc,
        expr: &Expr,
        param: Option<&Expr>,
        at_ms: i64,
    ) -> Result<Instant> {
        let instant = self.eval(expr, at_ms)?;
        let param = match param {
            Some(param) => Some(self.scalar_of(param, at_ms)?),
            None => None,
        };
        match func {
            MathFunc::Default => {
                let Some(fallback) = param else {
                    return bad("default needs a fallback value");
                };
                let mut vector = Vector::default();
                if let Some(scalar) = instant.scalar {
                    vector.push(BTreeMap::new(), scalar);
                } else {
                    for point in &instant.vector.0 {
                        // Prometheus treats NaN as missing here too, so an
                        // absent or NaN sample is replaced by the fallback.
                        let value = if point.value.is_nan() {
                            fallback
                        } else {
                            point.value
                        };
                        vector.push(point.labels.clone(), value);
                    }
                }
                // An empty result (the common case) gets one fallback sample,
                // with no labels, so `or vector(...)`-style defaults are not
                // needed.
                if vector.is_empty() {
                    vector.push(BTreeMap::new(), fallback);
                }
                return Ok(Instant {
                    vector,
                    scalar: None,
                });
            }
            // `histogram_quantile(q, v)`: group the classic histogram's `le`
            // buckets per histogram, then interpolate within the bucket that
            // holds the requested rank (Prometheus' `BucketQuantile`).
            MathFunc::HistogramQuantile => {
                let Some(quantile) = param else {
                    return bad("histogram_quantile needs a quantile");
                };
                if instant.scalar.is_some() {
                    return bad(
                        "histogram_quantile needs an instant vector of `le` buckets, not a scalar",
                    );
                }
                return Ok(Instant {
                    vector: histogram_quantile(quantile, &instant.vector)?,
                    scalar: None,
                });
            }
            MathFunc::ClampMin | MathFunc::ClampMax => {
                let Some(bound) = param else {
                    return bad("clamp_min/clamp_max need a bound");
                };
                let mut out = Vector::default();
                for point in &instant.vector.0 {
                    let value = match func {
                        MathFunc::ClampMin => go_max(point.value, bound),
                        _ => go_min(point.value, bound),
                    };
                    let mut labels = point.labels.clone();
                    labels.remove("__name__");
                    out.push(labels, value);
                }
                return Ok(Instant {
                    vector: out,
                    scalar: None,
                });
            }
            _ => {}
        }
        let to_nearest = param.unwrap_or(1.0);
        let mut vector = Vector::default();
        if let Some(scalar) = instant.scalar {
            let value = if func == MathFunc::Timestamp {
                at_ms as f64 / 1_000.0
            } else {
                apply_math(func, scalar, to_nearest, at_ms)
            };
            vector.push(BTreeMap::new(), value);
            return Ok(Instant {
                vector,
                scalar: None,
            });
        }
        for point in &instant.vector.0 {
            let value = match func {
                MathFunc::Timestamp => point.sample_ts.unwrap_or(at_ms) as f64 / 1_000.0,
                other => apply_math(other, point.value, to_nearest, at_ms),
            };
            let mut labels = point.labels.clone();
            labels.remove("__name__");
            vector.push(labels, value);
        }
        Ok(Instant {
            vector,
            scalar: None,
        })
    }
}

/// `label_copy` / `label_move`: a missing or empty source leaves the
/// destination untouched (VM skips instead of deleting), and the pairs run in
/// order, so a later pair sees an earlier pair's result.
fn label_copy(labels: &mut LabelSet, pairs: &[(String, String)], remove_source: bool) {
    for (src, dst) in pairs {
        let Some(value) = labels.get(src).filter(|value| !value.is_empty()) else {
            continue;
        };
        let value = value.clone();
        labels.insert(dst.clone(), value);
        // `label_move(q, "a", "a")` must not delete the label it just wrote.
        if remove_source && src != dst {
            labels.remove(src);
        }
    }
}

/// Labels of a fetched series, without `__name__`.
fn labels_of(series: &SeriesSamples) -> BTreeMap<String, String> {
    series
        .labels
        .iter()
        .map(|label| {
            (
                String::from_utf8_lossy(&label.name).into_owned(),
                String::from_utf8_lossy(&label.value).into_owned(),
            )
        })
        .collect()
}

fn group_key(
    labels: &BTreeMap<String, String>,
    grouping: Option<&Grouping>,
) -> Vec<(String, String)> {
    match grouping {
        None => Vec::new(),
        Some(Grouping::By(names)) => names
            .iter()
            .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
            .collect(),
        Some(Grouping::Without(names)) => {
            let drop: HashSet<&String> = names.iter().collect();
            labels
                .iter()
                .filter(|(name, _)| name.as_str() != "__name__" && !drop.contains(*name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect()
        }
    }
}

fn aggregate_values(op: AggOp, values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    match op {
        AggOp::Sum => values.iter().sum(),
        AggOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
        AggOp::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
        AggOp::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        AggOp::Count => values.len() as f64,
        AggOp::Group => 1.0,
        AggOp::Stddev => aggregate_values(AggOp::Stdvar, values).sqrt(),
        AggOp::Stdvar => {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64
        }
        // Handled by the caller, which needs the whole vector.
        AggOp::Topk | AggOp::Bottomk | AggOp::Quantile | AggOp::CountValues => f64::NAN,
    }
}

fn topk(op: AggOp, values: &Vector, k: usize, grouping: Option<&Grouping>) -> Vector {
    if k == 0 {
        return Vector::default();
    }
    let mut groups: BTreeMap<Vec<(String, String)>, Vec<&Point>> = BTreeMap::new();
    for point in &values.0 {
        groups
            .entry(group_key(&point.labels, grouping))
            .or_default()
            .push(point);
    }
    let mut out = Vector::default();
    for (_, mut group) in groups {
        match op {
            AggOp::Bottomk => group.sort_by(|a, b| a.value.total_cmp(&b.value)),
            _ => group.sort_by(|a, b| b.value.total_cmp(&a.value)),
        }
        for point in group.into_iter().take(k) {
            out.push(point.labels.clone(), point.value);
        }
    }
    out
}

/// `quantile(q, v)`, grouped like the other aggregations.
///
/// Each group's values are sorted and interpolated by `quantile_of`, the same
/// helper `quantile_over_time` uses.
fn quantile_agg(values: &Vector, quantile: f64, grouping: Option<&Grouping>) -> Vector {
    let mut groups: BTreeMap<Vec<(String, String)>, Vec<f64>> = BTreeMap::new();
    for point in &values.0 {
        groups
            .entry(group_key(&point.labels, grouping))
            .or_default()
            .push(point.value);
    }
    let mut out = Vector::default();
    for (key, mut group) in groups {
        group.sort_by(|a, b| a.total_cmp(b));
        out.push(key.into_iter().collect(), quantile_of(&group, quantile));
    }
    out
}

/// `count_values("label", v)` — one output series per distinct sample value.
///
/// Following Prometheus' `aggregationCountValues`: the value is rendered with
/// Go's `strconv.FormatFloat(f, 'f', -1, 64)` and written into `label`, and the
/// grouping key is then computed with that label included:
///
/// - no `by`/`without` → group by the value label alone (Prometheus appends it
///   to the empty grouping list), so the result is one series per value with
///   only that label;
/// - `by (...)` → the value label is appended to the `by` list;
/// - `without (...)` → the value label is already part of the metric, so it
///   survives unless it was named in the `without` list.
fn count_values(values: &Vector, label: &str, grouping: Option<&Grouping>) -> Vector {
    let grouped = match grouping {
        Some(Grouping::Without(names)) => Grouping::Without(names.clone()),
        Some(Grouping::By(names)) => {
            let mut names = names.clone();
            names.push(label.to_string());
            Grouping::By(names)
        }
        None => Grouping::By(vec![label.to_string()]),
    };
    let mut counts: BTreeMap<Vec<(String, String)>, usize> = BTreeMap::new();
    for point in &values.0 {
        let mut labels = point.labels.clone();
        labels.insert(label.to_string(), format_float_g(point.value));
        counts
            .entry(group_key(&labels, Some(&grouped)))
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
    let mut out = Vector::default();
    for (key, count) in counts {
        out.push(key.into_iter().collect(), count as f64);
    }
    out
}

/// Go's `strconv.FormatFloat(v, 'f', -1, 64)`: fixed-point notation, the
/// shortest representation that round-trips, and `NaN` / `+Inf` / `-Inf`.
fn format_float_g(value: f64) -> String {
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
    format!("{value}")
}

/// `labels.FormatOpenMetricsFloat`: Go's shortest `%g` formatting, with `".0"`
/// appended when the result would otherwise contain neither a `'.'` nor an
/// `'e'`.
///
/// Used for the quantile label of `histogram_quantiles`, where Prometheus
/// writes the quantile through this exact function, so `0.5` becomes `"0.5"`
/// and `1` becomes `"1.0"`.
fn format_open_metrics_float(value: f64) -> String {
    // Go compares before looking at the sign, so `-0` is `"0.0"` too.
    if value == 1.0 {
        return "1.0".to_string();
    }
    if value == 0.0 {
        return "0.0".to_string();
    }
    if value == -1.0 {
        return "-1.0".to_string();
    }
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
    let mut out = go_format_g(value);
    // Go looks for `e` and `.` only — never `E` — because `'g'` is lowercase.
    if !out.contains(['e', '.']) {
        out.push_str(".0");
    }
    out
}

/// Go's `strconv.FormatFloat(value, 'g', -1, 64)`: the shortest decimal that
/// round-trips, written in fixed-point notation while the decimal exponent
/// stays in `[-4, 6)` and in `d.dddde±dd` form outside it.
///
/// Rust's `{:e}` gives the same shortest digits and the same exponent as Go's
/// `%g` does, so the formatting is only a matter of putting the point where Go
/// puts it.
fn go_format_g(value: f64) -> String {
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` always writes an exponent");
    let exponent: i32 = exponent.parse().expect("`{:e}` writes an integer");
    let negative = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    if !(-4..6).contains(&exponent) {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exponent < 0 { '-' } else { '+' });
        let magnitude = exponent.unsigned_abs();
        if magnitude < 10 {
            out.push('0');
        }
        out.push_str(&magnitude.to_string());
        return out;
    }
    // `point` is how many digits sit before the decimal point; it can be zero
    // or negative, which is what the two padding branches handle.
    let point = exponent + 1;
    if point <= 0 {
        out.push_str("0.");
        out.push_str(&"0".repeat((-point) as usize));
        out.push_str(&digits);
    } else if point as usize >= digits.len() {
        out.push_str(&digits);
        out.push_str(&"0".repeat(point as usize - digits.len()));
    } else {
        let (whole, fraction) = digits.split_at(point as usize);
        out.push_str(whole);
        out.push('.');
        out.push_str(fraction);
    }
    out
}

fn apply_range_func(
    func: RangeFunc,
    samples: &[SamplePoint],
    range_ms: i64,
    param: Option<f64>,
    param2: Option<f64>,
) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    // MetricsQL treats NaN as a gap: VM runs `removeNanValues` over the input
    // series before *every* rollup function, so none of the `vm_*` arms below
    // ever sees one. Shadow `samples` with the filtered slice so each arm can
    // stay a literal transcription of the Go code.
    let vm_samples: Option<Vec<SamplePoint>> = if func.is_metricsql_rollup() {
        Some(
            samples
                .iter()
                .filter(|sample| !sample.value.is_nan())
                .cloned()
                .collect(),
        )
    } else {
        None
    };
    let samples: &[SamplePoint] = match &vm_samples {
        Some(filtered) => {
            if filtered.is_empty() {
                return None;
            }
            filtered
        }
        None => samples,
    };
    let first = samples.first()?;
    let last = samples.last()?;
    match func {
        RangeFunc::Rate | RangeFunc::Increase => {
            // Counter slope: a decrease means the counter reset, which
            // Prometheus treats as a drop to zero before continuing.
            let mut delta = 0.0;
            let mut previous = first.value;
            for sample in &samples[1..] {
                if sample.value >= previous {
                    delta += sample.value - previous;
                } else {
                    delta += sample.value;
                }
                previous = sample.value;
            }
            if func == RangeFunc::Increase {
                return Some(delta);
            }
            let span_ms = last.timestamp - first.timestamp;
            if span_ms <= 0 {
                return None;
            }
            Some(delta * 1_000.0 / span_ms as f64)
        }
        RangeFunc::Irate => {
            if samples.len() < 2 {
                return None;
            }
            let previous = &samples[samples.len() - 2];
            let delta = if last.value >= previous.value {
                last.value - previous.value
            } else {
                last.value
            };
            let span_ms = last.timestamp - previous.timestamp;
            if span_ms <= 0 {
                return None;
            }
            Some(delta * 1_000.0 / span_ms as f64)
        }
        RangeFunc::Delta => Some(last.value - first.value),
        RangeFunc::Idelta => {
            if samples.len() < 2 {
                return None;
            }
            Some(last.value - samples[samples.len() - 2].value)
        }
        RangeFunc::Changes => Some(
            samples
                .windows(2)
                .filter(|pair| pair[0].value != pair[1].value)
                .count() as f64,
        ),
        RangeFunc::Resets => Some(
            samples
                .windows(2)
                .filter(|pair| pair[1].value < pair[0].value)
                .count() as f64,
        ),
        RangeFunc::CountOverTime => Some(samples.len() as f64),
        RangeFunc::SumOverTime => Some(samples.iter().map(|s| s.value).sum()),
        RangeFunc::AvgOverTime => {
            Some(samples.iter().map(|s| s.value).sum::<f64>() / samples.len() as f64)
        }
        RangeFunc::MinOverTime => Some(
            samples
                .iter()
                .map(|s| s.value)
                .fold(f64::INFINITY, f64::min),
        ),
        RangeFunc::MaxOverTime => Some(
            samples
                .iter()
                .map(|s| s.value)
                .fold(f64::NEG_INFINITY, f64::max),
        ),
        RangeFunc::LastOverTime => Some(last.value),
        RangeFunc::StdvarOverTime => {
            let mean = samples.iter().map(|s| s.value).sum::<f64>() / samples.len() as f64;
            Some(
                samples
                    .iter()
                    .map(|s| (s.value - mean).powi(2))
                    .sum::<f64>()
                    / samples.len() as f64,
            )
        }
        RangeFunc::StddevOverTime => {
            let variance =
                apply_range_func(RangeFunc::StdvarOverTime, samples, range_ms, None, None)?;
            Some(variance.sqrt())
        }
        RangeFunc::QuantileOverTime => {
            let quantile = param?;
            let mut values: Vec<f64> = samples.iter().map(|s| s.value).collect();
            values.sort_by(|a, b| a.total_cmp(b));
            Some(quantile_of(&values, quantile))
        }
        // `absent_over_time` is answered by `range_func` from the matrix being
        // empty or not, so it never needs a per-series value.
        RangeFunc::AbsentOverTime => None,
        // MetricsQL: aggregate over time with a named aggregation.
        RangeFunc::AggrOverTime(op) => Some(aggregate_values(op, &values(samples))),
        // The timestamp of the extreme sample, in seconds (Prometheus reports
        // `timestamp()`-shaped numbers, not samples).
        RangeFunc::TMinOverTime => samples
            .iter()
            .min_by(|a, b| a.value.total_cmp(&b.value))
            .map(|s| s.timestamp as f64 / 1_000.0),
        RangeFunc::TMaxOverTime => samples
            .iter()
            .max_by(|a, b| a.value.total_cmp(&b.value))
            .map(|s| s.timestamp as f64 / 1_000.0),
        // Prometheus' newest additions, all read off the same ordered sample
        // slice. `first_over_time` is the oldest value; the `ts_of_*` forms
        // report seconds, like `timestamp()`.
        RangeFunc::FirstOverTime => samples.first().map(|s| s.value),
        RangeFunc::TsOfFirstOverTime => samples.first().map(|s| s.timestamp as f64 / 1_000.0),
        RangeFunc::TsOfLastOverTime => samples.last().map(|s| s.timestamp as f64 / 1_000.0),
        // These two use `>=` / `<=`, so the *last* of the equally extreme
        // samples wins — unlike `max_over_time` / `min_over_time`, which keep
        // the first. The `|| best.is_nan()` half only matters when the running
        // extreme is already NaN: Go's `NaN >= x` is false, so a NaN sample
        // never wins by itself, but the next value after one does.
        RangeFunc::TsOfMaxOverTime => {
            timestamp_of_extreme(samples, |current, best| current >= best || best.is_nan())
        }
        RangeFunc::TsOfMinOverTime => {
            timestamp_of_extreme(samples, |current, best| current <= best || best.is_nan())
        }
        // A series only reaches this arm when it has at least one sample, so
        // the answer is always 1 (Prometheus' `funcPresentOverTime`).
        RangeFunc::PresentOverTime => Some(1.0),
        // Median absolute deviation: median(|v - median(v)|). Prometheus
        // propagates NaN rather than dropping those samples, because a NaN
        // makes both medians undefined.
        RangeFunc::MadOverTime => {
            let mut values = values(samples);
            if values.iter().any(|value| value.is_nan()) {
                return Some(f64::NAN);
            }
            values.sort_by(|a, b| a.total_cmp(b));
            let median = quantile_of(&values, 0.5);
            let mut deviations: Vec<f64> =
                values.iter().map(|value| (value - median).abs()).collect();
            deviations.sort_by(|a, b| a.total_cmp(b));
            Some(quantile_of(&deviations, 0.5))
        }
        // Linear extrapolation over the range, evaluated `horizon` seconds past
        // the last sample (least squares, as Prometheus does).
        RangeFunc::PredictLinear => {
            let horizon = param?;
            let origin = samples[0].timestamp;
            let (slope, intercept) = linear_regression(samples, origin);
            // `intercept` is the fit's value at `origin`, so the last sample is
            // `(last - origin)` seconds along the line.
            let last = (samples.last()?.timestamp - origin) as f64 / 1_000.0;
            Some(intercept + slope * (last + horizon))
        }
        // `deriv` is the slope alone; ≤1 sample has no slope at all.
        RangeFunc::Deriv => {
            if samples.len() < 2 {
                return None;
            }
            let (slope, _) = linear_regression(samples, samples[0].timestamp);
            Some(slope)
        }
        // Holt's linear ("double exponential") smoothing, transcribed from
        // // Prometheus' `funcDoubleExponentialSmoothing`: seed the level with
        // the first sample and the trend with the first difference, then walk
        // the series forward. `calcTrendValue` keeps `b` unchanged on the very
        // first step (its `i == 0` case), so a two-sample series reproduces the
        // straight line through them. Fewer than two float samples has no
        // smoothing to do and the series is dropped.
        RangeFunc::DoubleExpSmoothing => {
            let (sf, tf) = (param?, param2?);
            if samples.len() < 2 {
                return None;
            }
            let mut s0 = 0.0;
            let mut s1 = samples[0].value;
            let mut b = samples[1].value - samples[0].value;
            for (i, sample) in samples.iter().enumerate().skip(1) {
                let x = sf * sample.value;
                b = calc_trend_value(i - 1, tf, s0, s1, b);
                let y = (1.0 - sf) * (s1 + b);
                s0 = s1;
                s1 = x + y;
            }
            Some(s1)
        }
        // The integral over time, in seconds, following Prometheus'
        // `funcIntegral`: three quadrature strategies, NaN treated as zero, and
        // a Kahan-compensated sum. The parameter is the strategy (0 = left
        // point, 1 = right point, 2 = trapezoid, the default).
        RangeFunc::Integral => {
            let strategy = match param {
                // An out-of-range or non-integral strategy is a warning in
                // Prometheus, not an error, and it falls back to the default.
                Some(arg)
                    if arg.is_finite() && arg.trunc() == arg && (0.0..=2.0).contains(&arg) =>
                {
                    arg as i64
                }
                _ => 2,
            };
            let nan_as_zero = |value: f64| if value.is_nan() { 0.0 } else { value };
            let mut sum = 0.0;
            let mut c = 0.0;
            let mut previous = samples[0].clone();
            for (i, sample) in samples.iter().enumerate() {
                let current = nan_as_zero(sample.value);
                let before = nan_as_zero(previous.value);
                let (value, c_value) = match strategy {
                    // Left-point rectangle: the interval takes the older
                    // sample's value.
                    0 => (before, 0.0),
                    // Right-point rectangle: the newer one's.
                    1 => (current, 0.0),
                    // Trapezoid: the mean of the two, with a halving *after*
                    // the sum so finite samples cannot overflow the average.
                    _ => {
                        if before == 0.0 && current == 0.0 {
                            (0.0, 0.0)
                        } else {
                            let (value, c_value) = kahan_inc(current, before, 0.0);
                            if value.is_infinite()
                                && !current.is_infinite()
                                && !before.is_infinite()
                            {
                                kahan_inc(current / 2.0, before / 2.0, 0.0)
                            } else {
                                (value / 2.0, c_value / 2.0)
                            }
                        }
                    }
                };
                // The first sample only seeds `previous`; every later one
                // contributes over the interval behind it.
                if i > 0 && (value != 0.0 || c_value != 0.0) {
                    let delta_t = (sample.timestamp - previous.timestamp) as f64 / 1_000.0;
                    let (new_sum, new_c) = kahan_inc(value * delta_t, sum, c + c_value * delta_t);
                    sum = new_sum;
                    c = new_c;
                }
                previous = sample.clone();
            }
            if sum.is_infinite() {
                Some(sum)
            } else {
                Some(sum + c)
            }
        }
        // ---- MetricsQL rollups (VictoriaMetrics' `rollup.go`)
        // ---------------- NaNs are already gone (`is_metricsql_rollup`
        // filtered them), and VM never feeds the sample preceding the window
        // into these, so `prevValue` is always NaN here — the case VM sees at
        // the very start of a series.
        RangeFunc::RangeOverTime => {
            // `rollupMax` - `rollupMin`: seed with the first value, then
            // compare.
            let max = vm_extreme(samples, |current, best| current > best)?;
            let min = vm_extreme(samples, |current, best| current < best)?;
            Some(max - min)
        }
        RangeFunc::MedianOverTime => {
            // `rollupMedian` -> `quantile(0.5, ...)`.
            let mut values: Vec<f64> = samples.iter().map(|s| s.value).collect();
            values.sort_by(|a, b| a.total_cmp(b));
            Some(quantile_of(&values, 0.5))
        }
        RangeFunc::Sum2OverTime => Some(samples.iter().map(|s| s.value * s.value).sum()),
        RangeFunc::GeomeanOverTime => {
            let mut product = 1.0;
            for sample in samples {
                product *= sample.value;
            }
            Some(product.powf(1.0 / samples.len() as f64))
        }
        RangeFunc::DistinctOverTime => {
            // VM uses `map[float64]struct{}`, and Go treats `-0` and `0` as
            // the same key, so canonicalise zero before hashing.
            let mut seen: Vec<u64> = Vec::with_capacity(samples.len());
            for sample in samples {
                let bits = if sample.value == 0.0 {
                    0
                } else {
                    sample.value.to_bits()
                };
                if !seen.contains(&bits) {
                    seen.push(bits);
                }
            }
            Some(seen.len() as f64)
        }
        // `modeNoNaNs(NaN, values)`: the longest run of equal values after
        // sorting, with VM's index bookkeeping transcribed as is.
        RangeFunc::ModeOverTime => {
            let mut values: Vec<f64> = samples.iter().map(|s| s.value).collect();
            values.sort_by(|a, b| a.total_cmp(b));
            let mut prev_value = f64::NAN;
            let mut j: i64 = -1;
            let mut max_run = 0i64;
            let mut mode = f64::NAN;
            for (i, value) in values.iter().copied().enumerate() {
                let i = i as i64;
                if prev_value == value {
                    continue;
                }
                let run = i - j;
                if run > max_run || mode.is_nan() {
                    max_run = run;
                    mode = prev_value;
                }
                j = i;
                prev_value = value;
            }
            let run = values.len() as i64 - j;
            if run > max_run || mode.is_nan() {
                mode = prev_value;
            }
            Some(mode)
        }
        // `rollupIncreases`: like `changes`, but only upward steps count, with
        // VM's 1e-12 relative tolerance for float noise. The first sample only
        // seeds the comparison because `prevValue` is NaN.
        RangeFunc::IncreasesOverTime => {
            let mut count = 0.0;
            let mut previous = samples[0].value;
            for sample in &samples[1..] {
                if sample.value > previous && !vm_is_rounding_noise(previous, sample.value) {
                    count += 1.0;
                }
                previous = sample.value;
            }
            Some(count)
        }
        // `decreases_over_time` *is* `rollupResets`.
        RangeFunc::DecreasesOverTime => Some(
            samples
                .windows(2)
                .filter(|pair| {
                    pair[1].value < pair[0].value
                        && !vm_is_rounding_noise(pair[0].value, pair[1].value)
                })
                .count() as f64,
        ),
        // MetricsQL's names for the two `ts_of_*` functions.
        RangeFunc::TFirstOverTime => samples.first().map(|s| s.timestamp as f64 / 1_000.0),
        RangeFunc::TLastOverTime => samples.last().map(|s| s.timestamp as f64 / 1_000.0),
    }
}

/// VM's `math.Abs(v-prevValue) < 1e-12*math.Abs(v)`: a step that small is
/// treated as floating-point noise rather than a real change.
fn vm_is_rounding_noise(previous: f64, value: f64) -> bool {
    (value - previous).abs() < 1e-12 * value.abs()
}

/// The value `samples` holds when comparing with `better` (VM's `rollupMax` /
/// `rollupMin`): seed with the first value, then walk forward.
fn vm_extreme(samples: &[SamplePoint], better: impl Fn(f64, f64) -> bool) -> Option<f64> {
    let mut best = samples.first()?.value;
    for sample in &samples[1..] {
        if better(sample.value, best) {
            best = sample.value;
        }
    }
    Some(best)
}

/// Prometheus' `kahansum.Inc`: adds `inc` to `sum` with compensation `c`,
/// Neumaier-style (the smaller term wins the comparison), returning the new
/// sum and compensation. An infinite intermediate result drops the
/// compensation, since there is nothing left to compensate for.
fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;
    let mut c = c;
    if t.is_infinite() {
        c = 0.0;
    } else if sum.abs() >= inc.abs() {
        c += (sum - t) + inc;
    } else {
        c += (inc - t) + sum;
    }
    (t, c)
}

/// Prometheus' `calcTrendValue`: the damped trend, with `i == 0` meaning "the
/// initial `b` stands".
fn calc_trend_value(i: usize, tf: f64, s0: f64, s1: f64, b: f64) -> f64 {
    if i == 0 {
        return b;
    }
    let x = tf * (s1 - s0);
    let y = (1.0 - tf) * b;
    x + y
}

/// Least squares fit of `samples`, with `x` in seconds from `origin_ms`.
///
/// Mirrors Prometheus' `linearRegression`, including its short cut for a
/// constant series: floating-point noise in the sums could otherwise turn
/// "all values equal" into a tiny non-zero slope.
fn linear_regression(samples: &[SamplePoint], origin_ms: i64) -> (f64, f64) {
    let n = samples.len() as f64;
    let first = samples[0].value;
    if samples.iter().all(|sample| sample.value == first) {
        // A constant series has slope 0 — unless the constant is infinite, in
        // which case Prometheus reports NaN for both.
        return if first.is_infinite() {
            (f64::NAN, f64::NAN)
        } else {
            (0.0, first)
        };
    }
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;
    for sample in samples {
        let x = (sample.timestamp - origin_ms) as f64 / 1_000.0;
        sum_x += x;
        sum_y += sample.value;
        sum_xy += x * sample.value;
        sum_x2 += x * x;
    }
    let cov_xy = sum_xy - sum_x * sum_y / n;
    let var_x = sum_x2 - sum_x * sum_x / n;
    let slope = if var_x == 0.0 { 0.0 } else { cov_xy / var_x };
    (slope, sum_y / n - slope * sum_x / n)
}

/// Evaluates a binary operation between two instant vectors.
///
/// The control flow follows Prometheus' `VectorBinop`: a one-to-many match is
/// evaluated as many-to-one with the sides swapped (and the operand order
/// restored when applying the operator), the right side is the "one" side, and
/// the result keeps the left side's labels apart from what the matching rules
/// and the operator itself change.
fn binary_vectors(
    op: BinaryOp,
    lhs: &Vector,
    rhs: &Vector,
    ret_bool: bool,
    matching: &VectorMatching,
) -> Result<Vector> {
    if op.is_set() {
        return set_vectors(op, lhs, rhs, matching);
    }
    if matching.card == Cardinality::ManyToMany {
        return bad(format!(
            "many-to-many matching is only allowed for set operators, not `{}`",
            op.name()
        ));
    }
    // Evaluate `a group_right b` as `b group_left a` and swap the operands
    // back when the operator is applied.
    let swapped = matching.card == Cardinality::OneToMany;
    let (many, one) = if swapped { (rhs, lhs) } else { (lhs, rhs) };

    // Every match key must be unique on the "one" side; two series that share
    // one would make the pairing ambiguous.
    let mut one_sides: HashMap<MatchKey, &Point> = HashMap::new();
    for point in &one.0 {
        let signature = matching_signature(&point.labels, matching);
        if let Some(previous) = one_sides.insert(signature.clone(), point) {
            let mut first = label_string(&previous.labels);
            let mut second = label_string(&point.labels);
            if second < first {
                std::mem::swap(&mut first, &mut second);
            }
            let side = if swapped { "left" } else { "right" };
            let group: BTreeMap<String, String> = signature.into_iter().collect();
            return bad(format!(
                "found duplicate series for the match group {} on the {side} hand-side of the \
                 operation: [{first}, {second}]; many-to-many matching not allowed: matching \
                 labels must be unique on one side",
                label_string(&group)
            ));
        }
    }

    let mut out = Vector::default();
    let mut taken_one_to_one: HashSet<MatchKey> = HashSet::new();
    // For `group_left`/`group_right` the grouping labels must still make the
    // result unique; track the result label sets per match key to catch a
    // collision instead of emitting two identical series.
    let mut taken_many: HashMap<MatchKey, HashSet<LabelSet>> = HashMap::new();

    for point in &many.0 {
        let signature = matching_signature(&point.labels, matching);
        let Some(other) = one_sides.get(&signature) else {
            continue;
        };
        // After a swap `point` is the original right operand, so put the
        // operands back in query order.
        let (left_value, right_value) = if swapped {
            (other.value, point.value)
        } else {
            (point.value, other.value)
        };
        let value = op.apply(left_value, right_value);
        let keep = if op.is_comparison() && !ret_bool {
            value == 1.0
        } else {
            true
        };

        let mut labels = point.labels.clone();
        // Arithmetic results describe a new quantity, and a `bool` comparison
        // no longer reports the left metric; a filtering comparison does.
        if changes_metric_schema(op) || ret_bool {
            labels.remove("__name__");
        }
        if matching.card == Cardinality::OneToOne {
            if matching.on {
                labels.retain(|name, _| matching.matching_labels.contains(name));
            } else {
                for name in &matching.matching_labels {
                    labels.remove(name);
                }
            }
        }
        // `group_left(...)` / `group_right(...)` labels come from the "one"
        // side, since that is the side that describes the group.
        for name in &matching.include {
            match other.labels.get(name) {
                Some(value) => {
                    labels.insert(name.clone(), value.clone());
                }
                None => {
                    labels.remove(name);
                }
            }
        }

        if matching.card == Cardinality::OneToOne {
            if !taken_one_to_one.insert(signature) {
                return bad(
                    "multiple matches for labels: many-to-one matching must be explicit \
                     (group_left/group_right)",
                );
            }
        } else {
            let inserted = taken_many.entry(signature).or_default();
            if !inserted.insert(labels.clone()) {
                return bad(
                    "multiple matches for labels: grouping labels must ensure unique matches",
                );
            }
        }

        if !keep {
            continue;
        }
        // A comparison without `bool` reports the left operand's value.
        out.push(
            labels,
            if op.is_comparison() && !ret_bool {
                left_value
            } else {
                value
            },
        );
    }
    Ok(out)
}

/// Whether an operator produces a different quantity than its operands, which
/// means the result can no longer be called by the left metric's name.
fn changes_metric_schema(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Pow
            | BinaryOp::Atan2
    )
}

/// `and` / `or` / `unless`: both sides are matched many-to-many, and the
/// samples are passed through unchanged.
fn set_vectors(
    op: BinaryOp,
    lhs: &Vector,
    rhs: &Vector,
    matching: &VectorMatching,
) -> Result<Vector> {
    if matching.card != Cardinality::ManyToMany {
        return bad(format!(
            "set operations must always be many-to-many, not `{}` matching",
            match matching.card {
                Cardinality::OneToOne => "one-to-one",
                Cardinality::ManyToOne => "many-to-one",
                Cardinality::OneToMany => "one-to-many",
                Cardinality::ManyToMany => "many-to-many",
            }
        ));
    }
    let mut out = Vector::default();
    match op {
        BinaryOp::And => {
            let right: HashSet<MatchKey> = rhs
                .0
                .iter()
                .map(|point| matching_signature(&point.labels, matching))
                .collect();
            for point in &lhs.0 {
                if right.contains(&matching_signature(&point.labels, matching)) {
                    out.push_point(point);
                }
            }
        }
        BinaryOp::Unless => {
            let right: HashSet<MatchKey> = rhs
                .0
                .iter()
                .map(|point| matching_signature(&point.labels, matching))
                .collect();
            for point in &lhs.0 {
                if !right.contains(&matching_signature(&point.labels, matching)) {
                    out.push_point(point);
                }
            }
        }
        BinaryOp::Or => {
            let left: HashSet<MatchKey> = lhs
                .0
                .iter()
                .map(|point| matching_signature(&point.labels, matching))
                .collect();
            for point in &lhs.0 {
                out.push_point(point);
            }
            for point in &rhs.0 {
                if !left.contains(&matching_signature(&point.labels, matching)) {
                    out.push_point(point);
                }
            }
        }
        _ => unreachable!("checked is_set"),
    }
    Ok(out)
}

/// The timestamp (in seconds) of the sample a `compareOverTime`-style scan
/// settles on, following Prometheus' loop exactly: start from the first
/// sample and replace the extreme when the comparison holds.
fn timestamp_of_extreme(samples: &[SamplePoint], wins: impl Fn(f64, f64) -> bool) -> Option<f64> {
    let first = samples.first()?;
    let mut best = first.value;
    let mut best_ts = first.timestamp;
    for sample in samples {
        if wins(sample.value, best) {
            best = sample.value;
            best_ts = sample.timestamp;
        }
    }
    Some(best_ts as f64 / 1_000.0)
}

/// The values of a series' samples, for the aggregation-over-time functions.
fn values(samples: &[SamplePoint]) -> Vec<f64> {
    samples.iter().map(|s| s.value).collect()
}

/// One classic histogram: the `le`/cumulative-count pairs grouped by the label
/// set that stays after `le` and `__name__` are dropped.
type ClassicHistograms = BTreeMap<BTreeMap<String, String>, Vec<(f64, f64)>>;

/// `histogram_quantile(q, v)` for classic `le` buckets.
///
/// Every series must carry a `le` bucket bound; `_sum`/`_count` series (or a
/// missing `by (le)` aggregation) are refused by name rather than dropped, so
/// a panel that passes the wrong metric hears about it instead of drawing an
/// empty graph.
fn histogram_quantile(quantile: f64, vector: &Vector) -> Result<Vector> {
    let mut out = Vector::default();
    for (labels, mut buckets) in classic_buckets("histogram_quantile", vector)? {
        out.push(labels, bucket_quantile(quantile, &mut buckets));
    }
    Ok(out)
}

/// `histogram_quantiles(v, "label", q1, q2, ...)` for classic `le` buckets.
///
/// One series per quantile per histogram, with the quantile rendered by
/// `format_open_metrics_float` into `label`. `__name__` is dropped and the
/// bucket groups are the same ones `histogram_quantile` uses; a quantile
/// outside `[0, 1]` still produces a series (Prometheus only warns), whose
/// value is `-Inf`, `+Inf` or `NaN`.
fn histogram_quantiles(quantiles: &[f64], label: &str, vector: &Vector) -> Result<Vector> {
    let groups = classic_buckets("histogram_quantiles", vector)?;
    let mut out = Vector::default();
    for quantile in quantiles {
        let name = format_open_metrics_float(*quantile);
        for (labels, buckets) in &groups {
            // Each quantile gets its own copy of the buckets so
            // `BucketQuantile` can sort and coalesce in place; Prometheus
            // reuses the slice and relies on those steps being idempotent.
            let mut buckets = buckets.clone();
            let mut labels = labels.clone();
            labels.insert(label.to_string(), name.clone());
            out.push(labels, bucket_quantile(*quantile, &mut buckets));
        }
    }
    Ok(out)
}

/// Groups a vector of classic `le` buckets into one histogram per label set.
///
/// Every series must carry a `le` bucket bound; `_sum`/`_count` series (or a
/// missing `by (le)` aggregation) are refused by name rather than dropped, so
/// a panel that passes the wrong metric hears about it instead of drawing an
/// empty graph. Prometheus emits a warning and skips such a series; this
/// evaluator has no annotation channel, so the error is the honest signal.
fn classic_buckets(function: &str, vector: &Vector) -> Result<ClassicHistograms> {
    let mut groups: ClassicHistograms = BTreeMap::new();
    for point in &vector.0 {
        let Some(raw) = point.labels.get("le") else {
            return bad(format!(
                "{function} needs a `le` label on every series; {} has none",
                label_string(&point.labels)
            ));
        };
        let Ok(upper) = raw.trim().parse::<f64>() else {
            return bad(format!(
                "{function} cannot read `le={raw}` as a bucket bound"
            ));
        };
        if upper.is_nan() {
            // Prometheus refuses a NaN bound for the same reason as an
            // unparseable one: every comparison against it is false.
            return bad(format!(
                "{function} cannot read `le={raw}` as a bucket bound"
            ));
        }
        let mut labels = point.labels.clone();
        labels.remove("le");
        labels.remove("__name__");
        groups.entry(labels).or_default().push((upper, point.value));
    }
    Ok(groups)
}

/// `histogram_fraction(lower, upper, v)` for classic `le` buckets.
fn histogram_fraction(lower: f64, upper: f64, vector: &Vector) -> Result<Vector> {
    let mut out = Vector::default();
    for (labels, mut buckets) in classic_buckets("histogram_fraction", vector)? {
        out.push(labels, bucket_fraction(lower, upper, &mut buckets));
    }
    Ok(out)
}

/// MetricsQL's `histogram_avg` / `histogram_stddev` / `histogram_stdvar` for
/// classic `le` buckets.
///
/// Each bucket is assumed to hold its observations uniformly between the
/// previous bound and its own, so its weight is the increase in the cumulative
/// count and its representative value is the midpoint of that interval. The
/// first bucket's interval starts at zero, which is VM's assumption as well
/// (`lePrev` and `vPrev` start at 0).
fn histogram_stat(op: HistogramStatOp, vector: &Vector) -> Result<Vector> {
    let mut out = Vector::default();
    for (labels, mut buckets) in classic_buckets(op.name(), vector)? {
        // VM sorts by `le`; `classic_buckets` already rejected NaN bounds, so
        // `total_cmp` only has to order the remaining values.
        buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
        out.push(labels, bucket_stat(op, &buckets));
    }
    Ok(out)
}

fn bucket_stat(op: HistogramStatOp, buckets: &[(f64, f64)]) -> f64 {
    let mut le_prev = 0.0;
    let mut value_prev = 0.0;
    let mut sum = 0.0;
    let mut sum2 = 0.0;
    let mut weight_total = 0.0;
    // The `+Inf` bucket has no midpoint, so VM skips it — and does not let it
    // update `lePrev`/`vPrev` either.
    for &(le, value) in buckets {
        if le.is_infinite() {
            continue;
        }
        let midpoint = (le + le_prev) / 2.0;
        let weight = value - value_prev;
        sum += midpoint * weight;
        sum2 += midpoint * midpoint * weight;
        weight_total += weight;
        le_prev = le;
        value_prev = value;
    }
    if weight_total == 0.0 {
        return f64::NAN;
    }
    let avg = sum / weight_total;
    if op == HistogramStatOp::Avg {
        return avg;
    }
    let mut variance = sum2 / weight_total - avg * avg;
    // VM clamps a negative result to zero: it can only come from float error,
    // since the weights are non-negative for a monotonic histogram.
    if variance < 0.0 {
        variance = 0.0;
    }
    if op == HistogramStatOp::Stdvar {
        variance
    } else {
        variance.sqrt()
    }
}

/// Prometheus' `BucketQuantile` for classic histograms.
///
/// Buckets are sorted by upper bound, buckets sharing a bound are coalesced
/// (counts added), and a count that is lower than its predecessor is clamped
/// up to it: Prometheus makes both corrections silently, since a decrease can
/// only come from inconsistent input or float noise.
///
/// The cases Prometheus answers with NaN are kept: fewer than two buckets, no
/// `+Inf` bucket, or zero observations. A quantile that lands in the `+Inf`
/// bucket answers the upper bound of the last finite bucket, as Prometheus
/// does — the distribution gives no information about how far into `+Inf` it
/// really is.
fn bucket_quantile(quantile: f64, buckets: &mut [(f64, f64)]) -> f64 {
    if quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    if buckets.is_empty() {
        return f64::NAN;
    }
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    if buckets.last().map(|(upper, _)| *upper) != Some(f64::INFINITY) {
        return f64::NAN;
    }
    let mut merged: Vec<(f64, f64)> = Vec::with_capacity(buckets.len());
    for (upper, count) in buckets.iter().copied() {
        if count.is_nan() {
            return f64::NAN;
        }
        match merged.last_mut() {
            Some((last_upper, last_count)) if *last_upper == upper => *last_count += count,
            _ => merged.push((upper, count)),
        }
    }
    let mut previous = merged[0].1;
    for entry in merged.iter_mut().skip(1) {
        if entry.1 < previous {
            entry.1 = previous;
        } else {
            previous = entry.1;
        }
    }
    if merged.len() < 2 {
        return f64::NAN;
    }
    let observations = merged[merged.len() - 1].1;
    if observations == 0.0 {
        return f64::NAN;
    }
    let rank = quantile * observations;
    // Only the finite buckets are searched: falling past them puts the answer
    // in the `+Inf` bucket.
    let index = merged[..merged.len() - 1]
        .iter()
        .position(|(_, count)| *count >= rank);
    match index {
        None => merged[merged.len() - 2].0,
        Some(0) if merged[0].0 <= 0.0 => merged[0].0,
        Some(index) => {
            let (end, count) = merged[index];
            let (start, below) = if index == 0 {
                (0.0, 0.0)
            } else {
                merged[index - 1]
            };
            let count = count - below;
            let rank = rank - below;
            start + (end - start) * (rank / count)
        }
    }
}

/// Prometheus' `BucketFraction` for classic histograms: the fraction of the
/// observations that fall between `lower` and `upper`.
///
/// The same sorting and coalescing as `bucket_quantile` applies, but *not* its
/// monotonicity clamp: Prometheus' `BucketFraction` does not force it either.
/// Special cases, kept as Prometheus has them: no buckets, no `+Inf` bucket or
/// zero observations answer NaN; a NaN bound answers NaN; `lower >= upper`
/// answers 0.
///
/// `lower` is exclusive for positive values and inclusive for negative ones,
/// `upper` the other way round — that follows from interpolating only when a
/// bound falls strictly *inside* a bucket. Observations above the last finite
/// bucket are not counted, so `histogram_fraction(-Inf, +Inf, v)` can be < 1.
fn bucket_fraction(lower: f64, upper: f64, buckets: &mut [(f64, f64)]) -> f64 {
    if buckets.is_empty() {
        return f64::NAN;
    }
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    if buckets.last().map(|(upper, _)| *upper) != Some(f64::INFINITY) {
        return f64::NAN;
    }
    let mut merged: Vec<(f64, f64)> = Vec::with_capacity(buckets.len());
    for (upper_bound, count) in buckets.iter().copied() {
        match merged.last_mut() {
            Some((last_upper, last_count)) if *last_upper == upper_bound => *last_count += count,
            _ => merged.push((upper_bound, count)),
        }
    }
    let observations = merged[merged.len() - 1].1;
    if observations == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let mut rank = 0.0;
    let mut lower_rank = 0.0;
    let mut upper_rank = 0.0;
    let mut lower_set = false;
    let mut upper_set = false;
    // A histogram whose first bucket ends at or below zero has an implicit
    // lower bound of -Inf; otherwise 0 is assumed.
    let mut lower_bound = if merged[0].0 <= 0.0 {
        f64::NEG_INFINITY
    } else {
        0.0
    };
    for (i, (upper_bound, count)) in merged.iter().copied().enumerate() {
        if i > 0 {
            lower_bound = merged[i - 1].0;
        }
        // Interpolation inside a bucket; an infinite lower bound cannot be
        // interpolated against, so the bucket's whole count is used (the same
        // effect the `+Inf` case gets for free).
        let interpolate = |value: f64| {
            if lower_bound == f64::NEG_INFINITY {
                count
            } else {
                rank + (count - rank) * (value - lower_bound) / (upper_bound - lower_bound)
            }
        };
        if !lower_set && lower_bound >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && lower_bound >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && lower_bound < lower && upper_bound > lower {
            lower_rank = interpolate(lower);
            lower_set = true;
        }
        if !upper_set && lower_bound < upper && upper_bound > upper {
            upper_rank = interpolate(upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank = count;
    }
    if !lower_set || lower_rank > observations {
        lower_rank = observations;
    }
    if !upper_set || upper_rank > observations {
        upper_rank = observations;
    }
    (upper_rank - lower_rank) / observations
}

/// Orders two label sets for `sort_by_label` / `sort_by_label_desc`.
///
/// Follows Prometheus' `funcSortByLabel`: walk the requested labels in order,
/// skipping ones whose values are equal; the first difference is decided by
/// `natsort.Compare`. When every requested label matches, the full label set
/// breaks the tie (lexicographically, as `labels.Compare` does), which keeps
/// the order deterministic under an unstable sort. `desc` reverses the whole
/// comparison — including the tiebreak, as Prometheus negates it.
fn sort_by_label_order(a: &LabelSet, b: &LabelSet, labels: &[String]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for label in labels {
        let left = a.get(label).map(String::as_str).unwrap_or("");
        let right = b.get(label).map(String::as_str).unwrap_or("");
        if left == right {
            continue;
        }
        // `natsort.Compare` is not a total order (see below), so ask it both
        // ways and only trust it when the two directions agree. A pathological
        // pair like "2" vs "02" precedes both ways; the full labelset then
        // decides, which is deterministic where Prometheus' unstable sort is
        // arbitrary.
        let forward = natsort_compare(left, right);
        let backward = natsort_compare(right, left);
        if forward && !backward {
            return Ordering::Less;
        }
        if backward && !forward {
            return Ordering::Greater;
        }
        break;
    }
    labels_compare(a, b)
}

/// `facette/natsort`.Compare`: chunk both strings into digit and non-digit runs
/// (Go's `(\d+|\D+)` over UTF-8) and compare chunk by chunk, numerically when
/// both chunks are numbers.
///
/// Note this is not a total order — that is what Prometheus uses, and the
/// full-labelset tiebreak in the caller makes the result deterministic.
fn natsort_compare(a: &str, b: &str) -> bool {
    let chunks_a = natsort_chunks(a);
    let chunks_b = natsort_chunks(b);
    for (i, chunk_a) in chunks_a.iter().enumerate() {
        if i >= chunks_b.len() {
            return false;
        }
        let chunk_b = &chunks_b[i];
        let int_a = chunk_a.parse::<i64>().ok().filter(|_| is_digits(chunk_a));
        let int_b = chunk_b.parse::<i64>().ok().filter(|_| is_digits(chunk_b));
        if let (Some(int_a), Some(int_b)) = (int_a, int_b) {
            if int_a == int_b {
                if i == chunks_a.len() - 1 {
                    return true;
                } else if i == chunks_b.len() - 1 {
                    return false;
                }
                continue;
            }
            return int_a < int_b;
        }
        if chunk_a == chunk_b {
            if i == chunks_a.len() - 1 {
                return true;
            } else if i == chunks_b.len() - 1 {
                return false;
            }
            continue;
        }
        return chunk_a < chunk_b;
    }
    false
}

fn is_digits(chunk: &str) -> bool {
    !chunk.is_empty() && chunk.bytes().all(|byte| byte.is_ascii_digit())
}

/// Splits into runs of ASCII digits and runs of everything else, matching Go's
/// `\d+|\D+`. A very long digit run still yields one chunk; it simply fails the
/// `i64` parse and falls back to string comparison, as Go's `strconv.Atoi`
/// error does.
fn natsort_chunks(value: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_digit = None;
    for ch in value.chars() {
        let digit = ch.is_ascii_digit();
        if let Some(previous) = current_digit {
            if previous != digit {
                chunks.push(std::mem::take(&mut current));
            }
        }
        current_digit = Some(digit);
        current.push(ch);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// `labels.Compare` from Prometheus: lexicographic on (name, value) pairs. The
/// label sets here are `BTreeMap`s, so the iteration order is the sorted one.
fn labels_compare(a: &LabelSet, b: &LabelSet) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let mut left = a.iter();
    let mut right = b.iter();
    loop {
        match (left.next(), right.next()) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some((name_a, value_a)), Some((name_b, value_b))) => {
                match name_a.cmp(name_b).then_with(|| value_a.cmp(value_b)) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
        }
    }
}

/// Renders a label set for an error message, e.g. `{job="api"}`.
fn label_string(labels: &LabelSet) -> String {
    let body = labels
        .iter()
        .map(|(name, value)| format!("{name}={value:?}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{body}}}")
}

/// Go's `math.Min`: NaN wins over any number, unlike Rust's `f64::min`.
fn go_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.min(b)
    }
}

/// Go's `math.Max`: NaN wins over any number, unlike Rust's `f64::max`.
fn go_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}

/// Prometheus' interpolation between the two neighbouring order statistics.
fn quantile_of(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() || quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    let rank = quantile * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return sorted[lower];
    }
    sorted[lower] + (sorted[upper] - sorted[lower]) * (rank - lower as f64)
}

fn apply_math(func: MathFunc, value: f64, to_nearest: f64, _at_ms: i64) -> f64 {
    match func {
        MathFunc::Abs => value.abs(),
        MathFunc::Ceil => value.ceil(),
        MathFunc::Floor => value.floor(),
        MathFunc::Sqrt => value.sqrt(),
        MathFunc::Exp => value.exp(),
        MathFunc::Ln => value.ln(),
        MathFunc::Log2 => value.log2(),
        MathFunc::Log10 => value.log10(),
        MathFunc::Sgn => {
            if value > 0.0 {
                1.0
            } else if value < 0.0 {
                -1.0
            } else {
                value
            }
        }
        MathFunc::Round => {
            if to_nearest == 0.0 {
                value
            } else {
                (value / to_nearest).round() * to_nearest
            }
        }
        MathFunc::Sin => value.sin(),
        MathFunc::Cos => value.cos(),
        MathFunc::Tan => value.tan(),
        MathFunc::Asin => value.asin(),
        MathFunc::Acos => value.acos(),
        MathFunc::Atan => value.atan(),
        MathFunc::Sinh => value.sinh(),
        MathFunc::Cosh => value.cosh(),
        MathFunc::Tanh => value.tanh(),
        MathFunc::Asinh => value.asinh(),
        MathFunc::Acosh => value.acosh(),
        MathFunc::Atanh => value.atanh(),
        MathFunc::Deg => value * (180.0 / std::f64::consts::PI),
        MathFunc::Rad => value * (std::f64::consts::PI / 180.0),
        // `timestamp` needs the sample's own timestamp, handled by the caller.
        // `default`, `histogram_quantile` and the clamps are handled before
        // they reach here.
        MathFunc::Timestamp
        | MathFunc::ClampMin
        | MathFunc::ClampMax
        | MathFunc::Default
        | MathFunc::HistogramQuantile => value,
    }
}

// ---------------------------------------------------------------------------
// Query entry points
// ---------------------------------------------------------------------------

/// The `data` object of an instant query.
pub async fn instant_query(
    backend: &Backend,
    expr: &Expr,
    at_ms: i64,
) -> Result<serde_json::Value> {
    let fetched = fetch(backend, expr, at_ms, at_ms).await?;
    let evaluator = Evaluator {
        fetched: &fetched,
        query_start_ms: at_ms,
        query_end_ms: at_ms,
        step_ms: 0,
    };
    let instant = evaluator.eval(expr, at_ms)?;
    if let Some(value) = instant.scalar {
        return Ok(serde_json::json!({
            "resultType": "scalar",
            "result": [seconds_json(at_ms), format_value(value)],
        }));
    }
    let mut result = Vec::with_capacity(instant.vector.0.len());
    for point in &instant.vector.0 {
        let ts = point.sample_ts.unwrap_or(at_ms);
        result.push(serde_json::json!({
            "metric": label_object(&point.labels),
            "value": [seconds_json(ts), format_value(point.value)],
        }));
    }
    Ok(serde_json::json!({
        "resultType": "vector",
        "result": result,
    }))
}

/// The `data` object of a range query.
///
/// Each step is evaluated independently over the same fetched samples, so the
/// cost is one engine scan per selector plus a per-step pass over memory.
pub async fn range_query(
    backend: &Backend,
    expr: &Expr,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<serde_json::Value> {
    if step_ms <= 0 {
        return bad("step must be positive");
    }
    if end_ms < start_ms {
        return bad("end must not be before start");
    }
    let steps = (end_ms - start_ms) / step_ms + 1;
    if steps > MAX_STEPS {
        return bad(format!(
            "query would produce {steps} points per series, more than the {MAX_STEPS} \
             Prometheus allows; increase the step or shorten the range"
        ));
    }

    let fetched = fetch(backend, expr, start_ms, end_ms).await?;
    let evaluator = Evaluator {
        fetched: &fetched,
        query_start_ms: start_ms,
        query_end_ms: end_ms,
        step_ms,
    };

    // Result series keyed by their exact label set, in first-seen order so the
    // output is stable across identical queries.
    let mut order: Vec<Vec<(String, String)>> = Vec::new();
    let mut series: HashMap<MatchKey, (LabelSet, Vec<(i64, f64)>)> = HashMap::new();

    let mut at = start_ms;
    loop {
        let instant = evaluator.eval(expr, at)?;
        let values = if let Some(scalar) = instant.scalar {
            let mut vector = Vector::default();
            vector.push(BTreeMap::new(), scalar);
            vector
        } else {
            instant.vector
        };
        for point in values.0 {
            let signature: Vec<(String, String)> = point
                .labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            let entry = series.entry(signature.clone()).or_insert_with(|| {
                order.push(signature);
                (point.labels.clone(), Vec::new())
            });
            entry.1.push((at, point.value));
        }
        if at >= end_ms {
            break;
        }
        at = (at + step_ms).min(end_ms);
    }

    let result: Vec<serde_json::Value> = order
        .into_iter()
        .filter_map(|signature| series.remove(&signature))
        .map(|(labels, values)| {
            serde_json::json!({
                "metric": label_object(&labels),
                "values": values
                    .into_iter()
                    .map(|(ts, value)| serde_json::json!([seconds_json(ts), format_value(value)]))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();

    Ok(serde_json::json!({
        "resultType": "matrix",
        "result": result,
    }))
}

/// The `metric` object of a result: the label set exactly as evaluated,
/// `__name__` included when it survived.
fn label_object(labels: &BTreeMap<String, String>) -> serde_json::Map<String, serde_json::Value> {
    labels
        .iter()
        .map(|(name, value)| (name.clone(), serde_json::Value::String(value.clone())))
        .collect()
}

#[cfg(test)]
mod tests {
    use metric_engine::Label;

    use super::*;

    fn parse_ok(query: &str) -> Expr {
        parse(query).unwrap_or_else(|e| panic!("{query:?} should parse: {e:?}"))
    }

    fn parse_err(query: &str) -> String {
        match parse(query) {
            Ok(expr) => panic!("{query:?} should be rejected, parsed as {expr:?}"),
            Err(e) => e.message().to_string(),
        }
    }

    fn selector(query: &str) -> Selector {
        match parse_ok(query) {
            Expr::Selector { selector, .. } => selector,
            other => panic!("{query:?} is not a selector: {other:?}"),
        }
    }

    #[test]
    fn test_selector_forms() {
        assert_eq!(selector("http_requests").metric, "http_requests");
        let s = selector(r#"http_requests{job="api",code=~"5.."}"#);
        assert_eq!(s.metric, "http_requests");
        assert_eq!(
            s.matchers,
            vec![
                (MatcherType::Eq, "job".into(), "api".into()),
                (MatcherType::Re, "code".into(), "5..".into()),
            ]
        );
        // `__name__` inside the braces names the metric.
        assert_eq!(selector(r#"{__name__="up"}"#).metric, "up");
        // Unquoted label values are accepted, as Prometheus allows.
        assert_eq!(selector("m{job=api}").matchers[0].2, "api");
        // Escapes survive the lexer.
        assert_eq!(selector(r#"m{path="/a\",b"}"#).matchers[0].2, "/a\",b");
        // `\\` is one backslash, so a Prometheus-style escaped dot reaches the
        // engine as `\.`.
        assert_eq!(selector(r#"m{re="\\.d+"}"#).matchers[0].2, r"\.d+");
        // An unknown escape keeps its backslash instead of being dropped: `\d`
        // must not silently become `d`.
        assert_eq!(selector(r#"m{re="\d+"}"#).matchers[0].2, r"\d+");
    }

    #[test]
    fn test_operator_precedence() {
        // `1 + 2 * 3` groups as `1 + (2 * 3)`.
        match parse_ok("1 + 2 * 3") {
            Expr::Binary {
                op: BinaryOp::Add,
                rhs,
                ..
            } => assert!(matches!(
                *rhs,
                Expr::Binary {
                    op: BinaryOp::Mul,
                    ..
                }
            )),
            other => panic!("wrong shape: {other:?}"),
        }
        // `^` binds tighter than `*`.
        assert!(matches!(
            parse_ok("2 * 3 ^ 2"),
            Expr::Binary {
                op: BinaryOp::Mul,
                ..
            }
        ));
        // Comparisons sit below arithmetic: `a - 1 > 0` is `(a - 1) > 0`.
        assert!(matches!(
            parse_ok("a - 1 > 0"),
            Expr::Binary {
                op: BinaryOp::Gt,
                ..
            }
        ));
        // `atan2` shares the precedence of `*`, so `2 + 3 atan2 4` is
        // `2 + (3 atan2 4)` and `2 * 3 atan2 4` is left-associative.
        match parse_ok("2 + 3 atan2 4") {
            Expr::Binary {
                op: BinaryOp::Add,
                rhs,
                ..
            } => assert!(matches!(
                *rhs,
                Expr::Binary {
                    op: BinaryOp::Atan2,
                    ..
                }
            )),
            other => panic!("wrong shape: {other:?}"),
        }
        match parse_ok("2 * 3 atan2 4") {
            Expr::Binary {
                op: BinaryOp::Atan2,
                lhs,
                ..
            } => assert!(matches!(
                *lhs,
                Expr::Binary {
                    op: BinaryOp::Mul,
                    ..
                }
            )),
            other => panic!("wrong shape: {other:?}"),
        }
    }

    #[test]
    fn test_atan2_binary_operator() {
        let series = [
            synthetic_labeled("a", &[("job", "api")], &[(0, 1.0)]),
            synthetic_labeled("b", &[("job", "api")], &[(0, 1.0)]),
        ];
        let mut fetched = HashMap::new();
        fetched.insert(selector_key(&selector("a")), vec![series[0].clone()]);
        fetched.insert(selector_key(&selector("b")), vec![series[1].clone()]);
        let run = |query: &str| {
            let expr = parse_ok(query);
            let evaluator = Evaluator {
                fetched: &fetched,
                query_start_ms: 0,
                query_end_ms: 0,
                step_ms: 0,
            };
            rendered(&evaluator.eval(&expr, 0).unwrap().vector)
        };
        // atan2(1, 1) is pi/4, and like every arithmetic operator the result
        // loses `__name__`.
        assert_eq!(
            run("a atan2 b"),
            vec![("{job=\"api\"}".to_string(), std::f64::consts::FRAC_PI_4)]
        );
        // The operand order is `lhs atan2 rhs`, not the reverse. Scalar-scalar
        // results land in `Instant::scalar`, not the vector.
        let scalar = |query: &str, want: f64| {
            let expr = parse_ok(query);
            let evaluator = Evaluator {
                fetched: &fetched,
                query_start_ms: 0,
                query_end_ms: 0,
                step_ms: 0,
            };
            let value = evaluator.eval(&expr, 0).unwrap().scalar.unwrap();
            assert!((value - want).abs() < 1e-12, "{query} gave {value}");
        };
        scalar("1 atan2 1", std::f64::consts::FRAC_PI_4);
        // atan2(0, -1) is pi, whereas atan2(-1, 0) is -pi/2.
        scalar("0 atan2 -1", std::f64::consts::PI);
        scalar("-1 atan2 0", -std::f64::consts::FRAC_PI_2);
    }

    #[test]
    fn test_query_context_scalars() {
        let fetched = HashMap::new();
        // A range query from 100 s to 160 s in 15 s steps.
        let evaluator = Evaluator {
            fetched: &fetched,
            query_start_ms: 100_000,
            query_end_ms: 160_000,
            step_ms: 15_000,
        };
        let run = |query: &str| {
            let expr = parse_ok(query);
            evaluator.eval(&expr, 130_000).unwrap().scalar.unwrap()
        };
        assert_eq!(run("start()"), 100.0);
        assert_eq!(run("end()"), 160.0);
        assert_eq!(run("range()"), 60.0);
        assert_eq!(run("step()"), 15.0);
        // They are ordinary scalars, so arithmetic and `max_of` compose.
        assert_eq!(run("end() - start()"), 60.0);
        assert_eq!(run("max_of(step(), 20)"), 20.0);
        // An instant query has no step, matching Prometheus folding `step()`
        // to 0 when start equals end; `range()` is then 0 too.
        let instant = Evaluator {
            fetched: &fetched,
            query_start_ms: 42_000,
            query_end_ms: 42_000,
            step_ms: 0,
        };
        let at_instant = |query: &str| {
            let expr = parse_ok(query);
            instant.eval(&expr, 42_000).unwrap().scalar.unwrap()
        };
        assert_eq!(at_instant("start()"), 42.0);
        assert_eq!(at_instant("end()"), 42.0);
        assert_eq!(at_instant("range()"), 0.0);
        assert_eq!(at_instant("step()"), 0.0);
        // A scalar is not a valid subquery body; the parser rejects it.
        let message = parse_err("start()[5m:1m]");
        assert!(message.contains("instant vector, got scalar"));
    }

    #[test]
    fn test_duration_lexing() {
        match parse_ok("m[1h30m]") {
            Expr::Range { range_ms, .. } => assert_eq!(range_ms, 90 * 60 * 1_000),
            other => panic!("not a range: {other:?}"),
        }
        match parse_ok("m offset 5m") {
            Expr::Selector { offset_ms, .. } => assert_eq!(offset_ms, 5 * 60 * 1_000),
            other => panic!("not a selector: {other:?}"),
        }
        // `1e3` is a number, not a duration, and `10s` is a duration.
        assert!(matches!(parse_ok("1e3"), Expr::Number(value) if value == 1000.0));
        assert!(matches!(parse_ok("10s"), Expr::Number(_)));
        // `Inf`/`NaN` are float literals, case-insensitively, and `-Inf` is a
        // unary minus on the literal rather than a metric named `Inf`.
        assert!(matches!(parse_ok("Inf"), Expr::Number(value) if value.is_infinite()));
        assert!(matches!(parse_ok("NaN"), Expr::Number(value) if value.is_nan()));
        match parse_ok("-Inf") {
            Expr::Unary { negate: true, expr } => {
                assert!(matches!(*expr, Expr::Number(value) if value.is_infinite()))
            }
            other => panic!("expected a negated number, got {other:?}"),
        }
    }

    #[test]
    fn test_supported_expressions_parse() {
        for query in [
            "sum(http_requests)",
            "sum by (job) (http_requests)",
            "sum without (instance) (http_requests)",
            "topk(3, http_requests)",
            "avg_over_time(http_requests[5m])",
            "quantile_over_time(0.9, http_requests[5m])",
            "clamp_min(http_requests, 0)",
            "clamp(http_requests, 0, 10)",
            "scalar(http_requests)",
            "deriv(http_requests[5m])",
            "year(http_requests)",
            "year()",
            "days_in_month(http_requests)",
            "quantile(0.9, http_requests)",
            "quantile by (job) (0.5, http_requests)",
            "round(http_requests, 0.5)",
            r#"rate(http_requests{job="api"}[5m] offset 1h)"#,
            "sort_desc(http_requests)",
            "histogram_quantile(0.9, http_latency_bucket)",
            "histogram_quantiles(http_latency_bucket, \"quantile\", 0.5, 0.9)",
            "histogram_quantile(0.5, sum by (le) (rate(http_latency_bucket[5m])))",
            "absent(http_requests)",
            "vector(1)",
            "time()",
            "pi()",
            "start()",
            "end()",
            "range()",
            "step()",
            "max_of(start(), 5s)",
            "range_over_time(http_requests[5m])",
            "median_over_time(http_requests[5m])",
            "sum2_over_time(http_requests[5m])",
            "geomean_over_time(http_requests[5m])",
            "distinct_over_time(http_requests[5m])",
            "mode_over_time(http_requests[5m])",
            "increases_over_time(http_requests[5m])",
            "decreases_over_time(http_requests[5m])",
            "tfirst_over_time(http_requests[5m])",
            "tlast_over_time(http_requests[5m])",
            r#"label_set(http_requests, "a", "1")"#,
            r#"label_del(http_requests, "a")"#,
            r#"label_keep(http_requests, "a")"#,
            r#"label_copy(http_requests, "a", "b")"#,
            r#"label_move(http_requests, "a", "b")"#,
            r#"label_value(http_requests, "a")"#,
            r#"alias(http_requests, "name")"#,
            "histogram_avg(http_latency_bucket)",
            "histogram_stddev(http_latency_bucket)",
            "histogram_stdvar(http_latency_bucket)",
            "-(a + b)",
            "a and b",
            "a or b",
            "a unless b",
            "a > bool 1",
        ] {
            parse_ok(query);
        }
    }

    #[test]
    fn test_metricsql_extras_parse_and_evaluate() {
        // `aggr_over_time` names an aggregation, so both bare and quoted names
        // are accepted.
        parse_ok(r#"aggr_over_time(avg, m[5m])"#);
        parse_ok(r#"aggr_over_time("avg", m[5m])"#);
        parse_ok("absent_over_time(m[5m])");
        parse_ok("tmin_over_time(m[5m])");
        parse_ok("tmax_over_time(m[5m])");
        parse_ok("present_over_time(m[5m])");
        parse_ok("mad_over_time(m[5m])");
        // `predict_linear` puts the range vector first.
        parse_ok("predict_linear(m[5m], 3600)");
        // `default(v, x)`.
        parse_ok("default(m, 0)");
        parse_ok(r#"default(m{job="x"}, 0)"#);
        // Errors name the problem.
        assert!(parse_err("aggr_over_time(5, m[5m])").contains("aggregation name"));
        assert!(parse_err("aggr_over_time(nonsense, m[5m])").contains("not an aggregation"));
        assert!(parse_err("aggr_over_time(topk, m[5m])").contains("takes a parameter"));
        assert!(parse_err("aggr_over_time(quantile, m[5m])").contains("takes a parameter"));
        assert!(parse_err("predict_linear(m[5m])").contains("expected Comma but found"));
    }

    #[test]
    fn test_aggr_and_timestamp_over_time() {
        let samples = [
            SamplePoint {
                timestamp: 0,
                value: 2.0,
            },
            SamplePoint {
                timestamp: 1_000,
                value: 4.0,
            },
            SamplePoint {
                timestamp: 2_000,
                value: 1.0,
            },
        ];
        let values = samples.iter().map(|s| s.value).collect::<Vec<f64>>();
        assert_eq!(values.len(), 3);
        assert_eq!(
            apply_range_func(
                RangeFunc::AggrOverTime(AggOp::Sum),
                &samples,
                3_000,
                None,
                None
            ),
            Some(7.0)
        );
        assert_eq!(
            apply_range_func(
                RangeFunc::AggrOverTime(AggOp::Avg),
                &samples,
                3_000,
                None,
                None
            ),
            Some(7.0 / 3.0)
        );
        assert_eq!(
            apply_range_func(
                RangeFunc::AggrOverTime(AggOp::Max),
                &samples,
                3_000,
                None,
                None
            ),
            Some(4.0)
        );
        assert_eq!(
            apply_range_func(
                RangeFunc::AggrOverTime(AggOp::Min),
                &samples,
                3_000,
                None,
                None
            ),
            Some(1.0)
        );
        // The extreme value is 4 at ts 1 s and 1 at ts 2 s.
        assert_eq!(
            apply_range_func(RangeFunc::TMinOverTime, &samples, 3_000, None, None),
            Some(2.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::TMaxOverTime, &samples, 3_000, None, None),
            Some(1.0)
        );
        // Linear fit of (0,2),(1,4),(2,1): slope -0.5, mean 7/3 at t=1, so the
        // line is 17/6 - t/2 and the prediction at t = last + 1 = 3 is 4/3.
        let predicted =
            apply_range_func(RangeFunc::PredictLinear, &samples, 3_000, Some(1.0), None)
                .expect("predict_linear returns a value");
        assert!((predicted - 4.0 / 3.0).abs() < 1e-9, "got {predicted}");
        assert!(values.len() == 3);
    }

    #[test]
    fn test_present_and_mad_over_time() {
        let samples = |values: &[(i64, f64)]| -> Vec<SamplePoint> {
            values
                .iter()
                .map(|&(timestamp, value)| SamplePoint { timestamp, value })
                .collect()
        };
        // `present_over_time` is 1 for any series that has a sample, whatever
        // the values are.
        let evident = samples(&[(0, 2.0), (1_000, 4.0)]);
        assert_eq!(
            apply_range_func(RangeFunc::PresentOverTime, &evident, 3_000, None, None),
            Some(1.0)
        );
        let all_nan = samples(&[(0, f64::NAN)]);
        assert_eq!(
            apply_range_func(RangeFunc::PresentOverTime, &all_nan, 3_000, None, None),
            Some(1.0)
        );
        // An empty series never reaches the function, but it must not report
        // presence either.
        assert_eq!(
            apply_range_func(RangeFunc::PresentOverTime, &[], 3_000, None, None),
            None
        );
        // mad = median(|v - median|): [1,2,3,4] has median 2.5 and deviations
        // [1.5,0.5,0.5,1.5], whose median is 1.
        let symmetric = samples(&[(0, 1.0), (1_000, 2.0), (2_000, 3.0), (3_000, 4.0)]);
        assert_eq!(
            apply_range_func(RangeFunc::MadOverTime, &symmetric, 4_000, None, None),
            Some(1.0)
        );
        // A single sample deviates from its own median by 0.
        assert_eq!(
            apply_range_func(
                RangeFunc::MadOverTime,
                &samples(&[(0, 7.0)]),
                1_000,
                None,
                None
            ),
            Some(0.0)
        );
        // NaN makes both medians undefined, so the whole result is NaN instead
        // of silently dropping the sample.
        let with_nan = samples(&[(0, 1.0), (1_000, f64::NAN), (2_000, 3.0)]);
        assert!(
            apply_range_func(RangeFunc::MadOverTime, &with_nan, 3_000, None, None)
                .unwrap()
                .is_nan()
        );
    }

    #[test]
    fn test_trigonometric_and_hyperbolic_functions() {
        // Each function is checked on a value inside its domain: `acosh` needs
        // x >= 1, `atanh` needs |x| < 1.
        let run = |value: f64, query: &str| {
            eval_synthetic(
                synthetic_labeled("m", &[("job", "api")], &[(0, value)]),
                "m",
                query,
                0,
            )
        };
        let expect = |value: f64, query: &str, want: f64| {
            let got = run(value, query);
            // Every one of these drops `__name__` and keeps the other labels.
            assert_eq!(got.len(), 1, "{query} produced {got:?}");
            assert_eq!(got[0].0, "{job=\"api\"}");
            assert!(
                (got[0].1 - want).abs() < 1e-12,
                "{query} gave {} not {want}",
                got[0].1
            );
        };
        expect(0.5, "sin(m)", 0.5f64.sin());
        expect(0.5, "cos(m)", 0.5f64.cos());
        expect(0.5, "tan(m)", 0.5f64.tan());
        expect(0.5, "asin(m)", 0.5f64.asin());
        expect(0.5, "acos(m)", 0.5f64.acos());
        expect(0.5, "atan(m)", 0.5f64.atan());
        expect(0.5, "sinh(m)", 0.5f64.sinh());
        expect(0.5, "cosh(m)", 0.5f64.cosh());
        expect(0.5, "tanh(m)", 0.5f64.tanh());
        expect(0.5, "asinh(m)", 0.5f64.asinh());
        expect(2.0, "acosh(m)", 2.0f64.acosh());
        expect(0.5, "atanh(m)", 0.5f64.atanh());
        expect(0.5, "deg(m)", 0.5 * 180.0 / std::f64::consts::PI);
        expect(0.5, "rad(m)", 0.5 * std::f64::consts::PI / 180.0);
    }

    #[test]
    fn test_trigonometric_functions_follow_ieee() {
        // Out-of-domain arguments produce NaN rather than an error, exactly as
        // Go's `math.Asin` / `math.Acosh` / `math.Atanh` do.
        let at = |value: f64| synthetic("m", &[(0, value)]);
        for (query, value) in [
            ("asin(m)", 2.0),
            ("acos(m)", 2.0),
            ("acosh(m)", 0.5),
            ("atanh(m)", 2.0),
        ] {
            assert!(
                eval_synthetic(at(value), "m", query, 0)[0].1.is_nan(),
                "{query} of {value} should be NaN"
            );
        }
        // `atanh(±1)` is ±Inf, and `deg`/`rad` keep the sign of ±Inf.
        assert_eq!(
            eval_synthetic(at(1.0), "m", "atanh(m)", 0)[0].1,
            f64::INFINITY
        );
        assert_eq!(
            eval_synthetic(at(-1.0), "m", "atanh(m)", 0)[0].1,
            f64::NEG_INFINITY
        );
        assert_eq!(
            eval_synthetic(at(f64::INFINITY), "m", "deg(m)", 0)[0].1,
            f64::INFINITY
        );
        // A scalar argument gives one label-less sample, like the other math
        // functions.
        let scalar = eval_synthetic(synthetic("m", &[(0, 0.0)]), "m", "sin(0)", 0);
        assert_eq!(scalar, vec![("{}".to_string(), 0.0)]);
    }

    #[test]
    fn test_natsort_matches_the_reference_implementation() {
        // The sample list from facette/natsort's README, with its documented
        // sorted order.
        let mut values = vec![
            "1000X Radonius Maximus",
            "10X Radonius",
            "200X Radonius",
            "20X Radonius",
            "20X Radonius Prime",
            "30X Radonius",
            "40X Radonius",
            "Allegia 50 Clasteron",
            "Allegia 500 Clasteron",
            "Allegia 50B Clasteron",
            "Allegia 51 Clasteron",
            "Allegia 6R Clasteron",
            "Alpha 100",
            "Alpha 2",
            "Alpha 200",
            "Alpha 2A",
            "Alpha 2A-8000",
            "Alpha 2A-900",
            "Callisto Morphamax",
            "Callisto Morphamax 500",
            "Callisto Morphamax 5000",
            "Callisto Morphamax 600",
            "Callisto Morphamax 6000 SE",
            "Callisto Morphamax 6000 SE2",
            "Callisto Morphamax 700",
            "Callisto Morphamax 7000",
            "Xiph Xlater 10000",
            "Xiph Xlater 2000",
            "Xiph Xlater 300",
            "Xiph Xlater 40",
            "Xiph Xlater 5",
            "Xiph Xlater 50",
            "Xiph Xlater 500",
            "Xiph Xlater 5000",
            "Xiph Xlater 58",
        ];
        values.sort_by(|left, right| {
            if natsort_compare(left, right) {
                std::cmp::Ordering::Less
            } else if natsort_compare(right, left) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        assert_eq!(
            values,
            vec![
                "10X Radonius",
                "20X Radonius",
                "20X Radonius Prime",
                "30X Radonius",
                "40X Radonius",
                "200X Radonius",
                "1000X Radonius Maximus",
                "Allegia 6R Clasteron",
                "Allegia 50 Clasteron",
                "Allegia 50B Clasteron",
                "Allegia 51 Clasteron",
                "Allegia 500 Clasteron",
                "Alpha 2",
                "Alpha 2A",
                "Alpha 2A-900",
                "Alpha 2A-8000",
                "Alpha 100",
                "Alpha 200",
                "Callisto Morphamax",
                "Callisto Morphamax 500",
                "Callisto Morphamax 600",
                "Callisto Morphamax 700",
                "Callisto Morphamax 5000",
                "Callisto Morphamax 6000 SE",
                "Callisto Morphamax 6000 SE2",
                "Callisto Morphamax 7000",
                "Xiph Xlater 5",
                "Xiph Xlater 40",
                "Xiph Xlater 50",
                "Xiph Xlater 58",
                "Xiph Xlater 300",
                "Xiph Xlater 500",
                "Xiph Xlater 2000",
                "Xiph Xlater 5000",
                "Xiph Xlater 10000",
            ]
        );
        // The reference implementation reports "precedes" even for equal
        // strings: when the last chunk matches it returns true. That is why
        // `sort_by_label_order` asks both directions instead of trusting it.
        assert!(natsort_compare("abc", "abc"));
        // Chunks are compared as integers, so `"02"` and `"2"` are equal and
        // the reference implementation reports "precedes" in both directions.
        assert!(natsort_compare("2", "02"));
        assert!(natsort_compare("02", "2"));
        // Empty strings have no chunks and precede nothing.
        assert!(!natsort_compare("", ""));
    }

    #[test]
    fn test_sort_by_label_orders_series_naturally() {
        let series = vec![
            synthetic_labeled("m", &[("instance", "node10")], &[(0, 1.0)]),
            synthetic_labeled("m", &[("instance", "node2")], &[(0, 2.0)]),
            synthetic_labeled("m", &[("instance", "node1")], &[(0, 3.0)]),
        ];
        let labels = |query: &str| -> Vec<String> {
            eval_series(series.clone(), "m", query, 0)
                .vector
                .0
                .iter()
                .map(|point| point.labels["instance"].clone())
                .collect()
        };
        // Natural order: node1, node2, node10 — not node1, node10, node2.
        assert_eq!(
            labels("sort_by_label(m, \"instance\")"),
            vec!["node1", "node2", "node10"]
        );
        assert_eq!(
            labels("sort_by_label_desc(m, \"instance\")"),
            vec!["node10", "node2", "node1"]
        );
        // Multiple labels are tried in order; values are untouched.
        let values: Vec<f64> =
            eval_series(series.clone(), "m", "sort_by_label(m, \"instance\")", 0)
                .vector
                .0
                .iter()
                .map(|point| point.value)
                .collect();
        assert_eq!(values, vec![3.0, 2.0, 1.0]);
        // The label argument is required and must be a valid name.
        assert!(parse_err("sort_by_label(m)").contains("at least one label name"));
        assert!(parse_err("sort_by_label(m, instance)").contains("quoted string"));
        assert!(parse_err("sort_by_label(m, \"1x\")").contains("invalid label name"));
    }

    #[test]
    fn test_sort_by_label_tiebreak_is_the_full_labelset() {
        // Two series sharing the sorted-on value: the full labelset decides, so
        // the order is deterministic in both directions.
        let series = vec![
            synthetic_labeled("m", &[("job", "api"), ("instance", "b")], &[(0, 1.0)]),
            synthetic_labeled("m", &[("job", "api"), ("instance", "a")], &[(0, 2.0)]),
        ];
        let order = |query: &str| -> Vec<String> {
            eval_series(series.clone(), "m", query, 0)
                .vector
                .0
                .iter()
                .map(|point| point.labels["instance"].clone())
                .collect()
        };
        assert_eq!(order("sort_by_label(m, \"job\")"), vec!["a", "b"]);
        assert_eq!(order("sort_by_label_desc(m, \"job\")"), vec!["b", "a"]);
    }

    #[test]
    fn test_metricsql_over_time_rollups() {
        let samples = |values: &[(i64, f64)]| -> Vec<SamplePoint> {
            values
                .iter()
                .map(|&(timestamp, value)| SamplePoint { timestamp, value })
                .collect()
        };
        let run = |func: RangeFunc, values: &[(i64, f64)]| {
            apply_range_func(func, &samples(values), 60_000, None, None)
        };
        let values = [(0, 1.0), (10_000, 3.0), (20_000, 2.0)];
        // `range_over_time` is max - min, so 3 - 1.
        assert_eq!(run(RangeFunc::RangeOverTime, &values), Some(2.0));
        // `median_over_time` is VM's `quantile(0.5, ...)`, i.e. the same
        // interpolation Prometheus uses: rank 1 of 3 is the middle sample.
        assert_eq!(run(RangeFunc::MedianOverTime, &values), Some(2.0));
        // 1 + 9 + 4.
        assert_eq!(run(RangeFunc::Sum2OverTime, &values), Some(14.0));
        // cbrt(1 * 3 * 2).
        let geomean = run(RangeFunc::GeomeanOverTime, &values).unwrap();
        assert!((geomean - 6f64.powf(1.0 / 3.0)).abs() < 1e-12, "{geomean}");
        // Three distinct values out of three samples.
        assert_eq!(run(RangeFunc::DistinctOverTime, &values), Some(3.0));
        assert_eq!(
            run(RangeFunc::DistinctOverTime, &[(0, 1.0), (1, 1.0), (2, 3.0)]),
            Some(2.0)
        );
        // `mode_over_time` picks the value with the longest run: 1, 1, 3 -> 1.
        assert_eq!(
            run(
                RangeFunc::ModeOverTime,
                &[(0, 1.0), (10_000, 3.0), (20_000, 1.0), (30_000, 1.0)]
            ),
            Some(1.0)
        );
        // A single sample is its own mode (VM's trailing-run branch).
        assert_eq!(run(RangeFunc::ModeOverTime, &[(0, 5.0)]), Some(5.0));
        // 1 -> 3 -> 2 has one increase (1 -> 3), and one decrease (3 -> 2).
        assert_eq!(run(RangeFunc::IncreasesOverTime, &values), Some(1.0));
        assert_eq!(run(RangeFunc::DecreasesOverTime, &values), Some(1.0));
        // A flat series has neither.
        let flat = [(0, 2.0), (10_000, 2.0), (20_000, 2.0)];
        assert_eq!(run(RangeFunc::IncreasesOverTime, &flat), Some(0.0));
        assert_eq!(run(RangeFunc::DecreasesOverTime, &flat), Some(0.0));
        // The 1e-12 relative tolerance: a step that small is float noise, not
        // an increase (VM issue #767).
        assert_eq!(
            run(
                RangeFunc::IncreasesOverTime,
                &[(0, 1e12), (10_000, 1e12 + 1.0)]
            ),
            Some(0.0)
        );
        // `tfirst_over_time` / `tlast_over_time` are the timestamps in
        // seconds, exactly like `ts_of_first_over_time` /
        // `ts_of_last_over_time`.
        assert_eq!(
            run(RangeFunc::TFirstOverTime, &values),
            run(RangeFunc::TsOfFirstOverTime, &values)
        );
        assert_eq!(
            run(RangeFunc::TLastOverTime, &values),
            run(RangeFunc::TsOfLastOverTime, &values)
        );
        assert_eq!(run(RangeFunc::TFirstOverTime, &values), Some(0.0));
        assert_eq!(run(RangeFunc::TLastOverTime, &values), Some(20.0));
        // VM drops NaNs from the input series before running any rollup
        // (`removeNanValues`), so a NaN is a gap here, not a value:
        // `range_over_time` sees only 1 and 4.
        let gappy = [(0, 1.0), (10_000, f64::NAN), (20_000, 4.0)];
        assert_eq!(run(RangeFunc::RangeOverTime, &gappy), Some(3.0));
        // A window holding nothing but NaNs has no samples left at all, so the
        // series is dropped.
        let poisoned = [(0, f64::NAN), (10_000, 4.0)];
        assert_eq!(run(RangeFunc::RangeOverTime, &poisoned), Some(0.0));
        assert_eq!(
            run(
                RangeFunc::RangeOverTime,
                &[(0, f64::NAN), (10_000, f64::NAN)]
            ),
            None
        );
        // `median_over_time` therefore ignores NaNs too, unlike Prometheus'
        // `quantile_over_time`, which lets them sort to the end.
        assert_eq!(
            run(
                RangeFunc::MedianOverTime,
                &[(0, 1.0), (10_000, f64::NAN), (20_000, 3.0), (30_000, 5.0)]
            ),
            Some(3.0)
        );
        // The mode sees the gaps removed as well.
        assert_eq!(
            run(
                RangeFunc::ModeOverTime,
                &[(0, 1.0), (10_000, f64::NAN), (20_000, 1.0), (30_000, 3.0)]
            ),
            Some(1.0)
        );
        // `-0` and `0` are the same Go map key.
        assert_eq!(
            run(RangeFunc::DistinctOverTime, &[(0, 0.0), (10_000, -0.0)]),
            Some(1.0)
        );

        // End to end: the names are wired into the parser, and the range
        // functions drop `__name__` (neither keeps it like
        // `first_over_time`).
        let series = synthetic("m", &[(0, 1.0), (10_000, 3.0), (20_000, 2.0)]);
        assert_eq!(
            eval_synthetic(series.clone(), "m", "range_over_time(m[60s])", 20_000),
            vec![("{}".to_string(), 2.0)]
        );
        assert_eq!(
            eval_synthetic(series.clone(), "m", "mode_over_time(m[60s])", 20_000),
            // Every value appears once, and VM keeps the first candidate on a
            // tie, which is the smallest value after sorting.
            vec![("{}".to_string(), 1.0)]
        );
        assert_eq!(
            eval_synthetic(series, "m", "tlast_over_time(m[60s])", 20_000),
            vec![("{}".to_string(), 20.0)]
        );
    }

    #[test]
    fn test_integral() {
        let samples = |values: &[(i64, f64)]| -> Vec<SamplePoint> {
            values
                .iter()
                .map(|&(timestamp, value)| SamplePoint { timestamp, value })
                .collect()
        };
        // A constant 1 sampled every 10 s across a 20 s span integrates to 20,
        // whichever quadrature rule is used.
        let flat = samples(&[(0, 1.0), (10_000, 1.0), (20_000, 1.0)]);
        for strategy in [Some(0.0), Some(1.0), Some(2.0), None] {
            assert_eq!(
                apply_range_func(RangeFunc::Integral, &flat, 20_000, strategy, None),
                Some(20.0),
                "strategy {strategy:?}"
            );
        }
        // A ramp tells the three rules apart: left point 0*10 + 1*10 = 10,
        // right point 1*10 + 2*10 = 30, trapezoid (0+1)/2*10 + (1+2)/2*10 = 20.
        let ramp = samples(&[(0, 0.0), (10_000, 1.0), (20_000, 2.0)]);
        assert_eq!(
            apply_range_func(RangeFunc::Integral, &ramp, 20_000, Some(0.0), None),
            Some(10.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::Integral, &ramp, 20_000, Some(1.0), None),
            Some(30.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::Integral, &ramp, 20_000, Some(2.0), None),
            Some(20.0)
        );
        // NaN is treated as zero, so the neighbouring non-zero values still
        // contribute: (1+0)/2*10 + (0+3)/2*10 = 20.
        let with_nan = samples(&[(0, 1.0), (10_000, f64::NAN), (20_000, 3.0)]);
        assert_eq!(
            apply_range_func(RangeFunc::Integral, &with_nan, 20_000, None, None),
            Some(20.0)
        );
        // A lone sample spans no interval, so the answer is 0 (and the series
        // still appears, unlike `deriv`).
        assert_eq!(
            apply_range_func(RangeFunc::Integral, &samples(&[(0, 5.0)]), 0, None, None),
            Some(0.0)
        );
        // An unusable strategy falls back to the trapezoidal default rather
        // than failing — Prometheus only warns.
        for strategy in [Some(3.0), Some(-1.0), Some(1.5), Some(f64::NAN)] {
            assert_eq!(
                apply_range_func(RangeFunc::Integral, &ramp, 20_000, strategy, None),
                Some(20.0),
                "strategy {strategy:?} should fall back to the trapezoid"
            );
        }
        // And the query form parses without a strategy as well as with one.
        let series = synthetic("m", &[(0, 1.0), (10_000, 1.0), (20_000, 1.0)]);
        assert_eq!(
            eval_synthetic(series.clone(), "m", "integral(m[60s])", 20_000),
            vec![("{}".to_string(), 20.0)]
        );
        assert_eq!(
            eval_synthetic(series, "m", "integral(m[60s], 0)", 20_000),
            vec![("{}".to_string(), 20.0)]
        );
    }

    #[test]
    fn test_double_exponential_smoothing() {
        // A perfectly linear series stays on its line: the trend seeded from
        // the first difference is exact, so every step reproduces `v[i]`.
        let linear = synthetic("m", &[(0, 1.0), (10_000, 2.0), (20_000, 3.0)]);
        assert_eq!(
            eval_synthetic(
                linear.clone(),
                "m",
                "double_exponential_smoothing(m[60s], 0.5, 0.5)",
                20_000
            ),
            vec![("{}".to_string(), 3.0)]
        );
        // A constant series is reproduced exactly too.
        let flat = synthetic("m", &[(0, 7.0), (10_000, 7.0), (20_000, 7.0)]);
        assert_eq!(
            eval_synthetic(
                flat,
                "m",
                "double_exponential_smoothing(m[60s], 0.5, 0.5)",
                20_000
            ),
            vec![("{}".to_string(), 7.0)]
        );
        // A two-sample series is the straight line through them, again because
        // `calcTrendValue(0, ...)` returns the seeded `b` untouched:
        // s1 = sf*v1 + (1-sf)*(v0 + (v1-v0)) = v1.
        let two = synthetic("m", &[(0, 2.0), (10_000, 8.0)]);
        assert_eq!(
            eval_synthetic(
                two,
                "m",
                "double_exponential_smoothing(m[60s], 0.9, 0.9)",
                10_000
            ),
            vec![("{}".to_string(), 8.0)]
        );
        // A non-linear series exercises the damping: hand-worked with the same
        // loop Prometheus runs (see the transcription in `apply_range_func`).
        let bumpy = synthetic(
            "m",
            &[(0, 5.0), (10_000, 3.0), (20_000, 4.0), (30_000, 6.0)],
        );
        let got = eval_synthetic(
            bumpy,
            "m",
            "double_exponential_smoothing(m[60s], 0.4, 0.6)",
            30_000,
        );
        assert_eq!(got.len(), 1);
        assert!(
            (got[0].1 - 2.952).abs() < 1e-12,
            "expected 2.952 for sf=0.4 tf=0.6, got {}",
            got[0].1
        );
        // Fewer than two samples: Prometheus drops the series.
        let single = synthetic("m", &[(0, 1.0)]);
        assert!(eval_synthetic(
            single,
            "m",
            "double_exponential_smoothing(m[60s], 0.5, 0.5)",
            0
        )
        .is_empty());
        // The factors must be strictly inside (0, 1); zero, one and
        // out-of-range are 400s rather than silent garbage.
        let series = synthetic("m", &[(0, 1.0), (10_000, 2.0)]);
        for query in [
            "double_exponential_smoothing(m[60s], 0, 0.5)",
            "double_exponential_smoothing(m[60s], 1, 0.5)",
            "double_exponential_smoothing(m[60s], 0.5, 0)",
            "double_exponential_smoothing(m[60s], 0.5, 1.5)",
        ] {
            let err = eval_err(series.clone(), "m", query, 10_000);
            assert!(
                err.contains("invalid smoothing factor") || err.contains("invalid trend factor"),
                "{query} gave {err}"
            );
        }
        // Name is dropped like every other derived range function.
        assert!(!eval_series(
            vec![synthetic("m", &[(0, 1.0), (10_000, 2.0)])],
            "m",
            "double_exponential_smoothing(m[60s], 0.5, 0.5)",
            10_000
        )
        .vector
        .0[0]
            .labels
            .contains_key("__name__"));
    }

    #[test]
    fn test_first_and_ts_of_over_time() {
        // 10, 20, 30, 30 at 0/10/20/30 s: the duplicated maximum is what
        // distinguishes `ts_of_max_over_time` (`>=`, last one) from a
        // `>`-based scan.
        let series = synthetic(
            "m",
            &[(0, 10.0), (10_000, 20.0), (20_000, 30.0), (30_000, 30.0)],
        );
        let at = 30_000;
        let run = |query: &str| eval_synthetic(series.clone(), "m", query, at);
        // `first_over_time` acts like `offset`, so Prometheus keeps the metric
        // name for it.
        assert_eq!(
            run("first_over_time(m[60s])"),
            vec![("{__name__=\"m\"}".into(), 10.0)]
        );
        assert_eq!(
            run("ts_of_first_over_time(m[60s])"),
            vec![("{}".into(), 0.0)]
        );
        assert_eq!(
            run("ts_of_last_over_time(m[60s])"),
            vec![("{}".into(), 30.0)]
        );
        // `>=` keeps the newest of the equal maxima; `<=` the newest minimum.
        assert_eq!(
            run("ts_of_max_over_time(m[60s])"),
            vec![("{}".into(), 30.0)]
        );
        assert_eq!(run("ts_of_min_over_time(m[60s])"), vec![("{}".into(), 0.0)]);
        // A single sample: every timestamp form agrees.
        let one = synthetic("m", &[(5_000, 7.0)]);
        for query in [
            "first_over_time(m[60s])",
            "ts_of_first_over_time(m[60s])",
            "ts_of_last_over_time(m[60s])",
            "ts_of_max_over_time(m[60s])",
            "ts_of_min_over_time(m[60s])",
        ] {
            let got = eval_synthetic(one.clone(), "m", query, 5_000);
            let (want, labels) = if query.starts_with("first_over") {
                // It keeps `__name__`, unlike the `ts_of_*` forms.
                (7.0, "{__name__=\"m\"}")
            } else {
                (5.0, "{}")
            };
            assert_eq!(got, vec![(labels.to_string(), want)], "{query}");
        }
    }

    #[test]
    fn test_first_and_last_over_time_keep_the_metric_name() {
        // Prometheus keeps `__name__` for `first_over_time` and
        // `last_over_time` because they act like `offset`; every other range
        // function drops it.
        let series = synthetic("m", &[(0, 1.0), (10_000, 2.0)]);
        let run =
            |query: &str| rendered(&eval_series(vec![series.clone()], "m", query, 10_000).vector);
        // A raw range selector: `__name__` comes from the series' metric name.
        assert_eq!(
            run("first_over_time(m[60s])"),
            vec![("{__name__=\"m\"}".to_string(), 1.0)]
        );
        assert_eq!(
            run("last_over_time(m[60s])"),
            vec![("{__name__=\"m\"}".to_string(), 2.0)]
        );
        // A subquery: the inner instant evaluation already carries it.
        assert_eq!(
            run("first_over_time(m[60s:10s])"),
            vec![("{__name__=\"m\"}".to_string(), 1.0)]
        );
        assert_eq!(
            run("last_over_time(m[60s:10s])"),
            vec![("{__name__=\"m\"}".to_string(), 2.0)]
        );
        // The timestamp forms are derived values, so they still drop it, and
        // so does every other range function.
        assert_eq!(
            run("ts_of_last_over_time(m[60s:10s])"),
            vec![("{}".to_string(), 10.0)]
        );
        assert_eq!(
            run("count_over_time(m[60s])"),
            vec![("{}".to_string(), 2.0)]
        );
    }

    #[test]
    fn test_ts_of_max_over_time_handles_nan() {
        // The predicate is `cur >= best || math.IsNaN(best)`. Go's `NaN >= x`
        // is always false, so a NaN sample never wins on its own — it only
        // matters that a NaN *best* makes the next value win. With 10, NaN, 5
        // the running best stays 10 the whole way, so the answer is its
        // timestamp.
        let series = synthetic("m", &[(0, 10.0), (10_000, f64::NAN), (20_000, 5.0)]);
        assert_eq!(
            eval_synthetic(series, "m", "ts_of_max_over_time(m[60s])", 20_000),
            vec![("{}".to_string(), 0.0)]
        );
        // Once the best is NaN, the next value replaces it: NaN, 5 → 5's time.
        let recovered = synthetic("m", &[(0, f64::NAN), (10_000, 5.0)]);
        assert_eq!(
            eval_synthetic(recovered, "m", "ts_of_max_over_time(m[60s])", 10_000),
            vec![("{}".to_string(), 10.0)]
        );
        // A lone trailing NaN does not win (`NaN >= best` is false), so the
        // earlier extreme's timestamp stays.
        let trailing = synthetic("m", &[(0, 10.0), (10_000, f64::NAN)]);
        assert_eq!(
            eval_synthetic(trailing, "m", "ts_of_max_over_time(m[60s])", 10_000),
            vec![("{}".to_string(), 0.0)]
        );
    }

    #[test]
    fn test_max_of_and_min_of_pick_a_scalar() {
        let run = |query: &str| eval_series(Vec::new(), "m", query, 0).scalar;
        // Plain numbers are scalars, so no series is needed.
        assert_eq!(run("max_of(1, 2)"), Some(2.0));
        assert_eq!(run("max_of(2, 1)"), Some(2.0));
        assert_eq!(run("min_of(1, 2)"), Some(1.0));
        assert_eq!(run("min_of(2, 1)"), Some(1.0));
        assert_eq!(run("max_of(-1, -2)"), Some(-1.0));
        // ±Inf compare normally.
        assert_eq!(run("max_of(-Inf, 1)"), Some(1.0));
        assert_eq!(run("min_of(1, Inf)"), Some(1.0));
        // NaN only survives when both sides are NaN: a single NaN is
        // replaced by the other operand, exactly like `funcMaxOf`'s
        // `if a < b || math.IsNaN(a) { a = b }`.
        assert_eq!(run("max_of(NaN, 2)"), Some(2.0));
        assert_eq!(run("max_of(2, NaN)"), Some(2.0));
        assert_eq!(run("min_of(NaN, 2)"), Some(2.0));
        assert_eq!(run("min_of(2, NaN)"), Some(2.0));
        assert!(run("max_of(NaN, NaN)").unwrap().is_nan());
        assert!(run("min_of(NaN, NaN)").unwrap().is_nan());
        // The result is a scalar, not a vector: a duration expression like
        // `max_of(step(), 5s)` is what these exist for.
        assert!(eval_series(Vec::new(), "m", "max_of(1, 2)", 0)
            .vector
            .0
            .is_empty());
        // A vector operand is read as its sole sample, as `clamp` does; two
        // series are an error rather than a silent pick.
        let one = synthetic("m", &[(0, 7.0)]);
        assert_eq!(
            eval_series(vec![one.clone()], "m", "max_of(m, 5)", 0).scalar,
            Some(7.0)
        );
        let message = parse_err("max_of(m, 5, 6)");
        assert!(message.contains("expected RParen"));
    }

    #[test]
    fn test_clamp_skips_an_inverted_range() {
        let series = synthetic_labeled("m", &[("job", "api")], &[(0, 5.0)]);
        let run = |query: &str| eval_synthetic(series.clone(), "m", query, 0);
        // Inside the range the value is untouched, outside it is pinned; the
        // metric name is dropped like every derived value.
        assert_eq!(
            run("clamp(m, 3, 4)"),
            vec![("{job=\"api\"}".to_string(), 4.0)]
        );
        assert_eq!(
            run("clamp(m, 6, 9)"),
            vec![("{job=\"api\"}".to_string(), 6.0)]
        );
        assert_eq!(
            run("clamp(m, 1, 9)"),
            vec![("{job=\"api\"}".to_string(), 5.0)]
        );
        // `max < min` is an empty result, not the untouched input.
        assert!(run("clamp(m, 9, 1)").is_empty());
        // NaN propagates through Go's math.Min/math.Max, so the sample stays
        // NaN instead of becoming a bound.
        let nan = synthetic("m", &[(0, f64::NAN)]);
        assert!(eval_synthetic(nan, "m", "clamp(m, 1, 9)", 0)[0].1.is_nan());
    }

    #[test]
    fn test_scalar_counts_float_samples() {
        let one = synthetic("m", &[(0, 7.0)]);
        assert_eq!(
            eval_series(vec![one], "m", "scalar(m)", 0).scalar,
            Some(7.0)
        );
        // No series and two series both yield NaN, per `funcScalar`.
        assert!(eval_series(Vec::new(), "m", "scalar(m)", 0)
            .scalar
            .unwrap()
            .is_nan());
        let two = vec![synthetic("m", &[(0, 1.0)]), synthetic("m", &[(0, 2.0)])];
        assert!(eval_series(two, "m", "scalar(m)", 0)
            .scalar
            .unwrap()
            .is_nan());
        // A scalar argument is already a single sample.
        assert_eq!(
            eval_series(Vec::new(), "m", "scalar(3)", 0).scalar,
            Some(3.0)
        );
    }

    #[test]
    fn test_deriv_is_a_least_squares_slope() {
        let samples = [
            SamplePoint {
                timestamp: 0,
                value: 2.0,
            },
            SamplePoint {
                timestamp: 1_000,
                value: 4.0,
            },
            SamplePoint {
                timestamp: 2_000,
                value: 1.0,
            },
        ];
        // The same fit `predict_linear` uses: slope -0.5 per second.
        let slope = apply_range_func(RangeFunc::Deriv, &samples, 3_000, None, None).unwrap();
        assert!((slope + 0.5).abs() < 1e-9, "got {slope}");
        // A constant series is exactly zero, not floating-point noise.
        let flat = [
            SamplePoint {
                timestamp: 0,
                value: 3.0,
            },
            SamplePoint {
                timestamp: 1_000,
                value: 3.0,
            },
        ];
        assert_eq!(
            apply_range_func(RangeFunc::Deriv, &flat, 3_000, None, None),
            Some(0.0)
        );
        // One sample has no slope at all, so the series disappears.
        assert_eq!(
            apply_range_func(RangeFunc::Deriv, &samples[..1], 3_000, None, None),
            None
        );
    }

    #[test]
    fn test_deriv_matches_predict_linear_slope() {
        let series = synthetic("m", &[(0, 10.0), (10_000, 20.0), (20_000, 30.0)]);
        let slope = eval_synthetic(series.clone(), "m", "deriv(m[60s])", 20_000);
        assert_eq!(slope.len(), 1);
        assert!((slope[0].1 - 1.0).abs() < 1e-12, "got {}", slope[0].1);
        // Two seconds ahead of the last sample on the same line: 32.
        let predicted = eval_synthetic(series, "m", "predict_linear(m[60s], 2)", 20_000);
        assert!((predicted[0].1 - 32.0).abs() < 1e-9);
    }

    #[test]
    fn test_date_functions_read_sample_values_as_unix_seconds() {
        // 2024-02-29T13:45:00Z — a leap day, so `days_in_month` and
        // `day_of_year` are both exercised.
        let leap = 1_709_214_300.0;
        let series = synthetic_labeled("m", &[("job", "api")], &[(0, leap)]);
        let run = |query: &str| eval_synthetic(series.clone(), "m", query, 0);
        assert_eq!(run("year(m)"), vec![("{job=\"api\"}".into(), 2024.0)]);
        assert_eq!(run("month(m)"), vec![("{job=\"api\"}".into(), 2.0)]);
        assert_eq!(run("day_of_month(m)"), vec![("{job=\"api\"}".into(), 29.0)]);
        // Thursday, with Sunday as 0 as in Go's `time.Weekday`.
        assert_eq!(run("day_of_week(m)"), vec![("{job=\"api\"}".into(), 4.0)]);
        // 31 + 29 = 60.
        assert_eq!(run("day_of_year(m)"), vec![("{job=\"api\"}".into(), 60.0)]);
        assert_eq!(
            run("days_in_month(m)"),
            vec![("{job=\"api\"}".into(), 29.0)]
        );
        assert_eq!(run("hour(m)"), vec![("{job=\"api\"}".into(), 13.0)]);
        assert_eq!(run("minute(m)"), vec![("{job=\"api\"}".into(), 45.0)]);
        // Without an argument the evaluation instant is used: 0 is the epoch,
        // a Thursday in January 1970.
        let at_zero = synthetic_labeled("m", &[("job", "api")], &[(0, leap)]);
        assert_eq!(
            eval_synthetic(at_zero, "m", "year()", 0),
            vec![("{}".to_string(), 1970.0)]
        );
        // Non-finite values have no date: they are read as the epoch instead of
        // hitting the unspecified integer conversion.
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let odd = synthetic_labeled("m", &[("job", "api")], &[(0, value)]);
            assert_eq!(
                eval_synthetic(odd, "m", "year(m)", 0),
                vec![("{job=\"api\"}".to_string(), 1970.0)]
            );
        }
        // An absurd timestamp is clamped rather than overflowing the calendar
        // arithmetic.
        let huge = synthetic_labeled("m", &[("job", "api")], &[(0, 1e30)]);
        let year = eval_synthetic(huge, "m", "year(m)", 0)[0].1;
        assert!(year.is_finite() && year > 1_000_000.0, "got {year}");
    }

    #[test]
    fn test_quantile_aggregation_groups_and_interpolates() {
        let series = vec![
            synthetic_labeled("m", &[("job", "api")], &[(0, 1.0)]),
            synthetic_labeled("m", &[("job", "api")], &[(0, 2.0)]),
            synthetic_labeled("m", &[("job", "db")], &[(0, 10.0)]),
            synthetic_labeled("m", &[("job", "db")], &[(0, 20.0)]),
        ];
        let run = |query: &str| rendered(&eval_series(series.clone(), "m", query, 0).vector);
        // Without `by` every sample is one group: [1, 2, 10, 20] interpolates
        // to 6 at the median.
        assert_eq!(run("quantile(0.5, m)"), vec![("{}".to_string(), 6.0)]);
        assert_eq!(
            run("quantile by (job) (0.5, m)"),
            vec![
                ("{job=\"api\"}".to_string(), 1.5),
                ("{job=\"db\"}".to_string(), 15.0)
            ]
        );
        // Out-of-range quantiles follow Prometheus: ±Inf, NaN for NaN.
        assert_eq!(
            run("quantile by (job) (2, m)"),
            vec![
                ("{job=\"api\"}".to_string(), f64::INFINITY),
                ("{job=\"db\"}".to_string(), f64::INFINITY)
            ]
        );
        assert_eq!(
            run("quantile by (job) (-1, m)"),
            vec![
                ("{job=\"api\"}".to_string(), f64::NEG_INFINITY),
                ("{job=\"db\"}".to_string(), f64::NEG_INFINITY)
            ]
        );
        assert!(run("quantile by (job) (0.0/0.0, m)")[0].1.is_nan());
        // The quantile is a required parameter, and it comes before the vector.
        assert!(parse_err("quantile(m)").contains("expected Comma but found"));
    }

    #[test]
    fn test_unsupported_constructs_are_named() {
        // Each message must say what is missing, not just "invalid".
        assert!(parse_err("nonsense_func(a)").contains("unknown function"));
        assert!(parse_err("rate(a)").contains("range vector"));
        assert!(parse_err("a{").contains("label name"));
        assert!(parse_err("").contains("empty"));
        // Set operators cannot be given grouping.
        assert!(parse_err("a and on(x) group_left b").contains("no grouping allowed"));
        // Prometheus' `fill` family is named rather than misparsed.
        assert!(parse_err("a + fill(0) b").contains("`fill` vector matching"));
    }

    #[test]
    fn test_subquery_parse() {
        // A plain range selector stays a `Range`, with `[5m:]` and `[5m:1m]`
        // becoming subqueries.
        assert!(matches!(parse_ok("a[5m]"), Expr::Range { .. }));
        match parse_ok("a[5m:1m]") {
            Expr::Subquery {
                range_ms, step_ms, ..
            } => {
                assert_eq!(range_ms, 5 * 60 * 1_000);
                assert_eq!(step_ms, 60 * 1_000);
            }
            other => panic!("a[5m:1m] parsed as {other:?}"),
        }
        match parse_ok("sum_over_time(a[5m:])") {
            Expr::RangeFunc { expr, .. } => {
                assert!(matches!(*expr, Expr::Subquery { step_ms: 0, .. }))
            }
            other => panic!("sum_over_time(a[5m:]) parsed as {other:?}"),
        }
        // Bare seconds and an offset after the subquery, as in Prometheus.
        match parse_ok("sum_over_time(a[30:10] offset 3s)") {
            Expr::RangeFunc { expr, .. } => match *expr {
                Expr::Subquery {
                    range_ms,
                    step_ms,
                    offset_ms,
                    ..
                } => {
                    assert_eq!((range_ms, step_ms, offset_ms), (30_000, 10_000, 3_000));
                }
                other => panic!("expected a subquery, got {other:?}"),
            },
            other => panic!("unexpected shape: {other:?}"),
        }
        // `@` on a subquery pins its evaluation instant.
        assert!(matches!(
            parse_ok("rate(a[5m:1m] @ 100)"),
            Expr::RangeFunc { .. }
        ));
        // Nested subqueries are allowed; a subquery of a range selector or of a
        // scalar is not.
        parse_ok("rate(sum_over_time(a[30s:10s])[50s:10s])");
        assert!(parse_err("5[5m:1m]").contains("instant vector, got scalar"));
        assert!(parse_err("(a[5m])[1m:1m]").contains("instant vector, got range vector"));
        assert!(parse_err("a[5m:0s]").contains("step must be positive"));
        assert!(parse_err("a[5m:1m][1m:1m]").contains("only be applied once"));
        // `offset` still needs a selector or a subquery, not a binary
        // expression.
        assert!(parse_err("(a + b) offset 5m").contains("selector or a subquery"));
    }

    /// One series with `samples`, ready for an `Evaluator` that never touches
    /// a backend.
    fn synthetic(metric: &str, samples: &[(i64, f64)]) -> SeriesSamples {
        SeriesSamples {
            metric_name: metric.as_bytes().to_vec(),
            labels: Vec::new(),
            samples: samples
                .iter()
                .map(|(timestamp, value)| SamplePoint {
                    timestamp: *timestamp,
                    value: *value,
                })
                .collect(),
        }
    }

    /// Evaluates `query` over one synthetic series at `at_ms`.
    fn eval_synthetic(
        series: SeriesSamples,
        selector_query: &str,
        query: &str,
        at_ms: i64,
    ) -> Vec<(String, f64)> {
        let mut fetched = HashMap::new();
        fetched.insert(selector_key(&selector(selector_query)), vec![series]);
        let expr = parse_ok(query);
        let evaluator = Evaluator {
            fetched: &fetched,
            query_start_ms: at_ms,
            query_end_ms: at_ms,
            step_ms: 0,
        };
        rendered(
            &evaluator
                .eval(&expr, at_ms)
                .unwrap_or_else(|e| panic!("{query:?} failed: {e:?}"))
                .vector,
        )
    }

    /// Evaluates `query` over one synthetic series and returns the error
    /// message, panicking if it unexpectedly succeeds.
    fn eval_err(series: SeriesSamples, selector_query: &str, query: &str, at_ms: i64) -> String {
        let mut fetched = HashMap::new();
        fetched.insert(selector_key(&selector(selector_query)), vec![series]);
        let expr = parse_ok(query);
        let evaluator = Evaluator {
            fetched: &fetched,
            query_start_ms: at_ms,
            query_end_ms: at_ms,
            step_ms: 0,
        };
        match evaluator.eval(&expr, at_ms) {
            Ok(instant) => panic!("{query:?} unexpectedly succeeded: {instant:?}"),
            Err(Error::Bad(message)) => message,
            Err(other) => panic!("{query:?} failed with an unexpected error: {other:?}"),
        }
    }

    /// Evaluates `query` over several synthetic series and returns the whole
    /// instant, so scalar results are visible too.
    fn eval_series(
        series: Vec<SeriesSamples>,
        selector_query: &str,
        query: &str,
        at_ms: i64,
    ) -> Instant {
        let mut fetched = HashMap::new();
        fetched.insert(selector_key(&selector(selector_query)), series);
        let expr = parse_ok(query);
        let evaluator = Evaluator {
            fetched: &fetched,
            query_start_ms: at_ms,
            query_end_ms: at_ms,
            step_ms: 0,
        };
        evaluator
            .eval(&expr, at_ms)
            .unwrap_or_else(|e| panic!("{query:?} failed: {e:?}"))
    }

    #[test]
    fn test_subquery_alignment_and_range_functions() {
        // The scratch data from Prometheus' `subquery.test`: samples at 0s and
        // 10s only.
        let total = || synthetic("metric_total", &[(0, 1.0), (10_000, 2.0)]);
        let run = |query: &str, at: i64| eval_synthetic(total(), "metric_total", query, at);
        // The grid starts strictly after `at - range`; points before the first
        // sample contribute nothing.
        assert_eq!(
            run("sum_over_time(metric_total[50s:10s])", 10_000),
            vec![("{}".to_string(), 3.0)]
        );
        assert_eq!(
            run("sum_over_time(metric_total[50s:5s])", 10_000),
            vec![("{}".to_string(), 4.0)]
        );
        // Five grid points at 260..300, each seeing the last value (2).
        assert_eq!(
            run("sum_over_time(metric_total[50s:10s])", 300_000),
            vec![("{}".to_string(), 10.0)]
        );
        // `rate` uses the grid timestamps: (2 - 1) over the 10s span.
        let rate = run("rate(metric_total[20s:10s])", 10_000);
        assert_eq!(rate.len(), 1);
        assert!((rate[0].1 - 0.1).abs() < 1e-12, "got {rate:?}");
        // Grid points 5,10,15,20 with values 1,2,2,2: delta 1 over 15s.
        let rate = run("rate(metric_total[20s:5s])", 20_000);
        assert_eq!(rate.len(), 1);
        assert!((rate[0].1 - 1.0 / 15.0).abs() < 1e-12, "got {rate:?}");
    }

    #[test]
    fn test_subquery_offset_and_default_step() {
        // `metric1_total 0+1x1000` at 10s: value = t / 10s, up to 1000s.
        let samples: Vec<(i64, f64)> = (0..=100).map(|i| (i as i64 * 10_000, i as f64)).collect();
        let metric = || synthetic("metric1_total", &samples);
        let run = |query: &str, at: i64| eval_synthetic(metric(), "metric1_total", query, at);
        // Window (970, 1000]: 980, 990, 1000 → 98 + 99 + 100.
        assert_eq!(
            run("sum_over_time(metric1_total[30s:10s])", 1_000_000),
            vec![("{}".to_string(), 297.0)]
        );
        // The offset shifts the grid to absolute multiples of the step, so
        // these all see the same three points.
        for offset in ["10s", "9s", "7s", "5s", "3s"] {
            let query = format!("sum_over_time(metric1_total[30s:10s] offset {offset})");
            assert_eq!(
                run(&query, 1_010_000),
                vec![("{}".to_string(), 297.0)],
                "{query}"
            );
        }
        // Bare seconds and a parenthesised inner expression parse to the same
        // query.
        assert_eq!(
            run("sum_over_time((metric1_total)[30:10] offset 3)", 1_010_000),
            vec![("{}".to_string(), 297.0)]
        );
        // `[3m:]` uses the default one-minute step: 60, 120, 180.
        assert_eq!(
            run("count_over_time(metric1_total[3m:])", 180_000),
            vec![("{}".to_string(), 3.0)]
        );
        // An `@` pins the subquery even when the parent instant is far away.
        assert_eq!(
            run("sum_over_time(metric1_total[30s:10s] @ 1000)", 5_000_000),
            vec![("{}".to_string(), 297.0)]
        );
    }

    #[test]
    fn test_nested_subquery() {
        let samples: Vec<(i64, f64)> = (0..=100).map(|i| (i as i64 * 10_000, i as f64)).collect();
        let series = synthetic("metric1_total", &samples);
        // From `subquery.test`: rate over the sum over 30s, both sampled every
        // 10s, is 0.3.
        let result = eval_synthetic(
            series,
            "metric1_total",
            "rate(sum_over_time(metric1_total[30s:10s])[50s:10s])",
            1_000_000,
        );
        assert_eq!(result.len(), 1);
        assert!((result[0].1 - 0.3).abs() < 1e-12, "got {result:?}");
    }

    #[test]
    fn test_subquery_scan_window() {
        // The scan must reach `at - range - step` (the first grid point, with
        // one step of alignment slack) minus the lookback, so every grid
        // point's selector has all the samples it can use.
        let expr = parse_ok("sum_over_time(metric1_total[5m:1m])");
        let selectors = {
            let mut out = Vec::new();
            expr.selectors(&mut out);
            out
        };
        assert_eq!(selectors.len(), 1);
        assert_eq!(selectors[0].metric, "metric1_total");
        let (start, end) = expr.scan_window(10_000, 10_000);
        assert_eq!(start, 10_000 - 300_000 - 60_000 - LOOKBACK_MS);
        assert_eq!(end, 10_001);
    }

    #[test]
    fn test_group_key_by_and_without() {
        let labels: BTreeMap<String, String> = [
            ("__name__".to_string(), "m".to_string()),
            ("job".to_string(), "api".to_string()),
            ("instance".to_string(), "a".to_string()),
        ]
        .into_iter()
        .collect();
        // No `by`/`without` aggregates over all label dimensions: one group
        // whose output labelset is empty.
        assert_eq!(group_key(&labels, None), Vec::new());
        assert_eq!(
            group_key(&labels, Some(&Grouping::By(vec!["job".into()]))),
            vec![("job".to_string(), "api".to_string())]
        );
        // `without` keeps everything except the named labels and `__name__`.
        assert_eq!(
            group_key(&labels, Some(&Grouping::Without(vec!["instance".into()]))),
            vec![("job".to_string(), "api".to_string())]
        );
        // An empty `without ()` drops only `__name__`, so it groups per
        // labelset — unlike the default above.
        assert_eq!(
            group_key(&labels, Some(&Grouping::Without(Vec::new()))),
            vec![
                ("instance".to_string(), "a".to_string()),
                ("job".to_string(), "api".to_string())
            ]
        );
    }

    #[test]
    fn test_default_aggregation_collapses_to_one_series() {
        // Prometheus' `sum(m)` with no `by`/`without` aggregates over every
        // label dimension and returns a single `{}` series; `without ()` is a
        // different thing (it keeps all labels but `__name__`).
        let series = vec![
            synthetic_labeled("m", &[("job", "api")], &[(0, 1.0), (1_000, 2.0)]),
            synthetic_labeled("m", &[("job", "db")], &[(0, 10.0)]),
        ];
        let run = |query: &str| rendered(&eval_series(series.clone(), "m", query, 2_000).vector);
        // The latest sample of each series: 2 + 10.
        assert_eq!(run("sum(m)"), vec![("{}".to_string(), 12.0)]);
        assert_eq!(run("count(m)"), vec![("{}".to_string(), 2.0)]);
        assert_eq!(
            run("count(m) without ()"),
            vec![
                ("{job=\"api\"}".to_string(), 1.0),
                ("{job=\"db\"}".to_string(), 1.0)
            ]
        );
        assert_eq!(
            run("sum by (job) (m)"),
            vec![
                ("{job=\"api\"}".to_string(), 2.0),
                ("{job=\"db\"}".to_string(), 10.0)
            ]
        );
    }

    #[test]
    fn test_count_values_counts_distinct_values() {
        // Prometheus' `count_values("v", m)`: one output series per distinct
        // sample value, with the value rendered into the given label and the
        // count as the sample value. The grouping key includes that label, so
        // without `by`/`without` the only output label is the value label.
        let series = vec![
            synthetic_labeled("m", &[("job", "api")], &[(0, 1.0)]),
            synthetic_labeled("m", &[("job", "api")], &[(0, 1.0)]),
            synthetic_labeled("m", &[("job", "db")], &[(0, 2.0)]),
        ];
        let run = |query: &str| rendered(&eval_series(series.clone(), "m", query, 0).vector);
        assert_eq!(
            run("count_values(\"v\", m)"),
            vec![
                ("{v=\"1\"}".to_string(), 2.0),
                ("{v=\"2\"}".to_string(), 1.0)
            ]
        );
        // `by (job)` appends the value label to the grouping list.
        assert_eq!(
            run("count_values by (job) (\"v\", m)"),
            vec![
                ("{job=\"api\",v=\"1\"}".to_string(), 2.0),
                ("{job=\"db\",v=\"2\"}".to_string(), 1.0)
            ]
        );
        // `without (...)` keeps the other labels and the value label.
        assert_eq!(
            run("count_values without (job) (\"v\", m)"),
            vec![
                ("{v=\"1\"}".to_string(), 2.0),
                ("{v=\"2\"}".to_string(), 1.0)
            ]
        );
        // The value label must be a quoted string and a valid label name.
        assert!(parse_err("count_values(v, m)").contains("quoted string"));
        assert!(parse_err("count_values(\"1v\", m)").contains("invalid label name"));
        // A string literal is not an expression on its own.
        assert!(parse_err("\"v\"").contains("expected a metric name"));
    }

    #[test]
    fn test_count_values_renders_values_like_go() {
        // Go's `strconv.FormatFloat(v, 'f', -1, 64)` never uses exponent
        // notation and prints `NaN` / `+Inf` / `-Inf`.
        assert_eq!(format_float_g(1.0), "1");
        assert_eq!(format_float_g(1.5), "1.5");
        assert_eq!(format_float_g(-2.0), "-2");
        assert_eq!(format_float_g(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(format_float_g(1e-10), "0.0000000001");
        assert_eq!(format_float_g(f64::NAN), "NaN");
        assert_eq!(format_float_g(f64::INFINITY), "+Inf");
        assert_eq!(format_float_g(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn test_aggregate_values() {
        let values = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(aggregate_values(AggOp::Sum, &values), 10.0);
        assert_eq!(aggregate_values(AggOp::Avg, &values), 2.5);
        assert_eq!(aggregate_values(AggOp::Min, &values), 1.0);
        assert_eq!(aggregate_values(AggOp::Max, &values), 4.0);
        assert_eq!(aggregate_values(AggOp::Count, &values), 4.0);
        assert_eq!(aggregate_values(AggOp::Group, &values), 1.0);
        // Population variance of 1,2,3,4 is 1.25.
        assert!((aggregate_values(AggOp::Stdvar, &values) - 1.25).abs() < 1e-12);
        assert!((aggregate_values(AggOp::Stddev, &values) - 1.25f64.sqrt()).abs() < 1e-12);
    }

    #[test]
    fn test_counter_functions_handle_resets() {
        let samples = [
            SamplePoint {
                timestamp: 0,
                value: 10.0,
            },
            SamplePoint {
                timestamp: 1_000,
                value: 20.0,
            },
            // Counter reset: 5 < 20, so Prometheus adds the post-reset value.
            SamplePoint {
                timestamp: 2_000,
                value: 5.0,
            },
            SamplePoint {
                timestamp: 3_000,
                value: 7.0,
            },
        ];
        assert_eq!(
            apply_range_func(RangeFunc::Increase, &samples, 4_000, None, None),
            Some(17.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::Delta, &samples, 4_000, None, None),
            Some(-3.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::Changes, &samples, 4_000, None, None),
            Some(3.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::Resets, &samples, 4_000, None, None),
            Some(1.0)
        );
        // Rate of the increase over three seconds.
        let rate = apply_range_func(RangeFunc::Rate, &samples, 4_000, None, None).unwrap();
        assert!((rate - 17.0 / 3.0).abs() < 1e-12);
        assert_eq!(
            apply_range_func(RangeFunc::LastOverTime, &samples, 4_000, None, None),
            Some(7.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::CountOverTime, &samples, 4_000, None, None),
            Some(4.0)
        );
    }

    #[test]
    fn test_bucket_fraction_interpolates_classic_buckets() {
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        // 10/20/30/40 observations up to 0.1/0.5/1/+Inf: 25 % of them lie in
        // each bucket, so each quarter span holds a quarter of the mass.
        let buckets = vec![(f64::INFINITY, 40.0), (0.5, 20.0), (0.1, 10.0), (1.0, 30.0)];
        assert!(approx(
            bucket_fraction(0.1, 0.5, &mut buckets.clone()),
            0.25
        ));
        assert!(approx(
            bucket_fraction(0.0, 1.0, &mut buckets.clone()),
            0.75
        ));
        assert!(approx(
            bucket_fraction(0.0, 0.1, &mut buckets.clone()),
            0.25
        ));
        // Inside the first bucket the lower implicit bound is 0, so 0.05..0.25
        // mixes the first bucket's interior with the second bucket's start.
        assert!(approx(
            bucket_fraction(0.05, 0.25, &mut buckets.clone()),
            0.21875
        ));
        // The whole distribution is 1, including the `+Inf` tail.
        assert!(approx(
            bucket_fraction(f64::NEG_INFINITY, f64::INFINITY, &mut buckets.clone()),
            1.0
        ));
        // Everything above the last finite bound is the `+Inf` bucket alone.
        assert!(approx(
            bucket_fraction(1.0, f64::INFINITY, &mut buckets.clone()),
            0.25
        ));
        // A range past the data holds nothing.
        assert_eq!(bucket_fraction(2.0, 3.0, &mut buckets.clone()), 0.0);
        // Bounds that are equal or inverted hold nothing — Prometheus answers
        // 0 rather than an error.
        assert_eq!(bucket_fraction(0.5, 0.5, &mut buckets.clone()), 0.0);
        assert_eq!(bucket_fraction(0.6, 0.4, &mut buckets.clone()), 0.0);
        // A NaN bound answers NaN.
        assert!(bucket_fraction(f64::NAN, 1.0, &mut buckets.clone()).is_nan());
        assert!(bucket_fraction(0.0, f64::NAN, &mut buckets.clone()).is_nan());
        // All-negative buckets get an implicit -Inf lower bound instead of 0.
        let negative = vec![(-1.0, 5.0), (0.0, 10.0), (f64::INFINITY, 20.0)];
        assert!(approx(
            bucket_fraction(f64::NEG_INFINITY, 0.0, &mut negative.clone()),
            0.5
        ));
        assert!(approx(
            bucket_fraction(-0.5, -0.25, &mut negative.clone()),
            0.0625
        ));
        // Equal bounds coalesce; unlike `bucket_quantile`, a decreasing count
        // is *not* clamped (Prometheus' `BucketFraction` has no monotonicity
        // fix), so this stays a plain cumulative read.
        let mut duplicate = vec![(0.5, 1.0), (0.5, 3.0), (f64::INFINITY, 4.0)];
        assert!(approx(bucket_fraction(0.0, 0.5, &mut duplicate), 1.0));
        let mut decreasing = vec![(0.1, 5.0), (0.5, 3.0), (f64::INFINITY, 5.0)];
        assert!(approx(
            bucket_fraction(f64::NEG_INFINITY, f64::INFINITY, &mut decreasing),
            1.0
        ));
        // Inputs that cannot describe a distribution answer NaN.
        let mut no_inf = vec![(0.1, 1.0), (1.0, 2.0)];
        assert!(bucket_fraction(0.0, 1.0, &mut no_inf).is_nan());
        let mut no_observations = vec![(0.5, 0.0), (f64::INFINITY, 0.0)];
        assert!(bucket_fraction(0.0, 1.0, &mut no_observations).is_nan());
        assert!(bucket_fraction(0.0, 1.0, &mut []).is_nan());
    }

    #[test]
    fn test_histogram_fraction_groups_buckets_per_histogram() {
        let bucket = |job: &str, le: &str, value: f64| Point {
            labels: BTreeMap::from([
                ("__name__".to_string(), "latency_bucket".to_string()),
                ("job".to_string(), job.to_string()),
                ("le".to_string(), le.to_string()),
            ]),
            value,
            sample_ts: None,
        };
        let mut vector = Vector::default();
        // `api`: cumulative 2/4/6/8 observations, `db`: 20/40/60/80 — ten
        // times the sample count in the same shape, so both fractions are
        // exactly 0.5 at an upper bound of 0.5.
        for (job, scale) in [("api", 1.0), ("db", 10.0)] {
            vector.0.push(bucket(job, "0.1", 2.0 * scale));
            vector.0.push(bucket(job, "0.5", 4.0 * scale));
            vector.0.push(bucket(job, "1", 6.0 * scale));
            vector.0.push(bucket(job, "+Inf", 8.0 * scale));
        }
        let out = histogram_fraction(0.0, 0.5, &vector).expect("both histograms are complete");
        assert_eq!(out.0.len(), 2);
        let value = |job: &str| {
            out.0
                .iter()
                .find(|point| point.labels.get("job") == Some(&job.to_string()))
                .unwrap_or_else(|| panic!("missing {job}"))
                .value
        };
        // Half of each distribution is at or below 0.5.
        assert!((value("api") - 0.5).abs() < 1e-9);
        assert!((value("db") - 0.5).abs() < 1e-9);
        for point in &out.0 {
            assert!(!point.labels.contains_key("__name__"));
            assert!(!point.labels.contains_key("le"));
        }
        // A series without `le` is refused by name, as for
        // `histogram_quantile`.
        let mut mixed = vector.clone();
        mixed.0.push(Point {
            labels: BTreeMap::from([("job".to_string(), "api".to_string())]),
            value: 0.0,
            sample_ts: None,
        });
        let error = histogram_fraction(0.0, 0.5, &mixed).unwrap_err();
        assert!(error.message().contains("`le` label"));
        assert!(error.message().contains("job=\"api\""));
        // End to end through the evaluator: one `le` series per bucket, 2/4/6/8
        // observations up to 0.1/0.5/1/+Inf.
        let series = vec![
            synthetic_labeled("latency_bucket", &[("le", "0.1")], &[(0, 2.0)]),
            synthetic_labeled("latency_bucket", &[("le", "0.5")], &[(0, 4.0)]),
            synthetic_labeled("latency_bucket", &[("le", "1")], &[(0, 6.0)]),
            synthetic_labeled("latency_bucket", &[("le", "+Inf")], &[(0, 8.0)]),
        ];
        let instant = eval_series(
            series.clone(),
            "latency_bucket",
            "histogram_fraction(0.5, 1, latency_bucket)",
            0,
        );
        // (6 - 4) / 8: the second bucket's mass only.
        assert_eq!(rendered(&instant.vector), vec![("{}".to_string(), 0.25)]);
        let instant = eval_series(
            series,
            "latency_bucket",
            "histogram_fraction(-Inf, +Inf, latency_bucket)",
            0,
        );
        assert_eq!(rendered(&instant.vector), vec![("{}".to_string(), 1.0)]);
    }

    #[test]
    fn test_histogram_stats_over_classic_buckets() {
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        let bucket = |le: &str, value: f64| Point {
            labels: BTreeMap::from([
                ("__name__".to_string(), "latency_bucket".to_string()),
                ("le".to_string(), le.to_string()),
            ]),
            value,
            sample_ts: None,
        };
        let mut vector = Vector::default();
        // Cumulative 2/4/6/8 observations up to 0.1/0.5/1/+Inf, i.e. two
        // observations per finite bucket.
        vector.0.push(bucket("0.1", 2.0));
        vector.0.push(bucket("0.5", 4.0));
        vector.0.push(bucket("1", 6.0));
        vector.0.push(bucket("+Inf", 8.0));
        // Midpoints 0.05, 0.3 and 0.75, two observations each: the mean is
        // (0.1 + 0.6 + 1.5) / 6.
        let avg = histogram_stat(HistogramStatOp::Avg, &vector).unwrap();
        assert_eq!(avg.0.len(), 1);
        assert!(approx(avg.0[0].value, 2.2 / 6.0));
        assert!(!avg.0[0].labels.contains_key("le"));
        assert!(!avg.0[0].labels.contains_key("__name__"));
        // The variance is E[x^2] - E[x]^2 over the same midpoints:
        // (2*0.0025 + 2*0.09 + 2*0.5625)/6 - (2.2/6)^2.
        let variance = (0.005 + 0.18 + 1.125) / 6.0 - (2.2f64 / 6.0).powi(2);
        let stdvar = histogram_stat(HistogramStatOp::Stdvar, &vector).unwrap();
        assert!(approx(stdvar.0[0].value, variance));
        let stddev = histogram_stat(HistogramStatOp::Stddev, &vector).unwrap();
        assert!(approx(stddev.0[0].value, variance.sqrt()));
        // The `+Inf` bucket carries no midpoint, so a histogram made only of
        // it has no weight at all and VM answers NaN.
        let mut infinite = Vector::default();
        infinite.0.push(bucket("+Inf", 8.0));
        assert!(
            histogram_stat(HistogramStatOp::Avg, &infinite).unwrap().0[0]
                .value
                .is_nan()
        );
        // With zero counts in every bucket the total weight is zero and VM
        // answers NaN (a non-empty first bucket would have weight, e.g.
        // 5 → 5 gives just the first interval's midpoint).
        let mut flat = Vector::default();
        flat.0.push(bucket("0.1", 0.0));
        flat.0.push(bucket("0.5", 0.0));
        assert!(histogram_stat(HistogramStatOp::Avg, &flat).unwrap().0[0]
            .value
            .is_nan());
        // A flat but non-zero histogram only weighs the first interval, since
        // no observations were added afterwards: 5 at both bounds means all
        // five are inside `[0, 0.1]`, midpoint 0.05.
        let mut start_only = Vector::default();
        start_only.0.push(bucket("0.1", 5.0));
        start_only.0.push(bucket("0.5", 5.0));
        assert!(approx(
            histogram_stat(HistogramStatOp::Avg, &start_only).unwrap().0[0].value,
            0.05
        ));
        // Two histograms are grouped apart by their non-`le` labels, and a
        // missing `le` is refused by name.
        let labeled = |job: &str, le: &str, value: f64| Point {
            labels: BTreeMap::from([
                ("job".to_string(), job.to_string()),
                ("le".to_string(), le.to_string()),
            ]),
            value,
            sample_ts: None,
        };
        let mut two = Vector::default();
        two.0.push(labeled("api", "1", 2.0));
        two.0.push(labeled("api", "+Inf", 4.0));
        two.0.push(labeled("db", "2", 4.0));
        two.0.push(labeled("db", "+Inf", 8.0));
        let out = histogram_stat(HistogramStatOp::Avg, &two).unwrap();
        assert_eq!(out.0.len(), 2);
        let value = |job: &str| {
            out.0
                .iter()
                .find(|point| point.labels.get("job") == Some(&job.to_string()))
                .unwrap_or_else(|| panic!("missing {job}"))
                .value
        };
        assert!(approx(value("api"), 0.5));
        assert!(approx(value("db"), 1.0));
        let mut mixed = two.clone();
        mixed.0.push(Point {
            labels: BTreeMap::from([("job".to_string(), "api".to_string())]),
            value: 0.0,
            sample_ts: None,
        });
        let error = histogram_stat(HistogramStatOp::Stddev, &mixed).unwrap_err();
        assert!(error.message().contains("`le` label"));
        assert!(error.message().contains("job=\"api\""));

        // End to end through the evaluator.
        let series = vec![
            synthetic_labeled("latency_bucket", &[("le", "0.1")], &[(0, 2.0)]),
            synthetic_labeled("latency_bucket", &[("le", "0.5")], &[(0, 4.0)]),
            synthetic_labeled("latency_bucket", &[("le", "1")], &[(0, 6.0)]),
            synthetic_labeled("latency_bucket", &[("le", "+Inf")], &[(0, 8.0)]),
        ];
        let instant = eval_series(
            series.clone(),
            "latency_bucket",
            "histogram_avg(latency_bucket)",
            0,
        );
        assert_eq!(instant.vector.0.len(), 1);
        assert!(approx(instant.vector.0[0].value, 2.2 / 6.0));
        let instant = eval_series(
            series,
            "latency_bucket",
            "histogram_stddev(latency_bucket)",
            0,
        );
        assert!(approx(instant.vector.0[0].value, variance.sqrt()));
        // A scalar has no buckets to summarise.
        assert!(
            eval_err(synthetic("m", &[(0, 1.0)]), "m", "histogram_avg(1)", 0)
                .contains("histogram_avg needs an instant vector of `le` buckets")
        );
    }

    #[test]
    fn test_histogram_quantile_interpolates_classic_buckets() {
        let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
        // 10/20/30/40 observations up to 0.1/0.5/1/+Inf.
        let buckets = vec![(f64::INFINITY, 40.0), (0.5, 20.0), (0.1, 10.0), (1.0, 30.0)];
        assert!(approx(bucket_quantile(0.5, &mut buckets.clone()), 0.5));
        assert!(approx(bucket_quantile(0.25, &mut buckets.clone()), 0.1));
        assert!(approx(bucket_quantile(0.75, &mut buckets.clone()), 1.0));
        // Into the `+Inf` bucket: the last finite bound, not `+Inf`, since the
        // bucket says nothing about how far past it the observations go.
        assert!(approx(bucket_quantile(0.95, &mut buckets.clone()), 1.0));
        assert!(approx(bucket_quantile(0.0, &mut buckets.clone()), 0.0));
        assert!(bucket_quantile(f64::NAN, &mut buckets.clone()).is_nan());
        assert_eq!(
            bucket_quantile(-1.0, &mut buckets.clone()),
            f64::NEG_INFINITY
        );
        assert_eq!(bucket_quantile(2.0, &mut buckets.clone()), f64::INFINITY);
        // Inputs that cannot describe a distribution answer NaN, as in
        // Prometheus, rather than an invented value.
        let mut no_inf = vec![(0.1, 1.0), (1.0, 2.0)];
        assert!(bucket_quantile(0.5, &mut no_inf).is_nan());
        let mut single = vec![(f64::INFINITY, 5.0)];
        assert!(bucket_quantile(0.5, &mut single).is_nan());
        let mut no_observations = vec![(0.5, 0.0), (f64::INFINITY, 0.0)];
        assert!(bucket_quantile(0.5, &mut no_observations).is_nan());
        // Equal bounds are coalesced and a decreasing count is clamped up;
        // Prometheus makes both corrections silently.
        let mut duplicate = vec![(0.5, 1.0), (0.5, 1.0), (f64::INFINITY, 4.0)];
        assert!(approx(bucket_quantile(0.5, &mut duplicate), 0.5));
        let mut decreasing = vec![(0.1, 5.0), (0.5, 3.0), (f64::INFINITY, 5.0)];
        assert!(approx(bucket_quantile(0.5, &mut decreasing), 0.05));
    }

    #[test]
    fn test_format_open_metrics_float() {
        // Ground truth from Go's `labels.FormatOpenMetricsFloat`.
        assert_eq!(format_open_metrics_float(1.0), "1.0");
        assert_eq!(format_open_metrics_float(0.0), "0.0");
        // `-0` compares equal to `0`, so Go answers "0.0" for it too.
        assert_eq!(format_open_metrics_float(-0.0), "0.0");
        assert_eq!(format_open_metrics_float(-1.0), "-1.0");
        assert_eq!(format_open_metrics_float(f64::NAN), "NaN");
        assert_eq!(format_open_metrics_float(f64::INFINITY), "+Inf");
        assert_eq!(format_open_metrics_float(f64::NEG_INFINITY), "-Inf");
        // The usual quantiles are plain shortest decimals.
        for (value, want) in [
            (0.5, "0.5"),
            (0.9, "0.9"),
            (0.99, "0.99"),
            (0.999, "0.999"),
            (1.5, "1.5"),
            (-0.5, "-0.5"),
            (0.0001, "0.0001"),
        ] {
            assert_eq!(format_open_metrics_float(value), want);
        }
        // Go switches to exponent form at a decimal exponent of 6 and below
        // -4; inside that window the number is written out in full and gets a
        // trailing ".0" when it would otherwise have no point.
        assert_eq!(format_open_metrics_float(100_000.0), "100000.0");
        assert_eq!(format_open_metrics_float(123_456.0), "123456.0");
        assert_eq!(format_open_metrics_float(1e6), "1e+06");
        assert_eq!(format_open_metrics_float(1_234_567.0), "1.234567e+06");
        assert_eq!(format_open_metrics_float(1e-5), "1e-05");
        assert_eq!(format_open_metrics_float(1e-7), "1e-07");
        assert_eq!(format_open_metrics_float(1e21), "1e+21");
        assert_eq!(
            format_open_metrics_float(1.2345678901234569e23),
            "1.2345678901234569e+23"
        );
    }

    #[test]
    fn test_histogram_quantiles_emits_one_series_per_quantile() {
        let bucket = |le: &str, value: f64| Point {
            labels: BTreeMap::from([
                ("__name__".to_string(), "latency_bucket".to_string()),
                ("job".to_string(), "api".to_string()),
                ("le".to_string(), le.to_string()),
            ]),
            value,
            sample_ts: None,
        };
        let mut vector = Vector::default();
        // 2/4/6/8 observations up to 0.1/0.5/1/+Inf.
        for (le, value) in [("0.1", 2.0), ("0.5", 4.0), ("1", 6.0), ("+Inf", 8.0)] {
            vector.0.push(bucket(le, value));
        }
        let out = histogram_quantiles(&[0.5, 0.9], "quantile", &vector)
            .expect("the histogram is complete");
        let rendered_out = rendered(&out);
        assert_eq!(rendered_out.len(), 2);
        // The median sits exactly on the upper bound of the bucket holding
        // rank 4; the 90th percentile falls past every finite bucket, so
        // Prometheus answers the last finite bound.
        assert_eq!(
            rendered_out,
            vec![
                ("{job=\"api\",quantile=\"0.5\"}".to_string(), 0.5),
                ("{job=\"api\",quantile=\"0.9\"}".to_string(), 1.0),
            ]
        );
        // An out-of-range quantile is not an error: Prometheus only warns and
        // still emits the series. The label holds the *requested* quantile
        // (`-0.5`, `1.5`), not the value it produces.
        let extremes = histogram_quantiles(&[-0.5, 1.5, f64::NAN], "q", &vector).unwrap();
        let values: Vec<(String, f64)> = rendered(&extremes);
        assert_eq!(
            values.iter().map(|(l, _)| l.as_str()).collect::<Vec<_>>(),
            vec![
                "{job=\"api\",q=\"-0.5\"}",
                "{job=\"api\",q=\"1.5\"}",
                "{job=\"api\",q=\"NaN\"}",
            ]
        );
        assert_eq!(values[0].1, f64::NEG_INFINITY);
        assert_eq!(values[1].1, f64::INFINITY);
        assert!(values[2].1.is_nan());
        // The quantile label is written with `labels.Builder.Set`, so a label
        // that already exists is overwritten.
        let overwritten = histogram_quantiles(&[0.5], "job", &vector).unwrap();
        assert_eq!(
            rendered(&overwritten),
            vec![("{job=\"0.5\"}".to_string(), 0.5)]
        );
        // End to end through the evaluator: `__name__` is dropped and one
        // series comes out per quantile.
        let series = vec![
            synthetic_labeled("latency_bucket", &[("le", "0.1")], &[(0, 2.0)]),
            synthetic_labeled("latency_bucket", &[("le", "0.5")], &[(0, 4.0)]),
            synthetic_labeled("latency_bucket", &[("le", "1")], &[(0, 6.0)]),
            synthetic_labeled("latency_bucket", &[("le", "+Inf")], &[(0, 8.0)]),
        ];
        let instant = eval_series(
            series,
            "latency_bucket",
            "histogram_quantiles(latency_bucket, \"quantile\", 0.5, 1)",
            0,
        );
        assert_eq!(
            rendered(&instant.vector),
            vec![
                ("{quantile=\"0.5\"}".to_string(), 0.5),
                ("{quantile=\"1.0\"}".to_string(), 1.0),
            ]
        );
        // Bad arguments: no quantile, too many quantiles, an invalid label
        // name, and a scalar input.
        assert!(parse_err("histogram_quantiles(latency_bucket, \"q\")")
            .contains("at least one quantile"));
        let too_many = format!(
            "histogram_quantiles(latency_bucket, \"q\", {})",
            "0.5, ".repeat(10) + "0.5"
        );
        assert!(parse_err(&too_many).contains("at most ten quantiles"));
        assert!(
            parse_err("histogram_quantiles(latency_bucket, \"q x\", 0.5)")
                .contains("invalid label name")
        );
        let error = eval_err(
            synthetic("latency_bucket", &[(0, 1.0)]),
            "latency_bucket",
            "histogram_quantiles(latency_bucket, \"q\", 0.5)",
            0,
        );
        assert!(error.contains("needs a `le` label"));
        let error = eval_err(
            synthetic("m", &[(0, 1.0)]),
            "m",
            "histogram_quantiles(1, \"q\", 0.5)",
            0,
        );
        assert!(error.contains("not a scalar"));
    }

    #[test]
    fn test_histogram_quantile_groups_buckets_per_histogram() {
        let bucket = |job: &str, le: &str, value: f64| Point {
            labels: BTreeMap::from([
                ("__name__".to_string(), "latency_bucket".to_string()),
                ("job".to_string(), job.to_string()),
                ("le".to_string(), le.to_string()),
            ]),
            value,
            sample_ts: None,
        };
        let mut vector = Vector::default();
        // `api`: 2/4/6/8 observations, `db`: 18/20/22/24.
        for (job, base) in [("api", 0.0), ("db", 16.0)] {
            vector.0.push(bucket(job, "0.1", base + 2.0));
            vector.0.push(bucket(job, "0.5", base + 4.0));
            vector.0.push(bucket(job, "1", base + 6.0));
            vector.0.push(bucket(job, "+Inf", base + 8.0));
        }
        let out = histogram_quantile(0.75, &vector).expect("both histograms are complete");
        assert_eq!(out.0.len(), 2);
        let value = |job: &str| {
            out.0
                .iter()
                .find(|point| point.labels.get("job") == Some(&job.to_string()))
                .unwrap_or_else(|| panic!("missing {job}"))
                .value
        };
        // api: rank 6 of 8 sits exactly on the 1 s bound.
        assert!((value("api") - 1.0).abs() < 1e-9);
        // db: rank 18 of 24 sits exactly on the 0.1 s bound (start 0, end 0.1,
        // 18 of 18 observations in the first bucket).
        assert!((value("db") - 0.1).abs() < 1e-9);
        // `__name__` and `le` are dropped; `job` stays as the grouping label.
        for point in &out.0 {
            assert!(!point.labels.contains_key("__name__"));
            assert!(!point.labels.contains_key("le"));
        }
        // A series without `le` is an error naming the series, not silently
        // dropped: a panel that passed `_sum`/`_count` would otherwise show
        // an empty graph.
        let mut mixed = vector.clone();
        mixed.0.push(Point {
            labels: BTreeMap::from([("job".to_string(), "api".to_string())]),
            value: 0.0,
            sample_ts: None,
        });
        let error = histogram_quantile(0.75, &mixed).unwrap_err();
        assert!(error.message().contains("`le` label"));
        assert!(error.message().contains("job=\"api\""));
    }

    #[test]
    fn test_quantile_interpolates() {
        let values = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(quantile_of(&values, 0.0), 1.0);
        assert_eq!(quantile_of(&values, 1.0), 4.0);
        assert_eq!(quantile_of(&values, 0.5), 2.5);
        assert_eq!(quantile_of(&values, 0.25), 1.75);
        assert!(quantile_of(&values, f64::NAN).is_nan());
    }

    #[test]
    fn test_binary_operator_math() {
        assert_eq!(BinaryOp::Add.apply(1.0, 2.0), 3.0);
        assert_eq!(BinaryOp::Mod.apply(7.0, 3.0), 1.0);
        assert_eq!(BinaryOp::Pow.apply(2.0, 10.0), 1024.0);
        assert_eq!(BinaryOp::Div.apply(1.0, 0.0), f64::INFINITY);
        assert_eq!(BinaryOp::Gt.apply(2.0, 1.0), 1.0);
        assert_eq!(BinaryOp::Gt.apply(1.0, 2.0), 0.0);
    }

    /// A point with the given labels and value, for the matching tests.
    fn point(pairs: &[(&str, &str)], value: f64) -> Point {
        Point {
            labels: pairs
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect(),
            value,
            sample_ts: None,
        }
    }

    fn vector(points: Vec<Point>) -> Vector {
        Vector(points)
    }

    /// Renders a result as `label=value` pairs sorted by label set, so a test
    /// can assert on the whole answer without depending on ordering.
    fn rendered(result: &Vector) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = result
            .0
            .iter()
            .map(|point| (label_string(&point.labels), point.value))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The matching modifiers of a parsed binary expression.
    fn matching(query: &str) -> VectorMatching {
        match parse_ok(query) {
            Expr::Binary { matching, .. } => matching,
            other => panic!("{query:?} is not a binary expression: {other:?}"),
        }
    }

    #[test]
    fn test_vector_matching_modifiers_parse() {
        // The unmodified form is one-to-one on everything but `__name__`.
        assert_eq!(matching("a + b"), VectorMatching::default());
        assert!(matching("a + b").is_default());

        let on = matching("a + on(instance) b");
        assert!(on.on);
        assert_eq!(on.matching_labels, vec!["instance".to_string()]);
        assert_eq!(on.card, Cardinality::OneToOne);

        let ignoring = matching("a + ignoring(instance, job) b");
        assert!(!ignoring.on);
        assert_eq!(
            ignoring.matching_labels,
            vec!["instance".to_string(), "job".to_string()]
        );

        // `group_left` / `group_right` come after `on`/`ignoring`, and their
        // label list is optional.
        let group_left = matching("a * on(job) group_left(instance) b");
        assert_eq!(group_left.card, Cardinality::ManyToOne);
        assert_eq!(group_left.include, vec!["instance".to_string()]);
        assert_eq!(matching("a * group_left b").include, Vec::<String>::new());

        let group_right = matching("a / ignoring(code) group_right(instance) b");
        assert_eq!(group_right.card, Cardinality::OneToMany);
        assert_eq!(group_right.include, vec!["instance".to_string()]);

        // `bool` sits before the matching modifiers.
        let bool_cmp = matching("a > bool on(job) b");
        assert!(bool_cmp.on);
        assert_eq!(bool_cmp.matching_labels, vec!["job".to_string()]);
        assert!(matches!(
            parse_ok("a > bool on(job) b"),
            Expr::Binary { ret_bool: true, .. }
        ));

        // Set operators are implicitly many-to-many.
        assert_eq!(matching("a and on(job) b").card, Cardinality::ManyToMany);
        assert_eq!(matching("a or b").card, Cardinality::ManyToMany);

        // An empty grouping list is legal in Prometheus.
        assert!(matching("a + on() b").on);
        assert!(matching("a + on() b").matching_labels.is_empty());

        // Errors name the problem.
        assert!(parse_err("a + on(instance) group_left(instance) b")
            .contains("must not occur in ON and GROUP clause"));
        assert!(parse_err("a and group_left b").contains("no grouping allowed"));
    }

    #[test]
    fn test_one_to_one_matching_drops_or_keeps_labels() {
        // Distinct metric names, same `instance`: `on(instance)` pairs them.
        let lhs = vector(vec![point(
            &[("__name__", "a"), ("instance", "i1"), ("job", "x")],
            2.0,
        )]);
        let rhs = vector(vec![point(
            &[("__name__", "b"), ("instance", "i1"), ("job", "y")],
            8.0,
        )]);

        // `on` keeps only the matching labels in the result.
        let on = VectorMatching {
            on: true,
            matching_labels: vec!["instance".to_string()],
            ..VectorMatching::default()
        };
        let result = binary_vectors(BinaryOp::Div, &lhs, &rhs, false, &on).unwrap();
        assert_eq!(
            rendered(&result),
            vec![("{instance=\"i1\"}".to_string(), 0.25)]
        );

        // `ignoring(job)` keeps the rest of the left labels, minus `__name__`
        // (arithmetic changes the metric's meaning).
        let ignoring = VectorMatching {
            matching_labels: vec!["job".to_string()],
            ..VectorMatching::default()
        };
        let result = binary_vectors(BinaryOp::Div, &lhs, &rhs, false, &ignoring).unwrap();
        assert_eq!(
            rendered(&result),
            vec![("{instance=\"i1\"}".to_string(), 0.25)]
        );
    }

    #[test]
    fn test_unmatched_series_are_dropped() {
        let lhs = vector(vec![
            point(&[("__name__", "a"), ("instance", "i1")], 1.0),
            point(&[("__name__", "a"), ("instance", "i2")], 2.0),
        ]);
        let rhs = vector(vec![point(&[("__name__", "b"), ("instance", "i1")], 10.0)]);
        let matching = VectorMatching {
            on: true,
            matching_labels: vec!["instance".to_string()],
            ..VectorMatching::default()
        };
        let result = binary_vectors(BinaryOp::Sub, &lhs, &rhs, false, &matching).unwrap();
        assert_eq!(
            rendered(&result),
            vec![("{instance=\"i1\"}".to_string(), -9.0)]
        );
    }

    #[test]
    fn test_group_left_copies_labels_from_the_one_side() {
        // The left side is the "many" side: two series share `job="api"`.
        let lhs = vector(vec![
            point(
                &[("__name__", "errors"), ("job", "api"), ("code", "500")],
                1.0,
            ),
            point(
                &[("__name__", "errors"), ("job", "api"), ("code", "503")],
                3.0,
            ),
        ]);
        // The right side is the "one" side, with a `version` only it knows.
        let rhs = vector(vec![point(
            &[("__name__", "requests"), ("job", "api"), ("version", "v2")],
            4.0,
        )]);
        let matching = VectorMatching {
            on: true,
            matching_labels: vec!["job".to_string()],
            card: Cardinality::ManyToOne,
            include: vec!["version".to_string()],
        };
        let result = binary_vectors(BinaryOp::Div, &lhs, &rhs, false, &matching).unwrap();
        assert_eq!(
            rendered(&result),
            vec![
                (
                    "{code=\"500\",job=\"api\",version=\"v2\"}".to_string(),
                    0.25
                ),
                (
                    "{code=\"503\",job=\"api\",version=\"v2\"}".to_string(),
                    0.75
                ),
            ]
        );
    }

    #[test]
    fn test_group_right_mirrors_group_left() {
        // `one group_right many` (one-to-many): the right side is the "many"
        // side, so the pair is evaluated as `many group_left one` and the
        // operand order is restored when applying the operator.
        let one = vector(vec![point(
            &[("__name__", "b"), ("instance", "i1"), ("y", "k")],
            20.0,
        )]);
        let many = vector(vec![
            point(&[("__name__", "a"), ("instance", "i1"), ("x", "1")], 10.0),
            point(&[("__name__", "a"), ("instance", "i1"), ("x", "2")], 2.0),
        ]);
        let matching = VectorMatching {
            on: true,
            matching_labels: vec!["instance".to_string()],
            card: Cardinality::OneToMany,
            include: vec!["y".to_string()],
        };
        // `one / many`: 20/10 and 20/2, not the inverse.
        let result = binary_vectors(BinaryOp::Div, &one, &many, false, &matching).unwrap();
        assert_eq!(
            rendered(&result),
            vec![
                ("{instance=\"i1\",x=\"1\",y=\"k\"}".to_string(), 2.0),
                ("{instance=\"i1\",x=\"2\",y=\"k\"}".to_string(), 10.0),
            ]
        );
        // A filtering comparison reports the left (one-side) value and keeps
        // `__name__` — and with `group_right` the labels come from the *many*
        // side of the query, which after the swap is the original right
        // operand, so the name is `a`.
        let result = binary_vectors(BinaryOp::Gt, &one, &many, false, &matching).unwrap();
        assert_eq!(
            rendered(&result),
            vec![
                (
                    "{__name__=\"a\",instance=\"i1\",x=\"1\",y=\"k\"}".to_string(),
                    20.0
                ),
                (
                    "{__name__=\"a\",instance=\"i1\",x=\"2\",y=\"k\"}".to_string(),
                    20.0
                ),
            ]
        );
    }

    #[test]
    fn test_matching_errors_name_the_problem() {
        // A match key that repeats on the "one" side is ambiguous.
        let lhs = vector(vec![point(&[("__name__", "a"), ("instance", "i1")], 1.0)]);
        let rhs = vector(vec![
            point(&[("__name__", "b"), ("instance", "i1"), ("code", "1")], 1.0),
            point(&[("__name__", "b"), ("instance", "i1"), ("code", "2")], 2.0),
        ]);
        let matching = VectorMatching {
            on: true,
            matching_labels: vec!["instance".to_string()],
            ..VectorMatching::default()
        };
        let error = binary_vectors(BinaryOp::Add, &lhs, &rhs, false, &matching).unwrap_err();
        assert!(error.message().contains("found duplicate series"));
        assert!(error
            .message()
            .contains("many-to-many matching not allowed"));

        // One-to-one matching that would emit two results for one series must
        // tell the caller to use `group_left`.
        let many = vector(vec![
            point(&[("__name__", "a"), ("instance", "i1")], 1.0),
            point(&[("__name__", "a"), ("instance", "i1")], 2.0),
        ]);
        let one = vector(vec![point(&[("__name__", "b"), ("instance", "i1")], 1.0)]);
        let error = binary_vectors(BinaryOp::Add, &many, &one, false, &matching).unwrap_err();
        assert!(error.message().contains("multiple matches for labels"));
        assert!(error.message().contains("group_left/group_right"));

        // Two results with identical labels are refused too, even with
        // grouping: they would be the same series twice.
        let grouped = VectorMatching {
            on: true,
            matching_labels: vec!["instance".to_string()],
            card: Cardinality::ManyToOne,
            include: Vec::new(),
        };
        let error = binary_vectors(BinaryOp::Add, &many, &one, false, &grouped).unwrap_err();
        assert!(error
            .message()
            .contains("grouping labels must ensure unique matches"));

        // Many-to-many is reserved for set operators.
        let many_to_many = VectorMatching {
            card: Cardinality::ManyToMany,
            ..VectorMatching::default()
        };
        let error = binary_vectors(BinaryOp::Add, &lhs, &rhs, false, &many_to_many).unwrap_err();
        assert!(error.message().contains("only allowed for set operators"));
    }

    #[test]
    fn test_set_operators_use_the_match_key() {
        // Different `code`, same `instance`: `on(instance)` pairs them.
        let lhs = vector(vec![
            point(
                &[("__name__", "a"), ("instance", "i1"), ("code", "500")],
                1.0,
            ),
            point(
                &[("__name__", "a"), ("instance", "i2"), ("code", "500")],
                2.0,
            ),
        ]);
        let rhs = vector(vec![point(
            &[("__name__", "b"), ("instance", "i1"), ("code", "200")],
            3.0,
        )]);
        let matching = VectorMatching {
            on: true,
            matching_labels: vec!["instance".to_string()],
            card: Cardinality::ManyToMany,
            include: Vec::new(),
        };
        let and = binary_vectors(BinaryOp::And, &lhs, &rhs, false, &matching).unwrap();
        assert_eq!(
            rendered(&and),
            vec![(
                "{__name__=\"a\",code=\"500\",instance=\"i1\"}".to_string(),
                1.0
            )]
        );
        // `unless` keeps the left series the right side does not match.
        let unless = binary_vectors(BinaryOp::Unless, &lhs, &rhs, false, &matching).unwrap();
        assert_eq!(
            rendered(&unless),
            vec![(
                "{__name__=\"a\",code=\"500\",instance=\"i2\"}".to_string(),
                2.0
            )]
        );
        // `or on(instance)` keeps every left series and only adds right ones
        // whose match key is absent on the left: `b` matches `i1`, so it is
        // dropped. Set operators pass their input through, `__name__`
        // included.
        let or = binary_vectors(BinaryOp::Or, &lhs, &rhs, false, &matching).unwrap();
        assert_eq!(
            rendered(&or),
            vec![
                (
                    "{__name__=\"a\",code=\"500\",instance=\"i1\"}".to_string(),
                    1.0
                ),
                (
                    "{__name__=\"a\",code=\"500\",instance=\"i2\"}".to_string(),
                    2.0
                ),
            ]
        );
        // With the default match key `code` is part of it, so nothing on the
        // left matches and both sides survive.
        let or_default = binary_vectors(
            BinaryOp::Or,
            &lhs,
            &rhs,
            false,
            &VectorMatching {
                card: Cardinality::ManyToMany,
                ..VectorMatching::default()
            },
        )
        .unwrap();
        assert_eq!(
            rendered(&or_default),
            vec![
                (
                    "{__name__=\"a\",code=\"500\",instance=\"i1\"}".to_string(),
                    1.0
                ),
                (
                    "{__name__=\"a\",code=\"500\",instance=\"i2\"}".to_string(),
                    2.0
                ),
                (
                    "{__name__=\"b\",code=\"200\",instance=\"i1\"}".to_string(),
                    3.0
                ),
            ]
        );
    }

    #[test]
    fn test_comparison_keeps_the_name_unless_bool() {
        let lhs = vector(vec![point(&[("__name__", "a"), ("instance", "i1")], 5.0)]);
        let rhs = vector(vec![point(&[("__name__", "b"), ("instance", "i1")], 1.0)]);
        let matching = VectorMatching {
            on: true,
            matching_labels: vec!["instance".to_string()],
            ..VectorMatching::default()
        };
        // With the default matching a filtering comparison keeps the left
        // series' labels, `__name__` included.
        let filtered =
            binary_vectors(BinaryOp::Gt, &lhs, &rhs, false, &VectorMatching::default()).unwrap();
        assert_eq!(
            rendered(&filtered),
            vec![("{__name__=\"a\",instance=\"i1\"}".to_string(), 5.0)]
        );
        // `on(...)` narrows the result to the matching labels, so `__name__`
        // is gone even though the comparison only filters: Prometheus'
        // `resultMetric` calls `Keep` for a one-to-one `on` match.
        let narrowed = binary_vectors(BinaryOp::Gt, &lhs, &rhs, false, &matching).unwrap();
        assert_eq!(
            rendered(&narrowed),
            vec![("{instance=\"i1\"}".to_string(), 5.0)]
        );
        // `bool` reports 1 and drops `__name__`.
        let boolean = binary_vectors(BinaryOp::Gt, &lhs, &rhs, true, &matching).unwrap();
        assert_eq!(
            rendered(&boolean),
            vec![("{instance=\"i1\"}".to_string(), 1.0)]
        );
        // A filtering comparison that fails emits nothing.
        let failed =
            binary_vectors(BinaryOp::Lt, &lhs, &rhs, false, &VectorMatching::default()).unwrap();
        assert!(failed.is_empty());
    }

    /// One series with `labels`, ready for an `Evaluator` that never touches a
    /// backend. `__name__` is not repeated here: the selector adds it.
    fn synthetic_labeled(
        metric: &str,
        labels: &[(&str, &str)],
        samples: &[(i64, f64)],
    ) -> SeriesSamples {
        SeriesSamples {
            metric_name: metric.as_bytes().to_vec(),
            labels: labels
                .iter()
                .map(|(name, value)| Label {
                    name: name.as_bytes().to_vec(),
                    value: value.as_bytes().to_vec(),
                })
                .collect(),
            samples: samples
                .iter()
                .map(|(timestamp, value)| SamplePoint {
                    timestamp: *timestamp,
                    value: *value,
                })
                .collect(),
        }
    }

    #[test]
    fn test_label_replace_rewrites_one_label() {
        let series = || {
            synthetic_labeled(
                "http_requests",
                &[("instance", "10.0.0.1:9090"), ("job", "api")],
                &[(0, 1.0)],
            )
        };
        let run = |query: &str| eval_synthetic(series(), "http_requests", query, 0);
        // `$1` and `${1}` both expand the capture, and the rest of the label
        // set (including `__name__`) is untouched.
        for replacement in ["$1:9100", "${1}:9100"] {
            let query = format!(
                r#"label_replace(http_requests, "instance", "{replacement}", "instance", "(.*):(.*)")"#
            );
            assert_eq!(
                run(&query),
                vec![(
                    "{__name__=\"http_requests\",instance=\"10.0.0.1:9100\",job=\"api\"}"
                        .to_string(),
                    1.0
                )],
                "{query}"
            );
        }
        // `$$` is a literal dollar sign.
        assert_eq!(
            run(r#"label_replace(http_requests, "instance", "$$$1", "instance", "(.*):(.*)")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"$10.0.0.1\",job=\"api\"}".to_string(),
                1.0
            )]
        );
        // A value that does not match the (anchored) pattern passes through
        // unchanged: the pattern must cover the whole label value.
        assert_eq!(
            run(r#"label_replace(http_requests, "job", "db", "job", "ap")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\",job=\"api\"}".to_string(),
                1.0
            )]
        );
        // An empty expansion deletes the destination label.
        assert_eq!(
            run(r#"label_replace(http_requests, "job", "", "job", ".*")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\"}".to_string(),
                1.0
            )]
        );
        // The pattern matches "api" here but its second group does not, and an
        // unmatched group expands to nothing — so `$2` deletes `job`.
        assert_eq!(
            run(r#"label_replace(http_requests, "job", "$2", "job", "(a)(x)?pi")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\"}".to_string(),
                1.0
            )]
        );
        // Writing `__name__` renames the metric instead of adding a label.
        assert_eq!(
            run(
                r#"label_replace(http_requests, "__name__", "renamed_$1", "__name__", "http_(.*)")"#
            ),
            vec![(
                "{__name__=\"renamed_requests\",instance=\"10.0.0.1:9090\",job=\"api\"}"
                    .to_string(),
                1.0
            )]
        );
    }

    #[test]
    fn test_label_join_joins_labels() {
        let series = || {
            synthetic_labeled(
                "http_requests",
                &[("instance", "10.0.0.1"), ("job", "api")],
                &[(0, 1.0)],
            )
        };
        let run = |query: &str| eval_synthetic(series(), "http_requests", query, 0);
        assert_eq!(
            run(r#"label_join(http_requests, "target", "/", "job", "instance")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1\",job=\"api\",\
                 target=\"api/10.0.0.1\"}"
                    .to_string(),
                1.0
            )]
        );
        // A missing source label contributes an empty string, so the separator
        // still appears (Prometheus joins the values unconditionally).
        assert_eq!(
            run(r#"label_join(http_requests, "target", "/", "job", "zone")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1\",job=\"api\",\
                 target=\"api/\"}"
                    .to_string(),
                1.0
            )]
        );
        // An empty join deletes the destination label, like Prometheus'
        // `labels.Builder.Set` with an empty value.
        assert_eq!(
            run(r#"label_join(http_requests, "job", "/", "zone")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1\"}".to_string(),
                1.0
            )]
        );
        // The last label wins when both source labels are the same.
        assert_eq!(
            run(r#"label_join(http_requests, "x", "-", "job", "job")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1\",job=\"api\",x=\"api-api\"}"
                    .to_string(),
                1.0
            )]
        );
    }

    #[test]
    fn test_metricsql_label_helpers() {
        let series = || {
            synthetic_labeled(
                "http_requests",
                &[("instance", "10.0.0.1:9090"), ("job", "api")],
                &[(0, 1.0)],
            )
        };
        let run = |query: &str| eval_synthetic(series(), "http_requests", query, 0);
        // `label_set` writes each pair, and an empty value deletes the label.
        assert_eq!(
            run(r#"label_set(http_requests, "job", "db", "zone", "eu")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\",job=\"db\",zone=\"eu\"}"
                    .to_string(),
                1.0
            )]
        );
        assert_eq!(
            run(r#"label_set(http_requests, "job", "")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\"}".to_string(),
                1.0
            )]
        );
        // `__name__` is a label like any other for `label_set`, and `alias` is
        // the same call spelled the MetricsQL way (its own rewrite rule:
        // `alias(q, name) = label_set(q, "__name__", name)`).
        for query in [
            r#"label_set(http_requests, "__name__", "renamed")"#,
            r#"alias(http_requests, "renamed")"#,
        ] {
            assert_eq!(
                run(query),
                vec![(
                    "{__name__=\"renamed\",instance=\"10.0.0.1:9090\",job=\"api\"}".to_string(),
                    1.0
                )],
                "{query}"
            );
        }
        // Pairs are applied in order, so the last value of a repeated label
        // wins.
        assert_eq!(
            run(r#"label_set(http_requests, "job", "db", "job", "cache")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\",job=\"cache\"}".to_string(),
                1.0
            )]
        );
        // A pair-less call is legal and changes nothing.
        assert_eq!(
            run("label_set(http_requests)"),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\",job=\"api\"}".to_string(),
                1.0
            )]
        );
        // VM keeps tags as arbitrary byte strings, so a name that Prometheus
        // would reject is accepted here (`label_replace` validates because
        // Prometheus does; these functions have no such rule upstream).
        assert_eq!(
            run(r#"label_set(http_requests, "k8s.io/name", "x")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\",job=\"api\",\
                 k8s.io/name=\"x\"}"
                    .to_string(),
                1.0
            )]
        );
        // `label_del` drops the named labels and keeps `__name__` unless it is
        // named.
        assert_eq!(
            run(r#"label_del(http_requests, "instance")"#),
            vec![("{__name__=\"http_requests\",job=\"api\"}".to_string(), 1.0)]
        );
        assert_eq!(
            run(r#"label_del(http_requests, "__name__")"#),
            vec![("{instance=\"10.0.0.1:9090\",job=\"api\"}".to_string(), 1.0)]
        );
        // `label_keep` drops everything not named, `__name__` included —
        // VM's `RemoveTagsOn` resets the metric group unless `__name__` is
        // kept. Keeping nothing leaves an empty label set.
        assert_eq!(
            run(r#"label_keep(http_requests, "job")"#),
            vec![("{job=\"api\"}".to_string(), 1.0)]
        );
        assert_eq!(
            run(r#"label_keep(http_requests, "job", "__name__")"#),
            vec![("{__name__=\"http_requests\",job=\"api\"}".to_string(), 1.0)]
        );
        assert_eq!(
            run("label_keep(http_requests)"),
            vec![("{}".to_string(), 1.0)]
        );
        // `label_copy` keeps the source and leaves the destination alone when
        // the source is missing (VM skips rather than deleting).
        assert_eq!(
            run(r#"label_copy(http_requests, "job", "team")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\",job=\"api\",\
                 team=\"api\"}"
                    .to_string(),
                1.0
            )]
        );
        assert_eq!(
            run(r#"label_copy(http_requests, "zone", "job")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\",job=\"api\"}".to_string(),
                1.0
            )]
        );
        // `label_move` deletes the source afterwards; the same source and
        // destination is a no-op instead of a deletion.
        assert_eq!(
            run(r#"label_move(http_requests, "job", "team")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\",team=\"api\"}".to_string(),
                1.0
            )]
        );
        assert_eq!(
            run(r#"label_move(http_requests, "job", "job")"#),
            vec![(
                "{__name__=\"http_requests\",instance=\"10.0.0.1:9090\",job=\"api\"}".to_string(),
                1.0
            )]
        );
        // `label_value` drops `__name__`, keeps the other labels, and replaces
        // the value with the named label's text.
        let numeric = || {
            synthetic_labeled(
                "http_requests",
                &[("job", "api"), ("port", "9090")],
                &[(0, 7.0)],
            )
        };
        assert_eq!(
            eval_synthetic(
                numeric(),
                "http_requests",
                r#"label_value(http_requests, "port")"#,
                0
            ),
            vec![("{job=\"api\",port=\"9090\"}".to_string(), 9090.0)]
        );
        // A label that is not a number, or does not exist, makes the value
        // NaN — and VM leaves NaN samples alone, so they stay NaN rather than
        // taking the parsed value.
        let value_of = |series: SeriesSamples, query: &str| -> f64 {
            eval_series(vec![series], "http_requests", query, 0)
                .vector
                .0[0]
                .value
        };
        assert!(value_of(numeric(), r#"label_value(http_requests, "job")"#).is_nan());
        assert!(value_of(numeric(), r#"label_value(http_requests, "zone")"#).is_nan());
        let gappy = synthetic_labeled("http_requests", &[("port", "9090")], &[(0, f64::NAN)]);
        assert!(value_of(gappy, r#"label_value(http_requests, "port")"#).is_nan());
    }

    #[test]
    fn test_label_functions_parse_errors() {
        // The regex is compiled at parse time, so a bad pattern names the
        // function that carried it.
        assert!(parse_err(r#"label_replace(m, "a", "$1", "b", "(")"#)
            .contains("invalid regular expression in label_replace"));
        // Label names are validated where Prometheus validates them: the
        // destination of both functions and the sources of `label_join`.
        assert!(parse_err(r#"label_replace(m, "1bad", "", "b", ".*")"#)
            .contains("invalid label name \"1bad\" in label_replace"));
        assert!(parse_err(r#"label_join(m, "a", "-", "b c")"#)
            .contains("invalid label name \"b c\" in label_join"));
        // Non-string arguments and missing labels are refused with a message
        // about the shape of the call.
        assert!(parse_err(r#"label_replace(m, 5, "", "b", ".*")"#).contains("quoted string"));
        assert!(parse_err(r#"label_join(m, "a", "-")"#).contains("at least one source label"));
        assert!(parse_err(r#"label_replace(m, "a", "$1", "b")"#).contains("expected Comma"));
        // The MetricsQL helpers reject an odd number of name/value strings,
        // and a non-string where a string belongs (VM's `getStringPairs`).
        assert!(parse_err(r#"label_set(m, "a")"#).contains("number of string args to label_set"));
        assert!(parse_err(r#"label_move(m, "a", "b", "c")"#)
            .contains("number of string args to label_move"));
        assert!(parse_err(r#"label_del(m, 5)"#).contains("quoted string"));
        assert!(parse_err(r#"label_value(m, 5)"#).contains("quoted string"));
        assert!(parse_err(r#"alias(m)"#).contains("expected Comma"));
        // A scalar has no labels to rewrite; this is an evaluation error, not
        // a parse error, so the shape still parses.
        let expr = parse_ok(r#"label_replace(1, "a", "", "b", ".*")"#);
        let fetched = HashMap::new();
        let evaluator = Evaluator {
            fetched: &fetched,
            query_start_ms: 0,
            query_end_ms: 0,
            step_ms: 0,
        };
        let error = evaluator.eval(&expr, 0).unwrap_err();
        assert!(error.message().contains("instant vector, got a scalar"));
        let expr = parse_ok(r#"label_set(1, "a", "b")"#);
        let error = evaluator.eval(&expr, 0).unwrap_err();
        assert!(
            error
                .message()
                .contains("label_set expects an instant vector, got a scalar"),
            "{}",
            error.message()
        );
    }
}
