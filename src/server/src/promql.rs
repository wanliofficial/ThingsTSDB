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
//!   `changes`, `resets`, `*_over_time`;
//! * instant functions — math (`abs`, `ceil`, `clamp_min`, ...), `timestamp`,
//!   `sort`, `sort_desc`, `absent`, `histogram_quantile`, and the generators
//!   `time`, `pi`, `vector`, plus MetricsQL's `default`;
//! * aggregations — `sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`,
//!   `stdvar`, `topk`, `bottomk`, with `by` / `without`;
//! * binary operators — arithmetic and comparison (with `bool`) between two
//!   scalars or a vector and a scalar, vector matching between two vectors
//!   (`on` / `ignoring`, `group_left` / `group_right`), and the set operators
//!   `and` / `or` / `unless`;
//! * a MetricsQL subset on top of PromQL — `aggr_over_time(<agg>, v[range])`,
//!   `absent_over_time`, `tmin_over_time` / `tmax_over_time`, and
//!   `predict_linear(v[range], t)`.
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
//! Deliberately **not** implemented: the string functions (`label_replace`,
//! `label_join`). Those are rejected with a message naming the gap: a panel
//! that silently receives an approximated answer where it asked for something
//! else would draw a wrong line, which is worse than an error.
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
            tokens.push(Token::Ident(chars[start..i].iter().collect()));
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
            _ => return None,
        })
    }

    /// `topk` / `bottomk` take a parameter before the vector.
    fn takes_param(self) -> bool {
        matches!(self, AggOp::Topk | AggOp::Bottomk)
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
    /// `predict_linear(v[range], t)`.
    PredictLinear,
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
            // `predict_linear` takes its range vector first, so it is
            // dispatched by name in `call` rather than through `parse`.
            _ => return None,
        })
    }

    fn takes_param(self) -> bool {
        matches!(self, RangeFunc::QuantileOverTime | RangeFunc::PredictLinear)
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
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Number(f64),
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
    },
    MathFunc {
        func: MathFunc,
        param: Option<Box<Expr>>,
        expr: Box<Expr>,
    },
    Sort {
        dir: SortDir,
        expr: Box<Expr>,
    },
    Absent(Box<Expr>),
    /// `vector(scalar)` lifts a scalar into a one-sample vector.
    Vector(Box<Expr>),
    /// `time()` — the evaluation instant in seconds.
    Time,
}

impl Expr {
    /// Collects every selector in the expression, so the engine can be asked
    /// for each one's samples once per request.
    pub fn selectors(&self, out: &mut Vec<Selector>) {
        match self {
            Expr::Number(_) | Expr::Time => {}
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
            Expr::RangeFunc { expr, param, .. } => {
                expr.selectors(out);
                if let Some(param) = param {
                    param.selectors(out);
                }
            }
            Expr::MathFunc { param, expr, .. } => {
                if let Some(param) = param {
                    param.selectors(out);
                }
                expr.selectors(out);
            }
            Expr::Sort { expr, .. } | Expr::Absent(expr) | Expr::Vector(expr) => {
                expr.selectors(out)
            }
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
            Expr::Number(_) | Expr::Time => {}
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
            Expr::RangeFunc { expr, param, .. } => {
                expr.widen_window(query_start_ms, query_end_ms, start, end);
                if let Some(param) = param {
                    param.widen_window(query_start_ms, query_end_ms, start, end);
                }
            }
            Expr::MathFunc { param, expr, .. } => {
                if let Some(param) = param {
                    param.widen_window(query_start_ms, query_end_ms, start, end);
                }
                expr.widen_window(query_start_ms, query_end_ms, start, end);
            }
            Expr::Sort { expr, .. } | Expr::Absent(expr) | Expr::Vector(expr) => {
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
            if is_ident && !op.is_set() {
                unreachable!("only set operators are spelled with words");
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
                        Some(Token::Number(value)) => AtModifier::At((value * 1_000.0) as i64),
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
                if matches!(inner, Expr::Number(_) | Expr::Time) {
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
            Some(Token::Number(seconds)) if seconds >= 0.0 => Ok((seconds * 1_000.0) as i64),
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
                    let expr = self.expression(0)?;
                    self.expect(Token::RParen)?;
                    Ok(Expr::Aggregation {
                        op,
                        expr: Box::new(expr),
                        param: None,
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
            "label_replace" | "label_join" => {
                return bad(format!(
                    "`{name}` is not supported: this endpoint does not rewrite labels"
                ))
            }
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
                        "`{}` needs no parameter inside aggr_over_time",
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
                });
            }
            // `predict_linear(v[range], t)` puts the range vector first and the
            // horizon after it, the reverse of the other parameterised range
            // functions, so it is dispatched separately too.
            "predict_linear" => {
                self.expect(Token::LParen)?;
                let inner = self.range_argument(name)?;
                self.expect(Token::Comma)?;
                let param = self.expression(0)?;
                self.expect(Token::RParen)?;
                return Ok(Expr::RangeFunc {
                    func: RangeFunc::PredictLinear,
                    expr: Box::new(inner),
                    param: Some(Box::new(param)),
                });
            }
            _ => {}
        }
        self.expect(Token::LParen)?;
        // Aggregations may carry a parameter first (`topk(5, expr)`).
        if let Some(op) = AggOp::parse(name) {
            if op.takes_param() {
                let param = self.expression(0)?;
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
                });
            }
            let inner = self.range_argument(name)?;
            self.expect(Token::RParen)?;
            return Ok(Expr::RangeFunc {
                func,
                expr: Box::new(inner),
                param: None,
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
            "pi" => {
                self.expect(Token::RParen)?;
                Ok(Expr::Number(std::f64::consts::PI))
            }
            other => bad(format!(
                "unknown function {other:?}; supported range functions are rate, irate, \
                 increase, delta, idelta, changes, resets and *_over_time, and the math \
                 functions abs, ceil, floor, sqrt, exp, ln, log2, log10, sgn, round, \
                 clamp_min, clamp_max, timestamp"
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
}

impl<'a> Evaluator<'a> {
    fn eval(&self, expr: &Expr, at_ms: i64) -> Result<Instant> {
        match expr {
            Expr::Number(value) => Ok(Instant::scalar(*value)),
            Expr::Time => Ok(Instant::scalar(at_ms as f64 / 1_000.0)),
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
            Expr::RangeFunc { func, expr, param } => {
                self.range_func(*func, expr, param.as_deref(), at_ms)
            }
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
            matrix.0.push(RangeSeries {
                labels: labels_of(series),
                samples,
            });
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

        if op.takes_param() {
            let k = match param {
                Some(param) => self.scalar_of(param, at_ms)?,
                None => return bad(format!("{} needs a parameter", op.name())),
            };
            return Ok(Instant {
                vector: topk(op, &values, k as usize, grouping),
                scalar: None,
            });
        }
        if param.is_some() {
            // `count_values` etc. would land here; they are not parsed.
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
        if func == RangeFunc::QuantileOverTime && param.is_none() {
            return bad("quantile_over_time needs a quantile parameter");
        }
        if let RangeFunc::AggrOverTime(op) = func {
            if matches!(op, AggOp::Topk | AggOp::Bottomk | AggOp::Group) {
                return bad(format!(
                    "`{}` is not an aggregation over_time accepts",
                    op.name()
                ));
            }
        }
        if func == RangeFunc::PredictLinear && param.is_none() {
            return bad("predict_linear needs a horizon in seconds");
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
            if let Some(value) = apply_range_func(func, &series.samples, range_ms, param) {
                let mut labels = series.labels.clone();
                labels.remove("__name__");
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
                        MathFunc::ClampMin => point.value.max(bound),
                        _ => point.value.min(bound),
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
        AggOp::Topk | AggOp::Bottomk => f64::NAN,
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

/// Evaluates one range function over a series' samples.
///
/// Returns `None` when the series cannot produce a value (fewer than two
/// samples for the slope functions, none for the aggregates).
fn apply_range_func(
    func: RangeFunc,
    samples: &[SamplePoint],
    range_ms: i64,
    param: Option<f64>,
) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
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
            let variance = apply_range_func(RangeFunc::StdvarOverTime, samples, range_ms, None)?;
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
        // Linear extrapolation over the range, evaluated `horizon` seconds past
        // the last sample (least squares, as Prometheus does).
        RangeFunc::PredictLinear => {
            let horizon = param?;
            let n = samples.len() as f64;
            let t_mean = samples
                .iter()
                .map(|s| s.timestamp as f64 / 1_000.0)
                .sum::<f64>()
                / n;
            let v_mean = samples.iter().map(|s| s.value).sum::<f64>() / n;
            let mut num = 0.0;
            let mut den = 0.0;
            for sample in samples {
                let dt = sample.timestamp as f64 / 1_000.0 - t_mean;
                num += dt * (sample.value - v_mean);
                den += dt * dt;
            }
            let slope = if den == 0.0 { 0.0 } else { num / den };
            let intercept = v_mean - slope * t_mean;
            let last = samples.last()?.timestamp as f64 / 1_000.0;
            Some(intercept + slope * (last + horizon))
        }
    }
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

/// The values of a series' samples, for the aggregation-over-time functions.
fn values(samples: &[SamplePoint]) -> Vec<f64> {
    samples.iter().map(|s| s.value).collect()
}

/// `histogram_quantile(q, v)` for classic `le` buckets.
///
/// Every series must carry a `le` bucket bound; `_sum`/`_count` series (or a
/// missing `by (le)` aggregation) are refused by name rather than dropped, so
/// a panel that passes the wrong metric hears about it instead of drawing an
/// empty graph.
fn histogram_quantile(quantile: f64, vector: &Vector) -> Result<Vector> {
    type Groups = BTreeMap<BTreeMap<String, String>, Vec<(f64, f64)>>;
    let mut groups: Groups = BTreeMap::new();
    for point in &vector.0 {
        let Some(raw) = point.labels.get("le") else {
            return bad(format!(
                "histogram_quantile needs a `le` label on every series; {} has none",
                label_string(&point.labels)
            ));
        };
        let Ok(upper) = raw.trim().parse::<f64>() else {
            return bad(format!(
                "histogram_quantile cannot read `le={raw}` as a bucket bound"
            ));
        };
        let mut labels = point.labels.clone();
        labels.remove("le");
        labels.remove("__name__");
        groups.entry(labels).or_default().push((upper, point.value));
    }
    let mut out = Vector::default();
    for (labels, mut buckets) in groups {
        out.push(labels, bucket_quantile(quantile, &mut buckets));
    }
    Ok(out)
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

/// Renders a label set for an error message, e.g. `{job="api"}`.
fn label_string(labels: &LabelSet) -> String {
    let body = labels
        .iter()
        .map(|(name, value)| format!("{name}={value:?}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{body}}}")
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
            "round(http_requests, 0.5)",
            r#"rate(http_requests{job="api"}[5m] offset 1h)"#,
            "sort_desc(http_requests)",
            "histogram_quantile(0.9, http_latency_bucket)",
            "histogram_quantile(0.5, sum by (le) (rate(http_latency_bucket[5m])))",
            "absent(http_requests)",
            "vector(1)",
            "time()",
            "pi()",
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
        // `predict_linear` puts the range vector first.
        parse_ok("predict_linear(m[5m], 3600)");
        // `default(v, x)`.
        parse_ok("default(m, 0)");
        parse_ok(r#"default(m{job="x"}, 0)"#);
        // Errors name the problem.
        assert!(parse_err("aggr_over_time(5, m[5m])").contains("aggregation name"));
        assert!(parse_err("aggr_over_time(nonsense, m[5m])").contains("not an aggregation"));
        assert!(parse_err("aggr_over_time(topk, m[5m])").contains("no parameter"));
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
            apply_range_func(RangeFunc::AggrOverTime(AggOp::Sum), &samples, 3_000, None),
            Some(7.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::AggrOverTime(AggOp::Avg), &samples, 3_000, None),
            Some(7.0 / 3.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::AggrOverTime(AggOp::Max), &samples, 3_000, None),
            Some(4.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::AggrOverTime(AggOp::Min), &samples, 3_000, None),
            Some(1.0)
        );
        // The extreme value is 4 at ts 1 s and 1 at ts 2 s.
        assert_eq!(
            apply_range_func(RangeFunc::TMinOverTime, &samples, 3_000, None),
            Some(2.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::TMaxOverTime, &samples, 3_000, None),
            Some(1.0)
        );
        // Linear fit of (0,2),(1,4),(2,1): slope -0.5, mean 7/3 at t=1, so the
        // line is 17/6 - t/2 and the prediction at t = last + 1 = 3 is 4/3.
        let predicted = apply_range_func(RangeFunc::PredictLinear, &samples, 3_000, Some(1.0))
            .expect("predict_linear returns a value");
        assert!((predicted - 4.0 / 3.0).abs() < 1e-9, "got {predicted}");
        assert!(values.len() == 3);
    }

    #[test]
    fn test_unsupported_constructs_are_named() {
        // Each message must say what is missing, not just "invalid".
        assert!(parse_err(r#"label_replace(a, "x", "$1", "y", "(.*)")"#).contains("label_replace"));
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
        };
        rendered(
            &evaluator
                .eval(&expr, at_ms)
                .unwrap_or_else(|e| panic!("{query:?} failed: {e:?}"))
                .vector,
        )
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
            apply_range_func(RangeFunc::Increase, &samples, 4_000, None),
            Some(17.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::Delta, &samples, 4_000, None),
            Some(-3.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::Changes, &samples, 4_000, None),
            Some(3.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::Resets, &samples, 4_000, None),
            Some(1.0)
        );
        // Rate of the increase over three seconds.
        let rate = apply_range_func(RangeFunc::Rate, &samples, 4_000, None).unwrap();
        assert!((rate - 17.0 / 3.0).abs() < 1e-12);
        assert_eq!(
            apply_range_func(RangeFunc::LastOverTime, &samples, 4_000, None),
            Some(7.0)
        );
        assert_eq!(
            apply_range_func(RangeFunc::CountOverTime, &samples, 4_000, None),
            Some(4.0)
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
        let failed = binary_vectors(BinaryOp::Lt, &lhs, &rhs, false, &matching).unwrap();
        assert!(failed.is_empty());
    }
}
