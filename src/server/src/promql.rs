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
//! * range functions — `rate`, `irate`, `increase`, `delta`, `idelta`,
//!   `changes`, `resets`, `*_over_time`;
//! * instant functions — math (`abs`, `ceil`, `clamp_min`, ...), `timestamp`,
//!   `sort`, `sort_desc`, `absent`, and the generators `time`, `pi`, `vector`,
//!   plus MetricsQL's `default`;
//! * aggregations — `sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`,
//!   `stdvar`, `topk`, `bottomk`, with `by` / `without`;
//! * binary operators — arithmetic and comparison (with `bool`) between two
//!   scalars or a vector and a scalar, exact-label matching between two
//!   vectors, and the set operators `and` / `or` / `unless`;
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
//! Deliberately **not** implemented: subqueries `[...:...]`, `on(...)` /
//! `ignoring(...)` / `group_left` / `group_right`, and the string functions
//! (`label_replace`, `label_join`). Those are rejected with a message naming
//! the gap: a panel that silently receives `a + b` where it asked for
//! `a + on(instance) b` would draw a wrong line, which is worse than an error.
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
    },
    Aggregation {
        op: AggOp,
        expr: Box<Expr>,
        param: Option<Box<Expr>>,
        grouping: Option<Grouping>,
    },
    RangeFunc {
        func: RangeFunc,
        expr: Box<Expr>,
        range_ms: i64,
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
                range_ms,
                param,
                ..
            } => {
                expr.widen_window(query_start_ms, query_end_ms, start, end);
                *start = (*start).min(query_start_ms - range_ms - LOOKBACK_MS);
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
            // `and on (x)` / `or ignoring (y)` etc. need vector matching, which
            // this evaluator does not implement (it matches on all labels).
            if let Some(Token::Ident(modifier)) = self.peek() {
                if matches!(
                    modifier.as_str(),
                    "on" | "ignoring" | "group_left" | "group_right"
                ) {
                    return bad(format!(
                        "`{modifier}` vector matching is not supported; this endpoint matches \
                         two vectors on all labels (except __name__)"
                    ));
                }
            }
            let ret_bool = if op.is_comparison() && self.peek_ident_is("bool") {
                self.pos += 1;
                true
            } else {
                false
            };
            if is_ident && !op.is_set() {
                unreachable!("only set operators are spelled with words");
            }
            let rhs = self.expression(bp + 1)?;
            lhs = Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                ret_bool,
            };
        }
        Ok(lhs)
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
            // Right associative, and the exponent may be a bare scalar.
            let rhs = self.expression(6)?;
            return Ok(Expr::Binary {
                op: BinaryOp::Pow,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                ret_bool: false,
            });
        }
        Ok(lhs)
    }

    /// Applies `[range]`, `offset d` and `@ t` to a primary expression.
    fn postfix(&mut self) -> Result<Expr> {
        let mut expr = self.primary()?;
        let mut offset_ms = 0;
        let mut at: Option<AtModifier> = None;
        let mut range_ms: Option<i64> = None;
        loop {
            match self.peek() {
                Some(Token::LBracket) => {
                    self.pos += 1;
                    let range = match self.next() {
                        Some(Token::Duration(ms)) => ms,
                        other => {
                            return bad(format!(
                                "expected a duration inside [ ] but found {}",
                                describe(other)
                            ))
                        }
                    };
                    // A `:` before the closing bracket makes this a subquery
                    // `[...:...]`, which the evaluator does not implement.
                    // Refusing at parse time keeps the error about the query
                    // the user actually wrote.
                    if !self.eat(&Token::RBracket) {
                        return bad(
                            "subqueries (`[...:...]`) are not supported; use a plain range \
                             selector such as `metric[5m]`",
                        );
                    }
                    range_ms = Some(range);
                }
                Some(Token::Ident(word)) if word == "offset" => {
                    self.pos += 1;
                    match self.next() {
                        Some(Token::Duration(ms)) => offset_ms = ms,
                        other => {
                            return bad(format!(
                                "expected a duration after 'offset' but found {}",
                                describe(other)
                            ))
                        }
                    }
                }
                Some(Token::Op("@")) => {
                    self.pos += 1;
                    match self.next() {
                        Some(Token::Number(value)) => {
                            at = Some(AtModifier::At((value * 1_000.0) as i64))
                        }
                        Some(Token::Ident(word)) if word == "start" => {
                            self.expect(Token::LParen)?;
                            self.expect(Token::RParen)?;
                            at = Some(AtModifier::Start);
                        }
                        Some(Token::Ident(word)) if word == "end" => {
                            self.expect(Token::LParen)?;
                            self.expect(Token::RParen)?;
                            at = Some(AtModifier::End);
                        }
                        other => {
                            return bad(format!(
                                "expected a timestamp after '@' but found {}",
                                describe(other)
                            ))
                        }
                    }
                }
                _ => break,
            }
        }
        if offset_ms != 0 || at.is_some() || range_ms.is_some() {
            expr = match expr {
                Expr::Selector {
                    selector,
                    offset_ms: inner_offset,
                    at: inner_at,
                } => {
                    let offset_ms = inner_offset
                        .checked_add(offset_ms)
                        .ok_or_else(|| Error::Bad("offset overflows".to_string()))?;
                    if range_ms.is_some() {
                        Expr::Range {
                            selector,
                            range_ms: range_ms.expect("checked above"),
                            offset_ms,
                            at: at.or(inner_at),
                        }
                    } else {
                        Expr::Selector {
                            selector,
                            offset_ms,
                            at: at.or(inner_at),
                        }
                    }
                }
                other => {
                    if range_ms.is_some() {
                        return bad("a range selector needs a plain metric name");
                    }
                    // `(expr) offset 5m` is legal PromQL, but only shifts the
                    // evaluation instant; refuse rather than ignore it.
                    return bad(format!(
                        "`offset` and `@` are only supported on a plain selector, \
                         not on this expression ({})",
                        render(&other)
                    ));
                }
            };
        }
        Ok(expr)
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
                let (inner, range_ms) = self.range_argument("aggr_over_time")?;
                self.expect(Token::RParen)?;
                return Ok(Expr::RangeFunc {
                    func: RangeFunc::AggrOverTime(op),
                    expr: Box::new(inner),
                    range_ms,
                    param: None,
                });
            }
            // `predict_linear(v[range], t)` puts the range vector first and the
            // horizon after it, the reverse of the other parameterised range
            // functions, so it is dispatched separately too.
            "predict_linear" => {
                self.expect(Token::LParen)?;
                let (inner, range_ms) = self.range_argument(name)?;
                self.expect(Token::Comma)?;
                let param = self.expression(0)?;
                self.expect(Token::RParen)?;
                return Ok(Expr::RangeFunc {
                    func: RangeFunc::PredictLinear,
                    expr: Box::new(inner),
                    range_ms,
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
                let (inner, range_ms) = self.range_argument(name)?;
                self.expect(Token::RParen)?;
                return Ok(Expr::RangeFunc {
                    func,
                    expr: Box::new(inner),
                    range_ms,
                    param: Some(Box::new(param)),
                });
            }
            let (inner, range_ms) = self.range_argument(name)?;
            self.expect(Token::RParen)?;
            return Ok(Expr::RangeFunc {
                func,
                expr: Box::new(inner),
                range_ms,
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

    /// Parses a function argument that must be a range vector, and returns it
    /// with its range. Prometheus accepts `metric[5m]` only.
    fn range_argument(&mut self, name: &str) -> Result<(Expr, i64)> {
        // Only a unary-level argument: `rate(a[5m]) * 2` must stop at the
        // closing parenthesis, not swallow the `* 2` into the argument (which
        // would then fail the range-vector check with a confusing message).
        let inner = self.expression(7)?;
        match inner {
            Expr::Range {
                selector,
                range_ms,
                offset_ms,
                at,
            } => Ok((
                Expr::Selector {
                    selector,
                    offset_ms,
                    at,
                },
                range_ms,
            )),
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
    pub labels: BTreeMap<String, String>,
    pub value: f64,
    /// The sample's own timestamp, when it came from a stored point. Derived
    /// values inherit it from their input so `timestamp()` can report it.
    pub sample_ts: Option<i64>,
}

impl Point {
    /// The identity used for exact vector matching: every label except
    /// `__name__`, as Prometheus does.
    fn signature(&self) -> Vec<(String, String)> {
        self.labels
            .iter()
            .filter(|(name, _)| name.as_str() != "__name__")
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }
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
    fn push(&mut self, labels: BTreeMap<String, String>, value: f64) {
        self.0.push(Point {
            labels,
            value,
            sample_ts: None,
        });
    }

    fn push_sample(&mut self, labels: BTreeMap<String, String>, value: f64, sample_ts: i64) {
        self.0.push(Point {
            labels,
            value,
            sample_ts: Some(sample_ts),
        });
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// One series' samples inside a range, ascending by timestamp.
#[derive(Debug, Clone, Default)]
pub struct RangeSeries {
    pub labels: BTreeMap<String, String>,
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
            } => self.binary(*op, lhs, rhs, *ret_bool, at_ms),
            Expr::Aggregation {
                op,
                expr,
                param,
                grouping,
            } => self.aggregate(*op, expr, param.as_deref(), grouping.as_ref(), at_ms),
            Expr::RangeFunc {
                func,
                expr,
                range_ms,
                param,
            } => self.range_func(*func, expr, *range_ms, param.as_deref(), at_ms),
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

    fn matrix_of(&self, expr: &Expr, range_ms: i64, at_ms: i64) -> Result<Matrix> {
        match expr {
            Expr::Selector {
                selector,
                offset_ms,
                at: pinned,
            } => {
                let at = self.resolve_at(*pinned, at_ms) - offset_ms;
                Ok(self.select_range(selector, at, range_ms))
            }
            Expr::Range { .. } => bad("a range vector is only valid as a function argument"),
            other => bad(format!(
                "expected a range vector such as `metric[5m]`, got {}",
                render(other)
            )),
        }
    }

    fn binary(
        &self,
        op: BinaryOp,
        lhs: &Expr,
        rhs: &Expr,
        ret_bool: bool,
        at_ms: i64,
    ) -> Result<Instant> {
        let left = self.eval(lhs, at_ms)?;
        let right = self.eval(rhs, at_ms)?;
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
                vector: self.binary_vectors(op, &left.vector, &right.vector, ret_bool)?,
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

    fn binary_vectors(
        &self,
        op: BinaryOp,
        lhs: &Vector,
        rhs: &Vector,
        ret_bool: bool,
    ) -> Result<Vector> {
        let mut out = Vector::default();
        if op.is_set() {
            let mut right: HashMap<Vec<(String, String)>, &Point> = HashMap::new();
            for point in &rhs.0 {
                right.insert(point.signature(), point);
            }
            for point in &lhs.0 {
                let signature = point.signature();
                match op {
                    BinaryOp::And => {
                        if right.contains_key(&signature) {
                            out.push(point.labels.clone(), point.value);
                        }
                    }
                    BinaryOp::Unless => {
                        if !right.contains_key(&signature) {
                            out.push(point.labels.clone(), point.value);
                        }
                    }
                    BinaryOp::Or => {
                        if !right.contains_key(&signature) {
                            out.push(point.labels.clone(), point.value);
                        }
                    }
                    _ => unreachable!("checked is_set"),
                }
            }
            if op == BinaryOp::Or {
                let left: HashSet<Vec<(String, String)>> =
                    lhs.0.iter().map(|point| point.signature()).collect();
                for point in &rhs.0 {
                    if !left.contains(&point.signature()) {
                        out.push(point.labels.clone(), point.value);
                    }
                }
            }
            return Ok(out);
        }

        let mut right: HashMap<Vec<(String, String)>, &Point> = HashMap::new();
        for point in &rhs.0 {
            right.insert(point.signature(), point);
        }
        for point in &lhs.0 {
            let Some(other) = right.get(&point.signature()) else {
                continue;
            };
            let value = op.apply(point.value, other.value);
            if op.is_comparison() && !ret_bool {
                if value == 1.0 {
                    out.push(point.labels.clone(), point.value);
                }
                continue;
            }
            let mut labels = point.labels.clone();
            if !op.is_comparison() {
                labels.remove("__name__");
            }
            out.push(labels, value);
        }
        Ok(out)
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
        range_ms: i64,
        param: Option<&Expr>,
        at_ms: i64,
    ) -> Result<Instant> {
        let matrix = self.matrix_of(expr, range_ms, at_ms)?;
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

/// The values of a series' samples, for the aggregation-over-time functions.
fn values(samples: &[SamplePoint]) -> Vec<f64> {
    samples.iter().map(|s| s.value).collect()
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
        // `default` and the clamps are handled before they reach here.
        MathFunc::Timestamp | MathFunc::ClampMin | MathFunc::ClampMax | MathFunc::Default => value,
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
    let mut series: HashMap<Vec<(String, String)>, (BTreeMap<String, String>, Vec<(i64, f64)>)> =
        HashMap::new();

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
        assert!(parse_err("a + on(instance) b").contains("vector matching"));
        assert!(parse_err(r#"label_replace(a, "x", "$1", "y", "(.*)")"#).contains("label_replace"));
        assert!(parse_err("a[5m:1m]").contains("subquer"));
        assert!(parse_err("nonsense_func(a)").contains("unknown function"));
        assert!(parse_err("rate(a)").contains("range vector"));
        assert!(parse_err("a{").contains("label name"));
        assert!(parse_err("").contains("empty"));
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
}
