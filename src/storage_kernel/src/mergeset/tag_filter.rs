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
// specific language governing permissions and
// limitations under the License.

//! A compiled tag filter and the query planner over an indexDB (ADR-0001 §5).
//!
//! Matchers compile to byte-level predicates over the canonical index rows.
//! The planner simplifies regexes, orders filters by cost, intersects the
//! posting sets and applies the time range through the per-day rows.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::mergeset::{
    indexdb::{self, append_tag, composite_key, escape},
    series_id_set::SeriesIdSet,
};

/// A tag matcher as accepted by the query planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagMatcher {
    /// `tag="value"`.
    Equal { key: Vec<u8>, value: Vec<u8> },
    /// `tag!="value"`.
    NotEqual { key: Vec<u8>, value: Vec<u8> },
    /// `tag=~"regex"`.
    Re { key: Vec<u8>, regex: Vec<u8> },
    /// `tag!~"regex"`.
    Nre { key: Vec<u8>, regex: Vec<u8> },
}

impl TagMatcher {
    /// Returns the tag key this matcher constrains.
    pub fn key(&self) -> &[u8] {
        match self {
            TagMatcher::Equal { key, .. }
            | TagMatcher::NotEqual { key, .. }
            | TagMatcher::Re { key, .. }
            | TagMatcher::Nre { key, .. } => key,
        }
    }

    /// Returns the match type in precedence order for cost sorting.
    pub fn match_type(&self) -> MatchType {
        match self {
            TagMatcher::Equal { .. } => MatchType::Equal,
            TagMatcher::NotEqual { .. } => MatchType::NotEqual,
            TagMatcher::Re { .. } => MatchType::Regex,
            TagMatcher::Nre { .. } => MatchType::Regex,
        }
    }
}

/// Match-type precedence for cost ordering (ADR-0001 §5 step 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MatchType {
    Equal,
    Regex,
    NotEqual,
}

/// A simplification result for a regex matcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegexSimplification {
    /// The regex matched the whole value exactly.
    Exact(Vec<u8>),
    /// A literal prefix that every match must start with.
    Prefix(Vec<u8>),
    /// An alternation of literals (`a|b|c`): every branch is exact.
    Alternation(Vec<Vec<u8>>),
    /// No useful simplification; run the regex.
    None,
}

/// Tries to simplify a regex into an equality or a prefix scan.
///
/// Handles the common Prometheus shapes: a fully anchored literal, a literal
/// followed by `.*`, and an alternation of literals (`a|b|c`). Returns `None`
/// when the regex needs the general engine. The simplifier is deliberately
/// conservative: only alphanumerics, `_`, `-` and `/` are literal, and `.` is
/// the regex wildcard unless escaped (`\.`), so the result never drops a valid
/// match.
pub fn simplify_regex(regex: &[u8]) -> RegexSimplification {
    let s = match std::str::from_utf8(regex) {
        Ok(s) => s,
        Err(_) => return RegexSimplification::None,
    };
    if s.is_empty() {
        return RegexSimplification::None;
    }
    // Alternation of literals: `a|b|c`. Every branch is an exact value, so the
    // planner unions their postings.
    if s.contains('|') {
        let branches = split_alternation(s);
        if branches.len() > 1 {
            let mut values = Vec::with_capacity(branches.len());
            let mut all_literal = true;
            for branch in branches {
                let (literal, used) = literal_prefix(branch);
                if used == branch.len() && !literal.is_empty() {
                    values.push(literal);
                } else {
                    all_literal = false;
                    break;
                }
            }
            if all_literal {
                return RegexSimplification::Alternation(values);
            }
        }
    }
    let (literal, used) = literal_prefix(s);
    // Full-match literal, e.g. `api`.
    if used == s.len() && !literal.is_empty() {
        return RegexSimplification::Exact(literal);
    }
    // `literal.*` -> prefix.
    if !literal.is_empty() && &s[used..] == ".*" {
        return RegexSimplification::Prefix(literal);
    }
    RegexSimplification::None
}

/// Consumes the literal characters at the start of `s`, returning the bytes
/// they spell and the number of source bytes consumed.
///
/// Escaped punctuation (`\.`, `\-`, ...) is literal and is unescaped in the
/// output. Anything with regex meaning (`.`, `*`, `[`, `(`, ...) stops the
/// scan, so the caller falls back to the regex engine.
fn literal_prefix(s: &str) -> (Vec<u8>, usize) {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' && i + 1 < bytes.len() {
            let next = bytes[i + 1];
            if next.is_ascii() && !next.is_ascii_alphanumeric() {
                out.push(next);
                i += 2;
                continue;
            }
            break;
        }
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'/') {
            out.push(b);
            i += 1;
            continue;
        }
        break;
    }
    (out, i)
}

/// Splits on unescaped `|` characters.
fn split_alternation(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'|' => {
                parts.push(&s[start..i]);
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Compiles a regex under Prometheus full-match semantics (`^(?:expr)$`).
///
/// The planner compiles once per matcher and reuses the handle across every
/// value of a tag key; compiling inside the per-value closure made a regex
/// filter compile one regex per row.
pub fn compile_regex(regex: &[u8]) -> Option<regex::bytes::Regex> {
    let pattern = std::str::from_utf8(regex).ok()?;
    regex::bytes::Regex::new(&format!("^(?:{pattern})$")).ok()
}

/// Tests whether `value` matches the regex under Prometheus full-match
/// semantics (`^(?:expr)$`). Used as the fallback when simplification fails.
pub fn regex_matches(regex: &[u8], value: &[u8]) -> bool {
    compile_regex(regex).is_some_and(|re| re.is_match(value))
}

/// A compiled positive filter the planner can seek directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositiveFilter {
    pub key: Vec<u8>,
    pub kind: PositiveKind,
}

/// The seekable form of a positive matcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositiveKind {
    /// One exact value: seek the posting row of that pair.
    Equal(Vec<u8>),
    /// A literal prefix: seek the tag key and keep the values that start with
    /// it.
    Prefix(Vec<u8>),
    /// An alternation of exact values (`a|b|c`): union their postings.
    AnyOf(Vec<Vec<u8>>),
    /// A general regex that no simplification covers: seek the tag key and run
    /// the engine over its values.
    Regex(Vec<u8>),
}

/// The planned query: the positive filters to seek, the negative filters to
/// subtract and the metric-group filter when the query is name-only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub positives: Vec<PositiveFilter>,
    pub negatives: Vec<TagMatcher>,
    /// The metric group from a `{__name__="foo"}` filter, when present.
    pub metric_group: Option<Vec<u8>>,
}

impl QueryPlan {
    /// Builds a plan from matchers, applying the simplification and ordering
    /// rules of ADR-0001 §5.
    pub fn from_matchers(matchers: &[TagMatcher]) -> Self {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        let mut metric_group = None;
        for m in matchers {
            match m {
                TagMatcher::Equal { key, value } if key == b"__name__" => {
                    metric_group = Some(value.clone());
                }
                TagMatcher::Equal { key, value } => positives.push(PositiveFilter {
                    key: key.clone(),
                    kind: PositiveKind::Equal(value.clone()),
                }),
                TagMatcher::Re { key, regex } => {
                    let kind = match simplify_regex(regex) {
                        RegexSimplification::Exact(v) => PositiveKind::Equal(v),
                        RegexSimplification::Prefix(p) => PositiveKind::Prefix(p),
                        RegexSimplification::Alternation(vs) => PositiveKind::AnyOf(vs),
                        RegexSimplification::None => PositiveKind::Regex(regex.clone()),
                    };
                    positives.push(PositiveFilter {
                        key: key.clone(),
                        kind,
                    });
                }
                TagMatcher::NotEqual { .. } | TagMatcher::Nre { .. } => negatives.push(m.clone()),
            }
        }
        // Order by cost: composite/prefix filters before full regex, then by
        // key for determinism. A seekable equality is cheaper than a tag-key
        // scan, which is cheaper than running the regex engine.
        positives.sort_by(|a, b| a.cost().cmp(&b.cost()).then_with(|| a.key.cmp(&b.key)));
        negatives.sort_by(|a, b| a.key().cmp(b.key()));
        Self {
            positives,
            negatives,
            metric_group,
        }
    }
}

impl PositiveFilter {
    /// Relative cost of resolving this filter, from cheapest to most expensive.
    fn cost(&self) -> u8 {
        match &self.kind {
            PositiveKind::Equal(_) | PositiveKind::AnyOf(_) => 0,
            PositiveKind::Prefix(_) => 1,
            PositiveKind::Regex(_) => 2,
        }
    }
}

/// Encodes the cache key of a positive filter.
///
/// The kind byte keeps equal/prefix/any-of/regex entries of the same key apart,
/// and every variable-length field is length-prefixed so that `key="a",
/// value="bc"` and `key="ab", value="c"` cannot collide.
fn filter_cache_key(filter: &PositiveFilter) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + filter.key.len());
    key.push(b'p');
    key.extend_from_slice(&(filter.key.len() as u32).to_be_bytes());
    key.extend_from_slice(&filter.key);
    match &filter.kind {
        PositiveKind::Equal(value) => {
            key.push(b'=');
            key.extend_from_slice(&(value.len() as u32).to_be_bytes());
            key.extend_from_slice(value);
        }
        PositiveKind::Prefix(prefix) => {
            key.push(b'>');
            key.extend_from_slice(&(prefix.len() as u32).to_be_bytes());
            key.extend_from_slice(prefix);
        }
        PositiveKind::Regex(regex) => {
            key.push(b'r');
            key.extend_from_slice(&(regex.len() as u32).to_be_bytes());
            key.extend_from_slice(regex);
        }
        PositiveKind::AnyOf(values) => {
            key.push(b'a');
            key.extend_from_slice(&(values.len() as u32).to_be_bytes());
            for value in values {
                key.extend_from_slice(&(value.len() as u32).to_be_bytes());
                key.extend_from_slice(value);
            }
        }
    }
    key
}

/// Appends `bytes` with a 32-bit length prefix.
fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Encodes a negative matcher for the result-cache key.
fn push_matcher(out: &mut Vec<u8>, matcher: &TagMatcher) {
    match matcher {
        TagMatcher::Equal { key, value } => {
            out.push(b'=');
            push_bytes(out, key);
            push_bytes(out, value);
        }
        TagMatcher::NotEqual { key, value } => {
            out.push(b'!');
            push_bytes(out, key);
            push_bytes(out, value);
        }
        TagMatcher::Re { key, regex } => {
            out.push(b'r');
            push_bytes(out, key);
            push_bytes(out, regex);
        }
        TagMatcher::Nre { key, regex } => {
            out.push(b'n');
            push_bytes(out, key);
            push_bytes(out, regex);
        }
    }
}

/// Encodes the cache key of a whole query: the plan plus the day range.
///
/// The result depends on the metric group, every positive and negative
/// matcher (in plan order, which is deterministic) and the days pushed down.
fn result_cache_key(plan: &QueryPlan, days: &[u64]) -> Vec<u8> {
    let mut key = Vec::with_capacity(32 + days.len() * 8);
    key.push(b'q');
    match &plan.metric_group {
        None => key.push(b'0'),
        Some(group) => {
            key.push(b'1');
            push_bytes(&mut key, group);
        }
    }
    key.extend_from_slice(&(plan.positives.len() as u32).to_be_bytes());
    for filter in &plan.positives {
        key.extend_from_slice(&filter_cache_key(filter));
    }
    key.extend_from_slice(&(plan.negatives.len() as u32).to_be_bytes());
    for matcher in &plan.negatives {
        push_matcher(&mut key, matcher);
    }
    key.extend_from_slice(&(days.len() as u32).to_be_bytes());
    for day in days {
        key.extend_from_slice(&day.to_be_bytes());
    }
    key
}

/// The namespace row lookups the resolver needs (ADR-0001 §4).
///
/// Implemented over a live mergeset [`Table`](crate::mergeset::table::Table) in
/// production and over an in-memory map in tests.
pub trait RowSource {
    /// All rows with `prefix`, sorted and deduplicated.
    fn search_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String>;
    /// All rows in `[start, end)`, sorted and deduplicated.
    fn search_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<Vec<u8>>, String>;
}

/// A budgeted, versioned cache of resolved posting sets (ADR-0001 §4.2).
///
/// A query materializes the metric-group row, the per-day series set and one
/// posting set per filter. Those sets only grow when new rows are written, so
/// they can be reused across queries as long as nothing changed: writes bump
/// the version, and an entry is only returned while its version matches.
///
/// The budget counts retained IDs, not bytes (`8 * ids` bytes), and the cache
/// is flushed when a new entry would exceed it.
pub struct FilterCache {
    version: AtomicU64,
    inner: Mutex<CacheInner>,
    budget_ids: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

#[derive(Default)]
struct CacheInner {
    retained: usize,
    entries: HashMap<Vec<u8>, (u64, Arc<SeriesIdSet>)>,
}

/// Retained IDs kept by an indexDB cache by default (8 bytes each, so 64 MiB).
pub const DEFAULT_CACHE_IDS: usize = 8 * 1024 * 1024;

impl FilterCache {
    pub fn new(budget_ids: usize) -> Self {
        Self {
            version: AtomicU64::new(0),
            inner: Mutex::new(CacheInner::default()),
            budget_ids,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// The current cache generation. A resolver snapshots it once and stamps
    /// every entry it inserts, so a concurrent write makes the whole
    /// generation stale in one atomic bump.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// Invalidates every entry. Called whenever new rows are written.
    pub fn invalidate(&self) {
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Number of retained IDs, exposed for observability and tests.
    pub fn retained_ids(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retained
    }

    fn get(&self, key: &[u8], version: u64) -> Option<Arc<SeriesIdSet>> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((entry_version, set)) = inner.entries.get(key) {
            if *entry_version == version {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(Arc::clone(set));
            }
        }
        // Drop the stale entry so a version bump never leaks memory.
        if let Some((_, stale)) = inner.entries.remove(key) {
            inner.retained = inner.retained.saturating_sub(stale.len());
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    fn put(&self, key: Vec<u8>, version: u64, set: Arc<SeriesIdSet>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.retained + set.len() > self.budget_ids {
            inner.entries.clear();
            inner.retained = 0;
        }
        if let Some((_, previous)) = inner.entries.insert(key, (version, set.clone())) {
            inner.retained = inner.retained.saturating_sub(previous.len());
        }
        inner.retained += set.len();
    }
}

impl Default for FilterCache {
    fn default() -> Self {
        Self::new(DEFAULT_CACHE_IDS)
    }
}

/// Resolves a plan against the rows of one indexDB (ADR-0001 §5).
///
/// Every namespace is seeked by prefix: the metric-group row gives the cross-
/// metric narrowing, the positive filters intersect their posting lists, the
/// per-day set applies the time range and the negatives are subtracted last.
/// An optional [`FilterCache`] reuses resolved sets across queries.
pub struct IndexResolver<'a, S: ?Sized> {
    db: &'a S,
    cache: Option<(&'a FilterCache, u64)>,
}

#[allow(clippy::needless_lifetimes)]
impl<'a, S: RowSource + ?Sized> IndexResolver<'a, S> {
    pub fn new(db: &'a S) -> Self {
        Self { db, cache: None }
    }

    /// Reuses posting-set resolutions through `cache`. The current cache
    /// generation is snapshotted here, so one query sees one consistent view.
    pub fn with_cache(mut self, cache: &'a FilterCache) -> Self {
        self.cache = Some((cache, cache.version()));
        self
    }

    /// Returns the cached set for `key`, or computes it and stores the result.
    fn resolve_cached(
        &self,
        key: Vec<u8>,
        compute: impl FnOnce() -> Result<SeriesIdSet, String>,
    ) -> Result<Arc<SeriesIdSet>, String> {
        if let Some((cache, version)) = self.cache {
            if let Some(set) = cache.get(&key, version) {
                return Ok(set);
            }
        }
        let set = Arc::new(compute()?);
        if let Some((cache, version)) = self.cache {
            cache.put(key, version, Arc::clone(&set));
        }
        Ok(set)
    }

    /// Resolves the plan to the set of matching series IDs.
    ///
    /// With a cache attached the whole answer is memoized too, so a repeated
    /// dashboard query does not re-intersect the per-filter sets. A result
    /// larger than half the budget is not stored: it would evict the filter
    /// sets it was built from on the next insert.
    pub fn resolve(&self, plan: &QueryPlan, days: &[u64]) -> Result<SeriesIdSet, String> {
        Ok((*self.resolve_arc(plan, days)?).clone())
    }

    /// Like [`IndexResolver::resolve`], but a cached result is handed back as
    /// an `Arc` so a large result set (millions of IDs) is not copied per
    /// query.
    pub fn resolve_arc(&self, plan: &QueryPlan, days: &[u64]) -> Result<Arc<SeriesIdSet>, String> {
        let Some((cache, version)) = self.cache else {
            return Ok(Arc::new(self.resolve_plan(plan, days)?));
        };
        let key = result_cache_key(plan, days);
        if let Some(set) = cache.get(&key, version) {
            return Ok(set);
        }
        let result = Arc::new(self.resolve_plan(plan, days)?);
        // A result larger than half the budget is not stored: it would evict
        // the filter sets it was built from on the next insert.
        if result.len() <= cache.budget_ids / 2 {
            cache.put(key, version, Arc::clone(&result));
        }
        Ok(result)
    }

    fn resolve_plan(&self, plan: &QueryPlan, days: &[u64]) -> Result<SeriesIdSet, String> {
        let mut candidate: Option<SeriesIdSet> = None;
        // The metric group narrows every namespace: ns1 postings are global,
        // so without it a filter would return series of other metrics.
        if let Some(group) = &plan.metric_group {
            candidate = Some((*self.group_postings(group)?).clone());
        }
        for filter in &plan.positives {
            let posting = self.lookup_positive(filter)?;
            candidate = Some(match candidate {
                None => (*posting).clone(),
                Some(acc) => SeriesIdSet::intersect(&acc, &posting),
            });
        }
        // Apply the time range through the per-day rows. An empty `days` means
        // the caller has no time constraint to push down.
        let per_day = if days.is_empty() {
            None
        } else {
            let mut per_day = SeriesIdSet::new();
            for &day in days {
                per_day.extend_from(self.day_series(day)?.as_ref());
            }
            Some(per_day)
        };
        let mut candidate = match (candidate, per_day) {
            // Nothing narrowed the query yet: the day set is the answer.
            (None, Some(per_day)) => per_day,
            (None, None) => SeriesIdSet::new(),
            (Some(c), None) => c,
            (Some(c), Some(per_day)) => SeriesIdSet::intersect(&c, &per_day),
        };
        // Subtract the negative filters.
        for neg in &plan.negatives {
            let excluded = self.lookup_negative(neg)?;
            candidate.subtract(&excluded);
        }
        Ok(candidate)
    }

    fn group_postings(&self, group: &[u8]) -> Result<Arc<SeriesIdSet>, String> {
        let prefix = indexdb::group_posting_row_prefix(group);
        let mut key = Vec::with_capacity(1 + prefix.len());
        key.push(b'e');
        key.extend_from_slice(&prefix);
        self.resolve_cached(key, || self.postings_exact(&prefix))
    }

    fn day_series(&self, day: u64) -> Result<Arc<SeriesIdSet>, String> {
        let prefix = indexdb::date_series_prefix(day);
        let mut key = Vec::with_capacity(9);
        key.push(b'd');
        key.extend_from_slice(&day.to_be_bytes());
        self.resolve_cached(key, || {
            // A day set can have one row per series (before a flush collapses
            // them) or one row for the whole day, so read every trailing ID.
            let mut ids = Vec::new();
            if let Ok(rows) = self.db.search_prefix(&prefix) {
                for row in rows {
                    if let Some(row_ids) = indexdb::decode_date_series_ids(&prefix, &row) {
                        ids.extend_from_slice(&row_ids);
                    }
                }
            }
            Ok(SeriesIdSet::from_ids(ids))
        })
    }

    /// Reads the posting rows of a tag key and unions their IDs, optionally
    /// keeping only the values `keep` accepts.
    ///
    /// `key_prefix` is `[ns][escaped key][0x02]`; the value is decoded from
    /// each row.
    fn postings(
        &self,
        key_prefix: &[u8],
        keep: impl Fn(&[u8]) -> bool,
    ) -> Result<(Vec<Vec<u8>>, SeriesIdSet), String> {
        let rows = self.db.search_prefix(key_prefix)?;
        let mut values = Vec::new();
        let mut ids = Vec::new();
        for row in &rows {
            let Some((value, row_ids)) = indexdb::decode_posting_row(key_prefix, row) else {
                continue;
            };
            if keep(&value) {
                values.push(value);
                ids.extend_from_slice(&row_ids);
            }
        }
        Ok((values, SeriesIdSet::from_ids(ids)))
    }

    /// Reads the IDs of the posting rows of one exact tag pair.
    ///
    /// `row_prefix` is `[ns][escaped key][0x02][escaped value][0x01]`, so the
    /// row is matched by the merged table and only the IDs are decoded.
    fn postings_exact(&self, row_prefix: &[u8]) -> Result<SeriesIdSet, String> {
        let rows = self.db.search_prefix(row_prefix)?;
        let mut ids = Vec::new();
        for row in &rows {
            if let Some(row_ids) = indexdb::decode_posting_ids(row_prefix, row) {
                ids.extend_from_slice(&row_ids);
            }
        }
        Ok(SeriesIdSet::from_ids(ids))
    }

    fn lookup_positive(&self, filter: &PositiveFilter) -> Result<Arc<SeriesIdSet>, String> {
        let cache_key = filter_cache_key(filter);
        self.resolve_cached(cache_key, || match &filter.kind {
            PositiveKind::Equal(value) => {
                let prefix = indexdb::posting_row_prefix(&filter.key, value);
                self.postings_exact(&prefix)
            }
            PositiveKind::AnyOf(values) => {
                let mut out = SeriesIdSet::new();
                for value in values {
                    let prefix = indexdb::posting_row_prefix(&filter.key, value);
                    out.extend_from(&self.postings_exact(&prefix)?);
                }
                Ok(out)
            }
            PositiveKind::Prefix(prefix_value) => {
                let (_, ids) = self.postings(&indexdb::tag_key_prefix(&filter.key), |value| {
                    value.starts_with(prefix_value)
                })?;
                Ok(ids)
            }
            PositiveKind::Regex(regex) => {
                let Some(re) = compile_regex(regex) else {
                    return Ok(SeriesIdSet::new());
                };
                let (_, ids) = self.postings(&indexdb::tag_key_prefix(&filter.key), |value| {
                    re.is_match(value)
                })?;
                Ok(ids)
            }
        })
    }

    fn lookup_negative(&self, matcher: &TagMatcher) -> Result<Arc<SeriesIdSet>, String> {
        match matcher {
            TagMatcher::NotEqual { key, value } => {
                let prefix = indexdb::posting_row_prefix(key, value);
                let mut cache_key = Vec::with_capacity(1 + prefix.len());
                cache_key.push(b'e');
                cache_key.extend_from_slice(&prefix);
                self.resolve_cached(cache_key, || self.postings_exact(&prefix))
            }
            TagMatcher::Nre { key, regex } => {
                let mut cache_key = Vec::with_capacity(2 + key.len() + regex.len());
                cache_key.push(b'r');
                cache_key.extend_from_slice(&(key.len() as u32).to_be_bytes());
                cache_key.extend_from_slice(key);
                cache_key.extend_from_slice(regex);
                self.resolve_cached(cache_key, || {
                    let Some(re) = compile_regex(regex) else {
                        return Ok(SeriesIdSet::new());
                    };
                    let (_, ids) =
                        self.postings(&indexdb::tag_key_prefix(key), |value| re.is_match(value))?;
                    Ok(ids)
                })
            }
            TagMatcher::Equal { .. } | TagMatcher::Re { .. } => Ok(Arc::new(SeriesIdSet::new())),
        }
    }
}

/// Builds the canonical index item key for a metric-group name lookup
/// (namespace 0 row), used by the ingestion lookup path.
pub fn metric_name_lookup_key(group: &[u8]) -> Vec<u8> {
    indexdb::metric_name_item(group)
}

/// Builds the canonical index item key for a tag posting row (namespace 1).
pub fn tag_posting_key(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = vec![indexdb::ns::TAG];
    append_tag(&mut out, key, value);
    out
}

/// Builds the canonical composite tag-key for a metric group + tag key.
pub fn composite_tag_key(group: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = vec![indexdb::ns::TAG];
    out.extend_from_slice(&composite_key(group, key));
    out
}

/// Escapes a key or value for index rows.
pub fn escape_value(src: &[u8]) -> Vec<u8> {
    let mut dst = Vec::with_capacity(src.len());
    escape(&mut dst, src);
    dst
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mergeset::indexdb;

    /// A `RowSource` over a sorted, deduplicated row list.
    struct MapRows(Vec<Vec<u8>>);

    impl MapRows {
        fn new(mut rows: Vec<Vec<u8>>) -> Self {
            rows.sort_unstable();
            rows.dedup();
            Self(rows)
        }
    }

    impl RowSource for MapRows {
        fn search_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, String> {
            Ok(self
                .0
                .iter()
                .filter(|r| r.starts_with(prefix))
                .cloned()
                .collect())
        }

        fn search_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<Vec<u8>>, String> {
            Ok(self
                .0
                .iter()
                .filter(|r| r.as_slice() >= start && r.as_slice() < end)
                .cloned()
                .collect())
        }
    }

    /// `(metric, tags, series id, days)`.
    type SeriesRow = (
        &'static [u8],
        &'static [(&'static [u8], &'static [u8])],
        u64,
        &'static [u64],
    );

    /// Builds the ns1/ns5 rows of two metrics:
    ///
    /// - `http_requests`: `{job=api, instance=host1}` = 1, `{job=api,
    ///   instance=host2}` = 2, `{job=web, instance=host1}` = 4;
    /// - `grpc_requests`: `{job=api, instance=host3}` = 3, `{job=web, instance=
    ///   host1}` = 5.
    fn rows() -> MapRows {
        let mut rows = Vec::new();
        let series: &[SeriesRow] = &[
            (
                b"http_requests",
                &[(b"job", b"api"), (b"instance", b"host1")],
                1,
                &[20000],
            ),
            (
                b"http_requests",
                &[(b"job", b"api"), (b"instance", b"host2")],
                2,
                &[20000],
            ),
            (
                b"grpc_requests",
                &[(b"job", b"api"), (b"instance", b"host3")],
                3,
                &[20000],
            ),
            (
                b"http_requests",
                &[(b"job", b"web"), (b"instance", b"host1")],
                4,
                &[20000],
            ),
            (
                b"grpc_requests",
                &[(b"job", b"web"), (b"instance", b"host1")],
                5,
                &[20001],
            ),
        ];
        for (metric, tags, id, days) in series {
            rows.push(indexdb::tag_posting_row(b"", metric, &[*id]));
            for (key, value) in tags.iter() {
                rows.push(indexdb::tag_posting_row(key, value, &[*id]));
            }
            for &day in days.iter() {
                rows.extend(indexdb::date_series_rows(day, &[*id]));
            }
        }
        MapRows::new(rows)
    }

    fn eq(key: &str, value: &str) -> TagMatcher {
        TagMatcher::Equal {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn name(group: &str) -> TagMatcher {
        TagMatcher::Equal {
            key: b"__name__".to_vec(),
            value: group.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_equal_intersection_and_metric_group() {
        let rows = rows();
        let resolver = IndexResolver::new(&rows);
        // Without the metric group, `job=api` spans both metrics.
        let plan = QueryPlan::from_matchers(&[eq("job", "api")]);
        assert_eq!(resolver.resolve(&plan, &[20000]).unwrap().ids(), &[1, 2, 3]);

        // The metric group narrows it to http_requests.
        let plan = QueryPlan::from_matchers(&[name("http_requests"), eq("job", "api")]);
        assert_eq!(resolver.resolve(&plan, &[20000]).unwrap().ids(), &[1, 2]);

        // Two positive filters intersect.
        let plan = QueryPlan::from_matchers(&[
            name("http_requests"),
            eq("job", "api"),
            eq("instance", "host1"),
        ]);
        assert_eq!(resolver.resolve(&plan, &[20000]).unwrap().ids(), &[1]);

        // A missing value matches nothing instead of everything.
        let plan = QueryPlan::from_matchers(&[name("http_requests"), eq("job", "missing")]);
        assert!(resolver.resolve(&plan, &[20000]).unwrap().is_empty());

        // A group the index does not know matches nothing.
        let plan = QueryPlan::from_matchers(&[name("unknown"), eq("job", "api")]);
        assert!(resolver.resolve(&plan, &[20000]).unwrap().is_empty());
    }

    #[test]
    fn test_negative_filters_subtract() {
        let rows = rows();
        let resolver = IndexResolver::new(&rows);
        let plan = QueryPlan::from_matchers(&[
            name("http_requests"),
            TagMatcher::NotEqual {
                key: b"job".to_vec(),
                value: b"api".to_vec(),
            },
        ]);
        assert_eq!(resolver.resolve(&plan, &[20000]).unwrap().ids(), &[4]);

        // `!~` subtracts every value the regex matches.
        let plan = QueryPlan::from_matchers(&[
            name("http_requests"),
            TagMatcher::Nre {
                key: b"job".to_vec(),
                regex: b"api|web".to_vec(),
            },
        ]);
        assert!(resolver.resolve(&plan, &[20000]).unwrap().is_empty());
    }

    #[test]
    fn test_time_range_prunes() {
        let rows = rows();
        let resolver = IndexResolver::new(&rows);
        let plan = QueryPlan::from_matchers(&[name("grpc_requests")]);
        assert_eq!(resolver.resolve(&plan, &[20000]).unwrap().ids(), &[3]);
        assert_eq!(resolver.resolve(&plan, &[20001]).unwrap().ids(), &[5]);
        assert_eq!(
            resolver.resolve(&plan, &[20000, 20001]).unwrap().ids(),
            &[3, 5]
        );
        // An empty day list means "no time constraint".
        assert_eq!(resolver.resolve(&plan, &[]).unwrap().ids(), &[3, 5],);
    }

    #[test]
    fn test_prefix_and_regex_filters() {
        let rows = rows();
        let resolver = IndexResolver::new(&rows);
        let plan = QueryPlan::from_matchers(&[
            name("http_requests"),
            TagMatcher::Re {
                key: b"instance".to_vec(),
                regex: b"host.*".to_vec(),
            },
        ]);
        assert_eq!(resolver.resolve(&plan, &[20000]).unwrap().ids(), &[1, 2, 4]);

        let plan = QueryPlan::from_matchers(&[
            name("http_requests"),
            TagMatcher::Re {
                key: b"instance".to_vec(),
                regex: b"host[12]".to_vec(),
            },
        ]);
        assert_eq!(resolver.resolve(&plan, &[20000]).unwrap().ids(), &[1, 2, 4]);

        let plan = QueryPlan::from_matchers(&[
            name("http_requests"),
            TagMatcher::Re {
                key: b"instance".to_vec(),
                regex: b"host2".to_vec(),
            },
        ]);
        assert_eq!(resolver.resolve(&plan, &[20000]).unwrap().ids(), &[2]);
    }

    #[test]
    fn test_simplify_regex() {
        assert_eq!(
            simplify_regex(b"api"),
            RegexSimplification::Exact(b"api".to_vec())
        );
        assert_eq!(
            simplify_regex(b"api.*"),
            RegexSimplification::Prefix(b"api".to_vec())
        );
        assert_eq!(
            simplify_regex(b"a|b|c"),
            RegexSimplification::Alternation(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
        assert_eq!(simplify_regex(b"^api"), RegexSimplification::None);
        assert_eq!(
            simplify_regex(b"a.*"),
            RegexSimplification::Prefix(b"a".to_vec())
        );
        // `.` is a wildcard, so a dotted regex must use the engine.
        assert_eq!(simplify_regex(b"2.."), RegexSimplification::None);
        assert_eq!(simplify_regex(b"a.c"), RegexSimplification::None);
        // Escaped dots and pipes are literal.
        assert_eq!(
            simplify_regex(b"1\\.2\\.3"),
            RegexSimplification::Exact(b"1.2.3".to_vec())
        );
        assert_eq!(
            simplify_regex(b"a\\|b"),
            RegexSimplification::Exact(b"a|b".to_vec())
        );
        // A wildcard branch disables the alternation shortcut.
        assert_eq!(simplify_regex(b"a|b.*"), RegexSimplification::None);
    }

    #[test]
    fn test_regex_full_match_semantics() {
        assert!(regex_matches(b"api", b"api"));
        assert!(!regex_matches(b"api", b"apix"));
        assert!(regex_matches(b"a.*", b"abc"));
        assert!(regex_matches(b"a.c", b"abc"));
        assert!(!regex_matches(b"a.c", b"ab"));
    }

    #[test]
    fn test_name_only_becomes_metric_group() {
        let plan = QueryPlan::from_matchers(&[TagMatcher::Equal {
            key: b"__name__".to_vec(),
            value: b"http_requests".to_vec(),
        }]);
        assert_eq!(
            plan.metric_group.as_deref(),
            Some(b"http_requests".as_ref())
        );
        assert!(plan.positives.is_empty());
    }

    #[test]
    fn test_plan_orders_cheap_filters_first() {
        let plan = QueryPlan::from_matchers(&[
            TagMatcher::Re {
                key: b"a".to_vec(),
                regex: b"[0-9]+".to_vec(),
            },
            eq("b", "1"),
            TagMatcher::Re {
                key: b"c".to_vec(),
                regex: b"pre.*".to_vec(),
            },
        ]);
        let kinds: Vec<&PositiveKind> = plan.positives.iter().map(|f| &f.kind).collect();
        assert!(matches!(kinds[0], PositiveKind::Equal(_)));
        assert!(matches!(kinds[1], PositiveKind::Prefix(_)));
        assert!(matches!(kinds[2], PositiveKind::Regex(_)));
    }

    #[test]
    fn test_canonical_keys() {
        assert_eq!(metric_name_lookup_key(b"foo")[0], indexdb::ns::METRIC_NAME);
        assert_eq!(tag_posting_key(b"job", b"api")[0], indexdb::ns::TAG);
        assert_eq!(composite_tag_key(b"foo", b"job")[1], 0xfe);
        assert_eq!(escape_value(b"a\x01b"), b"a\x00\x31b");
    }

    #[test]
    fn test_resolver_arc_hit_shares_the_set() {
        let rows = rows();
        let cache = FilterCache::new(1024);
        let plan = QueryPlan::from_matchers(&[name("http_requests"), eq("job", "api")]);
        let first = IndexResolver::new(&rows)
            .with_cache(&cache)
            .resolve_arc(&plan, &[20000])
            .unwrap();
        let second = IndexResolver::new(&rows)
            .with_cache(&cache)
            .resolve_arc(&plan, &[20000])
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn test_filter_cache_key_is_collision_free() {
        let split = PositiveFilter {
            key: b"a".to_vec(),
            kind: PositiveKind::Equal(b"bc".to_vec()),
        };
        let joined = PositiveFilter {
            key: b"ab".to_vec(),
            kind: PositiveKind::Equal(b"c".to_vec()),
        };
        assert_ne!(filter_cache_key(&split), filter_cache_key(&joined));

        let same_key = [
            PositiveKind::Equal(b"a".to_vec()),
            PositiveKind::Prefix(b"a".to_vec()),
            PositiveKind::Regex(b"a".to_vec()),
            PositiveKind::AnyOf(vec![b"a".to_vec()]),
        ];
        let mut keys: Vec<Vec<u8>> = same_key
            .into_iter()
            .map(|kind| {
                filter_cache_key(&PositiveFilter {
                    key: b"k".to_vec(),
                    kind,
                })
            })
            .collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), 4);
    }

    #[test]
    fn test_filter_cache_counts_hits_and_misses() {
        let cache = FilterCache::new(1024);
        let set = Arc::new(SeriesIdSet::from_ids([1, 2, 3]));
        assert!(cache.get(b"k", 0).is_none());
        assert_eq!(cache.misses(), 1);
        cache.put(b"k".to_vec(), 0, Arc::clone(&set));
        let hit = cache.get(b"k", 0).expect("entry is live");
        assert_eq!(hit.ids(), set.ids());
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.retained_ids(), 3);
    }

    #[test]
    fn test_filter_cache_invalidates_on_version_bump() {
        let cache = FilterCache::new(1024);
        cache.put(b"k".to_vec(), 0, Arc::new(SeriesIdSet::from_ids([1, 2, 3])));
        cache.invalidate();
        assert_eq!(cache.version(), 1);
        // The stale entry is neither returned nor retained.
        assert!(cache.get(b"k", cache.version()).is_none());
        assert_eq!(cache.retained_ids(), 0);
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn test_filter_cache_evicts_when_over_budget() {
        let cache = FilterCache::new(4);
        cache.put(b"a".to_vec(), 0, Arc::new(SeriesIdSet::from_ids([1, 2])));
        cache.put(b"b".to_vec(), 0, Arc::new(SeriesIdSet::from_ids([3, 4])));
        assert_eq!(cache.retained_ids(), 4);
        // The third set would exceed the budget, so the cache starts over.
        cache.put(b"c".to_vec(), 0, Arc::new(SeriesIdSet::from_ids([5, 6])));
        assert_eq!(cache.retained_ids(), 2);
        assert!(cache.get(b"a", 0).is_none());
        assert!(cache.get(b"c", 0).is_some());
    }

    #[test]
    fn test_resolver_reuses_cached_sets() {
        let rows = rows();
        let cache = FilterCache::new(1024);
        let plan = QueryPlan::from_matchers(&[name("http_requests"), eq("job", "api")]);
        let first = IndexResolver::new(&rows)
            .with_cache(&cache)
            .resolve(&plan, &[20000])
            .unwrap();
        assert_eq!(first.ids(), &[1, 2]);
        let (misses, hits) = (cache.misses(), cache.hits());
        // One result-key miss plus the metric group, `job=api` and the day.
        assert_eq!((misses, hits), (4, 0));
        // The second identical query hits the memoized result: no filter set
        // is touched again.
        let second = IndexResolver::new(&rows)
            .with_cache(&cache)
            .resolve(&plan, &[20000])
            .unwrap();
        assert_eq!(second.ids(), &[1, 2]);
        assert_eq!(cache.misses(), misses);
        assert_eq!(cache.hits(), hits + 1);
        // A different day range is a different result, but the metric-group
        // and `job=api` sets are reused.
        let third = IndexResolver::new(&rows)
            .with_cache(&cache)
            .resolve(&plan, &[20001])
            .unwrap();
        assert!(third.is_empty());
        // A result-key + day miss, but the metric-group and `job=api` hits.
        assert_eq!(cache.misses(), misses + 2);
        assert_eq!(cache.hits(), hits + 1 + 2);
    }
}
