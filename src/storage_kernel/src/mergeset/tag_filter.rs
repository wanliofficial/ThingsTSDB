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

/// Tests whether `value` matches the regex under Prometheus full-match
/// semantics (`^(?:expr)$`). Used as the fallback when simplification fails.
pub fn regex_matches(regex: &[u8], value: &[u8]) -> bool {
    let Ok(pattern) = std::str::from_utf8(regex) else {
        return false;
    };
    let Ok(re) = regex::bytes::Regex::new(&format!("^(?:{pattern})$")) else {
        return false;
    };
    re.is_match(value)
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

/// Resolves a plan against the rows of one indexDB (ADR-0001 §5).
///
/// Every namespace is seeked by prefix: the metric-group row gives the cross-
/// metric narrowing, the positive filters intersect their posting lists, the
/// per-day set applies the time range and the negatives are subtracted last.
pub struct IndexResolver<'a, S: ?Sized> {
    db: &'a S,
}

#[allow(clippy::needless_lifetimes)]
impl<'a, S: RowSource + ?Sized> IndexResolver<'a, S> {
    pub fn new(db: &'a S) -> Self {
        Self { db }
    }

    /// Resolves the plan to the set of matching series IDs.
    pub fn resolve(&self, plan: &QueryPlan, days: &[u64]) -> Result<SeriesIdSet, String> {
        let mut candidate: Option<SeriesIdSet> = None;
        // The metric group narrows every namespace: ns1 postings are global,
        // so without it a filter would return series of other metrics.
        if let Some(group) = &plan.metric_group {
            candidate = Some(self.group_postings(group)?);
        }
        for filter in &plan.positives {
            let posting = self.lookup_positive(filter)?;
            candidate = Some(match candidate {
                None => posting,
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
                per_day.extend_from(&self.day_series(day)?);
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

    fn group_postings(&self, group: &[u8]) -> Result<SeriesIdSet, String> {
        let prefix = indexdb::group_posting_row_prefix(group);
        self.postings_exact(&prefix)
    }

    fn day_series(&self, day: u64) -> Result<SeriesIdSet, String> {
        let prefix = indexdb::date_series_prefix(day);
        // A day set can have one row per series, so the prefix scan is the only
        // way to read it; the rows are `[ns][date][seriesID]`.
        let mut ids = Vec::new();
        if let Ok(rows) = self.db.search_prefix(&prefix) {
            for row in rows {
                if let Some((_, id)) = indexdb::decode_date_series_row(&row) {
                    ids.push(id);
                }
            }
        }
        Ok(SeriesIdSet::from_ids(ids))
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

    fn lookup_positive(&self, filter: &PositiveFilter) -> Result<SeriesIdSet, String> {
        match &filter.kind {
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
                let (_, ids) = self.postings(&indexdb::tag_key_prefix(&filter.key), |value| {
                    regex_matches(regex, value)
                })?;
                Ok(ids)
            }
        }
    }

    fn lookup_negative(&self, matcher: &TagMatcher) -> Result<SeriesIdSet, String> {
        match matcher {
            TagMatcher::NotEqual { key, value } => {
                let prefix = indexdb::posting_row_prefix(key, value);
                self.postings_exact(&prefix)
            }
            TagMatcher::Nre { key, regex } => {
                let (_, ids) = self.postings(&indexdb::tag_key_prefix(key), |value| {
                    regex_matches(regex, value)
                })?;
                Ok(ids)
            }
            TagMatcher::Equal { .. } | TagMatcher::Re { .. } => Ok(SeriesIdSet::new()),
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
}
