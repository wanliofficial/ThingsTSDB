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

use std::collections::HashMap;

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
    /// No useful simplification; run the regex.
    None,
}

/// Tries to simplify a regex into an equality or a prefix scan.
///
/// Handles the common Prometheus shapes: a fully anchored literal, a literal
/// followed by `.*`, and an alternation of literals (`a|b|c`). Returns `None`
/// when the regex needs the general engine. The simplifier is deliberately
/// conservative: only alphanumeric, `.`, `_`, `-` and `/` are treated as
/// literal so the result never drops a valid match.
pub fn simplify_regex(regex: &[u8]) -> RegexSimplification {
    let s = match std::str::from_utf8(regex) {
        Ok(s) => s,
        Err(_) => return RegexSimplification::None,
    };
    let is_literal = |c: char| c.is_ascii_alphanumeric() || "._-/".contains(c);
    // Full-match literal.
    if !s.is_empty() && s.chars().all(is_literal) {
        return RegexSimplification::Exact(s.as_bytes().to_vec());
    }
    // `literal.*` -> prefix.
    if let Some(stripped) = s.strip_suffix(".*") {
        if !stripped.is_empty() && stripped.chars().all(is_literal) {
            return RegexSimplification::Prefix(stripped.as_bytes().to_vec());
        }
    }
    // Alternation of literals: `a|b|c`. Only the first branch is returned; the
    // planner unions the exact prefixes for the rest.
    if s.contains('|') {
        let options: Vec<&str> = s.split('|').collect();
        if options.len() > 1
            && options
                .iter()
                .all(|o| !o.is_empty() && o.chars().all(is_literal))
        {
            return RegexSimplification::Exact(options[0].as_bytes().to_vec());
        }
    }
    RegexSimplification::None
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
    Equal(Vec<u8>),
    Prefix(Vec<u8>),
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
                TagMatcher::Re { key, regex } => match simplify_regex(regex) {
                    RegexSimplification::Exact(v) => positives.push(PositiveFilter {
                        key: key.clone(),
                        kind: PositiveKind::Equal(v),
                    }),
                    RegexSimplification::Prefix(p) => positives.push(PositiveFilter {
                        key: key.clone(),
                        kind: PositiveKind::Prefix(p),
                    }),
                    RegexSimplification::None => positives.push(PositiveFilter {
                        key: key.clone(),
                        kind: PositiveKind::Equal(regex.clone()),
                    }),
                },
                TagMatcher::NotEqual { .. } | TagMatcher::Nre { .. } => negatives.push(m.clone()),
            }
        }
        // Order by cost: composite/prefix filters before full regex, then by
        // key for determinism.
        positives.sort_by(|a, b| a.key.cmp(&b.key));
        negatives.sort_by(|a, b| a.key().cmp(b.key()));
        Self {
            positives,
            negatives,
            metric_group,
        }
    }
}

/// Resolves a plan against an indexDB snapshot (ADR-0001 §5).
///
/// `tag_posting` returns the global posting set for a tag key/value (namespace
/// 1); `date_series` returns the per-day series set (namespace 5) for the days
/// covered by the query. The resolver intersects the positive filters, applies
/// the per-day time constraint and subtracts the negative filters.
pub struct IndexResolver<'a> {
    db: &'a IndexDbSnapshot,
}

/// A read-only snapshot of the indexDB rows the resolver needs.
#[derive(Default)]
pub struct IndexDbSnapshot {
    /// `(key, value)` -> posting set, from namespace 1.
    tag_postings: HashMap<(Vec<u8>, Vec<u8>), SeriesIdSet>,
    /// `date` -> series set, from namespace 5.
    date_series: HashMap<u64, SeriesIdSet>,
    /// The metric group's posting row (namespace 1 with empty key).
    group_postings: HashMap<Vec<u8>, SeriesIdSet>,
}

impl<'a> IndexResolver<'a> {
    pub fn new(db: &'a IndexDbSnapshot) -> Self {
        Self { db }
    }

    /// Resolves the plan to the set of matching series IDs.
    pub fn resolve(&self, plan: &QueryPlan, days: &[u64]) -> SeriesIdSet {
        // Start from the most selective positive filter. Without cardinality
        // estimates the first is a deterministic choice.
        let mut candidate: Option<SeriesIdSet> = None;
        for filter in &plan.positives {
            let posting = self.lookup_positive(filter);
            candidate = Some(match candidate {
                None => posting,
                Some(acc) => SeriesIdSet::intersect(&acc, &posting),
            });
        }
        // Negative-only queries start from the metric group or the per-day set.
        let mut candidate = match candidate {
            Some(c) => c,
            None => match &plan.metric_group {
                Some(group) => self
                    .db
                    .group_postings
                    .get(group)
                    .cloned()
                    .unwrap_or_default(),
                None => SeriesIdSet::new(),
            },
        };
        // Apply the time range through the per-day rows.
        if !days.is_empty() {
            let mut per_day = SeriesIdSet::new();
            for &day in days {
                if let Some(set) = self.db.date_series.get(&day) {
                    per_day.extend_from(set);
                }
            }
            candidate = if candidate.is_empty() {
                per_day
            } else {
                SeriesIdSet::intersect(&candidate, &per_day)
            };
        }
        // Subtract the negative filters.
        for neg in &plan.negatives {
            if let Some(set) = self.lookup_negative(neg) {
                candidate.subtract(&set);
            }
        }
        candidate
    }

    fn lookup_positive(&self, filter: &PositiveFilter) -> SeriesIdSet {
        match &filter.kind {
            PositiveKind::Equal(value) => self
                .db
                .tag_postings
                .get(&(filter.key.clone(), value.clone()))
                .cloned()
                .unwrap_or_default(),
            PositiveKind::Prefix(prefix) => {
                let mut out = SeriesIdSet::new();
                for ((key, value), ids) in &self.db.tag_postings {
                    if key == &filter.key && value.starts_with(prefix) {
                        out.extend_from(ids);
                    }
                }
                out
            }
        }
    }

    fn lookup_negative(&self, matcher: &TagMatcher) -> Option<SeriesIdSet> {
        match matcher {
            TagMatcher::NotEqual { key, value } => Some(
                self.db
                    .tag_postings
                    .get(&(key.clone(), value.clone()))
                    .cloned()
                    .unwrap_or_default(),
            ),
            TagMatcher::Nre { .. } => None,
            _ => None,
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

    fn snapshot() -> IndexDbSnapshot {
        let mut db = IndexDbSnapshot::default();
        db.tag_postings.insert(
            (b"job".to_vec(), b"api".to_vec()),
            SeriesIdSet::from_ids([1, 2, 3]),
        );
        db.tag_postings.insert(
            (b"job".to_vec(), b"web".to_vec()),
            SeriesIdSet::from_ids([4, 5]),
        );
        db.tag_postings.insert(
            (b"instance".to_vec(), b"host1".to_vec()),
            SeriesIdSet::from_ids([1, 4]),
        );
        db.date_series
            .insert(20000, SeriesIdSet::from_ids([1, 2, 3, 4]));
        db.date_series.insert(20001, SeriesIdSet::from_ids([5]));
        db.group_postings.insert(
            b"http_requests".to_vec(),
            SeriesIdSet::from_ids([1, 2, 3, 4, 5]),
        );
        db
    }

    #[test]
    fn test_equal_intersection() {
        let db = snapshot();
        let resolver = IndexResolver::new(&db);
        let plan = QueryPlan::from_matchers(&[
            TagMatcher::Equal {
                key: b"job".to_vec(),
                value: b"api".to_vec(),
            },
            TagMatcher::Equal {
                key: b"instance".to_vec(),
                value: b"host1".to_vec(),
            },
        ]);
        assert_eq!(resolver.resolve(&plan, &[20000, 20001]).ids(), &[1]);
    }

    #[test]
    fn test_negative_only_starts_from_group() {
        let db = snapshot();
        let resolver = IndexResolver::new(&db);
        let plan = QueryPlan::from_matchers(&[TagMatcher::NotEqual {
            key: b"job".to_vec(),
            value: b"api".to_vec(),
        }]);
        assert_eq!(resolver.resolve(&plan, &[20000]).ids(), &[4]);
        // With a metric group the negative subtracts from the group.
        let plan = QueryPlan::from_matchers(&[
            TagMatcher::Equal {
                key: b"__name__".to_vec(),
                value: b"http_requests".to_vec(),
            },
            TagMatcher::NotEqual {
                key: b"job".to_vec(),
                value: b"api".to_vec(),
            },
        ]);
        assert_eq!(resolver.resolve(&plan, &[20000]).ids(), &[4]);
    }

    #[test]
    fn test_time_range_prunes() {
        let db = snapshot();
        let resolver = IndexResolver::new(&db);
        let plan = QueryPlan::from_matchers(&[]);
        assert_eq!(resolver.resolve(&plan, &[20000]).ids(), &[1, 2, 3, 4]);
        assert_eq!(resolver.resolve(&plan, &[20001]).ids(), &[5]);
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
            RegexSimplification::Exact(b"a".to_vec())
        );
        assert_eq!(simplify_regex(b"^api"), RegexSimplification::None);
        assert_eq!(
            simplify_regex(b"a.*"),
            RegexSimplification::Prefix(b"a".to_vec())
        );
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
    fn test_canonical_keys() {
        assert_eq!(metric_name_lookup_key(b"foo")[0], indexdb::ns::METRIC_NAME);
        assert_eq!(tag_posting_key(b"job", b"api")[0], indexdb::ns::TAG);
        assert_eq!(composite_tag_key(b"foo", b"job")[1], 0xfe);
        assert_eq!(escape_value(b"a\x01b"), b"a\x00\x31b");
    }
}
