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

//! Merge callbacks for the indexDB row families (ADR-0001 §3.7).
//!
//! The indexDB registers a callback per row shape. When two flushed parts are
//! merged the callback receives the prefix (the bytes before the value) and the
//! existing rows sharing that prefix. It returns the rows to keep, deduplicated
//! by their series IDs. The fixed-width posting rows mean the callback rewrites
//! only the matching rows; neighboring rows are untouched.

use crate::mergeset::{indexdb::ns, series_id_set::SeriesIdSet};

/// One posting row: the value (terminated key) and its series IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingRow {
    pub value: Vec<u8>,
    pub ids: SeriesIdSet,
}

/// Result of a merge: the deduplicated rows to emit for one prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeResult {
    pub rows: Vec<PostingRow>,
}

/// Callback for the tag/date posting families (namespaces 1, 5, 6).
///
/// `prefix` is the bytes between the namespace byte and the value (empty for
/// ordinary tag rows); `rows` are the existing rows sharing that prefix.
/// Returns the rows to keep, deduplicated per value. The callback may split a
/// row that exceeds the per-row cap, but must not reorder IDs.
pub fn merge_posting_rows(prefix: &[u8], rows: Vec<PostingRow>) -> MergeResult {
    let _ = prefix;
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut by_value: std::collections::HashMap<Vec<u8>, SeriesIdSet> =
        std::collections::HashMap::new();
    for row in rows {
        if !by_value.contains_key(&row.value) {
            order.push(row.value.clone());
        }
        let entry = by_value.entry(row.value).or_default();
        for id in row.ids.ids() {
            entry.insert(*id);
        }
    }
    let rows = order
        .into_iter()
        .map(|value| PostingRow {
            ids: by_value.remove(&value).expect("just inserted"),
            value,
        })
        .collect();
    MergeResult { rows }
}

/// Callback for the name->TSID family (namespaces 0 and 7).
///
/// Returns the single TSID row for the canonical name; duplicate registrations
/// of the same name collapse to the first TSID.
pub fn merge_name_tsid_rows(rows: Vec<Vec<u8>>) -> Option<Vec<u8>> {
    rows.into_iter().next()
}

/// Callback for the series-ID->TSID family (namespace 2).
///
/// Returns the single TSID for a series ID.
pub fn merge_series_id_tsid_rows(rows: Vec<Vec<u8>>) -> Option<Vec<u8>> {
    rows.into_iter().next()
}

/// Callback for the series-ID->name family (namespace 3).
///
/// Returns the canonical name bytes for a series ID.
pub fn merge_series_id_name_rows(rows: Vec<Vec<u8>>) -> Option<Vec<u8>> {
    rows.into_iter().next()
}

/// Callback for the date->series family (namespace 5).
///
/// Deduplicates the series IDs across rows for the same date.
pub fn merge_date_series_rows(rows: Vec<(u64, SeriesIdSet)>) -> SeriesIdSet {
    let mut out = SeriesIdSet::new();
    for (_, ids) in rows {
        out.extend_from(&ids);
    }
    out
}

/// Namespace 4 tombstone callback: a deleted series stays deleted.
pub fn merge_deleted_rows() -> bool {
    true
}

/// Namespace 0 marker used by [`merge_name_tsid_rows`] callers.
pub const NAMESPACE_METRIC_NAME: u8 = ns::METRIC_NAME;

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(xs: &[u64]) -> SeriesIdSet {
        SeriesIdSet::from_ids(xs.iter().copied())
    }

    #[test]
    fn test_merge_posting_rows_dedups_ids_per_value() {
        let rows = vec![
            PostingRow {
                value: b"api".to_vec(),
                ids: ids(&[1, 2, 3]),
            },
            PostingRow {
                value: b"api".to_vec(),
                ids: ids(&[3, 4]),
            },
            PostingRow {
                value: b"web".to_vec(),
                ids: ids(&[5]),
            },
        ];
        let merged = merge_posting_rows(b"", rows);
        assert_eq!(merged.rows.len(), 2);
        assert_eq!(merged.rows[0].value, b"api");
        assert_eq!(merged.rows[0].ids.ids(), &[1, 2, 3, 4]);
        assert_eq!(merged.rows[1].value, b"web");
        assert_eq!(merged.rows[1].ids.ids(), &[5]);
    }

    #[test]
    fn test_merge_name_tsid_keeps_first() {
        let tsid = [0xAB; 24];
        assert_eq!(
            merge_name_tsid_rows(vec![tsid.to_vec(), tsid.to_vec()]),
            Some(tsid.to_vec())
        );
    }

    #[test]
    fn test_merge_date_series_unions_ids() {
        let rows = vec![(20000, ids(&[1, 2])), (20000, ids(&[2, 3]))];
        assert_eq!(merge_date_series_rows(rows).ids(), &[1, 2, 3]);
    }

    #[test]
    fn test_merge_deleted_is_sticky() {
        assert!(merge_deleted_rows());
    }
}
