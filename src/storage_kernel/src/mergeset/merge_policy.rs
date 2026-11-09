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

//! Merge-part selection (ADR-0001 §3.6).
//!
//! The policy is pure: it takes the current parts and returns the window of
//! parts to merge (or `None` when nothing is worth merging). The background
//! worker in [`super::part_manager`] applies the selection. Keeping the policy
//! separate from the worker makes the acceptance tests (merge policy unit
//! tests, write amplification) deterministic and fast.

/// Merge-policy constants fixed by ADR-0001 §3.6.
pub const DEFAULT_PARTS_TO_MERGE: usize = 15;
const MIN_WINDOW: usize = 8;
const MIN_MERGE_MULTIPLIER: f64 = 1.7;
const MIN_MERGE_SCORE: f64 = 7.5;
const MAX_PART_SIZE: u64 = 400 * 1024 * 1024 * 1024;

/// A candidate part for merging, sized by its on-disk bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeCandidate {
    /// Stable identity used to report the chosen window (e.g. the part path).
    pub id: usize,
    /// Total bytes of the part (the write-amplification proxy input).
    pub size: u64,
}

/// The outcome of a selection pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    /// Merge these candidates (in the order they appear in `candidates`).
    Merge(Vec<MergeCandidate>),
    /// No window is worth merging right now.
    None,
}

/// Scores and selects the best contiguous window of parts to merge.
///
/// Candidates must be sorted by ascending size. A window of length
/// `MIN_WINDOW..=DEFAULT_PARTS_TO_MERGE` is rejected when it is too
/// unbalanced (`a[0].size * len < MIN_MERGE_MULTIPLIER * a[last].size`) or
/// would exceed `MAX_PART_SIZE`. Its score is `sum(sizes) / a[last].size`; the
/// best window wins and a merge only runs when that score is at least
/// `MIN_MERGE_SCORE`.
pub fn select_parts_to_merge(candidates: &[MergeCandidate]) -> Selection {
    if candidates.len() < MIN_WINDOW {
        return Selection::None;
    }
    let mut best: Option<(f64, Vec<MergeCandidate>)> = None;
    let max_len = candidates.len().min(DEFAULT_PARTS_TO_MERGE);
    for len in MIN_WINDOW..=max_len {
        for start in 0..=(candidates.len() - len) {
            let window = &candidates[start..start + len];
            let Some(score) = window_score(window) else {
                continue;
            };
            let better = match &best {
                Some((best_score, _)) => score > *best_score,
                None => true,
            };
            if better {
                best = Some((score, window.to_vec()));
            }
        }
    }
    match best {
        Some((score, window)) if score >= MIN_MERGE_SCORE => Selection::Merge(window),
        _ => Selection::None,
    }
}

/// Returns the score of a window, or `None` when the window is rejected.
fn window_score(window: &[MergeCandidate]) -> Option<f64> {
    let len = window.len();
    let last = window.last()?;
    let first = window.first()?;
    if !(MIN_WINDOW..=DEFAULT_PARTS_TO_MERGE).contains(&len) {
        return None;
    }
    // Reject unbalanced windows: the smallest part must be a non-negligible
    // fraction of the largest, otherwise the merge barely reduces the count.
    let min_ok =
        (first.size as u128) * (len as u128) >= (MIN_MERGE_MULTIPLIER * last.size as f64) as u128;
    if !min_ok {
        return None;
    }
    let total: u128 = window.iter().map(|c| c.size as u128).sum();
    if total > u128::from(MAX_PART_SIZE) {
        return None;
    }
    if last.size == 0 {
        return None;
    }
    Some(total as f64 / last.size as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sizes(n: usize) -> Vec<MergeCandidate> {
        (0..n)
            .map(|i| MergeCandidate { id: i, size: 100 })
            .collect()
    }

    fn sized(v: &[u64]) -> Vec<MergeCandidate> {
        v.iter()
            .enumerate()
            .map(|(i, &s)| MergeCandidate { id: i, size: s })
            .collect()
    }

    #[test]
    fn test_needs_minimum_window() {
        assert_eq!(select_parts_to_merge(&sizes(7)), Selection::None);
    }

    #[test]
    fn test_uniform_parts_score_high() {
        // 8 equal parts: score = 800/100 = 8 >= 7.5.
        assert_eq!(select_parts_to_merge(&sizes(8)), Selection::Merge(sizes(8)));
    }

    #[test]
    fn test_unbalanced_window_rejected() {
        // Tiny first part against a huge last part fails the 1.7x balance
        // check.
        let candidates = sized(&[1, 100, 100, 100, 100, 100, 100, 100]);
        assert_eq!(select_parts_to_merge(&candidates), Selection::None);
    }

    #[test]
    fn test_moderately_unbalanced_rejected() {
        // 10*8=80 < 1.7*100=170, so the last part dominates too much.
        let candidates = sized(&[10, 10, 10, 10, 10, 10, 10, 100]);
        assert_eq!(select_parts_to_merge(&candidates), Selection::None);
    }

    #[test]
    fn test_picks_best_window_among_overlaps() {
        // 15 equal parts: the 15-window scores 15, the best window.
        let selection = select_parts_to_merge(&sizes(15));
        match selection {
            Selection::Merge(w) => assert_eq!(w.len(), 15),
            Selection::None => panic!("expected a merge"),
        }
    }

    #[test]
    fn test_window_len_cap() {
        // More than DEFAULT_PARTS_TO_MERGE candidates still yields a window.
        let candidates = sizes(30);
        match select_parts_to_merge(&candidates) {
            Selection::Merge(w) => assert!(w.len() <= DEFAULT_PARTS_TO_MERGE),
            Selection::None => panic!("expected a merge"),
        }
    }

    #[test]
    fn test_empty_candidates() {
        assert_eq!(select_parts_to_merge(&[]), Selection::None);
    }

    #[test]
    fn test_multiplier_constant_documented() {
        // Guards the ADR constant against accidental drift.
        assert_eq!(MIN_MERGE_MULTIPLIER, 1.7);
        assert_eq!(MIN_MERGE_SCORE, 7.5);
        assert_eq!(DEFAULT_PARTS_TO_MERGE, 15);
    }
}
