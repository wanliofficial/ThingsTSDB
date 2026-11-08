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

use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use common::now;
use storage_kernel::tsf::compaction::small_parts_exceed_ratio;
use tracing::trace;

use crate::{compaction::Task, manifest::ManifestRef, sst::SstFile, types::Timestamp};

pub struct Picker {
    manifest: ManifestRef,
    ttl: Option<Duration>,
    strategy: TimeWindowCompactionStrategy,
}

impl Picker {
    pub fn new(
        manifest: ManifestRef,
        ttl: Option<Duration>,
        segment_duration: Duration,
        new_sst_max_size: u64,
        input_sst_max_num: usize,
        input_sst_min_num: usize,
    ) -> Self {
        Self {
            manifest,
            ttl,
            strategy: TimeWindowCompactionStrategy::new(
                segment_duration,
                new_sst_max_size,
                input_sst_max_num,
                input_sst_min_num,
            ),
        }
    }

    /// This function picks a candidate for compaction.
    /// Note: It can only execute sequentially, otherwise a SST may be picked by
    /// multiple threads(that's why it take a mutable self).
    pub async fn pick_candidate(&mut self) -> Option<Task> {
        let ssts = self.manifest.all_ssts().await;
        // `now()` and file time ranges are both in milliseconds.
        let now = now();
        let expire_time = self.ttl.map(|ttl| (now - ttl.as_millis() as i64).into());
        self.strategy.pick_candidate(ssts, expire_time, now.into())
    }
}

pub struct TimeWindowCompactionStrategy {
    segment_duration: Duration,
    new_sst_max_size: u64,
    input_sst_max_num: usize,
    input_sst_min_num: usize,
    /// Segments whose close trigger already produced a task. The trigger must
    /// run once per segment: while its window is open, a late sample that adds
    /// one small file would otherwise rewrite the whole partition again.
    close_compacted: HashSet<i64>,
}

impl TimeWindowCompactionStrategy {
    pub fn new(
        segment_duration: Duration,
        new_sst_max_size: u64,
        input_sst_max_num: usize,
        input_sst_min_num: usize,
    ) -> Self {
        Self {
            segment_duration,
            new_sst_max_size,
            input_sst_max_num,
            input_sst_min_num,
            close_compacted: HashSet::new(),
        }
    }

    pub fn pick_candidate(
        &mut self,
        ssts: Vec<SstFile>,
        expire_time: Option<Timestamp>,
        now: Timestamp,
    ) -> Option<Task> {
        let (uncompacted_files, expired_files) =
            Self::find_uncompacted_and_expired_files(ssts, expire_time);
        trace!(uncompacted_files = ?uncompacted_files, expired_files = ?expired_files, "Begin pick candidate");

        let files_by_segment = self.files_by_segment(uncompacted_files);
        // No segment may satisfy the compaction thresholds while there are
        // still expired files to delete, so a missing compaction candidate
        // must not abort the whole task generation.
        let compaction_files = self
            .pick_compaction_files(files_by_segment, now)
            .unwrap_or_default();

        if compaction_files.is_empty() && expired_files.is_empty() {
            return None;
        }

        for f in &compaction_files {
            f.mark_compaction();
        }
        for f in &expired_files {
            f.mark_compaction();
        }

        let task = Task {
            inputs: compaction_files,
            expireds: expired_files,
        };

        trace!(task = ?task, "End pick candidate");

        Some(task)
    }

    fn find_uncompacted_and_expired_files(
        files: Vec<SstFile>,
        expire_time: Option<Timestamp>,
    ) -> (Vec<SstFile>, Vec<SstFile>) {
        let mut uncompacted_files = vec![];
        let mut expired_files = vec![];

        for f in files {
            if !f.is_compaction() {
                if f.is_expired(expire_time) {
                    expired_files.push(f);
                } else {
                    uncompacted_files.push(f);
                }
            }
        }
        (uncompacted_files, expired_files)
    }

    fn files_by_segment(&self, files: Vec<SstFile>) -> BTreeMap<Timestamp, Vec<SstFile>> {
        let mut files_by_segment = BTreeMap::new();
        let segment_duration = self.segment_duration;
        for file in files {
            let segment = file.meta().time_range.start.truncate_by(segment_duration);
            trace!(segment = ?segment, file = ?file);
            files_by_segment
                .entry(segment)
                .or_insert_with(Vec::new)
                .push(file);
        }

        trace!(
            files = ?files_by_segment,
            "Group files of similar timestamp into segment"
        );
        files_by_segment
    }

    fn pick_compaction_files(
        &mut self,
        files_by_segment: BTreeMap<Timestamp, Vec<SstFile>>,
        now: Timestamp,
    ) -> Option<Vec<SstFile>> {
        // Forget segments whose close window has passed; their flag can no
        // longer change a decision.
        let window = self.segment_duration.as_millis() as i64 * 2;
        self.close_compacted
            .retain(|segment| *segment + window > now.0);

        for (segment, mut files) in files_by_segment.into_iter().rev() {
            trace!(segment = ?segment, files = ?files.len(), "Loop segment for pick files");
            if files.len() < 2 {
                continue;
            }

            // The count trigger keeps the existing batch behaviour; the ADR's
            // byte-ratio and partition-close triggers make a partition that is
            // mostly one big part converge to a single file.
            let by_count = files.len() >= self.input_sst_min_num;
            let closed =
                self.closed_recently(segment, now) && !self.close_compacted.contains(&segment.0);
            let by_bytes = small_parts_exceed_ratio(files.iter().map(|f| f.size() as u64));
            if !(by_count || closed || by_bytes) {
                continue;
            }
            trace!(by_count, closed, by_bytes, "Segment triggers compaction");

            // Prefer to compact smaller files first.
            files.sort_unstable_by_key(SstFile::size);
            trace!(sorted_files = ?files, "Sort files by size");

            let mut input_size = 0;
            // Suppose the comaction will reduce the size of files by 10%.
            let memory_limit = (self.new_sst_max_size as f64 * 1.1) as u64;

            let compaction_files = files
                .into_iter()
                .take(self.input_sst_max_num)
                .take_while(|f| {
                    input_size += f.size() as u64;
                    input_size <= memory_limit
                })
                .collect::<Vec<_>>();

            // A count-driven batch must stay a batch; the other triggers are
            // happy with whatever fits, as long as it merges something.
            let min_inputs = if by_count { self.input_sst_min_num } else { 2 };
            if compaction_files.len() >= min_inputs {
                if closed {
                    self.close_compacted.insert(segment.0);
                }
                return Some(compaction_files);
            }
        }

        None
    }

    /// True when the segment ended less than one segment duration ago: the
    /// partition-close trigger's window, right after a day (or whatever the
    /// configured segment is) ends.
    ///
    /// Together with [`Self::close_compacted`] this makes the trigger fire once
    /// per segment. A partition that closed long ago, or that already used its
    /// trigger, is only recompacted by the count and ratio rules, so a late
    /// sample does not rewrite a whole old partition.
    fn closed_recently(&self, segment: Timestamp, now: Timestamp) -> bool {
        let segment_ms = self.segment_duration.as_millis() as i64;
        let end = *segment + segment_ms;
        now.0 >= end && now.0 < end + segment_ms
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;
    use test_log::test;

    use super::*;
    use crate::sst::{FileFormat, FileMeta};

    #[test]
    fn test_pick_candidate() {
        let segment_duration = Duration::from_millis(20);
        let mut strategy = TimeWindowCompactionStrategy::new(segment_duration, 9999, 10, 2);

        let ssts = (0_i64..5_i64)
            .map(|i| {
                SstFile::new(
                    i as u64,
                    FileMeta {
                        max_sequence: i as u64,
                        min_sequence: 0,
                        num_rows: i as u32,
                        size: (100 - i) as u32, // size desc
                        time_range: (i * 10..(i * 10 + 10)).into(),
                        format: FileFormat::Parquet,
                    },
                )
            })
            .collect_vec();
        // A `now` well past the segments keeps the close trigger out of the
        // way, so this still exercises the count/TTL path.
        let task = strategy
            .pick_candidate(ssts.clone(), Some(15.into()), 1_000.into())
            .unwrap();

        // ssts should be grouped into three segments:
        // | 0 1 | 2 3 | 4 |
        let excepted_task = Task {
            inputs: vec![ssts[3].clone(), ssts[2].clone()],
            expireds: vec![ssts[0].clone()],
        };

        assert_eq!(task, excepted_task);

        // sst1, sst3, ss4 are in compaction, so it should not be picked again.
        // sst2, sst5 are in different segment, so it also should not be picked.
        let task = strategy.pick_candidate(ssts, None, 1_000.into());
        assert!(task.is_none());
    }

    /// One file per input, all in the same 20 ms segment starting at zero.
    fn files(sizes: &[u32]) -> Vec<SstFile> {
        sizes
            .iter()
            .enumerate()
            .map(|(i, size)| {
                SstFile::new(
                    i as u64,
                    FileMeta {
                        max_sequence: i as u64,
                        min_sequence: 0,
                        num_rows: 1,
                        size: *size,
                        // All inputs share the 20 ms segment [0, 20).
                        time_range: (i as i64..i as i64 + 1).into(),
                        format: FileFormat::Tsf,
                    },
                )
            })
            .collect_vec()
    }

    /// ADR-0002 §6.1: small parts next to a big part are compacted once they
    /// pass a quarter of the partition.
    #[test]
    fn test_small_part_ratio_trigger() {
        let mut strategy =
            TimeWindowCompactionStrategy::new(Duration::from_millis(20), 9999, 30, 5);

        // Big part 100 B, smalls 10 + 10: 20 B is under 25% of 120 B.
        let ssts = files(&[100, 10, 10]);
        assert!(strategy.pick_candidate(ssts, None, 1_000.into()).is_none());

        // Smalls 20 + 20: 40 B is over 25% of 140 B, so all three merge.
        let ssts = files(&[100, 20, 20]);
        let task = strategy
            .pick_candidate(ssts.clone(), None, 1_000.into())
            .unwrap();
        assert_eq!(task.inputs.len(), 3);
        assert!(task.expireds.is_empty());
    }

    /// Without a dominant big part the ratio would fire after every flush, so
    /// equal-sized files wait for the count trigger instead.
    #[test]
    fn test_equal_sized_parts_wait_for_the_count_trigger() {
        let mut strategy =
            TimeWindowCompactionStrategy::new(Duration::from_millis(20), 9999, 30, 5);

        let ssts = files(&[100, 100, 100, 100]);
        assert!(strategy.pick_candidate(ssts, None, 1_000.into()).is_none());

        let ssts = files(&[100, 100, 100, 100, 100]);
        let task = strategy.pick_candidate(ssts, None, 1_000.into()).unwrap();
        assert_eq!(task.inputs.len(), 5);
    }

    /// The partition-close trigger merges the parts of a segment that just
    /// ended, even when there are too few for the count rule.
    #[test]
    fn test_partition_close_trigger() {
        let mut strategy =
            TimeWindowCompactionStrategy::new(Duration::from_millis(20), 9999, 30, 5);
        let ssts = files(&[100, 100]);
        // Segment [0, 20) closed one millisecond ago.
        let task = strategy.pick_candidate(ssts, None, 21.into()).unwrap();
        assert_eq!(task.inputs.len(), 2);
        assert!(task.expireds.is_empty());

        // The trigger fires once per segment: a late sample that brings a
        // second file while the window is still open must not rewrite the
        // merged partition again.
        let ssts = files(&[100, 100]);
        assert!(strategy.pick_candidate(ssts, None, 22.into()).is_none());

        // A partition that closed long ago is left alone: a late sample must
        // not rewrite a whole old partition, so only the count and ratio rules
        // apply again.
        let mut strategy =
            TimeWindowCompactionStrategy::new(Duration::from_millis(20), 9999, 30, 5);
        let ssts = files(&[100, 100]);
        assert!(strategy.pick_candidate(ssts, None, 1_000.into()).is_none());
    }
}
