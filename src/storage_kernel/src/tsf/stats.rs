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

//! Process-wide codec, bloom and index-cache counters (ADR-0002 §13).
//!
//! These are advisory counters for operators, not a synchronisation mechanism:
//! every increment is relaxed. They are process-wide rather than per-reader
//! because the questions they answer ("which codec wins on this workload?",
//! "how often does the bloom filter save a lookup?", "do the index caches
//! earn their memory?") are about the deployment, not one file.
//!
//! `storage_kernel` has no logging or metrics dependency, so the counters live
//! here and the storage layer reads them: [`super::codec_stats`] is a snapshot.

use std::sync::atomic::{AtomicU64, Ordering};

use super::{codec::MarshalType, file::CacheStats};

/// A snapshot of the kernel's process-wide counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CodecStats {
    /// Pages encoded from scratch (flush, compaction, rewrite).
    pub pages: u64,
    /// Of those, pages stored uncompressed because no payload wanted ZSTD
    /// (ADR-0002 §4.4).
    pub uncompressed_pages: u64,
    /// Payload codec choices, indexed by `MarshalType::as_u8` (`1..=6`).
    /// Every page records one choice for each of its four payloads, so the
    /// sum over this array is `4 * pages`.
    pub codec_counts: [u64; 7],
    /// Series lookups the bloom filter rejected as definitely absent.
    pub bloom_rejects: u64,
    /// Index-cache totals merged across every read context. `entries` and
    /// `bytes` are per-context gauges and stay zero here.
    pub cache: CacheStats,
}

impl CodecStats {
    /// Number of payloads encoded with `codec`.
    pub fn codec_count(&self, codec: MarshalType) -> u64 {
        self.codec_counts[codec.as_u8() as usize]
    }

    /// Pages that used ZSTD on at least one payload.
    pub fn zstd_pages(&self) -> u64 {
        self.pages - self.uncompressed_pages
    }

    /// Fraction of encoded pages stored without ZSTD.
    pub fn uncompressed_ratio(&self) -> f64 {
        if self.pages == 0 {
            0.0
        } else {
            self.uncompressed_pages as f64 / self.pages as f64
        }
    }
}

static PAGES: AtomicU64 = AtomicU64::new(0);
static UNCOMPRESSED_PAGES: AtomicU64 = AtomicU64::new(0);
static CODEC_COUNTS: [AtomicU64; 7] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static BLOOM_REJECTS: AtomicU64 = AtomicU64::new(0);
static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static CACHE_EVICTIONS: AtomicU64 = AtomicU64::new(0);
static CACHE_REJECTS: AtomicU64 = AtomicU64::new(0);

/// Records the codecs chosen for one encoded page.
pub(super) fn record_page(codecs: &[MarshalType; 4], uncompressed: bool) {
    PAGES.fetch_add(1, Ordering::Relaxed);
    if uncompressed {
        UNCOMPRESSED_PAGES.fetch_add(1, Ordering::Relaxed);
    }
    for codec in codecs {
        CODEC_COUNTS[codec.as_u8() as usize].fetch_add(1, Ordering::Relaxed);
    }
}

/// Records one series lookup that the bloom filter ruled out.
pub(super) fn record_bloom_reject() {
    BLOOM_REJECTS.fetch_add(1, Ordering::Relaxed);
}

/// Merges a read context's cache counters into the process totals.
pub(super) fn record_cache(stats: &CacheStats) {
    CACHE_HITS.fetch_add(stats.hits, Ordering::Relaxed);
    CACHE_MISSES.fetch_add(stats.misses, Ordering::Relaxed);
    CACHE_EVICTIONS.fetch_add(stats.evictions, Ordering::Relaxed);
    CACHE_REJECTS.fetch_add(stats.rejects, Ordering::Relaxed);
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

/// Snapshots the process-wide counters.
pub fn codec_stats() -> CodecStats {
    let mut codec_counts = [0u64; 7];
    for (out, counter) in codec_counts.iter_mut().zip(CODEC_COUNTS.iter()) {
        *out = load(counter);
    }
    CodecStats {
        pages: load(&PAGES),
        uncompressed_pages: load(&UNCOMPRESSED_PAGES),
        codec_counts,
        bloom_rejects: load(&BLOOM_REJECTS),
        cache: CacheStats {
            hits: load(&CACHE_HITS),
            misses: load(&CACHE_MISSES),
            evictions: load(&CACHE_EVICTIONS),
            rejects: load(&CACHE_REJECTS),
            entries: 0,
            bytes: 0,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Totals are process-wide, so tests assert deltas; other tests running in
    /// parallel may add to the same counters.
    #[test]
    fn test_page_and_codec_counters() {
        let before = codec_stats();
        record_page(
            &[
                MarshalType::ZstdNearestDelta2,
                MarshalType::ZstdNearestDelta,
                MarshalType::Const,
                MarshalType::NearestDelta,
            ],
            false,
        );
        record_page(
            &[
                MarshalType::DeltaConst,
                MarshalType::NearestDelta2,
                MarshalType::NearestDelta,
                MarshalType::Const,
            ],
            true,
        );
        let after = codec_stats();

        assert_eq!(after.pages - before.pages, 2);
        assert_eq!(after.uncompressed_pages - before.uncompressed_pages, 1);
        assert_eq!(after.zstd_pages() - before.zstd_pages(), 1);
        let encoded: u64 = (0..7)
            .map(|codec| after.codec_counts[codec] - before.codec_counts[codec])
            .sum();
        assert_eq!(encoded, 8, "four payloads per page");
        assert_eq!(
            after.codec_count(MarshalType::Const) - before.codec_count(MarshalType::Const),
            2
        );
    }

    #[test]
    fn test_bloom_and_cache_counters() {
        let before = codec_stats();
        record_bloom_reject();
        record_cache(&CacheStats {
            hits: 3,
            misses: 2,
            evictions: 1,
            rejects: 4,
            entries: 99,
            bytes: 4096,
        });
        let after = codec_stats();

        assert_eq!(after.bloom_rejects - before.bloom_rejects, 1);
        assert_eq!(after.cache.hits - before.cache.hits, 3);
        assert_eq!(after.cache.misses - before.cache.misses, 2);
        assert_eq!(after.cache.evictions - before.cache.evictions, 1);
        assert_eq!(after.cache.rejects - before.cache.rejects, 4);
        // entries/bytes are per-context gauges, never summed.
        assert_eq!(after.cache.entries, 0);
        assert_eq!(after.cache.bytes, 0);
    }
}
