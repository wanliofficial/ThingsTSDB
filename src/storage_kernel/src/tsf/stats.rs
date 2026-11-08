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
//! "is the CRC hardware path in use?", "how often does the bloom filter save a
//! lookup?", "do the index caches earn their memory?") are about the
//! deployment, not one file.
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
    /// Compressed payload bytes per codec choice, same indexing as
    /// [`Self::codec_counts`]. The sum over this array is the total payload
    /// bytes written, which is what the codec ratio is computed from.
    pub codec_bytes: [u64; 7],
    /// CRC32C computations that used the SSE4.2 instruction.
    pub crc_hardware: u64,
    /// CRC32C computations that fell back to the slicing-by-8 table.
    pub crc_software: u64,
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

    /// Compressed bytes produced by `codec`.
    pub fn codec_bytes(&self, codec: MarshalType) -> u64 {
        self.codec_bytes[codec.as_u8() as usize]
    }

    /// Total payload bytes written (all four payloads, all pages).
    pub fn payload_bytes(&self) -> u64 {
        self.codec_bytes.iter().sum()
    }

    /// Fraction of CRC32C work that ran on the SSE4.2 instruction, or `None`
    /// before the first checksum. A persistent `Some(0.0)` means the build or
    /// the CPU lost the accelerated path.
    pub fn crc_hardware_ratio(&self) -> Option<f64> {
        let total = self.crc_hardware + self.crc_software;
        if total == 0 {
            None
        } else {
            Some(self.crc_hardware as f64 / total as f64)
        }
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
static CODEC_BYTES: [AtomicU64; 7] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static CRC_HARDWARE: AtomicU64 = AtomicU64::new(0);
static CRC_SOFTWARE: AtomicU64 = AtomicU64::new(0);
static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static CACHE_EVICTIONS: AtomicU64 = AtomicU64::new(0);
static CACHE_REJECTS: AtomicU64 = AtomicU64::new(0);

/// Records the codecs chosen for one encoded page and their payload sizes.
pub(super) fn record_page(payloads: &[(MarshalType, u64); 4], uncompressed: bool) {
    PAGES.fetch_add(1, Ordering::Relaxed);
    if uncompressed {
        UNCOMPRESSED_PAGES.fetch_add(1, Ordering::Relaxed);
    }
    for (codec, bytes) in payloads {
        let index = codec.as_u8() as usize;
        CODEC_COUNTS[index].fetch_add(1, Ordering::Relaxed);
        CODEC_BYTES[index].fetch_add(*bytes, Ordering::Relaxed);
    }
}

/// Records one CRC32C computation on the hardware path.
pub(super) fn record_crc_hardware() {
    CRC_HARDWARE.fetch_add(1, Ordering::Relaxed);
}

/// Records one CRC32C computation on the portable path.
pub(super) fn record_crc_software() {
    CRC_SOFTWARE.fetch_add(1, Ordering::Relaxed);
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
    let mut codec_bytes = [0u64; 7];
    for (out, counter) in codec_bytes.iter_mut().zip(CODEC_BYTES.iter()) {
        *out = load(counter);
    }
    CodecStats {
        pages: load(&PAGES),
        uncompressed_pages: load(&UNCOMPRESSED_PAGES),
        codec_counts,
        codec_bytes,
        crc_hardware: load(&CRC_HARDWARE),
        crc_software: load(&CRC_SOFTWARE),
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

    /// Totals are process-wide, so tests assert deltas. Other tests encode
    /// pages and checksum concurrently with this module, so deltas are lower
    /// bounds; identities computed from a single snapshot stay exact.
    #[test]
    fn test_page_and_codec_counters() {
        let before = codec_stats();
        record_page(
            &[
                (MarshalType::ZstdNearestDelta2, 100),
                (MarshalType::ZstdNearestDelta, 200),
                (MarshalType::Const, 3),
                (MarshalType::NearestDelta, 40),
            ],
            false,
        );
        record_page(
            &[
                (MarshalType::DeltaConst, 5),
                (MarshalType::NearestDelta2, 60),
                (MarshalType::NearestDelta, 70),
                (MarshalType::Const, 3),
            ],
            true,
        );
        let after = codec_stats();

        assert!(after.pages - before.pages >= 2);
        assert!(after.uncompressed_pages - before.uncompressed_pages >= 1);
        assert!(after.zstd_pages() > before.zstd_pages());
        let encoded: u64 = (0..7)
            .map(|codec| after.codec_counts[codec] - before.codec_counts[codec])
            .sum();
        assert!(encoded >= 8, "four payloads per page");
        assert!(
            after.codec_count(MarshalType::Const) - before.codec_count(MarshalType::Const) >= 2
        );

        let compressed: u64 = (0..7)
            .map(|codec| after.codec_bytes[codec] - before.codec_bytes[codec])
            .sum();
        assert!(compressed >= 100 + 200 + 3 + 40 + 5 + 60 + 70 + 3);
        assert!(
            after.codec_bytes(MarshalType::Const) - before.codec_bytes(MarshalType::Const) >= 6
        );
        assert_eq!(after.payload_bytes() - before.payload_bytes(), compressed);
    }

    #[test]
    fn test_crc_dispatch_counters() {
        let before = codec_stats();
        record_crc_hardware();
        record_crc_hardware();
        record_crc_software();
        let after = codec_stats();

        assert!(after.crc_hardware - before.crc_hardware >= 2);
        assert!(after.crc_software - before.crc_software >= 1);
        assert!(
            after.crc_hardware_ratio().is_some(),
            "ratio is Some once anything was checksummed"
        );
        assert_eq!(
            CodecStats::default().crc_hardware_ratio(),
            None,
            "no checksum yet"
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

        assert!(after.bloom_rejects - before.bloom_rejects >= 1);
        assert!(after.cache.hits - before.cache.hits >= 3);
        assert!(after.cache.misses - before.cache.misses >= 2);
        assert!(after.cache.evictions - before.cache.evictions >= 1);
        assert!(after.cache.rejects - before.cache.rejects >= 4);
        // entries/bytes are per-context gauges, never summed.
        assert_eq!(after.cache.entries, 0);
        assert_eq!(after.cache.bytes, 0);
    }
}
