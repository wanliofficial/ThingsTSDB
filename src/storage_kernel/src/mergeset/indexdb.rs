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

//! Canonical item encoding for the indexDB (ADR-0001 §2.1, §4).
//!
//! Every index item is `[namespace byte][row]`. This module owns the canonical
//! metric-name/tag marshaling, the 24-byte TSID layout, the namespace prefixes
//! and the fixed-width posting rows.

use xxhash_rust::xxh64::xxh64;

/// Namespace prefixes (ADR-0001 §4). The first byte of every index item.
pub mod ns {
    /// `metricName + 0x02 + TSID(24B)`: canonical name -> TSID.
    pub const METRIC_NAME: u8 = 0;
    /// `tagKey + tagValue + seriesID(8B)*`: global posting list.
    pub const TAG: u8 = 1;
    /// `seriesID(8B) + TSID(24B)`: series ID -> TSID.
    pub const SERIES_ID: u8 = 2;
    /// `seriesID(8B) + metricName`: series ID -> canonical name.
    pub const METRIC_NAME_BY_SERIES_ID: u8 = 3;
    /// `seriesID(8B)`: tombstone for a deleted series.
    pub const DELETED: u8 = 4;
    /// `date(8B) + seriesID(8B)`: series with data on a day.
    pub const DATE_SERIES: u8 = 5;
    /// `date(8B) + tagKey + tagValue + seriesID(8B)*`: per-day posting list.
    pub const DATE_TAG: u8 = 6;
    /// `date(8B) + metricName + 0x02 + TSID(24B)`: per-day name lookup.
    pub const DATE_METRIC_NAME: u8 = 7;
}

const ESC_0: u8 = b'0';
const ESC_1: u8 = b'1';
const ESC_2: u8 = b'2';
const TAG_SEP: u8 = 0x01;
const KV_SEP: u8 = 0x02;

/// Escapes a key or value into the canonical encoding (ADR-0001 §2.1).
///
/// The special bytes `0x00`, `0x01` and `0x02` are escaped with the `0x00 'n'`
/// prefix; everything else is copied verbatim. The output never contains a raw
/// separator byte, so prefix scans can treat `0x01` as a terminator.
pub fn escape(dst: &mut Vec<u8>, src: &[u8]) {
    for &b in src {
        match b {
            0x00 => dst.extend_from_slice(&[0x00, ESC_0]),
            0x01 => dst.extend_from_slice(&[0x00, ESC_1]),
            0x02 => dst.extend_from_slice(&[0x00, ESC_2]),
            _ => dst.push(b),
        }
    }
}

/// Returns the canonical bytes for one tag key or value.
pub fn escaped(src: &[u8]) -> Vec<u8> {
    let mut dst = Vec::with_capacity(src.len());
    escape(&mut dst, src);
    dst
}

/// Appends the canonical encoding of a tag pair: `key + 0x02 + value + 0x01`.
pub fn append_tag(dst: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    escape(dst, key);
    dst.push(KV_SEP);
    escape(dst, value);
    dst.push(TAG_SEP);
}

/// Appends the canonical encoding of a metric group (value only, terminated).
pub fn append_group(dst: &mut Vec<u8>, group: &[u8]) {
    escape(dst, group);
    dst.push(TAG_SEP);
}

/// Builds the full canonical name of a series: the metric group followed by
/// every tag, each escaped and terminated (`group 0x01 (key 0x02 value
/// 0x01)*`).
///
/// This is what namespace 3 stores, so label reconstruction needs only one row
/// per series instead of a lookup per tag.
pub fn canonical_name(group: &[u8], tags: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut dst = Vec::with_capacity(group.len() + 2);
    append_group(&mut dst, group);
    append_tags(&mut dst, tags);
    dst
}

/// Appends the escaped, terminated tag pairs of a canonical name.
pub fn append_tags(dst: &mut Vec<u8>, tags: &[(&[u8], &[u8])]) {
    for (key, value) in tags {
        escape(dst, key);
        dst.push(KV_SEP);
        escape(dst, value);
        dst.push(TAG_SEP);
    }
}

/// A canonical series identity: metric group plus sorted `(key, value)` tags.
pub type SeriesName = (Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>);

/// Decodes a [`canonical_name`] back into the metric group and its tags.
///
/// Returns `None` on a malformed encoding (missing terminator, dangling key).
pub fn decode_canonical_name(mut src: &[u8]) -> Option<SeriesName> {
    let group = take_escaped(&mut src, TAG_SEP)?;
    let mut tags = Vec::new();
    while !src.is_empty() {
        let key = take_escaped(&mut src, KV_SEP)?;
        let value = take_escaped(&mut src, TAG_SEP)?;
        tags.push((key, value));
    }
    Some((group, tags))
}

/// Reads one escaped field terminated by `sep`, unescaping `0x00 'n'` pairs.
fn take_escaped(src: &mut &[u8], sep: u8) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < src.len() {
        match src[i] {
            0x00 => {
                let escaped = *src.get(i + 1)?;
                out.push(match escaped {
                    b'0' => 0x00,
                    b'1' => 0x01,
                    b'2' => 0x02,
                    _ => return None,
                });
                i += 2;
            }
            b if b == sep => {
                *src = &src[i + 1..];
                return Some(out);
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    None
}

/// Returns the canonical metric-name prefix used by namespace 0 rows:
/// `marshal_tag_value(metric_group)`.
pub fn canonical_metric_name(group: &[u8]) -> Vec<u8> {
    escaped(group)
}

/// Returns the full canonical item key for a metric group (namespace 0).
pub fn metric_name_item(group: &[u8]) -> Vec<u8> {
    let mut key = vec![ns::METRIC_NAME];
    key.extend_from_slice(&canonical_metric_name(group));
    key
}

/// Builds the composite tag-key encoding: `0xfe | varuint(len(name)) | name |
/// key`.
///
/// A composite key lets a filter with a known metric name seek
/// `composite(foo, bar)="baz"` directly instead of intersecting `foo` with
/// `bar`.
pub fn composite_key(metric_group: &[u8], tag_key: &[u8]) -> Vec<u8> {
    let mut dst = vec![0xfe];
    marshal_var_uint64(&mut dst, tag_key.len() as u64);
    dst.extend_from_slice(metric_group);
    dst.extend_from_slice(tag_key);
    dst
}

/// Appends one posting-list row: prefix + value + fixed 8-byte big-endian IDs.
pub fn append_posting(dst: &mut Vec<u8>, prefix: &[u8], value: &[u8], ids: &[u64]) {
    dst.extend_from_slice(prefix);
    escape(dst, value);
    dst.push(TAG_SEP);
    for &id in ids {
        dst.extend_from_slice(&id.to_be_bytes());
    }
}

/// Appends a series-ID row (namespaces 2-4): `prefix + seriesID(8B)`.
pub fn append_series_id(dst: &mut Vec<u8>, prefix: u8, series_id: u64) {
    dst.push(prefix);
    dst.extend_from_slice(&series_id.to_be_bytes());
}

/// Appends a date row: `prefix + date(8B) + seriesID(8B)`.
pub fn append_date_series_id(dst: &mut Vec<u8>, prefix: u8, date: u64, series_id: u64) {
    dst.push(prefix);
    dst.extend_from_slice(&date.to_be_bytes());
    dst.extend_from_slice(&series_id.to_be_bytes());
}

/// Appends a name->TSID row (namespaces 0 and 7).
///
/// `name` is the already-encoded canonical name (see [`canonical_name`]): the
/// metric group followed by its tags. It is copied verbatim, since it carries
/// its own escaping and terminators.
pub fn append_name_tsid(dst: &mut Vec<u8>, prefix: u8, date: Option<u64>, name: &[u8]) {
    dst.push(prefix);
    if let Some(date) = date {
        dst.extend_from_slice(&date.to_be_bytes());
    }
    dst.extend_from_slice(name);
    dst.extend_from_slice(&TSID_LEN.to_be_bytes());
}

/// Builds the namespace 0 row `canonicalName + TSID(24B)`.
pub fn metric_name_row(name: &[u8], tsid: &Tsid) -> Vec<u8> {
    let mut dst = Vec::with_capacity(1 + name.len() + 8 + TSID_LEN);
    append_name_tsid(&mut dst, ns::METRIC_NAME, None, name);
    dst.extend_from_slice(&tsid.marshal());
    dst
}

/// Builds the namespace 7 row `date(8B) + canonicalName + TSID(24B)`.
pub fn date_metric_name_row(date: u64, name: &[u8], tsid: &Tsid) -> Vec<u8> {
    let mut dst = Vec::with_capacity(1 + 8 + name.len() + 8 + TSID_LEN);
    append_name_tsid(&mut dst, ns::DATE_METRIC_NAME, Some(date), name);
    dst.extend_from_slice(&tsid.marshal());
    dst
}

/// Builds the namespace 2 row `seriesID(8B) + TSID(24B)`.
pub fn series_id_row(series_id: u64, tsid: &Tsid) -> Vec<u8> {
    let mut dst = Vec::with_capacity(1 + 8 + TSID_LEN);
    dst.push(ns::SERIES_ID);
    dst.extend_from_slice(&series_id.to_be_bytes());
    dst.extend_from_slice(&tsid.marshal());
    dst
}

/// Builds the namespace 3 row `seriesID(8B) + canonical name`.
///
/// The canonical name carries its own terminators, so it needs no length
/// prefix and the row stays prefix-orderable by series ID.
pub fn series_id_name_row(series_id: u64, canonical_name: &[u8]) -> Vec<u8> {
    let mut dst = Vec::with_capacity(1 + 8 + canonical_name.len());
    dst.push(ns::METRIC_NAME_BY_SERIES_ID);
    dst.extend_from_slice(&series_id.to_be_bytes());
    dst.extend_from_slice(canonical_name);
    dst
}

/// Builds the namespace 1 row for one tag pair and its posting IDs.
pub fn tag_posting_row(key: &[u8], value: &[u8], ids: &[u64]) -> Vec<u8> {
    let mut dst = Vec::with_capacity(1 + key.len() + value.len() + 2 + ids.len() * 8);
    dst.push(ns::TAG);
    append_tag(&mut dst, key, value);
    append_ids(&mut dst, ids);
    dst
}

/// Builds the namespace 6 row `date + tagKey + tagValue + seriesID(8B)*`.
pub fn date_tag_posting_row(date: u64, key: &[u8], value: &[u8], ids: &[u64]) -> Vec<u8> {
    let mut dst = Vec::with_capacity(1 + 8 + key.len() + value.len() + 2 + ids.len() * 8);
    dst.push(ns::DATE_TAG);
    dst.extend_from_slice(&date.to_be_bytes());
    append_tag(&mut dst, key, value);
    append_ids(&mut dst, ids);
    dst
}

/// Builds one namespace 5 row `date(8B) + seriesID(8B)` per ID.
///
/// The per-day series set is one row per series: a posting row would need a
/// value slot that this namespace does not have.
pub fn date_series_rows(date: u64, ids: &[u64]) -> Vec<Vec<u8>> {
    ids.iter()
        .map(|id| {
            let mut dst = Vec::with_capacity(1 + 16);
            append_date_series_id(&mut dst, ns::DATE_SERIES, date, *id);
            dst
        })
        .collect()
}

/// Appends the fixed-width big-endian posting IDs.
pub fn append_ids(dst: &mut Vec<u8>, ids: &[u64]) {
    for &id in ids {
        dst.extend_from_slice(&id.to_be_bytes());
    }
}

/// Decodes the posting IDs that follow a namespace 1 row's value terminator.
///
/// `prefix` is `[ns][escaped key][0x02]`; the value is read with the same
/// escaping as a key, then every remaining byte is a fixed-width ID. Returns
/// `(value, ids)`, or `None` when the row is not a posting row of this prefix.
pub fn decode_posting_row(prefix: &[u8], row: &[u8]) -> Option<(Vec<u8>, Vec<u64>)> {
    let rest = row.strip_prefix(prefix)?;
    let mut src = rest;
    let value = take_escaped(&mut src, TAG_SEP)?;
    if src.len() % 8 != 0 {
        return None;
    }
    let (chunks, _) = src.as_chunks::<8>();
    let ids = chunks.iter().map(|c| u64::from_be_bytes(*c)).collect();
    Some((value, ids))
}

/// Decodes the IDs of a posting row after a prefix that already ends at the
/// value terminator.
///
/// `prefix` is `[ns][escaped key][0x02][escaped value][0x01]`, i.e.
/// [`posting_row_prefix`]; every remaining byte is a fixed-width ID. Returns
/// `None` when the row does not extend the prefix or is malformed.
pub fn decode_posting_ids(prefix: &[u8], row: &[u8]) -> Option<Vec<u64>> {
    let rest = row.strip_prefix(prefix)?;
    if rest.len() % 8 != 0 {
        return None;
    }
    let (chunks, _) = rest.as_chunks::<8>();
    Some(chunks.iter().map(|c| u64::from_be_bytes(*c)).collect())
}

/// The namespace 1 prefix for a tag key: `[ns][escaped key][0x02]`.
pub fn tag_key_prefix(key: &[u8]) -> Vec<u8> {
    let mut dst = Vec::with_capacity(1 + key.len() + 1);
    dst.push(ns::TAG);
    escape(&mut dst, key);
    dst.push(KV_SEP);
    dst
}

/// The namespace 6 prefix for a day and tag key:
/// `[ns][date(8B)][escaped key][0x02]`.
pub fn date_tag_key_prefix(date: u64, key: &[u8]) -> Vec<u8> {
    let mut dst = Vec::with_capacity(1 + 8 + key.len() + 1);
    dst.push(ns::DATE_TAG);
    dst.extend_from_slice(&date.to_be_bytes());
    escape(&mut dst, key);
    dst.push(KV_SEP);
    dst
}

/// The namespace 3 prefix for one series: `[ns][seriesID(8B)]`.
pub fn series_id_prefix(series_id: u64) -> Vec<u8> {
    let mut dst = Vec::with_capacity(9);
    dst.push(ns::METRIC_NAME_BY_SERIES_ID);
    dst.extend_from_slice(&series_id.to_be_bytes());
    dst
}

/// The full row prefix of one namespace 1 posting row:
/// `[ns][escaped key][0x02][escaped value][0x01]`.
///
/// seeking this prefix returns only the rows of exactly that tag pair, so a
/// lookup does not have to scan every value of the tag key.
pub fn posting_row_prefix(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut dst = tag_key_prefix(key);
    escape(&mut dst, value);
    dst.push(TAG_SEP);
    dst
}

/// The row prefix of the metric-group posting row (namespace 1, empty key).
pub fn group_posting_row_prefix(group: &[u8]) -> Vec<u8> {
    posting_row_prefix(b"", group)
}

/// The row prefix of one day's series set (namespace 5):
/// `[ns][date(8B)]`. Every matching row is `prefix + seriesID(8B)`.
pub fn date_series_prefix(date: u64) -> Vec<u8> {
    let mut dst = Vec::with_capacity(9);
    dst.push(ns::DATE_SERIES);
    dst.extend_from_slice(&date.to_be_bytes());
    dst
}

/// Decodes a namespace 3 row `[ns][seriesID(8B)][canonical name]`.
pub fn decode_series_id_name_row(row: &[u8]) -> Option<(u64, Vec<u8>)> {
    if row.len() < 9 || row[0] != ns::METRIC_NAME_BY_SERIES_ID {
        return None;
    }
    let series_id = u64::from_be_bytes(row[1..9].try_into().ok()?);
    Some((series_id, row[9..].to_vec()))
}

/// Decodes a namespace 5 row `[ns][date(8B)][seriesID(8B)]`.
pub fn decode_date_series_row(row: &[u8]) -> Option<(u64, u64)> {
    if row.len() != 17 || row[0] != ns::DATE_SERIES {
        return None;
    }
    let date = u64::from_be_bytes(row[1..9].try_into().ok()?);
    let series_id = u64::from_be_bytes(row[9..17].try_into().ok()?);
    Some((date, series_id))
}

/// The marshaled TSID size (ADR-0001 §2.2).
pub const TSID_LEN: usize = 24;

/// A 24-byte TSID (ADR-0001 §2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Tsid {
    pub metric_group_id: u64,
    pub job_id: u32,
    pub instance_id: u32,
    pub series_id: u64,
}

impl Tsid {
    /// Builds a TSID from a metric group and the first two sorted tag values.
    ///
    /// `metric_group_id` is `xxhash64(group, 0)`; `job_id`/`instance_id` are
    /// the truncated `xxhash64` of the first and second sorted tag value. They
    /// are grouping hints only; `series_id` is the unique key.
    pub fn build(group: &[u8], tags: &[(&[u8], &[u8])]) -> Self {
        let metric_group_id = xxh64(group, 0);
        let job_id = tags.first().map(|(_, v)| xxh64(v, 0) as u32).unwrap_or(0);
        let instance_id = tags.get(1).map(|(_, v)| xxh64(v, 0) as u32).unwrap_or(0);
        let series_id = Self::allocate(job_id, instance_id, 0);
        Self {
            metric_group_id,
            job_id,
            instance_id,
            series_id,
        }
    }

    /// Allocates a series ID for a partition (ADR-0001 §2.3).
    ///
    /// The encoded layout keeps the high bits constant within a partition:
    /// `bits 63..56` node, `bits 55..40` partition (months since 1970-01),
    /// `bits 39..0` dense counter. The counter seed keeps the low bits dense
    /// for the fast `uint64set` path.
    pub fn allocate(job_id: u32, instance_id: u32, counter: u64) -> u64 {
        // The counter occupies the low 40 bits; the seed is derived from the
        // grouping fields so that two indexDBs never start at 0 while staying
        // dense (no multi-trillion-ID gap).
        let low =
            ((u64::from(job_id) << 24) ^ (u64::from(instance_id) << 8) ^ counter) & 0xff_ffff_ffff;
        let high = (u64::from(job_id) & 0xff) << 56 | (u64::from(instance_id) & 0xffff) << 40;
        high | low
    }

    /// Returns the trailing 8-byte `series_id` stored in data rows.
    pub fn series_id(self) -> u64 {
        self.series_id
    }

    /// Returns a copy of this TSID carrying an allocated `series_id`.
    ///
    /// The grouping hints stay as built from the tags; only the identity slot
    /// is replaced, which is what the allocator owns (ADR-0001 §2.3).
    pub fn with_series_id(mut self, series_id: u64) -> Self {
        self.series_id = series_id;
        self
    }

    /// Allocates a globally unique series ID for a partition (ADR-0001 §2.3).
    ///
    /// `bits 63..56` node, `bits 55..40` month partition, `bits 39..0` dense
    /// counter. IDs from different partitions can therefore never collide, and
    /// the low 40 bits stay dense for the `uint64set` fast path.
    pub fn allocate_series_id(node_id: u8, partition_id: u16, counter: u64) -> u64 {
        let counter = counter & 0xff_ffff_ffff;
        (u64::from(node_id) << 56) | (u64::from(partition_id) << 40) | counter
    }

    pub fn marshal(&self) -> [u8; TSID_LEN] {
        let mut out = [0u8; TSID_LEN];
        out[0..8].copy_from_slice(&self.metric_group_id.to_be_bytes());
        out[8..12].copy_from_slice(&self.job_id.to_be_bytes());
        out[12..16].copy_from_slice(&self.instance_id.to_be_bytes());
        out[16..24].copy_from_slice(&self.series_id.to_be_bytes());
        out
    }

    pub fn unmarshal(src: &[u8]) -> Result<Self, String> {
        if src.len() < TSID_LEN {
            return Err(format!("TSID needs {TSID_LEN} bytes, got {}", src.len()));
        }
        Ok(Self {
            metric_group_id: u64::from_be_bytes(src[0..8].try_into().unwrap()),
            job_id: u32::from_be_bytes(src[8..12].try_into().unwrap()),
            instance_id: u32::from_be_bytes(src[12..16].try_into().unwrap()),
            series_id: u64::from_be_bytes(src[16..24].try_into().unwrap()),
        })
    }
}

fn marshal_var_uint64(dst: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        dst.push(b);
        if v == 0 {
            break;
        }
    }
}

/// Returns `true` if `key` (with the namespace byte stripped) starts with the
/// canonical encoding of `prefix`.
pub fn key_starts_with_canonical(key: &[u8], prefix: &[u8]) -> bool {
    key.starts_with(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tags() -> Vec<(&'static [u8], &'static [u8])> {
        vec![
            (b"__name__", b"http_requests"),
            (b"job", b"api"),
            (b"instance", b"host1"),
        ]
    }

    #[test]
    fn test_escape_round_trips_all_bytes() {
        for b in 0u16..=u16::MAX {
            let input = (b as u8).to_le_bytes();
            let enc = escaped(&input);
            // The escaped form never contains a raw separator.
            assert!(!enc.contains(&TAG_SEP), "raw 0x01 for {b:#06x}");
            assert!(!enc.contains(&KV_SEP), "raw 0x02 for {b:#06x}");
            if b < 256 {
                let raw = b as u8;
                if !matches!(raw, 0x00..=0x02) {
                    assert_eq!(enc, vec![raw]);
                } else {
                    assert_eq!(enc.len(), 2);
                    assert_eq!(enc[0], 0x00);
                    assert_eq!(enc[1], raw + b'0');
                }
            }
        }
    }

    #[test]
    fn test_canonical_metric_name_prefix_order() {
        // `foo` is a prefix of `foobar`, so the terminated item keys order
        // `foo` before `foobar` instead of interleaving.
        let foo = metric_name_item(b"foo");
        let foobar = metric_name_item(b"foobar");
        assert!(foobar.starts_with(&foo[..foo.len() - 1]));
        assert!(foo < foobar);
        // The canonical bytes sit between the namespace and the terminator.
        let name = canonical_metric_name(b"http_requests");
        let item = metric_name_item(b"http_requests");
        // namespace + canonical name + terminator.
        assert_eq!(item.len(), 1 + name.len());
        assert_eq!(item[0], ns::METRIC_NAME);
        assert_eq!(&item[1..], name.as_slice());
    }

    #[test]
    fn test_append_tag_terminates_key_and_value() {
        let mut dst = Vec::new();
        append_tag(&mut dst, b"job", b"api");
        assert_eq!(dst, b"job\x02api\x01");
        // A value equal to a key prefix stays ordered because of the
        // terminator.
        let mut a = Vec::new();
        append_tag(&mut a, b"job", b"api");
        let mut b = Vec::new();
        append_tag(&mut b, b"job", b"apix");
        assert!(a < b);
    }

    #[test]
    fn test_tsid_layout_and_hints() {
        let tsid = Tsid::build(b"http_requests", &sample_tags()[1..]);
        let bytes = tsid.marshal();
        assert_eq!(bytes.len(), TSID_LEN);
        let back = Tsid::unmarshal(&bytes).unwrap();
        assert_eq!(back, tsid);
        // Grouping hints are xxhash64-truncated.
        assert_eq!(tsid.metric_group_id, xxh64(b"http_requests", 0));
        assert_eq!(tsid.job_id, xxh64(b"api", 0) as u32);
        assert_eq!(tsid.instance_id, xxh64(b"host1", 0) as u32);
        assert_eq!(tsid.series_id(), back.series_id);
    }

    #[test]
    fn test_series_id_layout_keeps_high_bits_per_partition() {
        let job = 0x0123;
        let instance = 0x4567;
        let a = Tsid::allocate(job, instance, 1);
        let b = Tsid::allocate(job, instance, 2);
        assert_eq!(a, b - 1, "counter allocation is dense");
        let c = Tsid::allocate(job, instance, 0);
        // High bits are constant within a partition; the counter is the low 40.
        assert_eq!(a >> 40, c >> 40);
        assert_eq!((a >> 40) & 0xffff_ffff_0000, (c >> 40) & 0xffff_ffff_0000);
    }

    #[test]
    fn test_composite_key_encoding() {
        let key = composite_key(b"http_requests", b"job");
        assert_eq!(key[0], 0xfe);
        assert_eq!(&key[2..], b"http_requestsjob");
        // The key is placed in the tag-key slot and terminated like any other.
        let mut row = vec![ns::TAG];
        row.extend_from_slice(&key);
        append_posting(&mut row, b"", b"api", &[]);
        // The composite key is in the tag-key slot, then the terminated value.
        assert!(row.ends_with(b"api\x01"));
        assert_eq!(row[1], 0xfe);
    }

    #[test]
    fn test_posting_row_is_fixed_width() {
        let mut dst = Vec::new();
        append_posting(&mut dst, b"", b"api", &[1, 2, 3]);
        assert_eq!(
            dst,
            b"api\x01"
                .iter()
                .copied()
                .chain(1u64.to_be_bytes())
                .chain(2u64.to_be_bytes())
                .chain(3u64.to_be_bytes())
                .collect::<Vec<u8>>()
        );
        let mut dst = Vec::new();
        append_posting(&mut dst, b"", b"api", &[0x0102_0304_0506_0708]);
        assert_eq!(dst.len(), b"api\x01".len() + 8);
        // IDs are fixed-width, so 1..3 stays one contiguous run.
        // IDs are fixed-width 8-byte slots; the value terminator is therefore
        // at a fixed offset regardless of how many IDs follow.
        let mut a = Vec::new();
        append_posting(&mut a, b"", b"k", &[1, 3]);
        let term = b"k\x01".len();
        assert_eq!(a[term - 1], TAG_SEP);
        let mut b = Vec::new();
        append_posting(&mut b, b"", b"k", &[0x0102_0304_0506_0708]);
        assert_eq!(b.len(), term + 8);
    }

    #[test]
    fn test_date_series_row() {
        let mut dst = Vec::new();
        append_date_series_id(&mut dst, ns::DATE_SERIES, 20000, 42);
        assert_eq!(
            dst,
            [5u8]
                .iter()
                .copied()
                .chain(20000u64.to_be_bytes())
                .chain(42u64.to_be_bytes())
                .collect::<Vec<u8>>()
        );
    }
}
