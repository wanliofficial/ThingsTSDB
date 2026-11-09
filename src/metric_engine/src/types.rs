// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to
// you under the Apache License, Version 2.0 (the
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

use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetricId(pub u64);

impl From<u64> for MetricId {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeriesId(pub u64);

impl From<u64> for SeriesId {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

#[derive(Debug, Clone)]
pub struct Label {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

/// This is the main struct used for write, optional values will be filled in
/// different modules.
#[derive(Debug, Clone)]
pub struct Sample {
    pub name: Vec<u8>,
    pub labels: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
    /// hash of name
    pub name_id: Option<MetricId>,
    /// hash of labels(sorted)
    pub series_id: Option<SeriesId>,
}

impl Sample {
    /// Canonical encoding of the series this sample belongs to.
    pub fn series_key(&self) -> Vec<u8> {
        build_series_key(&self.name, &self.labels)
    }

    /// Series id is the hash of the canonical series key.
    pub fn series_id(&self) -> SeriesId {
        SeriesId(hash(&self.series_key()))
    }
}

/// Builds the canonical series key of a metric name and its labels.
///
/// Labels are sorted by `(name, value)` so the encoding is order-insensitive.
pub fn build_series_key(name: &[u8], labels: &[Label]) -> Vec<u8> {
    let mut labels = labels.iter().collect::<Vec<_>>();
    labels.sort_by(|a, b| (&a.name, &a.value).cmp(&(&b.name, &b.value)));

    let mut buf = Vec::new();
    put_bytes(&mut buf, name);
    for label in labels {
        put_bytes(&mut buf, &label.name);
        put_bytes(&mut buf, &label.value);
    }
    buf
}

pub fn hash(buf: &[u8]) -> u64 {
    seahash::hash(buf)
}

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn take_bytes(buf: &mut &[u8]) -> anyhow::Result<Vec<u8>> {
    if buf.len() < 4 {
        anyhow::bail!("truncated series key: no length prefix");
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    *buf = &buf[4..];
    if buf.len() < len {
        anyhow::bail!(
            "truncated series key: expect {len} bytes, {} left",
            buf.len()
        );
    }
    let bytes = buf[..len].to_vec();
    *buf = &buf[len..];
    Ok(bytes)
}

/// Decode the canonical series key back to metric name and labels, the
/// counterpart of [`Sample::series_key`].
pub fn decode_series_key(buf: &[u8]) -> Result<(Vec<u8>, Vec<Label>)> {
    let mut cursor = buf;
    let name = take_bytes(&mut cursor)?;
    let mut labels = Vec::new();
    while !cursor.is_empty() {
        let label_name = take_bytes(&mut cursor)?;
        let label_value = take_bytes(&mut cursor)?;
        labels.push(Label {
            name: label_name,
            value: label_value,
        });
    }
    Ok((name, labels))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(name: &str, value: &str) -> Label {
        Label {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn sample(name: &str, labels: Vec<Label>) -> Sample {
        Sample {
            name: name.as_bytes().to_vec(),
            labels,
            timestamp: 0,
            value: 0.0,
            name_id: None,
            series_id: None,
        }
    }

    #[test]
    fn test_series_key_is_label_order_insensitive() {
        let s1 = sample(
            "http_requests",
            vec![label("job", "proxy"), label("code", "200")],
        );
        let s2 = sample(
            "http_requests",
            vec![label("code", "200"), label("job", "proxy")],
        );
        assert_eq!(s1.series_key(), s2.series_key());
        assert_eq!(s1.series_id(), s2.series_id());
    }

    #[test]
    fn test_series_key_is_unambiguous() {
        // (a="bc") vs (ab="c") must produce different keys.
        let s1 = sample("m", vec![label("a", "bc")]);
        let s2 = sample("m", vec![label("ab", "c")]);
        assert_ne!(s1.series_key(), s2.series_key());

        // Different metric names always mean different series.
        let s3 = sample("m2", vec![label("a", "bc")]);
        assert_ne!(s1.series_key(), s3.series_key());
    }

    #[test]
    fn test_series_key_roundtrip() {
        let s = sample(
            "http_requests",
            vec![label("job", "proxy"), label("code", "200")],
        );
        let (name, mut labels) = decode_series_key(&s.series_key()).unwrap();
        assert_eq!(name, b"http_requests".to_vec());
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(labels[0].name, b"code".to_vec());
        assert_eq!(labels[0].value, b"200".to_vec());
        assert_eq!(labels[1].name, b"job".to_vec());
        assert_eq!(labels[1].value, b"proxy".to_vec());

        // Truncated keys must be rejected instead of panicking.
        assert!(decode_series_key(&s.series_key()[..5]).is_err());
    }
}
