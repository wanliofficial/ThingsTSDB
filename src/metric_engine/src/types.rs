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
    /// Canonical encoding of the series this sample belongs to:
    /// `name + sorted label pairs`, with length prefixes to avoid ambiguity.
    pub fn series_key(&self) -> Vec<u8> {
        let mut labels = self.labels.iter().collect::<Vec<_>>();
        labels.sort_by(|a, b| (&a.name, &a.value).cmp(&(&b.name, &b.value)));

        let mut buf = Vec::new();
        put_bytes(&mut buf, &self.name);
        for label in labels {
            put_bytes(&mut buf, &label.name);
            put_bytes(&mut buf, &label.value);
        }
        buf
    }

    /// Series id is the hash of the canonical series key.
    pub fn series_id(&self) -> SeriesId {
        SeriesId(hash(&self.series_key()))
    }
}

pub fn hash(buf: &[u8]) -> u64 {
    seahash::hash(buf)
}

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
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
}
