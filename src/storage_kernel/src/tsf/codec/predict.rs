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

//! Order-k polynomial prediction for gauge values (ADR-0002 §4.4).
//!
//! The nearest-delta codec stores the difference between adjacent bit
//! patterns, which is a poor fit for smooth gauges: consecutive `f64` values
//! differ in their low mantissa bits, so every sample costs 7-8 bytes. A
//! polynomial extrapolation of the *value* is much closer to the next sample,
//! so the residual bit pattern is small and zig-zag varint packs it into 1-3
//! bytes.
//!
//! The prediction runs in `f64` arithmetic on both ends. That is exact: IEEE
//! 754 double operations are deterministic, and the encoder and decoder
//! evaluate the same coefficients in the same order, so they derive the same
//! predicted bit pattern. The residual is the wrapping integer difference of
//! the bit patterns, so ``decode(encode(a))`` is bit-exact even when the
//! prediction itself is inaccurate.
//!
//! Payload layout (codec id 7):
//!
//! ```text
//! [order: u8]
//! [a[1]..a[order]: i64 LE]      order raw values; a[0] is the page header's
//!                               first value
//! [residual a[order+1] .. a[n]] zig-zag varint, one per remaining value
//! ```
//!
//! Non-finite values are not encoded with this codec: NaN payloads are not
//! reproducible across arithmetic, so those pages keep the nearest-delta path.

use super::int::{marshal_var_int64, unmarshal_var_int64};

/// Highest predictor order tried. Order k uses k previous values.
pub const MAX_ORDER: usize = 5;

/// Rows below which the raw warm-up values and the order byte cost more than
/// the prediction saves.
pub const MIN_ROWS: usize = 16;

/// Standard binomial coefficients of `(1 - x)^k`, most recent value first.
const COEFFS: [&[f64]; MAX_ORDER] = [
    &[2.0, -1.0],
    &[3.0, -3.0, 1.0],
    &[4.0, -6.0, 4.0, -1.0],
    &[5.0, -10.0, 10.0, -5.0, 1.0],
    &[6.0, -15.0, 20.0, -15.0, 6.0, -1.0],
];

/// Predicts the value following `history` with an order-`order` polynomial.
///
/// `history` holds bit patterns; the last `order` entries are used. The
/// summation order matters and must stay identical on the decode side.
#[inline]
fn predict(history: &[i64], order: usize) -> f64 {
    debug_assert!(history.len() >= order);
    let coeffs = COEFFS[order - 1];
    let mut sum = 0.0;
    for (j, &coeff) in coeffs.iter().enumerate() {
        let bits = history[history.len() - 1 - j];
        sum += coeff * f64::from_bits(bits as u64);
    }
    sum
}

/// Encodes `values` (f64 bit patterns) into `dst`, trying every order and
/// keeping the shortest payload.
///
/// Returns false without touching `dst` when the array is too short or holds a
/// non-finite value.
pub fn marshal(dst: &mut Vec<u8>, values: &[i64]) -> bool {
    if values.len() < MIN_ROWS {
        return false;
    }
    if !values
        .iter()
        .all(|&bits| f64::from_bits(bits as u64).is_finite())
    {
        return false;
    }

    let mut best: Option<(usize, Vec<u8>)> = None;
    let mut candidate = Vec::new();
    for order in 1..=MAX_ORDER {
        candidate.clear();
        candidate.push(order as u8);
        for &bits in &values[1..=order] {
            candidate.extend_from_slice(&bits.to_le_bytes());
        }
        for i in (order + 1)..values.len() {
            let predicted = predict(&values[..i], order).to_bits() as i64;
            marshal_var_int64(&mut candidate, values[i].wrapping_sub(predicted));
        }

        let better = match &best {
            Some((_, shortest)) => candidate.len() < shortest.len(),
            None => true,
        };
        if better {
            match &mut best {
                Some((best_order, shortest)) => {
                    *best_order = order;
                    std::mem::swap(shortest, &mut candidate);
                }
                None => best = Some((order, std::mem::take(&mut candidate))),
            }
        }
    }

    let Some((_, payload)) = best else {
        return false;
    };
    dst.extend_from_slice(&payload);
    true
}

/// Decodes a payload produced by [`marshal`], appending to `dst`.
pub fn unmarshal(
    dst: &mut Vec<i64>,
    src: &[u8],
    first_value: i64,
    items_count: usize,
) -> Result<(), String> {
    let Some((&order, rest)) = src.split_first() else {
        return Err("predicted payload is empty".to_string());
    };
    let order = order as usize;
    if order == 0 || order > MAX_ORDER {
        return Err(format!(
            "predicted payload has order {order}, expected 1..={MAX_ORDER}"
        ));
    }
    if items_count < order + 1 {
        return Err(format!(
            "predicted payload of order {order} needs at least {} values; got {items_count}",
            order + 1
        ));
    }
    let raw_len = order * 8;
    if rest.len() < raw_len {
        return Err(format!(
            "predicted payload of order {order} needs {raw_len} raw bytes; got {}",
            rest.len()
        ));
    }

    let start = dst.len();
    dst.reserve(items_count);
    dst.push(first_value);
    for chunk in rest[..raw_len].as_chunks::<8>().0 {
        dst.push(i64::from_le_bytes(*chunk));
    }

    let mut idx = raw_len;
    for i in (order + 1)..items_count {
        let (residual, used) = unmarshal_var_int64(&rest[idx..])
            .ok_or_else(|| format!("predicted payload ended before value {i}"))?;
        idx += used;
        let predicted = predict(&dst[start..start + i], order).to_bits();
        dst.push(predicted.wrapping_add(residual as u64) as i64);
    }
    if idx != rest.len() {
        return Err(format!(
            "{} trailing bytes after predicted payload",
            rest.len() - idx
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bits(values: &[f64]) -> Vec<i64> {
        values.iter().map(|v| v.to_bits() as i64).collect()
    }

    fn roundtrip(values: &[f64]) -> (usize, Vec<f64>) {
        let encoded = bits(values);
        let mut payload = Vec::new();
        assert!(marshal(&mut payload, &encoded));
        let mut decoded = Vec::new();
        unmarshal(&mut decoded, &payload, encoded[0], encoded.len()).unwrap();
        assert_eq!(decoded, encoded);
        (
            payload.len(),
            decoded.iter().map(|&b| f64::from_bits(b as u64)).collect(),
        )
    }

    #[test]
    fn test_sine_is_encoded_in_under_three_bytes_per_sample() {
        let values: Vec<f64> = (0..5_760).map(|i| (i as f64 * 0.01).sin()).collect();
        let (payload, decoded) = roundtrip(&values);
        assert_eq!(decoded, values);
        let per_sample = payload as f64 / values.len() as f64;
        assert!(
            per_sample < 3.0,
            "sine should compress well, got {per_sample:.2} B/sample"
        );
    }

    #[test]
    fn test_quantized_gauge_round_trips() {
        let values: Vec<f64> = (0..1_000)
            .map(|i| (i as f64 * 0.002).sin().round() / 1_000.0)
            .collect();
        let (payload, decoded) = roundtrip(&values);
        assert_eq!(decoded, values);
        assert!((payload as f64 / values.len() as f64) < 4.0);
    }

    #[test]
    fn test_constant_and_random_round_trip() {
        let constant = vec![42.5f64; 1_000];
        let (_, decoded) = roundtrip(&constant);
        assert_eq!(decoded, constant);

        // A pseudo-random walk is not smooth, but the codec must still be
        // lossless; selection against the nearest-delta fallback happens in
        // `marshal_values_f64`.
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let random: Vec<f64> = (0..1_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                f64::from_bits(0x3ff0_0000_0000_0000 | (state >> 12))
            })
            .collect();
        let (_, decoded) = roundtrip(&random);
        assert_eq!(decoded, random);
    }

    #[test]
    fn test_negative_zero_and_denormals_round_trip() {
        let values = vec![0.0, -0.0, 5e-324, -5e-324, 1.0, -1.0, 1e-300]
            .into_iter()
            .cycle()
            .take(64)
            .collect::<Vec<_>>();
        let (_, decoded) = roundtrip(&values);
        assert_eq!(
            decoded.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_non_finite_values_are_refused() {
        let mut payload = Vec::new();
        let mut values = bits(&(0..64).map(|i| i as f64).collect::<Vec<_>>());
        assert!(marshal(&mut payload, &values));
        payload.clear();
        values[10] = f64::NAN.to_bits() as i64;
        assert!(!marshal(&mut payload, &values));
        values[10] = f64::INFINITY.to_bits() as i64;
        assert!(!marshal(&mut payload, &values));
        assert!(payload.is_empty());
    }

    #[test]
    fn test_short_arrays_are_refused() {
        let mut payload = Vec::new();
        let values = bits(&(0..MIN_ROWS - 1).map(|i| i as f64).collect::<Vec<_>>());
        assert!(!marshal(&mut payload, &values));
        assert!(payload.is_empty());
    }

    #[test]
    fn test_corrupt_payloads_are_rejected() {
        let values: Vec<f64> = (0..64).map(|i| (i as f64 * 0.1).sin()).collect();
        let encoded = bits(&values);
        let mut payload = Vec::new();
        assert!(marshal(&mut payload, &encoded));

        // Every truncation must fail instead of returning partial data.
        for cut in 1..payload.len() {
            let mut decoded = Vec::new();
            assert!(
                unmarshal(&mut decoded, &payload[..cut], encoded[0], encoded.len()).is_err(),
                "truncation at {cut} bytes must be rejected"
            );
        }
        // Trailing bytes are rejected.
        let mut longer = payload.clone();
        longer.push(0);
        let mut decoded = Vec::new();
        assert!(unmarshal(&mut decoded, &longer, encoded[0], encoded.len()).is_err());
        // Bad order bytes are rejected.
        for order in [0u8, MAX_ORDER as u8 + 1] {
            let mut bad = payload.clone();
            bad[0] = order;
            let mut decoded = Vec::new();
            assert!(unmarshal(&mut decoded, &bad, encoded[0], encoded.len()).is_err());
        }
        // A row count that cannot cover the raw values is rejected.
        let mut decoded = Vec::new();
        assert!(unmarshal(&mut decoded, &payload, encoded[0], 2).is_err());
        // An empty payload is rejected.
        let mut decoded = Vec::new();
        assert!(unmarshal(&mut decoded, &[], 0, 64).is_err());
    }
}
