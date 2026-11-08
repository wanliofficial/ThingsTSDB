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

//! Block codecs for TSF pages.
//!
//! This module is a selective port of `esmetrics/crates/esm-encoding`
//! (Apache-2.0), which itself ports VictoriaMetrics v1.146.0
//! `lib/encoding`. See `src/storage_kernel/PORTING.md`.

mod compress;
mod encoding;
mod float;
mod int;
mod nearest_delta;
mod nearest_delta2;
mod predict;
pub mod simd;

#[cfg(test)]
mod testutil;

use std::cell::RefCell;

pub use compress::*;
pub use encoding::*;
pub use float::*;
pub use int::*;

/// Runs `f` with a thread-local `Vec<i64>` scratch buffer of exactly `len`
/// items.
///
/// The buffer is grown when needed but never cleared; callers must overwrite
/// every element before reading it. This avoids the per-page zero-fill of the
/// reference implementation while keeping the scratch allocation warm.
pub(crate) fn with_int64_scratch<R>(len: usize, f: impl FnOnce(&mut [i64]) -> R) -> R {
    thread_local! {
        static SCRATCH: RefCell<Vec<i64>> = const { RefCell::new(Vec::new()) };
    }
    SCRATCH.with(|cell| {
        let mut v = cell.borrow_mut();
        if v.len() < len {
            v.resize(len, 0);
        }
        f(&mut v[..len])
    })
}

/// Fast paths for the all-zero and all-one arrays used by const encoding.
///
/// The reference implementation uses SIMD helpers from `esm-common`; the
/// prototype keeps the same API with plain iterators.
mod fastnum {
    #[inline]
    pub(super) fn is_int64_zeros(a: &[i64]) -> bool {
        a.iter().all(|&v| v == 0)
    }

    #[inline]
    pub(super) fn is_int64_ones(a: &[i64]) -> bool {
        a.iter().all(|&v| v == 1)
    }
}
