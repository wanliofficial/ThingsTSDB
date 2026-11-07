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

//! Arrow integration for TSF v1.
//!
//! `storage_kernel::tsf` stays Arrow-free (ADR-0002 §11); this module owns
//! the conversion from typed columns to Arrow batches, zero-copy where the
//! buffer ownership allows it, and the ~8,192-row batching contract.

pub mod scan;

pub use scan::{
    decoded_columns_to_batch, filter_by_sequence, kernel_time_range, scan, scan_metric,
    scan_series, tsf_schema, BatchBuilder, TsfScanOptions, TsfScanRequest,
};
