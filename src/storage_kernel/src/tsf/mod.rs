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

//! TSF v1 prototype implementing the layout in
//! `docs/rfcs/20261028-data-file-adr.md`.

pub mod bloom;
pub mod checksum;
pub mod codec;
pub mod file;
pub mod page;

/// One row of the prototype schema `(i64 ts, f64 value, u64 seq, u64 reserved)`
/// keyed by metric and series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Row {
    pub metric_id: u64,
    pub series_id: u64,
    pub timestamp: i64,
    pub value: f64,
    pub sequence: u64,
    pub reserved: u64,
}
