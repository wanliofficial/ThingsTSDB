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

//! Mergeset LSM table for the Phase 1 index (ADR-0001).
//!
//! M1 covers block marshaling (plain/ZSTD), the directory part layout
//! (`metaindex -> index block -> block header -> data block`), sorted part
//! search and the k-way merge cursor. M2 adds the indexDB item encoding, the
//! row merge callbacks and the sorted series-ID set. Background merging,
//! caches and the index WAL come in later milestones.

pub mod block_cache;
pub mod block_header;
pub mod indexdb;
pub mod indexdb_table;
pub mod inmemory_block;
pub mod inmemory_part;
pub mod merge_callbacks;
pub mod merge_policy;
pub mod metaindex_row;
pub mod part;
pub mod part_manager;
pub mod series_id_set;
pub mod table;
pub mod table_search;
pub mod tag_filter;
pub mod timeutil;
pub mod wal;
