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

mod pb_types {
    include!(concat!(env!("OUT_DIR"), "/pb_types.sst.rs"));
}

/// Prometheus remote write/read protocol types. Both files must be included
/// under the same parent module so cross-file references like
/// `super::remote_write::TimeSeries` resolve.
pub mod prometheus {
    pub mod remote_write {
        include!(concat!(env!("OUT_DIR"), "/pb_types.remote_write.rs"));
    }
    pub mod remote_read {
        include!(concat!(env!("OUT_DIR"), "/pb_types.remote_read.rs"));
    }
}

pub use pb_types::*;
pub use prometheus::{remote_read::*, remote_write::*};
