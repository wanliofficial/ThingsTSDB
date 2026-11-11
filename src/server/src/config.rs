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

use common::ReadableDuration;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub port: u16,
    pub test: TestConfig, // for test
    pub metric_engine: MetricEngineConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 5000,
            test: TestConfig::default(),
            metric_engine: MetricEngineConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TestConfig {
    pub enable_write: bool,
    pub write_worker_num: usize,
    pub write_interval: ReadableDuration,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            enable_write: false,
            write_worker_num: 1,
            write_interval: ReadableDuration::millis(500),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricEngineConfig {
    /// Duration of one segment, data is managed and expired by segments.
    pub segment_duration: ReadableDuration,
    pub threads: ThreadConfig,
    pub storage: StorageConfig,
    /// Which index answers queries (ADR-0001 §12.5). Writes always dual-write
    /// the legacy tables and the mergeset index, so this can be changed
    /// without a backfill.
    pub index_mode: IndexModeConfig,
    /// Per-metric read-time retention (roadmap Phase 4): a metric listed here
    /// only exposes samples inside its window, even when it shares a segment
    /// with a longer-lived metric. Physical reclamation stays with the global
    /// segment TTL.
    pub retention: RetentionConfig,
    /// Static sharding and replication (roadmap Phase 4). Absent (the default)
    /// means a single local engine rooted at `storage.object_store.data_dir`.
    pub cluster: Option<ClusterConfig>,
}

impl Default for MetricEngineConfig {
    fn default() -> Self {
        Self {
            segment_duration: ReadableDuration::hours(12),
            threads: ThreadConfig::default(),
            storage: StorageConfig::default(),
            index_mode: IndexModeConfig::default(),
            retention: RetentionConfig::default(),
            cluster: None,
        }
    }
}

/// Static cluster topology (roadmap Phase 4).
///
/// ```toml
/// [metric_engine.cluster]
/// nodes = ["node-a", "node-b", "node-c"]
/// shards = 64
/// replicas = 2
/// ```
///
/// Every node name is a directory under `storage.object_store.data_dir`; the
/// server opens the `(node, shard)` engines of *every* node it is configured
/// with. For a real multi-process deployment each process would list only its
/// own node and reach the others over the network — this build has no
/// inter-node RPC, so the cluster is a local simulation of the placement, used
/// to exercise and operate the shard map.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    pub nodes: Vec<String>,
    pub shards: usize,
    pub replicas: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: vec!["node-a".to_string()],
            shards: 1,
            replicas: 1,
        }
    }
}

impl From<&ClusterConfig> for metric_engine::ClusterConfig {
    fn from(config: &ClusterConfig) -> Self {
        metric_engine::ClusterConfig::new(config.nodes.clone(), config.shards, config.replicas)
    }
}

/// Per-metric retention windows, keyed by metric name.
///
/// ```toml
/// [metric_engine.retention.per_metric]
/// http_requests = "30d"
/// node_cpu_seconds = "90d"
/// ```
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionConfig {
    pub per_metric: std::collections::BTreeMap<String, ReadableDuration>,
}

impl From<&RetentionConfig> for metric_engine::RetentionPolicy {
    fn from(config: &RetentionConfig) -> Self {
        let entries = config
            .per_metric
            .iter()
            .map(|(name, window)| (name.as_bytes().to_vec(), window.0.as_millis() as i64));
        metric_engine::RetentionPolicy::from_ms(entries)
    }
}

/// Query-path index selection, mirroring `metric_engine::IndexMode`.
#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum IndexModeConfig {
    /// Only the legacy inverted-index tables.
    Legacy,
    /// Only the month-partitioned mergeset index.
    New,
    /// Both, unioned; the default while a deployment migrates.
    #[default]
    DualRead,
}

impl From<IndexModeConfig> for metric_engine::IndexMode {
    fn from(value: IndexModeConfig) -> Self {
        match value {
            IndexModeConfig::Legacy => metric_engine::IndexMode::Legacy,
            IndexModeConfig::New => metric_engine::IndexMode::New,
            IndexModeConfig::DualRead => metric_engine::IndexMode::DualRead,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ThreadConfig {
    pub manifest_thread_num: usize,
    pub sst_thread_num: usize,
}

impl Default for ThreadConfig {
    fn default() -> Self {
        Self {
            manifest_thread_num: 2,
            sst_thread_num: 2,
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub object_store: ObjectStorageConfig,
    pub time_merge_storage: columnar_storage::config::StorageConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
pub enum ObjectStorageConfig {
    Local(LocalStorageConfig),
    S3Like(S3LikeStorageConfig),
}

impl Default for ObjectStorageConfig {
    fn default() -> Self {
        Self::Local(LocalStorageConfig::default())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LocalStorageConfig {
    pub data_dir: String,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "/tmp/horaedb".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3LikeStorageConfig {
    pub region: String,
    pub key_id: String,
    pub key_secret: String,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    #[serde(default)]
    pub http: HttpOptions,
    #[serde(default)]
    pub timeout: TimeoutOptions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpOptions {
    pub pool_max_idle_per_host: usize,
    pub timeout: ReadableDuration,
    pub keep_alive_timeout: ReadableDuration,
    pub keep_alive_interval: ReadableDuration,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            pool_max_idle_per_host: 1024,
            timeout: ReadableDuration::secs(15),
            keep_alive_timeout: ReadableDuration::secs(10),
            keep_alive_interval: ReadableDuration::secs(2),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimeoutOptions {
    // Non IO Operation like stat and delete, they operate on a single file, we
    // control them by setting timeout.
    pub timeout: ReadableDuration,
    // IO Operation like read and write, they operate on data directly, we
    // control them by setting io_timeout.
    pub io_timeout: ReadableDuration,
}

impl Default for TimeoutOptions {
    fn default() -> Self {
        Self {
            timeout: ReadableDuration::secs(10),
            io_timeout: ReadableDuration::secs(10),
        }
    }
}

#[inline]
fn default_max_retries() -> usize {
    3
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `[metric_engine.cluster]` table parses into the engine's topology, and
    /// leaving it out keeps the single-engine default.
    #[test]
    fn cluster_config_parses_into_a_topology() {
        let body = r#"
            [metric_engine.cluster]
            nodes = ["node-a", "node-b", "node-c"]
            shards = 64
            replicas = 2
        "#;
        let config: Config = toml::from_str(body).unwrap();
        let cluster = config.metric_engine.cluster.as_ref().unwrap();
        assert_eq!(cluster.nodes, vec!["node-a", "node-b", "node-c"]);
        assert_eq!(cluster.shards, 64);
        assert_eq!(cluster.replicas, 2);

        let topology: metric_engine::ClusterConfig = cluster.into();
        let map = metric_engine::ShardMap::new(topology).unwrap();
        assert_eq!(map.shard_count(), 64);
        assert_eq!(map.replicas(), 2);
        // Every shard has `replicas` distinct owners drawn from the node list.
        for shard in 0..map.shard_count() {
            let owners = map.owners(shard);
            assert_eq!(owners.len(), 2);
            assert_ne!(owners[0], owners[1]);
        }

        let plain: Config = toml::from_str("").unwrap();
        assert!(plain.metric_engine.cluster.is_none());
    }

    /// A topology the engine cannot satisfy is rejected rather than silently
    /// degraded into fewer copies.
    #[test]
    fn cluster_config_rejects_unreachable_replicas() {
        let cluster = ClusterConfig {
            nodes: vec!["a".into(), "b".into()],
            shards: 4,
            replicas: 3,
        };
        let err = metric_engine::ShardMap::new((&cluster).into()).unwrap_err();
        assert!(err.to_string().contains("replicas must be between"));
    }

    /// A `[metric_engine.retention.per_metric]` table parses into a
    /// `RetentionPolicy` keyed by metric name with millisecond windows.
    #[test]
    fn retention_config_parses_into_policy() {
        let body = r#"
            [metric_engine.retention.per_metric]
            http_requests = "1h"
            node_cpu_seconds = "2d"
        "#;
        let config: Config = toml::from_str(body).unwrap();
        let policy: metric_engine::RetentionPolicy = (&config.metric_engine.retention).into();

        assert_eq!(policy.len(), 2);
        assert_eq!(policy.retention_ms(b"http_requests"), Some(3_600_000));
        assert_eq!(
            policy.retention_ms(b"node_cpu_seconds"),
            Some(2 * 86_400_000)
        );
        // A metric not in the table has no retention window.
        assert_eq!(policy.retention_ms(b"other"), None);

        // The clamp uses the configured window: at now=10h, a 1h metric only
        // exposes samples from the last hour.
        let now = 10 * 3_600_000i64;
        let (start, end) = policy.clamp(b"http_requests", 0, now, now);
        assert_eq!(start, now - 3_600_000);
        assert_eq!(end, now);
    }
}
