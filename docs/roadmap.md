# 开发路线图：对标 VictoriaMetrics 并吸收 IoTDB 设计

> 状态：草案（2026-10-28）
> 前置文档：`docs/3high-design.md`（目标架构与指标）、`docs/rfcs/20240827-metric-engine.md`
> 参考实现：`/media/wanli/WD_SSD1/home/wanli/workspace/esmetrics`（VictoriaMetrics v1.146.0 的 Rust 移植，Apache-2.0）
> 参考项目：Apache IoTDB（TsFile、标签索引、Last Cache、内/外 compaction）

## 一、结论

此前的开发计划（P0 写缓冲+WAL、P1 Prometheus 兼容、P2 rollup、P3 对象存储/TTL、P4 基准与 CI）
**没有**覆盖 VictoriaMetrics 的存储内核：mergeset、indexDB、TSID 分配、数据分区/Part/Block、
自定义块编码、LSM 倒排索引及后台合并。仅有部分能力以不同形态存在（见第三节）。

本路线图把这些能力正式纳入计划，并同时吸收 Apache IoTDB 的 TsFile 文件布局、时间分区、
类型化编码、Last Cache 与 compaction 设计。目标不是整体替换现有实现，而是**分阶段演进，
在保留 Arrow/DataFusion 查询层的同时，替换索引与数据文件的存储内核**。

## 二、现状基线（commit `6245eb3c`）

- 写路径：WAL（`{table}/wal`，Arrow IPC 帧）+ 内存缓冲，按阈值/定时/查询前 flush 为一个
  Parquet SST；行级 `__seq__` 保证 last-write-wins。
- 文件：`{root}/data/{id}.sst`（Parquet，默认 PLAIN+Snappy），manifest 增量 + 快照合并。
- 分区：按 `segment_duration` 分组（测试为 2h），compaction 仅在段内进行。
- 保留：文件级 TTL（`time_range.end < now - ttl` 整文件删除）。
- 索引：`index(metric_id, tag_key, tag_value, tsid, registered_at)` 宽表，查询时扫描该表做
  Eq/Neq 过滤，再回查 series/data 表。
- 查询：DataFusion `ParquetExec → FilterExec → SortPreservingMergeExec → MergeExec(seq LWW)`，
  标签匹配与按序列分组在 `metric_engine/query.rs` 手写实现。
- 缺失：TSID 全局分配、mergeset、indexDB、正则匹配、bitmap posting、自定义时序块编码、
  rollup、dedup 区间语义、分区级删除。

## 三、VictoriaMetrics 组件差距对照

| 组件 | VM / esmetrics 参考 | 当前实现 | 结论 | 阶段 |
|---|---|---|---|---|
| mergeset（排序字节项 LSM） | `esm-mergeset/src/{table,table_merge,inmemory_part,part}.rs` | 无 | 新增 | P1 |
| indexDB（LSM 倒排索引） | `esm-storage/src/index/{mod,search,tag_filters,row_merge}.rs` | 宽表索引表 | 替换/并存 | P1 |
| TSID 分配与持久化 | `esm-storage/src/tsid.rs`（24B）、`index/create.rs` | `seahash64` 哈希，无分配器/冲突处理 | 重做 | P1 |
| 时间分区 / Part | `esm-storage/src/partition/`、`time_range.rs` | 按 segment 分组 | 对齐并扩展 | P1/P2 |
| 数据块与编码 | `esm-encoding/`、`block_header.rs`（81B）、`metaindex_row.rs` | 仅 Parquet 默认编码 | 新增 | P2 |
| 标签过滤 / posting 集合 | `index/search.rs`、`uint64set.rs` | Eq/Neq 索引表扫描，无正则 | 重做 | P1 |
| 查询执行与 rollup | `esm-promql/`、`esm-select/` | DataFusion + 手写分组，无 rollup | 扩展 | P3 |
| 去重区间语义 | `esm-storage/src/dedup.rs` | `__seq__` LWW，无 dedup interval | 扩展 | P2 |
| 保留策略 | `table.rs enforce_retention`、分区 drop | 文件级 TTL | 分区级删除 | P2 |
| LSM 后台合并 | `table_merge.rs`、`start_background_workers` | Parquet SST compaction | 新增索引合并 | P1 |

## 四、从 Apache IoTDB 吸收的设计

| IoTDB 设计 | 借鉴点 | 落地方式 | 阶段 |
|---|---|---|---|
| TsFile：Chunk Group / Column Chunk / Page，页级 min/max/first/last 统计 | 多级索引与谓词下推、按设备/测点物理连续 | 设计自己的 TsFile 风格文件格式（不追求字节兼容）；查询按页裁剪 | P2 |
| 类型化编码矩阵：GORILLA/CHIMP、TS_2DIFF、RLE、DICTIONARY、PLAIN + Snappy/LZ4/ZSTD | 时间戳与整数用二阶差分/bit-packing，浮点用 XOR，状态量用 RLE | 新增 `tsfile` 编码模块与按列选择编码器 | P2 |
| 可配置时间分区 + 按分区删除 | TTL 退化为删目录，O(1) | 分区管理器替代现有按 segment 分组 | P1/P2 |
| Last Cache | 毫秒级最新值查询 | 每序列最新值缓存，写路径同步更新 | P3 |
| Schema Template（设备模板） | 千万同构设备复用测点定义，避免元数据膨胀 | 指标模板/设备模板表，批量注册序列 | P1/P3 |
| Aligned Timeseries / 宽表设备模型 | 同一设备同刻多测点共享时间列 | 评估在 TSID/文件布局中按设备对齐 chunk group | P0 决策 |
| 内层/外层 Compaction | 页内合并 + Part 间合并，消除乱序与重复 | 分区内 small/big part 合并，索引 mergeset 独立合并 | P2 |
| 乱序写入 + WAL 恢复 | 开放窗口内乱序入 MemTable，超窗写 late 文件 | 扩展现有 WAL/缓冲，按分区维护开放窗口 | P2 |
| 树形路径模型（root.factory.device.sensor） | 路径即索引，规避标签基数爆炸 | 作为可选接入模型映射为 metric+labels，不替代标签模型 | P3+ |

不整体吸收的部分：IoTDB 2.0 的 Region/共识与完整 SQL 引擎（查询层继续用 DataFusion），
TsFile 字节级兼容（无生态绑定需求），以及 Java 侧生态。

## 五、目标架构（推荐：混合方案）

```
接入（Prometheus remote write / MQTT / Line Protocol）
        │
        ▼
TSID 分配 + IndexDB（mergeset LSM）── 标签过滤（Eq/Neq/Regex）→ TSID 集合
        │
        ▼
分区管理（按天/可配置）── Partition ── InMemory Part → Small Part → Big Part
        │                                    │
        │                                    └── TsFile 风格文件（Chunk Group/Page/编码/统计）
        ▼
查询层：分区裁剪 + 页级统计裁剪 + 向量化解码（Arrow）→ DataFusion / 自研算子 → rollup
```

- 逻辑模型保持 metric + labels（Prometheus 兼容）；物理上按 metric/device 前缀组织 chunk group，
  兼顾 IoTDB 的设备对齐读。
- 索引：采用 VM 的 TSID + indexDB + mergeset 方案（LSM 天然支持高基数与后台合并）；
  IoTDB 的 tag index 思想并入 indexdb 命名空间。
- 数据文件：新格式借鉴 TsFile 的两级结构（chunk group → page）与页级统计；编码器参考
  esmetrics `esm-encoding` 与 IoTDB 编码矩阵。
- 查询：沿用 Arrow 列式内存与 DataFusion，但扫描源从 Parquet 换成新文件格式，并增加
  TSID 集合驱动 + 页级裁剪 + rollup 选择。

## 六、阶段计划

### Phase 0：决策与验证（1–2 周）

- 输出 ADR：数据模型（单值序列 vs 设备宽表）、TSID 布局、索引与数据文件是否上对象存储、
  新旧引擎边界与开关。
- 已完成（提案）：`docs/rfcs/20261028-mergeset-index-adr.md`，确定 mergeset 格式与合并策略、
  24 字节 TSID、indexDB 命名空间、标签过滤、WAL/恢复顺序、双读迁移与 Phase 1 任务拆分。
- 已完成（提案）：`docs/rfcs/20261028-data-file-adr.md`，确定单值序列模型、UTC 天分区、
  TSF v1 单文件格式（chunk group / series chunk / page）、编码、页级裁剪、压实/去重、
  TTL 与双读迁移；数据文件与索引均待评审，对象存储 checkpoint 仍是开放问题。
- 验证：移植 `esm-mergeset` 最小 Table 做写入/查询/合并基准；按 TSF v1 原型对比 Parquet
  与 TsFile 风格页格式。
- 已完成（原型 + 基准）：`src/storage_kernel` 移植 VM codec、最小 mergeset 与 TSF v1 原型；
  `phase0_bench` 对比 Parquet：体积 -12%~+58%（counter 负载 1.57 B/sample，满足 ADR 目标），
  写入吞吐为 Parquet 的 70–99%，全扫描慢 4–7 倍，100 序列范围查询快 28–41 倍；mergeset
  写入 1.26M items/s、合并 2.24M items/s。详见 `docs/rfcs/20261028-phase0-benchmark.md`。
- 许可：esmetrics 与 VictoriaMetrics 均为 Apache-2.0，复用代码需保留 NOTICE/版权头；
  逐文件记录来源。
- 验收：ADR 评审通过；基准数据支撑格式选型；确定 P1 任务拆分。

### Phase 1：TSID + mergeset + IndexDB + 标签过滤（3–4 周）

- 新增 `src/storage_kernel/mergeset`（可先移植 `esm-mergeset`，本地磁盘 Part + 后台合并；
  对象存储作为 checkpoint，Phase 4 再评估纯对象存储实现）。
- 已完成（M1，2026-11-09）：目录式 Part（`metaindex → index block → block header`）、
  golden 字节布局、`Part::open` 全链路校验、单一 Table 的 k 路搜索游标、
  `parts.json` 原子替换、`mergeset_part` fuzz target（30 s smoke 无崩溃）。
- 已完成（M2，2026-11-09）：canonical 编码与命名空间行（ns0–ns7）、24 字节 TSID
  （xxhash64 分组提示 + 40 位编码 series_id）、composite key、行合并回调与去重、
  `SeriesIdSet` 交集/并集/差集、标签过滤 planner（`=`/`!=`/`=~`/`!~`、regex 简化、
  按代价排序、按日裁剪）、131 项测试。10M 行 posting 基准随 M3 实测。
- 已完成（M3，2026-11-09）：in-memory part（sealed + pending block，可搜索）、
  纯 merge 选择算法（8..=15 窗口、1.7× 平衡门、score≥7.5、8 项策略测试）、
  字节预算 LRU block cache、后台 flush + `max(cpus,4)` merger 循环（stop 通道收敛）、
  原子 claim 协议（锁内摘除选中窗口、锁外合并、失败回滚）、合并仅发布输出后才删源、
  `recover_from_disk` 从 `parts.json` 恢复并跳过损坏 part。含 crash/restart、并发 merger
  不重复 claim、`1 + merge_threads` 线程启动测试。写放大实测：1M 条 / 250 次 flush /
  22 次 merge 为 2.95×（目标 < 5×），`index_mergeset_bench` 可复现。
- 已完成（M4，2026-11-09）：Arrow-free index WAL（`wal.rs`，帧头 magic/len/type/
  partition/min_seq/max_seq/max_series_id/crc32c + varuint items 载荷，big-endian，
  CRC-32C 校验，撕裂尾帧检测）、`WalWriter` 段写与 fsync、按大小轮转段；`Table` 序列
  预留（`reserve_seq_range` 单调不重叠）与 flush 回调（失效 tag-filter 缓存）、
  `flush`/`merge_all` 把序列范围写入 part 头（merge 取源并集，不产生覆盖空洞）；
  `PartManager::write_index_items` 按 §6.1 先写 WAL+fsync 再缓冲、flush/merge 后
  原子写 `parts.json` 并按帧级 `SeqIntervalSet` 覆盖检查回收 WAL 段、`recover_from_disk`
  （冷启动无 `parts.json` 视为空）后 `replay_wal` 重放未覆盖帧。含真实
  `kill -9` 集成测试：子进程写 500 帧后被 SIGKILL，父进程从存活的 WAL 恢复全部
  item、无孤儿数据行。171 项测试通过。
- 已完成（M5，2026-11-09）：`metric_engine` 集成。`indexdb_table.rs` 月分区 `IndexDb`
  门面（写 ns0/ns1/ns1-composite/ns2/ns3/ns5/ns6/ns7，`search_series_ids`/
  `series_names`/`lookup_series_id`/`flush`）、`timeutil.rs` UTC 日/月运算；
  `Table::search_prefix`/`search_range` 合入 pending，`Table::set_parts` 让
  PartManager 发布 part；`flush_once` 写前排序去重，WAL 回收不删 active 段。
  `metric_engine::IndexDbManager` 按 `{root}/indexdb/{YYYY_MM}` 路由写入并去重
  `(series_id, day)`，查询只遍历磁盘上存在的月分区（宽时间范围不再打开数百个空
  indexDB），写出始终双写 legacy + mergeset；`IndexMode::{Legacy,New,DualRead}`
  默认双读，`try_open_with_index_mode` 可切换；query 双读合并，支持 `=`/`!=`/`=~`/`!~`；
  server 配置 `metric_engine.index_mode` 并接通 RE/NRE matcher。一致性测试：同一批
  数据在 Legacy 与 New 下对空/eq/neq/re/nre/交集/越界查询结果完全一致。195 项
  storage_kernel 测试 + kill9 集成测试、15 项 metric_engine 测试通过。flush 时按 row key
  折叠 posting 行（`MAX_POSTING_IDS_PER_ROW=1024` 分片，保证单行小于 64 KiB block），
  regex 每 matcher 只编译一次；新增 `index_query_bench`：20 万序列 / 320 万行实测索引
  51.8 B/序列（目标 <200）、写放大 1.46×（目标 <5×）、写入 ~1.83 万序列/s。
  查询侧新增 `FilterCache`（版本号失效 + 长度前缀 key + 按 retained IDs 计预算），
  缓存 metric group、当日集合和每个过滤器的 posting 集合；同一基准 800 次查询
  命中 2593 / 未命中 7（保留 78 万 IDs），Eq p99 从 27.9 ms 降到 1.8 ms（p50 0.57 ms），
  两标签 Eq p99 4.8 ms，前缀/通用正则 p99 1.45/3.1 ms，四种形态在 20 万序列下
  全部满足 p99<5ms。`SeriesIdSet::intersect` 在一侧小 16 倍以上时改用二分探测，
  避免点查询走完整个当日集合。剩余成本是两个大缓存集合（metric group × 当日集合）
  的线性归并，冷查询在 10M 序列下仍需位图/roaring 持久化表示，留待 Phase 3。
  已新增整查询结果缓存（`resolve_arc` 命中共享 `Arc`，不复制
  百万级结果）；`IndexDb::write_series_batch` 每 4096 序列一个 WAL 帧、metric_engine
  按月批量注册。容量基准（1000 万序列 / 1.6 亿行 / 100 次 flush）：索引 466 MiB、
  48.9 B/序列、写放大 1.43×、写入 1.81 万序列/s（29 万行/s），缓存保留 4820 万个 ID
  （38.5 B/序列）；四种查询形态热态 p50 2 us、p99 7–71 us，均 <5 ms；冷态首查
  1.4–7.7 s（需持久化位图表示）。同负载写入对比（20 万序列 × 50 轮 = 1000 万样本）：
  双写 9.75 万样本/s，索引注册一次性 3.3 s（占写入 3.2%）。剩余验收项仅冷查询位图。
- TSID：24B 布局（metricGroupID/jobID/instanceID/metricID）或等价 u64+冲突检测；
  tsid→metric name/labels 持久化；tsid 缓存。
- IndexDB：命名空间 tag→metricID、metricID→tsid、metricID→name、date metricName→tsid；
  正则 matcher 与匹配代价排序（参考 `index/tag_filters.rs`）。
- 查询：`metric_engine/query.rs` 改为 TSID 集合驱动，删除 index 宽表全扫描路径。
- 验收：1000 万序列下索引内存 < 200 B/序列；Eq/Regex 匹配 p99 < 5 ms；写入吞吐不下降。

### Phase 2：分区、Part、TsFile 风格数据文件、编码与保留（4–6 周）

- 分区管理：按天（可配置）分区，InMemory/Small/Big Part；开放窗口 + late 文件处理乱序；
  分区 drop 实现 O(1) TTL。
- 新文件格式 v1：chunk group（设备/metric）→ column chunk（measurement）→ page（约 8K 行/1 MB），
  页级统计（min/max/first/last/count），文件级 Bloom filter 与稀疏索引。
- 编码：时间戳 TS_2DIFF/bit-packing；浮点 GORILLA/CHIMP；整数 RLE/ZigZag+delta；字符串字典；
  块级 Snappy/LZ4/ZSTD（参考 `esm-encoding`、`block_header.rs`）。
- 去重：`dedup_interval` 语义（同区间内后写覆盖）+ 合并期去重；保留 `__seq__` 作为最终仲裁。
- Compaction：分区内 small/big part 合并（内层/外层），索引 mergeset 独立后台合并；
  写放大目标 < 5×。
- 验收：1–2 B/样本；单设备 30 天查询 p99 < 50 ms；kill -9 恢复 < 30 s；TTL 分区删除可观测。
- 已完成（2026-11-09）：双格式写入/查询（`StorageConfig::format`）、UTC 天分区与 O(1) TTL、
  manifest 单一真相的崩溃恢复与孤儿 GC、count/bytes/close 三触发 compaction、LWW 去重、
  跨天 Parquet 重写；TSF v1 文件/页编解码、CRC32C、列投影、SIMD 与并行解码、Arrow 零拷贝输出；
  `cargo-fuzz` 4 个 target 接入 CI 流程。浮点编码未采用 GORILLA/CHIMP（在此负载实测约
  65 B/样本），改为 `PredictedF64` 多项式预测 + 位模式残差。
- 已完成（T5 实测，50 序列 × 30 天 / 8.64M 行）：TSF 体积 2.46 B/样本（gauge；Parquet 16.71）、
  0.06（counter）。体积验收以 ADR-0002 的 2.5 B/样本为准（本节早期的 1–2 B 目标因浮点位模式
  噪声被 ADR 上调，理由记录在 ADR §4.2/§12）；其余指标：落盘吞吐 1.2–1.8M rows/s
  （Parquet 1.2–1.5M）；全扫描 36–50M rows/s（Parquet 24–25M）；1 天查询 p99 1.4–2.1 ms，
  30 天单序列 p99 9.5–17.3 ms（Parquet 51 ms）；页裁剪 3.3%；重启 187–314 ms。
  全部验收目标通过，详见 `docs/rfcs/20261028-phase2-benchmark.md`。

### Phase 3：查询执行、rollup 与缓存（3–4 周）

- 执行层：TSID 集合 + 时间范围 + 页统计裁剪；向量化过滤/聚合/降采样算子（Arrow）；
  Last Cache（每序列最新值）；rollup 表（1m/1h）与自动选择。
- 查询语言：先扩展现有 Search API（正则、label API），再评估移植 `esm-metricsql`/`esm-promql`
  的 MetricsQL 子集；Grafana 对接。
- 已完成（部分，2026-11-10）：新增 `metric_query_bench` 度量部署态 `DualRead`
  查询路径（索引解析 + 标签重建 + 数据扫描）；legacy 每 metric 的 series-key
  全表扫描改为缓存（新序列写入时失效），窄选择器（eq instance，1 序列）p50
  12.5 ms / p99 59 ms，宽选择器（18 万序列）p50 1.3–1.9 s、正则选择器 296 ms。
  剩余成本是宽选择的标签/样本物化、legacy 联合（逐序列解码 + 正则）与 ns3
  名字全量扫描；下一步为 IndexDb 名字缓存与每 metric legacy 覆盖标记。
- 验收：黄金查询集进 CI；仪表盘类查询 p99 < 50 ms；全量 ad-hoc 明确为秒级 SLO。

### Phase 4：运维与生态（持续）

- 对象存储（S3/OSS）数据与索引 checkpoint、冷热分层、本地块缓存。
- 每指标 TTL、备份恢复、分片/副本与再平衡、慢查询日志、可观测指标。
- TSBS IoT 场景基准、满载查询抖动测试、故障注入（kill -9、磁盘满、时钟回拨）。

## 七、兼容与迁移

- 以配置开关选择引擎（如 `storage.format = "parquet" | "tsfile"`），先支持双读：
  新引擎查询同时读取旧 Parquet SST，写入只走新格式。
- 元数据表（metrics/series/index）可继续留在现有 Parquet 存储，Phase 1 后 index 迁移到 IndexDB。
- 升级路径：manifest 中记录文件格式版本；后台任务逐步重写旧 SST 为新格式；提供回滚开关。
- 现有 WAL/缓冲机制保留，扩展为分区级 WAL 与开放窗口。

## 八、风险

- **对象存储 vs 本地磁盘假设**：mergeset/Part 设计面向本地 FS，需要 checkpoint/缓存层；
  Phase 0 必须定清楚单机与集群两种形态。
- **双引擎过渡期复杂度**：双读、格式版本、compaction 隔离；用开关与迁移任务控制。
- **索引与数据一致性**：TSID/IndexDB 与数据文件需共享 WAL 顺序；崩溃恢复要有明确顺序。
- **许可与归属**：移植 esmetrics/VM 代码需逐文件保留版权与来源说明，补充 NOTICE。
- **性能目标**：毫秒级只对特定查询与热/温数据承诺，SLO 中需写明查询类型与数据范围。

## 九、参考实现索引

- mergeset：`esmetrics/crates/esm-mergeset/src/{table.rs, table_merge.rs, inmemory_part.rs, part.rs}`
- indexDB：`esmetrics/crates/esm-storage/src/index/{mod.rs, create.rs, search.rs, tag_filters.rs}`
- TSID：`esmetrics/crates/esm-storage/src/tsid.rs`
- 分区/Part/Block：`esmetrics/crates/esm-storage/src/{partition/, block.rs, block_header.rs, metaindex_row.rs}`
- 编码：`esmetrics/crates/esm-encoding/src/{encoding.rs, int.rs, nearest_delta.rs, compress.rs}`
- 去重/保留：`esmetrics/crates/esm-storage/src/{dedup.rs, table.rs}`
- IoTDB 文档：TsFile 格式（Apache cwiki）、编码与压缩（Timecho 文档）、标签索引与 Last Cache
  （Apache IoTDB 用户手册）
