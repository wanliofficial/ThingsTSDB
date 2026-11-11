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
  数据在 Legacy 与 New 下对空/eq/neq/re/nre/交集/越界查询结果完全一致。196 项
  storage_kernel 测试 + kill9 集成测试、21 项 metric_engine 测试通过。flush 时按 row key
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
- 已完成（部分，2026-11-11）：Grafana 对接（Prometheus HTTP API 兼容层）。
  `server::prometheus` 在 `/prom/api/v1/*` 下按 Prometheus 的响应信封作答，
  Grafana 的 Prometheus 数据源可直连，无需中间转换器：
  - `GET /prom/api/v1/query`（即时值，`resultType=vector`，默认回看 5 分钟，与
    Prometheus 一致）、`GET /prom/api/v1/query_range`（`resultType=matrix`，
    `step` 以秒为单位，内部用 `search_downsampled(..., Last)` 让每个 step 桶出一点）；
  - `GET /prom/api/v1/series`、`/prom/api/v1/labels`、`/prom/api/v1/label/<name>/values`，
    供 dashboard 的变量下拉使用；三者只取每个序列的最新样本（走 last cache），
    不为一个变量列表去扫全量历史；
  - 时间戳按 Prometheus 语义用「秒」（允许小数），样本值按 Prometheus 的文本形式
    输出（`1.0`、`NaN`、`+Inf`），`__name__` 与其余标签一起进 `metric` 对象。
  - PromQL 只支持**选择器**子集：`metric`、`metric{a="v",b!="v",c=~"re",d!~"re"}`、
    `{__name__="metric"}`；函数、聚合、二元运算、区间向量一律返回
    `errorType=bad_data` 并说明原因，而不是给出一个与面板意图不符的结果。
    解析器单独测试了引号内转义与逗号切分（`m{path="/a\",b"}`）。
  - 实测捕获并修掉一个语义偏差：降采样桶按 epoch 对齐，末桶可能被标到 `end` 之后
    （15 s step 时出现 `end + 15 s` 的点），Grafana 会把这一步画到面板范围外；
    `clip_to_window` 现在裁掉 `end` 之后的点，并丢掉因此变空的序列。
  - 已有 `/api/v1/query`、`/api/v1/last`、`/api/v1/query_range` 保持原有的引擎形状
    响应不变（ADR §12.5、roadmap 均按该形状描述），新接口挂独立前缀。
- 已完成（部分，2026-11-10）：新增 `metric_query_bench` 度量部署态 `DualRead`
  查询路径（索引解析 + 标签重建 + 数据扫描）；legacy 每 metric 的 series-key
  全表扫描改为缓存（新序列写入时失效），窄选择器（eq instance，1 序列）p50
  12.0 ms / p99 25 ms，宽选择器（18 万序列）p50 1.4–1.9 s、正则选择器 207 ms。
  另加 `IndexDb` 的 series name 缓存（ID 不可变、不失效，上限 26.2 万条约 20 MiB），
  正则选择器 p99 从 438 ms 降至 341 ms、索引占比从 139 ms 降至 36 ms。另加每
  metric 的 legacy 覆盖标记（`group_series_ids` + `legacy_union_skips` 统计）：
  当新索引已覆盖该 metric 在查询日范围内的全部 legacy 序列时，`DualRead` 跳过
  legacy 联合；保守判定（迁移前数据或范围内无数据的序列使检查失败）保证旧数据
  仍可见。正则选择器进一步降至 p50 76 ms / p99 179 ms（索引占比 35 ms 为 posting
  扫描本身）。剩余成本是宽选择的标签/样本物化与冷查询位图表示。
- 已完成（部分，2026-11-10）：Last Cache 与降采样读路径。写入成功后在
  `MetricEngine::write` 内镜像每序列最新样本到 `LastCache`（默认 100 万序列、满后不再
  接纳新序列而不淘汰），`MetricEngine::last_values` 直接命中缓存，缺失时回退区间扫描
  并回填；20 万序列下即时值查询 p50 8.5 ms / p99 17.5 ms（同选择器的区间查询
  12.2 ms）。乱序写入保留最大时间戳、同时间戳后写覆盖。另加
  `search_downsampled(req, step_ms, agg)`（epoch 对齐分桶，avg/min/max/sum/count/last），
  即存储型 rollup 的读取形态；server 暴露 `/api/v1/last` 与
  `/api/v1/query_range?step=&agg=`。剩余：写入时维护的 1m/1h rollup 表与自动选择。
- 已完成（2026-11-11）：写入时维护的 1m/1h rollup 表与自动选择。`metric_engine::rollup`
  在 `write` 成功路径把样本折进分钟/小时桶（`count/sum/min/max/first/last`，`first/last`
  带时间戳以便粗粒度重聚合），只发布已关闭桶（`bucket_end + interval <= now`）；水位
  `{root}/rollup_state/{1m,1h}` 分割时间，`< wm` 读表、`>= wm` 读原始数据，先写表后推进
  水位，flush 失败回插桶并保持水位不变。迟到写入把该桶标 dirty 并在下次 flush 按原始
  数据重算重发，重算前 `coverage()` 降到 dirty 桶起点，查询因此回退原始扫描而不信任
  旧聚合。`search_downsampled` 按 step 自动选表（1h / 1m / 回退 raw），区间在桶边界处
  切成「前导部分桶 raw + 已发布表 + 尾部 raw」，避免历史桶里的区间外样本污染结果。
  三种集成测试以 raw `downsample` 为基准逐点比对：6 种聚合 × 3 种 step（含 1h 重聚合）
  × 3 种区间（含非对齐起点）、乱序/迟到写入与 dirty 重建、90 s 回退与重启后仍一致。
- 已完成（2026-11-11）：黄金查询集进 CI。`src/metric_engine/tests/golden_query_set.rs`
  固定部署（3 个 metric、8 个序列、一天确定性样本）加载后回答 19 条查询：4 种匹配器
  形状（`=`/`!=`/`=~`/`!~`）、仅 metric、双标签、limit 截断、亚分钟窗口、非对齐起点、
  空区间，以及 6 种聚合的 1m 降采样与 5m/1h 重聚合、90 s 回退；答案与
  `tests/golden/query_set.json` 比对（原始区间用「点数 + 顺序敏感校验和 + 首尾点」摘要，
  降采样区间保留全部点），并用 `THINGS_TSDB_UPDATE_GOLDEN=1` 显式刷新、diff 可审阅。
  同一次运行内还断言每条降采样查询与 raw `downsample` 逐点一致，因此 diff 同时指出
  是哪个读取路径变了。另有元测试保证黄金集覆盖全部匹配器与全部聚合、无重名，新形状
  不能悄悄漏测。`make test`（CI 的 unit-test job）已包含该集成测试。
- 已完成（2026-11-11）：仪表盘查询延迟验收（p99 < 50 ms）。新增
  `benchmarks --bin metric_rollup_bench`：200 序列 × 一天 10 s 采样（172.8 万样本）写入
  后，比对 `search_downsampled`（读 1m/1h rollup）与 `search` + `downsample`（全量
  raw）在同一查询形状下的延迟。结果（release，本机）：
  - 仪表盘形状（`limit=5`、一天、1m step、7200 点）：rollup p50 10.0 ms / p99 11.3 ms，
    raw p50 29.2 ms / p99 35.8 ms；
  - 1h max（`limit=5`）：rollup p99 5.6 ms，raw p99 33.8 ms；
  - 宽面板全 200 序列（一天、1h step、4320 点）：rollup p50 5.7 ms / p99 6.4 ms，
    raw p50 481 ms / p99 603 ms（85×）；
  - 宽面板 1m step（25.9 万输出点）rollup p99 169 ms：受限于必须物化的点数本身，
    与读取路径无关（raw 同形状 p99 549 ms）。
  即仪表盘类查询 p99 远低于 50 ms 目标，rollup 把宽时间范围的扫描成本从「样本数」
  降到「桶数」。
- 验收：黄金查询集进 CI（已完成）；仪表盘类查询 p99 < 50 ms（已完成，见上）；
  全量 ad-hoc 明确为秒级 SLO。

### Phase 4：运维与生态（持续）

- 已完成（部分，2026-11-10）：查询可观测性与慢查询日志（ADR §12.5.5、§13）。
  `server::metrics::ServerMetrics` 用原子计数器 + 累积延迟直方图（Prometheus
  桶模型）按查询类型（range/last/range_step）统计 p50/p99 分位数、series/样本
  总数与 4xx/5xx；查询超过阈值（默认 50 ms）输出结构化慢查询 `warn!` 日志。
  server 暴露 `/api/v1/stats`（Prometheus 文本）。联调时发现并修复一个潜在崩溃：
  `IndexDbManager` 查询区间现钳制到磁盘上存在的月分区跨度——HTTP API 默认开区间
  上界为 `i64::MAX`，此前使 `month_of_millis`（`(year-1970)*12`）溢出并 panic，
  也会打开数百个空的未来月分区；区间完全落在磁盘跨度外时坍缩为空区间、不打开任何
  月分区。回归测试覆盖 search/last_values 的 i64::MAX/未来/越界场景。
- 已完成（部分，2026-11-10）：每指标 TTL（读时裁剪）。`metric_engine::RetentionPolicy`
  保存 metric→retention 映射（`Arc<HashMap>` + copy-on-write，读无锁），
  `search`/`last_values`/`search_downsampled` 在解析索引前把区间下界抬到
  `now - retention`，因此过期样本对查询不可见——即使它与长保留期的指标共享同一
  segment。物理回收仍交给全局 segment TTL（按 segment 整体丢弃，保持 O(1)，不因
  查询重写共享 segment）。server 配置 `[metric_engine.retention.per_metric]`
  （如 `http_requests = "30d"`），经 `MetricEngine::with_retention` 传入。
  单测覆盖空策略、抬下界、区间坍缩、未配置指标、非正保留期、copy-on-write、
  时钟回拨 underflow；集成测试证明过期样本在 search/last_values 中不可见、近期样本
  存活、未配置指标不受影响、更宽窗口可见两段。
- 已完成（部分，2026-11-10）：备份恢复。`MetricEngine::flush_all` 把四个列存表
  （metrics/series/index/data）与 mergeset 索引刷到持久点，`backup(dest)` 再把引擎根
  前缀下所有对象（列存表 + mergeset 索引 + 本地 WAL）复制到目标前缀；`restore_from`
  反向复制后在新前缀打开引擎即得到同样的查询结果。`metric_engine::backup::copy_prefix`
  走 `ObjectStore::list/get/put`，因此 Local 与未来的 S3/OSS 通用；同前缀来源/目标
  直接报错。端到端测试用 `IndexMode::New` 证明 mergeset 索引也被复制、查询一致；
  单元测试覆盖目录布局保持与本地目录递归复制。S3/OSS 对象存储接线与跨桶备份仍待接。
- 已完成（部分，2026-11-10）：并发负载与查询抖动测试。4 个 writer 与 4 个 reader
  并发：writer 持续向 16 个序列写入、reader 并发执行 last_values/区间查询。断言不变量
  （无查询报错、解析出的序列数是已写集合的子集、写完后全量可见且一致）并输出读延迟
  分布作为抖动证据（160 次读：p50 0.46 ms / p99 37.9 ms / max 42.4 ms）。
- 已完成（部分，2026-11-10）：本地块缓存。`storage_kernel::mergeset::block_cache::BlockCache`
  （按字节预算 + FIFO 淘汰）此前是死代码，现接入 mergeset 读取路径：`Part` 新增
  `block_cache` 字段，`load_index_block` 命中缓存时直接反序列化缓存的 header，跳过
  pread + zstd 解压；缓存键为 `{part_dir}/{row_idx}`，条目以紧凑的 on-disk 编码保存，
  命中只需一次廉价 unmarshal。`PartManager::with_block_cache` 让 `recover_from_disk`、
  `flush_once`、`merge_once` 发布的所有 part 共享同一预算；`IndexDbConfig.block_cache_bytes`
  （默认 64 MiB，0 关闭）在 `IndexDb::open` 里装配。测试覆盖命中/未命中的查询结果
  与无缓存路径逐字节一致、1 字节预算下淘汰后仍正确。
- 已完成（部分，2026-11-10）：故障注入。`columnar_storage::fault` 提供装饰器式
  `FaultyStore`（`ObjectStore` 包装器，不依赖真实磁盘）：`Fault::operation(op, n)` 让
  某操作的前 n 次（`u64::MAX` = 永远）失败，`Fault::location(needle)` 让路径含子串的
  操作失败，命中次数记入 `FaultReport` 供断言；`SkewedClock` 复现时钟回拨，
  `SlowPutStore` 加宽并发抖动窗口。集成测试（`tests/fault_injection.rs`）验证三条
  不变量：数据文件 PUT 失败时 flush 的磁盘满错误传播到调用方、manifest 从不引用
  未提交文件（读要么报错要么看到空表）、缓冲帧被 `reinsert` 回 WAL 待重试；
  全部 PUT 失败时写入/刷盘报错而表保持一致；时钟回拨一小时或超前不丢行不 panic。
- 已完成（2026-11-11）：压实后延迟删除，修掉读-压实竞态。TSBS IoT 基准暴露一个真实
  bug：压实先在 manifest 提交新文件、随即删除旧文件，而已经按旧 manifest 解析出文件
  列表的读（这里是 rollup 的 dirty 桶重建扫描）打开这些对象时得到 `NotFound`，整个
  查询失败。现在 `SchedulerConfig::delete_grace`（默认 60 s，0 = 立即删除）控制删除
  延迟，`do_compaction` 提交后 `spawn` 一个延迟任务再删对象；`gc_orphans` 仍兜底回收
  任何残留。回归测试 `test_do_compaction_keeps_inputs_readable_before_the_grace_period`
  断言：压实提交后 manifest 只剩输出、按新 manifest 扫描能读回全部行、输入对象在宽限
  期内仍存在，并直接调用同一个 `delete_paths` 证明它们确实可回收。
  rollup 侧另加 `scan_data_with_retry`：重建 dirty 桶时若仍撞上已消失的文件，重解析
  manifest 后重试（最多 4 次，20 ms 间隔），把已发生的竞态从「查询失败」降级为
  「一次重试」。另把 `rebuild_dirty` 从「每个 dirty 桶一次扫描」改为「每个 metric 一次
  扫描」：一次全网迟到写入会标记上千个桶，逐桶扫描会把同一批文件读上千遍。
- 已完成（2026-11-11）：修掉 mergeset 刷盘的读可见性空洞。全量测试跑出
  `indexdb` 偶发「搜索返回空集」，根因不是测试抖动：`PartManager::flush_once` 先用
  `take_pending_with_seq` 把待刷条目从 `Table::pending` 取走，再排序、写 part、
  注册到 `Table.parts`，这中间读路径既看不到 pending 也看不到新 part —— 已 ack 的
  写入会短暂消失，一次 `indexdb` 搜索因此可能少返回系列。现在被取走的批次保存在
  `Table::flushing`（连同其 `flushing_seq`），`search_prefix`/`search_range` 把
  `pending` 与 `flushing` 并起来读；part 注册后 `finish_flush` 才清空，写 part 失败
  则 `abort_flush` 把条目（及其序列区间，保证重试的 part 头仍覆盖同一批 WAL 帧）放回
  pending。回归测试 `test_taken_batch_stays_searchable_until_its_part_is_published`
  在去掉 `flushing` 合并后立刻失败（读到 0 条），`test_aborted_batch_returns_to_pending`
  覆盖失败回滚。
- 已完成（2026-11-11）：修掉重启后 part id 复用导致的首次刷盘失败。`PartManager` 的
  part id 来自内存计数器 `next_part_id`，新进程从 0 开始；而 `recover_from_disk` 只把
  `parts.json` 里的 part 重新注册，没有把计数器推到已有 part 之后。于是**每次重启后**
  第一次 `flush_once` 都会尝试分配 `part-…0001`，与磁盘上已存在的同名目录冲突，
  `PartWriter::finish` 返回 `part … already has a metadata.json`，该轮刷盘失败（行被
  `abort_flush` 放回 pending），要等计数器追平历史高水位才能恢复写入。实测在重启的
  server 上复现（`part-manager loop error`）。现在 `recover_from_disk` 结束时扫描目录，
  把 `part-*` / `merge-*`（含中断合并留下的残留）里的最大 id 推进到计数器。
  回归测试 `test_flush_after_restart_does_not_reuse_a_part_id`：刷出一个 part，新
  `PartManager` 恢复后再刷，断言成功且新 part 与旧 part 目录不同。
- 对象存储（S3/OSS）数据与索引 checkpoint、冷热分层。
- 已完成（2026-11-11）：静态分片、quorum 副本与按前缀复制的再平衡。
  `metric_engine::cluster` 是一张纯函数的放置表 + 每 `(node, shard)` 一个引擎目录：
  - **series → shard** 用 jump consistent hash（`jump_hash`）。无状态、跨进程一致，
    桶数从 `n` 变到 `n+1` 只搬约 `1/n` 的 series —— 这是「改分片数很便宜」的前提。
  - **shard → node** 用 rendezvous hashing 取分数最高的 `replicas` 个节点，同分按
    名字排序，因此每个节点各自算出的放置完全相同。加/删节点只移动它自己的 shard。
  - 数据布局 `{root}/{node}/shard-{n}`，一个 shard 就是一个引擎根，所以**迁移一个
    shard = 复制一个前缀**，复用 `backup::copy_prefix`；本地盘和对象存储同一套代码。
  - 写入复制到 shard 的全部 owner，按 `replicas/2 + 1` 计 quorum；未达 quorum 返回
    「write below quorum」错误，达到 quorum 但有副本失败则通过 `WriteReport::failed`
    暴露降级（server 打 `warn!`），不静默吞掉。
  - 读查询全部引擎再合并：shard 之间的 series 不相交，所以合并只在副本之间去重
    （按 timestamp），结果按 `(metric, labels)` 排序，同一查询重复执行结果稳定。
  - 再平衡有两种入口：`rebalance(from, to)` 有旧拓扑时按 `migrations` 只复制真正换了
    owner 的 shard；`rebalance_from_disk(to)` 没有旧拓扑时（进程重启后的常态）直接
    扫盘算出「某副本的前缀为空而另一个有数据」，既覆盖新节点加入，也覆盖副本目录丢失
    后的补齐。二者都只复制、不删除；删数据是要人显式决定的操作。
  - server 侧：`[metric_engine.cluster] nodes/shards/replicas` 配置后走 `Backend::Cluster`
    （`write`/`search`/`last_values`/`search_downsampled`/`compact`/`flush_all` 都按
    cluster 语义分发），`GET /api/v1/cluster` 输出放置表，`POST /api/v1/rebalance`
    先 `flush_all` 再补副本，`?plan=1` 只出计划、`?nodes=a,b,c` 按新拓扑复制
    （`restart_required` 提示引擎在启动时固定，换放置需重启）。
  - 集成测试（`tests/cluster_lifecycle.rs`）走完整生命周期：写入 → 关掉重开（模拟重启）
    → 加节点后按新拓扑复制 → 删掉一个副本目录（模拟丢盘）后自愈 → 再读。断言方式不是
    「拿单个 shard 和集群总数比」（那只有在 shard 数 = 1 时才对），而是先按
    `ShardMap::shard_of` 和 series key 算出每个 shard 应持有哪些 hostname，再逐
    `(node, shard)` 目录核对，最后和集群整体读数对照，从而同时验证「放置正确」和
    「副本齐了」两件事。
  **明确不是共识协议**：没有选主、没有日志复制、没有自动修复。模型是「N 个节点持有同一
  shard 的相同副本、quorum 写、读任意节点合并」；落后节点靠重拷前缀或 `restore_from`
  追上。server 内的 cluster 是单进程内的放置模拟（每节点一个目录），因为本仓库还没有
  节点间 RPC。
- 已完成（2026-11-11）：TSBS IoT 场景基准。`benchmarks --bin tsbs_iot_bench`
  按 TSBS「IoT」用例的形态造数：N 台设备各带 `host`/`region` 标签，每台每 tick
  上报 8 个 reading（`temperature`、`humidity`、`pressure`、`wind_speed` 等），
  5% 的 tick 按「设备掉线后补传」
  重排（先发新样本、再发窗口内较旧的样本），这正是 out-of-order 写路径 + rollup
  dirty 重建要处理的形态。默认参数 500 设备 × 8 reading × 60 min = 1.44 M 样本、
  4000 个序列，实测（14 GiB / 8 核开发机，release）：
  - 写入吞吐 109,751 samples/s（1.44 M 样本 13.1 s，末尾 flush 2.0 s）；
  - `range` 查询按 `region` 过滤（125 序列）：p50 29.7 ms / p99 35.2 ms；
    单设备（1 序列）：p50 12.4 ms / p99 14.8 ms；宽 `service` 面板（63 序列）：
    p50 28.5 ms / p99 32.4 ms；
  - `latest` 全车队（500 序列）：p99 1.9 ms；
  - 降采样：1m（3780 点）p99 71.6 ms，1h（63 点）p99 5.5 ms。
  基准先跑出来的第一个结果不是延迟而是 panic：压实删掉了 rollup 重建正在读的文件，
  即上面「压实后延迟删除」那条的起因。

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
