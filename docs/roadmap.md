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
    `step` 以秒为单位，在 `[start, end]` 上逐 step 求值，见下面 PromQL 子集一节）；
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
- 已完成（2026-11-11）：PromQL 表达式子集（`server::promql`）。
  `/prom/api/v1/query` 与 `/query_range` 从「只认选择器」升级为一个真正的求值器：
  - **选择器**：`metric`、`metric{a="v",b!="v",c=~"re",d!~"re"}`、`{__name__="metric"}`，
    支持 `offset 5m` 与 `@ <ts>` / `@ start()` / `@ end()`；
  - **区间向量** `metric[5m]` 与区间函数：`rate`、`irate`、`increase`、`delta`、
    `idelta`、`changes`、`resets`、`deriv`、`present_over_time`、`mad_over_time`、
    `first_over_time`、`ts_of_first_over_time`、`ts_of_last_over_time`、
    `ts_of_max_over_time`、`ts_of_min_over_time`、`predict_linear`、
    `double_exponential_smoothing`、`integral`、
    `*_over_time`（`avg/min/max/sum/count/last/stddev/stdvar/quantile`）。
    `rate`/`increase` 按 Prometheus 规则处理计数器回绕（下降视为归零后继续累加），
    单位是「每秒」；`deriv` 是最小二乘斜率（每秒），只有 1 个样本时该序列不出现；
  - **即时函数**：`abs/ceil/floor/sqrt/exp/ln/log2/log10/sgn/round/clamp/clamp_min/
    clamp_max/timestamp/scalar`，三角函数与双曲函数
    `sin/cos/tan/asin/acos/atan/sinh/cosh/tanh/asinh/acosh/atanh/deg/rad`，
    以及 `sort`、`sort_desc`、`sort_by_label`、`sort_by_label_desc`、
    `sort_by_label_numeric`、`sort_by_label_numeric_desc`、`absent`、`vector()`、
    `time()`、`pi()`，查询上下文标量 `start()`/`end()`/`range()`/`step()`、
    `max_of`/`min_of`，MetricsQL 的 `*_over_time` 家族（`range_over_time`、
    `median_over_time`、`sum2_over_time`、`geomean_over_time`、`distinct_over_time`、
    `mode_over_time`、`increases_over_time`、`decreases_over_time`、
    `tfirst_over_time`、`tlast_over_time`），日期函数 `year/month/day_of_month/day_of_week/day_of_year/
    days_in_month/hour/minute`，以及 MetricsQL 的序列选择函数
    `drop_common_labels`、`drop_empty_series`、`labels_equal`、
    `label_graphite_group`、`limit_offset`、`union`（见下面单独一条）；
    （`histogram_quantile` / `histogram_quantiles` / `histogram_fraction` /
    `histogram_share` 见下面单独几条）
  - **聚合**：`sum/avg/min/max/count/group/stddev/stdvar/topk/bottomk/quantile/
    count_values`，以及 MetricsQL 的 `any/sum2/geomean/distinct/median/mode/mad/
    share/zscore/limitk/outliers_iqr/outliers_mad/outliersk/quantiles/histogram`（见
    下面单独条目），支持
    `by (...)`（前置与后置两种写法）与 `without (...)`；聚合结果去掉 `__name__`
    并保留分组标签，与 Prometheus 一致。不写 `by`/`without` 时按官方文档「聚合
    全部标签维度」处理，即整个向量并成一条无标签序列（`sum(m)` 只有一个
    `{}`）；注意这与显式的 `without ()` 不同 —— 后者保留除 `__name__` 外的
    全部标签，因此每个标签集各自一条；
  - **二元运算**：算术 `+ - * / % ^`、三角 `atan2`、比较 `== != > < >= <=`（含 `bool` 修饰符）、
    集合运算 `and / or / unless`；标量-向量与向量-向量（按除 `__name__` 外的全标签
    精确匹配）；算术结果丢弃 `__name__`，比较过滤时保留左侧标签与原值。
  - **向量匹配**（2026-11-11 补齐）：`on(...)` / `ignoring(...)` /
    `group_left(...)` / `group_right(...)`，语义照抄 Prometheus 的
    `VectorBinop`/`resultMetric`：
    - 匹配键 = `on` 列的标签，或「除 `__name__` 与 `ignoring` 列出的标签外的全部
      标签」；右侧是 "one" 侧，`group_right` 按镜像的 `group_left` 求值再还原
      操作数顺序（`a group_right b` 的除法不会算成 `b / a`）；
    - "one" 侧同一匹配键出现两条序列 → 报错并列出两条序列，指出这是一次
      隐式的 many-to-many；
    - one-to-one 匹配下同一条左侧序列被匹配两次 → 报错提示必须显式写
      `group_left`/`group_right`；`group_x` 下结果标签重复 → 报错提示分组标签
      必须保证唯一；
    - one-to-one 结果里 `on` 只保留匹配标签、`ignoring` 删掉这些标签；
      `group_x(...)` 的标签取自 "one" 侧；算术与 `bool` 比较丢弃 `__name__`，
      过滤型比较保留；
    - 集合运算强制 many-to-many（出现 `group_x` 直接报错），样本原样透传
      并保留 `__name__`；
    - 标量一侧带匹配修饰符、集合运算用于标量表达式，都报错而不是静默忽略；
      Prometheus 的 `fill`/`fill_left`/`fill_right` 明确点名不支持。
  - `label_replace` / `label_join`（2026-11-12 补齐，见下一条）只改标签、不动值与
    时间戳，因此走普通的逐 step 求值，不需要额外的存储支撑。
  - **求值策略**：每个选择器只向引擎要一次**原始样本**，窗口由表达式里最远的
    `offset`/`@`/range 推出（`Expr::scan_window`），之后每个 step 在内存里按
    Prometheus 的取点规则求值（即时选择器取 5 分钟回看内的最后一个样本，区间向量
    左开右闭）。这样 step 数不放大引擎调用次数。代价是这条路径不走 rollup 表
    （`search_downsampled`），宽区间查询读的是原始点；把求值器接到 rollup 上
    是后续工作。
  - 区间查询按 `start + k*step` 逐点求值，因此**不会**产生晚于 `end` 的点——
    这正是上一版用 epoch 对齐降采样后必须靠 `clip_to_window` 裁掉的偏差，新的
    求值路径从构造上消除了它。
  - 单元测试覆盖：运算符优先级（`1 + 2 * 3`、`^` 右结合、比较低于算术）、
    复合时长（`1h30m`）、四种匹配符与引号转义、聚合分组键（`by`/`without` 对
    `__name__` 的处理）、计数器回绕下的 `rate`/`increase`/`delta`/`changes`/`resets`、
    分位数插值、以及「不支持的构造必须报出具体名字」。
  - 向量匹配的单元测试覆盖：修饰符的解析形状（`on`/`ignoring`/`group_left`/
    `group_right` 是否带标签、`bool` 的顺序、集合运算隐式 many-to-many）、`on` 与
    `ignoring` 对结果标签的取舍、"one" 侧重复匹配键与 one-to-one 重复匹配的报错
    文案、`group_left` 从 "one" 侧取标签、`group_right` 的镜像求值（除法与比较的
    操作数顺序）、未匹配序列被丢弃、`and`/`or`/`unless` 按匹配键过滤并保留
    `__name__`、以及 `bool` 比较丢弃 `__name__` 而过滤型比较保留。
  - 端到端测试（走 actix 路由 + 真实引擎）覆盖：`sum by (job)` 出两条序列、
    `rate(metric[60s])` 对 0,1,2,3（10 s 间隔）按 `extrapolatedRate` 外推到窗口
    两端（计数器起点钳在零穿越处，因此 0→3 得 2/30、10→13 得 0.075）、`topk`、比较过滤与
    算术丢弃 `__name__`、`http_requests / on(job) http_retries` 得到 1.5/3.25、
    `group_left(zone)` 把 `zone` 带到三条 `errors` 序列上、`on(job)` 匹配
    两条同 `job` 序列时返回 400 并报 `found duplicate series`、
    `and on(job)` 保留 `__name__`，以及 `rate(裸选择器)` 返回 400 `bad_data`。
  - 已有 `/api/v1/query`、`/api/v1/last`、`/api/v1/query_range` 保持原有的引擎形状
    响应不变（ADR §12.5、roadmap 均按该形状描述），新接口挂独立前缀。
- 已完成（2026-11-11）：MetricsQL 子集（同一个 `server::promql` 求值器）。
  VictoriaMetrics 的 Grafana 数据源发的是 MetricsQL，其中一部分只是 PromQL 的
  超集语法，这里补上不需要额外存储支撑的几个：
  - `aggr_over_time(<agg>, v[range])`——聚合名可裸写也可加引号（`avg` / `"avg"`），
    接受 `sum/avg/min/max/count/stddev/stdvar`；`topk`/`bottomk` 要额外参数、
    `group` 对时间轴无意义，解析期即拒绝并点名；
  - `absent_over_time(v[range])`——区间内一条样本都没有时返回单条 `1`，否则返回空
    （与 Prometheus 同名函数同义）；`1` 的**标签**同样照 Prometheus 从区间向量选择器的
    等值匹配器推导（`absent` 同款 `createLabelsForAbsentFunction`：非等值匹配器删除
    对应标签、子查询/非选择器参数退化为无标签 `1`）；
  - `tmin_over_time` / `tmax_over_time`——返回极值样本的**时间戳**（秒）而不是值；
    这是 VM 的 `rollupTmin` / `rollupTmax`，比较用 `<=` / `>=`，所以并列极值时
    **最后一个**样本胜出（`[5,3,3]` 的 `tmin` 是第 3 个样本的时间戳），`-0`/`+0`
    也算相等；输入同样先过 `removeNanValues`，窗口里只有 NaN 时没有结果；
  - `predict_linear(v[range], t)`——对区间内样本做最小二乘直线拟合，**以求值时刻
    （`enh.Ts`）为原点**再外推 `t` 秒（与 Prometheus 的 `funcPredictLinear` 一致：
    拟合斜率而非取端点差；原点也必须与上游相同，否则最后采样点不在求值时刻时结果
    会整体偏移）；
  - `default(v, x)`——结果为空时补一条无标签的 `x`，已有样本中的 `NaN` 也换成 `x`
    （MetricsQL 把 NaN 当缺失值）。
  - `*_over_time` 家族（2026-11-13 补齐）：`range_over_time`（max-min）、
    `median_over_time`（VM 的 `quantile(0.5, …)`，先丢 NaN 再排序，因此与
    Prometheus 的 `quantile_over_time(0.5, …)` 在含 NaN 时结果不同）、
    `sum2_over_time`（平方和）、`geomean_over_time`（`prod^(1/n)`）、
    `distinct_over_time`（不同取值个数，`-0` 与 `0` 算同一个，照 Go map 的语义）、
    `mode_over_time`（`modeNoNaNs`：排序后最长等值游程，
    首个值按 VM 的 `prevValue` 逻辑参与）、`increases_over_time` /
    `decreases_over_time`（后者就是 `rollupResets`）、以及 `ts_of_*` 的
    MetricsQL 名字 `tfirst_over_time` / `tlast_over_time`。
    全部按 `rollup.go` 逐条对照，包括 `math.Abs(v-prevValue) < 1e-12*math.Abs(v)`
    的浮点噪声容忍。MetricsQL 把 NaN 当**缺口**：VM 在调用任何 rollup 之前先跑
    `removeNanValues`，所以这些函数先丢掉 NaN 样本（`distinct_over_time` 因此不会
    把 NaN 算成一个取值；整窗都是 NaN 时该序列不产出）；这正与 Prometheus 的
    `*_over_time` 保留 NaN 传播的做法不同，两者各自照自己的上游。这些函数的
    `prevValue` 恒为 NaN（上一窗口的样本不参与），与 VM 在序列起点的行为一致。
    均丢弃 `__name__`：VM 的 `rollupFuncsKeepMetricName` 默认保留
    名字（`avg_over_time`/`min_over_time`/`median_over_time`/`mode_over_time`
    都在表里），而本服务走 Prometheus 语义——只有 `first_over_time` /
    `last_over_time` 保名，其余 `*_over_time` 一律丢名，新函数沿用同一约定。
  - `rollup_*` 明确不做：它以 VictoriaMetrics 自己的 rollup 表为前提，而本服务的
    求值仍读原始样本，接受它会画出与 VM 不同的数——报错比给出错数好。
  - 其余 MetricsQL rollup 也明确不做：`zscore_over_time`（要按 0.6 分位估计
    scrape interval 再算 lag）、`integrate` / `lag` / `lifetime` /
    `ascent_over_time` / `descent_over_time` / `tlast_change_over_time`（要窗口前
    后之外的样本，也就是 VM 的 `prevValue` / `realNextValue`）、
    `count_values_over_time` / `quantiles_over_time`（一条输入序列产出多条输出，
    要给 range 求值加「一进多出」的通路）、
    `scrape_interval` / `stale_samples_over_time`（要 VM 自己的 staleness 标记与
    step 上下文）。这些缺的都是窗口外的数据或输出形状，硬做只能给出近似值。
    （阈值类 rollup `count_*` / `share_*` / `sum_*_over_time` 与 `duration_over_time`
    原先列在这里，核对源码后发现只依赖窗口内的样本与一个标量参数，已按 2026-11-14
    条目实现。）
  - 测试：解析与求值单测（`aggr_over_time` 的六种聚合、`tmin/tmax` 的时间戳语义、
    线性拟合的期望值、`aggr_over_time(topk, ...)` 与缺 horizon 的 `predict_linear`
    报错、新 `*_over_time` 家族各一条：max-min、NaN 被当作缺口丢弃后的中位数与
    整窗 NaN 的序列丢弃、平方和、几何平均、`-0`/`0` 的 distinct 计数、mode 的
    并列与单样本、increases/decreases 与 1e-12 容差、`tfirst/tlast` 与
    `ts_of_*` 等价），以及走 actix 路由的端到端用例（`aggr_over_time(avg, ...)`=1.5、
    `predict_linear(..., 3600)`=364、`default(不存在的指标, 7)` 单条无标签 7、
    `absent_over_time(不存在的指标[60s])`=1）。
- 已完成（2026-11-13）：MetricsQL 标签函数 `label_set` / `label_del` /
  `label_keep` / `label_copy` / `label_move` / `label_value` / `alias`
  （按 `app/vmselect/promql/transform.go` 逐条对照）：
  - `label_set(v, "a", "1", ...)` 按顺序写入 name/value 对，同一标签出现两次时后者
    生效，**空值删除该标签**；`alias(v, "name")` 就是
    `label_set(v, "__name__", "name")`（MetricsQL 的 `with` 模板里也是这么重写的）；
  - `label_del(v, "a", ...)` 删除点名标签；`label_keep(v, "a", ...)` 只留下点名的，
    `__name__` 在两者里都**只当普通标签**——`label_keep(v, "job")` 会连指标名一起
    丢掉（VM 的 `RemoveTagsOn` 在 `__name__` 没被保留时 `ResetMetricGroup`）；
  - `label_copy(v, "src", "dst", ...)` 源标签缺失或为空时**跳过**（不删 dst），
    `label_move` 复制后再删源，但 src 与 dst 同名时不删；
  - `label_value(v, "label")` 把每条非 NaN 样本的值换成该标签文本解析出的浮点
    （解析失败为 NaN），并丢掉 `__name__`；NaN 样本原样保留（VM 把 NaN 当缺口，
    `label_value(q, "l") default 123` 才有意义）；
  - 标签名不校验：VM 的标签是任意字节串（`k8s.io/name` 合法），这里同样接受；
    `label_replace` 的 dst 校验是 Prometheus 侧的规定，两者各自照上游。
  - 测试：`label_set` 的顺序/空值删除/无参数、`alias` 与 `label_set(__name__)` 等价、
    `label_del`/`label_keep` 对 `__name__` 的处理、`label_copy` 的缺失源跳过、
    `label_move` 的同名不删、`label_value` 的数值/非数值/缺标签/NaN 四种情况，
    以及奇数个字符串参数、非字符串参数、标量输入的报错。
- 已完成（2026-11-13）：MetricsQL 的另一批标签函数 `label_map` /
  `label_uppercase` / `label_lowercase` / `label_transform` / `label_match` /
  `label_mismatch`（同属 `transform.go`）：
  - `label_map(v, "label", "old", "new", ...)` 把参数折成一张 map，**只替换命中
    的取值**，未列出的取值原样保留；映射到空串时删除该标签（VM 的 `RemoveTag`）。
    同一键出现两次时后者覆盖（VM 用 Go map 存这张表，后写覆盖先写），缺失的标签
    按空串参与查表——所以 `"" -> "x"` 的映射会**新建**该标签，这同样是 VM 的
    `getDstValue` 行为；
  - `label_uppercase` / `label_lowercase` 只改点名的标签，其他标签不动；VM 至少
    要求一个标签名（`not enough args`），这里同样拒绝零参数；
  - `label_transform(v, "label", "regex", "replacement")` 用**未锚定**的 Go 正则
    做 `ReplaceAll`：替换标签值里每一处匹配，替换串按 Go `regexp.ExpandString`
    的语法展开（`$1`、`${name}`、`$$` 为字面 `$`）；展开结果为空串时删除该标签
    （VM 的 `RemoveTag`）；源标签缺失时按空串读入，因此能匹配 `""` 的正则会**新建**
    该标签（VM 的 `getDstValue` 会追加一个 tag）；`expand_replacement` 与
    `label_replace` 共用，故 `$1x` 的「最长名字」规则、行尾 `$`、未闭合的 `${`
    与空的 `${}` 都按字面复制；
  - `label_match` / `label_mismatch` 用**锚定**正则（`^(?:re)$`）过滤序列——
    匹配的留下 / 不匹配的留下，缺标签按空串处理，因此 `.*` 命中而 `.+` 不命中；
  - 正则都在解析期编译，失败时报 `bad_data` 并点名函数（`label_transform` 与
    `label_match` 的锚定差异照 `metricsql.CompileRegexp` /
    `CompileRegexpAnchored`）。
  - 测试：`label_map` 的命中/未命中/映射为空/同名键后写覆盖/`""→x` 新建标签、
    大小写只动点名标签、`label_transform`
    的逐处替换与捕获组展开（含空展开删标签、缺失标签被 `""` 匹配时新建、`${}`
    字面复制）、`label_match`/`label_mismatch` 的锚定语义与互补关系、
    缺标签按空串匹配、向量过滤后只剩一条；以及奇数参数、零标签名、非法正则、
    缺逗号、标量输入的报错。
- 已完成（2026-11-13）：MetricsQL 经典桶的汇总函数 `histogram_avg` /
  `histogram_stddev` / `histogram_stdvar`（`transform.go` 的
  `avgForLeTimeseries` / `stdvarForLeTimeseries`）。同名函数在 Prometheus 里只读
  native histogram（本服务没有该表示，roadmap 下文据此把它们列为不做），
  VictoriaMetrics 则按经典 `le` 桶估算，这里实现的是后者：
  - 每个桶视为在「上一个边界, 本边界」之间均匀分布，权重取累计计数的增量，
    代表值取区间中点；第一个区间的下界按 VM 的假设从 0 起（`lePrev = vPrev = 0`）；
  - `+Inf` 桶没有中点，被跳过，且不参与 `lePrev`/`vPrev` 的推进（照抄 VM 的
    `continue`）；
  - 权重总和为 0（只有 `+Inf` 桶，或各桶累计计数都是 0）时返回 NaN；方差用
    `E[x²] - E[x]²`，负数按 VM 的做法钳到 0，`histogram_stddev` 再开方；
  - 分组与 `histogram_quantile` 一致（除 `le`/`__name__` 外的全部标签），缺 `le` /
    `le` 不可解析 / NaN 的序列被跳过而不是报错；结果丢弃 `__name__` 与 `le`；
  - VM 的 `convertBucketsToVMRange` 那套 `vmrange` 标签转换不在这里实现——本服务
    只产出经典 `le` 桶（这也是既有 `histogram_quantile` 的约定）；
  - 测试：2/4/6/8 累计计数下 avg=2.2/6、方差 `E[x²]-E[x]²` 与 stddev=√var、
    `+Inf` 单桶与全 0 计数都返回 NaN、计数不变的非零直方图只计第一个区间的权重
    （5/5 → 0.05）、两组直方图分组互不影响、缺 `le` 的序列被跳过（不再报错），
    以及经求值器端到端丢掉 `__name__`/`le`、标量输入报错。
- 已完成（2026-11-14）：MetricsQL 的 `histogram_share(le, v[, boundsLabel])`
  （`transform.go` 的 `transformHistogramShare` / `share` / `fixBrokenBuckets` /
  `mergeSameLE`）：
  - 答案是在 `le` 及其以下的观测占比：先把每个桶的累计计数**向上钳平**
    （`fixBrokenBuckets`——NaN 计数取前一个桶的值，比前一个小的也取前一个），
    相同边界先合并（计数相加，`mergeSameLE`），再在包含 `le` 的那个桶里按
    `lower + (v-vPrev)/vLast * (le-lePrev)/(le-lePrev)` 线性插值，
    其中 `vLast` 是最后一个桶（通常是 `+Inf`）的计数；
  - 边界语义：`le` 恰好落在桶边界上时（`lePrev == leReq`）直接返回该桶占比；
    `le` 落在 `+Inf` 桶里返回最后一个有限桶的上界占比（`lower`，`upper` 为 1）；
    **没有** `+Inf` 桶且 `le` 超过所有桶时才返回 1（VM 的循环尾分支）；
  - `le` 为 NaN 或没有任何带 `le` 的序列时返回 NaN，负数返回 0，`+Inf` 返回 1；
  - 第三个参数是 VM 的 `boundsLabel`：给了它，每个直方图产出**三条**序列——占比本身
    （保留原有的同名标签值）、`boundsLabel="lower"` 与 `boundsLabel="upper"` 两个
    插值边界。VM 不校验这个标签名（标签是任意字节串），这里同样不校验；
  - 结果丢弃 `__name__` 与 `le`；缺 `le` / `le` 无法解析 / `le` 为 NaN 的序列
    被**跳过**（Prometheus 的 `resetHistograms` 是 warning + `continue`，VM 的
    `groupLeTimeseries` 直接 `continue`；本求值器没有 annotation 通道，只保留
    「跳过」）；
  - 测试：1/2/3/4 累计计数下 2→0.5、3→0.625、6→0.75、0.5→0.125、10→0.75 与
    `+Inf`/负数/NaN 的硬编码分支、无 `+Inf` 桶时超出末桶才为 1；计数递减与 NaN 计数
    被钳平后为 1；同边界合并后 1 与 1.5 都是 1；`boundsLabel` 产出三条序列且
    `lower`/`upper` 分别为 0.5/0.75、占比保留原标签值；缺 `le` 的序列被跳过；
    经求值器端到端丢名、两参数与三参数两种写法、标量输入报错。
- 已完成（2026-11-14）：MetricsQL 的 `buckets_limit(limit, v)`
  （`transform.go` 的 `transformBucketsLimit`），把一条直方图的经典 `le` 桶
  thinning 到每幅最多 `limit` 个：
  - 分组按 VM 的做法取「除 `le` 外的全部标签」（`__name__` 也参与分组），所以
    多幅直方图互不影响；桶数已不超过 `limit` 时原样保留全部桶；
  - 否则把桶按 `le` 升序排，每个桶的 `hits` 取累计计数相对上一桶的增量
    （第一个桶相对 0），先把两端**连续空桶**（`|hits| < 1e-9`）在仍超过 `limit`
    时丢弃，再把相邻两桶中 `hits` 之和最小的一对合并到上界成员里，直到只剩
    `limit` 个；首尾桶始终保留，合并后的桶沿用上界桶的 `le`、计数把下界桶的
    累计值并入；
  - `limit` 先按 Go 的 `float→int`（`floatToIntBounded`）取整，`<= 0` 报错
    `limit must be greater than 0; got <整数>`（照抄 VM 的 `got %d`，用的是取整后的
    整数而非原始浮点样本值），`< 3` 抬到 3（VM 保留首尾桶的考量）、`> 3` 原样保留
    （不封顶）；缺 `le` / `le` 不可解析 / `le` 为 NaN 的序列被跳过（VM 跳过前两者，
    但会保留 `le="NaN"`——其 `sort.Slice` 顺序不确定，这里统一跳过以保持确定性）；
    `le` 解析不 trim（Go 的 `strconv.ParseFloat` 拒绝前后空格）；
    `limit` 必须是数值样本值，字符串字面量在解析期被
    拒绝（VM 只在 `scalar()` 里特判字符串）；
  - 测试：桶数不超限时全保留（`limit=10` 对 8 个桶）；超限时按最小 hits 合并到
    `limit` 个且首末桶保留（8 桶 → `le` 1/64/+Inf，计数 1/7/8）；两端空桶被裁掉
    （6 桶 → `le` 2/4/8）；`limit` 抬到 3、`<= 0` 报错；字符串字面量解析期报错；
    缺 `le` 的序列被跳过、不产出；经求值器端到端保留 `__name__` 与原始 `le` 文本、
    标量输入报错。
- 已完成（2026-11-14）：MetricsQL 的 `bitmap_and(v, mask)` / `bitmap_or(v, mask)` /
  `bitmap_xor(v, mask)`（`transform.go` 的 `newTransformBitmap`），逐样本把向量的值
  与标量掩码做位运算：
  - 每个非 NaN 值按 Go 的 `uint64(float)` 截断成整数（向零截断，`[2^63, 2^64)`
    走无符号路径补回高位，越界/NaN/Inf 得「整数不定」`0x8000000000000000`），再与
    掩码做 `&` / `|` / `^`；值或掩码任一为 NaN 时结果 NaN；
  - 丢掉 `__name__`、保留其余标签（照抄 VM 的 `doTransformValues`），与 `abs` 等
    一元变换一致；
  - `go_f64_to_u64` 逐位复刻 Go amd64 的转换（Rust 的 `as` 会饱和，不能直接照搬）；
  - 测试：12 &/|/^ 10 == 8/14/6；按 instance 分组独立合并（10 & 10 == 10）；
    3.9 向零截断为 3；NaN 传播；`+Inf` 得不定值（`+Inf | 1` 置高位）；负数环绕
    （`-5 & 0xff`）；`[2^63, 2^64)` 保留低位；标量输入报错。
- 已完成（2026-11-14）：MetricsQL 的阈值 rollup `count_eq|le|gt|ne_over_time` /
  `share_eq|le|gt_over_time` / `sum_eq|le|gt_over_time` 与 `duration_over_time`
  （`app/vmselect/promql/rollup.go` 的 `newRollupFilter` / `newRollupAvgFilter` /
  `newRollupDurationOverTime`）：
  - 这一族把范围向量放在**第一个参数**（`count_le_over_time(v[5m], 100)`），与
    `predict_linear` / `integral` 同形，所以走同一段「range 在前」的解析分支；
  - 比较逐样本进行，`eq` 是 `==`、`le` 是 `<=`、`gt` 是 `>`、`ne` 是 `!=`；
    然后 `count_*` 数条数、`share_*` 再除以窗口内的样本数、`sum_*` 把这些样本的值
    相加。`sum_*` 从 0 起加，所以一条都不命中时是 `0` 而不是 NaN（`sumFilter*`
    的原样行为）；
  - 与所有 MetricsQL rollup 一样先丢 NaN：NaN 既不命中也不计入 `share_*` 的
    分母；整窗都是 NaN 时该序列不产出；
  - `duration_over_time(v[range], max_interval)` 累加相邻样本的时间差，**超过**
    `max_interval` 秒的间隔被跳过（`d <= dMax` 才计入，第一个间隔为 0）。
    `dMax` 是 `int64(max_interval * 1000)`，按 amd64 上 Go 的转换语义取整
    （向零截断，NaN/越界得 `MinInt64`，于是连第一个 0 间隔也不计）；
  - 结果一律丢 `__name__`（VM 的 `rollupFuncsKeepMetricName` 里没有这一族），
    沿用本仓库其它 MetricsQL rollup 的约定；
  - 测试：`count/share/sum` 四种比较的条数与分数、无命中时 `sum` 为 0 且
    `share` 为 0、NaN 被丢弃后分母同步缩小、整窗 NaN 不产出；
    `duration_over_time` 的间隔上限（5/10/15 秒对 0-10-20 的样本）、
    长间隔被跳过而它之后的短间隔仍计入、负上限连首个 0 间隔也不计；
    以及十个函数名经解析器端到端求值、缺第二个参数与 `share_ne_over_time` /
    `count_lt_over_time` / `sum_ne_over_time` 这些未实现拼写报未知函数。
- 已完成（2026-11-14）：MetricsQL 的序列选择函数 `drop_common_labels` /
  `drop_empty_series` / `labels_equal` / `label_graphite_group` / `limit_offset`
  （同属 `app/vmselect/promql/transform.go`，逐条对照源码）：
  - `drop_common_labels(v, ...)` 把各参数的序列**拼接**后统计每个标签名的取值分布，
    只要某个取值覆盖了全部序列就删掉整个标签名（VM 的 `RemoveTag`，`__name__`
    同样参与，所以同名序列合并后指标名也会被删）；`job` 这类各序列不同的标签保留；
  - `drop_empty_series(v)` 丢掉「空」序列——VM 的 `removeEmptySeries` 判据是全 NaN。
    本服务按 step 求值，单个 step 上的空序列就是取值为 NaN 的样本，因此这一步等价于
    丢掉 NaN 样本；VM 是整段窗口判空，跨 step 的差异与下面那批变换一并记录；
  - `labels_equal(v, "a", "b", ...)` 只保留点名标签取值全相同的序列，缺失标签按空串
    参与比较（VM 的 `GetTagValue` 返回 nil，`hasIdenticalLabelValues` 里就是 `""`），
    所以两个都缺的标签算相等、缺与不缺算不等；VM 要求至少 3 个参数
    （`unexpected number of args; got N; want at least 3`），这里在解析期要求至少
    两个标签名；
  - `label_graphite_group(v, 1, 2, ...)` 按 `.` 拆开 `__name__`，取指定下标的段用
    `.` 重新拼接，标签本身不动；越界/负下标什么都不贡献但**分隔符照写**
    （`label_graphite_group(m, 5, 0)` 对 `a.b` 得 `.a`），拼出来为空串时等于没有指标名，
    这里删掉 `__name__`。下标是标量表达式，`floatToIntBounded` 的语义（截断向零、
    钳到 int 范围）单独实现；
  - `limit_offset(limit, offset, v)` 先丢空序列再跳 `offset` 条、最多留 `limit` 条，
    顺序即输入顺序（VM 自己在 `maySortResults` 里把 `limit_offset` 列为「已排好」，
    不会事后重排）；`limit` / `offset` 为负时报错并点名参数。注意 VM 是按**整条
    序列**做偏移（同一条序列在整段 step 上都留下或都丢掉），本服务每个 step 独立
    求值，某条序列在某步缺席时偏移窗口会随之滑动——单 step 查询两者一致；
  - 测试：`drop_common_labels` 的单参数/多参数/单序列（全删光只剩 `{}`）、
    `drop_empty_series` 的 NaN 过滤、`labels_equal` 的相同/不同/双侧缺失、
    `label_graphite_group` 的正常下标/越界下标保留分隔符/全越界删名、
    `limit_offset` 的 NaN 先过滤再偏移、截断取整；以及参数过少、负数、标量输入
    各自的报错。

  同族的**跨 step 变换**明确不做：`keep_last_value` / `keep_next_value` /
  `interpolate` / `running_*` / `range_*` / `smooth_exponential` 在 VM 里作用在
  一条序列**整段 step 取值数组**上（补 NaN 缺口、前缀累积、整窗统计），而本服务
  每个 step 独立求值，没有「上一步/下一步的值」这条数据通路；按 step 硬做会在
  range 查询里给出与 VM 不同的曲线（`keep_last_value` 会退化成恒等），所以宁可
  报未知函数。要做需要把求值器改成 VM 那样一次产出整段序列，留作后续条目。
- 已完成（2026-11-14）：MetricsQL 的取值类聚合函数 `any` / `sum2` / `geomean` /
  `distinct` / `median` / `mode` / `mad` / `share` / `zscore` / `limitk` /
  `outliers_iqr` / `outliers_mad` / `outliersk`（`app/vmselect/promql/aggr.go`
  的 `aggrFuncs` 表与各 `aggrFunc*`）：
  - **标量折叠**（每组一个值）：`sum2` 是平方和、`geomean` 是 `prod^(1/n)`、
    `distinct` 是不同取值个数、`median` 就是 `quantile(0.5, …)`（复用
    `quantile_of`）、`mode` 复用 `modeNoNaNs`（与 `mode_over_time` 同一份实现）、
    `mad` 是 `median(|v - median(v)|)`。它们都先跳过 NaN 样本，整组都是 NaN 时
    返回 NaN——即 VM 的 `removeEmptySeries` + 各 `aggrFunc*` 里的
    `if count == 0 { … = nan }`；`distinct` 把 `-0` 与 `0` 算作同一个键（Go 的
    `map[float64]` 按 `==` 比较）；
  - **一进多出**（每条输入序列一条输出、**保留原始标签**，对应 VM 里
    `aggrFuncExt(..., keepOriginal=true)` 的调用者）：
    `any` 每组留第一条序列；`share` 把每个非负值除以该组非负值之和（负数与 NaN
    变 NaN）；`zscore` 用 VM 的 Welford 递推算组内均值与总体标准差，再算
    `(v-avg)/stddev`；`outliers_iqr` 留「超出 `[q1-1.5IQR, q3+1.5IQR]`」的点；
    `outliers_mad(tolerance, v)` 留超出 `median ± tolerance*MAD` 的点；
    `outliersk(k, v)` 按「与中位数的平方距离」取前 k 条（`getRangeTopKTimeseries`
    升序排再整体反转，所以输出是距离降序）；
  - 这里的「超界」判定是**逐点**的：VM 的 `aggrFuncOutliersIQR` / `aggrFuncOutliersMAD`
    按时间点分别算组内上下界，再「序列只要有任何一点超界就整条留下」；本服务每个
    step 独立求值、一条序列在这一步只有一个点，两种写法在这里等价（区间查询下
    VM 可能在前面的 step 就留下一条序列并沿用它在后续 step 的 NaN 值，本服务不会
    这样持有跨 step 状态，与 `limit_offset` 记录的是同一类差异）；
  - `limitk(k, v)` 按 VM 的 `getHash`（xxhash64，种子 0，先写 `__name__` 再按标签名
    排序写每个 name/value）升序取前 k 条，保证同一组输入的选集合稳定；
    `k`/`tolerance` 走 `floatToIntBounded`（NaN 与越界钳到边界），`k<=0` 返回空；
  - 分组照旧走 `group_key`，所以 `by`/`without` 都能用；聚合参数写在向量前
    （`limitk(2, m)`、`outliers_mad(1.5, m)`）；
  - `aggr_over_time` 现在只接受 `sum/avg/min/max/count/stddev/stdvar`（新增的
    MetricsQL 聚合名不在其上），解析期报错并列出可用名；
  - 测试：`sum2`/`geomean`/`distinct`/`median`/`mode`/`mad` 的数值与 NaN 缺口语义、
    `-0`/`0` 合并；`any` 保名、`share` 的 1/6 与 2/6、`zscore` 的 `±sqrt(1.5)`、
    `outliers_iqr` 的「无离群点」与 100 这条离群点、`outliers_mad(1, …)` 的同一边界、
    `outliersk(1, …)` 选中 100；`limitk(2, …)` 的条数与按哈希序的前缀、
    `limitk(0, …)` 为空；`metric_name_hash` 按 Go `xxhash/v2` 实测值逐条对照
    （`m`、`m{instance=a..d}` 与空 metric group）。
- 已完成（2026-11-14）：MetricsQL 的 `quantiles("label", q1, …, v)` /
  前缀式 `quantiles by (job) ("q", 0.5, m)`（同属
  `app/vmselect/promql/aggr.go` 的 `aggrFuncQuantiles`）：
  - 每条输入序列产出与分位参数个数相同条数的输出，输出标签是**去掉目标标签后
    再加上 `label=go_format_g(phi)`**（VM 的 `ts.MetricName.RemoveTag(dstLabel)`
    + `AddTag(dstLabel, fmt.Sprintf("%g", phi))`）；越界分位不报错，`phi<0` 得
    `-Inf`、`phi>1` 得 `+Inf`——沿用本服务 `histogram_quantiles` 已固定的
    `quantileSorted` 语义；
  - NaN 样本在分组前丢弃（VM 的 `aggrPrepareSeries` +
    `prepareForQuantileFloat64`），与目标标签同名的旧标签值被分位标签覆盖；
    标量被当作一条无标签序列；
  - 分组键用 `group_key` 之后才写分位标签，所以默认聚合（无 `by`/`without`）下
    每条输出都是 `{}` 加分位标签；后缀式（`quantiles("q",0.5,m) by (job)`）与
    前缀式（`quantiles by (job) ("q",0.5,m)`）两种写法都支持，与 VM
    `parseAggrFuncExpr` 的 `isIdentPrefix` 分支对齐；
  - 测试：单/多分位、越界分位（`±Inf`）、`by (job)`、同名标签覆盖、
    `%g` 标签值（`100000`→`"100000"`、`1e6`→`"1e+06"`）、NaN 是缺口、
    标量输入、两种分组写法与解析期参数校验。
- 已完成（2026-11-14）：MetricsQL 的 `histogram(v)`（`app/vmselect/promql/aggr.go`
  的 `aggrFuncHistogram` + `app/vmselect/promql/transform.go` 的
  `vmrangeBucketsToLE`，原生直方图实现在
  `github.com/VictoriaMetrics/metrics` 的 `histogram.go`）：
  - 每个 step 把一组值喂进 VM 的原生直方图：`Update` 丢弃 NaN 与负数，按
    `(log10(v) + 9) * 18` 定桶（`[1e-9, 1e18)` 上每十进制 18 桶），**恰好是
    10 的整数幂的值挪到低一档桶**，好让 `le` 保持 Prometheus 的「小于等于」语义；
    另有下溢桶 `0...1.000e-09` 与上溢桶 `1.000e+18...+Inf`；
  - 每个非空桶是一条 `vmrange="<start>...<end>"` 序列，桶边界照抄
    `initBucketRanges`：从 `1e-9` 起**逐次乘** `10^(1/18)`（不是 `powi`，因为
    后续要把格式化后的字符串再解析成浮点做缺口判定，浮点路径必须一致），两端都走
    Go 的 `%.3e`（一位整数 + 三位小数 + 两位指数，如 `8.799e-01`）；
  - 再走 `vmrangeBucketsToLE`：按上界排序、计数为 0 的桶跳过（并入下一个非空桶）、
    与前一个桶之间有缺口时补一条 `le=<当前桶下界>` 的序列（下溢桶的下界是 `0`，
    正好等于 VM `xsPrev` 的零值，所以它不补）、同界桶的计数相加、末桶若不到
    `+Inf` 则追加一条空的 `+Inf`（由最后的累计过程接住总数）；
  - 最后一遍把计数做成**累计**，即 Prometheus `_bucket` 的形状；
  - `histogram` 是标准聚合，所以 `by`/`without` 都支持（分组键走 `group_key`，
    结果不含 `__name__`），分组标签与 `le` 并列；标量被当作一条无标签序列的一条
    观测；全 NaN/负数的组不产出任何序列（VM 的 `removeEmptySeries` 之后再无桶）；
  - 测试：`visit` 与 `le` 的输出逐条对照一份 Go 端口（`1,2,3,4` 的 9 条序列、
    单值、`1,100` 的缺口、`1e-12` 下溢、`1e19` 上溢、`1,10,100,1000` 的 10 的幂
    语义、NaN/负数被丢弃、全 NaN 为空），桶边界表在两端与 `1e18` 上逐条核对，
    另测 `histogram(m)`、`histogram by (job) (m)` 与标量输入的分组/标签形状。
- 已完成（2026-11-14）：MetricsQL 的 `sort_by_label_numeric(v, "label", ...)` /
  `sort_by_label_numeric_desc` 与 `union(v1, v2, ...)`（同属
  `app/vmselect/promql/transform.go`，`newTransformFuncNumericSort` /
  `numericLess` / `transformUnion`）：
  - 数值排序用 VM 的 `numericLess`：把两个字符串按位置切成「数字前缀」与
    「非数字前缀」交替比较——数字前缀是可选正负号加至多一个小数点的数字
    （`.5` / `1.` / `+1.5` 都算），按 `ParseFloat` 后的数值比；非数字前缀按字节比；
    某一侧当前位置没有数字前缀时它更小（所以 `"a" < "1a"`）。`1.10` 与 `1.5`
    是 1.1 与 1.5，因此 `1.10 < 1.5`；
  - 与 `sort_by_label` 不同，它**没有平局打破**：第一个不同的标签上
    `numericLess` 两个方向都为假（如 `"1"` 与 `"01"`）即视为相等，稳定排序保持
    输入顺序；
  - 标签名不校验（VM 的标签是任意字节串，`a.b` 合法），但至少要求一个；
  - `union(v1, v2, ...)` 按顺序拼接各参数的序列，按**完整标签集**去重、先出现的
    保留（VM 以 `marshalMetricNameSorted` 的结果作键）；全标量参数
    （`union(1, 2)`）返回各自的无名序列而不是并成一条，标量与向量混用时标量算
    一条无标签序列再一起参与去重；零参数在解析期报 `at least one argument`；
  - 测试：`node1/node2/node10` 的正序与倒序、`1a < 02 < a`、`1.10 < 1.5`、
    `"1"`/`"01"` 保持输入顺序、`a.b` 这类标签名可解析、缺标签名与裸标识符报错；
    `union` 跨选择器的去重与先出现者胜、`union(1, 2)` 的两条无名序列、
    `union(1, m)` 的单条无标签标量序列，以及 `union()` / `union(m,)` 的报错。
- 已完成（2026-11-14，2026-11-18 修正 NaN 位置）：`sort(v)` / `sort_desc(v)`。
  **Prometheus 的 `sort` 和 `sort_desc` 都把 NaN 排在末尾**：`funcSort` 用
  `vectorByReverseValueHeap`、`funcSortDesc` 用 `vectorByValueHeap`，两者的
  `Less` 都是「`vi` 为 NaN 就返回 true」，再各自被 `sort.Reverse` 反转——净效果
  是 NaN 在两个方向上都落最后，只有数字互换两端。Go 复刻确认：
  `sort(2,NaN,1) = [1 2 NaN]`、`sort_desc(2,NaN,1) = [2 1 NaN]`。
  - 早先这一条按 VictoriaMetrics 的 `newTransformFuncSort` 实现（`a` 为 NaN
    就返回 `true`，即 NaN 在**两个方向都排最前**），而当时的实现又只在升序把 NaN
    放末尾、降序放最前，两边都不对。`sort` 是 PromQL 核心函数，现在按 Prometheus
    对齐，用 `sort_by_value_order` 显式表达「NaN 恒为最大、且 `desc` 不反转 NaN
    的比较」；
  - 相等值比较为 Equal，稳定排序保持输入顺序（Prometheus 的不稳定排序在相等时
    结果是任意的，也没有标签平局打破）；
  - 测试：值 2/NaN/1 三序列升序得 `c,a,b`、降序得 `a,c,b`（NaN 都在末尾）；
    `±Inf` 与 NaN 混排（升序 `ninf,inf,nan`、降序 `inf,ninf,nan`）；全 NaN
    输入保持输入顺序；多条 NaN 夹在数字之间时全部落到数字之后。
- 已完成（2026-11-11）：`histogram_quantile(q, v)`（经典直方图的 `le` 桶）。
  Grafana 的延迟面板几乎都写成 `histogram_quantile(0.9, sum by (le) (rate(x_bucket[5m])))`，
  之前这个函数会被当作未知函数拒绝。实现按 Prometheus 的 `BucketQuantile`：
  - 按「除 `le` 与 `__name__` 之外的全部标签」把桶归成一个直方图，`le` 解析为 f64
    （含 `+Inf`）；同一边界的桶先合并（计数相加），计数低于前一个桶时向上钳平——
    这两步 Prometheus 也是静默做的；
  - 在有限桶里找累计计数首次 ≥ `q * observations` 的桶，桶内线性插值（第一个桶的
    隐含下界为 0，若其上界 ≤ 0 则直接返回该上界）；`q` 落进 `+Inf` 桶时返回最后一个
    有限桶的上界，不假装知道分布的尾巴；
  - `q` 为 NaN / <0 / >1 分别返回 `NaN` / `-Inf` / `+Inf`；桶不足两个、缺 `+Inf` 桶、
    观测数为 0 返回 `NaN`——这些都是 Prometheus 的既有语义，照抄而不是另造一个数；
  - 单调性修正按 Prometheus 的 `ensureMonotonicAndIgnoreSmallDeltas`：先把相邻桶计数
    相对差 < 1e-12 的浮点噪声**吸收**成相等（`relatively_equal`，即 `prometheus/common`
    的 `RelativelyEqual`），被修正的桶**不推进**参考计数，后续真实增长仍以上一个干净
    值为基准；超过容差的下降才强制钳平；
  - 结果只保留分组标签（去掉 `le` 与 `__name__`）；缺 `le` / `le` 不可解析 /
    `le` 为 NaN 的序列被跳过（Prometheus 的 `resetHistograms` 是 warning +
    `continue`，VM 的 `groupLeTimeseries` 直接 `continue`）。
  - 测试：`bucket_quantile` 的插值/`+Inf`/NaN 分支、重复边界、递减计数的钳平、
    相对噪声 <1e-12 的吸收（`relatively_equal` 及「不推进参考计数」行为）、
    按标签分组与标签裁剪、缺 `le` 的跳过（含 `abc` / `" 1.5"` / `NaN`）；
    端到端（actix 路由 + 真实引擎）写入
    1/2/3/4 四个桶后断言 `histogram_quantile(0.5, http_latency_bucket)` 是单条
    无标签的 `0.5`，而 `histogram_quantile(0.5, http_requests)` 成功返回空结果。
- 已完成（2026-11-12）：PromQL 子查询 `expr[range:step]`（此前解析期直接拒绝）。
  语义照抄 Prometheus 的 `subqueryEvaluationTimes` / `runSubquery`：
  - step 栅格锚定在**绝对**的 step 整数倍上，起点是严格晚于 `at - offset - range`
    的第一个栅格点，终点是父求值时刻（有 `@` 时是 `@` 时刻）减去 `offset`；
    因此 `metric[50s:10s]` 在 10 s 处只取 0 s、10 s 两个点，而不是从 0 起再往前
    多取几个空点；
  - step 省略（`[5m:]`）时用默认 1 分钟求值间隔，与 Prometheus 的
    `noStepSubqueryIntervalFn` 默认值一致；
  - 每个栅格点都是一次普通的即时常量求值（即时选择器仍用 5 分钟回看），矩阵样本
    的时间戳是**栅格时刻**而不是底层样本时刻——这正是 `rate(m[20s:5s])` 按请求的
    step 外推、`irate`/`timestamp` 看到 step 时间的原因；
  - `[30:10]` 这类裸秒数、`offset`、`@` 都支持，`offset`/`@` 可以落在子查询本身；
    任意即时向量表达式都可以做子查询（`(a)[5m:1m]`、`sum_over_time(...)[...]`、
    `a + b` 整体），嵌套子查询也可用；标量或区间向量做子查询、写两个 `[ ]`、
    `[5m:0s]` 都在解析期点名拒绝；
  - 扫描窗口按 `range + step` 与 `offset`/`@` 推算，仍是一次取原始样本、内存里
    逐点求值；每个父 step 会重算该子查询（Prometheus 是整段父区间算一次再切窗口，
    数值一致，代价是重复的纯计算，原始样本仍只取一次）；这条路径同样不走 rollup。
  - 测试：解析形状（`[5m]` 仍是 `Range`、`[5m:]` 的 step 为 0、`offset`/`@` 落在
    子查询上、裸秒数、嵌套子查询、各种拒绝），求值对照 Prometheus 官方
    `subquery.test`（`sum_over_time(m[50s:10s])`=3、`[50s:5s]`=4、5 分钟处=10、
    `rate(m[20s:10s])`=0.1、`rate(m[20s:5s])`=1/15、offset 5/7/9/10 s 与
    `[30:10] offset 3` 均为 297、`[3m:]` 的默认 step 产生 3 个点、`@ 1000` 固定
    窗口、嵌套 `rate(sum_over_time(...)[...])`=0.3），以及 `scan_window` 覆盖
    `at - range - step - lookback`。
- 已完成（2026-11-12）：`label_replace` / `label_join`（此前解析期直接拒绝）。
  两者只改标签、不改样本值与时间戳，因此走普通的逐 step 求值：
  - `label_replace(v, dst, replacement, src, regex)`：pattern 按 Prometheus 的方式
    锚定（`^(?s:...)$`），源标签值**整串**匹配才替换；`replacement` 用 Go
    `ExpandString` 的语法展开（`$1`、`${name}`、`$$` 为字面 `$`、越界或未参与的
    捕获组展开为空），展开为空则**删除**目标标签；不匹配的序列原样透传。
    `expand_replacement` 逐行对照 Go 的 `expand`/`extract`：`$name` 取**最长**的
    名字字节串（所以 `$1x` 是组名 `"1x"`、展开为空而不是 `$1` 加字面 `x`）、
    不构成名字的 `$`（行尾 `$`、`${` 未闭合、空 `${}`）按字面复制；
  - `label_join(v, dst, separator, src...)`：把源标签按顺序用分隔符连接，缺失的源
    标签按空串参与（分隔符仍出现），结果为空同样删除目标标签；
  - 目标标签名（以及 `label_join` 的源标签名）在解析期按 Prometheus 现用的
    `UTF8Validation` 校验——`cmd/prometheus/main.go` 的 `init()` 把
    `model.NameValidationScheme` 设为 `model.UTF8Validation`，`IsValidLabelName`
    此时只要求「非空且合法 UTF-8」，所以 `1bad`、`b c`、`a.b`、`指标` 都是合法名，
    只有空串被拒绝（`label_names` 是 Rust `String`，UTF-8 由类型保证）；pattern 也在解析期编译，非法正则、非字符串参数、
    `label_join` 没有源标签都返回 `bad_data` 并点名；标量作第一参数在求值期报
    「expects an instant vector」；
  - 写 `__name__` 即重命名指标，其余情况保留 `__name__`；两个序列改写后标签相同时
    按 Prometheus 的逐 step 语义各留在结果里（不静默丢弃）。
  - 测试：`$1`/`${1}`/`$$` 展开、`$1x` 的「最长名字」规则、行尾 `$`/`${}`/`${1`
    的按字面复制、锚定导致的不匹配原样透传、空展开删除标签、
    未参与捕获组展开为空、`__name__` 重命名、`label_join` 的缺失源标签与空结果、
    解析期错误（非法正则/标签名/非字符串参数/缺源标签）、标量参数求值错误；端到端
    （actix 路由 + 真实引擎）覆盖 `label_replace` 派生 `instance` 且值不变、
    `label_join` 拼接 `api/`、非法正则返回 400 `bad_data`。
- 已完成（2026-11-12）：补齐 Grafana 面板高频的 PromQL 函数。语义逐条对照
  Prometheus 的实现：
  - `clamp(v, min, max)`：逐样本取 `max(min, min(max, value))`，丢弃 `__name__`；
    `max < min` 返回**空向量**（不是原值）。NaN 走 Go `math.Min/Max` 的语义，
    即 NaN 会传播，不会被钳成边界（Rust 的 `f64::min/max` 会忽略 NaN，因此
    `clamp` / `clamp_min` / `clamp_max` 统一改用 `go_min` / `go_max`）；
  - `round(v[, to_nearest])`：按 Prometheus `funcRound` 的**半向上**（向 +∞）取整，
    即 `floor(f*n + 0.5)/n`（`n = 1/to_nearest`），而非「半远离零」——所以
    `round(-2.5) = -2`（不是 `-3`）、`round(-0.5) = 0`；`to_nearest = 0` 时按
    Prometheus 返回 `NaN`（`1/0 = +Inf` 传播），而不是原值；
  - `scalar(v)`：恰好 1 个浮点样本时返回该值，0 个或多个返回 `NaN`，结果是
    `resultType=scalar`；MetricsQL 还额外接受**字符串字面量**，按 Go 的
    `strconv.ParseFloat` 解析为浮点数（`"1e3"`→1000、`"1e400"`→`+Inf`），
    无法解析时返回 `NaN` 而非报错，而 Prometheus 会直接拒绝该形式；
  - `deriv(v[range])`：最小二乘斜率（单位「每秒」），与 `predict_linear` 共用
    同一份 `linear_regression`。样本少于 2 个时该序列不出现；全等序列的斜率是
    精确的 0（避免浮点噪声），全为 Infinity 时按 Prometheus 返回 NaN。四个和
    （`sumX/sumY/sumXY/sumX2`）用 Prometheus 同款 Kahan（Neumaier）累加，长区间
    不再丢失朴素 `+=` 会损失的精度（500 样本 1e8 基线时斜率差 ~4e-11）；
    拟合原点与 `funcDeriv` 一致取**求值时刻** `enh.Ts`（斜率在精确算术下与原点
    无关，浮点下不是：两样本窗口上换原点会让结果差 ~1e-14 相对量）；
  - 日期函数 `day_of_month` / `day_of_week` / `day_of_year` / `days_in_month` /
    `hour` / `minute` / `month` / `year`：参数可选。有参数时把每个样本的
    **值**当作 Unix 秒（UTC，**向零截断**后计算——Go 的 `int64(el.F)`，样本自身的
    时间戳不参与）；无参数（`year()`）时用求值时刻（Go 的 `enh.Ts / 1000`，同样
    向零截断）。两者都丢弃 `__name__`，无参数形式返回单条无标签样本。日历换算用
    Howard Hinnant 的 `civil_from_days` 纯整数算法，不引入时区或时间库；
    `day_of_week` 与 Go 一致（周日为 0）；非有限值（NaN/Inf）没有日期，按 0 秒处理；
  - `quantile(q, v)` 聚合：按 `by`/`without` 分组后排序插值，复用
    `quantile_over_time` 的 `quantile_of`；`q` 为 NaN 时结果是 NaN，`q<0`/`q>1`
    分别是 `-Inf`/`+Inf`。`q` 是必填参数且写在向量前（`quantile by (job)
    (0.5, m)` 也支持），缺参数在解析期报错；`aggr_over_time` 仍拒绝它并提示改用
    `quantile_over_time`。插值与 Prometheus `quantile` **逐位一致**：
    `lo*(1-weight) + hi*weight`（`weight = rank - floor(rank)`），不是代数等价的
    `lo + (hi-lo)*weight`——后者在约 1/3 的输入上差 1 ULP。两种序都**无条件求值**：
    没有 `weight == 0` 提前返回（那样 `+Inf` 上邻会漏掉 `Inf*0 = NaN`），也没有
    「相邻样本相同就直接返回」的短路（`quantile(0.3, [3, 3])` 在 Go 里就是
    `2.9999999999999996`）。排序也照 `vectorByValueHeap`：升序但 **NaN 排最前**，
    不是 `total_cmp` 的「NaN 在 `+Inf` 之上」——`quantile(0.5, [3, NaN, 1])` 因此是
    `1` 而非 `3`，`quantile_over_time` 同理。
  - 测试：`clamp` 的上下界/`max<min` 空结果/NaN 传播；`scalar` 的 1 个、0 个、
    多个样本与标量参数，以及 MetricsQL 的字符串字面量形式
    （`"1.5"`/`"1e3"`/`"1e400"`/`"abc"`/`"3"`）；`deriv` 的斜率、常量序列、
    单样本消失以及与
    `predict_linear` 的斜率一致；日期函数在闰日 2024-02-29T13:45:00Z 上的
    全部分量与 `year()` 的求值时刻语义；`quantile` 的逐位插值（1 ULP 回归用例）、
     分组、越界与 NaN；
    `round` 的半向上（`round(-2.5) = -2`）、`to_nearest = 0` 得 NaN；
    解析测试覆盖全部新函数。
- 已完成（2026-11-12）：`present_over_time` 与 `mad_over_time`（PromQL 区间函数）。
  - `present_over_time(v[range])`：区间内只要有一条样本就返回 `1`（值本身不参与，
    NaN 也算存在）；区间为空时该序列不出现。标签与 `__name__` 原样保留，与
    Prometheus 的 `funcPresentOverTime` 一致；
  - `mad_over_time(v[range])`：中位数绝对偏差，即 `median(|v - median(v)|)`，复用
    `quantile_of` 的排序插值；任一样本为 NaN 时整个结果是 NaN（NaN 会让两个中位数
    都失去意义，Prometheus 也显式传播而不是丢弃该样本）；单样本的偏差为 `0`。
  - 测试：`present_over_time` 对普通序列与全 NaN 序列都返回 1、空区间不出现；
    `mad_over_time` 对 `[1,2,3,4]` 得 1、单样本得 0、含 NaN 得 NaN；解析测试
    覆盖两个新函数名。
- 已完成（2026-11-17）：`absent(v)` 的结果标签对齐 Prometheus 的
  `createLabelsForAbsentFunction`。此前参数为空时恒返回**无标签**的 `1`；现在把
  选择器的**等值匹配器**带进结果：
  - 只对选择器（`Selector`/`Range`）生效：按匹配器书写顺序遍历，跳过 metadata
    标签（任何 `__` 开头的名字，`__name__` 在内），`=` 匹配器首次出现时写入该
    标签，其余情况（`!=`/`=~`/`!~`，或同名 `=` 再次出现）删除该标签；
  - 因此 `absent(x{job="a",foo="bar"})` → `{job="a",foo="bar"} 1`，
    `absent(x{job="a",job="b",foo="bar"})` → `{foo="bar"} 1`（历史兼容行为），
    `absent(x{job!="a"})` → `{} 1`；参数不是选择器（如 `absent(sum(x))`）时仍是
    无标签的 `1`，选择器命中了样本时结果为空向量；
  - 标签在解析期按上面的规则算好并存进 `Expr::Absent`，求值时只在空向量上写出；
  - 测试：上面各例、`__` 前缀标签被跳过、以及「后置 `=` 绕过前置 `!=`」
    （Prometheus 的 `has` 只由 `=` 分支置位，所以 `{job!="a",job="b"}` 里
    `job="b"` 会保留）；活服务冒烟逐条验证同样的标签，日志 0 error。
- 已完成（2026-11-13）：`histogram_fraction(lower, upper, v)`（经典 `le` 桶）。
  `bucket_fraction` 逐行照抄 Prometheus：区间 `(lower, upper]` 内的观测占比，
  `lower` 正数时不含、负数时含，`upper` 反之（由仅在桶内部插值推出），首个桶
  的隐式下界全负时取 `-Inf`、否则取 `0`，因此 `(-Inf, +Inf)` 为 1（`+Inf` 尾部
  桶也算在内）。与 `bucket_quantile` 共用 `classic_buckets` 的桶分组，但有两点
  有意的差异：
  - 排序后**不做**单调性钳平（Prometheus 的 `BucketFraction` 没有
    `fixBadHistogramBuckets` 那步），递减计数按普通累计读；
  - 缺/无法解析/NaN 的 `le` **跳过该 series**（Prometheus 的 `resetHistograms`
    在 `ParseFloat` 失败或 NaN 时记 `BadBucketLabel` warning 后 `continue`，
    VM 的 `groupLeTimeseries` 直接 `continue`）；本求值器没有 annotation 通道，
    所以只保留「跳过」这一半行为，不报错。唯一有意偏差是 VM 对 `le="NaN"`
    会保留（`ParseFloat("NaN")` 成功），但 `NaN` 在 `sort.Slice` 里位置不确定，
    这里统一跳过；
  - 特殊值：无桶/无 `+Inf` 桶/观测数为 0 返回 NaN，NaN 界返回 NaN，`lower >= upper`
    返回 0（相等与倒置都为 0）。
  - 测试：`bucket_fraction` 的插值/`+Inf`/NaN/`-Inf` 隐式下界/重复边界/递减/无桶，
    按直方图分组与标签裁剪、缺 `le` 的跳过（只有坏 `le` 时结果为空），以及经求值器
    写出四个桶后断言 `histogram_fraction(0.5, 1, ...)`=0.25、`(-Inf, +Inf)`=1。
  - 顺带补上 PromQL 的 `Inf`/`NaN` 浮点字面量：此前 `-Inf` 会被词法分析当成
    名为 `Inf` 的指标（再与 `-` 组成一元负号），`histogram_fraction(-Inf, +Inf, ...)`
    因此悄悄算出 NaN。现在 `Inf`/`NaN`（大小写不敏感，`Infinity` 之类仍是标识符）
    按 Prometheus 的词法规则直接是浮点字面量；`@` 与 `[ ]` 里的时长/时间戳
    只接受有限值，`@ NaN` 报错而不是静默截成 0。
- 已完成（2026-11-12）：钉住「不写 `by`/`without`」的默认聚合语义。查
  Prometheus 官方文档与 `promql/engine.go`（`generateGroupingKey` 在 grouping
  为空且非 `without` 时直接返回 0）确认：此时是**聚合全部标签维度**，整个向量
  并成一条无标签序列，`without ()` 才是「除 `__name__` 外全标签分组」。代码原本
  已是前者，这一轮补上回归测试 `test_default_aggregation_collapses_to_one_series`
  与文档说明，避免再被误改成 `without ()`。
- 已完成（2026-11-13）：`count_values("label", v)` 聚合。
  - 输出「每个不同的样本值一条序列」：值按 Go 的
    `strconv.FormatFloat(v, 'f', -1, 64)` 渲染（定点、无指数、最短往返，
    `NaN`/`+Inf`/`-Inf` 用同名文本）写入给定标签，序列值是它出现的次数；
  - 分组键**在写入值标签之后**计算，因此与 Prometheus 的
    `aggregationCountValues` 一致：不写 `by`/`without` 时只按值标签分组
    （结果只剩那一个标签）；`by (...)` 把值标签追加到 `by` 列表；`without (...)`
    下值标签本来就在序列上，除非被点名删除；
  - 参数必须是带引号的字符串且是合法标签名（`[A-Za-z_][A-Za-z0-9_]*`），
    否则解析期报错；字符串字面量不能单独作为表达式（新增 `Expr::Str`，只作为
    `count_values` 的标签参数出现，求值期若单独出现会报错）；
  - `aggr_over_time` 继续拒绝 `count_values` 并点名（它不是 over-time 聚合）。
  - 测试：默认分组、`by (job)`、`without (job)` 三种形态的计数与标签；
    值渲染（`1`、`1.5`、`-2`、`0.30000000000000004`、`1e-10`、
    `NaN`/`+Inf`/`-Inf`）；非字符串参数与非法标签名在解析期报错。
- 已完成（2026-11-13）：三角函数与双曲函数
  `sin/cos/tan/asin/acos/atan/sinh/cosh/tanh/asinh/acosh/atanh`，以及
  `deg`（弧度→角度）、`rad`（角度→弧度）。语义照抄 Prometheus 的
  `simpleFloatFunc`：逐样本做一次标量运算、丢弃 `__name__`、保留其余标签；
  IEEE 结果原样透传（`asin(2)` / `acos(2)` / `acosh(0.5)` / `atanh(2)` 是 NaN，
  `atanh(±1)` 是 ±Inf），标量参数返回单条无标签样本。`atan2` 是二元运算符而非
  函数，不在本项范围内。
- 已完成（2026-11-13）：`sort_by_label(v, "label", ...)` / `sort_by_label_desc`。
  - 按给定标签依次比较，第一个不同的标签按 `facette/natsort` 的
    `Compare` 比较（把字符串切成 `\d+` 与 `\D+` 的块，两块都是数字时按整数比，
    否则按字符串比 —— 所以 `node2 < node10`）；全部给定标签相同时用完整标签集
    字典序打破平局（`labels.Compare`），`_desc` 连同平局一起反向，与
    Prometheus 的 `funcSortByLabel`/`funcSortByLabelDesc` 一致；
  - 参考实现的 `Compare` 并非严格全序（相等字符串与 `"2"`/`"02"` 这类
    数字相等的块两个方向都返回 true），直接塞进 Rust 的排序比较器会让顺序依赖
    输入顺序甚至触发 panic。这里的实现两个方向都问一次，只在结论一致时采用，
    否则退回完整标签集比较，保证确定的全序；
  - 标签参数必填且须是带引号字符串；缺失或未加引号在解析期报错。名称本身不校验
    ——Prometheus 的 `funcSortByLabel` 与 VM 的 `newTransformFuncSortByLabel`
    都直接查表（`Metric.Get`），接受任意名字如 `a.b`。只改变顺序，样本值与标签原样保留。
  - 测试：用 `facette/natsort` README 的整份样例与它文档给出的排序结果逐项对照、
    `node1/node2/node10` 的自然序与倒序、多标签与平局下的确定性，以及三种解析期
    错误。
- 已完成（2026-11-13）：`atan2` 二元运算符。Prometheus 把它列在「三角二元运算符」
  里，优先级与 `*` / `/` / `%` 同级、左结合，走与算术运算符相同的向量匹配路径
  （`on` / `ignoring` / `group_left` / `group_right` 都可用，`atan2` 是单词形式的
  运算符，解析器里原先「只有集合运算用单词」的断言相应放宽）。逐对样本取
  `lhs.atan2(rhs)`（即 Go 的 `math.Atan2(y, x)`，注意左操作数是 y），结果按算术
  运算的规则丢弃 `__name__`。测试覆盖优先级（`2 + 3 atan2 4` 与
  `2 * 3 atan2 4` 的结合形状）、向量对的 π/4、标量对、以及 `atan2(0,-1)=π` 与
  `atan2(-1,0)=-π/2` 的操作数顺序。
- 已完成（2026-11-13）：`first_over_time` 与四个 `ts_of_*_over_time`
  （`ts_of_first_over_time` / `ts_of_last_over_time` / `ts_of_max_over_time` /
  `ts_of_min_over_time`）。
  - `first_over_time(v[range])` 返回区间内**最旧**样本的值；
  - `ts_of_first_over_time` / `ts_of_last_over_time` 返回最旧/最新样本的
    时间戳（秒，与 `timestamp()` 同形状）；
  - `ts_of_max_over_time` / `ts_of_min_over_time` 照抄 Prometheus 的
    `compareOverTime`：比较用 `>=` / `<=`（不是 `max_over_time` 的 `>` / `<`），
    因此并列极值时取**最后一个**；NaN 的处理也照抄：谓词是
    `cur >= best || math.IsNaN(best)`，Go 里 `NaN >= x` 恒为假，所以 NaN 样本
    自己不会成为极值，只有「当前极值已是 NaN」时下一个样本才接管。用
    `10, NaN, 5`（答案是 `10` 的时间戳）、`NaN, 5`（答案是 `5` 的时间戳）与
    尾部单个 NaN（极值不变）三种用例钉住；
  - 标签规则同其他 range 函数：保留除 `__name__` 外的所有标签，空区间时该序列
    不出现。唯一的例外是 `first_over_time` / `last_over_time` 自己 —— 它们像
    `offset` 一样只是挑出一个已有样本，Prometheus 因此**保留** `__name__`
    （`engine.go` 的 `dropName := (e.Func.Name != "last_over_time" &&
    e.Func.Name != "first_over_time")`）；`ts_of_*` 是派生值，仍然丢弃。
    这顺带修掉了 `last_over_time` 之前丢指标名的既有偏差，并且让原始区间选择器与
    子查询两条路径一致（`select_range` 现在把 `__name__` 一并放进
    `RangeSeries.labels`，由 range 函数决定去留）。
- 已完成（2026-11-13）：`double_exponential_smoothing(v[range], sf, tf)`（Prometheus V2
  里的 `holt_winters`）。逐行照抄 `funcDoubleExponentialSmoothing` 与
  `calcTrendValue`：用第一个样本做水平初值 `s1`、用第一个差分做趋势初值 `b`，然后
  按 `x = sf*v[i]`、`b = calcTrendValue(i-1, tf, s0, s1, b)`、`y = (1-sf)*(s1+b)`、
  `s1 = x+y` 递推；`calcTrendValue` 的 `i == 0` 分支直接返回原 `b`，所以两样本序列
  恰好还原为过这两点的直线，测试用这个性质与「严格线性序列原样复现」钉住。
  - `sf` / `tf` 必须严格落在 `(0, 1)`；Prometheus 在越界时 panic，这里返回 400
    并点名是哪个因子。NaN 因子按 Go 的比较语义放行（`NaN <= 0` 与 `NaN >= 1` 都为假），
    结果自然是 NaN。
  - 少于两个样本时该序列不出现（Prometheus 的 `l < 2` 分支）；
  - 为此把 `Expr::RangeFunc` 的单参数扩成 `param` + `param2`，解析、`selectors`、
    `scan_window` 的窗口推导与错误信息一并覆盖第二个参数。
  - 测试：线性序列复现、常数序列、两样本等于端点、非线性序列（`5,3,4,6` 配
    `sf=0.4 tf=0.6` 得 2.952）、单样本被丢弃、四种越界因子各自报错、以及
    `__name__` 被丢弃。
- 已完成（2026-11-13）：`integral(v[range], strategy=2)`。照抄 `funcIntegral`：
  - 三种求积规则：`strategy=0` 左点矩形（区间取旧样本值）、`1` 右点矩形（取新样本
    值）、`2` 梯形（两端均值，默认）；
  - NaN 一律当 0，好让「相邻的非零样本」仍能贡献面积——`1, NaN, 3` 在 0/10/20 s
    上按梯形算得 `(1+0)/2*10 + (0+3)/2*10 = 20`；
  - 求和用 Prometheus 的 `kahansum.Inc`（Neumaier 变体：`|sum| >= |inc|` 时把误差
    记到 `sum` 一侧，否则记到 `inc` 一侧；中间结果溢出为 Inf 时清零补偿项），
    照抄是因为梯形面积要先把 `curr + prev` 加起来再除 2，大数相加本身就会吃掉
    小数；
  - 梯形分支照抄了那条溢出保护：只有 `value` 溢出而两端都有限时才改成「各自减半
    再相加」，否则才整体除 2；
  - `strategy` 非法（NaN / ±Inf / 非整数 / 越界）时 Prometheus 只发 warning 并退回
    梯形，这里同样退回默认而不是报错；参数缺失也走默认。**差异**：Prometheus 的
    warning 走 annotation，本服务的求值器没有注释通道，所以这类降级是静默的，
    roadmap 在此记录。
  - 测试：常数序列三种规则都等于 20、斜坡区隔三种规则（10 / 30 / 20）、NaN 归零、
    单样本等于 0（与 `deriv` 丢弃不同）、五种非法 strategy 都退回梯形，以及
    `integral(m[60s])` 与 `integral(m[60s], 0)` 两种查询写法。
- 已完成（2026-11-18）：聚合与区间函数的**浮点求和/方差**对齐 Prometheus 的
  Kahan / Welford 实现（此前一律朴素 `+=` / 两遍法，结果在病态输入上不一致）。
  逐条照抄当前上游（`promql/engine.go` 的 `aggregation`、`promql/functions.go`
  的 `funcSumOverTime` / `funcAvgOverTime` / `funcStdvarOverTime`、以及
  `util/kahansum.Inc`）：
  - `sum(v)` / `sum_over_time(v[range])`：用 `kahansum.Inc`（Neumaier 变体；
    中间结果溢出为 Inf 时把补偿项清零）累加，末尾 `sum + c`，若 `sum` 已是 Inf
    则直接返回它（再 `+c` 只会把 `±Inf` 变成 NaN）。聚合从首个样本播种、
    `sum_over_time` 从 0 起累加，与 Go 的两种写法一致；
  - `avg(v)` / `avg_over_time(v[range])`：与上游一样是**混合**策略——只要运行和
    不溢出就用 Kahan 累加，一旦某步会溢出就改用增量均值
    `mean += f/n - mean/n`（并用当前计数重算 `mean`、按比例缩放补偿项），
    返回 `mean + c`。这样 `avg([1e308,1e308,-1e308,-1e308])` 得精确的 0，
    而「先求和再除」会给出 `Inf - Inf = NaN`；
  - `stdvar(v)` / `stddev(v)`：**Welford 在线式**（`delta = f - mean;
    mean += delta/n; value += delta*(f - mean)`），不是两遍法。对
    `1e9 + {0.1,0.2,0.3,0.4}` 这种「均值远大于散布」的输入，两遍法会因求和丢位
    而偏离；
  - `stdvar_over_time` / `stddev_over_time`：**带 Kahan 的 Welford**（均值与
    平方偏差和各自带补偿项），与聚合的朴素 Welford 是两条不同代码路径——在
    `1e16 + {0,1,2,3}` 上聚合得 5、区间函数得 3，二者**本就不同**，因此分别照抄；
  - `min(v)` / `max(v)`：改为「用首个样本播种，仅当 `acc < f` / `acc > f`
    **或 `acc` 是 NaN** 时替换」。全 NaN 组得 NaN（旧的
    `fold(INFINITY, f64::min)` 会返回 `+Inf`）；中间插入的 NaN 不触发重启
    （与 NaN 比较恒为假，被自然忽略）；
  - MetricsQL 的 `aggr_over_time(<agg>, v[range])` 改为**委托**给同名的
    `*_over_time` 函数，保证两条入口逐位一致（VM 的定义就是按名字选 rollup）；
  - 有意**不改**的：VM 语义的 `sum2` / `geomean` / `median` / `mode` / `distinct`
    聚合与 `share` / `zscore` 等（VM 用朴素累加），保持原样；
  - 测试：用 Go 版复刻上述例程生成的黄金值做断言——`[1e16,1,-1e16,1]` 求和
    为 2（朴素为 1 或 0）、9 条 `[1e16, 1×8]` 跨序列求和为
    `1.0000000000000008e16`（朴素为 `1e16`）、Welford 方差
    `0.012499988079074598`、`1e16+{0..3}` 区间方差 3、溢出输入 `avg` 为 0；
    另有 NaN 播种和 `aggr_over_time` 等价的用例；活服务冒烟逐项复现同样的值，
    日志 0 error。
- 已完成（2026-11-18）：`changes()`、`min_over_time` / `max_over_time`、`topk` /
  `bottomk` 的 NaN 语义对齐 Prometheus。这是一类「NaN 比较恒为假」的坑：Rust 与
  Go 在这一点上行为相同，但**朴素写法**（`!=`、`fold(INFINITY, f64::min)`、
  `total_cmp`）都会把 NaN 当成普通值，于是与上游分道扬镳：
  - `changes(v[range])`：照抄 `funcChanges` 的
    `cur != prev && !(IsNaN(cur) && IsNaN(prev))`。裸 `!=` 会把 `NaN != NaN`
    看成 `true`，于是每个「连续两拍都是空缺」的区间都被误记成一次变化；
  - `min_over_time` / `max_over_time`：照抄 `compareOverTime`——用首个样本播种，
    仅当 `cur < best`（`>`）**或 `best` 是 NaN** 时替换。旧写法
    `fold(INFINITY, f64::min)` 对全 NaN 序列返回 `+Inf`/`-Inf`，现在返回 NaN；
    中间插入的 NaN 不参与（与 NaN 比较恒为假）；`ts_of_min/max_over_time` 用的
    `>=`/`<=` 与「取最后一个并列者」规则此前已对上，不动；
  - `topk` / `bottomk`：上游用小顶堆填 `k` 个槽位，堆的 `Less` 把 NaN 当成**最小**
    值，但只在遇到**真实样本**时才把它换掉（`heap[0] < s || (IsNaN(heap[0]) &&
    !IsNaN(s))`），最后 `sort.Reverse` 又把 NaN 甩到末尾——净效果是 NaN 永远比任何
    数字差、只有真实值不足 `k` 个时才占位。`f64::total_cmp` 恰恰相反（把 NaN 排在
    `+Inf` 之上），会让 `topk` 把 NaN 当作最大样本返回。改为显式的
    `prom_rank_cmp`：NaN 恒比数字差，普通值正常比较；
  - 两条输出顺序也照上游：`topk` 降序、`bottomk` 升序，NaN 在两者里都落在末尾；
  - 测试：`changes` 的 NaN→NaN 为 0、单边 NaN 为 1；全 NaN 区间的 min/max 为
    NaN、前导 NaN 被真实值替换、中段 NaN 不参与；`topk(3, ...)` 不含 NaN 而
    `topk(4, ...)` 把 NaN 放在末尾（bottomk 同理）；活服务冒烟逐项复现，日志 0 error。
- 已完成（2026-11-19）：`__type__` / `__unit__` 与 `__name__` 一样被当作**元数据
  标签**处理。上游把这些归到 `schema.IsMetadataLabel`（`schema/labels.go`），凡是
  「丢指标名」的地方都调用 `DropReserved(schema.IsMetadataLabel)`，把三个一起丢掉；
  PR #18680（`0d0ac178e5`，2026-10-10）把这条规则补齐到所有算子和函数上。本实现此前
  只认 `__name__`，于是：
  - **`without` 分组**：上游先把 `__name__`/`__type__`/`__unit__` 插进 grouping 再
    排序，所以 `sum without (job) (m)` 的输出去掉 `__type__`/`__unit__`；本实现只在
    `group_key` 里滤 `__name__`，两个只差 `__unit__` 的序列会保持分开；
  - **`ignoring` 匹配**：上游的 `matchLabels` 在 `ignoring` 时把三个元数据标签
    追加进忽略集合，因此 `a{__unit__="req"} + ignoring(job) b{__unit__="ms"}` 仍能
    配对（默认匹配本来就不看 `__name__`，现在也不看另两个）；
  - **算术/函数的标签丢弃**：`m * 2`、`clamp`、`round`、`label_value`、字符串运算、
    `histogram_quantile` 的分组等原本只 `remove("__name__")`，现在统一走新的
    `drop_metadata_labels`；
  - **`absent` 的标签推导**：上游只跳过这三个（`schema.IsMetadataLabel`），**不是**
    跳过所有 `__` 前缀的名字。`absent(x{__aaa="1",job="a"})` 上游给
    `{__aaa="1",job="a"} 1`，而本实现此前把 `__aaa` 也滤掉，只给 `{job="a"} 1`。
  新增 `test_metadata_labels_are_dropped_like_the_metric_name`，覆盖 `without`
  （未显式列出时也丢）、默认匹配、`ignoring`、`on`、算术与 `absent`，并已逐条验证
  回退修复后对应断言会失败；`test_absent_carries_selector_equality_matchers` 里
  「`__` 前缀一律跳过」的旧断言同时改正为「只跳过三个元数据标签」。
  注：ThingsTSDB 的写入路径不接受保留标签，所以这条差异只在外部导入带 `__type__`
  的数据、或查询里显式写这些匹配器时才可见，但语义仍按上游对齐。
- 已完成（2026-11-19）：`quantile` 的两处短路**恢复**。2026-11-18 那一轮照着当时
  的快照（2026-09）把 `weight == 0` 和「相邻样本相同」两个提前返回删掉了，理由是
  「上游无条件求值」。但上游后来又把它们加回来了，而且理由相反：
  - **`weight == 0` 提前返回**（`promql/quantile.go`，PR #19899，2026-10-13）：
    正好落在整秩上时不该插值。否则 `Inf * 0` 是 NaN，
    `quantile(0.5, [1, 2, +Inf])` 会变成 NaN，而上游给 `2`；`quantile(0.5,
    [-Inf, 2, 3])` 同理是 `2`。两个异号无穷相邻时（`[-Inf, +Inf]`）仍是 NaN，
    因为它们真的插值；
  - **「相邻样本逐位相同」提前返回**（PR #20180，2026-11-03）：`lo == hi` 时
    `lo*(1-w) + hi*w` 不一定还原 `lo`，`quantile(0.3, [3, 3])` 会得到
    `2.9999999999999996`。上游为此显式短路，并要求**逐位**相等才能短路，好让
    `-0` 与 `+0` 保持区别（`quantile(0.5, [-0, -0])` 是 `-0`，
    `quantile(0.5, [0, -0])` 是 `+0`）。
  两者都已照抄，测试改成断言新金值：`quantile(0.3, [3, 3]) == 3`、
  `quantile(0.5, [1, 2, +Inf]) == 2`、`quantile(0.5, [-0, -0])` 保留符号位。
- 已完成（2026-11-18）：`quantile` 的排序与上游逐位对齐：
  - **排序改为 NaN 排在最前**。Prometheus 用 `vectorByValueHeap` 排序，
    其 `Less` 在 `vi` 是 NaN 时返回 `true`，所以 NaN 比任何数都小；`f64::total_cmp`
    相反，把 NaN 排在 `+Inf` 之上。差异是可见的：
    `quantile(0.5, [3, NaN, 1])` 上游排成 `[NaN, 1, 3]` 得 `1`，`total_cmp` 排成
    `[1, 3, NaN]` 得 `3`；`quantile(0, …)` 则因为落到 NaN 上而返回 NaN。
    新增 `sort_for_quantile` 复刻该比较器，`quantile()` 聚合与
    `quantile_over_time` 都改用它（VM 侧的 `median`/`mad`/`outliers_*` 本来就先滤
    NaN，排序比较器对它们没有影响）；
  - 测试：`test_quantile_interpolates`、`test_quantile_sorts_nan_first_like_prometheus`
    （单位）与 `test_quantile_nan_ordering_end_to_end`（聚合 + `quantile_over_time`
    + 相同邻值），全部用 Go 复刻 `quantile()` 的金值；两条短路恢复后，前两个测试
    改断言 `quantile(0.3, [3, 3]) == 3` 与 `quantile(0.5, [1, 2, +Inf]) == 2`。
- 已完成（2026-11-19）：`deg()` / `rad()` 改回**折叠常数**。2026-11-18 那一轮按
  当时的快照把公式从 `v * (180/Pi)` 改成了 `v * 180 / math.Pi`（先乘后除），因为
  两者在 `deg(0.1)` 上差 1 ULP。但上游随后自己反转了（`promql/functions.go`，
  PR #19901，2026-10-13，标题就是 "avoid overflow in rad and deg"）：先乘 180 在
  大角度上会溢出，`deg(1e306)` 的中间积 `1e306 * 180` 是 `+Inf`，结果变成 `+Inf`，
  而折叠常数给正确的 `5.729577951308232e+307`；`rad(1e308)` 同理应为
  `1.7453292519943295e+306`。现在照上游写 `v * (180 / math.Pi)` /
  `v * (math.Pi / 180)`，测试同时断言这两个大数金值（取自
  `trig_functions.test`）与 `deg(0.1)` / `rad(-7.89)` 的 1 ULP 差别。
- 已完成（2026-11-18）：`tmin_over_time` / `tmax_over_time` 改为 VM 的并列极值
  语义。这两个是 MetricsQL 独有的 rollup（Prometheus 只有 `ts_of_min_over_time` /
  `ts_of_max_over_time`），实现却借了 `min_by` / `max_by` + `total_cmp`，三处都偏：
  - **并列极值取最后一个**。VM 的 `rollupTmin` / `rollupTmax` 用 `<=` / `>=`，注释
    写明「Get the last timestamp for the minimum value as most users expect」，
    所以 `[5, 3, 3]`（0/10/20 s）的 `tmin` 是 `20`；Rust 的 `min_by` 返回**第一个**
    最小元素，给出 `10`。（`max_by` 恰好返回最后一个最大元素，两个函数此前因此还互不
    对称。）现在两者都走 `timestamp_of_extreme` 的显式比较；
  - **`-0` 与 `+0` 视为相等**。`total_cmp` 把 `-0` 排在 `+0` 之下，于是
    `tmin_over_time([-0 @0s, +0 @10s])` 会停在 `0 s`，而 Go 的 `0 <= -0` 为真，
    取 `10 s`；`tmax` 在 `[+0, -0]` 上同样反着来；
  - **先滤 NaN**。VM 在调用任何 rollup 之前都会跑 `removeNanValues`
    （`eval.go` 的 `doParallel` 回调），这两个函数原先漏在
    `is_metricsql_rollup` 之外，于是 `tmax_over_time([1, NaN, 2])` 会挑中 NaN 的
    时间戳（`total_cmp` 认为 NaN 最大）；现在与其它 VM rollup 一样先滤 NaN，窗口里
    只有 NaN 时该序列没有结果。
  新增 `test_tmin_tmax_over_time_follow_vm_tie_breaks`，金值来自 `rollupTmin` /
  `rollupTmax` 的 Go 转写。
- 已完成（2026-11-19）：给 `sgn()` 补测试、给 `round()` 补退化边界断言。两者的
  实现本来就没问题，但 `sgn` 此前**一条测试都没有**，`round` 也只覆盖了常规值：
  - `sgn`：`funcSgn` 的分支是 `v < 0 → -1`、`v > 0 → 1`、`default → v`，所以
    `-0` 原样返回 `-0`（`-0 < 0` 为假，却真的是负零）、`NaN` 原样返回 `NaN`、
    `±Inf` 当普通量级给 `±1`、次正规 `±5e-324` 也给 `±1`。断言时不能只比
    `==`：`-0.0 == 0.0` 为真，值相等看不出符号，得再用 `is_sign_negative()`
    区分。新增 `test_sgn_signs`，用 Go 复刻 `funcSgn` 的金值逐项对照
    （`sgn(-0)` 的符号位、`sgn(NaN)`、`sgn(±Inf)`、次正规），并核对结果只丢
    `__name__`、保留其余标签、标量参数产出一条无标签样本。
  - `round`：补上 `toNearest = ±Inf`（`1/±Inf = ±0`，中间积塌成 `±0`，
    `floor(±0 + 0.5) = 0`，除回去是 `NaN`）、次正规 `5e-324`（倒数溢出成 `±Inf`，
    `Inf/Inf` 也是 `NaN`）、负 `toNearest`（两次除法把符号抵消，仍然远离零取整）
    与 `round(±Inf)`（原样保留）的断言，全部与 Go 的
    `floor(f*(1/n)+0.5)/(1/n)` 逐位一致，因此实现无需改动。
- 已完成（2026-11-19）：`timestamp(v)` 只在参数是**裸选择器**时报告样本时间戳。
  上游为此有一条专用通路：`engine.go` 的 `Call` 分支看到
  `timestamp` / `start_timestamp` 且第一个参数是 `*parser.VectorSelector` 时，改走
  `rangeEvalTimestampFunctionOverVectorSelector`，那里的样本带 `T: t`（底层样本
  时间）；其它任何参数都走普通 `rangeEval`，而它的 `gatherVector` 会把输入样本
  一律重写成 `T: ts`（**求值时刻**）。所以上游 `timestamp(m)` 是采样时刻，而
  `timestamp(m + 0)` / `timestamp(abs(m))` / `timestamp(sort(m))` /
  `timestamp(sum(m))` 都是 step 时刻。本实现原先对所有带 `sample_ts` 的结果都
  返回样本时间戳，于是上述派生形态全错（`timestamp(sort(m))` 在 15 s 求值时
  应得 15，却返回 10）。现在 `math_func` 多收一个「参数是否裸选择器」的标记，
  只有它为真且样本确实带了 `sample_ts` 时才用样本时间，否则用 `at_ms`。括号在
  解析期已经消掉，所以 `timestamp(((m)))` 仍算裸选择器（与
  `prom_functions.test` 的 `timestamp(((metric)))` 断言一致），`offset`/`@`
  也照旧跟着选中的样本走。新增
  `test_timestamp_reports_the_sample_time_only_for_a_bare_selector`，并已验证把
  修复回退后该测试会失败。
- 已完成（2026-11-19）：`deriv` 的拟合原点改为求值时刻，`linear_regression` 去掉
  `varX == 0` 的兜底。斜率在精确算术里与原点无关，所以 `deriv` 一直沿用第一个样本
  的时间戳；但浮点里 `sumX/sumXY/sumX2` 都随原点变化，`funcDeriv` 传的是
  `enh.Ts`。用 2000 条伪随机序列（2–13 个样本）对比，两种原点有 1811 条的斜率
  **不逐位相等**，差 1–3 ULP（两样本窗口最明显：`deriv` 于 265 ms / 15835 ms 两点、
  求值时刻 19835 ms 时 `-0.11199032644906443` vs `-0.1119903264490607`）。现在与
  `predict_linear` 一样取窗口的 `eval_ts`（直接调用 `apply_range_func` 的单元测试
  助手上退回第一个样本的时间戳）。另外 `linearRegression` 直接写
  `covXY / varX`：原先 `varX == 0.0` 时返回斜率 0，而上游不做这个兜底——时间戳
  全同时它算的是 `0/0`（NaN），浮点残差不为零时则是一个由残差决定的比值。新增
  `test_deriv_anchors_at_the_eval_timestamp`，金值取自 Go 的 `linearRegression`。
- 已完成（2026-11-19）：补 `bucket_quantile` / `bucket_fraction` 空输入的边界断言。
  两处空输入都已在正确位置返回 NaN，但缺测试钉住顺序（上游提交 `d5b1febeb1` /
  `0639411d7c`）：
  - `BucketQuantile` 的 `len(buckets) == 0 → NaN` 在 q 的三处越界短路**之后**，所以
    空输入 + q<0 仍得 `-Inf`、q>1 得 `+Inf`、q=0/0.5/1/NaN 得 NaN；
  - `BucketFraction` 的 `len(buckets) == 0 → NaN` 在 `lower >= upper → 0` **之前**，
    所以空输入 + 反向边界也得 NaN。新增 `test_bucket_fraction_interpolates_classic_buckets`
    的空输入全边界表格；`test_histogram_quantile_interpolates_classic_buckets` 补上
    空输入 + q=-1/q=2 的越界组合。回退任一顺序后对应断言失败（已验证）。
- 已完成（2026-11-19）：grouping / matching label 接受未加引号的冒号。上游
  PR #20169（未加引号 UTF-8 名称默认启用）一度让 `sum by (a:b)`、`a + on (a:b) b`
  这类「需要加引号」的 label 名被拒（因为它们含 `:`），随后提交 `2911912782`
  恢复：`on`/`ignoring`/`group_left`/`group_right` 与 `by`/`without` 的 label
  名不再做「需加引号」检查（只有 selector 里的 label name 仍不接受未加引号冒号）。
  本实现的 lexer 在名字续接处一律允许 `:`，因此 `by (a:b)`、`on (a:b)`、
  `group_left(a:b)` 都已可解析。新增 `test_vector_matching_modifiers_parse`
  的三条断言（`on(a:b)`、`group_left(a:b)`、`sum by (a:b)`）；回退 lexer 的
  冒号续接后该测试失败（已验证）。
- 已完成（2026-11-19）：`stdvar_over_time` / `stddev_over_time` 对 NaN/Inf
  首样本语义的确认与补测。与聚合不同，上游 `varianceOverTime` 是一个从 0
  开始的顺序 Kahan-Welford 遍历（无 group-init 播种），首样本 NaN/±Inf 通过
  `delta * (f - mean)` 自然传播为 NaN（`Inf * (f - Inf) = NaN`）。本实现
  逐字一致，补 `test_over_time_kahan_and_welford_match_prometheus` 的首样本
  NaN/+Inf/-Inf、混合序列与中间 NaN 断言（Go 金值 `stdvar_ot.go` 验证）。回退
  实现到聚合那种「从 0 播种累加器」不会改变 over_time 结果，但补测钉住了与
  上游一致的传播行为。
- 已完成（2026-11-19）：`stdvar`/`stddev` 聚合对 NaN/Inf 首样本的语义。上游
  Prometheus 在 `STDVAR`/`STDDEV` 分组的初始化分支里，当首样本是 NaN 或 ±Inf
  时将方差累加器 `floatValue` 初始化为 NaN，否则初始化为 0。这确保首样本是
  NaN/±Inf 的单元素组返回 NaN 而非 0（PR #20056 后续，当前 main 的
  `prom_engine.go:4349-4367`）。本实现的 `aggregate_values` 已按此修正；新增
  `test_stdvar_seeds_nan_for_nan_or_infinite_first_sample` 覆盖单样本 NaN/+Inf/-Inf、
  混合序列以及普通输入的边界。回退该初始化分支后测试失败（已验证）。
- 已完成（2026-11-19）：`integral()` 的梯形平均加溢出保护（补测试）。上游
  `funcIntegral`（提交 `e71425d03a`，PR #19729 后续）在梯形积分里先
  `(currVal+prevVal)` 再 `/2`，于是两个**有限**的大数（如 `1e308`）相加会先溢出成
  `±Inf`、平均变成 `Inf`。修法是：若 Kahan 和已为 `±Inf` 而两个输入都有限，就改成
  `currVal/2 + prevVal/2`。本实现的这一段早已照抄（`kahan_inc(current/2, before/2)`），
  只是注释还写着「sum 之后减半」，已更正。现在补上金值测试：常数 `1e308` 的
  `integral` 得 `1e308`（不是 `Inf`）、`-1e308` 得 `-1e308`、真正含 `+Inf` 样本的
  序列仍得 `+Inf`；回退溢出保护后第一条断言失败（已验证）。
- 已完成（2026-11-19）：解析器接受未加引号的 UTF-8 指标名。上游 PR #20169
  （"enable unquoted UTF-8 names by default"，2026-10-23）把 lexer 的
  `isNameStart` 扩为「ASCII 字母、`_`、或任意 Unicode 字母」，`isNameContinue`
  再额外允许 ASCII 数字和 `.`（`promql/parser/lex.go`）。此前实现的 lexer 只从
  ASCII 字母或 `_` 起头、续接仅 ASCII 字母数字 + `_` + `:`，于是 `指标.总数`
  报 `unexpected character '指'`。现在换成两个判定函数 `is_name_start` /
  `is_name_continue`，与上游一致：`指标.总数`、`指标_a.b`、`sum(指标.总数)`
  都能解析并求值；数字字面量不受影响（`1.5` 仍是一个数，`.` 只在名字的续接
  位置属于名字）。`is_name_byte`（`label_replace` 的 `$name` 展开）保持原样，
  那是替换模板的语法、与上游 `expandReplacement` 的 `isValidLabelName` 无关。
  新增 `test_utf8_metric_names_parse_and_evaluate`：四项 `parse_ok`、`1.5`
  仍是数字、以及一条中文名序列的端到端求值；回退 `is_name_start` 或
  `is_name_continue` 的 `.` 任一后该测试失败（已逐条验证）。
  注：不追求上游的表达式打印器（`Expr.String()`），本服务没有 AST 打印路径。
- 已完成（2026-11-19）：instant 查询中 range vector 函数也拒绝同一标签集冲突。
  上游 PR #18631 之后 range vector 函数（`max_over_time` / `rate` 等）的输出
  走 `mergeSeriesWithSameLabelset`（`promql/engine.go`）：同名标签集但时间戳
  不重叠的两条序列会被合并，真正重叠时报
  `vector cannot contain metrics with the same labelset`。此前实现的
  `range_query` 只在「每个 step 累积」时查重（见上一条），instant 查询路径
  `range_func` 没有检查——两条不同名、去掉 `__name__` 后剩余标签集相同的序列
  （`metric_1{common="x"}` 与 `metric_2{common="x"}`）在窗口内都有样本时，
  会静默输出两个 `{common="x"}` 点。现在 `range_func` 返回前用一个 `HashSet`
  检查重复标签集，冲突报同一 `same_labelset_error()`（`error_type() ==
  "execution"`）。instant 查询里所有点共享求值时刻，因此任何重复标签集都必然
  是冲突，无需区分合并。新增 `test_range_func_rejects_a_same_labelset_collision`，
  验证 `[30m]` 与 `[5m]` 两种窗口都报错、单序列正常返回；回退修复后该测试
  失败（已验证）。
- 已完成（2026-11-19）：一元负号丢掉元数据标签，range 查询拒绝同一 step 的标签集冲突。
  两处都来自上游 2026-09 之后的提交：
  - **一元负号**（`-m`）此前只把值取反、保留 `__name__`。上游把 `*parser.UnaryExpr`
    当作算术运算处理，在 `e.Op == parser.SUB` 分支里
    `DropReserved(schema.IsMetadataLabel)`（PR #18680 之前只丢 `__name__`），所以
    `-m` 的结果没有指标名，而 `+m` 是恒等变换、名字保留。现在照抄：负号分支走
    `drop_metadata_labels`，正号直接返回。`-0` 仍保留符号位（Go 的 `-x`）。
    新增 `test_unary_negation_drops_metadata_labels`，验证 `-a` 只剩 `job`、
    `+a` 保留 `__name__`、`-z`（z=0）的 `to_bits()` 是 `-0`。
  - **range 查询的标签集冲突**。上游 `rangeEval` 按标签集把每个 step 的输出收进
    `map[uint64]seriesAndTimestamp`，同一个标签集在同一个 step 出现两次就报
    `vector cannot contain metrics with the same labelset`（PR #18631 之前更严格，
    任何 step 重复都报错；现在允许跨 step 复用标签集）。本实现的 `range_query`
    用 `HashMap<MatchKey, Vec<(i64, f64)>>` 累积，同一 step 的两条同标签集样本会
    被静默追加成「一个时间戳两个值」，输出非法矩阵。现在拆出 `record_step`，
    每个 step 用一个 `HashSet` 检查重复，冲突返回
    `Error::Exec("vector cannot contain metrics with the same labelset")`
    （`error_type() == "execution"`，与上游的 panic-to-error 路径一致）。
    跨 step 复用标签集仍然合法，这也正是上游 `mergeSeriesWithSameLabelset`
    要支持的场景（`max_over_time({__name__=~"metric_.*"}[5m])` 两条交替序列在
    任一 step 都只有一条落在窗口内）。新增
    `test_record_step_rejects_a_labelset_collision`：同一 step 冲突报错、
    跨 step 复用正常累积、不同标签集各自成序列；回退修复后该测试失败（已验证）。
- 已完成（2026-11-19）：日期函数的时间戳改为 **地板取整**（`math.Floor`）。2026-11-18
  那一轮按当时的快照把 `dateWrapper` 读成「向零截断」（`int64(el.F)` /
  `enh.Ts/1000`），但上游随后修正了这一点（`promql/functions.go`，PR #20056，
  2026-10-26，"fix date functions for negative fractional timestamps"）：
  - **样本值改回 `int64(math.Floor(el.F))`**。负的小数时间戳属于**前一秒**：
    `year(vector(-0.5))` 是 `1969`、`minute(vector(-0.5))` 是 `59`、
    `minute(vector(-60.5))` 是 `58`、`hour(vector(-3600.001))` 是 `22`。
    向零截断会把这些全算成 epoch。`truncate_unix_seconds` 因此改名为
    `floor_unix_seconds`，NaN/±Inf 仍按 roadmap 既定语义读作 epoch（Go 在这里是
    未定义行为，amd64 产出 `math.MinInt64`、读成约 2.9e11 年，不复刻）；
  - **无参数形式改走 `time.UnixMilli(enh.Ts)`**（同一提交顺手换掉的），即先按
    **毫秒**保留、再交给 `time.Unix` 去 flooring。所以 `-1500` ms 是第 `-2` 秒
    （`hour() = 23`、`minute() = 59`），`-500` ms 是第 `-1` 秒，`-60001` ms 的
    `minute()` 是 `58`。实现在求值时刻上用 `div_euclid(1_000)` 复刻同一效果
    （对负毫秒向上取整到前一秒）；
  - 后续提交 `bb09ae7aa1` 只是补了 `-0.001` / `-3600.001` 的 epoch 边界用例，没有
    再改实现；
  - 测试：`test_date_functions_truncate_towards_zero_like_go` 改名为
    `test_date_functions_floor_negative_fractions_like_go`，金值取自 Go 复刻
    `dateWrapper`。覆盖 `-0.5` / `-0.001` / `-60.5` / `-3600.001` / `-86400`
    五个样本值的全部日期分量，以及 `-500` / `-1500` / `-60001` / `0` ms 的
    无参数形式；两条修复分别回退后对应断言会失败（已逐条验证）。
  - `CivilDate.year` 保持 `i64`（前一轮为避免 `i32` 截断而放宽），与本次无关。
- 已完成（2026-11-20）：MetricsQL 聚合的 `limit N` 修饰符
  （`app/vmselect/promql/parser.go` 的 `parseAggrFuncExpr` 尾部，
  `app/vmselect/promql/aggr.go` 的 `aggrPrepareSeries`）：
  - **语法**：聚合调用（含可选 `by` / `without` 修饰符）之后可跟 `limit <整数>`，
    大小写不敏感（`sum(x) limit 10` / `sum by (job) (x) limit 10` / `SUM(x) LIMIT 3`），
    前缀式与后缀式都支持。`limit` 后面必须是整数，非数字 / 缺失值 / 小数 / 科学计数法
    一律报 `cannot parse limit ...: not an integer`，照抄 VM 的 `strconv.Atoi`
    严格整数解析（拒绝 `2.5`、`abc`、`-1.5`）。**负数 limit 不可解析**：lexer 把 `-`
    当二元运算符（`binary_op.go` 的 `binaryOps` 表，`scanBinaryOpPrefix` 只认数字 /
    `.数字` 开头的数 token），所以 `limit -1` 走的是「`-` 之后缺操作数」路径，
    `Atoi("-")` 失败——`limit -1` 是**解析错误**而非「不限制」，只有 `limit 0`
    才关闭上限（已记入决策）；
  - **语义**：`aggrPrepareSeries` 的 `maxSeries > 0` 守卫——在**聚合之前**限制
    **分组数**（不是截断输出行）。保留输入顺序前 `limit` 个不同组键，已有组继续
    追加成员；`limit <= 0` 视为不限制。VM 用 Go map 迭代，溢出时具体保留哪几组
    在**上游本身不确定**（Go map 迭代顺序随机），本服务用确定性的输入顺序前 N 组，
    与 VM 的「最多 N 组」语义一致（已记入决策）。NaN 样本不占组（VM 先跑
    `removeEmptySeries`）。`count_values` 的分组键含 value label（VM 在
    `aggrFuncCountValues` 里先改写 modifier 追加 `dstLabel`），故 `limit` 限制的是
    「不同的 (分组 + 值) 组合」数；`any` 走 `aggrFuncExt(..., min(limit, 1), ...)`，
    显式正 limit 会把结果压到**单个组**（上游怪癖，忠实复刻）；
  - **适用聚合**：所有聚合（`sum` / `avg` / `count` / `min` / `max` / `stddev` /
    `stdvar` / `topk` / `bottomk` / `quantile` / `count_values` / `limitk` /
    `outliers_mad` / `outliersk` / `any` / `share` / `zscore` / `outliers_iqr`
    等），统一在求值前按分组截断；`quantiles` 与 `histogram` 在 VM 里同样是
    `AggrFuncExpr`（`aggrFuncQuantiles` / `newAggrFunc(aggrFuncHistogram)` 都传给
    `aggrFuncExt` 的是 `afa.ae.Limit`），所以也支持 `limit N`（前缀/后缀两种写法），
    `limit` 在求分位数 / 分桶之前按分组生效；而 `histogram_quantiles` /
    `histogram_fraction` / `histogram_share` / `histogram_stddev` /
    `histogram_stdvar` 在 VM 里是 **transform**（`transform.go` 的 transform 表）
    而非 `AggrFuncExpr`，不接受 `limit`；
  - 测试：`test_metricsql_aggregation_limit_modifier`（`limit 0/1/2/100`、
    `sum by (job)` 分组级限制、`count_values` 值级限制、`any` 的 `min(limit,1)` 怪癖、
    `sum(m)` 无修饰符合并为单组）、`test_metricsql_quantiles_limit_modifier`
    （`quantiles` 分组级限制、前缀/后缀写法）、`test_metricsql_histogram_aggregation`
    追加 `limit 1` 断言、`test_metricsql_aggregation_limit_parse_errors` 与
    `test_metricsql_quantiles_parse_errors`（非整数报错）、以及既有聚合解析测试
    补充 `limit` 语法断言。
- 已完成（2026-11-20）：`label_uppercase` / `label_lowercase` 改用 Go 的**简单大小写
  映射**（simple case mapping），并修正空标签的处理。这是逐函数核对 MetricsQL 标签
  变换时发现的两处真实差异：
  - **Rust 的 `to_uppercase` / `to_lowercase` 是 full mapping，Go 的
  `strings.ToUpper` / `ToLower` 是 simple mapping**。两者对绝大多数文字一致，但
  full mapping 会把少数 rune 展开成多个字符，而 Go 只产出一个 rune：`ß` 在 Go 里
  仍是 `ß`（`STRASSE` 是错的，应为 `STRAßE`）、连字 `ﬁ` 仍是 `ﬁ`（不是 `FI`）、
  希腊 `ᾀ` 映射到单字符 `ᾈ`（不是 `ἈΙ`）、土耳其 `İ` 小写成 `i`（不是 `i̇`）。
  实现新增 `go_to_upper` / `go_to_lower`：对「Rust full mapping 展开成多字符」的
  rune（扫描全部码点，共 103 个 upper + 1 个 lower）查 `GO_UPPER_EXPANSIONS` /
  `GO_LOWER_EXPANSIONS` 小表取 Go 的 simple 结果，其余直接取 Rust 单字符 full
  mapping（与 Go 一致）。表的金值由 Go 1.26 的 `unicode.ToUpper` / `ToLower` 全码点
  扫描生成。残余的 75 + 75 个差异全部是 **Unicode 版本漂移**（Rust 的 Unicode 表
  更新，含 Go 尚未收录的新码点映射，如 U+A7CB、U+10D70、U+16EB0 等），属于双向
  版本差异，不是映射规则差异；
  - **空标签会被删除**。VM 的 `transformLabelValueFunc` 走 `getDstValue`：缺失标签
  读成空串，应用 `f` 后仍空则 `RemoveTag`。所以 `job=""` 在
  `label_uppercase(m, "job")` 之后**消失**，而缺失的标签保持缺失。旧实现用
  `get_mut` 只改已存在的标签，会把 `job=""` 原样留下。
  - 测试：新增 `test_metricsql_label_case_uses_go_simple_mapping`，金值取自 Go 的
  `strings.ToUpper` / `ToLower`（`straße` / `ﬁle` / `ᾀ` / `ŉ` / `ǰ` / `İstanbul`
  / `ǅ` / 俄文 / 全角），并覆盖端到端标签改写与「空标签删除、缺失标签不创建」。
  - 同批核对确认一致的标签变换（无需改动）：`label_set`（空值删除、重复键后者胜）、
  `label_del` / `label_keep`（`RemoveTagsIgnoring` / `RemoveTagsOn`，`__name__`
  同普通标签）、`label_copy` / `label_move`（空源跳过且不删目标、`src == dst` 不删）、
  `label_value`（丢 `__name__`、解析失败为 NaN、NaN 样本保持且全 NaN 系列不删）、
  `alias`（= `label_set(q, "__name__", name)`）、`label_map`（Go map 后者胜、
  缺失标签按空串查表可创建、映射到空串删除）、`label_transform`（非锚定
  `replace_all` + Go 展开规则 + 空展开删除 + 缺失标签新建）、
  `label_match` / `label_mismatch`（`^(?:re)$` 锚定）、
  `label_join`（无条件插分隔符、空结果删标签）、`drop_common_labels`。
- 已完成（2026-11-19）：`topk` / `bottomk` 的 `k` 参数边界对齐 Prometheus。核对聚合时
  发现一个保真度缺口：上游 `aggregationK` 先 `k = min(int64(fParam),
  len(inputMatrix))`，再 `if k < 1 { return nil }`，所以 `topk(0, m)` /
  `topk(-1, m)` 一律返回空结果；而这里此前直接 `param as usize`，负的 `f64`
  参数会被 wrap 成一个巨大的 `usize`（例如 `-1.0` → `18446744073709551615`），
  `take(k)` 于是把整组序列全吐出来，与上游相反。同族的 `limitk` / `outliersk`
  早就走 `float_to_int_bounded(k).max(0) as usize`（NaN→0、负→0、分数向零截断），
  `topk` / `bottomk` 漏了这层钳位。现在统一改走同一钳位：
  - NaN→0、负→0（都返回空）、分数向零截断（`topk(2.9, m)` 保留 2），与 Go 复刻
    `k = min(int64(fParam), len); if k < 1 { empty }` 的金值一致；
  - `±Inf` 仍按普通极值排（`topk(3)` 给 `+Inf` 在前、`bottomk(3)` 给 `+Inf`
    在后），NaN 在两向都落末尾（`topk(5)` 把唯一的 NaN 排在末位）；
  - 分组下每组的排名独立保持（`topk(2, m)` 按 `g` 分组各取前 2）。
  - 测试：在 `test_topk_and_bottomk_rank_nan_last` 里补 `±Inf` 极值排序、分组
    `topk(2)`、负/零 `k` 返回空、分数 `k` 截断四组断言，金值取自 Go 复刻
    Prometheus 的堆填充 + 最终 `sort.Reverse`。
- 已完成（2026-11-18）：`predict_linear()` 的拟合原点改为**求值时刻**（`enh.Ts`），
  与 Prometheus `funcPredictLinear` 逐字对齐。此前用「首个样本时间戳」做原点、
  预测到「最后样本 + horizon」，只有当最后样本恰好落在求值时刻时才与上游一致；
  一旦最后一次采样早于查询时刻（常见于稀疏序列），整条预测就按斜率 × 时间差偏移。
  照抄上游：`slope, intercept = linearRegression(samples, enh.Ts)`、返回
  `slope*horizon + intercept`，原点即窗口起点计算时用的 `resolve_at(at, now)`
  （**未减 offset**，与上游 `enh.Ts` 一致）。`Extrapolation` 因此带上 `eval_ts`
  字段，`apply_range_func`（无窗口的单元测试助手）退回末样本时间戳以保持旧断言。
  - 连带修正一处从窗口外推改动起就漏更新的端到端断言：`rate(http_requests[60s])`
    的 0→3 计数器因零穿越钳位得 `2/30`、10→13 得 `0.075`（此前仍写着外推前的
    `0.1`），`predict_linear(..., 3600)` 由 `363` 改为 `364`。Go 复刻
    `linearRegression` + `extrapolatedRate` 确认逐位相符。
  - 测试：`test_predict_linear_anchors_at_evaluation_timestamp` 用 Go 金值
    覆盖「末样本 == 求值时刻」（`4/3`）与「求值时刻晚 1 s」（`5/6`）两个原点，
    以及 1 小时 horizon 放大差异（`-1798.1666…` / `-1798.6666…`）。
- 已完成（2026-11-18）：`rate()` / `increase()` / `delta()` 补上 Prometheus 的
  **窗口外推**（`extrapolatedRate`）。此前只按「首尾样本差 ÷ 首尾时间差」算斜率，
  于是查询步长大于采样间隔时结果系统性偏低：窗口里只有中间几个样本时，Prometheus
  会把变化量朝窗口两端拉伸，而这里不会。逐条照抄上游：
  - 计数器先处理回绕：相邻样本下降即视为一次 reset，把**前一个样本值**加到总量上
    （`delta` 是 gauge，不做这步）；
  - 再算窗口两端到首/末样本的距离 `durationToStart` / `durationToEnd`，以及平均
    采样间隔；某端距离超过 `1.1 × 平均间隔` 时，说明序列大概在窗口内部才开始/结束，
    只外推**半个平均间隔**，否则外推到窗口边界；
  - 计数器额外把起点钳在「计数器为零」的时刻（`sampledInterval * first/result`），
    以免外推出负数；
  - `factor = (sampled + start + end) / sampled`，`rate` 再把 **factor** 除以窗口
    秒数（上游是除 factor 而非除结果，末位 ULP 因此不同，照抄）；`sampled == 0`
    时 factor 取 1；
  - 单样本没有平均间隔，直接不输出（`delta` 也一样，与 `deriv` 不同）；
  - 窗口边界随 `offset` / `@` 平移（和样本一致）；`[range:step]` 子查询上游会被
    改写成等价的 MatrixSelector，因此同样外推——窗口从 `Range` 与 `Subquery`
    两种 AST 都取 `offset_ms` / `at`；
  - 单元测试用 `apply_range_func`（无窗口）走「不外推」路径，因此新增
    `apply_range_func_in(.., Some(Extrapolation))` 显式传窗口；
  - 测试：填满窗口的序列 factor 为 1（rate 1/s、increase 60）、只在中间 20–40 s
    的序列 factor 1.5（increase 25、rate 25/60）、计数器零点钳位（increase 14.5）、
    含下降的 gauge 用 `delta` 得 10 而按计数器算得 20；活服务冒烟逐项复现同样的
    值（`0.9999999999999999` 的 rate 是 factor 除法后的浮点末位，与 Go 一致），
    日志 0 error。
- 已完成（2026-11-13）：`max_of(a, b)` / `min_of(a, b)`（两个标量进、一个标量出）。
  Prometheus 把它们当作时长表达式里的辅助函数（`max_of(step(), 5s)`），同时也
  接受普通数字。语义照抄 `funcMaxOf` / `funcMinOf`，与 `max`/`min` 聚合的 NaN
  规则一致：`if a < b || math.IsNaN(a) { a = b }`（`min_of` 是 `>`），即 **NaN 只在
  两侧都是 NaN 时** 才保留，单侧 NaN 会被另一侧替换——注意这与 `clamp` 用
  `go_min`/`go_max` 的「NaN 传播」**不同**，是两条独立的 Go 代码路径。
  操作数按 `scalar_of` 读（标量本身，或单样本向量），结果恒为 `resultType=scalar`。
  - 测试：四种大小关系、负数、`±Inf`、单侧 NaN 被替换、双侧 NaN 为 NaN、
    结果是标量（向量为空），以及向量操作数取唯一值、参数多写一个时的解析报错。
- 已完成（2026-11-13）：查询上下文标量 `start()` / `end()` / `range()` / `step()`
  （Prometheus 的 `foldQueryContextFunctions` 在求值前把它们折成数字字面量）。
  - `start()`/`end()` 是**查询区间**的首尾（不是当前 step），`range()` 是
    `end - start`，都以秒为单位；
  - `step()` 在区间查询里是请求的 step，即时时 Prometheus 只在 `start != end` 时
    才给 step 值、否则折成 0（`foldQueryContextFunctions` 里那句 `if !start.Equal(end)`），
    本服务即时查询的 `step_ms` 就是 0，语义一致；
  - 它们本来就是标量，因此 `end() - start()`、`max_of(step(), 20)` 这类组合直接
    可用（这也是 `max_of`/`min_of` 存在的理由）；标量做子查询体会照常报
    「instant vector, got scalar」。
  - 测试：区间查询下四个值（100/160/60/15）、算术与 `max_of` 组合、即时查询下
    `step()`/`range()` 为 0、`start()[5m:1m]` 报错，并加入支持表达式清单。
- 已完成（2026-11-13；2026-11-15 修正分位数标签格式与**参数顺序**）：
  `histogram_quantiles("label", q1, ..., qN, v)`（经典 `le` 桶）。照抄
  `transformHistogramQuantiles` 的经典直方图分支：对每个分位数、每个直方图各
  输出一条序列，值是该分位数的 `BucketQuantile`（与 `histogram_quantile` 完全同一份
  实现）。**参数顺序**对齐 MetricsQL：标签在**第一个**参数、分位数在中间、桶
  序列在**最后一个**（VM 的 `getString(args[0])` 取标签、`args[len-1]` 取桶）；
  早先误写成 `(v, "label", q1...)` 会让 VM 语法的查询解析失败，已改正。
- **分位数标签用 Go 的 `%g`**（`format_g_percent`）：`histogram_quantiles` 是
  MetricsQL 函数，VM 用 `fmt.Sprintf("%g", phi)` 渲染标签，**不是** Prometheus 的
  OpenMetrics `FormatOpenMetricsFloat`——两者都基于 `strconv.FormatFloat(f,'g',-1,64)`，
  但 `%g` **绝不补 `.0`**，而 OpenMetrics 会在既无 `.` 也无 `e` 时补一条。所以
  `q=1` 渲染为 `"1"`（不是 `"1.0"`）、`q=100000` 为 `"100000"`（不是 `"100000.0"`）、
  `q=0` 为 `"0"`。NaN/`+Inf`/`-Inf` 保持 Go 的拼写（`"NaN"`/`"+Inf"`/`"-Inf"`），
  真正的负零 `format_g_percent(-0.0)` 得到 `"-0"`（`%g` 保留符号位）。有限值委托
  `go_format_g`（不补 `.0`），`{:e}` 在 Rust 里给出与 Go 相同的最短数字与指数，
  所以只需按 Go 的规则摆放小数点。
  - 分位数标签是 `labels.Builder.Set` 语义：分组里已有同名标签时**覆盖**；
    标签名不做校验——Prometheus 的 `histogram_quantiles` 是 VM 专有（Prometheus 侧
    用 `getOrCreateLblsWithQuantile` 同样不校验），本实现对齐 VM，任何非空字符串都
    直接作为标签名；
  - 参数个数照 Prometheus 的 `ArgTypes`/`Variadic` 折算：至少 1 个、最多 10 个
    分位数（`na = 4-1 = 3`，`na + Variadic(9) = 12` 个参数）。
  - 越界分位数（`q<0`/`q>1`/NaN）**不是错误**：Prometheus 只发 warning，照样输出
    一条 `-Inf`/`+Inf`/`NaN` 的序列，这里同样输出（没有 annotation 通道，warning
    略去）。
  - 测试：`format_g_percent` 按 Go 实测值逐条对照（`1`/`0`/`-0`→`"-0"`/`-1`/`NaN`/
    `±Inf`、常见分位数、`100000`/`999999`/`1e+06`/`1.234567e+06`/`1e-05`/`1e-07`/
    `1e+21`/`1.2345678901234569e+23`）；`histogram_quantiles` 参数顺序为
    `("label", q..., v)`，桶在前（旧写法）被解析器报「needs a quoted string」；
    对 2/4/6/8 桶给出中位数 0.5、q=0.9 落到最后一个有限桶上界 1、q=1 标签为 `"1"`
    且值为 1.0；越界与 NaN 分位数照样各出一条
    `-Inf`/`+Inf`/`NaN`，标签是**请求的分位数**（`"-0.5"`/`"1.5"`/`"NaN"`）而不是
    结果值；同名标签被覆盖；经求值器
    端到端丢弃 `__name__`；缺 `le` 的输入返回空结果，标量输入、零个分位数、
    11 个分位数、非法标签名各自报错。


- 剩余未实现的 Prometheus 函数（**明确不做**，理由记在这里）：
  - `histogram_count` / `histogram_sum`：上游走 `simpleHistogramFunc`，只处理
    `el.H != nil` 的 native histogram，经典 `le` 桶的浮点样本被**静默丢弃**（结果是
    空向量，而不是从 `_count`/`_sum` 推算）。本服务的样本类型只有 `f64`，没有
    native histogram 表示，照抄只会得到恒空的函数。（同名的
    `histogram_avg` / `histogram_stddev` / `histogram_stdvar` 在上文以
    **VictoriaMetrics 的经典桶语义**实现了：VM 从 `le` 桶估算，Prometheus 只读
    native histogram，两个上游本来就不一致，这里跟的是 VM。）
  - `info(v, ...)`：读取 `target_info` 一类元数据序列并与目标序列做标签连接，
    需要引擎侧的元数据视角，本服务没有；
  - `start_timestamp(v)`：依赖 `EvalNodeHelper.StartTimestamps`（由
    `use-start-timestamps` 特性填充的每序列起始时间），本服务没有这条数据通路。
  这三类缺的是**存储/引擎能力**而不是函数实现，硬写只会给出看似成功、实则空或
  伪造的结果。
- 剩余未实现的 VictoriaMetrics transform 函数（**明确不做**，理由记在这里）：
  - `now()`：直接返回当前墙钟秒数（`time.Now()`），与本服务固定求值时刻的确定性
    求值模型相悖，且无法稳定测试；
  - `rand(seed)` / `rand_normal(seed)` / `rand_exponential(seed)`：结果依赖 Go
    `math/rand` 的 PRNG 序列，需逐位复刻且该算法跨 step 有状态（每 step 前移一次
    抽样），与逐 `Instant` 模型冲突；
  - `timezone_offset(tz)`：按 IANA 时区名返回每样本偏移，需要时区数据库，与本服务
    「不引入时区库、用纯整数历法」的既定设计冲突；
  - `range_*` / `running_*` / `keep_last_value` / `keep_next_value` / `interpolate` /
    `smooth_exponential` / `remove_resets`：都修正**整段序列**（跨 step 才有意义），
    逐 `Instant` 求值时每个 step 只有一个样本，会退化成恒等或空结果，与已明确不做的
    跨 step 变换同类。
   - `topk_avg` / `topk_max` / `topk_min` / `topk_median` / `topk_last` /
    `bottomk_avg` / `bottomk_max` / `bottomk_min` / `bottomk_median` /
    `bottomk_last`：是 `getRangeTopKTimeseries` 家族的**整段序列**聚合——先对每个序列
    在其时间窗 `ts.Values` 上算 `f(values)`（均值 / 极值 / 中位数 / 末值），再按该标量
    取 top / bottom-k。与 `range_*` 同属跨 step 变换，逐 `Instant` 模型下每个 step 只有
    一个样本，`f` 退化为该样本本身，无法忠实复刻，故一并归入「明确不做」。
  这些要么是 wall-clock / 外部库依赖，要么需要 VM 的整段序列求值模型，本服务的逐
  `Instant` 架构无法忠实支持。


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
