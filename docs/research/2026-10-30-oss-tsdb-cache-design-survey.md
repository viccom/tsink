# 开源 TSDB 缓存与元数据设计调研（2026-10-30）

> **性质**：外部实现调研报告，非实施计划。若采纳其中建议，另开 `docs/plans/2026-XX-XX-*.md` 走 TDD 实施。
> **方法**：6 个仓库浅克隆到 `/root/myprojects/webhmi/_oss-refs/` 后**纯静态阅读**——未编译、未运行其测试，行为描述属代码级推断。tsink 侧数字为生产实测（rs-iot 实例，~10 点/s、30 series）。
> **基线**：tsink_fork HEAD `37648e6` + 工作区未提交的 P1 低速率写放大修复批次。

## 1. 调研对象

| 仓库 | 语言 | 修订 | 关键目录 |
|---|---|---|---|
| VictoriaMetrics | Go | `71587d5`（2026-10-29） | `lib/storage/`、`lib/mergeset/` |
| Prometheus | Go | `a87694b`（2026-10-29） | `tsdb/`（head/chunks/index/compact） |
| InfluxDB 1.8 | Go | `09783cb`（1.8 分支） | `tsdb/engine/tsm1/` |
| GreptimeDB | Rust | `ec59131`（2026-10-30） | `src/mito2/src/` |
| CnosDB | Rust | `14c13e3`（2026-10-24） | `tskv/src/` |
| TDengine 3.x | C | `88d8002`（2026-10-29） | `source/dnode/vnode/src/tsdb/`、`source/libs/wal/` |

选型理由：前三个是 WAL+不可变文件+分层合并这一家族的经典实现（与 tsink 同构）；后三个提供 Rust×2 与 C 实现参照，以及"终层收敛"的不同解法（TDengine 的 STT 专门解决小文件堆积）。

## 2. 六仓机制对照

| | 写缓存 | 落盘触发 | 进程内读缓存 | 元数据真相 | 终层处理 |
|---|---|---|---|---|---|
| VictoriaMetrics | 按 CPU 数分片行缓冲，每片 8MB（`raw_row.go:27`） | 2s tick（`raw_row.go:22`）+ 攒满 15 批（`partition.go:40`）成 part + 5s 落盘间隔（`partition.go:43`） | 只缓存元数据：`ibCache` 10% 内存（`part.go:15`）、mergeset index 10%/data 25%；数据块 mmap+`mincore` 回退 `pread`（`reader_at.go:298`） | 每月分区内 `parts.json` **只存 part 目录名**（`partition.go:1884-1897`），点数/时间范围编码在目录名；文件缺失回退目录扫描（`partition.go:1913-1930`） | **无终层**：大 part 持续合并，选批要求输出/最大输入 ≥1.7（`partition.go:1669`）、≤15 路（`partition.go:40`） |
| Prometheus | per-series 内存 chunk，1<<14 条带锁；封口 chunk 批量 mmap | **按对齐时间窗切块**（2h 对齐，120 样本（`head.go:266`）/1KiB（`chunk.go:65`）双阈值）；块范围指数化 2h/6h/30h（`db.go:650`，`db.go:57`） | block index 全 mmap，posting offset 每 32 条采样驻留（`index.go:1164`）；不缓存解压结果 | **无中央清单**：`os.ReadDir` 扫描 + 每块 `meta.json`；删除靠新块 meta 的 Parents 列表 | 时间窗合并，只合并"满窗"或墓碑 >5% |
| InfluxDB 1.8 | 16 分片（`ring.go:16`、`cache.go:22`）`map[string]values`，快照指针交换 | **25MB 或空闲 10 分钟**（`tsdb/config.go:29,34`）才快照→TSM | 无 block cache；整文件 mmap + 每 key 4B offsets 数组 | **无 manifest**：文件名 `gen-seq.tsm` 承载层级，文件内 footer 自描述 | 无终层；4h 冷期全量重写兜底（`config.go:38`）；L1 攒 8 代、其余层 4 代（`compact.go:287-290`） |
| GreptimeDB | region 内按时间窗分片列式 memtable（窗宽 1 天） | 全局 write_buffer/2 + 10 分钟定时（`m2/config.rs:255`） | 十余类 moka 缓存（按字节 weigher + LRU），数据页 512MB 级 | **增量 manifest**：每次编辑追加 `{version:020}.json` delta，每 10 版后台 checkpoint（`m2/config.rs:238`）+ `_last_checkpoint` 指针（`manifest/storage.rs:54,76,79`） | 无终层；TWCS：窗关闭后每窗一个 L1 文件 |
| CnosDB | 每 vnode 一个 MemCache，分片 + immut 切分 | 仅 `is_full` 触发；冷 vnode 60s tick、闲置 1h 才 flush | 文件读取器 LRU（Arc 保活）+ 索引 KV LRU；无页缓存 | **append-only VersionEdit 日志**，超 128MB 才重写 | **终层自合并**：`picker.rs:185` `if level == 4 { (level, level) }` |
| TDengine 3.x | 1024 桶 + 每表 5 层跳表；统一 buffer pool | 活动池超段大小即 commit（写路径顺带检查，无定时器） | 本地盘**无块缓存**：`pread` + 每迭代器 pin 缓冲；last 行 LRU | `current.json` 全量重写但**条目有界**（fset × 有限文件），两阶段 rename | **STT 分层**：层内文件数达 trigger^k 即合并（默认 trigger=2，`tdef.h:652`），溢出整层并入主 data 文件 ⇒ 单 fset 文件数有界（上界为推算值，未逐行验证） |

## 3. 三条跨实现共识

**共识一：本地盘不做解码块缓存，数据块交给 OS page cache。**
VM 用 `mincore` 探测页驻留、不驻留退回 `pread`；Prometheus 直接返回 mmap 子切片按需解码；TDengine 全程 `pread` + 2 个 pin 缓冲。→ tsink 的 mmap + page cache 路线正确，**不要新增解码块缓存**；该缓存的是小体积元数据。

**共识二：段清单的真相在段自身，中央清单只是可重建缓存，且绝不整份重写。**
Prometheus/InfluxDB 没有中央清单；GreptimeDB 增量 delta + 每 10 版 checkpoint + 指针；CnosDB append-only VersionEdit；VM 的 `parts.json` 只存名字（条目数=当前 part 数）。
→ tsink 的 `series_index.catalog.json` 是六家中唯一"条目随 uptime 无界增长 + 每次变更全量重写"的组合，这正是实测 **~300MB/h 元数据写盘（占总写盘 85%）** 的根因。

**共识三：没有"终层永不合并"。**
四条路线：无终层持续合并（VM）、按时间窗收敛（Prometheus/TDengine/GreptimeDB TWCS）、终层自合并（CnosDB）、溢出并入主文件使文件数 O(层数)（TDengine）。
→ tsink 的 L2 终层永不合并是结构性异类，对应 **+450 段/天** 的无界增长。

补充（方向验证）：六家全部用**字节/时间阈值**触发落盘，而非固定 tick 无条件封段——tsink 的 seal 门控（最少点数/最大 head 年龄/未落盘 WAL 上限）与业界做法一致。

## 4. tsink 现状与实测基线（2026-10-30）

| 指标 | 当前值 | 来源 |
|---|---|---|
| 段粒度 | 1,903 点/段（修复前 5.9） | `logs/history_monitor.csv` 9.6h 汇总 |
| CPU（单核，~10 点/s） | 均值 3.98%，p95 4.85% | 同上 |
| 线程归因 | `tsink-persisted-refresh` 2.55%（占 59%）、tokio 合计 ~1.0%、compaction 0.28%、flush 0.03% | 60s `/proc/*/task` 采样 |
| catalog | 9.2MB / 15,855 段；重写 23–35 次/h；**~300MB/h ≈ 5–7GB/天** | 计数器 + `/proc/<pid>/io` 交叉验证 |
| L2 段数 | +180 段/9.6h ≈ **+450/天**（终层不合并） | monitor CSV |
| 数据本身 | 写入 ~10 点/s；磁盘增长 3.4MB/天 | monitor CSV |
| mmap 预算 | 4GiB（`src/storage.rs:19`），实际数据 ~57MB，驱逐基本不发生 | 代码 |

## 5. 建议（按投入产出比排序）

### P0 — catalog 降级为增量维护的派生缓存（低风险，收益最大）

**外部做法**：GreptimeDB 增量 manifest——每次变更只追加一个 delta 文件，每 `manifest_checkpoint_distance=10` 个版本后台打全量 checkpoint，`_last_checkpoint` 指针指向最新 checkpoint；恢复 = checkpoint + ≤10 个 delta（`m2/config.rs:238`、`manifest/storage.rs:54,76,79`）。CnosDB 用 append-only VersionEdit（超 128MB 重写）。VM 用"只存名字的 parts.json + 目录兜底"。

**tsink 落点**：`src/engine/registry_catalog.rs`（`persist_registry_catalog_throttled` 及其 memo）、`src/engine/maintenance/registry_persistence.rs`（锚点/强制路径）、`src/engine/bootstrap/recovery.rs`（启动加载与兜底）。

**设计要点**：
1. `series_index.catalog.json` 保留为 checkpoint 格式；新增 `series_index.catalog.d/` 目录，持久化时追加 `{version:020}.delta.json`（内容 = 现有 `PendingPersistedSegmentDiff` 的 added/removed roots + 对应 entry），`fsync` 后返回；
2. delta 累计 ≥100 个或 ≥1MB → 后台写全量 checkpoint + 更新 `_last_checkpoint` 指针 → 删旧 delta（复用现有 checkpoint/关闭强制路径）；
3. 启动 = 读指针 → 载 checkpoint → 顺序重放 delta → **段目录扫描兜底**（现有 `startup_recovers_when_catalog_is_missing_or_corrupt` 语义不变）；
4. delta 引用已消失段（崩溃窗口）按现有 NotFound 容忍逻辑过滤。

**预期**：~300MB/h → checkpoint 摊薄后 ~3MB/h 量级（约 100×）。
**风险**：低（纯元数据）。**改动**：中。

### P1 — L2 终层收敛（中风险，治结构性增长）

**外部做法**：CnosDB 最小版——终层自合并（`picker.rs:185` `level==4 → (4,4)`）；TDengine STT 把文件数压到 O(层数)；VM 无终层持续合并（≥1.7 倍增、≤15 路）；Prometheus/GreptimeDB 按时间窗收敛。

**tsink 落点**：`src/engine/compactor/planning.rs`（`LevelTransition` 目前只有 `L0ToL1`/`L1ToL2`）、`src/engine/compactor/execution.rs`（`compact_segments` 与 2026-10-30 新增的 `finish_zero_output_compaction` 空输出纯删除路径）、`src/engine/lifecycle/shutdown.rs`（drain 的层级计数）。

**设计要点**：加 `L2ToL2` transition——当 L2 段数 > 阈值（如 32）时按 `min_ts` 选最老的 K 个合并为 1 个新 L2 段；删除复用现有 replacement marker（已支持空输出）。查询路径本就合并全部 L2 段，合并对读完全透明。

**预期**：段数 O(天数) → O(阈值)（存量 15.8k → 百级）；catalog 体积 9.2MB → ~60KB，**单此一项即可让 P0 之前的写放大再降一个数量级**；启动扫描快 ~100×。
**风险**：中（compaction 是数据路径；必须补墓碑/时间重叠/崩溃恢复/空输出测试）。**改动**：中。

### P2 — 维护拍事件化（低风险，治剩余 CPU 热点）

**外部做法**：InfluxDB 规划前先比 `LastModified`，未变直接返回（`compact.go:475-478`）；VM/CnosDB 无周期轮询（事件驱动 / 60s 低 freq tick）；GreptimeDB 仅在内存阈值/10 分钟定时动作。

**tsink 落点**：`src/engine/runtime.rs:430-478`（`tsink-persisted-refresh` 线程循环，park 间隔 = `DEFAULT_FLUSH_INTERVAL`=250ms，见 `runtime.rs:233-245`）。

**设计要点**：flush/compaction 完成时置脏标记；线程仅在脏时执行 `run_post_flush_maintenance_if_pending` / `sync_persisted_segments_from_disk_if_dirty`，非脏时 park 到 1–5s 粗兜底。
**前置**：先对两段调用各自打点（或 perf），确认 2.55% 花在哪一段——**本调研未做该采样，属待测项**。
**预期**：2.55% → <0.5%（估计）。**风险**：低。**改动**：小（20–50 行）。

### P3 — 时间窗对齐 seal（可选，P1 的前置简化）

**外部做法**：Prometheus 按 `rangeForTimestamp` 对齐窗切块（`head_append.go:2216`）；TDengine 按 fid（时间窗）分 file set；GreptimeDB 按 1 天窗分 memtable。
**tsink 落点**：`src/engine/config.rs`（`SealPolicy`）、`src/engine/state/active.rs`（`current_head_seal_decision`）。
**要点**：seal 边界对齐到固定窗（如 5 分钟），使同一窗内所有 series 同段，窗关闭后一次合并；让 P1 的 L2 合并总是"整窗合并"。**风险**：中。**改动**：中。**注意**：只有决定实施 P1 时才值得做。

### P4 — 段读取去掉"驱逐 + 重映射"（低优先级）

**外部做法**：VM `mmap` + `mincore` + `pread` 回退；TDengine 全程 `pread` + pin 缓冲；InfluxDB 只 `MADV_DONTNEED`、从不主动 munmap；CnosDB Arc 保活。
**tsink 落点**：`src/engine/state/segment_maps.rs:95`（`get_or_remap`）、`:111`（`remap`）、`src/storage.rs:19`（4GiB 预算）。
**要点**：查询持 pin（引用计数），归零才 unmap；或驱逐后读走 `pread` 回退。当前 57MB 数据 / 4GiB 预算下驱逐不发生，属前瞻性修复（对应审查 P1"驱逐窗口查询 NotFound"）。**风险**：低-中。**改动**：小-中。

### P5 — compaction 选择启发式（低风险，抑制写放大）

**外部做法**：VM 要求输出/最大输入 ≥1.7 且 ≤15 路（`partition.go:40,1669`）；InfluxDB 各层攒代数（`compact.go:287-290`）；Prometheus 只合"满窗"或墓碑 >5%。
**tsink 落点**：`src/engine/compactor/planning.rs`（`select_compaction_window`/`has_time_overlap`）。
**要点**：加"输出 ≈ 输入之和则跳过"与"倍增下限"判据，避免无意义重写。**风险**：低。**改动**：小。

## 6. 不建议照搬（对当前规模是过度设计）

- VM：fastcache 双代缓存 + 落盘 dump、按 CPU 数分片、15 路并行合并、按月分区 + 每小时轮转 indexDB；
- GreptimeDB：十余类缓存体系、对象存储 write cache（默认关）、staging manifest、远端 compaction；
- TDengine：三代 buffer pool、共享存储 bVec/pgCache、rocksdb 持久化 last cache、多副本 sync 回放；
- CnosDB：16 分片 LRU、GreedyMemoryPool 满即拒写、WAL entry 环形缓存；
- InfluxDB：1GB cache 上限、启动全量 mmap 所有 TSM、冷期全量重写全部文件（15.8k 段会放大成灾）。

## 7. 建议实施顺序

1. **P0**（纯元数据，风险最低，先吃掉 85% 写盘）；
2. **P1**（结构性收敛，需补测试；与 P0 独立，单独做也能降一个数量级）；
3. **P2**（先打点再改）；
4. P5 → P4 → P3（视是否继续追优化）。

## 8. 复核记录（2026-10-30 第二轮）

已用 `grep` 逐条核实并记入正文的承重事实：VM `maxRawRowsPerShard`（8MB/片）/`pendingRowsFlushInterval`（2s）/`dataFlushInterval`（5s）/`defaultPartsToMerge=15`/`minMergeMultiplier=1.7`/`parts.json` 只存名字 + 目录兜底（`partition.go:1884-1930`）；Prometheus `DefaultBlockDuration=2h`/`ExponentialBlockRanges`/`DefaultSamplesPerChunk=120`/`MaxBytesPerXORChunk=1024`/`os.ReadDir` 发现块/`symbolFactor=32`；InfluxDB 25MB/10min/4h 冷重写/8 代与 4 代（`compact.go:287-290`）/16 分片（`ring.go:16`）/`LastModified` 短路（`compact.go:475-478`）；GreptimeDB checkpoint distance=10/`_last_checkpoint`/10min 定时 flush；CnosDB 终层自合并（`picker.rs:185`）；TDengine `TSDB_DEFAULT_SST_TRIGGER=2`（`tdef.h:652`）。

**未完全核实、正文已标注为推断**：①TDengine "单 fset 文件数上界 15" 为按 trigger=2 与合并逻辑的推算，未逐行验证其上限公式；②GreptimeDB "13 类缓存" 为分类计数，未逐一列举验证；③各仓"读缓存容量占比"数字来自其默认配置常量，未在生产规模验证实际命中率。

## 9. 参考

本地路径（浅克隆，可删）：`/root/myprojects/webhmi/_oss-refs/{VictoriaMetrics,prometheus,influxdb-1.8,greptimedb,cnosdb,TDengine}`

上游：

- https://github.com/VictoriaMetrics/VictoriaMetrics
- https://github.com/prometheus/prometheus
- https://github.com/influxdata/influxdb（1.8 分支）
- https://github.com/GreptimeTeam/greptimedb
- https://github.com/cnosdb/cnosdb
- https://github.com/taosdata/TDengine

刷新方式：`git -C <repo> fetch --depth 1 && git -C <repo> reset --hard origin/HEAD`（浅克隆下 revision 会漂移，引用 file:line 前先确认）。

## 10. 局限

- 全部结论来自静态阅读，未编译/未运行被测仓库，也未做运行时性能验证；
- 六家均为集群或高基数场景设计，其"单机部分"才与本调研可比，容量参数不可直接移植；
- tsink 侧数字来自单一生产实例（~10 点/s、30 series、保留期长），高吞吐（r8 46K 点/s）场景需另测。
