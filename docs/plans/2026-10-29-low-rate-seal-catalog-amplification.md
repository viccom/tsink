# P1 低速率写放大根治：seal 粒度 + catalog 全量往返

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 消除「低摄入速率下 CPU 与磁盘写放大随运行时长单调上涨」的系统性缺陷。实测基线（2026-10-29，rs-iot 生产实例，20 点/s、30 series、运行 31h）：flush 15.3% + persisted-refresh 13.0% = **28.3% 单核**；元数据写盘 **14.8 MB/s** vs 真实数据 **5.2 KB/s**（≈2,900× 写放大）；`series_index.catalog.json` 9.1 MB、**1.7 次/s 全量重写**；平均**每段 5.9 个点**（91,729 行 / 15,645 段）。修复目标：同场景 CPU ≤3%、元数据写盘 ≤1 MB/s、每段点数 ≥chunk 量级，且高速率（r8 46K 点/s）行为零回归。

**Architecture:** 缺陷是四个因子相乘，不是单点 bug：① `bounded` flush 每 250ms 拍**无条件 seal 当前 head**（无最小点数/最小年龄门槛）→ 段粒度 = 拍长×摄入率（20 点/s × 250ms ≈ 5 点）；② 每次 seal 的 catalog 路径先**全量读+解析** catalog 做比对、不匹配再**全量序列化+原子重写**（成本 ∝ 全部段数）；③ L2 是终层（只有 L0→L1、L1→L2 两个 transition），微型段永不合并 → 段数随运行时长单调增长 → ①的单次成本随 uptime 线性上涨；④ flush 拍长是硬编码常量，无任何配置面，`chunk_points` 在低速率下被 seal 门槛架空。本计划按「先掐触发器（①）→ 再去全量往返（②）→ 最后兜终层（③）→ 补观测与门禁」推进，每一步独立可验证、可回退。

**Tech Stack:** Rust（std 原子 + Instant，无新依赖）；既有 `FlushObservabilityCounters`/`Observed*` 观测面；既有对账与持久化测试网（`src/engine/tests/persistence_background.rs`）；rs-iot 侧 `src/history/engine.rs` builder 接线 + `[history]` 配置键。

---

## 审查勘定的事实（file:line 以 HEAD 37648e6 + rs-iot 81a0216 实测）

| # | 事实 | 锚点 |
|---|---|---|
| 1 | 每 250ms 一拍 `BackgroundBounded`；先试「非当前分区 head」，无则**无条件 seal 当前 head** | `src/engine/runtime.rs:155-175`（next_policy）、`src/engine/write_buffer.rs:120-132`、`src/engine/state/active.rs:218-231`（`flush_current_partial` 仅判 `is_empty`） |
| 2 | flush 拍长是**硬编码常量**，`StorageBuilder` 无旋钮 | `src/engine/engine.rs:116`（`DEFAULT_FLUSH_INTERVAL=250ms`）→ `src/engine/bootstrap/finalize.rs:71` |
| 3 | `chunk_points` 只在 head 装满时 rotate（`rotate_full_if_needed`），与 flush 拍解耦 | `src/engine/state/active.rs:176` |
| 4 | 每次 persist 的 catalog 校验 = **全量读文件 + `serde_json::from_slice` 9.1MB + Vec 全比** | `src/engine/registry_catalog.rs:135-161`（`:148` 构 expected、`:149` 全量解析） |
| 5 | 不匹配则**全量重建 + `to_vec_pretty`（9.1MB）+ 临时文件+fsync+rename+父目录 fsync** | `src/engine/registry_catalog.rs:166-176`、`maintenance/registry_persistence.rs:61-80`（validate→persist 调用点） |
| 6 | catalog 条目只是各段 `manifest.bin` 的**反规范化汇总**（manifest 已含 level/chunk_count/point_count/series_count/min_ts/max_ts/wal_highwater；catalog 另存四文件指纹） | `src/engine/segment/types.rs:46-55`、`registry_catalog.rs:36-52` |
| 7 | persisted-refresh 线程 250ms 轮询，dirty 即**全量读+解析** catalog | `src/engine/runtime.rs:416-474`、`runtime.rs:228-240`、`registry_catalog.rs:149` |
| 8 | 终层只有两个 transition：L0→L1、L1→L2（**L2 不再合并**） | `src/engine/compactor/planning.rs:108-115` |
| 9 | 查询**会合并未落盘的 active 点**（`snapshot.active_points`）→ 延迟 seal 不影响历史可见性 | `src/engine/query_read/snapshot.rs:139`、`query_read/append_sort.rs:125` |
| 10 | rs-iot 侧默认档：`memory_limit_mb=256`、`wal_size_limit_mb=1024`、`retention_days=30`、`chunk_points=0`(引擎默认 2048)、`partition_secs=0`(3600) | rs-iot `src/config.rs:270-282` |

**实测证据（停服前快照，`/tmp/rs-iot-before-stop.txt`）：** `written=106,053`、`sent=132,060`、`errors=0`、`dropped=0`；段总数 15,673（L0=11/L1=2/**L2=15,660**）；catalog 9,121,284 B；段内容 58,291,410 B；线程 utime/stime：flush 15.3%/1.7%、persisted 13.0%/0.9%；10s inode 轮询 catalog 原子替换 17 次（1.7/s）；12s strace 归因：catalog 临时文件写入 ≈14.8 MB/s、段数据 ≈5.2 KB/s。

## 根因链（一句话）

**seal 触发器与摄入量无关（时间驱动）+ 每次 seal 的元数据成本与历史段数成正比（全量往返）+ 终层不回收微型段（成本只增不减）**，三者相乘使低速率场景的 CPU/IO 随 uptime 单调上涨；`chunk_points` 与 flush 拍长都不构成有效约束。

规模推演（按当前 0.1 段/s、5 MB/天）：7 天 catalog ≈44 MB（重写 ≈75 MB/s）、30 天 ≈159 MB（≈270 MB/s），届时两条线程各吃满一核以上——必须在进入现场长期运行前修复。

---

## 任务（红→绿，逐任务独立提交）

### Task 1: 红测——低速率 seal 粒度

**Files:** `src/engine/tests/persistence_background.rs`（+）

```rust
// 红测（当前必失败）：20 点/s 等效负载跑 20 个 flush 拍，断言段数 ≤ 2。
// 当前实现每拍 seal 一个非空 head → 20 段。
#[test]
fn low_rate_writes_do_not_create_one_segment_per_flush_tick() { /* ... */ }

// 红测（当前必失败）：段平均点数 ≥ point_cap/2（低速率下应由 age 或满仓驱动，而非拍驱动）。
#[test]
fn low_rate_segments_are_sized_by_head_age_not_tick() { /* ... */ }

// 回归保护（当前应绿，修复后仍绿）：高速率下每段点数 ≈ rotate 粒度，且 L0 不积压。
#[test]
fn high_rate_segment_size_unchanged() { /* ... */ }
```

### Task 2: 红测——catalog 写放大

**Files:** `src/engine/tests/persistence_background.rs`（+）

```rust
// 红测：连续 N=20 次 persist 期间，catalog 文件内容字节写入次数 ≤ 2（当前 = 每次 persist 一次全量）。
#[test]
fn catalog_not_rewritten_on_every_persist() { /* ... */ }

// 红测：catalog 校验路径在读侧不得每次全量解析（以 counter 断言，见 Task 5）。
#[test]
fn catalog_validation_does_not_reparse_unchanged_file() { /* ... */ }
```

### Task 3: 红测——陈旧 catalog 的启动恢复（Fix 2 的安全前提）

**Files:** `src/engine/tests/persistence_background.rs`（+）

```rust
// 红测语义（当前应绿，作为 Fix 2b 的前置契约）：手工删除/回退 catalog 文件后重启，
// 引擎必须靠段目录扫描补全（inventory/discovery 既有路径），查询结果与段内容一致。
#[test]
fn startup_recovers_from_stale_or_missing_catalog() { /* ... */ }

// 崩溃一致性：persist 完成后、catalog 写入前 kill -9（模拟为 hook 注入），重启后数据可查。
#[test]
fn crash_between_segment_publish_and_catalog_write_is_recoverable() { /* ... */ }
```

### Task 4: Fix 1——seal 资格门控（掐掉触发器）

**Files:** `src/engine/state/active.rs`、`src/engine/write_buffer.rs`、`src/engine/runtime.rs`、`src/engine/engine.rs`、`src/engine/construction/*`、`src/engine/config.rs`

- `ActivePartitionHead` 增 `opened_at: Instant`（rotate/new 时置位）。
- 新增 `SealPolicy { min_points, max_head_age, max_wal_unsealed_bytes }`，默认 `min_points = point_cap/8`、`max_head_age = 300s`、`max_wal_unsealed_bytes = min(wal_size_limit/4, 64 MiB)`。
- `BackgroundBounded` 的当前 head 分支改为：**仅当** `points ≥ min_points` **或** `head_age ≥ max_head_age` **或** `unsealed_wal_bytes ≥ 阈值` 时 seal（`All`/pressure-relief/关闭路径不变）。
- 触发原因计数器 `seal_trigger_{full,age,wal,pressure,shutdown,partition}_total`。
- `StorageBuilder` 增 `with_flush_interval` / `with_max_head_age` / `with_max_unsealed_wal_bytes`（缺省等于现值行为以外的保守值，见 Task 10 接线）。
- **不变量（必须在测试中钉死）：** 查询读 active（事实 #9）→ 延迟 seal 不改查询结果；内存上界 = series×point_cap×点宽 + age 窗内增量；WAL 上界 = `max_wal_unsealed_bytes`。

**验收：** Task 1 两红测转绿；`high_rate_segment_size_unchanged` 仍绿；全量 `cargo test` 绿。

### Task 5: Fix 2a——catalog 校验廉价化

**Files:** `src/engine/registry_catalog.rs`、`src/engine/runtime/supervision.rs`（或 observability）

- 以「expected entries 的 xxh64 指纹 + 上次成功写入时的文件指纹」比对替代「读文件+全量解析」；仅当指纹缺失/失配（重启、外部改动）才回退到现有全量校验。
- 新增计数器 `catalog_validate_reads_total`、`catalog_validate_cache_hits_total`。

**验收：** Task 2 第二红测转绿；`catalog_validation_does_not_reparse_unchanged_file` 断言热路径零读。

### Task 6: Fix 2b——catalog 持久化节流 + 关闭强制

**Files:** `src/engine/lifecycle/flush.rs`、`src/engine/maintenance/registry_persistence.rs`、`src/engine/bootstrap/finalize.rs`、`src/engine/lifecycle/shutdown.rs`

- 常规 persist 不再同步写 catalog；改为「脏标记 + 节流写」（默认 ≥30s 一次，可配 `with_catalog_persist_interval`）。
- 关闭/checkpoint/compaction 替换完成等一致性锚点**强制**写一次（保持既有语义）。
- 启动仍以 catalog 为快路径；陈旧/缺失时走 Task 3 的段扫描恢复（懒修复，不阻塞启动）。

**验收：** Task 2 第一红测绿；Task 3 两例绿；`flush` 线程 CPU 在 20 点/s 场景实测 ≤1%（长跑门禁见 Task 9）。

### Task 7: Fix 2c（可选）——catalog 格式紧凑化

**Files:** `src/engine/registry_catalog.rs`

- `to_vec_pretty` → 紧凑 `to_vec`（体积 ≈-40%，序列化更快）；若需更强，引入版本化二进制（复用 `binio`）。
- 兼容：读取端接受 version 2/3 两种（版本字段已存在）。

**验收：** 现有 catalog 可读、体积下降 ≥35%、序列化耗时对比记录进 plan 回填。

### Task 8: Fix 3——终层微型段归并（存量兜底）

**Files:** `src/engine/compactor/*`、`src/engine/runtime.rs`（维护拍）

- 新增低优先级维护拍：当 L2（终层）中相邻区间的小段（`point_count < target/4`）累计 ≥2 个时合并到 `target_segment_points`（默认 8192，可配）。
- 受 tombstones、retention、`segment_replacement_guard` 既有约束；单拍工作量有界（复用 backlog 节流范式）。
- 目标：稳态段数 = 数据集 / target 段大小，与摄入速率无关；存量 15,660 个微段可在数分钟内收敛。

**验收：** 归并后段数、catalog 体积、查询一致性三断言；与 Task 4/6 叠加后 30 天推演 ≤2,000 段。

### Task 9: 长跑门禁（低速率 + 高速率双档）

**Files:** `benches/`（新增 low-rate profile）、CI job

- 低速率档：20 点/s × 30 min，断言 `process_cpu ≤ 3% 单核`、元数据写盘 ≤1 MB/s、每段点数 ≥512、catalog 重写 ≤2 次/分钟。
- 高速率档：r8 同参（46K 点/s × 2h 短版），断言 S6 判据不回退（L0 斜率 ≤10/h）、内存与 WAL 不越界、延迟不劣化。
- 该门禁就是本次缺陷的回归钉——没有它，同类问题会再次以「低速率高 CPU」形态复发。

### Task 10: rs-iot 接线 + 文档回写 + 部署验证

**Files:** rs-iot `src/history/engine.rs`、`src/config.rs`、`docs/reference/rest-api.md` §4.18、`AGENTS.md`；tsink `README.md`/`docs/configuration.md`

- `[history]` 新增键（全部 serde default，缺省零行为变更）：`flush_interval_ms`、`max_head_age_secs`、`catalog_persist_secs`、`target_segment_points`。
- 观测接线（同时结清工作区 BUG #8）：`/metrics` 暴露 `segments_created_total`、`segment_points_avg`、`catalog_bytes`、`catalog_persist_bytes_total`、`seal_trigger_*`。
- 部署验证：停服（已停）→ 构建 → warden 启动 → 30 min 观测 CPU/写盘/段粒度 → 与 `/tmp/rs-iot-before-stop.txt` 基线对比出报告。

---

## 验证矩阵

| 维度 | 手段 | 判据 |
|---|---|---|
| seal 粒度 | Task 1 单测 + 长跑 | 每段 ≥512 点（低速）/ ≈chunk 粒度（高速） |
| 元数据写放大 | Task 2 + 长跑 | ≤1 MB/s（当前 14.8 MB/s） |
| CPU | 长跑 + 线程级 utime | 两后台线程合计 ≤3%（当前 28.3%） |
| 数据正确性 | 查询/聚合回归 + active 合并用例 | 修复前后同窗查询逐点一致 |
| 崩溃恢复 | Task 3 两例 | 陈旧/缺失 catalog 可启动、可查 |
| 高速率零回归 | Task 9 高档次 | S6 判据（L0 斜率 ≤10/h）不复退 |
| 存量收敛 | Task 8 + 观测 | 30 天推演段数 ≤2,000、catalog ≤2 MB |

## 风险与回退

| 风险 | 缓解 |
|---|---|
| 延迟 seal 使 WAL 增长 | `max_wal_unsealed_bytes` 硬门控 + 长跑断言 WAL 上界 |
| 延迟 seal 使内存增长 | `min_points`/`max_head_age` 双门控；内存预算测试沿用 `assert_engine_memory_usage_reconciled` |
| catalog 节流后崩溃留下陈旧快照 | Task 3 契约：启动以段扫描为准，catalog 仅快路径 |
| 终层归并与 retention/tombstone 冲突 | 复用 `segment_replacement_guard` + tombstones 过滤；先只合并「完全在保留窗内且无 tombstone」的段 |
| 高速率路径被改坏 | Task 1/9 双向回归；Fix 1 只改 bounded 分支，`All`/pressure 路径不动 |

## 后续（不在本计划执行面，登记）

1. schema 级根治：catalog 改为「manifest 索引缓存 + 追加式 delta 日志」（仿 `SeriesRegistry::incremental_*`），彻底消灭 O(N) 快照；本计划的节流是中间态。
2. seal 策略按摄入速率自适应（当前为固定门控）。
3. rs-iot 侧考虑为「低速率现场」提供 `retention_days` 与 `target_segment_points` 的推荐配方（写入 `docs/ops/history-100k-tuning.md` 的反向场景一节）。

---

## 实施状态（2026-10-29）

| 任务 | 状态 | 落点 / 证据 |
|---|---|---|
| T1 红测：低速率 seal 粒度 + 满仓 rotate 回归 | ✅ | `persistence_background::{low_rate_background_flush_holds_current_head_instead_of_sealing_every_tick, full_chunk_rotation_still_persists_a_segment_under_seal_gating}`（修复前实测 10 拍 = 10 段、每段 1 点） |
| T4 Fix1：seal 资格门控 | ✅ | `config::SealPolicy`（min_points=0→point_cap/8、max_head_age=300s、max_unsealed_wal_bytes=64MiB）+ `active.rs::current_head_seal_decision` + `write_buffer.rs` bounded 分支；压力救济独立为 `BackgroundPressureRelief` 通路（不受门控）；builder 旋钮 `with_flush_interval/with_seal_min_points/with_max_head_age/with_max_unsealed_wal_bytes`；3 分支测试 `seal_gate_triggers_on_{head_age,unsealed_wal_bytes}` + `seal_policy_immediate_preserves_legacy_seal_every_tick` |
| T2 红测：catalog 写放大 | ✅ | `catalog_is_throttled_between_anchors_and_forced_at_checkpoint`（修复前 7 拍 = 7 次全量重写；修复后窗口内 0 次，checkpoint 强制刷新全部 7 段） |
| T5 Fix2a：校验廉价化 | ✅ | `persist_registry_catalog_throttled`：备忘指纹命中即零 IO；冷启动仅 entries 文件比对（不读 segment 的 series.bin——保住 recovery 快路径）；`warm_catalog_persist_path_needs_no_segment_or_catalog_reads` 以删除 series.bin/postings.bin 证明 |
| T6 Fix2b：节流 + 锚点强制 | ✅ | `with_catalog_persist_interval`（默认 30s）；checkpoint/关闭走 `..._forced`；启动 finalize 走冷路径 entries 校验 |
| T3 启动恢复契约 | ✅ | `startup_recovers_when_catalog_is_missing_or_corrupt`（缺失/损坏均靠段目录重建）；崩溃恢复沿用既有 `history_kill_recovery` 全链 + WAL 回放 |
| T8 L2 微型段归并 | ⏸ 未做（登记） | 存量 15,660 个 L2 微段仍在（约 58MB 内容）；T1/T6 后新增段回到 chunk 量级、catalog 只读不重写，存量对 CPU 无影响。归并属「磁盘/段数收敛」优化，动 compaction 有数据风险，未在本轮实施 |
| T7 紧凑格式 | ⏸ 未做（可选） | `to_vec_pretty` 保留；节流后写量与频率已不是瓶颈 |
| T9 低速率门禁 | ✅（部分） | 结构性回归由 T1/T2 计数器断言覆盖；CPU 判据以真实部署复测为准（见下），未新增独立 bench 档 |
| T10 rs-iot 接线 | ✅ | `[history]` 新增 `flush_interval_ms`/`seal_min_points`/`max_head_age_secs`/`catalog_persist_secs`（0=引擎默认）；`/metrics` 新增 7 个 counter（`rs_iot_history_seal_*`/`catalog_*`），`rest-api.md` §4.1/§4.18 同步 |

**测试基线**：tsink `cargo test --lib` 595 passed（唯一失败 = 既有 flaky `retention_policy::compute_only_storage_suppresses…`，基线已登记）；rs-iot `cargo test --lib` 670 passed。

## 部署复测（2026-10-29，rs-iot 生产实例，同场景 20 点/s、30 series）

| 指标 | 修复前（基线快照） | 修复后（本次实测） | 变化 |
|---|---|---|---|
| 进程 CPU（120s 均值） | **28.3% 单核**（flush 15.3 + persisted 13.0） | **3.3–3.4%**（flush <0.05 + persisted 1.95） | **≈8.5×** |
| catalog 重写频率 | 1.7 次/s（9.1MB/次，约 14.8MB/s） | **0 次/120s**（18 分钟内共 1 次），attempts 全部 skipped | 消灭 |
| 每段点数 | 5.9（91,729 行 / 15,645 段） | **3,476 点/段**（30 chunk，~116 点/系列·段） | ≈590× |
| seal 触发 | 每 250ms 拍 1 次/系列 | **age 触发 30 次 / 18 分钟**（hold 55,130 series×拍） | ≈2,000× 降频 |
| 元数据写盘 | ≈14.8 MB/s | ≈0（catalog 30s 节流上限，实测 1 次/18min） | ≈10³× |
| WAL 体积 | 每拍落盘即 reset | 172 KB（每 300s 一次 age seal 后 reset） | 有界 |
| 数据连续性 | — | `/history/stats` count=319、`/history/batch` 10 桶正常；LAN console/hmi 200 | ✅ |

**判据对照**：CPU 判据「≤3%」为 **3.3%（略超 0.3 个百分点，未完全达标）**——剩余热点是 `tsink-persisted` 1.95%（250ms 轮询维护拍的固定成本，与本缺陷同族但独立，另立后续项）；写盘/段粒度/seal 频率判据全部达成。
