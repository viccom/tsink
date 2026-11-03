# P2 边缘基础库加固：catalog 增量/缓存有界 + L2 终层收敛 + 崩溃一致性门禁

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.
> **背景**：定位为「轻量级时序库模块 / 可独立运行 / 单机与边缘设备」。2026-10-30 实测（rs-iot，~10 点/s、30 series、9.6h）暴露三项资源/稳定性缺口 + 一项测试缺口。本计划按 TDD（先红后绿）推进，顺序：**先建崩溃门禁（W1）→ 再修缓存上界（W2）→ 再做 catalog 增量（W3）→ 最后动最危险的 L2 收敛（W4）**。W1 是后三项的安全网，不得后置。

**Goal:** 边缘设备上长期无人值守运行的四条硬指标：① 元数据写盘从 ~300MB/h（占总写盘 85%）降到 MB/h 级；② 段数从「随天数增长」（+450/天，存量 15,855）收敛到 O(阈值)；③ 常驻内存不随历史段数增长（Cache 有界 + 段数有界）；④ 崩溃/掉电一致性由可重复的测试证明，并进入 CI 门禁。

**Architecture:** 四项改动互不重叠、可独立回滚：W1 只加测试与脚本；W2 只动 `registry_catalog.rs` 的缓存结构与失效；W3 动 catalog 持久化格式（checkpoint 保留、增量 delta + 指针），**不改**段文件格式与查询路径；W4 动 compaction 选择与层级（新增 L2→L2 transition），**不改**段格式与 marker 格式（空输出语义 2026-10-30 已就绪）。

**Tech Stack:** 既有 crate 依赖即可（parking_lot / xxhash / serde_json / memmap2；`std::process::Child::kill` 即 SIGKILL，无需新增依赖）。不引入 moka/indexmap 等新依赖。

---

## 审查勘定的事实（file:line，以 HEAD 37648e6 + 工作区 P1 批为准）

| # | 事实 | 锚点 |
|---|---|---|
| 1 | catalog 全量重写：`to_vec_pretty` + 原子写；9.2MB / 15,855 段 | `src/engine/registry_catalog.rs:224-231` |
| 2 | catalog 已是「派生缓存」：启动校验失配即回退全量重建 | `src/engine/bootstrap/recovery.rs:74-103`、`registry_catalog.rs:286-313` |
| 3 | 条目构建只读 `manifest.bin`（hash 已存在 manifest 内，不读四个数据文件） | `src/engine/segment/loader.rs:291-299`、`registry_catalog.rs:359-408` |
| 4 | `CatalogEntryCache` 两个 HashMap 只增不删，无任何淘汰 | `src/engine/registry_catalog.rs:72-126`（`remove/retain/clear` 零命中） |
| 5 | series registry 已有「增量目录 + 原子写 + checkpoint 清理」先例，但写的是**全量快照**（体积小可行，catalog 不可照搬） | `src/engine/series/persistence.rs:20-104,344-425`；清理见 `maintenance/registry_persistence/context.rs:138-142` |
| 6 | 段清单排序键 `(lane, level, segment_id)`，口径明确可做增量 upsert | `registry_catalog.rs:355` |
| 7 | 只有 L0→L1 / L1→L2 两个 transition；L2 为终层 | `src/engine/compactor/planning.rs:5-9`、`compactor.rs:65-67` |
| 8 | 空输出替换已支持（纯删除 marker），L2 收敛可直接复用 | `compactor/execution.rs:58-70,105-112,431-470` |
| 9 | 段替换 marker 启动幂等重放 | `compactor/execution.rs:213-218` |
| 10 | compaction 删源段先于内存索引刷新（窗口期 mmap 重映射 NotFound） | `compactor/execution.rs:143-148` vs `maintenance/post_flush.rs:154-198` |
| 11 | 无 kill -9 类测试；durability 契约已有 API 但无证明 | `tests/`（11 个文件，无 crash 类）、`src/wal.rs:7-18`、`storage.rs:264-300` |
| 12 | 写 ack 语义可观测：`WriteAcknowledgement::{Volatile,Appended,Durable}`；引擎覆写 `insert_rows_with_result` 返回真实 ack | `storage.rs:264-300,631-648`、`wal.rs:7-27`、`engine/engine.rs:403` |
| 13 | catalog 的 `series_fingerprint` 驱动「启动不读 series.bin」快路径与 registry 快照对账；指纹失配 → 重建+全读 | `bootstrap/recovery.rs:140-180`（对账）、`:188-196`（快路径） |
| 14 | 管线已知 root 变更的 sources 构造入口已存在（增量 delta 可直接取用） | `maintenance/registry_persistence.rs:35-59` |

## 实施状态

| 任务 | 状态 | 备注 |
|---|---|---|
| W1-T1 | ✅ | harness 骨架 + 子进程协议 5 轮绿（连跑 3 次稳定）；**偏离**：计划锚点 `flush()` 失效——`Storage` trait 无 flush（storage.rs:627-1074），门禁改用 `close()`（完整持久化排空）+ 二次重开，T2+ 沿用 |
| W1-T2 | ✅ | 四杀点落地（append/flush/compact/recover × 5 seed）；**发现真实缺陷 D1**（不在计划预计三候选内）：干净 close/reopen 周期后中途被杀 → WAL 帧号回卷碰撞 published marker → 重开硬失败 `WAL publish boundary frame N is missing`（flush 2/5、compact 1/5 确定性复现；append/recover 绿）→ 已登记 **W1-T2fix**；`.tmp-seg-*` 残留现场捕获 1 例（按计划不断言只登记）；目录扫描口径为 lane 感知 `<data>/lane_*/segments/L*/`（简报 `<data>/segments/` 不准） |
| W1-T2fix | ✅ | D1 修复（segments.rs +21 行，TDD 两红→绿）：open 时同段持久化 marker 托底 `next_seq`（max 文件扫描值, marker.frame+1）+ 三水位 `advance_highwater_floor`（max-only 语义已核）；跨段不托底（帧号每段重计，老段边界由 `discard_unpublished_suffixes` 管）；`published_prefix_len` 真损坏检测原样保留。回归测试 `wal_reopen_after_reset_*` 2 例钉住可用性+静默丢失两态；crash test 4 模式×5 seed 连跑 2 次绿；lib 599 绿+唯一基线红；append 热路径零改动 |
| W1-T3 | ✅ | 门禁脚本 `scripts/check_crash_consistency.sh`（ITER=20/REPEAT/RUST_LOG 可覆盖，80 轮 6s ≤120s）+ ITER 参数化（缺省 5，非法值 panic）+ storage-engine.md「Crash-Consistency Testing」小节。验收：脚本 3 次连跑绿 + 注入变红有效——**能力边界实证**：字面「跳 fsync」注入门禁仍绿（SIGKILL 不丢 page cache，fsync 防掉电不防进程死），调整为跳 BufWriter flush（字节不出用户态）后门禁正确红（append r0 DataCorruption + 失败现场目录聚合 + 退出码 1），已还原 |
| W1-T4 | ⏳ | 计划标注可选，默认不做 |
| W2-T1 | ✅ | 三红测落地（registry_catalog.rs 底部 `#[cfg(test)] mod`，stub 全 cfg(test) 门控零生产行为改动）：编译红→断言红两段留证；lib 599 绿 + 4 红（3 新 + 1 基线）。契约钉死：capacity=4 插 6 → 终态 len=4 淘汰最旧 2；invalidate 双 map 齐删；series 字节预算超限淘汰。**约定**：内部构造 `with_capacity(entry, series_bytes)` 语义 0=无界（Default 结构性无界）；StorageBuilder 旋钮 0=不缓存仍按计划语义（T2 区分映射）。T2 建议补「命中刷新 touch 序」用例 |
| W2-T2 | ✅ | LRU 落地：`CatalogLruMap<V>` 原语（touch 序命中刷新 + 超限批量淘汰至 3/4 水位 + 字节预算，保底 1 条防 N9 重读回归）+ `invalidate` 接在 `remove_persisted_segment_roots` 索引应用后（唯一删除汇聚点，调用图已核）+ 双旋钮（默认 32768 / 32MiB，serde default；旋钮 0=不缓存 vs 内部 0=无界两层语义显式区分）+ 快照 `catalog_cache_evictions_total/entries`（serde default 兼容）。复用 `core_impl/utilities.rs` 的 `saturating_sub_fetch`。4 用例绿（含新增 touch 序）+ N9 45 绿 + lib 603 绿+唯一基线红 + crash 门禁 80 轮绿。**观测项**：crash test 首跑一闪失败 1 次（subagent 报告），此后其 14 连跑 + 160 轮门禁 + 本会话 80 轮门禁 + 2 连跑全绿未复现，W2-T3 复测关注 |
| W2-T3 | ✅ | soak 测试 + 驱动脚本 + performance-tuning.md「Catalog caches」。**2h 判据 PASS（2026-11-03 收）**：斜率 0.447MB/h ≤ 0.5、entries 18/32768、evictions 0、25 样本完整（RSS 7840→11192KB）；冒烟 600s 过（compaction 失效清槽实证）；lib/fmt/clippy 净 |
| W2 边界 | ✅ | `cargo test -p tsink --no-fail-fast` 全 target：lib 603 绿+1 基线红+soak ignored，11 个集成文件全绿；fmt/clippy 0 告警 |
| W3-T1 | ✅ | 三红测落地（断言红非超时红）：catalog 全量重写被红测实锤（mtime/len 变化）+ delta 目录缺失 + 快路径计数器自证有效（删 catalog→reads>0，有效 catalog→reads==0）。**发现例行 persist 语义**：节流跳过不重试、重试只在下一次发布拍（测试用 `settle_past_catalog_persist_throttle` 错峰）——W3-T4 实装注意。series.bin 计数器 = loader.rs `load_segment_dir_indexed` 分支 cfg(test) thread-local（启动唯一读路径）。remove 制造：3 个 L0 基线段 + 1000ms compaction 拍（50ms 会有 delta 竞态；close 路径 `compact_until_settled` 会吸收 remove 进强制 checkpoint 故不可用）。delta 数量断言（恰 1/恰 2）若 T4 做按拍合并需微调 |
| W3-T2 | ⏳ | 未开始 |
| W3-T2 | ✅ | 两红测落地（编译红→stub→断言红）：`catalog_checkpoint_consolidates_deltas`（阈值 3 触发合并语义：`_checkpoint.json` 的 checkpoint_seq==最后 delta seq、旧 delta 删、seq 锚点跨 reopen 存活）+ `catalog_forced_checkpoint_writes_full_file`（close 强制全量不回退）。StorageBuilder 旋钮 stub 三件套（默认常量 100/1MiB，存参不消费，`#[allow(dead_code)]` 待 T4 接线时删）。compaction 干扰隔离：delta 生产期 compaction interval=60s、段数压 3（close 期 L0 阈值 4 不吸收）。字节阈值支路只断 marker+清理+段数对账（不锁 seq，宽容两种实现序） |
| W3-T3 | ✅ | 三红测落地（缺失/截断/ stale 锚点幂等）。崩溃窗手法：「实时制造 + 就地捕获字节 + close 后回卷重放」（close 必然强制合并清 delta、Drop 触发完整 close、mem::forget 被 flock 挡——三路都堵死，回卷是唯一出路；对 T4 delta 字节格式零耦合，marker JSON 形状由计划钉死）。绿态判据已钉：回退必须删残留 delta、stale 锚点重放后 catalog 主文件字节不变（`SkippedValidatedFromFile` 原地验证语义；T4 若要顺带修 marker 需主动放宽）。`catalog_delta_apply_fallback_total` 计数接线属 T4 |
| W3-T4 | ⏳ | 未开始 |
| W3-T4 | ✅ | 8 红全绿（断言零改动，13/13 稳定）。核心：`{seq:020}.delta.json`（version/seq/added/removed/series_fingerprint，removed 口径同 compaction marker）+ `_checkpoint.json` 锚点 + 启动重构比对（验证强度不降：seq 连续性/版本/内容三重校验，失配即回退重建+清 delta）。指纹取最后已应用 delta，缺失关快路径并计数。强制 checkpoint 重置判据：主文件从未承载段条目→整体重置增量状态，否则常规合并（marker 跨 close/reopen 存活）。**计划偏差两处（已裁决接受）**：① delta 追加不做节流（节流本为约束 O(段数) 全量重建；追加 O(变更)，且节流跳过无重试拍会饿死变更——W3-T1 实证）；② 主文件缺失首拍回退全量写（delta 须锚定已存在 checkpoint）。**fsync 抖动观测**：8 线程并行下 8 测偶发 10s 轮询超时（1-2/10 轮），gdb+外部探针定位=ext4 journal 多实例 fsync 排队放大（非本实现缺陷，无数据不一致）；单线程/threads≤4 确定性稳。lib 611 绿+基线红（threads=4 与默认各验一次）+ N9 45 绿 + crash 门禁 80 轮绿 + fmt/clippy 净 |
| W3-T5 | ✅ 已部署 / ⏳ 24h 判据待收 | 2026-11-03 部署：rs-iot release 重编（单 `-p rs-iot`，tsink path 依赖带入 027b81b）→ 回滚备份 `target/release/rs-iot.pre-w3.bak-20261103` → warden `dev-rs-iot` 重启（新 pid 172029，旧 1040）→ 健康核验过（warden health ok/4 服务 0 失败、rs-iot 登录 OK）；field 栈未动（1041 持旧 inode，现场模拟不跟开发迭代）。监测：dev-history-monitor 已启动（60s×24h，CSV `rs-iot/logs/history_monitor.csv`）。**判据生效前置**：dev 栈数据流在部署前 ~36h 已停止（catalog mtime 11-01 21:34，非本次部署所致）——delta 目录将在恢复数据流后的首个发布拍出现；恢复手段（仓内 `scripts/meter_*` lua 实例 + `mock_modbus_tcp_2002.py` 或任意写路径）超出本次部署授权未执行。`proc_write_bytes` 采样列未加（需改 rs-iot 仓脚本，超授权）——手工观察命令：`cat /proc/<dev-rs-iot pid>/io`（write_bytes 差分/h）。文档部分已提交（027b81b） |
| W3 边界 | ✅ | 全 target：lib 611 绿+基线红+soak ignored，13 个集成 target 全绿；fmt/clippy 0 告警 |
| W4-T0 | ✅ | mmap 重映射窗口容错落地：`SegmentMapRemapResolver` 注入（`construction.rs` 构造后注入，回调按 persisted index 反查 slot→root→chunks 路径）+ remap NotFound 分支计数 `persisted_remap_not_found_total` + **恰一次重试**（重试失败抛原错误）。锁序：回调在 registry 锁外执行且只用 `try_read`（parking_lot 写者优先下阻塞式二次读会与排队写者互等死锁——try_read 失败退化为旧行为，正确性优先）。`persisted_index` Arc 化（读写语义不变）。红两段留证；residency 4 绿；lib 612 绿+基线红；crash 门禁 80 轮绿。**观测**：本任务后 1 次 lib 全量跑出现瞬态 2 failed（未捕获名字，随后 3 连跑 612+1 干净——与 W3-T4 登记的 fsync 并行抖动一致） |
| W4-T1 | ⏳ | 未开始 |
| W4-T1 | ✅ | 三红测落地（编译红→stub→断言红）：40 段 fixture（各 100 点、ts=i*100 全局唯一、值=ts 逐点可校验）+ 终止判据（连续 3 轮无进展判静默，上限 200 pass）+ 测 3 红点钉「收敛必须发生」（level==2 对未动输入恒真）。测 2 旋钮 stub 落 **Compactor 侧**（`DEFAULT_L2_TRIGGER=32` + `l2_trigger` 字段 + `with_l2_compaction_trigger`，对齐 l0/l1_trigger 惯例；测试直构 standalone Compactor 不经 StorageBuilder，StorageBuilder 旋钮 T4 随 transition 引入）。**独立证实既有 flaky**：`persistence_recovery::catalog_*` 三测在基线 stash 下 6 轮 2 红且名字不固定（8 线程并发 poll 10s 超时，红跑 ~10.5s vs 绿 ~1.4s）——与 W3-T4/W4-T0 两处观察同因，处理前 lib 全量在 N±1 failed 间波动 |
| W4-T2 | ⏳ | 未开始 |
| W4-T2 | ✅ | 两测落地：`l2_compaction_skips_large_segments`（断言红：6×50 点候选须合并消失 + 大段 mtime+内容指纹逐字节不变；budget 注入复用生产旋钮 `with_output_segment_point_budget(800)`→max_input=100，**大/小段取 200/50 点**与边界 100 两侧留距离——对 T4 的 ≤/< 语义不敏感；大段与小段 id 交错排列使「T4 不做过滤」缺陷在指纹断言必然暴露）+ `l2_compaction_terminates_when_only_large_segments_remain`（基线表征绿，判别力 T4 后生效：候选过滤缺失→队首窗口吞大段→compacted==true 或指纹红） |
| W4-T3 | ✅ | 表征绿（机制 level 无关已存在，与 W1 同款纪律）：`l2_compaction_marker_replay_is_idempotent`（marker 经**生产 writer** 落盘=真实崩溃字节形态；半应用 marker→源删/输出存/marker 消费/400 点可读/二次 finalize 指纹不变）+ 截断 marker 变体（既有语义=Err 且现场四点不动，如实钉死）。**登记既有事实**：损坏 marker 会永久阻塞后续合法 marker 重放（parse 失败先于任何删除动作）——是否改进属后续决策 |
| W4-T4 | ✅ | `L2ToL2` 落地（2026-11-03）：枚举+`LAST_TRANSITION_L2_TO_L2=3`+仲裁改三路「上次的放最后」稳定排序；size-tiered 选择（候选 `point_count ≤ output_segment_point_budget/8`（**raw 字段**，不随 point_cap 抬底）+ 触发「候选 ≥2 且 L2 总段数 > l2_trigger（0=关）」+ 窗口 = `(point_count, min_ts, id)` 升序前缀、受 `source_window_segments` **且累计点数 ≤ max_input** 双上限——累计上限是相对计划的收紧（skips_large 契约要求收敛输出不得成为新大段：6×50=300 点按预算合成一段即 300>100 会被指纹快照计为大段，`large_after.len()` 6≠5 必红），顺带保证尺寸受限配置下输出仍在候选池内、收敛必终止；retention 即删段排除 = Compactor 注入 `retention_cutoff_provider`（resources.rs 由 options 构造：wall-clock now − retention_window，enforced=false 为 None；文档论证方向安全性：engine 参考系 ≥ now → provider 截断 ≤ sweep 截断 → 排除集 ⊆ sweep 即删集，残余白写窗 = observed 与 now 的差，方向安全）；drain 排除 = `drain_compactors_with_budget` 命中 (2,2) 计数 `l2_to_l2_runs` 后 **break** 不续跑不置 backlog（后台每拍每 lane 至多 1 个收敛窗，15.8k/16≈1000 拍按 interval 摊开）；close 排除 = `PassScope::CloseDrain` 新入口 `compact_once_for_close`（`compact_until_settled` 改走 `compact_compactors_for_close_with_changes`），close 对存量收敛零时长贡献（scratch 实证：40 段 L2 下 5 次 close pass 全 idle，background 入口照常收敛）。观测：`CompactionObservabilityCounters/Snapshot.l2_to_l2_runs_total`（serde default）+ record 按 (2,2) 计。旋钮链：`StorageBuilder::with_l2_compaction_trigger`（默认 32，0=关）→ options → resources 双 compactor。T1 stub 转正（删 allow、真实消费）。**新增 2 钉**：`close_drain_defers_l2_terminal_convergence` + `compaction_drain_takes_single_l2_to_l2_window_and_counts_it`（硬约束防回归，lib 计数 617→619）。验证：7 契约绿 + compactor 29 绿 + lib 621 绿+基线红（4 线程，基线红经 stash 双证）+ crash 门禁 80 轮绿 + crash 集成 2 连绿 + fmt/clippy 净 |
| W4-T5 | ✅ harness+文档 / ⏳ 真实 16.5k 收敛大跑后台收（被 D2 两轮修复迭代，见下两行） | `l2_convergence_drill`（crate 内 env 门控 `#[ignore]`：基线 30 series 全窗 count/sum/last 采样 → 后台拍收敛（interval 1s）轮询 CSV 曲线（l2/l1/l0/RSS/runs）→ close/reopen 等值逐项断言 → final_l2≤48 判据；合成 96 段冒烟端到端 PASS：96→36、4 拍、峰值 RSS 22.9MB、等值精确相等）+ compaction.md「Terminal level convergence」小节 + architecture.md 层级图更新（连带修正窗口 8→16 陈旧值）。**已知边界**：>1M 点真大段不参与候选，若存量 >48 个此类段按设计超时 |
| W4-T5fix-1 | ✅ 已提交（539c871 后续 6ce7f09），**未触及真因** | 首轮归因（后被推翻）：「分配器恢复下界落后盘上 max id」——启动 `recover()` 以 inventory 扫描 max dirname 托底 next_segment_id + 单元红绿（`reopen_floors_segment_id_allocator_at_max_persisted_dirname`）。合成场景成立，但真实副本重跑**同 id 同点撞停**（运行时探针：`max_dirname=215145 loaded_next=215146`——启动值本来就一致，托底空转）→ 真因另有所在，见 W4-T5fix-2；托底本身无害保留（防未来恢复滞后） |
| W4-T5fix-2 | ✅ **D2 真因修复**（b2b0292） | **运行时分配器倒写**：`install_visible_persisted_state` 用**扫描时点快照**的 next_segment_id 无保护 store——目录指纹铁证（mtime 定序）：tick1-7 成功发布 3486a-f+34870（atomic 215146→215153），~10s 后 maintenance 的 index-rebuild 安装把 atomic **倒写回 215146** → tick8 撞自己 tick1 的 seg-3486a → 3 次重试（execution.rs `COMPACTION_OUTPUT_PUBLISH_ATTEMPTS=3`）撞满密集簇 → fail-fast。修复：安装改**单调托底只升不降**。TDD 红绿（`install_never_rewinds_the_segment_id_allocator`：101→100 倒带实锤红→绿）。注：诊断期间曾怀疑撕裂副本/恢复滞后，均被运行时探针推翻；首轮「副本撕裂」假象源于在**未停机**的原活目录上 cp -a（现流程=先停 dev-rs-iot 再拷贝） |
| W4-T5 观测 | — | drill harness 已补 tracing_subscriber 初始化（env_logger 捕不到 tracing 事件，诊断基建）；诊断临时探针已清理；失败副本现场 `/tmp/tsink-w4t5-converge/` 保留至 drill 验收完成 |

---

## W1. 崩溃一致性门禁（对应必改第 7 项）

**目标**：随机 SIGKILL 后重开存储，证明「已 ack 的写不丢、无重复点、catalog 可重建、启动不失败」。这是 W2/W3/W4 的安全网，**必须先落地**。

### T1 — 红：harness 骨架 + 子进程协议

- 新文件 `tests/crash_consistency_test.rs`。
- 子进程模式：`#[test] #[ignore] fn crash_child_mode()`，由环境变量 `TSINK_CRASH_CHILD=1` 激活；父测试用 `std::env::current_exe()` + `["--ignored","--exact","crash_child_mode","--nocapture"]` 拉起（无需新增 bin/example）。
- 子进程协议（journal 文件放在**数据目录外的 sibling 目录**，避免与存储自身 IO/清理互相干扰；逐批 fsync）：
  1. 追加 `intent <dev> <ts>`；
  2. 调 `insert_rows_with_result`（`Storage` trait，`storage.rs:640`；引擎覆写返回真实 ack，`engine.rs:403`），断言 `WriteAcknowledgement::Durable`（默认 `WalSyncMode::PerAppend`，`wal.rs:9-10`）；
  3. 追加 `ack <dev> <ts>`。
- 数据集：`dev-{0,1,2}` × `ts = 1_700_000_000 + seq`，值 = seq，便于校验。
- 父测试随机 50–500ms 后 `child.kill()`（Unix=SIGKILL），再用库打开同一目录（读取用 `Storage::select`，`storage.rs:648`）。
- **门禁断言（表征测试：当前实现应全绿；任何红 = 已知缺陷，先登记再修，不允许为转绿降低断言）**：
  - 每个 `ack` 点可读且值正确（`select` 单点校验）；
  - 无重复 `(dev, ts)`；
  - 打开后 `flush()` 成功、再次打开仍成功；
  - `intent` 无 `ack` 的点允许存在或不存在。
- 跑 5 轮（固定 seed 列表），任一断言失败即红。

### T2 — 红：四个杀点覆盖

- `TSINK_CRASH_MODE`：
  - `append`：纯写入风暴（WAL append 中途被杀）；
  - `flush`：`insert` + `flush()` 循环（段写入/rename 中途被杀）；
  - `compact`：预置 ≥6 个小段后循环触发 compaction（marker 半应用 / 源删除中途被杀）；
  - `recover`：数据集预置后打开存储并立刻被杀（启动重放中途被杀），父进程再开一次。
- 断言同 T1，另加：`compact` 模式后 `load_segments` 的段集合完整（无「段目录存在但 manifest 缺失」的引用）。
- 该任务只加测试不修实现；若发现真实缺陷，**新增修复任务并在此表登记**（预计：catalog 幻影 entry、`.tmp-seg-*` 残留、marker 半应用后的重复输出——均在既有审查 P2 清单内）。

### T3 — 门禁脚本 + 参数化

- 新脚本 `scripts/check_crash_consistency.sh`：`ITER=20 RUST_LOG=warn cargo test --test crash_consistency_test`——**注意**：`--ignored --exact crash_child_mode` 是父测试内部拉起子进程的 argv（`current_exe` 的参数），**不得**加在 cargo 调用上（否则只会运行被 ignore 的子模式本身，父测试被跳过、门禁形同虚设）；脚本负责循环与失败聚合，总时长目标 ≤120s。
- 文档：`docs/storage-engine.md` 增「崩溃一致性测试」小节（如何跑、覆盖哪四个杀点、断言清单）。
- **验收**：脚本在干净工作区连跑 3 次全绿；故意注入 `unsafe` 跳过 fsync（临时本地试验，不提交）时脚本必须变红，证明门禁有效。

### T4 — 观测接线（可选，与 W3 合并）

- `/metrics` 已有 `flush` 系列；崩溃测试只断言行为，不加指标。

---

## W2. CatalogEntryCache 有界 + 失效（对应必改第 3 项）

**目标**：常驻内存不随历史段数增长；被 compaction 删除的段立即从缓存移除。

### T1 — 红：淘汰与失效语义

- 在 `src/engine/registry_catalog.rs` 增加 `#[cfg(test)]` 单测（或 `src/engine/tests/persistence_background.rs`）：
  - `catalog_entry_cache_evicts_oldest_beyond_capacity`：容量 4，插入 6 个不同 root → 缓存长度 4，最旧 2 个被淘汰，且**淘汰后再次读取能正确重建**（内容一致）；
  - `catalog_entry_cache_drops_removed_root`：插入 root，调用失效接口 → 立即不在缓存（`len` 下降），后续 `get_or_build` 走磁盘并得到 `None`（目录已删）；
  - `catalog_series_cache_respects_byte_budget`：按估算字节计的上界，超限淘汰最旧，且不 panic。
- **红形态**：当前无淘汰 API → 编译失败即为红（TDD 允许先编译错），随后以断言红收口。

### T2 — 实现：有界 LRU + 失效 + 旋钮 + 指标

- `CatalogEntryCache`：`entries`/`series` 各加 `capacity` 与 `touch_seq: AtomicU64`；`get_or_build`/`get_or_load_series` 命中时更新 touch；插入后若 `len > capacity`，按 touch 升序淘汰至 `capacity * 3/4`（批量扫，避免每插一次 O(n)）。
- 失效接口 `invalidate(root)`；调用点必须落在**持久化索引实际应用 removed_roots 的刷新路径**（`record_changes` → persisted refresh 应用处），而非仅 `persisted_registry_catalog_sources_with_root_changes`（`maintenance/registry_persistence.rs:35-59`）构造 sources 的分支——后者不是唯一的删除入口；启动重建路径（`recovery.rs`）用独立一次性缓存，不受影响。
- `StorageBuilder` 新旋钮：`with_catalog_entry_cache_capacity(usize)`（**默认 32768**）、`with_catalog_series_cache_bytes(usize)`（默认 32MiB）。0 = 不缓存。
  - **容量默认值纪律（复核修订）**：在 W3 落地前，例行 persist 仍每次 `build_catalog_entries` 遍历**全部** sources 走 entry_cache（N9 语义）；若 capacity < 当前段数（生产实测 15,855），每次节流拍会重读 `capacity` 之外的 manifest.bin —— 这正是 N9 修掉的 openat 风暴回归。故默认必须显著大于当前段数；真正抑制无界增长的是 ① removed_roots 的即时失效 ② W4 把段数收敛到 O(阈值) ③ LRU 只是最后防线。W3-T4 落地后例行构建不再遍历全量（见下），该约束随之放松。
- 观测：`rs_iot` 侧不新增 family（tsink 公共快照），内部计数器 `catalog_cache_evictions_total` / `catalog_cache_entries` 进 `FlushObservabilitySnapshot`（serde default，向后兼容）。
- **验收**：T1 三测转绿；`cargo test --lib` 全量不新增红；N9 既有测试（`src/engine/tests/persistence_background.rs` 段级缓存节流用例）保持绿。

### T3 — 回归：长跑内存斜率

- 本地脚本（不入 CI）：以 10 点/s 模拟 2h，每 5min 采样 RSS 与 `catalog_cache_entries`，要求 RSS 斜率 ≤ 0.5MB/h 且缓存条目不超过容量上界。
- 文档：`docs/performance-tuning.md` 增两个旋钮说明与推荐值。

---

## W3. Catalog 增量持久化（对应必改第 1 项）

**目标**：例行 persist 只写「变更条目」的 delta（KB 级），周期性 checkpoint 摊薄全量写；启动仍保留全量校验作为正确性兜底。

### T1 — 红：增量写入与恢复

- `src/engine/tests/persistence_recovery.rs` 新增：
  - `catalog_delta_recovers_without_full_rewrite`：写入 → flush（产生 delta）→ 断言 catalog 文件 **mtime/长度未变**、delta 目录出现 1 个 delta；重开存储 → 对写入 series 的 `Storage::select` 全窗校验逐点一致（tsink 公共读 API 是 `select`，`storage.rs:648`——**没有** `list_metrics`，那是 rs-iot 侧遥测命名）；
  - `catalog_delta_replay_order_matters`：两次 flush 产生两个 delta（一个 add、一个 remove），重开后最终集合正确（验证序号顺序应用）；
  - `catalog_delta_preserves_startup_fast_path`（复核新增）：checkpoint 后存在 delta 的目录重开时，**不得**退回全量读 series.bin——`should_load_segment_series`（`bootstrap/recovery.rs:188-196`）依赖 `series_fingerprint` 存在且正确；delta 若不携带重算指纹，checkpoint 的指纹即陈旧，快路径静默失效。断言：delta 存在时启动仍走快路径（以"未打开任何 series.bin"为可观测判据，可在测试中临时计数 open 或断言启动耗时上界）。
- **红形态**：当前没有 delta 目录 → 断言失败（`std::fs::read_dir` 不存在）。

### T2 — 红：checkpoint 锚点与清理

- `catalog_checkpoint_consolidates_deltas`：delta 数达阈值（`catalog_delta_max_count`，默认 100）或字节达阈值（默认 1MiB）→ 触发全量 checkpoint；断言 checkpoint 包含全部条目、`_checkpoint.json` 的 `checkpoint_seq` 等于最后 delta 序号、旧 delta 被删除、目录不无限增长。
- `catalog_forced_checkpoint_writes_full_file`：`checkpoint_series_registry_index`（关闭/显式 checkpoint）仍强制全量（现有语义不回退）。

### T3 — 红：崩溃窗口降级

- `catalog_delta_missing_falls_back_to_rebuild`：
  - 手工删掉最新 delta（模拟「段已 rename、delta 未写」）→ 重开必须成功，且 `force_registry_checkpoint` 生效、catalog 重建为正确条目；
  - delta 内容截断（半写）→ 重开成功，回退重建，且旧 delta 被清理；
  - `_checkpoint.json` 指向旧序号但 checkpoint 已是新内容（重复应用）→ 重开成功且条目无重复（upsert 幂等）。
- 复用既有 `startup_recovers_when_catalog_is_missing_or_corrupt` 骨架（`persistence_recovery.rs`）。

### T4 — 实现：格式与流程

**文件布局**（与 checkpoint 同目录）：
```
series_index.catalog.json          # checkpoint（现有格式，version=2 不变）
series_index.catalog.d/            # 增量目录
  {seq:020}.delta.json             # {"version":2,"seq":u64,"added":[Entry...],"removed":["L1/seg-xxx",...]}
  _checkpoint.json                 # {"version":2,"checkpoint_seq":u64}
```
- `CatalogPersistMemo` 扩展：`next_seq: u64`、`last_roots: BTreeMap<PathBuf,(lane,entry)>`（用于 checkpoint 重建与漂移对账）、`deltas_since_checkpoint`、`delta_bytes_since_checkpoint`。启动时 `next_seq = max(checkpoint_seq, 现有 delta 最大 seq) + 1`。
- **例行 persist（复核修订：不做全量构建+求差，直接由管线 root 变更构造 delta）**：
  1. 节流窗口判定（不变，`catalog_persist_interval`）；
  2. 从调用方已知的 `(added_roots, removed_roots)`（`persisted_registry_catalog_sources_with_root_changes` 的输入侧；flush/compaction 管线本就知道自己改了哪些 root）构造 delta：`added` 条目只为**新增 root** 经 entry_cache 构建（O(变更)），`removed` 为根路径列表；
  3. 变更为空 → `SkippedUnchanged`（零构建、零 IO）；
  4. 否则原子写 `{next_seq}.delta.json`（tmp+rename+父目录 fsync）→ 更新 memo；
  5. delta 数/字节达阈值 → 全量 checkpoint：**从 sources 全量重建 entries + 与 `last_roots` 对账**（兼做漂移纠正：捕获任何未被管线上报的 root 变化，如外部删目录/上次 delta 写失败），写 catalog → 写 `_checkpoint.json` → 删 `seq ≤ checkpoint_seq` 的 delta。
  - 修订理由：原设计"每次全量 `build_catalog_entries` + 与 last_roots 求差"使 entry_cache 必须容纳全部段（15.8k×manifest 重读风险），且把 O(段数) CPU 留在每拍热路径上——与 W2 的缓存上界目标相互矛盾。改由管线上报变更后，热路径 O(变更)，全量构建退到 checkpoint（默认每 ~100 个 delta / ~3h 一次）。
- **delta 携带 `series_fingerprint`（复核新增，T1 第三测的门）**：每次写 delta 时用 series 缓存重算指纹（O(series) CPU、热缓存下零 IO）写入 delta 尾部；启动重放后取**最后一个 delta 的指纹**（无 delta 则取 checkpoint 的）。缺失/无法重算时按"指纹缺失"处理——快路径关闭（回退读 series.bin，正确性不变），并计数 `catalog_delta_fingerprint_fallback_total`。
- **启动**（`bootstrap/recovery.rs`）：
  1. 读 `_checkpoint.json`（缺失 → `checkpoint_seq=0`，视作无增量模式）；
  2. `validate_registry_catalog` 扩展：`actual` 由「checkpoint 条目 + 依次应用 `seq > checkpoint_seq` 的 delta（upsert/remove，幂等）」重构，再与 expected（段目录全量扫描）比对——**验证强度不降**；指纹取最后一个 delta（或 checkpoint）；
  3. 任一 delta 缺失/损坏/序号断层 → 丢弃全部 delta、走现有「失配即重建 + 强制 checkpoint」路径，并加计数器 `catalog_delta_apply_fallback_total`；
  4. `CatalogPersistOutcome::SkippedValidatedFromFile` 冷路径同步扩展为「checkpoint+delta 重构后比对」，不残留只读 checkpoint 文件的旧语义。
- **顺序纪律**：段文件 rename + 目录 fsync 完成 → delta 追加 + fsync → 才允许返回 persist 成功；delta 失败只标记 catalog 脏（下拍重试/强制 checkpoint），不使 flush 失败（catalog 是派生缓存）。
- 观测（`FlushObservabilitySnapshot`，serde default 兼容）：`catalog_delta_appended_total`、`catalog_delta_bytes_total`、`catalog_checkpoint_total`、`catalog_delta_replayed_total`、`catalog_delta_apply_fallback_total`、`catalog_delta_fingerprint_fallback_total`。
- 版本兼容：`PersistedRegistryCatalogFile.version` 保持 2（checkpoint 形状不变）；delta 与指针各自带 `version`，读取时校验，不符即回退重建。

### T5 — 实测复验

- 部署到 rs-iot（warden `rs-iot`），24h 监测脚本（`scripts/history_monitor.sh`）判据：
  - `catalog` 重写率 ≤ 60 次/h 且**写盘字节**从 ~300MB/h 降到 ≤ 10MB/h（新增采样列 `proc_write_bytes`）；
  - `catalog_bytes` 增长仍 ≈ 数据增长量级；
  - errors 增量 0。
- 文档：`docs/storage-engine.md`「Segment catalog」小节改写为 checkpoint+delta 模型；`docs/configuration.md` 增 `catalog_delta_max_count`/`catalog_delta_max_bytes` 与 `with_catalog_persist_interval` 的关系说明。

---

## W4. L2 终层收敛（对应必改第 2 项）

**目标**：L2 段数有界（默认 ~32），存量 15.8k 段被逐步收敛；不重复重写大段（写放大有界）；与墓碑/查询/崩溃恢复语义不变。

### T0 — 前置（小）：mmap 重映射窗口容错

- 红：`segment_map_remap_after_source_deleted_retries_once`——构造「索引仍指向已删除源段」的槽位，断言 `get_or_remap` 在 NotFound 时**重新解析 slot → 重试一次**，失败才返回错误；新增计数器 `persisted_remap_not_found_total`。
- 实现：`src/engine/state/segment_maps.rs:95-160`，NotFound 分支重新查 persisted index 的 root→slot 映射。
- 理由：W4 会让 L2 删除频率上升，先消除审查 P1 的窗口风险，避免把新功能建在已知缺陷上。

### T1 — 红：段数收敛与数据完整

- `src/engine/compactor/tests.rs`：
  - `l2_compaction_reduces_terminal_segment_count`：写 40 个 L2 小段（各 100 点，时间不重叠）→ `compact_once_with_changes` 反复直到无进展 → 断言段数 ≤ 阈值（32）且**所有时间戳/值全量可读、无重复**；
  - `l2_compaction_applies_tombstones`：部分点墓碑 → 合并后不可见，其余保留；
  - `l2_compaction_output_level_is_l2`：输出段 `manifest.level == 2`。
- **红形态**：L2 段数保持 40（无 L2 transition）。

### T2 — 红：不做无意义重写（写放大上界）

- `l2_compaction_skips_large_segments`：L2 混有「大段」（`point_count > max_input_points`）与小段 → 只选小段；大段 mtime/内容不变；
- `l2_compaction_terminates_when_only_large_segments_remain`：反复 pass 后 `compacted == false`，不得活锁（复用 `all_tombstoned_window_is_deleted_not_livelocked` 的断言形态）。

### T3 — 红：崩溃幂等

- `l2_compaction_marker_replay_is_idempotent`：手工写一个半应用的 L2 替换 marker（输出段已落盘、源未删）→ `finalize_pending_compaction_replacements` 后段集合正确、重复调用无副作用。

### T4 — 实现：`LevelTransition::L2ToL2`

- `planning.rs`：`LevelTransition` 增 `L2ToL2`；仲裁顺序改为 `[L0ToL1, L1ToL2, L2ToL2]` 轮转（沿用「上次的放最后」策略，`LAST_TRANSITION_L2_TO_L2 = 3`）。
- 选择规则（size-tiered，防止重写大段）：
  - 候选 = L2 中 `point_count <= max_input_points`（默认 `output_segment_point_budget / 8` = 1M 点）的段；
  - 触发 = 候选数 ≥ 2 **且** L2 总段数 > `l2_trigger`（默认 32）；
  - 窗口 = 候选按 `(point_count, min_ts)` 升序取最多 `source_window_segments`（16）；
  - 复用 `compact_segments(L2, window, tombstones)`（含空输出纯删除、marker 原子替换、`output_segment_point_budget` 分片）。
- **不进 close drain（复核新增）**：`L2ToL2` 只由后台 compaction 线程驱动；`drain_compactors_with_budget`（`lifecycle/shutdown.rs:96-126`，`stats.compacted=true` 即续跑直到预算耗尽）**排除** L2ToL2——否则 15.8k 存量段收敛会把每次 close 的 `COMPACTION_DRAIN_TIME_BUDGET` 烧满，优雅停机时间不可控。存量收敛由后台在运行期逐步完成（15.8k/16 ≈ 1000 pass，按 compaction_interval 摊开）。
- **与 retention 的相互作用（复核新增）**：候选筛选排除「已达退役 tier/时间窗」的段（retention 下一拍即将删除的段不参与合并，避免白写）；具体判据复用 tiering 的 tier/时间元数据。
- 配置：`StorageBuilder::with_l2_compaction_trigger(usize)`（默认 32，0=关闭→行为与现状一致）。
- 观测：`CompactionRunStats` 已带 level 字段；`lifecycle/shutdown.rs:112-116` 的 drain 计数加 `l2_to_l2_runs`；`StorageObservabilityCounters` 加对应累计量。
- **验收**：T1–T3 全绿；`cargo test --lib` 全量（除已知基线红 `retention_policy::compute_only_storage_suppresses*`）通过；W1 harness 在 `compact` 模式下连跑 20 轮绿。

### T5 — 存量收敛实测

- 在 rs-iot 数据目录（15,855 个 L2 段）上以副本目录跑一次启动 + drain：记录段数下降曲线、单次合并峰值内存、总耗时；要求段数降到 ≤ 阈值 + 一个窗口（≤ 48），查询结果与收敛前逐点一致（抽样 30 个 series × 全窗比对 count/sum/last）。
- 文档：`docs/compaction.md` 增「terminal level convergence」小节；`docs/architecture.md` 的层级图更新。

---

## 验证矩阵

| 层 | 命令 | 通过标准 |
|---|---|---|
| 单测（W2/W3/W4） | `cargo test --lib` | 新增红全绿；仅剩已知基线红 |
| 崩溃门禁（W1） | `scripts/check_crash_consistency.sh` | 20 轮 × 4 模式全绿；注入"跳过 fsync"变体必须红 |
| 集成 | `cargo test` | 现有 597+ 用例不回归 |
| fmt/clippy | `cargo fmt --all -- --check`；`cargo clippy --lib --all-targets` | 0 新增告警 |
| 生产复验（W3/W4） | rs-iot + `history_monitor.sh` | 写盘 ≤10MB/h；段数 ≤48 并稳定；errors=0；CPU 不升 |

## 风险与回退

| 风险 | 缓解 |
|---|---|
| catalog delta 应用顺序/幂等出错 → 启动条目错误 | 应用后仍与段目录全量比对（校验强度不降）；任何异常回退重建；W1 `recover` 杀点覆盖 |
| **delta 后 series_fingerprint 陈旧 → 启动快路径静默失效（复核新增）** | delta 携带重算指纹（T1 第三测钉住）；无法重算时显式关闭快路径并计数，不静默 |
| **entry_cache 容量 < 段数 → N9 openat 风暴回归（复核新增）** | 默认容量 32768 + removed_roots 即时失效；W3 落地后例行构建不再遍历全量，约束放松 |
| delta 目录在极端情况下增长（checkpoint 失败） | 阈值 + 每次成功后清理；失败计数器告警；启动强制重建后清空 |
| **L2ToL2 进 close drain → 优雅停机时间不可控（复核新增）** | L2ToL2 排除出 drain，仅后台驱动；存量收敛摊到运行期 |
| L2 合并误删数据 | 复用已验证的 marker 两阶段替换 + W1 compact 杀点；墓碑/重叠/空窗口三类测试；默认阈值可通过 `with_l2_compaction_trigger(0)` 一键回退 |
| L2 合并导致写放大 | size-tiered 上限（大段永不参与）+ 阈值触发 + 输出预算分片；实测写放大纳入 T5 |
| 缓存淘汰导致重建变慢 | 默认容量 32768（≈当前段数 2 倍），批量淘汰至 75%；条数/字节双旋钮 |

## 执行顺序

1. W1（T1→T3）：先有安全网，再动实现；
2. W2（T1→T3）：小、独立，先清掉内存无界项；
3. W3（T1→T5）：写放大根治，需生产复验；
4. W4（T0→T5）：最危险，最后做；T0 是它的前置。
