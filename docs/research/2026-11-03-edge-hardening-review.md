# edge-hardening 批次代码复核报告（2026-11-03）

> **性质**：对已完成批次（`origin/master..master`，26 笔提交，44d1dd9→76fb802）的**独立复核报告**，非实施计划。修复项另开 `docs/plans/` 走 TDD。
> **权威计划**：`docs/plans/2026-10-30-edge-hardening-catalog-terminal-crash.md`（其「实施状态」表为进度权威）。
> **方法**：三路只读深度审查子代理（W1 / W2+W3 / W4，第一轮）+ 两路横切面审查（旋钮观测文档 / 并发异常资源，第二轮）；主会话对**每一条**缺陷级结论做第一手取证——读代码行、穷举写点、实测运行、必要时自证伪。凡未能第一手复现的结论，标注「未复现」而非「确认」。
> **基线**：初版报告基线 HEAD `76fb802`；第二轮横切面基线 HEAD `15951e1`。
> **状态**：初版 7 项确认缺陷中 6 项已修复（C-1/C-2/C-4/C-5/C-6 + R-1），第二轮新增 4 项（C-7~C-10）待修。逐项状态见 §6。

## 0. 复核证据总表

| 取证 | 命令/方法 | 实际输出 |
|---|---|---|
| 全量库测试 | `cargo test -p tsink --lib -- --test-threads=4` | **622 passed / 2 failed / 2 ignored**；失败 = 已知基线红（`retention_policy::compute_only_storage_suppresses…`，root 下 chmod 失效）+ `catalog_checkpoint_consolidates_deltas`（单跑 `--nocapture` 0.92s 复绿，属已知 flaky 族） |
| 崩溃门禁裸跑 | `time bash scripts/check_crash_consistency.sh` | **8.9s**，输出「80 rounds total」 |
| 崩溃门禁全强度 | `time ITER=20 bash scripts/check_crash_consistency.sh` | **31s**，输出「80 rounds total」，PASS |
| 崩溃门禁 = ITER=5 | `cargo test --test crash_consistency_test`（无 ITER） vs `ITER=5` | **6.03s vs 6.03s**（完全一致） |
| `code()` 信号语义 | 独立 rustc 小程序：`sh -c 'kill -SEGV $$'` / `kill -9 $$` / `exit 3` | `code=None signal=Some(11)` / `code=None signal=Some(9)` / `code=Some(3)` |
| `segment_slot` 写点穷举 | `rg -n 'segment_slot\s*(=|:)' src/` | 唯一赋值点 `install.rs:307`（取 `next_segment_slot`）+ `install.rs:372`（递增）；无重绑路径 |
| 索引重建调用面 | `rg -n 'apply_loaded_segment_indexes\|install_persisted_index_rebuild' src/` | 生产调用者唯一：`bootstrap/hydrate.rs:51`（其余全为 `tests/`） |

---

## 1. 确认缺陷（第一手证据链闭合）

### C-1｜崩溃门禁脚本默认路径静默缩水 4 倍，且历史「80 轮」证据实为 20 轮

**证据链**
1. `scripts/check_crash_consistency.sh:14` `ITER="${ITER:-20}"` 只赋值未 `export`；`:17` 仅 `export RUST_LOG`。
2. 测试侧 `tests/crash_consistency_test.rs:201-215` `iter_count()` 读 `env::var("ITER")`，`NotPresent` → `DEFAULT_ITER`（`:86` 定义 `const DEFAULT_ITER: u64 = 5;`）。
3. 实测：裸跑 8.9s 打印「80 rounds」；显式 `ITER=20` 31s 打印「80 rounds」；裸跑与显式 `ITER=5` **同为 6.03s**——证明裸跑实际执行 4×5=20 轮。
4. 物理校验：80 轮仅非 recover 轮次的 kill 延迟均值就约 317ms×60 ≈ 19s，8.9s 不可能容纳。

**影响**：默认门禁强度 4 倍缩水且输出谎报；计划 W1-T3 行与 `docs/storage-engine.md:146-149` 的「80 轮 ~6s」是 20 轮的时长，历史「80 轮绿」证据需重算。引擎本身无缺陷（今日全强度实跑 PASS）。
**文档不符（同一根因的第二处证据）**：`docs/storage-engine.md:148` 写「Gate script: 20 rounds per mode (80 cycles), ~6 s」，实测全强度 80 轮 **31s**——文档把 20 轮的时长与 80 轮的轮数混写在一行，正是脚本 export 缺陷的镜像。
**修复**：`export ITER` 一行 + 更正文档/状态表轮数与时长；重跑一次全强度留证。

### C-2｜kill 断言无法区分子进程「被父 SIGKILL」与「致命信号自毙」

**证据链**
1. `tests/crash_consistency_test.rs:618-622`：`assert!(status.code().is_none(), "…exited on its own…")`。
2. 实测（见 §0 表）：`code()` 对 SIGSEGV(11) 与 SIGKILL(9) 均为 `None`，仅正常退出给数字。

**影响**：mmap 路径若存在内存安全缺陷导致子进程 segfault/OOM-kill，会被判为「正常被杀」放行，门禁绿无意义。修复一行：Unix 下断言 `ExitStatusExt::signal() == Some(9)`。

### C-3｜W4-T0 重映射容错在生产真实窗口内无效，P1 查询 NotFound 窗口未消除

**证据链（三个不变量逐一取证）**
1. **slot 只增不重绑**：`rg` 穷举全仓 `segment_slot` 赋值点——唯一来源 `install.rs:307`（`let segment_slot = persisted_index.next_segment_slot;`）与 `:372`（`saturating_add(1)`）；替换输出段走新 slot，旧 slot 只会被 `remove`（`:404-441` `segments_by_root.remove` → `segment_maps.remove(slot)`），永不指向替换段。
2. **索引重建只在启动期**：`apply_loaded_segment_indexes` 的唯一生产调用者是 `bootstrap/hydrate.rs:51`；`install_persisted_index_rebuild`（`install.rs:261-268`，`*current_persisted_index = persisted_index` + `registry.replace_all`）随之只在启动期发生，运行期不存在重排 slot 的路径。
3. **删源段先于索引刷新**：compaction 在 `compactor/execution.rs:405-410` 写 marker 并删源目录；内存索引要等维护拍 `maintenance/mod.rs:163` → `catalog_refresh/publication.rs:68-81`（`add_persisted_segments_from_loaded` + `remove_persisted_segment_roots`）才摘旧增新。
4. 窗口期内 resolver（`construction.rs:220-227`）从**同一个陈旧索引**反查 `segments_by_root`，返回的正是那条已删除路径；`segment_maps.rs:154-163` 重试同路径再失败，返回原错误。
5. `construction.rs:210-212` 的注释前提「索引里的 root→slot 已指向替换段」与实现相反（索引刷新后旧 slot 根本不存在）。
6. **同一错误前提在本批次新增代码里出现两次**：`construction.rs:210-212` 与 `state/segment_maps.rs` 的 `SegmentMapRemapResolver` 类型注释（diff 新增行「装配层按当前持久化索引实现（替换发生后索引里的 root→slot 已指向替换段）」）。注释把错误假设写成了契约，后续维护者会据此误判。

**定性**：这是**计划配方自身的假设错误**（`plan:205-210` 明文指定「NotFound 分支重新查 persisted index 的 root→slot 映射」），实现忠实于计划，但计划声称的动机「消除审查 P1 的窗口风险」未达成。净收益仅 `persisted_remap_not_found_total` 可观测性。测试 `segment_map_residency.rs:273-368` 注入虚构 resolver，钉住机制而非生产语义。该窗口在工作区 BUG 台账 #12 中本就登记为遗留，账实一致。
**修复方向**：根治须动删除时序（源目录删除推迟到索引刷新后）或 resolver 改查 `.compaction-replacements/` marker 目录；并补「索引未刷新窗口」的生产语义测试。

### C-4｜W3-T4 第 4 点未实现且未列入偏差清单——带未合并 delta 的重启必付一次 O(段数) 全量重写

**证据链**
1. 计划 `plan:186` 明文：「`SkippedValidatedFromFile` 冷路径同步扩展为『checkpoint+delta 重构后比对』，不残留只读 checkpoint 文件的旧语义」。
2. 实际 `registry_catalog.rs:665-683`：只读主文件比对 `actual.segments == expected_entries`，`actual` 不含 delta 重构（对照 `validate_registry_catalog_with_delta_report:1200-1214` 启动侧确实做了 `reconstructed` 重放）。
3. 调用链：重启校验成功 → `bootstrap/finalize.rs:34-40` 因 `force_registry_checkpoint_after_startup=false` 选 `Persist` → `persist_series_registry_index()`（`root_changes=None` 冷路径）→ 主文件（不含 delta 变更）≠ expected（含变更）→ `:705` 全量重写 + 清 delta。
4. 测试侧空洞：`persistence_recovery.rs:3452-3519` `catalog_delta_stale_checkpoint_replays_idempotently` 断言主文件字节不变，但其 fixture 经 `close()` 强制合并，主文件**已等于** expected，恰好命中 `SkippedValidatedFromFile`——未钉住「主文件陈旧 + 有效 delta」这一重启常态。

**触发面（已收窄）**：clean close 必合并清 delta，故干净重启不受影响；仅**携带未合并 delta 的重启**（崩溃/非优雅停机后首拍，delta 至多存活 100 笔窗口）走一次全量重写（15.9k 段参考量级 ≈ 9.2MB）。
**影响**：正确性无损（重写即合并、自愈、marker 一致），但这是计划明文要求、且是 `088d715` 偏差清单（仅声明「不节流」「首拍全量」两处）之外的**未声明第三处偏差**。

### C-5｜soak 判据用「双 map 合计」gauge 与「entries-only」容量比较——series 槽位存在时判据必红

**证据链**
1. `registry_catalog.rs:368-375` `observability_totals()` 返回 `(evictions, resident)`，其中 `resident = self.entries.resident + self.series.resident`（**两 map 合计**）。
2. `observability.rs:418-421` 把该合计值填入 `flush.catalog_cache_entries`。
3. `catalog_cache_soak.rs:222` 取 `capacity = DEFAULT_CATALOG_ENTRY_CACHE_CAPACITY`（仅 entries 的条数上限），`:238-240` 断言 `max_entries <= capacity`，失败消息称 "exceeded capacity bound"。
4. 语义冲突：series map 的槽位与 entries map 是**不同维度**（series 由字节预算约束，条数无上限）——只要 series map 有任意驻留槽位，合计就可能越过 entries 容量。
5. 文档同步放大了这个错误：`docs/performance-tuning.md:341` 写「resident slots across both maps (gauge). Must stay at or below the configured entry capacity」，`:353` 把同一断言写进 soak 判据描述。

**当前为何没红**：2h 实测 entries=18、series 槽位在该 profile 下也被计在 18 内（未越过 32768），所以判据碰巧通过——但**判据本身不自洽**，属「绿得偶然」。若段数接近容量或 series 活跃，该测试会报一个假的「缓存越界」。
**修复**：拆成两个 gauge（`catalog_cache_entries` 只取 entries map，新增 `catalog_cache_series_slots`），soak 判据分别对各自上界断言；或保留合计 gauge 但改名为 `catalog_cache_slots_total` 并把判据上界改为 `entries_capacity + series_slots_observed`。文档同步改。

### C-6｜淘汰水位 `capacity/4*3` 先除后乘——非 4 倍数容量下过度淘汰（cap 2/3 时退化到只留 1 条）

**证据链**
1. `registry_catalog.rs:286` `let count_water_mark = (self.capacity / 4 * 3).max(1);` —— 先 `capacity/4` 再 `*3`，而非 `capacity * 3 / 4`。
2. 实算 1..=32 与理想 3/4 水位的偏差：
   ```
   cap=3:  impl=1  ideal=2      cap=7:  impl=3  ideal=5   (偏差 2)
   cap=6:  impl=3  ideal=4      cap=11: impl=6  ideal=8   (偏差 2)
   ```
   共 15 个容量值偏低，最大偏差 2 条。`capacity=2` → 水位 `2/4*3=0 → max(1)=1`：插入第 3 条时一次性淘汰到只剩 1 条（越过 3/4 水位）。
3. 字节预算同型：`:287` `bytes_water_mark = self.budget_bytes / 4 * 3`。
4. **默认值不受影响**：`32768/4*3 = 24576` 恰为 3/4，`32MiB/4*3` 亦为整数——缺陷只在用户自定义非 4 倍数容量时出现。

**影响**：淘汰过度（命中率下降、淘汰计数虚高），非正确性问题；`capacity=1` 时保底（`:297`）保证不清空，最新 touch 的刚插入条目最后被扫到、不会被淘汰。`performance-tuning.md` 声称「bulk eviction to a 3/4 watermark」与实现不符。
**修复**：用 `capacity - capacity / 4`（= ceil(3/4)，无溢出、比 floor 多留一条更安全），字节预算同改；不要用 `capacity.saturating_mul(3)/4`——在 `usize::MAX` 时饱和到 4.6e18 而非真实的 1.38e19，反而更过度淘汰（已实算验证）。

---

## 2. 经复核确认的风险项

| # | 位置 | 结论 | 取证 |
|---|---|---|---|
| R-1 | `compactor/planning.rs:192` vs `:243` | L2 全量 manifest survey **先于** `l2_trigger==0` 短路：「0=关闭」行为成立、**零开销不成立**（16.5k 段下每后台拍 ~16.5k 次 manifest 读、零产出；收敛期总成本 O(N²/窗口)）。短路提前一行即消 | 代码顺序直读 |
| R-2 | `registry_catalog.rs:863-874` vs `:878-887` | 阈值合并失败 `?` 上抛 → `flush.rs:484-497` 回滚已发布段 + 返回 `Err`（flush 失败）；而 delta 追加失败只标脏不 fail flush。ENOSPC + 阈值临界下写路径持续失败（数据不丢，WAL 前缀未回收） | 两分支对照 + flush 调用点 |
| R-3 | `query_read/analysis.rs:125-128`、`append_sort.rs:75-83` | W1 门禁 Gate 2「无重复 (dev,ts)」被读路径去重层遮蔽：replay 重放类重复（persisted+active 跨源同 ts）恰好命中 `requires_exact_dedupe` 折叠；`storage-engine.md:172` 的「全扫描无重复对」超出断言能力 | 抽查代码属实 |
| R-4 | `persistence_recovery.rs:2302` | catalog_delta flaky 族 **4 线程下仍复现**（今日 1/622；此前记录仅 8 线程偶发）；10s 轮询超时与真实失败同量级（绿态 ~1.4s），建议 30s 或环境变量化 | 本会话实测 |
| R-5 | `registry_catalog.rs:139-157, 444-447` | series 字节预算估算未计 HashMap 节点(~48B)、PathBuf key 堆分配(~60-80B)、分配器粒度 → 实际足迹可达 32MiB 预算 ~2x；NotFound 空槽计 0 字节、字节预算永不淘汰它们 | 估算函数 + 插入点直读 |
| R-6 | `registry_catalog.rs:833-842` | 每笔 delta 全量重算 series 指纹；series 总量超预算后每拍发生预算规模的 series.bin 重读。30 series 现状在界内，高基数部署的边界风险 | 直读 |
| R-7 | `planning.rs:368-374` → `:170` | L2 survey 遇单个 DataCorruption 使整个 compaction pass 报错，L0/L1 可用工作一并拖停；drain 的 `?`（`shutdown.rs:119`）同样冒泡。**定性：非本批次引入的缺陷**——`load_level_compaction_candidates` 的 fail-fast 是既有代码（`planning.rs:353-376`），本批次新增的是「L2 也走这条路径」→ 新暴露面。W4 前 L2 损坏不影响 compaction，现在每拍持续失败直到人工介入 | 直读 survey + 仲裁循环 + drain 调用点 |
| R-8 | `crash_consistency_test.rs:379-385, 615` | recover 杀点无法观测是否真打进 WAL 重放窗口（只有 open-complete marker）；kill 延迟从 spawn 起算且最小 54ms（慢机假红风险）；D1 的跨段 WAL 轮转分支无崩溃级覆盖 | 直读 + 种子序列核算 |
| R-9 | `registry_catalog.rs:486-499` | `CatalogPersistMemo::from_disk` 只恢复 `next_seq`，`deltas_since_checkpoint` / `delta_bytes_since_checkpoint` 恒从 0 起 → 重启后需再积 100 笔才合并，盘上 delta 数可跨会话累积到 ~200（内存占用每笔 O(变更)，有界但翻倍）。**有界性成立，属口径风险非缺陷** | 直读 `:486-499` + `:529-530` |

### 已推翻的子代理结论（自证伪）

| 原结论 | 复核 | 结论 |
|---|---|---|
| F5（W2/W3 报告）：「reset 分支 `next_seq=1` 与残留 delta 冲突 → 变更丢失」 | `registry_catalog.rs:715-725`：`reset_delta_state` 仅在 `force && absorbed_checkpoint_seq.is_none() && !main_carries_segments` 成立，即**主文件不承载任何段条目**（实例从未全量同步过）；此时残留 delta 本就应被丢弃，`next_seq=1` 与「主文件即完整真值」自洽。启动侧 `:1182-1214` 的 `seq > checkpoint_seq` 连续校验也会把不可重放残留判为断层并回退重建 | **推翻，非缺陷** |
| 首轮称「W4-T0 修复了窗口」 | 见 C-3 | **推翻，窗口未消除** |
| 首轮称「80 轮 6s 门禁达标」 | 见 C-1 | **推翻，实为 20 轮** |

---

## 3. 已查未见问题（正面结论，附核对点）

- **D1（WAL 帧号回卷）**：同段 marker 托底覆盖可达状态空间；跨段归 `discard_unpublished_suffixes`；quarantine 分支次序正确；`saturating_add` 防溢出；`published_prefix_len` 两处真损坏检测原样保留（`wal/segments.rs:120-194`）。
- **D2（分配器倒写）**：全仓 `next_segment_id` 原子量写点穷举 = 构造初值 + flush/compaction 两处 `fetch_add` + `visibility.rs:46-52` 唯一 store（已单调化）——**无其他倒写点**。
- **LRU 记账**：`insert` 替换差量对账（`registry_catalog.rs:229-237`）、`remove`/`evict_over_limit` 全走 `saturating_sub_fetch`（`:261,:303`）、`resident` 增减全在 `slots` 锁内配对；`get` 持写锁刷新 touch（`:217-219`）→ 真 LRU 非 FIFO；淘汰保底 1 条且最新 touch 不被淘汰（`:297`）。本仓 fetch_sub 下溢历史未复发。
- **delta 崩溃窗矩阵**：主文件重写（`:705`）→ 删 delta（`:731`）→ 写 marker（`:732`）顺序正确；四个崩溃点逐一推演，无中间态能通过验证却错误（最坏退化为回退重建）；`pending_changes` 在重写路径 `:738` 被清，delta 失败路径 `:885` 保留，无重复/丢失。
- **L2 收敛机制**：仲裁稳定排序「上次赢家移队尾」正确；`max_input = (budget/8).max(1)` 防零；零输出窗口源段真删不留活锁同型模式；`CloseDrain` 的 `continue` 在 survey 之前（close 零 L2 开销）；`l2_to_l2_runs` 按 `(Some(2),Some(2))` 计一次即 break。
- **稳定性净面**：新增生产路径零 unwrap/expect；compaction 与 flush 经 `segment_replacement_guard` 串行；resolver 无 Arc 循环；`try_read` 是必要设计（持索引读锁进 remap 时阻塞式二次读会与排队写者互等）。
- **W4 实测自洽性**：1101 窗 ≈ 16,534/16、1s interval ≈ 1101 拍 ≈ 1200s 三数互证；等值断言含 NaN 位级比对。

---

## 4. 需修订的既有记录

1. 计划 W1-T3 行 + `docs/storage-engine.md:146-149`：「80 轮 ~6s」→ 应为「脚本 export 缺陷导致裸跑仅 20 轮；全强度 80 轮 ≈ 31s」。
2. W4-T0 状态行：补「生产 resolver 在真实窗口返回同一条已删除路径，P1 窗口未消除，净收益为计数器」，与工作区 BUG 台账 #12 对齐。
3. W3-T4 状态行偏差清单：补第三处「冷路径未按计划第 4 点做 delta 感知比对」。
4. `docs/storage-engine.md:172`「无重复对」声明：降格为「受读路径去重遮蔽，不构成 replay 重复的独立证据」。
5. `docs/performance-tuning.md:341` 与 `catalog_cache_soak.rs:238-240`：`catalog_cache_entries` 为双 map 合计却按 entries-only 容量判界，须拆分或改名（见 C-5）。
6. `docs/performance-tuning.md` 「bulk eviction to a 3/4 watermark」与 `registry_catalog.rs:286` 的先除后乘不符（见 C-6）。

## 5. 第二轮横切面审查新增发现（基线 `15951e1`，全部经主会话第一手复核）

### C-7｜soak 短跑命令被脚本无条件覆盖——「600 秒冒烟」实际跑 2 小时

**证据链**
1. `scripts/check_catalog_cache_soak.sh:26-27`：`SOAK_SECONDS="${SOAK_SECONDS:-7200}"` 后 `export TSINK_SOAK_SECONDS="$SOAK_SECONDS"`——**无条件覆盖**调用方环境里的 `TSINK_SOAK_SECONDS`。
2. `docs/performance-tuning.md:353` 与 `src/engine/tests/catalog_cache_soak.rs:13` 都给出 `TSINK_SOAK_SECONDS=600 scripts/check_catalog_cache_soak.sh   # short smoke run`。
3. 脚本自己的头注释（`:14`）用的是正确形式 `SOAK_SECONDS=600`——只有文档与测试注释写错变量名。
4. 实测复刻该环境处理段：`TSINK_SOAK_SECONDS=600` → 子进程看到 `7200`；`SOAK_SECONDS=600` → `600`。

**影响**：按文档做短冒烟的人实际跑 2 小时；且 10 分钟窗只有 3 个样本 → 走「跳过斜率」分支，与 2h 判据强度完全不同（判据假象）。
**修复**：文档/注释改 `SOAK_SECONDS=600`，或脚本改为 `export TSINK_SOAK_SECONDS="${TSINK_SOAK_SECONDS:-${SOAK_SECONDS:-7200}}"`（两者都做最稳）。

### C-8｜`catalog_delta_fingerprint_fallback_total` 在「无 delta + 指纹为 null」时不计数——升级场景计数器漏计

**证据链**
1. `registry_catalog.rs:1254-1255`：`let mut fingerprint = actual.series_fingerprint; let mut fingerprint_fallback = false;`
2. `:1301-1302`（循环内）：`fingerprint = delta.series_fingerprint; fingerprint_fallback = fingerprint.is_none();`
3. **无 delta 时循环体不执行** → `fingerprint_fallback` 恒 `false`，即使 `actual.series_fingerprint` 为 `None`。
4. `PersistedRegistryCatalogFile.series_fingerprint` 带 `#[serde(default)]`（`:81-83`）→ 旧版本 catalog 反序列化即为 `None`。
5. 文档 `storage-engine.md:440` 明确写「or when the fingerprint is `null`/unrecomputable — startup loads series metadata from every segment instead（counter `catalog_delta_fingerprint_fallback_total`）」。
6. 实际行为：快路径确实关闭（`recovery.rs` 的 `should_load_segment_series` 要求 `Some(fingerprint)`），但计数器不 +1。

**影响**：升级自旧版本（无 delta + 指纹 null）时，运维看到 `catalog_delta_fingerprint_fallback_total == 0` 会误判「快路径健康」，而实际每次启动都在全量读 `series.bin`。属**最典型的一次性升级场景**。
**修复**：`let mut fingerprint_fallback = actual.series_fingerprint.is_none();`

### C-9｜计划状态行宣称「已修正 architecture.md 窗口 8→16」，实物未改

**证据链**
1. `docs/plans/2026-10-30-edge-hardening-catalog-terminal-crash.md:61`（W4-T5 行）写「architecture.md 层级图更新（**连带修正窗口 8→16 陈旧值**）」。
2. 实际 `docs/architecture.md:415` 仍为 `selects a window of up to 8 source segments`；`docs/storage-engine.md:361` 仍为 `source_window_segments (default 8)`。
3. `git diff origin/master..master -- docs/architecture.md | grep -c 'up to 8'` = **0**——该行一字未动。
4. 代码真值 `compactor.rs:49` `DEFAULT_SOURCE_WINDOW_SEGMENTS: usize = 16`（origin/master 亦已是 16，属既有文档漂移）。

**影响**：状态表与实物不符（声称修了但没修），且两处文档对使用者的窗口参数描述错误一倍。
**修复**：改两处文档为 16；并在计划状态行如实改写（或标注「未修」）。

### C-10｜`with_chunk_points` 文档仍称控制输出段预算 `n × 512`，该关联已失效

**证据链**
1. `docs/compaction.md:243`：`with_chunk_points(n)` …「Also controls the output segment size budget (`n × 512` points per output segment)」。
2. 代码 `compactor.rs:51-61` 注释明文：`It used to be chunk_point_cap * 512 … It is now 8x the L0 flush shard size`；`DEFAULT_OUTPUT_SEGMENT_POINT_BUDGET = 8_388_608` 固定，`chunk_points` 不再影响输出预算。
3. 同一张表 `:244` 本批次刚新增了 `with_l2_compaction_trigger` 行——编辑过该表却留下上方错误关联。

**影响**：使用者按 `n×512` 估算输出段大小会差一个数量级。
**修复**：删除该关联句，改为「输出段预算固定 8,388,608 点（L0 flush shard 8 倍），由 `with_output_segment_point_budget` 覆盖」。

### 第二轮「文档不符」清单（不单列编号，按文件汇总）

| # | 位置 | 不符内容 |
|---|---|---|
| D-1 | `bootstrap/recovery.rs:146-148` vs `storage-engine.md:436` | 「主文件缺失 + 有 delta」被计为「delta 链回退」，归因误导（每次启动至多 +1，非漏计） |
| D-2 | `storage.rs:524-526` / `metrics.rs:277-279` vs `segment_maps.rs:200-205` | 计数器计「NotFound 事件」而非「成功重试」——resolver 返回 None 时也计数；测试 `segment_map_residency.rs:360-366` 反而固化了该口径 |
| D-3 | `storage-engine.md:426` / `configuration.md:49` | 称 delta 路径 `O(changes)`，实为 `O(changes + series)`（每笔 delta 全量重算指纹，含 `Vec<PersistedSeries>` 深拷贝） |
| D-4 | `compaction.md:48` | 仍写「checks L0 first, then L1」，实为三路含 `L2ToL2`（`planning.rs:176-182`） |
| D-5 | `configuration.md:3` | 自述「Complete listing of every configuration knob」，缺 3 个新旋钮（`catalog_entry_cache_capacity` / `catalog_series_cache_bytes` / `l2_compaction_trigger`） |
| D-6 | `configuration.md:35` | 称本表是 Rust API 与 Python 绑定的「equivalent」，但 `tsink-uniffi` builder 无这 5 个新旋钮、`UFlushObservabilitySnapshot` 无 8 个新计数器 |
| D-7 | `performance-tuning.md:478` | 「All internal counters are exposed at GET /metrics」被本批次 8 个新计数器证伪（既有绝对断言） |

### 第二轮「风险」清单（代码面，未单列编号）

| # | 位置 | 内容 |
|---|---|---|
| X-1 | `compactor/planning.rs:326-329` | 累计点数用裸 `+=`，同仓执行路径一律 `saturating_add`（`execution.rs:307-309`）——损坏 manifest 且 budget 极大时 debug panic/release 回绕 |
| X-2 | `storage.rs:1616-1620` vs `compactor.rs:222` | builder 侧无夹紧、Compactor 侧夹 `.max(1)`；`with_output_segment_point_budget(0)` 静默退化为「每点一段 + L2 收敛关闭」 |
| X-3 | `registry_catalog.rs:247-251` vs `:381` | gauge 读侧不持锁，并发下可瞬时观察到 `capacity+1`（soak 5min 采样几乎不可能命中） |
| X-4 | `registry_catalog.rs:863` + `series_cache_bytes=0` | series 缓存禁用时每笔 delta 全量 `series.bin` 磁盘重读（文档称「escape hatch」未点明放大倍数） |
| X-5 | `shutdown.rs:66-71,132` | `CompactionDrainPass::l2_to_l2_runs` 生产态无消费者（仅测试读），建议 `#[cfg(test)]` 或删 |

---

## 6. 修复状态（截至 2026-11-04）

| 项 | 状态 | 提交 |
|---|---|---|
| C-1 门禁 export ITER | ✅ 已修 | `b3c894c` |
| C-2 kill 断言 signal | ✅ 已修 | `d168f01` |
| C-6 淘汰水位 | ✅ 已修 | `fd356d0` |
| C-5 缓存 gauge 口径 | ✅ 已修 | `10e2b20` |
| R-1 L2 成本门 + 边界登记 | ✅ 已修 | `8692cfe` + `162bc58` |
| C-4 冷路径 delta 感知 | ✅ 已修 | `15951e1` |
| R-2 合并失败降级 / R-4 超时 | ✅ 已修 | `da5d76d` |
| **C-7 soak 短跑命令** | ✅ 已修 | `3ee1fb2` |
| **C-8 指纹回退计数漏计** | ✅ 已修 | `3ee1fb2` |
| **C-9 窗口 8→16 文档漂移** | ✅ 已修 | `3ee1fb2` |
| **C-10 chunk_points 关联失效** | ✅ 已修 | `3ee1fb2` |
| D-1~D-7 文档不符 | ✅ 已修 | `3ee1fb2` |
| X-1 裸 `+=` 改饱和 | ✅ 已修 | `3ee1fb2` |
| X-2~X-5 风险 | ⏳ 登记待排期 | — |
| C-3 删源时序根治 | ⏳ 需设计决策 | — |
| R-3 Gate 2 去重遮蔽 | ⏳ 需增强断言或登记边界 | — |
| R-5/R-6/R-7/R-8/R-9 | ⏳ 登记待排期 | — |
| 计划状态表（`plan:61` 的窗口声称） | ⏳ 待主会话订正 | — |

## 7. C-3 决策简报（唯一需架构裁决项）

### 现状与严重度
- 窗口：compaction 在 `execution.rs:146-162`（持 `segment_replacement_guard`）**先删源段目录、随即删 marker**；内存索引要等维护拍 `maintenance/mod.rs:163` → `publication.rs:68-81` 才摘旧。
- 窗口内查询：`query_read/snapshot.rs:97` `let Some(segment_map) = self.segment_maps.get_or_remap(*slot)? else {...}` —— `?` 把 NotFound 上抛为**查询失败**（瞬时，非静默丢数据）。
- 触发前提：该 slot 的 mmap 已被预算驱逐（未驱逐则读内存映射，无影响）。W4 引入 L2 收敛后替换频率上升 → 窗口更频繁，这是 T0 存在的理由。
- W4-T0 的 resolver 在真实窗口内返回**同一条已删除路径**（见 C-3 原条目）→ 净收益仅计数器。

### 被排除的方案（附排除理由）
| 方案 | 排除理由 |
|---|---|
| resolver 改查 `.compaction-replacements/` marker 反查输出段 | **原理不成立**：marker 在删源段后**立即删除**（`execution.rs:158-162`），窗口内已无 marker；且一次 compaction 可能产出多个输出段，单 slot 无法唯一定位到哪个输出（定位错会读到错误数据，比 NotFound 更坏） |
| 查询侧把 NotFound 改为「跳过该 chunk」 | 静默丢数据（查询结果少点），不可接受 |
| 查询侧在 NotFound 时重试整条查询 | 窗口内索引仍陈旧，重试仍失败；除非阻塞等索引刷新（引入查询侧等待与死锁风险） |

### 两个可选方案
| | A. 根治：待删暂存区（推荐若排期允许） | B. 接受 + 登记（推荐立即） |
|---|---|---|
| 做法 | 删源段改为**原子重命名**到 `.compaction-replacements/pending-delete/`（隐藏目录，loader 与候选扫描均忽略）→ marker 保留 → 维护拍索引刷新后删除暂存区 → 启动期 `finalize_pending_compaction_replacements` 兜底 | 保持现状；文档登记「窗口内查询可能瞬时失败，重试即成功」；保留计数器 |
| 为何安全 | 重命名原子且对扫描不可见（不会重复入选候选）；marker 保留使崩溃重放仍幂等（现有 `finalize_pending_compaction_replacements` 已幂等） | 不引入新失败面；数据零风险 |
| 改动面 | `execution.rs`（删源改暂存）、`publication.rs`/`maintenance`（刷新后清暂存）、启动 `finalize`（清残留暂存）、1-2 测试（真实窗口语义：删源后索引未刷新时查询不再失败） | 文档 2 处 + 1 测试钉住「重试可恢复」 |
| 风险 | 中：动删除时序是数据风险面；磁盘峰值多占一个窗口的源段 | 低：窗口期查询失败在部署上表现为偶发 500，需调用方重试 |
| 量级 | 一个完整任务（含红测与崩溃门禁） | 半小时 |

**主会话建议**：立即做 B（把已知窗口写进 `docs/storage-engine.md` 与 `docs/compaction.md`，并在计划登记 C-3 未解），A 单开任务排期——理由是本批次已证明「动删除时序」需要专门的崩溃窗设计（D1/D2 两次都是在这里翻车），不应用收尾阶段的余量去做。

---

## 8. 修复优先级

| 优先 | 事项 | 量级 |
|---|---|---|
| P0 | C-1 `export ITER` + 文档轮数修订 + 全强度重跑留证 | 一行 + 文档 |
| P0 | C-2 `signal()==Some(9)` 断言 | 一行 |
| P0 | C-5 缓存 gauge 口径拆分（判据假绿） | 小改 + 文档 |
| P0 | C-6 淘汰水位先乘后除 | 两行 |
| P1 | R-1 trigger==0 短路提前 | 一行移动 |
| P1 | C-4 冷路径 delta 感知比对（或先落偏差登记） | 半个任务 |
| P2 | R-2 合并失败降级或文档化、R-4 超时放宽 | 各小改 |
| P2 | C-3 方案 B（登记窗口） | 半小时 |
| P3 | C-3 方案 A（待删暂存区根治） | 一个完整任务 |
