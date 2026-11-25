# 缺陷报告：可见性栅栏持写锁 → 历史读路径全局阻塞（2026-11-25）

> 状态：**已修复（根因，2026-11-25）**。
>
> **真实根因 = 嵌套读锁重入**（不是 ABBA）：`select_with_options_api` 的降采样分支
> 先取外层可见性读栅栏，持着它调 `select_into` → `collect_points_for_series_into_with_plan`
> → `snapshot_series_read_sources` **对同一把锁二次请求读锁**。parking_lot 的公平性
> 让内层请求排在已排队的写者（发布拍）之后，而写者又在等这把外层读锁 →
> 查询与发布拍永久互等。rs-iot 的 history 查询全部带 agg+interval，因此全部命中。
>
> **修复**：拆出 `*_locked` 变体（`snapshot_series_read_sources_locked` /
> `collect_points_for_series_into_with_plan_locked` / `select_into_impl_locked` /
> `select_into_locked`），降采样分支内 3 处数据读取全部改走 `_locked`——对该锁
> 单次查询最多获取一次。回归测试 `visibility_fence_reentrant.rs` 用
> `query_visibility_fence_held_hook` 确定性复现：修复前 5.18s 报
> `visibility fence busy`，修复后 0.18s 返回数据（双向实证）。
>
> **兜底护栏**（保留）：`visibility_read_fence_bounded` 的 5s 读预算，防未来新增
> 读路径再引入同类嵌套时"永久挂死"，代价是极端竞争下个别查询显式报错。
>
> **已被推翻的旧结论**（本节早期版本，勿再引用）：
> - “ABBA 死锁（flush 的 replacement_gate ↔ 可见性栅栏）”——`flush.rs` 的锁序
>   调整保留（本身是合理改进），但**不是**现场根因；诊断显示从未出现 >5s 的持锁。
> - “写锁长持锁 5.25s 的性能抖动”——所有慢等待都精确贴 5000ms 读预算，是超时
>   而非持锁时长，说明没有长持锁。

## 一、现象（现场证据）

时间线（rs-iot 进程 uptime 9h53m，2026-11-25 08:2x 观察到）：

| 时刻 | 观测 |
|---|---|
| 07:51 | BI 大屏 `/hmi/view/iot-bi-station` 正常出数（KPI 有值、趋势已绘） |
| ~08:0x | 用户从外网 `https://iot.gxai.site/hmi/view/iot-bi-station` 看：**所有面板空白**（"暂无历史数据"） |
| 08:2x | 诊断：`GET /api/v1/history`（60s 窗口、任意 series、任意设备）**全部挂死**（curl 12s/25s 超时无响应） |
| 08:2x | 同时：`GET /api/v1/sysstatus` 正常返回（401→鉴权 1.5ms），`/api/v1/rtdatas` 正常 |
| 08:2x | history 引擎自报健康：`engine:"ok" degraded:false errors:0 written:1,315,598`（**写入仍在推进**） |
| 08:2x | 进程内无热点线程：全部线程 `futex_wait_queue`（`/proc/<pid>/task/*/wchan`），CPU 0%，`ss -tnp sport = :20000` 无在途连接 |
| 08:35 | 重启 rs-iot：**读立刻恢复**（单条查询 5.8ms、batch 5.8ms 返回 60 点） |
| 08:35 | 重启前一次优雅停机被 warden 判超时（15s）并**强杀进程树**——engine close 同样卡死 |

结论：不是数据缺失、不是权限、不是网络——是**存储引擎读路径被全局锁挡住**，重启即恢复。

## 二、复现

现场负载（rs-iot 宿主聚合通道 B，BI 大屏 14 面板 ≈ 23–39 条查询/轮，每 30s 一轮）：

```bash
# 每轮 23 条查询（6 张 KPI + 4 条趋势 + 排名/构成），窗 24h/1h 混合
python3 /tmp/bi_load.py 40     # 观察 20 分钟以上
```

复现要点：触发依赖**持续读负载 + 目录发布拍**（发布拍周期性出现，见下），
单纯写入或空读不触发；rs-iot 重启后立即恢复。

## 二.5、复现负载的直接证据（2026-11-25 现场）

- 10 个并发负载 worker（每 30s 一轮 23 条查询）压 40 分钟内：三个 worker 同拍
  **batch 20.1s 超时 + 单条探测超时**（`/tmp/bi_load*.log` round 26/34/38）——
  与用户外网报告"无任何历史数据"一致。
- 插桩版日志（`tsink::diag`）：楔死窗口内 **publication segments/persist step
  彻底停摆**（01:53–01:56 每 30s persist=0，此前每 30s ≈5–7 次），而
  **alarm fire/clear 照常**（每 30s ≈18–31 条）——发布拍持写锁卡死、写入路径
  不经过该锁，与"读全挂、写继续"完全吻合。
- 读侧 503 首现 01:56:20（latency=20001ms，REST 超时护栏生效）。

## 三、机制假设（tsink 侧）

- 读路径在降采样分支取 `visibility_read_fence()`（`src/engine/query_exec/select_api.rs:173`
  → `src/engine/visibility/fence.rs:32`，底层 `flush_visibility_lock.read()`）。
- 写路径在**目录发布拍**持**同一把锁的写锁**：
  `PersistedCatalogPublicationGuard`（`src/engine/maintenance/mod.rs:143-152`）
  在整个发布过程（加/删持久段 + 刷新段目录 + 落盘 series registry index）期间持有写锁。
- 发布拍周期性出现——现场日志每 6~9 分钟一条：
  `WARN tsink::engine::storage_engine::registry_catalog: Registry catalog checkpoint reconciled root drift
   (changes the publish pipeline did not report) previous_roots=28 current_roots=29`
  （`src/engine/registry_catalog.rs:1009 reconcile_last_roots`）。
- 若发布拍持写锁期间发生**同线程重入**（`std::sync::RwLock` 不可重入）或与
  持锁方互相等待，即成 ABBA 死锁：发布拍永不释放写锁 → **所有读者永久阻塞**。
  候选重入点：`replace_loaded_tombstones_index`（`src/engine/visibility/tombstones.rs:153`，
  自身 `visibility_write_fence()`）、`deletion.rs:26 with_visibility_write_stage`。

需要 tsink 侧确认的两点：

1. 发布拍内是否存在上述**同线程重入**写锁的调用（当前 `publication.rs:106` 用的是
   `*_locked` 变体，说明历史上踩过；但 `tombstones.rs:274` 的非 locked 变体仍在）。
2. 发布拍持写锁期间是否等待**别的锁**（registry 写锁 / chunk store 锁 / WAL 锁），
   与写路径构成 ABBA。

## 四、缓解（已落地，rs-iot 侧）

`rs-iot/src/api/routes_history.rs`：读加超时护栏（默认 20s，环境变量
`RSIOT_HISTORY_READ_TIMEOUT_SECS` 可覆盖）——超时返回 503
`{error:"history query timeout"}`，客户端不再无限等待（前端 BI 面板按序列字段化报错，
单面板失败不拖全屏）。e2e 用例：`tests/history_read_e2e.rs::history_read_timeout_returns_503`
（"永不返回的 reader" + 1s 预算 → 503）。

这只是**止损**：读超时后历史仍不可用，需要重启 rs-iot（或修好 tsink）才恢复。

## 五、已落地修复（tsink fork，2026-11-25）

1. **读侧有上限等待**（根本止损）：`try_read_visibility_fence_bounded(fence)`
   （`src/engine/visibility/fence.rs`，预算 `VISIBILITY_READ_WAIT_BUDGET_MS=5000`）。
   四个读侧调用点全部切换：
   - `query_exec/select_api.rs`（降采样 select 分支）
   - `query_read/snapshot.rs`（snapshot_series_read_sources——普通 select 路径）
   - `query_exec/time_range_filter.rs`（时间范围过滤）
   - `visibility/cache/summaries.rs`（可见性摘要缓存刷新）
   超预算 → `Err(visibility fence busy…)`：REST 层把错误落到该查询的 series error，
   面板显示"查询失败"而非空白；**读侧线程不再无限堆积**。
2. **flush 锁序修正**（消除 ABBA）：`lifecycle/flush.rs persist_verified_flush`
   的 `apply_known_dirty_persisted_refresh_if_pending()`（取可见性写锁）移出
   `segment_replacement_gate` 临界区。全局锁序统一为
   `replacement/compaction gate → 可见性栅栏` 单向，post-flush 的
   "可见性写锁 → replacement_gate" 不再构成环。
3. **发布拍可观测**（临时插桩保留）：`PersistedCatalogPublicationGuard` 记录
   获取等待 >200ms / 持锁 >5s 的 DIAG 日志（`tsink::diag`）；publication 内部
   add/remove/persist 三步耗时 INFO。现场证据即来自这套插桩。

回归测试：`src/engine/tests/visibility_fence_bounded.rs` —— 写锁被长期持有时，
读在预算内返回错误；撤销修复（read fence 改回无上限）→ 测试挂死（timeout exit 124），
已双向实证。

### 五.1 修复验证（2026-11-25）

**确定性回归测试** `visibility_fence_reentrant.rs`（双向实证）：

| 版本 | 结果 |
|---|---|
| 撤销修复（3 处改回 `select_into`） | **5.18s 后 FAILED**：`visibility fence busy: read waited over 5000ms`（精确复现现场错误） |
| 保留修复 | **0.18s ok**：查询返回数据，写者随后也拿到写锁 |

测试用 `query_visibility_fence_held_hook` 把查询停在外层读栅栏刚持有之后，再排一个
写者，然后放行——时序完全确定，不靠 sleep 撞运气。

**全量回归**：`cargo test --lib` 多次运行，641 passed / 0 failed 为常态；偶尔出现
`persistence_recovery::catalog_*` 或 `persistence_background::seal_gate_*` 单例失败，
**已确认是既有 flaky、与本次改动无关**：

| 树 | 全量结果 |
|---|---|
| 干净树（`git worktree` @ 3a2c31e，零改动） | 3 次中 1 次失败（`seal_gate_triggers_on_head_age`） |
| 本次修复树 | 3 次中 1 次失败（`catalog_delta_stale_checkpoint_replays_idempotently`） |

两类失败同源：这批测试用 `poll_until(30s)` 等后台落盘，机上有其它 CPU 负载时
（`cargo` 并行 + 6 个负载 worker）会超时；单独跑 `persistence_recovery::catalog`
5 次中前 2 次（此时后台正在跑对照实验）超时、后 3 次 2.85s 全绿。**改动归属：无。**

**负载验证**（6 并发 worker × 23 查询/30s）：统计口径收紧为“任何非 23/23 成功都算
失败”，结果见最后一节。

### 五.2 早期误判（保留记录，勿再引用）

- 首版判断为“ABBA 死锁（flush 的 replacement_gate ↔ 可见性栅栏）”：错。诊断显示
  从未出现 >5s 的持锁（发布拍自报长持锁 0 次）。
- 第二版判断为“写锁长持锁 5.25s 的性能抖动”：错。所有慢等待都精确贴在 5000ms 读
  预算上，是**超时**而非持锁时长——说明没有长持锁，是等锁方与持锁方互等。
- 同期统计口径错误：把 HTTP 500（fence busy）记成“成功返回”，据此得出“零失败”。
  正确口径下旧版本有 13 次查询失败、16 个未满额轮次。

## 六、影响面备注

- 该缺陷与 rs-iot 的 deadband 修正（压力/功率因数历史归档）**无关**——
  死区修正只改变写入量（written 从 ~0 到 1.3M），不改变读路径锁语义。
- 触发需要**持续读负载**：本仓 rs-iot 此前 8 个月无历史分析页面，
  读压力近零，故未暴露；接入 BI 大屏/趋势面板后触发窗口显著变短。
