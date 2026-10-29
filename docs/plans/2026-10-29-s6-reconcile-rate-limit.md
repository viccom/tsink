# S6 记账热路径降频对账化实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 把 flush 线程上"每次 persist 成功即全量重算内存记账"（O(series) 百万级引用扫描，≥4 次/秒）降频为 30s 量级的低频对账锚点，让逐出跟上 seal 速度（目标 L0 斜率 ≤10/h，r7 残余 +28/h）。

**Architecture:** 增量记账（五条主路径 O(touched) 原子加减）已是权威值，`refresh_memory_usage` 本就定位为对账面（`core_impl/utilities.rs:78-80` 注释自证）。本计划不动任何增量机制，只给 `lifecycle/flush.rs:765` 这一唯一热路径调用点加时间门控；bootstrap/shutdown/construction 锚点保持全量校准；漂移可观测（计数器 + WARN）。

**Tech Stack:** Rust（std 原子，无新依赖）、既有 FlushObservabilityCounters 观测面、既有对账测试网 `assert_engine_memory_usage_reconciled`。

---

## 审查勘定的事实（file:line 以 HEAD 50ba2a5 实测）

| # | 事实 | 锚点 |
|---|---|---|
| 1 | 热路径调用点：persist 成功即 refresh，仅此处是"每次"语义 | `src/engine/lifecycle/flush.rs:763-765`（`if self.memory.accounting_enabled && outcome.persisted { self.refresh_memory_usage(); }`） |
| 2 | flush 拍默认 250ms → 稳态 ≥4 次全量/秒，串行跑在 flush 线程 | `src/engine/engine.rs:116` `DEFAULT_FLUSH_INTERVAL` |
| 3 | 全量重算主体：64 active + 64 sealed shard 逐 point + registry + 7 个 O(100k) 容器 | `src/engine/maintenance/memory_accounting/context.rs:196` `refresh_memory_usage` |
| 4 | "refresh = 对账面"注释（Cut 1 后须同步更新，否则注释与行为矛盾） | `src/engine/core_impl/utilities.rs:78-80`（提到 "flush/bootstrap refresh snaps"） |
| 5 | 锚点调用（保持不动）：bootstrap / shutdown / construction | `bootstrap/finalize.rs:66`、`lifecycle/shutdown.rs:193`、`construction.rs:201`、`engine.rs:510` |
| 6 | 观测分支（**不动，登记边界**）：`memory_observability_snapshot` 在 accounting_enabled=false 时每次快照全量 refresh——10 万点场景必配预算（enabled=true）不触发 | `maintenance/memory_accounting.rs:203-206` |
| 7 | counters 先例（N8）：`replaced_published_roots_total` 等 | `src/engine/metrics.rs:219` `FlushObservabilityCounters`、`:239/:241` 字段 |
| 8 | 红测先例：探针计数（本计划复用 counters 自身作探针，不再加新探针） | `engine.rs:302` `visibility_full_estimation_probes` |
| 9 | 对账网：增量 vs 全量相等断言，15+ 调用 | `src/engine/tests/mod.rs:105` `assert_engine_memory_usage_reconciled` |
| 10 | 测试触发先例：`storage.flush_all_active().unwrap(); storage.persist_segment_with_outcome().unwrap()` | `tests/../engine/tests/deletion.rs:927-928`、`admission_control.rs:398` |
| 11 | MemoryAccountingState 字段装配处（新字段加这里） | `src/engine/construction/assembly.rs:157` `build_memory_accounting_state` |

**基线口径（本机 Linux 实测）**：`cargo test -p tsink`（单测+集成）= **1162 passed / 0 failed**。方案原文的 1314 是 Windows + server crate 口径，本机门禁以 1162 为基线，不得出现 failed。

**验收判据**（代码面，本计划可完成）：全测试绿于基线、clippy `-D warnings` 干净、fmt 干净、红→绿证据齐全。
**r8 bench / soak24 判据**（L0 斜率 ≤10/h、吞吐窗 ≥95K/s 等）：**不在本计划执行面**——需 10 万点压测环境，完成后另行安排（见 §后续）。

**分支**：`s6-reconcile-rate-limit`（已在 tsink_fork 主目录建好）。不使用 worktree：rs-iot 的 path 依赖 `../tsink_fork` 指向主目录，worktree 会造成 rs-iot 侧验证断层；本仓无并发使用。

---

### Task 1: 红测——persist 热路径对账频控

**Files:**
- Test: `src/engine/tests/memory_reconcile_gate.rs`（新文件，若 tests mod 有模块注册惯例则同步注册）

**Step 1: 写失败测试（两个用例）**

先勘察确认：`persist_segment_with_outcome()` 的实现链是否经过 `flush.rs:763-765` 所在函数（若是独立入口，改测 `flush_all_active` + persist 组合路径；若都绕开，找到测试可达的同一出口并在测试注释记录）。

```rust
// 依赖与风格照抄 deletion.rs:927 先例；storage 构造用小内存预算使 accounting_enabled=true
// （MemoryAccountingState 装配语义：memory_budget_bytes != u64::MAX 即启用，见 assembly.rs:158）。

/// S6 主测：连续多次 persist 成功，全量对账次数必须远小于 persist 次数。
/// 当前代码每次 persist 都 refresh（必然红）；实现后 30s 窗内仅 bootstrap 锚点校准过，
/// 测试窗内的 flush 热路径对账次数应为 0。
#[test]
fn flush_persist_hot_path_reconciles_at_most_once_per_window() {
    let storage = test_storage_with_memory_budget(); // 参照现有构造 helper
    write_some_rows(&storage);                        // 现有 helper 或手写 3 series×N 点
    for _ in 0..5 {
        storage.flush_all_active().unwrap();
        let outcome = storage.persist_segment_with_outcome().unwrap();
        assert!(outcome.persisted, "测试前置：persist 必须成功，否则频控断言无意义");
    }
    let reconciles = storage.flush_memory_reconciles_total(); // Task 2 暴露，本测试先红于此 API 缺失
    assert_eq!(reconciles, 0, "30s 门控窗内 flush 热路径不应触发全量对账");
}

/// 到期触发：手动把 last_full_reconcile_ms 回拨到"已过期"，下一次 persist 必须对账一次且仅一次。
#[test]
fn flush_persist_reconciles_once_when_interval_elapsed() {
    let storage = test_storage_with_memory_budget();
    write_some_rows(&storage);
    storage.memory.last_full_reconcile_ms.store(0, Ordering::Relaxed); // 回拨=起点即过期
    storage.flush_all_active().unwrap();
    storage.persist_segment_with_outcome().unwrap();
    assert_eq!(storage.flush_memory_reconciles_total(), 1);
    // 紧接着第二次 persist 不再对账（窗内）
    write_some_rows(&storage);
    storage.flush_all_active().unwrap();
    storage.persist_segment_with_outcome().unwrap();
    assert_eq!(storage.flush_memory_reconciles_total(), 1);
}
```

**Step 2: 跑红**

Run: `cargo test -p tsink --lib flush_persist -- --nocapture`
Expected: FAIL——编译错误（`flush_memory_reconciles_total` / `last_full_reconcile_ms` 不存在）即红的第一形态；若选择先加最小桩再断言计数（当前实现每次 refresh 但不计数 → reconciles 恒 0 用例 1 意外绿），**必须保证用例 2 或"计数>0"形态先红**：允许在 Task 1 内先加 `memory_reconciles_total` 计数器并在现有 refresh 调用点计数（不改行为），使用例 1 呈现"5 次 persist 计数=5"的红。两段式红（先计数后门控）均可接受，红的证据必须落盘到提交信息。

**Step 3: Commit（仅测试 + 计数器桩，行为未动）**

```bash
git add -A && git commit -m "test(s6): 红测——flush 热路径对账频控两用例（计数桩先行，行为未动）"
```

### Task 2: Cut 1——时间门控 + counters + 注释同步

**Files:**
- Modify: `src/engine/engine.rs`（常量区 ~:116 加 `DEFAULT_MEMORY_RECONCILE_INTERVAL`）
- Modify: `src/engine/construction/assembly.rs:157`（MemoryAccountingState 加 `last_full_reconcile_ms: AtomicU64::new(0)` 与 `constructed_at: Instant::now()`；Instant 字段非原子，混装合法——该 struct 已有 Duration 字段先例）
- Modify: `src/engine/metrics.rs:219 FlushObservabilityCounters`（加 `memory_reconciles_total: AtomicU64`，装配处同步 `AtomicU64::new(0)`）
- Modify: `src/engine/lifecycle/flush.rs:763-765`（调用点换门控包装）
- Modify: `src/engine/maintenance/memory_accounting.rs`（加 `reconcile_memory_usage_rate_limited`；暴露 `flush_memory_reconciles_total()` 读数）
- Modify: `src/engine/core_impl/utilities.rs:78-80`（注释更新：flush snaps → rate-limited reconciliation anchor）

**Step 1: 实现（最小面）**

```rust
// engine.rs 常量区（DEFAULT_* 风格）：
/// Minimum interval between full memory-usage reconciliation passes on the
/// flush thread (S6). Incremental deltas stay authoritative between passes;
/// this gate only rate-limits the O(series) full rescan. Bootstrap/shutdown
/// anchors are NOT gated (they call refresh_memory_usage directly).
pub(crate) const DEFAULT_MEMORY_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

// maintenance/memory_accounting.rs：
pub(in super::super) fn reconcile_memory_usage_rate_limited(&self) {
    let elapsed_ms = self.memory.constructed_at.elapsed().as_millis() as u64;
    let last = self.memory.last_full_reconcile_ms.load(Ordering::Relaxed);
    if elapsed_ms.saturating_sub(last) < DEFAULT_MEMORY_RECONCILE_INTERVAL.as_millis() as u64 {
        return;
    }
    // CAS 而非 store：flush 线程序列化调用，防御未来并发调用方重复全量
    if self.memory.last_full_reconcile_ms
        .compare_exchange(last, elapsed_ms, Ordering::AcqRel, Ordering::Relaxed)
        .is_err() { return; }
    self.refresh_memory_usage();
    self.observability.flush.memory_reconciles_total.fetch_add(1, Ordering::Relaxed);
}

// flush.rs:763-765 调用点：
if self.memory.accounting_enabled && outcome.persisted {
    self.reconcile_memory_usage_rate_limited();
}
```

可见性/字段路径按编译器提示修正（`observability` 在 flush 上下文的现有访问方式照抄 persist_noop_total 同位置先例）。**首次 persist 即被门控跳过（elapsed≈0 < 30s）是预期行为**：bootstrap 锚点（finalize.rs:66）已全量校准过。

**Step 2: 跑绿**

Run: `cargo test -p tsink --lib flush_persist memory_reconcile`
Expected: PASS（Task 1 两用例绿）

**Step 3: 全量门禁**

Run: `cargo test -p tsink`（1162+新增 全绿）、`cargo clippy -p tsink --all-targets -- -D warnings`、`cargo fmt --check`
Expected: 0 failed / clippy 干净 / fmt 干净

**Step 4: Commit**

```bash
git add -A && git commit -m "perf(s6): flush 记账热路径降频——每次 persist 全量重算改 30s 门控对账（增量为权威，锚点不门控；utilities 对账面注释同步）"
```

### Task 3: 回归保护——采样窗内语义不变（非红测，防回归）

**Files:**
- Test: `src/engine/tests/memory_reconcile_gate.rs`（追加用例）

**Step 1: 追加用例（复用对账网）**

```rust
/// 门控跳过期间增量值正确，且到期对账后值不变（增量==全量）。
/// 注意：本用例在 Cut 1 前也会绿（更频繁对账不破坏语义）——它是回归保护，不是 TDD 红测。
#[test]
fn sampled_window_keeps_incremental_authoritative_and_reconciles_equal() {
    let storage = test_storage_with_memory_budget();
    write_some_rows(&storage);
    storage.flush_all_active().unwrap();
    storage.persist_segment_with_outcome().unwrap();   // 门控跳过（窗内）
    assert_engine_memory_usage_reconciled(&storage);    // 增量 vs 强制全量：相等
    write_more_rows(&storage);                          // 窗内继续写
    storage.memory.last_full_reconcile_ms.store(0, Ordering::Relaxed);
    storage.flush_all_active().unwrap();
    storage.persist_segment_with_outcome().unwrap();   // 到期 → 对账一次
    assert_engine_memory_usage_reconciled(&storage);    // 仍相等
    assert!(storage.flush_memory_reconciles_total() >= 1);
}
```

**Step 2: 跑绿 + 全量门禁同 Task 2 Step 3**

**Step 3: Commit**

```bash
git add -A && git commit -m "test(s6): 采样窗内增量权威性与到期对账等值——回归保护用例"
```

### Task 4: Cut 2——对账漂移 WARN

**Files:**
- Modify: `src/engine/maintenance/memory_accounting.rs`（`reconcile_memory_usage_rate_limited` 内，refresh 前读增量值、refresh 后比较，偏差超阈值 WARN）

**Step 1: 实现**

```rust
let before_incremental = self.memory_used_value_for_reconcile(); // 现有 used 读数 API（按实际名）
let reconciled = self.refresh_memory_usage();
// 阈值：绝对差 > 1% 且 > 1 MiB 才 WARN（双重门槛防小值抖动；常量内联，不扩配置面）
let drift = before_incremental.abs_diff(reconciled);
if drift > (before_incremental / 100).max(1024 * 1024) {
    tracing::warn!("memory accounting drift between incremental and full recompute: incremental={before_incremental} reconciled={reconciled} drift={drift}");
}
```

tsink 已用 `tracing`（`registry_catalog` WARN 先例）。Drift 观测只告警不纠偏（refresh 返回值本就是全量值，调用后 used 已被置为权威全量——WARN 落在置值前比较）。

**Step 2: 门禁全量（同 Task 2 Step 3）+ 确认现有测试无 WARN 触发面变化**

Run: `cargo test -p tsink 2>&1 | tail -5`
Expected: 0 failed；若个别测试因漂移 WARN 变吵，检查是真漂移（bug 线索，停手上报）还是阈值过紧（调阈值重跑）。

**Step 3: Commit + 合并 master**

```bash
git add -A && git commit -m "feat(s6): 对账漂移双门槛 WARN——增量与全量偏差可观测（不纠偏）"
git checkout master && git merge --no-ff s6-reconcile-rate-limit -m "merge: s6-reconcile-rate-limit——flush 记账热路径降频对账化（门控+计数+漂移WARN）；测试基线全绿"
```

### Task 5: rs-iot 侧回归 + 部署（主目录 master 合并后）

**Files:** 无 rs-iot 代码改动（S6 纯引擎内部，无 API 变化）。

**Step 1:** `cd /root/myprojects/webhmi/rs-iot && cargo test -p rs-iot --features history 2>&1 | tail -3`
Expected: 871+ 全绿（基线 871，不新增失败）
**Step 2:** `cargo build --release -p rs-iot --features history` → 优雅重启（SIGINT；若 >60s 未退按既定预案 SIGTERM→SIGKILL，WAL 兜底已实证）→ sysstatus 验证 `engine=ok health=ok errors=0`
**Step 3:** 提交推送 tsink：`git push origin master`（走既有代理配置，http.version=HTTP/1.1 已设）

---

## 后续（不在本计划执行面，登记）

1. **r8 bench 复验**（6h，10 万点环境）：L0 斜率 / 吞吐窗 / `memory_reconciles_total` 频率观测（预期 ~120 次/小时 → 2 次/小时）——需压测环境（Windows bench 基建或本机新建 Linux bench，另行立项）
2. **soak 第 5 次**（24h+，判据 §五-J 清单 + L0 全程斜率 ≤10/h）
3. 若 r8 后斜率仍 >10/h：S6 排除，转下一候选（N7 close 排空 / 方案 C 两头免 zstd）

## 风险与回退（承接方案原表 + 审查补充）

| 风险 | 缓解 |
|---|---|
| 增量漂移在两次对账间累积 → 过度/不足逐出 | 30s 对账锚点仍在；双门槛 WARN；对账测试网 + Task 3 回归保护 |
| 首次 persist 即被门控（elapsed≈0）→ 启动初期无校准 | bootstrap 锚点（finalize.rs:66）已全量校准——设计如此，非缺陷 |
| 门控写死 30s 无法短窗测试 | 测试直接回拨 `last_full_reconcile_ms` 原子量（pub(crate) 可达），不扩配置面 |
| utilities.rs 注释与行为矛盾 | Task 2 显式同步（审查新增义务） |
| 降频后 L0 斜率未达标 | r8 数据说话；S6 排除后转下一候选 |
| 回退 | 单语义 revert 即回每次-persist 行为；无数据格式变更 |
