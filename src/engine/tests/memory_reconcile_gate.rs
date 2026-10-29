use std::path::Path;
use std::sync::atomic::Ordering;

use super::*;

/// S6 main probe: several consecutive successful persists within one
/// reconcile window must NOT trigger full O(series) recomputes on the flush
/// hot path. Before the rate limit lands this fails with one full reconcile
/// per persist; once gated, only the construction-time anchor has calibrated
/// the accounting inside the test window, so the flush-path reconcile count
/// stays at zero.
#[test]
fn flush_persist_hot_path_reconciles_at_most_once_per_window() {
    let temp_dir = TempDir::new().unwrap();
    let storage = test_storage_with_memory_budget(temp_dir.path());
    for round in 0..5i64 {
        write_reconcile_gate_rows(&storage, round * 10);
        storage.flush_all_active().unwrap();
        let outcome = storage.persist_segment_with_outcome().unwrap();
        assert!(
            outcome.persisted,
            "test precondition: every persist must publish, otherwise the rate-limit assertion is meaningless"
        );
    }
    let reconciles = storage.flush_memory_reconciles_total();
    assert_eq!(
        reconciles, 0,
        "flush hot path must not run full reconciles inside the 30s gate window"
    );
}

/// Expiry trigger: dialing `last_full_reconcile_ms` back to the epoch
/// simulates an elapsed interval, so the next persist must reconcile exactly
/// once, and the persist right after must stay gated inside the fresh window.
#[test]
fn flush_persist_reconciles_once_when_interval_elapsed() {
    let temp_dir = TempDir::new().unwrap();
    let storage = test_storage_with_memory_budget(temp_dir.path());
    write_reconcile_gate_rows(&storage, 0);
    // Dial back to the unix epoch: the reconcile interval has "elapsed".
    storage
        .memory
        .last_full_reconcile_ms
        .store(0, Ordering::Relaxed);
    storage.flush_all_active().unwrap();
    storage.persist_segment_with_outcome().unwrap();
    assert_eq!(storage.flush_memory_reconciles_total(), 1);
    // The very next persist stays inside the freshly opened window.
    write_reconcile_gate_rows(&storage, 10);
    storage.flush_all_active().unwrap();
    storage.persist_segment_with_outcome().unwrap();
    assert_eq!(storage.flush_memory_reconciles_total(), 1);
}

/// `memory_budget_bytes != u64::MAX` enables accounting (assembly.rs), with
/// background threads off so persists only happen through the test driver.
fn test_storage_with_memory_budget(root: &Path) -> ChunkStorage {
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.memory_budget_bytes = 1_000_000;
    ChunkStorage::new_with_data_path_and_options(
        4,
        None,
        Some(root.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        options,
    )
    .unwrap()
}

fn write_reconcile_gate_rows(storage: &ChunkStorage, base_ts: i64) {
    let labels = vec![Label::new("host", "a")];
    storage
        .insert_rows(&[
            Row::with_labels(
                "memory_reconcile_gate",
                labels.clone(),
                DataPoint::new(base_ts + 1, 1.0),
            ),
            Row::with_labels(
                "memory_reconcile_gate",
                labels.clone(),
                DataPoint::new(base_ts + 2, 2.0),
            ),
            Row::with_labels(
                "memory_reconcile_gate",
                labels,
                DataPoint::new(base_ts + 3, 3.0),
            ),
        ])
        .unwrap();
}

/// Regression protection (not a TDD red test — it would also pass with more
/// frequent reconciliation): while the gate skips, incremental accounting
/// stays authoritative, and once the interval elapses the reconciliation
/// pass lands on the same values (incremental == full recompute).
#[test]
fn sampled_window_keeps_incremental_authoritative_and_reconciles_equal() {
    let temp_dir = TempDir::new().unwrap();
    let storage = test_storage_with_memory_budget(temp_dir.path());
    write_reconcile_gate_rows(&storage, 0);
    storage.flush_all_active().unwrap();
    storage.persist_segment_with_outcome().unwrap(); // gated skip (fresh window)
    assert_engine_memory_usage_reconciled(&storage); // incremental == forced full
    write_reconcile_gate_rows(&storage, 30); // keep writing inside the window
    storage
        .memory
        .last_full_reconcile_ms
        .store(0, Ordering::Relaxed);
    storage.flush_all_active().unwrap();
    storage.persist_segment_with_outcome().unwrap(); // interval elapsed -> reconcile once
    assert_engine_memory_usage_reconciled(&storage); // still equal
    assert!(storage.flush_memory_reconciles_total() >= 1);
}

/// 生产故障复现（2026-10-29，rs-iot 归档停摆）：
///
/// 症状——rs-iot 侧 `history.stats.errors_by_kind.memory_budget` 持续增长、
/// `written` 归零、归档完全停摆；而 `sysstatus.history.memory_used_bytes` 报出
/// 2^64-240 的**下溢巨值**（u64 计数器被 fetch_sub 减过了头）。
///
/// 机制链：
///   ① 增量记账与 prune 的**口径不对称**（写入按增量加、prune 按全量 capacity 减）
///      → used 计数器长期 sub > add
///   ② `fetch_sub` 非饱和 → u64 下溢成巨值
///   ③ 预算检查 `used > budget` 恒真 → 写入被 memory_budget 全拒
///   ④ 无数据 seal → flush 无 persist → reconcile 不执行 → used 无法自愈
///      （死锁：used 永远停在巨值，归档永久停摆）
///
/// 本测试锁住两件事：
///   A. used 计数器**不得下溢**（饱和减）——下溢会把「记账漂移」放大成「永久停摆」
///   B. 预算拒写前必须先做一次全量 reconcile（真实值判断）——打破 ④ 的死锁
#[test]
fn memory_used_counter_never_underflows_into_huge_value() {
    let temp_dir = TempDir::new().unwrap();
    let storage = test_storage_with_memory_budget(temp_dir.path());
    // 人为制造「sub 多于 add」的记账漂移：走受保护的 sub 路径减一个远大于当前值的量
    let before = storage.memory.used_bytes.load(Ordering::Acquire);
    storage
        .sub_memory_usage_bytes(0, (before as usize).saturating_add(1_000_000));
    // 修复后：饱和减挡住越界，值落在 0 附近而非巨值
    let after = storage.memory.used_bytes.load(Ordering::Acquire);
    assert!(
        after < 1_000_000,
        "used_bytes 下溢成巨值（{after}）——预算检查将恒真，归档永久停摆"
    );
}

/// 预算检查的自愈：used 计数器漂移/下溢后，写入路径必须先 reconcile 再判断，
/// 而不是拿着陈旧（巨值）计数把写入全拒。
#[test]
fn admission_reconciles_before_rejecting_on_memory_budget() {
    let temp_dir = TempDir::new().unwrap();
    let storage = test_storage_with_memory_budget(temp_dir.path());
    // 制造记账漂移：把 used 顶到预算之上（模拟下溢后的巨值效果）
    storage
        .memory
        .used_bytes
        .store(u64::MAX - 10, Ordering::Release);
    // 写入必须成功——准入路径先 reconcile 回真实值（远小于预算）
    write_reconcile_gate_rows(&storage, 0);
    storage.flush_all_active().unwrap();
    storage
        .persist_segment_with_outcome()
        .expect("记账漂移不得导致写入被永久拒（归档停摆）");
}
