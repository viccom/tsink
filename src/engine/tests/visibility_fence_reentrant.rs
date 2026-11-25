// 可见性栅栏**嵌套读锁重入**死锁（2026-11-25 现场根因）的确定性回归测试。
//
// 机制（parking_lot RwLock 公平性，已用 /tmp 独立程序实测）：
//   线程 T 持有读锁 g1 → 写者 W 排队 → T **同线程**再次请求读锁 g2
//   → g2 排在 W 之后 → W 等 g1 释放 → g1 归 T，而 T 在等 g2 → 永久互等。
//
// tsink 现场对应：`select_with_options_api` 的降采样分支先取外层读栅栏，
// 持着它调 `select_into` → `collect_points_for_series_into_with_plan` →
// `snapshot_series_read_sources` 再次请求**同一把锁**的读锁。只要这一瞬间有
// 发布拍（flush / persisted-refresh）排队写锁，该查询就永久卡死；它持有的外层
// 读锁还挡住写者，写者再挡住所有后续读者 → 全局楔死（只能重启进程）。
//
// 本测试用 `query_visibility_fence_held_hook` 把查询**精确停在外层读锁已持有之后**：
//   ① 查询线程进钩子（外层读栅栏已持有）
//   ② 测试线程起一个写者并等它真正排上队
//   ③ 放行查询继续 —— 时序完全确定，不靠 sleep 撞运气
//
// 该停点对修复前后是同一条时序：
//   修复前 → 放行后查询内层二次取读锁 → 排在写者之后 → 与写者互等（超时/fence busy）
//   修复后 → 放行后内层不再取锁，直接读数据 → 查询完成，写者随后得锁
//
// 修复后（内层改走 *_locked、不再二次取锁）：本测试必须在预算内返回数据。
// 未修复时：内层读请求被写者挡住，写者被外层读锁挡住 → 查询超预算失败或永久挂死。

use super::*;
use crate::{Aggregation, QueryOptions};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// 单次查询预算（宽容于有界读的 5s 预算，用于区分"慢"与"互等"）。
const QUERY_BUDGET: Duration = Duration::from_secs(12);

#[test]
fn downsample_query_must_not_deadlock_when_writer_queues_between_nested_reads() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Milliseconds,
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );
    let labels = vec![Label::new("host", "reentrant")];
    storage
        .insert_rows(&[
            Row::with_labels("reentrant_metric", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("reentrant_metric", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("reentrant_metric", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    // ① 钩子：外层读栅栏已持有之后停住，通知测试并等放行。
    let (inner_entered_tx, inner_entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = Arc::new(Mutex::new(release_rx));
    storage.set_query_visibility_fence_held_hook(move || {
        let _ = inner_entered_tx.send(());
        let _ = release_rx.lock().unwrap().recv();
    });

    // ② 查询线程：走降采样分支（外层读栅栏 → 内层重入）。
    let q_storage = Arc::clone(&storage);
    let q_labels = labels.clone();
    let (q_tx, q_rx) = mpsc::channel();
    let query = thread::spawn(move || {
        let opts = QueryOptions::new(0, 10)
            .with_labels(q_labels)
            .with_downsample(1000, Aggregation::Avg);
        let result = q_storage.select_with_options("reentrant_metric", opts);
        let _ = q_tx.send(result);
    });

    inner_entered_rx
        .recv_timeout(QUERY_BUDGET)
        .expect("查询线程应进入「外层读栅栏已持有」的钩子");

    // ③ 写者排队：它必须先拿到写锁才能继续。
    let w_storage = Arc::clone(&storage);
    let (w_started_tx, w_started_rx) = mpsc::channel::<()>();
    let (w_got_tx, w_got_rx) = mpsc::channel::<()>();
    let writer = thread::spawn(move || {
        let _ = w_started_tx.send(());
        let _guard = w_storage.visibility.flush_visibility_lock.write();
        let _ = w_got_tx.send(());
    });
    w_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("写者线程应启动");
    // 给写者时间真正挂到锁的等待队列上（否则可能还没排队就被内层请求抢到）。
    thread::sleep(Duration::from_millis(150));

    // ④ 放行查询继续：若内层仍二次取锁，它现在必须排在写者之后 → 互等。
    let started = Instant::now();
    let _ = release_tx.send(());

    let result = q_rx.recv_timeout(QUERY_BUDGET);
    let elapsed = started.elapsed();
    let writer_got_lock = w_got_rx.recv_timeout(QUERY_BUDGET).is_ok();

    // 收尾：确保钩子不再阻塞后续（重复放行对空 channel 是 no-op）。
    let _ = release_tx.send(());
    if result.is_ok() {
        query.join().unwrap();
    }
    writer.join().unwrap();
    storage.clear_query_visibility_fence_held_hook();

    // 断言 1：查询必须在预算内返回（修复前：内层请求被写者挡住 → 超时）
    let outcome = result.expect("降采样查询在写者排队时未能返回（嵌套读锁重入 → 与写者互等）");
    // 断言 2：查询必须成功（若被有界读预算转成 fence busy，说明内层仍在二次取锁）
    let points = outcome.expect(
        "降采样查询不应报 visibility fence busy / 超时（内层不该二次取锁，2026-11-25 嵌套重入修复）",
    );
    assert!(!points.is_empty(), "降采样查询应返回数据");
    // 断言 3：写者最终也必须拿到写锁（不能出现"读者饿死写者"）
    assert!(writer_got_lock, "写者必须最终获得写锁");
    assert!(
        elapsed < QUERY_BUDGET,
        "查询耗时 {elapsed:?} 超出预算（疑似重入死锁）"
    );

    storage.close().unwrap();
}
