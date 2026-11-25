// 可见性栅栏读侧有上限等待（2026-11-25 现场修复）的回归测试。
//
// 现场症状：tsink 存储引擎的可见性栅栏（发布拍持写锁）一旦卡住（锁序反转 /
// 落盘 I/O 阻塞 / 线程异常），**所有** history 读会永久阻塞——写入仍在推进，
// sysstatus 仍报 engine:"ok"，运维只能重启进程（2026-11-25 本机现场）。
//
// 修复：读侧改为**有上限**等待（visibility_read_fence_bounded），超预算返回错误，
// 读不再被写永久拖死；"永久无历史数据"因此消除（表现为显式错误，而非静默挂死）。
//
// 本测试用"人为持有栅栏写锁"复现写侧卡住：读必须在预算内返回错误，而不是挂死。

use super::*;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn storage_with_label(temp_dir: &TempDir, _labels: Vec<Label>) -> Arc<ChunkStorage> {
    Arc::new(
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
    )
}

#[test]
fn read_returns_error_instead_of_blocking_forever_when_publication_holds_fence() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = storage_with_label(&temp_dir, labels.clone());

    storage
        .insert_rows(&[Row::with_labels(
            "fence_busy_metric",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    // 模拟"发布拍卡住"：后台线程持有可见性栅栏**写锁**不放。
    // 直接借用引擎内部的栅栏锁（测试在 crate 内，可访问私有字段）；用 Arc 传递，
    // 避免裸指针跨线程（Send 约束）。
    let holder_storage = Arc::clone(&storage);
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = thread::spawn(move || {
        let _guard = holder_storage.visibility.flush_visibility_lock.write();
        // 阻塞直到测试结束；期间任何读都不得挂死。
        let _ = release_rx.recv();
    });

    // 给写锁一点时间，确保读侧确实撞上持锁的写者。
    thread::sleep(Duration::from_millis(50));

    let started = Instant::now();
    let result = storage.select("fence_busy_metric", &labels, 0, 10);
    let elapsed = started.elapsed();

    let _ = release_tx.send(());
    holder.join().unwrap();

    // 读必须在有限时间内返回（预算 5s + 余量），且是错误而不是 Ok。
    assert!(
        elapsed < Duration::from_secs(8),
        "读不得无限阻塞（实际耗时 {elapsed:?}）"
    );
    let err = result.expect_err("写锁被长期持有时读必须返回错误，不能挂死");
    assert!(
        format!("{err}").contains("visibility fence busy"),
        "错误应说明是可见性栅栏忙，实际：{err}"
    );
}
