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
