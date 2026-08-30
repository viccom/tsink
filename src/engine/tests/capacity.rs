use super::*;
use crate::engine::series::SeriesKey;

fn new_memory_budget_test_storage(temp_dir: &TempDir, memory_budget_bytes: u64) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: None,
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap()
}

fn registry_growth_estimate_for_rows(storage: &ChunkStorage, rows: &[Row]) -> usize {
    let planned_series = rows
        .iter()
        .map(|row| SeriesKey {
            metric: row.metric().to_string(),
            labels: row.labels().to_vec(),
        })
        .collect::<Vec<_>>();
    storage
        .catalog
        .registry
        .read()
        .estimate_new_series_memory_growth_bytes(&planned_series)
        .unwrap()
}

#[test]
fn memory_budget_spills_to_l0_and_preserves_query_results() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = new_memory_budget_test_storage(&temp_dir, 64 * 1024);

    storage
        .insert_rows(&[
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(4, 4.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(5, 5.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(6, 6.0)),
        ])
        .unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    let snapshot = storage.observability_snapshot();
    assert!(
        snapshot.memory.active_and_sealed_bytes > 0,
        "test workload should include budgeted hot chunk state"
    );
    let tightened_budget = snapshot
        .memory
        .budgeted_bytes
        .saturating_sub(snapshot.memory.active_and_sealed_bytes)
        .saturating_add(1);
    storage
        .memory
        .budget_bytes
        .store(tightened_budget as u64, std::sync::atomic::Ordering::SeqCst);
    storage.enforce_memory_budget_if_needed().unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let l0_segments = load_segments_for_level(&lane_path, 0).unwrap();
    assert!(
        !l0_segments.is_empty(),
        "budget pressure should flush sealed chunks to L0 before close"
    );

    let points = storage.select("budget_metric", &labels, 0, 10).unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(4, 4.0),
            DataPoint::new(5, 5.0),
            DataPoint::new(6, 6.0),
        ]
    );
    assert_eq!(storage.memory_budget(), tightened_budget);
    let post_snapshot = storage.observability_snapshot();
    assert!(
        post_snapshot.memory.active_and_sealed_bytes < snapshot.memory.active_and_sealed_bytes,
        "spill pressure should reduce budgeted hot chunk state even when shared metadata remains"
    );

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("budget_metric", &labels)
        .unwrap()
        .series_id;
    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let sealed_count = sealed
        .get(&series_id)
        .map(|chunks| chunks.len())
        .unwrap_or(0);
    assert!(
        sealed_count < 3,
        "oldest sealed chunks should be evicted after spill"
    );

    storage.close().unwrap();
}

#[test]
fn reduced_hot_chunk_state_fits_previous_spill_budget() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = new_memory_budget_test_storage(&temp_dir, 64 * 1024);

    storage
        .insert_rows(&[
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(4, 4.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(5, 5.0)),
            Row::with_labels("budget_metric", labels.clone(), DataPoint::new(6, 6.0)),
        ])
        .unwrap();

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let l0_segments = load_segments_for_level(&lane_path, 0).unwrap();
    assert!(
        l0_segments.is_empty(),
        "encoded-only sealed chunks should keep this workload under the previous spill budget"
    );

    assert_eq!(
        storage.select("budget_metric", &labels, 0, 10).unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(4, 4.0),
            DataPoint::new(5, 5.0),
            DataPoint::new(6, 6.0),
        ]
    );
    assert!(storage.memory_used() <= storage.memory_budget());

    storage.close().unwrap();
}

#[test]
fn memory_budget_stats_reflect_builder_configuration() {
    let storage = StorageBuilder::new()
        .with_memory_limit(1234)
        .build()
        .unwrap();
    let snapshot = storage.observability_snapshot();

    assert_eq!(storage.memory_budget(), 1234);
    assert_eq!(storage.memory_used(), snapshot.memory.budgeted_bytes);
    assert_eq!(
        snapshot.memory.excluded_bytes,
        snapshot
            .memory
            .registry_bytes
            .saturating_add(snapshot.memory.metadata_cache_bytes)
            .saturating_add(snapshot.memory.persisted_index_bytes)
            .saturating_add(snapshot.memory.persisted_mmap_bytes)
            .saturating_add(snapshot.memory.tombstone_bytes),
        "excluded_bytes must always aggregate the non-budgeted components"
    );
}

#[test]
fn observability_memory_component_snapshot_is_readable_from_storage_trait_face() {
    // 2026-08-29 load-test follow-up: two aggregate gauges (used/budget) could
    // not attribute admission mis-rejections. Pin the per-component memory
    // breakdown fields and their availability through the `dyn Storage` face
    // (the rs-iot consumption shape), plus the budgeting scope where the
    // criterion covers only active+sealed state and the other components are
    // excluded from `budgeted_bytes` but aggregated in `excluded_bytes`.
    let storage: Arc<dyn Storage> = StorageBuilder::new()
        .with_wal_enabled(false)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    for i in 0..4 {
        storage
            .insert_rows(&[Row::with_labels(
                "component_snapshot_metric",
                vec![Label::new("host", format!("h{i}"))],
                DataPoint::new(1, i as f64),
            )])
            .unwrap();
    }

    let memory = &storage.observability_snapshot().memory;
    assert!(
        memory.registry_bytes > 0,
        "registered series must show up in the registry component"
    );
    assert!(
        memory.active_and_sealed_bytes > 0,
        "hot chunk state must show up in the active+sealed component"
    );
    assert_eq!(memory.budgeted_bytes, storage.memory_used());
    assert_eq!(
        memory.budgeted_bytes, memory.active_and_sealed_bytes,
        "the budget criterion must cover only evictable active+sealed state"
    );
    assert_eq!(
        memory.excluded_bytes,
        memory
            .registry_bytes
            .saturating_add(memory.metadata_cache_bytes)
            .saturating_add(memory.persisted_index_bytes)
            .saturating_add(memory.persisted_mmap_bytes)
            .saturating_add(memory.tombstone_bytes),
        "excluded_bytes must aggregate the non-budgeted components"
    );
}

#[test]
fn used_bytes_criterion_covers_only_active_and_sealed_state() {
    // Task 7 (2026-08-29 load-test follow-up): `memory_used`/`used_bytes` is the
    // admission criterion, so it must track only what the budget can evict —
    // active partition heads plus sealed chunks pending flush. Registry
    // dictionaries, metadata caches, the persisted index and mmap-backed
    // segments are process-RSS facts: they stay visible through the
    // observability component fields instead of raising the budget criterion
    // (the monotonically growing persisted-footprint floor that wedged
    // admission after restart in the 100k-series load test).
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_enabled(false)
            .with_current_time_override_for_tests(0)
            .build()
            .unwrap();
        for series_idx in 0..64 {
            storage
                .insert_rows(&[Row::with_labels(
                    "used_scope_metric",
                    vec![
                        Label::new("host", format!("host-{series_idx}")),
                        Label::new("rack", format!("rack-{}", series_idx % 8)),
                    ],
                    DataPoint::new(1, series_idx as f64),
                )])
                .unwrap();
        }
        let live_snapshot = storage.observability_snapshot();
        assert!(
            live_snapshot.memory.registry_bytes > 0,
            "registered series must show up in the registry component"
        );
        assert!(
            live_snapshot.memory.active_and_sealed_bytes > 0,
            "sealed chunks pending flush must show up in the active+sealed component"
        );
        assert_eq!(
            live_snapshot.memory.budgeted_bytes, live_snapshot.memory.active_and_sealed_bytes,
            "while writing, the budget criterion must cover only evictable active+sealed state"
        );
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_memory_limit(1_000_000)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    let snapshot = reopened.observability_snapshot();
    assert!(
        snapshot.memory.registry_bytes > 0,
        "reopened storage should account for persisted registry state"
    );
    assert!(
        snapshot.memory.persisted_index_bytes > 0,
        "reopened storage should account for persisted chunk refs and timestamp indexes"
    );
    assert!(
        snapshot.memory.persisted_mmap_bytes > 0,
        "reopened storage should account for mmap-backed segment state"
    );
    assert_eq!(
        reopened.memory_used(),
        0,
        "reopening loads registry/index/mmap but no in-memory chunks: the persisted footprint must not raise the budget criterion"
    );
    assert_eq!(
        snapshot.memory.budgeted_bytes, snapshot.memory.active_and_sealed_bytes,
        "the budget criterion must stay scoped to active+sealed after restart"
    );

    reopened.close().unwrap();
}

#[test]
fn registry_heavy_admission_rejection_exports_estimate_and_shortfall() {
    // Registry-heavy new-series rejections take the resolve-path admission
    // check; they must feed the same exported estimate/shortfall snapshot as
    // batch-estimate rejections so scrapers cannot tell the paths apart.
    let temp_dir = TempDir::new().unwrap();
    let storage = new_memory_budget_test_storage(&temp_dir, 2_048);

    let large_label_value = "x".repeat(4_096);
    let rows = vec![Row::with_labels(
        "registry_admission_gap_metric",
        vec![Label::new("host", large_label_value)],
        DataPoint::new(1, 1.0),
    )];
    let estimated_registry_growth = registry_growth_estimate_for_rows(&storage, &rows);
    assert!(
        estimated_registry_growth > 2_048,
        "4KiB label value must overflow the 2KiB budget by registry growth alone"
    );

    let err = storage.insert_rows(&rows).unwrap_err();
    let TsinkError::MemoryBudgetExceeded { budget, required } = err else {
        panic!("expected registry-heavy memory budget rejection, got {err:?}");
    };
    assert_eq!(budget, 2_048);

    let dyn_storage: &dyn Storage = &storage;
    let snapshot = dyn_storage.observability_snapshot();
    assert_eq!(snapshot.flush.admission_memory_rejections_total, 1);
    assert_eq!(
        snapshot.flush.admission_memory_last_estimate_bytes as usize,
        estimated_registry_growth
    );
    assert_eq!(
        snapshot.flush.admission_memory_last_shortfall_bytes as usize,
        required - budget
    );
}

#[test]
fn memory_budget_guard_rejects_writes_when_in_memory_budget_cannot_be_relaxed() {
    let storage = StorageBuilder::new()
        .with_wal_enabled(false)
        .with_memory_limit(1)
        .with_write_timeout(Duration::ZERO)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();

    let err = storage
        .insert_rows(&[Row::new("memory_guard_metric", DataPoint::new(1, 1.0))])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::MemoryBudgetExceeded { budget: 1, .. }
    ));
    assert!(
        storage
            .select("memory_guard_metric", &[], 0, 10)
            .unwrap()
            .is_empty(),
        "rejected writes must not mutate in-memory state"
    );
}

#[test]
fn memory_budget_rejects_registry_heavy_new_series_before_mutating_registry() {
    let temp_dir = TempDir::new().unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 1,
            write_timeout: Duration::ZERO,
            memory_budget_bytes: 2_048,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: None,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    let large_label_value = "x".repeat(4_096);
    let labels = vec![Label::new("host", large_label_value.clone())];
    let rows = vec![Row::with_labels(
        "registry_budget_metric",
        labels.clone(),
        DataPoint::new(1, 1.0),
    )];
    let estimated_registry_growth = registry_growth_estimate_for_rows(&storage, &rows);
    assert!(estimated_registry_growth > 0);
    storage.memory.budget_bytes.store(
        estimated_registry_growth.saturating_sub(1) as u64,
        std::sync::atomic::Ordering::Release,
    );

    let err = storage.insert_rows(&rows).unwrap_err();
    assert!(
        matches!(
            err,
            TsinkError::MemoryBudgetExceeded {
                budget,
                required
            } if budget + 1 == estimated_registry_growth && required > budget
        ),
        "unexpected error: {err:?}"
    );

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.memory.registry_bytes, 0);
    assert_eq!(
        snapshot.memory.active_and_sealed_bytes, 0,
        "admission should reject before active or sealed chunk state is created"
    );
    assert!(storage.catalog.registry.read().is_empty());
    assert!(
        storage
            .select("registry_budget_metric", &labels, 0, 10)
            .unwrap()
            .is_empty(),
        "rejected registry-heavy writes must not ingest points"
    );

    storage.close().unwrap();
}

#[test]
fn memory_budget_rejects_many_new_metrics_before_mutating_registry() {
    let temp_dir = TempDir::new().unwrap();
    let storage = new_memory_budget_test_storage(&temp_dir, usize::MAX as u64);

    let rows = (0..32)
        .map(|idx| {
            Row::with_labels(
                format!("metric_{idx:02}"),
                vec![Label::new("host", "shared")],
                DataPoint::new(1, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    let estimated_registry_growth = registry_growth_estimate_for_rows(&storage, &rows);
    assert!(estimated_registry_growth > 0);
    storage.memory.budget_bytes.store(
        estimated_registry_growth.saturating_sub(1) as u64,
        std::sync::atomic::Ordering::Release,
    );

    let err = storage.insert_rows(&rows).unwrap_err();
    assert!(
        matches!(err, TsinkError::MemoryBudgetExceeded { .. }),
        "unexpected error: {err:?}"
    );

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.memory.registry_bytes, 0);
    assert_eq!(snapshot.memory.active_and_sealed_bytes, 0);
    assert!(storage.catalog.registry.read().is_empty());
    assert!(storage.materialized_series_snapshot().is_empty());

    storage.close().unwrap();
}

#[test]
fn memory_budget_rejects_many_new_label_pairs_before_mutating_registry() {
    let temp_dir = TempDir::new().unwrap();
    let storage = new_memory_budget_test_storage(&temp_dir, usize::MAX as u64);

    let rows = (0..32)
        .map(|idx| {
            Row::with_labels(
                "label_heavy_metric",
                vec![
                    Label::new("host", format!("node-{idx:02}")),
                    Label::new("rack", format!("rack-{}", idx % 8)),
                    Label::new(format!("dynamic_label_{idx:02}"), format!("value_{idx:02}")),
                ],
                DataPoint::new(1, idx as f64),
            )
        })
        .collect::<Vec<_>>();
    let estimated_registry_growth = registry_growth_estimate_for_rows(&storage, &rows);
    assert!(estimated_registry_growth > 0);
    storage.memory.budget_bytes.store(
        estimated_registry_growth.saturating_sub(1) as u64,
        std::sync::atomic::Ordering::Release,
    );

    let err = storage.insert_rows(&rows).unwrap_err();
    assert!(
        matches!(err, TsinkError::MemoryBudgetExceeded { .. }),
        "unexpected error: {err:?}"
    );

    let snapshot = storage.observability_snapshot();
    assert_eq!(snapshot.memory.registry_bytes, 0);
    assert_eq!(snapshot.memory.active_and_sealed_bytes, 0);
    assert!(storage.catalog.registry.read().is_empty());
    assert!(storage.materialized_series_snapshot().is_empty());

    storage.close().unwrap();
}

#[test]
fn restart_memory_reconciliation_counts_persisted_registry_and_index_state() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_enabled(false)
            .with_current_time_override_for_tests(0)
            .build()
            .unwrap();

        for series_idx in 0..64 {
            storage
                .insert_rows(&[Row::with_labels(
                    "reopen_budget_metric",
                    vec![
                        Label::new("host", format!("host-{series_idx}")),
                        Label::new("rack", format!("rack-{}", series_idx % 8)),
                    ],
                    DataPoint::new(1, series_idx as f64),
                )])
                .unwrap();
        }

        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_memory_limit(1_000_000)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    let snapshot = reopened.observability_snapshot();
    let reconciled_used = reopened.memory_used();

    assert_eq!(snapshot.memory.budgeted_bytes, reconciled_used);
    assert!(
        snapshot.memory.registry_bytes > 0,
        "reopened storage should account for persisted registry state"
    );
    assert!(
        snapshot.memory.persisted_index_bytes > 0,
        "reopened storage should account for persisted chunk refs and timestamp indexes"
    );
    assert!(
        snapshot.memory.persisted_mmap_bytes > 0,
        "reopened storage should account for persisted mmap-backed segment state"
    );
    assert!(
        snapshot.memory.excluded_bytes > 0,
        "reopened persisted registry/index/mmap state must surface in the excluded aggregate"
    );
    assert_eq!(
        snapshot.memory.excluded_persisted_mmap_bytes, snapshot.memory.persisted_mmap_bytes,
        "all persisted mmap bytes are excluded from the budget criterion"
    );

    let tightened_budget = 1;
    reopened.close().unwrap();

    let over_budget = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_memory_limit(tightened_budget)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    let over_snapshot = over_budget.observability_snapshot();
    assert!(
        over_snapshot.memory.registry_bytes > 0
            && over_snapshot.memory.persisted_index_bytes > 0
            && over_snapshot.memory.persisted_mmap_bytes > 0,
        "restart reconciliation should expose the full persisted footprint through excluded components even under a tiny budget"
    );
    assert_eq!(
        over_budget.memory_used(),
        over_snapshot.memory.active_and_sealed_bytes,
        "the persisted footprint must not raise the budget criterion after restart"
    );
    over_budget.close().unwrap();
}

#[test]
fn restart_query_budget_counts_persisted_mmap_and_rejects_new_writes() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_enabled(false)
            .with_current_time_override_for_tests(0)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels(
                    "restart_query_budget_metric",
                    labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "restart_query_budget_metric",
                    labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();

        storage.close().unwrap();
    }

    let baseline = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_memory_limit(1_000_000)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    let baseline_snapshot = baseline.observability_snapshot();
    assert!(
        baseline_snapshot.memory.persisted_mmap_bytes > 0,
        "reopened storage should surface persisted mmap bytes in the excluded footprint"
    );
    assert_eq!(
        baseline_snapshot.memory.excluded_persisted_mmap_bytes,
        baseline_snapshot.memory.persisted_mmap_bytes,
        "persisted mmap bytes are excluded from the budget criterion"
    );
    let tightened_budget = baseline.memory_used().saturating_sub(1).max(1);
    baseline.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_memory_limit(tightened_budget)
        .with_write_timeout(Duration::ZERO)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    let reopened_snapshot = reopened.observability_snapshot();

    assert!(
        reopened_snapshot.memory.persisted_mmap_bytes > 0,
        "persisted mmap bytes should stay visible after reopening under a tightened budget"
    );
    assert_eq!(
        reopened.memory_used(),
        reopened_snapshot.memory.active_and_sealed_bytes,
        "persisted mmap bytes must not raise the budget criterion after restart"
    );
    assert_eq!(
        reopened
            .select("restart_query_budget_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
    );

    let err = reopened
        .insert_rows(&[Row::with_labels(
            "restart_query_budget_metric",
            labels.clone(),
            DataPoint::new(3, 3.0),
        )])
        .unwrap_err();
    assert!(
        matches!(
            err,
            TsinkError::MemoryBudgetExceeded { budget, required }
                if budget == tightened_budget && required > budget
        ),
        "unexpected error: {err:?}"
    );
    assert_eq!(
        reopened
            .select("restart_query_budget_metric", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
        "budget rejection after restart/query must not mutate persisted data",
    );

    reopened.close().unwrap();
}

#[test]
fn runtime_persisted_segment_load_updates_memory_budget_accounting() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_chunk_points(1)
            .with_wal_enabled(false)
            .with_current_time_override_for_tests(0)
            .build()
            .unwrap();

        for series_idx in 0..16 {
            storage
                .insert_rows(&[Row::with_labels(
                    "refresh_budget_metric",
                    vec![Label::new("host", format!("node-{series_idx}"))],
                    DataPoint::new(1, series_idx as f64),
                )])
                .unwrap();
        }

        storage.close().unwrap();
    }

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let loaded = load_segment_indexes(&lane_path).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(lane_path),
        None,
        loaded.next_segment_id,
        ChunkStorageOptions {
            memory_budget_bytes: 1_000_000,
            background_threads_enabled: false,
            background_fail_fast: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .add_persisted_segments_from_loaded(loaded.indexed_segments)
        .unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    let snapshot = storage.observability_snapshot();
    assert!(
        snapshot.memory.persisted_index_bytes > 0,
        "runtime persisted refresh should update the persisted-index component"
    );
    assert!(
        snapshot.memory.persisted_mmap_bytes > 0,
        "runtime persisted refresh should account for mmap-backed bytes"
    );
    assert!(
        snapshot.memory.excluded_bytes > 0,
        "runtime adopted persisted state must surface in the excluded aggregate"
    );
    assert_eq!(
        snapshot.memory.excluded_persisted_mmap_bytes, snapshot.memory.persisted_mmap_bytes,
        "runtime adopted mmap bytes are excluded from the budget criterion"
    );

    let tightened_budget = storage.memory_used().saturating_sub(1).max(1);
    storage.memory.budget_bytes.store(
        tightened_budget as u64,
        std::sync::atomic::Ordering::Release,
    );
    storage.enforce_memory_budget_if_needed().unwrap();
    assert_engine_memory_usage_reconciled(&storage);
    let post_tighten_snapshot = storage.observability_snapshot();
    assert!(
        post_tighten_snapshot.memory.persisted_index_bytes > 0
            && post_tighten_snapshot.memory.persisted_mmap_bytes > 0,
        "persisted-index accounting should remain visible after runtime segment adoption"
    );
    assert_eq!(
        storage.memory_used(),
        post_tighten_snapshot.memory.active_and_sealed_bytes,
        "runtime adopted persisted state must not raise the budget criterion"
    );

    storage.close().unwrap();
}

#[test]
fn delete_series_tombstone_accounting_reconciles_with_full_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = new_memory_budget_test_storage(&temp_dir, 1_000_000);

    storage
        .insert_rows(&[
            Row::with_labels(
                "delete_budget_metric",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "delete_budget_metric",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    let deleted = storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("delete_budget_metric")
                .with_matcher(SeriesMatcher::equal("host", "a")),
        )
        .unwrap();
    assert_eq!(deleted.matched_series, 1);
    assert_eq!(deleted.tombstones_applied, 1);
    assert!(
        storage.observability_snapshot().memory.tombstone_bytes > 0,
        "delete should budget the persisted tombstone footprint",
    );
    assert_engine_memory_usage_reconciled(&storage);

    storage.close().unwrap();
}

#[test]
fn visibility_state_batch_writes_perform_no_full_memory_estimation() {
    // Task 13 (2026-08-29 load-test follow-up): the three global visibility
    // caches used to be fully re-measured (an O(cached-series) scan over
    // every summary's ranges Vec) before AND after each per-shard cache
    // merge on the write path — roughly 390 entry accesses per row at 100k
    // series, executed under the recency mutex and all three cache write
    // locks. Steady-state batch writes must keep the full-estimation probe
    // at exactly zero; the full recompute stays reserved for the
    // reconciliation face (`refresh_memory_usage`).
    let temp_dir = TempDir::new().unwrap();
    let storage = new_memory_budget_test_storage(&temp_dir, 1_000_000);
    let labels = vec![Label::new("host", "a")];

    // First batch creates the series; the probe baseline starts after
    // creation so series-creation work cannot mask the steady-state check.
    storage
        .insert_rows(&[Row::with_labels(
            "visibility_probe_metric",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let before = storage.visibility_full_estimation_probes();
    storage
        .insert_rows(&[Row::with_labels(
            "visibility_probe_metric",
            labels.clone(),
            DataPoint::new(2, 2.0),
        )])
        .unwrap();
    assert_eq!(
        storage.visibility_full_estimation_probes() - before,
        0,
        "steady-state batch writes must not fully re-measure the visibility caches",
    );

    // Instrumentation liveness: the reconciliation face still runs the full
    // recompute, so the probe must advance once it executes.
    assert_engine_memory_usage_reconciled(&storage);
    assert!(
        storage.visibility_full_estimation_probes() > before,
        "the probe must count the full estimation performed by the reconciliation face",
    );

    storage.close().unwrap();
}

#[test]
fn visibility_state_incremental_accounting_matches_full_recompute_across_lifecycle() {
    // Task 13 safety net: incremental visibility-state memory accounting
    // (capacity baselines captured at the three cache mutation sites) must
    // stay byte-identical to the full recompute across series creation,
    // ranges-Vec growth/normalization/truncation, summary rebuilds, and
    // deletes. Any drift here poisons the admission criterion silently.
    let temp_dir = TempDir::new().unwrap();
    let storage = new_memory_budget_test_storage(&temp_dir, 4_000_000);
    let metric = "visibility_parity_metric";
    let labels = |host: &str| vec![Label::new("host", host)];

    // Stage A — create three series (summary/timestamp map inserts and the
    // first HashMap capacity growth).
    for host in ["a", "b", "c"] {
        storage
            .insert_rows(&[Row::with_labels(
                metric,
                labels(host),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();
    }
    assert_engine_memory_usage_reconciled(&storage);

    // Stage B — 40 disjoint ranges on series "a" across several batches:
    // ranges-Vec growth, normalization Vec replacement, and the
    // >SERIES_VISIBILITY_SUMMARY_MAX_RANGES truncation/drain path.
    for batch in 0..4u32 {
        let rows: Vec<Row> = (0..10u32)
            .map(|i| {
                let step = batch * 10 + i;
                Row::with_labels(
                    metric,
                    labels("a"),
                    DataPoint::new(2 + step as i64 * 2, step as f64),
                )
            })
            .collect();
        storage.insert_rows(&rows).unwrap();
        assert_engine_memory_usage_reconciled(&storage);
    }

    // Stage C — a contiguous run collapses into a single merged range
    // (normalize replaces the Vec with a differently sized allocation).
    let rows: Vec<Row> = (0..12i64)
        .map(|i| Row::with_labels(metric, labels("a"), DataPoint::new(100 + i, i as f64)))
        .collect();
    storage.insert_rows(&rows).unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    // Stage D — summary rebuild (replace path) for all live series.
    storage
        .refresh_series_visible_timestamp_cache(
            storage
                .visibility
                .materialized_series
                .read()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
        )
        .unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    // Stage E — delete (clear path removes cache entries and their ranges).
    let deleted = storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric(metric)
                .with_matcher(SeriesMatcher::equal("host", "a")),
        )
        .unwrap();
    assert_eq!(deleted.matched_series, 1);
    assert_engine_memory_usage_reconciled(&storage);

    // Stage F — writes after deletion re-grow cache entries on top of the
    // cleared maps.
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels("b"),
            DataPoint::new(500, 5.0),
        )])
        .unwrap();
    assert_engine_memory_usage_reconciled(&storage);

    storage.close().unwrap();
}

#[test]
fn cardinality_limit_rejects_new_series_beyond_limit() {
    let storage = StorageBuilder::new()
        .with_cardinality_limit(1)
        .with_current_time_override_for_tests(0)
        .build()
        .unwrap();
    let labels_a = vec![Label::new("host", "a")];
    let labels_b = vec![Label::new("host", "b")];

    storage
        .insert_rows(&[Row::with_labels(
            "cardinality_guard_metric",
            labels_a.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(
            "cardinality_guard_metric",
            labels_b.clone(),
            DataPoint::new(1, 2.0),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::CardinalityLimitExceeded { limit: 1, .. }
    ));

    storage
        .insert_rows(&[Row::with_labels(
            "cardinality_guard_metric",
            labels_a.clone(),
            DataPoint::new(2, 3.0),
        )])
        .unwrap();
    let points_a = storage
        .select("cardinality_guard_metric", &labels_a, 0, 10)
        .unwrap();
    assert_eq!(points_a.len(), 2);
    assert!(storage
        .select("cardinality_guard_metric", &labels_b, 0, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn cardinality_limit_rejection_does_not_grow_string_dictionaries() {
    let storage = ChunkStorage::new_with_data_path_and_options(
        4,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 1,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: 1,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: None,
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap();
    let baseline_labels = vec![Label::new("host", "baseline")];

    storage
        .insert_rows(&[Row::with_labels(
            "cardinality_dict_guard_metric",
            baseline_labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let (baseline_metric_len, baseline_label_name_len, baseline_label_value_len) = {
        let registry = storage.catalog.registry.read();
        (
            registry.metric_dictionary_len(),
            registry.label_name_dictionary_len(),
            registry.label_value_dictionary_len(),
        )
    };

    for attempt in 0..16 {
        let err = storage
            .insert_rows(&[Row::with_labels(
                format!("cardinality_dict_leak_metric_{attempt}"),
                vec![Label::new(
                    format!("dict_name_{attempt}"),
                    format!("dict_value_{attempt}"),
                )],
                DataPoint::new(2, attempt as f64),
            )])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::CardinalityLimitExceeded { limit: 1, .. }
        ));
    }

    let (metric_len, label_name_len, label_value_len) = {
        let registry = storage.catalog.registry.read();
        (
            registry.metric_dictionary_len(),
            registry.label_name_dictionary_len(),
            registry.label_value_dictionary_len(),
        )
    };

    assert_eq!(metric_len, baseline_metric_len);
    assert_eq!(label_name_len, baseline_label_name_len);
    assert_eq!(label_value_len, baseline_label_value_len);

    storage.close().unwrap();
}
