//! Compaction knob overrides through the public `StorageBuilder` face.
//!
//! The four compaction knobs (pass interval, drain time budget, source
//! window, output point budget) used to be crate-internal constants, so
//! soak-test deployments (rs-iot HistoryConfig) could not retune them in the
//! field. These tests lock the full wiring chain
//! `StorageBuilder -> ChunkStorageOptions -> assembly -> Compactor/runtime`
//! and pin the defaults to the crate constants so the knob face stays a pure
//! exposure of the existing behavior (zero change unless overridden).

use super::*;

fn storage_with_compaction_overrides(
    root: &Path,
    compaction_interval: Duration,
    drain_time_budget: Duration,
    source_window_segments: usize,
    output_segment_point_budget: usize,
) -> ChunkStorage {
    let builder = StorageBuilder::new()
        .with_data_path(root)
        .with_background_threads_enabled_for_tests(false)
        .with_compaction_interval(compaction_interval)
        .with_drain_time_budget(drain_time_budget)
        .with_source_window_segments(source_window_segments)
        .with_output_segment_point_budget(output_segment_point_budget);
    let options = ChunkStorageOptions::from(&builder);
    ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(root.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        options,
    )
    .unwrap()
}

#[test]
fn compaction_builder_override_window_and_output_budget_reach_compactor_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let storage = storage_with_compaction_overrides(
        temp_dir.path(),
        Duration::from_secs(5),
        Duration::from_secs(10),
        4,      // source window override (default: 16)
        20_000, // output segment point budget override (default: 8,388,608)
    );

    // Eight L0 segments of 6k points each, one series per segment, all
    // sharing one overlapping time span: one overlapping cluster of 8 that
    // only the source window can bound. The window override of 4 must cap
    // the pass at 4 sources (4 x 6k = 24k points), and the 20k output budget
    // must split that pass into 2 output L1 segments.
    for round in 0..8i64 {
        let labels = vec![Label::new("host", format!("node-{round}"))];
        let rows = (0..6_000)
            .map(|offset| {
                Row::with_labels(
                    "cpu",
                    labels.clone(),
                    DataPoint::new(round * 1_000 + offset * 10, offset as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.flush_all_active().unwrap();
        assert!(
            storage.persist_segment_with_outcome().unwrap().persisted,
            "fixture round must persist an L0 segment"
        );
    }

    let stats = storage
        .persisted
        .numeric_compactor
        .as_ref()
        .expect("numeric lane must have a compactor")
        .compact_once_with_stats()
        .unwrap();
    assert!(
        stats.compacted,
        "overriding knobs must not disable compaction"
    );
    assert_eq!(
        stats.source_segments, 4,
        "source window override must cap one pass at 4 of the 8 segments",
    );
    assert_eq!(stats.source_points, 24_000);
    assert_eq!(
        stats.output_segments, 2,
        "24k points against the 20k budget override must split into 2 outputs",
    );

    let lane_root = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let remaining_l0 = load_segments_for_level(&lane_root, 0).unwrap();
    assert_eq!(
        remaining_l0.len(),
        4,
        "8-segment cluster minus a 4-window pass must leave 4 L0 segments",
    );
    let l1_outputs = load_segments_for_level(&lane_root, 1).unwrap();
    assert_eq!(
        l1_outputs.len(),
        2,
        "on-disk L1 must carry exactly the 2 budget-split outputs",
    );
}

/// The pass interval and drain budget drive background thread park cadence
/// and gate deadlines, which are wall-clock behaviors no unit test can
/// observe deterministically; the minimal observable point is therefore the
/// assembled `ChunkStorageOptions` (the same product the runtime and drain
/// pass consume), asserted here for both overrides and defaults.
#[test]
fn compaction_builder_override_interval_and_drain_budget_reach_assembled_options() {
    let temp_dir = TempDir::new().unwrap();
    let builder = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_compaction_interval(Duration::from_secs(42))
        .with_drain_time_budget(Duration::from_millis(7_000));
    let options = ChunkStorageOptions::from(&builder);
    assert_eq!(options.compaction_interval, Duration::from_secs(42));
    assert_eq!(options.drain_time_budget, Duration::from_millis(7_000));

    // Defaults must stay pinned to the crate constants the knobs expose.
    let defaults =
        ChunkStorageOptions::from(&StorageBuilder::new().with_data_path(temp_dir.path()));
    assert_eq!(defaults.compaction_interval, DEFAULT_COMPACTION_INTERVAL);
    assert_eq!(
        defaults.drain_time_budget,
        crate::engine::COMPACTION_DRAIN_TIME_BUDGET,
    );
    assert_eq!(
        defaults.source_window_segments,
        crate::engine::compactor::DEFAULT_SOURCE_WINDOW_SEGMENTS,
    );
    assert_eq!(
        defaults.output_segment_point_budget,
        crate::engine::compactor::DEFAULT_OUTPUT_SEGMENT_POINT_BUDGET,
    );
}
