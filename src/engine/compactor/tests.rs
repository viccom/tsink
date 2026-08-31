use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tempfile::TempDir;

use super::{execution, finalize_pending_compaction_replacements, Compactor};
use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::encoder::Encoder;
use crate::engine::segment::{
    load_segments, load_segments_for_level, SegmentWriter, WalHighWatermark,
};
use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
use crate::engine::tombstone::{persist_tombstones, TombstoneRange, TOMBSTONES_FILE_NAME};
use crate::{
    HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, NativeHistogram, Value,
};

fn sample_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(42)),
        sum: 17.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(HistogramCount::Int(7)),
        negative_spans: vec![HistogramBucketSpan {
            offset: -1,
            length: 1,
        }],
        negative_deltas: vec![2],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 0,
            length: 2,
        }],
        positive_deltas: vec![3, 1],
        positive_counts: vec![],
        reset_hint: HistogramResetHint::No,
        custom_values: vec![0.25, 0.5],
    }
}

fn write_level_segment(
    temp_dir: &tempfile::TempDir,
    level: u8,
    segment_id: u64,
    series_id: u64,
    registry: &SeriesRegistry,
    points: &[(i64, f64)],
) {
    let mut chunks = HashMap::new();
    chunks.insert(series_id, vec![make_numeric_chunk(series_id, points)]);
    SegmentWriter::new(temp_dir.path(), level, segment_id)
        .unwrap()
        .write_segment(registry, &chunks)
        .unwrap();
}

#[test]
fn compaction_window_selection_fully_loads_at_most_window_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    // One overlapping cluster of 20 L0 segments: the window must pick 8 of
    // them, and planning must fully load only those 8 — not the whole level.
    for segment_id in 1..=20u64 {
        write_level_segment(
            &temp_dir,
            0,
            segment_id,
            series_id,
            &registry,
            &[(10, 1.0), (11, 2.0)],
        );
    }

    crate::engine::segment::reset_full_segment_dir_load_count();
    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(1000)),
    );
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted, "overlapping cluster must compact");

    let loads = crate::engine::segment::full_segment_dir_load_count();
    assert!(
        loads <= super::DEFAULT_SOURCE_WINDOW_SEGMENTS as u64,
        "single compaction pass fully loaded {loads} segment directories; window is {}",
        super::DEFAULT_SOURCE_WINDOW_SEGMENTS
    );

    // Window semantics are unchanged: the 8 lowest-id segments of the cluster
    // are compacted away and the rest of the level stays untouched.
    let remaining_l0_ids = load_segments_for_level(temp_dir.path(), 0)
        .unwrap()
        .into_iter()
        .map(|segment| segment.manifest.segment_id)
        .collect::<Vec<_>>();
    assert_eq!(remaining_l0_ids, (9..=20).collect::<Vec<_>>());
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 1).unwrap().len(),
        1,
        "compacted window must produce exactly one L1 output"
    );
}

#[test]
fn l1_to_l2_compaction_is_not_starved_by_continuous_l0_arrivals() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    // An L1 backlog (5 overlapping segments, eligible via time overlap) that
    // only L1->L2 compaction can drain, while fresh overlapping L0 segments
    // keep arriving every pass so L0->L1 is always eligible too.
    for (segment_id, ts) in [(1u64, 10i64), (2, 12), (3, 14), (4, 16), (5, 18)] {
        write_level_segment(
            &temp_dir,
            1,
            segment_id,
            series_id,
            &registry,
            &[(ts, ts as f64), (ts + 10, 100.0)],
        );
    }

    let compactor = Compactor::new(temp_dir.path(), 8);
    let mut l0_to_l1_runs = 0usize;
    let mut l1_to_l2_runs = 0usize;

    for round in 0..8u64 {
        let base_ts = 100 + (round as i64) * 100;
        let first_id = 100 + round * 10;
        write_level_segment(
            &temp_dir,
            0,
            first_id,
            series_id,
            &registry,
            &[(base_ts, 1.0), (base_ts + 50, 2.0)],
        );
        write_level_segment(
            &temp_dir,
            0,
            first_id + 1,
            series_id,
            &registry,
            &[(base_ts + 25, 3.0), (base_ts + 75, 4.0)],
        );

        let stats = compactor.compact_once_with_stats().unwrap();
        match (stats.source_level, stats.target_level) {
            (Some(0), Some(1)) => l0_to_l1_runs += 1,
            (Some(1), Some(2)) => l1_to_l2_runs += 1,
            _ => {}
        }
    }

    assert!(
        l0_to_l1_runs > 0,
        "test setup broken: L0->L1 never ran ({l0_to_l1_runs} runs)"
    );
    assert!(
        l1_to_l2_runs > 0,
        "L1->L2 starved under continuous L0 arrivals: L0->L1 ran {l0_to_l1_runs} times, L1->L2 ran {l1_to_l2_runs} times"
    );
}

#[test]
fn l0_backlog_is_not_starved_by_level_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    // Only L0 has work (12 non-overlapping segments, count-trigger eligible in
    // windows of 4). While the L0 backlog is at or above the trigger, every
    // pass must compact L0 regardless of which transition ran last.
    for segment_id in 1..=12u64 {
        write_level_segment(
            &temp_dir,
            0,
            segment_id,
            series_id,
            &registry,
            &[(segment_id as i64, segment_id as f64)],
        );
    }

    let compactor = Compactor::new(temp_dir.path(), 8);
    let mut l0_to_l1_runs = 0usize;
    loop {
        let stats = compactor.compact_once_with_stats().unwrap();
        if !stats.compacted {
            break;
        }
        assert_eq!(
            (stats.source_level, stats.target_level),
            (Some(0), Some(1)),
            "L0 backlog at trigger must keep winning passes"
        );
        l0_to_l1_runs += 1;
    }

    assert_eq!(
        l0_to_l1_runs, 3,
        "expected three window passes over 12 L0 segments"
    );
    assert!(load_segments_for_level(temp_dir.path(), 0)
        .unwrap()
        .is_empty());
}

#[test]
fn compacts_overlapping_l0_segments_into_l1() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

    assert!(l0.is_empty());
    assert_eq!(l1.len(), 1);

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].header.point_count, 4);
    let decoded = execution::decode_chunk_points_for_compaction(&chunks[0]).unwrap();
    assert_eq!(decoded[0].ts, 10);
    assert_eq!(decoded[3].ts, 30);
}

#[test]
fn compaction_preserves_histogram_payloads() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("rpc_duration_seconds", &[Label::new("job", "backend")])
        .unwrap()
        .series_id;

    let first_chunk = make_histogram_chunk(series_id, &[10, 20]);
    let second_chunk = make_histogram_chunk(series_id, &[30, 40]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let decoded = chunks
        .iter()
        .flat_map(|chunk| execution::decode_chunk_points_for_compaction(chunk).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(decoded.len(), 4);
    for (point, ts) in decoded.iter().zip([10, 20, 30, 40]) {
        assert_eq!(point.ts, ts);
        assert_eq!(point.value, Value::from(sample_histogram()));
    }
}

#[test]
fn compactor_uses_shared_segment_id_allocator() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let next_segment_id = Arc::new(AtomicU64::new(100));
    let compactor =
        Compactor::new_with_segment_id_allocator(temp_dir.path(), 8, Arc::clone(&next_segment_id));
    compactor.compact_once().unwrap();

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.segment_id, 100);
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 101);
}

#[test]
fn compactor_limits_source_window_per_pass() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    for segment_id in 1..=6 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_numeric_chunk(
                series_id,
                &[(segment_id as i64, segment_id as f64)],
            )],
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

    assert_eq!(l0.len(), 2);
    assert_eq!(l1.len(), 1);
    assert_eq!(
        l0.iter()
            .map(|segment| segment.manifest.segment_id)
            .collect::<Vec<_>>(),
        vec![5, 6]
    );
}

#[test]
fn compaction_output_point_budget_is_decoupled_from_chunk_point_cap() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "budget")])
        .unwrap()
        .series_id;

    // 4 non-overlapping L0 segments x 600k points = 2.4M points total. The
    // decoupled output budget is now 8x the L0 flush shard size (8,388,608
    // points), so the discriminating dataset scales accordingly: with
    // chunk_points=512 the legacy cap-derived budgets (262,144 / 1,048,576)
    // split this into multiple output segments; the 8M-point budget must keep
    // it in one segment so tuning chunk_points no longer multiplies the
    // output segment count.
    for (segment_id, base_ts) in [(1u64, 0i64), (2, 600_000), (3, 1_200_000), (4, 1_800_000)] {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            (0..10)
                .map(|chunk_index| {
                    let start = base_ts + chunk_index * 60_000;
                    make_numeric_chunk(
                        series_id,
                        &(start..start + 60_000)
                            .map(|ts| (ts, ts as f64))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 512);
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(
        stats.output_segments, 1,
        "2.4M points must stay in one output segment when chunk_points=512 \
         (the previous 1M-point budget split this into {})",
        stats.output_segments
    );

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.point_count, 2_400_000);

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let total_points = chunks
        .iter()
        .map(|chunk| chunk.header.point_count as usize)
        .sum::<usize>();
    assert_eq!(total_points, 2_400_000);
}

#[test]
fn compaction_output_point_budget_keeps_default_chunk_points_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "budget")])
        .unwrap()
        .series_id;

    // Same 2.4M-point shape as the decoupling test, but with the default
    // chunk_points=2048: the previous budget (2048 x 512 = 1,048,576) would
    // split this into three output segments; with the 8M-point budget the
    // default configuration must keep it in a single segment.
    for (segment_id, base_ts) in [(1u64, 0i64), (2, 600_000), (3, 1_200_000), (4, 1_800_000)] {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            (0..10)
                .map(|chunk_index| {
                    let start = base_ts + chunk_index * 60_000;
                    make_numeric_chunk(
                        series_id,
                        &(start..start + 60_000)
                            .map(|ts| (ts, ts as f64))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 2048);
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(stats.output_segments, 1);

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.point_count, 2_400_000);
}

#[test]
fn compactor_splits_large_output_into_multiple_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        let start = (segment_id as i64 - 1) * 2_400;
        let points = (start..start + 2_400)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();

        let mut chunks = HashMap::new();
        chunks.insert(series_id, vec![make_numeric_chunk(series_id, &points)]);
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    // The budget is decoupled from the chunk point cap, so the shard-boundary
    // behavior is exercised with an explicit small budget instead of leaning
    // on point_cap: 9600 points against a 4000-point budget must split, with
    // every output segment bounded by the budget plus one trailing chunk.
    let compactor = Compactor::new(temp_dir.path(), 2).with_output_segment_point_budget(4_000);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert!(l0.is_empty());
    assert!(l1.len() >= 2);
    assert!(l1
        .iter()
        .all(|segment| segment.manifest.point_count <= 4_000 + 2));

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let total_points = chunks
        .iter()
        .map(|chunk| chunk.header.point_count as usize)
        .sum::<usize>();
    assert_eq!(total_points, 9_600);
}

#[test]
fn compaction_output_budget_eight_million_reduces_segment_count() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "budget")])
        .unwrap()
        .series_id;

    // The "8-in-8-out" regression shape: 8 L0 flush-sized segments (~1M
    // points each). Each segment's timestamps are interleaved modulo 8 so the
    // segment time ranges overlap (one window takes all 8) while every point
    // timestamp stays unique across segments (merged total is exactly 8M
    // distinct points, no dedup shrinkage). An output budget equal to the L0
    // shard size re-emitted the 8M merged points as ~8 more 1M segments, so
    // the level segment count never shrank. The 8M-point budget must collapse
    // the whole window into a single ~8M-point L1 segment (8 -> 1).
    for segment_index in 0..8u64 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            (0..16)
                .map(|chunk_index| {
                    let k_start = chunk_index * 62_500;
                    make_numeric_chunk(
                        series_id,
                        &(k_start..k_start + 62_500)
                            .map(|k| {
                                let ts = segment_index as i64 + 8 * k;
                                (ts, ts as f64)
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_index + 1)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 2048);
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(stats.source_segments, 8);
    assert_eq!(
        stats.output_segments, 1,
        "8x1M-point overlapping L0 segments must collapse into one L1 output \
         segment, got {}",
        stats.output_segments
    );
    assert_eq!(stats.output_points, 8_000_000);

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert!(l0.is_empty());
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.point_count, 8_000_000);
}

#[test]
fn compactor_purges_tombstoned_points() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 15, end: 21 }]);
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    assert!(compactor.compact_once().unwrap());

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let decoded = execution::decode_chunk_points_for_compaction(&chunks[0]).unwrap();
    let timestamps = decoded
        .into_iter()
        .map(|point| point.ts)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![10, 30]);
}

#[test]
fn compaction_fails_when_any_source_segment_is_corrupted() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_writer = SegmentWriter::new(temp_dir.path(), 0, 1).unwrap();
    let mut first_chunks = HashMap::new();
    first_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    first_writer
        .write_segment(&registry, &first_chunks)
        .unwrap();

    let second_writer = SegmentWriter::new(temp_dir.path(), 0, 2).unwrap();
    let mut second_chunks = HashMap::new();
    second_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );
    second_writer
        .write_segment(&registry, &second_chunks)
        .unwrap();

    let corrupt_root = first_writer.layout().root.clone();
    let mut manifest_bytes = fs::read(first_writer.layout().manifest_path.clone()).unwrap();
    manifest_bytes[0] ^= 0xff;
    fs::write(first_writer.layout().manifest_path.clone(), manifest_bytes).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    let err = compactor.compact_once().unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("failed compaction validation"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("manifest crc32 mismatch"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains(&corrupt_root.display().to_string()),
        "unexpected error: {message}"
    );
    assert!(
        corrupt_root.exists(),
        "corrupt source root should stay in place"
    );
    assert!(
        second_writer.layout().root.exists(),
        "healthy source root should not be compacted away"
    );
    assert!(
        load_segments_for_level(temp_dir.path(), 1)
            .unwrap()
            .is_empty(),
        "compaction must not publish replacement output when any source root is corrupt"
    );
}

#[test]
fn finalize_pending_replacements_removes_marked_source_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut l0_chunks = HashMap::new();
    l0_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 1.0)])]);
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &l0_chunks)
        .unwrap();

    let mut l1_chunks = HashMap::new();
    l1_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 2.0)])]);
    SegmentWriter::new(temp_dir.path(), 1, 2)
        .unwrap()
        .write_segment(&registry, &l1_chunks)
        .unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l0.len(), 1);
    assert_eq!(l1.len(), 1);

    let source_root = l0[0].root.clone();
    let output_root = l1[0].root.clone();
    let marker_path = execution::write_compaction_replacement_marker(
        temp_dir.path(),
        std::slice::from_ref(&source_root),
        std::slice::from_ref(&output_root),
    )
    .unwrap();
    assert!(marker_path.exists());

    finalize_pending_compaction_replacements(temp_dir.path()).unwrap();

    assert!(!source_root.exists(), "source segment should be removed");
    assert!(
        output_root.exists(),
        "replacement output should be preserved"
    );
    assert!(
        !marker_path.exists(),
        "replacement marker should be removed after apply"
    );
}

fn make_numeric_chunk(series_id: u64, points: &[(i64, f64)]) -> Chunk {
    let points = points
        .iter()
        .map(|(ts, value)| ChunkPoint {
            ts: *ts,
            value: Value::F64(*value),
        })
        .collect::<Vec<_>>();

    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: points.len() as u16,
            min_ts: points.first().unwrap().ts,
            max_ts: points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}

fn make_histogram_chunk(series_id: u64, timestamps: &[i64]) -> Chunk {
    let histogram = sample_histogram();
    let points = timestamps
        .iter()
        .map(|ts| ChunkPoint {
            ts: *ts,
            value: Value::from(histogram.clone()),
        })
        .collect::<Vec<_>>();

    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Blob).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Blob,
            value_family: Some(SeriesValueFamily::Histogram),
            point_count: points.len() as u16,
            min_ts: points.first().unwrap().ts,
            max_ts: points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}
