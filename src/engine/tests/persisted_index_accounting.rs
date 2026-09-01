//! Locks the persisted-index memory accounting to the de-duplicated shape:
//! per-segment chunk-ref copies must not contribute to
//! `persisted_index_used_bytes`.
//!
//! The fixed topology mirrors `insert_indexed_segment_into_state` (two
//! segments, two series, five chunks) so the accounting is deterministic and
//! the expected byte totals can be pinned exactly.

use std::collections::HashMap;

use super::super::state::{
    PersistedSegmentState, PersistedSegmentTimeBucketIndex, PersistedSeriesTimeRangeSummary,
};
use super::super::tiering::{PersistedSegmentTier, SegmentLaneFamily};
use super::super::{ChunkStorage, PersistedChunkRef, PersistedIndexState};
use crate::engine::chunk::{TimestampCodecId, ValueCodecId, ValueLane};
use crate::engine::segment::SegmentManifest;

const FIXED_TOPOLOGY_CHUNK_COUNT: usize = 5;

fn topology_chunk_ref(
    segment_slot: usize,
    sequence: u64,
    min_ts: i64,
    max_ts: i64,
) -> PersistedChunkRef {
    PersistedChunkRef {
        level: 0,
        min_ts,
        max_ts,
        point_count: 1,
        sequence,
        chunk_offset: 0,
        chunk_len: 0,
        lane: ValueLane::Numeric,
        ts_codec: TimestampCodecId::DeltaVarint,
        value_codec: ValueCodecId::GorillaXorF64,
        segment_slot,
    }
}

struct TopologySegment {
    segment_slot: usize,
    root: &'static str,
    tier: PersistedSegmentTier,
    min_ts: i64,
    max_ts: i64,
    chunks_by_series: Vec<(u64, Vec<(u64, i64, i64)>)>,
}

fn topology_segments() -> Vec<TopologySegment> {
    vec![
        TopologySegment {
            segment_slot: 7,
            root: "/persisted-accounting/segment-a",
            tier: PersistedSegmentTier::Hot,
            min_ts: 0,
            max_ts: 70,
            chunks_by_series: vec![(1, vec![(1, 0, 10), (2, 40, 50)]), (2, vec![(3, 60, 70)])],
        },
        TopologySegment {
            segment_slot: 9,
            root: "/persisted-accounting/segment-b",
            tier: PersistedSegmentTier::Cold,
            min_ts: 80,
            max_ts: 110,
            chunks_by_series: vec![(1, vec![(4, 80, 90)]), (2, vec![(5, 100, 110)])],
        },
    ]
}

fn topology_segment_state(segment: &TopologySegment) -> PersistedSegmentState {
    let mut series_time_summaries = HashMap::<u64, PersistedSeriesTimeRangeSummary>::new();
    for (series_id, chunks) in &segment.chunks_by_series {
        for (_, min_ts, max_ts) in chunks {
            series_time_summaries
                .entry(*series_id)
                .and_modify(|summary| {
                    summary.min_ts = summary.min_ts.min(*min_ts);
                    summary.max_ts = summary.max_ts.max(*max_ts);
                })
                .or_insert(PersistedSeriesTimeRangeSummary {
                    min_ts: *min_ts,
                    max_ts: *max_ts,
                });
        }
    }

    let mut time_bucket_postings =
        PersistedSegmentTimeBucketIndex::new(segment.min_ts, segment.max_ts);
    for (series_id, chunks) in &segment.chunks_by_series {
        for (_, min_ts, max_ts) in chunks {
            time_bucket_postings.insert_series_range(*series_id, *min_ts, *max_ts);
        }
    }

    PersistedSegmentState {
        segment_slot: segment.segment_slot,
        lane: SegmentLaneFamily::Numeric,
        tier: segment.tier,
        manifest: SegmentManifest {
            segment_id: segment.segment_slot as u64,
            level: 0,
            chunk_count: segment
                .chunks_by_series
                .iter()
                .map(|(_, chunks)| chunks.len())
                .sum(),
            point_count: 0,
            series_count: segment.chunks_by_series.len(),
            min_ts: Some(segment.min_ts),
            max_ts: Some(segment.max_ts),
            wal_highwater: Default::default(),
        },
        time_bucket_postings: Some(time_bucket_postings),
        series_time_summaries,
    }
}

fn fixed_topology_index() -> PersistedIndexState {
    let mut index = PersistedIndexState::default();
    for segment in topology_segments() {
        for (series_id, chunks) in &segment.chunks_by_series {
            for &(sequence, min_ts, max_ts) in chunks {
                let chunk_ref = topology_chunk_ref(segment.segment_slot, sequence, min_ts, max_ts);
                index
                    .chunk_refs
                    .entry(*series_id)
                    .or_default()
                    .push(chunk_ref);
            }
        }
        index
            .segment_tiers
            .insert(segment.segment_slot, segment.tier);

        let segment_state = topology_segment_state(&segment);
        index
            .segments_by_root
            .insert(std::path::PathBuf::from(segment.root), segment_state);
    }
    index
}

/// Measured on the double-inclusive implementation (the removed segment-local
/// chunk-ref map still populated) for the exact fixed topology above.
const DOUBLE_INCLUSIVE_BASELINE: usize = 5290;

/// Double-inclusive baseline minus the segment-local map payload and the
/// removed per-segment `HashMap` field (2 segments x 48B struct shrink).
/// Re-measured for the Task 6 timestamp-index LRU cache: the per-chunk
/// `OnceLock` pre-seats left the persisted index entirely (their map
/// bookkeeping, ~336B for this topology, now lives in the budgeted cache's
/// own structures accounting instead).
const DEDUPED_EXPECTED: usize = 3770;

#[test]
fn persisted_index_accounting_excludes_segment_local_chunk_ref_copies() {
    let index = fixed_topology_index();
    let used = ChunkStorage::persisted_index_included_memory_usage_bytes(&index);
    assert_eq!(used, DEDUPED_EXPECTED);
    assert!(
        DOUBLE_INCLUSIVE_BASELINE.saturating_sub(DEDUPED_EXPECTED)
            >= FIXED_TOPOLOGY_CHUNK_COUNT * std::mem::size_of::<PersistedChunkRef>(),
        "dedup must save at least one PersistedChunkRef copy per chunk"
    );
}
