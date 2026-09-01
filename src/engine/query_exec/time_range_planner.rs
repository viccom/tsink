use std::collections::HashMap;

use roaring::RoaringTreemap;

use crate::engine::query::TieredQueryPlan;

use super::state::{PersistedChunkRef, PersistedIndexState, PersistedSegmentState};
use super::{ChunkStorage, SeriesId};

#[derive(Default)]
pub(super) struct PersistedTimeRangePruneResult {
    pub(super) overlapping_series_ids: RoaringTreemap,
    pub(super) exact_scan_chunk_refs: HashMap<SeriesId, Vec<PersistedChunkRef>>,
}

fn bitmap_cardinality(bitmap: &RoaringTreemap) -> usize {
    usize::try_from(bitmap.len()).unwrap_or(usize::MAX)
}

fn persisted_segment_overlaps_time_range(
    segment: &PersistedSegmentState,
    start: i64,
    end: i64,
    plan: TieredQueryPlan,
) -> bool {
    let (Some(min_ts), Some(max_ts)) = (segment.manifest.min_ts, segment.manifest.max_ts) else {
        return false;
    };
    max_ts >= start
        && min_ts < end
        && ChunkStorage::plan_includes_persisted_tier(plan, segment.tier)
}

fn persisted_segment_candidate_series_ids_for_time_range(
    segment: &PersistedSegmentState,
    chunk_refs: &HashMap<SeriesId, Vec<PersistedChunkRef>>,
    start: i64,
    end: i64,
) -> RoaringTreemap {
    segment
        .time_bucket_postings
        .as_ref()
        .map(|time_bucket_postings| time_bucket_postings.series_ids_in_time_range(start, end))
        .unwrap_or_else(|| {
            // Segment-local fallback for segments without time-bucket
            // postings: derive the segment's series from the global refs by
            // `segment_slot`.
            let mut series_ids = RoaringTreemap::new();
            for (series_id, refs) in chunk_refs {
                if refs
                    .iter()
                    .any(|chunk_ref| chunk_ref.segment_slot == segment.segment_slot)
                {
                    series_ids.insert(*series_id);
                }
            }
            series_ids
        })
}

fn append_overlapping_persisted_chunk_refs(
    exact_scan_chunk_refs: &mut HashMap<SeriesId, Vec<PersistedChunkRef>>,
    chunk_refs: &HashMap<SeriesId, Vec<PersistedChunkRef>>,
    segment: &PersistedSegmentState,
    series_id: SeriesId,
    start: i64,
    end: i64,
) -> bool {
    let Some(chunk_refs) = chunk_refs.get(&series_id) else {
        return false;
    };

    let mut appended = false;
    for chunk_ref in chunk_refs {
        if chunk_ref.segment_slot != segment.segment_slot {
            continue;
        }
        if chunk_ref.max_ts < start || chunk_ref.min_ts >= end {
            continue;
        }
        exact_scan_chunk_refs
            .entry(series_id)
            .or_default()
            .push(*chunk_ref);
        appended = true;
    }
    appended
}

pub(super) fn prune_persisted_exact_scan_candidates(
    persisted_index: &PersistedIndexState,
    series_ids: &RoaringTreemap,
    start: i64,
    end: i64,
    plan: TieredQueryPlan,
) -> PersistedTimeRangePruneResult {
    if series_ids.is_empty() {
        return PersistedTimeRangePruneResult::default();
    }

    let mut result = PersistedTimeRangePruneResult::default();
    let candidate_count = bitmap_cardinality(series_ids);
    for segment in persisted_index.segments_by_root.values() {
        if !persisted_segment_overlaps_time_range(segment, start, end, plan) {
            continue;
        }

        let mut overlapping_candidates = persisted_segment_candidate_series_ids_for_time_range(
            segment,
            &persisted_index.chunk_refs,
            start,
            end,
        );
        overlapping_candidates &= series_ids;
        if overlapping_candidates.is_empty() {
            continue;
        }

        let mut overlapping_series_in_segment = RoaringTreemap::new();
        for series_id in overlapping_candidates {
            if append_overlapping_persisted_chunk_refs(
                &mut result.exact_scan_chunk_refs,
                &persisted_index.chunk_refs,
                segment,
                series_id,
                start,
                end,
            ) {
                overlapping_series_in_segment.insert(series_id);
            }
        }
        if overlapping_series_in_segment.is_empty() {
            continue;
        }

        result.overlapping_series_ids |= overlapping_series_in_segment;
        if result.exact_scan_chunk_refs.len() >= candidate_count {
            break;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use roaring::RoaringTreemap;

    use super::super::state::PersistedSegmentTimeBucketIndex;
    use super::super::tiering::{PersistedSegmentTier, SegmentLaneFamily};
    use super::*;
    use crate::engine::chunk::{TimestampCodecId, ValueCodecId, ValueLane};
    use crate::engine::segment::{SegmentManifest, WalHighWatermark};

    fn bitmap(series_ids: &[SeriesId]) -> RoaringTreemap {
        series_ids.iter().copied().collect()
    }

    fn chunk_ref(segment_slot: usize, min_ts: i64, max_ts: i64) -> PersistedChunkRef {
        PersistedChunkRef {
            level: 0,
            min_ts,
            max_ts,
            point_count: 1,
            sequence: 0,
            chunk_offset: 0,
            chunk_len: 0,
            lane: ValueLane::Numeric,
            ts_codec: TimestampCodecId::DeltaVarint,
            value_codec: ValueCodecId::GorillaXorF64,
            segment_slot,
        }
    }

    fn segment(
        slot: usize,
        tier: PersistedSegmentTier,
        min_ts: i64,
        max_ts: i64,
        refs_by_series: HashMap<SeriesId, Vec<PersistedChunkRef>>,
        time_bucket_members: Option<&[(SeriesId, i64, i64)]>,
    ) -> PersistedSegmentState {
        let time_bucket_postings = time_bucket_members.map(|members| {
            let mut index = PersistedSegmentTimeBucketIndex::new(min_ts, max_ts);
            for (series_id, series_min_ts, series_max_ts) in members {
                index.insert_series_range(*series_id, *series_min_ts, *series_max_ts);
            }
            index
        });

        PersistedSegmentState {
            segment_slot: slot,
            lane: SegmentLaneFamily::Numeric,
            tier,
            manifest: SegmentManifest {
                segment_id: slot as u64,
                level: 0,
                chunk_count: refs_by_series.values().map(Vec::len).sum(),
                point_count: 0,
                series_count: refs_by_series.len(),
                min_ts: Some(min_ts),
                max_ts: Some(max_ts),
                wal_highwater: WalHighWatermark::default(),
            },
            time_bucket_postings,
            series_time_summaries: HashMap::new(),
        }
    }

    /// Assembles the persisted-index view the way `insert_indexed_segment_into_state`
    /// does: segments are installed alongside the global chunk refs aggregated
    /// across segments (sorted with the production comparator), optionally with
    /// extra refs that belong to no installed segment.
    fn prune_index(
        segments: Vec<PersistedSegmentState>,
        segment_refs: &[HashMap<SeriesId, Vec<PersistedChunkRef>>],
        extra_global_refs: &[(SeriesId, Vec<PersistedChunkRef>)],
    ) -> PersistedIndexState {
        let mut index = PersistedIndexState::default();
        for (segment, refs_by_series) in segments.into_iter().zip(segment_refs) {
            for (series_id, refs) in refs_by_series {
                index
                    .chunk_refs
                    .entry(*series_id)
                    .or_default()
                    .extend(refs.iter().copied());
            }
            index.segments_by_root.insert(
                PathBuf::from(format!("/seg-{}", segment.segment_slot)),
                segment,
            );
        }
        for (series_id, refs) in extra_global_refs {
            index
                .chunk_refs
                .entry(*series_id)
                .or_default()
                .extend(refs.iter().copied());
        }
        for refs in index.chunk_refs.values_mut() {
            refs.sort_by_key(|chunk_ref| {
                (
                    chunk_ref.min_ts,
                    chunk_ref.max_ts,
                    chunk_ref.sequence,
                    chunk_ref.chunk_offset,
                    chunk_ref.point_count,
                )
            });
        }
        index
    }

    #[test]
    fn prune_result_stays_bounded_to_candidate_series_ids() {
        let series_id = 7;
        let spill_series_id = 9;
        let refs = HashMap::from([
            (series_id, vec![chunk_ref(1, 40, 50)]),
            (spill_series_id, vec![chunk_ref(1, 40, 50)]),
        ]);
        let segment = segment(
            1,
            PersistedSegmentTier::Hot,
            0,
            100,
            refs.clone(),
            Some(&[(series_id, 40, 50), (spill_series_id, 40, 50)]),
        );
        let index = prune_index(vec![segment], &[refs], &[]);
        let candidates = bitmap(&[series_id]);

        let result = prune_persisted_exact_scan_candidates(
            &index,
            &candidates,
            45,
            46,
            TieredQueryPlan::from_cutoffs(45, 46, None, None),
        );

        assert_eq!(
            result.overlapping_series_ids.iter().collect::<Vec<_>>(),
            vec![series_id],
        );
        assert_eq!(
            result
                .exact_scan_chunk_refs
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![series_id],
        );
    }

    #[test]
    fn prune_selects_only_segment_local_overlapping_chunk_refs() {
        let series_id = 1;
        let other_series_id = 2;
        let early_refs = HashMap::from([
            (series_id, vec![chunk_ref(10, 0, 20), chunk_ref(10, 60, 80)]),
            (other_series_id, vec![chunk_ref(10, 5, 15)]),
        ]);
        let late_refs = HashMap::from([(
            series_id,
            vec![chunk_ref(20, 40, 50), chunk_ref(20, 42, 48)],
        )]);
        let early_segment = segment(
            10,
            PersistedSegmentTier::Hot,
            0,
            100,
            early_refs.clone(),
            Some(&[(series_id, 0, 80), (other_series_id, 5, 15)]),
        );
        let late_segment = segment(
            20,
            PersistedSegmentTier::Hot,
            0,
            100,
            late_refs.clone(),
            Some(&[(series_id, 40, 50)]),
        );
        let index = prune_index(
            vec![early_segment, late_segment],
            &[early_refs, late_refs],
            &[],
        );
        let candidates = bitmap(&[series_id, other_series_id]);

        let result = prune_persisted_exact_scan_candidates(
            &index,
            &candidates,
            45,
            50,
            TieredQueryPlan::from_cutoffs(45, 50, None, None),
        );

        assert_eq!(
            result.overlapping_series_ids.iter().collect::<Vec<_>>(),
            vec![series_id],
        );
        assert_eq!(
            result.exact_scan_chunk_refs,
            HashMap::from([(
                series_id,
                vec![chunk_ref(20, 40, 50), chunk_ref(20, 42, 48)]
            )]),
        );
    }

    #[test]
    fn prune_fallback_without_time_bucket_postings_scopes_to_segment_series() {
        let in_segment_series = 3;
        let candidate_without_segment_refs = 5;
        let non_candidate_series = 6;
        let fallback_refs = HashMap::from([
            (in_segment_series, vec![chunk_ref(10, 40, 50)]),
            (non_candidate_series, vec![chunk_ref(10, 0, 10)]),
        ]);
        let fallback_segment = segment(
            10,
            PersistedSegmentTier::Hot,
            0,
            100,
            fallback_refs.clone(),
            None,
        );
        // Candidate series whose only global refs belong to another slot:
        // the fallback must not leak them into this segment's scan.
        let extra_global_refs = [(candidate_without_segment_refs, vec![chunk_ref(30, 40, 50)])];
        let index = prune_index(vec![fallback_segment], &[fallback_refs], &extra_global_refs);
        let candidates = bitmap(&[in_segment_series, candidate_without_segment_refs]);

        let result = prune_persisted_exact_scan_candidates(
            &index,
            &candidates,
            45,
            50,
            TieredQueryPlan::from_cutoffs(45, 50, None, None),
        );

        assert_eq!(
            result.overlapping_series_ids.iter().collect::<Vec<_>>(),
            vec![in_segment_series],
        );
        assert_eq!(
            result.exact_scan_chunk_refs,
            HashMap::from([(in_segment_series, vec![chunk_ref(10, 40, 50)])]),
        );
    }

    #[test]
    fn prune_skips_segments_outside_the_query_tier_plan() {
        let hot_series = 1;
        let cold_series = 2;
        let hot_refs = HashMap::from([(hot_series, vec![chunk_ref(1, 40, 50)])]);
        let cold_refs = HashMap::from([(cold_series, vec![chunk_ref(2, 40, 50)])]);
        let hot_segment = segment(
            1,
            PersistedSegmentTier::Hot,
            0,
            100,
            hot_refs.clone(),
            Some(&[(hot_series, 40, 50)]),
        );
        let cold_segment = segment(
            2,
            PersistedSegmentTier::Cold,
            0,
            100,
            cold_refs.clone(),
            Some(&[(cold_series, 40, 50)]),
        );
        let index = prune_index(vec![hot_segment, cold_segment], &[hot_refs, cold_refs], &[]);
        let candidates = bitmap(&[hot_series, cold_series]);

        let result = prune_persisted_exact_scan_candidates(
            &index,
            &candidates,
            45,
            46,
            TieredQueryPlan::from_cutoffs(45, 46, None, None),
        );

        assert_eq!(
            result.overlapping_series_ids.iter().collect::<Vec<_>>(),
            vec![hot_series],
        );
        assert_eq!(
            result
                .exact_scan_chunk_refs
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![hot_series],
        );
    }
}
