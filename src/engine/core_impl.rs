use super::*;
use crate::engine::tombstone;

#[path = "core_impl/contexts.rs"]
mod contexts;
#[path = "core_impl/metadata_publication.rs"]
mod metadata_publication;
#[path = "core_impl/utilities.rs"]
mod utilities;
#[path = "core_impl/write_capabilities.rs"]
mod write_capabilities;
#[path = "core_impl/write_memory.rs"]
mod write_memory;
#[path = "core_impl/write_visibility.rs"]
mod write_visibility;

pub(super) use self::contexts::{
    CatalogContext, ChunkContext, PersistedRefreshContext, WriteApplyContext, WriteCommitContext,
    WritePrepareContext, WriteResolveContext, WriteSeriesValidationContext,
};
pub(super) use self::metadata_publication::*;
pub(super) use self::utilities::{
    current_unix_millis_u64, current_unix_timestamp_units, duration_to_timestamp_units,
    elapsed_nanos_u64, lane_for_value, partition_id_for_timestamp, persisted_chunk_payload,
    saturating_u64_from_usize, value_heap_bytes, MemoryDeltaBytes, VisibilityStateMemoryBaseline,
};
pub(super) use self::write_capabilities::*;
pub(super) use self::write_memory::*;
pub(super) use self::write_visibility::*;

impl ChunkStorage {
    pub(super) fn existing_series_ids_for_metric_series<I>(&self, series: I) -> Vec<SeriesId>
    where
        I: IntoIterator<Item = MetricSeries>,
    {
        let registry = self.catalog.registry.read();
        let mut series_ids = series
            .into_iter()
            .filter_map(|series| {
                registry
                    .resolve_existing(&series.name, &series.labels)
                    .map(|resolution| resolution.series_id)
            })
            .collect::<Vec<_>>();
        series_ids.sort_unstable();
        series_ids.dedup();
        series_ids
    }

    pub fn flush(&self) -> Result<()> {
        self.ensure_open()?;
        self.flush_pipeline_once()
    }

    pub(super) fn metric_series_for_ids<I>(&self, series_ids: I) -> Vec<MetricSeries>
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let registry = self.catalog.registry.read();
        series_ids
            .into_iter()
            .filter_map(|series_id| {
                registry
                    .decode_series_key(series_id)
                    .and_then(|series_key| {
                        (!rollups::is_internal_rollup_metric(&series_key.metric)).then_some(
                            MetricSeries {
                                name: series_key.metric,
                                labels: series_key.labels,
                            },
                        )
                    })
            })
            .collect()
    }

    pub(super) fn latest_visible_timestamp_for_series_locked(
        &self,
        series_id: SeriesId,
        persisted_index: &PersistedIndexState,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
    ) -> Result<Option<i64>> {
        self.latest_visible_timestamp_for_series_locked_with_ceiling(
            series_id,
            persisted_index,
            tombstone_ranges,
            i64::MAX,
        )
    }

    pub(super) fn latest_visible_bounded_timestamp_for_series_locked(
        &self,
        series_id: SeriesId,
        persisted_index: &PersistedIndexState,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        ceiling_inclusive: i64,
    ) -> Result<Option<i64>> {
        self.latest_visible_timestamp_for_series_locked_with_ceiling(
            series_id,
            persisted_index,
            tombstone_ranges,
            ceiling_inclusive,
        )
    }

    fn latest_visible_timestamp_for_series_locked_with_ceiling(
        &self,
        series_id: SeriesId,
        persisted_index: &PersistedIndexState,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        ceiling_inclusive: i64,
    ) -> Result<Option<i64>> {
        let mut latest = i64::MIN;

        {
            let active = self.active_shard(series_id).read();
            let sealed = self.sealed_shard(series_id).read();
            if let Some(state) = active.get(&series_id) {
                if let Some(active_latest) = Self::latest_visible_timestamp_in_points(
                    state.points_in_partition_order(),
                    tombstone_ranges,
                    latest,
                    ceiling_inclusive,
                ) {
                    latest = latest.max(active_latest);
                }
            }
            if let Some(chunks) = sealed.get(&series_id) {
                for chunk in chunks.values() {
                    if let Some(chunk_latest) = Self::latest_visible_timestamp_in_chunk(
                        chunk,
                        tombstone_ranges,
                        latest,
                        ceiling_inclusive,
                    )? {
                        latest = latest.max(chunk_latest);
                    }
                }
            }
        }

        if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
            for chunk_ref in chunks {
                if let Some(chunk_latest) = self.latest_visible_timestamp_in_persisted_chunk(
                    chunk_ref,
                    tombstone_ranges,
                    latest,
                    ceiling_inclusive,
                )? {
                    latest = latest.max(chunk_latest);
                }
            }
        }

        Ok((latest != i64::MIN).then_some(latest))
    }

    fn latest_visible_timestamp_in_points<'a, I>(
        points: I,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        floor_exclusive: i64,
        ceiling_inclusive: i64,
    ) -> Option<i64>
    where
        I: IntoIterator<Item = &'a ChunkPoint>,
    {
        points
            .into_iter()
            .filter_map(|point| {
                (point.ts > floor_exclusive
                    && point.ts <= ceiling_inclusive
                    && Self::timestamp_survives_tombstones(point.ts, tombstone_ranges))
                .then_some(point.ts)
            })
            .max()
    }

    fn latest_visible_timestamp_in_decoded_chunk_points(
        chunk: &Chunk,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        floor_exclusive: i64,
        ceiling_inclusive: i64,
    ) -> Result<Option<i64>> {
        let decoded = chunk.decode_points()?;
        Ok(Self::latest_visible_timestamp_in_points(
            decoded.iter(),
            tombstone_ranges,
            floor_exclusive,
            ceiling_inclusive,
        ))
    }

    pub(super) fn latest_visible_timestamp_in_chunk(
        chunk: &Chunk,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        floor_exclusive: i64,
        ceiling_inclusive: i64,
    ) -> Result<Option<i64>> {
        if chunk.header.max_ts <= floor_exclusive || chunk.header.min_ts > ceiling_inclusive {
            return Ok(None);
        }

        let Some(tombstone_ranges) = tombstone_ranges else {
            if chunk.header.max_ts <= ceiling_inclusive {
                return Ok(Some(chunk.header.max_ts));
            }
            return Self::latest_visible_timestamp_in_decoded_chunk_points(
                chunk,
                None,
                floor_exclusive,
                ceiling_inclusive,
            );
        };
        if tombstone::interval_fully_tombstoned(
            chunk.header.min_ts,
            chunk.header.max_ts,
            tombstone_ranges,
        ) {
            return Ok(None);
        }
        if chunk.header.max_ts <= ceiling_inclusive
            && !tombstone::timestamp_is_tombstoned(chunk.header.max_ts, tombstone_ranges)
        {
            return Ok(Some(chunk.header.max_ts));
        }

        Self::latest_visible_timestamp_in_decoded_chunk_points(
            chunk,
            Some(tombstone_ranges),
            floor_exclusive,
            ceiling_inclusive,
        )
    }

    fn persisted_chunk_timestamp_search_index(
        &self,
        chunk_ref: &PersistedChunkRef,
    ) -> Result<Arc<crate::engine::encoder::TimestampSearchIndex>> {
        if let Some(cached) = self.persisted.timestamp_indexes.get(chunk_ref.sequence) {
            return Ok(cached);
        }

        // Build outside the cache lock so a slow payload decode cannot stall
        // concurrent cache lookups; `install` collapses double-builds.
        let segment_map = self.resolve_persisted_segment_map(chunk_ref.segment_slot)?;
        let payload = crate::engine::segment::chunk_payload_from_record(
            segment_map.as_slice(),
            chunk_ref.chunk_offset,
            chunk_ref.chunk_len,
        )?;
        let built = Arc::new(Encoder::build_timestamp_search_index_from_payload(
            chunk_ref.ts_codec,
            chunk_ref.point_count as usize,
            payload.as_ref(),
        )?);
        let installed = self.persisted.timestamp_indexes.install(
            chunk_ref.sequence,
            chunk_ref.segment_slot,
            Arc::clone(&built),
        );
        // Account only a fresh install (a concurrent double-build's winner
        // was already accounted); evicted bytes flow back through the
        // maintenance recompute.
        if Arc::ptr_eq(&installed, &built) {
            let built_bytes = built.memory_usage_bytes();
            self.add_included_memory_component_bytes(
                &self.memory.persisted_index_used_bytes,
                built_bytes,
            );
        }
        Ok(installed)
    }

    fn latest_visible_timestamp_in_persisted_chunk_payload(
        &self,
        chunk_ref: &PersistedChunkRef,
        payload: &[u8],
        timestamp_search_index: &crate::engine::encoder::TimestampSearchIndex,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        floor_exclusive: i64,
        ceiling_inclusive: i64,
    ) -> Result<Option<i64>> {
        let ts_payload = Encoder::timestamp_payload_from_chunk_payload(payload)?;
        let Some(mut block_idx) = timestamp_search_index
            .candidate_block_for_ceiling(chunk_ref.point_count as usize, ceiling_inclusive)
        else {
            return Ok(None);
        };

        loop {
            let timestamps = timestamp_search_index.decode_block(
                chunk_ref.point_count as usize,
                ts_payload,
                block_idx,
            )?;
            let block_floor_reached = timestamps
                .first()
                .copied()
                .is_some_and(|timestamp| timestamp <= floor_exclusive);

            for timestamp in timestamps.into_iter().rev() {
                if timestamp > ceiling_inclusive {
                    continue;
                }
                if timestamp <= floor_exclusive {
                    return Ok(None);
                }
                if Self::timestamp_survives_tombstones(timestamp, tombstone_ranges) {
                    return Ok(Some(timestamp));
                }
            }

            if block_floor_reached || block_idx == 0 {
                return Ok(None);
            }
            block_idx = block_idx.saturating_sub(1);
        }
    }

    pub(super) fn latest_visible_timestamp_in_persisted_chunk(
        &self,
        chunk_ref: &PersistedChunkRef,
        tombstone_ranges: Option<&[tombstone::TombstoneRange]>,
        floor_exclusive: i64,
        ceiling_inclusive: i64,
    ) -> Result<Option<i64>> {
        if chunk_ref.max_ts <= floor_exclusive || chunk_ref.min_ts > ceiling_inclusive {
            return Ok(None);
        }

        let Some(tombstone_ranges) = tombstone_ranges else {
            if chunk_ref.max_ts <= ceiling_inclusive {
                return Ok(Some(chunk_ref.max_ts));
            }
            let timestamp_search_index = self.persisted_chunk_timestamp_search_index(chunk_ref)?;
            let segment_map = self.resolve_persisted_segment_map(chunk_ref.segment_slot)?;
            let payload = crate::engine::segment::chunk_payload_from_record(
                segment_map.as_slice(),
                chunk_ref.chunk_offset,
                chunk_ref.chunk_len,
            )?;
            return self.latest_visible_timestamp_in_persisted_chunk_payload(
                chunk_ref,
                payload.as_ref(),
                timestamp_search_index.as_ref(),
                None,
                floor_exclusive,
                ceiling_inclusive,
            );
        };
        if tombstone::interval_fully_tombstoned(
            chunk_ref.min_ts,
            chunk_ref.max_ts,
            tombstone_ranges,
        ) {
            return Ok(None);
        }
        if chunk_ref.max_ts <= ceiling_inclusive
            && !tombstone::timestamp_is_tombstoned(chunk_ref.max_ts, tombstone_ranges)
        {
            return Ok(Some(chunk_ref.max_ts));
        }

        let segment_map = self.resolve_persisted_segment_map(chunk_ref.segment_slot)?;
        let payload = crate::engine::segment::chunk_payload_from_record(
            segment_map.as_slice(),
            chunk_ref.chunk_offset,
            chunk_ref.chunk_len,
        )?;
        let timestamp_search_index = self.persisted_chunk_timestamp_search_index(chunk_ref)?;
        self.latest_visible_timestamp_in_persisted_chunk_payload(
            chunk_ref,
            payload.as_ref(),
            timestamp_search_index.as_ref(),
            Some(tombstone_ranges),
            floor_exclusive,
            ceiling_inclusive,
        )
    }
}
