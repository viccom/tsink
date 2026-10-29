use super::*;
use std::borrow::Cow;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::engine::storage_engine) struct MemoryDeltaBytes {
    pub(in crate::engine::storage_engine) added_bytes: usize,
    pub(in crate::engine::storage_engine) removed_bytes: usize,
}

impl MemoryDeltaBytes {
    pub(in crate::engine::storage_engine) fn from_totals(
        added_bytes: usize,
        removed_bytes: usize,
    ) -> Self {
        Self {
            added_bytes,
            removed_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn between(before: usize, after: usize) -> Self {
        let mut delta = Self::default();
        delta.record_change(before, after);
        delta
    }

    pub(in crate::engine::storage_engine) fn record_change(&mut self, before: usize, after: usize) {
        if after >= before {
            self.record_addition(after.saturating_sub(before));
        } else {
            self.record_removal(before.saturating_sub(after));
        }
    }

    pub(in crate::engine::storage_engine) fn record_addition(&mut self, bytes: usize) {
        self.added_bytes = self.added_bytes.saturating_add(bytes);
    }

    pub(in crate::engine::storage_engine) fn record_removal(&mut self, bytes: usize) {
        self.removed_bytes = self.removed_bytes.saturating_add(bytes);
    }

    pub(in crate::engine::storage_engine) fn record_replacement<T>(
        &mut self,
        added_bytes: usize,
        replaced: Option<&T>,
        measure: impl Fn(&T) -> usize,
    ) {
        self.record_addition(added_bytes);
        if let Some(replaced) = replaced {
            self.record_removal(measure(replaced));
        }
    }
}

pub(in crate::engine::storage_engine) fn saturating_u64_from_usize(value: usize) -> u64 {
    value.min(u64::MAX as usize) as u64
}

/// O(touched) baseline for incremental visibility-state memory accounting.
///
/// 2026-08-29 load-test follow-up (plan Task 13): the write path used to call
/// `series_visibility_state_memory_usage_bytes` before AND after every
/// visibility-cache mutation — two full O(cached-series) scans per touched
/// shard per batch (~390 entry accesses per row at 100k series), executed
/// while holding the recency mutex and all three global cache write locks.
///
/// The caches only mutate through the three `with_visibility_state_memory_delta`
/// call sites (merge-on-write, rebuild-on-refresh, clear-on-delete), and std
/// HashMap capacities only ever grow on insert. The exact before/after
/// difference of the full estimator therefore reduces to:
/// 1. each map's capacity delta (O(1) reads — removals never shrink std
///    HashMap capacity), and
/// 2. the ranges-Vec capacity delta of exactly the summaries the mutation
///    touches (O(1) per touched series; untouched entries cannot change).
///
/// `refresh_memory_usage()` keeps running the full recompute, so drift from
/// any future out-of-band mutation still surfaces through the reconciliation
/// face (reconcile tests plus the bootstrap/shutdown refresh snaps and the
/// rate-limited flush reconciliation anchor).
#[derive(Clone)]
pub(in crate::engine::storage_engine) struct VisibilityStateMemoryBaseline {
    summaries_capacity: usize,
    cache_capacity: usize,
    bounded_cache_capacity: usize,
    /// Ranges-Vec capacities of the pre-mutation summaries for the series
    /// this mutation may touch (distinct ids; absent entries contribute 0).
    touched_ranges_capacities: Vec<(SeriesId, usize)>,
}

impl VisibilityStateMemoryBaseline {
    pub(in crate::engine::storage_engine) fn capture(
        ranges_touched: &[SeriesId],
        summaries: &HashMap<SeriesId, SeriesVisibilitySummary>,
        cache: &HashMap<SeriesId, Option<i64>>,
        bounded_cache: &HashMap<SeriesId, Option<i64>>,
    ) -> Self {
        Self {
            summaries_capacity: summaries.capacity(),
            cache_capacity: cache.capacity(),
            bounded_cache_capacity: bounded_cache.capacity(),
            touched_ranges_capacities: ranges_touched
                .iter()
                .map(|series_id| {
                    let before_capacity = summaries
                        .get(series_id)
                        .map(|summary| summary.ranges.capacity())
                        .unwrap_or(0);
                    (*series_id, before_capacity)
                })
                .collect(),
        }
    }

    /// Exact incremental counterpart of the before/after difference of
    /// `series_visibility_state_memory_usage_bytes` for a mutation that only
    /// inserted into (or removed) the tracked maps and only mutated the
    /// ranges of the captured series.
    pub(in crate::engine::storage_engine) fn memory_delta_bytes(
        &self,
        summaries: &HashMap<SeriesId, SeriesVisibilitySummary>,
        cache: &HashMap<SeriesId, Option<i64>>,
        bounded_cache: &HashMap<SeriesId, Option<i64>>,
    ) -> MemoryDeltaBytes {
        let mut delta = MemoryDeltaBytes::default();
        delta.record_change(
            self.summaries_capacity
                .saturating_mul(std::mem::size_of::<(SeriesId, SeriesVisibilitySummary)>()),
            summaries
                .capacity()
                .saturating_mul(std::mem::size_of::<(SeriesId, SeriesVisibilitySummary)>()),
        );
        delta.record_change(
            self.cache_capacity
                .saturating_mul(std::mem::size_of::<(SeriesId, Option<i64>)>()),
            cache
                .capacity()
                .saturating_mul(std::mem::size_of::<(SeriesId, Option<i64>)>()),
        );
        delta.record_change(
            self.bounded_cache_capacity
                .saturating_mul(std::mem::size_of::<(SeriesId, Option<i64>)>()),
            bounded_cache
                .capacity()
                .saturating_mul(std::mem::size_of::<(SeriesId, Option<i64>)>()),
        );

        let range_stride = std::mem::size_of::<SeriesVisibilityRangeSummary>();
        for (series_id, before_capacity) in &self.touched_ranges_capacities {
            let after_capacity = summaries
                .get(series_id)
                .map(|summary| summary.ranges.capacity())
                .unwrap_or(0);
            delta.record_change(
                before_capacity.saturating_mul(range_stride),
                after_capacity.saturating_mul(range_stride),
            );
        }
        delta
    }

    /// cfg(test) parity guard: verifies this incremental delta against a real
    /// full before/after re-measurement for the SAME mutation. Every test
    /// build turns each cache mutation into an incremental-vs-full
    /// reconciliation, so accounting drift fails the exact test that caused
    /// it instead of surfacing as a distant admission mis-rejection.
    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn assert_matches_full_estimate(
        &self,
        delta: MemoryDeltaBytes,
        full_before: usize,
        summaries: &HashMap<SeriesId, SeriesVisibilitySummary>,
        cache: &HashMap<SeriesId, Option<i64>>,
        bounded_cache: &HashMap<SeriesId, Option<i64>>,
    ) {
        let full_after = ChunkStorage::series_visibility_state_memory_usage_bytes(
            summaries,
            cache,
            bounded_cache,
        );
        let incremental_net = delta.added_bytes.abs_diff(delta.removed_bytes);
        let full_net = full_after.abs_diff(full_before);
        assert_eq!(
            incremental_net, full_net,
            "visibility memory accounting drifted: incremental delta {incremental_net} B vs full re-measurement {full_net} B (before {full_before} B, after {full_after} B)"
        );
        assert_eq!(
            (delta.removed_bytes > delta.added_bytes),
            (full_after < full_before),
            "visibility memory accounting drift direction mismatch (before {full_before} B, after {full_after} B)"
        );
    }
}

pub(in crate::engine::storage_engine) fn elapsed_nanos_u64(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

pub(in crate::engine::storage_engine) fn current_unix_millis_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

pub(in crate::engine::storage_engine) fn duration_to_timestamp_units(
    duration: Duration,
    precision: TimestampPrecision,
) -> i64 {
    match precision {
        TimestampPrecision::Seconds => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        TimestampPrecision::Milliseconds => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        TimestampPrecision::Microseconds => i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
        TimestampPrecision::Nanoseconds => i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX),
    }
}

pub(in crate::engine::storage_engine) fn current_unix_timestamp_units(
    precision: TimestampPrecision,
) -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    duration_to_timestamp_units(now, precision)
}

pub(in crate::engine::storage_engine) fn partition_id_for_timestamp(
    timestamp: i64,
    partition_window: i64,
) -> i64 {
    timestamp.div_euclid(partition_window.max(1))
}

pub(in crate::engine::storage_engine) fn value_heap_bytes(value: &Value) -> usize {
    match value {
        Value::Bytes(bytes) => bytes.capacity(),
        Value::String(text) => text.capacity(),
        Value::Histogram(histogram) => {
            std::mem::size_of::<crate::NativeHistogram>()
                + histogram.negative_spans.capacity()
                    * std::mem::size_of::<crate::HistogramBucketSpan>()
                + histogram.negative_deltas.capacity() * std::mem::size_of::<i64>()
                + histogram.negative_counts.capacity() * std::mem::size_of::<f64>()
                + histogram.positive_spans.capacity()
                    * std::mem::size_of::<crate::HistogramBucketSpan>()
                + histogram.positive_deltas.capacity() * std::mem::size_of::<i64>()
                + histogram.positive_counts.capacity() * std::mem::size_of::<f64>()
                + histogram.custom_values.capacity() * std::mem::size_of::<f64>()
        }
        _ => 0,
    }
}

pub(in crate::engine::storage_engine) fn lane_for_value(value: &Value) -> ValueLane {
    match value {
        Value::Bytes(_) | Value::String(_) | Value::Histogram(_) => ValueLane::Blob,
        _ => ValueLane::Numeric,
    }
}

pub(in crate::engine::storage_engine) fn persisted_chunk_payload<'a>(
    persisted_segment_maps: &'a HashMap<usize, Arc<PlatformMmap>>,
    chunk_ref: &PersistedChunkRef,
) -> Result<Cow<'a, [u8]>> {
    let Some(mapped_segment) = persisted_segment_maps.get(&chunk_ref.segment_slot) else {
        return Err(TsinkError::DataCorruption(format!(
            "missing mapped segment slot {}",
            chunk_ref.segment_slot
        )));
    };
    crate::engine::segment::chunk_payload_from_record(
        mapped_segment.as_slice(),
        chunk_ref.chunk_offset,
        chunk_ref.chunk_len,
    )
}

impl ChunkStorage {
    /// Resolves a persisted segment slot to its resident mapping,
    /// transparently re-mapping the chunks file when the registry evicted
    /// it under the global persisted-mmap budget. Unknown slots (segment
    /// absent from the index) stay a corruption error.
    pub(in crate::engine::storage_engine) fn resolve_persisted_segment_map(
        &self,
        slot: usize,
    ) -> Result<Arc<PlatformMmap>> {
        self.persisted
            .segment_maps
            .get_or_remap(slot)?
            .ok_or_else(|| {
                TsinkError::DataCorruption(format!("missing mapped segment slot {slot}"))
            })
    }
    pub(in crate::engine::storage_engine) fn ensure_open(&self) -> Result<()> {
        if self
            .observability
            .health
            .fail_fast_triggered
            .load(Ordering::SeqCst)
        {
            return Err(TsinkError::StorageShuttingDown);
        }
        if self.coordination.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
            return Err(TsinkError::StorageClosed);
        }
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn install_data_path_process_lock(
        &self,
        data_path_process_lock: DataPathProcessLock,
    ) {
        *self.coordination.data_path_process_lock.lock() = Some(data_path_process_lock);
    }

    pub(in crate::engine::storage_engine) fn release_data_path_process_lock(&self) {
        self.coordination.data_path_process_lock.lock().take();
    }

    pub(in crate::engine::storage_engine) fn background_maintenance_gate(
        &self,
    ) -> MutexGuard<'_, ()> {
        self.coordination.background_maintenance_lock.lock()
    }

    pub(in crate::engine::storage_engine) fn compaction_gate(&self) -> MutexGuard<'_, ()> {
        self.coordination.compaction_lock.lock()
    }

    /// Serializes against compaction's segment-replacement marker operations
    /// only (marker write + source segment deletion). The flush persist
    /// pipeline takes this instead of [`Self::compaction_gate`] so a long
    /// compaction pass cannot stall `mark_durable_through` and WAL prefix
    /// reclaim; lock order is maintenance > compaction > segment replacement.
    pub(in crate::engine::storage_engine) fn segment_replacement_gate(&self) -> MutexGuard<'_, ()> {
        self.coordination.segment_replacement_lock.lock()
    }

    pub(in crate::engine::storage_engine) fn lock_compaction_gate(
        compaction_lock: &Mutex<()>,
    ) -> MutexGuard<'_, ()> {
        compaction_lock.lock()
    }

    pub(in crate::engine::storage_engine) fn current_timestamp_units(&self) -> i64 {
        #[cfg(test)]
        {
            let override_timestamp = self.current_time_override.load(Ordering::Acquire);
            if override_timestamp != i64::MIN {
                return override_timestamp;
            }
        }

        current_unix_timestamp_units(self.runtime.timestamp_precision)
    }

    pub(in crate::engine::storage_engine) fn current_future_skew_cutoff(&self) -> i64 {
        self.current_timestamp_units()
            .saturating_add(self.runtime.future_skew_window)
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn set_current_time_override(&self, timestamp: i64) {
        self.current_time_override
            .store(timestamp, Ordering::Release);
        self.bump_live_series_pruning_generation();
    }
}
