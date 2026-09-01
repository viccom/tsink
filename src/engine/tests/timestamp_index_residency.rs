//! Timestamp-search-index cache governance: global byte budget with LRU
//! eviction, transparent rebuild of evicted indexes, release on compaction
//! replacement, and zero pre-seated entries after install.
//!
//! These pin the resource face where resident memory grew unboundedly with
//! total persisted chunks because every chunk's lazily built anchor index
//! stayed cached forever after the first visibility read, and install
//! pre-seated an empty entry per chunk before any query touched it.

use std::sync::atomic::Ordering;

use super::super::PersistedChunkRef;
use super::*;

const ROUND_POINTS: i64 = 25;

fn persistent_storage_with_timestamp_index_budget(
    root: &Path,
    budget_bytes: usize,
) -> ChunkStorage {
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.timestamp_index_cache_budget_bytes = budget_bytes;
    ChunkStorage::new_with_data_path_and_options(
        ROUND_POINTS as usize,
        None,
        Some(root.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        options,
    )
    .unwrap()
}

fn persist_round(storage: &ChunkStorage, labels: &[Label], base_ts: i64) {
    let rows = (0..ROUND_POINTS)
        .map(|offset| {
            Row::with_labels(
                "cpu",
                labels.to_vec(),
                DataPoint::new(base_ts + offset, offset as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();
    storage.flush_all_active().unwrap();
    let outcome = storage.persist_segment_with_outcome().unwrap();
    assert!(outcome.persisted, "fixture round must persist a segment");
}

fn clear_sealed_chunks(storage: &ChunkStorage) {
    for shard in &storage.chunks.sealed_chunks {
        shard.write().clear();
    }
}

/// Runs a time-ranged visibility query strictly inside one round's chunk
/// span. The window end truncates the chunk (`end - 1 < max_ts`), so the
/// exact-scan read must consult the chunk's timestamp search index,
/// lazily building and caching it. Returns the selected series count.
fn touch_round_index(storage: &ChunkStorage, labels: &[Label], base_ts: i64) -> usize {
    clear_sealed_chunks(storage);
    let mut selection = SeriesSelection::new()
        .with_metric("cpu")
        .with_time_range(base_ts + 10, base_ts + 20);
    for label in labels {
        selection = selection.with_matcher(SeriesMatcher::new(
            &label.name,
            SeriesMatcherOp::Equal,
            &label.value,
        ));
    }
    let selected = storage.select_series(&selection).unwrap();
    assert!(
        !selected.is_empty(),
        "visibility query must find the fixture series"
    );
    selected.len()
}

/// The chunk ref covering `[base_ts, base_ts + 24]` for the fixture series
/// (one chunk per round at the fixture point cap).
fn round_chunk_ref(storage: &ChunkStorage, labels: &[Label], base_ts: i64) -> PersistedChunkRef {
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", labels)
        .unwrap()
        .series_id;
    let index = storage.persisted.persisted_index.read();
    *index
        .chunk_refs
        .get(&series_id)
        .expect("fixture series must have persisted chunk refs")
        .iter()
        .find(|chunk_ref| chunk_ref.min_ts == base_ts)
        .unwrap_or_else(|| panic!("fixture must persist a chunk starting at {base_ts}"))
}

fn round_chunk_sequence(storage: &ChunkStorage, labels: &[Label], base_ts: i64) -> u64 {
    round_chunk_ref(storage, labels, base_ts).sequence
}

fn resident_index_bytes(storage: &ChunkStorage) -> usize {
    usize::try_from(
        storage
            .persisted
            .timestamp_indexes
            .resident_bytes()
            .load(Ordering::Acquire),
    )
    .unwrap()
}

#[test]
fn timestamp_index_cache_budget_evicts_oldest_under_pressure() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    // 64 bytes hold at most two of the fixture's compact search indexes
    // (a 25-point chunk's index is ~32 bytes), so a third touch must evict
    // the oldest entry and keep residency within the budget.
    let storage = persistent_storage_with_timestamp_index_budget(temp_dir.path(), 64);

    for round in 0..3i64 {
        persist_round(&storage, &labels, round * 1000);
    }
    touch_round_index(&storage, &labels, 0);
    touch_round_index(&storage, &labels, 1000);
    touch_round_index(&storage, &labels, 2000);

    assert!(
        storage.persisted.timestamp_indexes.len() >= 1,
        "fixture touches must build and cache timestamp indexes"
    );
    assert!(
        !storage
            .persisted
            .timestamp_indexes
            .contains(round_chunk_sequence(&storage, &labels, 0)),
        "the oldest touched index must be evicted under the budget"
    );
    let resident = resident_index_bytes(&storage);
    assert!(
        resident <= 64,
        "resident index bytes {resident} must stay within the budget"
    );

    storage.close().unwrap();
}

#[test]
fn timestamp_index_cache_rebuilds_transparently_after_eviction() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    // A budget of 1 byte collapses residency to the single most recently
    // used index, so every re-read of an older round rebuilds it.
    let storage = persistent_storage_with_timestamp_index_budget(temp_dir.path(), 1);

    for round in 0..3i64 {
        persist_round(&storage, &labels, round * 1000);
    }

    let first_chunk_ref = round_chunk_ref(&storage, &labels, 0);
    let first_sequence = first_chunk_ref.sequence;
    assert_eq!(
        touch_round_index(&storage, &labels, 0),
        1,
        "the first touch must select the fixture series"
    );
    assert!(
        storage.persisted.timestamp_indexes.contains(first_sequence),
        "the first touch must cache its timestamp index"
    );
    let visible_before_eviction = storage
        .latest_visible_timestamp_in_persisted_chunk(&first_chunk_ref, None, 9, 19)
        .unwrap();

    assert_eq!(
        touch_round_index(&storage, &labels, 1000),
        1,
        "the evicting read must still select the fixture series"
    );
    assert!(
        !storage.persisted.timestamp_indexes.contains(first_sequence),
        "the policy floor keeps only the freshest index resident"
    );

    // Re-reading the first round must transparently rebuild the evicted
    // index, return the same visibility decision, and cache it again.
    assert_eq!(
        touch_round_index(&storage, &labels, 0),
        1,
        "a read after eviction must still select the fixture series"
    );
    assert!(
        storage.persisted.timestamp_indexes.contains(first_sequence),
        "the rebuilt index must be cached again"
    );
    let visible_after_rebuild = storage
        .latest_visible_timestamp_in_persisted_chunk(&first_chunk_ref, None, 9, 19)
        .unwrap();
    assert_eq!(
        visible_before_eviction, visible_after_rebuild,
        "rebuilt index must answer identically to the original build"
    );

    storage.close().unwrap();
}

#[test]
fn compaction_replacement_clears_timestamp_index_cache_for_source_slots() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_storage_with_timestamp_index_budget(temp_dir.path(), usize::MAX);

    // Four series sharing one time window: four overlapping L0 segments
    // that compaction must merge into a single L1 replacement.
    let all_labels = (0..4i64)
        .map(|round| vec![Label::new("host", format!("node-{round}"))])
        .collect::<Vec<_>>();
    for labels in &all_labels {
        let rows = (0..ROUND_POINTS)
            .map(|offset| {
                Row::with_labels("cpu", labels.clone(), DataPoint::new(offset, offset as f64))
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

    // Touch every source segment's index before compaction.
    let (source_roots, source_slots) = {
        let index = storage.persisted.persisted_index.read();
        let roots = index.segments_by_root.keys().cloned().collect::<Vec<_>>();
        let slots = index
            .segments_by_root
            .values()
            .map(|segment| segment.segment_slot)
            .collect::<Vec<_>>();
        (roots, slots)
    };
    assert_eq!(source_roots.len(), 4);
    for (round, labels) in all_labels.iter().enumerate() {
        touch_round_index(&storage, labels, 0);
        assert!(
            storage
                .persisted
                .timestamp_indexes
                .contains_slot(source_slots[round]),
            "source slot {} must have cached index entries before compaction",
            source_slots[round]
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
        "fixture must compact the overlapping cluster"
    );
    storage.refresh_dirty_persisted_segments_claimed().unwrap();

    {
        let index = storage.persisted.persisted_index.read();
        let live_slots = index
            .segments_by_root
            .values()
            .map(|segment| segment.segment_slot)
            .collect::<Vec<_>>();
        assert_eq!(
            live_slots.len(),
            1,
            "compaction must replace the cluster with one L1 segment"
        );
        for root in &source_roots {
            assert!(
                !index.segments_by_root.contains_key(root),
                "replacement must remove source roots from the index"
            );
        }
    }
    for slot in &source_slots {
        assert!(
            !storage.persisted.timestamp_indexes.contains_slot(*slot),
            "replacement must clear cached index entries for source slot {slot}"
        );
    }
    assert_eq!(
        storage.persisted.timestamp_indexes.len(),
        0,
        "the untouched replacement segment must not have cached index entries"
    );

    clear_sealed_chunks(&storage);
    for (round, labels) in all_labels.iter().enumerate() {
        let points = storage.select("cpu", labels, 0, ROUND_POINTS).unwrap();
        assert_eq!(
            points.len() as i64,
            ROUND_POINTS,
            "series {round} must read correctly from the replacement segment"
        );
    }

    storage.close().unwrap();
}

#[test]
fn install_no_longer_pre_seats_empty_timestamp_index_entries() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = persistent_storage_with_timestamp_index_budget(temp_dir.path(), usize::MAX);

    for round in 0..3i64 {
        persist_round(&storage, &labels, round * 1000);
    }
    assert_eq!(
        storage.persisted.timestamp_indexes.len(),
        0,
        "fresh installs must not pre-seat timestamp index entries"
    );
    assert_eq!(
        resident_index_bytes(&storage),
        0,
        "fresh installs must not account any resident index bytes"
    );

    storage.close().unwrap();

    // Boot reload must not pre-seat entries either.
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let loaded = load_segment_indexes(&lane_path).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.timestamp_index_cache_budget_bytes = usize::MAX;
    let storage = ChunkStorage::new_with_data_path_and_options(
        ROUND_POINTS as usize,
        None,
        Some(lane_path),
        None,
        loaded.next_segment_id,
        options,
    )
    .unwrap();
    storage.apply_loaded_segment_indexes(loaded, false).unwrap();
    assert_eq!(
        storage.persisted.timestamp_indexes.len(),
        0,
        "boot reload must not pre-seat timestamp index entries"
    );
    assert_eq!(
        resident_index_bytes(&storage),
        0,
        "boot reload must not account any resident index bytes"
    );

    storage.close().unwrap();
}
