//! Resident segment-map governance: global byte budget with LRU eviction,
//! transparent re-map of evicted segments, and release on compaction
//! replacement. These pin the resource face behind the deployment-level
//! terminations where commit grew monotonically with total segment data
//! because every live segment's chunk bytes stayed resident forever.

use std::sync::atomic::Ordering;

use super::*;

fn persistent_storage_with_mmap_budget(
    root: &Path,
    chunk_point_cap: usize,
    budget_bytes: usize,
) -> ChunkStorage {
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.persisted_mmap_budget_bytes = budget_bytes;
    ChunkStorage::new_with_data_path_and_options(
        chunk_point_cap,
        None,
        Some(root.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        options,
    )
    .unwrap()
}

fn persist_round(storage: &ChunkStorage, labels: &[Label], base_ts: i64, points: i64) {
    let rows = (0..points)
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

fn l0_segment_chunks_file_sizes(root: &Path) -> Vec<u64> {
    let mut sizes = std::fs::read_dir(root.join(NUMERIC_LANE_ROOT).join("segments").join("L0"))
        .unwrap()
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if !path.is_dir() {
                return None;
            }
            std::fs::metadata(path.join("chunks.bin"))
                .ok()
                .map(|meta| meta.len())
        })
        .collect::<Vec<_>>();
    sizes.sort_unstable();
    sizes
}

#[test]
fn mmap_residency_budget_collapses_to_freshest_map_and_remaps_transparently() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    // A budget of 1 byte forces the policy floor: only the most recently
    // installed mapping stays resident, everything older is evicted.
    let storage = persistent_storage_with_mmap_budget(temp_dir.path(), 8, 1);

    for round in 0..3i64 {
        persist_round(&storage, &labels, round * 1000, 25);
    }
    assert_eq!(
        storage.persisted.segment_maps.resident_count(),
        1,
        "with a budget below any single segment, residency must collapse to the freshest mapping"
    );

    // A full-range read crosses both evicted segments; every access must
    // transparently re-map from the chunks file with identical results.
    clear_sealed_chunks(&storage);
    let points = storage.select("cpu", &labels, 0, 3000).unwrap();
    assert_eq!(points.len(), 75, "re-mapped reads must return every point");
    assert_eq!(points[0].timestamp, 0);
    assert_eq!(points[74].timestamp, 2024);
    assert_eq!(points[0].value, 0.0);
    assert_eq!(points[74].value, 24.0);

    storage.close().unwrap();
}

#[test]
fn mmap_residency_budget_bounds_resident_bytes_across_restart_reload() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    {
        let storage = persistent_storage_with_mmap_budget(temp_dir.path(), 8, usize::MAX);
        for round in 0..3i64 {
            persist_round(&storage, &labels, round * 1000, 25);
        }
        storage.close().unwrap();
    }

    // Boot with a budget that cannot hold all three segments: the two most
    // recently loaded stay resident, the oldest is evicted.
    let sizes = l0_segment_chunks_file_sizes(temp_dir.path());
    assert_eq!(sizes.len(), 3, "fixture must persist three L0 segments");
    assert!(
        sizes.iter().all(|size| *size > 1),
        "fixture segments must be larger than the smallest possible budget"
    );
    let budget = usize::try_from(sizes[1] + sizes[2] + 1).unwrap();

    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.persisted_mmap_budget_bytes = budget;
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let loaded = load_segment_indexes(&lane_path).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(lane_path),
        None,
        loaded.next_segment_id,
        options,
    )
    .unwrap();
    storage.apply_loaded_segment_indexes(loaded, false).unwrap();

    assert_eq!(
        storage.persisted.segment_maps.resident_slots(),
        vec![1, 2],
        "boot reload must evict the oldest-loaded mapping under the budget"
    );
    let resident = usize::try_from(
        storage
            .persisted
            .segment_maps
            .resident_bytes()
            .load(Ordering::Acquire),
    )
    .unwrap();
    assert!(
        resident <= budget,
        "resident mapping bytes {resident} must stay within the budget {budget}"
    );

    clear_sealed_chunks(&storage);
    let points = storage.select("cpu", &labels, 0, 3000).unwrap();
    assert_eq!(
        points.len(),
        75,
        "evicted segments must re-map transparently"
    );
    assert_eq!(points[0].timestamp, 0);
    assert_eq!(points[74].timestamp, 2024);

    storage.close().unwrap();
}

#[test]
fn compaction_replacement_releases_source_segment_maps() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_storage_with_mmap_budget(temp_dir.path(), 8, usize::MAX);

    // Four series sharing one time window: four overlapping L0 segments
    // that compaction must merge into a single L1 replacement.
    let all_labels = (0..4i64)
        .map(|round| vec![Label::new("host", format!("node-{round}"))])
        .collect::<Vec<_>>();
    for labels in &all_labels {
        let rows = (0..25)
            .map(|offset| {
                Row::with_labels("cpu", labels.clone(), DataPoint::new(offset, offset as f64))
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.flush_all_active().unwrap();
        assert!(storage.persist_segment_with_outcome().unwrap().persisted);
    }

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
    for slot in &source_slots {
        assert!(
            storage.persisted.segment_maps.contains_slot(*slot),
            "fixture segments must be resident before compaction"
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
            assert!(
                !root.exists(),
                "compactor must delete the source segment directory on disk"
            );
        }
        assert_eq!(storage.persisted.segment_maps.resident_slots(), live_slots);
    }
    for slot in &source_slots {
        assert!(
            !storage.persisted.segment_maps.contains_slot(*slot),
            "replacement must release the source segment mapping (slot {slot})"
        );
    }

    clear_sealed_chunks(&storage);
    for (round, labels) in all_labels.iter().enumerate() {
        let points = storage.select("cpu", labels, 0, 25).unwrap();
        assert_eq!(
            points.len(),
            25,
            "series {round} must read correctly from the replacement segment"
        );
    }

    storage.close().unwrap();
}
