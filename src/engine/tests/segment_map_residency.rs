//! Resident segment-map governance: global byte budget with LRU eviction,
//! transparent re-map of evicted segments, and release on compaction
//! replacement. These pin the resource face behind the deployment-level
//! terminations where commit grew monotonically with total segment data
//! because every live segment's chunk bytes stayed resident forever.

use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::*;
use crate::engine::storage_engine::metrics::StorageObservabilityCounters;
use crate::engine::storage_engine::state::SegmentMapRegistry;
use crate::mmap::create_mmap;

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

/// W4-T0：compaction 先删源段目录、后刷新内存索引，被预算驱逐的槽位在窗口
/// 内重映射会撞上已删除的 chunks 路径。`get_or_remap` 必须在 NotFound 时按
/// 注入的 re-resolver（装配层按当前持久化索引实现）重新解析 slot 并重试一次
/// 装载；解析不到时只重试一次、返回原错误，且每次 NotFound 计入
/// `persisted_remap_not_found_total`。
#[test]
fn segment_map_remap_after_source_deleted_retries_once() {
    let temp_dir = TempDir::new().unwrap();
    // Two distinguishable files: the deleted source (40 bytes) and the
    // replacement the refreshed persisted index points at (80 bytes).
    let source_path = temp_dir.path().join("source").join("chunks.bin");
    let replacement_path = temp_dir.path().join("replacement").join("chunks.bin");
    std::fs::create_dir_all(source_path.parent().unwrap()).unwrap();
    std::fs::create_dir_all(replacement_path.parent().unwrap()).unwrap();
    std::fs::write(&source_path, vec![0u8; 40]).unwrap();
    std::fs::write(&replacement_path, vec![0u8; 80]).unwrap();

    let observability = Arc::new(StorageObservabilityCounters::default());
    let registry = SegmentMapRegistry::new(usize::MAX, Arc::clone(&observability));
    let slot = 7;

    let source_map = Arc::new(create_mmap(File::open(&source_path).unwrap()).unwrap());
    registry.install(slot, source_path.clone(), source_map);
    assert!(
        registry.get_or_remap(slot).unwrap().is_some(),
        "fixture slot must resolve while its file exists"
    );

    // Evict the mapping and delete the source file: the registry slot now
    // points at a path the compactor already removed (the replacement-delete
    // window), and a re-map hits NotFound.
    registry.evict_all_for_tests();
    assert_eq!(registry.resident_count(), 0);
    std::fs::remove_file(&source_path).unwrap();

    let resolver_calls = Arc::new(AtomicUsize::new(0));
    {
        let resolver_calls = Arc::clone(&resolver_calls);
        let replacement_path = replacement_path.clone();
        registry.set_remap_resolver(Arc::new(move |_slot| {
            resolver_calls.fetch_add(1, Ordering::SeqCst);
            Some(replacement_path.clone())
        }));
    }

    let map = registry
        .get_or_remap(slot)
        .unwrap()
        .expect("re-resolved remap must survive the deleted source path");
    assert_eq!(
        map.len(),
        80,
        "remap must follow the re-resolved replacement path, not the deleted source"
    );
    assert_eq!(
        resolver_calls.load(Ordering::SeqCst),
        1,
        "a NotFound remap must re-resolve exactly once"
    );
    assert_eq!(
        observability
            .flush
            .persisted_remap_not_found_total
            .load(Ordering::Acquire),
        1,
        "each NotFound remap must be counted"
    );

    // Negative branch: the refreshed index no longer knows the slot either.
    // The retry must happen exactly once and surface the original error —
    // a retry loop would drive the resolver call count past two.
    registry.evict_all_for_tests();
    std::fs::remove_file(&replacement_path).unwrap();
    {
        let resolver_calls = Arc::clone(&resolver_calls);
        registry.set_remap_resolver(Arc::new(move |_slot| {
            resolver_calls.fetch_add(1, Ordering::SeqCst);
            None
        }));
    }

    let err = registry
        .get_or_remap(slot)
        .err()
        .expect("remap must fail when the re-resolver also loses the slot");
    assert!(
        matches!(&err, TsinkError::IoWithPath { path, .. } if path == &source_path),
        "the original NotFound must surface when the slot cannot be re-resolved, got {err}"
    );
    assert_eq!(
        resolver_calls.load(Ordering::SeqCst),
        2,
        "the second NotFound must re-resolve exactly once, not loop"
    );
    assert_eq!(
        observability
            .flush
            .persisted_remap_not_found_total
            .load(Ordering::Acquire),
        2,
    );
}
