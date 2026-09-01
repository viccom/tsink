use super::*;

#[test]
fn reenable_wal_after_wal_disabled_run_ignores_stale_wal_generation() {
    let temp_dir = TempDir::new().unwrap();
    let stale_labels = vec![Label::new("host", "stale")];
    let fresh_labels = vec![Label::new("host", "fresh")];

    let stale_series_id = 1;
    let wal = FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    wal.append_series_definition(&SeriesDefinitionFrame {
        series_id: stale_series_id,
        metric: "metric".to_string(),
        labels: stale_labels.clone(),
    })
    .unwrap();
    let stale_batch = SamplesBatchFrame::from_points(
        stale_series_id,
        ValueLane::Numeric,
        &[
            ChunkPoint {
                ts: 1,
                value: Value::F64(1.0),
            },
            ChunkPoint {
                ts: 2,
                value: Value::F64(2.0),
            },
        ],
    )
    .unwrap();
    wal.append_samples(&[stale_batch]).unwrap();
    drop(wal);

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "metric",
                fresh_labels.clone(),
                DataPoint::new(10, 10.0),
            )])
            .unwrap();
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let fresh = reopened.select("metric", &fresh_labels, 0, 20).unwrap();
    assert_eq!(fresh, vec![DataPoint::new(10, 10.0)]);
    let stale = reopened.select("metric", &stale_labels, 0, 20).unwrap();
    assert!(stale.is_empty());

    reopened.close().unwrap();
}

#[test]
fn reopens_from_segment_files_without_wal() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("seg", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("seg", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("seg", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select("seg", &labels, 0, 10).unwrap();
    assert_eq!(points.len(), 3);
    assert_eq!(points[0], DataPoint::new(1, 1.0));
    assert_eq!(points[2], DataPoint::new(3, 3.0));

    let segments_root = temp_dir
        .path()
        .join(NUMERIC_LANE_ROOT)
        .join("segments")
        .join("L0");
    assert!(
        segments_root.exists(),
        "numeric lane segments should exist at {:?}",
        segments_root
    );
    let mut found_segment = false;
    for entry in std::fs::read_dir(segments_root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found_segment = true;
            assert!(path.join("manifest.bin").exists());
            assert!(path.join("chunks.bin").exists());
            assert!(path.join("chunk_index.bin").exists());
            assert!(path.join("series.bin").exists());
            assert!(path.join("postings.bin").exists());
        }
    }
    assert!(found_segment);

    reopened.close().unwrap();
}

#[test]
fn reopen_rejects_registry_snapshot_that_conflicts_with_segment_series_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let labels = vec![Label::new("host", "a")];
    let series_id = {
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("seg", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("seg", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let series_id = storage
            .catalog
            .registry
            .read()
            .resolve_existing("seg", &labels)
            .unwrap()
            .series_id;
        storage.close().unwrap();
        series_id
    };

    let conflicting_registry = SeriesRegistry::new();
    conflicting_registry
        .register_series_with_id(series_id, "wrong_metric", &[Label::new("host", "wrong")])
        .unwrap();
    conflicting_registry
        .persist_to_path(&checkpoint_path)
        .unwrap();
    let conflicting_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select("seg", &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        conflicting_checkpoint_bytes
    );
    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("healed checkpoint should reload after reopen");
    assert!(loaded_registry
        .registry
        .resolve_existing("seg", &labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("wrong_metric", &[Label::new("host", "wrong")])
        .is_none());
    reopened.close().unwrap();
}

#[test]
fn isolates_numeric_and_blob_segments_and_merges_in_queries() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_labels = vec![Label::new("kind", "numeric")];
    let blob_labels = vec![Label::new("kind", "blob")];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("mix", numeric_labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("mix", numeric_labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("mix", blob_labels.clone(), DataPoint::new(1, "a")),
                Row::with_labels("mix", blob_labels.clone(), DataPoint::new(2, "b")),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_wal_enabled(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let numeric = reopened.select("mix", &numeric_labels, 0, 10).unwrap();
    let blob = reopened.select("mix", &blob_labels, 0, 10).unwrap();
    assert_eq!(numeric.len(), 2);
    assert_eq!(blob.len(), 2);
    assert_eq!(blob[0], DataPoint::new(1, "a"));
    assert_eq!(blob[1], DataPoint::new(2, "b"));

    let mut all = reopened.select_all("mix", 0, 10).unwrap();
    all.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].1.len(), 2);
    assert_eq!(all[1].1.len(), 2);

    let numeric_root = temp_dir
        .path()
        .join(NUMERIC_LANE_ROOT)
        .join("segments")
        .join("L0");
    let blob_root = temp_dir
        .path()
        .join(BLOB_LANE_ROOT)
        .join("segments")
        .join("L0");
    assert!(numeric_root.exists());
    assert!(blob_root.exists());

    reopened.close().unwrap();
}

#[test]
fn replay_highwater_is_conservative_when_one_configured_lane_has_no_segments() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);
    let labels = vec![Label::new("kind", "numeric")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("watermark_gap", &labels)
        .unwrap()
        .series_id;

    let mut numeric_chunks = HashMap::new();
    numeric_chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, &[(1, 1.0)])],
    );
    SegmentWriter::new(&numeric_lane_path, 0, 1)
        .unwrap()
        .write_segment_with_wal_highwater(
            &registry,
            &numeric_chunks,
            WalHighWatermark {
                segment: 3,
                frame: 7,
            },
        )
        .unwrap();

    let loaded_numeric = load_segment_indexes(&numeric_lane_path).unwrap();
    let loaded_blob = load_segment_indexes(&blob_lane_path).unwrap();
    let merged = merge_loaded_segment_indexes(loaded_numeric, loaded_blob, true, true).unwrap();
    assert_eq!(merged.wal_replay_highwater, WalHighWatermark::default());
}

#[test]
fn persist_segment_rolls_back_published_lane_when_other_lane_fails() {
    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);
    std::fs::write(&blob_lane_path, b"not-a-directory").unwrap();

    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(numeric_lane_path.clone()),
        Some(blob_lane_path),
        1,
        ChunkStorageOptions::default(),
    )
    .unwrap();
    let numeric_labels = vec![Label::new("kind", "numeric")];
    let blob_labels = vec![Label::new("kind", "blob")];
    storage
        .insert_rows(&[
            Row::with_labels(
                "lane_atomicity",
                numeric_labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "lane_atomicity",
                blob_labels.clone(),
                DataPoint::new(1, "a"),
            ),
        ])
        .unwrap();
    storage.flush_all_active().unwrap();

    assert!(storage.persist_segment().is_err());
    assert!(load_segments_for_level(&numeric_lane_path, 0)
        .unwrap()
        .is_empty());
}

#[test]
fn persist_completes_while_a_compaction_pass_holds_the_compaction_gate() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);

    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(lane_path),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[Row::with_labels(
            "gate_persist",
            vec![Label::new("host", "a")],
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage.flush_all_active().unwrap();

    // Simulate a long compaction pass (e.g. a multi-GB window load/merge)
    // holding the compaction gate for the whole test body.
    let compaction_guard = storage.compaction_gate();

    let persisting_storage = Arc::clone(&storage);
    let (persist_done, persist_result) = std::sync::mpsc::channel();
    let persist_handle = std::thread::spawn(move || {
        let result = persisting_storage.persist_segment().map(|_| ());
        let _ = persist_done.send(result);
    });

    match persist_result.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("persist failed under a concurrent compaction pass: {err}"),
        Err(_) => panic!(
            "persist stayed blocked >5s on the compaction gate; a wedged compaction pass stalls mark_durable_through and WAL reclaim until death"
        ),
    }

    drop(compaction_guard);
    persist_handle.join().unwrap();
    storage.close().unwrap();
}

#[test]
fn persist_segment_stamps_wal_highwater_when_wal_is_enabled() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);

    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "wal_highwater_persist",
            labels,
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage.flush_all_active().unwrap();
    storage.persist_segment().unwrap();

    let segments = load_segments_for_level(&lane_path, 0).unwrap();
    assert!(!segments.is_empty());
    assert!(
        segments
            .iter()
            .any(|segment| segment.manifest.wal_highwater > WalHighWatermark::default()),
        "persisted segment must include WAL replay highwater when WAL is enabled"
    );

    storage.close().unwrap();
}

#[test]
fn close_reconciles_compacted_segments_before_checkpointing_registry() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join("lane_numeric");
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let labels = vec![Label::new("host", "bench")];
    let storage = ChunkStorage::new_with_data_path_and_options(
        1,
        None,
        Some(lane_path),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    for offset in 0..64 {
        storage
            .insert_rows(&[Row::with_labels(
                "close_compaction_checkpoint_metric",
                labels.clone(),
                DataPoint::new(1_000_000_000 + offset, offset as f64),
            )])
            .unwrap();
        storage.flush().unwrap();
    }

    storage
        .insert_rows(&[Row::with_labels(
            "close_compaction_checkpoint_metric",
            labels.clone(),
            DataPoint::new(1_000_000_064, 64.0),
        )])
        .unwrap();
    storage.flush().unwrap();
    storage.close().unwrap();

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("close should checkpoint the reconciled registry");
    assert!(loaded_registry
        .registry
        .resolve_existing("close_compaction_checkpoint_metric", &labels)
        .is_some());
}

#[test]
fn background_flush_seals_rotated_partition_before_reaching_chunk_cap() {
    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
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
            partition_window: 1,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            persisted_mmap_budget_bytes: crate::storage::DEFAULT_PERSISTED_MMAP_BUDGET_BYTES,
            timestamp_index_cache_budget_bytes:
                crate::storage::DEFAULT_TIMESTAMP_INDEX_CACHE_BUDGET_BYTES,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            drain_time_budget: crate::engine::COMPACTION_DRAIN_TIME_BUDGET,
            source_window_segments: crate::engine::compactor::DEFAULT_SOURCE_WINDOW_SEGMENTS,
            output_segment_point_budget:
                crate::engine::compactor::DEFAULT_OUTPUT_SEGMENT_POINT_BUDGET,
            background_threads_enabled: true,
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

    let labels = vec![Label::new("host", "a")];
    storage
        .insert_rows(&[
            Row::with_labels("partitioned", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("partitioned", labels.clone(), DataPoint::new(2, 2.0)),
        ])
        .unwrap();
    storage.flush_background_eligible_active().unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("partitioned", &labels)
        .unwrap()
        .series_id;

    let sealed = storage.chunks.sealed_chunks[ChunkStorage::series_shard_idx(series_id)].read();
    let chunks = sealed.get(&series_id).unwrap().values().collect::<Vec<_>>();
    assert_eq!(
        chunks.len(),
        1,
        "background flush should seal the non-current partition head before chunk cap"
    );
    assert_eq!(chunks[0].header.min_ts, 1);
    assert_eq!(chunks[0].header.max_ts, 1);

    let active = storage.chunks.active_builders[ChunkStorage::series_shard_idx(series_id)].read();
    assert_eq!(active.get(&series_id).unwrap().point_count(), 1);
}

#[test]
fn persist_reclaims_covered_wal_prefix_when_active_writes_block_full_reset() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let metric = "wal_prefix_reclaim";
    let labels = vec![Label::new("host", "a")];

    // Tiny segments (~one frame each) roll multiple WAL files quickly; a fresh
    // engine also starts without any registry catalog sidecar, so the reclaim
    // below must not depend on catalog validation succeeding.
    let wal = FramedWal::open_with_options(&wal_dir, WalSyncMode::PerAppend, 128, 88).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(lane_path),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    let mut ts = 1i64;
    for _ in 0..4 {
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), DataPoint::new(ts, ts as f64)),
                Row::with_labels(
                    metric,
                    labels.clone(),
                    DataPoint::new(ts + 1, (ts + 1) as f64),
                ),
            ])
            .unwrap();
        ts += 2;
    }
    storage.flush_all_active().unwrap();
    // Sustained-write model (T21): a frame lands after the would-be flush
    // snapshot, so the WAL keeps an uncovered active tail and the all-or-nothing
    // full reset can never fire while ingestion continues.
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(ts, ts as f64),
        )])
        .unwrap();

    let before = wal_segment_ids_and_size(&wal_dir);
    assert!(
        before.len() >= 3,
        "fixture must roll multiple WAL segments, got {before:?}"
    );

    storage.persist_segment().unwrap();

    let after = wal_segment_ids_and_size(&wal_dir);
    assert!(
        after.first().map(|(id, _)| *id).unwrap_or(0)
            > before.first().map(|(id, _)| *id).unwrap_or(0),
        "covered WAL prefix segments must be reclaimed after persist even when \
         active writes block a full reset: before={before:?} after={after:?}"
    );

    let points = storage.select(metric, &labels, 0, 100).unwrap();
    assert_eq!(points.len(), 9, "reclaim must not lose data");

    storage.close().unwrap();
}

#[test]
fn wal_disk_usage_stays_bounded_under_sustained_write_persist_cycles() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let metric = "wal_bounded_reclaim";
    let labels = vec![Label::new("host", "a")];

    let wal = FramedWal::open_with_options(&wal_dir, WalSyncMode::PerAppend, 128, 88).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(lane_path),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    let mut ts = 1i64;
    let rounds = 12;
    for round in 0..rounds {
        // Sustained-ingestion shape from the T21 incident: chunks seal
        // eagerly at the cap while the latest single-point write stays in an
        // unsealed head, so every persist snapshot lags the appended WAL and
        // the all-or-nothing full reset can never fire. Round 0 primes the
        // residue head (3 points -> one sealed chunk + one active point);
        // later rounds keep it (2 points -> one sealed chunk + residue).
        let inserts = if round == 0 { 3 } else { 2 };
        for _ in 0..inserts {
            storage
                .insert_rows(&[Row::with_labels(
                    metric,
                    labels.clone(),
                    DataPoint::new(ts, ts as f64),
                )])
                .unwrap();
            ts += 1;
        }
        storage.persist_segment().unwrap();
    }

    let after = wal_segment_ids_and_size(&wal_dir);
    assert!(
        after.len() <= 8,
        "WAL disk usage must stay bounded under sustained write+persist cycles, got {rounds} \
         rounds and {after:?} segments still on disk"
    );

    let points = storage.select(metric, &labels, 0, 1000).unwrap();
    assert_eq!(points.len() as i64, 3 + 2 * (rounds - 1));

    storage.close().unwrap();
}

#[test]
fn persist_splits_large_flush_snapshot_into_bounded_l0_segments() {
    let temp_dir = TempDir::new().unwrap();
    let storage =
        persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Milliseconds, 1024);

    // 8 series x 156 sealed chunks x 1024 points = 1,277,952 points: a
    // snapshot well past a ~1M-point shard budget. The un-sharded writer
    // turned snapshots like this (rs-iot terminal state: 581MB / 124M points)
    // into single giant L0 segments whose write + verify + catalog rewrite
    // cost fed back into ever longer persist cycles.
    for series_idx in 0..8u64 {
        let labels = vec![Label::new("host", format!("shard-{series_idx}"))];
        let base_ts = (series_idx as i64) * 200_000;
        let rows = (0..156 * 1024)
            .map(|offset| {
                Row::with_labels(
                    "flush_shard_metric",
                    labels.clone(),
                    DataPoint::new(base_ts + offset as i64, offset as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    let outcome = storage.persist_segment_with_outcome().unwrap();
    assert!(outcome.persisted);
    assert_eq!(outcome.points, 8 * 156 * 1024);
    assert!(
        outcome.segments >= 2,
        "a 1.28M-point flush snapshot must not persist as one unbounded L0 segment \
         (segments={})",
        outcome.segments
    );

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let l0 = load_segments_for_level(&lane_path, 0).unwrap();
    assert!(l0.len() >= 2, "on-disk L0 must carry the same shard split");
    for segment in &l0 {
        assert!(
            segment.manifest.point_count <= 1_048_576 + 65_535,
            "single L0 segment carried {} points; flush persist shards must stay bounded",
            segment.manifest.point_count
        );
    }
    let persisted_points: usize = l0.iter().map(|segment| segment.manifest.point_count).sum();
    assert_eq!(persisted_points, 8 * 156 * 1024);

    // Query correctness across shard boundaries after sealed-chunk eviction
    // pushed the reads onto the persisted view.
    let labels = vec![Label::new("host", "shard-3")];
    let points = storage
        .select("flush_shard_metric", &labels, 600_000, 601_000)
        .unwrap();
    assert_eq!(points.len(), 1_000);
    assert_eq!(points[0].timestamp, 600_000);
    assert_eq!(points[999].timestamp, 600_999);

    storage.close().unwrap();
}

#[test]
fn persist_multi_shard_snapshot_marks_full_highwater_durable_and_drains_wal() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Milliseconds, None);
    options.retention_enforced = false;
    let storage = ChunkStorage::new_with_data_path_and_options(
        1024,
        Some(wal),
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        options,
    )
    .unwrap();

    // Chunk-aligned insert per series (156 x 1024 points) so the flush
    // snapshot covers every appended WAL frame once persist runs. Batches
    // stay at one chunk cap each because WAL sample frames cap at u16 points.
    for series_idx in 0..8u64 {
        let labels = vec![Label::new("host", format!("shard-{series_idx}"))];
        let base_ts = (series_idx as i64) * 200_000;
        for chunk_idx in 0..156 {
            let rows = (0..1024)
                .map(|offset| {
                    Row::with_labels(
                        "flush_shard_wal_metric",
                        labels.clone(),
                        DataPoint::new(
                            base_ts + chunk_idx * 1024 + offset,
                            (chunk_idx * 1024 + offset) as f64,
                        ),
                    )
                })
                .collect::<Vec<_>>();
            storage.insert_rows(&rows).unwrap();
        }
    }

    let wal_bytes_before: u64 = wal_segment_ids_and_size(&wal_dir)
        .iter()
        .map(|(_, size)| size)
        .sum();
    assert!(
        wal_bytes_before > 0,
        "fixture must leave WAL bytes to reclaim after persist"
    );

    let appended_before_persist = storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .current_appended_highwater();
    assert!(
        appended_before_persist > WalHighWatermark::default(),
        "fixture must have appended WAL frames before persist"
    );

    let outcome = storage.persist_segment_with_outcome().unwrap();
    assert!(outcome.persisted);
    assert!(
        outcome.segments >= 2,
        "fixture must produce a multi-shard persist (segments={})",
        outcome.segments
    );

    // The durable watermark may only advance once every shard of the snapshot
    // is on disk, and it must then cover the whole appended prefix, so the WAL
    // reset can drain the log instead of leaking an uncovered tail.
    let wal = storage.persisted.wal.as_ref().unwrap();
    assert_eq!(
        wal.current_durable_highwater(),
        appended_before_persist,
        "multi-shard persist must advance the durable watermark to the full \
         snapshot highwater only after every shard landed"
    );
    let wal_bytes_after: u64 = wal_segment_ids_and_size(&wal_dir)
        .iter()
        .map(|(_, size)| size)
        .sum();
    assert_eq!(
        wal_bytes_after, 0,
        "full-coverage persist must drain the WAL (before={wal_bytes_before})"
    );

    let labels = vec![Label::new("host", "shard-7")];
    let points = storage
        .select("flush_shard_wal_metric", &labels, 1_400_000, 1_600_000)
        .unwrap();
    assert_eq!(
        points.len(),
        156 * 1024,
        "sharded persist must not lose data"
    );

    storage.close().unwrap();
}

#[test]
fn persist_small_flush_snapshot_stays_a_single_segment() {
    let temp_dir = TempDir::new().unwrap();
    let storage =
        persistent_numeric_storage(temp_dir.path(), TimestampPrecision::Milliseconds, 1024);

    // 3 series x 2048 points: far below any sane shard budget, so persist
    // must keep writing exactly one L0 segment per lane instead of paying
    // for needless sharding.
    for series_idx in 0..3u64 {
        let labels = vec![Label::new("host", format!("small-{series_idx}"))];
        let rows = (0..2048)
            .map(|offset| {
                Row::with_labels(
                    "flush_small_metric",
                    labels.clone(),
                    DataPoint::new(offset as i64, offset as f64),
                )
            })
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
    }

    let outcome = storage.persist_segment_with_outcome().unwrap();
    assert!(outcome.persisted);
    assert_eq!(
        outcome.segments, 1,
        "small snapshots must not be needlessly sharded"
    );

    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    assert_eq!(load_segments_for_level(&lane_path, 0).unwrap().len(), 1);

    let labels = vec![Label::new("host", "small-1")];
    let points = storage
        .select("flush_small_metric", &labels, 0, 2048)
        .unwrap();
    assert_eq!(points.len(), 2048);

    storage.close().unwrap();
}
