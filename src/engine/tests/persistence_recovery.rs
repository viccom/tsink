use super::*;
use crate::WriteAcknowledgement;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn write_startup_persisted_segment(data_path: &Path, metric: &str) -> PathBuf {
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "startup")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0)],
        )],
    );

    let writer = SegmentWriter::new(&lane_path, 0, 1).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();
    writer.layout().root.clone()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplaySeriesStateSnapshot {
    metric: String,
    labels: Vec<Label>,
    family: Option<crate::engine::series::SeriesValueFamily>,
    active_points: Vec<DataPoint>,
    sealed_chunks: Vec<Vec<DataPoint>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayStorageStateSnapshot {
    materialized_series_ids: Vec<u64>,
    series_by_id: BTreeMap<u64, ReplaySeriesStateSnapshot>,
}

fn replay_storage_state_snapshot(storage: &ChunkStorage) -> ReplayStorageStateSnapshot {
    let series_definitions = {
        let registry = storage.catalog.registry.read();
        registry
            .all_series_ids()
            .into_iter()
            .map(|series_id| {
                (
                    series_id,
                    registry.decode_series_key(series_id).unwrap(),
                    registry.series_value_family(series_id),
                )
            })
            .collect::<Vec<_>>()
    };

    let mut series_by_id = BTreeMap::new();
    for (series_id, series_key, family) in series_definitions {
        let active_points = storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map(|state| {
                state
                    .points_in_partition_order()
                    .map(|point| DataPoint::new(point.ts, point.value.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let sealed_chunks = storage
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .map(|chunks| {
                chunks
                    .values()
                    .map(|chunk| {
                        chunk
                            .decode_points()
                            .unwrap()
                            .into_iter()
                            .map(|point| DataPoint::new(point.ts, point.value))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        series_by_id.insert(
            series_id,
            ReplaySeriesStateSnapshot {
                metric: series_key.metric,
                labels: series_key.labels,
                family,
                active_points,
                sealed_chunks,
            },
        );
    }

    let mut materialized_series_ids = storage.materialized_series_snapshot();
    materialized_series_ids.sort_unstable();

    ReplayStorageStateSnapshot {
        materialized_series_ids,
        series_by_id,
    }
}

fn select_metric_series_sorted(storage: &ChunkStorage, metric: &str) -> Vec<MetricSeries> {
    let mut selected = storage
        .select_series(&SeriesSelection::new().with_metric(metric))
        .unwrap();
    selected.sort();
    selected
}

fn rewrite_wal_to_durable_writes(wal_dir: &Path, durable_highwater: WalHighWatermark) {
    let durable_writes = FramedWal::open(wal_dir, WalSyncMode::PerAppend)
        .unwrap()
        .replay_committed_writes_after_with_mode(
            WalHighWatermark::default(),
            WalReplayMode::Salvage,
        )
        .unwrap()
        .into_iter()
        .take_while(|write| write.highwater <= durable_highwater)
        .collect::<Vec<_>>();

    if wal_dir.exists() {
        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    let rebuilt = FramedWal::open(wal_dir, WalSyncMode::PerAppend).unwrap();
    for write in durable_writes {
        for definition in write.series_definitions {
            rebuilt.append_series_definition(&definition).unwrap();
        }
        if !write.sample_batches.is_empty() {
            rebuilt.append_samples(&write.sample_batches).unwrap();
        }
    }
}

fn new_wal_only_storage(wal_dir: &Path, sync_mode: WalSyncMode) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        2,
        Some(FramedWal::open(wal_dir, sync_mode).unwrap()),
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap()
}

fn flip_first_byte(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    assert!(
        !bytes.is_empty(),
        "expected {} to be non-empty",
        path.display()
    );
    bytes[0] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

fn flip_last_byte(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    let last = bytes
        .len()
        .checked_sub(1)
        .unwrap_or_else(|| panic!("expected {} to be non-empty", path.display()));
    bytes[last] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

fn replace_first_chunk_payload_with_invalid_encoded_payload(segment_root: &Path) {
    const CHUNKS_FILE_HEADER_LEN: usize = 16;
    const CHUNK_RECORD_FIXED_HEADER_LEN: usize = 4 + 4 + 34;
    const CHUNK_FLAGS_OFFSET: usize = CHUNKS_FILE_HEADER_LEN + 4 + 4 + 8 + 1 + 1 + 1;
    const PAYLOAD_LEN_OFFSET: usize =
        CHUNKS_FILE_HEADER_LEN + 4 + 4 + 8 + 1 + 1 + 1 + 1 + 2 + 8 + 8;
    const MANIFEST_FIRST_FILE_ENTRY_HASH_OFFSET: usize = 96 + 12;

    let chunks_path = segment_root.join("chunks.bin");
    let mut chunks_bytes = std::fs::read(&chunks_path).unwrap();
    assert_eq!(
        chunks_bytes[CHUNK_FLAGS_OFFSET], 0,
        "test helper expects the first chunk payload to be stored uncompressed"
    );

    let payload_len = u32::from_le_bytes(
        chunks_bytes[PAYLOAD_LEN_OFFSET..PAYLOAD_LEN_OFFSET + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let payload_offset = CHUNKS_FILE_HEADER_LEN + CHUNK_RECORD_FIXED_HEADER_LEN;
    let payload_end = payload_offset + payload_len;
    assert!(
        payload_len >= 4,
        "encoded chunk payload should be large enough to corrupt"
    );

    let invalid_ts_len = u32::try_from(payload_len).unwrap().saturating_add(1);
    chunks_bytes[payload_offset..payload_offset + 4].copy_from_slice(&invalid_ts_len.to_le_bytes());
    let payload_crc = crate::engine::binio::checksum32(&chunks_bytes[payload_offset..payload_end]);
    chunks_bytes[payload_end..payload_end + 4].copy_from_slice(&payload_crc.to_le_bytes());
    std::fs::write(&chunks_path, &chunks_bytes).unwrap();

    let manifest_path = segment_root.join("manifest.bin");
    let mut manifest_bytes = std::fs::read(&manifest_path).unwrap();
    let chunks_hash = xxhash_rust::xxh64::xxh64(&chunks_bytes, 0);
    manifest_bytes
        [MANIFEST_FIRST_FILE_ENTRY_HASH_OFFSET..MANIFEST_FIRST_FILE_ENTRY_HASH_OFFSET + 8]
        .copy_from_slice(&chunks_hash.to_le_bytes());
    let crc_offset = manifest_bytes.len() - 4;
    let manifest_crc = crate::engine::binio::checksum32(&manifest_bytes[..crc_offset]);
    manifest_bytes[crc_offset..].copy_from_slice(&manifest_crc.to_le_bytes());
    std::fs::write(manifest_path, manifest_bytes).unwrap();
}

fn startup_quarantined_segment_paths(level_root: &Path) -> Vec<PathBuf> {
    let prefix = format!(
        ".tmp-tsink-{}-",
        crate::engine::segment::STARTUP_SEGMENT_QUARANTINE_PURPOSE
    );
    let mut roots = match std::fs::read_dir(level_root) {
        Ok(entries) => entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_type = entry.file_type().ok()?;
                if !file_type.is_dir() {
                    return None;
                }
                let name = entry.file_name();
                let name = name.to_str()?;
                name.starts_with(&prefix).then_some(entry.path())
            })
            .collect::<Vec<_>>(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => panic!("failed to list {level_root:?}: {err}"),
    };
    roots.sort();
    roots
}

fn assert_startup_segment_is_quarantined(
    storage: &Arc<dyn Storage>,
    segment_root: &Path,
    metric: &str,
    expected_detail: &str,
) {
    let labels = vec![Label::new("host", "startup")];
    assert!(
        storage.select(metric, &labels, 0, 100).unwrap().is_empty(),
        "quarantined startup segment should not remain queryable"
    );
    let quarantined = startup_quarantined_segment_paths(
        segment_root
            .parent()
            .unwrap_or_else(|| panic!("missing parent for {}", segment_root.display())),
    );
    assert_eq!(quarantined.len(), 1);
    assert!(
        !segment_root.exists(),
        "corrupt startup segment root should be moved out of the visible inventory"
    );

    let health = storage.observability_snapshot().health;
    assert!(health.degraded);
    assert_eq!(health.maintenance_errors_total, 1);
    let message = health
        .last_maintenance_error
        .unwrap_or_else(|| "missing startup recovery error".to_string());
    assert!(
        message.contains(&segment_root.display().to_string()),
        "expected startup recovery error to include segment path, got: {message}"
    );
    assert!(
        message.contains(&quarantined[0].display().to_string()),
        "expected startup recovery error to include quarantine path, got: {message}"
    );
    assert!(
        message.contains(expected_detail),
        "expected startup recovery error to include detail '{expected_detail}', got: {message}"
    );
}

fn assert_runtime_refresh_failure(err: TsinkError, segment_root: &Path, expected_detail: &str) {
    let segment_display = segment_root.display().to_string();
    match err {
        TsinkError::DataCorruption(message) => {
            assert!(
                message.contains("runtime refresh"),
                "expected runtime refresh failure context, got: {message}"
            );
            assert!(
                message.contains(&segment_display),
                "expected runtime refresh error to include segment path {segment_display}, got: {message}"
            );
            assert!(
                message.contains(expected_detail),
                "expected runtime refresh error to include detail '{expected_detail}', got: {message}"
            );
        }
        other => panic!("expected runtime refresh corruption error, got {other:?}"),
    }
}

fn write_runtime_refresh_segment(
    data_path: &Path,
    metric: &str,
    series_id: u64,
    segment_id: u64,
    points: &[(i64, f64)],
) -> PathBuf {
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "startup")];
    let registry = SeriesRegistry::new();
    registry
        .register_series_with_id(series_id, metric, &labels)
        .unwrap();
    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, points)],
    );
    let writer = SegmentWriter::new(&lane_path, 0, segment_id).unwrap();
    writer.write_segment(&registry, &chunks_by_series).unwrap();
    writer.layout().root.clone()
}

fn assert_explicit_runtime_refresh_rejects_corrupted_added_segment(
    metric: &str,
    corrupt_file: &str,
    corrupt_file_fn: fn(&Path),
    expected_detail: &str,
) {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "startup")];
    let good_root = write_startup_persisted_segment(temp_dir.path(), metric);
    let storage = open_raw_numeric_storage_from_data_path(temp_dir.path());
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;
    let corrupt_root =
        write_runtime_refresh_segment(temp_dir.path(), metric, series_id, 2, &[(3, 3.0)]);
    corrupt_file_fn(&corrupt_root.join(corrupt_file));

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    let err = storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap_err();
    assert_runtime_refresh_failure(err, &corrupt_root, expected_detail);
    assert!(storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    let persisted_roots = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(persisted_roots, vec![good_root]);
}

#[test]
fn recovers_rows_from_wal_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let now = 100i64;

    {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("recover", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("recover", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("recover", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();
    }

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select("recover", &labels, 0, 10).unwrap();
    assert_eq!(points.len(), 3);
    assert_eq!(points[0], DataPoint::new(1, 1.0));
    assert_eq!(points[2], DataPoint::new(3, 3.0));

    reopened.close().unwrap();
}

#[test]
fn per_append_acknowledged_rows_survive_simulated_crash_window() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let storage = new_wal_only_storage(&wal_dir, WalSyncMode::PerAppend);

    let write_result = storage
        .insert_rows_with_result(&[Row::new("per_append_crash_window", DataPoint::new(1, 1.0))])
        .unwrap();
    assert_eq!(write_result.acknowledgement, WriteAcknowledgement::Durable);

    let wal = storage.persisted.wal.as_ref().unwrap();
    let appended_highwater = wal.current_appended_highwater();
    let durable_highwater = wal.current_durable_highwater();
    assert_eq!(durable_highwater, appended_highwater);

    drop(storage);
    rewrite_wal_to_durable_writes(&wal_dir, durable_highwater);

    let reopened = new_wal_only_storage(&wal_dir, WalSyncMode::PerAppend);
    reopened
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Salvage)
        .unwrap();
    assert_eq!(
        reopened
            .select("per_append_crash_window", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
}

#[test]
fn periodic_acknowledged_rows_can_be_lost_in_simulated_crash_window() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let storage = new_wal_only_storage(&wal_dir, WalSyncMode::Periodic(Duration::from_secs(3600)));

    let write_result = storage
        .insert_rows_with_result(&[Row::new("periodic_crash_window", DataPoint::new(1, 1.0))])
        .unwrap();
    assert_eq!(write_result.acknowledgement, WriteAcknowledgement::Appended);

    let wal = storage.persisted.wal.as_ref().unwrap();
    let appended_highwater = wal.current_appended_highwater();
    let durable_highwater = wal.current_durable_highwater();
    assert!(appended_highwater > WalHighWatermark::default());
    assert_eq!(durable_highwater, WalHighWatermark::default());

    drop(storage);
    rewrite_wal_to_durable_writes(&wal_dir, durable_highwater);

    let reopened = new_wal_only_storage(&wal_dir, WalSyncMode::Periodic(Duration::from_secs(3600)));
    reopened
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Salvage)
        .unwrap();
    assert!(
        reopened
            .select("periodic_crash_window", &[], 0, 10)
            .unwrap()
            .is_empty(),
        "periodic mode should expose a crash window until WAL sync or persistence makes the write durable"
    );
}

#[test]
fn batched_wal_replay_matches_direct_ingest_state_for_mixed_series_and_lanes() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let now = 100i64;
    let expected = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            background_threads_enabled: false,
            current_time_override: Some(now),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    let writes = vec![
        vec![
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "a")],
                DataPoint::new(10, 1.0),
            ),
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "b")],
                DataPoint::new(10, 10.0),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "a")],
                DataPoint::new(10, Value::String("cold".to_string())),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "b")],
                DataPoint::new(10, Value::Bytes(vec![1, 2])),
            ),
        ],
        vec![
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "a")],
                DataPoint::new(11, 2.0),
            ),
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "b")],
                DataPoint::new(11, 11.0),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "a")],
                DataPoint::new(11, Value::String("warm".to_string())),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "b")],
                DataPoint::new(11, Value::Bytes(vec![3, 4])),
            ),
        ],
        vec![
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "a")],
                DataPoint::new(12, 3.0),
            ),
            Row::with_labels(
                "replay_batch_cpu",
                vec![Label::new("host", "b")],
                DataPoint::new(12, 12.0),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "a")],
                DataPoint::new(12, Value::String("hot".to_string())),
            ),
            Row::with_labels(
                "replay_batch_log",
                vec![Label::new("host", "b")],
                DataPoint::new(12, Value::Bytes(vec![5, 6])),
            ),
        ],
    ];

    for rows in &writes {
        expected.insert_rows(rows).unwrap();
    }
    let expected_snapshot = replay_storage_state_snapshot(&expected);
    let expected_numeric_series = select_metric_series_sorted(&expected, "replay_batch_cpu");
    let expected_blob_series = select_metric_series_sorted(&expected, "replay_batch_log");

    {
        let wal_storage = ChunkStorage::new_with_data_path_and_options(
            2,
            Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
            None,
            None,
            1,
            ChunkStorageOptions {
                background_threads_enabled: false,
                current_time_override: Some(now),
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();
        for rows in &writes {
            wal_storage.insert_rows(rows).unwrap();
        }
    }

    let replayed = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
        None,
        None,
        1,
        ChunkStorageOptions {
            background_threads_enabled: false,
            current_time_override: Some(now),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    replayed
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Salvage)
        .unwrap();

    assert_eq!(replay_storage_state_snapshot(&replayed), expected_snapshot);
    assert_eq!(
        select_metric_series_sorted(&replayed, "replay_batch_cpu"),
        expected_numeric_series
    );
    assert_eq!(
        select_metric_series_sorted(&replayed, "replay_batch_log"),
        expected_blob_series
    );
}

#[test]
fn wal_replay_policy_strict_vs_salvage_applies_later_valid_frames_after_checksum_corruption() {
    let labels = vec![Label::new("host", "a")];
    let seed_corrupt_wal = |data_path: &Path| {
        let wal_dir = data_path.join(WAL_DIR_NAME);
        let definition = SeriesDefinitionFrame {
            series_id: 1,
            metric: "replay_policy".to_string(),
            labels: labels.clone(),
        };
        let first_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 1,
                value: Value::F64(1.0),
            }],
        )
        .unwrap();
        let corrupt_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 2,
                value: Value::F64(2.0),
            }],
        )
        .unwrap();
        let later_clean_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 3,
                value: Value::F64(3.0),
            }],
        )
        .unwrap();
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&definition).unwrap();
        wal.append_samples(std::slice::from_ref(&first_batch))
            .unwrap();
        wal.append_samples(&[corrupt_batch]).unwrap();
        wal.append_samples(&[later_clean_batch]).unwrap();

        let definition_frame_bytes =
            FramedWal::estimate_series_definition_frame_bytes(&definition).unwrap();
        let sample_frame_bytes =
            FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&first_batch)).unwrap();
        let checksum_offset = definition_frame_bytes + sample_frame_bytes + 20;
        let corrupt_segment_path = wal.path();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&corrupt_segment_path)
            .unwrap();
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        let mut checksum = [0u8; 4];
        file.read_exact(&mut checksum).unwrap();
        checksum[0] ^= 0xff;
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        file.write_all(&checksum).unwrap();
        file.flush().unwrap();
        wal_dir
    };

    let temp_dir = TempDir::new().unwrap();
    let wal_dir = seed_corrupt_wal(temp_dir.path());

    let strict_storage = ChunkStorage::new(
        8,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
    );
    let strict_err = strict_storage
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Strict)
        .unwrap_err();
    assert!(matches!(
        strict_err,
        TsinkError::DataCorruption(message)
            if message.contains("segment 0, frame 3")
                && message.contains("checksum mismatch")
    ));

    let strict_startup_dir = TempDir::new().unwrap();
    seed_corrupt_wal(strict_startup_dir.path());
    let startup_err = match StorageBuilder::new()
        .with_data_path(strict_startup_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .build()
    {
        Ok(_) => panic!("expected default startup replay to fail on WAL corruption"),
        Err(err) => err,
    };
    assert!(matches!(
        startup_err,
        TsinkError::DataCorruption(message)
            if message.contains("segment 0, frame 3")
                && message.contains("checksum mismatch")
    ));

    let replayed_timestamps = FramedWal::open(&wal_dir, WalSyncMode::PerAppend)
        .unwrap()
        .replay_committed_writes_after_with_mode(
            WalHighWatermark::default(),
            WalReplayMode::Salvage,
        )
        .unwrap()
        .into_iter()
        .flat_map(|write| write.sample_batches)
        .map(|batch| batch.decode_points().unwrap()[0].ts)
        .collect::<Vec<_>>();
    assert_eq!(replayed_timestamps, vec![1, 3]);

    let reopened = ChunkStorage::new(
        8,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
    );
    reopened
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Salvage)
        .unwrap();
    let series_id = reopened
        .catalog
        .registry
        .read()
        .resolve_existing("replay_policy", &labels)
        .unwrap()
        .series_id;
    let active = reopened.active_shard(series_id).read();
    let points = active
        .get(&series_id)
        .unwrap()
        .points_in_partition_order()
        .map(|point| DataPoint::new(point.ts, point.value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(points, vec![DataPoint::new(1, 1.0), DataPoint::new(3, 3.0)]);

    let salvage_startup_dir = TempDir::new().unwrap();
    seed_corrupt_wal(salvage_startup_dir.path());
    let salvaged = StorageBuilder::new()
        .with_data_path(salvage_startup_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .with_wal_replay_mode(WalReplayMode::Salvage)
        .build()
        .unwrap();
    assert_eq!(
        salvaged.select("replay_policy", &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(3, 3.0)]
    );
    salvaged
        .insert_rows(&[Row::with_labels(
            "replay_policy",
            labels.clone(),
            DataPoint::new(4, 4.0),
        )])
        .unwrap();
    salvaged.close().unwrap();

    let salvaged_reopened = StorageBuilder::new()
        .with_data_path(salvage_startup_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .with_wal_replay_mode(WalReplayMode::Salvage)
        .build()
        .unwrap();
    assert_eq!(
        salvaged_reopened
            .select("replay_policy", &labels, 0, 10)
            .unwrap(),
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(4, 4.0),
        ]
    );
    salvaged_reopened.close().unwrap();
}

#[test]
fn list_metrics_with_wal_rebuild_uses_configured_salvage_mode_on_corrupt_wal() {
    let labels = vec![Label::new("host", "a")];
    let metric = "replay_policy";
    let seed_corrupt_wal = |data_path: &Path| {
        let wal_dir = data_path.join(WAL_DIR_NAME);
        let definition = SeriesDefinitionFrame {
            series_id: 1,
            metric: metric.to_string(),
            labels: labels.clone(),
        };
        let first_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 1,
                value: Value::F64(1.0),
            }],
        )
        .unwrap();
        let corrupt_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 2,
                value: Value::F64(2.0),
            }],
        )
        .unwrap();
        let later_clean_batch = SamplesBatchFrame::from_points(
            1,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts: 3,
                value: Value::F64(3.0),
            }],
        )
        .unwrap();
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&definition).unwrap();
        wal.append_samples(std::slice::from_ref(&first_batch))
            .unwrap();
        wal.append_samples(&[corrupt_batch]).unwrap();
        wal.append_samples(&[later_clean_batch]).unwrap();

        let definition_frame_bytes =
            FramedWal::estimate_series_definition_frame_bytes(&definition).unwrap();
        let sample_frame_bytes =
            FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&first_batch)).unwrap();
        let checksum_offset = definition_frame_bytes + sample_frame_bytes + 20;
        let corrupt_segment_path = wal.path();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&corrupt_segment_path)
            .unwrap();
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        let mut checksum = [0u8; 4];
        file.read_exact(&mut checksum).unwrap();
        checksum[0] ^= 0xff;
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        file.write_all(&checksum).unwrap();
        file.flush().unwrap();
        wal_dir
    };

    let temp_dir = TempDir::new().unwrap();
    let wal_dir = seed_corrupt_wal(temp_dir.path());

    let strict_storage = ChunkStorage::new(
        8,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
    );
    strict_storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .invalidate_cached_series_definition_snapshot_for_test();
    let strict_err = strict_storage.list_metrics_with_wal().unwrap_err();
    assert!(matches!(
        strict_err,
        TsinkError::DataCorruption(message)
            if message.contains("segment 0, frame 3")
                && message.contains("checksum mismatch")
    ));

    let salvage_storage = ChunkStorage::new(
        8,
        Some(FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap()),
    );
    assert!(salvage_storage.list_metrics().unwrap().is_empty());
    salvage_storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_configured_replay_mode(WalReplayMode::Salvage);
    salvage_storage
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .invalidate_cached_series_definition_snapshot_for_test();
    assert_eq!(
        salvage_storage.list_metrics_with_wal().unwrap(),
        vec![MetricSeries {
            name: metric.to_string(),
            labels: labels.clone(),
        }]
    );

    strict_storage.close().unwrap();
    salvage_storage.close().unwrap();
}

#[test]
fn stale_wal_with_already_persisted_points_does_not_duplicate_query_results() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let now = 100i64;

    let stale_frames = {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();

        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        let frames = wal.replay_frames().unwrap();
        assert!(
            !frames.is_empty(),
            "expected WAL frames before close so we can simulate crash window replay"
        );

        storage.close().unwrap();
        frames
    };

    {
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        for frame in stale_frames {
            match frame {
                ReplayFrame::SeriesDefinition(definition) => {
                    wal.append_series_definition(&definition).unwrap();
                }
                ReplayFrame::Samples(batches) => {
                    wal.append_samples(&batches).unwrap();
                }
            }
        }
    }

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select("dupe_recovery", &labels, 0, 10).unwrap();
    assert_eq!(
        points,
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0)
        ]
    );

    reopened.close().unwrap();
}

#[test]
fn stale_wal_with_already_persisted_nan_points_does_not_duplicate_query_results() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let now = 100i64;

    let stale_frames = {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        storage
            .insert_rows(&[
                Row::with_labels(
                    "dupe_recovery_nan",
                    labels.clone(),
                    DataPoint::new(1, f64::NAN),
                ),
                Row::with_labels("dupe_recovery_nan", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        let frames = wal.replay_frames().unwrap();
        assert!(
            !frames.is_empty(),
            "expected WAL frames before close so we can simulate crash window replay"
        );

        storage.close().unwrap();
        frames
    };

    {
        let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
        for frame in stale_frames {
            match frame {
                ReplayFrame::SeriesDefinition(definition) => {
                    wal.append_series_definition(&definition).unwrap();
                }
                ReplayFrame::Samples(batches) => {
                    wal.append_samples(&batches).unwrap();
                }
            }
        }
    }

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened
        .select("dupe_recovery_nan", &labels, 0, 10)
        .unwrap();
    assert_eq!(points.len(), 2);
    assert_eq!(points[0].timestamp, 1);
    assert!(points[0].value_as_f64().is_some_and(f64::is_nan));
    assert_eq!(points[1], DataPoint::new(2, 2.0));

    reopened.close().unwrap();
}

#[test]
fn startup_replays_only_logically_published_wal_writes() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();

    let encode_write = |series_id: u64, metric: &str, ts: i64, value: f64| {
        let definition = SeriesDefinitionFrame {
            series_id,
            metric: metric.to_string(),
            labels: labels.clone(),
        };
        let batch = SamplesBatchFrame::from_points(
            series_id,
            ValueLane::Numeric,
            &[ChunkPoint {
                ts,
                value: Value::F64(value),
            }],
        )
        .unwrap();
        let estimated_bytes = FramedWal::estimate_series_definition_frame_bytes(&definition)
            .unwrap()
            .saturating_add(
                FramedWal::estimate_samples_frame_bytes(std::slice::from_ref(&batch)).unwrap(),
            );
        (
            estimated_bytes,
            FramedWal::encode_series_definition_frame_payload(&definition).unwrap(),
            FramedWal::encode_samples_frame_payload(std::slice::from_ref(&batch)).unwrap(),
        )
    };

    let (estimated_bytes, definition_payload, samples_payload) =
        encode_write(1, "wal_published_boundary", 1, 1.0);
    let mut published = wal.begin_logical_write(estimated_bytes).unwrap();
    published
        .append_series_definition_payload(&definition_payload)
        .unwrap();
    published.append_samples_payload(&samples_payload).unwrap();
    let published_highwater = published.persist_pending().unwrap();
    assert_eq!(
        published.publish_persisted().unwrap(),
        published_highwater,
        "published logical writes should advance the WAL visibility marker once",
    );

    let (estimated_bytes, definition_payload, samples_payload) =
        encode_write(2, "wal_staged_boundary", 2, 2.0);
    let mut staged = wal.begin_logical_write(estimated_bytes).unwrap();
    staged
        .append_series_definition_payload(&definition_payload)
        .unwrap();
    staged.append_samples_payload(&samples_payload).unwrap();
    let staged_highwater = staged.persist_pending().unwrap();
    assert!(
        staged_highwater > published_highwater,
        "the staged write should occupy a later WAL position than the published write",
    );
    std::mem::forget(staged);
    drop(wal);

    // Invariant: restart recovery only replays writes that crossed the logical WAL
    // publish boundary, even if later writes already persisted their bytes.
    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select("wal_published_boundary", &labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );
    assert!(
        reopened
            .select("wal_staged_boundary", &labels, 0, 10)
            .unwrap()
            .is_empty(),
        "recovery must ignore a write that persisted WAL bytes without publishing them",
    );
    assert!(
        !reopened
            .list_metrics_with_wal()
            .unwrap()
            .into_iter()
            .any(|series| series.name == "wal_staged_boundary" && series.labels == labels),
        "staged WAL metadata must stay hidden across restart until publication",
    );

    let reopened_wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
    assert_eq!(
        reopened_wal.current_published_highwater(),
        published_highwater
    );
    let committed = reopened_wal
        .replay_committed_writes_after(WalHighWatermark::default())
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].highwater, published_highwater);

    reopened.close().unwrap();
}

#[test]
fn flush_pipeline_post_publish_load_failure_replays_wal_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let now = 100i64;

    {
        let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(lane_path.clone()),
            None,
            1,
            ChunkStorageOptions {
                background_threads_enabled: false,
                current_time_override: Some(now),
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "flush_recovery",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        storage.set_persist_post_publish_hook(|roots| {
            let manifest_path = roots[0].join("manifest.bin");
            std::fs::remove_file(manifest_path).unwrap();
        });

        let err = storage.flush_pipeline_once().unwrap_err();
        assert!(matches!(
            err,
            TsinkError::DataCorruption(message) if message.contains("missing manifest.bin")
        ));
        storage.clear_persist_post_publish_hook();
    }

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select("flush_recovery", &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );

    reopened.close().unwrap();
}

#[test]
fn close_post_publish_load_failure_replays_wal_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let now = 100i64;

    {
        let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            Some(wal),
            Some(lane_path),
            None,
            1,
            ChunkStorageOptions {
                background_threads_enabled: false,
                current_time_override: Some(now),
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();

        storage
            .insert_rows(&[Row::with_labels(
                "close_recovery",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        storage.set_persist_post_publish_hook(|roots| {
            let manifest_path = roots[0].join("manifest.bin");
            std::fs::remove_file(manifest_path).unwrap();
        });

        let err = storage.close().unwrap_err();
        assert!(matches!(
            err,
            TsinkError::DataCorruption(message) if message.contains("missing manifest.bin")
        ));
    }

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(8)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select("close_recovery", &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0)]
    );

    reopened.close().unwrap();
}

#[test]
fn startup_quarantines_manifest_corruption_and_serves_healthy_data() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "startup")];
    let healthy_metric = "startup_manifest_healthy";
    write_runtime_refresh_segment(temp_dir.path(), healthy_metric, 1, 1, &[(1, 1.0), (2, 2.0)]);
    let segment_root = write_runtime_refresh_segment(
        temp_dir.path(),
        "startup_manifest_corrupt",
        2,
        2,
        &[(3, 3.0)],
    );
    flip_first_byte(&segment_root.join("manifest.bin"));

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();
    assert_eq!(
        storage.select(healthy_metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_startup_segment_is_quarantined(
        &storage,
        &segment_root,
        "startup_manifest_corrupt",
        "manifest crc32 mismatch",
    );
    storage.close().unwrap();
}

#[test]
fn startup_quarantines_chunk_index_corruption_and_serves_healthy_data() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "startup")];
    let healthy_metric = "startup_index_healthy";
    write_runtime_refresh_segment(temp_dir.path(), healthy_metric, 1, 1, &[(1, 1.0), (2, 2.0)]);
    let segment_root =
        write_runtime_refresh_segment(temp_dir.path(), "startup_index_corrupt", 2, 2, &[(3, 3.0)]);
    flip_first_byte(&segment_root.join("chunk_index.bin"));

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();
    assert_eq!(
        storage.select(healthy_metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_startup_segment_is_quarantined(
        &storage,
        &segment_root,
        "startup_index_corrupt",
        "manifest file hash mismatch for kind 2",
    );
    storage.close().unwrap();
}

#[test]
fn startup_quarantines_chunk_payload_corruption_and_serves_healthy_data() {
    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "startup")];
    let healthy_metric = "startup_chunk_healthy";
    write_runtime_refresh_segment(temp_dir.path(), healthy_metric, 1, 1, &[(1, 1.0), (2, 2.0)]);
    let segment_root =
        write_runtime_refresh_segment(temp_dir.path(), "startup_chunk_corrupt", 2, 2, &[(3, 3.0)]);
    flip_last_byte(&segment_root.join("chunks.bin"));

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();
    assert_eq!(
        storage.select(healthy_metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_startup_segment_is_quarantined(
        &storage,
        &segment_root,
        "startup_chunk_corrupt",
        "manifest file hash mismatch for kind 1",
    );
    storage.close().unwrap();
}

#[test]
fn startup_loads_persisted_metadata_without_decoding_chunk_payloads() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "startup_lazy_metadata";
    let labels = vec![Label::new("host", "startup")];
    let segment_root = write_startup_persisted_segment(temp_dir.path(), metric);
    replace_first_chunk_payload_with_invalid_encoded_payload(&segment_root);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    storage
        .select_series(&SeriesSelection::new().with_metric(metric))
        .unwrap();

    let err = storage
        .insert_rows(&[Row::with_labels(metric, labels, DataPoint::new(3, 3i64))])
        .unwrap_err();
    match err {
        TsinkError::ValueTypeMismatch { .. } => {}
        other => panic!("expected recovered value family to reject i64 writes, got {other:?}"),
    }

    storage.close().unwrap();
}

#[test]
fn startup_primes_list_metrics_with_wal_snapshot_without_replaying_wal_metadata() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let rebuild_called = Arc::new(AtomicBool::new(false));
    let metric = "startup_wal_metric_listing";
    let labels = vec![Label::new("host", "startup")];

    {
        let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
        let storage = ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(numeric_lane_path.clone()),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels(metric, labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();
    }

    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let reopened = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(numeric_lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    reopened
        .apply_loaded_segment_indexes(load_segment_indexes(&numeric_lane_path).unwrap(), false)
        .unwrap();
    reopened
        .replay_from_wal(WalHighWatermark::default(), WalReplayMode::Strict)
        .unwrap();

    reopened
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .set_cached_series_definition_rebuild_hook({
            let rebuild_called = Arc::clone(&rebuild_called);
            move || {
                rebuild_called.store(true, Ordering::SeqCst);
            }
        });

    let metrics = reopened.list_metrics_with_wal().unwrap();
    assert!(metrics
        .iter()
        .any(|series| series.name == metric && series.labels == labels));
    assert!(
        !rebuild_called.load(Ordering::SeqCst),
        "first post-restart list_metrics_with_wal call should use the startup-primed WAL metadata snapshot"
    );

    reopened
        .persisted
        .wal
        .as_ref()
        .unwrap()
        .clear_cached_series_definition_rebuild_hook();
    reopened.close().unwrap();
}

#[test]
fn startup_self_heals_corrupt_registry_checkpoint_after_segment_rebuild() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let metric = "startup_registry_self_heal";
    let labels = vec![Label::new("host", "startup")];
    let expected_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];
    let now = 100i64;

    {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), expected_points[0].clone()),
                Row::with_labels(metric, labels.clone(), expected_points[1].clone()),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    flip_first_byte(&checkpoint_path);
    let corrupt_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();
    assert!(
        SeriesRegistry::load_persisted_state(&checkpoint_path).is_err(),
        "corrupted checkpoint should fail persisted registry loading before restart",
    );

    let rebuilt = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        rebuilt.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    let healed_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();
    assert_ne!(healed_checkpoint_bytes, corrupt_checkpoint_bytes);
    assert!(!delta_path.exists());
    assert!(!delta_dir.exists());
    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("healed checkpoint should reload without segment rebuild");
    assert_eq!(loaded_registry.delta_series_count, 0);
    assert!(loaded_registry
        .registry
        .resolve_existing(metric, &labels)
        .is_some());
    rebuilt.close().unwrap();

    let checkpoint_before_second_restart = std::fs::read(&checkpoint_path).unwrap();
    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    assert_eq!(
        std::fs::read(&checkpoint_path).unwrap(),
        checkpoint_before_second_restart
    );
    assert!(!delta_path.exists());
    assert!(!delta_dir.exists());
    reopened.close().unwrap();
}

#[test]
fn startup_reopens_from_registry_catalog_without_segment_series_files() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
    let metric = "startup_registry_catalog_fast_path";
    let labels = vec![Label::new("host", "startup")];
    let expected_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), expected_points[0].clone()),
                Row::with_labels(metric, labels.clone(), expected_points[1].clone()),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let segment_root = load_segments_for_level(temp_dir.path().join(NUMERIC_LANE_ROOT), 0)
        .unwrap()
        .into_iter()
        .next()
        .expect("expected a persisted segment")
        .root;
    assert!(checkpoint_path.exists());
    assert!(catalog_path.exists());

    std::fs::remove_file(segment_root.join("series.bin")).unwrap();
    std::fs::remove_file(segment_root.join("postings.bin")).unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    assert_eq!(
        reopened
            .select_series(&SeriesSelection::new().with_metric(metric))
            .unwrap()
            .len(),
        1
    );
    let selected = reopened
        .select_series(
            &SeriesSelection::new()
                .with_metric(metric)
                .with_time_range(0, 10),
        )
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].name, metric);
    assert_eq!(selected[0].labels, labels);
    reopened.close().unwrap();
}

#[test]
fn startup_self_heals_conflicting_registry_snapshot_even_when_catalog_matches_segments() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let metric = "startup_registry_catalog_conflict";
    let labels = vec![Label::new("host", "startup")];
    let expected_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];
    let now = 100i64;

    let series_id = {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), expected_points[0].clone()),
                Row::with_labels(metric, labels.clone(), expected_points[1].clone()),
            ])
            .unwrap();
        storage.close().unwrap();
        SeriesRegistry::load_persisted_state(&checkpoint_path)
            .unwrap()
            .expect("initial checkpoint should exist")
            .registry
            .resolve_existing(metric, &labels)
            .unwrap()
            .series_id
    };

    assert!(checkpoint_path.exists());
    assert!(catalog_path.exists());

    let conflicting_registry = SeriesRegistry::new();
    conflicting_registry
        .register_series_with_id(
            series_id,
            "startup_registry_catalog_conflict_wrong",
            &[Label::new("host", "wrong")],
        )
        .unwrap();
    conflicting_registry
        .persist_to_path(&checkpoint_path)
        .unwrap();
    let conflicting_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();

    let rebuilt = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        rebuilt.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        conflicting_checkpoint_bytes
    );
    assert!(!delta_path.exists());
    assert!(!delta_dir.exists());
    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("healed checkpoint should reload after startup rebuild");
    assert!(loaded_registry
        .registry
        .resolve_existing(metric, &labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing(
            "startup_registry_catalog_conflict_wrong",
            &[Label::new("host", "wrong")]
        )
        .is_none());
    rebuilt.close().unwrap();
}

#[test]
fn startup_recovers_visible_series_when_checkpoint_and_catalog_lag_segments() {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
    let base_metric = "startup_registry_lag_base";
    let base_labels = vec![Label::new("host", "base")];
    let lagged_metric = "startup_registry_lag_new";
    let lagged_labels = vec![Label::new("host", "lagged")];
    let now = 100i64;

    {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(base_metric, base_labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels(base_metric, base_labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let stale_checkpoint_bytes = std::fs::read(&checkpoint_path).unwrap();
    let stale_catalog_bytes = std::fs::read(&catalog_path).unwrap();

    {
        let storage = builder_at_time(now)
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(lagged_metric, lagged_labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels(lagged_metric, lagged_labels.clone(), DataPoint::new(4, 4.0)),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        stale_checkpoint_bytes
    );
    assert_ne!(std::fs::read(&catalog_path).unwrap(), stale_catalog_bytes);

    std::fs::write(&checkpoint_path, &stale_checkpoint_bytes).unwrap();
    std::fs::write(&catalog_path, &stale_catalog_bytes).unwrap();

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select(base_metric, &base_labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
    );
    assert_eq!(
        reopened.select(lagged_metric, &lagged_labels, 0, 10).unwrap(),
        vec![DataPoint::new(3, 3.0), DataPoint::new(4, 4.0)],
        "startup should recover series that were visible in segments even when both registry files lag behind",
    );

    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        stale_checkpoint_bytes
    );
    assert_ne!(std::fs::read(&catalog_path).unwrap(), stale_catalog_bytes);
    let healed_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("healed registry checkpoint should reload after startup recovery");
    assert!(healed_registry
        .registry
        .resolve_existing(base_metric, &base_labels)
        .is_some());
    assert!(healed_registry
        .registry
        .resolve_existing(lagged_metric, &lagged_labels)
        .is_some());

    reopened.close().unwrap();
}

#[test]
fn applying_loaded_segment_indexes_reconciles_stale_registry_series_out_of_materialized_state() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "startup_registry_reconcile_metric";
    let labels = vec![Label::new("host", "persisted")];
    let ghost_metric = "startup_registry_reconcile_ghost";
    let ghost_labels = vec![Label::new("host", "ghost")];
    let expected_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), expected_points[0].clone()),
                Row::with_labels(metric, labels.clone(), expected_points[1].clone()),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("expected persisted registry snapshot");
    loaded_registry
        .registry
        .resolve_or_insert(ghost_metric, &ghost_labels)
        .unwrap();
    loaded_registry
        .registry
        .persist_to_path(&checkpoint_path)
        .unwrap();

    let reopened = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());

    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        expected_points
    );
    assert!(reopened
        .select_series(&SeriesSelection::new().with_metric(ghost_metric))
        .unwrap()
        .is_empty());

    let registry = reopened.catalog.registry.read();
    let actual_series_id = registry
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;
    assert!(registry
        .resolve_existing(ghost_metric, &ghost_labels)
        .is_none());
    drop(registry);

    assert_eq!(
        reopened.materialized_series_snapshot(),
        vec![actual_series_id]
    );
    reopened.close().unwrap();
}

#[test]
fn explicit_runtime_refresh_fails_when_visible_manifest_is_corrupted() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let metric = "runtime_refresh_manifest_corrupt";
    let labels = vec![Label::new("host", "startup")];
    let segment_root = write_startup_persisted_segment(temp_dir.path(), metric);
    let storage = open_raw_numeric_storage_from_data_path(temp_dir.path());

    flip_first_byte(&segment_root.join("manifest.bin"));

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    let err = storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap_err();
    assert_runtime_refresh_failure(err, &segment_root, "manifest crc32 mismatch");
    assert!(storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    let persisted_roots = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(persisted_roots, vec![segment_root]);
}

#[test]
fn explicit_runtime_refresh_fails_when_added_chunk_index_is_corrupted() {
    assert_explicit_runtime_refresh_rejects_corrupted_added_segment(
        "runtime_refresh_index_corrupt",
        "chunk_index.bin",
        flip_first_byte,
        "manifest file hash mismatch for kind 2",
    );
}

#[test]
fn explicit_runtime_refresh_fails_when_added_chunk_payload_is_corrupted() {
    assert_explicit_runtime_refresh_rejects_corrupted_added_segment(
        "runtime_refresh_chunk_corrupt",
        "chunks.bin",
        flip_last_byte,
        "manifest file hash mismatch for kind 1",
    );
}

#[test]
fn explicit_runtime_refresh_loads_added_segment_without_decoding_chunk_payloads() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let metric = "runtime_refresh_lazy_metadata";
    let labels = vec![Label::new("host", "startup")];
    let good_root = write_startup_persisted_segment(temp_dir.path(), metric);
    let storage = open_raw_numeric_storage_from_data_path(temp_dir.path());
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(metric, &labels)
        .unwrap()
        .series_id;
    let added_root =
        write_runtime_refresh_segment(temp_dir.path(), metric, series_id, 2, &[(3, 3.0)]);
    replace_first_chunk_payload_with_invalid_encoded_payload(&added_root);

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    assert!(!storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst));
    assert_eq!(
        storage.persisted.timestamp_indexes.len(),
        0,
        "runtime refresh should not eagerly build persisted timestamp search indexes",
    );
    let persisted_roots = storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(persisted_roots, vec![good_root, added_root]);
    let selected = select_metric_series_sorted(&storage, metric);
    assert_eq!(selected.len(), 1);
}

#[test]
fn query_prefers_compacted_generation_when_compaction_crash_leaves_both_generations() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let metric = "compaction_crash_dupe";
    let now = 100i64;

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;

    let mut stale_l0 = HashMap::new();
    stale_l0.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 0, 1)
        .unwrap()
        .write_segment(&registry, &stale_l0)
        .unwrap();

    let mut compacted_l1 = HashMap::new();
    compacted_l1.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 10.0), (2, 2.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 1, 2)
        .unwrap()
        .write_segment(&registry, &compacted_l1)
        .unwrap();

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let points = reopened.select(metric, &labels, 0, 10).unwrap();
    assert_eq!(
        points,
        vec![DataPoint::new(1, 10.0), DataPoint::new(2, 2.0)]
    );

    reopened.close().unwrap();
}

#[test]
fn query_prefers_compacted_generation_when_generations_cover_same_window_with_different_sizes() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let metric = "compaction_crash_point_count_precedence";
    let now = 100i64;

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;

    let mut stale_l0 = HashMap::new();
    stale_l0.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0), (2, 20.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 0, 1)
        .unwrap()
        .write_segment(&registry, &stale_l0)
        .unwrap();

    let mut compacted_l1 = HashMap::new();
    compacted_l1.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 10.0), (2, 2.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 1, 2)
        .unwrap()
        .write_segment(&registry, &compacted_l1)
        .unwrap();

    let reopened = builder_at_time(now)
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(3)
        .build()
        .unwrap();

    let points = reopened.select(metric, &labels, 0, 10).unwrap();
    assert_eq!(
        points,
        vec![DataPoint::new(1, 10.0), DataPoint::new(2, 2.0)]
    );

    reopened.close().unwrap();
}

#[test]
fn startup_replay_reclaims_wal_prefix_covered_by_persisted_segments() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let wal_dir = data_path.join(WAL_DIR_NAME);
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let metric = "wal_replay_reclaim";
    let labels = vec![Label::new("host", "a")];

    // Legacy-engine model (T21b): WAL frames accumulated across several
    // segments while the persisted state only covers a prefix of them.
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    let definition = SeriesDefinitionFrame {
        series_id,
        metric: metric.to_string(),
        labels: labels.clone(),
    };
    let wal = FramedWal::open_with_options(&wal_dir, WalSyncMode::PerAppend, 128, 88).unwrap();
    wal.append_series_definition(&definition).unwrap();
    for frame_idx in 0..4i64 {
        let batch = SamplesBatchFrame::from_points(
            series_id,
            ValueLane::Numeric,
            &[
                ChunkPoint {
                    ts: frame_idx * 2 + 1,
                    value: Value::F64((frame_idx * 2 + 1) as f64),
                },
                ChunkPoint {
                    ts: frame_idx * 2 + 2,
                    value: Value::F64((frame_idx * 2 + 2) as f64),
                },
            ],
        )
        .unwrap();
        wal.append_samples(std::slice::from_ref(&batch)).unwrap();
    }
    let writes = wal.replay_committed_writes().unwrap();
    assert_eq!(writes.len(), 4);
    // The persisted state covers the definition plus the first three sample
    // frames (ts 1..=6); the last frame (ts 7..=8) stays WAL-only.
    let covered_highwater = writes[2].highwater;
    drop(writes);
    drop(wal);

    let mut chunks_by_series = HashMap::new();
    chunks_by_series.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), (5, 5.0), (6, 6.0)],
        )],
    );
    SegmentWriter::new(&lane_path, 0, 1)
        .unwrap()
        .write_segment_with_wal_highwater(&registry, &chunks_by_series, covered_highwater)
        .unwrap();

    // A real flush publishes both lanes together; the replay floor is only
    // non-zero when every enabled lane has persisted segments, so seed the
    // blob lane with a covered segment as well.
    let blob_lane_path = data_path.join(BLOB_LANE_ROOT);
    let blob_labels = vec![Label::new("host", "blob")];
    let blob_series_id = registry
        .resolve_or_insert(metric, &blob_labels)
        .unwrap()
        .series_id;
    let blob_points = vec![ChunkPoint {
        ts: 1,
        value: Value::from("covered".to_string()),
    }];
    let blob_encoded = Encoder::encode_chunk_points(&blob_points, ValueLane::Blob).unwrap();
    let blob_chunk = Chunk {
        header: ChunkHeader {
            series_id: blob_series_id,
            lane: ValueLane::Blob,
            value_family: Some(SeriesValueFamily::Blob),
            point_count: blob_points.len() as u16,
            min_ts: 1,
            max_ts: 1,
            ts_codec: blob_encoded.ts_codec,
            value_codec: blob_encoded.value_codec,
        },
        points: blob_points,
        encoded_payload: blob_encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    };
    let mut blob_chunks_by_series = HashMap::new();
    blob_chunks_by_series.insert(blob_series_id, vec![blob_chunk]);
    SegmentWriter::new(&blob_lane_path, 0, 1)
        .unwrap()
        .write_segment_with_wal_highwater(&registry, &blob_chunks_by_series, covered_highwater)
        .unwrap();

    let before = wal_segment_ids_and_size(&wal_dir);
    assert!(
        before.len() >= 3,
        "fixture must roll multiple WAL segments, got {before:?}"
    );

    // Simulate a kill -9 restart: bootstrap replays the uncovered WAL tail and
    // must also reclaim the prefix that the persisted segments already cover.
    let storage = StorageBuilder::new()
        .with_data_path(data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    let after = wal_segment_ids_and_size(&wal_dir);
    assert!(
        after.first().map(|(id, _)| *id).unwrap_or(0)
            > before.first().map(|(id, _)| *id).unwrap_or(0),
        "startup replay must reclaim WAL prefix segments covered by persisted \
         segments: before={before:?} after={after:?}"
    );

    let points = storage.select(metric, &labels, 0, 100).unwrap();
    assert_eq!(points.len(), 8, "replay reclamation must not lose data");

    storage.close().unwrap();
}

/// T3（P1 安全前提）：catalog 是缓存而非正确性依赖——缺失或损坏时必须靠段目录
/// 扫描恢复（重启后可查），不得阻塞启动。
#[test]
fn startup_recovers_when_catalog_is_missing_or_corrupt() {
    use crate::storage::StorageBuilder;
    use crate::TimestampPrecision;

    for corrupt in [false, true] {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
        let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
        let metric = "catalog_recovery";
        let labels = vec![Label::new("host", "recovery")];
        let expected = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();
            for point in &expected {
                storage
                    .insert_rows(&[Row::with_labels(metric, labels.clone(), point.clone())])
                    .unwrap();
            }
            storage.close().unwrap();
        }

        if corrupt {
            std::fs::write(&catalog_path, b"{not a catalog").unwrap();
        } else {
            std::fs::remove_file(&catalog_path).unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap_or_else(|err| panic!("corrupt={corrupt} 时启动必须靠段扫描恢复：{err}"));
        assert_eq!(
            reopened.select(metric, &labels, 0, 10).unwrap(),
            expected,
            "corrupt={corrupt} 时重启后数据必须完整可查",
        );
        reopened.close().unwrap();
    }
}

// ---- W3-T1（红）：catalog 增量写入与恢复 -------------------------------------
//
// W3-T4 目标布局：catalog checkpoint（`series_index.catalog.json`）旁常驻增量
// 目录 `series_index.catalog.d/`（`{seq:020}.delta.json` + `_checkpoint.json`）。
// 当前实现没有该目录，例行 persist 一律全量重写 checkpoint——以下三测锁定目标
// 行为，红态失败点：delta 目录不存在（`read_dir` NotFound）+ checkpoint 被全量
// 重写（mtime/长度/内容变化）。delta 机制本身由 W3-T4 实现。

/// W3-T4 计划定名的 catalog 增量目录（与 checkpoint 同目录，尚不存在）。
const CATALOG_DELTA_DIR_NAME: &str = "series_index.catalog.d";

/// catalog delta 族测试的轮询等待上界。
///
/// 绿态下谓词命中即返回（等待上界不影响耗时）；上界只在真实卡住时消耗。
/// 原固定 10s 与真实失败耗时同量级（实测绿 ~1.4s、红 ~10.5s），在
/// `--test-threads=8` 下 ext4 journal 多实例 fsync 排队会把正常路径推过 10s
/// 造成假红。默认提到 30s，并允许 `TSINK_CATALOG_DELTA_POLL_TIMEOUT_MS` 覆盖
/// （慢盘/CI 可调大，快速验证失败路径可调小）。
fn catalog_delta_poll_timeout() -> Duration {
    std::env::var("TSINK_CATALOG_DELTA_POLL_TIMEOUT_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(30))
}

use std::time::Instant;

fn catalog_delta_dir(data_path: &Path) -> PathBuf {
    data_path.join(CATALOG_DELTA_DIR_NAME)
}

fn data_catalog_checkpoint_file(data_path: &Path) -> PathBuf {
    super::super::registry_catalog::catalog_path(&data_path.join(SERIES_INDEX_FILE_NAME))
}

/// catalog checkpoint 快照：mtime（symlink_metadata）+ 长度 + 全量内容。
/// 内容全量比对严格强于哈希比对。
#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogCheckpointSnapshot {
    modified: std::time::SystemTime,
    len: u64,
    bytes: Vec<u8>,
}

fn snapshot_catalog_checkpoint(path: &Path) -> Option<CatalogCheckpointSnapshot> {
    let meta = std::fs::symlink_metadata(path).ok()?;
    Some(CatalogCheckpointSnapshot {
        modified: meta.modified().ok()?,
        len: meta.len(),
        bytes: std::fs::read(path).ok()?,
    })
}

/// catalog checkpoint 里 `segments` 数组的长度；解析失败按 0 处理（只让轮询
/// 谓词更保守，不会把"已变化"误判成"未变化"）。
fn catalog_checkpoint_segment_count(bytes: &[u8]) -> usize {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("segments")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
        })
        .unwrap_or(0)
}

fn poll_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// 列出 delta 目录下的 `.delta.json` 文件（文件名排序 = seq 序）。
fn catalog_delta_files(delta_dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(delta_dir)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().ends_with(".delta.json") {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

/// W3-T1 三测共用的构建参数：小 flush/catalog 拍 + 无条件 seal，让例行
/// persist 在亚秒级产生 catalog 变更（compaction 间隔由各测按需覆盖）。
fn catalog_delta_builder(data_path: &Path) -> StorageBuilder {
    StorageBuilder::new()
        .with_data_path(data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(4)
        .with_flush_interval(Duration::from_millis(50))
        .with_catalog_persist_interval(Duration::from_millis(50))
        .with_seal_min_points(1)
}

/// 错开启动锚点（finalize 的节流 persist / 强制 checkpoint）设置的节流时间戳：
/// 发布拍内的 catalog persist 尝试若落在节流窗口内会被 SkippedThrottled，且该
/// 变更要等下一次发布才会重试——写批前先等一拍，保证首个发布拍的 persist
/// 必然通过节流。
fn settle_past_catalog_persist_throttle() {
    std::thread::sleep(Duration::from_millis(150));
}

fn insert_catalog_delta_series(
    storage: &Arc<dyn Storage>,
    metric: &str,
    labels: &[Label],
    ts_base: i64,
    point_count: i64,
) {
    let rows: Vec<Row> = (ts_base..ts_base + point_count)
        .map(|ts| Row::with_labels(metric, labels.to_vec(), DataPoint::new(ts, ts as f64)))
        .collect();
    storage.insert_rows(&rows).unwrap();
}

/// 确定性数据集：值 = ts（全窗逐点比对用）。
fn catalog_delta_expected_points(ts_base: i64, point_count: i64) -> Vec<DataPoint> {
    (ts_base..ts_base + point_count)
        .map(|ts| DataPoint::new(ts, ts as f64))
        .collect()
}

fn assert_catalog_delta_series_window(
    storage: &Arc<dyn Storage>,
    metric: &str,
    labels: &[Label],
    ts_base: i64,
    point_count: i64,
) {
    assert_eq!(
        storage.select(metric, labels, 0, i64::MAX).unwrap(),
        catalog_delta_expected_points(ts_base, point_count),
        "series {metric} must survive reopen with every point intact"
    );
}

fn count_lane_segments_at_level(lane_path: &Path, level: u8) -> usize {
    load_segments_for_level(lane_path, level)
        .map(|segments| segments.len())
        .unwrap_or(0)
}

/// T1-红：例行 persist 产生的 catalog 变更必须以 delta 落盘，checkpoint 文件
/// 保持不动；重开后对全部写入 series 做全窗逐点校验。
#[test]
fn catalog_delta_recovers_without_full_rewrite() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let delta_dir = catalog_delta_dir(data_path);
    let labels = vec![Label::new("host", "delta")];

    // 第一开：基线数据 + close（close 锚点走强制 checkpoint 全量落盘）。
    {
        let storage = catalog_delta_builder(data_path).build().unwrap();
        settle_past_catalog_persist_throttle();
        for idx in 0..30 {
            insert_catalog_delta_series(
                &storage,
                &format!("catalog-delta-{idx:02}"),
                &labels,
                1,
                4,
            );
        }
        storage.close().unwrap();
    }
    let baseline = snapshot_catalog_checkpoint(&catalog_file)
        .expect("close must leave a catalog checkpoint on disk");

    // 第二开：再写一批，例行 persist 应以 delta 记录新段而不是重写 checkpoint。
    {
        let storage = catalog_delta_builder(data_path).build().unwrap();
        settle_past_catalog_persist_throttle();
        insert_catalog_delta_series(&storage, "catalog-delta-30", &labels, 1, 4);
        let produced_change = poll_until(catalog_delta_poll_timeout(), || {
            delta_dir.is_dir()
                || std::fs::read(&catalog_file)
                    .map(|bytes| bytes != baseline.bytes)
                    .unwrap_or(false)
        });
        assert!(
            produced_change,
            "routine persist must record the new segment (delta or checkpoint change) \
             within the poll timeout"
        );

        // 红态断言：两处红都收集到同一次失败输出里。
        let mut failures = Vec::new();
        let current = snapshot_catalog_checkpoint(&catalog_file)
            .expect("catalog checkpoint must still exist while the storage is open");
        if current != baseline {
            failures.push(format!(
                "catalog checkpoint was fully rewritten by routine persist: \
                 mtime {:?} -> {:?}, len {} -> {}",
                baseline.modified, current.modified, baseline.len, current.len
            ));
        }
        match catalog_delta_files(&delta_dir) {
            Err(err) => failures.push(format!(
                "delta dir {} must exist after a routine catalog change: {err}",
                delta_dir.display()
            )),
            Ok(deltas) if deltas.len() != 1 => failures.push(format!(
                "expected exactly 1 delta file under {}, got {}",
                delta_dir.display(),
                deltas.len()
            )),
            Ok(_) => {}
        }
        assert!(
            failures.is_empty(),
            "catalog delta expectations failed:\n- {}",
            failures.join("\n- ")
        );
        storage.close().unwrap();
    }

    // 第三开：全窗逐点校验（delta 应用或 checkpoint 重建后数据都必须一致）。
    {
        let storage = catalog_delta_builder(data_path).build().unwrap();
        for idx in 0..=30 {
            assert_catalog_delta_series_window(
                &storage,
                &format!("catalog-delta-{idx:02}"),
                &labels,
                1,
                4,
            );
        }
        storage.close().unwrap();
    }
}

/// T1-红：两个 delta（先 add 后 remove）按 seq 序重放后，最终集合必须正确：
/// 被合并消失的段数据不重复可见，存活段数据完整。
///
/// remove 制造手法：基线留 3 个 L0 段（低于触发阈值 4，close 的
/// compact_until_settled 不会吸收合并），轮 1 的新段把 L0 推到 4；实例 B 用
/// 1s compaction 拍——delta#1(add) 在发布拍内落盘，合并发生在 ≥1s 后的
/// compaction 拍，两轮变更按时间错峰、序号有序。
#[test]
fn catalog_delta_replay_order_matters() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let delta_dir = catalog_delta_dir(data_path);
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "replay")];

    // 基线：3 个 L0 段（compaction 触发阈值 4 之下），close 后 checkpoint 全量
    // 落盘且不被 close 合并吸收。
    {
        let storage = catalog_delta_builder(data_path).build().unwrap();
        settle_past_catalog_persist_throttle();
        for round in 0..3 {
            let metric = format!("catalog-replay-base-{round}");
            let ts_base = 1 + round * 10;
            insert_catalog_delta_series(&storage, &metric, &labels, ts_base, 4);
            let published = poll_until(catalog_delta_poll_timeout(), || {
                count_lane_segments_at_level(&lane_path, 0) > round as usize
            });
            assert!(
                published,
                "baseline batch {round} must be published as an L0 segment within the poll timeout"
            );
        }
        storage.close().unwrap();
    }
    let baseline = snapshot_catalog_checkpoint(&catalog_file)
        .expect("close must leave a catalog checkpoint on disk");
    let baseline_count = catalog_checkpoint_segment_count(&baseline.bytes);
    assert_eq!(
        baseline_count, 3,
        "baseline checkpoint must carry exactly the 3 seeded L0 roots"
    );

    // 轮 1（add）：新 series → 第 4 个 L0 段发布 → 第 1 个 delta。
    {
        let storage = catalog_delta_builder(data_path)
            .with_compaction_interval(Duration::from_millis(1000))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        insert_catalog_delta_series(&storage, "catalog-replay-add", &labels, 31, 4);
        let round1_landed = poll_until(catalog_delta_poll_timeout(), || {
            delta_dir.is_dir()
                || std::fs::read(&catalog_file)
                    .map(|bytes| catalog_checkpoint_segment_count(&bytes) > baseline_count)
                    .unwrap_or(false)
        });
        assert!(
            round1_landed,
            "round 1 (add) must reach the persisted catalog within the poll timeout"
        );
        // 红态断言（先目录存在 = 红点，再数量）：合并拍在 ≥1s 后，此处读到的
        // delta 目录只含轮 1 的 add。
        match catalog_delta_files(&delta_dir) {
            Err(err) => panic!(
                "round 1 (add) must leave a delta under {}: {err}",
                delta_dir.display()
            ),
            Ok(deltas) => assert_eq!(
                deltas.len(),
                1,
                "round 1 (add) must produce exactly one delta before the compaction pass"
            ),
        }

        // 轮 2（remove）：后台 compaction 把 4 个 L0 段合并进 L1，源段 root
        // 消失 → 第 2 个 delta（无任何新增段，故不产生额外 add delta）。
        let round1_checkpoint = std::fs::read(&catalog_file).unwrap_or_default();
        let round1_count = catalog_checkpoint_segment_count(&round1_checkpoint);
        let merged = poll_until(catalog_delta_poll_timeout(), || {
            count_lane_segments_at_level(&lane_path, 0) < 4
                && count_lane_segments_at_level(&lane_path, 1) >= 1
        });
        assert!(
            merged,
            "background compaction must merge the 4 L0 segments into L1 within the poll timeout"
        );
        let round2_landed = poll_until(catalog_delta_poll_timeout(), || {
            catalog_delta_files(&delta_dir)
                .map(|deltas| deltas.len() >= 2)
                .unwrap_or(false)
                || std::fs::read(&catalog_file)
                    .map(|bytes| catalog_checkpoint_segment_count(&bytes) != round1_count)
                    .unwrap_or(false)
        });
        assert!(
            round2_landed,
            "round 2 (remove) must reach the persisted catalog within the poll timeout"
        );
        match catalog_delta_files(&delta_dir) {
            Err(err) => panic!(
                "round 2 (remove) must leave a second delta under {}: {err}",
                delta_dir.display()
            ),
            Ok(deltas) => assert_eq!(
                deltas.len(),
                2,
                "rounds 1+2 must produce exactly two deltas (add, remove)"
            ),
        }
        storage.close().unwrap();
    }

    // 重开：全窗逐点校验最终集合（合并后的 L1 段承载全部数据，恰出现一次）。
    {
        let storage = catalog_delta_builder(data_path).build().unwrap();
        for round in 0..3 {
            assert_catalog_delta_series_window(
                &storage,
                &format!("catalog-replay-base-{round}"),
                &labels,
                1 + round * 10,
                4,
            );
        }
        assert_catalog_delta_series_window(&storage, "catalog-replay-add", &labels, 31, 4);
        storage.close().unwrap();
    }
}

/// T1-红：checkpoint 后存在 delta 的目录重开时，不得退回全量读 series.bin——
/// 可观测判据 = 启动路径 series.bin 读取计数（cfg(test) 计数器，W3-T4 消费）
/// 不增长。红态失败点 = delta 目录不存在（前置断言）。
#[test]
fn catalog_delta_preserves_startup_fast_path() {
    crate::engine::segment::reset_startup_series_file_read_count();
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let delta_dir = catalog_delta_dir(data_path);
    let labels = vec![Label::new("host", "fastpath")];

    // 计数器自证：空目录首开无段可读，计数为 0。
    {
        let (storage, reads) = build_counting_series_reads(catalog_delta_builder(data_path));
        assert_eq!(reads, 0, "empty data dir must not read any series.bin");
        settle_past_catalog_persist_throttle();
        for idx in 0..6 {
            insert_catalog_delta_series(&storage, &format!("catalog-fast-{idx}"), &labels, 1, 4);
        }
        storage.close().unwrap();
    }

    // 计数器自证：删掉 catalog 强制回退段扫描时，计数必须增长（证明计数器
    // 探的是真实回退路径，而非恒 0 的空仪器）。
    std::fs::remove_file(&catalog_file).unwrap();
    {
        let (storage, reads) = build_counting_series_reads(catalog_delta_builder(data_path));
        assert!(
            reads > 0,
            "missing catalog must force the startup fallback that reads series.bin"
        );
        for idx in 0..6 {
            assert_catalog_delta_series_window(
                &storage,
                &format!("catalog-fast-{idx}"),
                &labels,
                1,
                4,
            );
        }
        storage.close().unwrap();
    }

    // 正常重开：catalog 有效 → 快路径，series.bin 计数不增长。
    let baseline = snapshot_catalog_checkpoint(&catalog_file)
        .expect("closed storage must leave a catalog checkpoint on disk");
    {
        let (storage, reads) = build_counting_series_reads(catalog_delta_builder(data_path));
        assert_eq!(
            reads, 0,
            "a valid checkpoint must keep startup on the catalog fast path"
        );

        // 制造 delta（复用例行 persist 链），然后重开验证快路径保持。
        settle_past_catalog_persist_throttle();
        insert_catalog_delta_series(&storage, "catalog-fast-post", &labels, 1, 4);
        let produced_change = poll_until(catalog_delta_poll_timeout(), || {
            delta_dir.is_dir()
                || std::fs::read(&catalog_file)
                    .map(|bytes| bytes != baseline.bytes)
                    .unwrap_or(false)
        });
        assert!(
            produced_change,
            "routine persist must record the new segment (delta or checkpoint change) \
             within the poll timeout"
        );
        // 红态断言（前置）：delta 必须存在。
        match catalog_delta_files(&delta_dir) {
            Err(err) => panic!(
                "a delta must exist under {} before the fast-path reopen: {err}",
                delta_dir.display()
            ),
            Ok(deltas) => assert!(
                !deltas.is_empty(),
                "delta dir must not be empty before the fast-path reopen"
            ),
        }
        storage.close().unwrap();

        // delta 存在时重开：仍走快路径 + 数据一致。
        let (reopened, reads) = build_counting_series_reads(catalog_delta_builder(data_path));
        assert_eq!(
            reads, 0,
            "startup with a checkpoint plus deltas must not read any series.bin"
        );
        for idx in 0..6 {
            assert_catalog_delta_series_window(
                &reopened,
                &format!("catalog-fast-{idx}"),
                &labels,
                1,
                4,
            );
        }
        assert_catalog_delta_series_window(&reopened, "catalog-fast-post", &labels, 1, 4);
        reopened.close().unwrap();
    }
}

/// 围绕 `build()` 统计本线程启动路径的 series.bin 读取次数。
fn build_counting_series_reads(builder: StorageBuilder) -> (Arc<dyn Storage>, u64) {
    let before = crate::engine::segment::startup_series_file_read_count();
    let storage = builder.build().unwrap();
    let reads = crate::engine::segment::startup_series_file_read_count() - before;
    (storage, reads)
}

// ---- W3-T2（红）：checkpoint 锚点与清理 --------------------------------------
//
// delta 数达到 `catalog_delta_max_count` 阈值（或字节量达到
// `catalog_delta_max_bytes`）必须触发全量 checkpoint 合并；close 的强制
// checkpoint 路径语义不回退。合并机制由 W3-T4 实现——红态失败点 = delta 目录
// 前置断言 / `_checkpoint.json` 不存在。两个旋钮当前为 stub（存参不消费）。

/// W3-T4 计划定名的合并标记文件（位于 delta 目录内，尚不存在）。
const CATALOG_CHECKPOINT_MARKER_NAME: &str = "_checkpoint.json";

/// 从 delta 文件名（`{seq:020}.delta.json`）解析序号。
fn catalog_delta_seq(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .strip_suffix(".delta.json")?
        .parse()
        .ok()
}

/// 读取 `_checkpoint.json` 里的 `checkpoint_seq`。
fn catalog_checkpoint_marker_seq(marker: &Path) -> Option<u64> {
    let bytes = std::fs::read(marker).ok()?;
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()?
        .get("checkpoint_seq")
        .and_then(serde_json::Value::as_u64)
}

/// 数值 lane 各层（L0-L2）在盘段数，与 catalog 条目数对账用；缺层按 0。
fn count_lane_segments_across_levels(lane_path: &Path) -> usize {
    (0..=2)
        .map(|level| count_lane_segments_at_level(lane_path, level))
        .sum()
}

/// T2-红：delta 数达到 `catalog_delta_max_count` 阈值必须触发全量 checkpoint
/// 合并——`_checkpoint.json` 的 `checkpoint_seq` 等于最后一个 delta 的序号，
/// 已合并 delta 被删除，checkpoint 与在盘段数对账；重开后全窗可查；后续变更
/// 的 delta 序号从 `checkpoint_seq + 1` 重新起算，目录不随写入无限增长。
#[test]
fn catalog_checkpoint_consolidates_deltas() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let delta_dir = catalog_delta_dir(data_path);
    let marker = delta_dir.join(CATALOG_CHECKPOINT_MARKER_NAME);
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "consolidate")];

    // 第一开：1 个基线 series，close 走强制全量 checkpoint。
    {
        let storage = catalog_delta_builder(data_path)
            .with_catalog_delta_max_count(3)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        insert_catalog_delta_series(&storage, "catalog-ckpt-base", &labels, 1, 4);
        storage.close().unwrap();
    }

    // 第二开：逐轮制造 delta（每轮一个新 series，各占一个发布拍）。阈值 3：
    // 前两轮低于阈值，可安全观察 delta 序号；第 3 轮达阈值触发合并。compaction
    // 拍压高，排除后台合并插入额外 delta 污染序号对账。
    {
        let storage = catalog_delta_builder(data_path)
            .with_catalog_delta_max_count(3)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        let before = snapshot_catalog_checkpoint(&catalog_file)
            .expect("reopen must leave the baseline catalog checkpoint readable");

        // 前两轮低于阈值：各产一个 delta 并落盘，文件名即序号。
        for round in 0..2usize {
            insert_catalog_delta_series(
                &storage,
                &format!("catalog-ckpt-{round}"),
                &labels,
                1 + round as i64 * 10,
                4,
            );
            let landed = poll_until(catalog_delta_poll_timeout(), || {
                catalog_delta_files(&delta_dir)
                    .map(|deltas| deltas.len() > round)
                    .unwrap_or(false)
            });
            assert!(
                landed,
                "round {round} must land as delta #{} under {} within the poll timeout",
                round + 1,
                delta_dir.display()
            );
        }
        let last_observed_seq = catalog_delta_files(&delta_dir)
            .unwrap_or_default()
            .iter()
            .filter_map(|path| catalog_delta_seq(path))
            .max()
            .expect("rounds below the threshold must expose delta seqs");

        // 第 3 轮达到阈值 → 全量 checkpoint 合并。
        insert_catalog_delta_series(&storage, "catalog-ckpt-2", &labels, 21, 4);
        let consolidated = poll_until(catalog_delta_poll_timeout(), || marker.exists());
        assert!(
            consolidated,
            "reaching catalog_delta_max_count must write {} within the poll timeout",
            marker.display()
        );
        let checkpoint_seq = catalog_checkpoint_marker_seq(&marker)
            .expect("_checkpoint.json must carry a numeric checkpoint_seq");
        assert_eq!(
            checkpoint_seq,
            last_observed_seq + 1,
            "checkpoint_seq must equal the seq of the last delta it consolidates"
        );
        assert!(
            catalog_delta_files(&delta_dir)
                .unwrap_or_default()
                .iter()
                .all(|path| catalog_delta_seq(path) > Some(checkpoint_seq)),
            "deltas with seq <= checkpoint_seq must be deleted after consolidation"
        );
        let consolidated_catalog = std::fs::read(&catalog_file)
            .expect("consolidation must leave the catalog main file readable");
        assert_ne!(
            consolidated_catalog, before.bytes,
            "consolidation must rewrite the catalog main file"
        );
        assert_eq!(
            catalog_checkpoint_segment_count(&consolidated_catalog),
            count_lane_segments_across_levels(&lane_path),
            "consolidated checkpoint must carry an entry for every on-disk segment"
        );
        storage.close().unwrap();
    }

    // 第三开：合并后的 checkpoint 承载全部数据；再产两个变更，delta 序号从
    // checkpoint_seq + 1 重新起算，目录不累计合并前的历史。
    {
        let storage = catalog_delta_builder(data_path)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        assert_catalog_delta_series_window(&storage, "catalog-ckpt-base", &labels, 1, 4);
        for round in 0..3 {
            assert_catalog_delta_series_window(
                &storage,
                &format!("catalog-ckpt-{round}"),
                &labels,
                1 + round * 10,
                4,
            );
        }
        let anchor_seq = catalog_checkpoint_marker_seq(&marker)
            .expect("reopen after consolidation must keep the checkpoint_seq anchor");
        settle_past_catalog_persist_throttle();
        for extra in 0..2usize {
            insert_catalog_delta_series(
                &storage,
                &format!("catalog-ckpt-extra-{extra}"),
                &labels,
                100 + extra as i64 * 10,
                4,
            );
            let landed = poll_until(catalog_delta_poll_timeout(), || {
                catalog_delta_files(&delta_dir)
                    .map(|deltas| deltas.len() > extra)
                    .unwrap_or(false)
            });
            assert!(
                landed,
                "post-consolidation change {extra} must land as a fresh delta within the poll timeout"
            );
        }
        let deltas = catalog_delta_files(&delta_dir).unwrap_or_default();
        assert_eq!(
            deltas.len(),
            2,
            "delta dir must restart after consolidation, not accumulate the \
             pre-consolidation history"
        );
        for (offset, path) in deltas.iter().enumerate() {
            assert_eq!(
                catalog_delta_seq(path),
                Some(anchor_seq + 1 + offset as u64),
                "post-consolidation delta seqs must continue from checkpoint_seq + 1"
            );
        }
        storage.close().unwrap();
    }

    // 第四开：合并后新产 delta 的数据也必须完整可查。
    {
        let storage = catalog_delta_builder(data_path).build().unwrap();
        for extra in 0..2 {
            assert_catalog_delta_series_window(
                &storage,
                &format!("catalog-ckpt-extra-{extra}"),
                &labels,
                100 + extra * 10,
                4,
            );
        }
        storage.close().unwrap();
    }

    // 字节阈值支路：`catalog_delta_max_bytes` 取极小值，首个 delta 即达字节
    // 阈值 → 同一条合并路径（marker + 全量主文件 + delta 清理）必须生效。
    {
        let bytes_dir = TempDir::new().unwrap();
        let bytes_path = bytes_dir.path();
        let bytes_catalog = data_catalog_checkpoint_file(bytes_path);
        let bytes_delta_dir = catalog_delta_dir(bytes_path);
        let bytes_marker = bytes_delta_dir.join(CATALOG_CHECKPOINT_MARKER_NAME);

        let storage = catalog_delta_builder(bytes_path)
            .with_catalog_delta_max_bytes(1)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        insert_catalog_delta_series(&storage, "catalog-ckpt-bytes", &labels, 1, 4);
        let consolidated = poll_until(catalog_delta_poll_timeout(), || bytes_marker.exists());
        assert!(
            consolidated,
            "reaching catalog_delta_max_bytes must write {} within the poll timeout",
            bytes_marker.display()
        );
        assert!(
            catalog_delta_files(&bytes_delta_dir)
                .unwrap_or_default()
                .is_empty(),
            "the byte-threshold delta must be consolidated away"
        );
        let bytes_catalog_file = std::fs::read(&bytes_catalog)
            .expect("byte-threshold consolidation must leave the catalog readable");
        assert_eq!(
            catalog_checkpoint_segment_count(&bytes_catalog_file),
            1,
            "byte-threshold consolidation must carry the on-disk segment"
        );
        assert_catalog_delta_series_window(&storage, "catalog-ckpt-bytes", &labels, 1, 4);
        storage.close().unwrap();

        let reopened = catalog_delta_builder(bytes_path).build().unwrap();
        assert_catalog_delta_series_window(&reopened, "catalog-ckpt-bytes", &labels, 1, 4);
        reopened.close().unwrap();
    }
}

/// T2-红：close 的强制 checkpoint 路径语义不回退——delta 存在时 close 必须
/// 全量重写 catalog 主文件、写 `_checkpoint.json`（checkpoint_seq = 最后一个
/// delta 的序号）并清掉全部已合并 delta；重开后全窗一致。
#[test]
fn catalog_forced_checkpoint_writes_full_file() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let delta_dir = catalog_delta_dir(data_path);
    let marker = delta_dir.join(CATALOG_CHECKPOINT_MARKER_NAME);
    let labels = vec![Label::new("host", "forced")];

    // 第一开：1 个基线 series + close（强制全量）。基线 1 段 + 第二开 2 段共
    // 3 个 L0，低于 close 合并阈值 4——排除 close 期 compaction 插入额外
    // remove delta 干扰 checkpoint_seq 对账。
    {
        let storage = catalog_delta_builder(data_path)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        insert_catalog_delta_series(&storage, "catalog-forced-base", &labels, 1, 4);
        storage.close().unwrap();
    }
    let before = snapshot_catalog_checkpoint(&catalog_file)
        .expect("close must leave a catalog checkpoint on disk");

    // 第二开：产 2 个 delta（默认阈值之下不触发自动合并），随后 close 走强制
    // checkpoint 路径。
    let last_delta_seq: Option<u64> = {
        let storage = catalog_delta_builder(data_path)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        for round in 0..2usize {
            insert_catalog_delta_series(
                &storage,
                &format!("catalog-forced-delta-{round}"),
                &labels,
                10 + round as i64 * 10,
                4,
            );
            let landed = poll_until(catalog_delta_poll_timeout(), || {
                catalog_delta_files(&delta_dir)
                    .map(|deltas| deltas.len() > round)
                    .unwrap_or(false)
            });
            assert!(
                landed,
                "round {round} must land as delta #{} before the forced close",
                round + 1
            );
        }
        let deltas = catalog_delta_files(&delta_dir).unwrap_or_default();
        assert!(
            !deltas.is_empty(),
            "precondition: deltas must exist under {} before the forced close",
            delta_dir.display()
        );
        let last = deltas
            .iter()
            .filter_map(|path| catalog_delta_seq(path))
            .max();
        storage.close().unwrap();
        last
    };
    let last_delta_seq = last_delta_seq.expect("deltas were observed before the forced close");

    // close 后：主文件全量重写、marker 记最后 delta 序号、delta 清理。
    {
        let mut failures = Vec::new();
        let after = snapshot_catalog_checkpoint(&catalog_file)
            .expect("closed storage must leave the catalog main file in place");
        if after == before {
            failures.push(
                "close must fully rewrite the catalog main file (mtime/len/bytes all unchanged)"
                    .to_string(),
            );
        }
        match catalog_checkpoint_marker_seq(&marker) {
            None => failures.push(format!(
                "close must write {} carrying a numeric checkpoint_seq",
                marker.display()
            )),
            Some(seq) if seq != last_delta_seq => failures.push(format!(
                "checkpoint_seq {seq} must equal the last delta seq {last_delta_seq}"
            )),
            Some(_) => {}
        }
        match catalog_delta_files(&delta_dir) {
            Err(err) => failures.push(format!(
                "delta dir {} must survive close: {err}",
                delta_dir.display()
            )),
            Ok(deltas) if !deltas.is_empty() => failures.push(format!(
                "close must delete the {} consolidated delta(s)",
                deltas.len()
            )),
            Ok(_) => {}
        }
        assert!(
            failures.is_empty(),
            "forced checkpoint after deltas failed:\n- {}",
            failures.join("\n- ")
        );
    }

    // 第三开：强制 checkpoint 承载全部数据（全窗逐点校验）。
    {
        let storage = catalog_delta_builder(data_path).build().unwrap();
        assert_catalog_delta_series_window(&storage, "catalog-forced-base", &labels, 1, 4);
        for round in 0..2 {
            assert_catalog_delta_series_window(
                &storage,
                &format!("catalog-forced-delta-{round}"),
                &labels,
                10 + round * 10,
                4,
            );
        }
        storage.close().unwrap();
    }
}

/// R-2 红：阈值合并支路的写失败必须降级为「标脏重试」，不得使 flush 失败。
///
/// 场景：`catalog_delta_max_count = 2` ⇒ 第 2 笔变更走合并支路（全量 checkpoint
/// 重写主文件）。此时把主文件路径替换成**目录**——rename(tmp, main) 必然失败，
/// 且该手段对 root 同样有效（`chmod 0444` 在 root 下无效，仓库已有该坑）。
/// 红态：合并支路的 `?` 把错误上抛 → 整条 flush 失败（`persist_segment_with_outcome`
/// 返回 Err + 回滚已发布段根）。绿态：返回 Ok、outcome 降级、变更留在
/// `pending_changes`，下一拍成功落盘。
#[test]
fn catalog_merge_write_failure_marks_dirty_instead_of_failing_flush() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let delta_dir = catalog_delta_dir(data_path);
    let labels = vec![Label::new("host", "merge-degrade")];

    // 第一开：基线 + 首笔变更（delta #1 落盘，阈值 2 未达）+ close 建立 checkpoint。
    {
        let storage = catalog_delta_builder(data_path)
            .with_catalog_delta_max_count(2)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        insert_catalog_delta_series(&storage, "catalog-merge-base", &labels, 1, 4);
        let landed = poll_until(catalog_delta_poll_timeout(), || {
            catalog_delta_files(&delta_dir)
                .map(|deltas| !deltas.is_empty())
                .unwrap_or(false)
        });
        assert!(
            landed,
            "precondition: the first change must land as delta #1 under {}",
            delta_dir.display()
        );
        storage.close().unwrap();
    }

    // 第二开：主文件路径被目录占据 ⇒ 任何 checkpoint 重写（含合并支路）必然失败。
    // 用一个无后台线程、可由测试驱动 persist 的实例，逐笔精确控制阈值。
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.flush_interval = Duration::from_secs(3600);
    options.compaction_interval = Duration::from_secs(3600);
    options.catalog_persist_interval = Duration::from_millis(50);
    options.catalog_delta_max_count = 2;
    options.seal_policy = super::super::config::SealPolicy::immediate();
    let storage = ChunkStorage::new_with_data_path_and_options(
        4,
        None,
        Some(lane_path.clone()),
        None,
        1,
        options,
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();
    settle_past_catalog_persist_throttle();

    // 首笔变更：delta #1（阈值 2 未达，走 delta 追加，主文件不被触碰）。
    let insert_merge_rows = |metric: &str, ts_base: i64| {
        let rows: Vec<Row> = (ts_base..ts_base + 4)
            .map(|ts| Row::with_labels(metric, labels.clone(), DataPoint::new(ts, ts as f64)))
            .collect();
        storage.insert_rows(&rows).unwrap();
    };
    insert_merge_rows("catalog-merge-a", 11);
    storage.flush_all_active().unwrap();
    let first = storage.persist_segment_with_outcome().unwrap();
    assert!(
        first.persisted,
        "precondition: the first publish tick must persist a segment"
    );
    let landed = poll_until(catalog_delta_poll_timeout(), || {
        catalog_delta_files(&delta_dir)
            .map(|deltas| !deltas.is_empty())
            .unwrap_or(false)
    });
    assert!(
        landed,
        "precondition: the first publish tick must append delta #1 under {}",
        delta_dir.display()
    );

    // 把主文件路径变成目录：checkpoint 重写的 rename 必然失败（root 也有效）。
    let blocked_main = catalog_file.clone();
    std::fs::remove_file(&blocked_main).expect("baseline main file must exist before blocking");
    std::fs::create_dir(&blocked_main).expect("a directory in place of the main file must build");
    assert!(
        blocked_main.is_dir(),
        "blocking precondition: the main catalog path must be a directory"
    );

    // 第二笔变更：阈值 2 达到 ⇒ 合并支路 → 主文件重写失败。
    insert_merge_rows("catalog-merge-b", 21);
    storage.flush_all_active().unwrap();
    let second = storage.persist_segment_with_outcome();

    // 清理阻塞，避免影响后续断言/重开。
    let merge_result = match second {
        Ok(outcome) => {
            std::fs::remove_dir(&blocked_main).ok();
            Ok(outcome)
        }
        Err(err) => {
            std::fs::remove_dir(&blocked_main).ok();
            Err(err)
        }
    };
    let outcome = merge_result.unwrap_or_else(|err| {
        panic!(
            "R-2: a failed threshold consolidation must degrade to mark-dirty, not fail the \
             flush; the publish tick returned {err:?}"
        )
    });
    assert!(
        outcome.persisted,
        "the segment itself must still publish; only the catalog write degrades"
    );

    // 降级自证：变更没有丢——下一拍（主文件已恢复可写）必须把积压变更一起落盘。
    // 合并支路成功 ⇒ 主文件承载全部段；delta 追加成功 ⇒ delta 数增长到 2。
    settle_past_catalog_persist_throttle();
    insert_merge_rows("catalog-merge-c", 31);
    storage.flush_all_active().unwrap();
    let third = storage
        .persist_segment_with_outcome()
        .expect("the retry publish tick must succeed once the main file is writable again");
    assert!(
        third.persisted,
        "the retry publish tick must publish its segment"
    );
    let recovered = poll_until(catalog_delta_poll_timeout(), || {
        let main_ok = std::fs::read(&blocked_main)
            .ok()
            .map(|bytes| catalog_checkpoint_segment_count(&bytes) > 0)
            .unwrap_or(false);
        let deltas = catalog_delta_files(&delta_dir)
            .map(|d| d.len())
            .unwrap_or(0);
        main_ok || deltas >= 2
    });
    assert!(
        recovered,
        "the dirty-marked changes must be retried by the next publish beat: neither a \
         consolidated main file nor two deltas appeared under {}",
        data_path.display()
    );

    // 数据面自证：重启后积压变更的数据都可查（catalog 只是缓存，段数据不丢）。
    storage.close().ok();
    let reopened = catalog_delta_builder(data_path).build().unwrap();
    assert_catalog_delta_series_window(&reopened, "catalog-merge-a", &labels, 11, 4);
    assert_catalog_delta_series_window(&reopened, "catalog-merge-b", &labels, 21, 4);
    assert_catalog_delta_series_window(&reopened, "catalog-merge-c", &labels, 31, 4);
    reopened.close().unwrap();
}

/// R-2 反向锚点：降级**只**适用于例行发布拍的合并支路；close 的强制 checkpoint
/// 仍必须真实失败。关停时无法持久化 catalog 是运维必须知道的错误，不能静默吞掉
/// （否则「关停成功」的假象会掩盖盘面不可写）。
#[test]
fn catalog_close_forced_checkpoint_still_fails_when_unwritable() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let labels = vec![Label::new("host", "close-anchor")];

    {
        let storage = catalog_delta_builder(data_path)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        insert_catalog_delta_series(&storage, "catalog-close-anchor", &labels, 1, 4);
        storage.close().unwrap();
    }
    assert!(
        catalog_file.is_file(),
        "precondition: the baseline close must leave a catalog main file"
    );

    // 先正常重开（启动校验需要读主文件），再把主文件路径换成目录 ⇒ close 的
    // 强制 checkpoint 重写必然失败。
    let storage = catalog_delta_builder(data_path)
        .with_compaction_interval(Duration::from_secs(60))
        .build()
        .expect("reopen must succeed while the main file is still readable");
    std::fs::remove_file(&catalog_file).expect("baseline main file must exist");
    std::fs::create_dir(&catalog_file).expect("a directory in place of the main file must build");

    let close_result = storage.close();
    std::fs::remove_dir(&catalog_file).ok();

    let err = close_result.expect_err(
        "close must surface the forced-checkpoint write failure; only routine publish ticks degrade",
    );
    assert!(
        matches!(err, TsinkError::IoWithPath { .. }),
        "close must report the underlying I/O failure, got {err:?}"
    );
}

// ---- W3-T3（红）：崩溃窗口降级 ------------------------------------------------
//
// 崩溃窗盘面不由 close 产生（close 必然强制合并并清 delta，语义见 W3-T2），而是
// 「实时制造 + 就地捕获 + 回卷重放」：第二开逐轮产 delta 并捕获字节，close 之后
// 把 catalog 主文件回卷到基线内容、delta 目录按捕获字节复原、合并 marker 移除
// ——等效于崩溃发生在任何合并/强制 checkpoint 之前。三个子场景只差对盘面的破坏
// 方式：
//   1. 删掉 seq 最大的 delta（「段已 rename、delta 未写」）；
//   2. 最新 delta 内容截断一半（半写）；
//   3. marker 回拨旧序号 + 已合并 delta 复原（「checkpoint 已写、marker 未更新」，
//      重开时重复应用）。
// 红态失败点 = delta 从不出现（前置断言，同 W3-T1/T2）。回退语义的绿态判据：
// 重开成功 + 全窗逐点一致 + catalog 主文件强制重写为与在盘段对账的正确条目 +
// 残留 delta 清理；回退计数/告警等可观测项由 W3-T4 接线
// （`catalog_delta_apply_fallback_total`）。

/// 制造崩溃窗前态：基线 close（checkpoint 只含基线段）→ 第二开逐轮产 delta 并
/// 就地捕获字节（delta 落盘即不可变，文件名即 seq）→ close（其强制合并产物随后
/// 由调用方回卷覆盖）。返回（基线 checkpoint 快照, 捕获的 delta（文件名, 字节））。
fn catalog_delta_crash_window_fixture(
    data_path: &Path,
    labels: &[Label],
    base_metric: &str,
    delta_metrics: &[&str],
) -> (CatalogCheckpointSnapshot, Vec<(String, Vec<u8>)>) {
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let delta_dir = catalog_delta_dir(data_path);

    {
        let storage = catalog_delta_builder(data_path)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        insert_catalog_delta_series(&storage, base_metric, labels, 1, 4);
        storage.close().unwrap();
    }
    let baseline = snapshot_catalog_checkpoint(&catalog_file)
        .expect("close must leave a catalog checkpoint on disk");

    let mut captured: Vec<(String, Vec<u8>)> = Vec::new();
    {
        let storage = catalog_delta_builder(data_path)
            .with_compaction_interval(Duration::from_secs(60))
            .build()
            .unwrap();
        settle_past_catalog_persist_throttle();
        for (round, metric) in delta_metrics.iter().enumerate() {
            insert_catalog_delta_series(&storage, metric, labels, 11 + round as i64 * 10, 4);
            let landed = poll_until(catalog_delta_poll_timeout(), || {
                catalog_delta_files(&delta_dir)
                    .map(|deltas| deltas.len() > round)
                    .unwrap_or(false)
            });
            assert!(
                landed,
                "delta round {round} ({metric}) must land under {} within the poll timeout",
                delta_dir.display()
            );
            let newest = catalog_delta_files(&delta_dir)
                .unwrap_or_default()
                .pop()
                .expect("the delta polled as landed must still be listed");
            captured.push((
                newest.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(&newest)
                    .expect("a landed delta must stay readable while the storage is open"),
            ));
        }
        storage.close().unwrap();
    }
    (baseline, captured)
}

/// 把盘面回卷成「任何合并/强制 checkpoint 之前」的崩溃窗：catalog 主文件回到
/// 基线内容（不含 delta 段）、delta 目录按捕获字节复原、合并 marker 移除。
fn rewind_catalog_to_crash_window(
    data_path: &Path,
    baseline: &CatalogCheckpointSnapshot,
    captured: &[(String, Vec<u8>)],
) {
    std::fs::write(data_catalog_checkpoint_file(data_path), &baseline.bytes).unwrap();
    let delta_dir = catalog_delta_dir(data_path);
    let _ = std::fs::remove_dir_all(&delta_dir);
    std::fs::create_dir_all(&delta_dir).unwrap();
    for (name, bytes) in captured {
        std::fs::write(delta_dir.join(name), bytes).unwrap();
    }
}

/// 回退重建后的 catalog 必须是「正确条目」：离开回卷前的陈旧内容，且条目数与
/// 在盘段数（数值 lane 各层）对账。
fn assert_catalog_rebuilt_to_on_disk_segments(
    data_path: &Path,
    stale: &CatalogCheckpointSnapshot,
    failures: &mut Vec<String>,
) {
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let bytes = match std::fs::read(&catalog_file) {
        Ok(bytes) => bytes,
        Err(err) => {
            failures.push(format!(
                "catalog checkpoint {} must stay readable after the fallback rebuild: {err}",
                catalog_file.display()
            ));
            return;
        }
    };
    if bytes == stale.bytes {
        failures.push(format!(
            "fallback must force-rewrite the catalog checkpoint {} away from the stale content",
            catalog_file.display()
        ));
    }
    let on_disk = count_lane_segments_across_levels(&data_path.join(NUMERIC_LANE_ROOT));
    let carried = catalog_checkpoint_segment_count(&bytes);
    if carried != on_disk {
        failures.push(format!(
            "rebuilt catalog must carry an entry for every on-disk segment: \
             {carried} entries vs {on_disk} segments"
        ));
    }
}

/// T3-红：最新 delta 缺失（「段已 rename、delta 未写」崩溃窗）→ 重开必须成功并
/// 回退重建：残留旧 delta 一并清理，全窗数据逐点一致。
#[test]
fn catalog_delta_missing_falls_back_to_rebuild() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let delta_dir = catalog_delta_dir(data_path);
    let labels = vec![Label::new("host", "fallback")];

    let (baseline, captured) = catalog_delta_crash_window_fixture(
        data_path,
        &labels,
        "catalog-fallback-base",
        &["catalog-fallback-b", "catalog-fallback-c"],
    );
    rewind_catalog_to_crash_window(data_path, &baseline, &captured);

    // 崩溃窗：段已 rename、delta 未写——只删 seq 最大的那个 delta，旧 delta 留存。
    let newest_name = captured
        .last()
        .expect("fixture must capture two deltas")
        .0
        .clone();
    std::fs::remove_file(delta_dir.join(&newest_name)).expect("the newest delta must be removable");
    let remaining = catalog_delta_files(&delta_dir)
        .expect("the older delta must stay in place inside the crash window");
    assert_eq!(
        remaining.len(),
        1,
        "crash window shape: exactly the older delta must remain under {}",
        delta_dir.display()
    );

    // 重开：delta 断层不得阻塞启动；回退重建后全窗逐点一致。
    let storage = catalog_delta_builder(data_path)
        .with_compaction_interval(Duration::from_secs(60))
        .build()
        .expect("reopen must succeed when the newest delta is missing");
    assert_catalog_delta_series_window(&storage, "catalog-fallback-base", &labels, 1, 4);
    assert_catalog_delta_series_window(&storage, "catalog-fallback-b", &labels, 11, 4);
    assert_catalog_delta_series_window(&storage, "catalog-fallback-c", &labels, 21, 4);

    // 回退语义（绿态判据，W3-T4 实现）：catalog 强制重写 + 残留 delta 清理。
    let rebuilt_in_time = poll_until(catalog_delta_poll_timeout(), || {
        std::fs::read(data_catalog_checkpoint_file(data_path))
            .map(|bytes| bytes != baseline.bytes)
            .unwrap_or(false)
    });
    let cleaned_in_time = poll_until(catalog_delta_poll_timeout(), || {
        catalog_delta_files(&delta_dir)
            .map(|deltas| deltas.is_empty())
            .unwrap_or(true)
    });
    let mut failures = Vec::new();
    if !rebuilt_in_time {
        failures.push(
            "fallback must force-rewrite the catalog checkpoint away from the stale baseline \
             (force_registry_checkpoint after startup)"
                .to_string(),
        );
    }
    if !cleaned_in_time {
        failures.push(format!(
            "fallback must clean the leftover delta(s) under {}",
            delta_dir.display()
        ));
    }
    if rebuilt_in_time && cleaned_in_time {
        assert_catalog_rebuilt_to_on_disk_segments(data_path, &baseline, &mut failures);
    }
    assert!(
        failures.is_empty(),
        "missing-delta fallback expectations failed:\n- {}",
        failures.join("\n- ")
    );
    storage.close().unwrap();
}

/// T3-红：最新 delta 半写截断 → 重开必须成功并回退重建：损坏 delta 与残留旧
/// delta 一并清理，全窗数据逐点一致。
#[test]
fn catalog_delta_truncated_falls_back_to_rebuild() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let delta_dir = catalog_delta_dir(data_path);
    let labels = vec![Label::new("host", "truncated")];

    let (baseline, captured) = catalog_delta_crash_window_fixture(
        data_path,
        &labels,
        "catalog-truncated-base",
        &["catalog-truncated-b", "catalog-truncated-c"],
    );
    rewind_catalog_to_crash_window(data_path, &baseline, &captured);

    // 崩溃窗：最新 delta 半写——内容截断一半（JSON 解析必然失败）。
    let (newest_name, newest_bytes) = captured.last().expect("fixture must capture two deltas");
    assert!(
        newest_bytes.len() >= 4,
        "delta must be long enough to truncate meaningfully, got {} bytes",
        newest_bytes.len()
    );
    let newest_path = delta_dir.join(newest_name);
    let mut truncated = newest_bytes.clone();
    truncated.truncate(truncated.len() / 2);
    std::fs::write(&newest_path, truncated).expect("truncated delta must be writable");
    assert_eq!(
        catalog_delta_files(&delta_dir)
            .expect("both deltas must stay listed inside the crash window")
            .len(),
        2,
        "crash window shape: the truncated delta must remain on disk"
    );

    // 重开：损坏 delta 不得阻塞启动；回退重建后全窗逐点一致。
    let storage = catalog_delta_builder(data_path)
        .with_compaction_interval(Duration::from_secs(60))
        .build()
        .expect("reopen must succeed when the newest delta is half-written");
    assert_catalog_delta_series_window(&storage, "catalog-truncated-base", &labels, 1, 4);
    assert_catalog_delta_series_window(&storage, "catalog-truncated-b", &labels, 11, 4);
    assert_catalog_delta_series_window(&storage, "catalog-truncated-c", &labels, 21, 4);

    // 回退语义（绿态判据，W3-T4 实现）：catalog 强制重写 + 全部 delta 清理。
    let rebuilt_in_time = poll_until(catalog_delta_poll_timeout(), || {
        std::fs::read(data_catalog_checkpoint_file(data_path))
            .map(|bytes| bytes != baseline.bytes)
            .unwrap_or(false)
    });
    let cleaned_in_time = poll_until(catalog_delta_poll_timeout(), || {
        catalog_delta_files(&delta_dir)
            .map(|deltas| deltas.is_empty())
            .unwrap_or(true)
    });
    let mut failures = Vec::new();
    if !rebuilt_in_time {
        failures.push(
            "fallback must force-rewrite the catalog checkpoint away from the stale baseline \
             (force_registry_checkpoint after startup)"
                .to_string(),
        );
    }
    if !cleaned_in_time {
        failures.push(format!(
            "fallback must clean the truncated delta and the leftover older delta(s) under {}",
            delta_dir.display()
        ));
    }
    if rebuilt_in_time && cleaned_in_time {
        assert_catalog_rebuilt_to_on_disk_segments(data_path, &baseline, &mut failures);
    }
    assert!(
        failures.is_empty(),
        "truncated-delta fallback expectations failed:\n- {}",
        failures.join("\n- ")
    );
    storage.close().unwrap();
}

/// catalog checkpoint 的 segments 数组按 (lane, level, segment_id) 不得出现重复
/// 条目——重复应用若非幂等（upsert 退化为追加并落盘），会在这里暴露。
fn assert_no_duplicate_catalog_entries(catalog_bytes: &[u8]) {
    let value: serde_json::Value = serde_json::from_slice(catalog_bytes)
        .expect("catalog checkpoint must stay valid JSON after the delta replay");
    let segments = value
        .get("segments")
        .and_then(serde_json::Value::as_array)
        .expect("catalog checkpoint must carry a segments array");
    let mut seen = std::collections::BTreeSet::new();
    for segment in segments {
        // serde_json::Value 未实现 Ord（无 `ord` feature），用 JSON 字面串作键。
        let key = (
            segment.get("lane").map(serde_json::Value::to_string),
            segment.get("level").map(serde_json::Value::to_string),
            segment.get("segment_id").map(serde_json::Value::to_string),
        );
        let key = format!("{:?}/{:?}/{:?}", key.0, key.1, key.2);
        assert!(
            seen.insert(key.clone()),
            "catalog must not carry duplicate segment entries after delta replay: {key}"
        );
    }
}

/// T3-红：`_checkpoint.json` 指向旧序号但 checkpoint 内容已合并了这些 delta
/// （「checkpoint 已写、marker 未更新」崩溃窗）→ 重开时 seq > checkpoint_seq 的
/// delta 会被重复应用；upsert 幂等要求重开成功、条目无重复、不退化成回退重写。
#[test]
fn catalog_delta_stale_checkpoint_replays_idempotently() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let delta_dir = catalog_delta_dir(data_path);
    let marker = delta_dir.join(CATALOG_CHECKPOINT_MARKER_NAME);
    let labels = vec![Label::new("host", "stale")];

    let (baseline, captured) = catalog_delta_crash_window_fixture(
        data_path,
        &labels,
        "catalog-stale-base",
        &["catalog-stale-b", "catalog-stale-c"],
    );
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let consolidated = snapshot_catalog_checkpoint(&catalog_file)
        .expect("close must leave the consolidated catalog checkpoint on disk");
    assert_ne!(
        consolidated.bytes, baseline.bytes,
        "close must have consolidated the delta segments into the checkpoint"
    );
    // 红态前置断言：当前实现没有 `_checkpoint.json`。
    let marker_seq = catalog_checkpoint_marker_seq(&marker)
        .expect("close must write _checkpoint.json carrying a numeric checkpoint_seq");

    // 构造「checkpoint 已写、marker 未更新」崩溃窗：marker 回拨到旧序号，已合并
    // 的 delta 按捕获字节复原（崩溃发生在 marker 更新与 delta 清理之前）。
    let max_captured_seq = captured
        .iter()
        .filter_map(|(name, _)| catalog_delta_seq(Path::new(name)))
        .max()
        .expect("captured deltas must carry parseable seqs");
    let stale_seq = marker_seq.saturating_sub(1);
    assert!(
        stale_seq < max_captured_seq,
        "stale anchor {stale_seq} must trail at least one restored delta \
         (max captured seq {max_captured_seq}) so the reopen genuinely re-applies deltas"
    );
    std::fs::create_dir_all(&delta_dir).expect("delta dir must exist for the crash-window state");
    std::fs::write(
        &marker,
        format!("{{\"version\":2,\"checkpoint_seq\":{stale_seq}}}"),
    )
    .expect("stale marker must be writable");
    for (name, bytes) in &captured {
        std::fs::write(delta_dir.join(name), bytes).expect("restored delta must be writable");
    }

    // 重开：重复应用不得产生重复条目，也不得退化成回退重建。
    let reopened = catalog_delta_builder(data_path)
        .with_compaction_interval(Duration::from_secs(60))
        .build()
        .expect("reopen must succeed when the checkpoint marker trails the checkpoint content");
    assert_catalog_delta_series_window(&reopened, "catalog-stale-base", &labels, 1, 4);
    assert_catalog_delta_series_window(&reopened, "catalog-stale-b", &labels, 11, 4);
    assert_catalog_delta_series_window(&reopened, "catalog-stale-c", &labels, 21, 4);

    // 幂等判据：catalog 主文件保持原样（回退重建会强制重写它）；文件内不得出现
    // 重复段条目。
    let after = std::fs::read(&catalog_file)
        .expect("catalog checkpoint must stay readable after the idempotent replay");
    assert_eq!(
        after, consolidated.bytes,
        "idempotent delta replay must validate the reconstructed catalog in place; \
         a rewrite here means the replay degraded into the fallback rebuild"
    );
    assert_no_duplicate_catalog_entries(&after);
    reopened.close().unwrap();
}

/// 冷路径（重启后无变更上报的 persist）必须与启动校验同一口径：把「checkpoint
/// 主文件 + 未合并 delta」重构后比对，命中即**原地验证**——不重写主文件、不删
/// delta、不推 marker。否则每次携带未合并 delta 的重启都必付一次 O(段数) 全量
/// 重写，delta 的收益被限制在单次会话内。
///
/// 盘面 = 真实的「发布拍已落 delta、close 之前崩溃」：主文件是上一会话 close 的
/// checkpoint（只含基线段），delta seq 1/2 完整，marker 从未写过（checkpoint_seq
/// =0 ⇒ 两笔 delta 全部可重放）。启动校验能通过 ⇒ finalize 选 `Persist`
/// （`root_changes=None`）⇒ 走的就是这条冷路径。
#[test]
fn catalog_cold_path_validates_stale_checkpoint_with_deltas_in_place() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let delta_dir = catalog_delta_dir(data_path);
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let marker = delta_dir.join(CATALOG_CHECKPOINT_MARKER_NAME);
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "cold-path")];

    let (baseline, captured) = catalog_delta_crash_window_fixture(
        data_path,
        &labels,
        "catalog-cold-base",
        &["catalog-cold-b", "catalog-cold-c"],
    );
    rewind_catalog_to_crash_window(data_path, &baseline, &captured);

    // 盘面自证：主文件只含基线段（陈旧），delta 完好且可重放，marker 不存在。
    let checkpoint_before = snapshot_catalog_checkpoint(&catalog_file)
        .expect("the crash window must leave the stale checkpoint readable");
    let on_disk_segments = count_lane_segments_across_levels(&lane_path);
    assert_eq!(
        catalog_checkpoint_segment_count(&checkpoint_before.bytes) + captured.len(),
        on_disk_segments,
        "crash-window shape: the stale checkpoint must trail the deltas by exactly \
         the delta-carried segments ({} checkpoint entries + {} deltas vs {} on-disk segments)",
        catalog_checkpoint_segment_count(&checkpoint_before.bytes),
        captured.len(),
        on_disk_segments
    );
    assert!(
        !marker.exists(),
        "crash-window shape: no consolidation marker may exist before the reopen"
    );
    assert_eq!(
        catalog_delta_files(&delta_dir)
            .expect("the crash window must keep the delta dir")
            .len(),
        captured.len(),
        "crash-window shape: every captured delta must be back on disk"
    );

    // 重开：启动校验重放 delta 通过 ⇒ 冷路径必须重构比对后原地验证。
    let reopened = catalog_delta_builder(data_path)
        .with_compaction_interval(Duration::from_secs(60))
        .build()
        .expect("reopen must succeed on a stale checkpoint plus valid deltas");
    assert_catalog_delta_series_window(&reopened, "catalog-cold-base", &labels, 1, 4);
    assert_catalog_delta_series_window(&reopened, "catalog-cold-b", &labels, 11, 4);
    assert_catalog_delta_series_window(&reopened, "catalog-cold-c", &labels, 21, 4);

    let mut failures = Vec::new();
    let after = match std::fs::read(&catalog_file) {
        Ok(bytes) => Some(bytes),
        Err(err) => {
            failures.push(format!(
                "catalog checkpoint {} must stay readable after the cold-path persist: {err}",
                catalog_file.display()
            ));
            None
        }
    };
    if let Some(after) = after {
        if after != checkpoint_before.bytes {
            failures.push(format!(
                "cold-path persist must validate the checkpoint+delta reconstruction in place \
                 and not rewrite the main file: len {} -> {}, mtime {:?} -> {:?}",
                checkpoint_before.len,
                after.len(),
                checkpoint_before.modified,
                std::fs::metadata(&catalog_file)
                    .and_then(|meta| meta.modified())
                    .ok()
            ));
        }
    }
    if let Ok(deltas) = catalog_delta_files(&delta_dir) {
        if deltas.len() != captured.len() {
            failures.push(format!(
                "an in-place validation must leave the {} replayable delta(s) untouched, found {}",
                captured.len(),
                deltas.len()
            ));
        }
    }
    if marker.exists() {
        failures.push(format!(
            "an in-place validation must not advance {}",
            marker.display()
        ));
    }
    let checkpoints_after_reopen = reopened
        .observability_snapshot()
        .flush
        .catalog_checkpoint_total;
    if checkpoints_after_reopen != 0 {
        failures.push(format!(
            "cold-path persist counted {checkpoints_after_reopen} full checkpoint write(s); \
             an in-place validation writes none"
        ));
    }
    assert!(
        failures.is_empty(),
        "cold-path in-place validation failed:\n- {}",
        failures.join("\n- ")
    );

    // 会话内继续发布：delta 序号接着重放链续写（seq 3），主文件仍不动——
    // 证明备忘（next_seq / last_entries_hash / last_file_len）已按主文件真实状态
    // 更新，而不是退化成「下一拍再全量重写」。
    settle_past_catalog_persist_throttle();
    insert_catalog_delta_series(&reopened, "catalog-cold-d", &labels, 31, 4);
    let appended = poll_until(catalog_delta_poll_timeout(), || {
        catalog_delta_files(&delta_dir)
            .map(|deltas| deltas.len() > captured.len())
            .unwrap_or(false)
    });
    assert!(
        appended,
        "the post-reopen publish tick must append a delta under {} within the poll timeout",
        delta_dir.display()
    );
    let after_publish = std::fs::read(&catalog_file)
        .expect("the main file must stay readable after the post-reopen publish");
    assert_eq!(
        after_publish, checkpoint_before.bytes,
        "a validated cold path must keep the main file untouched for the rest of the session"
    );
    reopened.close().unwrap();
}

/// Highest `seg-<hex>` dirname id under one level directory (`0` if none).
fn max_segment_dirname_id(level_dir: &Path) -> u64 {
    std::fs::read_dir(level_dir)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            u64::from_str_radix(name.strip_prefix("seg-")?, 16).ok()
        })
        .max()
        .unwrap_or(0)
}

/// W4-T5fix (D2): the recovered segment-id allocator must floor at the max
/// `seg-*` dirname id present on disk, not only at the manifest ids it
/// happened to load.
///
/// Fixture: a valid segment dir whose *manifest* declares id 1 but whose
/// *dirname* occupies id 2 (`seg-0000000000000002`). This is the degraded
/// on-disk shape the W4-T5 real-copy drill hit: the persisted allocator
/// bookkeeping trails the real dirname max (interrupted runs, non-atomic
/// copies of live directories, cross-version debris). Recovery derives
/// `next = max(loaded manifest ids) + 1 = 2` — exactly the occupied dirname —
/// so the first post-reopen persist published `seg-0000000000000002` and died
/// with `InvalidConfiguration("segment directory already exists: …")`. The
/// fix floors the recovered allocator at `max(dirname ids) + 1`.
#[test]
fn reopen_floors_segment_id_allocator_at_max_persisted_dirname() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let lane_root = data_path.join(NUMERIC_LANE_ROOT);
    let level_dir = |level: u8| lane_root.join("segments").join(format!("L{level}"));

    // Baseline instance: one real close so the registry snapshot and a real
    // id-1 L0 segment exist on disk.
    let labels = vec![Label::new("host", "allocator-drift")];
    let rows_for = |base_ts: i64| -> Vec<Row> {
        (0..4)
            .map(|offset| {
                Row::with_labels(
                    "allocator-drift-metric",
                    labels.clone(),
                    DataPoint::new(base_ts + offset, offset as f64 + 0.5),
                )
            })
            .collect()
    };
    {
        let storage = StorageBuilder::new()
            .with_data_path(data_path)
            // Keep background ticks from racing the test-driven close persist.
            .with_flush_interval(Duration::from_secs(3600))
            .with_compaction_interval(Duration::from_secs(3600))
            .build()
            .expect("baseline storage must open on a fresh dir");
        storage.insert_rows(&rows_for(1_000)).unwrap();
        storage.close().expect("baseline close");
    }

    // Craft the divergence in L0 (the level close-time persist publishes to):
    // write a real L0 segment with id 1 into an off-lane staging base, then
    // move its dirname into the lane as id 2. The manifest (and all payload
    // files) stay keyed to id 1, so recovery still counts manifest id 1 while
    // the L0 dirname space is occupied up to id 2.
    let registry = SeriesRegistry::load_persisted_state(&data_path.join(SERIES_INDEX_FILE_NAME))
        .expect("registry snapshot must load")
        .expect("baseline close must persist the series registry")
        .registry;
    let series_id = registry
        .resolve_or_insert("allocator-drift-metric", &labels)
        .expect("drift series must resolve")
        .series_id;
    let mut chunks = HashMap::new();
    chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(
            series_id,
            &[(10_000, 1.0), (10_001, 2.0), (10_002, 3.0)],
        )],
    );
    let drift_base = data_path.join("drift-staging");
    SegmentWriter::new(&drift_base, 0, 1)
        .expect("drift segment writer")
        .write_segment(&registry, &chunks)
        .expect("drift segment write");
    std::fs::rename(
        drift_base
            .join("segments")
            .join("L0")
            .join("seg-0000000000000001"),
        level_dir(0).join("seg-0000000000000002"),
    )
    .expect("drift dirname move");

    // Fixture sanity: the L0 dirname space reaches id 2 while every manifest
    // id on disk is 1.
    assert_eq!(max_segment_dirname_id(&level_dir(0)), 2);

    // Reopen: manifest-derived next would be 2 — exactly the occupied dirname.
    let storage = StorageBuilder::new()
        .with_data_path(data_path)
        .with_flush_interval(Duration::from_secs(3600))
        .with_compaction_interval(Duration::from_secs(3600))
        .build()
        .expect("reopen must succeed on the drifted dir");

    // Close persist allocates from the recovered allocator. Before the fix
    // this published id 2 and failed with
    // `InvalidConfiguration("segment directory already exists: …seg-0000000000000002")`;
    // after the fix the allocator starts above the on-disk dirname max and
    // every published segment avoids conflicts with existing dirs.
    storage.insert_rows(&rows_for(2_000)).unwrap();
    storage
        .close()
        .expect("close persist must publish above the on-disk max dirname id");

    // The new segment's dirname must not collide with any pre-existing dir:
    // id 2 was the max before close, so the fresh segment must sit above it.
    let l0_max_after = max_segment_dirname_id(&level_dir(0));
    assert!(
        l0_max_after > 2,
        "close persist must have published an L0 segment with dirname id above the \
         pre-close max (2); observed max L0 dirname id {l0_max_after}"
    );
}

/// C-8（红）：checkpoint 指纹为 `null` 且**没有**任何 delta 可重放时，
/// `catalog_delta_fingerprint_fallback_total` 必须计 1。
///
/// 这是「从旧版本升级」的典型盘面：旧版 catalog 主文件不带
/// `series_fingerprint`（字段 `#[serde(default)]` ⇒ 反序列化为 `None`），
/// 上一会话正常关闭后 delta 目录已被清理（无 delta 可重放）。启动校验通过
/// （段集匹配），快路径确实关闭（每个段都要全量读 `series.bin`），但当前实现
/// 把 `fingerprint_fallback` 只放在 delta 重放循环里赋值 ⇒ 无 delta 时恒
/// `false` ⇒ 计数器显示 0，与 `docs/storage-engine.md`「指纹为 null 也要计数」
/// 的约定不符。
///
/// 盘面构造：建实例 → 写段 → close（生成带指纹的 checkpoint 并清理 delta）→
/// 手工把主文件 `series_fingerprint` 改成 `null` → 删除 delta 目录（连 marker
/// 一起，`checkpoint_seq` 回落到 0）→ reopen → 读快照断言计数。
#[test]
fn catalog_fingerprint_fallback_counts_null_checkpoint_without_deltas() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let catalog_file = data_catalog_checkpoint_file(data_path);
    let delta_dir = catalog_delta_dir(data_path);
    let labels = vec![Label::new("host", "null-fingerprint")];

    // 第一开：基线数据 + close（强制 checkpoint：主文件带指纹、delta 清理）。
    {
        let storage = catalog_delta_builder(data_path).build().unwrap();
        settle_past_catalog_persist_throttle();
        for idx in 0..3 {
            insert_catalog_delta_series(&storage, &format!("catalog-null-fp-{idx}"), &labels, 1, 4);
        }
        storage.close().unwrap();
    }

    // 盘面自证：主文件存在且确实带指纹（否则这条用例构造的不是「旧版本」场景）。
    let checkpoint = std::fs::read(&catalog_file).expect("close must leave a catalog checkpoint");
    let mut value: serde_json::Value =
        serde_json::from_slice(&checkpoint).expect("catalog checkpoint must be valid JSON");
    assert!(
        !value["series_fingerprint"].is_null(),
        "precondition: a fresh close writes a checkpoint that carries a series fingerprint"
    );
    assert!(
        value["segments"]
            .as_array()
            .map(|segments| !segments.is_empty())
            .unwrap_or(false),
        "precondition: the checkpoint must carry the on-disk segments"
    );

    // 模拟旧版本盘面：指纹字段为 null（旧版序列化没有该字段 ⇒ serde default
    // 同样得到 None；两种构造等价，这里取显式 null 以固定盘面形状）。
    value["series_fingerprint"] = serde_json::Value::Null;
    std::fs::write(&catalog_file, serde_json::to_vec_pretty(&value).unwrap())
        .expect("the null-fingerprint checkpoint must be writable");
    // 无 delta：上一会话正常关闭已清理 delta 文件，这里把目录（含 marker）一并
    // 删除，保证重放循环一次也不执行。
    let _ = std::fs::remove_dir_all(&delta_dir);
    assert!(
        !delta_dir.exists(),
        "precondition: the crash-free disk state carries no delta directory"
    );

    // 重开：段集匹配 ⇒ 校验通过，但指纹缺失 ⇒ 回退计数必须为 1。
    let reopened = catalog_delta_builder(data_path)
        .with_compaction_interval(Duration::from_secs(60))
        .build()
        .expect("reopen must succeed on a valid catalog with a null fingerprint");
    let snapshot = reopened.observability_snapshot();
    assert_eq!(
        snapshot.flush.catalog_delta_fingerprint_fallback_total, 1,
        "a null checkpoint fingerprint with no delta to replay must count one fallback \
         (startup reads every segment's series.bin), got {}",
        snapshot.flush.catalog_delta_fingerprint_fallback_total
    );
    assert_eq!(
        snapshot.flush.catalog_delta_replayed_total, 0,
        "no delta exists on this disk state, so no replay may be counted"
    );
    assert_eq!(
        snapshot.flush.catalog_delta_apply_fallback_total, 0,
        "the catalog validated, so no apply fallback may be counted"
    );

    // 数据面自证：指纹回退只影响启动快慢，不影响数据。
    for idx in 0..3 {
        assert_catalog_delta_series_window(
            &reopened,
            &format!("catalog-null-fp-{idx}"),
            &labels,
            1,
            4,
        );
    }
    reopened.close().unwrap();
}
