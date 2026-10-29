use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::warn;
use xxhash_rust::xxh64::Xxh64;

use super::tiering::SegmentLaneFamily;
use super::*;
use crate::engine::fs_utils::{path_exists_no_follow, write_file_atomically_and_sync_parent};
use crate::engine::segment::IndexedSegment;
use crate::engine::series::{SeriesId, SeriesRegistry};
use crate::Label;

const REGISTRY_CATALOG_FILE_NAME: &str = "series_index.catalog.json";
const REGISTRY_CATALOG_VERSION: u32 = 2;

#[derive(Debug, Clone)]
pub(super) struct PersistedRegistryCatalogSource {
    pub(super) lane: SegmentLaneFamily,
    pub(super) root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogFile {
    version: u32,
    segments: Vec<PersistedRegistryCatalogEntry>,
    #[serde(default)]
    series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogEntry {
    lane: SegmentLaneFamily,
    level: u8,
    segment_id: u64,
    chunk_count: usize,
    point_count: usize,
    series_count: usize,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    wal_highwater_segment: u64,
    wal_highwater_frame: u64,
    chunks_len: u64,
    chunks_hash64: u64,
    chunk_index_len: u64,
    chunk_index_hash64: u64,
    series_len: u64,
    series_hash64: u64,
    postings_len: u64,
    postings_hash64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedRegistrySeriesFingerprint {
    series_count: usize,
    series_hash64: u64,
}

#[derive(Debug, Clone)]
pub(super) struct ValidatedRegistryCatalog {
    pub(super) series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
}

/// 段级 catalog entry 缓存（N9b）：段不可变且 segment_id 单调不复用 →
/// entry 按 root 永久有效。post-flush 维护拍高频构建 catalog 时全量重读
/// 段 manifest 是 openat 风暴根因（万级段 = 单核 CPU）；集合指纹缓存在
/// compaction 持续产段的实例上永不命中（实测），按段缓存后每拍只读新增段。
/// NotFound(None) 同缓存：消失是 compaction 删除的持久结果（N1 窗口瞬态
/// 场景下轮 sources 更新后自愈，catalog 是校验侧车不承载段数据）。
#[derive(Default)]
pub(super) struct CatalogEntryCache {
    entries: Mutex<HashMap<PathBuf, Option<PersistedRegistryCatalogEntry>>>,
    series: Mutex<HashMap<PathBuf, Vec<crate::engine::segment::PersistedSeries>>>,
}

impl CatalogEntryCache {
    fn get_or_build(
        &self,
        source: &PersistedRegistryCatalogSource,
    ) -> Result<Option<PersistedRegistryCatalogEntry>> {
        let mut guard = self.entries.lock();
        if let Some(cached) = guard.get(&source.root) {
            return Ok(cached.clone());
        }
        let built = build_catalog_entry(source)?;
        guard.insert(source.root.clone(), built.clone());
        Ok(built)
    }

    /// 段 series 元数据（series.bin）按 root 缓存——同 entry 语义：段不可变
    /// 永久有效；NotFound 缓存为空 Vec（compaction 删除持久；N1 瞬态窗口
    /// 下轮 sources 更新自愈）。
    fn get_or_load_series(
        &self,
        root: &Path,
    ) -> Result<Vec<crate::engine::segment::PersistedSeries>> {
        let mut guard = self.series.lock();
        if let Some(cached) = guard.get(root) {
            return Ok(cached.clone());
        }
        let loaded = match crate::engine::segment::load_segment_series_metadata(root) {
            Ok(series) => series,
            Err(TsinkError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
                warn!(
                    root = %root.display(),
                    "Persisted segment root disappeared while fingerprinting registry catalog series; skipping it"
                );
                Vec::new()
            }
            Err(TsinkError::IoWithPath { path, source })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                warn!(
                    root = %path.display(),
                    "Persisted segment root disappeared while fingerprinting registry catalog series; skipping it"
                );
                Vec::new()
            }
            Err(err) => return Err(err),
        };
        guard.insert(root.to_path_buf(), loaded.clone());
        Ok(loaded)
    }
}

pub(super) fn catalog_path(snapshot_path: &Path) -> PathBuf {
    snapshot_path
        .parent()
        .map(|parent| parent.join(REGISTRY_CATALOG_FILE_NAME))
        .unwrap_or_else(|| PathBuf::from(REGISTRY_CATALOG_FILE_NAME))
}

pub(super) fn validate_registry_catalog(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
) -> Result<Option<ValidatedRegistryCatalog>> {
    let path = catalog_path(snapshot_path);
    if !path_exists_no_follow(&path).map_err(|source| TsinkError::IoWithPath {
        path: path.clone(),
        source,
    })? {
        return Ok(None);
    }

    let expected_segments = build_catalog_entries(sources, entry_cache)?;
    let actual = serde_json::from_slice::<PersistedRegistryCatalogFile>(
        &std::fs::read(&path).map_err(|source| TsinkError::IoWithPath {
            path: path.clone(),
            source,
        })?,
    )?;
    if actual.segments != expected_segments {
        return Ok(None);
    }

    Ok(Some(ValidatedRegistryCatalog {
        series_fingerprint: actual.series_fingerprint,
    }))
}

/// 写出 catalog 并返回其 series_fingerprint。entries 构建走段级缓存，
/// 集合渐变时只读新增段（N9b）。
pub(super) fn persist_registry_catalog(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
) -> Result<Option<PersistedRegistrySeriesFingerprint>> {
    let path = catalog_path(snapshot_path);
    let catalog = build_catalog(sources, entry_cache)?;
    let fingerprint = catalog.series_fingerprint.clone();
    let bytes = serde_json::to_vec_pretty(&catalog)?;
    write_file_atomically_and_sync_parent(&path, &bytes)?;
    Ok(fingerprint)
}

pub(super) fn inventory_sources(
    inventory: &super::tiering::SegmentInventory,
) -> Vec<PersistedRegistryCatalogSource> {
    inventory
        .entries()
        .iter()
        .map(|entry| PersistedRegistryCatalogSource {
            lane: entry.lane,
            root: entry.root.clone(),
        })
        .collect()
}

pub(super) fn validate_registry_snapshot(
    registry: &SeriesRegistry,
    indexed_segments: &[IndexedSegment],
    expected: &PersistedRegistrySeriesFingerprint,
) -> Result<()> {
    let series_ids = indexed_segment_series_ids(indexed_segments);
    let actual = fingerprint_registry_series(registry, &series_ids)?;
    if actual == *expected {
        return Ok(());
    }

    Err(TsinkError::DataCorruption(
        "persisted registry snapshot conflicts with persisted segment series metadata".to_string(),
    ))
}

fn build_catalog(
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
) -> Result<PersistedRegistryCatalogFile> {
    Ok(PersistedRegistryCatalogFile {
        version: REGISTRY_CATALOG_VERSION,
        segments: build_catalog_entries(sources, entry_cache)?,
        series_fingerprint: Some(build_series_fingerprint_from_sources(sources, entry_cache)?),
    })
}

fn build_catalog_entries(
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
) -> Result<Vec<PersistedRegistryCatalogEntry>> {
    let mut segments = sources
        .iter()
        .map(|source| entry_cache.get_or_build(source))
        .collect::<Result<Vec<Option<PersistedRegistryCatalogEntry>>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    segments.sort_by_key(|entry| (entry.lane, entry.level, entry.segment_id));
    Ok(segments)
}

fn build_catalog_entry(
    source: &PersistedRegistryCatalogSource,
) -> Result<Option<PersistedRegistryCatalogEntry>> {
    let fingerprint = match crate::engine::segment::read_segment_manifest_fingerprint(&source.root)
    {
        Ok(fingerprint) => fingerprint,
        // N1 (2026-08-30 压测 T20 两次 os error 3)：compaction 替换会先删除源段目录，
        // 再让后台 refresh 线程把内存索引对齐。窗口期内 catalog 构建/校验仍会引用
        // 已消失的段根——Windows 上表现为 os error 3（路径不存在）。侧车只是校验
        // 缓存：跳过已消失的源并告警，段数据本身由替换输出承载，未丢失。
        Err(TsinkError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            warn!(
                root = %source.root.display(),
                "Persisted segment root disappeared while building the registry catalog; skipping it"
            );
            return Ok(None);
        }
        Err(TsinkError::IoWithPath { path, source })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            warn!(
                root = %path.display(),
                "Persisted segment root disappeared while building the registry catalog; skipping it"
            );
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    let [chunks, chunk_index, series, postings] = fingerprint.files;
    Ok(Some(PersistedRegistryCatalogEntry {
        lane: source.lane,
        level: fingerprint.manifest.level,
        segment_id: fingerprint.manifest.segment_id,
        chunk_count: fingerprint.manifest.chunk_count,
        point_count: fingerprint.manifest.point_count,
        series_count: fingerprint.manifest.series_count,
        min_ts: fingerprint.manifest.min_ts,
        max_ts: fingerprint.manifest.max_ts,
        wal_highwater_segment: fingerprint.manifest.wal_highwater.segment,
        wal_highwater_frame: fingerprint.manifest.wal_highwater.frame,
        chunks_len: chunks.file_len,
        chunks_hash64: chunks.hash64,
        chunk_index_len: chunk_index.file_len,
        chunk_index_hash64: chunk_index.hash64,
        series_len: series.file_len,
        series_hash64: series.hash64,
        postings_len: postings.file_len,
        postings_hash64: postings.hash64,
    }))
}

fn build_series_fingerprint_from_sources(
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut series_by_id = BTreeMap::<SeriesId, crate::engine::segment::PersistedSeries>::new();
    let mut series_id_by_key = BTreeMap::<(String, Vec<Label>), SeriesId>::new();

    for source in sources {
        // N9c：series 元数据按 root 缓存（段不可变）——validate 不命中期间
        // 每拍 persist 的全量 series.bin 重读是 flush 线程残余负载根因。
        let segment_series = entry_cache.get_or_load_series(&source.root)?;
        for series in segment_series {
            match series_by_id.get(&series.series_id) {
                Some(existing)
                    if existing.metric == series.metric && existing.labels == series.labels => {}
                Some(_) => {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} conflicts across persisted segment metadata",
                        series.series_id
                    )));
                }
                None => {
                    let key = (series.metric.clone(), series.labels.clone());
                    if let Some(existing_id) = series_id_by_key.get(&key) {
                        if *existing_id != series.series_id {
                            return Err(TsinkError::DataCorruption(format!(
                                "series key already bound to id {}, persisted segment metadata tried to bind {}",
                                existing_id, series.series_id
                            )));
                        }
                    } else {
                        series_id_by_key.insert(key, series.series_id);
                    }
                    series_by_id.insert(series.series_id, series);
                }
            }
        }
    }

    Ok(fingerprint_series(series_by_id.values()))
}

fn indexed_segment_series_ids(indexed_segments: &[IndexedSegment]) -> Vec<SeriesId> {
    let mut series_ids = BTreeSet::new();
    for segment in indexed_segments {
        for entry in &segment.chunk_index.entries {
            series_ids.insert(entry.series_id);
        }
    }
    series_ids.into_iter().collect()
}

fn fingerprint_registry_series(
    registry: &SeriesRegistry,
    series_ids: &[SeriesId],
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut series = Vec::with_capacity(series_ids.len());
    for series_id in series_ids {
        let Some(series_key) = registry.decode_series_key(*series_id) else {
            return Err(TsinkError::DataCorruption(format!(
                "persisted registry snapshot is missing series id {} required by persisted segments",
                series_id
            )));
        };
        series.push(crate::engine::segment::PersistedSeries {
            series_id: *series_id,
            metric: series_key.metric,
            labels: series_key.labels,
            value_family: None,
        });
    }
    Ok(fingerprint_series(series.iter()))
}

fn fingerprint_series<'a>(
    series: impl IntoIterator<Item = &'a crate::engine::segment::PersistedSeries>,
) -> PersistedRegistrySeriesFingerprint {
    let mut count = 0usize;
    let mut hasher = Xxh64::new(0);
    for series in series {
        count = count.saturating_add(1);
        hasher.update(&series.series_id.to_le_bytes());
        update_len_prefixed_bytes(&mut hasher, series.metric.as_bytes());
        hasher.update(&(series.labels.len() as u64).to_le_bytes());
        for label in &series.labels {
            update_len_prefixed_bytes(&mut hasher, label.name.as_bytes());
            update_len_prefixed_bytes(&mut hasher, label.value.as_bytes());
        }
    }
    PersistedRegistrySeriesFingerprint {
        series_count: count,
        series_hash64: hasher.digest(),
    }
}

fn update_len_prefixed_bytes(hasher: &mut Xxh64, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}
