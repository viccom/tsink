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

/// series 缓存槽位的估算堆字节口径（W2-T2）：每条 `PersistedSeries` 计
/// 结构体标量（含 `value_family`）+ metric String 堆字节 + labels Vec
/// （每条计一个 Vec 头 + 每 Label 结构与 name/value 堆字节）。不含分配器
/// 对齐/元数据开销（系统性偏差每条 ≤ 数十字节），只用于缓存淘汰预算，
/// 不参与内存记账。
fn estimated_series_bytes(series: &[crate::engine::segment::PersistedSeries]) -> usize {
    const SERIES_FIXED: usize = std::mem::size_of::<crate::engine::segment::PersistedSeries>();
    const VEC_HEADER: usize = std::mem::size_of::<Vec<Label>>();
    const LABEL_FIXED: usize = std::mem::size_of::<Label>();

    series
        .iter()
        .map(|series| {
            SERIES_FIXED
                + VEC_HEADER
                + series.metric.len()
                + series
                    .labels
                    .iter()
                    .map(|label| LABEL_FIXED + label.name.len() + label.value.len())
                    .sum::<usize>()
        })
        .sum()
}

/// 单 map 有界缓存原语：全局 touch 序（命中刷新）+ 超限批量淘汰至 3/4 水位。
///
/// 两层「0」语义显式区分，勿混：
/// - 内部构造（`Default` / `with_capacity`）：limit = 0 表示**无界**——
///   `Default` 即 (0, 0)，保持 N9b 既有无界行为不变。
/// - StorageBuilder 旋钮：0 表示**不缓存**（disabled），由
///   `CatalogEntryCache::from_knobs` 翻译成 `enabled = false`——既不命中
///   也不存储，直接走盘上构建/加载。
struct CatalogLruMap<V> {
    /// disabled（旋钮 0）：不命中不存储。
    enabled: bool,
    /// 条数上限；0 = 按条数无界。
    capacity: usize,
    /// 估算字节预算；0 = 按字节无界。
    budget_bytes: u64,
    touch_seq: AtomicU64,
    evictions_total: AtomicU64,
    /// 当前驻留条目数（gauge；全部变更都发生在 `slots` 锁内，读侧免锁）。
    resident: AtomicU64,
    used_bytes: AtomicU64,
    slots: Mutex<HashMap<PathBuf, CatalogCacheSlot<V>>>,
}

struct CatalogCacheSlot<V> {
    value: V,
    touch: u64,
    approx_bytes: u64,
}

impl<V> Default for CatalogLruMap<V> {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: 0,
            budget_bytes: 0,
            touch_seq: AtomicU64::new(0),
            evictions_total: AtomicU64::new(0),
            resident: AtomicU64::new(0),
            used_bytes: AtomicU64::new(0),
            slots: Mutex::new(HashMap::new()),
        }
    }
}

impl<V: Clone> CatalogLruMap<V> {
    fn new(enabled: bool, capacity: usize, budget_bytes: usize) -> Self {
        Self {
            enabled,
            capacity,
            budget_bytes: saturating_u64_from_usize(budget_bytes),
            ..Default::default()
        }
    }

    fn get(&self, root: &Path) -> Option<V> {
        if !self.enabled {
            return None;
        }
        let mut slots = self.slots.lock();
        let slot = slots.get_mut(root)?;
        slot.touch = self.touch_seq.fetch_add(1, Ordering::Relaxed);
        Some(slot.value.clone())
    }

    fn insert(&self, root: PathBuf, value: V, approx_bytes: u64) {
        if !self.enabled {
            return;
        }
        let mut slots = self.slots.lock();
        let touch = self.touch_seq.fetch_add(1, Ordering::Relaxed);
        match slots.get_mut(&root) {
            Some(slot) => {
                // 并发双 miss 双构建的罕见竞态：覆盖值、字节差量对账、touch 刷新。
                let previous_bytes = slot.approx_bytes;
                slot.value = value;
                slot.touch = touch;
                slot.approx_bytes = approx_bytes;
                self.adjust_used_bytes(previous_bytes, approx_bytes);
            }
            None => {
                slots.insert(
                    root,
                    CatalogCacheSlot {
                        value,
                        touch,
                        approx_bytes,
                    },
                );
                self.resident.fetch_add(1, Ordering::Relaxed);
                self.used_bytes.fetch_add(approx_bytes, Ordering::Relaxed);
            }
        }
        self.evict_over_limit(&mut slots);
    }

    fn remove(&self, root: &Path) {
        if !self.enabled {
            return;
        }
        let mut slots = self.slots.lock();
        if let Some(slot) = slots.remove(root) {
            self.resident.fetch_sub(1, Ordering::Relaxed);
            saturating_sub_fetch(&self.used_bytes, slot.approx_bytes);
        }
    }

    fn adjust_used_bytes(&self, previous: u64, current: u64) {
        if current > previous {
            self.used_bytes
                .fetch_add(current - previous, Ordering::Relaxed);
        } else if current < previous {
            saturating_sub_fetch(&self.used_bytes, previous - current);
        }
    }

    /// 超限批量淘汰（插入路径调用）：一次按 touch 升序扫描，同时朝
    /// count `capacity * 3/4` 与 bytes `budget * 3/4` 两个水位收敛；摊还
    /// O(1)/插入。任何时刻至少保留 1 条（touch 最新的刚插入条目不淘汰）——
    /// 单条目超预算时宁可越界也不全清，全清会让下次读取必然 miss，重演
    /// N9 修掉的重读风暴。
    fn evict_over_limit(&self, slots: &mut HashMap<PathBuf, CatalogCacheSlot<V>>) {
        let count_over = self.capacity > 0 && slots.len() > self.capacity;
        let bytes_over =
            self.budget_bytes > 0 && self.used_bytes.load(Ordering::Relaxed) > self.budget_bytes;
        if !count_over && !bytes_over {
            return;
        }
        let count_water_mark = (self.capacity / 4 * 3).max(1);
        let bytes_water_mark = self.budget_bytes / 4 * 3;
        let mut used_bytes = self.used_bytes.load(Ordering::Relaxed);
        let mut by_touch = slots
            .iter()
            .map(|(root, slot)| (root.clone(), slot.touch))
            .collect::<Vec<_>>();
        by_touch.sort_unstable_by_key(|(_, touch)| *touch);
        for (root, _) in by_touch {
            let count_satisfied = !count_over || slots.len() <= count_water_mark;
            let bytes_satisfied = !bytes_over || used_bytes <= bytes_water_mark;
            if (count_satisfied && bytes_satisfied) || slots.len() <= 1 {
                break;
            }
            if let Some(slot) = slots.remove(&root) {
                used_bytes = used_bytes.saturating_sub(slot.approx_bytes);
                self.resident.fetch_sub(1, Ordering::Relaxed);
                saturating_sub_fetch(&self.used_bytes, slot.approx_bytes);
                self.evictions_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// 段级 catalog entry 缓存（N9b）：段不可变且 segment_id 单调不复用 →
/// entry 按 root 永久有效。post-flush 维护拍高频构建 catalog 时全量重读
/// 段 manifest 是 openat 风暴根因（万级段 = 单核 CPU）；集合指纹缓存在
/// compaction 持续产段的实例上永不命中（实测），按段缓存后每拍只读新增段。
/// NotFound(None) 同缓存：消失是 compaction 删除的持久结果（N1 窗口瞬态
/// 场景下轮 sources 更新后自愈，catalog 是校验侧车不承载段数据）。
///
/// W2-T2：`Default`（无界）之外新增有界语义——`from_knobs` 接 StorageBuilder
/// 旋钮（0 = 不缓存），entries 按条数、series 按估算字节独立淘汰；removed_roots
/// 实际应用到持久化索引后必须 `invalidate`（删除打破「段不可变 → 永久有效」
/// 前提，不失效会让陈旧指纹与已删段的 series 元数据一直存活到重启）。
#[derive(Default)]
pub(super) struct CatalogEntryCache {
    entries: CatalogLruMap<Option<PersistedRegistryCatalogEntry>>,
    series: CatalogLruMap<Vec<crate::engine::segment::PersistedSeries>>,
}

/// entries 槽位的估算字节（该 map 无字节预算，仅 used_bytes 观测口径）：
/// 条目结构 + key PathBuf 的近似堆占用。
const CATALOG_ENTRY_SLOT_ESTIMATED_BYTES: u64 = (std::mem::size_of::<
    Option<PersistedRegistryCatalogEntry>,
>() + std::mem::size_of::<PathBuf>()
    + 16) as u64;

impl CatalogEntryCache {
    /// 内部构造：`0` = 无界（`Default`（0, 0）即既有无界行为）。
    #[cfg(test)]
    fn with_capacity(entry_capacity: usize, series_budget_bytes: usize) -> Self {
        Self {
            entries: CatalogLruMap::new(true, entry_capacity, 0),
            series: CatalogLruMap::new(true, 0, series_budget_bytes),
        }
    }

    /// StorageBuilder 旋钮构造：旋钮 `0` = 不缓存（disabled——既不命中也不
    /// 存储，直接走盘上构建/加载）；非 0 旋钮 = 对应 map 有界。与内部
    /// `with_capacity` 的「0 = 无界」是两层语义。
    pub(super) fn from_knobs(entry_capacity: usize, series_budget_bytes: usize) -> Self {
        Self {
            entries: CatalogLruMap::new(entry_capacity > 0, entry_capacity, 0),
            series: CatalogLruMap::new(series_budget_bytes > 0, 0, series_budget_bytes),
        }
    }

    /// removed_roots 已实际应用到持久化索引后调用：双 map 齐删该 root
    /// （含 None/空 Vec 槽）。见结构体文档的失效前提。
    pub(super) fn invalidate(&self, root: &Path) {
        self.entries.remove(root);
        self.series.remove(root);
    }

    pub(super) fn invalidate_roots(&self, roots: &[PathBuf]) {
        for root in roots {
            self.invalidate(root);
        }
    }

    /// （累计淘汰条数, 当前驻留条目数）——entries 与 series 两 map 合计。
    pub(super) fn observability_totals(&self) -> (u64, u64) {
        (
            self.entries.evictions_total.load(Ordering::Relaxed)
                + self.series.evictions_total.load(Ordering::Relaxed),
            self.entries.resident.load(Ordering::Relaxed)
                + self.series.resident.load(Ordering::Relaxed),
        )
    }

    // `#[cfg(test)]` 观测访问器：W2-T1 红阶段钉断言用，保留供缓存语义测试。
    #[cfg(test)]
    fn entries_len(&self) -> usize {
        self.entries.slots.lock().len()
    }

    #[cfg(test)]
    fn series_len(&self) -> usize {
        self.series.slots.lock().len()
    }

    #[cfg(test)]
    fn entries_contains(&self, root: &Path) -> bool {
        self.entries.slots.lock().contains_key(root)
    }

    #[cfg(test)]
    fn series_contains(&self, root: &Path) -> bool {
        self.series.slots.lock().contains_key(root)
    }

    fn get_or_build(
        &self,
        source: &PersistedRegistryCatalogSource,
    ) -> Result<Option<PersistedRegistryCatalogEntry>> {
        if let Some(cached) = self.entries.get(&source.root) {
            return Ok(cached);
        }
        let built = build_catalog_entry(source)?;
        self.entries.insert(
            source.root.clone(),
            built.clone(),
            CATALOG_ENTRY_SLOT_ESTIMATED_BYTES,
        );
        Ok(built)
    }

    /// 段 series 元数据（series.bin）按 root 缓存——同 entry 语义：段不可变
    /// 永久有效；NotFound 缓存为空 Vec（compaction 删除持久；N1 瞬态窗口
    /// 下轮 sources 更新自愈）。
    fn get_or_load_series(
        &self,
        root: &Path,
    ) -> Result<Vec<crate::engine::segment::PersistedSeries>> {
        if let Some(cached) = self.series.get(root) {
            return Ok(cached);
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
        let approx_bytes = saturating_u64_from_usize(estimated_series_bytes(&loaded));
        self.series
            .insert(root.to_path_buf(), loaded.clone(), approx_bytes);
        Ok(loaded)
    }
}

pub(super) fn catalog_path(snapshot_path: &Path) -> PathBuf {
    snapshot_path
        .parent()
        .map(|parent| parent.join(REGISTRY_CATALOG_FILE_NAME))
        .unwrap_or_else(|| PathBuf::from(REGISTRY_CATALOG_FILE_NAME))
}

/// catalog 持久化备忘（P1）：节流窗口 + 内容指纹，消灭「每次 persist 全量
/// 读+解析+重写」的 O(段数) 往返。
///
/// 语义前提：catalog 是各段 `manifest.bin` 的反规范化缓存，不是正确性依赖——
/// 启动恢复从段目录扫描补全（`bootstrap::recovery` 的 validate 失配即回退重建），
/// 因此常规 persist 允许滞后，只有 checkpoint/关闭等一致性锚点强制刷新。
#[derive(Default)]
pub(super) struct CatalogPersistMemo {
    state: Mutex<CatalogPersistMemoState>,
}

#[derive(Default, Clone, Copy)]
struct CatalogPersistMemoState {
    last_persisted_at: Option<Instant>,
    last_entries_hash: Option<u64>,
    last_file_len: Option<u64>,
}

/// 一次 catalog 持久化的结果（调用侧用于计数器归因）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CatalogPersistOutcome {
    Persisted,
    /// 节流窗口内：不构建、不读盘、不写盘。
    SkippedThrottled,
    /// 内容与上次写入一致且文件未被外部改动（备忘命中，零 IO）。
    SkippedUnchanged,
    /// 冷启动/备忘失效：读文件做 entries 比对，命中即采纳文件指纹（零重写，
    /// 且不读取 segment 的 series.bin——保持「catalog 有效即可恢复」语义）。
    SkippedValidatedFromFile,
}

/// 按「节流窗口 + 内容指纹」持久化 catalog（P1）。
///
/// 与旧路径的区别：不再每次 `read` + `serde_json::from_slice` 全量比对，
/// 也不在内容未变时重写文件。`force=true` 仅用于 checkpoint/关闭锚点。
pub(super) fn persist_registry_catalog_throttled(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
    memo: &CatalogPersistMemo,
    min_interval: Duration,
    force: bool,
) -> Result<(
    Option<PersistedRegistrySeriesFingerprint>,
    CatalogPersistOutcome,
)> {
    let path = catalog_path(snapshot_path);
    let now = Instant::now();
    {
        let state = memo.state.lock();
        if !force {
            if let Some(last) = state.last_persisted_at {
                if now.saturating_duration_since(last) < min_interval {
                    return Ok((None, CatalogPersistOutcome::SkippedThrottled));
                }
            }
        }
    }

    // entries 构建走段级缓存：集合渐变时只读新增段（N9b），无全量 manifest 重读。
    let expected_entries = build_catalog_entries(sources, entry_cache)?;
    let entries_hash = hash_catalog_entries(&expected_entries);
    let file_len = std::fs::metadata(&path).map(|meta| meta.len()).ok();
    {
        let mut state = memo.state.lock();
        if state.last_entries_hash == Some(entries_hash) && state.last_file_len == file_len {
            state.last_persisted_at = Some(now);
            return Ok((None, CatalogPersistOutcome::SkippedUnchanged));
        }
    }

    // 冷启动 / 跨窗口：先只读文件做 entries 比对。命中则直接采纳文件的
    // series_fingerprint ——不读任何 segment 的 series.bin（catalog 有效即
    // 足以恢复，见 `startup_reopens_from_registry_catalog_without_segment_series_files`）。
    if let Ok(bytes) = std::fs::read(&path) {
        if let Ok(actual) = serde_json::from_slice::<PersistedRegistryCatalogFile>(&bytes) {
            if actual.segments == expected_entries {
                let mut state = memo.state.lock();
                state.last_persisted_at = Some(now);
                state.last_entries_hash = Some(entries_hash);
                state.last_file_len = Some(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                return Ok((
                    actual.series_fingerprint,
                    CatalogPersistOutcome::SkippedValidatedFromFile,
                ));
            }
        }
    }

    let catalog = PersistedRegistryCatalogFile {
        version: REGISTRY_CATALOG_VERSION,
        segments: expected_entries,
        series_fingerprint: Some(build_series_fingerprint_from_sources(sources, entry_cache)?),
    };
    let fingerprint = catalog.series_fingerprint.clone();
    let bytes = serde_json::to_vec_pretty(&catalog)?;
    write_file_atomically_and_sync_parent(&path, &bytes)?;

    let mut state = memo.state.lock();
    state.last_persisted_at = Some(now);
    state.last_entries_hash = Some(entries_hash);
    state.last_file_len = Some(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
    Ok((fingerprint, CatalogPersistOutcome::Persisted))
}

/// 参与内容比对的全部条目标量字段 → 单一指纹（避免序列化数 MB 字节做比较）。
fn hash_catalog_entries(entries: &[PersistedRegistryCatalogEntry]) -> u64 {
    let mut hasher = Xxh64::new(0);
    hasher.update(
        &u64::try_from(entries.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    for entry in entries {
        let lane_tag: u8 = match entry.lane {
            SegmentLaneFamily::Numeric => 0,
            SegmentLaneFamily::Blob => 1,
        };
        hasher.update(&[lane_tag, entry.level]);
        hasher.update(&entry.segment_id.to_le_bytes());
        hasher.update(
            &u64::try_from(entry.chunk_count)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(
            &u64::try_from(entry.point_count)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(
            &u64::try_from(entry.series_count)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(&entry.min_ts.unwrap_or(i64::MIN).to_le_bytes());
        hasher.update(&entry.max_ts.unwrap_or(i64::MIN).to_le_bytes());
        hasher.update(&entry.wal_highwater_segment.to_le_bytes());
        hasher.update(&entry.wal_highwater_frame.to_le_bytes());
        hasher.update(&entry.chunks_len.to_le_bytes());
        hasher.update(&entry.chunks_hash64.to_le_bytes());
        hasher.update(&entry.chunk_index_len.to_le_bytes());
        hasher.update(&entry.chunk_index_hash64.to_le_bytes());
        hasher.update(&entry.series_len.to_le_bytes());
        hasher.update(&entry.series_hash64.to_le_bytes());
        hasher.update(&entry.postings_len.to_le_bytes());
        hasher.update(&entry.postings_hash64.to_le_bytes());
    }
    hasher.digest()
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

/// W2-T1（edge-hardening）：CatalogEntryCache 淘汰与失效语义测试（红）。
/// 放在本文件而非 tests/persistence_background.rs：`get_or_build` /
/// `get_or_load_series` 是模块私有方法，子模块测试可直接调用，零可见性改动；
/// 且这三个用例测的是缓存结构语义（淘汰/失效）而非持久化管线。真实段目录用
/// SegmentWriter 直接产（manifest 二进制格式不手搓），不需要 storage 实例。
#[cfg(test)]
mod catalog_entry_cache_tests {
    use super::*;
    use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
    use crate::engine::durability::WalHighWatermark;
    use crate::engine::encoder::Encoder;
    use crate::engine::segment::PersistedSeries;
    use crate::engine::series::SeriesValueFamily;
    use crate::value::Value;
    use tempfile::TempDir;

    /// 产一个真实段目录（manifest.bin + series.bin 全套）：root 由 segment_id
    /// 确定，manifest 二进制格式不手搓。
    fn write_numeric_segment(
        lane_path: &Path,
        registry: &SeriesRegistry,
        series_id: SeriesId,
        segment_id: u64,
        points: &[(i64, f64)],
    ) -> PathBuf {
        let chunk_points = points
            .iter()
            .map(|(ts, value)| ChunkPoint {
                ts: *ts,
                value: Value::F64(*value),
            })
            .collect::<Vec<_>>();
        let encoded = Encoder::encode_chunk_points(&chunk_points, ValueLane::Numeric).unwrap();
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![Chunk {
                header: ChunkHeader {
                    series_id,
                    lane: ValueLane::Numeric,
                    value_family: Some(SeriesValueFamily::F64),
                    point_count: chunk_points.len() as u16,
                    min_ts: chunk_points.first().unwrap().ts,
                    max_ts: chunk_points.last().unwrap().ts,
                    ts_codec: encoded.ts_codec,
                    value_codec: encoded.value_codec,
                },
                points: chunk_points,
                encoded_payload: encoded.payload,
                wal_highwater: WalHighWatermark::default(),
            }],
        );
        let writer = SegmentWriter::new(lane_path, 0, segment_id).unwrap();
        writer.write_segment(registry, &chunks).unwrap();
        writer.layout().root.clone()
    }

    fn numeric_source(root: PathBuf) -> PersistedRegistryCatalogSource {
        PersistedRegistryCatalogSource {
            lane: SegmentLaneFamily::Numeric,
            root,
        }
    }

    fn resolve_one_series(metric: &str, labels: &[Label], registry: &SeriesRegistry) -> SeriesId {
        registry
            .resolve_or_insert(metric, labels)
            .unwrap()
            .series_id
    }

    /// PersistedSeries 未 derive PartialEq：字段级比较。
    fn assert_series_lists_equal(left: &[PersistedSeries], right: &[PersistedSeries]) {
        assert_eq!(left.len(), right.len());
        for (left_series, right_series) in left.iter().zip(right.iter()) {
            assert_eq!(left_series.series_id, right_series.series_id);
            assert_eq!(left_series.metric, right_series.metric);
            assert_eq!(left_series.labels, right_series.labels);
            assert_eq!(left_series.value_family, right_series.value_family);
        }
    }

    /// W2-T2 新增：命中刷新 touch 序 → 淘汰序改变。capacity=4 插 A B C D，
    /// 多次命中 A，再插 E 触发淘汰：A 必须存活，未命中的最旧者（B、C）出局。
    #[test]
    fn catalog_entry_cache_hit_refreshes_touch_order() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join("numeric");
        let registry = SeriesRegistry::new();
        let labels = vec![Label::new("host", "touch-order")];
        let series_id = resolve_one_series("catalog_cache_touch_metric", &labels, &registry);

        let roots = (1..=5u64)
            .map(|segment_id| {
                write_numeric_segment(
                    &lane_path,
                    &registry,
                    series_id,
                    segment_id,
                    &[(segment_id as i64, segment_id as f64)],
                )
            })
            .collect::<Vec<_>>();
        let sources = roots
            .iter()
            .map(|root| numeric_source(root.clone()))
            .collect::<Vec<_>>();

        let cache = CatalogEntryCache::with_capacity(4, 0);
        for source in &sources[..4] {
            cache
                .get_or_build(source)
                .unwrap()
                .expect("real segment root must build an entry");
        }
        // 命中 A（roots[0]）两次：A 的 touch 序刷新到最新。
        cache.get_or_build(&sources[0]).unwrap();
        cache.get_or_build(&sources[0]).unwrap();
        // 第 5 次插入触发淘汰至 3：最旧的两个未命中者（B、C）出局。
        cache.get_or_build(&sources[4]).unwrap();

        assert!(
            cache.entries_contains(&roots[0]),
            "touched root must survive eviction (touch order refreshed by hits)"
        );
        assert!(
            !cache.entries_contains(&roots[1]),
            "untouched oldest root must be evicted first"
        );
        assert!(!cache.entries_contains(&roots[2]));
        assert!(cache.entries_contains(&roots[3]));
        assert!(cache.entries_contains(&roots[4]));
        assert_eq!(
            cache.entries_len(),
            3,
            "eviction must stop at the capacity * 3/4 watermark"
        );
    }

    /// 目标语义（W2-T2 钉死）：capacity=4 时第 5 次插入触发淘汰至 3（最旧
    /// root1/root2 出局），第 6 次插入后 len=4 不再触发——终态 len==4；
    /// 淘汰后再次读取（盘上还在）能正确重建且内容一致。
    #[test]
    fn catalog_entry_cache_evicts_oldest_beyond_capacity() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join("numeric");
        let registry = SeriesRegistry::new();
        let labels = vec![Label::new("host", "eviction")];
        let series_id = resolve_one_series("catalog_cache_eviction_metric", &labels, &registry);

        let roots = (1..=6u64)
            .map(|segment_id| {
                write_numeric_segment(
                    &lane_path,
                    &registry,
                    series_id,
                    segment_id,
                    &[(segment_id as i64, segment_id as f64)],
                )
            })
            .collect::<Vec<_>>();
        let sources = roots
            .iter()
            .map(|root| numeric_source(root.clone()))
            .collect::<Vec<_>>();

        let cache = CatalogEntryCache::with_capacity(4, 0);
        let mut built = Vec::new();
        for source in &sources {
            built.push(
                cache
                    .get_or_build(source)
                    .unwrap()
                    .expect("real segment root must build an entry"),
            );
        }

        assert_eq!(
            cache.entries_len(),
            4,
            "inserting beyond entry capacity must evict down to capacity"
        );
        assert!(
            !cache.entries_contains(&roots[0]),
            "oldest root #1 must be evicted"
        );
        assert!(
            !cache.entries_contains(&roots[1]),
            "oldest root #2 must be evicted"
        );
        for root in &roots[2..] {
            assert!(
                cache.entries_contains(root),
                "recent roots must stay cached"
            );
        }

        let rebuilt = cache
            .get_or_build(&sources[0])
            .unwrap()
            .expect("evicted-but-on-disk root must rebuild");
        assert_eq!(
            rebuilt, built[0],
            "rebuilt entry must match the pre-eviction entry"
        );
    }

    /// 目标语义：失效接口同时清 entries 与 series 两个 map（含 None/空 Vec
    /// 槽）；失效后目录已删 → get_or_build 走磁盘得 None（NotFound 语义），
    /// 不得复活已失效的槽。
    #[test]
    fn catalog_entry_cache_drops_removed_root() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join("numeric");
        let registry = SeriesRegistry::new();
        let labels = vec![Label::new("host", "invalidate")];
        let series_id = resolve_one_series("catalog_cache_invalidate_metric", &labels, &registry);
        let root =
            write_numeric_segment(&lane_path, &registry, series_id, 1, &[(1, 1.0), (2, 2.0)]);
        let source = numeric_source(root.clone());

        let cache = CatalogEntryCache::with_capacity(0, 0);
        assert!(cache.get_or_build(&source).unwrap().is_some());
        assert_eq!(cache.get_or_load_series(&root).unwrap().len(), 1);
        assert_eq!(cache.entries_len(), 1);
        assert_eq!(cache.series_len(), 1);

        cache.invalidate(&root);

        assert_eq!(
            cache.entries_len(),
            0,
            "invalidate must drop the entries slot immediately"
        );
        assert_eq!(
            cache.series_len(),
            0,
            "invalidate must drop the series slot too"
        );
        assert!(!cache.entries_contains(&root));
        assert!(!cache.series_contains(&root));

        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(
            cache.get_or_build(&source).unwrap(),
            None,
            "removed root must rebuild to None from disk"
        );
        assert_eq!(
            cache.entries_len(),
            1,
            "None (NotFound) is itself a cached outcome"
        );
    }

    /// 目标语义：series 缓存按估算字节计上界，超预算按 touch 序淘汰最旧，
    /// 且不 panic；被淘汰的 root 盘上还在 → 再读能重建出原数据。
    #[test]
    fn catalog_series_cache_respects_byte_budget() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join("numeric");
        let registry = SeriesRegistry::new();

        // 三个段，每段一个独立 series → series.bin 各自非空。
        let roots = (1..=3u64)
            .map(|segment_id| {
                let labels = vec![Label::new("host", format!("budget-{segment_id}"))];
                let series_id =
                    resolve_one_series("catalog_series_budget_metric", &labels, &registry);
                write_numeric_segment(
                    &lane_path,
                    &registry,
                    series_id,
                    segment_id,
                    &[(segment_id as i64, segment_id as f64)],
                )
            })
            .collect::<Vec<_>>();

        // 预算给 1 字节：任何真实 series 元数据都超限 → 必须触发淘汰。
        let cache = CatalogEntryCache::with_capacity(0, 1);
        let loaded = roots
            .iter()
            .map(|root| cache.get_or_load_series(root).unwrap())
            .collect::<Vec<_>>();
        assert!(loaded.iter().all(|series| series.len() == 1));

        assert!(
            cache.series_len() < roots.len(),
            "series cache must evict beyond the byte budget (len = {})",
            cache.series_len()
        );
        assert!(
            !cache.series_contains(&roots[0]),
            "oldest root must be evicted first when over budget"
        );

        let reloaded = cache.get_or_load_series(&roots[0]).unwrap();
        assert_series_lists_equal(&reloaded, &loaded[0]);
    }
}
