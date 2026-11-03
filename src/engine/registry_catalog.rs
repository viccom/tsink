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

/// W3-T4：catalog 增量目录与合并标记（与 checkpoint 同目录）。
/// 目录内文件：`{seq:020}.delta.json`（追加）+ `_checkpoint.json`（seq 锚点）。
const REGISTRY_CATALOG_DELTA_DIR_NAME: &str = "series_index.catalog.d";
const REGISTRY_CATALOG_DELTA_MARKER_NAME: &str = "_checkpoint.json";
/// delta 文件与 `_checkpoint.json` 各自携带的版本；读取时校验，不符即回退重建。
const REGISTRY_CATALOG_DELTA_FILE_VERSION: u32 = 2;

/// catalog checkpoint 旁的增量目录：`<snapshot 目录>/series_index.catalog.d`。
pub(super) fn catalog_delta_dir_path(snapshot_path: &Path) -> PathBuf {
    catalog_path(snapshot_path)
        .parent()
        .map(|parent| parent.join(REGISTRY_CATALOG_DELTA_DIR_NAME))
        .unwrap_or_else(|| PathBuf::from(REGISTRY_CATALOG_DELTA_DIR_NAME))
}

/// 增量目录内的合并标记：checkpoint_seq 之上的 delta 已被吸收进主文件。
pub(super) fn catalog_checkpoint_marker_path(snapshot_path: &Path) -> PathBuf {
    catalog_delta_dir_path(snapshot_path).join(REGISTRY_CATALOG_DELTA_MARKER_NAME)
}

/// 一次 catalog 增量落盘的内容（与 checkpoint 同目录的 `{seq:020}.delta.json`）。
/// `added` 为新增段的完整 entry（复用 checkpoint 条目序列化）；`removed` 为被
/// 合并/删除段根的相对路径字符串（口径与 compaction marker 的
/// `segment_rel_path` 一致：相对 checkpoint 所在目录，外部根退化为完整路径，
/// 重放侧按 `segments/L{level}/seg-{id}` 后缀解析）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogDeltaFile {
    version: u32,
    seq: u64,
    #[serde(default)]
    added: Vec<PersistedRegistryCatalogEntry>,
    #[serde(default)]
    removed: Vec<String>,
    /// 写 delta 时按 series 缓存重算的全量指纹；重放取最后一个已应用 delta 的
    /// 该字段作为启动快路径指纹。None = 写入时无法重算 → 快路径关闭。
    #[serde(default)]
    series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
}

/// `_checkpoint.json`：盘上唯一的 seq 锚点——`checkpoint_seq`（含）之前的变更
/// 已全部吸收进 catalog 主文件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegistryCatalogCheckpointMarker {
    version: u32,
    checkpoint_seq: u64,
}

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
    /// 启动重放实际应用的 delta 数（W3-T4 观测：`catalog_delta_replayed_total`）。
    pub(super) deltas_replayed: u64,
    /// 已应用 delta 的指纹缺失/无法重算 → 快路径关闭（W3-T4 观测：
    /// `catalog_delta_fingerprint_fallback_total`）。
    pub(super) fingerprint_fallback: bool,
}

/// 启动期 catalog delta 重放统计（bootstrap 阶段实例尚未建立，经
/// `StartupFinalizeState` 播种进本实例的 FlushObservabilityCounters）。
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct CatalogDeltaStartupStats {
    pub(super) replayed_total: u64,
    pub(super) apply_fallback_total: u64,
    pub(super) fingerprint_fallback_total: u64,
}

/// 启动校验的 delta 侧报告：`validated` 之外的字段供 recovery 区分
/// 「无 delta 的普通失配」与「delta 断层/损坏触发的回退」。
pub(super) struct CatalogDeltaValidationReport {
    pub(super) validated: Option<ValidatedRegistryCatalog>,
    pub(super) deltas_present: bool,
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
    /// count `capacity - capacity/4` 与 bytes `budget - budget/4` 两个水位
    /// 收敛；摊还 O(1)/插入。水位取**向上取整的 3/4**（`x - x/4` 恒等于
    /// `ceil(3x/4)`），不用 `x/4*3`（先除后乘，非 4 倍数容量下偏低、最多少
    /// 2 条 → 过度淘汰），也不用 `x*3/4` 的饱和乘法（`usize::MAX` 时饱和到
    /// 4.6e18 而非真实的 1.38e19，同样过度淘汰）；`x - x/4` 对任意 `x` 无溢出。
    /// 任何时刻至少保留 1 条（touch 最新的刚插入条目不淘汰）——
    /// 单条目超预算时宁可越界也不全清，全清会让下次读取必然 miss，重演
    /// N9 修掉的重读风暴。
    fn evict_over_limit(&self, slots: &mut HashMap<PathBuf, CatalogCacheSlot<V>>) {
        let count_over = self.capacity > 0 && slots.len() > self.capacity;
        let bytes_over =
            self.budget_bytes > 0 && self.used_bytes.load(Ordering::Relaxed) > self.budget_bytes;
        if !count_over && !bytes_over {
            return;
        }
        let count_water_mark = self.capacity - self.capacity / 4;
        let bytes_water_mark = self.budget_bytes - self.budget_bytes / 4;
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

    /// `(累计淘汰条数, entries map 常驻槽位, series map 常驻槽位)`。
    ///
    /// 淘汰计数取双 map 合计（运维关心总淘汰量）；常驻槽位**必须分开报**：
    /// entries 受条数容量约束（`with_catalog_entry_cache_capacity`），series 受
    /// 字节预算约束、条数无上限——合计值套 entries-only 容量判据会报假的
    /// 「缓存越界」（W2-T2 soak 判据曾因此绿得偶然）。
    pub(super) fn observability_totals(&self) -> (u64, u64, u64) {
        (
            self.entries.evictions_total.load(Ordering::Relaxed)
                + self.series.evictions_total.load(Ordering::Relaxed),
            self.entries.resident.load(Ordering::Relaxed),
            self.series.resident.load(Ordering::Relaxed),
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
///
/// W3-T4 增量扩展：
/// - `next_seq`：下一笔变更的 delta 序号；盘上锚点 = `_checkpoint.json` 的
///   `checkpoint_seq`，构造时 `next_seq = max(checkpoint_seq, max_delta_seq) + 1`；
/// - `last_roots`：上次持久化（delta 追加或 checkpoint 重写）认定的 root 集合，
///   checkpoint 全量重建时做漂移对账（捕获未被管线上报的 root 变化）；
/// - `deltas_since_checkpoint` / `delta_bytes_since_checkpoint`：合并阈值计数；
/// - `pending_changes`：delta 写盘失败时保留、待下一拍重试落盘的 root 变更
///   （后写覆盖先写）；非空即 catalog 脏。
pub(super) struct CatalogPersistMemo {
    state: Mutex<CatalogPersistMemoState>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CatalogRootChangeOp {
    Added,
    Removed,
}

impl CatalogPersistMemo {
    /// 从盘面恢复 delta 序号锚点（构造期一次性 IO）。`snapshot_path` 缺失
    /// （ComputeOnly / 无持久化）时给默认态，永不参与 delta 路径。
    pub(super) fn from_disk(snapshot_path: Option<&Path>) -> Self {
        let mut state = CatalogPersistMemoState::default();
        if let Some(snapshot_path) = snapshot_path {
            let checkpoint_seq = read_catalog_checkpoint_marker_seq(snapshot_path).unwrap_or(0);
            let max_delta_seq = list_catalog_delta_files(snapshot_path)
                .last()
                .map(|(seq, _)| *seq)
                .unwrap_or(0);
            state.next_seq = checkpoint_seq.max(max_delta_seq).saturating_add(1);
        }
        Self {
            state: Mutex::new(state),
        }
    }
}

#[derive(Clone)]
struct CatalogPersistMemoState {
    last_persisted_at: Option<Instant>,
    last_entries_hash: Option<u64>,
    last_file_len: Option<u64>,
    /// 下一笔变更的 delta 序号（含被 checkpoint 直接吸收的那一笔）。
    next_seq: u64,
    /// 上次持久化认定的 root 集合：root → (lane, entry 或 None=已消失)。
    /// checkpoint 全量重建时做漂移对账。
    last_roots: BTreeMap<PathBuf, (SegmentLaneFamily, Option<PersistedRegistryCatalogEntry>)>,
    /// `_checkpoint.json` 锚点之后已落盘的 delta 数/字节（合并阈值计数）。
    deltas_since_checkpoint: u64,
    delta_bytes_since_checkpoint: u64,
    /// delta 写失败时保留、待下一拍重试落盘的 root 变更（后写覆盖先写）。
    pending_changes: BTreeMap<PathBuf, CatalogRootChangeOp>,
}

impl Default for CatalogPersistMemoState {
    fn default() -> Self {
        Self {
            last_persisted_at: None,
            last_entries_hash: None,
            last_file_len: None,
            // marker 缺失 ⇒ checkpoint_seq=0 ⇒ 首笔变更序号 1（0 会撞上
            // 「seq > checkpoint_seq 才重放」的启动过滤）。
            next_seq: 1,
            last_roots: BTreeMap::new(),
            deltas_since_checkpoint: 0,
            delta_bytes_since_checkpoint: 0,
            pending_changes: BTreeMap::new(),
        }
    }
}

/// 一次 catalog 持久化的结果（调用侧用于计数器归因）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CatalogPersistOutcome {
    /// 全量重写 catalog 主文件（冷路径 / 关闭锚点）。
    Persisted,
    /// 例行发布拍：delta 追加落盘（携带序列化字节数）。
    DeltaAppended { bytes: u64 },
    /// delta 数/字节达阈值：本笔变更直接并入全量 checkpoint（主文件 + 标记 +
    /// delta 清理）。
    CheckpointConsolidated,
    /// 节流窗口内：不构建、不读盘、不写盘；变更并入备忘待下一拍。
    SkippedThrottled,
    /// 内容与上次写入一致且文件未被外部改动（备忘命中，零 IO）。
    SkippedUnchanged,
    /// 冷启动/备忘失效：读文件做 entries 比对，命中即采纳文件指纹（零重写，
    /// 且不读取 segment 的 series.bin——保持「catalog 有效即可恢复」语义）。
    SkippedValidatedFromFile,
    /// catalog 写盘失败（delta 追加或阈值合并）：catalog 已标脏（下拍重试/
    /// 强制 checkpoint），不使 flush 失败（catalog 是派生缓存）。
    DeltaWriteFailed,
}

/// 调用方随 sources 一并上报的本拍 root 变更（W3-T4：例行 persist 由管线
/// 变更直接构造 delta，不做全量构建求差）。
#[derive(Debug, Clone, Copy)]
pub(super) struct CatalogRootChanges<'a> {
    pub(super) added_roots: &'a [PathBuf],
    pub(super) removed_roots: &'a [PathBuf],
}

/// 按「节流窗口 + 内容指纹」持久化 catalog（P1 / W3-T4 delta 化）。
///
/// 三种模式：
/// - `force=true`：checkpoint/关闭锚点——全量重建主文件 + 写/更新
///   `_checkpoint.json` + 清 delta（无视节流）；
/// - `root_changes=None`（冷路径，无变更信息）：旧行为——节流 + 内容指纹去重
///   的全量持久化（启动 finalize / 全量扫描发布仍走这里）；
/// - `root_changes=Some`（例行发布拍）：由管线上报的 root 变更构造增量——
///   热路径 O(变更)；delta 数/字节达阈值时整笔并入全量 checkpoint。
///
/// `force` 优先于 `root_changes`：强制刷新即全量，变更信息无需消费。
#[allow(clippy::too_many_arguments)]
pub(super) fn persist_registry_catalog_throttled(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    root_changes: Option<CatalogRootChanges<'_>>,
    entry_cache: &CatalogEntryCache,
    memo: &CatalogPersistMemo,
    min_interval: Duration,
    delta_max_count: usize,
    delta_max_bytes: u64,
    force: bool,
) -> Result<(
    Option<PersistedRegistrySeriesFingerprint>,
    CatalogPersistOutcome,
)> {
    let now = Instant::now();
    // 主文件缺失 ⇒ 无锚定基线，delta 无从重放：首拍直接全量写 checkpoint
    // （无 builder 引导的实例没有 finalize 写底，首个发布拍即首次持久化）。
    let main_file_present = path_exists_no_follow(&catalog_path(snapshot_path)).unwrap_or(false);
    if force || root_changes.is_none() || !main_file_present {
        return persist_registry_catalog_checkpoint(
            snapshot_path,
            sources,
            entry_cache,
            memo,
            now,
            min_interval,
            force,
            None,
            None,
        );
    }
    persist_registry_catalog_delta(
        snapshot_path,
        sources,
        root_changes.expect("root changes checked above"),
        entry_cache,
        memo,
        now,
        delta_max_count,
        delta_max_bytes,
    )
}

/// 冷路径 / 强制锚点：全量重建 entries 并重写主文件（内容指纹去重语义保留），
/// 主文件写成功后把 delta 锚点推进到「全部已吸收」（marker + 残留 delta 清理）。
///
/// `absorbed_checkpoint_seq`：合并路径传入本笔变更应占用的 seq（marker 直接
/// 记它）；`None` = 冷路径/强制锚点，吸收到 `next_seq - 1`。
#[allow(clippy::too_many_arguments)]
fn persist_registry_catalog_checkpoint(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
    memo: &CatalogPersistMemo,
    now: Instant,
    min_interval: Duration,
    force: bool,
    absorbed_checkpoint_seq: Option<u64>,
    absorbed_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
) -> Result<(
    Option<PersistedRegistrySeriesFingerprint>,
    CatalogPersistOutcome,
)> {
    let path = catalog_path(snapshot_path);
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

    // 冷启动 / 跨窗口：主文件 + 未合并 delta 重构后与 expected 比对——与启动
    // 校验同一实现（`validate_catalog_checkpoint_with_deltas`），不残留「只读
    // checkpoint 本体」的旧语义。命中即**原地验证**：不重写主文件、不删 delta、
    // 不推 marker（携带未合并 delta 的重启不再付一次 O(段数) 全量重写），并直接
    // 采纳重构视图的 series_fingerprint ——不读任何 segment 的 series.bin
    // （catalog 有效即足以恢复，见
    // `startup_reopens_from_registry_catalog_without_segment_series_files`）。
    //
    // 仅冷路径（`!force`）适用：`force` 是关闭/显式 checkpoint 锚点，契约是
    // 「主文件即全量真值 + delta 清空 + marker 前移」，原地验证会留下未合并
    // delta 与旧 marker，锚点语义回退。
    if !force {
        if let Ok(bytes) = std::fs::read(&path) {
            if let Ok(actual) = serde_json::from_slice::<PersistedRegistryCatalogFile>(&bytes) {
                let deltas = list_catalog_delta_files(snapshot_path);
                if let Some(validated) = validate_catalog_checkpoint_with_deltas(
                    snapshot_path,
                    actual,
                    &deltas,
                    &expected_entries,
                ) {
                    let mut state = memo.state.lock();
                    state.last_persisted_at = Some(now);
                    // hash 取重构后（= expected）的条目指纹，len 必须取**主文件
                    // 实际字节长度**（主文件本身未重写）：二者配成「内容指纹 +
                    // 文件未被外部改动」的判据，下一拍才不会被误判成
                    // SkippedUnchanged。
                    state.last_entries_hash = Some(entries_hash);
                    state.last_file_len = Some(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                    // 主文件 + delta 已共同反映全部变更：备忘里的 pending delta
                    // 变更视为已吸收（重构失配时才会走到下面的全量重写）。
                    state.pending_changes.clear();
                    return Ok((
                        validated.series_fingerprint,
                        CatalogPersistOutcome::SkippedValidatedFromFile,
                    ));
                }
            }
        }
    }

    let fingerprint = match absorbed_fingerprint {
        Some(fingerprint) => Some(fingerprint),
        None => Some(build_series_fingerprint_from_sources(sources, entry_cache)?),
    };
    // 重置判据（须在主文件重写前采样）：重写前的主文件不承载任何段条目
    // （缺失/损坏/空）⇒ 本实例从未把段集全量同步到盘，delta 序号空间没有
    // 可延续的盘上锚点基线 ⇒ close 强制 checkpoint 吸收后整体重置增量状态
    // （下一会话从 seq=1 重新起算，主文件即完整真值）。主文件已承载段集
    // ⇒ 常规合并：marker 锚点跨 close/reopen 存活（W3-T2 契约）。
    let main_carries_segments = std::fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<PersistedRegistryCatalogFile>(&bytes).ok())
        .map(|file| !file.segments.is_empty())
        .unwrap_or(false);
    let catalog = PersistedRegistryCatalogFile {
        version: REGISTRY_CATALOG_VERSION,
        segments: expected_entries,
        series_fingerprint: fingerprint.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&catalog)?;
    write_file_atomically_and_sync_parent(&path, &bytes)?;

    // 主文件已吸收锚点之前的全部变更：按重置判据处置增量状态。marker 写在
    // 主文件之后——中途崩溃留下的「新主文件 + 旧 marker + 旧 delta」由启动
    // 重放的幂等 upsert 吸收，不会误判断层。
    // - 强制锚点 + 主文件此前无段集：整体重置——目录（含 marker）删除；
    // - 合并路径（absorbed_checkpoint_seq=Some）或盘上已有增量状态：marker
    //   推进到「全部已吸收」并清残留 delta 文件（目录与锚点保留，后续 delta
    //   序号从 checkpoint_seq+1 续起）；
    // - 无增量状态：不创建目录、不写 marker。
    let reset_delta_state = force && absorbed_checkpoint_seq.is_none() && !main_carries_segments;
    let checkpoint_seq = {
        let mut state = memo.state.lock();
        let checkpoint_seq =
            absorbed_checkpoint_seq.unwrap_or_else(|| state.next_seq.saturating_sub(1));
        let delta_state_present = !list_catalog_delta_files(snapshot_path).is_empty()
            || catalog_checkpoint_marker_path(snapshot_path).is_file()
            || !state.pending_changes.is_empty();
        if reset_delta_state {
            remove_catalog_delta_dir(snapshot_path);
            state.next_seq = 1;
        } else if absorbed_checkpoint_seq.is_some() || delta_state_present {
            // 先删 delta 文件再写 marker：marker 可见即意味着残留 delta 已清
            // （测试轮询 marker 后立即对账目录，两步不可倒置）。顺序对崩溃安全
            // 无影响——主文件此刻已重写为全量真值，任何崩溃点的盘面要么自洽、
            // 要么由启动重放/回退重建兜底。
            remove_catalog_delta_files(snapshot_path);
            write_catalog_checkpoint_marker(snapshot_path, checkpoint_seq)?;
            state.next_seq = checkpoint_seq.saturating_add(1);
        }
        reconcile_last_roots(&mut state, snapshot_path, sources, entry_cache);
        state.deltas_since_checkpoint = 0;
        state.delta_bytes_since_checkpoint = 0;
        state.pending_changes.clear();
        state.last_persisted_at = Some(now);
        state.last_entries_hash = Some(entries_hash);
        state.last_file_len = Some(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        checkpoint_seq
    };
    tracing::debug!(
        checkpoint_seq,
        reset_delta_state,
        "Registry catalog checkpoint absorbed all pending catalog deltas"
    );
    Ok((fingerprint, CatalogPersistOutcome::Persisted))
}

/// 例行发布拍（W3-T4）：由管线上报的 root 变更直接构造 delta。
///
/// 不做节流：delta 追加是 O(变更)（added 条目走段级缓存、指纹走 series 缓存），
/// `catalog_persist_interval` 节流的本意是限流 O(段数) 的全量重建（保留在冷
/// 路径与合并/强制锚点）；且发布拍只在真正产段时出现——被节流吞掉的变更可能
/// 没有下一拍可重试，delta 必须随拍落盘。
///
/// 顺序纪律：段 rename + 目录 fsync 已由发布管线完成 → delta 原子写（tmp +
/// rename + 父目录 fsync）→ 返回成功。delta 追加**或阈值合并**的写失败都只标脏
/// （下拍重试/达阈值即再试合并），不使 flush 失败——catalog 是派生缓存。close /
/// 显式锚点的强制 checkpoint 不走这里，仍按真实失败上抛。
#[allow(clippy::too_many_arguments)]
fn persist_registry_catalog_delta(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    root_changes: CatalogRootChanges<'_>,
    entry_cache: &CatalogEntryCache,
    memo: &CatalogPersistMemo,
    now: Instant,
    delta_max_count: usize,
    delta_max_bytes: u64,
) -> Result<(
    Option<PersistedRegistrySeriesFingerprint>,
    CatalogPersistOutcome,
)> {
    // 合并备忘里写失败残留的变更与本拍变更（同 root 后写覆盖先写：发布拍
    // 的时间序保证 current 晚于 pending）。
    let merged_changes = {
        let state = memo.state.lock();
        let mut merged = state.pending_changes.clone();
        for root in root_changes.added_roots {
            merged.insert(root.clone(), CatalogRootChangeOp::Added);
        }
        for root in root_changes.removed_roots {
            merged.insert(root.clone(), CatalogRootChangeOp::Removed);
        }
        merged
    };

    if merged_changes.is_empty() {
        let mut state = memo.state.lock();
        state.last_persisted_at = Some(now);
        return Ok((None, CatalogPersistOutcome::SkippedUnchanged));
    }

    // added 条目只为新增 root 经 entry_cache 构建（O(变更)），removed 为根
    // 路径列表；指纹按 series 缓存全量重算（O(series) CPU，热缓存零 IO）。
    let sources_by_root = sources
        .iter()
        .map(|source| (source.root.as_path(), source))
        .collect::<BTreeMap<_, _>>();
    let mut added_entries = Vec::new();
    let mut added_root_entries = BTreeMap::new();
    for (root, op) in &merged_changes {
        if *op != CatalogRootChangeOp::Added {
            continue;
        }
        let Some(source) = sources_by_root.get(root.as_path()) else {
            warn!(
                root = %root.display(),
                "Catalog delta: reported added root is absent from published sources; the next checkpoint reconciles it"
            );
            continue;
        };
        let entry = entry_cache.get_or_build(source)?;
        if entry.is_none() {
            warn!(
                root = %root.display(),
                "Catalog delta: added root disappeared before its entry could be built; the next checkpoint reconciles it"
            );
        }
        added_entries.extend(entry.iter().cloned());
        added_root_entries.insert(root.clone(), (source.lane, entry));
    }
    let removed_paths = merged_changes
        .iter()
        .filter(|(_, op)| **op == CatalogRootChangeOp::Removed)
        .map(|(root, _)| catalog_delta_removed_path(snapshot_path, root))
        .collect::<Vec<_>>();

    // 指纹构建失败不阻断 delta（结构正确性优先）：写 None，启动按「指纹缺失」
    // 关闭快路径（回退读 series.bin，正确性不变）。
    let series_fingerprint = match build_series_fingerprint_from_sources(sources, entry_cache) {
        Ok(fingerprint) => Some(fingerprint),
        Err(err) => {
            warn!(
                error = %err,
                "Catalog delta: failed to recompute the series fingerprint; startup fast path will be disabled"
            );
            None
        }
    };

    let seq = memo.state.lock().next_seq;
    let delta = PersistedRegistryCatalogDeltaFile {
        version: REGISTRY_CATALOG_DELTA_FILE_VERSION,
        seq,
        added: added_entries,
        removed: removed_paths,
        series_fingerprint: series_fingerprint.clone(),
    };
    let bytes = serde_json::to_vec(&delta)?;
    let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);

    // 合并阈值（笔数或字节）任一达到 → 本笔变更直接并入全量 checkpoint：
    // checkpoint_seq 取本笔变更的序号（= delta 本应占用的 seq），落盘后不再
    // 产生该笔的 delta 文件。
    let threshold_reached = {
        let state = memo.state.lock();
        state.deltas_since_checkpoint.saturating_add(1) >= delta_max_count.max(1) as u64
            || state.delta_bytes_since_checkpoint.saturating_add(bytes_len) > delta_max_bytes
    };
    if threshold_reached {
        // 合并支路的写失败与 delta 追加失败同一口径：catalog 是派生缓存（启动可从
        // 段目录重建；WAL 前缀回收由 flush 的 durability 水位驱动，不依赖 catalog），
        // 合并失败只标脏下拍重试，不使 flush 失败。此处传入的 `force=true` 是
        // 「合并即全量、无视节流」语义（吸收 absorbed_checkpoint_seq=Some），
        // 不是 close/显式锚点的强制刷新——后者走 `persist_registry_catalog_throttled`
        // 的 force 分支，仍按真实失败上抛（关停时无法持久化必须报错）。
        return match persist_registry_catalog_checkpoint(
            snapshot_path,
            sources,
            entry_cache,
            memo,
            now,
            Duration::ZERO,
            true,
            Some(seq),
            series_fingerprint,
        ) {
            Ok((fingerprint, _)) => {
                Ok((fingerprint, CatalogPersistOutcome::CheckpointConsolidated))
            }
            Err(err) => {
                warn!(
                    error = %err,
                    seq,
                    "Catalog checkpoint consolidation failed; marking the catalog dirty for the next publish beat"
                );
                let mut state = memo.state.lock();
                state.pending_changes = merged_changes;
                Ok((None, CatalogPersistOutcome::DeltaWriteFailed))
            }
        };
    }

    if let Err(err) = append_catalog_delta_file(snapshot_path, seq, &bytes) {
        warn!(
            error = %err,
            seq,
            "Catalog delta append failed; marking the catalog dirty for the next publish beat"
        );
        let mut state = memo.state.lock();
        state.pending_changes = merged_changes;
        return Ok((None, CatalogPersistOutcome::DeltaWriteFailed));
    }

    {
        let mut state = memo.state.lock();
        state.next_seq = seq.saturating_add(1);
        for (root, (lane, entry)) in added_root_entries {
            state.last_roots.insert(root, (lane, entry));
        }
        for (root, op) in &merged_changes {
            if *op == CatalogRootChangeOp::Removed {
                state.last_roots.remove(root);
            }
        }
        state.deltas_since_checkpoint = state.deltas_since_checkpoint.saturating_add(1);
        state.delta_bytes_since_checkpoint =
            state.delta_bytes_since_checkpoint.saturating_add(bytes_len);
        state.pending_changes.clear();
        state.last_persisted_at = Some(now);
        // 主文件相对 sources 已过期：冷路径去重必须退到文件比对/重写。
        state.last_entries_hash = None;
    }
    Ok((
        series_fingerprint,
        CatalogPersistOutcome::DeltaAppended { bytes: bytes_len },
    ))
}

/// checkpoint 吸收后的漂移对账：以 sources 全量重建 last_roots（entries 已由
/// 缓存命中复用），集合与上次认定不一致即说明存在未被管线上报的 root 变化
/// （如外部删目录、上次 delta 写失败）——全量重写本身即纠正，这里补告警。
fn reconcile_last_roots(
    state: &mut CatalogPersistMemoState,
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
) {
    let mut next_roots = BTreeMap::new();
    for source in sources {
        let entry = entry_cache.get_or_build(source).ok().flatten();
        next_roots.insert(source.root.clone(), (source.lane, entry));
    }
    if !state.last_roots.is_empty() && state.last_roots.keys().ne(next_roots.keys()) {
        let removed: Vec<&PathBuf> = state.last_roots.keys().collect();
        let added: Vec<&PathBuf> = next_roots.keys().collect();
        warn!(
            snapshot = %snapshot_path.display(),
            previous_roots = removed.len(),
            current_roots = added.len(),
            "Registry catalog checkpoint reconciled root drift (changes the publish pipeline did not report)"
        );
    }
    state.last_roots = next_roots;
}

/// removed 根的落盘口径：相对 checkpoint 所在目录（与 compaction marker 的
/// `segment_rel_path` 一致）；根在目录之外（tiered/镜像根）时退化为完整路径，
/// 重放侧按 `segments/L{level}/seg-{id}` 后缀解析身份。
fn catalog_delta_removed_path(snapshot_path: &Path, root: &Path) -> String {
    let base = catalog_path(snapshot_path)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    root.strip_prefix(&base)
        .map(|relative| relative.to_string_lossy().into_owned())
        .unwrap_or_else(|_| root.to_string_lossy().into_owned())
}

/// 从段根路径解析重放所需的段身份 `(level, segment_id)`。segment_id 由全局
/// 单调计数器分配、跨 lane 唯一，故 (level, segment_id) 足以定位条目。
fn parse_segment_identity_from_root_str(root: &str) -> Option<(u8, u64)> {
    let mut components = Path::new(root).components().rev();
    let segment_component = components.next()?.as_os_str().to_str()?;
    let segment_id = u64::from_str_radix(segment_component.strip_prefix("seg-")?, 16).ok()?;
    let level_component = components.next()?.as_os_str().to_str()?;
    let level = level_component.strip_prefix('L')?.parse::<u8>().ok()?;
    Some((level, segment_id))
}

/// 列出 delta 目录下按 seq 升序的 delta 文件；不可解析/临时残留文件忽略。
fn list_catalog_delta_files(snapshot_path: &Path) -> Vec<(u64, PathBuf)> {
    let dir = catalog_delta_dir_path(snapshot_path);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut deltas = Vec::new();
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(seq_text) = name.strip_suffix(".delta.json") else {
            continue;
        };
        if seq_text.is_empty() || !seq_text.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        if let Ok(seq) = seq_text.parse::<u64>() {
            deltas.push((seq, entry.path()));
        }
    }
    deltas.sort_by_key(|(seq, _)| *seq);
    deltas
}

/// delta 目录内现存 delta 文件数（启动回退计数用：有 delta 而校验失败才算
/// delta 引发的回退）。
pub(super) fn count_catalog_delta_files(snapshot_path: &Path) -> usize {
    list_catalog_delta_files(snapshot_path).len()
}

fn read_catalog_checkpoint_marker_seq(snapshot_path: &Path) -> Option<u64> {
    let bytes = std::fs::read(catalog_checkpoint_marker_path(snapshot_path)).ok()?;
    let marker: PersistedRegistryCatalogCheckpointMarker = serde_json::from_slice(&bytes).ok()?;
    if marker.version != REGISTRY_CATALOG_DELTA_FILE_VERSION {
        return None;
    }
    Some(marker.checkpoint_seq)
}

/// 写/更新 `_checkpoint.json`（原子写 + 父目录 fsync）。delta 目录不存在时先建
/// 并同步其父目录（保证目录项本身持久）。
fn write_catalog_checkpoint_marker(snapshot_path: &Path, checkpoint_seq: u64) -> Result<()> {
    let dir = catalog_delta_dir_path(snapshot_path);
    if !dir.is_dir() {
        std::fs::create_dir_all(&dir).map_err(|source| TsinkError::IoWithPath {
            path: dir.clone(),
            source,
        })?;
        crate::engine::fs_utils::sync_parent_dir(&dir)?;
    }
    let marker = PersistedRegistryCatalogCheckpointMarker {
        version: REGISTRY_CATALOG_DELTA_FILE_VERSION,
        checkpoint_seq,
    };
    let bytes = serde_json::to_vec_pretty(&marker)?;
    write_file_atomically_and_sync_parent(&catalog_checkpoint_marker_path(snapshot_path), &bytes)
}

/// 原子追加一笔 delta。
///
/// 落位顺序刻意安排：tmp 文件（含内容 fsync）先写入数据目录，再建增量目录、
/// 跨目录 rename 落位、同步两个目录——把「增量目录已可见但首个 delta 尚未
/// 落位」的窗口从 fsync 量级压缩到 rename 的微秒级（外部观察者以目录存在性
/// 判定 delta 到达，窗口过大轮询方会读到空目录）。
fn append_catalog_delta_file(snapshot_path: &Path, seq: u64, bytes: &[u8]) -> Result<()> {
    let dir = catalog_delta_dir_path(snapshot_path);
    let path = dir.join(format!("{seq:020}.delta.json"));
    // tmp 锚在数据目录（增量目录的父目录）：内容写盘并 fsync 后才建目录。
    let staging_anchor = catalog_path(snapshot_path).with_file_name("catalog-delta-staging");
    let tmp_path = crate::engine::fs_utils::write_tmp_and_sync(&staging_anchor, bytes)?;
    if !dir.is_dir() {
        std::fs::create_dir_all(&dir).map_err(|source| TsinkError::IoWithPath {
            path: dir.clone(),
            source,
        })?;
    }
    if let Err(err) = crate::engine::fs_utils::rename_tmp(&tmp_path, &path) {
        let _ = crate::engine::fs_utils::remove_file_if_exists(&tmp_path);
        return Err(err);
    }
    // 持久化语义（W3-T4 复核）：内容 fsync 在 write_tmp_and_sync 内完成；rename
    // 后对增量目录 fsync 使目录项持久。两次 fsync 刻意保持——journal 提交频率
    // 下调会放大单次事务的回写窗口（8 实例并行拍下实测 fsync 尖峰秒级化）。
    // SIGKILL 场景页缓存存活、目录项必然可见；掉电场景目录项丢失 → 启动重放
    // 按序号断层走回退重建（段目录扫描兜底，正确性不变）。
    crate::engine::fs_utils::sync_parent_dir(&path)?;
    Ok(())
}

/// 删除 delta 目录内全部 delta 文件（保留目录与 `_checkpoint.json`——测试
/// 契约要求目录在 close 合并后仍存在）。
fn remove_catalog_delta_files(snapshot_path: &Path) {
    for (_, path) in list_catalog_delta_files(snapshot_path) {
        if let Err(err) = std::fs::remove_file(&path) {
            warn!(
                path = %path.display(),
                error = %err,
                "Failed to remove consolidated catalog delta file"
            );
        }
    }
}

/// 活跃 close 的整体重置：删除整个增量目录（delta 文件 + marker）。目录缺失
/// 视为已重置；删除失败只告警——残留状态由启动重放/回退兜底，不使 close 失败。
fn remove_catalog_delta_dir(snapshot_path: &Path) {
    let dir = catalog_delta_dir_path(snapshot_path);
    if let Err(err) = crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&dir) {
        warn!(
            path = %dir.display(),
            error = %err,
            "Failed to remove the catalog delta directory during close reset"
        );
    }
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

/// 兼容包装：只取校验结论（tests / 既有调用方）。
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn validate_registry_catalog(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
) -> Result<Option<ValidatedRegistryCatalog>> {
    Ok(validate_registry_catalog_with_delta_report(snapshot_path, sources, entry_cache)?.validated)
}

/// 启动校验（W3-T4）：actual 不再只读 checkpoint 主文件，而是「主文件条目 +
/// 依次应用 seq > checkpoint_seq 的 delta（幂等 upsert/remove）」重构后与
/// expected（段目录全量扫描）比对——验证强度不降。
///
/// 回退决策树（一律返回 `validated: None`，由 recovery 走既有的「失配即重建
/// + 强制 checkpoint」路径，残留 delta 由强制 checkpoint 清理）：
/// - 主文件缺失/解析失败：`Err`（保持既有语义）；
/// - marker 损坏/版本不符、任一 delta 解析失败/版本不符/seq 与文件名不符、
///   已应用 delta 序号断层（未从 checkpoint_seq+1 连续起算）：`Ok(None)`；
/// - 重构结果与 expected 失配：`Ok(None)`。
///
/// 指纹取最后一个已应用 delta 的 `series_fingerprint`（无已应用 delta 则取
/// checkpoint 的）；缺失即 `fingerprint_fallback`，快路径关闭（正确性不变）。
pub(super) fn validate_registry_catalog_with_delta_report(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    entry_cache: &CatalogEntryCache,
) -> Result<CatalogDeltaValidationReport> {
    let path = catalog_path(snapshot_path);
    let deltas = list_catalog_delta_files(snapshot_path);
    if !path_exists_no_follow(&path).map_err(|source| TsinkError::IoWithPath {
        path: path.clone(),
        source,
    })? {
        return Ok(CatalogDeltaValidationReport {
            validated: None,
            deltas_present: !deltas.is_empty(),
        });
    }

    let expected_segments = build_catalog_entries(sources, entry_cache)?;
    let actual = serde_json::from_slice::<PersistedRegistryCatalogFile>(
        &std::fs::read(&path).map_err(|source| TsinkError::IoWithPath {
            path: path.clone(),
            source,
        })?,
    )?;

    let report = CatalogDeltaValidationReport {
        validated: validate_catalog_checkpoint_with_deltas(
            snapshot_path,
            actual,
            &deltas,
            &expected_segments,
        ),
        deltas_present: !deltas.is_empty(),
    };
    Ok(report)
}

/// 校验核心：把「checkpoint 主文件条目 + 依次应用 seq > checkpoint_seq 的 delta」
/// 重构后与 `expected_segments`（调用方已构建的段目录真值）比对。
///
/// 启动校验与冷路径 persist 共用这一份实现（两处口径必须一致：冷路径原地验证
/// 通过的盘面，重开后启动校验也必须通过）。
///
/// 返回 `None` = 不可采纳（调用方各自回退）：
/// - marker 损坏/版本不符、任一 delta 解析失败/版本不符/seq 与文件名不符、
///   已应用 delta 序号断层（未从 checkpoint_seq+1 连续起算）；
/// - 重构结果与 `expected_segments` 失配。
fn validate_catalog_checkpoint_with_deltas(
    snapshot_path: &Path,
    actual: PersistedRegistryCatalogFile,
    deltas: &[(u64, PathBuf)],
    expected_segments: &[PersistedRegistryCatalogEntry],
) -> Option<ValidatedRegistryCatalog> {
    let checkpoint_seq = match std::fs::read(catalog_checkpoint_marker_path(snapshot_path)) {
        Ok(bytes) => {
            let marker: PersistedRegistryCatalogCheckpointMarker =
                match serde_json::from_slice(&bytes) {
                    Ok(marker) => marker,
                    Err(_) => return None,
                };
            if marker.version != REGISTRY_CATALOG_DELTA_FILE_VERSION {
                return None;
            }
            marker.checkpoint_seq
        }
        // marker 缺失 → checkpoint_seq = 0，seq ≥ 1 的 delta 仍参与重放
        // （首笔 checkpoint 之前崩溃的实例以此恢复增量）。
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(_) => return None,
    };

    let mut reconstructed = actual.segments;
    // 指纹缺失的判定必须以 checkpoint 自身为起点：旧版本主文件反序列化得到
    // `None`（字段 `#[serde(default)]`），且**无 delta 可重放**时循环一次也不
    // 执行——若把判定只放在循环体内，这条「升级后首次启动」的典型盘面会显示
    // 回退计数 0（而启动确实逐段读了 series.bin）。
    let mut fingerprint_fallback = actual.series_fingerprint.is_none();
    let mut fingerprint = actual.series_fingerprint;
    let mut replayed = 0u64;
    for (offset, (seq, delta_path)) in deltas
        .iter()
        .filter(|(seq, _)| *seq > checkpoint_seq)
        .enumerate()
    {
        // 序号必须从 checkpoint_seq + 1 连续起算：断层意味着有 delta 缺失
        // （「段已 rename、delta 未写」崩溃窗），其上变更不可再补。
        let expected_seq = checkpoint_seq.saturating_add(1 + offset as u64);
        if *seq != expected_seq {
            return None;
        }
        let bytes = match std::fs::read(delta_path) {
            Ok(bytes) => bytes,
            Err(_) => return None,
        };
        let delta: PersistedRegistryCatalogDeltaFile = match serde_json::from_slice(&bytes) {
            Ok(delta) => delta,
            Err(_) => return None,
        };
        if delta.version != REGISTRY_CATALOG_DELTA_FILE_VERSION || delta.seq != *seq {
            return None;
        }
        apply_catalog_delta_to_entries(&delta, &mut reconstructed);
        fingerprint = delta.series_fingerprint;
        fingerprint_fallback = fingerprint.is_none();
        replayed += 1;
    }

    reconstructed.sort_by_key(|entry| (entry.lane, entry.level, entry.segment_id));
    if reconstructed != expected_segments {
        return None;
    }

    Some(ValidatedRegistryCatalog {
        series_fingerprint: fingerprint,
        deltas_replayed: replayed,
        fingerprint_fallback,
    })
}

/// 幂等应用一笔 delta：added 按段身份 (lane, level, segment_id) upsert，
/// removed 按解析出的 (level, segment_id) 摘除（segment_id 全局唯一，跨 lane
/// 不冲突）。无法解析的 removed 路径跳过并告警——只可能造成重构失配（回退
/// 重建），不会造成误验证。
fn apply_catalog_delta_to_entries(
    delta: &PersistedRegistryCatalogDeltaFile,
    entries: &mut Vec<PersistedRegistryCatalogEntry>,
) {
    for added in &delta.added {
        entries.retain(|entry| {
            entry.lane != added.lane
                || entry.level != added.level
                || entry.segment_id != added.segment_id
        });
        entries.push(added.clone());
    }
    for removed in &delta.removed {
        match parse_segment_identity_from_root_str(removed) {
            Some((level, segment_id)) => {
                entries.retain(|entry| entry.level != level || entry.segment_id != segment_id)
            }
            None => warn!(
                removed = %removed,
                "Catalog delta replay: unparseable removed root; the next checkpoint reconciles it"
            ),
        }
    }
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

    /// 造 `capacity + 1` 个真实段并全部插入条数有界缓存（无字节预算），返回
    /// 超限淘汰后的驻留条数。
    fn insert_overflowing_by_count(capacity: usize) -> usize {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join("numeric");
        let registry = SeriesRegistry::new();
        let labels = vec![Label::new("host", format!("watermark-{capacity}"))];
        let series_id = resolve_one_series("catalog_cache_watermark_metric", &labels, &registry);
        let sources = (1..=capacity as u64 + 1)
            .map(|segment_id| {
                numeric_source(write_numeric_segment(
                    &lane_path,
                    &registry,
                    series_id,
                    segment_id,
                    &[(segment_id as i64, segment_id as f64)],
                ))
            })
            .collect::<Vec<_>>();
        let cache = CatalogEntryCache::with_capacity(capacity, 0);
        for source in &sources {
            cache
                .get_or_build(source)
                .unwrap()
                .expect("real segment root must build an entry");
        }
        cache.entries_len()
    }

    /// 目标语义：`observability_totals()` 的 resident 只反映 **entries map**
    /// （受 `with_catalog_entry_cache_capacity` 约束），不得把 series map 的
    /// 常驻槽位（字节预算约束、条数无上限）并进来——否则 soak 判据拿 entries
    /// 容量去卡双 map 合计，只要 series 有任意驻留就报假的「缓存越界」。
    #[test]
    fn catalog_cache_observability_resident_counts_entries_map_only() {
        let cache = CatalogEntryCache::default();
        for i in 0..2u64 {
            cache
                .entries
                .insert(PathBuf::from(format!("/seg/entries-{i}")), None, 0);
        }
        for i in 0..5u64 {
            cache
                .series
                .insert(PathBuf::from(format!("/seg/series-{i}")), Vec::new(), 0);
        }
        assert_eq!(cache.entries_len(), 2);
        assert_eq!(cache.series_len(), 5);

        let (evictions, entries_resident, series_resident) = cache.observability_totals();
        assert_eq!(evictions, 0);
        assert_eq!(
            entries_resident, 2,
            "entries resident gauge must count the entries map only (2), not entries + series (7)"
        );
        assert_eq!(
            series_resident, 5,
            "series slots are reported separately (byte-budget bound, no count cap)"
        );
    }

    /// 淘汰计数保持双 map合计（运维关心总淘汰量，且与「容量越界」语义无关）。
    #[test]
    fn catalog_cache_observability_evictions_count_both_maps() {
        let cache = CatalogEntryCache::with_capacity(2, 1);
        // entries：条数容量 2，插 3 条 → 淘汰 1 条。
        for i in 0..3u64 {
            cache
                .entries
                .insert(PathBuf::from(format!("/seg/entries-{i}")), None, 8);
        }
        // series：字节预算 1，每条 8 字节 → 每次插入都超预算并淘汰。
        for i in 0..3u64 {
            cache
                .series
                .insert(PathBuf::from(format!("/seg/series-{i}")), Vec::new(), 8);
        }

        let entries_evictions = cache.entries.evictions_total.load(Ordering::Relaxed);
        let series_evictions = cache.series.evictions_total.load(Ordering::Relaxed);
        assert!(
            entries_evictions > 0 && series_evictions > 0,
            "both maps must have evicted (entries={entries_evictions}, series={series_evictions})"
        );

        let (evictions, _entries_resident, _series_resident) = cache.observability_totals();
        assert_eq!(
            evictions,
            entries_evictions + series_evictions,
            "evictions_total must stay the sum across both maps"
        );
    }

    /// 淘汰水位必须是 ceil(3/4) = `capacity - capacity/4`：`capacity/4*3`
    /// 先除后乘，在非 4 倍数容量下偏低（最多少 2 条），导致过度淘汰。
    #[test]
    fn catalog_cache_eviction_watermark_is_three_quarters() {
        let mut mismatches = Vec::new();
        for capacity in [5usize, 6, 7, 10, 11] {
            let expected = capacity - capacity / 4;
            let actual = insert_overflowing_by_count(capacity);
            if actual != expected {
                mismatches.push(format!(
                    "capacity={capacity}: expected {expected} (capacity - capacity/4), \
                     got {actual} (capacity/4*3 = {})",
                    capacity / 4 * 3
                ));
            }
        }
        assert!(
            mismatches.is_empty(),
            "eviction must stop at the ceil(3/4) watermark:\n{}",
            mismatches.join("\n")
        );

        // 极小容量：cap=2 的 3/4 水位是 2，插入第 3 条后必须仍留 2 条；
        // `2/4*3 = 0 → max(1)` 会一次性清到只剩 1 条，越过水位。
        let tiny = insert_overflowing_by_count(2);
        assert_eq!(
            tiny, 2,
            "capacity=2 must keep 2 slots (ceil(3/4) watermark), not collapse to 1"
        );
    }
}
