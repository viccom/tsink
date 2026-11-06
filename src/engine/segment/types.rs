use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::engine::chunk::Chunk;
use crate::engine::durability::WalHighWatermark;
use crate::engine::index::ChunkIndex;
use crate::engine::series::{SeriesId, SeriesValueFamily};
use crate::mmap::PlatformMmap;
use crate::Label;

use super::postings::SegmentPostingsIndex;

#[derive(Debug, Clone)]
pub struct SegmentLayout {
    pub root: PathBuf,
    pub chunks_path: PathBuf,
    pub chunk_index_path: PathBuf,
    pub series_path: PathBuf,
    pub postings_path: PathBuf,
    pub manifest_path: PathBuf,
}

impl SegmentLayout {
    pub fn new(base: impl AsRef<Path>, level: u8, segment_id: u64) -> Self {
        let root = base
            .as_ref()
            .join("segments")
            .join(format!("L{level}"))
            .join(format!("seg-{segment_id:016x}"));
        Self::from_root(root)
    }

    pub(crate) fn from_root(root: PathBuf) -> Self {
        Self {
            chunks_path: root.join("chunks.bin"),
            chunk_index_path: root.join("chunk_index.bin"),
            series_path: root.join("series.bin"),
            postings_path: root.join("postings.bin"),
            manifest_path: root.join("manifest.bin"),
            root,
        }
    }
}

/// 哨兵文件名：compaction 已发布替换（marker 落盘）但**尚未**删除的源段目录
/// 内的标记文件。段目录扫描（`collect_segment_dirs`，所有列举路径的唯一收口）
/// 跳过含该文件的目录，使「待删源段」在选窗、inventory、启动扫描与 retention
/// sweep 里都不可见——源段目录本身留在盘上，直到内存持久化索引刷新完成
/// （延迟删除，消除 compaction 后查询 NotFound 窗口，C-3）。
///
/// 权威待删清单仍是 `.compaction-replacements/` 内的 marker（含输出段列表，
/// 崩溃兜底用）；哨兵只是「扫描不可见」的快速标记，两者由同一发布序列维护：
/// 阶段一先写 marker 再写哨兵，阶段二先删源目录（含哨兵）再删 marker。
pub(crate) const PENDING_REPLACEMENT_DELETE_SENTINEL: &str = ".pending-replacement-delete";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentManifest {
    pub segment_id: u64,
    pub level: u8,
    pub chunk_count: usize,
    pub point_count: usize,
    pub series_count: usize,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    pub wal_highwater: WalHighWatermark,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentContentFingerprint {
    pub(crate) manifest: SegmentManifest,
    pub(crate) files: [SegmentFileFingerprint; super::format::MANIFEST_FILE_ENTRY_COUNT],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentFileFingerprint {
    pub(crate) kind: u8,
    pub(crate) file_len: u64,
    pub(crate) hash64: u64,
}

#[derive(Debug, Clone)]
pub struct PersistedSeries {
    pub series_id: SeriesId,
    pub metric: String,
    pub labels: Vec<Label>,
    pub value_family: Option<SeriesValueFamily>,
}

#[derive(Debug, Default)]
pub struct LoadedSegments {
    pub next_segment_id: u64,
    pub series: Vec<PersistedSeries>,
    pub chunks_by_series: HashMap<SeriesId, Vec<Chunk>>,
}

#[derive(Debug, Clone)]
pub struct LoadedSegment {
    pub root: PathBuf,
    pub manifest: SegmentManifest,
    pub series: Vec<PersistedSeries>,
    pub chunks_by_series: HashMap<SeriesId, Vec<Chunk>>,
}

pub struct IndexedSegment {
    pub root: PathBuf,
    pub manifest: SegmentManifest,
    pub series: Vec<PersistedSeries>,
    pub(crate) postings: SegmentPostingsIndex,
    pub chunk_index: ChunkIndex,
    pub chunks_mmap: PlatformMmap,
}

#[derive(Default)]
pub struct LoadedSegmentIndexes {
    pub next_segment_id: u64,
    pub series: Vec<PersistedSeries>,
    pub indexed_segments: Vec<IndexedSegment>,
    pub wal_replay_highwater: WalHighWatermark,
}
