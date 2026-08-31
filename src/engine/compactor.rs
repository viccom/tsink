use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::durability::WalHighWatermark;
use crate::engine::encoder::Encoder;
use crate::engine::fs_utils::{remove_dir_if_exists, write_file_atomically_and_sync_parent};
use crate::engine::segment::{
    is_not_found_error, list_segment_dirs, load_segment_for_compaction,
    load_segments_runtime_strict, read_segment_manifest, segment_validation_error, LoadedSegment,
    PersistedSeries, SegmentManifest, SegmentValidationContext, SegmentWriter,
};
use crate::engine::series::{SeriesId, SeriesRegistry};
use crate::engine::tombstone::{
    load_tombstones, timestamp_is_tombstoned, TombstoneMap, TombstoneRange, TOMBSTONES_FILE_NAME,
};
use crate::{Result, TsinkError};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

#[path = "compactor/execution.rs"]
mod execution;
#[path = "compactor/planning.rs"]
mod planning;

pub(in crate::engine) use self::execution::finalize_pending_compaction_replacements;

const DEFAULT_L0_TRIGGER: usize = 4;
const DEFAULT_L1_TRIGGER: usize = 4;
const DEFAULT_SOURCE_WINDOW_SEGMENTS: usize = 8;
/// Point budget for one compaction output segment, independent of the chunk
/// point cap. It used to be `chunk_point_cap * 512`, which silently shrank the
/// budget (and multiplied the output segment / file count by the same factor)
/// whenever chunk_points was tuned below its 2048 default (rs-iot runs
/// chunk_points=512 and got a 4x segment amplification). It is now 8x the L0
/// flush shard size (1M points): a budget equal to the shard size made an
/// L0->L1 merge of 8x1M re-emit ~8x1M ("8-in-8-out"), so the level segment
/// count never shrank. 8 x 1,048,576 = 8,388,608 points.
const DEFAULT_OUTPUT_SEGMENT_POINT_BUDGET: usize = 8_388_608;
const COMPACTION_REPLACEMENT_DIR: &str = ".compaction-replacements";
const COMPACTION_REPLACEMENT_VERSION: u16 = 1;
static COMPACTION_REPLACEMENT_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Level-fair arbitration state: which level transition compacted last.
/// `LAST_TRANSITION_L0_TO_L1` / `LAST_TRANSITION_L1_TO_L2` make the next pass
/// prefer the other transition first, so a continuously-triggering L0->L1
/// cannot starve L1->L2 (the 4640-segment L1 backlog root cause).
const LAST_TRANSITION_NONE: u8 = 0;
const LAST_TRANSITION_L0_TO_L1: u8 = 1;
const LAST_TRANSITION_L1_TO_L2: u8 = 2;

type SeriesChunkRefs<'a> = HashMap<SeriesId, Vec<&'a Chunk>>;
type MergeSegmentsOutput<'a> = (Vec<PersistedSeries>, SeriesChunkRefs<'a>);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactionReplacementMarker {
    version: u16,
    source_segments: Vec<String>,
    output_segments: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionRunStats {
    pub compacted: bool,
    pub source_level: Option<u8>,
    pub target_level: Option<u8>,
    pub source_segments: usize,
    pub output_segments: usize,
    pub source_chunks: usize,
    pub output_chunks: usize,
    pub source_points: usize,
    pub output_points: usize,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub(super) struct CompactionOutcome {
    pub(super) stats: CompactionRunStats,
    pub(super) source_roots: Vec<PathBuf>,
    pub(super) output_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionLevel {
    L0,
    L1,
    L2,
}

#[derive(Debug, Clone)]
pub struct Compactor {
    data_path: PathBuf,
    point_cap: usize,
    output_segment_point_budget: usize,
    l0_trigger: usize,
    l1_trigger: usize,
    next_segment_id: Option<Arc<AtomicU64>>,
    last_compacted_transition: Arc<AtomicU8>,
    /// When wired into a live engine, serializes this compactor's
    /// segment-replacement marker operations against the flush persist
    /// pipeline's registry catalog build. `None` for standalone compactors
    /// (tests, retention rewriters) that own their data path exclusively.
    segment_replacement_lock: Option<Arc<Mutex<()>>>,
}

impl Compactor {
    pub fn new(data_path: impl AsRef<Path>, point_cap: usize) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            output_segment_point_budget: DEFAULT_OUTPUT_SEGMENT_POINT_BUDGET,
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            next_segment_id: None,
            last_compacted_transition: Arc::new(AtomicU8::new(LAST_TRANSITION_NONE)),
            segment_replacement_lock: None,
        }
    }

    pub fn new_with_segment_id_allocator(
        data_path: impl AsRef<Path>,
        point_cap: usize,
        next_segment_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            output_segment_point_budget: DEFAULT_OUTPUT_SEGMENT_POINT_BUDGET,
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            next_segment_id: Some(next_segment_id),
            last_compacted_transition: Arc::new(AtomicU8::new(LAST_TRANSITION_NONE)),
            segment_replacement_lock: None,
        }
    }

    /// Overrides the per-output-segment point budget. Intentionally not a
    /// public `StorageBuilder` knob: no consumer needs to tune it yet, so the
    /// decoupled budget stays an internal constant with a crate-internal
    /// override for tests.
    #[cfg(test)]
    pub(in crate::engine) fn with_output_segment_point_budget(mut self, budget: usize) -> Self {
        self.output_segment_point_budget = budget.max(1);
        self
    }

    /// Wires this compactor into a live engine's segment-replacement lock so
    /// its marker write/apply phases serialize against the flush persist
    /// pipeline instead of forcing persist to wait out whole compaction passes.
    pub fn with_segment_replacement_lock(mut self, lock: Arc<Mutex<()>>) -> Self {
        self.segment_replacement_lock = Some(lock);
        self
    }

    /// Acquires the segment-replacement gate for the caller's next marker
    /// operations. Returns `None` for standalone compactors without a lock.
    fn segment_replacement_guard(&self) -> Option<MutexGuard<'_, ()>> {
        self.segment_replacement_lock
            .as_ref()
            .map(|lock| lock.lock())
    }

    pub fn compact_once(&self) -> Result<bool> {
        Ok(self.compact_once_with_stats()?.compacted)
    }

    pub fn compact_once_with_stats(&self) -> Result<CompactionRunStats> {
        Ok(self.compact_once_with_changes()?.stats)
    }
}

#[cfg(test)]
#[path = "compactor/tests.rs"]
mod tests;
