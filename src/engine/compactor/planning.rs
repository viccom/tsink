use super::*;

/// Which level transition compacted most recently, as recorded by
/// [`Compactor::compact_once_with_changes`] through its arbitration pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LevelTransition {
    L0ToL1,
    L1ToL2,
}

impl LevelTransition {
    fn source(self) -> CompactionLevel {
        match self {
            Self::L0ToL1 => CompactionLevel::L0,
            Self::L1ToL2 => CompactionLevel::L1,
        }
    }

    fn target(self) -> CompactionLevel {
        match self {
            Self::L0ToL1 => CompactionLevel::L1,
            Self::L1ToL2 => CompactionLevel::L2,
        }
    }

    fn count_trigger(self, compactor: &Compactor) -> usize {
        match self {
            Self::L0ToL1 => compactor.l0_trigger,
            Self::L1ToL2 => compactor.l1_trigger,
        }
    }

    fn arbitration_tag(self) -> u8 {
        match self {
            Self::L0ToL1 => super::LAST_TRANSITION_L0_TO_L1,
            Self::L1ToL2 => super::LAST_TRANSITION_L1_TO_L2,
        }
    }
}

/// A manifest-only view of a persisted segment, used to survey a level for
/// compaction window selection. Selection needs only the segment id and time
/// range, so the level is surveyed from manifests and only the chosen window
/// is fully loaded (was: the entire level payload-loaded every pass, 5.8GB at
/// 4640 L1 segments).
#[derive(Debug, Clone)]
pub(super) struct SegmentSelectionMeta {
    pub(super) root: PathBuf,
    pub(super) manifest: SegmentManifest,
}

impl CompactionSelectionEntry for SegmentSelectionMeta {
    fn selection_segment_id(&self) -> u64 {
        self.manifest.segment_id
    }

    fn selection_min_ts(&self) -> Option<i64> {
        self.manifest.min_ts
    }

    fn selection_max_ts(&self) -> Option<i64> {
        self.manifest.max_ts
    }
}

impl CompactionSelectionEntry for LoadedSegment {
    fn selection_segment_id(&self) -> u64 {
        self.manifest.segment_id
    }

    fn selection_min_ts(&self) -> Option<i64> {
        self.manifest.min_ts
    }

    fn selection_max_ts(&self) -> Option<i64> {
        self.manifest.max_ts
    }
}

/// The manifest fields compaction window selection is defined over, shared by
/// the manifest-only survey entries and fully loaded segments so both run the
/// exact same selection algorithm.
pub(super) trait CompactionSelectionEntry {
    fn selection_segment_id(&self) -> u64;
    fn selection_min_ts(&self) -> Option<i64>;
    fn selection_max_ts(&self) -> Option<i64>;
}

impl Compactor {
    /// Level-fair arbitration: whichever transition compacted last is tried
    /// second on the next pass. Under sustained writes L0->L1 stays eligible
    /// forever, so a first-win policy starves L1->L2 and the L1 backlog grows
    /// without bound (observed: 4640 segments after a 7h run). Round-robin
    /// keeps both transitions making progress while each level's own trigger
    /// semantics (count threshold / time overlap, minimum 2 segments) stay
    /// unchanged; a level that is not eligible simply yields the pass.
    pub(in crate::engine) fn compact_once_with_changes(&self) -> Result<CompactionOutcome> {
        {
            // Marker application deletes source segment directories, so it must
            // serialize against the flush persist pipeline's registry catalog
            // build. The rest of the pass (survey, window load, merge, output
            // writes) does not delete anything and runs outside this gate.
            let _replacement_guard = self.segment_replacement_guard();
            finalize_pending_compaction_replacements(&self.data_path)?;
        }
        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;

        let prefer_l1_to_l2 = self.last_compacted_transition.load(Ordering::Acquire)
            == super::LAST_TRANSITION_L0_TO_L1;
        let transitions = if prefer_l1_to_l2 {
            [LevelTransition::L1ToL2, LevelTransition::L0ToL1]
        } else {
            [LevelTransition::L0ToL1, LevelTransition::L1ToL2]
        };

        for transition in transitions {
            if let Some(outcome) = self.try_compact_level(transition, &tombstones)? {
                self.last_compacted_transition
                    .store(transition.arbitration_tag(), Ordering::Release);
                return Ok(outcome);
            }
        }

        Ok(CompactionOutcome::default())
    }

    fn try_compact_level(
        &self,
        transition: LevelTransition,
        tombstones: &TombstoneMap,
    ) -> Result<Option<CompactionOutcome>> {
        let source = transition.source();
        let target = transition.target();
        let count_trigger = transition.count_trigger(self);
        let source_level = level_to_u8(source);
        let target_level = level_to_u8(target);

        // Survey the level from manifests only (one lightweight read per
        // segment), then defer the full payload load to the selected window.
        let mut candidates = load_level_compaction_candidates(&self.data_path, source_level)?;
        if candidates.len() < 2 {
            return Ok(None);
        }

        candidates.sort_by_key(|candidate| candidate.manifest.segment_id);

        let should_compact = candidates.len() >= count_trigger || has_time_overlap(&candidates);
        if !should_compact {
            return Ok(None);
        }

        let window =
            select_compaction_window(&candidates, count_trigger, DEFAULT_SOURCE_WINDOW_SEGMENTS);
        if window.len() < 2 {
            return Ok(None);
        }

        let mut segments = Vec::with_capacity(window.len());
        for candidate in &window {
            if let Some(segment) = load_segment_for_compaction(&candidate.root)? {
                segments.push(segment);
            }
        }
        if segments.len() < 2 {
            return Ok(None);
        }

        let loaded_window = segments.iter().collect::<Vec<_>>();
        let mut outcome = self.compact_segments(target_level, &loaded_window, tombstones)?;
        outcome.stats.compacted = true;
        outcome.stats.source_level = Some(source_level);
        outcome.stats.target_level = Some(target_level);
        Ok(Some(outcome))
    }
}

/// Reads one manifest per segment directory of a level (skipping directories
/// that disappeared concurrently) and validates each manifest's stored level
/// against its directory, with the same compaction-validation failure modes
/// the pre-window level count enforced.
pub(super) fn load_level_compaction_candidates(
    base: &Path,
    level: u8,
) -> Result<Vec<SegmentSelectionMeta>> {
    let expected_level_dir = format!("L{level}");
    let mut candidates = Vec::new();

    for root in list_segment_dirs(base)? {
        if root
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            != Some(expected_level_dir.as_str())
        {
            continue;
        }

        match read_segment_manifest(&root) {
            Ok(manifest) => {
                if manifest.level != level {
                    return Err(segment_validation_error(
                        &root,
                        SegmentValidationContext::Compaction,
                        &format!(
                            "segment directory level mismatch: stored under L{level}, manifest says L{}",
                            manifest.level
                        ),
                    ));
                }
                candidates.push(SegmentSelectionMeta { root, manifest });
            }
            Err(err) if is_not_found_error(&err) => continue,
            Err(TsinkError::DataCorruption(details)) => {
                return Err(segment_validation_error(
                    &root,
                    SegmentValidationContext::Compaction,
                    &details,
                ));
            }
            Err(err) => return Err(err),
        }
    }

    Ok(candidates)
}

pub(super) fn has_time_overlap<T: CompactionSelectionEntry>(segments: &[T]) -> bool {
    let mut ranges = segments
        .iter()
        .filter_map(|segment| {
            Some((
                segment.selection_min_ts()?,
                segment.selection_max_ts()?,
                segment.selection_segment_id(),
            ))
        })
        .collect::<Vec<_>>();

    if ranges.len() < 2 {
        return false;
    }

    ranges.sort_by_key(|(min_ts, _, segment_id)| (*min_ts, *segment_id));

    let mut current_max = ranges[0].1;
    for (min_ts, max_ts, _) in ranges.into_iter().skip(1) {
        if min_ts <= current_max {
            return true;
        }
        current_max = current_max.max(max_ts);
    }

    false
}

pub(super) fn select_compaction_window<T: CompactionSelectionEntry>(
    segments: &[T],
    count_trigger: usize,
    max_segments: usize,
) -> Vec<&T> {
    let max_segments = max_segments.max(2);
    if let Some(indexes) = overlapping_window_indexes(segments, max_segments) {
        return indexes
            .into_iter()
            .filter_map(|index| segments.get(index))
            .collect();
    }

    let window_len = count_trigger.max(2).min(max_segments).min(segments.len());
    segments.iter().take(window_len).collect()
}

#[derive(Debug, Clone, Copy)]
struct SegmentTimeRange {
    index: usize,
    segment_id: u64,
    min_ts: i64,
    max_ts: i64,
}

fn overlapping_window_indexes<T: CompactionSelectionEntry>(
    segments: &[T],
    max_segments: usize,
) -> Option<Vec<usize>> {
    let mut ranges = segments
        .iter()
        .enumerate()
        .filter_map(|(index, segment)| {
            Some(SegmentTimeRange {
                index,
                segment_id: segment.selection_segment_id(),
                min_ts: segment.selection_min_ts()?,
                max_ts: segment.selection_max_ts()?,
            })
        })
        .collect::<Vec<_>>();

    if ranges.len() < 2 {
        return None;
    }

    ranges.sort_by_key(|range| (range.min_ts, range.segment_id));

    let mut cluster_start = 0usize;
    let mut cluster_max = ranges[0].max_ts;

    for idx in 1..ranges.len() {
        if ranges[idx].min_ts <= cluster_max {
            cluster_max = cluster_max.max(ranges[idx].max_ts);
            continue;
        }

        if idx.saturating_sub(cluster_start) >= 2 {
            return Some(select_cluster_indexes(
                &ranges[cluster_start..idx],
                max_segments,
            ));
        }

        cluster_start = idx;
        cluster_max = ranges[idx].max_ts;
    }

    if ranges.len().saturating_sub(cluster_start) >= 2 {
        return Some(select_cluster_indexes(
            &ranges[cluster_start..],
            max_segments,
        ));
    }

    None
}

fn select_cluster_indexes(cluster: &[SegmentTimeRange], max_segments: usize) -> Vec<usize> {
    let mut indexes = cluster.iter().map(|range| range.index).collect::<Vec<_>>();
    indexes.sort_unstable();
    indexes.truncate(max_segments.max(2));
    indexes
}

pub(super) fn level_to_u8(level: CompactionLevel) -> u8 {
    match level {
        CompactionLevel::L0 => 0,
        CompactionLevel::L1 => 1,
        CompactionLevel::L2 => 2,
    }
}
