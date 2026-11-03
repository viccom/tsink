use super::*;

// Test instrumentation: counts segment-manifest reads performed by the level
// survey (`load_level_compaction_candidates`) on the current thread.
// Thread-local so parallel tests cannot pollute each other's counts. The L2→L2
// gate tests reset it to assert that a pass which cannot trigger pays no
// manifest I/O at all (one open+read+parse per L2 segment otherwise).
#[cfg(test)]
thread_local! {
    static LEVEL_MANIFEST_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn reset_level_manifest_read_count() {
    LEVEL_MANIFEST_READS.with(|count| count.set(0));
}

#[cfg(test)]
pub(super) fn level_manifest_read_count() -> u64 {
    LEVEL_MANIFEST_READS.with(std::cell::Cell::get)
}

/// Which level transition compacted most recently, as recorded by
/// [`Compactor::compact_once_with_changes`] through its arbitration pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LevelTransition {
    L0ToL1,
    L1ToL2,
    L2ToL2,
}

impl LevelTransition {
    fn source(self) -> CompactionLevel {
        match self {
            Self::L0ToL1 => CompactionLevel::L0,
            Self::L1ToL2 => CompactionLevel::L1,
            Self::L2ToL2 => CompactionLevel::L2,
        }
    }

    fn target(self) -> CompactionLevel {
        match self {
            Self::L0ToL1 => CompactionLevel::L1,
            Self::L1ToL2 => CompactionLevel::L2,
            Self::L2ToL2 => CompactionLevel::L2,
        }
    }

    fn count_trigger(self, compactor: &Compactor) -> usize {
        match self {
            Self::L0ToL1 => compactor.l0_trigger,
            Self::L1ToL2 => compactor.l1_trigger,
            Self::L2ToL2 => compactor.l2_trigger,
        }
    }

    fn arbitration_tag(self) -> u8 {
        match self {
            Self::L0ToL1 => super::LAST_TRANSITION_L0_TO_L1,
            Self::L1ToL2 => super::LAST_TRANSITION_L1_TO_L2,
            Self::L2ToL2 => super::LAST_TRANSITION_L2_TO_L2,
        }
    }

    /// The transition whose arbitration tag matches, for rebuilding the
    /// rotation order after a pass recorded the previous winner.
    fn from_arbitration_tag(tag: u8) -> Option<Self> {
        match tag {
            super::LAST_TRANSITION_L0_TO_L1 => Some(Self::L0ToL1),
            super::LAST_TRANSITION_L1_TO_L2 => Some(Self::L1ToL2),
            super::LAST_TRANSITION_L2_TO_L2 => Some(Self::L2ToL2),
            _ => None,
        }
    }
}

/// Which compaction work a single pass may take on. Terminal-level (L2→L2)
/// convergence is background progress work only, so close-time passes defer
/// it: a legacy backlog of tens of thousands of terminal segments would
/// otherwise keep the close drain `compact_until_settled` looping on
/// convergence passes (16 segments per pass, ~1000 passes at 15.8k segments)
/// and stretch graceful shutdown by an unbounded, data-dependent amount.
/// The background worker picks convergence up one window per tick instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PassScope {
    /// Standalone and background passes: every eligible transition may run.
    All,
    /// Close drain passes: L2→L2 convergence is deferred to the background.
    CloseDrain,
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
    /// last on the next pass. Under sustained writes L0->L1 stays eligible
    /// forever, so a first-win policy starves L1->L2 and the L1 backlog grows
    /// without bound (observed: 4640 segments after a 7h run). Round-robin
    /// keeps every transition making progress while each level's own trigger
    /// semantics (count threshold / time overlap, minimum 2 segments) stay
    /// unchanged; a level that is not eligible simply yields the pass.
    pub(in crate::engine) fn compact_once_with_changes(&self) -> Result<CompactionOutcome> {
        self.compact_once_scoped(PassScope::All)
    }

    /// Close-drain variant of [`Compactor::compact_once_with_changes`]:
    /// terminal-level (L2→L2) convergence is deferred to the background
    /// worker so a legacy L2 backlog cannot extend graceful shutdown (see
    /// [`PassScope::CloseDrain`]).
    pub(in crate::engine) fn compact_once_for_close(&self) -> Result<CompactionOutcome> {
        self.compact_once_scoped(PassScope::CloseDrain)
    }

    fn compact_once_scoped(&self, scope: PassScope) -> Result<CompactionOutcome> {
        {
            // Marker application deletes source segment directories, so it must
            // serialize against the flush persist pipeline's registry catalog
            // build. The rest of the pass (survey, window load, merge, output
            // writes) does not delete anything and runs outside this gate.
            let _replacement_guard = self.segment_replacement_guard();
            finalize_pending_compaction_replacements(&self.data_path)?;
        }
        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;

        // "The previous winner goes to the back": keep the natural order but
        // move the transition that ran last to the end, so the two (now
        // three) transitions rotate fairly instead of a continuously
        // triggering one monopolizing every pass.
        let last = LevelTransition::from_arbitration_tag(
            self.last_compacted_transition.load(Ordering::Acquire),
        );
        let mut transitions = [
            LevelTransition::L0ToL1,
            LevelTransition::L1ToL2,
            LevelTransition::L2ToL2,
        ];
        transitions.sort_by_key(|transition| Some(*transition) == last);

        for transition in transitions {
            if scope == PassScope::CloseDrain && transition == LevelTransition::L2ToL2 {
                continue;
            }
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

        if transition == LevelTransition::L2ToL2 {
            // Cost gate before the survey: a convergence pass that provably
            // cannot trigger must not pay for one manifest open+read+parse per
            // L2 segment (16.5k reads per 5s tick, zero output, forever, when
            // convergence is disabled). `0` disables convergence outright; a
            // directory count at or below the trigger cannot exceed it in the
            // survey either, since candidates ⊆ directories (the survey only
            // drops directories that vanished concurrently). Anything above
            // the trigger falls through to the full survey, so trigger
            // semantics stay byte-for-byte the same.
            if count_trigger == 0
                || count_level_segment_dirs(&self.data_path, source_level)? <= count_trigger
            {
                return Ok(None);
            }
        }

        // Survey the level from manifests only (one lightweight read per
        // segment), then defer the full payload load to the selected window.
        let mut candidates = load_level_compaction_candidates(&self.data_path, source_level)?;

        if transition == LevelTransition::L2ToL2 {
            return self.try_compact_l2_convergence(count_trigger, candidates, tombstones);
        }

        if candidates.len() < 2 {
            return Ok(None);
        }

        candidates.sort_by_key(|candidate| candidate.manifest.segment_id);

        let should_compact = candidates.len() >= count_trigger || has_time_overlap(&candidates);
        if !should_compact {
            return Ok(None);
        }

        let window =
            select_compaction_window(&candidates, count_trigger, self.source_window_segments);
        if window.len() < 2 {
            return Ok(None);
        }

        self.load_window_and_compact(source_level, target_level, &window, tombstones)
    }

    /// L2 terminal-level convergence (size-tiered): rewrites only segments
    /// small enough that merging them shrinks the segment count, never the
    /// bulk of the terminal data. Plan semantics:
    /// - candidates: segments with `point_count <= max_input_points`
    ///   (`output_segment_point_budget / 8`);
    /// - trigger: at least 2 candidates AND the L2 total segment count above
    ///   `l2_trigger` (0 disables convergence entirely);
    /// - window: candidates sorted by `(point_count, min_ts)` ascending
    ///   (smallest and oldest fragments first), capped at
    ///   `source_window_segments` AND at a cumulative point total of
    ///   `max_input_points` — so the merged output itself is never larger
    ///   than a candidate, stays inside the candidate pool's size class
    ///   (a pass never mints a new "large" segment) and a size-squeezed
    ///   configuration terminates instead of re-merging its own outputs
    ///   forever;
    /// - retention pre-exclusion: segments whose `max_ts` is already below
    ///   the active retention cutoff are dropped from candidacy — retention's
    ///   next sweep deletes them outright, so merging them would rewrite data
    ///   that is about to disappear.
    fn try_compact_l2_convergence(
        &self,
        count_trigger: usize,
        candidates: Vec<SegmentSelectionMeta>,
        tombstones: &TombstoneMap,
    ) -> Result<Option<CompactionOutcome>> {
        if count_trigger == 0 || candidates.len() <= count_trigger {
            return Ok(None);
        }

        let max_input_points = (self.output_segment_point_budget / 8).max(1);
        let retention_cutoff = self.active_retention_cutoff();
        let mut eligible: Vec<&SegmentSelectionMeta> = candidates
            .iter()
            .filter(|candidate| candidate.manifest.point_count <= max_input_points)
            .filter(|candidate| {
                // Retention's own expiry judgment (`max_ts < retention
                // cutoff`, cf. tiering policy `desired_tier_for_manifest`);
                // manifests without timestamps are retained by retention and
                // stay eligible here.
                match (candidate.manifest.max_ts, retention_cutoff) {
                    (Some(max_ts), Some(cutoff)) => max_ts >= cutoff,
                    _ => true,
                }
            })
            .collect();
        if eligible.len() < 2 {
            return Ok(None);
        }

        eligible.sort_by(|a, b| {
            a.manifest
                .point_count
                .cmp(&b.manifest.point_count)
                .then_with(|| {
                    a.manifest
                        .min_ts
                        .cmp(&b.manifest.min_ts)
                        .then_with(|| a.manifest.segment_id.cmp(&b.manifest.segment_id))
                })
        });

        // Ascending prefix: stop at the window cap or once one more segment
        // would push the cumulative window total past `max_input_points`.
        let mut cumulative_points = 0usize;
        let window: Vec<&SegmentSelectionMeta> = eligible
            .iter()
            .take(self.source_window_segments.max(2))
            .take_while(|candidate| {
                cumulative_points += candidate.manifest.point_count;
                cumulative_points <= max_input_points
            })
            .copied()
            .collect();
        if window.len() < 2 {
            return Ok(None);
        }

        self.load_window_and_compact(
            level_to_u8(CompactionLevel::L2),
            level_to_u8(CompactionLevel::L2),
            &window,
            tombstones,
        )
    }

    fn load_window_and_compact(
        &self,
        source_level: u8,
        target_level: u8,
        window: &[&SegmentSelectionMeta],
        tombstones: &TombstoneMap,
    ) -> Result<Option<CompactionOutcome>> {
        let mut segments = Vec::with_capacity(window.len());
        for candidate in window {
            if let Some(segment) = load_segment_for_compaction(&candidate.root)? {
                segments.push(segment);
            }
        }
        if segments.len() < 2 {
            return Ok(None);
        }

        let loaded_window = segments.iter().collect::<Vec<_>>();
        let mut outcome = self.compact_segments(target_level, &loaded_window, tombstones)?;
        // 不强改 stats.compacted（2026-10-30 审查修复）：零输出窗口（全墓碑/
        // 无 series）的完成语义由 execution 决定——此前强改 true 曾让调用方对
        // 未删除的源段记录幻影 removed_roots，drain 循环每轮重选同一窗口（活锁）。
        // 窗口既然选中并跑完，层级标注照记（drain 的 l0→l1/l1→l2 计数用）。
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

        #[cfg(test)]
        LEVEL_MANIFEST_READS.with(|count| count.set(count.get() + 1));
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
