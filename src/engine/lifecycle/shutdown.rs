use super::*;
use parking_lot::{Mutex, MutexGuard};
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy)]
struct LifecycleShutdownContext<'a> {
    background_maintenance_lock: &'a Mutex<()>,
    compaction_lock: &'a Mutex<()>,
    write_limiter: &'a crate::concurrency::Semaphore,
    write_timeout: std::time::Duration,
    numeric_compactor: Option<&'a Compactor>,
    blob_compactor: Option<&'a Compactor>,
    pending_persisted_segment_diff: &'a Mutex<PendingPersistedSegmentDiff>,
    persisted_index_dirty: &'a AtomicBool,
    observability: &'a StorageObservabilityCounters,
}

impl<'a> LifecycleShutdownContext<'a> {
    fn background_maintenance_gate(self) -> MutexGuard<'a, ()> {
        self.background_maintenance_lock.lock()
    }

    fn compaction_gate(self) -> MutexGuard<'a, ()> {
        self.compaction_lock.lock()
    }

    fn acquire_close_write_permits(self) -> Result<Vec<crate::concurrency::SemaphoreGuard<'a>>> {
        self.write_limiter.acquire_all(self.write_timeout)
    }

    fn compact_until_settled(self, max_passes: usize) -> Result<usize> {
        let _compaction_guard = self.compaction_gate();
        let mut passes = 0usize;
        for _ in 0..max_passes.max(1) {
            let changes = ChunkStorage::compact_compactors_with_changes(
                self.numeric_compactor,
                self.blob_compactor,
                Some(self.observability),
            )?;
            if changes.is_empty() {
                break;
            }
            self.pending_persisted_segment_diff.lock().merge(changes);
            self.persisted_index_dirty.store(true, Ordering::SeqCst);
            passes = passes.saturating_add(1);
        }
        Ok(passes)
    }

    fn persisted_index_dirty(self) -> bool {
        self.persisted_index_dirty.load(Ordering::SeqCst)
    }

    fn record_deferred_dirty_refresh(self, err: &TsinkError) {
        self.observability
            .record_maintenance_error("close dirty persisted refresh", err);
    }
}

/// Per-pass outcome of a budgeted compaction drain over all compactors.
#[derive(Debug, Default)]
pub(in crate::engine::storage_engine) struct CompactionDrainPass {
    pub(in crate::engine::storage_engine) changes: PendingPersistedSegmentDiff,
    pub(in crate::engine::storage_engine) compaction_runs: usize,
    pub(in crate::engine::storage_engine) l0_to_l1_runs: usize,
    pub(in crate::engine::storage_engine) l1_to_l2_runs: usize,
    pub(in crate::engine::storage_engine) backlog_remains: bool,
}

impl ChunkStorage {
    fn lifecycle_shutdown_context(&self) -> LifecycleShutdownContext<'_> {
        LifecycleShutdownContext {
            background_maintenance_lock: &self.coordination.background_maintenance_lock,
            compaction_lock: self.coordination.compaction_lock.as_ref(),
            write_limiter: &self.runtime.write_limiter,
            write_timeout: self.runtime.write_timeout,
            numeric_compactor: self.persisted.numeric_compactor.as_ref(),
            blob_compactor: self.persisted.blob_compactor.as_ref(),
            pending_persisted_segment_diff: &self.persisted.pending_persisted_segment_diff,
            persisted_index_dirty: self.persisted.persisted_index_dirty.as_ref(),
            observability: self.observability.as_ref(),
        }
    }

    /// Drains eligible compaction work: keeps taking window passes through
    /// [`Compactor::compact_once_with_changes`] until a pass reports no work
    /// (settled) or the lane's share of the pass time budget is spent. Each
    /// iteration re-enters the level round-robin arbitration, so a sustained
    /// L0 backlog cannot starve L1->L2 inside the drain. The budget gates
    /// *starting* the next pass, so one drain always performs at least one
    /// compaction attempt and may overshoot the budget by a single pass; the
    /// background worker holds the compaction gate for at most
    /// `budget + one pass`, bounding how long close/manual compaction waits.
    /// Each lane gets its own full budget so a permanently backlogged numeric
    /// lane cannot starve the blob lane out of its turns.
    pub(in crate::engine::storage_engine) fn drain_compactors_with_budget(
        numeric_compactor: Option<&Compactor>,
        blob_compactor: Option<&Compactor>,
        observability: Option<&StorageObservabilityCounters>,
        time_budget: std::time::Duration,
    ) -> Result<CompactionDrainPass> {
        let mut pass = CompactionDrainPass::default();
        for compactor in [numeric_compactor, blob_compactor].into_iter().flatten() {
            let deadline = Instant::now() + time_budget;
            loop {
                let (changes, stats) = Self::run_compactor_pass_once(compactor, observability)?;
                pass.changes.merge(changes);
                if !stats.compacted {
                    break;
                }
                pass.compaction_runs += 1;
                match (stats.source_level, stats.target_level) {
                    (Some(0), Some(1)) => pass.l0_to_l1_runs += 1,
                    (Some(1), Some(2)) => pass.l1_to_l2_runs += 1,
                    _ => {}
                }
                if Instant::now() >= deadline {
                    // Eligible work was still being found when the budget ran
                    // out; flag it so the worker expedites the next pass.
                    pass.backlog_remains = true;
                    break;
                }
            }
        }
        Ok(pass)
    }

    pub(in super::super) fn compact_compactors_with_changes(
        numeric_compactor: Option<&Compactor>,
        blob_compactor: Option<&Compactor>,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<PendingPersistedSegmentDiff> {
        let mut changes = PendingPersistedSegmentDiff::default();
        if let Some(compactor) = numeric_compactor {
            changes.merge(Self::run_compactor_once(compactor, observability)?);
        }
        if let Some(compactor) = blob_compactor {
            changes.merge(Self::run_compactor_once(compactor, observability)?);
        }
        Ok(changes)
    }

    fn run_compactor_once(
        compactor: &Compactor,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<PendingPersistedSegmentDiff> {
        Ok(Self::run_compactor_pass_once(compactor, observability)?.0)
    }

    fn run_compactor_pass_once(
        compactor: &Compactor,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<(
        PendingPersistedSegmentDiff,
        crate::engine::compactor::CompactionRunStats,
    )> {
        let started = Instant::now();
        match compactor.compact_once_with_changes() {
            Ok(outcome) => {
                let stats = outcome.stats;
                if let Some(obs) = observability {
                    obs.record_compaction_result(stats, elapsed_nanos_u64(started));
                }
                let mut changes = PendingPersistedSegmentDiff::default();
                if stats.compacted {
                    changes.record_changes(outcome.output_roots, outcome.source_roots);
                }
                Ok((changes, stats))
            }
            Err(err) => {
                if let Some(obs) = observability {
                    obs.record_compaction_error(elapsed_nanos_u64(started));
                }
                Err(err)
            }
        }
    }

    fn close_should_defer_dirty_refresh_error(err: &TsinkError) -> bool {
        crate::engine::segment::segment_validation_error_message(err).is_some()
            || matches!(err, TsinkError::Other(message) if message.contains("during runtime refresh"))
    }

    fn execute_close_pipeline(&self) -> Result<()> {
        let shutdown = self.lifecycle_shutdown_context();
        let _background_maintenance_guard = shutdown.background_maintenance_gate();
        let mut deferred_dirty_refresh = false;
        let _write_permits = shutdown.acquire_close_write_permits()?;
        self.flush_all_active()?;
        self.persist_segment()?;
        self.sweep_expired_persisted_segments()?;
        if self.memory_budget_value() != usize::MAX {
            self.refresh_memory_usage();
        }
        shutdown.compact_until_settled(CLOSE_COMPACTION_MAX_PASSES)?;
        if shutdown.persisted_index_dirty() || self.has_known_persisted_segment_changes() {
            if let Err(err) = self.refresh_dirty_persisted_segments_claimed() {
                if Self::close_should_defer_dirty_refresh_error(&err) {
                    shutdown.record_deferred_dirty_refresh(&err);
                    tracing::warn!(
                        error = %err,
                        "Close deferred dirty persisted refresh and kept the last visible catalog"
                    );
                    deferred_dirty_refresh = true;
                } else {
                    return Err(err);
                }
            }
        }
        self.persist_tombstones_index()?;
        if deferred_dirty_refresh {
            self.checkpoint_series_registry_index_allow_invalid_catalog()?;
        } else {
            self.checkpoint_series_registry_index()?;
        }
        Ok(())
    }

    pub(in super::super) fn close_impl(&self) -> Result<()> {
        self.start_close_transition()?;
        let close_result = self.execute_close_pipeline();
        self.finish_close_transition(close_result)
    }
}
