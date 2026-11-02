use super::*;

pub(super) struct StartupFinalizePhase;

#[derive(Clone, Copy)]
pub(super) struct StartupFinalizeState {
    pub(super) force_registry_checkpoint_after_startup: bool,
    pub(super) reconcile_registry_with_persisted: bool,
    /// W3-T4：启动期 catalog delta 重放统计（recovery 产生，run 内播种）。
    pub(super) catalog_delta_stats: registry_catalog::CatalogDeltaStartupStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegistryPersistenceAction {
    Checkpoint,
    Persist,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StartupFinalizeActions {
    pub(super) registry_persistence: RegistryPersistenceAction,
    pub(super) run_sync_maintenance: bool,
    pub(super) start_background_threads: bool,
    pub(super) schedule_startup_maintenance: bool,
}

impl StartupFinalizeState {
    pub(super) fn actions(
        self,
        background_threads_enabled: bool,
        background_fail_fast: bool,
    ) -> StartupFinalizeActions {
        StartupFinalizeActions {
            registry_persistence: if self.force_registry_checkpoint_after_startup
                || !self.reconcile_registry_with_persisted
            {
                RegistryPersistenceAction::Checkpoint
            } else {
                RegistryPersistenceAction::Persist
            },
            run_sync_maintenance: !background_threads_enabled || background_fail_fast,
            start_background_threads: background_threads_enabled,
            schedule_startup_maintenance: background_threads_enabled && !background_fail_fast,
        }
    }
}

impl StartupFinalizePhase {
    pub(super) fn run(
        storage: &Arc<ChunkStorage>,
        finalize_state: StartupFinalizeState,
        background_threads_enabled: bool,
        background_fail_fast: bool,
    ) -> Result<()> {
        let actions = finalize_state.actions(background_threads_enabled, background_fail_fast);
        // W3-T4：启动期 delta 重放/回退统计并入实例观测（先播种再执行
        // registry 持久化，保证首个观测快照已包含启动期数字）。
        storage.seed_catalog_delta_startup_stats(finalize_state.catalog_delta_stats);

        if actions.run_sync_maintenance {
            storage.sweep_expired_persisted_segments()?;
            storage.reconcile_live_metadata_indexes()?;
        }

        match actions.registry_persistence {
            RegistryPersistenceAction::Checkpoint => storage.checkpoint_series_registry_index()?,
            RegistryPersistenceAction::Persist => storage.persist_series_registry_index()?,
        }

        if storage.memory_budget_value() != usize::MAX {
            storage.refresh_memory_usage();
            storage.enforce_memory_budget_if_needed()?;
        }

        if actions.start_background_threads {
            storage.start_background_flush_thread(storage.runtime.flush_interval)?;
            storage.start_background_persisted_refresh_thread()?;
            storage.start_background_rollup_thread(DEFAULT_ROLLUP_INTERVAL)?;
            if actions.schedule_startup_maintenance {
                storage.schedule_startup_maintenance();
            }
        }

        Ok(())
    }
}
