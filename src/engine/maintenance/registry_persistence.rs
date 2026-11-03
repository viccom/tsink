mod context;

use super::super::*;

impl ChunkStorage {
    pub(in super::super) fn replace_registry_from_persisted_state(
        &self,
        registry: SeriesRegistry,
        delta_series_count: usize,
    ) -> Result<()> {
        self.registry_persistence_context()
            .replace_registry_from_persisted_state(
                registry,
                self.tombstone_read_context().max_tombstoned_series_id(),
                delta_series_count,
            )
    }

    pub(super) fn persisted_registry_catalog_sources(
        &self,
    ) -> Vec<registry_catalog::PersistedRegistryCatalogSource> {
        let persisted_index = self.persisted.persisted_index.read();
        persisted_index
            .segments_by_root
            .iter()
            .map(
                |(root, segment)| registry_catalog::PersistedRegistryCatalogSource {
                    lane: segment.lane,
                    root: root.clone(),
                },
            )
            .collect()
    }

    pub(in super::super) fn persisted_registry_catalog_sources_with_root_changes(
        &self,
        added_roots: &[PathBuf],
        removed_roots: &[PathBuf],
    ) -> Result<Vec<registry_catalog::PersistedRegistryCatalogSource>> {
        let mut sources = self
            .persisted_registry_catalog_sources()
            .into_iter()
            .map(|source| (source.root.clone(), source))
            .collect::<BTreeMap<_, _>>();
        for root in removed_roots {
            sources.remove(root);
        }
        for root in added_roots {
            let (lane, _tier) = self.persisted_segment_location_for_root(root)?;
            sources.insert(
                root.clone(),
                registry_catalog::PersistedRegistryCatalogSource {
                    lane,
                    root: root.clone(),
                },
            );
        }
        Ok(sources.into_values().collect())
    }

    fn persist_series_registry_catalog_index_with_sources(
        &self,
        checkpoint_path: &Path,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
    ) -> Result<()> {
        self.persist_series_registry_catalog_index_with_sources_mode(
            checkpoint_path,
            sources,
            &[],
            &[],
            false,
        )
    }

    /// checkpoint/关闭锚点：无视节流窗口强制刷新 catalog（全量重写主文件 +
    /// 写/更新 `_checkpoint.json` + 清 delta）。
    fn persist_series_registry_catalog_index_with_sources_forced(
        &self,
        checkpoint_path: &Path,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
    ) -> Result<()> {
        self.persist_series_registry_catalog_index_with_sources_mode(
            checkpoint_path,
            sources,
            &[],
            &[],
            true,
        )
    }

    /// `added_roots`/`removed_roots` 为空 = 冷路径（无变更信息，如启动
    /// finalize / 全量扫描发布），保持旧行为；非空 = 例行发布拍，走 W3-T4
    /// delta 路径（热路径 O(变更)，达阈值并入全量 checkpoint）。
    fn persist_series_registry_catalog_index_with_sources_mode(
        &self,
        checkpoint_path: &Path,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
        added_roots: &[PathBuf],
        removed_roots: &[PathBuf],
        force: bool,
    ) -> Result<()> {
        // P1：catalog 是段 manifest 的反规范化缓存。旧实现在每次 persist
        // （低速率下 ~1.7 次/s）都「全量读 + 解析 + 逐条比对」，失配再全量
        // pretty-JSON 重写——CPU 与写盘成本 ∝ 累计段数，随 uptime 线性上涨。
        // 现在常规路径按 `catalog_persist_interval` 节流；W3-T4 起发布拍直接
        // 追加 delta（O(变更)），checkpoint/关闭锚点强制全量刷新。
        let root_changes = if force || (added_roots.is_empty() && removed_roots.is_empty()) {
            None
        } else {
            Some(registry_catalog::CatalogRootChanges {
                added_roots,
                removed_roots,
            })
        };
        let (_, outcome) = registry_catalog::persist_registry_catalog_throttled(
            checkpoint_path,
            sources,
            root_changes,
            &self.persisted.catalog_validate_cache,
            &self.persisted.catalog_persist_memo,
            self.runtime.catalog_persist_interval,
            self.runtime.catalog_delta_max_count,
            self.runtime.catalog_delta_max_bytes,
            force,
        )?;
        self.observability
            .flush
            .catalog_persist_attempts_total
            .fetch_add(1, Ordering::Relaxed);
        match outcome {
            registry_catalog::CatalogPersistOutcome::Persisted
            | registry_catalog::CatalogPersistOutcome::CheckpointConsolidated => {
                self.observability
                    .flush
                    .catalog_persisted_total
                    .fetch_add(1, Ordering::Relaxed);
                self.observability
                    .flush
                    .catalog_checkpoint_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            registry_catalog::CatalogPersistOutcome::DeltaAppended { bytes } => {
                self.observability
                    .flush
                    .catalog_delta_appended_total
                    .fetch_add(1, Ordering::Relaxed);
                self.observability
                    .flush
                    .catalog_delta_bytes_total
                    .fetch_add(saturating_u64_from_usize(bytes as usize), Ordering::Relaxed);
            }
            registry_catalog::CatalogPersistOutcome::SkippedThrottled
            | registry_catalog::CatalogPersistOutcome::SkippedUnchanged
            | registry_catalog::CatalogPersistOutcome::SkippedValidatedFromFile
            | registry_catalog::CatalogPersistOutcome::DeltaWriteFailed => {
                // DeltaWriteFailed 归入 skip 口径：消耗了一次尝试但无落盘效果
                // （失败详情由 delta 追加/阈值合并路径 WARN 记录）。
                self.observability
                    .flush
                    .catalog_persist_skipped_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn checkpoint_series_registry_index_with_policy(
        &self,
        allow_invalid_catalog: bool,
    ) -> Result<()> {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        let delta_path = SeriesRegistry::incremental_path(checkpoint_path);
        let delta_dir_path = SeriesRegistry::incremental_dir(checkpoint_path);
        self.registry_persistence_context()
            .checkpoint_series_registry_index(
                checkpoint_path,
                &delta_path,
                &delta_dir_path,
                allow_invalid_catalog,
                |checkpoint_path| {
                    let sources = self.persisted_registry_catalog_sources();
                    self.persist_series_registry_catalog_index_with_sources_forced(
                        checkpoint_path,
                        &sources,
                    )
                },
            )
    }

    pub(in super::super) fn checkpoint_series_registry_index(&self) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy(false)
    }

    pub(in super::super) fn checkpoint_series_registry_index_allow_invalid_catalog(
        &self,
    ) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy(true)
    }

    pub(in super::super) fn persist_series_registry_index(&self) -> Result<()> {
        let sources = self.persisted_registry_catalog_sources();
        self.persist_series_registry_index_with_catalog_sources(&sources)
    }

    pub(in super::super) fn persist_series_registry_index_with_catalog_sources(
        &self,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
    ) -> Result<()> {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        let delta_path = SeriesRegistry::incremental_path(checkpoint_path);
        let delta_dir_path = SeriesRegistry::incremental_dir(checkpoint_path);
        self.registry_persistence_context()
            .persist_series_registry_index(
                checkpoint_path,
                &delta_path,
                &delta_dir_path,
                sources,
                |checkpoint_path, sources| {
                    self.persist_series_registry_catalog_index_with_sources(
                        checkpoint_path,
                        sources,
                    )
                },
            )
    }

    /// W3-T4 例行发布拍入口：sources 之外携带本拍 root 变更，catalog 走
    /// delta 追加（`registry_persistence` 持久锁内执行，序列号/备忘互斥由
    /// 持久锁保证）。
    pub(in super::super) fn persist_series_registry_index_with_catalog_sources_and_root_changes(
        &self,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
        added_roots: &[PathBuf],
        removed_roots: &[PathBuf],
    ) -> Result<()> {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        let delta_path = SeriesRegistry::incremental_path(checkpoint_path);
        let delta_dir_path = SeriesRegistry::incremental_dir(checkpoint_path);
        self.registry_persistence_context()
            .persist_series_registry_index(
                checkpoint_path,
                &delta_path,
                &delta_dir_path,
                sources,
                |checkpoint_path, sources| {
                    self.persist_series_registry_catalog_index_with_sources_mode(
                        checkpoint_path,
                        sources,
                        added_roots,
                        removed_roots,
                        false,
                    )
                },
            )
    }

    /// 启动期 delta 重放统计播种（bootstrap 校验发生在实例构造之前，经
    /// `StartupFinalizeState` 传入后并入本实例计数器）。
    pub(in super::super) fn seed_catalog_delta_startup_stats(
        &self,
        stats: registry_catalog::CatalogDeltaStartupStats,
    ) {
        self.observability
            .flush
            .catalog_delta_replayed_total
            .fetch_add(stats.replayed_total, Ordering::Relaxed);
        self.observability
            .flush
            .catalog_delta_apply_fallback_total
            .fetch_add(stats.apply_fallback_total, Ordering::Relaxed);
        self.observability
            .flush
            .catalog_delta_fingerprint_fallback_total
            .fetch_add(stats.fingerprint_fallback_total, Ordering::Relaxed);
    }
}
