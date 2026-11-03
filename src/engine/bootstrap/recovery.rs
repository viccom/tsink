use tracing::warn;

use super::super::tiering::{SegmentInventory, SegmentLaneFamily};
use super::discovery::{StartupDiscoveredState, StartupInventoryState, StartupRegistryState};
use super::finalize::StartupFinalizeState;
use super::planning::StartupPlan;
use super::*;
use crate::engine::segment::{
    load_segment_indexes_from_dirs_startup_recoverable_with_series, LoadedSegmentIndexes,
    StartupQuarantinedSegment,
};

pub(super) struct StartupRecoveryPhase;

pub(super) struct StartupRecoveryState {
    pub(super) registry: StartupRegistryState,
    pub(super) loaded_segments: LoadedSegmentIndexes,
    pub(super) startup_quarantined: Vec<StartupQuarantinedSegment>,
    /// W3-T4：启动期 catalog delta 重放统计（实例构造前产生，经 finalize 播种）。
    pub(super) catalog_delta_stats: registry_catalog::CatalogDeltaStartupStats,
}

impl StartupRecoveryPhase {
    pub(super) fn recover(
        plan: &StartupPlan,
        discovered: StartupDiscoveredState,
    ) -> Result<StartupRecoveryState> {
        let StartupDiscoveredState {
            mut registry,
            mut inventory,
        } = discovered;
        // W4-T5fix (D2)：托底值在 inventory 被部分 move 之前取出。
        let max_dirname_segment_id = inventory.max_dirname_segment_id;

        let (validated_catalog, catalog_delta_stats) = validate_startup_registry_catalog(
            plan.paths().series_index_path.as_deref(),
            &mut registry,
            &inventory,
        );
        let mut loaded_segments = load_startup_segment_state(
            plan,
            &mut registry,
            &mut inventory,
            validated_catalog.as_ref(),
        )?;
        reconcile_startup_registry_snapshot(
            plan.paths().series_index_path.as_deref(),
            plan,
            &mut registry,
            &mut inventory,
            validated_catalog.as_ref(),
            &mut loaded_segments,
        )?;
        floor_next_segment_id_at_max_dirname(&mut loaded_segments, max_dirname_segment_id);

        Ok(StartupRecoveryState {
            registry,
            loaded_segments,
            startup_quarantined: inventory.startup_quarantined,
            catalog_delta_stats,
        })
    }
}

impl StartupRecoveryState {
    pub(super) fn finalize_state(&self) -> StartupFinalizeState {
        StartupFinalizeState {
            force_registry_checkpoint_after_startup: self
                .registry
                .force_registry_checkpoint_after_startup,
            reconcile_registry_with_persisted: self.registry.reconcile_registry_with_persisted,
            catalog_delta_stats: self.catalog_delta_stats,
        }
    }

    pub(super) fn loaded_segments(&self) -> &LoadedSegmentIndexes {
        &self.loaded_segments
    }
}

/// W4-T5fix (D2)：把恢复出的段 id 分配器下界托底到盘上最大 `seg-<hex>`
/// dirname id + 1。
///
/// 恢复侧的 `next_segment_id` 只由成功加载的 manifest 的 `segment_id` 推导；
/// 任何「盘上 dirname 占位 id 超过已计数 manifest id」的实例（中断运行残留、
/// 活目录的非原子拷贝、跨版本遗留、被跳过/隔离/去重丢弃的目录），下一次
/// 创建段就会撞上 `InvalidConfiguration("segment directory already exists")`
/// （W4-T5 真实副本 drill 确定性暴露）。dirname 是盘上唯一不受加载结果
/// 影响的占位事实，而启动本来就要 list 全部 lane × tier × level 的段目录，
/// 该托底零额外文件 IO。`next` 已 ≥ 托底值时行为完全不变。
fn floor_next_segment_id_at_max_dirname(
    loaded_segments: &mut LoadedSegmentIndexes,
    max_dirname_segment_id: u64,
) {
    let dirname_floor = max_dirname_segment_id.saturating_add(1).max(1);
    if dirname_floor <= loaded_segments.next_segment_id {
        return;
    }
    warn!(
        recovered_next_segment_id = loaded_segments.next_segment_id,
        max_dirname_segment_id,
        floored_next_segment_id = dirname_floor,
        "Recovered segment id allocator trails the max on-disk segment dirname; \
         raising the allocator lower bound to avoid publishing a colliding segment"
    );
    loaded_segments.next_segment_id = dirname_floor;
}

/// 启动校验入口：返回校验结论与 W3-T4 delta 重放统计。
///
/// 统计口径：
/// - `replayed_total` / `fingerprint_fallback_total`：校验成功时取自报告
///   （重放条数 / 已应用 delta 的指纹缺失）；
/// - `apply_fallback_total`：盘上存在 delta 但未能干净应用（缺失/损坏/断层/
///   重构失配）→ 丢弃全部 delta 走「失配即重建 + 强制 checkpoint」并计数。
fn validate_startup_registry_catalog(
    series_index_path: Option<&Path>,
    registry: &mut StartupRegistryState,
    inventory: &StartupInventoryState,
) -> (
    Option<registry_catalog::ValidatedRegistryCatalog>,
    registry_catalog::CatalogDeltaStartupStats,
) {
    if !registry.reconcile_registry_with_persisted {
        return (None, registry_catalog::CatalogDeltaStartupStats::default());
    }

    let index_path = series_index_path.expect("persisted registry requires a snapshot path");
    let catalog_sources = registry_catalog::inventory_sources(&inventory.segment_inventory);
    // bootstrap 一次性校验：独立临时段级缓存（无跨拍复用价值，N9b 签参统一）
    let entry_cache = registry_catalog::CatalogEntryCache::default();
    match registry_catalog::validate_registry_catalog_with_delta_report(
        index_path,
        &catalog_sources,
        &entry_cache,
    ) {
        Ok(report) => {
            let stats = registry_catalog::CatalogDeltaStartupStats {
                replayed_total: report
                    .validated
                    .as_ref()
                    .map(|validated| validated.deltas_replayed)
                    .unwrap_or(0),
                apply_fallback_total: u64::from(
                    report.deltas_present && report.validated.is_none(),
                ),
                fingerprint_fallback_total: report
                    .validated
                    .as_ref()
                    .map(|validated| u64::from(validated.fingerprint_fallback))
                    .unwrap_or(0),
            };
            if report.validated.is_none() {
                registry.force_registry_checkpoint();
            }
            (report.validated, stats)
        }
        Err(err) => {
            warn!(
                path = %index_path.display(),
                error = %err,
                "Failed to validate persisted registry catalog sidecar; rebuilding startup metadata from segments"
            );
            registry.force_registry_checkpoint();
            (
                None,
                registry_catalog::CatalogDeltaStartupStats {
                    apply_fallback_total: u64::from(
                        registry_catalog::count_catalog_delta_files(index_path) > 0,
                    ),
                    ..registry_catalog::CatalogDeltaStartupStats::default()
                },
            )
        }
    }
}

fn load_startup_segment_state(
    plan: &StartupPlan,
    registry: &mut StartupRegistryState,
    inventory: &mut StartupInventoryState,
    validated_catalog: Option<&registry_catalog::ValidatedRegistryCatalog>,
) -> Result<LoadedSegmentIndexes> {
    let (numeric_lane_enabled, blob_lane_enabled) = plan.lane_flags();
    let load_segment_series = should_load_segment_series(validated_catalog);
    let (mut loaded_segments, load_quarantined) = load_startup_segments_with_recovery(
        &inventory.segment_inventory,
        load_segment_series,
        numeric_lane_enabled,
        blob_lane_enabled,
    )?;

    if inventory.apply_quarantines(load_quarantined) {
        registry.force_registry_checkpoint();
        if registry.reconcile_registry_with_persisted || !load_segment_series {
            registry.discard_persisted_registry();
            let (reloaded_segments, reloaded_quarantined) = load_startup_segments_with_recovery(
                &inventory.segment_inventory,
                true,
                numeric_lane_enabled,
                blob_lane_enabled,
            )?;
            loaded_segments = reloaded_segments;
            if inventory.apply_quarantines(reloaded_quarantined) {
                registry.force_registry_checkpoint();
            }
        }
    }

    Ok(loaded_segments)
}

fn reconcile_startup_registry_snapshot(
    series_index_path: Option<&Path>,
    plan: &StartupPlan,
    registry: &mut StartupRegistryState,
    inventory: &mut StartupInventoryState,
    validated_catalog: Option<&registry_catalog::ValidatedRegistryCatalog>,
    loaded_segments: &mut LoadedSegmentIndexes,
) -> Result<()> {
    let (
        Some(loaded_registry),
        Some(registry_catalog::ValidatedRegistryCatalog {
            series_fingerprint: Some(series_fingerprint),
            ..
        }),
    ) = (registry.persisted_registry.as_ref(), validated_catalog)
    else {
        return Ok(());
    };

    if let Err(err) = registry_catalog::validate_registry_snapshot(
        &loaded_registry.registry,
        &loaded_segments.indexed_segments,
        series_fingerprint,
    ) {
        let index_path = series_index_path.expect("persisted registry requires a snapshot path");
        warn!(
            path = %index_path.display(),
            error = %err,
            "Persisted registry snapshot conflicts with persisted segment metadata; rebuilding startup metadata from segments"
        );
        registry.force_registry_checkpoint();
        registry.discard_persisted_registry();

        let (numeric_lane_enabled, blob_lane_enabled) = plan.lane_flags();
        let (reloaded_segments, reloaded_quarantined) = load_startup_segments_with_recovery(
            &inventory.segment_inventory,
            true,
            numeric_lane_enabled,
            blob_lane_enabled,
        )?;
        *loaded_segments = reloaded_segments;
        if inventory.apply_quarantines(reloaded_quarantined) {
            registry.force_registry_checkpoint();
        }
    }

    Ok(())
}

fn should_load_segment_series(
    validated_catalog: Option<&registry_catalog::ValidatedRegistryCatalog>,
) -> bool {
    !matches!(
        validated_catalog,
        Some(registry_catalog::ValidatedRegistryCatalog {
            series_fingerprint: Some(_),
            ..
        })
    )
}

fn load_startup_segments_with_recovery(
    inventory: &SegmentInventory,
    load_series: bool,
    numeric_lane_enabled: bool,
    blob_lane_enabled: bool,
) -> Result<(LoadedSegmentIndexes, Vec<StartupQuarantinedSegment>)> {
    let loaded_numeric = load_segment_indexes_from_dirs_startup_recoverable_with_series(
        inventory.roots_for_lane(SegmentLaneFamily::Numeric),
        load_series,
    )?;
    let loaded_blob = load_segment_indexes_from_dirs_startup_recoverable_with_series(
        inventory.roots_for_lane(SegmentLaneFamily::Blob),
        load_series,
    )?;
    let mut quarantined = loaded_numeric.quarantined;
    quarantined.extend(loaded_blob.quarantined);
    Ok((
        super::merge_loaded_segment_indexes(
            loaded_numeric.loaded,
            loaded_blob.loaded,
            numeric_lane_enabled,
            blob_lane_enabled,
        )?,
        quarantined,
    ))
}
