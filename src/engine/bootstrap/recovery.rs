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
