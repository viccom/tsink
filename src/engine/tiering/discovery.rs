use std::path::Path;

use tracing::warn;

use super::super::config::TieredStorageConfig;
use super::super::{Result, TsinkError};
use super::inventory::SegmentInventoryAccumulator;
use super::layout::SegmentScanTarget;
use super::{SegmentInventory, SegmentInventoryEntry, SegmentPathResolver};
use crate::engine::segment::{
    list_segment_dirs, quarantine_invalid_startup_segment, read_segment_manifest,
    segment_validation_error, segment_validation_error_message, SegmentValidationContext,
    StartupQuarantinedSegment,
};

#[derive(Debug, Clone)]
pub(in crate::engine::storage_engine) struct StartupRecoveredSegmentInventory {
    pub(in crate::engine::storage_engine) inventory: SegmentInventory,
    pub(in crate::engine::storage_engine) quarantined: Vec<StartupQuarantinedSegment>,
    /// W4-T5fix (D2)：启动扫描覆盖的全部 lane × tier × level 里最大的
    /// `seg-<hex>` dirname id（无任何段目录时为 0）。恢复分配器下界的托底
    /// 依据——manifest 加载计数会漏掉被跳过/隔离/去重丢弃的目录，dirname
    /// 是盘上唯一不被任何加载结果影响的占位事实。复用启动已有的 dir
    /// listing 计算，零额外文件 IO（纯字符串解析）。
    pub(in crate::engine::storage_engine) max_dirname_segment_id: u64,
}

/// Parses the segment id encoded in a `seg-<hex>` segment dirname. Mirrors
/// the `seg-{id:016x}` writer format (`segment::types::SegmentLayout`) and
/// the hex parsing precedent in `registry_catalog` replay; non-`seg-` names
/// and non-hex suffixes (staging/quarantine debris) yield `None`.
fn segment_dirname_segment_id(file_name: &std::ffi::OsStr) -> Option<u64> {
    let hex = file_name.to_str()?.strip_prefix("seg-")?;
    u64::from_str_radix(hex, 16).ok()
}

pub(in crate::engine::storage_engine) fn build_segment_inventory_startup_recoverable(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
) -> Result<StartupRecoveredSegmentInventory> {
    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let mut deduped = SegmentInventoryAccumulator::default();
    let mut quarantined = Vec::new();
    let mut max_dirname_segment_id = 0u64;

    for target in resolver.inventory_scan_targets() {
        let target_max =
            scan_startup_segment_roots_recoverable(&target, &mut deduped, &mut quarantined)?;
        max_dirname_segment_id = max_dirname_segment_id.max(target_max);
    }

    Ok(StartupRecoveredSegmentInventory {
        inventory: deduped.finish(),
        quarantined,
        max_dirname_segment_id,
    })
}

pub(in crate::engine::storage_engine) fn build_segment_inventory_runtime_strict(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
) -> Result<SegmentInventory> {
    build_segment_inventory_fail_on_invalid(
        numeric_lane_path,
        blob_lane_path,
        tiered_storage,
        SegmentValidationContext::RuntimeRefresh,
    )
}

pub(in crate::engine::storage_engine) fn build_segment_inventory_fail_on_invalid(
    numeric_lane_path: Option<&Path>,
    blob_lane_path: Option<&Path>,
    tiered_storage: Option<&TieredStorageConfig>,
    context: SegmentValidationContext,
) -> Result<SegmentInventory> {
    let resolver = SegmentPathResolver::new(numeric_lane_path, blob_lane_path, tiered_storage);
    let mut deduped = SegmentInventoryAccumulator::default();

    for target in resolver.inventory_scan_targets() {
        scan_segment_roots(&target, context, &mut deduped)?;
    }

    Ok(deduped.finish())
}

fn scan_segment_roots(
    target: &SegmentScanTarget,
    context: SegmentValidationContext,
    deduped: &mut SegmentInventoryAccumulator,
) -> Result<()> {
    for root in list_segment_dirs(&target.base_path)? {
        let manifest = match read_segment_manifest(&root) {
            Ok(manifest) => manifest,
            Err(err) if is_not_found_error(&err) => {
                warn!(
                    path = %root.display(),
                    error = %err,
                    "Segment directory disappeared during tiered inventory scan; skipping"
                );
                continue;
            }
            Err(TsinkError::DataCorruption(msg)) => {
                return Err(segment_validation_error(&root, context, &msg));
            }
            Err(err) => return Err(err),
        };

        deduped.insert(SegmentInventoryEntry {
            lane: target.lane,
            tier: target.tier,
            root,
            manifest,
        });
    }

    Ok(())
}

fn scan_startup_segment_roots_recoverable(
    target: &SegmentScanTarget,
    deduped: &mut SegmentInventoryAccumulator,
    quarantined: &mut Vec<StartupQuarantinedSegment>,
) -> Result<u64> {
    let mut max_dirname_segment_id = 0u64;
    for root in list_segment_dirs(&target.base_path)? {
        if let Some(dirname_id) = root.file_name().and_then(segment_dirname_segment_id) {
            max_dirname_segment_id = max_dirname_segment_id.max(dirname_id);
        }
        let manifest = match read_segment_manifest(&root) {
            Ok(manifest) => manifest,
            Err(err) if is_not_found_error(&err) => {
                warn!(
                    path = %root.display(),
                    error = %err,
                    "Segment directory disappeared during tiered inventory scan; skipping"
                );
                continue;
            }
            Err(err) => {
                let Some(details) = segment_validation_error_message(&err) else {
                    return Err(err);
                };
                quarantined.push(quarantine_invalid_startup_segment(&root, &details)?);
                continue;
            }
        };

        deduped.insert(SegmentInventoryEntry {
            lane: target.lane,
            tier: target.tier,
            root,
            manifest,
        });
    }

    Ok(max_dirname_segment_id)
}

fn is_not_found_error(err: &TsinkError) -> bool {
    // Mirror segment::validation::is_not_found_error: IoWithPath carries the
    // same NotFound source kind for concurrently removed segment roots.
    match err {
        TsinkError::Io(io_err) => io_err.kind() == std::io::ErrorKind::NotFound,
        TsinkError::IoWithPath { source, .. } => source.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}
