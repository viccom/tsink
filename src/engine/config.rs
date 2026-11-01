use super::tiering::{PersistedSegmentTier, SegmentLaneFamily, SEGMENT_CATALOG_FILE_NAME};
use super::*;

#[derive(Debug, Clone)]
pub(super) struct TieredStorageConfig {
    pub(super) object_store_root: PathBuf,
    pub(super) segment_catalog_path: Option<PathBuf>,
    pub(super) mirror_hot_segments: bool,
    pub(super) hot_retention_window: i64,
    pub(super) warm_retention_window: i64,
}

impl TieredStorageConfig {
    pub(super) fn lane_path(&self, lane: SegmentLaneFamily, tier: PersistedSegmentTier) -> PathBuf {
        let tier_name = match tier {
            PersistedSegmentTier::Hot => "hot",
            PersistedSegmentTier::Warm => "warm",
            PersistedSegmentTier::Cold => "cold",
        };
        self.object_store_root
            .join(tier_name)
            .join(lane.root_name())
    }
}

#[derive(Debug, Clone)]
pub(super) struct ChunkStorageOptions {
    pub(super) timestamp_precision: TimestampPrecision,
    pub(super) retention_window: i64,
    pub(super) future_skew_window: i64,
    pub(super) retention_enforced: bool,
    pub(super) runtime_mode: StorageRuntimeMode,
    pub(super) partition_window: i64,
    pub(super) max_active_partition_heads_per_series: usize,
    pub(super) max_writers: usize,
    pub(super) write_timeout: Duration,
    pub(super) flush_interval: Duration,
    pub(super) seal_policy: SealPolicy,
    pub(super) catalog_persist_interval: Duration,
    pub(super) memory_budget_bytes: u64,
    pub(super) persisted_mmap_budget_bytes: usize,
    /// Global byte budget for resident timestamp-search-index cache entries.
    /// Default:
    /// [`DEFAULT_TIMESTAMP_INDEX_CACHE_BUDGET_BYTES`](crate::storage::DEFAULT_TIMESTAMP_INDEX_CACHE_BUDGET_BYTES).
    pub(super) timestamp_index_cache_budget_bytes: usize,
    pub(super) cardinality_limit: usize,
    pub(super) wal_size_limit_bytes: u64,
    pub(super) admission_poll_interval: Duration,
    /// Park interval between background compaction passes when the last pass
    /// drained to quiescence (backlogged passes park on the shorter
    /// `drain`-cadence interval instead). Default:
    /// [`DEFAULT_COMPACTION_INTERVAL`](super::DEFAULT_COMPACTION_INTERVAL).
    pub(super) compaction_interval: Duration,
    /// Time budget one background drain pass may spend starting new window
    /// compactions before yielding the compaction gate. Default:
    /// [`COMPACTION_DRAIN_TIME_BUDGET`](super::COMPACTION_DRAIN_TIME_BUDGET).
    pub(super) drain_time_budget: Duration,
    /// How many same-level segments one compaction pass may merge. Default:
    /// [`DEFAULT_SOURCE_WINDOW_SEGMENTS`](crate::engine::compactor::DEFAULT_SOURCE_WINDOW_SEGMENTS).
    pub(super) source_window_segments: usize,
    /// Point budget for one compaction output segment, independent of the
    /// chunk point cap. Default:
    /// [`DEFAULT_OUTPUT_SEGMENT_POINT_BUDGET`](crate::engine::compactor::DEFAULT_OUTPUT_SEGMENT_POINT_BUDGET).
    pub(super) output_segment_point_budget: usize,
    pub(super) background_threads_enabled: bool,
    pub(super) background_fail_fast: bool,
    pub(super) metadata_shard_count: Option<u32>,
    pub(super) remote_segment_cache_policy: RemoteSegmentCachePolicy,
    pub(super) remote_segment_refresh_interval: Duration,
    pub(super) tiered_storage: Option<TieredStorageConfig>,
    #[cfg(test)]
    pub(super) current_time_override: Option<i64>,
}

/// 后台 bounded flush 对「当前 partition head」的 seal 门控（P1）。
///
/// 背景：`BackgroundBounded` 每拍（默认 250ms）无条件 seal 非空 head，会让段粒度等于
/// 「拍长 × 摄入率」——低速现场实测 5.9 点/段；而每次 seal 都要全量重写注册表 catalog
/// （成本 ∝ 累计段数），导致 CPU 与写放大随 uptime 线性上涨。门控让 head 攒批：
/// 只有满足下列任一条件才 seal。满仓 rotate、分区滚动、准入压力与显式/关闭 flush
/// 不受此门控影响。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SealPolicy {
    /// 未满仓 head 的最小 seal 点数；`0` = 由 `point_cap/8` 推导（最低 1）。
    pub(super) min_points: usize,
    /// head 最长存活时间；到期强制 seal（低速下也保证 WAL 与内存有界）。
    /// `Duration::ZERO` = 立即到期（历史语义）。
    pub(super) max_head_age: Duration,
    /// 未落盘 WAL 字节上限；达到即 seal（高速率下的硬安全阀）。
    pub(super) max_unsealed_wal_bytes: u64,
}

impl SealPolicy {
    /// 关闭门控：任何非空 head 立即 seal（修复前的历史语义；测试夹具显式选用）。
    #[cfg(test)]
    pub(super) fn immediate() -> Self {
        Self {
            min_points: 1,
            max_head_age: Duration::ZERO,
            max_unsealed_wal_bytes: 1,
        }
    }

    /// 实际生效的最小点数：显式值优先，否则按 point_cap/8 推导。
    pub(super) fn effective_min_points(&self, point_cap: usize) -> usize {
        if self.min_points == 0 {
            (point_cap / 8).max(1)
        } else {
            self.min_points
        }
    }
}

impl Default for SealPolicy {
    fn default() -> Self {
        Self {
            min_points: 0,
            max_head_age: DEFAULT_MAX_HEAD_AGE,
            max_unsealed_wal_bytes: DEFAULT_MAX_UNSEALED_WAL_BYTES,
        }
    }
}

impl Default for ChunkStorageOptions {
    fn default() -> Self {
        Self {
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: duration_to_timestamp_units(
                DEFAULT_RETENTION,
                TimestampPrecision::Nanoseconds,
            ),
            future_skew_window: duration_to_timestamp_units(
                DEFAULT_FUTURE_SKEW_ALLOWANCE,
                TimestampPrecision::Nanoseconds,
            )
            .max(0),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: duration_to_timestamp_units(
                DEFAULT_PARTITION_DURATION,
                TimestampPrecision::Nanoseconds,
            )
            .max(1),
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: crate::cgroup::default_workers_limit().max(1),
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            seal_policy: SealPolicy::default(),
            catalog_persist_interval: DEFAULT_CATALOG_PERSIST_INTERVAL,
            memory_budget_bytes: u64::MAX,
            persisted_mmap_budget_bytes: crate::storage::DEFAULT_PERSISTED_MMAP_BUDGET_BYTES,
            timestamp_index_cache_budget_bytes:
                crate::storage::DEFAULT_TIMESTAMP_INDEX_CACHE_BUDGET_BYTES,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            drain_time_budget: COMPACTION_DRAIN_TIME_BUDGET,
            source_window_segments: crate::engine::compactor::DEFAULT_SOURCE_WINDOW_SEGMENTS,
            output_segment_point_budget:
                crate::engine::compactor::DEFAULT_OUTPUT_SEGMENT_POINT_BUDGET,
            background_threads_enabled: true,
            background_fail_fast: true,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval:
                crate::storage::DEFAULT_REMOTE_SEGMENT_REFRESH_INTERVAL,
            tiered_storage: None,
            #[cfg(test)]
            current_time_override: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct StoragePathLayout {
    pub(super) numeric_lane_path: Option<PathBuf>,
    pub(super) blob_lane_path: Option<PathBuf>,
    pub(super) series_index_path: Option<PathBuf>,
    pub(super) wal_path: Option<PathBuf>,
}

impl From<&StorageBuilder> for StoragePathLayout {
    fn from(builder: &StorageBuilder) -> Self {
        let base_data_path = if builder.runtime_mode() == StorageRuntimeMode::ComputeOnly {
            None
        } else {
            builder.data_path().map(|path| path.to_path_buf())
        };
        let (numeric_lane_path, blob_lane_path) = if let Some(base_path) = &base_data_path {
            (
                Some(base_path.join(NUMERIC_LANE_ROOT)),
                Some(base_path.join(BLOB_LANE_ROOT)),
            )
        } else {
            (None, None)
        };

        let series_index_path = base_data_path
            .as_ref()
            .map(|base_path| base_path.join(SERIES_INDEX_FILE_NAME));
        let wal_path = base_data_path
            .as_ref()
            .map(|base_path| base_path.join(WAL_DIR_NAME));

        Self {
            numeric_lane_path,
            blob_lane_path,
            series_index_path,
            wal_path,
        }
    }
}

impl From<&StorageBuilder> for ChunkStorageOptions {
    fn from(builder: &StorageBuilder) -> Self {
        let timestamp_precision = builder.timestamp_precision();
        let runtime_mode = builder.runtime_mode();
        let has_data_path =
            builder.data_path().is_some() && runtime_mode == StorageRuntimeMode::ReadWrite;
        let needs_compute_only_refresh_worker = runtime_mode == StorageRuntimeMode::ComputeOnly
            && builder.object_store_path().is_some();
        Self {
            timestamp_precision,
            retention_window: duration_to_timestamp_units(builder.retention(), timestamp_precision),
            future_skew_window: duration_to_timestamp_units(
                DEFAULT_FUTURE_SKEW_ALLOWANCE,
                timestamp_precision,
            )
            .max(0),
            retention_enforced: builder.retention_enforced(),
            runtime_mode,
            partition_window: duration_to_timestamp_units(
                builder.partition_duration(),
                timestamp_precision,
            )
            .max(1),
            max_active_partition_heads_per_series: builder.max_active_partition_heads_per_series(),
            max_writers: builder.max_writers(),
            write_timeout: builder.write_timeout(),
            flush_interval: builder.flush_interval(),
            seal_policy: SealPolicy {
                min_points: builder.seal_min_points(),
                max_head_age: builder.max_head_age(),
                max_unsealed_wal_bytes: builder.max_unsealed_wal_bytes(),
            },
            catalog_persist_interval: builder.catalog_persist_interval(),
            memory_budget_bytes: builder.memory_limit_bytes().min(u64::MAX as usize) as u64,
            persisted_mmap_budget_bytes: builder.persisted_mmap_budget_bytes(),
            timestamp_index_cache_budget_bytes: builder.timestamp_index_cache_budget_bytes(),
            cardinality_limit: builder.cardinality_limit(),
            wal_size_limit_bytes: builder.wal_size_limit_bytes().min(u64::MAX as usize) as u64,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: builder.compaction_interval(),
            drain_time_budget: builder.drain_time_budget(),
            source_window_segments: builder.source_window_segments(),
            output_segment_point_budget: builder.output_segment_point_budget(),
            background_threads_enabled: {
                let enabled = has_data_path || needs_compute_only_refresh_worker;
                #[cfg(test)]
                {
                    builder
                        .background_threads_enabled_override_for_tests()
                        .unwrap_or(enabled)
                }
                #[cfg(not(test))]
                {
                    enabled
                }
            },
            background_fail_fast: builder.background_fail_fast(),
            metadata_shard_count: builder.metadata_shard_count().filter(|count| *count > 0),
            remote_segment_cache_policy: builder.remote_segment_cache_policy(),
            remote_segment_refresh_interval: builder.remote_segment_refresh_interval(),
            tiered_storage: builder.object_store_path().map(|path| TieredStorageConfig {
                object_store_root: path.to_path_buf(),
                segment_catalog_path: builder
                    .data_path()
                    .filter(|_| runtime_mode == StorageRuntimeMode::ReadWrite)
                    .map(|data_path| data_path.join(SEGMENT_CATALOG_FILE_NAME)),
                mirror_hot_segments: runtime_mode == StorageRuntimeMode::ReadWrite
                    && builder.mirror_hot_segments_to_object_store(),
                hot_retention_window: duration_to_timestamp_units(
                    builder
                        .hot_tier_retention()
                        .unwrap_or_else(|| builder.retention()),
                    timestamp_precision,
                )
                .max(0),
                warm_retention_window: duration_to_timestamp_units(
                    builder
                        .warm_tier_retention()
                        .unwrap_or_else(|| builder.retention()),
                    timestamp_precision,
                )
                .max(0),
            }),
            #[cfg(test)]
            current_time_override: builder.current_time_override_for_tests(),
        }
    }
}
