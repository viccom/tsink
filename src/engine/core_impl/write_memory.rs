use super::*;

// Budget-scope note (2026-08-29 load-test follow-up): these component helpers
// track memory categories that are EXCLUDED from the budget criterion
// (`used_bytes` covers only evictable active heads plus sealed chunks pending
// flush). They maintain the per-component observability counters and the
// `shared_used_bytes` aggregate (surfaced as `excluded_bytes`), but never touch
// the budget criterion itself.
pub(super) fn add_included_memory_component_bytes(
    accounting_enabled: bool,
    component: &AtomicU64,
    shared_used_bytes: &AtomicU64,
    bytes: usize,
) {
    if !accounting_enabled || bytes == 0 {
        return;
    }

    let increment = saturating_u64_from_usize(bytes);
    component.fetch_add(increment, Ordering::AcqRel);
    shared_used_bytes.fetch_add(increment, Ordering::AcqRel);
}

pub(super) fn sub_included_memory_component_bytes(
    accounting_enabled: bool,
    component: &AtomicU64,
    shared_used_bytes: &AtomicU64,
    bytes: usize,
) {
    if !accounting_enabled || bytes == 0 {
        return;
    }

    let decrement = saturating_u64_from_usize(bytes);
    saturating_sub_fetch(component, decrement);
    saturating_sub_fetch(shared_used_bytes, decrement);
}

pub(super) fn reset_included_memory_component_bytes(
    accounting_enabled: bool,
    component: &AtomicU64,
    shared_used_bytes: &AtomicU64,
    bytes: usize,
) {
    if !accounting_enabled {
        return;
    }

    let next = saturating_u64_from_usize(bytes);
    let previous = component.swap(next, Ordering::AcqRel);
    if next >= previous {
        let delta = next.saturating_sub(previous);
        shared_used_bytes.fetch_add(delta, Ordering::AcqRel);
    } else {
        let delta = previous.saturating_sub(next);
        saturating_sub_fetch(shared_used_bytes, delta);
    }
}

pub(super) fn with_included_memory_delta<T, R>(
    accounting_enabled: bool,
    component: &AtomicU64,
    shared_used_bytes: &AtomicU64,
    state: &mut T,
    measure: impl Fn(&T) -> usize,
    mutate: impl FnOnce(&mut T) -> R,
) -> R {
    if !accounting_enabled {
        return mutate(state);
    }

    let before = measure(state);
    let result = mutate(state);
    let after = measure(state);
    if after >= before {
        add_included_memory_component_bytes(
            true,
            component,
            shared_used_bytes,
            after.saturating_sub(before),
        );
    } else {
        sub_included_memory_component_bytes(
            true,
            component,
            shared_used_bytes,
            before.saturating_sub(after),
        );
    }
    result
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct RegistryMemoryContext<'a> {
    pub(in crate::engine::storage_engine) accounting_enabled: bool,
    pub(in crate::engine::storage_engine) registry: &'a RwLock<SeriesRegistry>,
    pub(in crate::engine::storage_engine) registry_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) shared_used_bytes: &'a AtomicU64,
}

impl<'a> RegistryMemoryContext<'a> {
    pub(in crate::engine::storage_engine) fn sync_registry_memory_usage(self) {
        let registry_used = self.registry.read().memory_usage_bytes();
        reset_included_memory_component_bytes(
            self.accounting_enabled,
            self.registry_used_bytes,
            self.shared_used_bytes,
            registry_used,
        );
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct RegistryBookkeepingContext<'a> {
    pub(in crate::engine::storage_engine) accounting_enabled: bool,
    pub(in crate::engine::storage_engine) pending_series_ids: &'a RwLock<BTreeSet<SeriesId>>,
    pub(in crate::engine::storage_engine) metadata_used_bytes: &'a AtomicU64,
    pub(in crate::engine::storage_engine) shared_used_bytes: &'a AtomicU64,
}

impl<'a> RegistryBookkeepingContext<'a> {
    pub(in crate::engine::storage_engine) fn mark_series_pending<I>(self, series_ids: I)
    where
        I: IntoIterator<Item = SeriesId>,
    {
        let mut series_ids = series_ids.into_iter().collect::<Vec<_>>();
        series_ids.sort_unstable();
        series_ids.dedup();
        if series_ids.is_empty() {
            return;
        }

        let mut pending = self.pending_series_ids.write();
        with_included_memory_delta(
            self.accounting_enabled,
            self.metadata_used_bytes,
            self.shared_used_bytes,
            &mut pending,
            |pending| ChunkStorage::btree_set_series_id_memory_usage_bytes(pending),
            |pending| {
                for series_id in series_ids {
                    pending.insert(series_id);
                }
            },
        );
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct ShardMemoryAccountingContext<'a> {
    pub(in crate::engine::storage_engine) accounting_enabled: bool,
    pub(in crate::engine::storage_engine) used_bytes_by_shard:
        &'a [AtomicU64; IN_MEMORY_SHARD_COUNT],
    pub(in crate::engine::storage_engine) used_bytes: &'a AtomicU64,
}

impl<'a> ShardMemoryAccountingContext<'a> {
    pub(in crate::engine::storage_engine) fn account_memory_delta(
        self,
        shard_idx: usize,
        delta: MemoryDeltaBytes,
    ) {
        if !self.accounting_enabled {
            return;
        }

        if delta.added_bytes >= delta.removed_bytes {
            let increment =
                saturating_u64_from_usize(delta.added_bytes.saturating_sub(delta.removed_bytes));
            self.used_bytes_by_shard[shard_idx].fetch_add(increment, Ordering::AcqRel);
            self.used_bytes.fetch_add(increment, Ordering::AcqRel);
        } else {
            let decrement =
                saturating_u64_from_usize(delta.removed_bytes.saturating_sub(delta.added_bytes));
            // 饱和减（BUG#3 家族收敛）：与 ChunkStorage::sub_memory_usage_bytes
            // 同原语——计数器永不越界，真实值由准入路径 reconcile 纠正
            saturating_sub_fetch(&self.used_bytes_by_shard[shard_idx], decrement);
            saturating_sub_fetch(self.used_bytes, decrement);
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct PersistedSealedBudgetContext<'a> {
    pub(in crate::engine::storage_engine) memory: ShardMemoryAccountingContext<'a>,
    pub(in crate::engine::storage_engine) backpressure_lock: &'a Mutex<()>,
    pub(in crate::engine::storage_engine) has_persisted_lanes: bool,
    pub(in crate::engine::storage_engine) persisted_chunk_watermarks:
        &'a RwLock<HashMap<SeriesId, u64>>,
    pub(in crate::engine::storage_engine) sealed_chunks:
        &'a [SealedChunkShard; IN_MEMORY_SHARD_COUNT],
    pub(in crate::engine::storage_engine) flush_metrics:
        &'a super::super::metrics::FlushObservabilityCounters,
}

#[derive(Clone, Copy)]
pub(in crate::engine::storage_engine) struct SealedChunkPublishContext<'a> {
    pub(in crate::engine::storage_engine) sealed_chunks:
        &'a [SealedChunkShard; IN_MEMORY_SHARD_COUNT],
    pub(in crate::engine::storage_engine) next_chunk_sequence: &'a AtomicU64,
    pub(in crate::engine::storage_engine) memory: ShardMemoryAccountingContext<'a>,
    #[cfg(test)]
    pub(in crate::engine::storage_engine) pre_publish_hook:
        &'a RwLock<Option<Arc<IngestCommitHook>>>,
}

impl<'a> SealedChunkPublishContext<'a> {
    pub(in crate::engine::storage_engine) fn append_finalized_chunks_to_sealed_shard<I>(
        self,
        shard_idx: usize,
        finalized: I,
    ) where
        I: IntoIterator<Item = (SeriesId, Chunk)>,
    {
        let account_memory = self.memory.accounting_enabled;
        #[cfg(test)]
        if let Some(hook) = self.pre_publish_hook.read().clone() {
            hook();
        }
        let mut sealed = self.sealed_chunks[shard_idx].write();
        let mut memory_delta = MemoryDeltaBytes::default();
        for (series_id, chunk) in finalized {
            let chunk = chunk.into_sealed_storage();
            let chunk_bytes = if account_memory {
                ChunkStorage::chunk_memory_usage_bytes(&chunk)
            } else {
                0
            };
            let sequence = self.next_chunk_sequence.fetch_add(1, Ordering::SeqCst);
            let key = SealedChunkKey::from_chunk(&chunk, sequence);
            let replaced = sealed
                .entry(series_id)
                .or_default()
                .insert(key, Arc::new(chunk));
            if account_memory {
                memory_delta.record_replacement(chunk_bytes, replaced.as_ref(), |chunk| {
                    ChunkStorage::chunk_memory_usage_bytes(chunk)
                });
            }
        }
        drop(sealed);
        self.memory.account_memory_delta(shard_idx, memory_delta);
    }
}

#[cfg(test)]
mod saturating_tests {
    use super::*;

    /// BUG#3 家族收敛回归（2026-10-30 审查）：写路径记账上下文
    /// （`ShardMemoryAccountingContext::account_memory_delta`）与排除域组件
    /// 计数（`sub_included_memory_component_bytes`）曾直接 `fetch_sub`——
    /// removed > current 时下溢成 2^64-x 巨值。必须与 flush 侧
    /// `sub_memory_usage_bytes` 的饱和减（2026-10-29 生产修复）同语义：
    /// 宁可停在 0（低估，由 reconcile 纠正）也不越界成巨值。
    #[test]
    fn accounting_subtraction_saturates_instead_of_wrapping() {
        let used = AtomicU64::new(100);
        let shards: [AtomicU64; IN_MEMORY_SHARD_COUNT] =
            std::array::from_fn(|i| AtomicU64::new(if i == 0 { 100 } else { 0 }));
        let ctx = ShardMemoryAccountingContext {
            accounting_enabled: true,
            used_bytes_by_shard: &shards,
            used_bytes: &used,
        };
        ctx.account_memory_delta(0, MemoryDeltaBytes::from_totals(0, 240));
        assert_eq!(
            used.load(Ordering::Acquire),
            0,
            "全局 used_bytes 必须饱和到 0"
        );
        assert_eq!(shards[0].load(Ordering::Acquire), 0, "分片计数必须饱和到 0");

        let component = AtomicU64::new(50);
        let shared = AtomicU64::new(50);
        sub_included_memory_component_bytes(true, &component, &shared, 90);
        assert_eq!(
            component.load(Ordering::Acquire),
            0,
            "排除域组件计数必须饱和到 0"
        );
        assert_eq!(
            shared.load(Ordering::Acquire),
            0,
            "shared_used_bytes 必须饱和到 0"
        );
    }
}
