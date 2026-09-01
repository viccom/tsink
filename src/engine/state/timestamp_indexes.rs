//! Budgeted cache of lazily built timestamp search indexes for persisted
//! chunks, mirroring the segment-map registry's governance: a global byte
//! budget with LRU eviction, transparent rebuild of evicted entries from
//! the chunk payload on the next visibility read, and per-slot clearing
//! when compaction replacement removes a segment from the persisted index.
//!
//! Locking: the cache owns its own `RwLock` and is always acquired nested
//! inside a persisted-index lock (read or write), never the reverse; cache
//! methods never touch the persisted index, so no lock-order cycle is
//! possible.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::engine::encoder::TimestampSearchIndex;

struct TimestampIndexEntry {
    segment_slot: usize,
    index: Option<Arc<TimestampSearchIndex>>,
    last_used: AtomicU64,
}

struct TimestampIndexCacheInner {
    entries: HashMap<u64, TimestampIndexEntry>,
}

pub(in crate::engine::storage_engine) struct TimestampIndexCache {
    budget_bytes: usize,
    clock: AtomicU64,
    resident_bytes: AtomicU64,
    inner: RwLock<TimestampIndexCacheInner>,
}

impl TimestampIndexCache {
    pub(in crate::engine::storage_engine) fn new(budget_bytes: usize) -> Self {
        Self {
            budget_bytes,
            clock: AtomicU64::new(0),
            resident_bytes: AtomicU64::new(0),
            inner: RwLock::new(TimestampIndexCacheInner {
                entries: HashMap::new(),
            }),
        }
    }

    /// Cache-held index bytes. Mirrored into the memory-accounting
    /// component exported as the persisted-index gauge.
    pub(in crate::engine::storage_engine) fn resident_bytes(&self) -> &AtomicU64 {
        &self.resident_bytes
    }

    /// Approximate structural overhead (entries and bookkeeping) for the
    /// included persisted-index memory component.
    pub(in crate::engine::storage_engine) fn structures_memory_usage_bytes(&self) -> usize {
        let inner = self.inner.read();
        std::mem::size_of::<Self>()
            .saturating_add(std::mem::size_of::<TimestampIndexCacheInner>())
            .saturating_add(
                inner
                    .entries
                    .len()
                    .saturating_mul(std::mem::size_of::<TimestampIndexEntry>()),
            )
    }

    fn entry_index_bytes(index: &TimestampSearchIndex) -> u64 {
        u64::try_from(index.memory_usage_bytes()).unwrap_or(u64::MAX)
    }

    fn touch(entry: &TimestampIndexEntry, clock: &AtomicU64) {
        let stamp = clock.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        entry.last_used.store(stamp, Ordering::Release);
    }

    /// Returns the cached index for a chunk sequence, refreshing its LRU
    /// stamp. `None` (miss or pre-seated placeholder) means the caller must
    /// rebuild from the chunk payload and pass the result to
    /// [`Self::install`].
    pub(in crate::engine::storage_engine) fn get(
        &self,
        sequence: u64,
    ) -> Option<Arc<TimestampSearchIndex>> {
        let inner = self.inner.read();
        let entry = inner.entries.get(&sequence)?;
        Self::touch(entry, &self.clock);
        entry.index.clone()
    }

    /// Installs a freshly built index (or fills a pre-seeded placeholder),
    /// returning the winning cached entry so concurrent double-builds
    /// converge on one instance.
    pub(in crate::engine::storage_engine) fn install(
        &self,
        sequence: u64,
        segment_slot: usize,
        index: Arc<TimestampSearchIndex>,
    ) -> Arc<TimestampSearchIndex> {
        let mut inner = self.inner.write();
        let entry = inner
            .entries
            .entry(sequence)
            .or_insert_with(|| TimestampIndexEntry {
                segment_slot,
                index: None,
                last_used: AtomicU64::new(0),
            });
        if let Some(existing) = entry.index.as_ref() {
            Self::touch(entry, &self.clock);
            return Arc::clone(existing);
        }
        self.resident_bytes
            .fetch_add(Self::entry_index_bytes(&index), Ordering::Release);
        entry.index = Some(Arc::clone(&index));
        Self::touch(entry, &self.clock);
        self.enforce_budget(&mut inner);
        index
    }

    /// Clears every cached entry for a segment slot; compaction replacement
    /// removes the whole segment, so its chunk indexes must not survive.
    pub(in crate::engine::storage_engine) fn remove_for_slot(&self, segment_slot: usize) {
        let mut inner = self.inner.write();
        let mut released_bytes = 0u64;
        inner.entries.retain(|_, entry| {
            if entry.segment_slot != segment_slot {
                return true;
            }
            if let Some(index) = entry.index.as_ref() {
                released_bytes = released_bytes.saturating_add(Self::entry_index_bytes(index));
            }
            false
        });
        if released_bytes > 0 {
            self.resident_bytes
                .fetch_sub(released_bytes, Ordering::Release);
        }
    }

    /// Atomically clears the whole cache (boot rebuild renumbers segment
    /// slots, so every prior entry is stale).
    pub(in crate::engine::storage_engine) fn clear(&self) {
        let mut inner = self.inner.write();
        inner.entries.clear();
        self.resident_bytes.store(0, Ordering::Release);
    }

    /// Evicts least-recently-used indexes until the cache-held total is
    /// within the budget. At least one index always stays resident: a single
    /// index larger than the whole budget must remain servable instead of
    /// thrashing between evict and rebuild.
    fn enforce_budget(&self, inner: &mut TimestampIndexCacheInner) {
        let budget = u64::try_from(self.budget_bytes).unwrap_or(u64::MAX);
        let mut resident = self.resident_bytes.load(Ordering::Acquire);
        while resident > budget {
            if inner
                .entries
                .values()
                .filter(|entry| entry.index.is_some())
                .count()
                <= 1
            {
                break;
            }
            let Some(evict_sequence) = inner
                .entries
                .iter()
                .filter(|(_, entry)| entry.index.is_some())
                .min_by_key(|(_, entry)| entry.last_used.load(Ordering::Acquire))
                .map(|(sequence, _)| *sequence)
            else {
                break;
            };
            let Some(entry) = inner.entries.get_mut(&evict_sequence) else {
                break;
            };
            let Some(index) = entry.index.take() else {
                break;
            };
            let index_bytes = Self::entry_index_bytes(&index);
            resident = resident.saturating_sub(index_bytes);
            self.resident_bytes
                .fetch_sub(index_bytes, Ordering::Release);
            inner.entries.remove(&evict_sequence);
        }
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn len(&self) -> usize {
        self.inner.read().entries.len()
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn contains(&self, sequence: u64) -> bool {
        self.inner.read().entries.contains_key(&sequence)
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn contains_slot(&self, segment_slot: usize) -> bool {
        self.inner
            .read()
            .entries
            .values()
            .any(|entry| entry.segment_slot == segment_slot)
    }
}
