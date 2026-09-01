//! Resident segment-map cache with a global byte budget and LRU eviction.
//!
//! Chunk payloads of persisted segments are materialized once per segment
//! (`PlatformMmap`: a file mapping on Unix; owned heap bytes on Windows,
//! where segment files must stay deletable) and kept resident for query,
//! replay, and visibility reads. Without a bound this residency grows
//! monotonically with total segment data — the resource face behind the
//! three deployment-level terminations (commit exhaustion while segment
//! data kept growing) — so the registry caps cache-held bytes at
//! `budget_bytes`, evicting the least recently used mappings first.
//! Evicted segments re-map transparently from their chunks file on the
//! next access; entries are dropped for good when compaction replacement
//! removes the segment from the persisted index.
//!
//! Locking: the registry owns its own `RwLock` and is always acquired
//! nested inside a persisted-index lock (read or write), never the
//! reverse. Read paths therefore re-map a missing slot while holding only
//! an index read guard, and no lock-order cycle is possible because
//! registry methods never touch the persisted index.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::mmap::{create_mmap, PlatformMmap};
use crate::{Result, TsinkError};

struct SegmentMapEntry {
    chunks_path: PathBuf,
    map: Option<Arc<PlatformMmap>>,
    last_used: AtomicU64,
}

struct SegmentMapRegistryInner {
    entries: HashMap<usize, SegmentMapEntry>,
}

pub(in crate::engine::storage_engine) struct SegmentMapRegistry {
    budget_bytes: usize,
    clock: AtomicU64,
    resident_bytes: AtomicU64,
    inner: RwLock<SegmentMapRegistryInner>,
}

fn map_len_as_u64(map: &PlatformMmap) -> u64 {
    u64::try_from(map.len()).unwrap_or(u64::MAX)
}

impl SegmentMapRegistry {
    pub(in crate::engine::storage_engine) fn new(budget_bytes: usize) -> Self {
        Self {
            budget_bytes,
            clock: AtomicU64::new(0),
            resident_bytes: AtomicU64::new(0),
            inner: RwLock::new(SegmentMapRegistryInner {
                entries: HashMap::new(),
            }),
        }
    }

    /// Cache-held resident bytes. Mirrored into the memory-accounting
    /// component exported as the persisted-mmap gauge; in-flight query
    /// snapshots may hold additional `Arc` clones until they finish.
    pub(in crate::engine::storage_engine) fn resident_bytes(&self) -> &AtomicU64 {
        &self.resident_bytes
    }

    /// Approximate structural overhead (entries, paths, bookkeeping) for
    /// the included persisted-index memory component.
    pub(in crate::engine::storage_engine) fn structures_memory_usage_bytes(&self) -> usize {
        let inner = self.inner.read();
        let mut bytes = std::mem::size_of::<Self>()
            .saturating_add(std::mem::size_of::<SegmentMapRegistryInner>());
        for entry in inner.entries.values() {
            bytes = bytes
                .saturating_add(std::mem::size_of::<SegmentMapEntry>())
                .saturating_add(entry.chunks_path.capacity());
        }
        bytes
    }

    fn touch(entry: &SegmentMapEntry, clock: &AtomicU64) {
        let stamp = clock.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        entry.last_used.store(stamp, Ordering::Release);
    }

    /// Resolves a slot to its resident mapping, re-mapping the chunks file
    /// on miss. `Ok(None)` means the slot is not registered at all — the
    /// caller treats that as a missing-segment corruption, exactly like a
    /// segment absent from the persisted index.
    pub(in crate::engine::storage_engine) fn get_or_remap(
        &self,
        slot: usize,
    ) -> Result<Option<Arc<PlatformMmap>>> {
        {
            let inner = self.inner.read();
            if let Some(entry) = inner.entries.get(&slot) {
                if let Some(map) = entry.map.as_ref() {
                    Self::touch(entry, &self.clock);
                    return Ok(Some(Arc::clone(map)));
                }
            }
        }
        self.remap(slot)
    }

    fn remap(&self, slot: usize) -> Result<Option<Arc<PlatformMmap>>> {
        let chunks_path = {
            let inner = self.inner.write();
            match inner.entries.get(&slot) {
                Some(entry) => {
                    if let Some(map) = entry.map.as_ref() {
                        Self::touch(entry, &self.clock);
                        return Ok(Some(Arc::clone(map)));
                    }
                    entry.chunks_path.clone()
                }
                None => return Ok(None),
            }
        };

        // Read the file outside the registry lock so a slow re-map (a full
        // segment read on Windows) cannot stall resident-slot lookups.
        let map = Arc::new(Self::load_map(&chunks_path)?);

        let mut inner = self.inner.write();
        // The slot may have been removed (replacement) or concurrently
        // re-mapped while the file was being read; both win over ours.
        match inner.entries.get_mut(&slot) {
            Some(entry) => {
                if let Some(existing) = entry.map.as_ref() {
                    Self::touch(entry, &self.clock);
                    return Ok(Some(Arc::clone(existing)));
                }
                entry.map = Some(Arc::clone(&map));
                Self::touch(entry, &self.clock);
                self.resident_bytes
                    .fetch_add(map_len_as_u64(&map), Ordering::Release);
            }
            None => return Ok(None),
        }
        self.enforce_budget(&mut inner);
        Ok(Some(map))
    }

    fn load_map(chunks_path: &Path) -> Result<PlatformMmap> {
        let file = File::open(chunks_path).map_err(|err| TsinkError::IoWithPath {
            path: chunks_path.to_path_buf(),
            source: err,
        })?;
        create_mmap(file).map_err(|err| TsinkError::MemoryMap {
            path: chunks_path.to_path_buf(),
            details: err.to_string(),
        })
    }

    /// Registers a slot with its chunks-file path and the already-loaded
    /// mapping (the loader materializes and verifies the file). Installing
    /// an existing slot keeps the registered path and replaces the mapping.
    pub(in crate::engine::storage_engine) fn install(
        &self,
        slot: usize,
        chunks_path: PathBuf,
        map: Arc<PlatformMmap>,
    ) {
        let mut inner = self.inner.write();
        let entry = inner
            .entries
            .entry(slot)
            .or_insert_with(|| SegmentMapEntry {
                chunks_path,
                map: None,
                last_used: AtomicU64::new(0),
            });
        if let Some(previous) = entry.map.replace(Arc::clone(&map)) {
            self.resident_bytes
                .fetch_sub(map_len_as_u64(&previous), Ordering::Release);
        }
        self.resident_bytes
            .fetch_add(map_len_as_u64(&map), Ordering::Release);
        Self::touch(entry, &self.clock);
        self.enforce_budget(&mut inner);
    }

    /// Drops a slot entirely (mapping plus its path bookkeeping). Called
    /// when compaction replacement removes the segment from the index so
    /// the residency is released and the entry cannot be resurrected by a
    /// later re-map of a deleted file.
    pub(in crate::engine::storage_engine) fn remove(&self, slot: usize) -> bool {
        let mut inner = self.inner.write();
        match inner.entries.remove(&slot) {
            Some(entry) => {
                if let Some(map) = entry.map {
                    self.resident_bytes
                        .fetch_sub(map_len_as_u64(&map), Ordering::Release);
                }
                true
            }
            None => false,
        }
    }

    /// Atomically replaces the registry contents (boot index rebuild,
    /// where a freshly prepared state is swapped in wholesale). Slots are
    /// installed in the given order so eviction keeps the most recently
    /// installed mapping when the budget cannot hold everything.
    pub(in crate::engine::storage_engine) fn replace_all(
        &self,
        installed: Vec<(usize, PathBuf, Arc<PlatformMmap>)>,
    ) {
        let mut inner = self.inner.write();
        inner.entries.clear();
        self.resident_bytes.store(0, Ordering::Release);
        for (slot, chunks_path, map) in installed {
            let map_len = map_len_as_u64(&map);
            let entry = SegmentMapEntry {
                chunks_path,
                map: Some(map),
                last_used: AtomicU64::new(0),
            };
            Self::touch(&entry, &self.clock);
            inner.entries.insert(slot, entry);
            self.resident_bytes.fetch_add(map_len, Ordering::Release);
        }
        self.enforce_budget(&mut inner);
    }

    /// Evicts least-recently-used mappings until the cache-held total is
    /// within the budget. At least one mapping always stays resident: a
    /// single segment larger than the whole budget must remain servable
    /// instead of thrashing between evict and re-map.
    fn enforce_budget(&self, inner: &mut SegmentMapRegistryInner) {
        let budget = u64::try_from(self.budget_bytes).unwrap_or(u64::MAX);
        let mut resident = self.resident_bytes.load(Ordering::Acquire);
        while resident > budget {
            if inner
                .entries
                .values()
                .filter(|entry| entry.map.is_some())
                .count()
                <= 1
            {
                break;
            }
            let Some(evict_slot) = inner
                .entries
                .iter()
                .filter(|(_, entry)| entry.map.is_some())
                .min_by_key(|(_, entry)| entry.last_used.load(Ordering::Acquire))
                .map(|(slot, _)| *slot)
            else {
                break;
            };
            let Some(entry) = inner.entries.get_mut(&evict_slot) else {
                break;
            };
            let Some(map) = entry.map.take() else {
                break;
            };
            let map_len = map_len_as_u64(&map);
            resident = resident.saturating_sub(map_len);
            self.resident_bytes.fetch_sub(map_len, Ordering::Release);
        }
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn resident_count(&self) -> usize {
        let inner = self.inner.read();
        inner
            .entries
            .values()
            .filter(|entry| entry.map.is_some())
            .count()
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn resident_slots(&self) -> Vec<usize> {
        let inner = self.inner.read();
        let mut slots = inner
            .entries
            .iter()
            .filter(|(_, entry)| entry.map.is_some())
            .map(|(slot, _)| *slot)
            .collect::<Vec<_>>();
        slots.sort_unstable();
        slots
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn contains_slot(&self, slot: usize) -> bool {
        self.inner.read().entries.contains_key(&slot)
    }

    /// Empties every resident mapping while keeping path bookkeeping, so
    /// tests can exercise paths that must not depend on cached bytes.
    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn evict_all_for_tests(&self) {
        let mut inner = self.inner.write();
        for entry in inner.entries.values_mut() {
            entry.map = None;
        }
        self.resident_bytes.store(0, Ordering::Release);
    }
}
