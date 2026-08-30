//! Bounded identity cache: series identity (metric + canonical labels) to
//! its registry resolution, so repeat resolves skip the dictionary walk
//! (metric/label dictionary read locks, canonicalize clone, SeriesKeyIds
//! hash and per-shard read lock).
//!
//! The cache is deliberately NOT part of registry memory accounting: it is
//! an advisory lookup structure (not series state), admission growth
//! estimates must not depend on resolve side effects, and its heap usage is
//! bounded by `SERIES_IDENTITY_CACHE_MAX_BYTES`.

use std::collections::HashMap;

use super::{DictionaryId, Label, LabelPairId, SeriesId, SeriesResolution};

/// Soft upper bound on identity-cache heap usage before a wholesale clear.
/// The cache is advisory: on overflow it clears and rebuilds itself from the
/// dictionary path, trading one cold pass for bounded memory.
const SERIES_IDENTITY_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;

/// Fixed per-entry overhead: entry struct fields, chain Vec slot and
/// HashMap bucket.
const SERIES_IDENTITY_CACHE_ENTRY_OVERHEAD_BYTES: usize = 64;

#[derive(Debug, Default)]
pub(super) struct SeriesIdentityCache {
    by_identity_hash: HashMap<u64, Vec<SeriesIdentityCacheEntry>>,
    estimated_bytes: usize,
}

#[derive(Debug)]
struct SeriesIdentityCacheEntry {
    metric: String,
    /// Canonical (sorted) labels, matching registry canonicalization.
    labels: Vec<Label>,
    metric_id: DictionaryId,
    label_pairs: Vec<LabelPairId>,
    series_id: SeriesId,
}

impl SeriesIdentityCache {
    /// Heap-usage estimate used by unit tests to pin eviction bookkeeping
    /// (the cache is otherwise unaccounted; see module docs).
    #[cfg(test)]
    pub(super) fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    /// Drops every entry.
    pub(super) fn clear(&mut self) {
        self.by_identity_hash.clear();
        self.estimated_bytes = 0;
    }

    /// Drops entries pointing at any of the given series ids. Used when a
    /// batch rollback removes series that an unprotected resolve_existing
    /// may have cached between creation and rollback.
    pub(super) fn remove_series_ids(&mut self, series_ids: &[SeriesId]) {
        let mut removed_any = false;
        for chain in self.by_identity_hash.values_mut() {
            let before = chain.len();
            chain.retain(|entry| !series_ids.contains(&entry.series_id));
            if chain.len() != before {
                // Entry bytes are tracked per insert; recompute the chain
                // share below via a full rescan once, outside this loop.
                removed_any = true;
            }
        }
        if removed_any {
            self.by_identity_hash.retain(|_, chain| !chain.is_empty());
            self.estimated_bytes = self
                .by_identity_hash
                .values()
                .flatten()
                .map(|entry| {
                    Self::entry_heap_bytes(&entry.metric, &entry.labels, &entry.label_pairs)
                })
                .fold(0usize, |acc, bytes| acc.saturating_add(bytes));
        }
    }

    /// Looks up a cached resolution. Keys are 64-bit series identity hashes;
    /// candidates are verified against the full canonical key, so a hash
    /// collision can only fall through to the dictionary walk — it can never
    /// produce a wrong series id.
    pub(super) fn resolve(&self, metric: &str, labels: &[Label]) -> Option<SeriesResolution> {
        let identity_hash = crate::label::stable_series_identity_hash(metric, labels);
        self.by_identity_hash
            .get(&identity_hash)?
            .iter()
            .find(|entry| {
                entry.metric == metric
                    && crate::label::labels_equal_canonical(&entry.labels, labels)
            })
            .map(|entry| SeriesResolution {
                series_id: entry.series_id,
                metric_id: entry.metric_id,
                label_pairs: entry.label_pairs.clone(),
                created: false,
            })
    }

    /// Records a resolution for a series that already existed when the
    /// current write batch started (`created == false`). Series created by a
    /// batch that might still roll back are best not recorded; if one leaks
    /// into the cache anyway, `remove_series_ids` on rollback evicts it.
    pub(super) fn record(
        &mut self,
        metric: &str,
        canonical_labels: &[Label],
        resolution: &SeriesResolution,
    ) {
        if self.estimated_bytes >= SERIES_IDENTITY_CACHE_MAX_BYTES {
            self.clear();
        }

        let identity_hash = crate::label::stable_series_identity_hash(metric, canonical_labels);
        let entry_bytes = Self::entry_heap_bytes(metric, canonical_labels, &resolution.label_pairs);
        self.by_identity_hash
            .entry(identity_hash)
            .or_default()
            .push(SeriesIdentityCacheEntry {
                metric: metric.to_string(),
                labels: canonical_labels.to_vec(),
                metric_id: resolution.metric_id,
                label_pairs: resolution.label_pairs.clone(),
                series_id: resolution.series_id,
            });
        self.estimated_bytes = self.estimated_bytes.saturating_add(entry_bytes);
    }

    fn entry_heap_bytes(metric: &str, labels: &[Label], label_pairs: &[LabelPairId]) -> usize {
        // Mirrors StringDictionary's capacity-based string accounting, using
        // lengths as the lower bound for the owned entry copies.
        let string_bytes = metric.len().saturating_mul(2);
        let label_string_bytes = labels.iter().fold(0usize, |acc, label| {
            acc.saturating_add(label.name.len().saturating_mul(2))
                .saturating_add(label.value.len().saturating_mul(2))
        });
        let label_vec_bytes = labels.len().saturating_mul(std::mem::size_of::<Label>());
        let pair_vec_bytes = label_pairs
            .len()
            .saturating_mul(std::mem::size_of::<LabelPairId>());

        string_bytes
            .saturating_add(label_string_bytes)
            .saturating_add(label_vec_bytes)
            .saturating_add(pair_vec_bytes)
            .saturating_add(SERIES_IDENTITY_CACHE_ENTRY_OVERHEAD_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolution(series_id: SeriesId) -> SeriesResolution {
        SeriesResolution {
            series_id,
            metric_id: 1,
            label_pairs: vec![LabelPairId {
                name_id: 7,
                value_id: 9,
            }],
            created: false,
        }
    }

    #[test]
    fn record_and_resolve_roundtrip_with_order_invariant_lookup() {
        let mut cache = SeriesIdentityCache::default();
        let labels = vec![Label::new("host", "a"), Label::new("zone", "use1")];
        cache.record("cpu", &labels.clone(), &resolution(11));
        assert!(cache.estimated_bytes() > 0);

        let hit = cache.resolve("cpu", &labels).unwrap();
        assert_eq!(hit.series_id, 11);
        assert!(!hit.created);

        let flipped = vec![Label::new("zone", "use1"), Label::new("host", "a")];
        assert_eq!(cache.resolve("cpu", &flipped).unwrap().series_id, 11);

        assert!(cache.resolve("cpu", &[Label::new("host", "b")]).is_none());
        assert!(cache.resolve("mem", &labels).is_none());
        // Different value under an equal-length label set must not match.
        assert!(cache
            .resolve(
                "cpu",
                &[Label::new("host", "a"), Label::new("zone", "use2")]
            )
            .is_none());
    }

    #[test]
    fn remove_series_ids_evicts_only_targeted_entries() {
        let mut cache = SeriesIdentityCache::default();
        cache.record("cpu", &[Label::new("host", "a")], &resolution(1));
        cache.record("mem", &[Label::new("host", "a")], &resolution(2));
        let bytes_with_both = cache.estimated_bytes();

        cache.remove_series_ids(&[2]);

        assert!(cache.resolve("cpu", &[Label::new("host", "a")]).is_some());
        assert!(cache.resolve("mem", &[Label::new("host", "a")]).is_none());
        assert!(cache.estimated_bytes() < bytes_with_both);
        assert!(cache.estimated_bytes() > 0);

        cache.remove_series_ids(&[1]);
        assert_eq!(cache.estimated_bytes(), 0);
        assert!(cache.resolve("cpu", &[Label::new("host", "a")]).is_none());
    }
}
