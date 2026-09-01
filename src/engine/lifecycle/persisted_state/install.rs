use super::*;

impl ChunkStorage {
    pub(super) fn accumulate_max_timestamp(current: &mut Option<i64>, candidate: Option<i64>) {
        if let Some(candidate) = candidate {
            *current = Some(
                current
                    .map(|existing| existing.max(candidate))
                    .unwrap_or(candidate),
            );
        }
    }

    pub(super) fn normalize_series_ids(mut series_ids: Vec<SeriesId>) -> Vec<SeriesId> {
        series_ids.sort_unstable();
        series_ids.dedup();
        series_ids
    }

    pub(super) fn with_lifecycle_persisted_index_publication<R>(
        publication: LifecyclePublicationContext<'_>,
        mutate: impl FnOnce(&mut PersistedIndexState) -> R,
    ) -> R {
        let mut persisted_index = publication.persisted_index.write();
        let timestamp_indexes_total = || {
            publication
                .timestamp_indexes
                .structures_memory_usage_bytes()
                .saturating_add(
                    usize::try_from(
                        publication
                            .timestamp_indexes
                            .resident_bytes()
                            .load(Ordering::Acquire),
                    )
                    .unwrap_or(usize::MAX),
                )
        };
        let before_included = if publication.accounting_enabled {
            Self::persisted_index_included_memory_usage_bytes(&persisted_index)
                .saturating_add(publication.segment_maps.structures_memory_usage_bytes())
                .saturating_add(timestamp_indexes_total())
        } else {
            0
        };
        let before_mmap = if publication.accounting_enabled {
            usize::try_from(
                publication
                    .segment_maps
                    .resident_bytes()
                    .load(Ordering::Acquire),
            )
            .unwrap_or(usize::MAX)
        } else {
            0
        };
        let result = mutate(&mut persisted_index);
        // Budget scope (2026-08-29 load-test follow-up): persisted index and
        // mmap bytes are EXCLUDED from the budget criterion; they only feed
        // their observability components and the `shared_used_bytes`
        // aggregate surfaced as `excluded_bytes`.
        let account = |component: &std::sync::atomic::AtomicU64, before: usize, after: usize| {
            if !publication.accounting_enabled || before == after {
                return;
            }
            let delta = saturating_u64_from_usize(before.abs_diff(after));
            if after >= before {
                component.fetch_add(delta, Ordering::AcqRel);
                publication
                    .shared_used_bytes
                    .fetch_add(delta, Ordering::AcqRel);
            } else {
                component.fetch_sub(delta, Ordering::AcqRel);
                publication
                    .shared_used_bytes
                    .fetch_sub(delta, Ordering::AcqRel);
            }
        };
        account(
            publication.persisted_index_used_bytes,
            before_included,
            Self::persisted_index_included_memory_usage_bytes(&persisted_index)
                .saturating_add(publication.segment_maps.structures_memory_usage_bytes())
                .saturating_add(timestamp_indexes_total()),
        );
        account(
            publication.persisted_mmap_used_bytes,
            before_mmap,
            usize::try_from(
                publication
                    .segment_maps
                    .resident_bytes()
                    .load(Ordering::Acquire),
            )
            .unwrap_or(usize::MAX),
        );
        result
    }

    pub(super) fn apply_loaded_segments_state_update(
        &self,
        loaded_segments: Vec<IndexedSegment>,
        loaded_series: &[PersistedSeries],
    ) -> Result<PersistedSegmentsStateUpdate> {
        let publication = self.lifecycle_publication_context();
        let segment_maps = &self.persisted.segment_maps;
        let metadata_delta = publication.runtime_metadata_delta;
        let state_update = Self::with_lifecycle_persisted_index_publication(
            publication,
            move |persisted_index| -> Result<PersistedSegmentsStateUpdate> {
                let mut state_update = PersistedSegmentsStateUpdate::default();
                let mut installed_maps = Vec::new();
                for segment in loaded_segments {
                    let (segment_lane, segment_tier) =
                        self.persisted_segment_location_for_root(&segment.root)?;
                    let (segment_series, segment_max_ts, _segment_bounded_max_ts) = self
                        .insert_indexed_segment_into_state(
                            segment_lane,
                            segment_tier,
                            persisted_index,
                            segment,
                            &mut installed_maps,
                        )?;
                    state_update.affected_series.extend(segment_series);
                    Self::accumulate_max_timestamp(
                        &mut state_update.loaded_max_timestamp,
                        segment_max_ts,
                    );
                }

                // Registry installs stay inside the same persisted-index
                // write section so a reader can never observe chunk refs
                // whose segment-map entry (with its re-map path) is missing.
                for (slot, chunks_path, map) in installed_maps {
                    segment_maps.install(slot, chunks_path, map);
                }

                state_update.affected_series =
                    Self::normalize_series_ids(state_update.affected_series);
                Self::insert_loaded_series_into_merged_postings(persisted_index, loaded_series);
                metadata_delta.reconcile_series_ids_locked(
                    persisted_index,
                    state_update.affected_series.iter().copied(),
                );
                Ok(state_update)
            },
        )?;
        Ok(state_update)
    }

    pub(super) fn apply_segment_root_removal_state_update(
        &self,
        roots: &[PathBuf],
    ) -> Result<PersistedSegmentRemovalStateUpdate> {
        let publication = self.lifecycle_publication_context();
        let segment_maps = &self.persisted.segment_maps;
        let timestamp_indexes = &self.persisted.timestamp_indexes;
        let metadata_delta = publication.runtime_metadata_delta;
        let state_update = Self::with_lifecycle_persisted_index_publication(
            publication,
            |persisted_index| -> Result<PersistedSegmentRemovalStateUpdate> {
                let mut affected_series = Vec::new();
                let mut fully_removed_series = BTreeSet::new();
                let mut removed_any = false;

                for root in roots {
                    let (removed_series, removed_from_postings, removed_slots) =
                        Self::remove_segment_root_from_state(persisted_index, root);
                    // Releasing the registry entry inside the same write
                    // section drops the resident mapping (commit) and the
                    // re-map path, so a removed segment cannot be
                    // resurrected from a deleted chunks file; the segment's
                    // cached timestamp indexes go with it.
                    for slot in removed_slots {
                        segment_maps.remove(slot);
                        timestamp_indexes.remove_for_slot(slot);
                    }
                    removed_any |= !removed_series.is_empty();
                    affected_series.extend(removed_series);
                    fully_removed_series.extend(removed_from_postings);
                }

                let affected_series = Self::normalize_series_ids(affected_series);
                let fully_removed_series =
                    fully_removed_series.into_iter().collect::<Vec<SeriesId>>();
                self.remove_series_ids_from_merged_postings(
                    persisted_index,
                    &fully_removed_series,
                )?;
                metadata_delta
                    .reconcile_series_ids_locked(persisted_index, affected_series.iter().copied());
                Ok(PersistedSegmentRemovalStateUpdate {
                    removed_any,
                    affected_series,
                })
            },
        )?;
        Ok(state_update)
    }

    pub(super) fn prepare_persisted_index_install(
        &self,
        loaded: LoadedSegmentIndexes,
    ) -> Result<PreparedPersistedIndexInstall> {
        let LoadedSegmentIndexes {
            indexed_segments,
            next_segment_id,
            ..
        } = loaded;
        let mut persisted_index = PersistedIndexState::default();
        let mut installed_maps = Vec::new();
        let mut loaded_max_timestamp = None;
        for indexed_segment in indexed_segments {
            let (segment_lane, segment_tier) =
                self.persisted_segment_location_for_root(&indexed_segment.root)?;
            let (_, segment_max_ts, _segment_bounded_max_ts) = self
                .insert_indexed_segment_into_state(
                    segment_lane,
                    segment_tier,
                    &mut persisted_index,
                    indexed_segment,
                    &mut installed_maps,
                )?;
            Self::accumulate_max_timestamp(&mut loaded_max_timestamp, segment_max_ts);
        }

        let persisted_series_ids =
            Self::normalize_series_ids(persisted_index.chunk_refs.keys().copied().collect());
        Ok(PreparedPersistedIndexInstall {
            persisted_index,
            segment_maps: installed_maps,
            persisted_series_ids,
            loaded_max_timestamp,
            next_segment_id: next_segment_id.max(1),
        })
    }

    pub(super) fn reconcile_registry_with_persisted_series_ids(&self, series_ids: &[SeriesId]) {
        let keep = series_ids.iter().copied().collect::<BTreeSet<_>>();
        self.lifecycle_publication_context()
            .mutate_registry(|registry| registry.retain_series_ids(&keep));
    }

    pub(super) fn install_persisted_index_rebuild(
        &self,
        prepared: PreparedPersistedIndexInstall,
    ) -> Result<()> {
        let PreparedPersistedIndexInstall {
            persisted_index,
            segment_maps: installed_maps,
            persisted_series_ids,
            loaded_max_timestamp,
            next_segment_id,
        } = prepared;
        let publication = self.lifecycle_publication_context();
        let registry = &self.persisted.segment_maps;
        let timestamp_indexes = &self.persisted.timestamp_indexes;
        let metadata_delta = publication.runtime_metadata_delta;
        Self::with_lifecycle_persisted_index_publication(
            publication,
            move |current_persisted_index| {
                *current_persisted_index = persisted_index;
                // The prepared state renumbers segment slots from scratch,
                // so the registry is replaced wholesale inside the same
                // write section; its budget enforcement evicts the least
                // recently installed mappings on boot reload. The timestamp
                // index cache is cleared for the same reason: every prior
                // entry is keyed by stale sequences and slots.
                registry.replace_all(installed_maps);
                timestamp_indexes.clear();
                metadata_delta.rebuild_locked(current_persisted_index);
            },
        );

        self.install_visible_persisted_state(PersistedVisibleStateInstall {
            refresh_series_ids: persisted_series_ids,
            mark_refreshed_series_materialized: true,
            loaded_max_timestamp,
            next_segment_id: Some(next_segment_id),
        })
    }

    fn insert_indexed_segment_into_state(
        &self,
        segment_lane: SegmentLaneFamily,
        segment_tier: PersistedSegmentTier,
        persisted_index: &mut PersistedIndexState,
        indexed_segment: IndexedSegment,
        installed_maps: &mut PreparedSegmentMaps,
    ) -> Result<(Vec<SeriesId>, Option<i64>, Option<i64>)> {
        debug_assert!(
            indexed_segment.manifest.series_count == 0
                || indexed_segment.postings.series_postings.is_empty()
                || indexed_segment.series.is_empty()
                || !indexed_segment.postings.metric_postings.is_empty(),
            "segments with loaded postings should carry metric postings"
        );
        let root = indexed_segment.root;
        if persisted_index.segments_by_root.contains_key(&root) {
            return Ok((Vec::new(), None, None));
        }
        let segment_manifest = indexed_segment.manifest;
        let segment_level = segment_manifest.level;
        let chunks_path = SegmentLayout::from_root(root.clone()).chunks_path;
        let chunk_map = Arc::new(indexed_segment.chunks_mmap);
        let chunk_entries = indexed_segment.chunk_index.entries;

        let segment_slot = persisted_index.next_segment_slot;
        let mut series_time_summaries =
            HashMap::<SeriesId, state::PersistedSeriesTimeRangeSummary>::new();
        let mut time_bucket_postings = segment_manifest
            .min_ts
            .zip(segment_manifest.max_ts)
            .map(|(min_ts, max_ts)| state::PersistedSegmentTimeBucketIndex::new(min_ts, max_ts));
        let mut affected_series = BTreeSet::new();
        let mut loaded_max_timestamp: Option<i64> = None;
        let mut loaded_bounded_max_timestamp: Option<i64> = None;
        let future_skew_cutoff = self.current_future_skew_cutoff();
        let mut next_chunk_sequence = persisted_index.next_chunk_sequence.max(1);

        for entry in chunk_entries {
            let sequence = next_chunk_sequence;
            next_chunk_sequence = sequence.saturating_add(1);
            Self::accumulate_max_timestamp(&mut loaded_max_timestamp, Some(entry.max_ts));
            let bounded_candidate = if entry.max_ts <= future_skew_cutoff {
                Some(entry.max_ts)
            } else if entry.min_ts <= future_skew_cutoff {
                Some(entry.min_ts)
            } else {
                None
            };
            Self::accumulate_max_timestamp(&mut loaded_bounded_max_timestamp, bounded_candidate);

            let chunk_ref = PersistedChunkRef {
                level: segment_level,
                min_ts: entry.min_ts,
                max_ts: entry.max_ts,
                point_count: entry.point_count,
                sequence,
                chunk_offset: entry.chunk_offset,
                chunk_len: entry.chunk_len,
                lane: entry.lane,
                ts_codec: entry.ts_codec,
                value_codec: entry.value_codec,
                segment_slot,
            };

            if let Some(time_bucket_postings) = time_bucket_postings.as_mut() {
                time_bucket_postings.insert_series_range(
                    entry.series_id,
                    entry.min_ts,
                    entry.max_ts,
                );
            }
            series_time_summaries
                .entry(entry.series_id)
                .and_modify(|summary| {
                    summary.min_ts = summary.min_ts.min(entry.min_ts);
                    summary.max_ts = summary.max_ts.max(entry.max_ts);
                })
                .or_insert(state::PersistedSeriesTimeRangeSummary {
                    min_ts: entry.min_ts,
                    max_ts: entry.max_ts,
                });
            persisted_index
                .chunk_refs
                .entry(entry.series_id)
                .or_default()
                .push(chunk_ref);
            affected_series.insert(entry.series_id);
        }

        persisted_index.next_segment_slot = persisted_index.next_segment_slot.saturating_add(1);
        persisted_index.next_chunk_sequence = next_chunk_sequence;
        installed_maps.push((segment_slot, chunks_path, Arc::clone(&chunk_map)));
        persisted_index
            .segment_tiers
            .insert(segment_slot, segment_tier);

        for series_id in &affected_series {
            if let Some(refs) = persisted_index.chunk_refs.get_mut(series_id) {
                Self::sort_persisted_chunk_refs(refs.as_mut_slice());
            }
        }

        persisted_index.segments_by_root.insert(
            root,
            state::PersistedSegmentState {
                segment_slot,
                lane: segment_lane,
                tier: segment_tier,
                manifest: segment_manifest,
                time_bucket_postings,
                series_time_summaries,
            },
        );

        Ok((
            affected_series.into_iter().collect(),
            loaded_max_timestamp,
            loaded_bounded_max_timestamp,
        ))
    }

    fn remove_segment_root_from_state(
        persisted_index: &mut PersistedIndexState,
        root: &Path,
    ) -> (Vec<SeriesId>, Vec<SeriesId>, Vec<usize>) {
        let Some(removed_segment) = persisted_index.segments_by_root.remove(root) else {
            return (Vec::new(), Vec::new(), Vec::new());
        };

        let removed_slot = removed_segment.segment_slot;
        persisted_index
            .segment_tiers
            .remove(&removed_segment.segment_slot);
        let mut affected_series = BTreeSet::new();
        let mut fully_removed_series = Vec::new();
        // `series_time_summaries` is populated by the same per-chunk loop that
        // feeds the global chunk refs, so its keys are exactly the series with
        // refs in this segment; the global refs are filtered back out by
        // `segment_slot` (sequences are unique per chunk). The segment's
        // cached timestamp indexes are cleared separately by slot (the cache
        // is bounded, so a slot scan is O(cache size)).
        for series_id in removed_segment.series_time_summaries.keys().copied() {
            affected_series.insert(series_id);
            let Some(existing_refs) = persisted_index.chunk_refs.get_mut(&series_id) else {
                continue;
            };
            existing_refs.retain(|candidate| candidate.segment_slot != removed_slot);
            if existing_refs.is_empty() {
                persisted_index.chunk_refs.remove(&series_id);
                fully_removed_series.push(series_id);
            }
        }

        (
            affected_series.into_iter().collect(),
            fully_removed_series,
            vec![removed_slot],
        )
    }
}
