use super::*;

impl ChunkStorage {
    pub(super) fn install_visible_persisted_state(
        &self,
        install: PersistedVisibleStateInstall,
    ) -> Result<()> {
        let PersistedVisibleStateInstall {
            refresh_series_ids,
            mark_refreshed_series_materialized,
            loaded_max_timestamp,
            next_segment_id,
        } = install;
        let refresh_series_ids = Self::normalize_series_ids(refresh_series_ids);
        let publication = self.lifecycle_publication_context();

        // Keep cache refresh ahead of materialization and publication bumps.
        self.refresh_series_visible_timestamp_cache_locked(refresh_series_ids.iter().copied())?;
        if mark_refreshed_series_materialized {
            let inserted_series_ids = publication
                .materialized_series
                .insert_materialized_series_ids(refresh_series_ids.iter().copied());
            if !inserted_series_ids.is_empty() {
                publication
                    .runtime_metadata_delta
                    .reconcile_series_ids(inserted_series_ids.iter().copied());
                publication
                    .metadata_shards
                    .publish_materialized_series_ids(inserted_series_ids.iter().copied());
            }
        }
        if let Some(loaded_max_timestamp) = loaded_max_timestamp {
            self.update_max_observed_timestamp(loaded_max_timestamp);
        }
        if let Some(next_segment_id) = next_segment_id {
            // W4-T5fix（D2 真因）：安装值是**扫描时点快照**，扫描与安装之间
            // 分配器可能已把 id 发给新段（compaction 输出）。分配器是单调
            // 下界——安装只能托底、不能倒写：倒写会让下一次发布撞上自己
            // 刚发布的段目录（真实副本 drill 确定性复现，见
            // allocator_install_monotonic_tests）。
            // W4-T5fix（D2 真因）：安装值是**扫描时点快照**，扫描与安装之间
            // 分配器可能已把 id 发给新段（compaction 输出）。分配器是单调
            // 下界——安装只能托底、不能倒写：倒写会让下一次发布撞上自己
            // 刚发布的段目录（真实副本 drill 确定性复现，见
            // allocator_install_monotonic_tests）。
            let floor = next_segment_id.max(1);
            let current = self.persisted.next_segment_id.load(Ordering::SeqCst);
            if floor > current {
                self.persisted
                    .next_segment_id
                    .store(floor, Ordering::SeqCst);
            }
        }
        self.bump_visibility_state_generation();
        Ok(())
    }
}

#[cfg(test)]
mod allocator_install_monotonic_tests {
    use super::*;
    use crate::{DataPoint, Row, Storage};
    use tempfile::TempDir;

    /// W4-T5fix（D2 真因）：运行时 index-rebuild 安装携带的是**扫描时点快照**
    /// 的 `next_segment_id`；扫描与安装之间分配器已把 id 发给新段（compaction
    /// 输出），无保护 store 会把分配器**倒写回过去**，后续分配撞上自己刚发布
    /// 的段目录（真实副本 drill 确定性复现：atomic 215153 → 被回写 215146 →
    /// 撞 tick1 的 seg-3486a）。安装必须是单调托底，永不允许回退。
    #[test]
    fn install_never_rewinds_the_segment_id_allocator() {
        let temp = TempDir::new().unwrap();
        let options = crate::engine::storage_engine::config::ChunkStorageOptions::default();
        let storage = ChunkStorage::new_with_data_path_and_options(
            4,
            None,
            Some(temp.path().join("lane_numeric")),
            None,
            100,
            options,
        )
        .unwrap();

        // 产一个真实段（id 100，来自起始下界），分配器推进到 101。
        // 时间戳必须「新鲜」：过期点会被默认 retention 丢弃，段建不出来。
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        storage
            .insert_rows(&[Row::new("allocator-monotonic", DataPoint::new(now, 1.0))])
            .unwrap();
        storage.flush_all_active().unwrap();
        storage.persist_segment_with_outcome().unwrap();
        let current = storage.persisted.next_segment_id.load(Ordering::SeqCst);
        assert!(
            current >= 101,
            "precondition: seal must advance the allocator past 100, got {current}"
        );

        // 过期快照安装：模拟「扫描在分配之前、安装在分配之后」的重扫重建路径。
        storage
            .install_visible_persisted_state(PersistedVisibleStateInstall {
                refresh_series_ids: Vec::new(),
                mark_refreshed_series_materialized: false,
                loaded_max_timestamp: None,
                next_segment_id: Some(100),
            })
            .unwrap();

        let after = storage.persisted.next_segment_id.load(Ordering::SeqCst);
        assert!(
            after >= current,
            "stale install rewound the segment id allocator: {current} -> {after}"
        );

        // 行为面收口：倒带一旦发生，下一次段发布必然撞上自己刚发布的目录。
        storage
            .insert_rows(&[Row::new(
                "allocator-monotonic",
                DataPoint::new(now + 1, 2.0),
            )])
            .unwrap();
        storage.flush_all_active().unwrap();
        storage.persist_segment_with_outcome().unwrap();
        let final_next = storage.persisted.next_segment_id.load(Ordering::SeqCst);
        assert!(
            final_next > after || final_next >= current,
            "allocator must stay above every published id (after={after}, final={final_next})"
        );
    }
}
