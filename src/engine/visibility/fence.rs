use super::*;

/// 读侧等待可见性栅栏的预算（毫秒）。超过即返回错误——读不被写永久拖死。
pub const VISIBILITY_READ_WAIT_BUDGET_MS: u64 = 5_000;

/// 有上限的可见性栅栏读等待（供各 query context 委托）。
///
/// **兜底护栏**（2026-11-25）：真正的根因是嵌套读锁重入（同线程持外层读栅栏期间
/// 二次请求该锁，遇发布拍排队写锁即与写者互等），已在
/// `select_with_options_api` 降采样分支改用 `*_locked` 变体消除（见回归测试
/// `visibility_fence_reentrant.rs`）。
///
/// 本预算是第二道防线：任何**未来新增**的读路径若再引入同类嵌套、或发布拍真的
/// 卡住，读侧也只会等有限时间后显式报错，而不是永久挂死（现场原本只能重启进程）。
pub(in crate::engine::storage_engine) fn try_read_visibility_fence_bounded(
    fence: &RwLock<()>,
) -> Result<RwLockReadGuard<'_, ()>> {
    match fence.try_read_for(std::time::Duration::from_millis(VISIBILITY_READ_WAIT_BUDGET_MS)) {
        Some(guard) => Ok(guard),
        None => Err(TsinkError::Other(format!(
            "visibility fence busy: read waited over {VISIBILITY_READ_WAIT_BUDGET_MS}ms for a publication in progress"
        ))),
    }
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn build_visibility_state() -> VisibilityState {
        VisibilityState {
            tombstones: RwLock::new(HashMap::new()),
            materialized_series: RwLock::new(BTreeSet::new()),
            series_visibility_summaries: RwLock::new(HashMap::new()),
            series_visible_max_timestamps: RwLock::new(HashMap::new()),
            series_visible_bounded_max_timestamps: RwLock::new(HashMap::new()),
            visibility_state_generation: AtomicU64::new(0),
            live_series_pruning_generation: AtomicU64::new(0),
            max_observed_timestamp: AtomicI64::new(i64::MIN),
            max_bounded_observed_timestamp: AtomicI64::new(i64::MIN),
            recency_state_lock: Mutex::new(()),
            flush_visibility_lock: RwLock::new(()),
        }
    }

    pub(in crate::engine::storage_engine) fn visibility_state_generation(&self) -> u64 {
        self.visibility
            .visibility_state_generation
            .load(Ordering::Acquire)
    }

    pub(in crate::engine::storage_engine) fn bump_visibility_state_generation(&self) {
        self.visibility
            .visibility_state_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::engine::storage_engine) fn visibility_read_fence(
        &self,
    ) -> RwLockReadGuard<'_, ()> {
        self.visibility.flush_visibility_lock.read()
    }

    /// 读侧**有上限**的栅栏等待（2026-11-25 现场修复）。
    ///
    /// 背景：可见性栅栏是发布拍与读路径共用的写/读锁。写侧若在持锁期间卡住
    /// （锁序反转 / 落盘 I/O 阻塞 / 线程异常），读侧会**永久**阻塞——现场表现为
    /// 所有 history 查询永不返回（写入仍在推进），运维只能重启进程。
    ///
    /// 这里给读侧一个等待预算：超预算即返回错误，让调用方（REST 层）把"读超时"
    /// 显式报出来，而不是让请求无限挂住。写侧仍按原语义阻塞（发布拍必须完成），
    /// 但**读不再被写拖死**——这就是"永久无历史数据"被消除的关键。
    pub(in crate::engine::storage_engine) fn visibility_read_fence_bounded(
        &self,
    ) -> Result<RwLockReadGuard<'_, ()>> {
        try_read_visibility_fence_bounded(&self.visibility.flush_visibility_lock)
    }

    pub(in crate::engine::storage_engine) fn visibility_write_fence(
        &self,
    ) -> RwLockWriteGuard<'_, ()> {
        let t0 = std::time::Instant::now();
        let guard = self.visibility.flush_visibility_lock.write();
        let waited = t0.elapsed();
        if waited > std::time::Duration::from_millis(200) {
            tracing::warn!(
                target: "tsink::diag",
                waited_ms = waited.as_millis() as u64,
                thread = std::thread::current().name().unwrap_or("?"),
                "DIAG write fence acquired (slow)"
            );
        }
        guard
    }

    pub(in crate::engine::storage_engine) fn with_visibility_write_stage<R>(
        &self,
        f: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        let _visibility_guard = self.visibility_write_fence();
        f()
    }

    fn update_atomic_max_timestamp(target: &AtomicI64, ts: i64) {
        let mut current = target.load(Ordering::Acquire);
        while ts > current {
            match target.compare_exchange_weak(current, ts, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    pub(in crate::engine::storage_engine) fn update_max_observed_timestamp(&self, ts: i64) {
        Self::update_atomic_max_timestamp(&self.visibility.max_observed_timestamp, ts);
    }

    pub(in crate::engine::storage_engine) fn max_observed_timestamp(&self) -> Option<i64> {
        let max_observed = self
            .visibility
            .max_observed_timestamp
            .load(Ordering::Acquire);
        (max_observed != i64::MIN).then_some(max_observed)
    }

    pub(in crate::engine::storage_engine) fn bounded_recency_reference_timestamp(
        &self,
    ) -> Option<i64> {
        let max_bounded = self
            .visibility
            .max_bounded_observed_timestamp
            .load(Ordering::Acquire);
        (max_bounded != i64::MIN).then_some(max_bounded)
    }

    pub(in crate::engine::storage_engine) fn retention_recency_reference_timestamp(
        &self,
    ) -> Option<i64> {
        let max_observed = self
            .visibility
            .max_observed_timestamp
            .load(Ordering::Acquire);
        let bounded_reference = self.bounded_recency_reference_timestamp();
        if max_observed == i64::MIN && bounded_reference.is_none() {
            return None;
        }

        Some(
            bounded_reference
                .map(|reference| reference.max(self.current_timestamp_units()))
                .unwrap_or_else(|| self.current_timestamp_units()),
        )
    }
}
