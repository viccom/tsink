use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use tempfile::TempDir;

use super::{execution, finalize_pending_compaction_replacements, CompactionRunStats, Compactor};
use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::encoder::Encoder;
use crate::engine::segment::{
    load_segments, load_segments_for_level, SegmentWriter, WalHighWatermark,
};
use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
use crate::engine::tombstone::{persist_tombstones, TombstoneRange, TOMBSTONES_FILE_NAME};
use crate::{
    HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, NativeHistogram, Value,
};

fn sample_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(42)),
        sum: 17.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(HistogramCount::Int(7)),
        negative_spans: vec![HistogramBucketSpan {
            offset: -1,
            length: 1,
        }],
        negative_deltas: vec![2],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 0,
            length: 2,
        }],
        positive_deltas: vec![3, 1],
        positive_counts: vec![],
        reset_hint: HistogramResetHint::No,
        custom_values: vec![0.25, 0.5],
    }
}

fn write_level_segment(
    temp_dir: &tempfile::TempDir,
    level: u8,
    segment_id: u64,
    series_id: u64,
    registry: &SeriesRegistry,
    points: &[(i64, f64)],
) {
    let mut chunks = HashMap::new();
    chunks.insert(series_id, vec![make_numeric_chunk(series_id, points)]);
    SegmentWriter::new(temp_dir.path(), level, segment_id)
        .unwrap()
        .write_segment(registry, &chunks)
        .unwrap();
}

#[test]
fn compaction_window_selection_fully_loads_at_most_window_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    // One overlapping cluster of 20 L0 segments: the window must pick 16 of
    // them, and planning must fully load only those 16 — not the whole level.
    for segment_id in 1..=20u64 {
        write_level_segment(
            &temp_dir,
            0,
            segment_id,
            series_id,
            &registry,
            &[(10, 1.0), (11, 2.0)],
        );
    }

    crate::engine::segment::reset_full_segment_dir_load_count();
    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(1000)),
    );
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted, "overlapping cluster must compact");

    let loads = crate::engine::segment::full_segment_dir_load_count();
    assert!(
        loads <= super::DEFAULT_SOURCE_WINDOW_SEGMENTS as u64,
        "single compaction pass fully loaded {loads} segment directories; window is {}",
        super::DEFAULT_SOURCE_WINDOW_SEGMENTS
    );

    // Window semantics are unchanged: the 16 lowest-id segments of the
    // cluster are compacted away and the rest of the level stays untouched.
    let remaining_l0_ids = load_segments_for_level(temp_dir.path(), 0)
        .unwrap()
        .into_iter()
        .map(|segment| segment.manifest.segment_id)
        .collect::<Vec<_>>();
    assert_eq!(remaining_l0_ids, (17..=20).collect::<Vec<_>>());
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 1).unwrap().len(),
        1,
        "compacted window must produce exactly one L1 output"
    );
}

#[test]
fn l1_to_l2_compaction_is_not_starved_by_continuous_l0_arrivals() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    // An L1 backlog (5 overlapping segments, eligible via time overlap) that
    // only L1->L2 compaction can drain, while fresh overlapping L0 segments
    // keep arriving every pass so L0->L1 is always eligible too.
    for (segment_id, ts) in [(1u64, 10i64), (2, 12), (3, 14), (4, 16), (5, 18)] {
        write_level_segment(
            &temp_dir,
            1,
            segment_id,
            series_id,
            &registry,
            &[(ts, ts as f64), (ts + 10, 100.0)],
        );
    }

    let compactor = Compactor::new(temp_dir.path(), 8);
    let mut l0_to_l1_runs = 0usize;
    let mut l1_to_l2_runs = 0usize;

    for round in 0..8u64 {
        let base_ts = 100 + (round as i64) * 100;
        let first_id = 100 + round * 10;
        write_level_segment(
            &temp_dir,
            0,
            first_id,
            series_id,
            &registry,
            &[(base_ts, 1.0), (base_ts + 50, 2.0)],
        );
        write_level_segment(
            &temp_dir,
            0,
            first_id + 1,
            series_id,
            &registry,
            &[(base_ts + 25, 3.0), (base_ts + 75, 4.0)],
        );

        let stats = compactor.compact_once_with_stats().unwrap();
        match (stats.source_level, stats.target_level) {
            (Some(0), Some(1)) => l0_to_l1_runs += 1,
            (Some(1), Some(2)) => l1_to_l2_runs += 1,
            _ => {}
        }
    }

    assert!(
        l0_to_l1_runs > 0,
        "test setup broken: L0->L1 never ran ({l0_to_l1_runs} runs)"
    );
    assert!(
        l1_to_l2_runs > 0,
        "L1->L2 starved under continuous L0 arrivals: L0->L1 ran {l0_to_l1_runs} times, L1->L2 ran {l1_to_l2_runs} times"
    );
}

#[test]
fn l0_backlog_is_not_starved_by_level_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    // Only L0 has work (12 non-overlapping segments, count-trigger eligible in
    // windows of 4). While the L0 backlog is at or above the trigger, every
    // pass must compact L0 regardless of which transition ran last.
    for segment_id in 1..=12u64 {
        write_level_segment(
            &temp_dir,
            0,
            segment_id,
            series_id,
            &registry,
            &[(segment_id as i64, segment_id as f64)],
        );
    }

    let compactor = Compactor::new(temp_dir.path(), 8);
    let mut l0_to_l1_runs = 0usize;
    loop {
        let stats = compactor.compact_once_with_stats().unwrap();
        if !stats.compacted {
            break;
        }
        assert_eq!(
            (stats.source_level, stats.target_level),
            (Some(0), Some(1)),
            "L0 backlog at trigger must keep winning passes"
        );
        l0_to_l1_runs += 1;
    }

    assert_eq!(
        l0_to_l1_runs, 3,
        "expected three window passes over 12 L0 segments"
    );
    assert!(load_segments_for_level(temp_dir.path(), 0)
        .unwrap()
        .is_empty());
}

/// 三路仲裁必须是**真轮转**：L0→L1 / L1→L2 / L2→L2 三层同时持续可触发时，
/// 三种 transition 都必须在有限次数内各轮到一次。
///
/// 红态（「上次赢家移队尾」）：只有上次赢家被判为 `true` 排到最后，稳定排序
/// 让其余两个保持自然序。`last=L0 → [L1,L2,L0]`（L1 赢）→ `last=L1 →
/// [L0,L2,L1]`（L0 赢）→ 回到 `last=L0`……L0/L1 交替、L2 恒在索引 1/2 而
/// 位置 0 总有人赢，L2ToL2 永不执行。
///
/// fixture：三层各预置远超触发阈值的段数（L0 60 / L1 40 / L2 100，阈值
/// L0=4、L1=4、L2=2），同层段的时间窗互不重叠，所以每层只走 count 触发、
/// 窗口固定（L0/L1 每次消耗 4 段产 1 段，L2 每次消耗 16 段产 1 段）。即使
/// 某一层每拍都赢，12 次 pass 也消耗不到阈值以下：三层在整个 N 次 pass 期间
/// 始终可触发，序列只由仲裁顺序决定。
#[test]
fn three_way_arbitration_round_robin_reaches_every_transition() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "rotation")])
        .unwrap()
        .series_id;

    for index in 0..60u64 {
        let ts = index as i64 * 10;
        write_level_segment(
            &temp_dir,
            0,
            index + 1,
            series_id,
            &registry,
            &[(ts, 1.0), (ts + 1, 2.0)],
        );
    }
    for index in 0..40u64 {
        let ts = 10_000 + index as i64 * 10;
        write_level_segment(
            &temp_dir,
            1,
            100 + index,
            series_id,
            &registry,
            &[(ts, 1.0), (ts + 1, 2.0)],
        );
    }
    for index in 0..100u64 {
        let ts = 1_000_000 + index as i64 * 10;
        write_level_segment(
            &temp_dir,
            2,
            200 + index,
            series_id,
            &registry,
            &[(ts, 1.0), (ts + 1, 2.0)],
        );
    }

    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(1000)),
    )
    .with_l2_compaction_trigger(2);

    const PASSES: usize = 12;
    let mut observed = Vec::with_capacity(PASSES);
    for pass in 0..PASSES {
        let stats = compactor.compact_once_with_changes().unwrap().stats;
        let transition = match (stats.source_level, stats.target_level) {
            (Some(0), Some(1)) => "L0->L1",
            (Some(1), Some(2)) => "L1->L2",
            (Some(2), Some(2)) => "L2->L2",
            other => panic!(
                "第 {pass} 次 pass：三层都在触发阈值之上，必须有 transition 赢下这一拍，\
                 实际 {other:?}（stats={stats:?}）"
            ),
        };
        observed.push(transition);
    }

    for expected in ["L0->L1", "L1->L2", "L2->L2"] {
        assert!(
            observed.contains(&expected),
            "{expected} 在 {PASSES} 次 pass 内一次都没发生——三层同时持续可触发时必须真轮转\
             （从上次赢家的后继开始排列），而不是「上次赢家移队尾」。实际序列：{observed:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// W4-T1（红）：L2 终层收敛三测。L2 目前是终层（只有 L0→L1 / L1→L2 两个
// transition），三测的红态都源于「无 L2ToL2 transition → L2 段数不降」；
// W4-T4 引入 `LevelTransition::L2ToL2` 后转绿（触发 = 候选 ≥ 2 且 L2 总段数
// > l2_trigger，默认 32，0 = 关闭；候选 = point_count ≤ output_segment_point_
// budget/8；窗口 ≤ source_window_segments）。
// ---------------------------------------------------------------------------

/// W4-T1 三测共用 fixture：在 L2 写 `count` 个小段（段 id 1..=count），每段
/// 100 点，时间窗互不重叠（段 i 覆盖 [i*100, (i+1)*100)），值 = ts as f64
/// （确定性，可逐点校验）。
fn write_l2_small_segments(
    temp_dir: &tempfile::TempDir,
    registry: &SeriesRegistry,
    series_id: u64,
    count: usize,
) {
    for segment_index in 0..count {
        let base_ts = segment_index as i64 * 100;
        let points = (base_ts..base_ts + 100)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();
        write_level_segment(
            temp_dir,
            2,
            segment_index as u64 + 1,
            series_id,
            registry,
            &points,
        );
    }
}

/// 反复驱动 compaction 直到无进展（连续 3 轮 `compacted == false`）或达到
/// `max_passes` 上限，返回每轮 stats。红态（无 L2ToL2）首轮即无进展；绿态
/// （W4-T4 落地）收敛到阈值下后停。连续多轮而非单轮判停，容忍轮转仲裁下
/// 单轮偶发的无进展 pass。
fn drive_l2_compaction_to_quiescence(
    compactor: &Compactor,
    max_passes: usize,
) -> Vec<CompactionRunStats> {
    let mut passes = Vec::new();
    let mut idle_rounds = 0usize;
    for _ in 0..max_passes {
        let stats = compactor.compact_once_with_changes().unwrap().stats;
        if stats.compacted {
            idle_rounds = 0;
        } else {
            idle_rounds += 1;
            if idle_rounds >= 3 {
                break;
            }
        }
        passes.push(stats);
    }
    passes
}

/// 全量读取 base 下所有段的 `series_id` 数值点，返回 (ts, value) 对。
/// `load_segments` 不应用墓碑文件——读到的即数据文件层实际内容（绿态判据：
/// 墓碑点必须物理消失，而非仅被读取层过滤）。
fn decoded_numeric_points(base: &Path, series_id: u64) -> Vec<(i64, f64)> {
    let loaded = load_segments(base).unwrap();
    loaded
        .chunks_by_series
        .get(&series_id)
        .map(|chunks| {
            chunks
                .iter()
                .flat_map(|chunk| execution::decode_chunk_points_for_compaction(chunk).unwrap())
                .map(|point| match point.value {
                    Value::F64(value) => (point.ts, value),
                    other => panic!("fixture 只写数值点，读到 {other:?}"),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

#[test]
fn l2_compaction_reduces_terminal_segment_count() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "l2-terminal")])
        .unwrap()
        .series_id;

    write_l2_small_segments(&temp_dir, &registry, series_id, 40);

    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        2048,
        Arc::new(AtomicU64::new(1000)),
    );
    drive_l2_compaction_to_quiescence(&compactor, 200);

    let l2 = load_segments_for_level(temp_dir.path(), 2).unwrap();
    assert!(
        l2.len() <= 32, // W4-T4 默认触发阈值（l2_trigger = 32）
        "40 个 L2 小段收敛后必须 ≤ 32 段（W4-T4 默认触发阈值），实际 {} 段 \
         ——红态：L2 是终层，无 L2ToL2 transition，段数不降",
        l2.len()
    );

    // 数据完整：40 × 100 点全量可读、时间戳无重复、值 = ts as f64。
    let points = decoded_numeric_points(temp_dir.path(), series_id);
    assert_eq!(points.len(), 4_000, "40 段 × 100 点必须全量可读");
    let mut timestamps = points.iter().map(|(ts, _)| *ts).collect::<Vec<_>>();
    timestamps.sort_unstable();
    let expected: Vec<i64> = (0..4_000).collect();
    assert_eq!(
        timestamps, expected,
        "合并后时间戳必须恰好是 0..4000 各一次（全保留且无重复）"
    );
    for (ts, value) in &points {
        assert_eq!(*value, *ts as f64, "ts={ts} 的值必须保持不变");
    }
}

#[test]
fn l2_compaction_applies_tombstones() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "l2-tombstone")])
        .unwrap()
        .series_id;

    // 8 个 100 点小段（ts 0..800）。默认触发 32 下不满足「总段数 > 触发」，
    // 本测用 W4-T4 旋钮把阈值下调到 4（stub 阶段存参不消费 → 断言红）。
    write_l2_small_segments(&temp_dir, &registry, series_id, 8);

    // 墓碑半开区间 [0, 150)：段 0 全灭（ts 0..=99）+ 段 1 前半（ts 100..=149）；
    // ts ∈ [150, 800) 的 650 点必须保留。
    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 0, end: 150 }]);
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        2048,
        Arc::new(AtomicU64::new(1000)),
    )
    .with_l2_compaction_trigger(4);
    drive_l2_compaction_to_quiescence(&compactor, 200);

    let l2 = load_segments_for_level(temp_dir.path(), 2).unwrap();
    assert!(
        l2.len() < 8,
        "8 个 L2 小段（> 触发阈值 4）必须收敛合并、段数必须下降，实际 {} 段 \
         ——红态：无 L2ToL2 transition，无合并",
        l2.len()
    );

    // 绿态判据经读取校验：墓碑点从数据文件层物理消失，其余点完整保留。
    let points = decoded_numeric_points(temp_dir.path(), series_id);
    let mut timestamps = points.iter().map(|(ts, _)| *ts).collect::<Vec<_>>();
    timestamps.sort_unstable();
    let expected: Vec<i64> = (150..800).collect();
    assert_eq!(
        timestamps, expected,
        "墓碑区间 [0, 150) 的点合并后必须不可见，其余 650 点完整保留"
    );
    for (ts, value) in &points {
        assert_eq!(*value, *ts as f64, "ts={ts} 的值必须保持不变");
    }
}

#[test]
fn l2_compaction_output_level_is_l2() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "l2-terminal")])
        .unwrap()
        .series_id;

    write_l2_small_segments(&temp_dir, &registry, series_id, 40);

    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        2048,
        Arc::new(AtomicU64::new(1000)),
    );
    let passes = drive_l2_compaction_to_quiescence(&compactor, 200);

    // 前置：收敛必须真的发生过，否则 level 断言对未动的输入段空转（输入段
    // 本来就是 level 2）。
    assert!(
        passes.iter().any(|stats| stats.compacted),
        "前置：L2 收敛必须发生（红态：无 L2ToL2 transition，{} 次 pass 无一压缩）",
        passes.len()
    );

    let l2 = load_segments_for_level(temp_dir.path(), 2).unwrap();
    assert!(
        !l2.is_empty(),
        "收敛后 L2 必须仍有存活段（纯删除仅发生在全墓碑窗口，本 fixture 无墓碑）"
    );
    assert!(
        l2.iter().all(|segment| segment.manifest.level == 2),
        "L2 终层收敛的输出段必须留在 L2（manifest.level == 2），不得降级"
    );
    assert!(
        load_segments_for_level(temp_dir.path(), 1)
            .unwrap()
            .is_empty(),
        "终层收敛不得把输出写到 L1"
    );
    assert!(
        load_segments_for_level(temp_dir.path(), 0)
            .unwrap()
            .is_empty(),
        "终层收敛不得把输出写到 L0"
    );
}

// ---------------------------------------------------------------------------
// W4-T2（红 + 基线表征）：L2 收敛不做无意义重写（写放大上界）。按 W4-T4
// 计划语义钉死：候选 = point_count ≤ max_input_points（=
// output_segment_point_budget/8），触发 = 候选 ≥ 2 且 L2 总段数 > l2_trigger，
// 窗口 ≤ source_window_segments。
// - 测 1 `l2_compaction_skips_large_segments`：红态 = 无 L2ToL2 → 小段不合并。
// - 测 2 `l2_compaction_terminates_when_only_large_segments_remain`：当前必然
//   绿（基线表征），判别力在 W4-T4 落地后生效——候选过滤缺失会反复重选大段
//   窗口烧预算，届时指纹断言转红；T4 后必须保持绿。
// ---------------------------------------------------------------------------

/// budget 注入值：默认 8M 点太大、fixture 写不起，压到 800 → max_input_points
/// = 100：50 点段为候选、200 点段为大段（与边界 100 保持距离，对 ≤ / < 的
/// 边界语义不敏感）。
const W4_T2_OUTPUT_BUDGET: usize = 800;
/// W4-T4 候选判据：point_count ≤ output_segment_point_budget / 8。
const W4_T2_MAX_INPUT_POINTS: usize = W4_T2_OUTPUT_BUDGET / 8;

/// W4-T2 共用 fixture：在 L2 写 `specs.len()` 个段（段 id 1..=len），第 i 段
/// specs[i] 个点，时间窗首尾相接但互不重叠（段 i 覆盖 [cursor, cursor+size)），
/// 值 = ts as f64（确定性，可逐点校验）。返回总点数（供可读性断言）。
fn write_l2_segments_of_sizes(
    temp_dir: &tempfile::TempDir,
    registry: &SeriesRegistry,
    series_id: u64,
    specs: &[usize],
) -> usize {
    let mut cursor = 0i64;
    let mut total_points = 0usize;
    for (segment_index, &point_count) in specs.iter().enumerate() {
        let points = (cursor..cursor + point_count as i64)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();
        write_level_segment(
            temp_dir,
            2,
            segment_index as u64 + 1,
            series_id,
            registry,
            &points,
        );
        cursor += point_count as i64;
        total_points += point_count;
    }
    total_points
}

/// 段目录指纹：(目录树内最新 mtime, 全部文件按相对路径序拼接内容的哈希)。
/// 目录自身 mtime 一并计入——文件增删即使剩余内容不变也会改变目录 mtime。
/// 用于断言大段在 L2 收敛中未被重写（mtime + 内容逐字节不变）。
fn segment_dir_fingerprint(root: &Path) -> (SystemTime, u64) {
    fn visit(dir: &Path, prefix: &str, newest: &mut SystemTime, hasher: &mut DefaultHasher) {
        let mut paths: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        paths.sort_unstable();
        for path in paths {
            let meta = fs::metadata(&path).unwrap();
            *newest = (*newest).max(meta.modified().unwrap());
            let relative = format!("{prefix}{}", path.file_name().unwrap().to_string_lossy());
            if meta.is_dir() {
                visit(&path, &format!("{relative}/"), newest, hasher);
            } else {
                relative.hash(hasher);
                let bytes = fs::read(&path).unwrap();
                hasher.write(&(bytes.len() as u64).to_le_bytes());
                hasher.write(&bytes);
            }
        }
    }

    let mut hasher = DefaultHasher::new();
    let mut newest = fs::metadata(root).unwrap().modified().unwrap();
    visit(root, "", &mut newest, &mut hasher);
    (newest, hasher.finish())
}

/// 快照 L2 中全部大段（point_count > max_input_points）的段目录指纹，键 =
/// segment_id。跑前/跑后各调一次并比对，即可断言大段未被重写。
fn snapshot_large_segment_fingerprints(
    base: &Path,
    max_input_points: usize,
) -> HashMap<u64, (SystemTime, u64)> {
    load_segments_for_level(base, 2)
        .unwrap()
        .into_iter()
        .filter(|segment| segment.manifest.point_count > max_input_points)
        .map(|segment| {
            (
                segment.manifest.segment_id,
                segment_dir_fingerprint(&segment.root),
            )
        })
        .collect()
}

#[test]
fn l2_compaction_skips_large_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "l2-skip-large")])
        .unwrap()
        .series_id;

    // 6 个 50 点小段（候选）与 5 个 200 点大段按段 id 交错排列（1..=11）：
    // 非重叠路径的窗口按 id 从队首取——若 L2ToL2 不做 max_input_points 过滤，
    // 队首窗口必然吞进大段；做过滤则窗口只含小段。总段数 11 > l2_trigger=2、
    // 候选 6 ≥ 2，满足 W4-T4 触发条件。
    let specs: [usize; 11] = [50, 200, 50, 200, 50, 200, 50, 200, 50, 200, 50];
    let total_points = write_l2_segments_of_sizes(&temp_dir, &registry, series_id, &specs);

    let large_before = snapshot_large_segment_fingerprints(temp_dir.path(), W4_T2_MAX_INPUT_POINTS);
    assert_eq!(large_before.len(), 5, "fixture 必须恰好含 5 个大段");
    let segments_before = load_segments_for_level(temp_dir.path(), 2).unwrap().len();

    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        2048,
        Arc::new(AtomicU64::new(1000)),
    )
    .with_output_segment_point_budget(W4_T2_OUTPUT_BUDGET)
    .with_l2_compaction_trigger(2);
    drive_l2_compaction_to_quiescence(&compactor, 200);

    // 判据 1（红形态收口）：候选小段必须被 L2ToL2 合并消失——段数下降。
    let segments_after = load_segments_for_level(temp_dir.path(), 2).unwrap();
    assert!(
        segments_after.len() < segments_before,
        "6 个 50 点小段（候选）必须被 L2ToL2 合并、段数必须从 {segments_before} 下降，\
         实际 {} 段——红态：L2 是终层，无 L2ToL2 transition，段数不降",
        segments_after.len()
    );

    // 判据 2（写放大上界）：大段 mtime 与内容逐字节不变——收敛只准动候选段，
    // 重写大段 = 重写全部历史数据。
    let large_after = snapshot_large_segment_fingerprints(temp_dir.path(), W4_T2_MAX_INPUT_POINTS);
    assert_eq!(
        large_after.len(),
        large_before.len(),
        "大段必须原样保留，不得被删除或替换"
    );
    for (segment_id, before) in &large_before {
        assert_eq!(
            before,
            large_after.get(segment_id).expect("大段不得消失"),
            "大段 segment_id={segment_id} 被 L2 收敛重写（mtime/内容变化）\
             ——候选过滤（point_count ≤ max_input_points）失效"
        );
    }

    // 判据 3：数据完整——小段合并是保数据的，全部点可读、值不变。
    let points = decoded_numeric_points(temp_dir.path(), series_id);
    assert_eq!(points.len(), total_points, "全部点必须完整可读");
    for (ts, value) in &points {
        assert_eq!(*value, *ts as f64, "ts={ts} 的值必须保持不变");
    }
}

#[test]
fn l2_compaction_terminates_when_only_large_segments_remain() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "l2-large-only")])
        .unwrap()
        .series_id;

    // 10 个 200 点大段（point_count 全部 > max_input_points=100），段数 10 >
    // l2_trigger=2：候选为 0，L2ToL2 永不满足触发。
    let specs: [usize; 10] = [200; 10];
    let total_points = write_l2_segments_of_sizes(&temp_dir, &registry, series_id, &specs);

    let large_before = snapshot_large_segment_fingerprints(temp_dir.path(), W4_T2_MAX_INPUT_POINTS);
    assert_eq!(large_before.len(), 10, "fixture 必须恰好含 10 个大段");
    let segments_before = load_segments_for_level(temp_dir.path(), 2).unwrap().len();

    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        2048,
        Arc::new(AtomicU64::new(1000)),
    )
    .with_output_segment_point_budget(W4_T2_OUTPUT_BUDGET)
    .with_l2_compaction_trigger(2);

    // 有限 pass 内每轮 compacted == false：不得活锁、不得空转重选大段窗口。
    for pass in 0..50u32 {
        let stats = compactor.compact_once_with_changes().unwrap().stats;
        assert!(
            !stats.compacted,
            "第 {pass} 轮 pass 压缩了只含大段的 L2——候选过滤（point_count ≤ \
             max_input_points）失效，写放大上界被突破"
        );
    }

    // 大段全部原样（mtime + 内容）、段数不变、无输出泄漏到低层。
    let large_after = snapshot_large_segment_fingerprints(temp_dir.path(), W4_T2_MAX_INPUT_POINTS);
    assert_eq!(large_after.len(), large_before.len(), "大段不得消失");
    for (segment_id, before) in &large_before {
        assert_eq!(
            before,
            large_after.get(segment_id).expect("大段不得消失"),
            "大段 segment_id={segment_id} 被重写（mtime/内容变化）"
        );
    }
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 2).unwrap().len(),
        segments_before,
        "只有大段时 L2 段数不得变化"
    );
    assert!(load_segments_for_level(temp_dir.path(), 1)
        .unwrap()
        .is_empty());
    assert!(load_segments_for_level(temp_dir.path(), 0)
        .unwrap()
        .is_empty());

    // 终止后静止语义成立：继续驱动到静止，全程无压缩。
    let tail = drive_l2_compaction_to_quiescence(&compactor, 10);
    assert!(
        tail.iter().all(|stats| !stats.compacted),
        "50 轮后仍未静止：L2ToL2 在只有大段时反复选中窗口（活锁/空转）"
    );

    // 数据完整：全部点可读、值不变。
    let points = decoded_numeric_points(temp_dir.path(), series_id);
    assert_eq!(points.len(), total_points, "全部点必须完整可读");
    for (ts, value) in &points {
        assert_eq!(*value, *ts as f64, "ts={ts} 的值必须保持不变");
    }
}

// ---------------------------------------------------------------------------
// L2ToL2 触发门控的成本面：不需要收敛的拍不得做 manifest survey。
// 旧实现无条件先 `load_level_compaction_candidates`（每段一次 open+read+
// parse），`count_trigger == 0` 与「段数 ≤ 阈值」两种必然不收敛的拍也要把
// 全部 L2 manifest 读一遍——关闭收敛（l2_trigger=0）时每个后台拍（5s）对
// 16.5k 段白读 16.5k 次，永久持续。两测用 `level_manifest_read_count`
// （thread-local，planning.rs 埋点）断言 survey 次数为 0。
// ---------------------------------------------------------------------------

/// `l2_trigger = 0`（关闭收敛）：L2 有 20 个小段也不得读任何 manifest。
#[test]
fn l2_trigger_zero_skips_manifest_survey() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "l2-trigger-zero")])
        .unwrap()
        .series_id;
    write_l2_small_segments(&temp_dir, &registry, series_id, 20);

    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        2048,
        Arc::new(AtomicU64::new(1000)),
    )
    .with_l2_compaction_trigger(0);

    super::planning::reset_level_manifest_read_count();
    let stats = compactor.compact_once_with_stats().unwrap();
    let reads = super::planning::level_manifest_read_count();

    assert!(
        !stats.compacted,
        "l2_trigger=0 必须永久关闭收敛，实际 {stats:?}"
    );
    assert_eq!(
        reads, 0,
        "l2_trigger=0 的拍不得做 L2 manifest survey（每段一次 open+read+parse），\
         实际读了 {reads} 次 manifest（20 段）"
    );
}

/// L2 段数（20）不超过默认触发阈值（32）：同样不得读任何 manifest。
#[test]
fn l2_count_at_or_below_trigger_skips_manifest_survey() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "l2-below-trigger")])
        .unwrap()
        .series_id;
    write_l2_small_segments(&temp_dir, &registry, series_id, 20);

    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        2048,
        Arc::new(AtomicU64::new(1000)),
    );

    super::planning::reset_level_manifest_read_count();
    let stats = compactor.compact_once_with_stats().unwrap();
    let reads = super::planning::level_manifest_read_count();

    assert!(
        !stats.compacted,
        "20 段 ≤ 默认触发阈值 32，不得收敛，实际 {stats:?}"
    );
    assert_eq!(
        reads, 0,
        "L2 段数 ≤ 触发阈值的拍不得做 L2 manifest survey，\
         实际读了 {reads} 次 manifest（20 段）"
    );
}

// ---------------------------------------------------------------------------
// W4-T3（表征绿）：L2 替换 marker 的崩溃幂等重放。计划将 T3 标「红」，但所
// 测机制（`finalize_pending_compaction_replacements` 对 `<data>/
// .compaction-replacements/` 内 marker 的启动重放，execution.rs）已存在且
// level 无关——本测以 L2 形态表征既有行为，预期绿；跑出断言红 = 真实缺陷
// 登记。W4-T4 让 L2 输出段真实存在后，本测是其崩溃安全网。
// ---------------------------------------------------------------------------

/// 崩溃时序：压缩已写完输出段、marker 已原子落盘、源段尚未删除时进程死掉。
/// 启动重放（第一次 finalize）必须把半应用状态收敛到终态：源段全部消失、
/// 输出段存活、marker 被消费、数据可读且正确；重复调用（marker 已空）必须
/// Ok 且段集合不变（幂等，目录指纹 mtime+内容逐字节比对）。
#[test]
fn l2_compaction_marker_replay_is_idempotent() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "l2-marker-crash")])
        .unwrap()
        .series_id;

    // 4 个 L2 源段（段 id 1..=4，各 100 点，ts 首尾相接覆盖 [0, 400)）。
    write_l2_small_segments(&temp_dir, &registry, series_id, 4);

    // L2 输出段：源段的合并形态（全部 400 点），段 id 1000 避开源 id 空间。
    // 真实时序里它由压缩在发布 marker 前落盘——直接写盘即「输出已落盘」。
    let merged_points: Vec<(i64, f64)> = (0..400i64).map(|ts| (ts, ts as f64)).collect();
    write_level_segment(&temp_dir, 2, 1000, series_id, &registry, &merged_points);

    let mut source_roots = Vec::new();
    let mut output_root = None;
    for segment in load_segments_for_level(temp_dir.path(), 2).unwrap() {
        if segment.manifest.segment_id == 1000 {
            output_root = Some(segment.root.clone());
        } else {
            source_roots.push(segment.root.clone());
        }
    }
    let output_root = output_root.expect("fixture 必须含输出段");
    assert_eq!(source_roots.len(), 4, "fixture 必须恰好 4 个源段");
    assert!(
        source_roots.iter().all(|root| root.exists()),
        "半应用前置：源段必须在盘"
    );

    // 半应用 marker：经生产写入路径原样落盘（原子写 + fsync），apply 未执行
    // ——等价于 marker 写完、apply 前崩溃的真实现场。
    let marker_path = execution::write_compaction_replacement_marker(
        temp_dir.path(),
        &source_roots,
        std::slice::from_ref(&output_root),
    )
    .unwrap();
    assert!(marker_path.exists(), "半应用前置：marker 必须在盘");

    // 第一次 finalize：启动重放把半应用状态收敛到终态。
    finalize_pending_compaction_replacements(temp_dir.path()).unwrap();

    for root in &source_roots {
        assert!(!root.exists(), "源段 {} 必须被重放删除", root.display());
    }
    assert!(output_root.exists(), "输出段必须存活");
    assert!(!marker_path.exists(), "marker 必须被消费");
    let marker_dir = temp_dir.path().join(super::COMPACTION_REPLACEMENT_DIR);
    assert_eq!(
        fs::read_dir(&marker_dir).unwrap().count(),
        0,
        "重放后 marker 目录必须为空"
    );

    // 段集合正确：L2 只剩输出段（id=1000）——本 fixture 输出=源合并形态，
    // 数据集合相同，只有段集合断言能区分「读到输出段」与「源段未删」。
    let l2_ids: Vec<u64> = load_segments_for_level(temp_dir.path(), 2)
        .unwrap()
        .into_iter()
        .map(|segment| segment.manifest.segment_id)
        .collect();
    assert_eq!(l2_ids, vec![1000], "重放后 L2 必须只剩输出段");
    let points = decoded_numeric_points(temp_dir.path(), series_id);
    assert_eq!(points.len(), 400, "合并形态 400 点必须全量可读");
    for (ts, value) in &points {
        assert_eq!(*value, *ts as f64, "ts={ts} 的值必须保持不变");
    }

    // 幂等：marker 已空，二次 finalize 必须 Ok 且一切不变（目录指纹
    // mtime + 内容逐字节比对、源段不复活、marker 目录仍为空）。
    let output_fingerprint = segment_dir_fingerprint(&output_root);
    finalize_pending_compaction_replacements(temp_dir.path()).unwrap();
    assert_eq!(
        segment_dir_fingerprint(&output_root),
        output_fingerprint,
        "二次 finalize 不得触碰存活段（mtime/内容变化）"
    );
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 2).unwrap().len(),
        1,
        "二次 finalize 后段集合不得变化"
    );
    for root in &source_roots {
        assert!(!root.exists(), "二次 finalize 后源段不得复活");
    }
    assert_eq!(
        fs::read_dir(&marker_dir).unwrap().count(),
        0,
        "二次 finalize 后 marker 目录仍须为空"
    );
}

/// 变体（防半写 marker）：截断的 marker JSON。既有语义（execution.rs
/// `parse_compaction_replacement_marker` 失败经 `?` 向上传播，不跳过）：
/// finalize 返回 Err 且现场完全不动——源不删、输出不删、损坏 marker 留在
/// 盘上（不消费，重放保持可重试）。只表征既有行为，不新增要求。
#[test]
fn finalize_pending_replacements_rejects_truncated_marker() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "l2-marker-truncated")])
        .unwrap()
        .series_id;

    write_l2_small_segments(&temp_dir, &registry, series_id, 2);
    let merged_points: Vec<(i64, f64)> = (0..200i64).map(|ts| (ts, ts as f64)).collect();
    write_level_segment(&temp_dir, 2, 1000, series_id, &registry, &merged_points);

    let mut source_roots = Vec::new();
    let mut output_root = None;
    for segment in load_segments_for_level(temp_dir.path(), 2).unwrap() {
        if segment.manifest.segment_id == 1000 {
            output_root = Some(segment.root.clone());
        } else {
            source_roots.push(segment.root.clone());
        }
    }
    let output_root = output_root.expect("fixture 必须含输出段");

    // 手写截断 marker：合法 payload 的前半（半写现场；生产写入是原子替换 +
    // fsync，该状态只可能来自写路径 bug 或盘损坏——防御性表征）。
    let marker_dir = temp_dir.path().join(super::COMPACTION_REPLACEMENT_DIR);
    fs::create_dir_all(&marker_dir).unwrap();
    let truncated_marker = marker_dir.join("replace-truncated.json");
    fs::write(
        &truncated_marker,
        br#"{"version":1,"source_segments":["segm"#,
    )
    .unwrap();

    let result = finalize_pending_compaction_replacements(temp_dir.path());
    assert!(
        result.is_err(),
        "截断 marker 必须报错（既有语义：parse 失败向上传播，不静默跳过）"
    );

    // 现场完全不动：源段在盘、输出段在盘、损坏 marker 未被消费。
    for root in &source_roots {
        assert!(root.exists(), "报错路径不得删除源段 {}", root.display());
    }
    assert!(output_root.exists(), "报错路径不得动输出段");
    assert!(
        truncated_marker.exists(),
        "损坏 marker 不得被消费（保持可重试，等后续修复介入）"
    );
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 2).unwrap().len(),
        3,
        "报错路径不得改变段集合"
    );
}

#[test]
fn compacts_overlapping_l0_segments_into_l1() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

    assert!(l0.is_empty());
    assert_eq!(l1.len(), 1);

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].header.point_count, 4);
    let decoded = execution::decode_chunk_points_for_compaction(&chunks[0]).unwrap();
    assert_eq!(decoded[0].ts, 10);
    assert_eq!(decoded[3].ts, 30);
}

#[test]
fn compaction_preserves_histogram_payloads() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("rpc_duration_seconds", &[Label::new("job", "backend")])
        .unwrap()
        .series_id;

    let first_chunk = make_histogram_chunk(series_id, &[10, 20]);
    let second_chunk = make_histogram_chunk(series_id, &[30, 40]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let decoded = chunks
        .iter()
        .flat_map(|chunk| execution::decode_chunk_points_for_compaction(chunk).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(decoded.len(), 4);
    for (point, ts) in decoded.iter().zip([10, 20, 30, 40]) {
        assert_eq!(point.ts, ts);
        assert_eq!(point.value, Value::from(sample_histogram()));
    }
}

#[test]
fn compactor_uses_shared_segment_id_allocator() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let next_segment_id = Arc::new(AtomicU64::new(100));
    let compactor =
        Compactor::new_with_segment_id_allocator(temp_dir.path(), 8, Arc::clone(&next_segment_id));
    compactor.compact_once().unwrap();

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.segment_id, 100);
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 101);
}

#[test]
fn compactor_limits_source_window_per_pass() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    // 18 L0 segments whose time ranges form one overlapping chain (segment i
    // spans [2(i-1), 2(i-1)+17]), so window selection runs on the
    // overlap-cluster path where only the source window cap can bound the
    // pass. A window of 16 consumes segments 1..=16 and leaves the two
    // newest untouched; every timestamp stays unique across segments.
    for segment_id in 1..=18u64 {
        let i = segment_id as i64 - 1;
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_numeric_chunk(
                series_id,
                &[(2 * i, i as f64), (2 * i + 17, 100.0)],
            )],
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

    assert_eq!(l0.len(), 2);
    assert_eq!(l1.len(), 1);
    assert_eq!(
        l0.iter()
            .map(|segment| segment.manifest.segment_id)
            .collect::<Vec<_>>(),
        vec![17, 18]
    );
}

#[test]
fn compaction_output_point_budget_is_decoupled_from_chunk_point_cap() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "budget")])
        .unwrap()
        .series_id;

    // 4 non-overlapping L0 segments x 600k points = 2.4M points total. The
    // decoupled output budget is now 8x the L0 flush shard size (8,388,608
    // points), so the discriminating dataset scales accordingly: with
    // chunk_points=512 the legacy cap-derived budgets (262,144 / 1,048,576)
    // split this into multiple output segments; the 8M-point budget must keep
    // it in one segment so tuning chunk_points no longer multiplies the
    // output segment count.
    for (segment_id, base_ts) in [(1u64, 0i64), (2, 600_000), (3, 1_200_000), (4, 1_800_000)] {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            (0..10)
                .map(|chunk_index| {
                    let start = base_ts + chunk_index * 60_000;
                    make_numeric_chunk(
                        series_id,
                        &(start..start + 60_000)
                            .map(|ts| (ts, ts as f64))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 512);
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(
        stats.output_segments, 1,
        "2.4M points must stay in one output segment when chunk_points=512 \
         (the previous 1M-point budget split this into {})",
        stats.output_segments
    );

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.point_count, 2_400_000);

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let total_points = chunks
        .iter()
        .map(|chunk| chunk.header.point_count as usize)
        .sum::<usize>();
    assert_eq!(total_points, 2_400_000);
}

#[test]
fn compaction_output_point_budget_keeps_default_chunk_points_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "budget")])
        .unwrap()
        .series_id;

    // Same 2.4M-point shape as the decoupling test, but with the default
    // chunk_points=2048: the previous budget (2048 x 512 = 1,048,576) would
    // split this into three output segments; with the 8M-point budget the
    // default configuration must keep it in a single segment.
    for (segment_id, base_ts) in [(1u64, 0i64), (2, 600_000), (3, 1_200_000), (4, 1_800_000)] {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            (0..10)
                .map(|chunk_index| {
                    let start = base_ts + chunk_index * 60_000;
                    make_numeric_chunk(
                        series_id,
                        &(start..start + 60_000)
                            .map(|ts| (ts, ts as f64))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 2048);
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(stats.output_segments, 1);

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.point_count, 2_400_000);
}

#[test]
fn compactor_splits_large_output_into_multiple_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        let start = (segment_id as i64 - 1) * 2_400;
        let points = (start..start + 2_400)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();

        let mut chunks = HashMap::new();
        chunks.insert(series_id, vec![make_numeric_chunk(series_id, &points)]);
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    // The budget is decoupled from the chunk point cap, so the shard-boundary
    // behavior is exercised with an explicit small budget instead of leaning
    // on point_cap: 9600 points against a 4000-point budget must split, with
    // every output segment bounded by the budget plus one trailing chunk.
    let compactor = Compactor::new(temp_dir.path(), 2).with_output_segment_point_budget(4_000);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert!(l0.is_empty());
    assert!(l1.len() >= 2);
    assert!(l1
        .iter()
        .all(|segment| segment.manifest.point_count <= 4_000 + 2));

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let total_points = chunks
        .iter()
        .map(|chunk| chunk.header.point_count as usize)
        .sum::<usize>();
    assert_eq!(total_points, 9_600);
}

#[test]
fn compaction_output_budget_eight_million_reduces_segment_count() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "budget")])
        .unwrap()
        .series_id;

    // The "8-in-8-out" regression shape: 8 L0 flush-sized segments (~1M
    // points each). Each segment's timestamps are interleaved modulo 8 so the
    // segment time ranges overlap (one window takes all 8) while every point
    // timestamp stays unique across segments (merged total is exactly 8M
    // distinct points, no dedup shrinkage). An output budget equal to the L0
    // shard size re-emitted the 8M merged points as ~8 more 1M segments, so
    // the level segment count never shrank. The 8M-point budget must collapse
    // the whole window into a single ~8M-point L1 segment (8 -> 1).
    for segment_index in 0..8u64 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            (0..16)
                .map(|chunk_index| {
                    let k_start = chunk_index * 62_500;
                    make_numeric_chunk(
                        series_id,
                        &(k_start..k_start + 62_500)
                            .map(|k| {
                                let ts = segment_index as i64 + 8 * k;
                                (ts, ts as f64)
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_index + 1)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 2048);
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(stats.source_segments, 8);
    assert_eq!(
        stats.output_segments, 1,
        "8x1M-point overlapping L0 segments must collapse into one L1 output \
         segment, got {}",
        stats.output_segments
    );
    assert_eq!(stats.output_points, 8_000_000);

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert!(l0.is_empty());
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.point_count, 8_000_000);
}

#[test]
fn compaction_source_window_sixteen_merges_sixteen_l0_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "window")])
        .unwrap()
        .series_id;

    // 16 L0 flush-sized segments (~1M points each) with timestamps interleaved
    // modulo 16: every segment time range overlaps its neighbours (one
    // overlap cluster, so a single window may take all 16) while every point
    // timestamp stays unique across segments (the merged total is exactly 16M
    // distinct points, no dedup shrinkage). An 8-segment window swallowed only
    // half the cluster per pass; with the window at 16 and the 8M-point output
    // budget, one pass must take the whole 16M-point window and re-emit it as
    // exactly two budget-sized L1 segments (16M / 8,388,608).
    for segment_index in 0..16u64 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            (0..16)
                .map(|chunk_index| {
                    let k_start = chunk_index * 62_500;
                    make_numeric_chunk(
                        series_id,
                        &(k_start..k_start + 62_500)
                            .map(|k| {
                                let ts = segment_index as i64 + 16 * k;
                                (ts, ts as f64)
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_index + 1)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 2048);
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(
        stats.source_segments, 16,
        "a 16-segment source window must swallow the whole overlap cluster"
    );
    assert_eq!(
        stats.output_segments, 2,
        "16x1M-point overlapping L0 segments must re-emit as two budget-sized \
         L1 output segments (16M points / 8M budget), got {}",
        stats.output_segments
    );
    assert_eq!(stats.output_points, 16_000_000);

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert!(l0.is_empty());
    assert_eq!(l1.len(), 2);
    assert_eq!(
        l1.iter()
            .map(|segment| segment.manifest.point_count)
            .sum::<usize>(),
        16_000_000
    );
    assert!(l1
        .iter()
        .all(|segment| segment.manifest.point_count <= 8_388_608 + 2048));
}

/// 全墓碑窗口（2026-10-30 审查修复）：窗口内所有点被墓碑盖住时，正确的
/// 替换是**纯删除**（空输出 marker）。此前 execution 返回 compacted=false 且
/// 不删源段，planning 又把标志强改 true——调用方对仍在磁盘上的源段记录幻影
/// removed_roots（内存索引被摘除）、drain 循环每轮重选同一窗口烧光预算（活锁）。
#[test]
fn all_tombstoned_window_is_deleted_not_livelocked() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    // 全范围墓碑：两段（时间重叠，必成窗口）所有点全部被盖住
    let mut tombstones = HashMap::new();
    tombstones.insert(
        series_id,
        vec![TombstoneRange {
            start: 0,
            end: 1000,
        }],
    );
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    let outcome = compactor.compact_once_with_changes().unwrap();
    assert!(
        outcome.stats.compacted,
        "空输出替换也是完成了一次压缩（否则调用方无从推进）"
    );

    // 源段必须真的从磁盘消失（红形态：残留 + 幻影 removed_roots + 活锁）
    let loaded = load_segments(temp_dir.path()).unwrap();
    assert!(
        loaded.chunks_by_series.is_empty(),
        "全墓碑窗口的源段应被删除，实际仍加载到 {:?}",
        loaded.chunks_by_series.keys().collect::<Vec<_>>()
    );

    // 第二轮 pass：窗口已消失，无事可做（红形态：每轮重选同一窗口恒 true）
    let second = compactor.compact_once_with_changes().unwrap();
    assert!(
        !second.stats.compacted,
        "死窗口删除后不应再被压缩选中（活锁复现）"
    );
}

#[test]
fn compactor_purges_tombstoned_points() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 15, end: 21 }]);
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    assert!(compactor.compact_once().unwrap());

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let decoded = execution::decode_chunk_points_for_compaction(&chunks[0]).unwrap();
    let timestamps = decoded
        .into_iter()
        .map(|point| point.ts)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![10, 30]);
}

#[test]
fn compaction_fails_when_any_source_segment_is_corrupted() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_writer = SegmentWriter::new(temp_dir.path(), 0, 1).unwrap();
    let mut first_chunks = HashMap::new();
    first_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    first_writer
        .write_segment(&registry, &first_chunks)
        .unwrap();

    let second_writer = SegmentWriter::new(temp_dir.path(), 0, 2).unwrap();
    let mut second_chunks = HashMap::new();
    second_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );
    second_writer
        .write_segment(&registry, &second_chunks)
        .unwrap();

    let corrupt_root = first_writer.layout().root.clone();
    let mut manifest_bytes = fs::read(first_writer.layout().manifest_path.clone()).unwrap();
    manifest_bytes[0] ^= 0xff;
    fs::write(first_writer.layout().manifest_path.clone(), manifest_bytes).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    let err = compactor.compact_once().unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("failed compaction validation"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("manifest crc32 mismatch"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains(&corrupt_root.display().to_string()),
        "unexpected error: {message}"
    );
    assert!(
        corrupt_root.exists(),
        "corrupt source root should stay in place"
    );
    assert!(
        second_writer.layout().root.exists(),
        "healthy source root should not be compacted away"
    );
    assert!(
        load_segments_for_level(temp_dir.path(), 1)
            .unwrap()
            .is_empty(),
        "compaction must not publish replacement output when any source root is corrupt"
    );
}

#[test]
fn finalize_pending_replacements_removes_marked_source_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut l0_chunks = HashMap::new();
    l0_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 1.0)])]);
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &l0_chunks)
        .unwrap();

    let mut l1_chunks = HashMap::new();
    l1_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 2.0)])]);
    SegmentWriter::new(temp_dir.path(), 1, 2)
        .unwrap()
        .write_segment(&registry, &l1_chunks)
        .unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l0.len(), 1);
    assert_eq!(l1.len(), 1);

    let source_root = l0[0].root.clone();
    let output_root = l1[0].root.clone();
    let marker_path = execution::write_compaction_replacement_marker(
        temp_dir.path(),
        std::slice::from_ref(&source_root),
        std::slice::from_ref(&output_root),
    )
    .unwrap();
    assert!(marker_path.exists());

    finalize_pending_compaction_replacements(temp_dir.path()).unwrap();

    assert!(!source_root.exists(), "source segment should be removed");
    assert!(
        output_root.exists(),
        "replacement output should be preserved"
    );
    assert!(
        !marker_path.exists(),
        "replacement marker should be removed after apply"
    );
}

fn make_numeric_chunk(series_id: u64, points: &[(i64, f64)]) -> Chunk {
    let points = points
        .iter()
        .map(|(ts, value)| ChunkPoint {
            ts: *ts,
            value: Value::F64(*value),
        })
        .collect::<Vec<_>>();

    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: points.len() as u16,
            min_ts: points.first().unwrap().ts,
            max_ts: points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}

// ---------------------------------------------------------------------------
// Chunk pass-through fast path (non-overlapping series skip decode/merge).
// ---------------------------------------------------------------------------

#[test]
fn compaction_passthrough_non_overlapping_chunks_without_decoding() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "passthrough")])
        .unwrap()
        .series_id;

    // Function level: two non-overlapping chunks with payloads must pass
    // through byte-for-byte, built by field-level copies only — never a
    // whole-chunk clone (which would materialize the decoded points).
    let first = make_numeric_chunk(series_id, &[(0, 1.0), (5, 2.0)]);
    let second = make_numeric_chunk(series_id, &[(6, 3.0), (20, 4.0)]);
    let chunk_refs = [&first, &second];

    crate::engine::chunk::reset_chunk_clone_count();
    let mut emitted = Vec::new();
    let passed_through =
        execution::try_pass_through_series_chunks(series_id, &chunk_refs, 8, None, None, |chunk| {
            emitted.push(chunk);
            Ok(())
        })
        .unwrap();

    assert!(passed_through, "non-overlapping chunks must pass through");
    assert_eq!(
        emitted.len(),
        2,
        "pass-through must emit exactly one chunk per input chunk"
    );
    assert_eq!(
        crate::engine::chunk::chunk_clone_count(),
        0,
        "pass-through emit must never clone a whole chunk"
    );
    for (output, source) in emitted.iter().zip(chunk_refs.iter()) {
        assert!(output.points.is_empty());
        assert_eq!(output.header.min_ts, source.header.min_ts);
        assert_eq!(output.header.max_ts, source.header.max_ts);
        assert_eq!(output.header.point_count, source.header.point_count);
        assert_eq!(output.header.ts_codec, source.header.ts_codec);
        assert_eq!(output.header.value_codec, source.header.value_codec);
        assert_eq!(
            output.encoded_payload, source.encoded_payload,
            "pass-through payload must stay byte-for-byte identical"
        );
    }

    // End to end: four non-overlapping L0 segments (the default count trigger)
    // compact into one L1 output whose chunks are the inputs verbatim.
    let source_windows = [
        [(0, 1.0), (9, 2.0)],
        [(10, 3.0), (19, 4.0)],
        [(20, 5.0), (29, 6.0)],
        [(30, 7.0), (39, 8.0)],
    ];
    for (offset, points) in source_windows.iter().enumerate() {
        let mut chunks = HashMap::new();
        chunks.insert(series_id, vec![make_numeric_chunk(series_id, points)]);
        SegmentWriter::new(temp_dir.path(), 0, offset as u64 + 1)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    // Snapshot the loaded-form payloads before the pass.
    let mut source_payloads_by_min_ts = HashMap::new();
    for segment in load_segments_for_level(temp_dir.path(), 0).unwrap() {
        for chunk in segment.chunks_by_series.get(&series_id).unwrap() {
            source_payloads_by_min_ts.insert(chunk.header.min_ts, chunk.encoded_payload.clone());
        }
    }
    assert_eq!(source_payloads_by_min_ts.len(), 4);

    crate::engine::chunk::reset_chunk_clone_count();
    // A shared id allocator keeps the pass from re-loading every segment
    // just to allocate the output id, so the only remaining clones are the
    // four window loads (one per input chunk, loader.rs chunk materialize).
    let compactor =
        Compactor::new_with_segment_id_allocator(temp_dir.path(), 8, Arc::new(AtomicU64::new(100)));
    let stats = compactor.compact_once_with_stats().unwrap();

    assert!(stats.compacted);
    assert_eq!(stats.source_points, 8);
    assert_eq!(stats.output_points, 8, "pass-through must conserve points");
    assert_eq!(
        stats.output_chunks, 4,
        "pass-through must neither merge nor re-split chunks"
    );
    assert_eq!(
        stats.passthrough_chunks, 4,
        "every passed-through chunk must be counted in run stats"
    );
    assert_eq!(
        stats.passthrough_points, 8,
        "passed-through points must be counted in run stats"
    );
    assert_eq!(
        crate::engine::chunk::chunk_clone_count(),
        4,
        "only the four window-load clones may happen; pass-through emit adds none"
    );

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert!(l0.is_empty());
    assert_eq!(l1.len(), 1);
    let output_chunks = l1[0].chunks_by_series.get(&series_id).unwrap();
    assert_eq!(output_chunks.len(), 4);
    for chunk in output_chunks {
        assert_eq!(
            chunk.encoded_payload,
            *source_payloads_by_min_ts
                .get(&chunk.header.min_ts)
                .expect("output chunk min_ts must match a source chunk"),
            "reloaded output payload must be byte-for-byte identical to its source"
        );
    }
}

#[test]
fn compaction_passthrough_falls_back_on_time_overlap() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "overlap-fallback")])
        .unwrap()
        .series_id;

    // Closed ranges sharing the ts=20 endpoint overlap — the same predicate
    // the query-side scan uses (query_read/analysis.rs) — so pass-through
    // must decline and leave the decode/merge path in charge.
    let first = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second = make_numeric_chunk(series_id, &[(20, 3.0), (30, 4.0)]);
    let chunk_refs = [&first, &second];

    let mut emitted = 0usize;
    let passed_through =
        execution::try_pass_through_series_chunks(series_id, &chunk_refs, 8, None, None, |_| {
            emitted += 1;
            Ok(())
        })
        .unwrap();
    assert!(
        !passed_through,
        "a shared timestamp must force the decode path"
    );
    assert_eq!(emitted, 0, "a declined pass-through must not emit anything");

    // End to end: overlapping segments keep the decode/merge semantics of
    // compacts_overlapping_l0_segments_into_l1 (one ordered chunk, exact
    // duplicates of ts+value deduped, distinct same-ts values both kept).
    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first]);
    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();
    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(
        stats.passthrough_chunks, 0,
        "an overlap-forced decode pass must not count pass-through chunks"
    );
    assert_eq!(
        stats.passthrough_points, 0,
        "an overlap-forced decode pass must not count pass-through points"
    );

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    assert_eq!(chunks.len(), 1, "overlapping chunks must merge into one");
    assert_eq!(chunks[0].header.point_count, 4);
    let timestamps = execution::decode_chunk_points_for_compaction(&chunks[0])
        .unwrap()
        .into_iter()
        .map(|point| point.ts)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![10, 20, 20, 30]);
}

#[test]
fn compaction_passthrough_falls_back_on_tombstones() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "tombstone-fallback")])
        .unwrap()
        .series_id;

    // The fallback is deliberately conservative this round: any tombstone
    // range for the series — even one intersecting nothing — declines
    // pass-through, because point-level filtering is only correct there.
    let first = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second = make_numeric_chunk(series_id, &[(30, 3.0), (40, 4.0)]);
    let chunk_refs = [&first, &second];
    let ranges = [TombstoneRange { start: 5, end: 6 }];

    let mut emitted = 0usize;
    let passed_through = execution::try_pass_through_series_chunks(
        series_id,
        &chunk_refs,
        8,
        Some(&ranges),
        None,
        |_| {
            emitted += 1;
            Ok(())
        },
    )
    .unwrap();
    assert!(
        !passed_through,
        "tombstoned series must force the decode path"
    );
    assert_eq!(emitted, 0, "a declined pass-through must not emit anything");

    // End to end: tombstoned points stay purged
    // (compactor_purges_tombstoned_points semantics; the overlapping source
    // segments are what makes the pass eligible).
    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();
    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 15, end: 21 }]);
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    assert!(compactor.compact_once().unwrap());

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let timestamps = execution::decode_chunk_points_for_compaction(&chunks[0])
        .unwrap()
        .into_iter()
        .map(|point| point.ts)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![10, 30]);
}

#[test]
fn compaction_passthrough_falls_back_when_retention_cutoff_set() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "retention-fallback")])
        .unwrap()
        .series_id;

    // Retention rewrites drop points below the cutoff, so even non-
    // overlapping chunks must take the decode path.
    let first = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second = make_numeric_chunk(series_id, &[(30, 3.0), (40, 4.0)]);
    let chunk_refs = [&first, &second];

    let mut emitted = 0usize;
    let passed_through = execution::try_pass_through_series_chunks(
        series_id,
        &chunk_refs,
        8,
        None,
        Some(25),
        |_| {
            emitted += 1;
            Ok(())
        },
    )
    .unwrap();
    assert!(
        !passed_through,
        "retention rewrites must force the decode path"
    );
    assert_eq!(emitted, 0, "a declined pass-through must not emit anything");

    // End to end: the retention rewrite keeps dropping expired points.
    let mut chunks = HashMap::new();
    chunks.insert(
        series_id,
        vec![make_numeric_chunk(
            series_id,
            &[(1, 1.0), (2, 2.0), (100, 100.0), (101, 101.0)],
        )],
    );
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &chunks)
        .unwrap();

    let loaded_l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    assert_eq!(loaded_l0.len(), 1);

    let compactor = Compactor::new(temp_dir.path(), 8);
    let outcome = compactor
        .stage_segment_rewrite_with_retention(&loaded_l0[0], 50)
        .unwrap();
    assert_eq!(outcome.output_roots.len(), 1);

    let reloaded = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let output = reloaded
        .iter()
        .find(|segment| segment.root == outcome.output_roots[0])
        .expect("retention rewrite output must be loadable");
    let timestamps = output
        .chunks_by_series
        .get(&series_id)
        .unwrap()
        .iter()
        .flat_map(|chunk| execution::decode_chunk_points_for_compaction(chunk).unwrap())
        .map(|point| point.ts)
        .collect::<Vec<_>>();
    assert_eq!(
        timestamps,
        vec![100, 101],
        "expired points must stay pruned"
    );
}

fn make_histogram_chunk(series_id: u64, timestamps: &[i64]) -> Chunk {
    let histogram = sample_histogram();
    let points = timestamps
        .iter()
        .map(|ts| ChunkPoint {
            ts: *ts,
            value: Value::from(histogram.clone()),
        })
        .collect::<Vec<_>>();

    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Blob).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Blob,
            value_family: Some(SeriesValueFamily::Histogram),
            point_count: points.len() as u16,
            min_ts: points.first().unwrap().ts,
            max_ts: points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}

#[test]
fn close_drain_defers_l2_terminal_convergence() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "scratch-close")])
        .unwrap()
        .series_id;
    write_l2_small_segments(&temp_dir, &registry, series_id, 40);
    let compactor = Compactor::new_with_segment_id_allocator(
        temp_dir.path(),
        2048,
        Arc::new(AtomicU64::new(1000)),
    );
    for _ in 0..5 {
        let stats = compactor.compact_once_for_close().unwrap().stats;
        assert!(
            !stats.compacted,
            "close drain must defer L2 convergence, got {stats:?}"
        );
    }
    let l2 = load_segments_for_level(temp_dir.path(), 2).unwrap();
    assert_eq!(l2.len(), 40, "close drain must not touch the L2 backlog");
    let stats = compactor.compact_once_with_changes().unwrap().stats;
    assert!(stats.compacted, "background entry must still converge");
    assert_eq!((stats.source_level, stats.target_level), (Some(2), Some(2)));
}

/// C-3（延迟删除）：pending replacement 的源段目录**仍在盘上**（延迟删除窗口，
/// 查询可读），但必须从所有段目录扫描里消失。
///
/// 窗口态模拟：marker 已落盘、每个源段目录内带哨兵——等价于「compaction 已
/// 发布替换、索引刷新前的维护窗口」的生产现场。若扫描仍能看到这些源段，
/// 选窗就会把「已经被替换」的源段再合并一次（源段被合并进新输出，marker
/// 随后删除的却仍是同一批源段 → 数据重复/丢失），inventory 与 retention
/// sweep 也会与 marker 的待删清单打架。
///
/// 断言面用 `load_segments_for_level` / `count_level_segment_dirs` /
/// `list_segment_dirs`——三者都经 `collect_segment_dirs`，即全部扫描路径的
/// 唯一收口。
#[test]
fn pending_replacement_sources_are_excluded_from_candidates() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "pending-source-exclusion")])
        .unwrap()
        .series_id;

    // L2 里 4 个小源段（ts 首尾相接 [0,400)）+ 1 个合并输出段（同 400 点）。
    write_l2_small_segments(&temp_dir, &registry, series_id, 4);
    let merged_points: Vec<(i64, f64)> = (0..400i64).map(|ts| (ts, ts as f64)).collect();
    write_level_segment(&temp_dir, 2, 1000, series_id, &registry, &merged_points);

    let mut source_roots = Vec::new();
    let mut output_root = None;
    for segment in load_segments_for_level(temp_dir.path(), 2).unwrap() {
        if segment.manifest.segment_id == 1000 {
            output_root = Some(segment.root.clone());
        } else {
            source_roots.push(segment.root.clone());
        }
    }
    let output_root = output_root.expect("fixture 必须含输出段");
    assert_eq!(source_roots.len(), 4, "fixture 必须恰好 4 个源段");

    // 生产写入路径的产物：marker（权威待删清单）+ 每源段目录内的哨兵
    // （扫描不可见标记）。直接调用生产写入函数，不手工拼装。
    execution::write_compaction_replacement_marker(
        temp_dir.path(),
        &source_roots,
        std::slice::from_ref(&output_root),
    )
    .unwrap();

    // 窗口态前置：源段目录与哨兵都在盘（延迟删除 = 文件还在，查询仍可读）。
    // 哨兵名按契约写字面量（与 `segment::PENDING_REPLACEMENT_DELETE_SENTINEL`
    // 同值）：本测要能在未实现的 HEAD 上编译并断言失败，而不是编译错误。
    for root in &source_roots {
        assert!(root.exists(), "窗口态前置：源段必须仍在盘");
        assert!(
            root.join(".pending-replacement-delete").exists(),
            "窗口态前置：源段必须带哨兵：{}",
            root.display()
        );
    }

    // 核心断言：pending 源段对扫描不可见，只剩输出段。
    let visible = load_segments_for_level(temp_dir.path(), 2).unwrap();
    assert_eq!(
        visible.len(),
        1,
        "pending 源段必须被排除出候选，实际可见 {} 段（含源段时会被重复合并）",
        visible.len()
    );
    assert_eq!(
        visible[0].manifest.segment_id, 1000,
        "唯一可见段必须是输出段"
    );
    assert_eq!(
        crate::engine::segment::count_level_segment_dirs(temp_dir.path(), 2).unwrap(),
        1,
        "L2 目录计数（选窗前的廉价预闸）也必须只看到输出段"
    );
    let listed = crate::engine::segment::list_segment_dirs(temp_dir.path()).unwrap();
    assert_eq!(
        listed.len(),
        1,
        "全量段目录列举（inventory/启动扫描）也必须只看到输出段，实际 {} 项",
        listed.len()
    );

    // marker 未被消费：删除还没发生（阶段二在索引刷新后的下一拍）。
    let marker_dir = temp_dir.path().join(super::COMPACTION_REPLACEMENT_DIR);
    assert_eq!(
        fs::read_dir(&marker_dir).unwrap().count(),
        1,
        "marker 必须仍待处理"
    );
}

/// C-3 不变式（双向）：**哨兵存在 ⟺ marker 存在**。
///
/// 正向由 `pending_replacement_sources_are_excluded_from_candidates` 覆盖
/// （写 marker 即写哨兵）。这里覆盖反向的失败路径：某个源段的哨兵写不进去时，
/// 整拍必须失败**且**已经写下的哨兵要被撤掉、marker 不能留下。否则那些源段
/// 会永久对扫描不可见（无 marker 就无人删它们），比整拍失败严重得多。
///
/// 注入方式：把一个源段目录换成普通文件，使其 `join(SENTINEL)` 的父路径不是
/// 目录 → `create_new` 报 NotADirectory（真实写失败，不依赖权限或 mock）。
#[test]
fn sentinel_write_failure_rolls_back_marker_and_sentinels() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "sentinel-rollback")])
        .unwrap()
        .series_id;

    write_l2_small_segments(&temp_dir, &registry, series_id, 2);
    let merged_points: Vec<(i64, f64)> = (0..200i64).map(|ts| (ts, ts as f64)).collect();
    write_level_segment(&temp_dir, 2, 1000, series_id, &registry, &merged_points);

    let mut source_roots = Vec::new();
    let mut output_root = None;
    for segment in load_segments_for_level(temp_dir.path(), 2).unwrap() {
        if segment.manifest.segment_id == 1000 {
            output_root = Some(segment.root.clone());
        } else {
            source_roots.push(segment.root.clone());
        }
    }
    let output_root = output_root.expect("fixture 必须含输出段");
    assert_eq!(source_roots.len(), 2, "fixture 必须恰好 2 个源段");

    // 第一个源段正常（哨兵会写成功），第二个源段换成普通文件（哨兵写必然失败，
    // 且失败发生在第一个之后 → 触发回滚分支）。
    let broken_root = source_roots[1].clone();
    fs::remove_dir_all(&broken_root).unwrap();
    fs::write(&broken_root, b"not a directory").unwrap();

    let result = execution::write_compaction_replacement_marker(
        temp_dir.path(),
        &source_roots,
        std::slice::from_ref(&output_root),
    );
    assert!(
        result.is_err(),
        "哨兵写失败必须让 marker 发布整拍失败，实际 {result:?}"
    );

    // 反向不变式：不留 marker、不留任何哨兵。
    let marker_dir = temp_dir.path().join(super::COMPACTION_REPLACEMENT_DIR);
    assert_eq!(
        fs::read_dir(&marker_dir)
            .map(|dir| dir.count())
            .unwrap_or(0),
        0,
        "哨兵写失败必须回滚 marker（否则源段永久不可见且无人删）"
    );
    assert!(
        !source_roots[0].join(".pending-replacement-delete").exists(),
        "已写入的哨兵必须被回滚：{}",
        source_roots[0].display()
    );

    // 现场可读：正常源段与输出段仍完整。
    assert!(source_roots[0].join("manifest.bin").exists());
    assert!(output_root.join("manifest.bin").exists());
}
