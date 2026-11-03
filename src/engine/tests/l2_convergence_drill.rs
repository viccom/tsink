//! W4-T5: existing-segment L2 terminal-convergence drill (env-gated, `#[ignore]`).
//!
//! Runs one storage open + background-drain cycle over a *copy* of a real
//! data directory and records how the L2 segment count decays under the
//! W4-T4 `L2ToL2` size-tiered convergence, then verifies the converged store
//! still returns point-identical data for a deterministic series sample.
//!
//! Convergence is driven exclusively through the background compaction
//! thread: close-drain structurally skips L2→L2 and each background tick
//! merges at most one window, so the harness opens with a short
//! `compaction_interval` and polls the on-disk segment directories.
//!
//! # Modes
//!
//! - **Real copy** — set `TSINK_CONVERGE_DATA_DIR` to the *copy* of the
//!   history-layer directory (the layer containing `lane_numeric`). Never
//!   point this at a live storage directory: the drill opens it with a
//!   process lock and runs compaction on it.
//! - **Synthetic smoke** — set `TSINK_CONVERGE_SYNTHETIC_SEGMENTS=<n>` (and
//!   leave `TSINK_CONVERGE_DATA_DIR` unset) to build a small synthetic
//!   fixture in a temp dir first and drill it. This is the zero-setup path
//!   used to validate the harness itself.
//! - Neither set: the test returns immediately (zero env, zero side
//!   effects), so the normal `cargo test --lib` run is unaffected.
//!
//! # Environment
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `TSINK_CONVERGE_DATA_DIR` | — | Copy of the history-layer data dir (required for the real run) |
//! | `TSINK_CONVERGE_SYNTHETIC_SEGMENTS` | — | Build a synthetic fixture with this many L2 segments first (smoke mode) |
//! | `TSINK_CONVERGE_CSV` | `/tmp/tsink-l2-convergence.csv` | Poll-curve CSV output path |
//! | `TSINK_CONVERGE_TIMEOUT_SECS` | `7200` | Panic if convergence has not reached the stop threshold in this wall time |
//! | `TSINK_CONVERGE_POLL_SECS` | `5` | Seconds between curve samples |
//! | `TSINK_CONVERGE_SAMPLE_SERIES` | `30` | First-N (name-sorted) series compared before/after convergence |

use super::*;

use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::time::Instant;

/// Convergence stop threshold: `DEFAULT_L2_TRIGGER` (32) plus one
/// `DEFAULT_SOURCE_WINDOW_SEGMENTS` (16) window of slack, per the W4-T5 plan.
const DRILL_L2_STOP_COUNT: usize = crate::engine::compactor::DEFAULT_L2_TRIGGER
    + crate::engine::compactor::DEFAULT_SOURCE_WINDOW_SEGMENTS;

const DRILL_CSV_DEFAULT: &str = "/tmp/tsink-l2-convergence.csv";
const DRILL_TIMEOUT_DEFAULT_SECS: u64 = 7200;
const DRILL_POLL_DEFAULT_SECS: u64 = 5;
const DRILL_SAMPLE_SERIES_DEFAULT: usize = 30;

const CSV_HEADER: &str = "elapsed_s,l2_count,l1_count,l0_count,rss_kb,l2_to_l2_runs_total";

/// Per-series equality fingerprint compared before/after convergence.
#[derive(Clone, Debug)]
struct SeriesAggregate {
    name: String,
    point_count: usize,
    sum: f64,
    last_ts: i64,
    last_value: Value,
}

fn series_sort_key(series: &MetricSeries) -> String {
    let mut labels = series
        .labels
        .iter()
        .map(|label| format!("{}={}", label.name, label.value))
        .collect::<Vec<_>>();
    labels.sort_unstable();
    format!("{}|{}", series.name, labels.join(","))
}

/// `Ok` on exact equality, `Err` with the two inputs otherwise. NaN compares
/// equal to NaN: numeric lanes may legitimately carry NaN samples and the
/// passthrough merge preserves them bit-for-bit.
fn f64_exact_eq(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a == b
}

fn value_exact_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::F64(left), Value::F64(right)) => f64_exact_eq(*left, *right),
        _ => a == b,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::F64(v) => Some(*v),
        Value::I64(v) => Some(*v as f64),
        Value::U64(v) => Some(*v as f64),
        _ => None,
    }
}

fn open_drill_storage(data_dir: &Path) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(data_dir)
        // Convergence is background-tick work only (close drain skips L2→L2),
        // so shorten the tick from the 5s default. `with_compaction_interval`
        // has no minimum clamp — it parks verbatim — so 1s is valid and 0
        // would spin; 1s yields roughly one convergence window per second.
        .with_compaction_interval(Duration::from_secs(1))
        .build()
        .expect("drill storage must open on the converged copy")
}

/// Enumerates series through the public API, deterministically name-sorts
/// them, keeps the first `sample_size`, and aggregates each one over the full
/// window. The window starts at `0` (not `i64::MIN`) because partition math
/// on `i64::MIN` can overflow in debug builds; every persisted timestamp is
/// non-negative.
fn collect_series_aggregates(storage: &dyn Storage, sample_size: usize) -> Vec<SeriesAggregate> {
    let mut series = storage.list_metrics_with_wal().expect("series enumeration");
    series.sort_by_key(series_sort_key);
    series.truncate(sample_size);

    series
        .iter()
        .map(|item| {
            let points = storage
                .select(&item.name, &item.labels, 0, i64::MAX)
                .unwrap_or_else(|err| panic!("full-window select for {}: {err}", item.name));
            let mut points = points;
            points.sort_by_key(|point| point.timestamp);
            let mut sum = 0.0f64;
            for point in &points {
                if let Some(value) = value_as_f64(&point.value) {
                    sum += value;
                }
            }
            let last = points.last().expect("sampled series must be non-empty");
            SeriesAggregate {
                name: item.name.clone(),
                point_count: points.len(),
                sum,
                last_ts: last.timestamp,
                last_value: last.value.clone(),
            }
        })
        .collect()
}

/// Counts `seg-*` directories per level: `[L0, L1, L2]`.
fn count_level_segments(data_dir: &Path) -> [usize; 3] {
    let mut counts = [0usize; 3];
    for (level, count) in counts.iter_mut().enumerate() {
        let level_dir = data_dir
            .join(NUMERIC_LANE_ROOT)
            .join("segments")
            .join(format!("L{level}"));
        let Ok(entries) = std::fs::read_dir(&level_dir) else {
            continue;
        };
        *count = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_name().to_string_lossy().starts_with("seg-")
                    && entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
            })
            .count();
    }
    counts
}

fn read_vm_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .ok();
        }
    }
    None
}

/// Builds a synthetic convergence fixture: a handful of series written
/// through the public API (so the persisted registry owns their IDs), closed,
/// then `<l2_segment_count>` tiny hand-crafted L2 segments laid out directly
/// in the on-disk copy layout (same technique as the compactor tests'
/// `write_l2_segments_of_sizes`). Every crafted segment carries 24 fresh,
/// disjoint timestamps, so convergence merging is pure passthrough.
fn build_synthetic_l2_fixture(data_dir: &Path, l2_segment_count: usize) {
    const SERIES_COUNT: usize = 8;
    const BASELINE_POINTS_PER_SERIES: usize = 40;
    const CRAFTED_POINTS_PER_SEGMENT: usize = 24;

    let storage = open_drill_storage(data_dir);
    let mut rows = Vec::new();
    for series_index in 0..SERIES_COUNT {
        let labels = vec![Label::new("host", format!("node-{series_index}"))];
        for offset in 0..BASELINE_POINTS_PER_SERIES {
            rows.push(Row::with_labels(
                format!("conv-metric-{series_index}"),
                labels.clone(),
                DataPoint::new(
                    (1_000 + series_index * 1_000 + offset) as i64,
                    (series_index * BASELINE_POINTS_PER_SERIES + offset) as f64 + 0.25,
                ),
            ));
        }
    }
    storage.insert_rows(&rows).expect("fixture baseline insert");
    storage.close().expect("fixture baseline close");

    // The close persisted the registry snapshot; load it back so the crafted
    // segments reference the same series IDs the storage assigned.
    let snapshot_path = data_dir.join(SERIES_INDEX_FILE_NAME);
    let loaded = SeriesRegistry::load_persisted_state(&snapshot_path)
        .expect("fixture registry snapshot must load")
        .expect("close must persist the series registry");
    let registry = loaded.registry;
    let mut series_ids = Vec::with_capacity(SERIES_COUNT);
    for series_index in 0..SERIES_COUNT {
        let resolution = registry
            .resolve_or_insert(
                &format!("conv-metric-{series_index}"),
                &[Label::new("host", format!("node-{series_index}"))],
            )
            .expect("fixture series resolution");
        assert!(
            !resolution.created,
            "fixture series must already exist in the persisted registry"
        );
        series_ids.push(resolution.series_id);
    }

    let lane_root = data_dir.join(NUMERIC_LANE_ROOT);
    for segment_index in 0..l2_segment_count {
        let series_id = series_ids[segment_index % SERIES_COUNT];
        let base_ts = 10_000_000i64 + segment_index as i64 * CRAFTED_POINTS_PER_SEGMENT as i64;
        let points = (0..CRAFTED_POINTS_PER_SEGMENT)
            .map(|offset| {
                (
                    base_ts + offset as i64,
                    (segment_index * CRAFTED_POINTS_PER_SEGMENT + offset) as f64 + 0.5,
                )
            })
            .collect::<Vec<_>>();
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(series_id, &points)],
        );
        SegmentWriter::new(&lane_root, 2, 10_000 + segment_index as u64)
            .expect("crafted segment writer")
            .write_segment(&registry, &chunks)
            .expect("crafted L2 segment write");
    }
}

fn assert_aggregates_equal(baseline: &[SeriesAggregate], converged: &[SeriesAggregate]) {
    assert_eq!(
        baseline.len(),
        converged.len(),
        "sample size changed across convergence"
    );
    for (before, after) in baseline.iter().zip(converged.iter()) {
        assert_eq!(before.name, after.name, "sample order changed");
        assert_eq!(
            before.point_count, after.point_count,
            "series {} point count changed across convergence",
            before.name
        );
        assert!(
            f64_exact_eq(before.sum, after.sum),
            "series {} sum changed across convergence: before={:?} after={:?}",
            before.name,
            before.sum,
            after.sum,
        );
        assert_eq!(
            before.last_ts, after.last_ts,
            "series {} last timestamp changed across convergence",
            before.name
        );
        assert!(
            value_exact_eq(&before.last_value, &after.last_value),
            "series {} last value changed across convergence: before={:?} after={:?}",
            before.name,
            before.last_value,
            after.last_value,
        );
    }
}

#[test]
#[ignore = "env-gated convergence drill; see module docs (W4-T5)"]
fn l2_convergence_drill() {
    let data_dir_env = std::env::var("TSINK_CONVERGE_DATA_DIR").ok();
    let synthetic_segments_env = std::env::var("TSINK_CONVERGE_SYNTHETIC_SEGMENTS").ok();
    let (data_dir, _temp_guard) = match (data_dir_env, synthetic_segments_env) {
        (Some(dir), None) => (PathBuf::from(dir), None),
        (None, Some(count)) => {
            let l2_segment_count = count
                .parse::<usize>()
                .expect("TSINK_CONVERGE_SYNTHETIC_SEGMENTS must be a usize");
            let temp_dir = TempDir::new().expect("synthetic fixture temp dir");
            build_synthetic_l2_fixture(temp_dir.path(), l2_segment_count);
            (temp_dir.path().to_path_buf(), Some(temp_dir))
        }
        (Some(_), Some(_)) => {
            panic!("set only one of TSINK_CONVERGE_DATA_DIR / TSINK_CONVERGE_SYNTHETIC_SEGMENTS")
        }
        (None, None) => return,
    };

    let csv_path = PathBuf::from(
        std::env::var("TSINK_CONVERGE_CSV").unwrap_or_else(|_| DRILL_CSV_DEFAULT.to_string()),
    );
    let timeout = Duration::from_secs(
        std::env::var("TSINK_CONVERGE_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DRILL_TIMEOUT_DEFAULT_SECS),
    );
    let poll = Duration::from_secs(
        std::env::var("TSINK_CONVERGE_POLL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DRILL_POLL_DEFAULT_SECS),
    );
    let sample_size = std::env::var("TSINK_CONVERGE_SAMPLE_SERIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DRILL_SAMPLE_SERIES_DEFAULT);

    let started = Instant::now();
    let mut peak_rss_kb = read_vm_rss_kb().unwrap_or(0);

    // 1. Baseline sampling on the opened copy.
    let storage = open_drill_storage(&data_dir);
    let baseline = collect_series_aggregates(storage.as_ref(), sample_size);
    assert!(
        !baseline.is_empty(),
        "drill data dir exposes no series to sample"
    );
    let [l0, l1, l2] = count_level_segments(&data_dir);
    println!(
        "l2-convergence-drill: baseline samples={} l0={l0} l1={l1} l2={l2} stop_threshold={DRILL_L2_STOP_COUNT}",
        baseline.len(),
    );

    // 2. Convergence drive: poll the on-disk levels until L2 reaches the
    //    stop threshold, recording the decay curve (written incrementally so
    //    a timeout still leaves the full curve on disk).
    let mut csv = std::fs::File::create(&csv_path).expect("drill csv create");
    std::writeln!(csv, "{CSV_HEADER}").expect("drill csv header");
    let (l0_final, l1_final, l2_final, runs_total) = loop {
        let [l0, l1, l2] = count_level_segments(&data_dir);
        let runs = storage
            .observability_snapshot()
            .compaction
            .l2_to_l2_runs_total;
        let rss = read_vm_rss_kb().unwrap_or(0);
        peak_rss_kb = peak_rss_kb.max(rss);
        let elapsed_s = started.elapsed().as_secs();
        std::writeln!(csv, "{elapsed_s},{l2},{l1},{l0},{rss},{runs}").expect("drill csv row");
        csv.flush().expect("drill csv flush");

        if l2 <= DRILL_L2_STOP_COUNT {
            break (l0, l1, l2, runs);
        }
        if started.elapsed() >= timeout {
            panic!(
                "l2-convergence-drill: TIMEOUT after {:?}; final l0={l0} l1={l1} l2={l2} \
                 runs_total={runs} peak_rss_kb={peak_rss_kb}; curve at {}",
                started.elapsed(),
                csv_path.display(),
            );
        }
        std::thread::sleep(poll);
    };
    let elapsed = started.elapsed();

    // 3. Equivalence check across a close + reopen: the same deterministic
    //    sample must aggregate identically after convergence.
    storage.close().expect("drill close before recheck");
    drop(storage);
    let reopened = open_drill_storage(&data_dir);
    let converged = collect_series_aggregates(reopened.as_ref(), sample_size);
    assert_aggregates_equal(&baseline, &converged);
    reopened.close().expect("drill final close");

    println!(
        "l2-convergence-drill: PASS final_l2={l2_final} final_l1={l1_final} final_l0={l0_final} \
         peak_rss_kb={peak_rss_kb} elapsed={}s runs={runs_total} samples={}",
        elapsed.as_secs(),
        converged.len(),
    );
}
