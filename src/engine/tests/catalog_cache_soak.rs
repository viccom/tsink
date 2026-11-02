//! W2-T3（edge-hardening）：CatalogEntryCache 长跑内存斜率回归（soak）。
//!
//! 本地手动长跑（`#[ignore]`，不入 CI）：以生产对照口径（30 series × 总速率
//! 10 点/s、默认旋钮 32768 entries / 32 MiB series、WAL PerAppend）持续写入，
//! 每 5min 采样一次 RSS 与 `catalog_cache_entries`，判定「常驻内存不随历史
//! 段数增长」：RSS 斜率 ≤ 0.5 MB/h 且缓存驻留不超过容量上界。
//!
//! 跑法（推荐经驱动脚本，release 构建）：
//!
//! ```text
//! scripts/check_catalog_cache_soak.sh                     # 默认 7200s
//! TSINK_SOAK_SECONDS=600 scripts/check_catalog_cache_soak.sh   # 短冒烟
//! ```
//!
//! 环境变量：
//! - `TSINK_SOAK_SECONDS`：写入持续秒数（默认 7200）。
//! - `TSINK_SOAK_CSV`：CSV 输出路径（默认 /tmp/tsink-catalog-cache-soak.csv）。
//! - `TSINK_SOAK_DATA_DIR`：数据目录（默认 /tmp/tsink-catalog-cache-soak-data，
//!   开跑前清残留）。
//!
//! 判定口径：样本数 ≥ 4 时剔除前 25% 作为 warmup，对余下 RSS 样本做最小二乘，
//! 斜率换算 MB/h 后断言 ≤ 0.5；样本数 < 4（短冒烟窗）跳过斜率判定只打警告。
//! `catalog_cache_entries ≤ DEFAULT_CATALOG_ENTRY_CACHE_CAPACITY` 任何时候都判定。

use super::*;
use std::io::Write;
use std::time::Instant;

const SOAK_SERIES: usize = 30;
const SOAK_TOTAL_RATE_PER_SEC: usize = 10;
const SAMPLE_INTERVAL_SECS: u64 = 300;
/// 计划 W2-T3 判据：RSS 斜率上界。
const RSS_SLOPE_LIMIT_MB_PER_HOUR: f64 = 0.5;
/// 30 series 轮转 10 点/s → 每 series 3s 一个点。
const PER_SERIES_STEP_NANOS: i64 = 3_000_000_000;

#[derive(Debug, Clone)]
struct SoakSample {
    elapsed_s: u64,
    rss_kb: u64,
    cache_entries: u64,
    cache_evictions_total: u64,
    /// 累计持久化段数（flush 快照）：单调递增，是驱动 catalog 增长的
    /// 「历史段数」口径；快照字段恒可得，无需 -1 回退。
    segment_count: u64,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn parse_seconds(name: &str, default: u64) -> u64 {
    match env_or(name, "").parse::<u64>() {
        Ok(seconds) => seconds,
        Err(_) => {
            if env_or(name, "").is_empty() {
                default
            } else {
                panic!("soak: {name} must be a u64 seconds value");
            }
        }
    }
}

/// RSS 从 /proc/self/status 的 VmRSS 行读取（kB）。
fn read_rss_kb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status")
        .expect("read /proc/self/status (soak test targets Linux)");
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest
                .split_whitespace()
                .next()
                .expect("VmRSS line carries a kB value");
            return kb.parse().expect("VmRSS kB value parses as u64");
        }
    }
    panic!("VmRSS entry not found in /proc/self/status");
}

fn take_sample(storage: &dyn Storage, elapsed_s: u64) -> SoakSample {
    let snapshot = storage.observability_snapshot();
    SoakSample {
        elapsed_s,
        rss_kb: read_rss_kb(),
        cache_entries: snapshot.flush.catalog_cache_entries,
        cache_evictions_total: snapshot.flush.catalog_cache_evictions_total,
        segment_count: snapshot.flush.persisted_segments_total,
    }
}

fn record_sample(
    storage: &dyn Storage,
    elapsed_s: u64,
    csv: &mut std::fs::File,
    samples: &mut Vec<SoakSample>,
) {
    let sample = take_sample(storage, elapsed_s);
    println!(
        "[soak] elapsed={}s rss_kb={} entries={} evictions={} segments={}",
        sample.elapsed_s,
        sample.rss_kb,
        sample.cache_entries,
        sample.cache_evictions_total,
        sample.segment_count,
    );
    writeln!(
        csv,
        "{},{},{},{},{}",
        sample.elapsed_s,
        sample.rss_kb,
        sample.cache_entries,
        sample.cache_evictions_total,
        sample.segment_count,
    )
    .expect("write soak csv row");
    csv.flush().expect("flush soak csv row");
    samples.push(sample);
}

/// 最小二乘斜率（kB/s）；x = elapsed_s，y = rss_kb。退化（<2 点或零方差）返回 None。
fn least_squares_slope_kb_per_s(samples: &[(f64, f64)]) -> Option<f64> {
    let n = samples.len() as f64;
    if n < 2.0 {
        return None;
    }
    let sum_x: f64 = samples.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = samples.iter().map(|(_, y)| y).sum();
    let sum_xx: f64 = samples.iter().map(|(x, _)| x * x).sum();
    let sum_xy: f64 = samples.iter().map(|(x, y)| x * y).sum();
    let denominator = n * sum_xx - sum_x * sum_x;
    if denominator == 0.0 {
        return None;
    }
    Some((n * sum_xy - sum_x * sum_y) / denominator)
}

#[test]
#[ignore]
fn catalog_cache_soak_long_run() {
    let soak_seconds = parse_seconds("TSINK_SOAK_SECONDS", 7200);
    let csv_path = env_or("TSINK_SOAK_CSV", "/tmp/tsink-catalog-cache-soak.csv");
    let data_dir = env_or("TSINK_SOAK_DATA_DIR", "/tmp/tsink-catalog-cache-soak-data");

    // 开跑前清残留：数据目录进程锁与上一次的半程数据都不得影响本次判定。
    if Path::new(&data_dir).exists() {
        std::fs::remove_dir_all(&data_dir).expect("remove stale soak data dir");
    }
    std::fs::create_dir_all(&data_dir).expect("create soak data dir");

    println!(
        "soak config: seconds={soak_seconds} csv={csv_path} data_dir={data_dir} \
         series={SOAK_SERIES} total_rate={SOAK_TOTAL_RATE_PER_SEC}/s \
         sample_interval={SAMPLE_INTERVAL_SECS}s \
         knobs=default(catalog entries={}/series-bytes={} MiB) wal=default(PerAppend)",
        crate::storage::DEFAULT_CATALOG_ENTRY_CACHE_CAPACITY,
        crate::storage::DEFAULT_CATALOG_SERIES_CACHE_BYTES / (1024 * 1024),
    );

    // 默认旋钮面 = 生产默认（entries 32768 / series 32MiB / WAL 开启 PerAppend）。
    let storage = StorageBuilder::new()
        .with_data_path(&data_dir)
        .build()
        .expect("build soak storage");

    let mut csv = std::fs::File::create(&csv_path).expect("create soak csv file");
    writeln!(
        csv,
        "elapsed_s,rss_kb,catalog_cache_entries,catalog_cache_evictions_total,segment_count"
    )
    .expect("write soak csv header");

    let base_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after UNIX epoch")
        .as_nanos() as i64;
    // 每 series 时间戳错开 1ms，同 series 点距 3s。
    let mut next_ts: Vec<i64> = (0..SOAK_SERIES)
        .map(|i| base_ts + i as i64 * 1_000_000)
        .collect();

    let start = Instant::now();
    let soak_duration = Duration::from_secs(soak_seconds);
    let mut samples: Vec<SoakSample> = Vec::new();

    record_sample(storage.as_ref(), 0, &mut csv, &mut samples);
    let mut next_sample_at = SAMPLE_INTERVAL_SECS;

    let mut tick: u64 = 0;
    loop {
        let elapsed = start.elapsed().as_secs();
        if elapsed >= next_sample_at {
            record_sample(storage.as_ref(), elapsed, &mut csv, &mut samples);
            next_sample_at += SAMPLE_INTERVAL_SECS;
        }
        if start.elapsed() >= soak_duration {
            break;
        }
        // 一个 tick = 1s 墙钟，一批 10 行轮转 30 series。
        let mut rows = Vec::with_capacity(SOAK_TOTAL_RATE_PER_SEC);
        for j in 0..SOAK_TOTAL_RATE_PER_SEC {
            let series_idx =
                ((tick * SOAK_TOTAL_RATE_PER_SEC as u64 + j as u64) % SOAK_SERIES as u64) as usize;
            let ts = next_ts[series_idx];
            next_ts[series_idx] = ts + PER_SERIES_STEP_NANOS;
            rows.push(Row::with_labels(
                "soak_metric",
                vec![Label::new("series", series_idx.to_string())],
                DataPoint::new(ts, (series_idx % 100) as f64),
            ));
        }
        storage.insert_rows(&rows).expect("soak insert_rows");
        tick += 1;
        std::thread::sleep(Duration::from_secs(1));
    }

    storage.close().expect("close soak storage");

    let capacity = crate::storage::DEFAULT_CATALOG_ENTRY_CACHE_CAPACITY as u64;
    let max_entries = samples
        .iter()
        .map(|s| s.cache_entries)
        .max()
        .unwrap_or_default();
    let max_evictions = samples
        .iter()
        .map(|s| s.cache_evictions_total)
        .max()
        .unwrap_or_default();
    let duration_s = samples.last().map(|s| s.elapsed_s).unwrap_or_default();

    let mut failures: Vec<String> = Vec::new();

    // 缓存驻留上界判定（任何窗口都执行——这正是 W2-T2 的行为回归点）。
    if max_entries > capacity {
        failures.push(format!(
            "catalog_cache_entries {max_entries} exceeded capacity bound {capacity}"
        ));
    }

    // RSS 斜率判定：样本 < 4 跳过（只警告）；否则剔除前 25% warmup 后拟合。
    let slope_mb_per_hour = if samples.len() < 4 {
        eprintln!(
            "soak: {} samples (< 4); RSS slope judgement SKIPPED — window too short for a meaningful fit",
            samples.len()
        );
        None
    } else {
        let warmup = samples.len() / 4;
        let fit: Vec<(f64, f64)> = samples[warmup..]
            .iter()
            .map(|s| (s.elapsed_s as f64, s.rss_kb as f64))
            .collect();
        least_squares_slope_kb_per_s(&fit).map(|kb_per_s| kb_per_s * 3600.0 / 1024.0)
    };

    if let Some(slope) = slope_mb_per_hour {
        if slope > RSS_SLOPE_LIMIT_MB_PER_HOUR {
            failures.push(format!(
                "rss slope {slope:.4} MB/h exceeded limit {RSS_SLOPE_LIMIT_MB_PER_HOUR} MB/h"
            ));
        }
    }

    let slope_display = slope_mb_per_hour
        .map(|slope| format!("{slope:.4}MB/h"))
        .unwrap_or_else(|| "skipped(samples<4)".to_string());
    let verdict = if failures.is_empty() { "PASS" } else { "FAIL" };
    println!(
        "catalog-cache-soak: {verdict} slope={slope_display} entries={max_entries}/{capacity} duration={duration_s}s evictions={max_evictions} samples={}",
        samples.len(),
    );

    if !failures.is_empty() {
        panic!(
            "catalog-cache-soak failed: {}\nfull samples: {samples:#?}",
            failures.join("; "),
        );
    }
}
