//! W1-T1 crash-consistency characterization harness (edge-hardening plan).
//!
//! Protocol: a child test process (`crash_child_mode`, activated by
//! `TSINK_CRASH_CHILD=1`) writes batches round-robin across `dev-0/1/2` with
//! `insert_rows_with_result`, asserting every batch is acknowledged
//! `WriteAcknowledgement::Durable` (default `WalSyncMode::PerAppend`). Before
//! each batch it appends `intent <dev> <ts>` lines to a journal file living in
//! a sibling directory of the data directory (kept outside the engine's own
//! IO/cleanup scope), fsyncs, writes the batch, then appends `ack <dev> <ts>`
//! lines and fsyncs again. The parent kills the child with SIGKILL after a
//! seeded 50-500ms delay, reopens the same data directory with the library,
//! and asserts the crash-consistency gates:
//!
//! 1. every `ack`ed point is readable with the expected value (`ts - base`);
//! 2. the full dataset contains no duplicate `(dev, ts)`;
//! 3. the explicit persistence drain succeeds after reopen, and a second
//!    reopen succeeds too;
//! 4. `intent`-without-`ack` points may exist or be missing (no assertion).
//!
//! Gate 3 note: the plan calls for `flush()`, but `flush` is an inherent
//! method of the internal `ChunkStorage` (`src/engine/core_impl.rs`) and is
//! NOT exposed on the public `Storage` trait, so a test-only file cannot call
//! it on the reopened `Arc<dyn Storage>`. `Storage::close()` runs the full
//! persistence drain (pipeline flush + seal + catalog checkpoint) and is the
//! closest faithful public-API equivalent; the gate therefore asserts
//! `close()` success plus a second successful reopen.
//!
//! Characterization test: the current implementation is expected to pass. Any
//! red assertion is a suspected defect — do not weaken assertions and do not
//! fix the implementation here; register the failure output instead.

use std::collections::HashSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tsink::{DataPoint, Row, Storage, StorageBuilder, WalSyncMode};

/// Dataset base timestamp: `ts = TS_BASE + seq`, value = seq.
const TS_BASE: i64 = 1_700_000_000;
/// Rows per batch, all for one device (round-robin across devices).
const BATCH_SIZE: usize = 8;
/// Runaway guard only; the child is normally ended by the parent's SIGKILL.
const MAX_BATCHES: u64 = 1_000_000;
/// Fixed seeds so the whole run is reproducible.
const SEEDS: [u64; 5] = [
    0xA5A5_0001,
    0xA5A5_0002,
    0xA5A5_0003,
    0xA5A5_0004,
    0xA5A5_0005,
];

const ENV_CHILD: &str = "TSINK_CRASH_CHILD";
const ENV_DATA_DIR: &str = "TSINK_CRASH_DATA_DIR";
const ENV_JOURNAL: &str = "TSINK_CRASH_JOURNAL_PATH";

/// SplitMix64: dependency-free deterministic PRNG.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

fn kill_delay(seed: u64) -> Duration {
    let mut rng = SplitMix64(seed);
    Duration::from_millis(50 + rng.next() % 451)
}

fn round_base_dir(seed: u64, round: usize) -> PathBuf {
    env::temp_dir().join(format!("tsink-crash-w1t1-{seed:08x}-{round}"))
}

fn open_storage(data_dir: &Path) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(data_dir)
        .with_wal_enabled(true)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()
        .expect("open storage on crash directory")
}

/// Child entry point. Inert unless the parent injected `TSINK_CRASH_CHILD=1`.
#[test]
#[ignore]
fn crash_child_mode() {
    if env::var(ENV_CHILD).as_deref() != Ok("1") {
        // Prevent accidental standalone runs from writing anything.
        return;
    }
    let data_dir =
        PathBuf::from(env::var(ENV_DATA_DIR).expect("child requires TSINK_CRASH_DATA_DIR"));
    let journal_path =
        PathBuf::from(env::var(ENV_JOURNAL).expect("child requires TSINK_CRASH_JOURNAL_PATH"));
    fs::create_dir_all(&data_dir).expect("create child data dir");
    if let Some(parent) = journal_path.parent() {
        fs::create_dir_all(parent).expect("create child journal dir");
    }

    let storage = open_storage(&data_dir);
    // Scoped import: `io::Write::flush` would otherwise collide with
    // `Storage::flush` at the parent's gate-3 call site.
    use std::io::Write;
    let mut journal = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)
        .expect("open crash journal");

    let mut seq: i64 = 0;
    for batch_idx in 0..MAX_BATCHES {
        let dev = format!("dev-{}", batch_idx % 3);

        let mut intent_buf = String::with_capacity(BATCH_SIZE * 32);
        let mut rows = Vec::with_capacity(BATCH_SIZE);
        for i in 0..BATCH_SIZE {
            let ts = TS_BASE + seq + i as i64;
            intent_buf.push_str(&format!("intent {dev} {ts}\n"));
            rows.push(Row::new(
                dev.clone(),
                DataPoint::new(ts, (seq + i as i64) as f64),
            ));
        }
        journal
            .write_all(intent_buf.as_bytes())
            .expect("journal intent append");
        journal.sync_all().expect("journal intent fsync");

        let result = storage
            .insert_rows_with_result(&rows)
            .unwrap_or_else(|err| panic!("batch {batch_idx} write failed: {err}"));
        assert!(
            result.is_durable(),
            "batch {batch_idx} not durable under WalSyncMode::PerAppend, got {:?}",
            result.acknowledgement
        );

        let mut ack_buf = String::with_capacity(BATCH_SIZE * 32);
        for _ in 0..BATCH_SIZE {
            let ts = TS_BASE + seq;
            ack_buf.push_str(&format!("ack {dev} {ts}\n"));
            seq += 1;
        }
        journal
            .write_all(ack_buf.as_bytes())
            .expect("journal ack append");
        journal.sync_all().expect("journal ack fsync");
    }
    // Reached only if the parent failed to kill us in time (runaway guard).
}

#[test]
fn crash_kill_reopens_with_acked_rows_intact() {
    for (round, &seed) in SEEDS.iter().enumerate() {
        run_crash_round(round, seed);
    }
}

fn run_crash_round(round: usize, seed: u64) {
    let base = round_base_dir(seed, round);
    let _ = fs::remove_dir_all(&base); // clear residue from a previous run
    let data_dir = base.join("data");
    let journal_path = base.join("journal").join("journal.log");
    fs::create_dir_all(base.join("journal")).expect("create round base dirs");
    let child_log_path = base.join("child.log");

    // Spawn the ignored child test from this same test binary. Child
    // stdout/stderr are redirected to <base>/child.log for failure diagnosis.
    let child_log = File::create(&child_log_path).expect("create child log");
    let child_log_err = child_log.try_clone().expect("clone child log handle");
    let exe = env::current_exe().expect("current_exe resolves test binary");
    let mut child = Command::new(&exe)
        .args(["--ignored", "--exact", "crash_child_mode", "--nocapture"])
        .env(ENV_CHILD, "1")
        .env(ENV_DATA_DIR, &data_dir)
        .env(ENV_JOURNAL, &journal_path)
        .stdout(Stdio::from(child_log))
        .stderr(Stdio::from(child_log_err))
        .spawn()
        .expect("spawn crash child process");

    let delay = kill_delay(seed);
    thread::sleep(delay);
    child.kill().expect("SIGKILL crash child");
    let status = child.wait().expect("reap crash child");

    let ctx = format!(
        "round {round} seed {seed:#x} delay {:?} dir {} child_log {}",
        delay,
        base.display(),
        child_log_path.display()
    );
    assert!(
        status.code().is_none(),
        "crash child exited on its own ({status:?}); protocol violation or child panic — {ctx}"
    );

    let (intents, acks) = parse_journal(&journal_path);
    assert!(
        !acks.is_empty(),
        "no acknowledged writes after {delay:?}: child was killed before the first batch completed, harness would be vacuous — {ctx}"
    );
    assert!(
        acks.len() >= intents.len() / 2,
        "journal shape suspicious: {} intents but only {} acks; child likely panicked early — {ctx}",
        intents.len(),
        acks.len()
    );

    // Gate 1: every acked point is readable with the exact expected value.
    let storage = open_storage(&data_dir);
    for (dev, ts) in &acks {
        let points = storage
            .select(dev, &[], *ts, *ts + 1)
            .unwrap_or_else(|err| panic!("select acked {dev}@{ts} failed: {err} — {ctx}"));
        assert_eq!(
            points.len(),
            1,
            "acked point {dev}@{ts} must be readable exactly once, got {} points — {ctx}",
            points.len()
        );
        assert_eq!(
            points[0].value_as_f64(),
            Some((ts - TS_BASE) as f64),
            "acked point {dev}@{ts} has wrong value — {ctx}"
        );
    }

    // Gate 2: the full dataset has no duplicate (dev, ts).
    let mut seen: HashSet<(String, i64)> = HashSet::new();
    for dev_idx in 0..3 {
        let dev = format!("dev-{dev_idx}");
        let points = storage
            .select(&dev, &[], 0, i64::MAX)
            .unwrap_or_else(|err| panic!("full select {dev} failed: {err} — {ctx}"));
        for point in &points {
            assert!(
                seen.insert((dev.clone(), point.timestamp)),
                "duplicate (dev, ts) in storage: {dev}@{} — {ctx}",
                point.timestamp
            );
            assert_eq!(
                point.value_as_f64(),
                Some((point.timestamp - TS_BASE) as f64),
                "stored point {dev}@{} has wrong value — {ctx}",
                point.timestamp
            );
        }
    }
    let acked_set: HashSet<(String, i64)> = acks.iter().cloned().collect();
    assert!(
        acked_set.is_subset(&seen),
        "storage is missing acked points after full scan — {ctx}"
    );

    // Gate 3: the explicit persistence drain succeeds after reopen, and a
    // second reopen succeeds too. (`flush()` is not on the public Storage
    // trait — see module docs — so close() stands in as the drain step.)
    storage
        .close()
        .unwrap_or_else(|err| panic!("close after reopen failed: {err} — {ctx}"));
    drop(storage);
    let reopened = open_storage(&data_dir);
    let (first_dev, first_ts) = &acks[0];
    let spot = reopened
        .select(first_dev, &[], *first_ts, *first_ts + 1)
        .unwrap_or_else(|err| panic!("select after second reopen failed: {err} — {ctx}"));
    assert_eq!(
        spot.len(),
        1,
        "acked point {first_dev}@{first_ts} lost after close + second reopen — {ctx}"
    );
    assert_eq!(
        spot[0].value_as_f64(),
        Some((first_ts - TS_BASE) as f64),
        "value drifted after close + second reopen — {ctx}"
    );
    reopened
        .close()
        .unwrap_or_else(|err| panic!("second close failed: {err} — {ctx}"));
    let reopened_again = open_storage(&data_dir);
    let spot = reopened_again
        .select(first_dev, &[], *first_ts, *first_ts + 1)
        .unwrap_or_else(|err| panic!("select after third reopen failed: {err} — {ctx}"));
    assert_eq!(
        spot.len(),
        1,
        "acked point {first_dev}@{first_ts} lost after third reopen — {ctx}"
    );
    reopened_again
        .close()
        .unwrap_or_else(|err| panic!("third close failed: {err} — {ctx}"));

    // Round passed: clean up so repeated runs keep /tmp tidy. Failed rounds
    // keep their directory for diagnosis.
    let _ = fs::remove_dir_all(&base);
}

/// One journal line: `intent <dev> <ts>` or `ack <dev> <ts>`.
type JournalEntries = Vec<(String, i64)>;

fn parse_journal(path: &Path) -> (JournalEntries, JournalEntries) {
    let mut content = String::new();
    File::open(path)
        .unwrap_or_else(|err| panic!("open journal {}: {err}", path.display()))
        .read_to_string(&mut content)
        .unwrap_or_else(|err| panic!("read journal {}: {err}", path.display()));

    let mut intents = Vec::new();
    let mut acks = Vec::new();
    let lines: Vec<&str> = content.lines().collect();
    for (idx, line) in lines.iter().enumerate() {
        if line.is_empty() {
            continue;
        }
        let mut tokens = line.split_whitespace();
        let parsed = match (tokens.next(), tokens.next(), tokens.next(), tokens.next()) {
            (Some(kind), Some(dev), Some(ts), None) => ts
                .parse::<i64>()
                .ok()
                .map(|ts| (kind.to_string(), dev.to_string(), ts)),
            _ => None,
        };
        match parsed {
            Some((kind, dev, ts)) if kind == "intent" => intents.push((dev, ts)),
            Some((kind, dev, ts)) if kind == "ack" => acks.push((dev, ts)),
            _ => {
                // Only the final line may be torn: SIGKILL can race the last
                // append. A corrupt line anywhere else is a protocol failure.
                assert_eq!(
                    idx,
                    lines.len() - 1,
                    "corrupt non-tail journal line {idx}: {line:?} in {}",
                    path.display()
                );
            }
        }
    }
    (intents, acks)
}
