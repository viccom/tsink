//! W1-T2 crash-consistency characterization harness (edge-hardening plan).
//!
//! Four kill points, selected per child process via `TSINK_CRASH_MODE`
//! (missing/invalid values panic loudly in the child — a protocol violation
//! must be loud):
//!
//! - `append`: pure write storm; the SIGKILL lands mid-WAL-append.
//! - `flush`: continuous writes under a tiny `flush_interval` plus periodic
//!   `close()`/reopen epochs; the kill can land mid segment write/rename.
//!   (The plan's `flush()` loop is realized as "tiny flush interval +
//!   continuous writes + periodic close()/reopen": `flush` is an inherent
//!   method of the internal `ChunkStorage` and is NOT on the public
//!   `Storage` trait, so a test-only file cannot call it — same substitution
//!   as the gate-3 drain below.)
//! - `compact`: preseed >= 6 tiny L0 segments (one `close()` per segment),
//!   then churn under a tiny `compaction_interval` with continuous small
//!   writes; the kill can land mid marker application / source deletion.
//! - `recover`: two child processes. Step (a) `TSINK_CRASH_PHASE=preseed`:
//!   plain write storm until SIGKILL, deliberately leaving an unflushed WAL
//!   tail. Step (b) `TSINK_CRASH_PHASE=main`: the child's very `open()` (WAL
//!   replay) is the work under test; the parent kills it after a seeded
//!   20-80 ms delay. Whether the kill lands mid-replay or after open
//!   completed depends on timing — both are acceptable per the plan; each
//!   round reports which one happened (via the child's `open-complete`
//!   stdout marker).
//!
//! Protocol: the child (`crash_child_mode`, activated by
//! `TSINK_CRASH_CHILD=1`) writes batches round-robin across `dev-0/1/2` with
//! `insert_rows_with_result`, asserting every batch is acknowledged
//! `WriteAcknowledgement::Durable` (default `WalSyncMode::PerAppend`). Before
//! each batch it appends `intent <dev> <ts>` lines to a journal file living
//! in a sibling directory of the data directory (kept outside the engine's
//! own IO/cleanup scope), fsyncs, writes the batch, then appends
//! `ack <dev> <ts>` lines and fsyncs again. The parent kills the child with
//! SIGKILL after a seeded delay, reopens the same data directory with the
//! library, and asserts the crash-consistency gates:
//!
//! 1. every `ack`ed point is readable with the expected value (`ts - base`);
//! 2. the full dataset contains no duplicate `(dev, ts)`;
//! 3. the explicit persistence drain succeeds after reopen, and a second
//!    reopen succeeds too;
//! 4. `intent`-without-`ack` points may exist or be missing (no assertion).
//!
//! `compact` rounds additionally scan the on-disk layout (mirroring the
//! loader's `<lane>/segments/L<n>/` one-level scan): every non-hidden
//! directory entry must contain a `manifest.bin`. A directory without one is the
//! "segment dir exists but manifest missing" defect candidate — red means
//! register, do not fix.
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
/// Base of the deterministic seed sequence: round `i` of a mode runs with
/// seed `SEED_BASE + i`, so SplitMix64 keeps the kill delays distinct across
/// rounds while the whole sweep stays reproducible.
const SEED_BASE: u64 = 0xA5A5_0001;
/// Rounds per kill mode when `ITER` is unset: keeps a plain `cargo test`
/// fast while still sweeping every kill point.
const DEFAULT_ITER: u64 = 5;
/// Parent-only knob sizing the seed sweep (rounds per kill mode).
const ENV_ITER: &str = "ITER";
/// All kill modes, one full seed sweep each.
const MODES: [&str; 4] = ["append", "flush", "compact", "recover"];
/// Tiny L0 segments preseeded before the compact churn loop (plan: >= 6).
const COMPACT_PRESEED_SEGMENTS: usize = 8;
/// Batches per preseeded segment: 3 batches = one per device.
const COMPACT_PRESEED_BATCHES: u64 = 3;
/// Flush/compaction churn intervals: small enough that several background
/// ticks fit inside the 50-500 ms kill window.
const CHURN_TICK: Duration = Duration::from_millis(50);
/// The recover open-phase child parks here after `open()` if the parent's
/// kill has not arrived yet (runaway guard only).
const RECOVER_OPEN_PARK: Duration = Duration::from_secs(60);

const ENV_CHILD: &str = "TSINK_CRASH_CHILD";
const ENV_MODE: &str = "TSINK_CRASH_MODE";
const ENV_PHASE: &str = "TSINK_CRASH_PHASE";
const ENV_DATA_DIR: &str = "TSINK_CRASH_DATA_DIR";
const ENV_JOURNAL: &str = "TSINK_CRASH_JOURNAL_PATH";

/// Which kill point a child process exercises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrashMode {
    Append,
    Flush,
    Compact,
    Recover,
}

/// `recover` runs two child processes: `Preseed` (write storm until killed,
/// leaving a WAL tail) and `Main` (open = WAL replay, killed mid-flight).
/// Every other mode runs a single `Main` child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrashPhase {
    Preseed,
    Main,
}

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

/// Seeded 50-500 ms delay before the parent SIGKILLs a writing child.
fn kill_delay(seed: u64) -> Duration {
    let mut rng = SplitMix64(seed);
    Duration::from_millis(50 + rng.next() % 451)
}

/// Seeded 20-80 ms delay before the parent SIGKILLs the recover open child:
/// short enough to often land inside binary startup / WAL replay.
fn open_kill_delay(seed: u64) -> Duration {
    let mut rng = SplitMix64(seed ^ 0x0BEF_0000_0000_0001);
    Duration::from_millis(20 + rng.next() % 61)
}

fn round_base_dir(mode: &str, seed: u64, round: usize) -> PathBuf {
    env::temp_dir().join(format!("tsink-crash-w1t2-{mode}-{seed:08x}-{round}"))
}

fn try_open_storage(data_dir: &Path) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
    Ok(StorageBuilder::new()
        .with_data_path(data_dir)
        .with_wal_enabled(true)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()?)
}

fn open_storage(data_dir: &Path) -> Arc<dyn Storage> {
    try_open_storage(data_dir).expect("open storage on crash directory")
}

fn open_flush_storage(data_dir: &Path) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(data_dir)
        .with_wal_enabled(true)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .with_flush_interval(CHURN_TICK)
        .build()
        .expect("open flush-churn storage")
}

fn open_compact_storage(data_dir: &Path) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(data_dir)
        .with_wal_enabled(true)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .with_flush_interval(CHURN_TICK)
        .with_compaction_interval(CHURN_TICK)
        .build()
        .expect("open compact-churn storage")
}

fn open_journal(journal_path: &Path) -> File {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal_path)
        .expect("open crash journal")
}

/// Rounds per kill mode: the `ITER` environment variable, `DEFAULT_ITER`
/// when unset. A value that fails to parse or is `0` panics loudly — a
/// misconfigured crash gate must fail, not silently shrink the sweep.
fn iter_count() -> u64 {
    match env::var(ENV_ITER) {
        Err(env::VarError::NotPresent) => DEFAULT_ITER,
        Ok(raw) => {
            let rounds = raw.parse::<u64>().unwrap_or_else(|err| {
                panic!("{ENV_ITER}={raw:?} is not a valid round count: {err}")
            });
            assert!(
                rounds > 0,
                "{ENV_ITER}={raw:?} must be a positive round count (>= 1)"
            );
            rounds
        }
        Err(err) => panic!("reading {ENV_ITER}: {err}"),
    }
}

fn parse_child_mode() -> CrashMode {
    match env::var(ENV_MODE) {
        Ok(value) => match value.as_str() {
            "append" => CrashMode::Append,
            "flush" => CrashMode::Flush,
            "compact" => CrashMode::Compact,
            "recover" => CrashMode::Recover,
            other => panic!("invalid {ENV_MODE}={other:?}: expected append|flush|compact|recover"),
        },
        Err(err) => panic!("child requires {ENV_MODE} (append|flush|compact|recover): {err}"),
    }
}

fn parse_child_phase() -> CrashPhase {
    match env::var(ENV_PHASE) {
        Ok(value) => match value.as_str() {
            "preseed" => CrashPhase::Preseed,
            "main" => CrashPhase::Main,
            other => panic!("invalid {ENV_PHASE}={other:?}: expected preseed|main"),
        },
        Err(err) => panic!("child requires {ENV_PHASE} (preseed|main): {err}"),
    }
}

/// Child entry point. Inert unless the parent injected `TSINK_CRASH_CHILD=1`.
#[test]
#[ignore]
fn crash_child_mode() {
    if env::var(ENV_CHILD).as_deref() != Ok("1") {
        // Prevent accidental standalone runs from writing anything.
        return;
    }
    let mode = parse_child_mode();
    let phase = parse_child_phase();
    let data_dir =
        PathBuf::from(env::var(ENV_DATA_DIR).expect("child requires TSINK_CRASH_DATA_DIR"));
    let journal_path =
        PathBuf::from(env::var(ENV_JOURNAL).expect("child requires TSINK_CRASH_JOURNAL_PATH"));
    fs::create_dir_all(&data_dir).expect("create child data dir");
    if let Some(parent) = journal_path.parent() {
        fs::create_dir_all(parent).expect("create child journal dir");
    }

    if phase == CrashPhase::Preseed {
        // recover step (a): append-style write storm until SIGKILL; the
        // unflushed WAL tail is exactly what the recover open will replay.
        assert_eq!(
            mode,
            CrashMode::Recover,
            "preseed phase is only valid for {ENV_MODE}=recover"
        );
        let storage = open_storage(&data_dir);
        let mut journal = open_journal(&journal_path);
        let mut seq = 0;
        write_batches(&storage, &mut journal, &mut seq, MAX_BATCHES);
        return; // reached only if the parent failed to kill us (runaway)
    }

    match mode {
        CrashMode::Append => {
            let storage = open_storage(&data_dir);
            let mut journal = open_journal(&journal_path);
            let mut seq = 0;
            write_batches(&storage, &mut journal, &mut seq, MAX_BATCHES);
        }
        CrashMode::Flush => {
            let mut journal = open_journal(&journal_path);
            let mut seq = 0;
            flush_churn_child(&data_dir, &mut journal, &mut seq);
        }
        CrashMode::Compact => {
            let mut journal = open_journal(&journal_path);
            let mut seq = 0;
            compact_child(&data_dir, &mut journal, &mut seq);
        }
        CrashMode::Recover => recover_open_child(&data_dir),
    }
}

/// `append` kill point: pure write storm, SIGKILL lands mid-WAL-append.
/// (Also the body of the recover preseed phase.)
///
/// Scoped import: `io::Write::flush` would otherwise collide with
/// `Storage::flush` in callers that touch the trait.
fn write_batches(storage: &Arc<dyn Storage>, journal: &mut File, seq: &mut i64, max_batches: u64) {
    use std::io::Write;

    for batch_idx in 0..max_batches {
        let dev = format!("dev-{}", batch_idx % 3);

        let mut intent_buf = String::with_capacity(BATCH_SIZE * 32);
        let mut rows = Vec::with_capacity(BATCH_SIZE);
        for i in 0..BATCH_SIZE {
            let ts = TS_BASE + *seq + i as i64;
            intent_buf.push_str(&format!("intent {dev} {ts}\n"));
            rows.push(Row::new(
                dev.clone(),
                DataPoint::new(ts, (*seq + i as i64) as f64),
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
            let ts = TS_BASE + *seq;
            ack_buf.push_str(&format!("ack {dev} {ts}\n"));
            *seq += 1;
        }
        journal
            .write_all(ack_buf.as_bytes())
            .expect("journal ack append");
        journal.sync_all().expect("journal ack fsync");
    }
}

/// `flush` kill point: segments are written and renamed by the background
/// flusher and by `close()` drains. Loop small epochs of
/// `open(tiny flush interval) -> write -> close()`; the SIGKILL can land
/// mid-write, mid-background-flush, or mid-close (segment write/rename).
/// The journal and the global `seq` carry across epochs.
fn flush_churn_child(data_dir: &Path, journal: &mut File, seq: &mut i64) {
    loop {
        let storage = open_flush_storage(data_dir);
        let batches = 5 + (*seq as u64 % 21); // 5..=25 batches per epoch
        write_batches(&storage, journal, seq, batches);
        storage.close().expect("flush-mode epoch close failed");
        drop(storage);
    }
}

/// `compact` kill point: preseed >= 6 tiny L0 segments (each `close()` seals
/// exactly one — deterministic, no reliance on background ticks), then churn
/// under a tiny compaction interval with continuous small writes so the
/// background compactor keeps merging windows; the SIGKILL can land mid
/// marker application or mid source-segment deletion.
fn compact_child(data_dir: &Path, journal: &mut File, seq: &mut i64) {
    for _ in 0..COMPACT_PRESEED_SEGMENTS {
        let storage = open_storage(data_dir);
        write_batches(&storage, journal, seq, COMPACT_PRESEED_BATCHES);
        storage.close().expect("compact preseed close failed");
        drop(storage);
    }
    let storage = open_compact_storage(data_dir);
    write_batches(&storage, journal, seq, MAX_BATCHES);
}

/// `recover` kill point step (b): the `open()` itself (WAL replay of the
/// tail the preseed child left behind) is the work under test. The
/// `open-complete` marker on stdout lets the parent report whether the kill
/// landed before or after replay finished.
fn recover_open_child(data_dir: &Path) {
    // Underscore-prefixed binding: keep the storage (and its background
    // threads) alive while parked, without touching it further.
    let _storage = open_storage(data_dir);
    println!("crash-child: open-complete");
    thread::sleep(RECOVER_OPEN_PARK);
}

#[test]
fn crash_kill_reopens_with_acked_rows_intact() {
    let rounds = iter_count();
    for mode in MODES {
        for round in 0..rounds {
            let seed = SEED_BASE + round;
            run_crash_round(mode, round as usize, seed);
        }
    }
}

fn run_crash_round(mode: &str, round: usize, seed: u64) {
    let base = round_base_dir(mode, seed, round);
    let _ = fs::remove_dir_all(&base); // clear residue from a previous run
    let data_dir = base.join("data");
    let journal_path = base.join("journal").join("journal.log");
    fs::create_dir_all(base.join("journal")).expect("create round base dirs");
    let exe = env::current_exe().expect("current_exe resolves test binary");

    let delay = kill_delay(seed);
    let mut ctx = format!(
        "mode {mode} round {round} seed {seed:#x} dir {} journal {}",
        base.display(),
        journal_path.display()
    );

    let open_log_path = base.join("child-open.log");
    match mode {
        "recover" => {
            let preseed_log = base.join("child-preseed.log");
            spawn_kill_child(
                &exe,
                mode,
                "preseed",
                &data_dir,
                &journal_path,
                &preseed_log,
                delay,
            );
            let open_delay = open_kill_delay(seed);
            spawn_kill_child(
                &exe,
                mode,
                "main",
                &data_dir,
                &journal_path,
                &open_log_path,
                open_delay,
            );
            ctx.push_str(&format!(
                " preseed_delay {delay:?} open_delay {open_delay:?}"
            ));
        }
        _ => {
            let child_log_path = base.join("child.log");
            spawn_kill_child(
                &exe,
                mode,
                "main",
                &data_dir,
                &journal_path,
                &child_log_path,
                delay,
            );
            ctx.push_str(&format!(" delay {delay:?}"));
        }
    }

    let (intents, acks) = parse_journal(&journal_path);
    assert!(
        !acks.is_empty(),
        "no acknowledged writes: child was killed before the first batch completed, harness would be vacuous — {ctx}"
    );
    assert!(
        acks.len() >= intents.len() / 2,
        "journal shape suspicious: {} intents but only {} acks; child likely panicked early — {ctx}",
        intents.len(),
        acks.len()
    );

    let storage = try_open_storage(&data_dir)
        .unwrap_or_else(|err| panic!("reopen after crash failed: {err} — {ctx}"));

    let compact_diag = if mode == "compact" {
        // Compact-mode extra gate: no "segment dir exists but manifest
        // missing" references on disk, mirroring the loader's scan.
        assert_segment_dirs_have_manifests(&data_dir, &ctx);
        Some(segment_diagnostics(&data_dir))
    } else {
        None
    };

    // Gate 1: every acked point is readable with the exact expected value.
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
    let reopened = try_open_storage(&data_dir)
        .unwrap_or_else(|err| panic!("second reopen failed: {err} — {ctx}"));
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
    let reopened_again = try_open_storage(&data_dir)
        .unwrap_or_else(|err| panic!("third reopen failed: {err} — {ctx}"));
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

    // Diagnostics (captured by libtest; visible with --nocapture and on
    // failure). Recover: did the kill land inside startup/replay or after
    // open completed? Compact: how far did segment layering get?
    if mode == "recover" {
        let marker = fs::read_to_string(&open_log_path)
            .map(|log| log.contains("crash-child: open-complete"))
            .unwrap_or(false);
        let coverage = if marker {
            "kill landed AFTER open completed (post-open kill; replay-mid-kill not covered this round)"
        } else {
            "kill landed BEFORE open completed (binary startup / WAL replay window)"
        };
        println!("crash-consistency [{ctx}]: recover open-phase coverage: {coverage}");
    }
    if let Some(diag) = compact_diag {
        println!("crash-consistency [{ctx}]: segment layout {diag}");
    }

    // Round passed: clean up so repeated runs keep /tmp tidy. Failed rounds
    // keep their directory for diagnosis.
    let _ = fs::remove_dir_all(&base);
}

/// Spawn one ignored child test of this same binary in the given mode/phase,
/// SIGKILL it after `delay`, reap it, and assert it died from the signal.
fn spawn_kill_child(
    exe: &Path,
    mode: &str,
    phase: &str,
    data_dir: &Path,
    journal_path: &Path,
    log_path: &Path,
    delay: Duration,
) {
    // Child stdout/stderr are redirected to <base>/child*.log for failure
    // diagnosis (and for the recover open-coverage marker).
    let log = File::create(log_path)
        .unwrap_or_else(|err| panic!("create child log {}: {err}", log_path.display()));
    let log_err = log.try_clone().expect("clone child log handle");
    let mut child = Command::new(exe)
        .args(["--ignored", "--exact", "crash_child_mode", "--nocapture"])
        .env(ENV_CHILD, "1")
        .env(ENV_MODE, mode)
        .env(ENV_PHASE, phase)
        .env(ENV_DATA_DIR, data_dir)
        .env(ENV_JOURNAL, journal_path)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err))
        .spawn()
        .expect("spawn crash child process");

    thread::sleep(delay);
    child.kill().expect("SIGKILL crash child");
    let status = child.wait().expect("reap crash child");
    assert!(
        status.code().is_none(),
        "crash child (mode {mode} phase {phase}) exited on its own ({status:?}); protocol violation or child panic, see {}",
        log_path.display()
    );
}

/// Mirrors the loader's on-disk scan (`collect_segment_dirs`,
/// `src/engine/segment/loader.rs`: `<lane>/segments/L<n>/`, one level deep,
/// hidden entries ignored, missing directories treated as empty): every
/// non-hidden directory entry under every lane's `segments/L*/` must contain
/// `manifest.bin`, the one file every loadable segment carries. A directory
/// without it is the plan's "segment dir exists but manifest missing" defect
/// candidate — red means register, do not fix.
fn assert_segment_dirs_have_manifests(data_dir: &Path, ctx: &str) {
    for segments_root in lane_segments_roots(data_dir) {
        let levels = match fs::read_dir(&segments_root) {
            Ok(levels) => levels,
            Err(err) => panic!("scan {}: {err} — {ctx}", segments_root.display()),
        };
        for level in levels {
            let level = match level {
                Ok(level) => level,
                Err(err) => panic!(
                    "scan level entry in {}: {err} — {ctx}",
                    segments_root.display()
                ),
            };
            if !level.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let level_name = level.file_name().to_string_lossy().to_string();
            if level_name.starts_with('.') || !level_name.starts_with('L') {
                continue;
            }
            let entries = match fs::read_dir(level.path()) {
                Ok(entries) => entries,
                Err(err) => panic!("scan {}: {err} — {ctx}", level.path().display()),
            };
            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(err) => panic!(
                        "scan segment entry in {}: {err} — {ctx}",
                        level.path().display()
                    ),
                };
                if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with('.') {
                    continue;
                }
                let manifest = entry.path().join("manifest.bin");
                assert!(
                    manifest.exists(),
                    "segment directory {} exists without manifest.bin (dir-without-manifest defect candidate) — {ctx}",
                    entry.path().display()
                );
            }
        }
    }
}

/// `segments/L*/` roots of every lane directory under the data root (e.g.
/// `<data>/lane_numeric/segments`). Missing directories are skipped — the
/// loader treats them as empty, and the scan mirrors that.
fn lane_segments_roots(data_dir: &Path) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(entries) = fs::read_dir(data_dir) {
        for entry in entries.flatten() {
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            let segments = entry.path().join("segments");
            if segments.is_dir() {
                roots.push(segments);
            }
        }
    }
    roots
}

/// On-disk segment summary for diagnostics: `lane_numeric L0=8 L1=2 ...`
/// (non-hidden segment dirs per level, per lane) plus a count of hidden
/// entries (`.tmp-seg-*` style crash residue, a registered P2 disk-hygiene
/// candidate) under the level dirs. Hidden entries are counted for the
/// report only, never asserted on.
fn segment_diagnostics(data_dir: &Path) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut hidden = 0usize;
    for segments_root in lane_segments_roots(data_dir) {
        let lane = segments_root
            .parent()
            .map(|p| {
                p.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        if let Ok(levels) = fs::read_dir(&segments_root) {
            for level in levels.flatten() {
                if !level.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let level_name = level.file_name().to_string_lossy().to_string();
                if !level_name.starts_with('L') {
                    continue;
                }
                let mut dirs = 0usize;
                if let Ok(entries) = fs::read_dir(level.path()) {
                    for entry in entries.flatten() {
                        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                            continue;
                        }
                        if entry.file_name().to_string_lossy().starts_with('.') {
                            hidden += 1;
                        } else {
                            dirs += 1;
                        }
                    }
                }
                parts.push(format!("{lane}/{level_name}={dirs}"));
            }
        }
    }
    format!("{} hidden_tmp_dirs={hidden}", parts.join(" "))
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
