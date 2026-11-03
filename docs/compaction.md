# Compaction

tsink uses LSM-style leveled compaction to merge small L0 segments produced by the
write pipeline into progressively larger L1 and L2 segments. Compaction reduces the
number of segment files, eliminates duplicate and tombstoned data points, and keeps
query fan-out bounded as data accumulates over time.

---

## L0 / L1 / L2 levels

Every segment carries a level tag written into its manifest at creation time. There are
three levels:

| Level | Written by | Compacted from |
|---|---|---|
| **L0** | Flush pipeline (WAL → segments) | — |
| **L1** | Compactor | L0 |
| **L2** | Compactor | L1 |

Segments at the same level are independent: they may cover overlapping or non-overlapping
time ranges and may belong to different series. The compactor merges segments within a
level and writes the output one level higher. L2 is the default terminal level: no data
moves out of it, but same-level (L2→L2) size-tiered convergence runs once the L2 segment
count exceeds the convergence trigger (see [Terminal level convergence](#terminal-level-convergence-l2l2)).
Segments still age out through retention expiry.

The engine maintains two separate compactors — one for the **numeric lane** (float64
series) and one for the **blob lane** (bytes and native histograms). Each compactor
operates on its own directory subtree and carries its own segment-ID allocator backed by
a shared atomic counter.

---

## Trigger conditions

A compaction pass fires for a given level when either of the following is true:

- **Count trigger** — the number of eligible segments at the source level reaches the
  configured threshold (default **4** for both L0→L1 and L1→L2).
- **Time overlap** — any two segments at the source level have overlapping time ranges,
  regardless of segment count.

The overlap trigger exists because overlapping segments break the sorted-segments
assumption that queries rely on: merging them eagerly keeps the persisted view coherent
and avoids returning duplicates at query time.

Each `compact_once` call walks three transitions — L0→L1, L1→L2 and L2→L2 (terminal
convergence) — in a rotating order rather than a fixed one, and stops at the first
eligible one, so at most one transition is compacted per call. The rotation is
level-fair arbitration (`planning.rs`, `compact_once_scoped`): the next pass starts at
the transition *after* the one that compacted last and walks forward (wrapping), so
three transitions take turns (L0→L1, L1→L2, L2→L2, then L1→L2, L2→L2, L0→L1, …). That
is a true round-robin — with L0→L1 and L1→L2 both continuously eligible, merely moving
the previous winner to the back of the queue would ping-pong between those two and
starve L2→L2 forever. Close-drain passes skip the L2→L2 entry entirely so a legacy
terminal backlog cannot extend shutdown.

---

## Window selection

Rather than compacting all available source segments at once, each pass selects a
**window** of up to `DEFAULT_SOURCE_WINDOW_SEGMENTS` (16) segments. The selection
algorithm:

1. If any segments have overlapping time ranges, the algorithm identifies the smallest
   cluster of overlapping segments and selects up to 16 of them by their original storage
   order. This ensures overlaps are resolved before anything else.
2. If there are no overlaps, the algorithm takes the oldest `count_trigger` (≥ 2)
   segments sorted by segment ID.

A window must contain at least two segments; a single-segment window is always a no-op.

---

## Merge algorithm

The merge is a k-way heap merge that streams all source data points in timestamp order
across all input chunks for the same series:

1. Each input chunk is decoded into individual data points and placed behind a cursor.
2. A min-heap ordered by `(timestamp, chunk_order)` drives the merge — the earliest
   timestamp across all cursors is drained first.
3. A point is **skipped** if:
   - It is an exact duplicate of the previously emitted point — same `(ts, value)` pair.
   - Its timestamp falls within a tombstoned range.
   - Its timestamp is older than the retention cutoff (when compaction is used to enforce
     retention).
4. Surviving points are accumulated into output chunks. When a chunk reaches
   `chunk_point_cap` points it is closed and a new one is started.
5. When accumulated output points reach the **segment point budget** —
   `chunk_point_cap × 512` — the current set of chunks is written as a new output
   segment and the accumulator is reset. This caps output segment size even when many
   source segments are merged at once.

Chunk encoding for output chunks uses the same adaptive codec pipeline as all other
segments: delta/XOR encoding for timestamps and values, with optional zstd compression.

The WAL highwater carried by an output segment is the **maximum** highwater across all
source segments, allowing the WAL to reclaim space for any data already compacted to
persistent segments.

---

## Terminal level convergence (L2→L2)

L2 has no lower level to receive its output, so historically it was purely terminal:
segments accumulated there forever and aged out only through retention. Engines upgraded
in place (or configured with very low write rates) can therefore carry a large backlog of
tiny legacy L2 segments. The **L2→L2 convergence** pass shrinks that backlog with
size-tiered same-level merges that never rewrite the bulk of the terminal data:

- **Candidates** — only segments whose `point_count` is at most
  `output_segment_point_budget / 8` (1,048,576 points at the default budget of
  8,388,608). Larger segments are never selected, so a pass can never mint a new
  "large" segment.
- **Trigger** — at least 2 eligible candidates AND the total L2 segment count strictly
  above the convergence trigger (`l2_compaction_trigger`, default **32**; `0` disables
  convergence entirely, restoring a purely terminal L2).
- **Trigger gate cost** — the gate is decided before the level survey, from the L2
  segment directory count alone (a directory listing; no manifest is opened). `0` and a
  directory count at or below the trigger therefore cost **zero manifest reads** on
  every background tick: disabling convergence is a true zero-cost switch, not a
  convergence pass that surveys every L2 manifest and then declines to merge. The
  survey (one open+read+parse per segment) runs only when the directory count already
  exceeds the trigger.
- **Gate cost boundary (deliberate trade-off)** — the short-circuit path reads no
  manifest, so two checks that live inside the survey do not run on it: the
  `segment directory level mismatch` validation (the only place in the engine that
  compares a segment directory's level name against `manifest.level`), and the fail-fast
  that rejects an unreadable manifest as `DataCorruption`. This is the price of the
  zero-cost switch, not an omission: both are diagnostics for hand-edited directories or
  upstream bugs rather than integrity gates. Integrity is covered independently — segment
  file fingerprints are verified on every segment load (`verify_file_manifest_entry`),
  startup recovery quarantines invalid segment directories, and loading a selected window
  goes through the strict `SegmentLoadPolicy::FailOnInvalid(Compaction)` path. Both
  survey-time checks return as soon as the L2 directory count rises above the trigger.
- **Window** — candidates sorted by `(point_count, min_ts, segment_id)` ascending
  (smallest and oldest fragments first), capped at `source_window_segments` (16) and at
  a cumulative point total of `output_segment_point_budget / 8`. The merged output is
  therefore never larger than a candidate, stays inside the candidate size class, and a
  size-squeezed configuration terminates instead of re-merging its own outputs forever.
- **Retention pre-exclusion** — segments whose `max_ts` already falls below the active
  retention cutoff are excluded from candidacy: retention's next sweep deletes them
  outright, so merging them would rewrite data that is about to disappear.
- **Merge** — the selected window goes through the same k-way merge, deduplication,
  tombstone filtering, and atomic replacement as any other pass; the output is written
  back to L2.

Convergence progress is deliberately spread over background ticks: a close-time drain
structurally skips L2→L2 (a backlog of tens of thousands of terminal segments would
otherwise extend graceful shutdown by an unbounded, data-dependent amount), and a
background drain consumes at most one convergence window per lane per tick before
reporting settled. Draining a 15.8k-segment legacy backlog therefore takes roughly
15.8k / 16 ≈ 1000 ticks — at the default 5-second interval, on the order of an hour —
instead of one long gate hold. Manual `compact_once` style passes and background passes
are unaffected.

The pass counter is exposed as `l2_to_l2_runs_total` in
`CompactionObservabilitySnapshot`. To measure convergence over an existing production
directory, copy the data directory and run the env-gated drill test
`engine::storage_engine::tests::l2_convergence_drill::l2_convergence_drill`
(`#[ignore]`; set `TSINK_CONVERGE_DATA_DIR` to the copy, optionally
`TSINK_CONVERGE_CSV` / `TSINK_CONVERGE_TIMEOUT_SECS` / `TSINK_CONVERGE_POLL_SECS` /
`TSINK_CONVERGE_SAMPLE_SERIES` — see the test's module docs). It records the L2 decay
curve to CSV, then verifies a deterministic series sample returns point-identical
results before and after convergence.

---

## Tombstone handling

Before each compaction pass the tombstone store is loaded from disk. A tombstone is a
series-scoped time range (`start` inclusive, `end` exclusive). Any point whose timestamp
falls within a tombstone range for its series is silently discarded during the merge.
Points tombstoned in one compaction pass will never reappear in subsequent reads because
they are permanently excluded from the output segments.

---

## Atomic segment replacement

Compaction writes output segments to disk first, then atomically swaps them in to replace
the source segments. This two-phase protocol makes interruptions safe:

1. **Write outputs** — each output segment is written to a new directory under the lane
   root. If writing fails, any partial output directories are removed (rollback).
2. **Write replacement marker** — a JSON file is written atomically to
   `.compaction-replacements/replace-<ts>-<nonce>.json`. The marker lists the relative
   paths of both source and output segments.
3. **Apply replacement** — the source segment directories are removed and the marker file
   is deleted.

If the process is interrupted between steps 2 and 3, the marker file survives on disk.
The next `compact_once` call (or startup) calls `finalize_pending_compaction_replacements`
which replays all pending markers in sorted order, completing any interrupted
replacements. An output-side manifest must exist for the replacement to be applied; if
it does not, the replacement is treated as a corruption error.

### Known read window between step 3 and the index refresh

Step 3 deletes the source segment directories, but the in-memory persisted index is only
updated on the next maintenance tick (`apply_persisted_catalog_transition_phase`). That
leaves a short window in which the index still references a source segment whose files
are gone. A query that reaches that segment inside the window and whose chunk mapping was
already evicted by the mmap budget fails with `NotFound` instead of returning data — the
query is transiently rejected, never silently wrong, and a retry once the index has
refreshed succeeds.

`SegmentMapRemapResolver` (W4-T0) does not close this window: slots are assigned
monotonically and never re-bound (`install.rs`), so re-resolving a slot against the
still-stale index yields the same deleted path. The counter
`persisted_remap_not_found_total` therefore measures *how often the window is hit*, not
how often it was repaired. Two ways to shrink the window exist — defer source deletion
into a hidden staging directory until after the index refresh, or tolerate the error at
the query layer — neither is implemented today; closing it is tracked as review item C-3
in `docs/research/2026-11-03-edge-hardening-review.md`.

---

## Background thread

The compaction background thread runs continuously while the storage engine is open. It
sleeps for `compaction_interval` (default **5 seconds**) between passes and wakes
immediately when the flush pipeline signals that new segments have been written.

The thread runs both the numeric compactor and the blob compactor in sequence on each
wakeup. A mutex (`compaction_lock`) serialises the thread against manual compaction
calls and against snapshot operations.

On `close()`, the engine acquires all write permits, flushes active state to segments,
and then runs up to **128** compaction passes to drain any remaining work before
shutting down background threads.

When `background_fail_fast` is enabled (the default), a compaction error marks the
storage engine as unhealthy and causes subsequent write and flush operations to return
an error.

---

## Observability

The `observability_snapshot()` method exposes a `CompactionObservabilitySnapshot` with
cumulative counters:

| Field | Description |
|---|---|
| `runs_total` | Total `compact_once` calls |
| `success_total` | Runs that produced at least one output segment |
| `noop_total` | Runs where no compaction was needed |
| `errors_total` | Runs that returned an error |
| `source_segments_total` | Cumulative source segments consumed |
| `output_segments_total` | Cumulative output segments produced |
| `source_chunks_total` | Cumulative source chunks processed |
| `output_chunks_total` | Cumulative output chunks written |
| `source_points_total` | Cumulative input data points before dedup/tombstone filtering |
| `output_points_total` | Cumulative output data points after filtering |
| `duration_nanos_total` | Cumulative wall-clock time spent in compaction |
| `l2_to_l2_runs_total` | Completed L2→L2 terminal-convergence passes |

The ratio `output_points_total / source_points_total` indicates how much deduplication
or tombstone removal is occurring over time.

---

## Tuning

| `StorageBuilder` method | Default | Effect |
|---|---|---|
| `with_chunk_points(n)` | 2048 | Maximum points per chunk. It controls chunk granularity only — the output segment point budget is independent of it: a fixed 8,388,608 points (8× the 1M-point L0 flush shard), overridable with `with_output_segment_point_budget`. Larger values produce fewer, bigger chunks and reduce compaction frequency but increase per-segment memory and I/O cost. |
| `with_l2_compaction_trigger(n)` | 32 | L2 total segment count above which same-level terminal convergence may run. `0` disables convergence (L2 stays purely terminal) at zero cost: no L2 manifest is surveyed. Likewise no survey happens while the L2 segment count stays at or below `n`. |

There are no builder methods for the L0/L1 count triggers; they are fixed at 4. The
compaction interval is also fixed at 5 seconds in the engine defaults.

---

## Interaction with retention

Compaction and retention are separate operations. The retention sweep
(`sweep_expired_persisted_segments`) deletes entire segments whose time range falls
entirely below the retention cutoff. When a segment partially overlaps the cutoff, the
compactor rewrites it (`stage_segment_rewrite_with_retention`) and drops points below
the cutoff during the merge, producing a smaller output segment at the same level rather
than advancing it upward.
