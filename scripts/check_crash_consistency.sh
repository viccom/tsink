#!/usr/bin/env bash
# Crash-consistency gate (W1-T3): runs the SIGKILL harness in
# tests/crash_consistency_test.rs — 4 kill modes x ITER rounds per run.
#
# Usage: [ITER=20] [REPEAT=1] [RUST_LOG=warn] scripts/check_crash_consistency.sh
#
# NOTE: do NOT pass `--ignored --exact crash_child_mode` to cargo here. That
# argv belongs to the *parent* test, which re-executes this test binary to
# spawn its killable children. Adding it to the cargo invocation would run
# only the ignored child mode itself, skip the parent gate entirely, and
# leave the door unguarded while looking green.
set -euo pipefail

ITER="${ITER:-20}"
REPEAT="${REPEAT:-1}"
RUST_LOG="${RUST_LOG:-warn}"
export RUST_LOG

run_start=$SECONDS
failures=0

for rep in $(seq 1 "$REPEAT"); do
  rep_start=$SECONDS
  echo "==> crash-consistency run ${rep}/${REPEAT} (ITER=${ITER}, $((4 * ITER)) rounds)"
  if cargo test --test crash_consistency_test; then
    echo "PASS: run ${rep} ($((SECONDS - rep_start))s)"
  else
    status=$?
    failures=$((failures + 1))
    echo "FAIL: run ${rep} exited with status ${status} ($((SECONDS - rep_start))s)"
    echo "Most recent crash scene dirs (newest first, failed rounds only):"
    ls -dt /tmp/tsink-crash-* 2>/dev/null | head -n 10 || true
  fi
done

elapsed=$((SECONDS - run_start))
rounds=$((4 * ITER * REPEAT))
if [[ ${failures} -eq 0 ]]; then
  echo "crash-consistency gate: PASS (${REPEAT} run(s), ${rounds} rounds total, ${elapsed}s)"
else
  echo "crash-consistency gate: FAIL (${failures}/${REPEAT} run(s) failed, ${rounds} rounds attempted, ${elapsed}s)"
  exit 1
fi
