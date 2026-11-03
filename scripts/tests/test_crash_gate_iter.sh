#!/usr/bin/env bash
# Regression guard for scripts/check_crash_consistency.sh: the ITER the gate
# resolves must reach the `cargo test` child process. If it is not exported, a
# bare `scripts/check_crash_consistency.sh` runs the test binary's DEFAULT_ITER
# (5) rounds per mode while the script reports 4 * ITER (80) rounds.
#
# The gate's `--dry-run` prints ITER exactly as a child shell sees it, so a
# missing `export` surfaces as `<unset>` instead of being masked by the parent
# shell's own value. No cargo run, no timing assertion.
#
# Usage: bash scripts/tests/test_crash_gate_iter.sh
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
gate="${script_dir}/check_crash_consistency.sh"

failures=0

check() {
  local label=$1 expected=$2
  shift 2
  local actual status=0
  actual=$("$@" 2>&1) || status=$?
  if [[ ${status} -ne 0 ]]; then
    echo "FAIL - ${label}: gate exited ${status}: ${actual}"
    failures=$((failures + 1))
  elif [[ ${actual} == "${expected}" ]]; then
    echo "ok   - ${label}: ${actual}"
  else
    echo "FAIL - ${label}: expected ${expected}, got ${actual}"
    failures=$((failures + 1))
  fi
}

# Bare run (ITER absent from the caller's environment): the gate must hand
# 20 rounds per mode to the cargo child.
check "bare run defaults to 20 rounds per mode" "ITER=20" \
  env -u ITER "${gate}" --dry-run

# A caller-supplied value must win over the default.
check "caller-supplied ITER wins" "ITER=7" \
  env ITER=7 "${gate}" --dry-run

if [[ ${failures} -eq 0 ]]; then
  echo "test_crash_gate_iter: PASS"
else
  echo "test_crash_gate_iter: FAIL (${failures} check(s) failed)"
  exit 1
fi
