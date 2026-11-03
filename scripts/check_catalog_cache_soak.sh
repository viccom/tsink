#!/usr/bin/env bash
# Catalog-cache soak gate (W2-T3): long-run memory-slope regression.
#
# Runs the #[ignore]d lib test catalog_cache_soak::catalog_cache_soak_long_run
# in release mode: 30 series x 10 points/s with default knobs (catalog entry
# cache 32768 / series cache 32 MiB, WAL PerAppend), sampling RSS,
# catalog_cache_entries (entries map only) and catalog_cache_series_slots
# (observation only) every 5 min. PASS requires RSS slope <= 0.5 MB/h
# (first 25% samples dropped as warmup; skipped with a warning under 4
# samples) and catalog_cache_entries never above the entry capacity bound.
#
# Usage:
#   scripts/check_catalog_cache_soak.sh                 # full 2h gate (7200s)
#   SOAK_SECONDS=600 scripts/check_catalog_cache_soak.sh  # short smoke run
#
# Env knobs:
#   SOAK_SECONDS  duration forwarded to TSINK_SOAK_SECONDS (default 7200)
#   SOAK_CSV      csv path forwarded to TSINK_SOAK_CSV
#                 (default /tmp/tsink-catalog-cache-soak.csv)
#
# NOTE on the test-filter argv: `--ignored` selects only #[ignore]d tests and
# `--exact` matches a FULL test path, so the module-qualified name is required
# here (a bare `catalog_cache_soak` matches nothing under --exact).
set -euo pipefail

SOAK_SECONDS="${SOAK_SECONDS:-7200}"
export TSINK_SOAK_SECONDS="$SOAK_SECONDS"
if [[ -n "${SOAK_CSV:-}" ]]; then
  export TSINK_SOAK_CSV="$SOAK_CSV"
fi
CSV_PATH="${TSINK_SOAK_CSV:-/tmp/tsink-catalog-cache-soak.csv}"

TEST_NAME="engine::storage_engine::tests::catalog_cache_soak::catalog_cache_soak_long_run"

echo "==> catalog-cache soak: ${SOAK_SECONDS}s, csv=${CSV_PATH}"
log="$(mktemp /tmp/tsink-catalog-cache-soak.XXXXXX.log)"
trap 'rm -f "$log"' EXIT

status=0
cargo test -p tsink --release --lib -- \
  --ignored --exact "$TEST_NAME" --nocapture 2>&1 | tee "$log" || status=$?

if [[ $status -ne 0 ]]; then
  echo "catalog-cache soak driver: cargo test exited with status ${status}"
  exit "$status"
fi

# cargo test passing is not enough: require the explicit conclusion line.
if grep -q 'catalog-cache-soak: PASS ' "$log"; then
  echo "catalog-cache soak driver: PASS conclusion line found"
  echo "csv: ${CSV_PATH}"
  exit 0
fi

echo "catalog-cache soak driver: cargo test reported success but no PASS conclusion line was printed"
exit 1
