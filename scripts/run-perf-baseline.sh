#!/usr/bin/env bash
set -euo pipefail

# Run the T4 deadlock-freedom load test N times back-to-back to
# characterize variance on noisy consumer hardware. Per run, captures:
#   - the test's own T4 PERF SUMMARY block (latencies, throughput,
#     pg_stat_database / pg_stat_io / WAL deltas, top pg_stat_statements)
#   - a single T4_CSV_VALUES line (machine-parseable, all numeric metrics)
#   - vmstat samples to a side log (CPU breakdown + context switches +
#     iowait/steal — answers "was the host the bottleneck?")
#
# After all runs, emits a cross-run aggregate table with min / median /
# mean / max / range for every metric and a per-run vmstat summary.
#
# Defaults below are tuned for an 8-thread consumer laptop:
#
#   T4_BASELINE_RUNS=5         runs back-to-back
#   T4_DURATION_SECS=300       seconds per run
#   T4_WRITERS=100             spec-target concurrency
#   T4_VMSTAT_INTERVAL=5       vmstat sample interval (seconds)
#
# Override via environment:
#
#   T4_BASELINE_RUNS=3 T4_DURATION_SECS=600 ./scripts/run-perf-baseline.sh
#
# Each run starts with a warm Postgres process; only the first run's
# buffer cache is cold. The acct_test DB is dropped + recreated by
# run-tests.sh between every run, so transfer/account state does not
# leak across runs. pg_stat_statements is reset by the test itself at
# run start.

cd "$(dirname "$0")/.."

N_RUNS="${T4_BASELINE_RUNS:-5}"
DURATION="${T4_DURATION_SECS:-300}"
WRITERS="${T4_WRITERS:-100}"
VMSTAT_INTERVAL="${T4_VMSTAT_INTERVAL:-5}"

LOG_DIR="/tmp/t4_baseline_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

VMSTAT_OK=1
if ! command -v vmstat >/dev/null 2>&1; then
  echo "WARNING: vmstat not available; OS-level capture disabled" >&2
  VMSTAT_OK=0
fi

echo "==> Multi-run T4 perf baseline"
echo "    runs:     $N_RUNS"
echo "    duration: ${DURATION}s per run"
echo "    writers:  $WRITERS"
echo "    vmstat:   ${VMSTAT_INTERVAL}s interval (enabled=$VMSTAT_OK)"
echo "    logs:     $LOG_DIR"
echo

OVERALL_START=$(date +%s)

for i in $(seq 1 "$N_RUNS"); do
  echo "============================================================"
  echo "== Run $i / $N_RUNS  ($(date -Iseconds))"
  echo "============================================================"

  VMSTAT_LOG="$LOG_DIR/run_${i}_vmstat.log"
  VMSTAT_PID=""
  if [ "$VMSTAT_OK" -eq 1 ]; then
    vmstat "$VMSTAT_INTERVAL" > "$VMSTAT_LOG" 2>&1 &
    VMSTAT_PID=$!
  fi

  T4_DURATION_SECS="$DURATION" T4_WRITERS="$WRITERS" \
    ./scripts/run-tests.sh --test load_deadlock_freedom -- --ignored --nocapture \
    2>&1 | tee "$LOG_DIR/run_${i}.log"

  if [ -n "$VMSTAT_PID" ]; then
    kill "$VMSTAT_PID" 2>/dev/null || true
    wait "$VMSTAT_PID" 2>/dev/null || true
  fi
  echo
done

OVERALL_END=$(date +%s)
TOTAL_S=$(( OVERALL_END - OVERALL_START ))

# ============== Cross-run aggregate ==============

CSV_FILE="$LOG_DIR/all_runs.csv"
grep -h '^T4_CSV_VALUES:' "$LOG_DIR"/run_*.log 2>/dev/null \
  | sed 's/^T4_CSV_VALUES: //' > "$CSV_FILE"

if [ ! -s "$CSV_FILE" ]; then
  echo "ERROR: no T4_CSV_VALUES lines found in run logs" >&2
  exit 1
fi

HEADER_LINE=$(grep -h '^T4_CSV_HEADER:' "$LOG_DIR"/run_*.log 2>/dev/null \
              | head -1 | sed 's/^T4_CSV_HEADER: //')

echo
echo "============================================================"
echo "== AGGREGATE across $N_RUNS runs (${TOTAL_S}s total wall clock)"
echo "============================================================"

awk -v hdr="$HEADER_LINE" -F',' '
{
  for (i = 1; i <= NF; i++) vals[i, NR] = $i + 0
  n = NR
  cols = NF
}
END {
  split(hdr, names, ",")
  printf "%-25s %14s %14s %14s %14s %14s\n", "metric", "min", "median", "mean", "max", "range"
  printf "%-25s %14s %14s %14s %14s %14s\n", \
    "-------------------------", "--------------", "--------------", "--------------", "--------------", "--------------"
  for (i = 1; i <= cols; i++) {
    delete arr
    for (j = 1; j <= n; j++) arr[j] = vals[i, j]
    # insertion sort (n is small — 3 to 10 typically)
    for (a = 2; a <= n; a++) {
      key = arr[a]
      b = a - 1
      while (b >= 1 && arr[b] > key) { arr[b+1] = arr[b]; b-- }
      arr[b+1] = key
    }
    minv = arr[1]; maxv = arr[n]
    if (n % 2 == 1) med = arr[int((n+1)/2)]
    else            med = (arr[n/2] + arr[n/2 + 1]) / 2
    sum = 0
    for (j = 1; j <= n; j++) sum += arr[j]
    mean = sum / n
    range = maxv - minv
    printf "%-25s %14.3f %14.3f %14.3f %14.3f %14.3f\n", names[i], minv, med, mean, maxv, range
  }
}' "$CSV_FILE"

# ============== Per-run host (vmstat) ==============

if [ "$VMSTAT_OK" -eq 1 ]; then
  echo
  echo "============================================================"
  echo "== Host CPU / IO during each run (vmstat means)"
  echo "==   us=user% sy=sys% id=idle% wa=iowait% st=steal% cs=ctxsw/s"
  echo "============================================================"
  for i in $(seq 1 "$N_RUNS"); do
    VMSTAT_LOG="$LOG_DIR/run_${i}_vmstat.log"
    [ -s "$VMSTAT_LOG" ] || continue
    awk -v run="$i" '
    # Skip 2 header lines, skip first sample (cumulative-since-boot).
    NR > 3 && NF >= 17 {
      cs += $12; us += $13; sy += $14; id += $15; wa += $16; st += $17
      if ($15 + 0 < min_id || min_id == "") min_id = $15
      if ($15 + 0 > max_id || max_id == "") max_id = $15
      n++
    }
    END {
      if (n > 0) {
        printf "  run %d: us=%5.1f%%  sy=%5.1f%%  id=%5.1f%% (min=%s max=%s)  wa=%4.1f%%  st=%4.1f%%  cs=%6.0f/s  (n=%d samples)\n", \
          run, us/n, sy/n, id/n, min_id, max_id, wa/n, st/n, cs/n, n
      }
    }' "$VMSTAT_LOG"
  done
fi

echo
echo "Per-run logs:     $LOG_DIR/run_*.log"
[ "$VMSTAT_OK" -eq 1 ] && echo "Per-run vmstat:   $LOG_DIR/run_*_vmstat.log"
echo "Combined CSV:     $CSV_FILE"
echo "Done."
