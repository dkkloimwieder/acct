#!/usr/bin/env bash
set -euo pipefail

# Multi-shape T4 perf baseline. Runs the load test across a *matrix*
# of (writers, events-per-batch) configurations to characterize the
# regime structure: single-writer scaling vs concurrency-bound vs
# the catastrophic combinations.
#
# Defaults below are the v0 baseline matrix (committed in
# perf_baseline_v0.md):
#
#   T4_CONFIGS="1:5:20 1:1000:1000 32:5:20 100:5:20 100:100:100"
#   T4_BASELINE_RUNS=3
#   T4_DURATION_SECS=300
#
# Each config triple is "writers:events_min:events_max". For each
# config, runs T4_BASELINE_RUNS back-to-back; aggregates per config;
# emits a cross-config median comparison at the end.
#
# Override the matrix to characterize a single point ad-hoc:
#
#   T4_CONFIGS="1:1000:1000" T4_BASELINE_RUNS=1 T4_DURATION_SECS=60 \
#     ./scripts/run-perf-baseline.sh
#
# Captures per run: T4 PERF SUMMARY block, T4_CSV_VALUES,
# vmstat sidecar (CPU breakdown + ctxsw rate). See
# tests/load_deadlock_freedom.rs for the in-test instrumentation.

cd "$(dirname "$0")/.."

DEFAULT_CONFIGS="1:5:20 1:1000:1000 32:5:20 100:5:20 100:100:100"
CONFIGS="${T4_CONFIGS:-$DEFAULT_CONFIGS}"
N_RUNS="${T4_BASELINE_RUNS:-3}"
DURATION="${T4_DURATION_SECS:-300}"
VMSTAT_INTERVAL="${T4_VMSTAT_INTERVAL:-5}"
# Which cargo test binary to drive. Default is the deadlock-freedom
# probe (worst-case lock contention). Set to load_realistic_workload
# (acct-2ey) for the cross-account-spread shape.
T4_BINARY_NAME="${T4_BINARY:-load_deadlock_freedom}"

LOG_DIR="/tmp/t4_baseline_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

VMSTAT_OK=1
if ! command -v vmstat >/dev/null 2>&1; then
  echo "WARNING: vmstat not available; OS-level capture disabled" >&2
  VMSTAT_OK=0
fi

echo "==> Multi-shape T4 perf baseline"
echo "    binary:      $T4_BINARY_NAME"
echo "    runs/config: $N_RUNS"
echo "    duration:    ${DURATION}s per run"
echo "    vmstat:      ${VMSTAT_INTERVAL}s interval (enabled=$VMSTAT_OK)"
echo "    configs:"
for cfg in $CONFIGS; do
  IFS=: read -r W EMIN EMAX <<<"$cfg"
  echo "      writers=$W  events=${EMIN}..${EMAX}"
done
echo "    logs:        $LOG_DIR"
echo

OVERALL_START=$(date +%s)

run_one() {
  local cfg_dir="$1" run_idx="$2" w="$3" emin="$4" emax="$5"
  local vmstat_log="$cfg_dir/run_${run_idx}_vmstat.log"
  local vmstat_pid=""
  if [ "$VMSTAT_OK" -eq 1 ]; then
    vmstat "$VMSTAT_INTERVAL" > "$vmstat_log" 2>&1 &
    vmstat_pid=$!
  fi
  # Forward all caller-set T4_* env vars (T4_BENCH_SKUS for the
  # realistic workload, etc.) plus the per-config writers/events.
  T4_DURATION_SECS="$DURATION" T4_WRITERS="$w" \
    T4_EVENTS_MIN="$emin" T4_EVENTS_MAX="$emax" \
    ./scripts/run-tests.sh --test "$T4_BINARY_NAME" -- --ignored --nocapture \
    > "$cfg_dir/run_${run_idx}.log" 2>&1
  if [ -n "$vmstat_pid" ]; then
    kill "$vmstat_pid" 2>/dev/null || true
    wait "$vmstat_pid" 2>/dev/null || true
  fi
}

# === Run all configs × runs ===
for cfg in $CONFIGS; do
  IFS=: read -r W EMIN EMAX <<<"$cfg"
  CFG_TAG="w${W}_e${EMIN}-${EMAX}"
  CFG_DIR="$LOG_DIR/$CFG_TAG"
  mkdir -p "$CFG_DIR"
  echo "============================================================"
  echo "== Config: writers=$W events=${EMIN}..${EMAX}  ($N_RUNS runs)"
  echo "============================================================"
  for i in $(seq 1 "$N_RUNS"); do
    echo "[$(date -Is)]  $CFG_TAG  run $i/$N_RUNS"
    run_one "$CFG_DIR" "$i" "$W" "$EMIN" "$EMAX"
  done
  echo
done

OVERALL_END=$(date +%s)
TOTAL_S=$((OVERALL_END - OVERALL_START))

# === Per-config aggregate ===
echo "============================================================"
echo "== AGGREGATE per config (${TOTAL_S}s total wall clock)"
echo "============================================================"
for cfg in $CONFIGS; do
  IFS=: read -r W EMIN EMAX <<<"$cfg"
  CFG_TAG="w${W}_e${EMIN}-${EMAX}"
  CFG_DIR="$LOG_DIR/$CFG_TAG"
  CSV_FILE="$CFG_DIR/all_runs.csv"
  grep -h '^T4_CSV_VALUES:' "$CFG_DIR"/run_*.log 2>/dev/null \
    | sed 's/^T4_CSV_VALUES: //' > "$CSV_FILE"
  HEADER_LINE=$(grep -h '^T4_CSV_HEADER:' "$CFG_DIR"/run_*.log 2>/dev/null \
                | head -1 | sed 's/^T4_CSV_HEADER: //')
  if [ ! -s "$CSV_FILE" ]; then
    echo "  (config $CFG_TAG: no data)"
    continue
  fi
  echo
  echo "---- Config: writers=$W events=${EMIN}..${EMAX} ----"
  awk -v hdr="$HEADER_LINE" -F',' '
  { for (i=1;i<=NF;i++) vals[i,NR]=$i+0; n=NR; cols=NF }
  END {
    split(hdr, names, ",")
    printf "  %-25s %14s %14s %14s %14s\n", "metric", "min", "median", "mean", "max"
    for (i=1;i<=cols;i++) {
      delete arr
      for (j=1;j<=n;j++) arr[j]=vals[i,j]
      for (a=2;a<=n;a++){k=arr[a];b=a-1;while(b>=1&&arr[b]>k){arr[b+1]=arr[b];b--};arr[b+1]=k}
      minv=arr[1]; maxv=arr[n]
      if (n%2==1) med=arr[int((n+1)/2)]; else med=(arr[n/2]+arr[n/2+1])/2
      s=0; for (j=1;j<=n;j++) s+=arr[j]; mean=s/n
      printf "  %-25s %14.3f %14.3f %14.3f %14.3f\n", names[i], minv, med, mean, maxv
    }
  }' "$CSV_FILE"
done

# === Cross-config medians (the headline summary) ===
echo
echo "============================================================"
echo "== Cross-config medians (the headline)"
echo "==   bps=batches/s  evps=events/s  p* in ms"
echo "============================================================"
printf "  %-22s %10s %10s %10s %10s %10s %12s\n" config bps evps p50_ms p95_ms p99_ms wal_MB
for cfg in $CONFIGS; do
  IFS=: read -r W EMIN EMAX <<<"$cfg"
  CFG_TAG="w${W}_e${EMIN}-${EMAX}"
  CSV_FILE="$LOG_DIR/$CFG_TAG/all_runs.csv"
  [ -s "$CSV_FILE" ] || continue
  awk -F',' -v tag="$CFG_TAG" '
  { for (i=1;i<=NF;i++) vals[i,NR]=$i+0; n=NR }
  function med_of(col,    arr,a,b,k,m) {
    delete arr
    for (j=1;j<=n;j++) arr[j]=vals[col,j]
    for (a=2;a<=n;a++){k=arr[a];b=a-1;while(b>=1&&arr[b]>k){arr[b+1]=arr[b];b--};arr[b+1]=k}
    if (n%2==1) m=arr[int((n+1)/2)]; else m=(arr[n/2]+arr[n/2+1])/2
    return m
  }
  END {
    bps   = med_of(7)
    evps  = med_of(8)
    p50   = med_of(9)  / 1000.0
    p95   = med_of(10) / 1000.0
    p99   = med_of(11) / 1000.0
    walMB = med_of(26) / 1024.0 / 1024.0
    printf "  %-22s %10.2f %10.1f %10.1f %10.1f %10.1f %12.1f\n", tag, bps, evps, p50, p95, p99, walMB
  }' "$CSV_FILE"
done

# === vmstat per config ===
if [ "$VMSTAT_OK" -eq 1 ]; then
  echo
  echo "============================================================"
  echo "== vmstat means per run (us=user% sy=sys% id=idle% wa=iowait% cs=ctx/s)"
  echo "============================================================"
  for cfg in $CONFIGS; do
    IFS=: read -r W EMIN EMAX <<<"$cfg"
    CFG_TAG="w${W}_e${EMIN}-${EMAX}"
    CFG_DIR="$LOG_DIR/$CFG_TAG"
    echo "  ---- $CFG_TAG ----"
    for i in $(seq 1 "$N_RUNS"); do
      V="$CFG_DIR/run_${i}_vmstat.log"
      [ -s "$V" ] || continue
      awk -v run="$i" '
      NR>3 && NF>=17 {
        cs+=$12; us+=$13; sy+=$14; id+=$15; wa+=$16; st+=$17
        if ($15+0 < min_id || min_id=="") min_id=$15
        if ($15+0 > max_id || max_id=="") max_id=$15
        n++
      }
      END { if (n>0) printf "    run %d: us=%5.1f  sy=%5.1f  id=%5.1f (min=%s max=%s)  wa=%4.1f  cs=%6.0f/s\n", run, us/n, sy/n, id/n, min_id, max_id, wa/n, cs/n }
      ' "$V"
    done
  done
fi

echo
echo "Logs: $LOG_DIR"
echo "Done."
