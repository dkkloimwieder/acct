#!/usr/bin/env bash
set -euo pipefail

# acct-8hv2 architectural audit: rig-noise + per-scenario replication driver.
#
# Runs `phase1_mixed_workload` N_RUNS times with GAP_SECS between runs,
# under whatever T4_* env vars the caller exported (writers, duration,
# psync mode, sampler, etc.). Each run drops+recreates acct_test from
# scratch (via scripts/run-tests.sh) for identical-condition replication.
#
# Output: $LOG_DIR/run_<i>.log per run; $LOG_DIR/summary.txt extracts
# the key metrics ready for median+IQR analysis.
#
# Defaults match acct-8hv2 Phase A baseline:
#   AUDIT_RUNS=5 (5 replicate runs)
#   AUDIT_GAP_SECS=30 (buffer-pool settle window between runs)
#   T4_DURATION_SECS=60 (short runs per ezm methodology)
#   T4_WRITERS=32 (1s6r baseline)
#   T4_REPORT_TIMINGS=1 (capture h73o section decomposition)
#
# Scenario tag goes in the log dir name. Caller sets it via AUDIT_TAG=A1.
# Example invocation:
#
#   AUDIT_TAG=A1_sync_baseline T4_USE_PSYNC=0 ./scripts/run-audit-replicate.sh
#   AUDIT_TAG=C2_psync         T4_USE_PSYNC=1 ./scripts/run-audit-replicate.sh

cd "$(dirname "$0")/.."

N_RUNS="${AUDIT_RUNS:-5}"
GAP_SECS="${AUDIT_GAP_SECS:-30}"
TAG="${AUDIT_TAG:-untagged}"

export T4_DURATION_SECS="${T4_DURATION_SECS:-60}"
export T4_WRITERS="${T4_WRITERS:-32}"
export T4_REPORT_TIMINGS="${T4_REPORT_TIMINGS:-1}"

LOG_DIR="/tmp/audit_8hv2/${TAG}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

echo "==> acct-8hv2 replication run"
echo "    tag:          $TAG"
echo "    runs:         $N_RUNS"
echo "    duration:     ${T4_DURATION_SECS}s per run"
echo "    writers:      $T4_WRITERS"
echo "    use_psync:    ${T4_USE_PSYNC:-0}"
echo "    gap_between:  ${GAP_SECS}s"
echo "    logs:         $LOG_DIR"
echo

START=$(date +%s)
for i in $(seq 1 "$N_RUNS"); do
  echo "[$(date -Is)]  run $i/$N_RUNS"
  LOGFILE="$LOG_DIR/run_${i}.log"
  ./scripts/run-tests.sh --release --test load_phase1_mixed_workload -- \
    --ignored --exact --nocapture phase1_mixed_workload \
    > "$LOGFILE" 2>&1
  echo "  saved: $LOGFILE  ($(wc -l < "$LOGFILE") lines)"
  if [ "$i" -lt "$N_RUNS" ]; then
    echo "  sleeping ${GAP_SECS}s before next run..."
    sleep "$GAP_SECS"
  fi
done
END=$(date +%s)
TOTAL=$((END - START))

echo
echo "==> All $N_RUNS runs complete (${TOTAL}s wall clock). Extracting metrics."

SUMMARY="$LOG_DIR/summary.txt"
{
  echo "# acct-8hv2 replication summary"
  echo "# tag=$TAG runs=$N_RUNS duration=${T4_DURATION_SECS}s writers=$T4_WRITERS use_psync=${T4_USE_PSYNC:-0} gap=${GAP_SECS}s"
  echo
  echo "## Per-run headlines (extracted via grep)"
  for i in $(seq 1 "$N_RUNS"); do
    LOGFILE="$LOG_DIR/run_${i}.log"
    echo
    echo "### run $i"
    grep -E '^ops:|^combined wrapper latency_us:|^deadlocks: delta=|^throughput' "$LOGFILE" || true
  done

  echo
  echo "## Per-run combined-wrapper p50/p95/p99/p99.9 (us)"
  printf '%-6s %12s %12s %12s %12s %12s\n' run p50 p95 p99 p99_9 max
  for i in $(seq 1 "$N_RUNS"); do
    LOGFILE="$LOG_DIR/run_${i}.log"
    awk -v run="$i" '
      /^combined wrapper latency_us:/ {
        p50=p95=p99=p999=mx=0
        for (i=1;i<=NF;i++) {
          split($i, kv, "=")
          if (kv[1]=="p50") p50=kv[2]+0
          else if (kv[1]=="p95") p95=kv[2]+0
          else if (kv[1]=="p99") p99=kv[2]+0
          else if (kv[1]=="p99.9") p999=kv[2]+0
          else if (kv[1]=="max") { gsub(/[^0-9]/,"",kv[2]); mx=kv[2]+0 }
        }
        printf "%-6s %12d %12d %12d %12d %12d\n", run, p50, p95, p99, p999, mx
      }
    ' "$LOGFILE"
  done

  echo
  echo "## Per-run ops + throughput + deadlocks"
  printf '%-6s %10s %10s %10s %10s %10s %10s\n' run total ok skip err throughput deadlocks
  for i in $(seq 1 "$N_RUNS"); do
    LOGFILE="$LOG_DIR/run_${i}.log"
    awk -v run="$i" '
      /^ops:/ {
        tot=ok=sk=er=0; thr=""
        for (i=1;i<=NF;i++) {
          split($i, kv, "=")
          if (kv[1]=="total") tot=kv[2]+0
          else if (kv[1]=="ok") ok=kv[2]+0
          else if (kv[1]=="skip") sk=kv[2]+0
          else if (kv[1]=="err") er=kv[2]+0
          else if (kv[1]=="throughput") thr=kv[2]
        }
      }
      /^deadlocks: delta=/ {
        for (i=1;i<=NF;i++) if ($i ~ /^delta=/) { split($i, kv, "="); dl=kv[2]+0 }
        gsub("/s","",thr)
        printf "%-6s %10d %10d %10d %10d %10.1f %10d\n", run, tot, ok, sk, er, thr+0, dl
      }
    ' "$LOGFILE"
  done

  echo
  echo "## Per-wrapper section p50/p95/p99 across runs (us) [h73o decomposition]"
  echo "## columns: run wrapper section n p50 p95 p99 max"
  for i in $(seq 1 "$N_RUNS"); do
    LOGFILE="$LOG_DIR/run_${i}.log"
    # Section block columns: wrapper section n p50 p95 p99 max  (7 fields)
    awk -v run="$i" '
      /^--- acct-h73o:/ { inblk=1; next }
      /^=====/         { inblk=0 }
      inblk && /^post_/ {
        printf "%-6s %-24s %-22s %10d %10d %10d %10d %12d\n",
               run, $1, $2, $3, $4, $5, $6, $7
      }
    ' "$LOGFILE"
  done

  echo
  echo "## Per-op p50/p95/p99 across runs (us)"
  echo "## columns: run op n p50 p95 p99 max"
  for i in $(seq 1 "$N_RUNS"); do
    LOGFILE="$LOG_DIR/run_${i}.log"
    # Per-op block: op n p50 p95 p99 max  (6 fields, op is a single token)
    awk -v run="$i" '
      /^--- per-op latency/ { inblk=1; next }
      /^deadlocks:/         { inblk=0 }
      inblk && /^(po_receipt|ap_bill|wo_start|op_move|wo_complete|so_ship|customer_invoice|ar_payment|return)/ {
        printf "%-6s %-22s %10d %10d %10d %10d %12d\n",
               run, $1, $2, $3, $4, $5, $6
      }
    ' "$LOGFILE"
  done
} > "$SUMMARY"

echo "==> Summary: $SUMMARY"
echo
echo "Done. To compute medians/IQRs, see $SUMMARY and run analysis offline."
