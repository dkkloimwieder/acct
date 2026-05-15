#!/usr/bin/env bash
# M3.2 (acct-4d4n.8) Q-A fan_in bench.
#
# Runs N=8 concurrent backends all applying to the SAME (sku, location)
# pool on the SAME shard for a fixed duration, then counts the rows in
# poc_test_rows to compute throughput. Repeats for each pool_lock_mode.
#
# Args: DURATION_S (default 30)  RUNS (default 4)  WORKERS (default 8)
# Output: results-m32-pool-lock-fan-in.md
set -euo pipefail

DSN='postgres://acct:acct_dev@localhost:5111/acct_poc_queue'
PSQL="psql -At $DSN"
DURATION_S=${DURATION_S:-30}
RUNS=${RUNS:-4}
WORKERS=${WORKERS:-8}
MODES=(none pool_locks pool_lock_anchors)
SKU=4242
LOC=1
RESULTS_FILE="$(dirname "$0")/results-m32-pool-lock-fan-in.md"
TMPDIR="${TMPDIR:-/tmp}/m32-$$"
mkdir -p "$TMPDIR"
trap 'rm -rf "$TMPDIR"' EXIT

# Per-worker bash function: loops applies until $TMPDIR/stop appears.
# Inner batch sized for prompt stop response under pool_locks contention
# (10 applies per psql call → worker checks stop ~1-2s after signal).
#
# CRITICAL: each apply is wrapped in BEGIN/COMMIT to force its own txn.
# Without this, `psql -c "stmt1; stmt2; ..."` runs all 10 statements in
# ONE implicit transaction, which holds the pool_lock for the entire
# inner batch and creates a circular wait under pool_locks contention
# (committer-B blocks on committer-A's pool_lock, while A's slot still
# needs B to drain it — the queue's wait/wake loop has no visibility
# into PG-level row locks). Explicit per-apply txns release the row
# lock immediately so the next backend can acquire it.
worker_loop() {
  local wid="$1"
  local mode="$2"
  local pidfile="$TMPDIR/w${wid}.pid"
  echo $$ > "$pidfile"
  local n=0
  local inner_batch=10
  while [[ ! -f "$TMPDIR/stop" ]]; do
    local sql="SET poc_ledger.pool_lock_mode = '$mode'; "
    for j in $(seq 1 $inner_batch); do
      sql+="BEGIN; SELECT 1 FROM poc_ledger_apply($SKU, $LOC, 1, $((wid * 10000000 + n * 100 + j)), 'mock'); COMMIT; "
    done
    $PSQL -c "$sql" > /dev/null 2>&1 || true
    n=$((n + inner_batch))
  done
  echo "$n" > "$TMPDIR/w${wid}.count"
}

declare -A SAMPLES   # SAMPLES["$mode.$run"] = throughput

echo "M3.2 Q-A pool-lock fan_in bench: D=${DURATION_S}s, runs=${RUNS}, workers=${WORKERS}"
echo "Pool: sku=$SKU loc=$LOC (forces single-shard fan_in)"
echo

for mode in "${MODES[@]}"; do
  for run in $(seq 1 "$RUNS"); do
    # Reset state — both the cost-table rows AND the per-shard shmem
    # (committer_pid, head/tail, slot pool). Resetting shmem is
    # load-bearing: a backend killed mid-drain leaves committer_pid
    # held, which would lock out future backends until the M5b
    # lease-timeout work ships. Doing it pre-run guarantees we start
    # from a known-clean shmem state per cell.
    $PSQL -c "TRUNCATE poc_test_rows, poc_pool_locks, poc_pool_lock_anchors, poc_cost_consumptions;" > /dev/null
    $PSQL -c "SELECT poc_ledger_shard_reset(s) FROM generate_series(0, 15) AS s;" > /dev/null
    rm -f "$TMPDIR"/stop "$TMPDIR"/w*.pid "$TMPDIR"/w*.count
    sync; sleep 1
    # Snapshot starting count (should be 0 after truncate).
    local_start=$($PSQL -c "SELECT count(*) FROM poc_test_rows;")
    # Spawn workers.
    t0=$(date +%s%N)
    for w in $(seq 1 "$WORKERS"); do
      worker_loop "$w" "$mode" &
    done
    sleep "$DURATION_S"
    touch "$TMPDIR/stop"
    wait
    t1=$(date +%s%N)
    elapsed_ms=$(( (t1 - t0) / 1000000 ))
    rows=$($PSQL -c "SELECT count(*) FROM poc_test_rows;")
    tps=$(awk "BEGIN { printf \"%.0f\", $rows / ($elapsed_ms / 1000.0) }")
    printf "  mode=%-20s run=%d  rows=%-6s elapsed=%dms  tps=%s\n" \
      "$mode" "$run" "$rows" "$elapsed_ms" "$tps"
    SAMPLES["$mode.$run"]="$tps"
    sleep 5  # cool-down between runs
  done
done

# Aggregate.
{
  echo "# M3.2 (acct-4d4n.8) Q-A pool-lock granularity — fan_in bench"
  echo
  echo "Workload: ${WORKERS} concurrent backends, single (sku=${SKU}, loc=${LOC}) pool, method='mock', duration ${DURATION_S}s per run, ${RUNS} runs per cell."
  echo
  echo "Throughput is rows-into-poc_test_rows divided by wall-clock elapsed (each apply produces one row)."
  echo
  echo "## Per-run throughput (transfers/s)"
  echo
  printf "| mode | "
  for run in $(seq 1 "$RUNS"); do printf "run %d | " "$run"; done
  printf "median |\n"
  printf '%s' "|---|"
  for run in $(seq 1 "$RUNS"); do printf '%s' "---|"; done
  printf '%s\n' "---|"
  for mode in "${MODES[@]}"; do
    printf "| %s | " "$mode"
    samples=()
    for run in $(seq 1 "$RUNS"); do
      v="${SAMPLES["$mode.$run"]:-NA}"
      samples+=("$v")
      printf "%s | " "$v"
    done
    # Sort numerically, take median.
    sorted=$(printf '%s\n' "${samples[@]}" | sort -n)
    n=${#samples[@]}
    mid=$(( n / 2 ))
    if (( n % 2 == 1 )); then
      median=$(echo "$sorted" | sed -n "$((mid + 1))p")
    else
      a=$(echo "$sorted" | sed -n "${mid}p")
      b=$(echo "$sorted" | sed -n "$((mid + 1))p")
      median=$(awk "BEGIN { printf \"%.0f\", ($a + $b) / 2 }")
    fi
    printf "**%s** |\n" "$median"
  done
  echo
  echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$RESULTS_FILE"

echo
echo "Wrote $RESULTS_FILE"
