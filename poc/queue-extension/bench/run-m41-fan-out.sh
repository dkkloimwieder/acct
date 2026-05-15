#!/usr/bin/env bash
# M4.1 (acct-4d4n.9) fan_out bench.
#
# N concurrent backends, each rotating through G SKUs, all distinct
# (sku, location) pairs hashing across multiple shards. Measures
# aggregate throughput as N varies, samples poc_ledger_shard_stats_all
# mid-run to capture parallelism evidence.
#
# Args: DURATION_S (default 15)  RUNS (default 3)  WORKERS_LIST (default "1 4 8 16")  SKUS_PER_WORKER (default 4)
# Output: results-m41-multi-shard.md
set -euo pipefail

DSN='postgres://acct:acct_dev@localhost:5111/acct_poc_queue'
PSQL="psql -At $DSN"
DURATION_S=${DURATION_S:-15}
RUNS=${RUNS:-3}
WORKERS_LIST=${WORKERS_LIST:-"1 4 8 16"}
SKUS_PER_WORKER=${SKUS_PER_WORKER:-4}
LOC=1
RESULTS_FILE="$(dirname "$0")/results-m41-multi-shard.md"
TMPDIR="${TMPDIR:-/tmp}/m41-$$"
mkdir -p "$TMPDIR"
trap 'rm -rf "$TMPDIR"' EXIT

# Per-worker bash function: loops applies until $TMPDIR/stop appears.
# Worker rotates through SKUS_PER_WORKER distinct SKUs to spread load
# across multiple shards (the hash routes deterministically; with N>=4
# workers × SKUS_PER_WORKER=4 we get >=16 distinct SKUs hashing across
# the 16 shards).
#
# Each apply wrapped in explicit BEGIN/COMMIT — psql -c multi-stmt
# would otherwise run all in ONE implicit txn, holding state across
# wrapper boundaries (per feedback_psql_c_single_transaction).
worker_loop() {
  local wid="$1"
  local n=0
  local inner_batch=10
  while [[ ! -f "$TMPDIR/stop" ]]; do
    local sql=""
    for j in $(seq 1 $inner_batch); do
      # SKU per worker varies: (wid base) + (n + j) mod SKUS_PER_WORKER.
      local sku_off=$(( (n + j) % SKUS_PER_WORKER ))
      local sku=$(( wid * 1000 + sku_off ))
      sql+="BEGIN; SELECT 1 FROM poc_ledger_apply($sku, $LOC, 1, $((wid * 10000000 + n * 100 + j)), 'mock'); COMMIT; "
    done
    $PSQL -c "$sql" > /dev/null 2>&1 || true
    n=$((n + inner_batch))
  done
  echo "$n" > "$TMPDIR/w${wid}.count"
}

# Live committer sampler: fires every 50ms during the run; writes one
# row per snapshot with the count of shards with committer_pid != 0
# (a snapshot of simultaneous parallelism).
sampler_loop() {
  local cell_tag="$1"
  while [[ ! -f "$TMPDIR/stop" ]]; do
    local active_committers
    active_committers=$($PSQL -c "
      SELECT count(*) FROM poc_ledger_shard_stats_all() WHERE committer_pid != 0;
    " 2>/dev/null || echo 0)
    echo "${cell_tag},${active_committers}" >> "$TMPDIR/samples.csv"
    sleep 0.05
  done
}

declare -A TPS_RESULTS
declare -A ACTIVE_SHARDS

echo "M4.1 fan_out bench: D=${DURATION_S}s, runs=${RUNS}, workers=${WORKERS_LIST}, skus_per_worker=${SKUS_PER_WORKER}"
echo "DSN: $DSN"
echo

for n_workers in $WORKERS_LIST; do
  for run in $(seq 1 "$RUNS"); do
    # Sweep + reset every cell. Stale committer_pid from a killed
    # bench would otherwise lock out new backends until M5b ships.
    $PSQL -c "
      SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
       WHERE datname='acct_poc_queue' AND pid <> pg_backend_pid();
    " > /dev/null 2>&1 || true
    sleep 1
    $PSQL -c "TRUNCATE poc_test_rows, poc_pool_locks, poc_pool_lock_anchors, poc_cost_consumptions;" > /dev/null
    $PSQL -c "SELECT poc_ledger_shard_reset(s) FROM generate_series(0, 15) AS s;" > /dev/null
    rm -f "$TMPDIR"/stop "$TMPDIR"/w*.count
    sync; sleep 1

    # Spawn workers + sampler in background.
    t0=$(date +%s%N)
    for w in $(seq 1 "$n_workers"); do
      worker_loop "$w" &
    done
    sampler_loop "n${n_workers}_run${run}" &
    SAMPLER_PID=$!

    sleep "$DURATION_S"
    touch "$TMPDIR/stop"
    wait
    t1=$(date +%s%N)

    elapsed_ms=$(( (t1 - t0) / 1000000 ))
    rows=$($PSQL -c "SELECT count(*) FROM poc_test_rows;")
    tps=$(awk "BEGIN { printf \"%.0f\", $rows / ($elapsed_ms / 1000.0) }")

    # Active shards = shards with committer_tx_seq > 0 at end-of-run.
    active=$($PSQL -c "SELECT count(*) FROM poc_ledger_shard_stats_all() WHERE committer_tx_seq > 0;")

    printf "  n_workers=%-3d run=%d  rows=%-7s elapsed=%dms  tps=%s  active_shards=%s\n" \
      "$n_workers" "$run" "$rows" "$elapsed_ms" "$tps" "$active"

    TPS_RESULTS["$n_workers.$run"]="$tps"
    ACTIVE_SHARDS["$n_workers.$run"]="$active"

    sleep 3
  done
done

# Aggregate.
{
  echo "# M4.1 (acct-4d4n.9) multi-shard hash routing — fan_out bench"
  echo
  echo "Workload: N concurrent backends, each rotating through ${SKUS_PER_WORKER} SKUs at loc=${LOC}; SKU keys spread across 16 shards via splitmix64 hash; method='mock' (bypasses cost-method snapshot SPI to isolate the queue + committer); duration ${DURATION_S}s per run, ${RUNS} runs per cell."
  echo
  echo "Throughput is rows-into-poc_test_rows divided by wall-clock elapsed."
  echo "Active shards = shards with committer_tx_seq > 0 at end of run."
  echo
  echo "## Per-cell throughput (transfers/s) and active-shard count"
  echo
  printf "| n_workers |"
  for run in $(seq 1 "$RUNS"); do printf " run %d tps |" "$run"; done
  printf " median tps | active_shards (median) |\n"
  printf '|---|'
  for run in $(seq 1 "$RUNS"); do printf '%s' '---|'; done
  printf '%s\n' '---|---|'

  for n_workers in $WORKERS_LIST; do
    printf "| %d |" "$n_workers"
    tps_samples=()
    act_samples=()
    for run in $(seq 1 "$RUNS"); do
      v="${TPS_RESULTS["$n_workers.$run"]:-NA}"
      tps_samples+=("$v")
      printf " %s |" "$v"
      act_samples+=("${ACTIVE_SHARDS["$n_workers.$run"]:-NA}")
    done

    sorted=$(printf '%s\n' "${tps_samples[@]}" | sort -n)
    n=${#tps_samples[@]}
    mid=$(( n / 2 ))
    if (( n % 2 == 1 )); then
      tps_med=$(echo "$sorted" | sed -n "$((mid + 1))p")
    else
      a=$(echo "$sorted" | sed -n "${mid}p")
      b=$(echo "$sorted" | sed -n "$((mid + 1))p")
      tps_med=$(awk "BEGIN { printf \"%.0f\", ($a + $b) / 2 }")
    fi

    asorted=$(printf '%s\n' "${act_samples[@]}" | sort -n)
    amid=$(( n / 2 ))
    if (( n % 2 == 1 )); then
      act_med=$(echo "$asorted" | sed -n "$((amid + 1))p")
    else
      a=$(echo "$asorted" | sed -n "${amid}p")
      b=$(echo "$asorted" | sed -n "$((amid + 1))p")
      act_med=$(awk "BEGIN { printf \"%.0f\", ($a + $b) / 2 }")
    fi

    printf " **%s** | %s |\n" "$tps_med" "$act_med"
  done

  echo
  echo "## Parallelism evidence (active-committer snapshots)"
  echo
  echo "Sampled at 50ms intervals during each run; each sample counts shards with \`committer_pid != 0\` at that instant. Per-cell: mean + max simultaneous committers observed."
  echo
  if [[ -s "$TMPDIR/samples.csv" ]]; then
    echo "| cell | samples | mean active committers | max active committers |"
    echo "|---|---|---|---|"
    awk -F, '
      {
        cell = $1; n[cell]++; sum[cell] += $2;
        if ($2 > max[cell]) max[cell] = $2;
      }
      END {
        for (c in n) {
          printf "| %s | %d | %.2f | %d |\n", c, n[c], sum[c]/n[c], max[c];
        }
      }
    ' "$TMPDIR/samples.csv" | sort
  else
    echo "(no samples captured)"
  fi

  echo
  echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$RESULTS_FILE"

echo
echo "Wrote $RESULTS_FILE"
