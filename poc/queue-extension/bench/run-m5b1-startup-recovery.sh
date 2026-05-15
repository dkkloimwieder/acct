#!/usr/bin/env bash
# M5b.1 (acct-4d4n.12) startup recovery worker acceptance tests.
#
# Two scenarios per the bd issue + plan (option a — Phase A only;
# Phase B compensation emission deferred to M6.1):
#
#   1. counter seeding across restart: after a postmaster restart, the
#      per-shard committer_tx_seq counters must be seeded past every
#      committer_tx_id that survived in the durable cost tables. New
#      applies on the same (sku, loc) must observe strictly-greater
#      committer_tx_ids than any pre-restart row.
#
#   2. fresh-database no-op: on a postmaster restart against a fresh
#      DB with no cost rows, the worker logs phase A scanned 0 / phase
#      B scanned 0 and exits cleanly (no FATAL, no PG-wide failure).
#
# Args: none.
# Output: per-test PASS/FAIL on stdout, total wall time at end.
set -euo pipefail

DSN='postgres://acct:acct_dev@localhost:5111/acct_poc_queue'
PSQL="psql -At $DSN"

t_start=$(date +%s%N)
fail_count=0

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    echo "  PASS $label"
  else
    echo "  FAIL $label: expected '$expected' got '$actual'"
    fail_count=$((fail_count + 1))
  fi
}

assert_ge() {
  local label="$1" minimum="$2" actual="$3"
  if (( actual >= minimum )); then
    echo "  PASS $label (got $actual >= $minimum)"
  else
    echo "  FAIL $label: expected >= '$minimum' got '$actual'"
    fail_count=$((fail_count + 1))
  fi
}

wait_for_pg() {
  for _ in $(seq 1 30); do
    if $PSQL -c "SELECT 1" > /dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "FATAL: PG did not become ready within 15s"
  exit 1
}

# Sweep before any test setup.
$PSQL -c "
  SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
   WHERE datname='acct_poc_queue' AND pid <> pg_backend_pid() AND backend_type = 'client backend';
" > /dev/null 2>&1 || true
sleep 1

# ── Test 1: counter seeding across restart ────────────────────────────
echo "Test 1: counter seeding across postmaster restart"
$PSQL -c "TRUNCATE poc_test_rows, poc_pool_locks, poc_pool_lock_anchors, poc_cost_consumptions, poc_cost_depletions, poc_cost_layers, poc_cost_avg;" > /dev/null
$PSQL -c "SELECT poc_ledger_shard_reset(s) FROM generate_series(0, 15) AS s;" > /dev/null

# Seed a few applies. Use 5 distinct SKUs at loc=1 to spread across
# shards via splitmix64. The 'mock' method does a single consumption
# INSERT per event and gives deterministic per-call costs (130, 131,
# 132, ... — committer-tx-sequence-dependent).
for i in 1 2 3 4 5 6 7 8 9 10; do
  SKU=$(( 100 + i ))
  $PSQL -c "SELECT poc_ledger_apply($SKU, 1, 5, $((i * 1000 + 50001)), 'mock');" > /dev/null
done

# Capture per-shard committer_tx_seq state BEFORE restart. Also capture
# the max committer_tx_id we wrote in poc_cost_consumptions (across all
# pools — for sanity check).
pre_shards_csv=$($PSQL -c "SELECT shard_idx || ':' || committer_tx_seq FROM poc_ledger_shard_stats_all() WHERE committer_tx_seq > 0 ORDER BY shard_idx;" | tr '\n' ',' | sed 's/,$//')
pre_durable_max=$($PSQL -c "SELECT COALESCE(MAX(committer_tx_id), 0) FROM poc_cost_consumptions;")
echo "    pre-restart non-zero shards: $pre_shards_csv"
echo "    pre-restart max durable committer_tx_id: $pre_durable_max"

# Capture pre-restart per-shard map for after-restart comparison.
declare -A pre_shard_max
while IFS=':' read -r s v; do
  pre_shard_max[$s]=$v
done < <(echo "$pre_shards_csv" | tr ',' '\n' | grep -v '^$')

# Restart container.
echo "    docker restart acct-postgres ..."
docker restart acct-postgres > /dev/null
wait_for_pg
# Give the bgworker a moment to run (start_time=ConsistentState).
sleep 2

# Capture worker phase A log line. The worker writes to PG's log
# stream; docker logs aggregates them. Grep for the most recent.
phase_a_line=$(docker logs --tail 200 acct-postgres 2>&1 | grep "poc_ledger_startup_recovery: phase A" | tail -1)
phase_b_line=$(docker logs --tail 200 acct-postgres 2>&1 | grep "poc_ledger_startup_recovery: phase B" | tail -1)
echo "    $phase_a_line"
echo "    $phase_b_line"

# Each shard that had pre-restart activity should have its seq re-seeded
# to AT LEAST the pre-restart value (post-restart shmem starts at 0;
# without seeding the worker observed there would be 0 here).
post_shards_csv=$($PSQL -c "SELECT shard_idx || ':' || committer_tx_seq FROM poc_ledger_shard_stats_all() WHERE committer_tx_seq > 0 ORDER BY shard_idx;" | tr '\n' ',' | sed 's/,$//')
echo "    post-restart non-zero shards: $post_shards_csv"

# Decompose post-restart and assert each pre-restart-active shard has
# its counter >= pre value.
declare -A post_shard_max
while IFS=':' read -r s v; do
  post_shard_max[$s]=$v
done < <(echo "$post_shards_csv" | tr ',' '\n' | grep -v '^$')

for shard in "${!pre_shard_max[@]}"; do
  pre=${pre_shard_max[$shard]}
  post=${post_shard_max[$shard]:-0}
  assert_ge "shard $shard committer_tx_seq seeded post-restart" "$pre" "$post"
done

# Issue ONE more apply per pre-active shard; its committer_tx_id must
# be > the pre-restart shard max (no collision with a row that already
# has the same id in the durable table).
collision_count=0
for shard in "${!pre_shard_max[@]}"; do
  pre=${pre_shard_max[$shard]}
  # Find a (sku, loc) that hashes to this shard.
  matched_sku=""
  for sku in $(seq 200 300); do
    s=$($PSQL -c "SELECT poc_ledger_shard_for($sku, 1)")
    if [[ "$s" == "$shard" ]]; then
      matched_sku="$sku"
      break
    fi
  done
  if [[ -z "$matched_sku" ]]; then
    continue
  fi
  ISSUE_ID=$((99200 + shard))
  $PSQL -c "SELECT poc_ledger_apply($matched_sku, 1, 5, $ISSUE_ID, 'mock');" > /dev/null
  new_tx_id=$($PSQL -c "SELECT committer_tx_id FROM poc_cost_consumptions WHERE issue_id = $ISSUE_ID;")
  if (( new_tx_id <= pre )); then
    echo "  FAIL shard $shard: new apply got committer_tx_id=$new_tx_id <= pre-restart max=$pre (collision risk)"
    collision_count=$((collision_count + 1))
    fail_count=$((fail_count + 1))
  fi
done
if (( collision_count == 0 )); then
  echo "  PASS no committer_tx_id collisions on new applies post-restart"
fi

# ── Test 2: fresh-database no-op ──────────────────────────────────────
echo
echo "Test 2: fresh-database restart (worker exits cleanly with 0-scan)"
# TRUNCATE again + shard_reset to simulate fresh.
$PSQL -c "TRUNCATE poc_test_rows, poc_pool_locks, poc_pool_lock_anchors, poc_cost_consumptions, poc_cost_depletions, poc_cost_layers, poc_cost_avg;" > /dev/null
$PSQL -c "SELECT poc_ledger_shard_reset(s) FROM generate_series(0, 15) AS s;" > /dev/null

docker restart acct-postgres > /dev/null
wait_for_pg
sleep 2

phase_a_line=$(docker logs --tail 100 acct-postgres 2>&1 | grep "poc_ledger_startup_recovery: phase A" | tail -1)
phase_b_line=$(docker logs --tail 100 acct-postgres 2>&1 | grep "poc_ledger_startup_recovery: phase B" | tail -1)
echo "    $phase_a_line"
echo "    $phase_b_line"

if echo "$phase_a_line" | grep -q "scanned 0 pool rows"; then
  echo "  PASS phase A 0-scan log present"
else
  echo "  FAIL phase A did not show 0-scan log"
  fail_count=$((fail_count + 1))
fi
if echo "$phase_b_line" | grep -q "scanned 0 distinct"; then
  echo "  PASS phase B 0-scan log present"
else
  echo "  FAIL phase B did not show 0-scan log"
  fail_count=$((fail_count + 1))
fi

# Ensure the worker exited normally (exit code 1 from BackgroundWorker
# is the "ran to completion" convention per pgrx; not a FATAL).
# `|| true` keeps the substitution non-fatal under pipefail when grep
# finds no match (the expected case).
fatal_line=$(docker logs --tail 50 acct-postgres 2>&1 | grep -E "FATAL.*poc_ledger" | tail -1 || true)
if [[ -z "$fatal_line" ]]; then
  echo "  PASS no FATAL log lines for poc_ledger worker"
else
  echo "  FAIL FATAL line present: $fatal_line"
  fail_count=$((fail_count + 1))
fi

# All shards should be at committer_tx_seq=0 (nothing to seed).
zero_count=$($PSQL -c "SELECT COUNT(*) FROM poc_ledger_shard_stats_all() WHERE committer_tx_seq = 0;")
assert_eq "all 16 shards at committer_tx_seq=0 (fresh)" "16" "$zero_count"

# ── Wrap up ──────────────────────────────────────────────────────────
t_end=$(date +%s%N)
total_ms=$(( (t_end - t_start) / 1000000 ))
echo
echo "==> Total wall-clock: ${total_ms}ms"
if (( fail_count == 0 )); then
  echo "==> ALL TESTS PASSED"
  exit 0
else
  echo "==> ${fail_count} ASSERTION(S) FAILED"
  exit 1
fi
