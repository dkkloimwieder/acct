#!/usr/bin/env bash
# M5a.1 (acct-4d4n.10) orphan recovery acceptance tests.
#
# Two scenarios per the bd issue:
#   1. pre_insert death: committer drained but died before INSERT.
#      Recovery should mark slot ABANDONED; cost tables stay empty.
#   2. post_insert death: committer INSERTed but died before slot fill.
#      Recovery should backfill slot from the cost rows.
#
# Test mechanic: inject a known-dead PID into shmem via the
# poc_ledger_inject_dead_committer test surface. The PID is from a
# `true` subprocess that we wait on, so kill(pid, 0) returns ESRCH.
# This is equivalent to a committer that crashed mid-routine — the
# orphan recovery code path is exercised identically.
#
# Args: none.
# Output: per-test PASS/FAIL on stdout, total wall time at end.
set -euo pipefail

DSN='postgres://acct:acct_dev@localhost:5111/acct_poc_queue'
PSQL="psql -At $DSN"
SHARD=0

t_start=$(date +%s%N)
fail_count=0

# Sweep before any test setup (orphan shells protocol).
$PSQL -c "
  SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
   WHERE datname='acct_poc_queue' AND pid <> pg_backend_pid() AND backend_type = 'client backend';
" > /dev/null 2>&1 || true
sleep 1

reset_state() {
  $PSQL -c "TRUNCATE poc_cost_compensation_depletions, poc_cost_compensation_consumptions, poc_test_rows, poc_pool_locks, poc_pool_lock_anchors, poc_cost_consumptions, poc_cost_depletions, poc_cost_layers, poc_cost_avg;" > /dev/null
  $PSQL -c "SELECT poc_ledger_shard_reset(s) FROM generate_series(0, 15) AS s;" > /dev/null
}

# Spawn a `true` subprocess, capture PID, wait for it to exit. Resulting
# PID is now guaranteed dead — pg_pid_alive will return false. PG runs
# in a Docker container so we need a PID from the CONTAINER, not the
# host. Use docker exec.
make_dead_pid() {
  local pid
  pid=$(docker exec acct-postgres bash -c 'true & echo $! ; wait' 2>/dev/null)
  echo "$pid"
}

# Resolve a (sku, loc) → shard so we know where to inject. Pick a
# (sku, loc) that hashes to shard 0 for predictability.
pick_sku_for_shard() {
  local target="$1"
  for sku in $(seq 100 200); do
    local s
    s=$($PSQL -c "SELECT poc_ledger_shard_for($sku, 1)")
    if [[ "$s" == "$target" ]]; then
      echo "$sku"
      return
    fi
  done
  echo "ERROR: no sku found that hashes to shard $target" >&2
  return 1
}

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    echo "  PASS $label"
  else
    echo "  FAIL $label: expected '$expected' got '$actual'"
    fail_count=$((fail_count + 1))
  fi
}

# ── Test 1: pre_insert death ─────────────────────────────────────────
echo "Test 1: pre_insert death (cost tables empty → slot ABANDONED)"
reset_state
SKU=$(pick_sku_for_shard "$SHARD")
ISSUE_ID=99001
# Push a request without electing the committer; slot stamped with issue_id.
$PSQL -c "SELECT * FROM poc_ledger_push_only($SKU, 1, 5, $ISSUE_ID, 'mock');" > /dev/null
# Capture slot state — should be 1 (SLOT_ALLOCATED).
state_before=$($PSQL -c "SELECT poc_ledger_slot_state($SHARD, 0)")
assert_eq "slot state pre-recovery is SLOT_ALLOCATED (1)" "1" "$state_before"

# Inject dead committer.
DEAD_PID=$(make_dead_pid)
$PSQL -c "SELECT poc_ledger_inject_dead_committer($SHARD, $DEAD_PID, 0);" > /dev/null

# Run recovery from a different backend.
t0=$(date +%s%N)
outcome_row=$($PSQL -F'|' -c "SELECT outcome, stale_pid, recovered, abandoned FROM poc_ledger_orphan_recovery_tick($SHARD);")
t1=$(date +%s%N)
elapsed_ms=$(( (t1 - t0) / 1000000 ))
echo "    recovery took ${elapsed_ms}ms (target <= 2 * committer_lease_ms = 200ms)"

# Parse outcome.
outcome=$(echo "$outcome_row" | cut -d'|' -f1)
stale_pid=$(echo "$outcome_row" | cut -d'|' -f2)
recovered=$(echo "$outcome_row" | cut -d'|' -f3)
abandoned=$(echo "$outcome_row" | cut -d'|' -f4)

assert_eq "outcome=took_over" "took_over" "$outcome"
assert_eq "stale_pid matches injected" "$DEAD_PID" "$stale_pid"
assert_eq "recovered=0 (no cost rows)" "0" "$recovered"
assert_eq "abandoned=1 (slot marked abandoned)" "1" "$abandoned"

# Slot state should now be SLOT_ABANDONED (3).
state_after=$($PSQL -c "SELECT poc_ledger_slot_state($SHARD, 0)")
assert_eq "slot state post-recovery is SLOT_ABANDONED (3)" "3" "$state_after"

# Cost tables should be empty.
rows_cons=$($PSQL -c "SELECT count(*) FROM poc_cost_consumptions WHERE issue_id = $ISSUE_ID;")
rows_dep=$($PSQL -c "SELECT count(*) FROM poc_cost_depletions WHERE issue_id = $ISSUE_ID;")
assert_eq "poc_cost_consumptions empty for issue $ISSUE_ID" "0" "$rows_cons"
assert_eq "poc_cost_depletions empty for issue $ISSUE_ID" "0" "$rows_dep"

# ── Test 2: post_insert death ────────────────────────────────────────
echo
echo "Test 2: post_insert death (cost rows present → slot BACKFILLED)"
reset_state
SKU=$(pick_sku_for_shard "$SHARD")
ISSUE_ID=99002
EXPECTED_UNIT=42
EXPECTED_QTY=7
EXPECTED_TOTAL=$(( EXPECTED_UNIT * EXPECTED_QTY ))

# Push a request without electing the committer.
$PSQL -c "SELECT * FROM poc_ledger_push_only($SKU, 1, $EXPECTED_QTY, $ISSUE_ID, 'mock');" > /dev/null
state_before=$($PSQL -c "SELECT poc_ledger_slot_state($SHARD, 0)")
assert_eq "slot state pre-recovery is SLOT_ALLOCATED (1)" "1" "$state_before"

# Simulate "committer INSERTed before death" by manually INSERTing a
# cost row keyed on the same issue_id. This is what a committer's
# Spi::connect_mut sub-tx would have committed before death.
$PSQL -c "
  INSERT INTO poc_cost_consumptions
    (sku_id, location_id, qty, applied_unit_cost, consumed_at,
     consumed_seq, issue_id, method_used, committer_tx_id, user_tx_xid)
  VALUES ($SKU, 1, $EXPECTED_QTY, $EXPECTED_UNIT, clock_timestamp(),
          0, $ISSUE_ID, 'mock', 9999, '0'::xid8);
" > /dev/null

# Inject dead committer.
DEAD_PID=$(make_dead_pid)
$PSQL -c "SELECT poc_ledger_inject_dead_committer($SHARD, $DEAD_PID, 0);" > /dev/null

# Run recovery.
t0=$(date +%s%N)
outcome_row=$($PSQL -F'|' -c "SELECT outcome, stale_pid, recovered, abandoned FROM poc_ledger_orphan_recovery_tick($SHARD);")
t1=$(date +%s%N)
elapsed_ms=$(( (t1 - t0) / 1000000 ))
echo "    recovery took ${elapsed_ms}ms (target <= 2 * committer_lease_ms = 200ms)"

outcome=$(echo "$outcome_row" | cut -d'|' -f1)
stale_pid=$(echo "$outcome_row" | cut -d'|' -f2)
recovered=$(echo "$outcome_row" | cut -d'|' -f3)
abandoned=$(echo "$outcome_row" | cut -d'|' -f4)

assert_eq "outcome=took_over" "took_over" "$outcome"
assert_eq "stale_pid matches injected" "$DEAD_PID" "$stale_pid"
assert_eq "recovered=1 (slot backfilled)" "1" "$recovered"
assert_eq "abandoned=0" "0" "$abandoned"

state_after=$($PSQL -c "SELECT poc_ledger_slot_state($SHARD, 0)")
assert_eq "slot state post-recovery is SLOT_FILLED (2)" "2" "$state_after"

# Read the backfilled slot result. Aggregated unit/total should match.
result=$($PSQL -F'|' -c "SELECT applied_unit_cost, applied_total_cost FROM poc_ledger_slot_result($SHARD, 0);")
result_unit=$(echo "$result" | cut -d'|' -f1)
result_total=$(echo "$result" | cut -d'|' -f2)
assert_eq "backfilled applied_unit_cost" "$EXPECTED_UNIT" "$result_unit"
assert_eq "backfilled applied_total_cost" "$EXPECTED_TOTAL" "$result_total"

# ── Test 3: live committer → no takeover ─────────────────────────────
echo
echo "Test 3: live committer → outcome=held (no takeover)"
reset_state
# Inject a LIVE pid. Use a stable PG auxiliary process (background
# writer) that exists for the lifetime of the cluster — using the
# caller's own pg_backend_pid() would be racy because each `psql -c`
# call is a fresh session that exits before the next call observes it.
LIVE_PID=$($PSQL -c "SELECT pid FROM pg_stat_activity WHERE backend_type = 'background writer' LIMIT 1;")
$PSQL -c "SELECT poc_ledger_inject_dead_committer($SHARD, $LIVE_PID, 0);" > /dev/null
# Different backend tries to take over.
outcome=$($PSQL -c "SELECT outcome FROM poc_ledger_orphan_recovery_tick($SHARD);")
assert_eq "outcome=held (live pid)" "held" "$outcome"

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
