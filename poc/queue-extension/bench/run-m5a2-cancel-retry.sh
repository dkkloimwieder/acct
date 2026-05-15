#!/usr/bin/env bash
# M5a.2 (acct-4d4n.11) waiter cancel + dedup-replay acceptance tests.
#
# Three scenarios per the bd issue:
#   1. cancel mid-wait: a backend pushes a request and blocks in
#      WaitLatch behind a non-progressing committer (we inject the
#      background writer's PID into committer_pid so pg_pid_alive=true
#      keeps the waiter parked indefinitely). A second session cancels
#      the waiter via pg_cancel_backend. The cancel cleanup must mark
#      the slot ABANDONED before ProcessInterrupts unwinds.
#   2. retry returns cached: two sequential apply calls with the same
#      issue_id. The second call hits dedup-lookup at process_group
#      and returns the cached aggregate without writing a duplicate
#      cost row.
#   3. cancel-then-retry sequential: cancel-mid-wait leaves a stale
#      ring entry; a manual drain converts that entry to a durable
#      cost row (the spec's "committer still INSERTs" leg); retry of
#      the same issue_id then hits dedup-lookup and returns the cached
#      result without writing a duplicate row.
#
# (Note: concurrent same-issue_id pushes from multiple backends will
# land in a single batch and hit M2.1's "same-issue-id-within-batch
# dedup gap" — an INSERT collision on the UNIQUE constraint. That is
# out of M5a.2 scope and tracked separately.)
#
# Args: none.
# Output: per-test PASS/FAIL on stdout, total wall time at end.
set -euo pipefail

DSN='postgres://acct:acct_dev@localhost:5111/acct_poc_queue'
PSQL="psql -At $DSN"
SHARD=0

t_start=$(date +%s%N)
fail_count=0

# Sweep orphan shells before any test setup.
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

bg_writer_pid() {
  $PSQL -c "SELECT pid FROM pg_stat_activity WHERE backend_type = 'background writer' LIMIT 1;"
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

# ── Test 1: cancel mid-wait ──────────────────────────────────────────
echo "Test 1: cancel mid-wait → slot ABANDONED, no durable row"
reset_state
SKU=$(pick_sku_for_shard "$SHARD")
ISSUE_ID=99101

# Inject a LIVE committer (bg writer) so the waiter parks behind it.
# pg_pid_alive=true → try_acquire_or_takeover returns Held → WaitLatch.
LIVE_PID=$(bg_writer_pid)
$PSQL -c "SELECT poc_ledger_inject_dead_committer($SHARD, $LIVE_PID, 0);" > /dev/null

# Spawn waiter session in background. We need its pg_backend_pid so we
# can cancel it; psql exits per-statement so a single bash subshell
# can't expose the pid to us mid-call. Use a heredoc that records the
# pid into a temp file before the apply call blocks.
PID_FILE=$(mktemp)
WAITER_OUT=$(mktemp)
(
  $PSQL <<SQL > "$WAITER_OUT" 2>&1
\\copy (SELECT pg_backend_pid()) TO PROGRAM 'cat > $PID_FILE'
SELECT * FROM poc_ledger_apply($SKU, 1, 5, $ISSUE_ID, 'mock');
SQL
) &
WAITER_BASH_PID=$!

# Give the waiter time to push its request and enter WaitLatch.
sleep 1

if [[ ! -s "$PID_FILE" ]]; then
  echo "  FAIL waiter never wrote its pg_backend_pid"
  fail_count=$((fail_count + 1))
  kill "$WAITER_BASH_PID" 2>/dev/null || true
  rm -f "$PID_FILE" "$WAITER_OUT"
else
  WAITER_PG_PID=$(cat "$PID_FILE")
  echo "    waiter pg_backend_pid=$WAITER_PG_PID  injected committer=$LIVE_PID"

  # Confirm waiter is parked in WaitLatch on Extension wait_event.
  wait_event=$($PSQL -c "SELECT COALESCE(wait_event_type || ':' || wait_event, 'null') FROM pg_stat_activity WHERE pid = $WAITER_PG_PID;")
  echo "    waiter wait_event: $wait_event"

  # Slot should be SLOT_ALLOCATED (1) at this point.
  state_during=$($PSQL -c "SELECT poc_ledger_slot_state($SHARD, 0)")
  assert_eq "slot state mid-wait is SLOT_ALLOCATED (1)" "1" "$state_during"

  # Cancel the waiter.
  t0=$(date +%s%N)
  $PSQL -c "SELECT pg_cancel_backend($WAITER_PG_PID);" > /dev/null
  wait "$WAITER_BASH_PID" 2>/dev/null || true
  t1=$(date +%s%N)
  elapsed_ms=$(( (t1 - t0) / 1000000 ))
  echo "    cancel → waiter exit took ${elapsed_ms}ms"

  # Waiter session should have errored with QUERY_CANCELED.
  if grep -qE 'cancel|57014' "$WAITER_OUT"; then
    echo "  PASS waiter errored with cancellation"
  else
    echo "  FAIL waiter did not error as expected; output:"
    sed 's/^/      /' "$WAITER_OUT"
    fail_count=$((fail_count + 1))
  fi

  # Slot must be SLOT_ABANDONED (3) — the M5a.2 cleanup before
  # ProcessInterrupts.
  state_after=$($PSQL -c "SELECT poc_ledger_slot_state($SHARD, 0)")
  assert_eq "slot state post-cancel is SLOT_ABANDONED (3)" "3" "$state_after"

  # No durable cost row should exist (committer never drained).
  rows_cons=$($PSQL -c "SELECT count(*) FROM poc_cost_consumptions WHERE issue_id = $ISSUE_ID;")
  assert_eq "poc_cost_consumptions empty for issue $ISSUE_ID" "0" "$rows_cons"

  # Committer_pid stays at the injected bg-writer (NOT our pid, so
  # M5a.2 cleanup did not release it). Confirms the conditional
  # release_committer branch.
  committer_after=$($PSQL -c "SELECT committer_pid FROM poc_ledger_shard_stats_all() WHERE shard_idx = $SHARD;")
  assert_eq "committer_pid untouched (still bg writer)" "$LIVE_PID" "$committer_after"

  rm -f "$PID_FILE" "$WAITER_OUT"
fi

# ── Test 2: retry returns cached ─────────────────────────────────────
echo
echo "Test 2: retry returns cached (dedup-lookup, 1 durable row)"
reset_state
SKU=$(pick_sku_for_shard "$SHARD")
ISSUE_ID=99102

# Pre-seed an avg state so 'mock' returns deterministic numbers via
# its 100 cost unit constant (MockMethod is pure; first call writes,
# second call hits dedup).
r1=$($PSQL -F'|' -c "SELECT applied_unit_cost, applied_total_cost FROM poc_ledger_apply($SKU, 1, 5, $ISSUE_ID, 'mock');")
r2=$($PSQL -F'|' -c "SELECT applied_unit_cost, applied_total_cost FROM poc_ledger_apply($SKU, 1, 5, $ISSUE_ID, 'mock');")
echo "    r1 = $r1"
echo "    r2 = $r2"
assert_eq "r1 == r2 (cached replay)" "$r1" "$r2"

# Exactly one durable row.
rows_cons=$($PSQL -c "SELECT count(*) FROM poc_cost_consumptions WHERE issue_id = $ISSUE_ID;")
assert_eq "exactly 1 poc_cost_consumptions row for issue $ISSUE_ID" "1" "$rows_cons"

# Sanity: the dedup-lookup also returns the same cost row contents.
durable_unit=$($PSQL -c "SELECT applied_unit_cost FROM poc_cost_consumptions WHERE issue_id = $ISSUE_ID;")
r1_unit=$(echo "$r1" | cut -d'|' -f1)
assert_eq "durable unit matches r1 unit" "$r1_unit" "$durable_unit"

# ── Test 3: cancel-then-retry sequential ─────────────────────────────
echo
echo "Test 3: cancel mid-wait → manual drain → retry hits dedup-lookup"
reset_state
SKU=$(pick_sku_for_shard "$SHARD")
ISSUE_ID=99103

# Step a: park a waiter behind a fake live committer (same as Test 1).
LIVE_PID=$(bg_writer_pid)
$PSQL -c "SELECT poc_ledger_inject_dead_committer($SHARD, $LIVE_PID, 0);" > /dev/null

PID_FILE=$(mktemp)
WAITER_OUT=$(mktemp)
(
  $PSQL <<SQL > "$WAITER_OUT" 2>&1
\\copy (SELECT pg_backend_pid()) TO PROGRAM 'cat > $PID_FILE'
SELECT * FROM poc_ledger_apply($SKU, 1, 5, $ISSUE_ID, 'mock');
SQL
) &
WAITER_BASH_PID=$!
sleep 1

if [[ ! -s "$PID_FILE" ]]; then
  echo "  FAIL waiter never wrote its pg_backend_pid"
  fail_count=$((fail_count + 1))
  kill "$WAITER_BASH_PID" 2>/dev/null || true
else
  WAITER_PG_PID=$(cat "$PID_FILE")

  # Step b: cancel waiter.
  $PSQL -c "SELECT pg_cancel_backend($WAITER_PG_PID);" > /dev/null
  wait "$WAITER_BASH_PID" 2>/dev/null || true

  # Slot abandoned; ring still holds the stale entry; no durable row yet.
  rows_pre=$($PSQL -c "SELECT count(*) FROM poc_cost_consumptions WHERE issue_id = $ISSUE_ID;")
  assert_eq "no durable row before manual drain" "0" "$rows_pre"

  # Step c: clear fake committer + run one manual tick. The committer
  # drains the stale ring entry, INSERTs the durable row, fails to fill
  # the (already-abandoned) slot — exactly the spec'd "committer still
  # INSERTs durable row" leg of §3.3.
  $PSQL -c "SELECT poc_ledger_inject_dead_committer($SHARD, 0, 0);" > /dev/null
  drained=$($PSQL -c "SELECT poc_ledger_committer_tick($SHARD, 1024);" 2>&1 | tr -d ' \n')
  echo "    manual tick drained: $drained"

  rows_after_tick=$($PSQL -c "SELECT count(*) FROM poc_cost_consumptions WHERE issue_id = $ISSUE_ID;")
  assert_eq "durable row landed after manual drain" "1" "$rows_after_tick"

  # Step d: retry with same issue_id. Should hit dedup-lookup and
  # return the cached result without writing a duplicate row.
  r_retry=$($PSQL -F'|' -c "SELECT applied_unit_cost, applied_total_cost FROM poc_ledger_apply($SKU, 1, 5, $ISSUE_ID, 'mock');")
  echo "    retry result = $r_retry"

  rows_after_retry=$($PSQL -c "SELECT count(*) FROM poc_cost_consumptions WHERE issue_id = $ISSUE_ID;")
  assert_eq "still exactly 1 durable row after retry (dedup hit)" "1" "$rows_after_retry"

  # Durable row stores applied_unit_cost; total is qty × unit derived
  # at apply-time. Verify both via that relationship.
  durable_unit_qty=$($PSQL -F'|' -c "SELECT applied_unit_cost, qty FROM poc_cost_consumptions WHERE issue_id = $ISSUE_ID;")
  durable_unit=$(echo "$durable_unit_qty" | cut -d'|' -f1)
  durable_qty=$(echo "$durable_unit_qty" | cut -d'|' -f2)
  retry_unit=$(echo "$r_retry" | cut -d'|' -f1)
  retry_total=$(echo "$r_retry" | cut -d'|' -f2)
  expected_total=$(( durable_unit * durable_qty ))
  assert_eq "retry unit matches durable row" "$durable_unit" "$retry_unit"
  assert_eq "retry total = durable_unit × durable_qty" "$expected_total" "$retry_total"

  # Quiescence: no SLOT_ALLOCATED leaked anywhere.
  alloc_total=$($PSQL -c "
    SELECT COUNT(*) FROM (
      SELECT poc_ledger_slot_state(s, i) AS st
      FROM generate_series(0,15) s,
           generate_series(0,15) i
    ) sub WHERE st = 1;")
  assert_eq "no SLOT_ALLOCATED leaked (quiescence)" "0" "$alloc_total"

  rm -f "$PID_FILE" "$WAITER_OUT"
fi

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
