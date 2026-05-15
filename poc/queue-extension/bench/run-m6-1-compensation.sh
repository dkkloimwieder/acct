#!/usr/bin/env bash
# M6.1 (acct-4d4n.16) compensation semantics acceptance.
#
# Tests the XactCallback(ABORT) → Compensate-ring-enqueue → committer
# drain → poc_cost_compensation_* rows path. Three scenarios:
#
#   T1 commit-no-compensate: apply happens inside an auto-commit
#       SELECT; the durable cost row stays; the XactCallback fires on
#       COMMIT and clears the dirty xid without enqueueing. Assertion:
#       zero compensation rows.
#
#   T2 abort-AVG: caller pushes the apply onto the ring inside a BEGIN
#       block, sleeps long enough for a separate session to drain the
#       ring (the cost row gets stamped with caller's xid and persisted
#       in the drainer's own tx), then ROLLBACKs. The XactCallback
#       enqueues a Compensate entry. Drainer ticks again, processing
#       the compensate → one reversal row in
#       poc_cost_compensation_consumptions.
#
#   T3 abort-FIFO-N-layer: pre-stage 5 layers via poc_ledger_receive,
#       caller pushes a single FIFO issue qty=5 (walks all 5 layers),
#       drainer writes 5 depletion rows, caller ROLLBACKs, drainer
#       processes compensate → 5 rows in poc_cost_compensation_depletions.
#       This is the cardinality test: N layers → N compensation rows.
#
# Coordination across sessions: psql session A runs BEGIN ... pg_sleep
#   ... ROLLBACK in the background; the main script `sleep`s briefly
#   to let A reach pg_sleep, then one-shots `poc_ledger_committer_tick`
#   on every shard from a fresh session B. After A's wait expires and
#   the ROLLBACK fires (the XactCallback runs there), the script ticks
#   shards a second time to drain the Compensate entry. The window is
#   wide enough that timing flakes shouldn't surface; if they do, bump
#   $A_SLEEP_S.
#
# Args: none.
# Output: per-assertion PASS/FAIL on stdout; total wall time at end.
set -euo pipefail

DSN='postgres://acct:acct_dev@localhost:5111/acct_poc_queue'
PSQL="psql -At $DSN"
SKU=601
LOC=1
A_SLEEP_S=3

t_start=$(date +%s%N)
fail_count=0

# Sweep before any test setup. Filter to client backends so the
# slot_audit / startup_recovery bgworkers stay alive (SIGTERM = clean
# exit, no relaunch).
$PSQL -c "
  SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
   WHERE datname='acct_poc_queue'
     AND pid <> pg_backend_pid()
     AND backend_type = 'client backend';
" > /dev/null 2>&1 || true
sleep 0.5

reset_state() {
  $PSQL -c "TRUNCATE poc_cost_compensation_depletions, poc_cost_compensation_consumptions, poc_test_rows, poc_pool_locks, poc_pool_lock_anchors, poc_cost_depletions, poc_cost_consumptions, poc_cost_layers, poc_cost_avg RESTART IDENTITY CASCADE;" > /dev/null
  $PSQL -c "SELECT poc_ledger_shard_reset(s) FROM generate_series(0, 15) AS s;" > /dev/null
}

tick_all() {
  $PSQL -c "SELECT s, poc_ledger_committer_tick(s) FROM generate_series(0, 15) AS s WHERE poc_ledger_committer_tick(s) > 0;" > /dev/null 2>&1 || true
}

# ── Test 1: commit-no-compensate ─────────────────────────────────────
echo "Test 1: COMMIT path produces no compensation rows"
reset_state

# Seed AVG pool with a receipt so the consume has something to draw on.
$PSQL -c "SELECT poc_ledger_receive_avg($SKU, $LOC, 10, 100);" > /dev/null

# Auto-commit consume — apply does push + drain inline in caller's tx.
# Caller's tx COMMITs (psql -c auto-commits), XactCallback fires on
# COMMIT and clears the dirty xid without enqueueing compensate.
$PSQL -c "SELECT poc_ledger_apply($SKU, $LOC, 1, 80001, 'avg');" > /dev/null

# Belt-and-braces: tick all shards to drain any phantom compensate.
tick_all

# Assertion 1a: 1 consumption row landed (the COMMIT happened).
cons_count=$($PSQL -c "SELECT COUNT(*) FROM poc_cost_consumptions WHERE issue_id = 80001;")
if [[ "$cons_count" == "1" ]]; then
  echo "  PASS 1 consumption row from successful COMMIT"
else
  echo "  FAIL expected 1 consumption row, got $cons_count"
  fail_count=$((fail_count + 1))
fi

# Assertion 1b: 0 compensation rows.
comp_count=$($PSQL -c "SELECT COUNT(*) FROM poc_cost_compensation_consumptions;")
if [[ "$comp_count" == "0" ]]; then
  echo "  PASS 0 compensation rows after COMMIT"
else
  echo "  FAIL expected 0 compensation rows, got $comp_count"
  fail_count=$((fail_count + 1))
fi

# ── Test 2: abort AVG → 1 compensation_consumption row ───────────────
echo
echo "Test 2: ABORT after push+drain in separate sessions → 1 comp_consumption"
reset_state

# Seed AVG pool.
$PSQL -c "SELECT poc_ledger_receive_avg($SKU, $LOC, 100, 100);" > /dev/null

# Capture A's xid as the test runs. The caller's tx is a BEGIN /
# push_only / pg_sleep / ROLLBACK block; we log txid_current() before
# the sleep so the main script can grep it back.
(
  $PSQL <<EOF
\\set ON_ERROR_STOP 1
\\timing off
BEGIN;
SELECT 'XID:' || pg_current_xact_id()::TEXT AS marker;
SELECT poc_ledger_push_only($SKU, $LOC, 2, 80002, 'avg');
SELECT pg_sleep($A_SLEEP_S);
ROLLBACK;
EOF
) > /tmp/m6_1_a.out 2>&1 &
A_PID=$!

# Give A time to reach pg_sleep — by which point the push has landed
# and the dirty xid is stamped.
sleep 0.5

# B drains in its own session/tx; the cost row gets committed with A's
# xid stamped.
tick_all

# Wait for A's pg_sleep to expire + ROLLBACK to fire + XactCallback to
# enqueue compensate. Add a small margin to absorb timing jitter.
wait $A_PID
sleep 0.3

# B drains again — picks up the compensate entry, INSERTs
# poc_cost_compensation_consumptions row.
tick_all

# Extract A's xid from the captured output.
a_xid=$(grep -oE 'XID:[0-9]+' /tmp/m6_1_a.out | head -1 | cut -d: -f2)
echo "    A's user_tx_xid = $a_xid"

# Assertion 2a: 1 poc_cost_consumptions row stamped with A's xid (the
# drain wrote it in B's tx, surviving A's ABORT).
if [[ -n "$a_xid" ]]; then
  cons_count=$($PSQL -c "SELECT COUNT(*) FROM poc_cost_consumptions WHERE user_tx_xid = ($a_xid::TEXT)::xid8;")
  if [[ "$cons_count" == "1" ]]; then
    echo "  PASS 1 consumption row stamped with A's xid"
  else
    echo "  FAIL expected 1 consumption row, got $cons_count"
    fail_count=$((fail_count + 1))
  fi
else
  echo "  FAIL could not extract A's xid from output:"
  cat /tmp/m6_1_a.out | head -10
  fail_count=$((fail_count + 1))
fi

# Assertion 2b: 1 compensation_consumptions row, reverses_consumption_id
# pointing at the consumption row above.
if [[ -n "$a_xid" ]]; then
  comp_count=$($PSQL -c "SELECT COUNT(*) FROM poc_cost_compensation_consumptions WHERE user_tx_xid = ($a_xid::TEXT)::xid8;")
  if [[ "$comp_count" == "1" ]]; then
    echo "  PASS 1 compensation_consumptions row stamped with A's xid"
  else
    echo "  FAIL expected 1 compensation row, got $comp_count"
    fail_count=$((fail_count + 1))
  fi
  # Bonus: verify FK linkage.
  fk_match=$($PSQL -c "
    SELECT COUNT(*)
      FROM poc_cost_compensation_consumptions cc
      JOIN poc_cost_consumptions c ON c.consumption_id = cc.reverses_consumption_id
     WHERE cc.user_tx_xid = ($a_xid::TEXT)::xid8;
  ")
  if [[ "$fk_match" == "1" ]]; then
    echo "  PASS reverses_consumption_id resolves to the matching consumption row"
  else
    echo "  FAIL FK linkage broken: $fk_match join rows"
    fail_count=$((fail_count + 1))
  fi
fi

# ── Test 3: abort FIFO 5-layer → 5 compensation_depletion rows ───────
echo
echo "Test 3: ABORT after FIFO 5-layer issue → 5 comp_depletion (cardinality)"
reset_state

# Pre-stage 5 layers via plain poc_ledger_receive (commits each).
for i in 1 2 3 4 5; do
  $PSQL -c "SELECT poc_ledger_receive($SKU, $LOC, 1, 10 + $i);" > /dev/null
done
layer_count=$($PSQL -c "SELECT COUNT(*) FROM poc_cost_layers WHERE sku_id = $SKU AND location_id = $LOC;")
echo "    pre-staged layer count = $layer_count"

# A: BEGIN, push FIFO issue qty=5, sleep, ROLLBACK.
(
  $PSQL <<EOF
\\set ON_ERROR_STOP 1
\\timing off
BEGIN;
SELECT 'XID:' || pg_current_xact_id()::TEXT AS marker;
SELECT poc_ledger_push_only($SKU, $LOC, 5, 80003, 'fifo');
SELECT pg_sleep($A_SLEEP_S);
ROLLBACK;
EOF
) > /tmp/m6_1_a3.out 2>&1 &
A_PID=$!

sleep 0.5
tick_all
wait $A_PID
sleep 0.3
tick_all

a_xid=$(grep -oE 'XID:[0-9]+' /tmp/m6_1_a3.out | head -1 | cut -d: -f2)
echo "    A's user_tx_xid = $a_xid"

# Assertion 3a: 5 depletion rows stamped with A's xid.
if [[ -n "$a_xid" ]]; then
  dep_count=$($PSQL -c "SELECT COUNT(*) FROM poc_cost_depletions WHERE user_tx_xid = ($a_xid::TEXT)::xid8;")
  if [[ "$dep_count" == "5" ]]; then
    echo "  PASS 5 depletion rows (FIFO walked 5 layers) stamped with A's xid"
  else
    echo "  FAIL expected 5 depletion rows, got $dep_count"
    fail_count=$((fail_count + 1))
  fi
fi

# Assertion 3b: cardinality — 5 compensation_depletion rows, each
# pointing at a distinct depletion row.
if [[ -n "$a_xid" ]]; then
  comp_count=$($PSQL -c "SELECT COUNT(*) FROM poc_cost_compensation_depletions WHERE user_tx_xid = ($a_xid::TEXT)::xid8;")
  if [[ "$comp_count" == "5" ]]; then
    echo "  PASS 5 compensation_depletions rows (cardinality preserved)"
  else
    echo "  FAIL expected 5 compensation_depletions rows, got $comp_count"
    fail_count=$((fail_count + 1))
  fi
  distinct_reverses=$($PSQL -c "SELECT COUNT(DISTINCT reverses_depletion_id) FROM poc_cost_compensation_depletions WHERE user_tx_xid = ($a_xid::TEXT)::xid8;")
  if [[ "$distinct_reverses" == "5" ]]; then
    echo "  PASS 5 distinct reverses_depletion_id values (one per original depletion)"
  else
    echo "  FAIL expected 5 distinct reverses_depletion_id, got $distinct_reverses"
    fail_count=$((fail_count + 1))
  fi
  # Each compensation row points at a distinct layer (FIFO walked all 5).
  distinct_layers=$($PSQL -c "SELECT COUNT(DISTINCT layer_id) FROM poc_cost_compensation_depletions WHERE user_tx_xid = ($a_xid::TEXT)::xid8;")
  if [[ "$distinct_layers" == "5" ]]; then
    echo "  PASS 5 distinct layer_id values (one per pre-staged layer)"
  else
    echo "  FAIL expected 5 distinct layer_id, got $distinct_layers"
    fail_count=$((fail_count + 1))
  fi
fi

# Assertion 3c: idempotent re-tick. Calling tick_all again should not
# duplicate compensation rows (ON CONFLICT DO NOTHING on the UNIQUE
# (reverses_depletion_id)).
tick_all
if [[ -n "$a_xid" ]]; then
  comp_count_after=$($PSQL -c "SELECT COUNT(*) FROM poc_cost_compensation_depletions WHERE user_tx_xid = ($a_xid::TEXT)::xid8;")
  if [[ "$comp_count_after" == "5" ]]; then
    echo "  PASS re-tick is idempotent (still 5 rows; UNIQUE swallowed re-attempts)"
  else
    echo "  FAIL re-tick produced extra rows: $comp_count_after != 5"
    fail_count=$((fail_count + 1))
  fi
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
