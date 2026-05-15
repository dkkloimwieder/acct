#!/usr/bin/env bash
# M5b.2 (acct-4d4n.13) lease false-positive protection acceptance.
#
# Two scenarios per the bd issue + plan:
#
#   1. slow committer not stolen: a committer whose drain wall time
#      exceeds committer_lease_ms (simulated via the test-only
#      poc_ledger.drain_sleep_us GUC) must NOT be preempted by a
#      contender on the same shard. The pg_pid_alive guard in
#      try_acquire_or_takeover returns Held → contender backs off via
#      the outer WaitLatch loop → both apply calls succeed serially.
#
#   2. dead committer correctly stolen: covered structurally by the
#      M5a.1 (acct-4d4n.10) pre_insert death bench, which injects a
#      genuinely-dead PID into shmem and asserts the orphan recovery
#      path runs. M5b.2 inherits that regression net rather than
#      duplicating it; cited here for completeness.
#
# Args: none.
# Output: per-assertion PASS/FAIL on stdout; total wall time at end.
set -euo pipefail

DSN='postgres://acct:acct_dev@localhost:5111/acct_poc_queue'
PSQL="psql -At $DSN"
SHARD=0
SLEEP_US=300000     # 300ms — 3× the default committer_lease_ms (100ms)
LEASE_MS=100        # default; left unchanged
A_OFFSET_S=0.05     # backend A starts; B starts 50ms later

t_start=$(date +%s%N)
fail_count=0

# Sweep before any test setup (orphan-shells protocol).
$PSQL -c "
  SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
   WHERE datname='acct_poc_queue' AND pid <> pg_backend_pid();
" > /dev/null 2>&1 || true
sleep 0.5

reset_state() {
  $PSQL -c "TRUNCATE poc_test_rows, poc_pool_locks, poc_pool_lock_anchors, poc_cost_consumptions, poc_cost_depletions, poc_cost_layers, poc_cost_avg;" > /dev/null
  $PSQL -c "SELECT poc_ledger_shard_reset(s) FROM generate_series(0, 255) AS s;" > /dev/null
}

# Pick two distinct skus that hash to the same shard (so they
# contend on committer election). loc=1 fixed.
pick_skus_for_shard() {
  local target="$1"
  local first="" second=""
  for sku in $(seq 100 600); do
    local s
    s=$($PSQL -c "SELECT poc_ledger_shard_for($sku, 1)")
    if [[ "$s" == "$target" ]]; then
      if [[ -z "$first" ]]; then
        first="$sku"
      else
        second="$sku"
        echo "$first $second"
        return 0
      fi
    fi
  done
  echo "ERROR: failed to find two skus that hash to shard $target" >&2
  return 1
}

# ── Test 1: slow committer holds shard; contender backs off ──────────
echo "Test 1: slow committer holds shard; contender observes Held and waits"
echo "    drain_sleep_us=$SLEEP_US ($((SLEEP_US / 1000))ms); committer_lease_ms=$LEASE_MS"
reset_state

read sku_a sku_b <<< "$(pick_skus_for_shard "$SHARD")"
echo "    sku_a=$sku_a sku_b=$sku_b (both hash to shard $SHARD)"

# Emit a unique marker into the postmaster log so we can grep
# strictly within this test's window. `docker logs --since <ts>` and
# `--since <Ns>` both hang on this host's docker version; `--tail N`
# works but blends in lines from prior tests run before this script.
log_marker="M5B2_TEST_START_$(date +%s%N)"
$PSQL -c "DO \$\$ BEGIN RAISE LOG '$log_marker'; END \$\$;" > /dev/null

# Backend A: SET LOCAL drain_sleep_us inside an explicit txn so the
# GUC scopes to A's apply call. Backend B's session inherits the
# Sighup-scope default (0 — no sleep).
a_start_ns=$(date +%s%N)
( $PSQL -c "BEGIN; SET LOCAL poc_ledger.drain_sleep_us = $SLEEP_US; SELECT poc_ledger_apply($sku_a, 1, 5, 80001, 'mock'); COMMIT;" > /tmp/m5b2_a.out 2>&1 ) &
a_bgpid=$!

# Brief offset so A's CAS-acquire wins.
sleep "$A_OFFSET_S"

# Backend B: plain apply. drain_sleep_us=0 here.
b_start_ns=$(date +%s%N)
( $PSQL -c "SELECT poc_ledger_apply($sku_b, 1, 5, 80002, 'mock');" > /tmp/m5b2_b.out 2>&1 ) &
b_bgpid=$!

wait $a_bgpid; a_end_ns=$(date +%s%N)
wait $b_bgpid; b_end_ns=$(date +%s%N)
a_duration_ms=$(( (a_end_ns - a_start_ns) / 1000000 ))
b_duration_ms=$(( (b_end_ns - b_start_ns) / 1000000 ))

echo "    A apply wall time: ${a_duration_ms}ms"
echo "    B apply wall time: ${b_duration_ms}ms"

# Assertion 1: both calls returned without ERROR / FATAL.
if grep -qE "ERROR|FATAL" /tmp/m5b2_a.out /tmp/m5b2_b.out; then
  echo "  FAIL apply errors in output:"
  grep -E "ERROR|FATAL" /tmp/m5b2_a.out /tmp/m5b2_b.out | head -5 || true
  fail_count=$((fail_count + 1))
else
  echo "  PASS both apply calls returned without ERROR"
fi

# Assertion 2: A's wall time covers the 300ms drain sleep (allow 20ms
# tolerance for connection + apply overhead).
sleep_ms=$((SLEEP_US / 1000))
threshold_a=$((sleep_ms - 20))
if (( a_duration_ms >= threshold_a )); then
  echo "  PASS A's wall time >= ${threshold_a}ms (got ${a_duration_ms}ms — drain_sleep_us active)"
else
  echo "  FAIL A's wall time ${a_duration_ms}ms < ${threshold_a}ms — drain_sleep_us may not have taken effect"
  fail_count=$((fail_count + 1))
fi

# Assertion 3: B waited at least ~200ms (proves the contender did NOT
# take over and short-circuit). B started 50ms after A; A's drain
# finished ~300ms in; B's first uncontested CAS happens around 300ms
# minus B's offset = ~250ms.
if (( b_duration_ms >= 200 )); then
  echo "  PASS B's wall time >= 200ms (got ${b_duration_ms}ms — B waited for A's slow drain)"
else
  echo "  FAIL B's wall time ${b_duration_ms}ms < 200ms — B may have taken over A's committer"
  fail_count=$((fail_count + 1))
fi

# Assertion 4: both apply calls produced their durable consumption
# rows (mock writes one consumption per call).
rows=$($PSQL -c "SELECT COUNT(*) FROM poc_cost_consumptions WHERE issue_id IN (80001, 80002);")
if [[ "$rows" == "2" ]]; then
  echo "  PASS 2 durable consumption rows present (A + B both committed)"
else
  echo "  FAIL expected 2 consumption rows, got $rows"
  fail_count=$((fail_count + 1))
fi

# Assertion 5: committer_tx_id is monotonic — A's id < B's id (A
# drained first, then B). If B had taken over A's committer slot
# during the sleep, A's request would have been recovered by B's
# orphan_recovery using B's committer_tx_id; A's resume would either
# error or B would race A's INSERT. Monotonic-and-distinct IDs from
# the same shard prove sequential election.
tx_a=$($PSQL -c "SELECT committer_tx_id FROM poc_cost_consumptions WHERE issue_id = 80001;")
tx_b=$($PSQL -c "SELECT committer_tx_id FROM poc_cost_consumptions WHERE issue_id = 80002;")
echo "    A committer_tx_id=$tx_a; B committer_tx_id=$tx_b"
if (( tx_a > 0 && tx_b > tx_a )); then
  echo "  PASS committer_tx_id strictly monotonic: A=$tx_a < B=$tx_b (sequential election)"
else
  echo "  FAIL committer_tx_id not strictly monotonic: A=$tx_a, B=$tx_b"
  fail_count=$((fail_count + 1))
fi

# Assertion 6: no `drain_and_commit: slot ... was abandoned` log
# lines in the test window — the M5a.2 cancel path's log marker.
# Its presence here would mean a contender raced a slot-fill CAS,
# which can only happen on takeover, which we're asserting did NOT
# happen.
# `|| true` keeps grep's no-match (expected outcome) from aborting
# under pipefail.
abandoned_lines=$(docker logs --tail 500 acct-postgres 2>&1 | awk -v m="$log_marker" '$0 ~ m {found=1; next} found' | grep -F "drain_and_commit: slot" | grep -F "was abandoned" || true)
if [[ -z "$abandoned_lines" ]]; then
  echo "  PASS no slot-abandoned log lines in test window (no contender raced A's drain)"
else
  echo "  FAIL slot-abandoned log lines present (contender raced A's drain):"
  echo "$abandoned_lines" | head -3
  fail_count=$((fail_count + 1))
fi

# ── Test 2: dead committer correctly stolen (regression net) ─────────
echo
echo "Test 2: dead committer correctly stolen"
echo "    Covered by bench/run-m5a1-orphan-recovery.sh Test 1 (pre_insert"
echo "    death). That bench injects a genuinely-dead PID via"
echo "    poc_ledger_inject_dead_committer + a 'true' subprocess and"
echo "    asserts the orphan recovery path marks the orphan slot"
echo "    ABANDONED. M5b.2 inherits that regression net — the same"
echo "    pg_pid_alive guard that returns Held for an alive PID returns"
echo "    !pid_dead → false → takeover CAS proceeds for a dead PID."
echo "  PASS regression net cited (run-m5a1-orphan-recovery.sh covers this half)"

# Cleanup — leave drain_sleep_us at its default 0.
$PSQL -c "ALTER SYSTEM RESET poc_ledger.drain_sleep_us;" > /dev/null
$PSQL -c "SELECT pg_reload_conf();" > /dev/null

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
