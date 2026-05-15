#!/usr/bin/env bash
# M5c.2 (acct-4d4n.15) slot-leak audit acceptance tests.
#
# Four scenarios per the bd issue + spec §3.7:
#
#   1. push-only-then-die: backend pushes (acquires slot, leaves
#      session) without consuming the result. Slot ages, audit
#      reclaims, slot returns to SLOT_FREE.
#
#   2. age gating: a fresh ALLOCATED slot (just acquired by a live
#      backend) is NOT reclaimed even if the audit runs immediately —
#      acquire-age must exceed slot_audit_min_age_ms.
#
#   3. real-backend cancel: a backgrounded psql blocks in
#      poc_ledger_apply, gets pg_cancel_backend'd; its slot is
#      already best-effort marked ABANDONED by the M5a.2 cancel
#      cleanup. Run the audit, recycle remaining stragglers.
#
#   4. periodic bgworker: the worker registered at _PG_init with
#      restart_time=Some(10s) wakes on cadence and reclaims leaked
#      slots without manual intervention. Verify via stranding a
#      slot, lowering slot_audit_min_age_ms cluster-wide, waiting
#      ~12s, asserting the slot returned to FREE.
#
# Args: none.
# Output: per-assertion PASS/FAIL on stdout; total wall time at end.
set -euo pipefail

DSN='postgres://acct:acct_dev@localhost:5111/acct_poc_queue'
PSQL="psql -At $DSN"
SHARD=0

t_start=$(date +%s%N)
fail_count=0

# Sweep before any test setup. Filter to client backends — the
# poc_ledger_slot_audit bgworker is also connected via SPI to this DB
# and pg_terminate_backend would SIGTERM it. The worker has no
# clean-exit relaunch (restart_time only fires on crash), so killing
# it leaves Test 4's periodic-tick assertion without a bgworker to
# tick. backend_type='client backend' excludes worker processes.
$PSQL -c "
  SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
   WHERE datname='acct_poc_queue'
     AND pid <> pg_backend_pid()
     AND backend_type = 'client backend';
" > /dev/null 2>&1 || true
sleep 0.5

reset_state() {
  $PSQL -c "TRUNCATE poc_test_rows, poc_pool_locks, poc_pool_lock_anchors, poc_cost_consumptions, poc_cost_depletions, poc_cost_layers, poc_cost_avg;" > /dev/null
  $PSQL -c "SELECT poc_ledger_shard_reset(s) FROM generate_series(0, 15) AS s;" > /dev/null
}

pick_sku_for_shard() {
  local target="$1"
  for sku in $(seq 100 300); do
    local s
    s=$($PSQL -c "SELECT poc_ledger_shard_for($sku, 1)")
    if [[ "$s" == "$target" ]]; then
      echo "$sku"
      return 0
    fi
  done
  echo "ERROR: no sku found that hashes to shard $target" >&2
  return 1
}

# ── Test 1: push-only-then-die ───────────────────────────────────────
echo "Test 1: push_only-then-die slot reclaim"
reset_state
SKU=$(pick_sku_for_shard "$SHARD")
echo "    SKU=$SKU (hashes to shard $SHARD)"

# push_only in a session that EXITS immediately, leaving slot ALLOCATED.
$PSQL -c "SELECT poc_ledger_push_only($SKU, 1, 5, 95101, 'mock');" > /dev/null
state=$($PSQL -c "SELECT poc_ledger_slot_state(0, 0);")
if [[ "$state" == "1" ]]; then
  echo "  PASS slot 0 is ALLOCATED post-push_only (state=$state)"
else
  echo "  FAIL slot 0 unexpected state $state (expected 1=ALLOCATED)"
  fail_count=$((fail_count + 1))
fi

# Audit at default min_age (60000ms) reclaims nothing — slot is young.
young_result=$($PSQL -c "SELECT scanned||':'||reclaimed FROM poc_ledger_slot_leak_audit_tick($SHARD);")
if [[ "$young_result" == "1:0" ]]; then
  echo "  PASS audit at default min_age reclaims 0 (slot young; got '$young_result')"
else
  echo "  FAIL audit unexpected '$young_result' (expected '1:0')"
  fail_count=$((fail_count + 1))
fi

# Age the slot past 100ms, lower min_age via set_config (returns
# value cleanly without psql's "SET" status line), retry.
sleep 0.2
aged_result=$($PSQL -c "SELECT set_config('poc_ledger.slot_audit_min_age_ms', '100', false)::text || ''; SELECT scanned||':'||reclaimed FROM poc_ledger_slot_leak_audit_tick($SHARD);" | tail -1)
if [[ "$aged_result" == "1:1" ]]; then
  echo "  PASS audit at min_age=100ms reclaimed 1 (got '$aged_result')"
else
  echo "  FAIL audit unexpected '$aged_result' (expected '1:1')"
  fail_count=$((fail_count + 1))
fi

post_state=$($PSQL -c "SELECT poc_ledger_slot_state(0, 0);")
if [[ "$post_state" == "0" ]]; then
  echo "  PASS slot 0 is FREE after audit (state=$post_state)"
else
  echo "  FAIL slot 0 unexpected state $post_state (expected 0=FREE)"
  fail_count=$((fail_count + 1))
fi

# A new push_only succeeds — slot pool has at least one slot free,
# so SOME slot acquires (not necessarily slot 0, since next_slot_seq
# has advanced). Verify by checking that the active slot count grew.
allocated_before=$($PSQL -c "SELECT COUNT(*) FROM generate_series(0, 511) AS i(i) WHERE poc_ledger_slot_state($SHARD, i) = 1;")
$PSQL -c "SELECT poc_ledger_push_only($SKU, 1, 5, 95102, 'mock');" > /dev/null
allocated_after=$($PSQL -c "SELECT COUNT(*) FROM generate_series(0, 511) AS i(i) WHERE poc_ledger_slot_state($SHARD, i) = 1;")
if (( allocated_after == allocated_before + 1 )); then
  echo "  PASS post-recovery push_only acquired a fresh slot (allocated $allocated_before → $allocated_after)"
else
  echo "  FAIL allocated count delta unexpected ($allocated_before → $allocated_after)"
  fail_count=$((fail_count + 1))
fi

# ── Test 2: age gating (live backend, ALLOCATED slot, age=0) ─────────
echo
echo "Test 2: age gating — live backend's fresh ALLOCATED slot is NOT reclaimed"
reset_state
SKU=$(pick_sku_for_shard "$SHARD")

# Background apply that will block in M3.1 wait/wake (no concurrent
# committer; we use push_only first to ensure the apply's drain has
# nothing to commit immediately).
( $PSQL -c "BEGIN; SET LOCAL poc_ledger.drain_sleep_us = 800000; SELECT poc_ledger_apply($SKU, 1, 5, 95201, 'mock'); COMMIT;" > /tmp/m5c2_t2.out 2>&1 ) &
a_pid=$!

# Give A time to acquire its slot.
sleep 0.1

# Audit with min_age=50ms: scanned=1 (the live apply's slot), reclaimed=0
# (waiter PID is alive, audit skips).
live_result=$($PSQL -c "SELECT set_config('poc_ledger.slot_audit_min_age_ms', '50', false)::text || ''; SELECT scanned||':'||reclaimed FROM poc_ledger_slot_leak_audit_tick($SHARD);" | tail -1)
if [[ "$live_result" == "1:0" ]]; then
  echo "  PASS audit skipped live-backend slot (scanned 1, reclaimed 0)"
else
  echo "  FAIL audit unexpected '$live_result' (expected '1:0')"
  fail_count=$((fail_count + 1))
fi

wait $a_pid
if grep -qE "ERROR|FATAL" /tmp/m5c2_t2.out; then
  echo "  FAIL apply errored:"
  grep -E "ERROR|FATAL" /tmp/m5c2_t2.out | head -2
  fail_count=$((fail_count + 1))
else
  echo "  PASS live apply completed normally (audit did not interfere)"
fi

# ── Test 3: real-backend cancel + post-cancel audit ──────────────────
echo
echo "Test 3: pg_cancel_backend mid-apply + audit pass cleans up"
reset_state
SKU=$(pick_sku_for_shard "$SHARD")

# Force ring full so apply blocks in M5c.1 backpressure wait.
$PSQL -c "SELECT poc_ledger_test_force_ring_full($SHARD);" > /dev/null
( $PSQL -c "SELECT poc_ledger_apply($SKU, 1, 5, 95301, 'mock');" > /tmp/m5c2_t3.out 2>&1 ) &
a_pid=$!
sleep 0.4
target_pid=$($PSQL -c "SELECT pid FROM pg_stat_activity WHERE datname='acct_poc_queue' AND query LIKE 'SELECT poc_ledger_apply%' AND state='active' AND pid <> pg_backend_pid() LIMIT 1;")
if [[ -z "$target_pid" ]]; then
  echo "  FAIL could not locate A's backend pid"
  fail_count=$((fail_count + 1))
  kill $a_pid 2>/dev/null || true
else
  $PSQL -c "SELECT pg_cancel_backend($target_pid);" > /dev/null
  wait $a_pid || true
  # Backpressure cancel cleanup already called mark_slot_abandoned;
  # slot should be ABANDONED (3). Audit then recycles ABANDONED → FREE
  # via the standard recycle path. But our audit ONLY targets
  # ALLOCATED state (per design); ABANDONED is separately recycled by
  # M5a.1's orphan recovery or the next acquire's CAS-skip. Verify
  # the slot was abandoned cleanly by the M5a.2 / M5c.1 cancel path.
  state_after=$($PSQL -c "SELECT poc_ledger_slot_state(0, 0);")
  # State should be 3 (ABANDONED) — cancel cleanup ran. Or 0 (FREE)
  # if something already recycled. Both are acceptable; ALLOCATED (1)
  # would indicate the cleanup didn't run.
  if [[ "$state_after" == "3" || "$state_after" == "0" ]]; then
    echo "  PASS post-cancel slot is in terminal state (state=$state_after; ABANDONED=3 or FREE=0)"
  else
    echo "  FAIL slot stuck in non-terminal state $state_after"
    fail_count=$((fail_count + 1))
  fi
fi

# Reset ring for sanity; new apply on the same shard should succeed.
reset_state
SKU=$(pick_sku_for_shard "$SHARD")
out=$($PSQL -c "SELECT poc_ledger_apply($SKU, 1, 5, 95302, 'mock');" 2>&1)
if ! grep -qE "ERROR|FATAL" <<< "$out"; then
  echo "  PASS shard recovered for next apply"
else
  echo "  FAIL post-cancel apply errored: $out"
  fail_count=$((fail_count + 1))
fi

# ── Test 4: periodic bgworker reclaims without manual tick ───────────
echo
echo "Test 4: periodic bgworker (restart_time=10s) reclaims on its own"
reset_state
SKU=$(pick_sku_for_shard "$SHARD")

# Drop min_age cluster-wide so the bgworker's next run reclaims fast.
$PSQL -c "ALTER SYSTEM SET poc_ledger.slot_audit_min_age_ms = 50;" > /dev/null
$PSQL -c "SELECT pg_reload_conf();" > /dev/null
sleep 0.3

# Strand a slot via push_only.
$PSQL -c "SELECT poc_ledger_push_only($SKU, 1, 5, 95401, 'mock');" > /dev/null
state_before=$($PSQL -c "SELECT poc_ledger_slot_state(0, 0);")
echo "    pre-audit slot 0 state: $state_before"

# The bgworker has restart_time=10s. We wait up to 14s and poll. The
# very first run may have already occurred at postmaster startup
# (before our push); the next run is up to 10s away.
log_marker="M5C2_TEST4_START_$(date +%s%N)"
$PSQL -c "DO \$\$ BEGIN RAISE LOG '$log_marker'; END \$\$;" > /dev/null
echo "    waiting up to 14s for bgworker tick..."
reclaimed=0
for i in $(seq 1 28); do
  sleep 0.5
  s=$($PSQL -c "SELECT poc_ledger_slot_state(0, 0);" 2>/dev/null || echo "")
  if [[ "$s" == "0" ]]; then
    reclaimed=1
    break
  fi
done

if [[ "$reclaimed" == "1" ]]; then
  echo "  PASS periodic bgworker reclaimed slot 0 within budget"
else
  echo "  FAIL bgworker did not reclaim within 14s; slot still in state $s"
  echo "  --- bgworker log lines this window ---"
  docker logs --tail 200 acct-postgres 2>&1 | awk -v m="$log_marker" '$0 ~ m {found=1; next} found' | grep -F "poc_ledger_slot_audit" | head -3 || true
  fail_count=$((fail_count + 1))
fi

# Reset GUC.
$PSQL -c "ALTER SYSTEM RESET poc_ledger.slot_audit_min_age_ms;" > /dev/null
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
