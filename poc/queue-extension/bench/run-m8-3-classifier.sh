#!/usr/bin/env bash
# M8.3 (acct-4d4n.19) bottleneck classifier acceptance.
#
# Spec §5.7: per-cell B1/B2/B3/B5 capture + label emission. Extension
# owns B3 (plan_apply CPU sum since reset) and B5 (wake-latency
# histogram). B1 (LWLock) and B2 (WAL) are caller-sampled and supplied
# numerically to the classifier.
#
# Tests:
#   T1 — Zero state: snapshot fields all zero after reset.
#   T2 — Single applies: B3 sum increments + B5 captures samples;
#        snapshot diff vs prior shows monotonic growth.
#   T3 — Classifier labels: cells with synthetic component shares
#        produce expected single-dimension and mixed labels.
#   T4 — wake_latency_stats accessor: returns non-zero after applies.
#
# Args: none. Output: per-assertion PASS/FAIL + total wall time.
set -euo pipefail

DSN='postgres://acct:acct_dev@localhost:5111/acct_poc_queue'
PSQL="psql -At $DSN"
fail_count=0
t_start=$(date +%s%N)

# Sweep first.
psql -At 'postgres://acct:acct_dev@localhost:5111/postgres' -c "
  SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
   WHERE datname='acct_poc_queue' AND pid <> pg_backend_pid() AND backend_type = 'client backend';
" > /dev/null 2>&1 || true
sleep 0.5

reset_state() {
  $PSQL -c "TRUNCATE poc_cost_compensation_depletions, poc_cost_compensation_consumptions, poc_test_rows, poc_pool_locks, poc_pool_lock_anchors, poc_cost_consumptions, poc_cost_depletions, poc_cost_layers, poc_cost_avg;" > /dev/null
  $PSQL -c "SELECT poc_ledger_shard_reset(s) FROM generate_series(0, 255) AS s;" > /dev/null
  $PSQL -c "SELECT poc_ledger_method_stats_reset();" > /dev/null
  $PSQL -c "SELECT poc_ledger_recovery_stats_reset();" > /dev/null
  $PSQL -c "SELECT poc_ledger_bottleneck_stats_reset();" > /dev/null
}

# ── Test 1: zero state ───────────────────────────────────────────────
echo "Test 1: zero state after bottleneck_stats_reset"
reset_state

b3=$($PSQL -c "SELECT (poc_ledger_bottleneck_snapshot()->>'b3_plan_apply_total_ns')::BIGINT;")
b5_count=$($PSQL -c "SELECT (poc_ledger_bottleneck_snapshot()->>'b5_wake_count')::BIGINT;")
b5_total=$($PSQL -c "SELECT (poc_ledger_bottleneck_snapshot()->>'b5_wake_total_ns')::BIGINT;")
p50=$($PSQL -c "SELECT (poc_ledger_bottleneck_snapshot()->>'b5_p50_ns')::BIGINT;")
p99=$($PSQL -c "SELECT (poc_ledger_bottleneck_snapshot()->>'b5_p99_ns')::BIGINT;")

if [[ "$b3" == "0" && "$b5_count" == "0" && "$b5_total" == "0" && "$p50" == "0" && "$p99" == "0" ]]; then
  echo "  PASS all snapshot fields zero after reset"
else
  echo "  FAIL snapshot fields: b3=$b3 b5_count=$b5_count b5_total=$b5_total p50=$p50 p99=$p99"
  fail_count=$((fail_count + 1))
fi

wake_count=$($PSQL -c "SELECT sample_count FROM poc_ledger_wake_latency_stats();")
if [[ "$wake_count" == "0" ]]; then
  echo "  PASS poc_ledger_wake_latency_stats sample_count == 0"
else
  echo "  FAIL wake_latency_stats sample_count = $wake_count"
  fail_count=$((fail_count + 1))
fi

# ── Test 2: monotonic B3 + B5 under applies ─────────────────────────
echo
echo "Test 2: B3 + B5 increment under applies"
reset_state
$PSQL -c "INSERT INTO poc_cost_avg (sku_id, location_id, running_qty, running_value) VALUES (700, 1, 100000, 10000000);" > /dev/null

snap0=$($PSQL -c "SELECT poc_ledger_bottleneck_snapshot()::TEXT;")
for i in 1 2 3 4 5; do
  $PSQL -c "SELECT poc_ledger_apply(700, 1, 1, $((90000 + i))::BIGINT, 'avg');" > /dev/null
done
snap1=$($PSQL -c "SELECT poc_ledger_bottleneck_snapshot()::TEXT;")

b3_after=$($PSQL -c "SELECT (poc_ledger_bottleneck_snapshot()->>'b3_plan_apply_total_ns')::BIGINT;")
b5_count_after=$($PSQL -c "SELECT (poc_ledger_bottleneck_snapshot()->>'b5_wake_count')::BIGINT;")
b5_total_after=$($PSQL -c "SELECT (poc_ledger_bottleneck_snapshot()->>'b5_wake_total_ns')::BIGINT;")

echo "    snap0: $snap0"
echo "    snap1: $snap1"

if (( b3_after >= 5 )); then
  echo "  PASS b3_plan_apply_total_ns >= 5 (got $b3_after — 5 plan_apply calls)"
else
  echo "  FAIL b3_plan_apply_total_ns = $b3_after"
  fail_count=$((fail_count + 1))
fi

if (( b5_count_after >= 5 )); then
  echo "  PASS b5_wake_count >= 5 (got $b5_count_after)"
else
  echo "  FAIL b5_wake_count = $b5_count_after"
  fail_count=$((fail_count + 1))
fi

if (( b5_total_after > 0 )); then
  echo "  PASS b5_wake_total_ns > 0 (got $b5_total_after)"
else
  echo "  FAIL b5_wake_total_ns = $b5_total_after"
  fail_count=$((fail_count + 1))
fi

# ── Test 3: classifier labels ────────────────────────────────────────
echo
echo "Test 3: classifier label emission"
reset_state

# 3a — idle cell.
snap_idle_start=$($PSQL -c "SELECT poc_ledger_bottleneck_snapshot()::TEXT;")
snap_idle_end=$($PSQL -c "SELECT poc_ledger_bottleneck_snapshot()::TEXT;")
label_idle=$($PSQL -c "SELECT poc_ledger_bottleneck_classify('$snap_idle_start'::JSONB, '$snap_idle_end'::JSONB, 1000, 0, 0);")
echo "    idle cell label: $label_idle"
if [[ "$label_idle" == "idle" ]]; then
  echo "  PASS idle cell classifies as 'idle'"
else
  echo "  FAIL idle cell classified as '$label_idle' (expected 'idle')"
  fail_count=$((fail_count + 1))
fi

# 3b — synthetic B1-dominant via caller-supplied numerics.
# Snapshots are zero-equal; we just supply a large b1_lock_ms relative
# to wall_ms.
snap_z='{"b3_plan_apply_total_ns":0,"b5_wake_total_ns":0,"b5_wake_count":0,"b5_p50_ns":0,"b5_p99_ns":0,"ts_ns":0}'
label_b1=$($PSQL -c "SELECT poc_ledger_bottleneck_classify('$snap_z'::JSONB, '$snap_z'::JSONB, 1000, 700, 0);")
echo "    b1-dominant cell label: $label_b1"
if [[ "$label_b1" == "B1:lwlock" ]]; then
  echo "  PASS b1-dominant cell (700ms lock / 1000ms wall) classifies as 'B1:lwlock'"
else
  echo "  FAIL b1-dominant classified as '$label_b1' (expected 'B1:lwlock')"
  fail_count=$((fail_count + 1))
fi

# 3c — synthetic B2-dominant (WAL fsync).
label_b2=$($PSQL -c "SELECT poc_ledger_bottleneck_classify('$snap_z'::JSONB, '$snap_z'::JSONB, 1000, 0, 800);")
echo "    b2-dominant cell label: $label_b2"
if [[ "$label_b2" == "B2:wal" ]]; then
  echo "  PASS b2-dominant cell (800ms WAL / 1000ms wall) classifies as 'B2:wal'"
else
  echo "  FAIL b2-dominant classified as '$label_b2' (expected 'B2:wal')"
  fail_count=$((fail_count + 1))
fi

# 3d — synthetic mixed B1+B2 (each ~35%).
label_mix=$($PSQL -c "SELECT poc_ledger_bottleneck_classify('$snap_z'::JSONB, '$snap_z'::JSONB, 1000, 350, 350);")
echo "    mixed B1+B2 cell label: $label_mix"
if [[ "$label_mix" == "mixed:B1+B2" || "$label_mix" == "mixed:B2+B1" ]]; then
  echo "  PASS mixed B1+B2 cell (350+350ms / 1000ms wall) classifies as '$label_mix'"
else
  echo "  FAIL mixed B1+B2 classified as '$label_mix' (expected 'mixed:B1+B2' or 'mixed:B2+B1')"
  fail_count=$((fail_count + 1))
fi

# 3e — synthetic B3-dominant via crafted JSONB snapshots. The
# classifier reads b3_plan_apply_total_ns from the end-snapshot diff;
# we can drive any share by choosing the snapshot values vs wall_ms.
# (The real extension-owned B3 is exercised separately by Test 2's
# monotonic-growth assertion + Test 4's wake_latency_stats sanity.)
snap_b3_start='{"b3_plan_apply_total_ns":0,"b5_wake_total_ns":0,"b5_wake_count":0,"b5_p50_ns":0,"b5_p99_ns":0,"ts_ns":0}'
# B3=700_000_000 ns = 700 ms of plan_apply over a 1000 ms wall.
snap_b3_end='{"b3_plan_apply_total_ns":700000000,"b5_wake_total_ns":0,"b5_wake_count":0,"b5_p50_ns":0,"b5_p99_ns":0,"ts_ns":0}'
label_b3=$($PSQL -c "SELECT poc_ledger_bottleneck_classify('$snap_b3_start'::JSONB, '$snap_b3_end'::JSONB, 1000, 0, 0);")
echo "    b3-dominant cell label: $label_b3"
if [[ "$label_b3" == "B3:cpu" ]]; then
  echo "  PASS b3-dominant cell (700ms CPU / 1000ms wall) classifies as 'B3:cpu'"
else
  echo "  FAIL b3-dominant classified as '$label_b3' (expected 'B3:cpu')"
  fail_count=$((fail_count + 1))
fi

# 3f — synthetic B5-dominant via wake total. B5=800_000_000 / 1000 ms.
snap_b5_start='{"b3_plan_apply_total_ns":0,"b5_wake_total_ns":0,"b5_wake_count":0,"b5_p50_ns":0,"b5_p99_ns":0,"ts_ns":0}'
snap_b5_end='{"b3_plan_apply_total_ns":0,"b5_wake_total_ns":800000000,"b5_wake_count":1,"b5_p50_ns":0,"b5_p99_ns":0,"ts_ns":0}'
label_b5=$($PSQL -c "SELECT poc_ledger_bottleneck_classify('$snap_b5_start'::JSONB, '$snap_b5_end'::JSONB, 1000, 0, 0);")
echo "    b5-dominant cell label: $label_b5"
if [[ "$label_b5" == "B5:wake" ]]; then
  echo "  PASS b5-dominant cell (800ms wake / 1000ms wall) classifies as 'B5:wake'"
else
  echo "  FAIL b5-dominant classified as '$label_b5' (expected 'B5:wake')"
  fail_count=$((fail_count + 1))
fi

# 3g — Real-workload sanity: 50 applies must produce a non-idle label
# at sub-millisecond wall scope. Use snapshot diff against a 1ms wall
# is doomed because B3 << 1ms; instead, verify the snapshot ITSELF
# reports B3 > 0 + B5 > 0 (already covered by Test 2). No assert here;
# real-workload classification is the M9.3 harness's responsibility.

# ── Test 4: wake_latency_stats accessor ──────────────────────────────
echo
echo "Test 4: wake_latency_stats accessor under load"
reset_state
$PSQL -c "INSERT INTO poc_cost_avg (sku_id, location_id, running_qty, running_value) VALUES (702, 1, 100000, 10000000);" > /dev/null
# Drive a fresh burst so this test is independent of Test 3's state.
for i in $(seq 1 50); do
  $PSQL -c "SELECT poc_ledger_apply(702, 1, 1, $((92000 + i))::BIGINT, 'avg');" > /dev/null 2>&1 || true
done
read p50_t4 p99_t4 cnt_t4 <<< "$($PSQL -F' ' -c "SELECT p50_ns, p99_ns, sample_count FROM poc_ledger_wake_latency_stats();")"
echo "    p50=$p50_t4 p99=$p99_t4 sample_count=$cnt_t4"

if (( cnt_t4 >= 50 )); then
  echo "  PASS sample_count >= 50 (got $cnt_t4)"
else
  echo "  FAIL sample_count = $cnt_t4"
  fail_count=$((fail_count + 1))
fi

if (( p99_t4 >= p50_t4 )); then
  echo "  PASS p99 >= p50 ($p99_t4 >= $p50_t4 ns)"
else
  echo "  FAIL p99 < p50 ($p99_t4 < $p50_t4)"
  fail_count=$((fail_count + 1))
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
