#!/usr/bin/env bash
# acct-e5fz Part A: batch_size_max sweep on routed AllWac.
# 4 sizes × 3 scenarios × 1 path = 12 runs total.
# Captures throughput + ack p99 + commit_group size distribution per cell.
set -u

cd "$(dirname "$0")/.."

TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUTDIR=results/phase6/bench-e5fz
INDEX=${OUTDIR}/run-${TS}.log

mkdir -p ${OUTDIR}
exec > >(tee -a ${INDEX}) 2>&1

echo "==> bench timestamp: ${TS}"
echo "==> outdir: ${OUTDIR}"
echo

DURATION=${DURATION:-30s}
MAX_CALLERS=${MAX_CALLERS:-20}
SEED_COUNT=${SEED_COUNT:-1000}

DSN="postgres://acct:acct_dev@localhost:5111/poc_v3"

run_one() {
  local sid=$1
  local bsm=$2
  local logfile=${OUTDIR}/${sid}-bsm${bsm}-${TS}.log
  local jsonfile=${OUTDIR}/${sid}-bsm${bsm}-${TS}.json

  echo "==> ${sid} batch_size_max=${bsm} at $(date -u +%H:%M:%S)"
  docker restart acct-postgres >/dev/null 2>&1
  sleep 5

  docker exec acct-postgres psql -U acct -d poc_v3 \
    -c "ALTER SYSTEM SET ledger_routed.batch_size_max = ${bsm};" \
    -c "SELECT pg_reload_conf();" \
    -c "SHOW ledger_routed.batch_size_max;" \
    > ${OUTDIR}/${sid}-bsm${bsm}-${TS}.guc.log 2>&1

  cargo run --release -q -p ledger-harness -- run \
    --scenario ${sid} --path routed --duration ${DURATION} \
    --method-mix all-wac --seed-count ${SEED_COUNT} \
    --max-callers ${MAX_CALLERS} --no-sampler \
    --output ${jsonfile} \
    > ${logfile} 2>&1
  local rc=$?
  echo "    exit=${rc} json=${jsonfile}"
  tail -3 ${logfile} | sed 's/^/    | /'
  echo
}

for sid in s2 s4 s5; do
  for bsm in 50 200 1000 5000; do
    run_one ${sid} ${bsm}
  done
done

# Restore default
echo "==> restoring default batch_size_max=50"
docker exec acct-postgres psql -U acct -d poc_v3 \
  -c "ALTER SYSTEM RESET ledger_routed.batch_size_max;" \
  -c "SELECT pg_reload_conf();" > /dev/null 2>&1

echo "==> bench complete at $(date -u +%H:%M:%S)"
echo "==> index: ${INDEX}"
