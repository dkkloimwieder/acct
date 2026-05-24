#!/usr/bin/env bash
# acct-w2dn re-sweep: single-session bsm ∈ {50, 1000, 5000, 10000, 50000}
# on s4 + s5 routed AllWac. Removes cross-session noise between e5fz
# (separate run) and the w2dn initial validation (new default bsm=50000
# only). 10 cells total.
set -u

cd "$(dirname "$0")/.."

TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUTDIR=results/phase6/bench-w2dn
INDEX=${OUTDIR}/resweep-${TS}.log

mkdir -p ${OUTDIR}
exec > >(tee -a ${INDEX}) 2>&1

echo "==> resweep timestamp: ${TS}"
echo "==> outdir: ${OUTDIR}"
echo

DURATION=${DURATION:-30s}
MAX_CALLERS=${MAX_CALLERS:-20}
SEED_COUNT=${SEED_COUNT:-1000}

run_one() {
  local sid=$1
  local bsm=$2
  local logfile=${OUTDIR}/resweep-${sid}-bsm${bsm}-${TS}.log
  local jsonfile=${OUTDIR}/resweep-${sid}-bsm${bsm}-${TS}.json

  echo "==> ${sid} batch_size_max=${bsm} at $(date -u +%H:%M:%S)"
  docker restart acct-postgres >/dev/null 2>&1
  sleep 5

  docker exec acct-postgres psql -U acct -d poc_v3 \
    -c "ALTER SYSTEM SET ledger_routed.batch_size_max = ${bsm};" \
    -c "SELECT pg_reload_conf();" \
    -c "SHOW ledger_routed.batch_size_max;" \
    > ${OUTDIR}/resweep-${sid}-bsm${bsm}-${TS}.guc.log 2>&1

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

for sid in s4 s5; do
  for bsm in 50 1000 5000 10000 50000; do
    run_one ${sid} ${bsm}
  done
done

# Restore default (50000 now)
echo "==> restoring default batch_size_max"
docker exec acct-postgres psql -U acct -d poc_v3 \
  -c "ALTER SYSTEM RESET ledger_routed.batch_size_max;" \
  -c "SELECT pg_reload_conf();" > /dev/null 2>&1

echo "==> resweep complete at $(date -u +%H:%M:%S)"
echo "==> index: ${INDEX}"
