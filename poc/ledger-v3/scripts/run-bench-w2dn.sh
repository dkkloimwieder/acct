#!/usr/bin/env bash
# acct-w2dn: validation bench for the batch_size_max=50000 reframe.
# Runs s2/s4/s5 routed AllWac at the new default; confirms (a) s4
# throughput parity restored (vs e5fz bsm=50), (b) s5 grows commit_groups
# to natural window size, (c) s2 unchanged.
set -u

cd "$(dirname "$0")/.."

TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUTDIR=results/phase6/bench-w2dn
INDEX=${OUTDIR}/run-${TS}.log

mkdir -p ${OUTDIR}
exec > >(tee -a ${INDEX}) 2>&1

echo "==> bench timestamp: ${TS}"
echo "==> outdir: ${OUTDIR}"
echo

DURATION=${DURATION:-30s}
MAX_CALLERS=${MAX_CALLERS:-20}
SEED_COUNT=${SEED_COUNT:-1000}

run_one() {
  local sid=$1
  local logfile=${OUTDIR}/${sid}-bsm-default-${TS}.log
  local jsonfile=${OUTDIR}/${sid}-bsm-default-${TS}.json

  echo "==> ${sid} batch_size_max=default at $(date -u +%H:%M:%S)"
  docker restart acct-postgres >/dev/null 2>&1
  sleep 5

  docker exec acct-postgres psql -U acct -d poc_v3 \
    -c "SHOW ledger_routed.batch_size_max;" \
    -c "SHOW ledger_routed.batch_window_us;" \
    > ${OUTDIR}/${sid}-bsm-default-${TS}.guc.log 2>&1

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
  run_one ${sid}
done

echo "==> bench complete at $(date -u +%H:%M:%S)"
echo "==> index: ${INDEX}"
