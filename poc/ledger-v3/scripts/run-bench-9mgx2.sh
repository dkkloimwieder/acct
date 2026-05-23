#!/usr/bin/env bash
# acct-9mgx.2 bench: s2 + s5 × direct + routed × AllLifo + AllWac baseline.
# 8 runs total. Compares cross-method throughput under identical scenarios.
set -u

cd "$(dirname "$0")/.."

TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUTDIR=results/phase6/bench-9mgx2
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
  local path=$2
  local mix=$3
  local logfile=${OUTDIR}/${sid}-${path}-${mix}-${TS}.log
  local jsonfile=${OUTDIR}/${sid}-${path}-${mix}-${TS}.json

  echo "==> ${sid} ${path} ${mix} at $(date -u +%H:%M:%S)"
  docker restart acct-postgres >/dev/null 2>&1
  sleep 5

  cargo run --release -q -p ledger-harness -- run \
    --scenario ${sid} --path ${path} --duration ${DURATION} \
    --method-mix ${mix} --seed-count ${SEED_COUNT} \
    --max-callers ${MAX_CALLERS} --no-sampler \
    --output ${jsonfile} \
    > ${logfile} 2>&1
  local rc=$?
  echo "    exit=${rc} json=${jsonfile}"
  tail -3 ${logfile} | sed 's/^/    | /'
  echo
}

for sid in s2 s5; do
  for path in direct routed; do
    for mix in all-lifo all-wac; do
      run_one ${sid} ${path} ${mix}
    done
  done
done

echo "==> bench complete at $(date -u +%H:%M:%S)"
echo "==> index: ${INDEX}"
