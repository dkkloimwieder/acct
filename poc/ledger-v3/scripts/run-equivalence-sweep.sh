#!/usr/bin/env bash
set -u

cd "$(dirname "$0")/.."

# METHOD_MIX env var selects which cost-method's universe to sweep.
# Default 'all-wac' preserves backwards-compat with acct-h5gs/9mgx.5
# invocations; alternatives: all-wac-periodic (9mgx.6), all-fifo (9mgx.1).
METHOD_MIX=${METHOD_MIX:-all-wac}

TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUTDIR=results/phase6/equivalence
INDEX=${OUTDIR}/run-${METHOD_MIX}-${TS}.log

mkdir -p ${OUTDIR}
exec > >(tee -a ${INDEX}) 2>&1

echo "==> equivalence sweep timestamp: ${TS}"
echo "==> method-mix: ${METHOD_MIX}"
echo "==> outdir: ${OUTDIR}"
echo

run_scenario() {
  local sid=$1
  local mode=$2
  local suffix=$3
  local logfile=${OUTDIR}/${sid}-${suffix}-${METHOD_MIX}-${TS}.log

  echo "==> ${sid} ${mode} at $(date -u +%H:%M:%S)"
  docker restart acct-postgres >/dev/null 2>&1
  sleep 5

  if [ "${mode}" = "strict" ]; then
    cargo run --release -q -p ledger-harness -- equivalence \
      --scenario ${sid} --method-mix ${METHOD_MIX} --strict > ${logfile} 2>&1
  else
    cargo run --release -q -p ledger-harness -- equivalence \
      --scenario ${sid} --method-mix ${METHOD_MIX} > ${logfile} 2>&1
  fi
  local rc=$?
  echo "    exit=${rc} log=${logfile}"
  tail -8 ${logfile} | sed 's/^/    | /'
  echo
}

for sid in s1 s2 s3 s4 s5 s6; do
  run_scenario ${sid} lenient lenient
done

for sid in s2 s4 s5; do
  for trial in 1 2 3; do
    run_scenario ${sid} strict "strict-trial${trial}"
  done
done

echo "==> sweep complete at $(date -u +%H:%M:%S)"
echo "==> index: ${INDEX}"
