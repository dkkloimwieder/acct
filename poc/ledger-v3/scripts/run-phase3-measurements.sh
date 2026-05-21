#!/usr/bin/env bash
# Phase 3 first-pass measurement run (200 caller cap, 30s window).
#
# Scope reduction vs design-v3 §11 captured under acct-cs5k notes:
# the dev container's max_connections=320 cannot fit S5/S6 at the
# advertised 1000 callers. Caps via --max-callers; full PG-tuned run
# tracked as cs5k-followup.
#
# Outputs one JSON per scenario to results/phase3/.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

DURATION="${DURATION:-30s}"
MAX_CALLERS="${MAX_CALLERS:-200}"
OUT_DIR="${OUT_DIR:-results/phase3}"
TS=$(date +%Y-%m-%dT%H-%M-%S)
mkdir -p "$OUT_DIR"

echo "==> building ledger-harness (release)"
cargo build --release -p ledger-harness 2>&1 | tail -3

echo "==> seeding pool universe (idempotent)"
cargo run --release -p ledger-harness -- seed-pools \
    --count 10000 --skus 1000 --locations 10 --method-mix mixed

for sc in s1 s2 s3 s4 s5 s6; do
    OUT="$OUT_DIR/${sc}-direct-${TS}.json"
    echo "==> running $sc → $OUT"
    docker restart acct-postgres >/dev/null
    # Wait for postgres to accept connections (max ~30s).
    for _ in $(seq 1 30); do
        if docker exec acct-postgres pg_isready -U acct >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    cargo run --release -p ledger-harness -- run \
        --scenario "$sc" --path direct --duration "$DURATION" \
        --max-callers "$MAX_CALLERS" \
        --output "$OUT"
done

echo "==> all scenarios complete; outputs under $OUT_DIR/"
