#!/usr/bin/env bash
# Offline strict-FIFO/LIFO replay oracle driver (acct-0at4.7).
#
# 1. Reseeds poc_v3_1 all-fifo depth-10 and drives a mixed receipts+depletions
#    scenario (s18) to produce a real Path C trx_line stream.
# 2. Dumps pool / seed-layers / lines to TSV.
# 3. Runs the replay oracle in all three modes (ordering, synth, real), emitting
#    the markdown tables to stdout. bench/replay-oracle-results.md is the authored
#    writeup that wraps these tables with the premise verdict; redirect stdout to
#    regenerate its table bodies.
#
# The oracle is a standalone (workspace-excluded) crate — see bench/replay-oracle/.
# It depends on the REAL ledger-core, so the "recorded provisional" arm is the
# shipped transform, not a re-implementation. Characterization only (read-only
# analysis of what the hot path recorded); no docker restart needed since the
# --method-mix reseed TRUNCATEs to a clean fixture.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
DSN='postgres://acct:acct_dev@localhost:5111/poc_v3_1'
HARNESS="$ROOT/target/release/ledger-harness"
ORACLE="$HERE/replay-oracle/target/release/replay-oracle"
DUMP="$(mktemp -d)"
trap 'rm -rf "$DUMP"' EXIT

echo "== build oracle ==" >&2
( cd "$HERE/replay-oracle" && cargo build --release >&2 )

echo "== drive s18 (all-fifo, depth 10, mixed receipts+depletions) ==" >&2
"$HARNESS" run \
  --scenario s18 --mode direct-per-call --duration 15s \
  --method-mix all-fifo --seed-count 10000 --seed-skus 1000 --seed-locations 10 \
  --seed-depth 10 --max-callers 50 --no-sampler >&2

echo "== dump pool / seed-layers / lines to TSV ==" >&2
psql "$DSN" -v ON_ERROR_STOP=1 >&2 <<SQL
\copy (SELECT id, method::text, provisional_basis::text FROM pool) TO '$DUMP/pool.tsv' (FORMAT csv, DELIMITER E'\t')
\copy (SELECT pool_id, layer_id, qty, unit_cost FROM pool_state WHERE layer_id <> 0) TO '$DUMP/layers.tsv' (FORMAT csv, DELIMITER E'\t')
\copy (SELECT tl.id, tl.pool_id, tl.line_type::text, tl.qty, tl.unit_cost, t.posted_at::text FROM trx_line tl JOIN trx t ON t.id = tl.trx_id ORDER BY tl.pool_id, tl.id) TO '$DUMP/lines.tsv' (FORMAT csv, DELIMITER E'\t')
SQL

"$ORACLE" ordering
"$ORACLE" synth
"$ORACLE" real "$DUMP"
