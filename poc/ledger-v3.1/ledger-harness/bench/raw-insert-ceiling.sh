#!/usr/bin/env bash
# Raw INSERT INTO trx_line substrate ceiling (acct-0at4.9 deliverable B).
#
# WHAT IT PRODUCES
#   The insert-only upper bound (FEEDBACK-TESTING #9 / design-v3.1 §18): plain
#   `INSERT INTO trx_line` throughput on the same box, with NO ledger logic
#   (no hydration, no plan_apply, no pool_state, no pool_lock). This bounds how
#   much of Path C's per-trx cost is the SUBSTRATE (row write + PK/FK
#   maintenance + WAL/fsync) versus the LEDGER (everything Path C does on top).
#   Path C's hot path still pays these same trx_line writes plus its own work;
#   the gap between this ceiling and Path C's achieved rate is the ledger tax.
#
#   Rows carry the real trx_line shape (valid trx_id + pool_id FKs, PK, enum
#   line_type) so the substrate cost includes the index/FK maintenance Path C
#   also pays — this is the honest substrate floor, not an unconstrained heap
#   append.
#
# THREE REGIMES bracket the substrate:
#   bulk         one multi-row INSERT, single commit  → fsync-AMORTIZED ceiling
#                (max raw bandwidth; the "batch throughput" the ask names)
#   batch-<N>    N rows per tx, commit each batch      → realistic drain rate
#                (mirrors the staging-committer SKIP LOCKED batch commit)
#   single-tx    1 row per tx, commit each             → fsync-BOUND floor
#                (the per-submission synchronous ceiling; direct-single sits
#                 above this floor and below the bulk ceiling)
#
# USAGE
#   bash raw-insert-ceiling.sh
#   BULK_ROWS=500000 SINGLE_ROWS=20000 bash raw-insert-ceiling.sh
#
# ENV
#   DSN         Path C database DSN            (default poc_v3_1)
#   BULK_ROWS   rows for the bulk + batch regimes   (default 200000)
#   SINGLE_ROWS rows for the single-tx regime       (default 10000)
#   BATCHES     batch sizes for batch-commit regime (default "200 1000")
#   CONTAINER   docker container name               (default acct-postgres)
#   OUTFILE     JSON results output                 (default results/raw-insert-<ts>.json)

set -uo pipefail

DSN="${DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3_1}"
BULK_ROWS="${BULK_ROWS:-200000}"
SINGLE_ROWS="${SINGLE_ROWS:-10000}"
BATCHES="${BATCHES:-200 1000}"
CONTAINER="${CONTAINER:-acct-postgres}"

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TS="$(date -u +%Y-%m-%dT%H-%M-%S)"
OUTFILE="${OUTFILE:-$WORKSPACE_DIR/results/raw-insert-${TS}.json}"

command -v jq >/dev/null || { echo "FATAL: jq required" >&2; exit 2; }
DBNAME="${DSN##*/}"
DBUSER="$(printf '%s' "$DSN" | sed -nE 's#.*://([^:/@]+).*#\1#p')"; DBUSER="${DBUSER:-acct}"
psql() { docker exec -i "$CONTAINER" psql -U "$DBUSER" -d "$DBNAME" "$@"; }

echo "==> raw INSERT ceiling: bulk=$BULK_ROWS batch={$BATCHES} single=$SINGLE_ROWS  DSN=$DSN"

# Sentinel trx to satisfy the trx_id FK (trx.id is GENERATED ALWAYS — omit it;
# unique source_id past the existing max avoids the (trx_type,source_id) UNIQUE).
TRX_ID=$(psql -tA -c "WITH ins AS (INSERT INTO trx (trx_type, source_id, posted_at) SELECT 'po_receipt', COALESCE(max(source_id),0)+1, now() FROM trx WHERE trx_type='po_receipt' RETURNING id) SELECT id FROM ins;" 2>/dev/null | grep -E '^[0-9]+$' | head -1)
if ! [[ "$TRX_ID" =~ ^[0-9]+$ ]]; then echo "FATAL: bad sentinel trx_id='$TRX_ID'" >&2; exit 2; fi
echo "==> sentinel trx_id=$TRX_ID"

psql -v ON_ERROR_STOP=1 -q <<'SQL'
CREATE OR REPLACE PROCEDURE bench_raw_insert(p_rows int, p_batch int, p_trx_id bigint)
LANGUAGE plpgsql AS $$
DECLARE
    v_i    int := 0;
    v_t0   timestamptz;
    v_secs double precision;
BEGIN
    -- trx_line.id is GENERATED ALWAYS AS IDENTITY — omit it; identity-sequence
    -- cost is legitimately part of the substrate write that Path C also pays.
    v_t0 := clock_timestamp();
    IF p_batch <= 0 THEN
        INSERT INTO trx_line (trx_id, pool_id, line_type, source_id, qty, unit_cost)
        SELECT p_trx_id, 1 + (gs % 1000), 'po_receipt_line', NULL, 1, 100
        FROM generate_series(1, p_rows) AS gs;
        COMMIT;
    ELSE
        WHILE v_i < p_rows LOOP
            INSERT INTO trx_line (trx_id, pool_id, line_type, source_id, qty, unit_cost)
            SELECT p_trx_id, 1 + ((v_i + gs) % 1000), 'po_receipt_line', NULL, 1, 100
            FROM generate_series(1, LEAST(p_batch, p_rows - v_i)) AS gs;
            COMMIT;
            v_i := v_i + p_batch;
        END LOOP;
    END IF;
    v_secs := extract(epoch FROM clock_timestamp() - v_t0);
    RAISE NOTICE 'RESULT rows=% batch=% secs=% rows_per_sec=%',
        p_rows, p_batch, round(v_secs::numeric, 4),
        round((p_rows / GREATEST(v_secs, 1e-9))::numeric, 1);
END;
$$;
SQL

rows="[]"
run_regime() { # label rows batch
    local label="$1" nrows="$2" batch="$3"
    local out rps secs
    out=$(psql -c "CALL bench_raw_insert($nrows, $batch, $TRX_ID);" 2>&1)
    rps=$(printf '%s' "$out" | sed -nE 's/.*rows_per_sec=([0-9.]+).*/\1/p' | tail -1)
    secs=$(printf '%s' "$out" | sed -nE 's/.*secs=([0-9.]+).*/\1/p' | tail -1)
    if [ -z "$rps" ]; then echo "  !! regime $label produced no result:"; printf '%s\n' "$out" | tail -3; return; fi
    printf '%-14s %-10s %-12s %-14s\n' "$label" "$nrows" "$batch" "$rps"
    rows=$(echo "$rows" | jq --arg l "$label" --argjson n "$nrows" --argjson b "$batch" \
        --argjson rps "$rps" --argjson secs "${secs:-0}" \
        '. + [{regime:$l, rows:$n, batch:$b, rows_per_sec:$rps, secs:$secs}]')
}

printf '\n%-14s %-10s %-12s %-14s\n' "regime" "rows" "batch/tx" "rows/sec"
printf -- '------------------------------------------------------------\n'
run_regime "bulk"        "$BULK_ROWS" 0
for b in $BATCHES; do run_regime "batch-$b" "$BULK_ROWS" "$b"; done
run_regime "single-tx"   "$SINGLE_ROWS" 1

echo
echo "==> cleanup: removing $(psql -tAc "SELECT count(*) FROM trx_line WHERE trx_id=$TRX_ID;" 2>/dev/null) bench rows"
psql -q -c "DELETE FROM trx_line WHERE trx_id=$TRX_ID; DELETE FROM trx WHERE id=$TRX_ID;" >/dev/null 2>&1

mkdir -p "$(dirname "$OUTFILE")"
jq -n --arg ts "$TS" --arg dsn "$DSN" --argjson regimes "$rows" \
    '{measurement:"raw_insert_trx_line_ceiling", ts:$ts, dsn:$dsn, note:"no ledger logic; real trx_line shape (PK + trx_id/pool_id FK + enum line_type)", regimes:$regimes}' \
    > "$OUTFILE"
echo "==> results: $OUTFILE"
