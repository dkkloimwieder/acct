#!/usr/bin/env bash
# Strict-mode Path A depth curve (acct-0at4.9 deliverable A).
#
# WHAT IT PRODUCES
#   The linear-in-depth curve that is the ENTIRE motivation for Path C
#   (design-v3.1 §11.2 / §18, FEEDBACK-TESTING #9). Path C's headline is that
#   per-trx lock-hold is FLAT as pool depth grows (its hot path never reads
#   layer rows). Strict Path A — ledger-v3's `ledger_submit_trx`, the full
#   synchronous recompute — must show the CONTRASTING shape: per-trx cost
#   growing ~linearly with pool depth, because it hydrates the pool's ENTIRE
#   cost-layer state (ledger-direct/src/hydration.rs:44, no LIMIT) and the FIFO
#   walk is O(depth·log depth) (ledger-core/src/layered.rs:144,153). Without
#   this curve, the flat-vs-depth result proves nothing about the DECISION.
#
# WHY A MICROBENCH, NOT THE v3 HARNESS
#   The v3 `run` workload is RECEIPTS-ONLY (workload.rs:76) and receipts on a
#   FIFO pool APPEND a layer each — so depth drifts within a run and cannot be
#   pinned. This driver instead exercises the same `ledger_submit_trx` SPI
#   directly at a CONTROLLED, constant depth: it pre-seeds D layers per pool,
#   each with a huge per-layer qty, so 1-unit depletions never empty the front
#   layer — depth stays exactly D across the whole window while every call still
#   pays the full O(D) hydration + walk. Per-call latency is measured
#   server-side (clock_timestamp deltas, single tx) to isolate the O(depth)
#   COMPUTE term from client round-trip / fsync / cross-caller-contention noise
#   — the depth-independent constants that would otherwise swamp the signal on
#   this noisy workstation. The structural shape (FIFO linear vs WAC flat) is
#   the load-robust finding.
#
# BUILT-IN CONTROL
#   The same bench runs against WAC pools (a single cumulative pool_state row,
#   O(1) by construction). WAC must be FLAT across the depth axis; FIFO must
#   grow. Same code path, same box, same minute — only the method differs — so
#   a rising FIFO curve next to a flat WAC line cannot be an artifact of
#   depth-correlated measurement noise.
#
# USAGE
#   bash pathA-strict-depth-curve.sh
#   DEPTHS="1 10 100 1000 10000" ITERS=2000 POOLS=64 bash pathA-strict-depth-curve.sh
#
# ENV
#   DSN       Path A database DSN                     (default poc_v3)
#   DEPTHS    layer-depth axis, space-separated       (default 1 10 100 1000 5000 10000)
#   ITERS     depletions timed per (method,depth)     (default 2000)
#   POOLS     distinct FIFO/WAC pools cycled          (default 64)
#   METHODS   which methods to sweep                  (default "fifo wac")
#   CONTAINER docker container name                   (default acct-postgres)
#   OUTFILE   JSON results output                     (default results/pathA-depth-<ts>.json)

set -uo pipefail

DSN="${DSN:-postgres://acct:acct_dev@localhost:5111/poc_v3}"
DEPTHS="${DEPTHS:-1 10 100 1000 5000 10000}"
ITERS="${ITERS:-2000}"
POOLS="${POOLS:-64}"
METHODS="${METHODS:-fifo wac}"
CONTAINER="${CONTAINER:-acct-postgres}"

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TS="$(date -u +%Y-%m-%dT%H-%M-%S)"
OUTFILE="${OUTFILE:-$WORKSPACE_DIR/results/pathA-depth-${TS}.json}"

command -v jq >/dev/null || { echo "FATAL: jq required" >&2; exit 2; }

# The host DSN (localhost:5111) does not resolve inside the container, where PG
# listens on 5432. Connect container-local via -U/-d parsed from the DSN.
DBNAME="${DSN##*/}"
DBUSER="$(printf '%s' "$DSN" | sed -nE 's#.*://([^:/@]+).*#\1#p')"
DBUSER="${DBUSER:-acct}"
psql() { docker exec -i "$CONTAINER" psql -U "$DBUSER" -d "$DBNAME" "$@"; }

echo "==> Path A strict depth curve: methods={$METHODS} depths={$DEPTHS} iters=$ITERS pools=$POOLS"
echo "==> DSN=$DSN"

# ── The server-side timed microbench function. Resets transactional state,
#    injects the requested depth, then times ITERS single-unit depletions.
psql -v ON_ERROR_STOP=1 -q <<'SQL'
CREATE OR REPLACE FUNCTION bench_path_a(p_method text, p_depth bigint, p_iters int, p_pools int)
RETURNS TABLE(
    method text, depth bigint, iters int, pools int,
    p50_us double precision, p95_us double precision, p99_us double precision,
    mean_us double precision, min_us double precision, max_us double precision,
    wall_ms double precision, throughput_tps double precision)
LANGUAGE plpgsql AS $$
DECLARE
    v_lat  double precision[] := ARRAY[]::double precision[];
    v_t0   timestamptz;
    v_w0   timestamptz;
    v_pid  bigint;
    v_i    int;
    v_line jsonb;
BEGIN
    -- Reset transactional state; keep pool/sku/location/account.
    TRUNCATE posting_lines_provisional, posting_line_dimension, posting_line,
             trx_line, trx, pool_state, pool_lock RESTART IDENTITY;

    -- Inject controlled depth. Huge per-layer qty ⇒ 1-unit depletions never
    -- empty the front FIFO layer ⇒ depth stays exactly p_depth for the window.
    IF p_method IN ('fifo','lifo','specific') THEN
        INSERT INTO pool_state (pool_id, layer_seq, qty, unit_cost, last_trx_line_id)
        SELECT p.id, gs, 1000000000, 100, 0
        FROM pool p CROSS JOIN generate_series(1, p_depth) AS gs
        WHERE p.method = p_method::pool_method;
    ELSIF p_method IN ('wac','wac_periodic') THEN
        -- Single cumulative row: qty=qty_sum, unit_cost=value_sum (a TOTAL).
        INSERT INTO pool_state (pool_id, layer_seq, qty, unit_cost, last_trx_line_id)
        SELECT p.id, 0, 1000000000, 100000000000, 0
        FROM pool p WHERE p.method = p_method::pool_method;
    END IF;
    -- std: no pool_state rows; nothing to inject.

    v_w0 := clock_timestamp();
    FOR v_i IN 1..p_iters LOOP
        v_pid := 1 + (v_i % p_pools);
        v_line := jsonb_build_array(jsonb_build_object(
            'pool_id', v_pid, 'line_type', 'transfer_shipment_line',
            'qty', -1, 'unit_cost', 100,
            'debit_account', 2, 'credit_account', 1));
        v_t0 := clock_timestamp();
        PERFORM ledger_submit_trx('transfer_shipment', v_i,
                                  '2026-05-21T12:00:00+00:00', v_line);
        v_lat := array_append(v_lat,
                    (extract(epoch FROM clock_timestamp() - v_t0) * 1e6)::double precision);
    END LOOP;

    RETURN QUERY
    SELECT p_method, p_depth, p_iters, p_pools,
           percentile_cont(0.5)  WITHIN GROUP (ORDER BY x),
           percentile_cont(0.95) WITHIN GROUP (ORDER BY x),
           percentile_cont(0.99) WITHIN GROUP (ORDER BY x),
           avg(x), min(x), max(x),
           (extract(epoch FROM clock_timestamp() - v_w0) * 1e3)::double precision,
           (p_iters / GREATEST(extract(epoch FROM clock_timestamp() - v_w0), 1e-9))::double precision
    FROM unnest(v_lat) AS x;
END;
$$;
SQL
[ $? -ne 0 ] && { echo "FATAL: bench function install failed" >&2; exit 2; }

rows="[]"
for method in $METHODS; do
    echo
    echo "==> setup universe: $POOLS pools, method=$method"
    # Full reset + fresh contiguous universe (pool ids 1..POOLS, accounts 1,2).
    psql -v ON_ERROR_STOP=1 -q -v pools="$POOLS" -v method="$method" <<'SQL'
TRUNCATE posting_lines_provisional, posting_line_dimension, posting_line,
         trx_line, trx, pool_state, pool_lock, pool, sku, location, account
         RESTART IDENTITY CASCADE;
INSERT INTO account (code, name, type) VALUES
    ('1000-inv','Inventory','asset'), ('2000-ap','AP Unsettled','liability');
INSERT INTO sku (code, name)
    SELECT format('SKU-%s', g), format('Seeded SKU %s', g)
    FROM generate_series(1, :pools) g;
INSERT INTO location (code, name) VALUES ('LOC-001','Seeded Loc 1');
INSERT INTO pool (sku_id, location_id, method)
    SELECT s.id, (SELECT id FROM location LIMIT 1), :'method'::pool_method
    FROM sku s ORDER BY s.id;
SQL
    [ $? -ne 0 ] && { echo "FATAL: universe setup failed for $method" >&2; exit 2; }

    printf '\n%-8s %-8s  %-10s %-10s %-10s %-10s  %-10s\n' \
        "method" "depth" "p50(us)" "p95(us)" "p99(us)" "mean(us)" "tput(tps)"
    printf -- '--------------------------------------------------------------------------\n'

    for depth in $DEPTHS; do
        # WAC/STD have no layer depth — measure once at the natural state and
        # label it depth=1 (the O(1) reference); skip the redundant re-runs.
        if [ "$method" != "fifo" ] && [ "$method" != "lifo" ] && [ "$method" != "specific" ] && [ "$depth" != "1" ]; then
            continue
        fi
        csv=$(psql -tAF',' -c \
            "SELECT method,depth,iters,pools,p50_us,p95_us,p99_us,mean_us,min_us,max_us,wall_ms,throughput_tps FROM bench_path_a('$method',$depth,$ITERS,$POOLS);" 2>/dev/null)
        if [ -z "$csv" ]; then echo "  !! ($method,$depth) produced no row" >&2; continue; fi
        IFS=',' read -r r_m r_d r_it r_p r_p50 r_p95 r_p99 r_mean r_min r_max r_wall r_tps <<<"$csv"
        printf '%-8s %-8s  %-10.1f %-10.1f %-10.1f %-10.1f  %-10.1f\n' \
            "$r_m" "$r_d" "$r_p50" "$r_p95" "$r_p99" "$r_mean" "$r_tps"
        rows=$(echo "$rows" | jq \
            --arg m "$r_m" --argjson d "$r_d" --argjson it "$r_it" --argjson p "$r_p" \
            --argjson p50 "$r_p50" --argjson p95 "$r_p95" --argjson p99 "$r_p99" \
            --argjson mean "$r_mean" --argjson mn "$r_min" --argjson mx "$r_max" \
            --argjson wall "$r_wall" --argjson tps "$r_tps" \
            '. + [{method:$m, depth:$d, iters:$it, pools:$p, p50_us:$p50, p95_us:$p95, p99_us:$p99, mean_us:$mean, min_us:$mn, max_us:$mx, wall_ms:$wall, throughput_tps:$tps}]')
    done
done

mkdir -p "$(dirname "$OUTFILE")"
jq -n --arg ts "$TS" --argjson iters "$ITERS" --argjson pools "$POOLS" \
    --arg dsn "$DSN" --argjson steps "$rows" \
    '{measurement:"pathA_strict_depth_curve", ts:$ts, iters:$iters, pools:$pools, dsn:$dsn, metric:"per_call server compute latency (clock_timestamp deltas, single tx, fsync-excluded)", steps:$steps}' \
    > "$OUTFILE"
echo
echo "==> results: $OUTFILE"
