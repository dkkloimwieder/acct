-- SPIKE-B (acct-0at4.11.2) differential correctness: the single-statement
-- commutative variant (ledger_submit_trx_single_c) must produce byte-identical
-- ledger state to the RMW baseline (ledger_submit_trx_c) on identical inputs.
--
-- Method: for each case, force ONE pool's aggregate to a controlled state, run
-- the BASELINE, snapshot (pool_state aggregate + the trx_line + the posting_line
-- it wrote), restore the SAME controlled state, run the SPIKE with identical
-- inputs (distinct source_id to dodge the trx UNIQUE), snapshot again. The final
-- SELECT reports, per case, whether the two flavors AGREE on every field. Running
-- on one pool means the posting accounts are identical too, so the diff covers
-- debit/credit/amount, not just the arithmetic.
--
-- Requires a seeded universe (run  ... --method-mix all-fifo --seed-depth N  first)
-- and banker_div installed (bench/spike-b-setup.sql).

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS spike_b_snap;
CREATE TEMP TABLE spike_b_snap (
    case_id   text,
    flavor    text,          -- 'baseline' | 'spike'
    ps_qty    bigint,
    ps_uc     bigint,
    ps_vs     bigint,
    tl_qty    bigint,
    tl_uc     bigint,
    pl_event  text,
    pl_amount bigint,
    pl_debit  bigint,
    pl_credit bigint
);

DO $$
DECLARE
    pid       bigint;
    ts        text := '2026-05-25T12:00:00+00:00';
    sid       bigint := 900000000;   -- high, unique source_id space for this probe
    -- each case is (case_id, controlled state qty/uc/vs OR NULL for empty,
    --               line_type, qty, unit_cost)
    r record;
    cases     text[][] := ARRAY[
        -- case,   qty0,  uc0,   vs0,   line_type,              qty,   uc
        ['R1_first_receipt',  NULL,  NULL,  NULL,  'po_receipt_line',       '100', '500'],
        ['R2_receipt_avg',    '100', '500', '50000','po_receipt_line',      '50',  '800'],
        ['R3_receipt_halfeven','1',  '1',   '1',   'po_receipt_line',       '1',   '2'],
        ['D1_deplete_avg',    '150', '600', '90000','transfer_shipment_line','-50', '0'],
        ['D2_deplete_empty',  '50',  '600', '30000','transfer_shipment_line','-50', '0'],
        ['D3_deplete_halfeven','3',  '2',   '7',   'transfer_shipment_line','-1',  '0']
    ];
    c         text[];
    flv       text;
    fn_source bigint;
    lines     jsonb;
    tlid      bigint;
BEGIN
    SELECT min(id) INTO pid FROM pool;
    IF pid IS NULL THEN
        RAISE EXCEPTION 'no pools seeded — run the harness reseed first';
    END IF;

    FOREACH c SLICE 1 IN ARRAY cases LOOP
        lines := jsonb_build_array(jsonb_build_object(
            'pool_id', pid, 'line_type', c[5], 'qty', c[6]::bigint, 'unit_cost', c[7]::bigint));

        FOREACH flv IN ARRAY ARRAY['baseline','spike'] LOOP
            sid := sid + 1;

            -- (re)establish the controlled aggregate state for this pool.
            DELETE FROM pool_state WHERE pool_id = pid AND layer_id = 0;
            IF c[2] IS NOT NULL THEN
                INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum)
                VALUES (pid, 0, c[2]::bigint, c[3]::bigint, c[4]::bigint);
            END IF;

            IF flv = 'baseline' THEN
                PERFORM ledger_submit_trx_c('po_receipt', sid, ts, lines);
            ELSE
                PERFORM ledger_submit_trx_single_c('po_receipt', sid, ts, lines);
            END IF;

            -- resolve the trx_line just written by this source_id.
            SELECT tl.id INTO tlid
              FROM trx_line tl JOIN trx t ON t.id = tl.trx_id
             WHERE t.source_id = sid;

            INSERT INTO spike_b_snap
            SELECT c[1], flv,
                   ps.qty, ps.unit_cost, ps.value_sum,
                   tl.qty, tl.unit_cost,
                   pl.event_type::text, pl.amount, pl.debit_account, pl.credit_account
              FROM trx_line tl
              LEFT JOIN pool_state ps ON ps.pool_id = pid AND ps.layer_id = 0
              LEFT JOIN posting_line pl ON pl.trx_line_id = tl.id
             WHERE tl.id = tlid;
        END LOOP;
    END LOOP;
END $$;

-- Per-case agreement verdict: baseline vs spike on every field.
SELECT b.case_id,
       (b.ps_qty  = s.ps_qty  AND b.ps_uc  = s.ps_uc  AND b.ps_vs = s.ps_vs
        AND b.tl_qty = s.tl_qty AND b.tl_uc = s.tl_uc
        AND b.pl_event = s.pl_event AND b.pl_amount = s.pl_amount
        AND b.pl_debit = s.pl_debit AND b.pl_credit = s.pl_credit) AS agree,
       s.ps_qty, s.ps_uc, s.ps_vs, s.tl_qty, s.tl_uc, s.pl_event, s.pl_amount
  FROM spike_b_snap b
  JOIN spike_b_snap s ON s.case_id = b.case_id AND s.flavor = 'spike'
 WHERE b.flavor = 'baseline'
 ORDER BY b.case_id;
