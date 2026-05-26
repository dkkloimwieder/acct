//! Bulk-write helpers (direct §5.1 step 8 / routed §6.4 step 10).
//!
//! The four primitives, in FK order:
//!   1. `insert_trx`                  — RETURNING trx.id (1 row)
//!   2. `insert_trx_lines`            — UNNEST INSERT RETURNING trx_line.ids (input order)
//!   3. `apply_pool_state_mutations`  — aggregate UPSERT / layer INSERT / layer DELETE
//!   4. `insert_posting_lines`        — UNNEST INSERT
//!
//! Direct flavor (`submit::ledger_submit_trx_c`) calls `apply_plan_result`, the
//! per-submission convenience wrapper that runs all four in order. The routed
//! committer (`committer::write_commit_group`) drives the primitives itself: it
//! calls `insert_trx` / `insert_trx_lines` / `insert_posting_lines` once per
//! submission, applies only each submission's *layer* mutations (specific pools)
//! inline, and writes the *aggregate* row once per pool from the final working
//! snapshot — that collapse is how a whole commit_group's depletions become one
//! aggregate UPSERT (§6.7), so it does not use `apply_plan_result`.
//!
//! v3.1 deltas vs the strict bulk_write: pool_state carries no `value_sum` /
//! `last_trx_line_id` and trx_line has no `trx_seq`. Mutations are
//! `UpsertAggregate` (layer_id = 0), `InsertLayer` (layer_id = the receipt
//! trx_line.id, resolved from RETURNING), and `DeleteLayer`. There is no
//! provisional-posting side table (that is recalc/close, out of scope §13).

use chrono::{DateTime, Utc};
use ledger_core::{PlanResult, PoolStateMutation, PostingLineRequest, TrxLineOutput};
use pgrx::prelude::*;

/// 8.1 — INSERT INTO trx. Returns the new trx.id.
///
/// A duplicate `(trx_type, source_id)` violates the table's UNIQUE constraint
/// and raises here, aborting the caller's tx — the direct-flavor idempotency
/// backstop (§2.2).
pub fn insert_trx(
    trx_type: &str,
    source_id: i64,
    posted_at: DateTime<Utc>,
) -> Result<i64, pgrx::spi::Error> {
    let id: Option<i64> = Spi::get_one_with_args(
        "INSERT INTO trx (trx_type, source_id, posted_at) \
         VALUES ($1::text::trx_type, $2, $3::text::timestamptz) \
         RETURNING id",
        &[
            trx_type.to_string().into(),
            source_id.into(),
            posted_at.to_rfc3339().into(),
        ],
    )?;
    Ok(id.unwrap_or_default())
}

/// 8.2 — Bulk INSERT INTO trx_line ... RETURNING id, realigned to input order.
///
/// trx_line.id is `GENERATED ALWAYS AS IDENTITY`; identity values are drawn in
/// the order the INSERT...SELECT feeds rows, and `ORDER BY ord` fixes that order
/// to the input array order. So sorting the RETURNING ids ascending recovers the
/// input-order alignment without depending on RETURNING's own row order. The
/// returned Vec is index-aligned to `outputs`, consumable by the mutation and
/// posting-line helpers.
pub fn insert_trx_lines(
    trx_id: i64,
    outputs: &[TrxLineOutput],
) -> Result<Vec<i64>, pgrx::spi::Error> {
    if outputs.is_empty() {
        return Ok(Vec::new());
    }

    let pool_id: Vec<i64> = outputs.iter().map(|o| o.pool_id).collect();
    let line_type: Vec<String> =
        outputs.iter().map(|o| o.line_type.as_sql().to_string()).collect();
    let source_id: Vec<Option<i64>> = outputs.iter().map(|o| o.source_id).collect();
    let qty: Vec<i64> = outputs.iter().map(|o| o.qty).collect();
    let unit_cost: Vec<i64> = outputs.iter().map(|o| o.unit_cost).collect();
    let source_trx_line_id: Vec<Option<i64>> =
        outputs.iter().map(|o| o.source_trx_line_id).collect();

    let mut ids: Vec<i64> = Spi::connect(|client| -> Result<Vec<i64>, pgrx::spi::Error> {
        let mut out = Vec::with_capacity(outputs.len());
        let mut t = client.select(
            "INSERT INTO trx_line \
               (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id) \
             SELECT $1, pid, lt::line_type, sid, q, uc, stl \
               FROM UNNEST($2::bigint[], $3::text[], $4::bigint[], $5::bigint[], $6::bigint[], $7::bigint[]) \
                    WITH ORDINALITY AS t(pid, lt, sid, q, uc, stl, ord) \
              ORDER BY ord \
             RETURNING id",
            None,
            &[
                trx_id.into(),
                pool_id.into(),
                line_type.into(),
                source_id.into(),
                qty.into(),
                unit_cost.into(),
                source_trx_line_id.into(),
            ],
        )?;
        while let Some(row) = t.next() {
            out.push(row.get::<i64>(1)?.unwrap_or(0));
        }
        Ok(out)
    })?;

    ids.sort_unstable();
    debug_assert_eq!(ids.len(), outputs.len(), "trx_line RETURNING dropped a row");
    Ok(ids)
}

/// 8.3 — Apply pool_state mutations in three batches: aggregate UPSERT, layer
/// INSERT, layer DELETE. Each batch skips its SPI when empty.
///
/// `InsertLayer` resolves `layer_trx_line_idx` against `trx_line_ids` (the
/// vector returned by `insert_trx_lines`): the layer's `layer_id` is the
/// receipt's own trx_line.id.
pub fn apply_pool_state_mutations(
    mutations: &[PoolStateMutation],
    trx_line_ids: &[i64],
) -> Result<(), pgrx::spi::Error> {
    if mutations.is_empty() {
        return Ok(());
    }

    // Aggregate upserts (layer_id = 0).
    let mut up_pid = Vec::new();
    let mut up_qty = Vec::new();
    let mut up_uc = Vec::new();

    // Materialized layer inserts (specific receipts).
    let mut ins_pid = Vec::new();
    let mut ins_lid = Vec::new();
    let mut ins_qty = Vec::new();
    let mut ins_uc = Vec::new();

    // Materialized layer deletes (specific depletions).
    let mut del_pid = Vec::new();
    let mut del_lid = Vec::new();

    for m in mutations {
        match *m {
            PoolStateMutation::UpsertAggregate { pool_id, qty, unit_cost } => {
                up_pid.push(pool_id);
                up_qty.push(qty);
                up_uc.push(unit_cost);
            }
            PoolStateMutation::InsertLayer {
                pool_id,
                layer_trx_line_idx,
                qty,
                unit_cost,
            } => {
                ins_pid.push(pool_id);
                ins_lid.push(trx_line_ids[layer_trx_line_idx]);
                ins_qty.push(qty);
                ins_uc.push(unit_cost);
            }
            PoolStateMutation::DeleteLayer { pool_id, layer_id } => {
                del_pid.push(pool_id);
                del_lid.push(layer_id);
            }
        }
    }

    if !up_pid.is_empty() {
        Spi::run_with_args(
            "INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost) \
             SELECT pid, 0, q, uc \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[]) AS t(pid, q, uc) \
             ON CONFLICT (pool_id, layer_id) DO UPDATE \
                SET qty = EXCLUDED.qty, unit_cost = EXCLUDED.unit_cost",
            &[up_pid.into(), up_qty.into(), up_uc.into()],
        )?;
    }

    if !ins_pid.is_empty() {
        Spi::run_with_args(
            "INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost) \
             SELECT pid, lid, q, uc \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[]) \
                    AS t(pid, lid, q, uc)",
            &[ins_pid.into(), ins_lid.into(), ins_qty.into(), ins_uc.into()],
        )?;
    }

    if !del_pid.is_empty() {
        Spi::run_with_args(
            "DELETE FROM pool_state \
              USING UNNEST($1::bigint[], $2::bigint[]) AS d(pid, lid) \
              WHERE pool_state.pool_id = d.pid AND pool_state.layer_id = d.lid",
            &[del_pid.into(), del_lid.into()],
        )?;
    }

    Ok(())
}

/// 8.4 — Bulk INSERT INTO posting_line. `trx_line_idx` resolves against the
/// input-order `trx_line_ids` from step 8.2.
pub fn insert_posting_lines(
    requests: &[PostingLineRequest],
    trx_line_ids: &[i64],
) -> Result<(), pgrx::spi::Error> {
    if requests.is_empty() {
        return Ok(());
    }

    let tl_id: Vec<i64> = requests.iter().map(|r| trx_line_ids[r.trx_line_idx]).collect();
    let event_type: Vec<String> =
        requests.iter().map(|r| r.event_type.as_sql().to_string()).collect();
    let amount: Vec<i64> = requests.iter().map(|r| r.amount).collect();
    let debit: Vec<i64> = requests.iter().map(|r| r.debit_account).collect();
    let credit: Vec<i64> = requests.iter().map(|r| r.credit_account).collect();
    let posted_at: Vec<String> = requests.iter().map(|r| r.posted_at.to_rfc3339()).collect();

    Spi::run_with_args(
        "INSERT INTO posting_line \
           (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
         SELECT tl, et::posting_event_type, amt, deb, cr, pa::timestamptz \
           FROM UNNEST($1::bigint[], $2::text[], $3::bigint[], $4::bigint[], $5::bigint[], $6::text[]) \
                AS t(tl, et, amt, deb, cr, pa)",
        &[
            tl_id.into(),
            event_type.into(),
            amount.into(),
            debit.into(),
            credit.into(),
            posted_at.into(),
        ],
    )?;
    Ok(())
}

/// Run the full §5.1 step 8 sequence and return the new trx.id. The direct-flavor
/// convenience wrapper; the routed committer drives the primitives directly.
pub fn apply_plan_result(
    trx_type: &str,
    source_id: i64,
    posted_at: DateTime<Utc>,
    plan: &PlanResult,
) -> Result<i64, pgrx::spi::Error> {
    let trx_id = insert_trx(trx_type, source_id, posted_at)?;
    let trx_line_ids = insert_trx_lines(trx_id, &plan.trx_lines)?;
    apply_pool_state_mutations(&plan.pool_state_mutations, &trx_line_ids)?;
    insert_posting_lines(&plan.posting_lines, &trx_line_ids)?;
    Ok(trx_id)
}
