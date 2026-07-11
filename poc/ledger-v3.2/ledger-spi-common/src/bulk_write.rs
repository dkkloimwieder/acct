//! Bulk-write helpers for the ledger-core dispatch paths (STD / specific).
//!
//! The primitives, in FK order:
//!   1. `insert_trx`                  — RETURNING trx.id (1 row)
//!   2. `insert_trx_lines`            — UNNEST INSERT RETURNING trx_line.ids (input order)
//!   3. `apply_pool_state_mutations`  — aggregate UPSERT / layer INSERT / layer DELETE
//!   4. `insert_posting_lines`        — UNNEST INSERT
//!
//! `ledger-direct`'s submit path inserts the trx itself (the row is shared with
//! the commutative-CTE lines of the same submission), then calls
//! `apply_plan_result` with the known trx_id to write a plan's lines, mutations,
//! and postings.
//!
//! trx_line writes carry the denormalized `posted_at` (recalc-d D1): every line
//! of a submission stamps the trx's business time, which is the recalc engine's
//! R-1 ordering key. The write path owns supplying the TRUE business time.
//!
//! Each statement is parsed + planned once per backend and kept for the life of
//! the process (`SPI_keepplan`, via pgrx's `prepare_mut(..).keep()`), cached in a
//! `thread_local`: `SPI_execute_plan` reruns the cached generic plan with new
//! parameters and skips the parse/analyze/plan pipeline. The statements are
//! parameterized, so one generic plan serves all calls; the plancache
//! revalidates and replans transparently if the schema changes.

use std::cell::RefCell;
use std::thread::LocalKey;

use chrono::{DateTime, Utc};
use ledger_core::{PlanResult, PoolStateMutation, PostingLineRequest, TrxLineOutput};
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::PgOid;
use pgrx::prelude::*;
use pgrx::spi::{OwnedPreparedStatement, SpiTupleTable};

// Type tags for `oids_of!` — the param-type OIDs handed to `prepare_mut` come
// from the same `IntoDatum::type_oid()` the args' `.into()` uses, so prepare-time
// and execute-time OIDs match by construction (`From<T> for DatumWithOid`).
type Int8 = i64;
type Text = String;
type Int8Arr = Vec<i64>;
type Int8ArrNullable = Vec<Option<i64>>;
type TextArr = Vec<String>;

thread_local! {
    static PLAN_INSERT_TRX: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_INSERT_TRX_LINES: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_UPSERT_AGGREGATE: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_INSERT_LAYER: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_DELETE_LAYER: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_INSERT_POSTING_LINES: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
}

/// Execute a kept (`SPI_keepplan`) prepared statement, preparing + keeping it on
/// first use for this backend. All callers mutate, so the plan is built with
/// `prepare_mut` (read_only = false) and run via `update` (marks the xact mutable).
/// `read_rows` consumes the result table — RETURNING readers collect ids, the
/// write-only statements ignore it.
fn run_prepared<R>(
    slot: &'static LocalKey<RefCell<Option<OwnedPreparedStatement>>>,
    sql: &str,
    arg_oids: &[PgOid],
    args: &[DatumWithOid<'_>],
    read_rows: impl FnOnce(SpiTupleTable<'_>) -> Result<R, pgrx::spi::Error>,
) -> Result<R, pgrx::spi::Error> {
    Spi::connect_mut(|client| {
        slot.with_borrow_mut(|opt| -> Result<(), pgrx::spi::Error> {
            if opt.is_none() {
                *opt = Some(client.prepare_mut(sql, arg_oids)?.keep());
            }
            Ok(())
        })?;
        slot.with_borrow(|opt| {
            let plan = opt.as_ref().expect("plan prepared above");
            let table = client.update(plan, None, args)?;
            read_rows(table)
        })
    })
}

/// 1 — INSERT INTO trx. Returns the new trx.id.
///
/// A duplicate `(trx_type, source_id)` violates the table's UNIQUE constraint
/// and raises here, aborting the caller's tx — the idempotency backstop (§2.2).
pub fn insert_trx(
    trx_type: &str,
    source_id: i64,
    posted_at: DateTime<Utc>,
) -> Result<i64, pgrx::spi::Error> {
    let args: [DatumWithOid<'_>; 3] = [
        trx_type.to_string().into(),
        source_id.into(),
        posted_at.to_rfc3339().into(),
    ];
    run_prepared(
        &PLAN_INSERT_TRX,
        "INSERT INTO trx (trx_type, source_id, posted_at) \
         VALUES ($1::text::trx_type, $2, $3::text::timestamptz) \
         RETURNING id",
        &pgrx::oids_of![Text, Int8, Text],
        &args,
        |mut table| {
            Ok(match table.next() {
                Some(row) => row.get::<i64>(1)?.unwrap_or_default(),
                None => 0,
            })
        },
    )
}

/// 2 — Bulk INSERT INTO trx_line ... RETURNING id, realigned to input order.
///
/// trx_line.id is `GENERATED ALWAYS AS IDENTITY`; identity values are drawn in
/// the order the INSERT...SELECT feeds rows, and `ORDER BY ord` fixes that order
/// to the input array order. So sorting the RETURNING ids ascending recovers the
/// input-order alignment without depending on RETURNING's own row order. The
/// returned Vec is index-aligned to `outputs`, consumable by the mutation and
/// posting-line helpers.
///
/// Every line stamps the submission's `posted_at` (the D1 denorm — recalc's R-1
/// ordering key).
pub fn insert_trx_lines(
    trx_id: i64,
    posted_at: DateTime<Utc>,
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

    let args: [DatumWithOid<'_>; 8] = [
        trx_id.into(),
        pool_id.into(),
        line_type.into(),
        source_id.into(),
        qty.into(),
        unit_cost.into(),
        source_trx_line_id.into(),
        posted_at.to_rfc3339().into(),
    ];

    let mut ids = run_prepared(
        &PLAN_INSERT_TRX_LINES,
        "INSERT INTO trx_line \
           (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id, posted_at) \
         SELECT $1, pid, lt::line_type, sid, q, uc, stl, $8::text::timestamptz \
           FROM UNNEST($2::bigint[], $3::text[], $4::bigint[], $5::bigint[], $6::bigint[], $7::bigint[]) \
                WITH ORDINALITY AS t(pid, lt, sid, q, uc, stl, ord) \
          ORDER BY ord \
         RETURNING id",
        &pgrx::oids_of![Int8, Int8Arr, TextArr, Int8ArrNullable, Int8Arr, Int8Arr, Int8ArrNullable, Text],
        &args,
        |mut table| {
            let mut out = Vec::with_capacity(outputs.len());
            while let Some(row) = table.next() {
                out.push(row.get::<i64>(1)?.unwrap_or(0));
            }
            Ok(out)
        },
    )?;

    ids.sort_unstable();
    debug_assert_eq!(ids.len(), outputs.len(), "trx_line RETURNING dropped a row");
    Ok(ids)
}

/// 3 — Apply pool_state mutations in three batches: aggregate UPSERT, layer
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
    let mut up_vs = Vec::new();

    // Materialized layer inserts (specific receipts).
    let mut ins_pid = Vec::new();
    let mut ins_lid = Vec::new();
    let mut ins_qty = Vec::new();
    let mut ins_uc = Vec::new();
    let mut ins_vs = Vec::new();

    // Materialized layer deletes (specific depletions).
    let mut del_pid = Vec::new();
    let mut del_lid = Vec::new();

    for m in mutations {
        match *m {
            PoolStateMutation::UpsertAggregate { pool_id, qty, unit_cost, value_sum } => {
                up_pid.push(pool_id);
                up_qty.push(qty);
                up_uc.push(unit_cost);
                up_vs.push(value_sum);
            }
            PoolStateMutation::InsertLayer {
                pool_id,
                layer_trx_line_idx,
                qty,
                unit_cost,
                value_sum,
            } => {
                ins_pid.push(pool_id);
                ins_lid.push(trx_line_ids[layer_trx_line_idx]);
                ins_qty.push(qty);
                ins_uc.push(unit_cost);
                ins_vs.push(value_sum);
            }
            PoolStateMutation::DeleteLayer { pool_id, layer_id } => {
                del_pid.push(pool_id);
                del_lid.push(layer_id);
            }
        }
    }

    if !up_pid.is_empty() {
        let args: [DatumWithOid<'_>; 4] =
            [up_pid.into(), up_qty.into(), up_uc.into(), up_vs.into()];
        run_prepared(
            &PLAN_UPSERT_AGGREGATE,
            "INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum) \
             SELECT pid, 0, q, uc, vs \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[]) \
                    AS t(pid, q, uc, vs) \
             ON CONFLICT (pool_id, layer_id) DO UPDATE \
                SET qty = EXCLUDED.qty, unit_cost = EXCLUDED.unit_cost, \
                    value_sum = EXCLUDED.value_sum",
            &pgrx::oids_of![Int8Arr, Int8Arr, Int8Arr, Int8Arr],
            &args,
            |_table| Ok(()),
        )?;
    }

    if !ins_pid.is_empty() {
        let args: [DatumWithOid<'_>; 5] =
            [ins_pid.into(), ins_lid.into(), ins_qty.into(), ins_uc.into(), ins_vs.into()];
        run_prepared(
            &PLAN_INSERT_LAYER,
            "INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum) \
             SELECT pid, lid, q, uc, vs \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[]) \
                    AS t(pid, lid, q, uc, vs)",
            &pgrx::oids_of![Int8Arr, Int8Arr, Int8Arr, Int8Arr, Int8Arr],
            &args,
            |_table| Ok(()),
        )?;
    }

    if !del_pid.is_empty() {
        let args: [DatumWithOid<'_>; 2] = [del_pid.into(), del_lid.into()];
        run_prepared(
            &PLAN_DELETE_LAYER,
            "DELETE FROM pool_state \
              USING UNNEST($1::bigint[], $2::bigint[]) AS d(pid, lid) \
              WHERE pool_state.pool_id = d.pid AND pool_state.layer_id = d.lid",
            &pgrx::oids_of![Int8Arr, Int8Arr],
            &args,
            |_table| Ok(()),
        )?;
    }

    Ok(())
}

/// 4 — Bulk INSERT INTO posting_line. `trx_line_idx` resolves against the
/// input-order `trx_line_ids` from step 2.
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

    let args: [DatumWithOid<'_>; 6] = [
        tl_id.into(),
        event_type.into(),
        amount.into(),
        debit.into(),
        credit.into(),
        posted_at.into(),
    ];

    run_prepared(
        &PLAN_INSERT_POSTING_LINES,
        "INSERT INTO posting_line \
           (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
         SELECT tl, et::posting_event_type, amt, deb, cr, pa::timestamptz \
           FROM UNNEST($1::bigint[], $2::text[], $3::bigint[], $4::bigint[], $5::bigint[], $6::text[]) \
                AS t(tl, et, amt, deb, cr, pa)",
        &pgrx::oids_of![Int8Arr, TextArr, Int8Arr, Int8Arr, Int8Arr, TextArr],
        &args,
        |_table| Ok(()),
    )
}

/// Write a plan's lines, pool_state mutations, and posting lines against an
/// already-inserted trx. The caller owns the trx INSERT (`insert_trx`) because
/// one submission's trx row is shared between the ledger-core path and the
/// commutative-CTE lines.
pub fn apply_plan_result(
    trx_id: i64,
    posted_at: DateTime<Utc>,
    plan: &PlanResult,
) -> Result<(), pgrx::spi::Error> {
    let trx_line_ids = insert_trx_lines(trx_id, posted_at, &plan.trx_lines)?;
    apply_pool_state_mutations(&plan.pool_state_mutations, &trx_line_ids)?;
    insert_posting_lines(&plan.posting_lines, &trx_line_ids)?;
    Ok(())
}
