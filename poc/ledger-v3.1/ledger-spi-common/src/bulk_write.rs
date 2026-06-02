//! Bulk-write helpers (direct §5.1 step 8 / routed §6.4 step 10).
//!
//! The four primitives, in FK order:
//!   1. `insert_trx`                  — RETURNING trx.id (1 row)
//!   2. `insert_trx_lines`            — UNNEST INSERT RETURNING trx_line.ids (input order)
//!   3. `apply_pool_state_mutations`  — aggregate UPSERT / layer INSERT / layer DELETE
//!   4. `insert_posting_lines`        — UNNEST INSERT
//!
//! Direct flavor (`submit::ledger_submit_trx_c`) calls `apply_plan_result`, the
//! per-submission convenience wrapper that runs the per-submission primitives in
//! order. The routed committer (`committer::plan_and_write`) plans every
//! submission sequentially (WAC running average), then drives the *batch*
//! variants — `insert_trx_batch` (8.1b) / `insert_trx_lines_batch` (8.2b) /
//! `insert_posting_lines_batch` (8.4b) — to write the whole commit group in one
//! multi-row INSERT per table (acct-sczx Lever A). It applies each submission's
//! *layer* mutations (specific pools) against that submission's slice of the
//! batched trx_line ids and writes the *aggregate* row once per pool from the
//! final working snapshot — that collapse is how a whole commit_group's
//! depletions become one aggregate UPSERT (§6.7), so it does not use
//! `apply_plan_result`.
//!
//! Each statement is parsed + planned once per backend and kept for the life of
//! the process (`SPI_keepplan`, via pgrx's `prepare_mut(..).keep()`), cached in a
//! `thread_local`. The committer is a long-lived BGWorker, so a kept plan is
//! reused across every commit group: `SPI_execute_plan` reruns the cached generic
//! plan with new parameters and skips the parse/analyze/plan pipeline that
//! dominated the apply hot path (acct-q6sx: ~47% of committer CPU was per-call
//! parse+analyze+plan; acct-sczx Lever B). The statements are parameterized, so
//! one generic plan serves all calls; the plancache revalidates and replans
//! transparently if the schema changes.
//!
//! v3.1 deltas vs the strict bulk_write: pool_state carries `value_sum` (the
//! cumulative book value behind the running average, acct-0qps) but no
//! `last_trx_line_id`, and trx_line has no `trx_seq`. Mutations are
//! `UpsertAggregate` (layer_id = 0), `InsertLayer` (layer_id = the receipt
//! trx_line.id, resolved from RETURNING), and `DeleteLayer`. There is no
//! provisional-posting side table (that is recalc/close, out of scope §13).

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
    static PLAN_INSERT_TRX_BATCH: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_INSERT_TRX_LINES: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_INSERT_TRX_LINES_BATCH: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
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

/// 8.1b — Batch INSERT INTO trx for a whole commit group (acct-sczx Lever A).
/// Returns trx.ids realigned to input order. trx.id is GENERATED ALWAYS AS
/// IDENTITY; identity values are drawn in the order the INSERT...SELECT feeds
/// rows, `ORDER BY ord` fixes that to the input array order, so sorting the
/// RETURNING ids ascending recovers input-order alignment (same trick as 8.2).
/// A duplicate `(trx_type, source_id)` anywhere in the batch raises here; the
/// committer runs the whole phase in a subtransaction, so the abort rolls back
/// the entire attempt and the §6.8 re-drive re-queries `trx` to drop the
/// now-visible offender(s) — it never needs to know which row collided.
pub fn insert_trx_batch(
    trx_type: &[String],
    source_id: &[i64],
    posted_at: &[DateTime<Utc>],
) -> Result<Vec<i64>, pgrx::spi::Error> {
    debug_assert_eq!(trx_type.len(), source_id.len());
    debug_assert_eq!(trx_type.len(), posted_at.len());
    if trx_type.is_empty() {
        return Ok(Vec::new());
    }

    let tt: Vec<String> = trx_type.to_vec();
    let sid: Vec<i64> = source_id.to_vec();
    let pa: Vec<String> = posted_at.iter().map(|t| t.to_rfc3339()).collect();

    let args: [DatumWithOid<'_>; 3] = [tt.into(), sid.into(), pa.into()];

    let mut ids = run_prepared(
        &PLAN_INSERT_TRX_BATCH,
        "INSERT INTO trx (trx_type, source_id, posted_at) \
         SELECT tt::trx_type, sid, pa::timestamptz \
           FROM UNNEST($1::text[], $2::bigint[], $3::text[]) \
                WITH ORDINALITY AS t(tt, sid, pa, ord) \
          ORDER BY ord \
         RETURNING id",
        &pgrx::oids_of![TextArr, Int8Arr, TextArr],
        &args,
        |mut table| {
            let mut out = Vec::with_capacity(trx_type.len());
            while let Some(row) = table.next() {
                out.push(row.get::<i64>(1)?.unwrap_or(0));
            }
            Ok(out)
        },
    )?;

    ids.sort_unstable();
    debug_assert_eq!(ids.len(), trx_type.len(), "trx batch RETURNING dropped a row");
    Ok(ids)
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

    let args: [DatumWithOid<'_>; 7] = [
        trx_id.into(),
        pool_id.into(),
        line_type.into(),
        source_id.into(),
        qty.into(),
        unit_cost.into(),
        source_trx_line_id.into(),
    ];

    let mut ids = run_prepared(
        &PLAN_INSERT_TRX_LINES,
        "INSERT INTO trx_line \
           (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id) \
         SELECT $1, pid, lt::line_type, sid, q, uc, stl \
           FROM UNNEST($2::bigint[], $3::text[], $4::bigint[], $5::bigint[], $6::bigint[], $7::bigint[]) \
                WITH ORDINALITY AS t(pid, lt, sid, q, uc, stl, ord) \
          ORDER BY ord \
         RETURNING id",
        &pgrx::oids_of![Int8, Int8Arr, TextArr, Int8ArrNullable, Int8Arr, Int8Arr, Int8ArrNullable],
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

/// 8.2b — Batch INSERT INTO trx_line across a whole commit group (acct-sczx
/// Lever A). Identical to 8.2 except `trx_id` is a per-row array (`$1`) instead
/// of a scalar, so one INSERT carries every submission's trx_lines, each tagged
/// with its own submission's trx.id from 8.1b. The returned Vec is index-aligned
/// to `outputs` (the flattened, submission-ordered line stream) via the same
/// `ORDER BY ord` + ascending-sort recovery, so the caller can slice it back
/// per submission to resolve posting-line and layer-mutation references.
pub fn insert_trx_lines_batch(
    trx_id_per_row: &[i64],
    outputs: &[TrxLineOutput],
) -> Result<Vec<i64>, pgrx::spi::Error> {
    debug_assert_eq!(trx_id_per_row.len(), outputs.len());
    if outputs.is_empty() {
        return Ok(Vec::new());
    }

    let tid: Vec<i64> = trx_id_per_row.to_vec();
    let pool_id: Vec<i64> = outputs.iter().map(|o| o.pool_id).collect();
    let line_type: Vec<String> =
        outputs.iter().map(|o| o.line_type.as_sql().to_string()).collect();
    let source_id: Vec<Option<i64>> = outputs.iter().map(|o| o.source_id).collect();
    let qty: Vec<i64> = outputs.iter().map(|o| o.qty).collect();
    let unit_cost: Vec<i64> = outputs.iter().map(|o| o.unit_cost).collect();
    let source_trx_line_id: Vec<Option<i64>> =
        outputs.iter().map(|o| o.source_trx_line_id).collect();

    let args: [DatumWithOid<'_>; 7] = [
        tid.into(),
        pool_id.into(),
        line_type.into(),
        source_id.into(),
        qty.into(),
        unit_cost.into(),
        source_trx_line_id.into(),
    ];

    let mut ids = run_prepared(
        &PLAN_INSERT_TRX_LINES_BATCH,
        "INSERT INTO trx_line \
           (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id) \
         SELECT tid, pid, lt::line_type, sid, q, uc, stl \
           FROM UNNEST($1::bigint[], $2::bigint[], $3::text[], $4::bigint[], $5::bigint[], $6::bigint[], $7::bigint[]) \
                WITH ORDINALITY AS t(tid, pid, lt, sid, q, uc, stl, ord) \
          ORDER BY ord \
         RETURNING id",
        &pgrx::oids_of![Int8Arr, Int8Arr, TextArr, Int8ArrNullable, Int8Arr, Int8Arr, Int8ArrNullable],
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
    debug_assert_eq!(ids.len(), outputs.len(), "trx_line batch RETURNING dropped a row");
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
    insert_posting_lines_with_tlid(tl_id, requests)
}

/// 8.4b — Batch INSERT INTO posting_line with trx_line ids pre-resolved by the
/// caller (acct-sczx Lever A). `trx_line_id_per_row[i]` is the resolved
/// trx_line.id for `requests[i]`; the committer resolves each submission's
/// posting lines against that submission's slice of the batched trx_line ids,
/// then flattens, so a whole commit group's posting_lines write in one INSERT.
pub fn insert_posting_lines_batch(
    trx_line_id_per_row: &[i64],
    requests: &[PostingLineRequest],
) -> Result<(), pgrx::spi::Error> {
    debug_assert_eq!(trx_line_id_per_row.len(), requests.len());
    if requests.is_empty() {
        return Ok(());
    }
    insert_posting_lines_with_tlid(trx_line_id_per_row.to_vec(), requests)
}

/// Shared body for 8.4 / 8.4b: build the column arrays from `requests` against an
/// already-resolved `tl_id` (parallel to `requests`), then run the kept plan.
fn insert_posting_lines_with_tlid(
    tl_id: Vec<i64>,
    requests: &[PostingLineRequest],
) -> Result<(), pgrx::spi::Error> {
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
