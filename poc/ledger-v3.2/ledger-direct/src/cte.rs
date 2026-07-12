//! The commutative single-statement CTEs (SPIKE-B shape, design-v3.2 §3).
//!
//! Each statement folds the aggregate mutation + trx_line (+ posting_line for
//! WAC) into ONE kept-plan statement on PG's own tuple lock — no `pool_lock`
//! table, no sorted-acquisition protocol, no Rust round-trip. The derived
//! running-average unit_cost is computed in-statement via `banker_div`
//! (migration 0010, byte-faithful to `ledger_core::numeric::banker_div`).
//!
//! Data-modifying CTEs all execute even when unreferenced by the final SELECT;
//! the `FROM mv` / `FROM ins_tl` chaining makes every downstream write cascade
//! off the aggregate mutation, so an empty `mv` (the WAC qty gate failing)
//! atomically writes nothing and surfaces as a `None` return.
//!
//! Four statements:
//! - WAC receipt / depletion — strict: the qty gate rides in the depletion's
//!   WHERE (`qty - Δ >= 0`), the applied cost is the pre-update running average
//!   (PG 18 `RETURNING old.unit_cost`), and the cost leg posts. Formula-equal to
//!   `ledger_core::wac` (the SPIKE-B differential verification).
//! - FIFO/LIFO receipt / depletion — alt-C appends (design-v3.1 §16): no gate
//!   (the aggregate goes negative and is flagged, not rejected), no posting_line
//!   (recalc posts the only cost legs), observed cost recorded on the line
//!   (running average, or the standard basis passed by the caller). The
//!   unit_cost derivation preserves the prior average whenever the new qty is
//!   <= 0 (§3.6: no meaningful average into an unreplenished short position);
//!   the missing-aggregate case (depletion before any receipt) upserts the row
//!   at negative qty with observed cost 0.
//!
//! All trx_line writes stamp the submission's posted_at (recalc-d D1).

use std::cell::RefCell;
use std::thread::LocalKey;

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::PgOid;
use pgrx::prelude::*;
use pgrx::spi::{OwnedPreparedStatement, SpiTupleTable};

type Int8 = i64;
type Text = String;

thread_local! {
    static PLAN_WAC_RECEIPT: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_WAC_DEPLETE: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_FL_RECEIPT: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_FL_DEPLETE: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
}

/// $1 pool_id, $2 qty(>0), $3 unit_cost, $4 trx_id, $5 line_type, $6 line
/// source_id, $7 posted_at, $8 debit, $9 credit.
const WAC_RECEIPT: &str = "\
WITH mv AS ( \
  INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum) \
  VALUES ($1, 0, $2, banker_div($2::numeric * $3::numeric, $2), ($2::numeric * $3::numeric)::bigint) \
  ON CONFLICT (pool_id, layer_id) DO UPDATE \
     SET qty       = pool_state.qty + $2, \
         value_sum = (pool_state.value_sum::numeric + $2::numeric * $3::numeric)::bigint, \
         unit_cost = banker_div(pool_state.value_sum::numeric + $2::numeric * $3::numeric, \
                                pool_state.qty + $2) \
  RETURNING 1 AS ok \
), \
ins_tl AS ( \
  INSERT INTO trx_line (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id, posted_at) \
  SELECT $4, $1, $5::line_type, $6, $2, $3, NULL::bigint, $7::timestamptz FROM mv \
  RETURNING id \
), \
ins_pl AS ( \
  INSERT INTO posting_line (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
  SELECT ins_tl.id, 'inventory_receipt'::posting_event_type, ($2::numeric * $3::numeric)::bigint, \
         $8, $9, $7::timestamptz FROM ins_tl \
) \
SELECT id FROM ins_tl";

/// $1 pool_id, $2 |qty|(>0), $3 trx_id, $4 line_type, $5 line source_id,
/// $6 posted_at, $7 debit (swapped pair), $8 credit. Empty result = the qty
/// gate failed (or no aggregate row exists) => InsufficientInventory.
const WAC_DEPLETE: &str = "\
WITH mv AS ( \
  UPDATE pool_state ps \
     SET qty       = ps.qty - $2, \
         value_sum = CASE WHEN ps.qty - $2 = 0 THEN 0 \
                          ELSE (ps.value_sum::numeric - $2::numeric * ps.unit_cost::numeric)::bigint END, \
         unit_cost = CASE WHEN ps.qty - $2 > 0 \
                          THEN banker_div(ps.value_sum::numeric - $2::numeric * ps.unit_cost::numeric, \
                                          ps.qty - $2) \
                          ELSE ps.unit_cost END \
   WHERE ps.pool_id = $1 AND ps.layer_id = 0 AND ps.qty - $2 >= 0 \
  RETURNING old.unit_cost AS applied_uc \
), \
ins_tl AS ( \
  INSERT INTO trx_line (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id, posted_at) \
  SELECT $3, $1, $4::line_type, $5, 0 - $2, mv.applied_uc, NULL::bigint, $6::timestamptz FROM mv \
  RETURNING id \
), \
ins_pl AS ( \
  INSERT INTO posting_line (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
  SELECT ins_tl.id, 'inventory_depletion'::posting_event_type, \
         ($2::numeric * mv.applied_uc::numeric)::bigint, $7, $8, $6::timestamptz FROM ins_tl, mv \
) \
SELECT id FROM ins_tl";

/// $1 pool_id, $2 qty(>0), $3 unit_cost, $4 trx_id, $5 line_type, $6 line
/// source_id, $7 posted_at. No posting_line (alt C); the unit_cost derivation
/// preserves the prior average when the receipt lands on a still-short pool
/// (new qty <= 0).
const FL_RECEIPT: &str = "\
WITH mv AS ( \
  INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum) \
  VALUES ($1, 0, $2, banker_div($2::numeric * $3::numeric, $2), ($2::numeric * $3::numeric)::bigint) \
  ON CONFLICT (pool_id, layer_id) DO UPDATE \
     SET qty       = pool_state.qty + $2, \
         value_sum = (pool_state.value_sum::numeric + $2::numeric * $3::numeric)::bigint, \
         unit_cost = CASE WHEN pool_state.qty + $2 > 0 \
                          THEN banker_div(pool_state.value_sum::numeric + $2::numeric * $3::numeric, \
                                          pool_state.qty + $2) \
                          ELSE pool_state.unit_cost END \
  RETURNING 1 AS ok \
), \
ins_tl AS ( \
  INSERT INTO trx_line (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id, posted_at) \
  SELECT $4, $1, $5::line_type, $6, $2, $3, NULL::bigint, $7::timestamptz FROM mv \
  RETURNING id \
) \
SELECT id FROM ins_tl";

/// $1 pool_id, $2 |qty|(>0), $3 standard-basis observed cost (NULL for
/// running_avg), $4 trx_id, $5 line_type, $6 line source_id, $7 posted_at.
/// Unconditional: no qty gate — the aggregate goes negative and is flagged
/// (design-v3.1 §16). A depletion with no aggregate row yet upserts one at
/// negative qty; its observed cost falls back to 0 (`old` is the NULL row on
/// the insert arm). No posting_line.
///
/// The observed cost clamps at 0 (GREATEST): a short pool replenished with
/// less value than it shorted can cross back to positive qty with a NEGATIVE
/// book value, driving the derived average negative — a flagged, nonsensical
/// state with no meaningful observation basis (trx_line CHECKs unit_cost >= 0;
/// recalc assigns the authoritative cost). The clamped cost also drives the
/// value_sum decrement — on BOTH arms, so the missing-row upsert can never
/// move value_sum by a cost the line does not record — keeping value_sum ==
/// Σ signed qty×recorded-cost reconcilable to the lines. (Negative standard
/// costs are additionally unrepresentable: `standard_cost` CHECKs
/// unit_cost >= 0.)
const FL_DEPLETE: &str = "\
WITH mv AS ( \
  INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum) \
  VALUES ($1, 0, 0 - $2, 0, (($2::numeric * GREATEST(COALESCE($3, 0), 0)::numeric) * -1)::bigint) \
  ON CONFLICT (pool_id, layer_id) DO UPDATE \
     SET qty       = pool_state.qty - $2, \
         value_sum = CASE WHEN pool_state.qty - $2 = 0 THEN 0 \
                          ELSE (pool_state.value_sum::numeric \
                                - $2::numeric * GREATEST(COALESCE($3, pool_state.unit_cost), 0)::numeric)::bigint END, \
         unit_cost = CASE WHEN pool_state.qty - $2 > 0 \
                          THEN banker_div(pool_state.value_sum::numeric \
                                          - $2::numeric * GREATEST(COALESCE($3, pool_state.unit_cost), 0)::numeric, \
                                          pool_state.qty - $2) \
                          ELSE pool_state.unit_cost END \
  RETURNING GREATEST(COALESCE($3, old.unit_cost, 0), 0) AS applied_uc \
), \
ins_tl AS ( \
  INSERT INTO trx_line (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id, posted_at) \
  SELECT $4, $1, $5::line_type, $6, 0 - $2, mv.applied_uc, NULL::bigint, $7::timestamptz FROM mv \
  RETURNING id \
) \
SELECT id FROM ins_tl";

/// Run a kept-plan CTE and read back the single trx_line.id (`None` when the
/// primary SELECT returns no rows — the WAC depletion gate-fail signal).
fn run_cte(
    slot: &'static LocalKey<RefCell<Option<OwnedPreparedStatement>>>,
    sql: &str,
    arg_oids: &[PgOid],
    args: &[DatumWithOid<'_>],
) -> Result<Option<i64>, pgrx::spi::Error> {
    Spi::connect_mut(|client| {
        slot.with_borrow_mut(|opt| -> Result<(), pgrx::spi::Error> {
            if opt.is_none() {
                *opt = Some(client.prepare_mut(sql, arg_oids)?.keep());
            }
            Ok(())
        })?;
        slot.with_borrow(|opt| {
            let plan = opt.as_ref().expect("plan prepared above");
            let mut table: SpiTupleTable = client.update(plan, None, args)?;
            Ok(match table.next() {
                Some(row) => row.get::<i64>(1)?,
                None => None,
            })
        })
    })
}

pub(crate) fn wac_receipt(
    pool_id: i64,
    qty: i64,
    unit_cost: i64,
    trx_id: i64,
    line_type: &str,
    line_source_id: Option<i64>,
    posted_at_rfc3339: &str,
    debit: i64,
    credit: i64,
) -> Result<Option<i64>, pgrx::spi::Error> {
    let args: [DatumWithOid<'_>; 9] = [
        pool_id.into(),
        qty.into(),
        unit_cost.into(),
        trx_id.into(),
        line_type.to_string().into(),
        line_source_id.into(),
        posted_at_rfc3339.to_string().into(),
        debit.into(),
        credit.into(),
    ];
    run_cte(
        &PLAN_WAC_RECEIPT,
        WAC_RECEIPT,
        &pgrx::oids_of![Int8, Int8, Int8, Int8, Text, Int8, Text, Int8, Int8],
        &args,
    )
}

pub(crate) fn wac_deplete(
    pool_id: i64,
    abs_qty: i64,
    trx_id: i64,
    line_type: &str,
    line_source_id: Option<i64>,
    posted_at_rfc3339: &str,
    debit: i64,
    credit: i64,
) -> Result<Option<i64>, pgrx::spi::Error> {
    let args: [DatumWithOid<'_>; 8] = [
        pool_id.into(),
        abs_qty.into(),
        trx_id.into(),
        line_type.to_string().into(),
        line_source_id.into(),
        posted_at_rfc3339.to_string().into(),
        debit.into(),
        credit.into(),
    ];
    run_cte(
        &PLAN_WAC_DEPLETE,
        WAC_DEPLETE,
        &pgrx::oids_of![Int8, Int8, Int8, Text, Int8, Text, Int8, Int8],
        &args,
    )
}

pub(crate) fn fl_receipt(
    pool_id: i64,
    qty: i64,
    unit_cost: i64,
    trx_id: i64,
    line_type: &str,
    line_source_id: Option<i64>,
    posted_at_rfc3339: &str,
) -> Result<Option<i64>, pgrx::spi::Error> {
    let args: [DatumWithOid<'_>; 7] = [
        pool_id.into(),
        qty.into(),
        unit_cost.into(),
        trx_id.into(),
        line_type.to_string().into(),
        line_source_id.into(),
        posted_at_rfc3339.to_string().into(),
    ];
    run_cte(
        &PLAN_FL_RECEIPT,
        FL_RECEIPT,
        &pgrx::oids_of![Int8, Int8, Int8, Int8, Text, Int8, Text],
        &args,
    )
}

pub(crate) fn fl_deplete(
    pool_id: i64,
    abs_qty: i64,
    standard_observed: Option<i64>,
    trx_id: i64,
    line_type: &str,
    line_source_id: Option<i64>,
    posted_at_rfc3339: &str,
) -> Result<Option<i64>, pgrx::spi::Error> {
    let args: [DatumWithOid<'_>; 7] = [
        pool_id.into(),
        abs_qty.into(),
        standard_observed.into(),
        trx_id.into(),
        line_type.to_string().into(),
        line_source_id.into(),
        posted_at_rfc3339.to_string().into(),
    ];
    run_cte(
        &PLAN_FL_DEPLETE,
        FL_DEPLETE,
        &pgrx::oids_of![Int8, Int8, Int8, Int8, Text, Int8, Text],
        &args,
    )
}
