//! SPIKE-B (acct-0at4.11.2): single-statement commutative aggregate variant.
//!
//! FEEDBACK-ARCH.md problem #4 / alternative B. The RMW baseline
//! (`submit::ledger_submit_trx_c`) holds `pool_lock` FOR UPDATE across a hydrate
//! read, a round-trip into ledger-core (Rust), and four bulk writes — the lock is
//! held that long *because the running-average arithmetic lives outside SQL*. But
//! the aggregate mutation is commutative, and PostgreSQL will serialize it on the
//! aggregate row's own tuple lock. This variant folds hydrate + plan + write for
//! the aggregate-only paths (running-average receipt; running-average or
//! standard-basis depletion) into **one** CTE statement:
//!
//!   - NO `pool_lock` table, NO sorted-acquisition protocol, NO deadlock ordering.
//!   - The derived `unit_cost` is computed in SQL via `banker_div` (round-half-to-
//!     even, ported byte-faithfully in `bench/spike-b-setup.sql`).
//!   - A depletion reads its running average under the same tuple lock via PG 18
//!     `RETURNING old.unit_cost`; the strict-qty gate rides in the UPDATE's WHERE
//!     (`qty - Δ >= 0`), and an empty RETURNING => InsufficientInventory.
//!
//! Scope (deliberate, throwaway): single pool, single line — the shape of the two
//! benchmark scenarios it is measured on (S1 WAC receipt / S5 FIFO deplete, both
//! `Simple` = one line). Multi-pool would reintroduce the very lock-ordering
//! question this spike is trying to delete, confounding the measurement. Only the
//! aggregate methods (fifo/lifo/wac) and the simple receipt/transfer line types
//! the workload emits are handled; anything else fails loud.
//!
//! Config resolution (pool method/basis, posting accounts, standard cost) is one
//! UNLOCKED read of reference data — mirroring the baseline's non-kept hydrate
//! selects, and legitimately outside the critical section because that data is not
//! the contended hot state. Only the CTE (kept plan, like the baseline's writes)
//! touches the aggregate row.

use std::cell::RefCell;
use std::thread::LocalKey;

use chrono::{DateTime, Utc};
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::PgOid;
use pgrx::prelude::*;
use pgrx::spi::{OwnedPreparedStatement, SpiTupleTable};
use serde::Deserialize;

type Int8 = i64;
type Text = String;

#[derive(Deserialize)]
struct LineJson {
    pool_id: i64,
    line_type: String,
    #[serde(default)]
    source_id: Option<i64>,
    qty: i64,
    unit_cost: i64,
}

thread_local! {
    static PLAN_RECEIPT: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
    static PLAN_DEPLETE: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };
}

/// SPIKE-B entry point: submit one single-line transaction via the single-
/// statement commutative aggregate path. Same signature as
/// `ledger_submit_trx_c` so the harness swaps only the function name (identical
/// argument marshalling — a fair head-to-head).
#[pg_extern]
fn ledger_submit_trx_single_c(
    trx_type: &str,
    source_id: i64,
    posted_at: &str,
    lines: pgrx::JsonB,
) -> i64 {
    let posted_at: DateTime<Utc> = match DateTime::parse_from_rfc3339(posted_at) {
        Ok(t) => t.with_timezone(&Utc),
        Err(e) => bail(
            PgSqlErrorCode::ERRCODE_INVALID_DATETIME_FORMAT,
            format!("ledger_submit_trx_single_c: posted_at not RFC3339: {e}"),
        ),
    };

    let line_jsons: Vec<LineJson> = match serde_json::from_value(lines.0) {
        Ok(v) => v,
        Err(e) => bail(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!("ledger_submit_trx_single_c: lines JSONB decode failed: {e}"),
        ),
    };

    // Single-pool single-line scope (§ spike): exactly one line.
    if line_jsons.len() != 1 {
        bail(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!(
                "ledger_submit_trx_single_c: spike handles exactly 1 line, got {}",
                line_jsons.len()
            ),
        );
    }
    let line = &line_jsons[0];
    if line.qty == 0 {
        bail(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "ledger_submit_trx_single_c: qty must be non-zero".to_string(),
        );
    }

    // ── config resolution (UNLOCKED reference-data read) ───────────────
    let cfg = resolve_cfg(line.pool_id);

    // Only the aggregate methods are foldable into one commutative statement.
    match cfg.method.as_str() {
        "fifo" | "lifo" | "wac" => {}
        other => bail(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!(
                "ledger_submit_trx_single_c: spike supports fifo/lifo/wac aggregate paths, \
                 pool {} is '{}'",
                line.pool_id, other
            ),
        ),
    }

    // Posting account pair for this line_type (mirror of PostingAccounts::pair_for
    // for the simple receipt/transfer line types the workload emits).
    let pair = match line.line_type.as_str() {
        "po_receipt_line" => cfg.receipt,
        "transfer_shipment_line" | "transfer_receipt_line" => cfg.transfer,
        other => bail(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!(
                "ledger_submit_trx_single_c: spike supports po_receipt_line/transfer_* only, \
                 got '{other}'"
            ),
        ),
    };

    let posted_at_txt = posted_at.to_rfc3339();

    if line.qty > 0 {
        // ── receipt: aggregate running-average update (INSERT .. ON CONFLICT) ──
        // Direction as-is (debit inventory, credit contra). trx_line records the
        // asserted cost; posting amount = qty*unit_cost.
        let (debit, credit) = pair;
        let args: [DatumWithOid<'_>; 10] = [
            line.pool_id.into(),
            line.qty.into(),
            line.unit_cost.into(),
            trx_type.to_string().into(),
            source_id.into(),
            posted_at_txt.into(),
            line.line_type.clone().into(),
            line.source_id.into(),
            debit.into(),
            credit.into(),
        ];
        match run_cte(&PLAN_RECEIPT, RECEIPT_CTE, &args) {
            Some(id) => id,
            // A receipt never gates out; a None here means the trx UNIQUE
            // constraint or a downstream error swallowed the row — surface loud.
            None => bail(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "ledger_submit_trx_single_c: receipt returned no trx id".to_string(),
            ),
        }
    } else {
        // ── depletion: swap the pair (debit contra, credit inventory) ──
        let (debit, credit) = (pair.1, pair.0);
        let absq = match line.qty.checked_abs() {
            Some(a) => a,
            None => bail(
                PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "ledger_submit_trx_single_c: qty.abs() overflow".to_string(),
            ),
        };
        // Applied cost basis: 'standard' passes the resolved standard cost;
        // running-average passes NULL and the CTE reads old.unit_cost under lock.
        let applied_std: Option<i64> = if cfg.basis == "standard" {
            match cfg.standard_cost {
                Some(c) => Some(c),
                None => bail(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!(
                        "ledger_submit_trx_single_c: standard-basis pool {} has no standard_cost",
                        line.pool_id
                    ),
                ),
            }
        } else {
            None
        };
        let args: [DatumWithOid<'_>; 10] = [
            line.pool_id.into(),
            absq.into(),
            applied_std.into(),
            trx_type.to_string().into(),
            source_id.into(),
            posted_at_txt.into(),
            line.line_type.clone().into(),
            line.source_id.into(),
            debit.into(),
            credit.into(),
        ];
        match run_cte(&PLAN_DEPLETE, DEPLETE_CTE, &args) {
            Some(id) => id,
            // Empty RETURNING == the qty gate (qty - Δ >= 0) failed.
            None => bail(
                PgSqlErrorCode::ERRCODE_CHECK_VIOLATION,
                format!(
                    "ledger_submit_trx_single_c: insufficient inventory on pool {} (requested {})",
                    line.pool_id, absq
                ),
            ),
        }
    }
}

/// Receipt CTE: one commutative aggregate UPSERT + trx/trx_line/posting_line, all
/// in one statement. The aggregate row (layer_id = 0) is the only contended tuple.
/// $1 pool_id, $2 qty(>0), $3 unit_cost, $4 trx_type, $5 source_id, $6 posted_at,
/// $7 line_type, $8 line source_id, $9 debit, $10 credit.
const RECEIPT_CTE: &str = "\
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
ins_trx AS ( \
  INSERT INTO trx (trx_type, source_id, posted_at) \
  SELECT $4::trx_type, $5, $6::timestamptz FROM mv \
  RETURNING id \
), \
ins_tl AS ( \
  INSERT INTO trx_line (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id) \
  SELECT ins_trx.id, $1, $7::line_type, $8, $2, $3, NULL::bigint FROM ins_trx \
  RETURNING id \
), \
ins_pl AS ( \
  INSERT INTO posting_line (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
  SELECT ins_tl.id, 'inventory_receipt'::posting_event_type, ($2::numeric * $3::numeric)::bigint, \
         $9, $10, $6::timestamptz FROM ins_tl \
  RETURNING trx_line_id \
) \
SELECT id FROM ins_trx";

/// Depletion CTE: one commutative aggregate UPDATE (running-average or standard
/// basis) + trx/trx_line/posting_line. The qty gate rides in the WHERE; an empty
/// mv (gate fail) cascades to an empty result => InsufficientInventory. The
/// applied cost is COALESCE($3, old.unit_cost): $3 = standard cost for the
/// standard basis, NULL => the pre-update running average read under the tuple
/// lock via PG 18 RETURNING old. $1 pool_id, $2 |qty|(>0), $3 applied_std(nullable),
/// $4 trx_type, $5 source_id, $6 posted_at, $7 line_type, $8 line source_id,
/// $9 debit, $10 credit.
const DEPLETE_CTE: &str = "\
WITH mv AS ( \
  UPDATE pool_state ps \
     SET qty       = ps.qty - $2, \
         value_sum = CASE WHEN ps.qty - $2 = 0 THEN 0 \
                          ELSE (ps.value_sum::numeric \
                                - $2::numeric * COALESCE($3::numeric, ps.unit_cost::numeric))::bigint END, \
         unit_cost = CASE WHEN ps.qty - $2 > 0 \
                          THEN banker_div(ps.value_sum::numeric \
                                          - $2::numeric * COALESCE($3::numeric, ps.unit_cost::numeric), \
                                          ps.qty - $2) \
                          ELSE ps.unit_cost END \
   WHERE ps.pool_id = $1 AND ps.layer_id = 0 AND ps.qty - $2 >= 0 \
  RETURNING COALESCE($3, old.unit_cost) AS applied_uc \
), \
ins_trx AS ( \
  INSERT INTO trx (trx_type, source_id, posted_at) \
  SELECT $4::trx_type, $5, $6::timestamptz FROM mv \
  RETURNING id \
), \
ins_tl AS ( \
  INSERT INTO trx_line (trx_id, pool_id, line_type, source_id, qty, unit_cost, source_trx_line_id) \
  SELECT ins_trx.id, $1, $7::line_type, $8, -$2, mv.applied_uc, NULL::bigint FROM ins_trx, mv \
  RETURNING id \
), \
ins_pl AS ( \
  INSERT INTO posting_line (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
  SELECT ins_tl.id, 'inventory_depletion'::posting_event_type, \
         ($2::numeric * mv.applied_uc::numeric)::bigint, $9, $10, $6::timestamptz FROM ins_tl, mv \
  RETURNING trx_line_id \
) \
SELECT id FROM ins_trx";

struct Cfg {
    method: String,
    basis: String,
    receipt: (i64, i64),
    transfer: (i64, i64),
    standard_cost: Option<i64>,
}

/// Resolve pool routing + posting accounts + standard cost in one unlocked read.
/// Not under any lock: this is static reference data, not the contended hot
/// aggregate — moving it out of the critical section is part of the win.
fn resolve_cfg(pool_id: i64) -> Cfg {
    Spi::connect(|client| {
        let mut t = client.select(
            "SELECT p.method::text, p.provisional_basis::text, \
                    m.receipt_debit, m.receipt_credit, \
                    m.transfer_debit, m.transfer_credit, \
                    sc.unit_cost \
               FROM pool p \
               JOIN posting_account_map m ON m.sku_id = p.sku_id AND m.location_id = p.location_id \
               LEFT JOIN standard_cost sc ON sc.sku_id = p.sku_id AND sc.location_id = p.location_id \
              WHERE p.id = $1",
            None,
            &[pool_id.into()],
        )?;
        match t.next() {
            Some(row) => Ok(Cfg {
                method: row.get::<String>(1)?.unwrap_or_default(),
                basis: row.get::<String>(2)?.unwrap_or_default(),
                receipt: (
                    row.get::<i64>(3)?.unwrap_or(0),
                    row.get::<i64>(4)?.unwrap_or(0),
                ),
                transfer: (
                    row.get::<i64>(5)?.unwrap_or(0),
                    row.get::<i64>(6)?.unwrap_or(0),
                ),
                standard_cost: row.get::<i64>(7)?,
            }),
            None => Ok(Cfg {
                method: String::new(),
                basis: String::new(),
                receipt: (0, 0),
                transfer: (0, 0),
                standard_cost: None,
            }),
        }
    })
    .unwrap_or_else(|e: pgrx::spi::Error| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_submit_trx_single_c: cfg resolution failed: {e}"),
        )
    })
}

/// Run a kept-plan CTE and read back the single `trx.id` (or None when the
/// primary SELECT returns no rows — the depletion gate-fail signal). Kept plan
/// (SPI_keepplan) so the per-caller backend loop amortizes parse/plan, matching
/// the baseline's kept bulk-write plans.
fn run_cte(
    slot: &'static LocalKey<RefCell<Option<OwnedPreparedStatement>>>,
    sql: &str,
    args: &[DatumWithOid<'_>],
) -> Option<i64> {
    let oids: [PgOid; 10] = pgrx::oids_of![
        Int8, Int8, Int8, Text, Int8, Text, Text, Int8, Int8, Int8
    ];
    let res: Result<Option<i64>, pgrx::spi::Error> = Spi::connect_mut(|client| {
        slot.with_borrow_mut(|opt| -> Result<(), pgrx::spi::Error> {
            if opt.is_none() {
                *opt = Some(client.prepare_mut(sql, &oids)?.keep());
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
    });
    match res {
        Ok(v) => v,
        Err(e) => bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_submit_trx_single_c: CTE execution failed: {e}"),
        ),
    }
}

#[allow(unreachable_code)]
fn bail(code: PgSqlErrorCode, msg: String) -> ! {
    ereport!(ERROR, code, msg);
    unreachable!()
}
