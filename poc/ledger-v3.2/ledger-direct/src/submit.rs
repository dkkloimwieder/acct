//! `ledger_submit_trx` — the v3.2 direct hot path (design-v3.2 §3/§4), plus the
//! shared `apply_submission` the staging drain reuses.
//!
//! Wire contract (§4): callers send inventory facts only — `(pool_id,
//! line_type, qty, unit_cost)` per line. The ledger resolves debit/credit (and
//! the STD variance account) from `posting_account_map`; a touched pool with no
//! row there fails loud (MissingPostingAccounts) regardless of method — FIFO/
//! LIFO appends post no journal row on the hot path, but the recalc engine
//! posts their cost legs later and must find the config then, so the check is
//! at submit time.
//!
//! Per-submission pipeline:
//!   0. backpressure admission check (direct path only, recalc-c §5): a
//!      submission touching a throttled pool is rejected with SQLSTATE 53400
//!      before any write — one empty-index probe in the common case
//!   1. one UNLOCKED reference-data read resolves every touched pool's method,
//!      basis, posting accounts, and standard cost (not the contended hot
//!      state — legitimately outside any critical section)
//!   2. INSERT trx (the `(trx_type, source_id)` idempotency backstop raises
//!      here on a duplicate)
//!   3. ledger-core phase — STD / specific lines, in original submission
//!      order: pool-row locks (ascending), hydrate, `plan_apply`, bulk write
//!   4. commutative phase — WAC / FIFO / LIFO lines, sorted by (pool_id,
//!      submission index): one CTE statement per line
//!
//! Lock order is a fixed two-tier global order — ledger-core pool-row locks
//! (ascending) strictly before any commutative tuple lock (ascending pool) —
//! so concurrent mixed-method submissions cannot deadlock: every transaction
//! acquires all tier-1 locks before its first tier-2 lock, and each tier is
//! internally sorted. Cross-pool line order is semantically free (each line
//! touches only its own pool); within one pool, submission order is preserved
//! (same-pool lines share a dispatch path and the sort is stable on index).
//!
//! Any line failure raises ereport(ERROR) and aborts the whole submission —
//! the trx row and every already-applied line roll back together.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use ledger_core::{plan_apply, PoolMethod, PostingAccounts, TrxLineRequest};
use pgrx::prelude::*;
use serde::Deserialize;

use crate::cte;
use crate::ledger_error_map;
use ledger_spi_common::line_type::decode_line_type;
use ledger_spi_common::{bulk_write, hydration, pool_row_lock};

/// JSONB element shape for `lines[]` (and `ledger_inbox.lines`). Posting
/// accounts are NOT on the line (§3.7/§4).
#[derive(Deserialize)]
pub(crate) struct LineJson {
    pub pool_id: i64,
    pub line_type: String,
    #[serde(default)]
    pub source_id: Option<i64>,
    pub qty: i64,
    pub unit_cost: i64,
}

/// Per-pool config resolved by the unlocked reference-data read.
struct PoolCfg {
    method: PoolMethod,
    standard_basis: bool,
    sku_id: i64,
    location_id: i64,
    accounts: Option<PostingAccounts>,
    standard_cost: Option<i64>,
}

/// Caller-facing SPI: submit one transaction.
///
/// `trx_type` is the SQL trx_type enum text (po_receipt, transfer_shipment, …).
/// `posted_at` is RFC3339 text and MUST be the true business/effective time —
/// it lands on every trx_line as the recalc engine's R-1 ordering key
/// (recalc-d D1).
/// `lines` is a JSONB array of `{pool_id, line_type, source_id?, qty, unit_cost}`.
///
/// Returns the new trx.id. Raises ereport!(ERROR, ...) on any failure; the
/// caller's user-tx then aborts.
#[pg_extern]
fn ledger_submit_trx(trx_type: &str, source_id: i64, posted_at: &str, lines: pgrx::JsonB) -> i64 {
    let posted_at: DateTime<Utc> = match DateTime::parse_from_rfc3339(posted_at) {
        Ok(t) => t.with_timezone(&Utc),
        Err(e) => bail(
            PgSqlErrorCode::ERRCODE_INVALID_DATETIME_FORMAT,
            format!("ledger_submit_trx: posted_at not RFC3339: {e}"),
        ),
    };
    let line_jsons: Vec<LineJson> = match serde_json::from_value(lines.0) {
        Ok(v) => v,
        Err(e) => bail(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!("ledger_submit_trx: lines JSONB decode failed: {e}"),
        ),
    };
    apply_submission(trx_type, source_id, posted_at, &line_jsons, true)
}

/// The full submission pipeline (shared by `ledger_submit_trx` and the staging
/// drain). Raises ereport!(ERROR, ...) on any failure.
///
/// `enforce_backpressure` gates the recalc-c §5 admission check: true on the
/// direct path; false from the staging drain, whose envelopes were gated by
/// the `ledger_inbox` trigger at enqueue time — admitted work is applied, never
/// retroactively failed.
pub(crate) fn apply_submission(
    trx_type: &str,
    source_id: i64,
    posted_at: DateTime<Utc>,
    line_jsons: &[LineJson],
    enforce_backpressure: bool,
) -> i64 {
    // Decode lines; zero-qty lines are no-ops (ledger-core's own convention).
    let mut lines: Vec<(usize, TrxLineRequest)> = Vec::with_capacity(line_jsons.len());
    for (idx, l) in line_jsons.iter().enumerate() {
        if l.qty == 0 {
            continue;
        }
        let lt = match decode_line_type(&l.line_type) {
            Some(t) => t,
            None => bail(
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("ledger_submit_trx: unknown line_type '{}'", l.line_type),
            ),
        };
        lines.push((
            idx,
            TrxLineRequest {
                pool_id: l.pool_id,
                line_type: lt,
                source_id: l.source_id,
                qty: l.qty,
                unit_cost: l.unit_cost,
            },
        ));
    }

    // 1. Unlocked reference-data read for every touched pool. Fails loud on an
    //    unknown pool or a missing posting_account_map row (§4).
    let touched: Vec<i64> =
        lines.iter().map(|(_, l)| l.pool_id).collect::<BTreeSet<i64>>().into_iter().collect();
    if enforce_backpressure {
        check_backpressure(&touched);
    }
    let cfgs = resolve_cfgs(&touched);
    for pid in &touched {
        let cfg = match cfgs.get(pid) {
            Some(c) => c,
            None => {
                ledger_error_map::raise_ledger_error(ledger_core::LedgerError::UnknownPool(*pid));
                unreachable!()
            }
        };
        if cfg.accounts.is_none() {
            ledger_error_map::raise_ledger_error(
                ledger_core::LedgerError::MissingPostingAccounts {
                    sku_id: cfg.sku_id,
                    location_id: cfg.location_id,
                },
            );
            unreachable!()
        }
    }

    // 2. The trx row, shared by both phases. A duplicate (trx_type, source_id)
    //    raises 23505 here — the idempotency backstop.
    let trx_id = match bulk_write::insert_trx(trx_type, source_id, posted_at) {
        Ok(id) => id,
        Err(e) => bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_submit_trx: trx insert failed: {e}"),
        ),
    };

    let is_core = |pid: i64| {
        matches!(
            cfgs.get(&pid).map(|c| c.method),
            Some(PoolMethod::Std) | Some(PoolMethod::Specific)
        )
    };

    // 3. Ledger-core phase (STD / specific), original submission order.
    let core_lines: Vec<TrxLineRequest> =
        lines.iter().filter(|(_, l)| is_core(l.pool_id)).map(|(_, l)| l.clone()).collect();
    if !core_lines.is_empty() {
        let core_pools: Vec<i64> = core_lines
            .iter()
            .map(|l| l.pool_id)
            .collect::<BTreeSet<i64>>()
            .into_iter()
            .collect();
        if let Err(e) = pool_row_lock::lock_pool_rows(&core_pools) {
            bail(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("ledger_submit_trx: pool-row lock acquisition failed: {e}"),
            );
        }
        let mut snapshot = match hydration::hydrate_snapshot(&core_pools) {
            Ok(s) => s,
            Err(e) => bail(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("ledger_submit_trx: snapshot hydration failed: {e}"),
            ),
        };
        let plan = match plan_apply(&mut snapshot, &core_lines, posted_at) {
            Ok(p) => p,
            Err(err) => {
                ledger_error_map::raise_ledger_error(err);
                unreachable!()
            }
        };
        if let Err(e) = bulk_write::apply_plan_result(trx_id, posted_at, &plan) {
            bail(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("ledger_submit_trx: bulk-write failed: {e}"),
            );
        }
    }

    // 4. Commutative phase (WAC / FIFO / LIFO), sorted by (pool_id, submission
    //    index) — the tier-2 ascending tuple-lock order.
    let mut cte_lines: Vec<&(usize, TrxLineRequest)> =
        lines.iter().filter(|(_, l)| !is_core(l.pool_id)).collect();
    cte_lines.sort_by_key(|(idx, l)| (l.pool_id, *idx));

    let posted_at_txt = posted_at.to_rfc3339();
    for (_, line) in cte_lines {
        let cfg = cfgs.get(&line.pool_id).expect("validated above");
        dispatch_cte_line(line, cfg, trx_id, &posted_at_txt);
    }

    trx_id
}

/// One commutative line: pick the statement by (method, qty sign), the account
/// pair by line_type (receipt direction, swapped for depletions — the §3.7
/// uniform direction rule).
fn dispatch_cte_line(line: &TrxLineRequest, cfg: &PoolCfg, trx_id: i64, posted_at_txt: &str) {
    let lt = line.line_type.as_sql();
    let accounts = cfg.accounts.as_ref().expect("validated in apply_submission");

    match (cfg.method, line.qty > 0) {
        (PoolMethod::Wac, true) => {
            let (debit, credit) = accounts.pair_for(line.line_type);
            let res = cte::wac_receipt(
                line.pool_id,
                line.qty,
                line.unit_cost,
                trx_id,
                lt,
                line.source_id,
                posted_at_txt,
                debit,
                credit,
            );
            expect_row(res, "wac receipt", line.pool_id);
        }
        (PoolMethod::Wac, false) => {
            let (rcv_debit, rcv_credit) = accounts.pair_for(line.line_type);
            let abs_qty = abs_qty(line);
            match cte::wac_deplete(
                line.pool_id,
                abs_qty,
                trx_id,
                lt,
                line.source_id,
                posted_at_txt,
                rcv_credit,
                rcv_debit,
            ) {
                Ok(Some(_)) => {}
                // Empty RETURNING == the qty gate (qty - Δ >= 0) failed, or the
                // pool has no aggregate row yet. Strict methods reject.
                Ok(None) => bail(
                    PgSqlErrorCode::ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION,
                    format!(
                        "insufficient inventory: pool={} requested={abs_qty}",
                        line.pool_id
                    ),
                ),
                Err(e) => bail(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("ledger_submit_trx: wac depletion failed: {e}"),
                ),
            }
        }
        (PoolMethod::Fifo | PoolMethod::Lifo, true) => {
            let res = cte::fl_receipt(
                line.pool_id,
                line.qty,
                line.unit_cost,
                trx_id,
                lt,
                line.source_id,
                posted_at_txt,
            );
            expect_row(res, "fifo/lifo receipt", line.pool_id);
        }
        (PoolMethod::Fifo | PoolMethod::Lifo, false) => {
            // Observed-cost basis (pool.provisional_basis): 'standard' records
            // the resolved standard cost; 'running_avg' records the pre-update
            // aggregate average (NULL => RETURNING old inside the statement).
            let standard_observed: Option<i64> = if cfg.standard_basis {
                match cfg.standard_cost {
                    Some(c) => Some(c),
                    None => {
                        ledger_error_map::raise_ledger_error(
                            ledger_core::LedgerError::MissingStandardCost {
                                sku_id: cfg.sku_id,
                                location_id: cfg.location_id,
                            },
                        );
                        unreachable!()
                    }
                }
            } else {
                None
            };
            let res = cte::fl_deplete(
                line.pool_id,
                abs_qty(line),
                standard_observed,
                trx_id,
                lt,
                line.source_id,
                posted_at_txt,
            );
            expect_row(res, "fifo/lifo depletion", line.pool_id);
        }
        (PoolMethod::Std | PoolMethod::Specific, _) => unreachable!("core methods filtered out"),
    }
}

fn abs_qty(line: &TrxLineRequest) -> i64 {
    match line.qty.checked_abs() {
        Some(a) => a,
        None => bail(
            PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("ledger_submit_trx: qty.abs() overflow on pool {}", line.pool_id),
        ),
    }
}

/// The unconditional statements (receipts, fifo/lifo depletions) always write
/// their line; an empty result means a bug, not a business condition.
fn expect_row(res: Result<Option<i64>, pgrx::spi::Error>, what: &str, pool_id: i64) {
    match res {
        Ok(Some(_)) => {}
        Ok(None) => bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_submit_trx: {what} on pool {pool_id} returned no trx_line id"),
        ),
        Err(e) => bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_submit_trx: {what} on pool {pool_id} failed: {e}"),
        ),
    }
}

/// Admission control (recalc-c §5): reject a submission touching a
/// backpressure-throttled pool with SQLSTATE 53400. The probe doubles as the
/// design's cheap global "backpressure active" flag — the throttled set is
/// empty in the common case, so the ANY-lookup is one empty-index probe; only
/// an engaged pool makes it a consult. Zero-qty lines were dropped before
/// `touched` was built, matching the `ledger_inbox` trigger's gate.
fn check_backpressure(touched: &[i64]) {
    if touched.is_empty() {
        return;
    }
    let ids: Vec<i64> = touched.to_vec();
    let hit = Spi::connect(|client| -> Result<Option<(i64, i64)>, pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT pool_id, engage_events FROM recalc_backpressure \
              WHERE pool_id = ANY($1::bigint[]) \
              ORDER BY pool_id LIMIT 1",
            None,
            &[ids.into()],
        )?;
        Ok(match t.next() {
            Some(row) => Some((
                row.get::<i64>(1)?.expect("recalc_backpressure.pool_id NOT NULL"),
                row.get::<i64>(2)?.expect("recalc_backpressure.engage_events NOT NULL"),
            )),
            None => None,
        })
    })
    .unwrap_or_else(|e: pgrx::spi::Error| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_submit_trx: backpressure probe failed: {e}"),
        )
    });
    if let Some((pool_id, engage_events)) = hit {
        bail(
            PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
            format!(
                "ledger_submit_trx: pool {pool_id} is backpressure-throttled \
                 (unsettled backlog {engage_events} reached bound)"
            ),
        );
    }
}

/// Resolve method / basis / posting accounts / standard cost for every touched
/// pool in one UNLOCKED reference-data read (static config, not the contended
/// hot state). A pool absent from `pool` is absent from the map (UnknownPool);
/// a pool with no posting_account_map row gets `accounts: None`
/// (MissingPostingAccounts — checked for every touched pool, §4).
fn resolve_cfgs(pool_ids: &[i64]) -> HashMap<i64, PoolCfg> {
    if pool_ids.is_empty() {
        return HashMap::new();
    }
    let ids: Vec<i64> = pool_ids.to_vec();
    Spi::connect(|client| -> Result<HashMap<i64, PoolCfg>, pgrx::spi::Error> {
        let mut out = HashMap::new();
        let mut t = client.select(
            "SELECT p.id, p.method::text, p.provisional_basis::text, p.sku_id, p.location_id, \
                    m.receipt_debit, m.receipt_credit, \
                    m.transfer_debit, m.transfer_credit, \
                    m.build_debit, m.build_credit, \
                    m.scrap_debit, m.scrap_credit, \
                    m.adjustment_debit, m.adjustment_credit, \
                    m.revaluation_debit, m.revaluation_credit, \
                    m.variance_acct, \
                    sc.unit_cost \
               FROM pool p \
               LEFT JOIN posting_account_map m \
                 ON m.sku_id = p.sku_id AND m.location_id = p.location_id \
               LEFT JOIN standard_cost sc \
                 ON sc.sku_id = p.sku_id AND sc.location_id = p.location_id \
              WHERE p.id = ANY($1::bigint[])",
            None,
            &[ids.into()],
        )?;
        while let Some(row) = t.next() {
            let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
            let method = match row.get::<String>(2)?.unwrap_or_default().as_str() {
                "fifo" => PoolMethod::Fifo,
                "lifo" => PoolMethod::Lifo,
                "wac" => PoolMethod::Wac,
                "std" => PoolMethod::Std,
                "specific" => PoolMethod::Specific,
                _ => continue, // unknown enum value — surfaces as UnknownPool
            };
            let standard_basis = row.get::<String>(3)?.unwrap_or_default() == "standard";
            let sku_id: i64 = row.get::<i64>(4)?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(5)?.unwrap_or(0);
            // receipt_debit is NOT NULL in the table, so a NULL here means the
            // LEFT JOIN found no posting_account_map row.
            let accounts = match row.get::<i64>(6)? {
                Some(receipt_debit) => {
                    let g = |c: usize| -> i64 { row.get::<i64>(c).ok().flatten().unwrap_or(0) };
                    Some(PostingAccounts {
                        receipt: (receipt_debit, g(7)),
                        transfer: (g(8), g(9)),
                        build: (g(10), g(11)),
                        scrap: (g(12), g(13)),
                        adjustment: (g(14), g(15)),
                        revaluation: (g(16), g(17)),
                        variance: row.get::<i64>(18)?,
                    })
                }
                None => None,
            };
            let standard_cost: Option<i64> = row.get::<i64>(19)?;
            out.insert(
                pid,
                PoolCfg { method, standard_basis, sku_id, location_id, accounts, standard_cost },
            );
        }
        Ok(out)
    })
    .unwrap_or_else(|e: pgrx::spi::Error| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_submit_trx: config resolution failed: {e}"),
        )
    })
}

#[allow(unreachable_code)]
pub(crate) fn bail(code: PgSqlErrorCode, msg: String) -> ! {
    ereport!(ERROR, code, msg);
    unreachable!()
}
