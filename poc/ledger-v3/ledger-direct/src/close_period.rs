//! `ledger_close_period` SPI: wac_periodic period close orchestration
//! (acct-s6fa).
//!
//! Synchronous in the caller's user-tx, mirroring `ledger_submit_trx`'s
//! posture. Period close is a rare heavy operation; sync execution avoids
//! a routed/async control plane for the PoC.
//!
//! # Pipeline
//!
//! 1. SELECT period FOR UPDATE — raise P_INVALID_STATE if state != 'open'.
//! 2. UPDATE state = 'closing'.
//! 3. SELECT unfinalized `posting_lines_provisional` joined to `posting_line`,
//!    filtered by `posted_at` within [start_date, end_date + 1 day).
//! 4. For each affected pool, acquire `pool_lock` FOR UPDATE in ascending
//!    pool_id order (R4: lock pool before reading/writing its state).
//! 5. Compute per-pool `final_avg = Σ in-period receipt value / Σ qty`.
//!    Pools with zero in-period receipts → raise (cannot compute final_avg).
//! 6. Per provisional row: `variance = final_avg * qty - provisional_amount`.
//! 7. Insert one `revaluation_run` trx for the close + one variance trx_line
//!    per non-zero-variance provisional row (qty=0; unit_cost=variance/qty
//!    for audit). Allocate `trx_seq` per pool by reading `MAX(trx_seq)` once
//!    per pool under lock and incrementing.
//! 8. Insert variance posting_lines:
//!      - variance > 0: same (debit, credit) as original depletion; amount = variance
//!      - variance < 0: SWAPPED (debit, credit); amount = |variance|
//!      - variance = 0: no posting (variance_posting_line_id stays NULL on the
//!        finalized row)
//! 9. Adjust `pool_state.unit_cost` (= value_sum for wac_periodic, per the
//!    per-method storage contract) by `-Σ variance` per pool — the
//!    cumulative-sum bookkeeping correction for re-stating depletions at
//!    final_avg instead of provisional_amount.
//! 10. UPDATE each provisional row: `finalized_at = now()`, `variance_amount`,
//!     `variance_posting_line_id` (NULL when variance == 0).
//! 11. UPDATE period: state='closed', closed_at=now().
//! 12. Return JSONB summary `{ period_id, provisional_finalized, variance_posted,
//!     zero_variance, pools_affected }`.
//!
//! # Variance routing in PoC
//!
//! For PoC simplicity, variance flows through the SAME debit/credit accounts
//! as the original depletion (with direction swap on negative variance). Main
//! acct routes through a dedicated `variance_wac_periodic` account_kind. The
//! PoC simplification is documented and acceptable since the equivalence
//! check operates on per-pool value totals — direct routing yields identical
//! ledger end-state.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use pgrx::prelude::*;
use serde_json::json;

use crate::pool_lock;

/// SPI entry point: close a period.
///
/// Returns JSONB summary on success. Raises `ereport!(ERROR, ...)` and
/// aborts the caller's tx on any failure (period not open, no in-period
/// receipts for a pool with provisional rows, etc.).
#[pg_extern(volatile)]
fn ledger_close_period(period_id: i64) -> pgrx::JsonB {
    let result = close_period_impl(period_id);
    pgrx::JsonB(json!(result))
}

#[derive(Debug)]
#[allow(dead_code)] // posting_line_id retained for diagnostics / future asserts
struct Provisional {
    id: i64,
    posting_line_id: i64,
    pool_id: i64,
    qty: i64,
    provisional_amount: i64,
    debit_account: i64,
    credit_account: i64,
    posted_at_rfc3339: String,
}

#[derive(Debug, Default)]
struct CloseSummary {
    period_id: i64,
    provisional_finalized: i64,
    variance_posted: i64,
    zero_variance: i64,
    pools_affected: i64,
}

impl CloseSummary {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "period_id": self.period_id,
            "provisional_finalized": self.provisional_finalized,
            "variance_posted": self.variance_posted,
            "zero_variance": self.zero_variance,
            "pools_affected": self.pools_affected,
        })
    }
}

fn close_period_impl(period_id: i64) -> serde_json::Value {
    // 1. SELECT period FOR UPDATE; state gate.
    let period = match read_period_for_update(period_id) {
        Ok(p) => p,
        Err(e) => ereport_internal(format!("ledger_close_period: read period: {e}")),
    };
    let (start_date, end_date, state) = period;
    if state != "open" {
        ereport_invalid_state(format!(
            "ledger_close_period: period {period_id} is in state '{state}', expected 'open'"
        ));
    }

    // 2. state -> 'closing'.
    if let Err(e) = run_sql(
        "UPDATE accounting_period SET state='closing' WHERE id=$1",
        &[period_id.into()],
    ) {
        ereport_internal(format!("ledger_close_period: set closing: {e}"));
    }

    // 3. Read unfinalized provisional rows in period.
    let provisionals = match read_unfinalized_in_period(&start_date, &end_date) {
        Ok(p) => p,
        Err(e) => ereport_internal(format!("ledger_close_period: read provisionals: {e}")),
    };

    if provisionals.is_empty() {
        // Trivial close: no provisional rows.
        if let Err(e) = run_sql(
            "UPDATE accounting_period SET state='closed', closed_at=now() WHERE id=$1",
            &[period_id.into()],
        ) {
            ereport_internal(format!("ledger_close_period: set closed: {e}"));
        }
        return CloseSummary {
            period_id,
            ..Default::default()
        }
        .to_json();
    }

    let pool_ids: Vec<i64> = provisionals
        .iter()
        .map(|p| p.pool_id)
        .collect::<BTreeSet<i64>>()
        .into_iter()
        .collect();

    // 4. Acquire pool_lock on each affected pool.
    if let Err(e) = pool_lock::acquire_pool_locks(&pool_ids) {
        ereport_internal(format!("ledger_close_period: pool_lock: {e}"));
    }

    // 5. Per-pool final_avg from in-period receipts.
    let final_avg = match compute_final_avg_per_pool(&pool_ids, &start_date, &end_date) {
        Ok(m) => m,
        Err(e) => ereport_internal(format!("ledger_close_period: final_avg: {e}")),
    };
    for pid in &pool_ids {
        if !final_avg.contains_key(pid) {
            ereport_invalid_state(format!(
                "ledger_close_period: pool {pid} has provisional depletions in period {period_id} \
                 but no in-period receipts — cannot compute final_avg \
                 (main acct equivalent: P0020 wac_periodic_close_no_receipts)"
            ));
        }
    }

    // 6. Per-provisional variance + per-pool aggregation.
    let mut variances: Vec<(i64, i64)> = Vec::with_capacity(provisionals.len()); // (provisional.id, variance)
    let mut pool_variance_sum: BTreeMap<i64, i64> = BTreeMap::new();
    for p in &provisionals {
        let avg = final_avg[&p.pool_id];
        // variance = avg * qty - provisional_amount  (signed)
        let restated = match avg.checked_mul(p.qty) {
            Some(v) => v,
            None => ereport_internal(format!(
                "ledger_close_period: overflow restating pool {} (avg={}, qty={})",
                p.pool_id, avg, p.qty
            )),
        };
        let variance = match restated.checked_sub(p.provisional_amount) {
            Some(v) => v,
            None => ereport_internal(format!(
                "ledger_close_period: overflow variance pool {}",
                p.pool_id
            )),
        };
        variances.push((p.id, variance));
        *pool_variance_sum.entry(p.pool_id).or_insert(0) += variance;
    }

    // 7. Insert one revaluation_run trx for the close + variance trx_lines.
    let close_trx_id = match insert_close_trx(period_id) {
        Ok(id) => id,
        Err(e) => ereport_internal(format!("ledger_close_period: close trx: {e}")),
    };

    // Allocate trx_seq per pool from MAX(trx_seq)+1, incrementing per
    // variance row on that pool. Skip pools whose every variance == 0
    // (no posting_line emitted; no trx_seq advance).
    let mut next_seq: HashMap<i64, i64> = match read_max_trx_seq_per_pool(&pool_ids) {
        Ok(m) => m,
        Err(e) => ereport_internal(format!("ledger_close_period: max_trx_seq: {e}")),
    };

    // Collect variance trx_line + posting_line bulk-write material.
    let mut tl_pool_id: Vec<i64> = Vec::new();
    let mut tl_unit_cost: Vec<i64> = Vec::new();
    let mut tl_trx_seq: Vec<i64> = Vec::new();
    let mut pl_amount: Vec<i64> = Vec::new();
    let mut pl_debit: Vec<i64> = Vec::new();
    let mut pl_credit: Vec<i64> = Vec::new();
    let mut pl_posted_at: Vec<String> = Vec::new();

    // For each provisional row we also need to know which posting_line (if
    // any) was emitted so we can stamp variance_posting_line_id back. The
    // bulk-write returns posting_line.id in input order. We mark which
    // provisional indices have a posting using parallel `posting_for_prov`
    // (None for zero-variance rows).
    let mut posting_for_prov: Vec<Option<usize>> = vec![None; provisionals.len()];

    let mut variance_posted: i64 = 0;
    let mut zero_variance: i64 = 0;
    for (idx, (_pid, var)) in variances.iter().enumerate() {
        if *var == 0 {
            zero_variance += 1;
            continue;
        }
        variance_posted += 1;
        let prov = &provisionals[idx];
        let abs_var = var.unsigned_abs() as i64;
        let (debit, credit) = if *var > 0 {
            (prov.debit_account, prov.credit_account)
        } else {
            (prov.credit_account, prov.debit_account)
        };

        let seq = next_seq.entry(prov.pool_id).or_insert(0);
        *seq += 1;
        tl_pool_id.push(prov.pool_id);
        // unit_cost on the variance trx_line is per-unit variance for audit;
        // qty is 0 (cost-only adjustment). prov.qty > 0 always.
        tl_unit_cost.push(var / prov.qty);
        tl_trx_seq.push(*seq);

        pl_amount.push(abs_var);
        pl_debit.push(debit);
        pl_credit.push(credit);
        pl_posted_at.push(prov.posted_at_rfc3339.clone());

        posting_for_prov[idx] = Some(tl_pool_id.len() - 1);
    }

    let mut variance_posting_ids: Vec<i64> = Vec::new();
    if !tl_pool_id.is_empty() {
        let trx_line_ids = match insert_variance_trx_lines(
            close_trx_id,
            &tl_pool_id,
            &tl_unit_cost,
            &tl_trx_seq,
        ) {
            Ok(ids) => ids,
            Err(e) => ereport_internal(format!(
                "ledger_close_period: variance trx_lines: {e}"
            )),
        };
        variance_posting_ids = match insert_variance_posting_lines(
            &trx_line_ids,
            &pl_amount,
            &pl_debit,
            &pl_credit,
            &pl_posted_at,
        ) {
            Ok(ids) => ids,
            Err(e) => ereport_internal(format!(
                "ledger_close_period: variance posting_lines: {e}"
            )),
        };
    }

    // 9. Adjust pool_state.unit_cost by -Σ variance per pool. Apply even
    // when sum == 0 (no-op): empty UPDATE arrays are skipped below.
    let mut delta_pid: Vec<i64> = Vec::new();
    let mut delta_amt: Vec<i64> = Vec::new();
    for (pid, sum) in &pool_variance_sum {
        if *sum != 0 {
            delta_pid.push(*pid);
            delta_amt.push(*sum);
        }
    }
    if !delta_pid.is_empty() {
        if let Err(e) = run_sql_with_args(
            "UPDATE pool_state \
                SET unit_cost = pool_state.unit_cost - t.delta \
               FROM UNNEST($1::bigint[], $2::bigint[]) AS t(pid, delta) \
              WHERE pool_state.pool_id = t.pid AND pool_state.layer_seq = 0",
            &[delta_pid.into(), delta_amt.into()],
        ) {
            ereport_internal(format!("ledger_close_period: pool_state adj: {e}"));
        }
    }

    // 10. Stamp finalized rows back into posting_lines_provisional.
    let mut prov_id_v: Vec<i64> = Vec::new();
    let mut var_amt_v: Vec<i64> = Vec::new();
    let mut var_pl_id_v: Vec<Option<i64>> = Vec::new();
    for (idx, (pid, var)) in variances.iter().enumerate() {
        prov_id_v.push(*pid);
        var_amt_v.push(*var);
        var_pl_id_v.push(posting_for_prov[idx].map(|i| variance_posting_ids[i]));
    }
    if let Err(e) = run_sql_with_args(
        "UPDATE posting_lines_provisional plp \
            SET finalized_at = now(), \
                variance_amount = t.va, \
                variance_posting_line_id = t.vp \
           FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[]) AS t(pid, va, vp) \
          WHERE plp.id = t.pid",
        &[prov_id_v.into(), var_amt_v.into(), var_pl_id_v.into()],
    ) {
        ereport_internal(format!("ledger_close_period: finalize prov rows: {e}"));
    }

    // 11. state -> 'closed'.
    if let Err(e) = run_sql(
        "UPDATE accounting_period SET state='closed', closed_at=now() WHERE id=$1",
        &[period_id.into()],
    ) {
        ereport_internal(format!("ledger_close_period: set closed: {e}"));
    }

    CloseSummary {
        period_id,
        provisional_finalized: provisionals.len() as i64,
        variance_posted,
        zero_variance,
        pools_affected: pool_ids.len() as i64,
    }
    .to_json()
}

fn read_period_for_update(
    period_id: i64,
) -> Result<(String, String, String), pgrx::spi::Error> {
    // Acquire the row lock via run_with_args (read-write SPI context;
    // FOR UPDATE rejected in client.select's read-only context).
    let locked_count: Option<i64> = Spi::get_one_with_args(
        "WITH locked AS ( \
            SELECT id FROM accounting_period WHERE id = $1 FOR UPDATE \
         ) SELECT count(*) FROM locked",
        &[period_id.into()],
    )?;
    if locked_count.unwrap_or(0) == 0 {
        return Err(pgrx::spi::Error::CursorNotFound(format!(
            "no accounting_period with id={period_id}"
        )));
    }

    // Now safe to read the row's fields via the read-only client.
    Spi::connect(|client| {
        let mut t = client.select(
            "SELECT start_date::text, end_date::text, state \
               FROM accounting_period WHERE id = $1",
            Some(1),
            &[period_id.into()],
        )?;
        if let Some(row) = t.next() {
            let s: String = row.get::<String>(1)?.unwrap_or_default();
            let e: String = row.get::<String>(2)?.unwrap_or_default();
            let st: String = row.get::<String>(3)?.unwrap_or_default();
            Ok((s, e, st))
        } else {
            Err(pgrx::spi::Error::CursorNotFound(format!(
                "no accounting_period with id={period_id}"
            )))
        }
    })
}

fn read_unfinalized_in_period(
    start: &str,
    end: &str,
) -> Result<Vec<Provisional>, pgrx::spi::Error> {
    Spi::connect(|client| {
        let mut out: Vec<Provisional> = Vec::new();
        let mut t = client.select(
            "SELECT plp.id, plp.posting_line_id, plp.pool_id, plp.qty, plp.provisional_amount, \
                    pl.debit_account, pl.credit_account, to_char(pl.posted_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.US+00:00') \
               FROM posting_lines_provisional plp \
               JOIN posting_line pl ON pl.id = plp.posting_line_id \
              WHERE plp.finalized_at IS NULL \
                AND pl.posted_at >= $1::date::timestamptz \
                AND pl.posted_at <  ($2::date + 1)::timestamptz \
              ORDER BY plp.id",
            None,
            &[start.to_string().into(), end.to_string().into()],
        )?;
        while let Some(row) = t.next() {
            out.push(Provisional {
                id: row.get::<i64>(1)?.unwrap_or(0),
                posting_line_id: row.get::<i64>(2)?.unwrap_or(0),
                pool_id: row.get::<i64>(3)?.unwrap_or(0),
                qty: row.get::<i64>(4)?.unwrap_or(0),
                provisional_amount: row.get::<i64>(5)?.unwrap_or(0),
                debit_account: row.get::<i64>(6)?.unwrap_or(0),
                credit_account: row.get::<i64>(7)?.unwrap_or(0),
                posted_at_rfc3339: row.get::<String>(8)?.unwrap_or_default(),
            });
        }
        Ok(out)
    })
}

fn compute_final_avg_per_pool(
    pool_ids: &[i64],
    start: &str,
    end: &str,
) -> Result<HashMap<i64, i64>, pgrx::spi::Error> {
    Spi::connect(|client| {
        let mut out: HashMap<i64, i64> = HashMap::new();
        let mut t = client.select(
            "SELECT tl.pool_id, SUM(pl.amount)::bigint, SUM(tl.qty)::bigint \
               FROM posting_line pl \
               JOIN trx_line tl ON tl.id = pl.trx_line_id \
              WHERE pl.event_type = 'inventory_receipt' \
                AND tl.pool_id = ANY($1::bigint[]) \
                AND pl.posted_at >= $2::date::timestamptz \
                AND pl.posted_at <  ($3::date + 1)::timestamptz \
              GROUP BY tl.pool_id",
            None,
            &[
                pool_ids.to_vec().into(),
                start.to_string().into(),
                end.to_string().into(),
            ],
        )?;
        while let Some(row) = t.next() {
            let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
            // SUM returns NUMERIC in PG; pgrx's i64 fetch fails on that.
            // Cast to bigint in the SELECT for both sums.
            let val: i64 = row.get::<i64>(2)?.unwrap_or(0);
            let qty: i64 = row.get::<i64>(3)?.unwrap_or(0);
            if qty > 0 {
                out.insert(pid, val / qty);
            } else if qty < 0 {
                // Unusual; treat as undefined (raise upstream).
                continue;
            }
        }
        Ok(out)
    })
}

fn read_max_trx_seq_per_pool(
    pool_ids: &[i64],
) -> Result<HashMap<i64, i64>, pgrx::spi::Error> {
    Spi::connect(|client| {
        let mut out: HashMap<i64, i64> = HashMap::new();
        for pid in pool_ids {
            out.insert(*pid, 0);
        }
        let mut t = client.select(
            "SELECT pool_id, COALESCE(MAX(trx_seq), 0)::bigint \
               FROM trx_line WHERE pool_id = ANY($1::bigint[]) GROUP BY pool_id",
            None,
            &[pool_ids.to_vec().into()],
        )?;
        while let Some(row) = t.next() {
            let pid: i64 = row.get::<i64>(1)?.unwrap_or(0);
            let seq: i64 = row.get::<i64>(2)?.unwrap_or(0);
            out.insert(pid, seq);
        }
        Ok(out)
    })
}

fn insert_close_trx(period_id: i64) -> Result<i64, pgrx::spi::Error> {
    let id: Option<i64> = Spi::get_one_with_args(
        "INSERT INTO trx (trx_type, source_id, posted_at) \
         VALUES ('revaluation_run', $1, now()) RETURNING id",
        &[period_id.into()],
    )?;
    Ok(id.unwrap_or_default())
}

fn insert_variance_trx_lines(
    trx_id: i64,
    pool_ids: &[i64],
    unit_costs: &[i64],
    trx_seqs: &[i64],
) -> Result<Vec<i64>, pgrx::spi::Error> {
    Spi::connect(|client| {
        let mut out: Vec<i64> = Vec::with_capacity(pool_ids.len());
        let mut t = client.select(
            "INSERT INTO trx_line (trx_id, pool_id, line_type, qty, unit_cost, trx_seq) \
             SELECT $1, pid, 'revaluation_line'::line_type, 0, uc, ts \
               FROM UNNEST($2::bigint[], $3::bigint[], $4::bigint[]) AS t(pid, uc, ts) \
             RETURNING id",
            None,
            &[
                trx_id.into(),
                pool_ids.to_vec().into(),
                unit_costs.to_vec().into(),
                trx_seqs.to_vec().into(),
            ],
        )?;
        while let Some(row) = t.next() {
            out.push(row.get::<i64>(1)?.unwrap_or(0));
        }
        Ok(out)
    })
}

fn insert_variance_posting_lines(
    trx_line_ids: &[i64],
    amounts: &[i64],
    debits: &[i64],
    credits: &[i64],
    posted_ats: &[String],
) -> Result<Vec<i64>, pgrx::spi::Error> {
    Spi::connect(|client| {
        let mut out: Vec<i64> = Vec::with_capacity(trx_line_ids.len());
        let mut t = client.select(
            "INSERT INTO posting_line (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
             SELECT tl, 'variance'::posting_event_type, amt, deb, cr, pa::timestamptz \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::text[]) \
                    AS t(tl, amt, deb, cr, pa) \
             RETURNING id",
            None,
            &[
                trx_line_ids.to_vec().into(),
                amounts.to_vec().into(),
                debits.to_vec().into(),
                credits.to_vec().into(),
                posted_ats.to_vec().into(),
            ],
        )?;
        while let Some(row) = t.next() {
            out.push(row.get::<i64>(1)?.unwrap_or(0));
        }
        Ok(out)
    })
}

fn run_sql(
    sql: &str,
    args: &[pgrx::datum::DatumWithOid<'_>],
) -> Result<(), pgrx::spi::Error> {
    Spi::run_with_args(sql, args)
}

fn run_sql_with_args(
    sql: &str,
    args: &[pgrx::datum::DatumWithOid<'_>],
) -> Result<(), pgrx::spi::Error> {
    Spi::run_with_args(sql, args)
}

#[allow(unreachable_code)]
fn ereport_invalid_state(msg: String) -> ! {
    ereport!(
        ERROR,
        PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
        msg
    );
    unreachable!()
}

#[allow(unreachable_code)]
fn ereport_internal(msg: String) -> ! {
    ereport!(ERROR, PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, msg);
    unreachable!()
}
