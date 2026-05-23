//! Bulk-write helpers (design-v3 §5.4 step 9).
//!
//! Path B mirror of `ledger-direct/src/bulk_write.rs`. Same SQL shape;
//! the committer (acct-usn2) invokes these inside its own
//! `BackgroundWorker::transaction(...)` block, one PG transaction per
//! commit_group. Per locked plan §G Q3: copy-paste from ledger-direct
//! to keep the PoC straight (resist premature abstraction); when both
//! paths stabilize, an `apply_plan_result` shared helper may move into
//! `ledger-core` if measurement justifies.
//!
//! Four entry points, called in FK order by
//! `committer::process_commit_group`:
//!   1. `insert_trx`                — RETURNING trx.id (1 row)
//!   2. `insert_trx_lines`          — UNNEST INSERT RETURNING trx_line.ids
//!   3. `apply_pool_state_mutations`— up to 4 SQL stmts (Insert / Upsert /
//!                                    Update / Delete) over the planned mutations
//!   4. `insert_posting_lines`      — UNNEST INSERT
//!
//! Each entry point skips its SPI on empty input. The pool_state mutations
//! and posting_lines reference trx_line.id by resolving the
//! `last_trx_line_idx` / `trx_line_idx` index against the input-order id
//! vector returned by step 2.
//!
//! INSERT ... SELECT ... UNNEST RETURNING — PG returns rows in the order
//! the SELECT produces them, which here mirrors the input array order.
//! Defensive remap by `(pool_id, trx_seq)` UNIQUE inside
//! `insert_trx_lines` so a hypothetical RETURNING reorder doesn't corrupt
//! downstream id resolution.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use ledger_core::{
    PlanResult, PoolStateMutation, PostingLineRequest, ProvisionalPostingRequest, TrxLineOutput,
};
use pgrx::prelude::*;

/// 9.1 — INSERT INTO trx. Returns the new trx.id.
///
/// `posted_at` is rendered RFC3339 and cast to timestamptz inside the
/// SQL — sidesteps the pg_epoch ↔ unix_epoch micros conversion that
/// `pgrx::datum::TimestampWithTimeZone::try_from(i64)` needs.
#[allow(dead_code)] // wired by committer::process_commit_group (acct-usn2)
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

/// 9.2 — Bulk INSERT INTO trx_line ... RETURNING id, in input order.
///
/// The PlanResult contract is that `trx_lines[i]` corresponds to the i-th
/// inserted row, so the returned Vec is index-aligned and consumable by
/// the downstream mutation helpers.
#[allow(dead_code)]
pub fn insert_trx_lines(
    trx_id: i64,
    outputs: &[TrxLineOutput],
) -> Result<Vec<i64>, pgrx::spi::Error> {
    if outputs.is_empty() {
        return Ok(Vec::new());
    }

    let pool_id: Vec<i64> = outputs.iter().map(|o| o.pool_id).collect();
    let line_type: Vec<String> = outputs.iter().map(|o| o.line_type.as_sql().to_string()).collect();
    let source_id: Vec<Option<i64>> = outputs.iter().map(|o| o.source_id).collect();
    let qty: Vec<i64> = outputs.iter().map(|o| o.qty).collect();
    let unit_cost: Vec<i64> = outputs.iter().map(|o| o.unit_cost).collect();
    let trx_seq: Vec<i64> = outputs.iter().map(|o| o.trx_seq).collect();
    let source_trx_line_id: Vec<Option<i64>> =
        outputs.iter().map(|o| o.source_trx_line_id).collect();

    // The (pool_id, trx_seq) UNIQUE constraint defends against a
    // RETURNING-order-not-input-order regression: we re-resolve ids via
    // a map keyed on (pool_id, trx_seq) so the contract is enforced
    // regardless of which row PG returns first.
    let returned: Vec<(i64, i64, i64)> =
        Spi::connect(|client| -> Result<Vec<(i64, i64, i64)>, pgrx::spi::Error> {
            let mut out = Vec::with_capacity(outputs.len());
            let mut t = client.select(
                "INSERT INTO trx_line \
                   (trx_id, pool_id, line_type, source_id, qty, unit_cost, trx_seq, source_trx_line_id) \
                 SELECT $1, pid, lt::line_type, sid, q, uc, ts, stl \
                   FROM UNNEST($2::bigint[], $3::text[], $4::bigint[], $5::bigint[], $6::bigint[], $7::bigint[], $8::bigint[]) \
                        AS t(pid, lt, sid, q, uc, ts, stl) \
                 RETURNING id, pool_id, trx_seq",
                None,
                &[
                    trx_id.into(),
                    pool_id.into(),
                    line_type.into(),
                    source_id.into(),
                    qty.into(),
                    unit_cost.into(),
                    trx_seq.into(),
                    source_trx_line_id.into(),
                ],
            )?;
            while let Some(row) = t.next() {
                let id: i64 = row.get::<i64>(1)?.unwrap_or(0);
                let pid: i64 = row.get::<i64>(2)?.unwrap_or(0);
                let ts: i64 = row.get::<i64>(3)?.unwrap_or(0);
                out.push((id, pid, ts));
            }
            Ok(out)
        })?;

    let map: HashMap<(i64, i64), i64> = returned
        .into_iter()
        .map(|(id, pid, ts)| ((pid, ts), id))
        .collect();
    let ids: Vec<i64> = outputs
        .iter()
        .map(|o| {
            map.get(&(o.pool_id, o.trx_seq))
                .copied()
                .expect("trx_line RETURNING dropped a row")
        })
        .collect();
    Ok(ids)
}

/// 9.3-9.6 — Apply pool_state mutations in four batches: Insert, Upsert,
/// Update, Delete. Each batch skips SPI when empty.
///
/// Insert/Upsert resolve `last_trx_line_idx` against `trx_line_ids` (the
/// vector returned by `insert_trx_lines`).
#[allow(dead_code)]
pub fn apply_pool_state_mutations(
    mutations: &[PoolStateMutation],
    trx_line_ids: &[i64],
) -> Result<(), pgrx::spi::Error> {
    if mutations.is_empty() {
        return Ok(());
    }

    let mut ins_pid = Vec::new();
    let mut ins_seq = Vec::new();
    let mut ins_qty = Vec::new();
    let mut ins_uc = Vec::new();
    let mut ins_last = Vec::new();

    let mut up_pid = Vec::new();
    let mut up_seq = Vec::new();
    let mut up_qty = Vec::new();
    let mut up_uc = Vec::new();
    let mut up_last = Vec::new();

    let mut upd_pid = Vec::new();
    let mut upd_seq = Vec::new();
    let mut upd_qty = Vec::new();

    let mut del_pid = Vec::new();
    let mut del_seq = Vec::new();

    for m in mutations {
        match *m {
            PoolStateMutation::Insert {
                pool_id,
                layer_seq,
                qty,
                unit_cost,
                last_trx_line_idx,
            } => {
                ins_pid.push(pool_id);
                ins_seq.push(layer_seq);
                ins_qty.push(qty);
                ins_uc.push(unit_cost);
                ins_last.push(trx_line_ids[last_trx_line_idx]);
            }
            PoolStateMutation::Upsert {
                pool_id,
                layer_seq,
                qty,
                unit_cost,
                last_trx_line_idx,
            } => {
                up_pid.push(pool_id);
                up_seq.push(layer_seq);
                up_qty.push(qty);
                up_uc.push(unit_cost);
                up_last.push(trx_line_ids[last_trx_line_idx]);
            }
            PoolStateMutation::Update {
                pool_id,
                layer_seq,
                qty,
            } => {
                upd_pid.push(pool_id);
                upd_seq.push(layer_seq);
                upd_qty.push(qty);
            }
            PoolStateMutation::Delete { pool_id, layer_seq } => {
                del_pid.push(pool_id);
                del_seq.push(layer_seq);
            }
        }
    }

    if !ins_pid.is_empty() {
        Spi::run_with_args(
            "INSERT INTO pool_state (pool_id, layer_seq, qty, unit_cost, last_trx_line_id) \
             SELECT pid, seq, q, uc, last FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[]) \
                  AS t(pid, seq, q, uc, last)",
            &[
                ins_pid.into(),
                ins_seq.into(),
                ins_qty.into(),
                ins_uc.into(),
                ins_last.into(),
            ],
        )?;
    }

    if !up_pid.is_empty() {
        Spi::run_with_args(
            "INSERT INTO pool_state (pool_id, layer_seq, qty, unit_cost, last_trx_line_id) \
             SELECT pid, seq, q, uc, last FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[]) \
                  AS t(pid, seq, q, uc, last) \
             ON CONFLICT (pool_id, layer_seq) DO UPDATE \
                SET qty = EXCLUDED.qty, unit_cost = EXCLUDED.unit_cost, last_trx_line_id = EXCLUDED.last_trx_line_id",
            &[
                up_pid.into(),
                up_seq.into(),
                up_qty.into(),
                up_uc.into(),
                up_last.into(),
            ],
        )?;
    }

    if !upd_pid.is_empty() {
        Spi::run_with_args(
            "UPDATE pool_state SET qty = u.q \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[]) AS u(pid, seq, q) \
              WHERE pool_state.pool_id = u.pid AND pool_state.layer_seq = u.seq",
            &[upd_pid.into(), upd_seq.into(), upd_qty.into()],
        )?;
    }

    if !del_pid.is_empty() {
        Spi::run_with_args(
            "DELETE FROM pool_state \
              USING UNNEST($1::bigint[], $2::bigint[]) AS d(pid, seq) \
              WHERE pool_state.pool_id = d.pid AND pool_state.layer_seq = d.seq",
            &[del_pid.into(), del_seq.into()],
        )?;
    }

    Ok(())
}

/// 9.7 — Bulk INSERT INTO posting_line ... RETURNING id, in input order.
///
/// Returns the posting_line.id list so `insert_provisional_postings` can
/// resolve `ProvisionalPostingRequest.posting_line_idx` (acct-s6fa). Resolves
/// `trx_line_idx` against the input-order `trx_line_ids` from step 9.2.
#[allow(dead_code)]
pub fn insert_posting_lines(
    requests: &[PostingLineRequest],
    trx_line_ids: &[i64],
) -> Result<Vec<i64>, pgrx::spi::Error> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let tl_id: Vec<i64> = requests.iter().map(|r| trx_line_ids[r.trx_line_idx]).collect();
    let event_type: Vec<String> = requests
        .iter()
        .map(|r| r.event_type.as_sql().to_string())
        .collect();
    let amount: Vec<i64> = requests.iter().map(|r| r.amount).collect();
    let debit: Vec<i64> = requests.iter().map(|r| r.debit_account).collect();
    let credit: Vec<i64> = requests.iter().map(|r| r.credit_account).collect();
    let posted_at: Vec<String> = requests.iter().map(|r| r.posted_at.to_rfc3339()).collect();

    let ids: Vec<i64> = Spi::connect(|client| -> Result<Vec<i64>, pgrx::spi::Error> {
        let mut out = Vec::with_capacity(requests.len());
        let mut t = client.select(
            "INSERT INTO posting_line (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
             SELECT tl, et::posting_event_type, amt, deb, cr, pa::timestamptz \
               FROM UNNEST($1::bigint[], $2::text[], $3::bigint[], $4::bigint[], $5::bigint[], $6::text[]) \
                    AS t(tl, et, amt, deb, cr, pa) \
             RETURNING id",
            None,
            &[
                tl_id.into(),
                event_type.into(),
                amount.into(),
                debit.into(),
                credit.into(),
                posted_at.into(),
            ],
        )?;
        while let Some(row) = t.next() {
            out.push(row.get::<i64>(1)?.unwrap_or(0));
        }
        Ok(out)
    })?;

    debug_assert_eq!(ids.len(), requests.len());
    Ok(ids)
}

/// 9.8 — Bulk INSERT INTO posting_lines_provisional (acct-s6fa).
///
/// One row per wac_periodic depletion in the submission. Skipped when
/// `provisionals` is empty (no wac_periodic depletions touched).
#[allow(dead_code)]
pub fn insert_provisional_postings(
    provisionals: &[ProvisionalPostingRequest],
    posting_line_ids: &[i64],
) -> Result<(), pgrx::spi::Error> {
    if provisionals.is_empty() {
        return Ok(());
    }

    let pl_id: Vec<i64> = provisionals
        .iter()
        .map(|p| posting_line_ids[p.posting_line_idx])
        .collect();
    let pool_id: Vec<i64> = provisionals.iter().map(|p| p.pool_id).collect();
    let qty: Vec<i64> = provisionals.iter().map(|p| p.qty).collect();
    let prov_amount: Vec<i64> = provisionals.iter().map(|p| p.provisional_amount).collect();

    Spi::run_with_args(
        "INSERT INTO posting_lines_provisional (posting_line_id, pool_id, qty, provisional_amount) \
         SELECT pl, pi, q, pa \
           FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[]) \
                AS t(pl, pi, q, pa)",
        &[
            pl_id.into(),
            pool_id.into(),
            qty.into(),
            prov_amount.into(),
        ],
    )?;
    Ok(())
}

/// Convenience wrapper: run the full §5.4 step 9 sequence given the
/// per-submission fields and a fresh `PlanResult`. The committer
/// (acct-usn2) calls this once per submission after running
/// `ledger_core::plan_apply` against the hydrated snapshot inside its
/// pristine-replay loop.
#[allow(dead_code)]
pub fn apply_plan_result(
    trx_type: &str,
    source_id: i64,
    posted_at: DateTime<Utc>,
    plan: &PlanResult,
) -> Result<i64, pgrx::spi::Error> {
    let trx_id = insert_trx(trx_type, source_id, posted_at)?;
    let trx_line_ids = insert_trx_lines(trx_id, &plan.trx_lines)?;
    apply_pool_state_mutations(&plan.pool_state_mutations, &trx_line_ids)?;
    let posting_line_ids = insert_posting_lines(&plan.posting_lines, &trx_line_ids)?;
    insert_provisional_postings(&plan.provisional_postings, &posting_line_ids)?;
    Ok(trx_id)
}
