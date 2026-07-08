//! SPIKE-A (acct-0at4.11.1): staging-table drain — the alternative-A committer.
//!
//! FEEDBACK-ARCH.md #3 / alt A: the shmem routed stack (CQ ring, arena, router
//! union-find, identity/generation election, the pg_xact_status/eject apparatus)
//! is pure queue management that exists only because enqueue escaped the caller's
//! transaction. A durable, transactional, ordered queue already exists under
//! Postgres: a table. This module is the throwaway staging-table committer that
//! benchmarks that claim.
//!
//! `ledger_staging_drain_c(limit)` claims up to `limit` pending `ledger_inbox`
//! rows with `FOR UPDATE SKIP LOCKED ORDER BY id`, applies them as ONE coalesced
//! commit group, marks them done, and returns the number of trx committed. The
//! caller (a harness "committer" connection) loops this call; K such connections
//! stand in for the routed flavor's K BGWorker committers (COMMITTER_COUNT = 4).
//!
//! The apply path is byte-identical to the routed committer's: it reuses
//! `pool_lock::acquire_pool_locks`, `hydration::hydrate_snapshot`,
//! `plan_apply_provisional`, and the batched `bulk_write::*` helpers verbatim.
//! `apply_commit_group` mirrors `ledger-routed-c` `committer::plan_and_write`
//! (the §6.7 collapse: plan each submission sequentially into a trial snapshot,
//! drop-and-continue on a LedgerError, then one aggregate UPSERT per touched pool)
//! — reproduced here rather than shared so the routed crate under measurement is
//! not perturbed. Only the transport differs: a table claim instead of a shmem
//! commit_group.
//!
//! Throwaway-spike simplifications vs the routed committer, all benign on the
//! measured happy path (no crashes, unique source_ids, depletions that fit):
//!   - No per-submission subtransaction / §6.8 SQLSTATE classification. In the
//!     happy path the only Postgres error a batch could raise is a 23505 on a
//!     duplicate (trx_type, source_id), which the single-submission-per-source_id
//!     workload never produces; a LedgerError (e.g. InsufficientInventory) is a
//!     Rust `Err` handled by drop-and-continue, not a Postgres ERROR.
//!   - Naive claim (no pool→committer partition). Per-committer FIFO holds within
//!     one claim; cross-committer same-pool order is not guaranteed — the same
//!     non-guarantee routed has across chunks (acct-0at4.1), so it is the fair
//!     basis for a throughput comparison. Per-pool-partitioned "FIFO by
//!     construction" is the correctness follow-up, not built here.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use ledger_core::{
    plan_apply_provisional, PlanResult, PoolStateMutation, PostingLineRequest, Snapshot,
    TrxLineOutput, TrxLineRequest,
};
use pgrx::prelude::*;
use serde::Deserialize;

use ledger_spi_common::line_type::decode_line_type;
use ledger_spi_common::{bulk_write, hydration, pool_lock};

/// JSONB element shape for `ledger_inbox.lines` — identical to the direct
/// flavor's `LineJson` (submit.rs): a stringly-typed line_type decoded here.
#[derive(Deserialize)]
struct LineJson {
    pool_id: i64,
    line_type: String,
    #[serde(default)]
    source_id: Option<i64>,
    qty: i64,
    unit_cost: i64,
}

/// A claimed inbox row decoded and ready to apply against the working snapshot.
struct Prepared {
    trx_type: String,
    source_id: i64,
    posted_at: DateTime<Utc>,
    lines: Vec<TrxLineRequest>,
}

/// Claim up to `p_limit` pending `ledger_inbox` rows (SKIP LOCKED, id order),
/// apply them as one coalesced commit group, mark them done, and return the
/// number of rows **claimed** this drain (the committer loop's progress signal;
/// 0 means the queue is empty). Committed trx are counted independently by the
/// harness observer; a dropped depletion still counts as claimed and is marked
/// done — matching routed's drop-and-continue so it is not re-claimed forever.
#[pg_extern]
fn ledger_staging_drain_c(p_limit: i32) -> i64 {
    let limit = p_limit.max(1) as i64;

    // 1. Claim a batch. FOR UPDATE SKIP LOCKED needs a read-write SPI context
    //    (same reason pool_lock uses connect_mut). Raw tuples are pulled inside
    //    the closure; decoding happens after so error handling stays simple.
    let raw: Vec<(i64, String, i64, String, serde_json::Value)> =
        Spi::connect_mut(|client| -> Result<_, pgrx::spi::Error> {
            let mut table = client.update(
                "SELECT id, trx_type, source_id, posted_at, lines \
                   FROM ledger_inbox \
                  WHERE NOT done \
                  ORDER BY id \
                    FOR UPDATE SKIP LOCKED \
                  LIMIT $1",
                None,
                &[limit.into()],
            )?;
            let mut out = Vec::new();
            while let Some(row) = table.next() {
                let id = row.get::<i64>(1)?.expect("ledger_inbox.id NOT NULL");
                let trx_type = row.get::<String>(2)?.expect("ledger_inbox.trx_type NOT NULL");
                let source_id = row.get::<i64>(3)?.expect("ledger_inbox.source_id NOT NULL");
                let posted_at = row.get::<String>(4)?.expect("ledger_inbox.posted_at NOT NULL");
                let lines = row.get::<pgrx::JsonB>(5)?.expect("ledger_inbox.lines NOT NULL").0;
                out.push((id, trx_type, source_id, posted_at, lines));
            }
            Ok(out)
        })
        .unwrap_or_else(|e| ereport_internal(format!("ledger_staging_drain_c: claim failed: {e}")));

    if raw.is_empty() {
        return 0;
    }

    // 2. Decode claimed rows into Prepared submissions.
    let mut ids: Vec<i64> = Vec::with_capacity(raw.len());
    let mut prepared: Vec<Prepared> = Vec::with_capacity(raw.len());
    for (id, trx_type, source_id, posted_at_s, lines_json) in raw {
        ids.push(id);
        let posted_at: DateTime<Utc> = match DateTime::parse_from_rfc3339(&posted_at_s) {
            Ok(t) => t.with_timezone(&Utc),
            Err(e) => ereport_internal(format!(
                "ledger_staging_drain_c: row {id} posted_at not RFC3339: {e}"
            )),
        };
        let line_jsons: Vec<LineJson> = match serde_json::from_value(lines_json) {
            Ok(v) => v,
            Err(e) => ereport_internal(format!(
                "ledger_staging_drain_c: row {id} lines decode failed: {e}"
            )),
        };
        let lines: Vec<TrxLineRequest> = line_jsons
            .iter()
            .map(|l| {
                let lt = decode_line_type(&l.line_type).unwrap_or_else(|| {
                    ereport_internal(format!(
                        "ledger_staging_drain_c: row {id} unknown line_type '{}'",
                        l.line_type
                    ))
                });
                TrxLineRequest {
                    pool_id: l.pool_id,
                    line_type: lt,
                    source_id: l.source_id,
                    qty: l.qty,
                    unit_cost: l.unit_cost,
                }
            })
            .collect();
        prepared.push(Prepared { trx_type, source_id, posted_at, lines });
    }

    // 3. Touched pools = ascending union across the whole claimed group.
    let touched: Vec<i64> = prepared
        .iter()
        .flat_map(|p| p.lines.iter().map(|l| l.pool_id))
        .collect::<BTreeSet<i64>>()
        .into_iter()
        .collect();

    let claimed = ids.len() as i64;

    // 4. Lock → hydrate → coalesced plan+write (the routed committer's apply).
    let _committed = apply_commit_group(&touched, &prepared)
        .unwrap_or_else(|e| ereport_internal(format!("ledger_staging_drain_c: apply failed: {e}")));

    // 5. Mark every claimed row done (committed + dropped alike), in the same
    //    transaction as the ledger writes — so a mid-drain crash commits nothing
    //    and the rows are re-claimed cleanly (WAL is the recovery protocol).
    Spi::connect_mut(|client| -> Result<(), pgrx::spi::Error> {
        client.update(
            "UPDATE ledger_inbox SET done = true WHERE id = ANY($1)",
            None,
            &[ids.into()],
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| ereport_internal(format!("ledger_staging_drain_c: mark-done failed: {e}")));

    claimed
}

/// Lock the touched pools, hydrate their snapshot, and apply the claimed group.
fn apply_commit_group(touched: &[i64], prepared: &[Prepared]) -> Result<usize, pgrx::spi::Error> {
    pool_lock::acquire_pool_locks(touched)?;
    let snapshot = hydration::hydrate_snapshot(touched)?;
    plan_and_write(snapshot, prepared)
}

/// Drop-and-continue forward pass + batched write for the whole claimed group.
///
/// Mirror of `ledger-routed-c` `committer::plan_and_write` (the §6.7 collapse):
/// each submission is planned into a trial clone; on `Ok` the clone becomes the
/// working snapshot and the plan is queued, on a `plan_apply_provisional` Err the
/// clone is discarded (Path C has no cross-trx hot-path state). Then one batched
/// write per table — trx / trx_line / posting_line — plus one aggregate UPSERT
/// per touched pool read from the post-pass snapshot. Returns the committed count.
fn plan_and_write(mut snapshot: Snapshot, prepared: &[Prepared]) -> Result<usize, pgrx::spi::Error> {
    let mut planned: Vec<(&Prepared, PlanResult)> = Vec::with_capacity(prepared.len());
    let mut agg_pools: BTreeSet<i64> = BTreeSet::new();

    for p in prepared {
        let mut trial = snapshot.clone();
        match plan_apply_provisional(&mut trial, &p.lines, p.posted_at) {
            Ok(plan) => {
                for m in &plan.pool_state_mutations {
                    if let PoolStateMutation::UpsertAggregate { pool_id, .. } = m {
                        agg_pools.insert(*pool_id);
                    }
                }
                snapshot = trial;
                planned.push((p, plan));
            }
            Err(_) => { /* drop-and-continue */ }
        }
    }

    if planned.is_empty() {
        return Ok(0);
    }

    // Batched write across the group (one multi-row INSERT per table). The FK
    // chain trx.id → trx_line.id → posting_line.trx_line_id is recovered via the
    // same UNNEST WITH ORDINALITY + ascending-id alignment the routed path uses.
    let trx_types: Vec<String> = planned.iter().map(|(p, _)| p.trx_type.clone()).collect();
    let source_ids: Vec<i64> = planned.iter().map(|(p, _)| p.source_id).collect();
    let posted_ats: Vec<DateTime<Utc>> = planned.iter().map(|(p, _)| p.posted_at).collect();
    let trx_ids = bulk_write::insert_trx_batch(&trx_types, &source_ids, &posted_ats)?;

    let total_lines: usize = planned.iter().map(|(_, plan)| plan.trx_lines.len()).sum();
    let mut flat_lines: Vec<TrxLineOutput> = Vec::with_capacity(total_lines);
    let mut line_trx_id: Vec<i64> = Vec::with_capacity(total_lines);
    for (i, (_, plan)) in planned.iter().enumerate() {
        for line in &plan.trx_lines {
            flat_lines.push(line.clone());
            line_trx_id.push(trx_ids[i]);
        }
    }
    let flat_line_ids = bulk_write::insert_trx_lines_batch(&line_trx_id, &flat_lines)?;

    let total_postings: usize = planned.iter().map(|(_, plan)| plan.posting_lines.len()).sum();
    let mut posting_tlid: Vec<i64> = Vec::with_capacity(total_postings);
    let mut flat_postings: Vec<PostingLineRequest> = Vec::with_capacity(total_postings);
    let mut offset = 0usize;
    for (_, plan) in &planned {
        let len = plan.trx_lines.len();
        let slice = &flat_line_ids[offset..offset + len];
        offset += len;

        // Layer mutations (specific pools only; none for FIFO/LIFO provisional).
        let layer_muts: Vec<PoolStateMutation> = plan
            .pool_state_mutations
            .iter()
            .filter(|m| !matches!(m, PoolStateMutation::UpsertAggregate { .. }))
            .cloned()
            .collect();
        bulk_write::apply_pool_state_mutations(&layer_muts, slice)?;

        for req in &plan.posting_lines {
            posting_tlid.push(slice[req.trx_line_idx]);
            flat_postings.push(req.clone());
        }
    }
    bulk_write::insert_posting_lines_batch(&posting_tlid, &flat_postings)?;

    // One aggregate UPSERT per touched pool, read from the post-pass snapshot.
    let agg_muts: Vec<PoolStateMutation> = agg_pools
        .iter()
        .filter_map(|&pid| {
            snapshot.aggregate(pid).map(|r| PoolStateMutation::UpsertAggregate {
                pool_id: pid,
                qty: r.qty,
                unit_cost: r.unit_cost,
                value_sum: r.value_sum,
            })
        })
        .collect();
    bulk_write::apply_pool_state_mutations(&agg_muts, &[])?;

    Ok(planned.len())
}

#[allow(unreachable_code)]
fn ereport_internal(msg: String) -> ! {
    ereport!(ERROR, PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, msg);
    unreachable!()
}
