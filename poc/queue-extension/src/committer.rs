//! acct-4d4n.3 (M1.2) + acct-4d4n.4 (M2.1):
//! committer election + batch drain with PocCostMethod dispatch.
//!
//! M1.2 wired the basic round-trip: caller pushes one event, wins the
//! CAS election, drains its own batch of 1, INSERTs into the
//! placeholder `poc_test_rows`, fills its slot, returns. M2.1 plugs
//! the PocCostMethod trait in:
//!
//! - The drained batch is grouped by `(pool, method_tag)` and
//!   dispatched through `cost_method::resolve_method`.
//! - A single batched dedup-lookup SPI (spec §1.6 step 17d) covers
//!   ALL issue_ids in the group — ONE query regardless of batch size.
//! - Events are partitioned into three categories per spec §3.10:
//!     replayed   → result already in `poc_cost_*`; cached values
//!                  returned without re-running `plan_apply`.
//!     to_plan    → fresh events; passed to `method.plan_apply`.
//!     to_plan after `plan_apply` further splits into:
//!     success    → INSERT new rows.
//!     event-error → no rows; per-event error_code returned in slot.
//! - Per-event errors do NOT roll back the batch — spec §3.10 is
//!   explicit that the sub-tx commits with only success rows. M2.1
//!   has no sub-tx (see M1.2 docstring) but preserves the spec's
//!   commit semantics: error events leave no row, success events
//!   write one row each, replay events touch nothing.
//!
//! ## Schema dependencies
//!
//! The dedup-lookup reads `poc_cost_depletions` UNION ALL
//! `poc_cost_consumptions`; both tables are created in cost_method.rs.
//! MockMethod only writes consumptions; M2.2's FifoMethod will exercise
//! the depletions branch when it ships.
//!
//! ## Sub-transaction discipline — still deferred
//!
//! Same rationale as M1.2: spec §1.6 step 17b prescribes
//! `BeginInternalSubTransaction("poc_committer_batch")` around the SPI
//! work so a batch-level failure can be rolled back independently of
//! the caller's user-tx. The placeholder INSERTs here still have no
//! realistic failure path (CHECK qty>0 is the only constraint
//! reachable from `MockMethod`'s output, and the wrapper enforces that
//! upstream), and the resource-owner conflicts with plpgsql DO-block
//! callers haven't been resolved yet. We revisit at M3.2 when pool
//! locks introduce real serialization-failure paths.

use crate::cost_method::{
    self, PocApplyBatch, PocApplyEvent, PocApplyResult, PocConsumptionRow,
    PocDepletionRow, PocError, PocPoolKey, PocSnapshot,
};
use crate::queue::{
    self, DrainedApply, METHOD_AVG, METHOD_FIFO, METHOD_STD, POC_SHARD_COUNT,
    SLOT_FILLED,
};
use pgrx::prelude::*;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ── Schema (poc_test_rows) ────────────────────────────────────────────
//
// Spec §1.2 references `poc_test_rows` as the M1.x INSERT placeholder.
// M2.1 keeps the table around for backward-compat with the M1.2
// acceptance scripts, but the live cost-tracking writes go into
// `poc_cost_consumptions` / `poc_cost_depletions` (see cost_method.rs).

pgrx::extension_sql!(
    r#"
    CREATE TABLE poc_test_rows (
        row_id          BIGSERIAL PRIMARY KEY,
        shard_idx       INTEGER  NOT NULL,
        slot_idx        INTEGER  NOT NULL,
        request_seq     BIGINT   NOT NULL,
        committer_tx_id BIGINT   NOT NULL,
        user_tx_xid     xid8     NOT NULL,
        pool_hash       BIGINT   NOT NULL,
        method_tag      SMALLINT NOT NULL,
        event_sku_id    BIGINT   NOT NULL,
        event_location_id BIGINT NOT NULL,
        event_qty       BIGINT   NOT NULL,
        qty_fake        BIGINT   NOT NULL,
        inserted_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );
    CREATE INDEX poc_test_rows_committer_tx_id_idx
        ON poc_test_rows (shard_idx, committer_tx_id);
    "#,
    name = "poc_test_rows_schema",
);

// ── Helpers ───────────────────────────────────────────────────────────

fn clock_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// splitmix64 — cheap, decent avalanche, deterministic.
fn pool_hash(sku_id: i64, location_id: i64) -> u64 {
    let mut x: u64 = (sku_id as u64).wrapping_mul(0x9E3779B97F4A7C15);
    x ^= (location_id as u64).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

fn shard_for(pool_hash_val: u64) -> usize {
    (pool_hash_val & (POC_SHARD_COUNT as u64 - 1)) as usize
}

fn method_tag_for(method_name: &str) -> Option<u8> {
    match method_name {
        "fifo" | "FIFO" => Some(METHOD_FIFO),
        "avg" | "AVG" => Some(METHOD_AVG),
        "std" | "STD" => Some(METHOD_STD),
        // M2.1 only — "mock" routes to METHOD_FIFO tag for storage
        // purposes (the method_tag is just a discriminator; resolve_method
        // dispatches all four to MockMethod for now).
        "mock" | "MOCK" => Some(METHOD_FIFO),
        _ => None,
    }
}

// ── Dedup-lookup result rows ──────────────────────────────────────────

/// Single row returned by the §1.6 step 17d UNION-ALL query. The
/// `unit_cost` column is `applied_unit_cost` on the consumption side
/// or per-layer `unit_cost` on the depletion side; for replay
/// aggregation, FIFO needs all rows summed (qty + cost_amount), AVG/STD
/// has at most one row per (issue, method).
#[derive(Debug, Clone)]
struct DedupRow {
    // Kept for diagnostics; the issue_id is also the HashMap key in
    // dedup_lookup's return value, so this field isn't read after the
    // map is built. The dead_code allow scopes narrowly here.
    #[allow(dead_code)]
    issue_id: i64,
    qty: i64,
    unit_cost: i64,
}

/// Aggregate dedup rows into a per-issue replay result. For
/// consumptions there's exactly one row per issue (UNIQUE on (issue,
/// method)); for depletions there can be N rows (one per layer).
struct ReplaySummary {
    qty: i64,
    /// Weighted-average unit cost across all matching rows.
    /// For consumptions this is the single row's applied_unit_cost.
    /// For FIFO depletions this is (sum cost_amount) / (sum qty).
    weighted_unit_cost: i64,
}

fn aggregate_replay(rows: &[DedupRow]) -> ReplaySummary {
    let mut sum_qty: i64 = 0;
    let mut sum_cost: i128 = 0;
    for r in rows {
        sum_qty = sum_qty.saturating_add(r.qty);
        sum_cost = sum_cost.saturating_add((r.qty as i128) * (r.unit_cost as i128));
    }
    let weighted = if sum_qty != 0 {
        (sum_cost / (sum_qty as i128)) as i64
    } else {
        0
    };
    ReplaySummary {
        qty: sum_qty,
        weighted_unit_cost: weighted,
    }
}

// ── Dedup-lookup SPI ──────────────────────────────────────────────────
//
// Spec §1.6 step 17d: ONE SPI per group covering ALL issue_ids in
// the batch via WHERE issue_id = ANY($1::bigint[]) AND method_used = $2.
// The UNION ALL unifies depletion rows (multi-row per issue) and
// consumption rows (one-row per issue) under a common shape so the
// caller doesn't need to know which kind of method emitted the
// original.

fn dedup_lookup(issue_ids: &[i64], method_name: &str) -> HashMap<i64, Vec<DedupRow>> {
    let mut out: HashMap<i64, Vec<DedupRow>> = HashMap::new();
    if issue_ids.is_empty() {
        return out;
    }
    Spi::connect(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            issue_ids.to_vec().into(),
            method_name.to_string().into(),
        ];
        let tup = client
            .select(
                "SELECT issue_id, qty, unit_cost \
                   FROM poc_cost_depletions \
                  WHERE issue_id = ANY($1::BIGINT[]) AND method_used = $2 \
                  UNION ALL \
                 SELECT issue_id, qty, applied_unit_cost AS unit_cost \
                   FROM poc_cost_consumptions \
                  WHERE issue_id = ANY($1::BIGINT[]) AND method_used = $2",
                None,
                &args,
            )
            .expect("dedup-lookup SPI");
        for row in tup {
            let issue_id: i64 = row["issue_id"].value().unwrap().unwrap();
            let qty: i64 = row["qty"].value().unwrap().unwrap();
            let unit_cost: i64 = row["unit_cost"].value().unwrap().unwrap();
            out.entry(issue_id).or_default().push(DedupRow {
                issue_id,
                qty,
                unit_cost,
            });
        }
    });
    out
}

// ── Bulk INSERT helpers ───────────────────────────────────────────────

fn insert_consumptions(
    rows: &[PocConsumptionRow],
    method_name: &str,
    committer_tx_id: i64,
    consumed_at_micros: i64,
    consumed_seq_base: i64,
) {
    if rows.is_empty() {
        return;
    }
    let sku_ids: Vec<i64> = rows.iter().map(|r| r.sku_id).collect();
    let location_ids: Vec<i64> = rows.iter().map(|r| r.location_id).collect();
    let qtys: Vec<i64> = rows.iter().map(|r| r.qty).collect();
    let unit_costs: Vec<i64> = rows.iter().map(|r| r.applied_unit_cost).collect();
    let issue_ids: Vec<i64> = rows.iter().map(|r| r.issue_id).collect();
    let user_tx_xids: Vec<i64> = rows.iter().map(|r| r.user_tx_xid).collect();
    let consumed_seqs: Vec<i64> = (0..rows.len() as i64)
        .map(|i| consumed_seq_base.wrapping_add(i))
        .collect();

    Spi::connect_mut(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            sku_ids.into(),
            location_ids.into(),
            qtys.into(),
            unit_costs.into(),
            issue_ids.into(),
            method_name.to_string().into(),
            committer_tx_id.into(),
            user_tx_xids.into(),
            consumed_at_micros.into(),
            consumed_seqs.into(),
        ];
        client
            .update(
                "INSERT INTO poc_cost_consumptions \
                   (sku_id, location_id, qty, applied_unit_cost, consumed_at, \
                    consumed_seq, issue_id, method_used, committer_tx_id, user_tx_xid) \
                 SELECT sku, loc, q, uc, \
                        to_timestamp(($9::BIGINT) / 1e6) AT TIME ZONE 'UTC', \
                        cs, iss, $6, $7::BIGINT, ux::TEXT::xid8 \
                   FROM unnest($1::BIGINT[], $2::BIGINT[], $3::BIGINT[], $4::BIGINT[], \
                               $5::BIGINT[], $8::BIGINT[], $10::BIGINT[]) \
                        AS x(sku, loc, q, uc, iss, ux, cs)",
                None,
                &args,
            )
            .expect("insert_consumptions: bulk INSERT");
    });
}

fn insert_depletions(
    rows: &[PocDepletionRow],
    method_name: &str,
    committer_tx_id: i64,
    consumed_at_micros: i64,
    consumed_seq_base: i64,
) {
    if rows.is_empty() {
        return;
    }
    let layer_ids: Vec<i64> = rows.iter().map(|r| r.layer_id).collect();
    let qtys: Vec<i64> = rows.iter().map(|r| r.qty).collect();
    let unit_costs: Vec<i64> = rows.iter().map(|r| r.unit_cost).collect();
    let issue_ids: Vec<i64> = rows.iter().map(|r| r.issue_id).collect();
    let user_tx_xids: Vec<i64> = rows.iter().map(|r| r.user_tx_xid).collect();
    let consumed_seqs: Vec<i64> = (0..rows.len() as i64)
        .map(|i| consumed_seq_base.wrapping_add(i))
        .collect();

    Spi::connect_mut(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            layer_ids.into(),
            qtys.into(),
            unit_costs.into(),
            issue_ids.into(),
            method_name.to_string().into(),
            committer_tx_id.into(),
            user_tx_xids.into(),
            consumed_at_micros.into(),
            consumed_seqs.into(),
        ];
        client
            .update(
                "INSERT INTO poc_cost_depletions \
                   (layer_id, qty, unit_cost, consumed_at, consumed_seq, \
                    issue_id, method_used, committer_tx_id, user_tx_xid) \
                 SELECT l, q, uc, \
                        to_timestamp(($8::BIGINT) / 1e6) AT TIME ZONE 'UTC', \
                        cs, iss, $5, $6::BIGINT, ux::TEXT::xid8 \
                   FROM unnest($1::BIGINT[], $2::BIGINT[], $3::BIGINT[], $4::BIGINT[], \
                               $7::BIGINT[], $9::BIGINT[]) \
                        AS x(l, q, uc, iss, ux, cs)",
                None,
                &args,
            )
            .expect("insert_depletions: bulk INSERT");
    });
}

// ── M1.2-compat poc_test_rows INSERT ──────────────────────────────────
//
// Continues to write one row per drained request into the placeholder
// table so the M1.2 acceptance tests keep passing. Independent of the
// M2.1 cost-method dispatch; serves as the "did the committer fire"
// audit trail under MockMethod.

fn insert_test_rows(
    shard_idx: usize,
    drained: &[DrainedApply],
    committer_tx_id: i64,
) {
    if drained.is_empty() {
        return;
    }
    let shard_idxs: Vec<i32> = drained.iter().map(|_| shard_idx as i32).collect();
    let slot_idxs: Vec<i32> = drained.iter().map(|d| d.slot_idx as i32).collect();
    let request_seqs: Vec<i64> = drained.iter().map(|d| d.request_seq as i64).collect();
    let ctx_ids: Vec<i64> = drained.iter().map(|_| committer_tx_id).collect();
    let user_tx_xids: Vec<i64> = drained.iter().map(|d| d.user_tx_xid).collect();
    let pool_hashes: Vec<i64> = drained.iter().map(|d| d.pool_hash as i64).collect();
    let method_tags: Vec<i32> = drained.iter().map(|d| d.method_tag as i32).collect();
    let event_sku_ids: Vec<i64> = drained.iter().map(|d| d.event_sku_id).collect();
    let event_locs: Vec<i64> = drained.iter().map(|d| d.event_location_id).collect();
    let event_qtys: Vec<i64> = drained.iter().map(|d| d.event_qty).collect();

    Spi::connect_mut(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            shard_idxs.into(),
            slot_idxs.into(),
            request_seqs.into(),
            ctx_ids.into(),
            user_tx_xids.into(),
            pool_hashes.into(),
            method_tags.into(),
            event_sku_ids.into(),
            event_locs.into(),
            event_qtys.into(),
        ];
        client
            .update(
                "INSERT INTO poc_test_rows \
                 (shard_idx, slot_idx, request_seq, committer_tx_id, user_tx_xid, \
                  pool_hash, method_tag, event_sku_id, event_location_id, \
                  event_qty, qty_fake) \
                 SELECT s, sl, rs, ctx, ux::TEXT::xid8, ph, mt, sku, loc, q, q \
                   FROM unnest($1::INT[], $2::INT[], $3::BIGINT[], $4::BIGINT[], \
                               $5::BIGINT[], $6::BIGINT[], $7::INT[], $8::BIGINT[], \
                               $9::BIGINT[], $10::BIGINT[]) \
                        AS x(s, sl, rs, ctx, ux, ph, mt, sku, loc, q)",
                None,
                &args,
            )
            .expect("insert_test_rows: poc_test_rows");
    });
}

// ── Group-level processing ────────────────────────────────────────────
//
// One pool × one method = one group. Implements §1.6 step 17 + §3.10
// 3-category partitioning. Caller (drain_and_commit) groups the
// drained Vec by (pool_hash, method_tag) and calls this per group.

#[derive(Debug, Clone, Copy)]
enum SlotResolution {
    /// plan_apply emitted a row OR the dedup-lookup matched.
    Filled {
        applied_unit_cost: i64,
        applied_total_cost: i64,
    },
    /// plan_apply returned a per-event error.
    Error { code: u16 },
}

fn process_group(
    drained: &[DrainedApply],
    method_tag: u8,
    pool: PocPoolKey,
    committer_tx_id: i64,
    consumed_at_micros: i64,
) -> Vec<(u32, SlotResolution)> {
    let method_name = cost_method::method_name_for_tag(method_tag);
    let method = match cost_method::resolve_method(method_name) {
        Some(m) => m,
        None => {
            // Should be unreachable — method_tag is bounded by the push
            // path. Surface as MethodNotFound on every slot in the group.
            return drained
                .iter()
                .map(|d| {
                    (
                        d.slot_idx,
                        SlotResolution::Error {
                            code: PocError::MethodNotFound.as_code(),
                        },
                    )
                })
                .collect();
        }
    };

    // (1) Collect issue_ids → dedup-lookup ONE SPI per group.
    let issue_ids: Vec<i64> = drained.iter().map(|d| d.event_issue_id).collect();
    let dedup_rows = dedup_lookup(&issue_ids, method_name);

    // (2) Partition events into replayed vs to_plan.
    let mut to_plan_events: Vec<PocApplyEvent> = Vec::new();
    let mut replayed: HashMap<u32, SlotResolution> = HashMap::new();
    for (idx, d) in drained.iter().enumerate() {
        if let Some(rows) = dedup_rows.get(&d.event_issue_id) {
            // Replayed: aggregate the cached rows and surface result.
            let agg = aggregate_replay(rows);
            replayed.insert(
                d.slot_idx,
                SlotResolution::Filled {
                    applied_unit_cost: agg.weighted_unit_cost,
                    applied_total_cost: agg
                        .weighted_unit_cost
                        .saturating_mul(agg.qty),
                },
            );
        } else {
            // Fresh event — pass into plan_apply.
            to_plan_events.push(PocApplyEvent {
                event_seq: idx as u64,
                qty: d.event_qty,
                at_micros: consumed_at_micros,
                issue_id: d.event_issue_id,
                user_tx_xid: d.user_tx_xid,
                slot_idx: d.slot_idx,
            });
        }
    }

    // (3) Build snapshot. M2.1 MockMethod ignores snapshot; M2.3's
    // AVG / STD will populate it via SPI here, and M2.2 reads layers.
    let snapshot = PocSnapshot {
        pool,
        ..Default::default()
    };

    // (4) Plan. plan_apply is pure; safe to call without sub-tx.
    let result: PocApplyResult = if to_plan_events.is_empty() {
        PocApplyResult::default()
    } else {
        let batch = PocApplyBatch {
            pool,
            events: to_plan_events.clone(),
        };
        method.plan_apply(&batch, &snapshot)
    };

    // (5) Partition plan_apply output: success rows go to INSERT, errors
    // bypass INSERT entirely.
    let mut success_rows_for_slot: HashMap<u64, SlotResolution> = HashMap::new();
    let mut error_rows_for_slot: HashMap<u64, SlotResolution> = HashMap::new();
    for er in &result.per_event {
        match er.error {
            Some(e) => {
                error_rows_for_slot.insert(
                    er.event_seq,
                    SlotResolution::Error { code: e.as_code() },
                );
            }
            None => {
                success_rows_for_slot.insert(
                    er.event_seq,
                    SlotResolution::Filled {
                        applied_unit_cost: er.applied_unit_cost,
                        applied_total_cost: er.applied_total_cost,
                    },
                );
            }
        }
    }

    // (6) INSERT success rows (consumption + depletion).
    if !result.consumption_inserts.is_empty() {
        insert_consumptions(
            &result.consumption_inserts,
            method_name,
            committer_tx_id,
            consumed_at_micros,
            0,
        );
    }
    if !result.depletion_inserts.is_empty() {
        insert_depletions(
            &result.depletion_inserts,
            method_name,
            committer_tx_id,
            consumed_at_micros,
            0,
        );
    }

    // (7) Map back to per-slot resolutions in ORIGINAL batch order. The
    // event_seq we assigned at step (2) matches to_plan position; we
    // need the (slot_idx → resolution) map. Replayed entries already
    // carry slot_idx.
    let mut out: Vec<(u32, SlotResolution)> =
        Vec::with_capacity(drained.len());
    for (idx, d) in drained.iter().enumerate() {
        if let Some(r) = replayed.get(&d.slot_idx) {
            out.push((d.slot_idx, *r));
            continue;
        }
        // Not replayed — find its event_seq in to_plan_events (which we
        // built by walking drained in order; for fresh events the
        // event_seq equals the source `idx`).
        let event_seq = idx as u64;
        if let Some(r) = success_rows_for_slot.get(&event_seq) {
            out.push((d.slot_idx, *r));
        } else if let Some(r) = error_rows_for_slot.get(&event_seq) {
            out.push((d.slot_idx, *r));
        } else {
            // Method emitted neither success nor error for an event it
            // received — bug in the method impl. Surface as
            // MethodNotFound (closest existing variant) to keep the
            // slot unstuck.
            out.push((
                d.slot_idx,
                SlotResolution::Error {
                    code: PocError::MethodNotFound.as_code(),
                },
            ));
        }
    }
    out
}

// ── Committer drain (entry point used by poc_ledger_apply + tick) ─────
//
// Returns the count of drained requests so callers can detect "no work
// available" without an extra SQL probe.

fn drain_and_commit(shard_idx: usize, batch_size_max: usize) -> usize {
    let drained: Vec<DrainedApply> =
        queue::drain_apply_batch(shard_idx, batch_size_max);
    if drained.is_empty() {
        return 0;
    }

    let committer_tx_id = queue::next_committer_tx_id(shard_idx);
    let consumed_at_micros = (clock_ns() / 1_000) as i64;

    // Per spec §1.6 step 16: group by (pool_hash, method_tag). The
    // drained Vec is already from one shard but pools within a shard
    // can vary (hash collision), and methods can vary per event. Group
    // them, then process each group independently.
    let mut groups: HashMap<(i64, u8), Vec<DrainedApply>> = HashMap::new();
    for d in drained.iter() {
        groups
            .entry((d.pool_hash as i64, d.method_tag))
            .or_default()
            .push(d.clone());
    }

    // Backward-compat: still write to poc_test_rows so M1.2 scripts work.
    insert_test_rows(shard_idx, &drained, committer_tx_id);

    // Per-group processing → resolutions.
    let mut all_resolutions: Vec<(u32, SlotResolution)> = Vec::new();
    for ((_pool_hash, method_tag), group) in groups.iter() {
        let pool = PocPoolKey {
            sku_id: group[0].event_sku_id,
            location_id: group[0].event_location_id,
        };
        let resolutions = process_group(
            group,
            *method_tag,
            pool,
            committer_tx_id,
            consumed_at_micros,
        );
        all_resolutions.extend(resolutions);
    }

    // Fill each slot. State flips to SLOT_FILLED regardless of error;
    // the error_code field carries the per-event outcome.
    for (slot_idx, resolution) in all_resolutions {
        let (unit, total, err_code) = match resolution {
            SlotResolution::Filled {
                applied_unit_cost,
                applied_total_cost,
            } => (applied_unit_cost, applied_total_cost, 0u16),
            SlotResolution::Error { code } => (0, 0, code),
        };
        let res = queue::fill_slot_result_with_error(
            shard_idx,
            slot_idx,
            unit,
            total,
            committer_tx_id,
            err_code,
        );
        if let Err(actual) = res {
            pgrx::error!(
                "drain_and_commit: slot {} fill CAS failed (actual state {})",
                slot_idx,
                actual
            );
        }
    }

    drained.len()
}

// ── SQL surface (M1.2 + M2.1 entry points) ────────────────────────────

/// Push a request and immediately drain it (single-backend convenience
/// wrapper). Caller wins the committer election by definition.
#[pg_extern]
fn poc_ledger_apply(
    sku_id: i64,
    location_id: i64,
    qty: i64,
    issue_id: default!(i64, 0),
    method: default!(String, "'fifo'"),
) -> TableIterator<
    'static,
    (
        name!(shard_idx, i32),
        name!(slot_idx, i32),
        name!(request_seq, i64),
        name!(committer_tx_id, i64),
        name!(applied_unit_cost, i64),
        name!(applied_total_cost, i64),
        name!(error_code, i32),
    ),
> {
    let method_tag = match method_tag_for(&method) {
        Some(t) => t,
        None => pgrx::error!(
            "poc_ledger_apply: unknown method '{}' (expected fifo/avg/std/mock)",
            method
        ),
    };

    let user_tx_xid: i64 = unsafe {
        let full = pgrx::pg_sys::GetCurrentFullTransactionId();
        full.value as i64
    };

    let ph = pool_hash(sku_id, location_id);
    let shard_idx = shard_for(ph);

    let slot_idx = queue::acquire_slot(shard_idx).unwrap_or_else(|| {
        pgrx::error!(
            "poc_ledger_apply: slot pool exhausted on shard {} \
             (M5c.1 backpressure not yet implemented)",
            shard_idx
        )
    });

    let my_pid: i32 = unsafe { pgrx::pg_sys::MyProcPid };
    let request_seq = queue::ring_push_apply(
        shard_idx,
        slot_idx,
        ph,
        my_pid,
        method_tag,
        qty,
        0,
        issue_id,
        sku_id,
        location_id,
        user_tx_xid,
    )
    .unwrap_or_else(|_| {
        pgrx::error!(
            "poc_ledger_apply: shard {} ring full (M5c.1 backpressure \
             not yet implemented)",
            shard_idx
        )
    });

    let won = queue::try_acquire_committer(shard_idx, my_pid, clock_ns());
    if !won {
        pgrx::error!(
            "poc_ledger_apply: committer election lost on shard {} \
             (M1.2 expects single-backend — M3.1 introduces waiter path)",
            shard_idx
        );
    }
    let _drained = drain_and_commit(shard_idx, 1024);
    queue::release_committer(shard_idx);

    let (state, applied_unit_cost, applied_total_cost, ctx_id, error_code) =
        queue::read_slot_result_with_error(shard_idx, slot_idx);
    if state != SLOT_FILLED {
        pgrx::error!(
            "poc_ledger_apply: own slot {} not SLOT_FILLED after drain (state={})",
            slot_idx,
            state
        );
    }
    let _ = queue::recycle_slot(shard_idx, slot_idx);

    TableIterator::once((
        shard_idx as i32,
        slot_idx as i32,
        request_seq as i64,
        ctx_id,
        applied_unit_cost,
        applied_total_cost,
        error_code as i32,
    ))
}

/// Push a request without electing the committer. Used by M2.1 tests
/// to assemble multi-event batches that one subsequent
/// `poc_ledger_committer_tick` drains together.
#[pg_extern]
fn poc_ledger_push_only(
    sku_id: i64,
    location_id: i64,
    qty: i64,
    issue_id: i64,
    method: default!(String, "'fifo'"),
) -> TableIterator<
    'static,
    (
        name!(shard_idx, i32),
        name!(slot_idx, i32),
        name!(request_seq, i64),
    ),
> {
    let method_tag = match method_tag_for(&method) {
        Some(t) => t,
        None => pgrx::error!(
            "poc_ledger_push_only: unknown method '{}' (expected fifo/avg/std/mock)",
            method
        ),
    };
    let user_tx_xid: i64 = unsafe {
        let full = pgrx::pg_sys::GetCurrentFullTransactionId();
        full.value as i64
    };
    let ph = pool_hash(sku_id, location_id);
    let shard_idx = shard_for(ph);
    let slot_idx = queue::acquire_slot(shard_idx).unwrap_or_else(|| {
        pgrx::error!(
            "poc_ledger_push_only: slot pool exhausted on shard {}",
            shard_idx
        )
    });
    let my_pid: i32 = unsafe { pgrx::pg_sys::MyProcPid };
    let request_seq = queue::ring_push_apply(
        shard_idx,
        slot_idx,
        ph,
        my_pid,
        method_tag,
        qty,
        0,
        issue_id,
        sku_id,
        location_id,
        user_tx_xid,
    )
    .unwrap_or_else(|_| {
        pgrx::error!(
            "poc_ledger_push_only: shard {} ring full",
            shard_idx
        )
    });
    TableIterator::once((shard_idx as i32, slot_idx as i32, request_seq as i64))
}

/// Try to win the committer election on `shard_idx` and drain up to
/// `batch_size_max` requests. Returns the count of drained requests
/// (0 if either the election was lost or the ring was empty).
#[pg_extern]
fn poc_ledger_committer_tick(
    shard_idx: i32,
    batch_size_max: default!(i32, 1024),
) -> i64 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return -1;
    }
    let my_pid: i32 = unsafe { pgrx::pg_sys::MyProcPid };
    let won = queue::try_acquire_committer(shard_idx as usize, my_pid, clock_ns());
    if !won {
        return 0;
    }
    let drained = drain_and_commit(shard_idx as usize, batch_size_max.max(1) as usize);
    queue::release_committer(shard_idx as usize);
    drained as i64
}

/// Read a slot's result tuple. Returns the state + result + error_code.
/// Does NOT recycle the slot (caller may want to inspect the recycle
/// behavior or run multiple reads).
#[pg_extern]
fn poc_ledger_slot_result(
    shard_idx: i32,
    slot_idx: i32,
) -> TableIterator<
    'static,
    (
        name!(state, i32),
        name!(applied_unit_cost, i64),
        name!(applied_total_cost, i64),
        name!(committer_tx_id, i64),
        name!(error_code, i32),
    ),
> {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return TableIterator::once((-1, 0, 0, 0, -1));
    }
    let (state, unit, total, ctx, err) = queue::read_slot_result_with_error(
        shard_idx as usize,
        slot_idx as u32,
    );
    TableIterator::once((
        state as i32,
        unit,
        total,
        ctx,
        err as i32,
    ))
}

/// Test-only: recycle a slot back to SLOT_FREE. Mirrors the recycle
/// that `poc_ledger_apply` does inline.
#[pg_extern]
fn poc_ledger_slot_recycle_after_read(shard_idx: i32, slot_idx: i32) -> i32 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return -1;
    }
    match queue::recycle_slot(shard_idx as usize, slot_idx as u32) {
        Ok(prev) => prev as i32,
        Err(actual) => -(actual as i32) - 1,
    }
}

/// Returns the per-shard `committer_tx_seq` AFTER the most recent
/// fetch_add. Useful for tests that assert C2 I4 (monotonic per shard).
#[pg_extern]
fn poc_ledger_shard_committer_tx_seq(shard_idx: i32) -> i64 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return -1;
    }
    let arena = queue::POC_SHARD_ARENA.share();
    arena.shards[shard_idx as usize]
        .committer_tx_seq
        .load(std::sync::atomic::Ordering::Acquire) as i64
}

/// Tests routing a `(sku, location)` pair to its destination shard.
/// Mirrors the internal `shard_for(pool_hash(...))` computation so
/// tests don't need to reimplement the hash.
#[pg_extern]
fn poc_ledger_shard_for(sku_id: i64, location_id: i64) -> i32 {
    shard_for(pool_hash(sku_id, location_id)) as i32
}
