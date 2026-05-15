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
    PocDepletionRow, PocError, PocLayerView, PocPoolKey, PocSnapshot,
};
use crate::queue::{
    self, DrainedApply, METHOD_AVG, METHOD_FIFO, METHOD_MOCK, METHOD_STD,
    POC_SHARD_COUNT, SLOT_ABANDONED, SLOT_FILLED, TakeoverOutcome,
};
use pgrx::prelude::*;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ── PG Latch wait-event flags (latch.h) ───────────────────────────────
//
// Defined locally rather than imported from `pg_sys` so the build
// doesn't depend on whichever subset of latch.h #defines bindgen
// happens to export in any given pgrx release. Values match
// `src/include/storage/latch.h` and have been stable across PG12+.
const WL_LATCH_SET: i32 = 1 << 0;
const WL_TIMEOUT: i32 = 1 << 3;
const WL_POSTMASTER_DEATH: i32 = 1 << 4;

/// Loser-wait timeout in milliseconds. The committer SetLatches the
/// waiter when the slot is filled, so this only fires when (a) the
/// signal was dropped (e.g. waiter PID became invalid) or (b) the
/// loser needs to re-attempt the CAS election because no committer
/// is currently draining. 100ms is well under the 5-min cache window
/// from scheduling primitives and gives 10× per-second retry headroom.
const WAIT_TIMEOUT_MS: i64 = 100;

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
        "mock" | "MOCK" => Some(METHOD_MOCK),
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
    /// Exact sum-of-products: Σ(layer.qty × layer.unit_cost). Matches
    /// the fresh `plan_apply` path's `applied_total_cost`.
    total_cost: i64,
    /// Truncating integer division of total_cost / qty. Matches the
    /// fresh path's `applied_unit_cost`.
    weighted_unit_cost: i64,
}

fn aggregate_replay(rows: &[DedupRow]) -> ReplaySummary {
    let mut sum_qty: i64 = 0;
    let mut sum_cost: i128 = 0;
    for r in rows {
        sum_qty = sum_qty.saturating_add(r.qty);
        sum_cost = sum_cost.saturating_add((r.qty as i128) * (r.unit_cost as i128));
    }
    let total_cost = sum_cost.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
    let weighted = if sum_qty != 0 {
        total_cost / sum_qty
    } else {
        0
    };
    ReplaySummary {
        qty: sum_qty,
        total_cost,
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

// ── Snapshot construction (per spec §1.6 R4) ──────────────────────────
//
// Reads `poc_cost_layers` for a `(sku_id, location_id)` pool with
// `effective_qty = layer.qty - SUM(depletion.qty)` per layer. The query
// holds `FOR UPDATE OF l` so concurrent committers serialize at the
// layer-row grain — M2.2 single-backend doesn't exercise contention,
// but the lock acquisition is the structural prep for M3.x.
//
// M2.2 only needs the FIFO layer view; AVG/STD/Mock skip the SPI and
// return an empty snapshot. M2.3 extends with avg_unit_cost +
// standard_cost lookups.

/// AVG snapshot: read `poc_cost_avg` for the pool under FOR UPDATE.
/// Both fields can be NULL when no receipt has ever upserted the row;
/// AvgMethod surfaces InsufficientInventory in that case.
fn build_snapshot_avg(pool: PocPoolKey) -> PocSnapshot {
    let mut avg_unit_cost: Option<i64> = None;
    let mut total_qty: i64 = 0;
    Spi::connect(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            pool.sku_id.into(),
            pool.location_id.into(),
        ];
        let tup = client
            .select(
                "SELECT running_qty, running_value \
                   FROM poc_cost_avg \
                  WHERE sku_id = $1 AND location_id = $2 \
                  FOR UPDATE",
                None,
                &args,
            )
            .expect("build_snapshot_avg SPI");
        for row in tup {
            let qty: i64 = row["running_qty"].value().unwrap().unwrap_or(0);
            let value: i64 = row["running_value"].value().unwrap().unwrap_or(0);
            total_qty = qty;
            if qty > 0 {
                avg_unit_cost = Some(value / qty);
            }
        }
    });
    PocSnapshot {
        pool,
        layers: vec![],
        avg_unit_cost,
        standard_cost: None,
        total_available_qty: total_qty,
    }
}

/// STD snapshot: read `poc_standard_costs` by sku_id. Pool-key
/// location is ignored — std lookups are per-SKU only.
fn build_snapshot_std(pool: PocPoolKey) -> PocSnapshot {
    let mut standard_cost: Option<i64> = None;
    Spi::connect(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![pool.sku_id.into()];
        let tup = client
            .select(
                "SELECT unit_cost FROM poc_standard_costs WHERE sku_id = $1",
                None,
                &args,
            )
            .expect("build_snapshot_std SPI");
        for row in tup {
            let uc: i64 = row["unit_cost"].value().unwrap().unwrap();
            standard_cost = Some(uc);
        }
    });
    PocSnapshot {
        pool,
        layers: vec![],
        avg_unit_cost: None,
        standard_cost,
        total_available_qty: 0,
    }
}

/// Post-plan_apply hook for AVG: decrement the running aggregate by
/// the sum of consumption qty + qty·avg from the just-INSERTed rows.
/// Preserves the avg invariant `(V - q·avg) / (Q - q) = avg`.
fn decrement_avg_aggregate(pool: PocPoolKey, rows: &[PocConsumptionRow]) {
    if rows.is_empty() {
        return;
    }
    let mut sum_qty: i64 = 0;
    let mut sum_value: i128 = 0;
    for r in rows {
        sum_qty = sum_qty.saturating_add(r.qty);
        sum_value = sum_value
            .saturating_add((r.qty as i128) * (r.applied_unit_cost as i128));
    }
    let dec_value =
        sum_value.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
    Spi::connect_mut(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            sum_qty.into(),
            dec_value.into(),
            pool.sku_id.into(),
            pool.location_id.into(),
        ];
        client
            .update(
                "UPDATE poc_cost_avg \
                    SET running_qty   = running_qty   - $1, \
                        running_value = running_value - $2, \
                        last_updated  = clock_timestamp() \
                  WHERE sku_id = $3 AND location_id = $4",
                None,
                &args,
            )
            .expect("decrement_avg_aggregate UPDATE");
    });
}

fn build_snapshot_fifo(pool: PocPoolKey) -> PocSnapshot {
    let mut layers: Vec<PocLayerView> = Vec::new();
    let mut total_qty: i64 = 0;

    Spi::connect(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            pool.sku_id.into(),
            pool.location_id.into(),
        ];
        let tup = client
            .select(
                "SELECT l.layer_id, l.unit_cost, l.born_seq, \
                        (l.qty - COALESCE(d.consumed, 0))::BIGINT AS effective_qty, \
                        (EXTRACT(EPOCH FROM l.born_at) * 1000000)::BIGINT AS born_at_micros \
                   FROM poc_cost_layers l \
                   LEFT JOIN LATERAL ( \
                        SELECT SUM(qty) AS consumed \
                          FROM poc_cost_depletions \
                         WHERE layer_id = l.layer_id \
                   ) d ON true \
                  WHERE l.sku_id = $1 AND l.location_id = $2 \
                    AND (l.qty - COALESCE(d.consumed, 0)) > 0 \
                  ORDER BY l.born_at, l.born_seq \
                  FOR UPDATE OF l",
                None,
                &args,
            )
            .expect("build_snapshot_fifo SPI");
        for row in tup {
            let layer_id: i64 = row["layer_id"].value().unwrap().unwrap();
            let unit_cost: i64 = row["unit_cost"].value().unwrap().unwrap();
            let born_seq: i64 = row["born_seq"].value().unwrap().unwrap();
            let effective_qty: i64 =
                row["effective_qty"].value().unwrap().unwrap();
            let born_at_micros: i64 =
                row["born_at_micros"].value().unwrap().unwrap_or(0);
            total_qty = total_qty.saturating_add(effective_qty);
            layers.push(PocLayerView {
                layer_id,
                unit_cost,
                effective_qty,
                born_at_micros,
                born_seq,
            });
        }
    });

    PocSnapshot {
        pool,
        layers,
        avg_unit_cost: None,
        standard_cost: None,
        total_available_qty: total_qty,
    }
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
            // Uses exact total_cost (not unit×qty) so replay equals
            // fresh-path return values byte-for-byte. `_qty` is
            // unused; ReplaySummary keeps it for diagnostics.
            let agg = aggregate_replay(rows);
            let _qty = agg.qty;
            replayed.insert(
                d.slot_idx,
                SlotResolution::Filled {
                    applied_unit_cost: agg.weighted_unit_cost,
                    applied_total_cost: agg.total_cost,
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

    // (3a) M3.2 Q-A: acquire the pool lock when there is work to plan
    // and the GUC selects a non-'none' mode. Replay-only groups skip
    // this SPI. The lock is held until the surrounding txn commits.
    if !to_plan_events.is_empty() {
        acquire_pool_lock(pool);
    }

    // (3b) Build snapshot per method. MOCK ignores it; FIFO reads
    // layers; AVG reads the running aggregate; STD reads the per-SKU
    // standard cost. Skipping SPI when there's nothing to plan
    // (all events were dedup hits) keeps idempotent replays cheap.
    let snapshot = if to_plan_events.is_empty() {
        PocSnapshot {
            pool,
            ..Default::default()
        }
    } else {
        match method_name {
            "fifo" => build_snapshot_fifo(pool),
            "avg" => build_snapshot_avg(pool),
            "std" => build_snapshot_std(pool),
            _ => PocSnapshot {
                pool,
                ..Default::default()
            },
        }
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
        // AVG running aggregate decrement runs AFTER consumption INSERT
        // so the next snapshot read reflects the just-committed drain.
        // Decrementing q×avg preserves running_value/running_qty=avg.
        if method_name == "avg" {
            decrement_avg_aggregate(pool, &result.consumption_inserts);
        }
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

// ── M3.2 (acct-4d4n.8) Q-A pool lock ──────────────────────────────────
//
// Serializes committer-to-committer work on the same `(sku_id,
// location_id)` pool when `poc_ledger.pool_lock_mode` is set to a
// non-'none' value. Called by process_group once dedup-lookup has
// confirmed there is work to plan; replay-only batches skip the SPI.
//
// The two table flavours (`poc_pool_locks` vs `poc_pool_lock_anchors`)
// share the same SQL shape — INSERT ... ON CONFLICT DO UPDATE acquires
// a row lock either way. The UPDATE is a no-op self-assignment whose
// only purpose is to take the row lock. 'pool_locks' bumps an audit
// `lock_version` column; 'pool_lock_anchors' has no payload.
fn acquire_pool_lock(pool: PocPoolKey) {
    let mode = crate::pool_lock_mode_str();
    let sql: &'static str = match mode.as_str() {
        "none" => return,
        "pool_locks" => {
            "INSERT INTO poc_pool_locks (sku_id, location_id) \
             VALUES ($1, $2) \
             ON CONFLICT (sku_id, location_id) DO UPDATE \
                 SET lock_version = poc_pool_locks.lock_version + 1"
        }
        "pool_lock_anchors" => {
            "INSERT INTO poc_pool_lock_anchors (sku_id, location_id) \
             VALUES ($1, $2) \
             ON CONFLICT (sku_id, location_id) DO UPDATE \
                 SET sku_id = poc_pool_lock_anchors.sku_id"
        }
        _ => return,
    };
    Spi::connect_mut(|client| {
        let _ = client
            .update(
                sql,
                None,
                &[pool.sku_id.into(), pool.location_id.into()],
            )
            .expect("acquire_pool_lock: UPDATE failed");
    });
}

/// Wake the backend waiting on a slot, if any. Reads the slot's
/// `waiter_pid` (set by `set_slot_waiter_pid` at acquire time);
/// `BackendPidGetProc` returns NULL if the backend has exited or the
/// PID is stale, in which case the wake is silently skipped (the slot
/// will be reclaimed by M5c.2's slot-leak audit). `SetLatch` is
/// idempotent and may target the current backend (committer waking
/// itself for its own slot) — both cases are safe per PG Latch
/// semantics. (acct-4d4n.7, M3.1)
fn signal_waiter(shard_idx: usize, slot_idx: u32) {
    let pid = queue::read_slot_waiter_pid(shard_idx, slot_idx);
    if pid == 0 {
        return;
    }
    unsafe {
        let proc_ = pgrx::pg_sys::BackendPidGetProc(pid);
        if !proc_.is_null() {
            pgrx::pg_sys::SetLatch(&mut (*proc_).procLatch);
        }
    }
}

/// M5c.1 (acct-4d4n.14): wake the backend waiting on the ring for a
/// free slot. Called by `drain_and_commit` after head has been
/// advanced (i.e. ring capacity has opened up). Single-waiter at MVP:
/// the wake targets the one PID registered via
/// `queue::set_ring_full_waiter`; a second waiter that lost the CAS
/// rides the WaitLatch 100ms timeout and retries the push itself —
/// correct because the timeout is far below `queue_full_timeout_ms`.
fn signal_ring_full_waiter(shard_idx: usize) {
    let pid = queue::read_ring_full_waiter(shard_idx);
    if pid == 0 {
        return;
    }
    unsafe {
        let proc_ = pgrx::pg_sys::BackendPidGetProc(pid);
        if !proc_.is_null() {
            pgrx::pg_sys::SetLatch(&mut (*proc_).procLatch);
        }
    }
}

fn drain_and_commit(shard_idx: usize, batch_size_max: usize) -> usize {
    let drained: Vec<DrainedApply> =
        queue::drain_apply_batch(shard_idx, batch_size_max);
    if drained.is_empty() {
        return 0;
    }

    // M5b.2 (acct-4d4n.13): test-only slow-committer simulation. When
    // `poc_ledger.drain_sleep_us` > 0, sleep here AFTER work has been
    // drained off the ring but BEFORE the per-event SPI INSERTs. The
    // committer_pid is still set on this shard, so a contender that
    // probes try_acquire_or_takeover during the sleep sees the lease
    // (possibly expired) AND pg_pid_alive=true → returns Held → backs
    // off. No takeover. Production callers leave the GUC at 0; this
    // branch is a single load + compare → free in the hot path.
    let sleep_us = crate::drain_sleep_us_now();
    if sleep_us > 0 {
        std::thread::sleep(std::time::Duration::from_micros(sleep_us as u64));
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
            // M5a.2 (acct-4d4n.11): a SLOT_ABANDONED CAS failure is
            // the canceled-caller case — caller marked their slot
            // abandoned before ProcessInterrupts unwound them. The
            // committer's durable INSERT just landed (this batch's
            // SPI INSERT precedes the per-event fill loop), so the
            // spec-required "committer still INSERTs durable row"
            // leg of §3.3 is satisfied. Skip the fill, leave the
            // slot abandoned (later recycled), and continue the
            // batch — raising would unwind the whole transaction
            // and roll back the durable row.
            //
            // Any other state (FREE / FILLED) is a real invariant
            // violation: FREE means we drained a phantom entry;
            // FILLED means double-fill. Both raise.
            if actual == SLOT_ABANDONED {
                pgrx::log!(
                    "drain_and_commit: slot {} was abandoned (caller \
                     canceled); durable row preserved",
                    slot_idx
                );
            } else {
                pgrx::error!(
                    "drain_and_commit: slot {} fill CAS failed (actual state {})",
                    slot_idx,
                    actual
                );
            }
        }
        // M3.1 (acct-4d4n.7): wake the backend waiting on this slot.
        // Skipped silently if no waiter was stamped (push-only paths)
        // or the waiter PID has gone stale (M5c.2 will reap).
        signal_waiter(shard_idx, slot_idx);
    }

    // M5c.1 (acct-4d4n.14): drain advanced head inside `drain_apply_batch`
    // (line `shard.head.store(head + n)`), so ring capacity opened up.
    // Wake the per-shard backpressure waiter (if any) so they can
    // retry their `ring_push_apply`.
    signal_ring_full_waiter(shard_idx);

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

    let my_pid: i32 = unsafe { pgrx::pg_sys::MyProcPid };

    let slot_idx = queue::acquire_slot(shard_idx).unwrap_or_else(|| {
        pgrx::error!(
            "poc_ledger_apply: slot pool exhausted on shard {} \
             (M5c.1 backpressure not yet implemented)",
            shard_idx
        )
    });

    // M3.1 (acct-4d4n.7): stamp the waiter PID BEFORE the ring push so
    // a peer committer that drains the request immediately can wake
    // us via `signal_waiter`.
    queue::set_slot_waiter_pid(shard_idx, slot_idx, my_pid);

    // M5c.1 (acct-4d4n.14): backpressure wait around ring_push_apply.
    // First attempt is non-blocking; on RingFull the wait loop sleeps
    // on MyLatch up to `queue_full_timeout_ms`, retrying the push on
    // each wake. A peer committer's `drain_and_commit` wakes us via
    // `signal_ring_full_waiter` once it advances head (slot freed).
    //
    // Slot order rationale (Q3 from the M5c.1 plan): the result-slot
    // was already acquired above; we hold it through the backpressure
    // wait. Cancel mid-wait releases the slot best-effort
    // (mark_slot_abandoned) and a future M5c.2 audit will reap any
    // slots left ALLOCATED past their backend's death. The alternative
    // — acquire-slot AFTER backpressure — would require restructuring
    // the apply path to thread slot_idx deeper into push, and adds
    // complexity disproportionate to the resource leak window.
    let timeout_ms = crate::queue_full_timeout_ms_now() as i64;
    let bp_start_ns = clock_ns();
    let request_seq: u64;
    'backpressure: loop {
        match queue::ring_push_apply(
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
        ) {
            Ok(seq) => {
                request_seq = seq;
                break 'backpressure;
            }
            Err(_) => {
                // Check the budget BEFORE sleeping so a fast caller
                // with timeout_ms=0 returns immediately rather than
                // sleeping 100ms.
                let elapsed_ms =
                    ((clock_ns() - bp_start_ns) / 1_000_000) as i64;
                if elapsed_ms >= timeout_ms {
                    // Best-effort slot cleanup before raising. The
                    // slot is still SLOT_ALLOCATED — release it so a
                    // future apply can reuse it. M5c.2's slot-leak
                    // audit covers stragglers from any path that
                    // misses this.
                    let _ = queue::mark_slot_abandoned(shard_idx, slot_idx);
                    queue::clear_ring_full_waiter_if_self(shard_idx, my_pid);
                    pgrx::ereport!(
                        pgrx::PgLogLevel::ERROR,
                        pgrx::PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
                        format!(
                            "poc_ledger_apply: shard {} ring full; \
                             queue_full_timeout_ms={} exhausted",
                            shard_idx, timeout_ms
                        ),
                    );
                }
                // Claim the per-shard waiter slot. CAS-from-0
                // succeeds for the first contender; subsequent
                // contenders ride the 100ms WaitLatch timeout and
                // re-attempt push without claiming.
                let _ = queue::set_ring_full_waiter(shard_idx, my_pid);
                unsafe {
                    pgrx::pg_sys::ResetLatch(pgrx::pg_sys::MyLatch);
                }
                // Re-check before sleeping (in case a peer advanced
                // head between our failed push and the ResetLatch).
                let remaining_ms = (timeout_ms - elapsed_ms).max(1);
                let wait_ms = remaining_ms.min(WAIT_TIMEOUT_MS);
                unsafe {
                    let _ = pgrx::pg_sys::WaitLatch(
                        pgrx::pg_sys::MyLatch,
                        WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                        wait_ms,
                        pgrx::pg_sys::PG_WAIT_EXTENSION,
                    );
                    if pgrx::pg_sys::InterruptPending != 0 {
                        if pgrx::pg_sys::QueryCancelPending != 0
                            || pgrx::pg_sys::ProcDiePending != 0
                        {
                            let _ = queue::mark_slot_abandoned(shard_idx, slot_idx);
                            queue::clear_ring_full_waiter_if_self(shard_idx, my_pid);
                        }
                        pgrx::pg_sys::ProcessInterrupts();
                    }
                }
            }
        }
    }
    queue::clear_ring_full_waiter_if_self(shard_idx, my_pid);

    // M3.1 (acct-4d4n.7) wait/wake loop. Spec §1.6 steps 6-14:
    //
    // - If we win the committer-PID CAS, drain the batch (which fills
    //   our own slot among others), release the committer role, loop
    //   back. The next iteration's state-check breaks out via SLOT_FILLED.
    // - If we lose, sleep on MyLatch until a peer committer fires
    //   SetLatch on us (`signal_waiter` in `drain_and_commit`) OR a
    //   100ms safety timeout. On wake we re-check slot state; if
    //   still ALLOCATED we retry the CAS election to bypass the
    //   case where the prior committer drained without our request
    //   in the batch (lost ring-tail race).
    //
    // The ResetLatch-then-check-then-Wait order matches the PG
    // idiom (parallel.c et al.): SetLatch fires raise the flag,
    // ResetLatch lowers it, the state check after ResetLatch sees any
    // committed work, and WaitLatch sleeps with no risk of missed wake.
    loop {
        unsafe { pgrx::pg_sys::ResetLatch(pgrx::pg_sys::MyLatch); }

        let state = queue::slot_state(shard_idx, slot_idx);
        if state == SLOT_FILLED {
            break;
        }
        if state == SLOT_ABANDONED {
            pgrx::error!(
                "poc_ledger_apply: slot {} unexpectedly ABANDONED (M5a.2 path)",
                slot_idx
            );
        }

        // M5a.1 (acct-4d4n.10): acquire-or-takeover. On takeover from
        // a dead PID, run orphan recovery so any of OUR slot's prior
        // committer state (in pre/post-INSERT death paths) reconciles
        // BEFORE this committer drains its own batch. Without this,
        // an orphaned slot upstream of `slot_idx` would leak and the
        // current backend would never observe SLOT_FILLED for its own
        // request — recovery is what unwedges this shard.
        let lease_ms = committer_lease_ms_now();
        let lease_ns: u64 = (lease_ms.max(1) as u64).saturating_mul(1_000_000);
        match queue::try_acquire_or_takeover(shard_idx, my_pid, clock_ns(), lease_ns) {
            TakeoverOutcome::Acquired => {
                let _drained = drain_and_commit(shard_idx, 1024);
                queue::release_committer(shard_idx);
                continue;
            }
            TakeoverOutcome::TookOver { stale_pid: _ } => {
                let _ = orphan_recovery(shard_idx);
                let _drained = drain_and_commit(shard_idx, 1024);
                queue::release_committer(shard_idx);
                continue;
            }
            TakeoverOutcome::Held | TakeoverOutcome::Lost => {
                // Fall through to WaitLatch — another live committer
                // is draining (or just won the race).
            }
        }

        unsafe {
            let _ = pgrx::pg_sys::WaitLatch(
                pgrx::pg_sys::MyLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                WAIT_TIMEOUT_MS,
                pgrx::pg_sys::PG_WAIT_EXTENSION,
            );
            // CHECK_FOR_INTERRUPTS via the InterruptPending guard;
            // cleaner FFI surface than the C-macro form.
            if pgrx::pg_sys::InterruptPending != 0 {
                // M5a.2 (acct-4d4n.11): cancel cleanup BEFORE
                // ProcessInterrupts. ProcessInterrupts longjmps via
                // ereport ERROR and unwinds this function without
                // reaching the slot read below — without cleanup the
                // slot stays SLOT_ALLOCATED until shmem reset and
                // committers leak the lease past their backend's death.
                //
                // Best-effort path:
                //   1. mark_slot_abandoned (CAS allocated → abandoned).
                //      If the committer already filled the slot the
                //      CAS fails — fine, the durable row is committed
                //      and a future retry hits dedup-lookup.
                //   2. If we currently hold committer (acquire →
                //      long-wait → cancel race), release_committer
                //      so a peer can take over the shard.
                //
                // signal_waiter on a now-cancelled backend is safe:
                // BackendPidGetProc returns null once the backend
                // exits and the SetLatch is gated on that.
                if pgrx::pg_sys::QueryCancelPending != 0
                    || pgrx::pg_sys::ProcDiePending != 0
                {
                    let _ = queue::mark_slot_abandoned(shard_idx, slot_idx);
                    if queue::read_committer_pid(shard_idx) == my_pid {
                        queue::release_committer(shard_idx);
                    }
                }
                pgrx::pg_sys::ProcessInterrupts();
            }
        }
    }

    let (_state, applied_unit_cost, applied_total_cost, ctx_id, error_code) =
        queue::read_slot_result_with_error(shard_idx, slot_idx);
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

/// Receipt-side entry point: creates a `poc_cost_layers` row directly.
/// Receipts bypass the queue because there's no contention math —
/// inserting a layer is a single SQL INSERT, no committer pricing
/// involved. Returns the new layer_id.
///
/// `committer_tx_id` defaults to 0 / `user_tx_xid` defaults to '0'::xid8
/// via the table defaults; the receipt-time bookkeeping isn't exercised
/// until M5b recovery (a receipt-layer row without a committer is fine
/// — only depletions need to round-trip through the committer for the
/// per-event audit chain).
#[pg_extern]
fn poc_ledger_receive(
    sku_id: i64,
    location_id: i64,
    qty: i64,
    unit_cost: i64,
    source_kind: default!(String, "'receipt'"),
) -> i64 {
    if qty <= 0 {
        pgrx::error!("poc_ledger_receive: qty must be > 0 (got {})", qty);
    }
    let mut layer_id: i64 = 0;
    Spi::connect_mut(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            sku_id.into(),
            location_id.into(),
            qty.into(),
            unit_cost.into(),
            source_kind.into(),
        ];
        let tup = client
            .update(
                "INSERT INTO poc_cost_layers \
                   (sku_id, location_id, qty, unit_cost, source_kind) \
                 VALUES ($1, $2, $3, $4, $5) \
                 RETURNING layer_id",
                None,
                &args,
            )
            .expect("poc_ledger_receive: INSERT poc_cost_layers");
        for row in tup {
            layer_id = row["layer_id"].value().unwrap().unwrap();
        }
    });
    layer_id
}

/// AVG receipt: UPSERT `poc_cost_avg` adding qty + qty·unit_cost to
/// the running aggregate for `(sku, location)`. Distinct from
/// `poc_ledger_receive` (which is FIFO-shaped layer creation) because
/// AVG has no per-layer concept — only the rolling totals matter.
#[pg_extern]
fn poc_ledger_receive_avg(
    sku_id: i64,
    location_id: i64,
    qty: i64,
    unit_cost: i64,
) -> i32 {
    if qty <= 0 {
        pgrx::error!("poc_ledger_receive_avg: qty must be > 0 (got {})", qty);
    }
    if unit_cost < 0 {
        pgrx::error!(
            "poc_ledger_receive_avg: unit_cost must be >= 0 (got {})",
            unit_cost
        );
    }
    let value = (qty as i128).saturating_mul(unit_cost as i128);
    let value_i64 = value.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
    Spi::connect_mut(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            sku_id.into(),
            location_id.into(),
            qty.into(),
            value_i64.into(),
        ];
        client
            .update(
                "INSERT INTO poc_cost_avg \
                   (sku_id, location_id, running_qty, running_value) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (sku_id, location_id) DO UPDATE \
                   SET running_qty   = poc_cost_avg.running_qty   + EXCLUDED.running_qty, \
                       running_value = poc_cost_avg.running_value + EXCLUDED.running_value, \
                       last_updated  = clock_timestamp()",
                None,
                &args,
            )
            .expect("poc_ledger_receive_avg UPSERT");
    });
    0
}

/// STD seed: set the standard cost for a SKU. Idempotent UPSERT; the
/// PoC keeps a flat table (no time-phased revisions).
#[pg_extern]
fn poc_ledger_set_standard_cost(sku_id: i64, unit_cost: i64) -> i32 {
    if unit_cost <= 0 {
        pgrx::error!(
            "poc_ledger_set_standard_cost: unit_cost must be > 0 (got {})",
            unit_cost
        );
    }
    Spi::connect_mut(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![
            sku_id.into(),
            unit_cost.into(),
        ];
        client
            .update(
                "INSERT INTO poc_standard_costs (sku_id, unit_cost) \
                 VALUES ($1, $2) \
                 ON CONFLICT (sku_id) DO UPDATE \
                   SET unit_cost    = EXCLUDED.unit_cost, \
                       effective_at = clock_timestamp()",
                None,
                &args,
            )
            .expect("poc_ledger_set_standard_cost UPSERT");
    });
    0
}

/// Tests routing a `(sku, location)` pair to its destination shard.
/// Mirrors the internal `shard_for(pool_hash(...))` computation so
/// tests don't need to reimplement the hash.
#[pg_extern]
fn poc_ledger_shard_for(sku_id: i64, location_id: i64) -> i32 {
    shard_for(pool_hash(sku_id, location_id)) as i32
}

// ── M5a.1 orphan recovery (acct-4d4n.10) ──────────────────────────────
//
// When a committer dies between drain and slot-fill, the orphaned
// slots stay in SLOT_ALLOCATED with a non-zero issue_id (stamped at
// ring_push_apply). Recovery walks those slots and reconciles each:
//
//   - cost rows exist for issue_id → "post-INSERT death": the dying
//     committer's SPI sub-tx committed before death. Backfill the
//     slot from the aggregated rows.
//
//   - cost rows absent (after a short backoff) → "pre-INSERT death":
//     no work survived. Mark slot ABANDONED.
//
// The backoff handles the very narrow window between the SPI sub-tx
// committing and PG's snapshot machinery making the rows visible to
// the recovery backend. We cap total wait at `committer_lease_ms * 10`
// (default 1s) so the takeover acceptance criterion
// "within 2 × committer_lease_ms p99" can be met by the FAST PATH
// where rows are immediately visible (the slow path is correctness,
// not perf).
//
// The recovery backend looks up cost rows using the SAME UNION-ALL
// shape as the live dedup-lookup, keyed by issue_id only (method is
// derived per-row from the `method_used` column).

#[derive(Debug, Clone, Copy)]
struct RecoveryRow {
    qty: i64,
    unit_cost: i64,
}

/// One-shot SPI lookup for `issue_id` across consumption + depletion
/// tables. Returns aggregated rows per the dedup-lookup shape. Empty
/// vec = no cost rows yet visible.
fn recovery_lookup_one(issue_id: i64) -> Vec<RecoveryRow> {
    let mut out: Vec<RecoveryRow> = Vec::new();
    Spi::connect(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![issue_id.into()];
        let tup = client
            .select(
                "SELECT qty, unit_cost \
                   FROM poc_cost_depletions \
                  WHERE issue_id = $1 \
                  UNION ALL \
                 SELECT qty, applied_unit_cost AS unit_cost \
                   FROM poc_cost_consumptions \
                  WHERE issue_id = $1",
                None,
                &args,
            )
            .expect("recovery_lookup_one SPI");
        for row in tup {
            let qty: i64 = row["qty"].value().unwrap().unwrap();
            let unit_cost: i64 = row["unit_cost"].value().unwrap().unwrap();
            out.push(RecoveryRow { qty, unit_cost });
        }
    });
    out
}

fn aggregate_recovery_rows(rows: &[RecoveryRow]) -> (i64, i64, i64) {
    let mut sum_qty: i64 = 0;
    let mut sum_cost: i128 = 0;
    for r in rows {
        sum_qty = sum_qty.saturating_add(r.qty);
        sum_cost = sum_cost.saturating_add((r.qty as i128) * (r.unit_cost as i128));
    }
    let total_cost = sum_cost.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
    let weighted = if sum_qty != 0 {
        total_cost / sum_qty
    } else {
        0
    };
    (sum_qty, total_cost, weighted)
}

/// Read poc_ledger.committer_lease_ms via SHOW. Returns 100ms default
/// on parse failure (matches the GUC's compile-time default).
fn committer_lease_ms_now() -> i64 {
    let mut out: i64 = 100;
    Spi::connect(|client| {
        if let Ok(tup) = client.select(
            "SELECT current_setting('poc_ledger.committer_lease_ms')::INT AS v",
            None,
            &[],
        ) {
            for row in tup {
                if let Some(v) = row["v"].value::<i32>().ok().flatten() {
                    out = v as i64;
                }
            }
        }
    });
    out
}

/// Reconcile a single orphan slot: backoff-poll cost tables; on hit
/// backfill via `fill_slot_result_with_error`; on miss after the
/// total budget mark ABANDONED. Returns `true` if filled, `false` if
/// abandoned. Wakes the waiter on either outcome.
fn reconcile_orphan_slot(
    shard_idx: usize,
    orphan: &queue::OrphanSlot,
    committer_tx_id: i64,
    total_budget_ms: i64,
) -> bool {
    let mut waited_ms: i64 = 0;
    let mut next_step_ms: i64 = 10;
    loop {
        let rows = recovery_lookup_one(orphan.issue_id);
        if !rows.is_empty() {
            let (_qty, total_cost, weighted) = aggregate_recovery_rows(&rows);
            let res = queue::fill_slot_result_with_error(
                shard_idx,
                orphan.slot_idx,
                weighted,
                total_cost,
                committer_tx_id,
                0,
            );
            if res.is_err() {
                // Slot transitioned out of ALLOCATED while we worked;
                // a concurrent fill won. Leave it; signal the waiter
                // defensively in case the concurrent fill skipped
                // signal_waiter (shouldn't, but harmless).
            }
            signal_waiter(shard_idx, orphan.slot_idx);
            return true;
        }
        if waited_ms >= total_budget_ms {
            // No rows in budget — pre-INSERT death.
            let _ = queue::mark_slot_abandoned(shard_idx, orphan.slot_idx);
            signal_waiter(shard_idx, orphan.slot_idx);
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(next_step_ms as u64));
        waited_ms += next_step_ms;
        next_step_ms = (next_step_ms * 2).min(total_budget_ms - waited_ms).max(1);
    }
}

/// Run orphan recovery against a shard. Walks SLOT_ALLOCATED slots
/// with a stamped issue_id and reconciles each. Returns `(recovered,
/// abandoned)` counts. Intended to be called by the new committer
/// immediately after a successful takeover (see
/// `try_acquire_or_takeover` returning `TookOver`), or via the SQL
/// surface `poc_ledger_orphan_recovery_tick` for tests.
fn orphan_recovery(shard_idx: usize) -> (usize, usize) {
    let orphans = queue::collect_orphan_slots(shard_idx);
    if orphans.is_empty() {
        return (0, 0);
    }
    let lease_ms = committer_lease_ms_now();
    // Total budget = 2 × committer_lease_ms. The exponential backoff
    // (10/20/40/80ms…) catches the SPI sub-tx commit-visibility window;
    // beyond ~80ms the row is either visible or never going to appear
    // (the dying committer's sub-tx either committed atomically or
    // aborted). The spec's `lease_ms * 10` ceiling is conservative for
    // the M5b.1 startup-scan path where there's no caller waiting; the
    // M5a.1 runtime path optimises for the acceptance criterion
    // "lease takeover completes within 2 × committer_lease_ms p99".
    let budget_ms = lease_ms.saturating_mul(2).max(50);
    // committer_tx_id stamped on backfilled slots — uses the takeover
    // backend's own next id so the audit trail attributes recovery to
    // this committer, not the dead one.
    let committer_tx_id = queue::next_committer_tx_id(shard_idx);
    let mut recovered: usize = 0;
    let mut abandoned: usize = 0;
    for o in orphans.iter() {
        if reconcile_orphan_slot(shard_idx, o, committer_tx_id, budget_ms) {
            recovered += 1;
        } else {
            abandoned += 1;
        }
    }
    (recovered, abandoned)
}

/// Try to acquire the committer on `shard_idx`, including the M5a.1
/// dead-PID takeover path. On takeover, run orphan recovery before
/// returning. Returns a row that captures what happened so tests +
/// the apply loop can act on it.
///
/// Outcome encoding (text):
///   acquired      — empty slot, standard CAS-from-0
///   took_over     — stale PID was dead; CAS-replaced + recovery ran
///   held          — committer alive; no takeover
///   lost          — CAS lost to a concurrent acquirer
#[pg_extern]
fn poc_ledger_orphan_recovery_tick(
    shard_idx: i32,
) -> TableIterator<
    'static,
    (
        name!(outcome, String),
        name!(stale_pid, i32),
        name!(recovered, i32),
        name!(abandoned, i32),
    ),
> {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return TableIterator::once(("invalid_shard".to_string(), 0, 0, 0));
    }
    let my_pid: i32 = unsafe { pgrx::pg_sys::MyProcPid };
    let lease_ms = committer_lease_ms_now();
    let lease_ns: u64 = (lease_ms.max(1) as u64).saturating_mul(1_000_000);
    let outcome = queue::try_acquire_or_takeover(
        shard_idx as usize,
        my_pid,
        clock_ns(),
        lease_ns,
    );
    match outcome {
        TakeoverOutcome::Acquired => {
            queue::release_committer(shard_idx as usize);
            TableIterator::once(("acquired".to_string(), 0, 0, 0))
        }
        TakeoverOutcome::TookOver { stale_pid } => {
            let (rec, aba) = orphan_recovery(shard_idx as usize);
            queue::release_committer(shard_idx as usize);
            TableIterator::once((
                "took_over".to_string(),
                stale_pid,
                rec as i32,
                aba as i32,
            ))
        }
        TakeoverOutcome::Held => {
            TableIterator::once(("held".to_string(), 0, 0, 0))
        }
        TakeoverOutcome::Lost => {
            TableIterator::once(("lost".to_string(), 0, 0, 0))
        }
    }
}

/// Test-only entry point matching `queue::inject_dead_committer`.
/// Sets the per-shard committer slot to a fake PID + stale timestamp
/// so the takeover path is exercised without proc_exit. Caller must
/// supply a PID that is genuinely not a live PG backend.
#[pg_extern]
fn poc_ledger_inject_dead_committer(
    shard_idx: i32,
    fake_pid: i32,
    fake_acquired_at_ns: i64,
) -> bool {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return false;
    }
    queue::inject_dead_committer(
        shard_idx as usize,
        fake_pid,
        fake_acquired_at_ns as u64,
    );
    true
}

/// M4.1 (acct-4d4n.9): per-shard stats view aggregating head/tail/depth,
/// the elected committer PID, the per-shard committer_tx_seq, and the
/// sequence counters into one TableIterator. Used by the multi-shard
/// bench harness to observe cross-shard parallelism (multiple
/// `committer_pid != 0` simultaneously) and per-shard fairness
/// (per-shard `committer_tx_seq` deltas).
#[pg_extern]
fn poc_ledger_shard_stats_all() -> TableIterator<
    'static,
    (
        name!(shard_idx, i32),
        name!(head, i64),
        name!(tail, i64),
        name!(depth, i64),
        name!(committer_pid, i32),
        name!(committer_tx_seq, i64),
        name!(next_request_seq, i64),
        name!(next_slot_seq, i64),
    ),
> {
    // Materialize all rows up front under repeated short critical
    // sections rather than streaming. POC_SHARD_COUNT is 16, so the
    // snapshot is cheap and avoids any chance of `share()` interleaving
    // with concurrent committer activity through TableIterator yields.
    let mut rows: Vec<(i32, i64, i64, i64, i32, i64, i64, i64)> =
        Vec::with_capacity(POC_SHARD_COUNT);
    for shard_idx in 0..POC_SHARD_COUNT {
        let (h, t) = queue::shard_head_tail(shard_idx);
        let depth = t.wrapping_sub(h) as i64;
        let cp = queue::read_committer_pid(shard_idx);
        let cs = queue::read_committer_tx_seq(shard_idx) as i64;
        let nrs = queue::read_next_request_seq(shard_idx) as i64;
        let nss = queue::read_next_slot_seq(shard_idx) as i64;
        rows.push((shard_idx as i32, h as i64, t as i64, depth, cp, cs, nrs, nss));
    }
    TableIterator::new(rows.into_iter())
}

// ── M5b.1 (acct-4d4n.12): startup recovery worker ─────────────────────
//
// One-shot bgworker registered in `_PG_init`. Runs once on postmaster
// startup with `start_time = ConsistentState`, `restart_time = None`.
//
// Phase A — counter seeding:
//   Scan `(sku_id, location_id, MAX(committer_tx_id))` across both
//   `poc_cost_consumptions` and `poc_cost_depletions`. For each row,
//   recompute the destination shard via `pool_hash + shard_for` and
//   keep a per-shard running max. Then bump each shard's shmem
//   `committer_tx_seq` to that max via `seed_committer_tx_at_least`.
//
//   Without this, post-restart shmem starts at 0 and the first apply
//   on a shard returns `committer_tx_id = 1` — colliding with any
//   pre-restart durable row that already wrote `committer_tx_id = 1`.
//   Invariant I4 (monotonic per-shard committer_tx_seq) requires that
//   the next emitted value strictly exceeds every value seen in the
//   audit history.
//
// Phase B — xid abort scan (STUB):
//   Walk distinct `user_tx_xid` and call `TransactionIdDidAbort` for
//   each. For aborted xids, log a placeholder pending the real
//   compensation enqueue path that M6.1 (acct-4d4n.16) will provide.
//   Spec §3.5; production xid-watermark tracking is deferred to
//   design-v2 per §7 Q-F.

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn poc_ledger_startup_recovery_main(
    _arg: pgrx::pg_sys::Datum,
) {
    use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);

    let dbname = crate::recovery_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    BackgroundWorker::transaction(|| {
        run_startup_recovery_once();
    });
    // restart_time = None means PG does not relaunch us — we exit
    // here and stay exited until the next postmaster start.
}

fn run_startup_recovery_once() {
    // Pre-check: if the extension's tables don't exist yet (very fresh
    // database, CREATE EXTENSION hasn't been run, or the wrong
    // recovery_database GUC), exit cleanly with a log rather than
    // raising ERROR on a missing-relation SPI failure. Without this
    // gate the bgworker dies on first install and the next manual
    // CREATE EXTENSION + restart would have nothing to seed against.
    let mut have_tables = false;
    Spi::connect(|client| {
        let table = client.select(
            "SELECT to_regclass('poc_cost_consumptions') IS NOT NULL \
               AND to_regclass('poc_cost_layers') IS NOT NULL \
               AND to_regclass('poc_cost_depletions') IS NOT NULL \
               AS ok",
            None,
            &[],
        );
        if let Ok(tab) = table {
            for row in tab {
                let ok: Option<bool> = row["ok"].value().ok().flatten();
                have_tables = ok.unwrap_or(false);
            }
        }
    });
    if !have_tables {
        pgrx::log!(
            "poc_ledger_startup_recovery: cost tables not present in \
             database '{}' (fresh install or wrong recovery_database \
             GUC); exiting without recovery",
            crate::recovery_database_str()
        );
        return;
    }

    // Phase A — seed per-shard counters.
    //
    // Three sources of (sku_id, location_id, committer_tx_id) tuples:
    //   - poc_cost_consumptions (AVG/STD per-event row; carries sku+loc)
    //   - poc_cost_layers (FIFO receipts; carries sku+loc)
    //   - poc_cost_depletions (FIFO consumption rows; JOIN via layer_id
    //     to poc_cost_layers to derive sku+loc)
    //
    // Subquery columns must be aliased (`u.*`) — UNION ALL branches
    // expose names via the *outer* subquery, not directly. Without
    // `u.sku_id` etc. PG raises "column does not exist" at parse time.
    let mut per_shard_max: Vec<i64> = vec![0; POC_SHARD_COUNT];
    let mut scanned_pool_rows: u64 = 0;

    Spi::connect(|client| {
        let table = client.select(
            "SELECT u.sku_id, u.location_id, MAX(u.committer_tx_id)::BIGINT AS maxtx \
               FROM (\
                 SELECT sku_id, location_id, committer_tx_id FROM poc_cost_consumptions \
                 UNION ALL \
                 SELECT sku_id, location_id, committer_tx_id FROM poc_cost_layers \
                 UNION ALL \
                 SELECT l.sku_id, l.location_id, d.committer_tx_id \
                   FROM poc_cost_depletions d \
                   JOIN poc_cost_layers l ON d.layer_id = l.layer_id\
               ) u \
              GROUP BY u.sku_id, u.location_id",
            None,
            &[],
        );
        if let Ok(tab) = table {
            for row in tab {
                let sku: Option<i64> = row["sku_id"].value().ok().flatten();
                let loc: Option<i64> = row["location_id"].value().ok().flatten();
                let maxtx: Option<i64> = row["maxtx"].value().ok().flatten();
                if let (Some(sku), Some(loc), Some(maxtx)) = (sku, loc, maxtx) {
                    let ph = pool_hash(sku, loc);
                    let s = shard_for(ph);
                    if maxtx > per_shard_max[s] {
                        per_shard_max[s] = maxtx;
                    }
                    scanned_pool_rows += 1;
                }
            }
        }
    });

    let mut shards_seeded = 0usize;
    let mut max_overall: i64 = 0;
    for (idx, &mx) in per_shard_max.iter().enumerate() {
        if mx > 0 {
            queue::seed_committer_tx_at_least(idx, mx);
            shards_seeded += 1;
            if mx > max_overall {
                max_overall = mx;
            }
        }
    }

    pgrx::log!(
        "poc_ledger_startup_recovery: phase A — scanned {} pool rows, \
         seeded {}/{} shards, max committer_tx_id observed = {}",
        scanned_pool_rows,
        shards_seeded,
        POC_SHARD_COUNT,
        max_overall
    );

    // Phase B — xid abort scan (M6.1 will replace the log! with an
    // actual compensation enqueue).
    let mut total_xids: u64 = 0;
    let mut aborted_xids: u64 = 0;

    Spi::connect(|client| {
        let table = client.select(
            "SELECT DISTINCT u.user_tx_xid::TEXT AS xid_text FROM (\
               SELECT user_tx_xid FROM poc_cost_consumptions \
               UNION ALL \
               SELECT user_tx_xid FROM poc_cost_layers \
               UNION ALL \
               SELECT user_tx_xid FROM poc_cost_depletions\
             ) u",
            None,
            &[],
        );
        if let Ok(tab) = table {
            for row in tab {
                let xid_text: Option<String> =
                    row["xid_text"].value().ok().flatten();
                if let Some(xid_text) = xid_text {
                    total_xids += 1;
                    // xid8 textual form is a u64 decimal; pg_xact
                    // lookups truncate to xid (u32). xid=0/1/2 are
                    // PG-reserved (InvalidTransactionId / Bootstrap /
                    // Frozen) and never abort-pending.
                    if let Ok(full) = xid_text.parse::<u64>() {
                        let xid32 = full as u32;
                        if xid32 < 3 {
                            continue;
                        }
                        let aborted = unsafe {
                            pgrx::pg_sys::TransactionIdDidAbort(xid32.into())
                        };
                        if aborted {
                            aborted_xids += 1;
                            pgrx::log!(
                                "poc_ledger_startup_recovery: aborted \
                                 user_tx_xid {} (xid32={}); compensation \
                                 emission deferred to M6.1 (acct-4d4n.16)",
                                xid_text,
                                xid32
                            );
                        }
                    }
                }
            }
        }
    });

    pgrx::log!(
        "poc_ledger_startup_recovery: phase B — scanned {} distinct \
         user_tx_xids, {} aborted (logged only; emission deferred to M6.1)",
        total_xids,
        aborted_xids
    );
}
