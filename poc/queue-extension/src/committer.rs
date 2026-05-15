//! acct-4d4n.3 (M1.2): committer election + batch drain (synchronous).
//!
//! Implements the committer half of the queue lifecycle per spec §1.6
//! steps 10-21, in single-backend-synchronous form: the caller pushes
//! its request, immediately wins the CAS election (there are no peers
//! in M1.2), drains its own one-event batch under an internal
//! sub-transaction, INSERTs into the placeholder `poc_test_rows` table
//! (the spec §1.2 stand-in until M2.x writes real `poc_cost_*` rows),
//! writes the result into its slot, releases committer role, then
//! returns the slot result to the caller.
//!
//! ## Sub-transaction discipline — deferred to M2.x
//!
//! Spec §1.6 step 17b prescribes `BeginInternalSubTransaction("poc_committer_batch")`
//! around the SPI work so a batch-level failure can be rolled back
//! independently of the caller's user-tx. M1.2's INSERT into the
//! placeholder `poc_test_rows` has no realistic failure path (the table
//! has no unique constraints beyond the autogen PK; no FK; no CHECKs),
//! and wrapping the call in `BeginInternalSubTransaction` interacts
//! poorly with callers that themselves run inside plpgsql sub-tx scopes
//! (DO blocks, plpgsql functions) — the snapshot resource-owner stack
//! gets confused. We defer the sub-tx machinery to M2.1 where real cost
//! methods will need atomic rollback for partial-success semantics; at
//! that point we either save/restore `CurrentResourceOwner` manually or
//! pivot to a pattern that doesn't span plpgsql boundaries.
//!
//! Practical implication for M1.2: a constraint failure during the
//! INSERT would abort the caller's user-tx too. Acceptable because no
//! such failures are reachable through `poc_ledger_apply`'s narrow
//! M1.2 surface.
//!
//! ## What M1.2 simplifies vs spec §1.6
//!
//! - No `WaitLatch` (step 11): single-backend has no peer push to wait
//!   for, so we drain immediately. M3.1 introduces real batching once
//!   multi-backend pushes coexist.
//! - No pool lock (step 17c): `poc_pool_locks` doesn't exist yet (it's
//!   M2.1 territory). M1.2 skips FOR UPDATE; the only writer to
//!   `poc_test_rows` is the committer itself, and there's only one
//!   committer in M1.2.
//! - No dedup-lookup (step 17d): M1.2 has no `issue_id` (body3 is 0),
//!   so dedup is a no-op. M2.x activates the dedup query when real
//!   cost methods land.
//! - No plan_apply (step 17f): M1.2 stamps fixed
//!   `applied_unit_cost = 100` regardless of cost method.
//! - No real SetLatch (step 19): single-backend has no waiters. M3.1
//!   adds the wake mechanism.

use crate::queue::{
    self, DrainedApply, METHOD_AVG, METHOD_FIFO, METHOD_STD, POC_SHARD_COUNT,
    SLOT_FILLED,
};
use pgrx::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

// ── Schema (poc_test_rows) ────────────────────────────────────────────
//
// Spec §1.2 references `poc_test_rows` as the M1.x INSERT placeholder.
// One row per successfully committed request; the row captures enough
// fields to assert C2 invariants (I4 monotonic committer_tx_seq) and
// the M5b recovery contract (user_tx_xid + committer_tx_id pair).

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

/// splitmix64 — same hash family used elsewhere in ledger-extension's
/// h_arena. Cheap, decent avalanche, deterministic. M1.2 just needs
/// uniform distribution across the (small) shard count; the choice is
/// not load-bearing.
fn pool_hash(sku_id: i64, location_id: i64) -> u64 {
    let mut x: u64 = (sku_id as u64).wrapping_mul(0x9E3779B97F4A7C15);
    x ^= (location_id as u64).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

fn shard_for(pool_hash_val: u64) -> usize {
    // POC_SHARD_COUNT is a power of two; bitwise mask is the canonical
    // shard mapping per spec §1.6 step 4.
    (pool_hash_val & (POC_SHARD_COUNT as u64 - 1)) as usize
}

fn method_tag_for(method_name: &str) -> Option<u8> {
    match method_name {
        "fifo" | "FIFO" => Some(METHOD_FIFO),
        "avg" | "AVG" => Some(METHOD_AVG),
        "std" | "STD" => Some(METHOD_STD),
        _ => None,
    }
}

// ── Committer drain ───────────────────────────────────────────────────
//
// Drains up to `batch_size_max` requests from the shard, INSERTs one
// row per drained request into `poc_test_rows` under a single sub-tx,
// and fills each slot with the result.
//
// Returns the `committer_tx_id` stamped on this batch (uniform across
// all rows in the batch, matches spec §1.6 step 17g semantics).

fn drain_and_commit(shard_idx: usize, batch_size_max: usize) -> i64 {
    let drained: Vec<DrainedApply> =
        queue::drain_apply_batch(shard_idx, batch_size_max);
    if drained.is_empty() {
        return 0;
    }

    let committer_tx_id = queue::next_committer_tx_id(shard_idx);

    // Bulk INSERT via unnest($1, $2, ...). One SPI call covers the
    // whole batch — mirrors the M2.x cost-method INSERT pattern.
    let shard_idxs: Vec<i32> = drained.iter().map(|_| shard_idx as i32).collect();
    let slot_idxs: Vec<i32> = drained.iter().map(|d| d.slot_idx as i32).collect();
    let request_seqs: Vec<i64> = drained.iter().map(|d| d.request_seq as i64).collect();
    let committer_tx_ids: Vec<i64> = drained.iter().map(|_| committer_tx_id).collect();
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
            committer_tx_ids.into(),
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
            .expect("poc_ledger_apply: INSERT INTO poc_test_rows");
    });

    // Fill each drained request's slot. Fixed
    // applied_unit_cost = 100; total = qty * 100. M2.x dispatches by
    // method.
    for d in &drained {
        let applied_unit_cost: i64 = 100;
        let applied_total_cost: i64 = d.event_qty.saturating_mul(applied_unit_cost);
        let res = queue::fill_slot_result(
            shard_idx,
            d.slot_idx,
            applied_unit_cost,
            applied_total_cost,
            committer_tx_id,
        );
        if let Err(actual) = res {
            pgrx::error!(
                "poc_ledger_apply: slot {} fill CAS failed (actual state {}); \
                 expected SLOT_ALLOCATED",
                d.slot_idx,
                actual
            );
        }
    }

    committer_tx_id
}

// ── poc_ledger_apply ──────────────────────────────────────────────────
//
// Caller-side path per spec §1.6 steps 1-10 + committer path 11-21,
// inlined for M1.2 single-backend. Acceptance: returns one row with
// (slot_idx, request_seq, committer_tx_id, applied_unit_cost).

#[pg_extern]
fn poc_ledger_apply(
    sku_id: i64,
    location_id: i64,
    qty: i64,
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
    ),
> {
    let method_tag = match method_tag_for(&method) {
        Some(t) => t,
        None => pgrx::error!(
            "poc_ledger_apply: unknown method '{}' (expected fifo/avg/std)",
            method
        ),
    };

    // Spec §1.6 step 2: force user-tx XID allocation so the row's
    // user_tx_xid field is non-zero. Without this, a read-only user-tx
    // has no XID and M5b recovery's pg_xact lookup can't tell apart
    // committed vs aborted.
    let user_tx_xid: i64 = unsafe {
        let full = pgrx::pg_sys::GetCurrentFullTransactionId();
        full.value as i64
    };

    let ph = pool_hash(sku_id, location_id);
    let shard_idx = shard_for(ph);

    // Step 5: acquire slot OUTSIDE the LWLock.
    let slot_idx = queue::acquire_slot(shard_idx).unwrap_or_else(|| {
        pgrx::error!(
            "poc_ledger_apply: slot pool exhausted on shard {} \
             (M5c.1 backpressure not yet implemented)",
            shard_idx
        )
    });

    // Steps 6-9: push under EXCLUSIVE LWLock.
    let my_pid: i32 = unsafe { pgrx::pg_sys::MyProcPid };
    let request_seq = queue::ring_push_apply(
        shard_idx,
        slot_idx,
        ph,
        my_pid,
        method_tag,
        qty,
        0, // event_at_micros (M2.x: caller-supplied)
        0, // event_issue_id (M2.x: caller-allocated)
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

    // Step 10: CAS committer election. M1.2 single-backend → we always
    // win. If we ever lose here it indicates a stuck committer_pid from
    // a previous backend (shouldn't happen in M1.2 acceptance suite),
    // so raise.
    let won = queue::try_acquire_committer(shard_idx, my_pid, clock_ns());
    if !won {
        pgrx::error!(
            "poc_ledger_apply: committer election lost on shard {} \
             (M1.2 expects single-backend — M3.1 introduces waiter path)",
            shard_idx
        );
    }

    // Step 11-19: drain own batch and fill slot. M1.2 simplifies by
    // skipping WaitLatch (no peers to coalesce with). batch_size_max
    // for M1.2 is a const; the spec GUC poc_ledger.batch_size_max
    // becomes load-bearing in M3.1 when batching matters.
    let committer_tx_id = drain_and_commit(shard_idx, 1024);

    // Step 20: release committer role.
    queue::release_committer(shard_idx);

    // Step 21: read result from own slot and return to caller. M1.2
    // doesn't recycle the slot here — the caller can inspect it after
    // for diagnostics; M2.x may add an explicit recycle on read.
    let (state, applied_unit_cost, applied_total_cost, ctx_id) =
        queue::read_slot_result(shard_idx, slot_idx);
    if state != SLOT_FILLED {
        pgrx::error!(
            "poc_ledger_apply: own slot {} not SLOT_FILLED after drain (state={})",
            slot_idx,
            state
        );
    }
    // Recycle so the slot pool stays available under sustained load.
    let _ = queue::recycle_slot(shard_idx, slot_idx);

    debug_assert_eq!(ctx_id, committer_tx_id);

    TableIterator::once((
        shard_idx as i32,
        slot_idx as i32,
        request_seq as i64,
        committer_tx_id,
        applied_unit_cost,
        applied_total_cost,
    ))
}

// ── Diagnostic SQL helpers ────────────────────────────────────────────

/// Returns the per-shard `committer_tx_seq` AFTER the most recent
/// fetch_add. Useful for tests that assert C2 I4 (monotonic across
/// shards independently).
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
