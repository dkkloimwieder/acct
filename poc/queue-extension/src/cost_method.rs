//! acct-4d4n.4 (M2.1): PocCostMethod trait surface + types + MockMethod.
//!
//! Implements the minimal trait surface from spec §1.3, the row-type
//! payloads emitted by `plan_apply`, the `PocError` enum surfaced as
//! per-event failures, and a `MockMethod` stub that drives the M2.1
//! per-event partial-success and replay tests. Real FIFO / AVG / STD
//! land in M2.2 / M2.3 and slot in via the same trait.
//!
//! ## Trait shape (per spec §1.3)
//!
//! `plan_apply` MUST be:
//! - Deterministic and pure (no SPI, no shmem mutation). The snapshot
//!   is the entire input world; the result is the entire output.
//! - Per-event partial-success aware: emit `PocEventResult.error =
//!   Some(_)` for an event without poisoning the rest of the batch.
//! - Homogeneous per call: one `(pool, method)` per call. The
//!   committer groups by `(pool, method_tag)` before dispatch.
//!
//! Determinism + purity are the architectural lever: the same batch
//! against the same snapshot always produces the same rows, so dedup
//! by `(issue_id, method)` can replay an old result without re-running
//! `plan_apply`. M5b recovery and M6 compensation both rely on this.
//!
//! ## What M2.1 deliberately does NOT do
//!
//! - No real FIFO / AVG / STD methods. `MockMethod` is the only impl;
//!   M2.2 and M2.3 add the real ones.
//! - No snapshot SPI helpers (those are M2.3's `build_snapshot` work).
//!   `MockMethod` ignores the snapshot.
//! - No multi-method dispatch by SKU (poc_sku_method_assignments
//!   table); the caller passes `method` directly. M3.1 caches per-SKU
//!   method assignments.

// M2.1 reserves several spec-defined fields and trait methods for the
// M2.2 / M2.3 / M5b consumers (PocLayerRow source_kind etc., PocApplyResult
// layer_inserts, PocCostMethod::method_id). Allow dead_code to keep them
// visible until those milestones land.
#![allow(dead_code)]

use pgrx::prelude::*;

// ── Types per spec §1.3 ───────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PocPoolKey {
    pub sku_id: i64,
    pub location_id: i64,
}

#[derive(Debug, Clone)]
pub struct PocApplyEvent {
    /// In-batch ordering assigned by the committer at drain time.
    /// Identifies which `PocEventResult` belongs to which input event.
    pub event_seq: u64,
    /// Signed; positive = consumption (the M2.1 path). Receipts are a
    /// separate flow (M2.2 adds layer-creating receipts).
    pub qty: i64,
    pub at_micros: i64,
    /// Caller-supplied globally-unique handle. Drives dedup-lookup.
    pub issue_id: i64,
    /// Carried for compensation recovery (M5b / M6).
    pub user_tx_xid: i64,
    /// Original slot index — committer uses this to fill the slot at
    /// step 17g. Not part of the spec's PocApplyEvent (the spec's
    /// PocApplyEvent is method-facing); we stash it here as it
    /// flows through the same Vec anyway.
    pub slot_idx: u32,
}

#[derive(Debug, Clone)]
pub struct PocApplyBatch {
    pub pool: PocPoolKey,
    pub events: Vec<PocApplyEvent>,
}

/// FIFO layer view for the M2.2 snapshot. M2.1's MockMethod ignores
/// snapshot contents.
#[derive(Debug, Clone)]
pub struct PocLayerView {
    pub layer_id: i64,
    pub unit_cost: i64,
    pub effective_qty: i64,
    pub born_at_micros: i64,
    pub born_seq: i64,
}

#[derive(Debug, Clone, Default)]
pub struct PocSnapshot {
    pub pool: PocPoolKey,
    pub layers: Vec<PocLayerView>,
    pub avg_unit_cost: Option<i64>,
    pub standard_cost: Option<i64>,
    pub total_available_qty: i64,
}

/// Per-event outcome from `plan_apply`. Either applied with a unit
/// cost, or errored with a `PocError` variant. NEVER both populated:
/// a method that returns `error = Some(_)` must leave the cost fields
/// at 0 (the committer treats them as undefined on error paths).
#[derive(Debug, Clone)]
pub struct PocEventResult {
    pub event_seq: u64,
    pub applied_unit_cost: i64,
    pub applied_total_cost: i64,
    pub error: Option<PocError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PocError {
    /// Method requires positive inventory and `total_available_qty` is
    /// less than the requested consumption. Surfaced as error_code = 1.
    InsufficientInventory,
    /// Caller passed a method name the registry doesn't recognise.
    /// Surfaced as error_code = 2.
    MethodNotFound,
    /// Wrapper-level signal that the committer's sub-tx INSERT failed
    /// at the batch level. M2.1 doesn't emit this (no sub-tx yet —
    /// see committer.rs docstring); M2.x extends. Surfaced as error_code = 3.
    BatchFailed,
}

impl PocError {
    pub fn as_code(&self) -> u16 {
        match self {
            PocError::InsufficientInventory => 1,
            PocError::MethodNotFound => 2,
            PocError::BatchFailed => 3,
        }
    }
}

/// FIFO output row. One per layer touched by a consumption event.
#[derive(Debug, Clone)]
pub struct PocDepletionRow {
    pub layer_id: i64,
    pub qty: i64,
    pub unit_cost: i64,
    pub issue_id: i64,
    pub event_seq: u64,
    pub user_tx_xid: i64,
}

/// AVG / STD / Mock output row. One per consumption event.
#[derive(Debug, Clone)]
pub struct PocConsumptionRow {
    pub sku_id: i64,
    pub location_id: i64,
    pub qty: i64,
    pub applied_unit_cost: i64,
    pub issue_id: i64,
    pub event_seq: u64,
    pub user_tx_xid: i64,
}

/// Receipt-side layer emission. M2.1 doesn't exercise; M2.2 does.
#[derive(Debug, Clone)]
pub struct PocLayerRow {
    pub sku_id: i64,
    pub location_id: i64,
    pub qty: i64,
    pub unit_cost: i64,
    pub source_kind: String,
    pub source_ref: Option<i64>,
    pub user_tx_xid: i64,
}

#[derive(Debug, Clone, Default)]
pub struct PocApplyResult {
    pub per_event: Vec<PocEventResult>,
    pub depletion_inserts: Vec<PocDepletionRow>,
    pub consumption_inserts: Vec<PocConsumptionRow>,
    pub layer_inserts: Vec<PocLayerRow>,
}

// ── Trait surface ─────────────────────────────────────────────────────

pub trait PocCostMethod: Send + Sync + 'static {
    fn method_id(&self) -> &'static str;
    fn plan_apply(
        &self,
        batch: &PocApplyBatch,
        snapshot: &PocSnapshot,
    ) -> PocApplyResult;
}

// ── MockMethod ────────────────────────────────────────────────────────
//
// Drives M2.1 acceptance. Behavior contract:
//
// - If `event.qty > 1_000_000_000`: returns
//   `error = Some(InsufficientInventory)`. Tests use sentinel
//   qty=2_000_000_000 to trigger this path.
// - Otherwise: emits one `PocConsumptionRow` with
//   `applied_unit_cost = 100 + (issue_id & 0xFF)` (so different events
//   in the same batch get distinguishable costs, which makes the
//   replay test more rigorous — a replayed event's cached cost must
//   match the original).
//
// Wired in `resolve_method` for method names "mock", "fifo", "avg",
// and "std" — M2.2 / M2.3 will swap the real methods in for "fifo" /
// "avg" / "std" and keep "mock" as the M2.1 fixture.

pub struct MockMethod;

const MOCK_ERROR_QTY_THRESHOLD: i64 = 1_000_000_000;

impl PocCostMethod for MockMethod {
    fn method_id(&self) -> &'static str {
        "mock"
    }

    fn plan_apply(
        &self,
        batch: &PocApplyBatch,
        _snapshot: &PocSnapshot,
    ) -> PocApplyResult {
        let mut per_event: Vec<PocEventResult> = Vec::with_capacity(batch.events.len());
        let mut consumption_inserts: Vec<PocConsumptionRow> = Vec::new();
        for ev in &batch.events {
            if ev.qty > MOCK_ERROR_QTY_THRESHOLD {
                per_event.push(PocEventResult {
                    event_seq: ev.event_seq,
                    applied_unit_cost: 0,
                    applied_total_cost: 0,
                    error: Some(PocError::InsufficientInventory),
                });
                continue;
            }
            let unit = 100 + (ev.issue_id & 0xFF);
            let total = ev.qty.saturating_mul(unit);
            per_event.push(PocEventResult {
                event_seq: ev.event_seq,
                applied_unit_cost: unit,
                applied_total_cost: total,
                error: None,
            });
            consumption_inserts.push(PocConsumptionRow {
                sku_id: batch.pool.sku_id,
                location_id: batch.pool.location_id,
                qty: ev.qty,
                applied_unit_cost: unit,
                issue_id: ev.issue_id,
                event_seq: ev.event_seq,
                user_tx_xid: ev.user_tx_xid,
            });
        }
        PocApplyResult {
            per_event,
            depletion_inserts: vec![],
            consumption_inserts,
            layer_inserts: vec![],
        }
    }
}

// ── Method registry ───────────────────────────────────────────────────
//
// M2.1 dispatches all four labels (mock / fifo / avg / std) to
// `MockMethod` — sufficient for the M2.1 acceptance tests. M2.2
// replaces the "fifo" arm with the real FifoMethod; M2.3 swaps in
// AvgMethod and StdMethod for "avg" / "std". The shape stays static
// (no global state, no HashMap — match arms over a tag) because the
// method set is small and known.

/// Resolve a method name to the singleton implementation. `static`
/// references are sound — the methods are zero-sized and have no
/// per-instance state. Returns None if the name is unknown.
pub fn resolve_method(method: &str) -> Option<&'static dyn PocCostMethod> {
    static MOCK: MockMethod = MockMethod;
    match method {
        "fifo" => Some(&crate::fifo::FIFO),
        "avg" => Some(&crate::avg::AVG),
        "std" => Some(&crate::std_cost::STD),
        "mock" => Some(&MOCK),
        _ => None,
    }
}

/// Convert the method_tag u8 carried on `PocPendingRequest.body0` to
/// the method name string. M2.1 dispatches all tags to MockMethod via
/// `resolve_method`; the tag → name mapping stays stable so future
/// methods can register their tag here.
pub fn method_name_for_tag(method_tag: u8) -> &'static str {
    use crate::queue::{METHOD_AVG, METHOD_FIFO, METHOD_MOCK, METHOD_STD};
    match method_tag {
        METHOD_FIFO => "fifo",
        METHOD_AVG => "avg",
        METHOD_STD => "std",
        METHOD_MOCK => "mock",
        _ => "mock",
    }
}

// ── Schema (poc_cost_layers / poc_cost_depletions / poc_cost_consumptions) ──
//
// Spec §1.2 schema, minus poc_pool_locks (M3.2) and
// poc_cost_compensations (M6.1). M2.1 lands the consumption / depletion
// tables so the dedup-lookup query (§1.6 step 17d) has the UNION ALL
// target it expects; MockMethod writes to consumptions only.
//
// poc_cost_layers ships now too so `poc_cost_depletions.layer_id`'s
// foreign key constraint resolves at extension load. M2.2 INSERTs the
// first real receipts; until then the table is empty.
//
// `requires = [poc_test_rows_schema]` ordering ensures these CREATEs
// run after the M1.2 table (no functional dependency — just keeps the
// schema file stably ordered).

pgrx::extension_sql!(
    r#"
    CREATE SEQUENCE poc_cost_layers_born_seq;

    CREATE TABLE poc_cost_layers (
        layer_id        BIGSERIAL PRIMARY KEY,
        sku_id          BIGINT NOT NULL,
        location_id     BIGINT NOT NULL,
        qty             BIGINT NOT NULL CHECK (qty > 0),
        unit_cost       BIGINT NOT NULL,
        born_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        born_seq        BIGINT NOT NULL DEFAULT nextval('poc_cost_layers_born_seq'),
        source_kind     TEXT NOT NULL,
        source_ref      BIGINT,
        committer_tx_id BIGINT NOT NULL DEFAULT 0,
        user_tx_xid     xid8 NOT NULL DEFAULT '0'::xid8
    );
    CREATE INDEX poc_cost_layers_pool
        ON poc_cost_layers (sku_id, location_id, born_at, born_seq);
    CREATE INDEX poc_cost_layers_user_tx
        ON poc_cost_layers (user_tx_xid);

    CREATE TABLE poc_cost_depletions (
        depletion_id    BIGSERIAL PRIMARY KEY,
        layer_id        BIGINT NOT NULL REFERENCES poc_cost_layers(layer_id),
        qty             BIGINT NOT NULL CHECK (qty > 0),
        unit_cost       BIGINT NOT NULL,
        consumed_at     TIMESTAMPTZ NOT NULL,
        consumed_seq    BIGINT NOT NULL,
        issue_id        BIGINT NOT NULL,
        method_used     TEXT NOT NULL,
        committer_tx_id BIGINT NOT NULL,
        user_tx_xid     xid8 NOT NULL,
        UNIQUE (issue_id, method_used, layer_id)
    );
    CREATE INDEX poc_cost_depletions_layer
        ON poc_cost_depletions (layer_id);
    CREATE INDEX poc_cost_depletions_issue
        ON poc_cost_depletions (issue_id);
    CREATE INDEX poc_cost_depletions_user_tx
        ON poc_cost_depletions (user_tx_xid);

    CREATE TABLE poc_cost_consumptions (
        consumption_id    BIGSERIAL PRIMARY KEY,
        sku_id            BIGINT NOT NULL,
        location_id       BIGINT NOT NULL,
        qty               BIGINT NOT NULL CHECK (qty > 0),
        applied_unit_cost BIGINT NOT NULL,
        consumed_at       TIMESTAMPTZ NOT NULL,
        consumed_seq      BIGINT NOT NULL,
        issue_id          BIGINT NOT NULL,
        method_used       TEXT NOT NULL,
        committer_tx_id   BIGINT NOT NULL,
        user_tx_xid       xid8 NOT NULL,
        UNIQUE (issue_id, method_used)
    );
    CREATE INDEX poc_cost_consumptions_pool
        ON poc_cost_consumptions (sku_id, location_id, consumed_at);
    CREATE INDEX poc_cost_consumptions_issue
        ON poc_cost_consumptions (issue_id);
    CREATE INDEX poc_cost_consumptions_user_tx
        ON poc_cost_consumptions (user_tx_xid);

    -- M2.3 AVG running aggregate per pool. Receipts UPSERT increment;
    -- the committer decrements after consumption INSERT (post-plan_apply
    -- hook in process_group). Snapshot helper reads running_value /
    -- running_qty under FOR UPDATE.
    CREATE TABLE poc_cost_avg (
        sku_id        BIGINT NOT NULL,
        location_id   BIGINT NOT NULL,
        running_qty   BIGINT NOT NULL DEFAULT 0 CHECK (running_qty >= 0),
        running_value BIGINT NOT NULL DEFAULT 0 CHECK (running_value >= 0),
        last_updated  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (sku_id, location_id)
    );

    -- M2.3 STD lookup. One row per SKU; PoC keeps it flat (no time-
    -- phased revisions). Production would version by effective_at.
    CREATE TABLE poc_standard_costs (
        sku_id       BIGINT PRIMARY KEY,
        unit_cost    BIGINT NOT NULL CHECK (unit_cost > 0),
        effective_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        posted_by    TEXT
    );

    -- M3.2 (acct-4d4n.8) Q-A pool-lock candidates. Both tables share the
    -- same (sku_id, location_id) primary key; the committer acquires a
    -- row-lock here at the top of process_group when there is work to
    -- plan. The two flavours differ only in payload — `poc_pool_locks`
    -- carries an audit `lock_version` column; `poc_pool_lock_anchors`
    -- is minimal. The GUC `poc_ledger.pool_lock_mode` selects which
    -- table (or 'none' for the no-extra-SPI baseline that relies on
    -- the cost-table FOR UPDATE inside the snapshot builders).
    CREATE TABLE poc_pool_locks (
        sku_id       BIGINT NOT NULL,
        location_id  BIGINT NOT NULL,
        lock_version BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (sku_id, location_id)
    );

    CREATE TABLE poc_pool_lock_anchors (
        sku_id       BIGINT NOT NULL,
        location_id  BIGINT NOT NULL,
        PRIMARY KEY (sku_id, location_id)
    );
    "#,
    name = "poc_cost_schema",
    requires = ["poc_test_rows_schema"],
);

// ── Per-event error codes (SQL surface) ───────────────────────────────
//
// `poc_ledger_error_code_to_text` lets tests assert error semantics
// without baking literal integers into SQL. Returns NULL for code = 0
// (no error).

#[pg_extern]
fn poc_ledger_error_code_to_text(code: i32) -> Option<&'static str> {
    match code {
        0 => None,
        1 => Some("InsufficientInventory"),
        2 => Some("MethodNotFound"),
        3 => Some("BatchFailed"),
        _ => Some("Unknown"),
    }
}
