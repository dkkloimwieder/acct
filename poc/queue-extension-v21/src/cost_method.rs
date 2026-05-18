//! PocV21CostMethod trait + event/snapshot types per spec §1.3.
//!
//! M1.3 (acct-wrwf). FIFO is the first concrete impl (in `fifo.rs`).
//! M2.1 (acct-hmuf) adds AVG (in `avg.rs`); STD at M2.2.
//!
//! The trait is pure: `apply_one` reads snapshot + one event, mutates
//! snapshot in-memory, pushes row vectors onto an accumulator.
//! Zero SPI, zero shmem mutation. The committer pipeline (Step 5)
//! writes the result vectors to disk via bulk UNNEST INSERT.
//!
//! `plan_apply` is a convenience wrapper that loops; the per-event
//! shape lets the committer dispatch per-SKU method without grouping.

use pgrx::Uuid;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PocV21EventType {
    InvAdjust,
    InvIssue,
    PoReceipt,
    SoShipment,
    /// M6.1 (acct-o1yv). Multi-pool atomic: K components + 1 WIP + 1
    /// output, expanded into K+1 PocV21Events at staging read. Negative
    /// qty = component consumption (per its own method); positive qty =
    /// output emission at unit_cost = total_component_cost / output_qty.
    /// `wo_id` / `op_id` populated on both component and output events
    /// so each method's posting_line uses the WIP-pool stub account.
    WoComplete,
}

#[derive(Debug, Clone)]
pub struct PocV21Event {
    pub correlation_id: Uuid,
    pub issue_id: i64,
    pub event_type: PocV21EventType,
    pub sku_id: i64,
    pub location_id: i64,
    pub qty: i64, // signed
    pub unit_cost: i64, // for receipt events; 0 for consumption events
    pub business_date_jdate: i32, // PG-internal date format (days since 2000-01-01)
    pub doc_chrono: i64,
    pub document_id: i64,
    pub sub_priority: i32,
    pub user_tx_xid: u64,
    pub at_micros: i64,
    /// M6.1: WO id / operation id for WoComplete events; 0 otherwise.
    /// Used by method posting_line emission to route to a WIP-pool stub
    /// account on the inventory-offset side.
    pub wo_id: i64,
    pub op_id: i64,
}

#[derive(Debug, Clone)]
pub struct LayerView {
    /// DB layer_id for layers hydrated from `poc_v21_cost_layers`. For
    /// layers created earlier in the same SuperBatch (FIFO emit_layer)
    /// this is 0 — the real BIGSERIAL id is only known after Step 5b's
    /// `RETURNING`. Such layers carry their position in
    /// `result.layer_inserts` via `layer_insert_index` so Step 5c can
    /// translate the depletion's reference to the real id (acct-shpc.9).
    pub layer_id: i64,
    /// `Some(idx)` for in-SB-emitted layers (idx into
    /// `PocV21ApplyResult.layer_inserts`); `None` for hydrated layers.
    /// Pre-acct-0frn this was unnecessary because the disjoint router
    /// rule guaranteed Receipt + Consume of the same SKU never landed
    /// in one SuperBatch. The grouped rule (acct-0frn) packs them
    /// together and exposes the in-SB depletion path.
    pub layer_insert_index: Option<usize>,
    pub unit_cost: i64,
    pub effective_qty: i64, // remaining qty after in-batch depletions
    pub born_at_micros: i64,
    pub born_seq: i64,
    pub correlation_id: Uuid,
}

#[derive(Debug, Default, Clone)]
pub struct SkuPoolState {
    /// FIFO order: sort by (born_at_micros, born_seq). Layers with
    /// effective_qty == 0 are kept (FIFO walks past them) until the
    /// committer Step 5 omits them from cost_layers INSERTs. (Re-
    /// hydration after depletion in a future tick re-fetches via
    /// effective_qty > 0 filter, so zero-qty layers are transient.)
    pub layers: Vec<LayerView>,
    /// Highest born_seq observed for this pool. Used by FIFO to assign
    /// new layer born_seq = max(existing) + 1.
    pub max_born_seq: i64,
    /// AVG method running state. `avg_unit_cost` is the weighted-average
    /// unit cost; `avg_total_qty` is the pool's signed quantity. Both
    /// hydrated from `poc_v21_avg_pool_state` at Step 3 for AVG-method
    /// pools; mutated by `AvgMethod::apply_one`; UPSERTed back at
    /// Step 5 for any pool with `avg_dirty=true`. `avg_dirty` is the
    /// "this batch touched the AVG state" flag — covers both receipts
    /// (which shift avg_unit_cost) and consumptions (which only
    /// decrement avg_total_qty).
    pub avg_unit_cost: i64,
    pub avg_total_qty: i64,
    pub avg_dirty: bool,
}

#[derive(Debug, Default)]
pub struct PocV21Snapshot {
    pub sku_pools: HashMap<(i64, i64), SkuPoolState>,
    /// per-SKU method assignment from poc_v21_sku_method_assignments.
    /// SKUs absent from this map default to "fifo" at M1.3 (test
    /// convenience; later milestones MUST be explicit).
    pub method_assignments: HashMap<i64, &'static str>,
    /// max consumed_seq per layer_id (seeded from
    /// poc_v21_cost_depletions).
    pub max_consumed_seq_per_layer: HashMap<i64, i64>,
    /// Per-in-SB-layer consumed_seq counter. Keyed by
    /// `layer_insert_index` (position in
    /// `PocV21ApplyResult.layer_inserts`) because in-SB layers all
    /// carry `layer_id == 0` until Step 5b RETURNING, so a single
    /// `HashMap<i64, _>` would collapse them. Combined with
    /// `max_consumed_seq_per_layer` the dispatcher routes new vs.
    /// hydrated layers to disjoint counters (acct-shpc.9).
    pub max_consumed_seq_per_new_layer: HashMap<usize, i64>,
    /// Latest effective standard cost per (sku, location), hydrated
    /// by the committer at Step 3 via DISTINCT ON … ORDER BY
    /// effective_from DESC. STD events fail with
    /// `standard_cost_missing` if the (sku, location) is absent.
    pub standard_costs: HashMap<(i64, i64), i64>,
}

#[derive(Debug, Default)]
pub struct PocV21ApplyResult {
    pub per_event: Vec<PocV21EventResult>,
    pub layer_inserts: Vec<PocV21LayerRow>,
    pub depletion_inserts: Vec<PocV21DepletionRow>,
    pub consumption_inserts: Vec<PocV21ConsumptionRow>,
    pub posting_line_inserts: Vec<PocV21PostingLineRow>,
    pub posting_line_inventory_inserts: Vec<PocV21PostingLineInventoryRow>,
}

#[derive(Debug, Clone)]
pub struct PocV21EventResult {
    pub correlation_id: Uuid,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PocV21LayerRow {
    pub sku_id: i64,
    pub location_id: i64,
    pub qty: i64,
    pub unit_cost: i64,
    pub born_at_micros: i64,
    pub born_seq: i64,
    pub source_kind: &'static str,
    pub source_ref: Option<i64>,
    pub correlation_id: Uuid,
    pub user_tx_xid: u64,
}

#[derive(Debug, Clone)]
pub struct PocV21ConsumptionRow {
    /// AVG-style consumption: no layer_id; emits at running pool avg.
    pub sku_id: i64,
    pub location_id: i64,
    pub qty: i64,
    pub unit_cost: i64,
    pub consumed_at_micros: i64,
    pub consumed_seq: i64,
    pub issue_id: i64,
    pub method_used: &'static str,
    pub correlation_id: Uuid,
    pub user_tx_xid: u64,
}

#[derive(Debug, Clone)]
pub struct PocV21DepletionRow {
    /// Layer to deplete (DB BIGSERIAL). For depletions against
    /// pre-existing (hydrated) layers this is the real id and
    /// `layer_insert_index` is `None`. For depletions against layers
    /// emitted earlier in this same SuperBatch, `layer_id == 0` and
    /// `layer_insert_index` carries the position in
    /// `PocV21ApplyResult.layer_inserts` — Step 5c resolves the real
    /// BIGSERIAL after Step 5b's `RETURNING` (acct-shpc.9).
    pub layer_id: i64,
    /// `Some(idx)` when the depleted layer is in-SB-emitted; `None`
    /// for hydrated layers. Mirrors `LayerView.layer_insert_index`.
    pub layer_insert_index: Option<usize>,
    pub qty: i64,
    pub unit_cost: i64,
    pub consumed_at_micros: i64,
    pub consumed_seq: i64,
    pub issue_id: i64,
    pub method_used: &'static str,
    pub correlation_id: Uuid,
    pub user_tx_xid: u64,
}

#[derive(Debug, Clone)]
pub struct PocV21PostingLineRow {
    pub business_date_jdate: i32,
    pub doc_chrono: i64,
    pub document_id: i64,
    pub sub_priority: i32,
    pub event_type: &'static str,
    pub amount: i64,
    pub debit_account: Option<i64>,
    pub credit_account: Option<i64>,
    pub correlation_id: Uuid,
    pub user_tx_xid: u64,
}

#[derive(Debug, Clone)]
pub struct PocV21PostingLineInventoryRow {
    /// Resolved at Step 5 INSERT via DEFAULT VALUES / RETURNING.
    /// For M1.3 we use a transient ordinal that links each inventory
    /// row to the posting-line row in the same per-event group.
    pub posting_line_ordinal: usize,
    pub sku_id: i64,
    pub location_id: i64,
    pub qty: i64,
    pub layer_id: Option<i64>,
}

pub trait PocV21CostMethod: Send + Sync {
    fn method_id(&self) -> &'static str;

    /// Apply a single event to the snapshot; push row vectors onto
    /// `result`. Returns the per-event result (correlation_id + optional
    /// error_code). The committer dispatcher picks the method per-SKU
    /// and calls `apply_one` event-by-event, so a batch may interleave
    /// methods (FIFO event then AVG event then FIFO event again).
    fn apply_one(
        &self,
        event: &PocV21Event,
        snapshot: &mut PocV21Snapshot,
        result: &mut PocV21ApplyResult,
    ) -> PocV21EventResult;
}
