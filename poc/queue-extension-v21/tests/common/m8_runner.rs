//! M8.1 (acct-toj6) — workload-generator harness for the 9 bake-off
//! shapes (spec §5.2 + Amendment 01 §S9).
//!
//! Differs from v2's `m9_runner.rs` in three load-bearing ways:
//!
//!   1. **Async enqueue model**. `poc_v21_enqueue` returns immediately;
//!      success is observed via `poc_v21_submission_status` reaching a
//!      terminal state (`committed`, `failed`, `replayed`). Per-call
//!      latency is enqueue → terminal-state-observed.
//!
//!   2. **Per-envelope correlation_id**. Every envelope is identified by
//!      a fresh `Uuid::new_v4()`; the runner polls submission_status
//!      WHERE correlation_id = $1 with a 1ms tick.
//!
//!   3. **Multi-pool envelopes**. `wo_complete` envelopes touch K
//!      component pools + 1 WIP pool + 1 output pool. The runner
//!      assembles `pool_keys = {"sku": [...], "wip": [[wo, op]]}`
//!      per envelope.
//!
//! Latency model: per-backend submit-and-poll. Each backend's
//! outstanding-envelope count is exactly 1 at a time; throughput
//! comes from N parallel backends. Matches v2 bench semantics.

#![allow(dead_code)]

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use uuid::Uuid;

pub const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc_queue_v21";
pub const LOCATION_ID: i64 = 1;
const POLL_TICK_MS: u64 = 1;
const TERMINAL_TIMEOUT_SECS: u64 = 10;
const SEED_LAYER_QTY: i64 = 1_000_000_000;
const SEED_LAYER_UNIT_COST: i64 = 100;

// ──────────────────────────────────────────────────────────────────────
// Shape definitions
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    S1FanOutSimple,
    S2FanOutWo,
    S3FanContestedWo,
    S4FanInWo,
    S5HotPool,
    S6LargeWo,
    S7VeryLargeWo,
    S8MixedEventMixedMethod,
    S9CausalChain,
}

impl Shape {
    pub fn name(&self) -> &'static str {
        match self {
            Self::S1FanOutSimple => "s1_fan_out_simple",
            Self::S2FanOutWo => "s2_fan_out_wo",
            Self::S3FanContestedWo => "s3_fan_contested_wo",
            Self::S4FanInWo => "s4_fan_in_wo",
            Self::S5HotPool => "s5_hot_pool",
            Self::S6LargeWo => "s6_large_wo",
            Self::S7VeryLargeWo => "s7_very_large_wo",
            Self::S8MixedEventMixedMethod => "s8_mixed_event_mixed_method",
            Self::S9CausalChain => "s9_causal_chain",
        }
    }

    pub fn all() -> &'static [Shape] {
        &[
            Shape::S1FanOutSimple,
            Shape::S2FanOutWo,
            Shape::S3FanContestedWo,
            Shape::S4FanInWo,
            Shape::S5HotPool,
            Shape::S6LargeWo,
            Shape::S7VeryLargeWo,
            Shape::S8MixedEventMixedMethod,
            Shape::S9CausalChain,
        ]
    }

    pub fn from_name(s: &str) -> Option<Shape> {
        Self::all().iter().copied().find(|sh| sh.name() == s)
    }

    /// g = SKU-pool population. For S9 g is the raw-component count
    /// per backend (assembly count is g/2).
    pub fn g(&self) -> usize {
        match self {
            Self::S1FanOutSimple => 5000,
            Self::S2FanOutWo => 5000,
            Self::S3FanContestedWo => 50,
            Self::S4FanInWo => 1,
            Self::S5HotPool => 100,
            Self::S6LargeWo => 5000,
            Self::S7VeryLargeWo => 5000,
            Self::S8MixedEventMixedMethod => 1000,
            Self::S9CausalChain => 10,
        }
    }

    /// K = component count per envelope (0 for non-wo shapes).
    pub fn k(&self) -> usize {
        match self {
            Self::S1FanOutSimple | Self::S5HotPool => 1,
            Self::S2FanOutWo
            | Self::S3FanContestedWo
            | Self::S4FanInWo
            | Self::S8MixedEventMixedMethod
            | Self::S9CausalChain => 5,
            Self::S6LargeWo => 15,
            Self::S7VeryLargeWo => 50,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// Public types
// ──────────────────────────────────────────────────────────────────────

pub struct WorkloadConfig {
    pub shape: Shape,
    pub n_backends: usize,
    pub duration_secs: u64,
    /// S9-only: components-vs-receipt margin per triplet. 0 = exact
    /// match (clean commit). -1 = WO requests more than PoReceipt
    /// provides (must fail with InsufficientInventory). Ignored for
    /// other shapes.
    pub s9_margin: i64,
}

#[derive(Debug, Clone)]
pub struct ShapeResult {
    pub name: &'static str,
    pub g: usize,
    pub k: usize,
    pub n_backends: usize,
    pub duration_secs: u64,
    pub total_envelopes: u64,
    pub committed: u64,
    pub failed: u64,
    pub throughput_eps: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub router_envelopes_delta: i64,
    pub router_superbatch_delta: i64,
    pub avg_envelopes_per_sb: f64,
    pub method_breakdown: Option<HashMap<String, u64>>,
    pub s9_margin: Option<i64>,
}

// ──────────────────────────────────────────────────────────────────────
// Pool + reset helpers
// ──────────────────────────────────────────────────────────────────────

pub async fn connect_pool(max_conns: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .acquire_timeout(Duration::from_secs(30))
        .connect(POC_DSN)
        .await
        .expect("connect to acct_poc_queue_v21")
}

/// TRUNCATE PoC tables + reset router stats. Drains shmem first via the
/// in_flight + total_envelopes settle pattern from common/mod.rs.
pub async fn reset_state(pool: &PgPool) -> Result<(), sqlx::Error> {
    let drain_start = Instant::now();
    let mut last_total: i64 = -1;
    loop {
        let in_flight: i64 = sqlx::query_scalar(
            "SELECT (COALESCE((poc_v21_staging_state_counts()->>'pending')::bigint, 0) \
                   + COALESCE((poc_v21_staging_state_counts()->>'processing')::bigint, 0) \
                   + COALESCE((poc_v21_staging_state_counts()->>'routed')::bigint, 0))",
        )
        .fetch_one(pool)
        .await
        .unwrap_or(0);
        let total: i64 = sqlx::query_scalar("SELECT poc_v21_router_total_envelopes()")
            .fetch_one(pool)
            .await
            .unwrap_or(-1);
        if in_flight == 0 && total == last_total {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let recheck: i64 = sqlx::query_scalar("SELECT poc_v21_router_total_envelopes()")
                .fetch_one(pool)
                .await
                .unwrap_or(-1);
            if recheck == total {
                break;
            }
            last_total = recheck;
        } else {
            last_total = total;
        }
        if drain_start.elapsed() > Duration::from_secs(15) {
            eprintln!(
                "reset_state: didn't settle in 15s (in_flight={in_flight}, total={total})"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sqlx::query(
        "TRUNCATE poc_v21_cost_layers, poc_v21_cost_depletions, \
                  poc_v21_cost_consumptions, poc_v21_posting_lines, \
                  poc_v21_posting_line_inventory, poc_v21_submission_status, \
                  poc_v21_pool_locks, poc_v21_wip_pool_locks, poc_v21_avg_pool_state, \
                  poc_v21_persistent_staging, poc_v21_standard_costs, \
                  poc_v21_sku_method_assignments \
                  RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await?;

    sqlx::query("SELECT poc_v21_router_stats_reset()")
        .execute(pool)
        .await?;

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────
// Pre-seed per shape
// ──────────────────────────────────────────────────────────────────────

/// Per-shape pre-seed: FIFO layers for component SKUs; AVG state for
/// AVG-method SKUs; standard_costs for STD-method SKUs.
///
/// Sizing rationale: each pool gets one huge layer
/// (SEED_LAYER_QTY = 1e9 units at unit_cost=100). Even S7 K=50 at
/// 60s × 1000 evps × 50 components/envelope = 3M consumptions never
/// scratches the surface. Eliminates depletion mid-run as a noise
/// source.
pub async fn pre_seed_shape(
    pool: &PgPool,
    shape: Shape,
    n_backends: usize,
) -> Result<(), sqlx::Error> {
    match shape {
        Shape::S9CausalChain => pre_seed_s9(pool, n_backends).await,
        Shape::S8MixedEventMixedMethod => pre_seed_mixed_method(pool, shape).await,
        _ => pre_seed_uniform_fifo(pool, shape).await,
    }
}

async fn pre_seed_uniform_fifo(pool: &PgPool, shape: Shape) -> Result<(), sqlx::Error> {
    let sku_range = sku_range_for_shape(shape);
    bulk_seed_fifo_layers(pool, sku_range).await?;
    Ok(())
}

async fn pre_seed_mixed_method(pool: &PgPool, shape: Shape) -> Result<(), sqlx::Error> {
    let sku_range = sku_range_for_shape(shape);
    bulk_seed_fifo_layers(pool, sku_range.clone()).await?;

    // Split [sku_range] into thirds: 1st third FIFO (default, no row needed),
    // 2nd third AVG, 3rd third STD. method_id column on
    // poc_v21_sku_method_assignments selects the cost strategy.
    let g = shape.g() as i64;
    let lo = *sku_range.start();
    let third = (g / 3).max(1);
    let avg_start = lo + third;
    let std_start = lo + 2 * third;
    let hi = *sku_range.end();

    let mut assignments: Vec<(i64, &str)> = Vec::new();
    for sku in avg_start..std_start {
        assignments.push((sku, "avg"));
    }
    for sku in std_start..=hi {
        assignments.push((sku, "std"));
    }
    if !assignments.is_empty() {
        let sku_ids: Vec<i64> = assignments.iter().map(|(s, _)| *s).collect();
        let methods: Vec<String> = assignments.iter().map(|(_, m)| m.to_string()).collect();
        sqlx::query(
            "INSERT INTO poc_v21_sku_method_assignments (sku_id, method_id) \
             SELECT s, m FROM UNNEST($1::bigint[], $2::text[]) AS t(s, m) \
             ON CONFLICT (sku_id) DO UPDATE SET method_id = EXCLUDED.method_id",
        )
        .bind(&sku_ids)
        .bind(&methods)
        .execute(pool)
        .await?;
    }

    // AVG state pre-seeded for AVG-method SKUs (running avg starting from
    // the FIFO layer's effective average — single layer at uc=100 means
    // avg_unit_cost=100, total_qty=SEED_LAYER_QTY).
    let avg_skus: Vec<i64> = (avg_start..std_start).collect();
    if !avg_skus.is_empty() {
        sqlx::query(
            "INSERT INTO poc_v21_avg_pool_state \
                (sku_id, location_id, avg_unit_cost, total_qty, last_updated_at, last_committer_tx_id) \
             SELECT s, $1, $2, $3, now(), 0 \
               FROM UNNEST($4::bigint[]) AS t(s) \
             ON CONFLICT (sku_id, location_id) DO UPDATE SET \
                 avg_unit_cost = EXCLUDED.avg_unit_cost, total_qty = EXCLUDED.total_qty",
        )
        .bind(LOCATION_ID)
        .bind(SEED_LAYER_UNIT_COST)
        .bind(SEED_LAYER_QTY)
        .bind(&avg_skus)
        .execute(pool)
        .await?;
    }

    // STD costs: every SKU in std_skus range gets unit_cost=100.
    let std_skus: Vec<i64> = (std_start..=hi).collect();
    if !std_skus.is_empty() {
        sqlx::query(
            "INSERT INTO poc_v21_standard_costs (sku_id, location_id, unit_cost, effective_from) \
             SELECT s, $1, $2, now() - interval '1 day' \
               FROM UNNEST($3::bigint[]) AS t(s)",
        )
        .bind(LOCATION_ID)
        .bind(SEED_LAYER_UNIT_COST)
        .bind(&std_skus)
        .execute(pool)
        .await?;
    }

    Ok(())
}

async fn pre_seed_s9(pool: &PgPool, n_backends: usize) -> Result<(), sqlx::Error> {
    // S9: per backend, raw SKUs 10000 + b*100 .. +9 ; assembly SKUs
    // 20000 + b*100 .. +4. PoReceipt seeds raw inventory in-bench;
    // pre-seed is only required so the schema doesn't gate against
    // empty SKUs. We pre-seed an arbitrary FIFO layer on each so the
    // first PoReceipt doesn't have to bootstrap a brand-new pool —
    // matches real-world warm-cache shape.
    let mut all_skus: Vec<i64> = Vec::new();
    for b in 0..n_backends as i64 {
        for off in 0..10 {
            all_skus.push(10000 + b * 100 + off);
        }
        for off in 0..5 {
            all_skus.push(20000 + b * 100 + off);
        }
    }
    if all_skus.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO poc_v21_cost_layers \
            (sku_id, location_id, qty, unit_cost, born_at, born_seq, source_kind, \
             correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
         SELECT s, $1, $2, $3, now() - interval '1 minute', 1, 'po_receipt', \
                gen_random_uuid(), '1'::xid8, 1, 1 \
           FROM UNNEST($4::bigint[]) AS t(s)",
    )
    .bind(LOCATION_ID)
    .bind(1_i64) // tiny seed; bench PoReceipts add real inventory
    .bind(SEED_LAYER_UNIT_COST)
    .bind(&all_skus)
    .execute(pool)
    .await?;
    Ok(())
}

fn sku_range_for_shape(shape: Shape) -> std::ops::RangeInclusive<i64> {
    let g = shape.g() as i64;
    let k = shape.k() as i64;
    match shape {
        // S4 fan_in uses K distinct component SKUs even though g=1; the
        // K pool keys come from SKUs 1..=K and the single-pool fan-in
        // shape lives in the WIP + output domain. Pre-seed 1..=K so
        // every component pool has stock.
        Shape::S4FanInWo => 1..=k.max(g),
        _ => 1..=g,
    }
}

async fn bulk_seed_fifo_layers(
    pool: &PgPool,
    sku_range: std::ops::RangeInclusive<i64>,
) -> Result<(), sqlx::Error> {
    let skus: Vec<i64> = sku_range.collect();
    if skus.is_empty() {
        return Ok(());
    }
    // Bulk INSERT via UNNEST keeps the seed phase O(1) round-trips
    // instead of O(g). For S2/S6/S7 with g=5000 that's the difference
    // between 5000 INSERTs and 1.
    sqlx::query(
        "INSERT INTO poc_v21_cost_layers \
            (sku_id, location_id, qty, unit_cost, born_at, born_seq, source_kind, \
             correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
         SELECT s, $1, $2, $3, now() - interval '1 minute', 1, 'po_receipt', \
                gen_random_uuid(), '1'::xid8, 1, 1 \
           FROM UNNEST($4::bigint[]) AS t(s)",
    )
    .bind(LOCATION_ID)
    .bind(SEED_LAYER_QTY)
    .bind(SEED_LAYER_UNIT_COST)
    .bind(&skus)
    .execute(pool)
    .await?;
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────
// Per-shape SKU + method picking
// ──────────────────────────────────────────────────────────────────────

/// Pick the SKU pool for a non-S9 envelope. For multi-component
/// shapes the returned SKU is the first component; the rest follow
/// `build_components`.
pub fn pick_sku(
    shape: Shape,
    backend_idx: usize,
    n_backends: usize,
    iter: usize,
    rng: &mut SmallRng,
) -> i64 {
    let g = shape.g() as i64;
    match shape {
        Shape::S4FanInWo => 1,
        Shape::S1FanOutSimple
        | Shape::S2FanOutWo
        | Shape::S6LargeWo
        | Shape::S7VeryLargeWo => {
            let per = (g / n_backends as i64).max(1);
            let base = (backend_idx as i64) * per + 1;
            let off = (iter as i64) % per;
            (base + off).min(g)
        }
        Shape::S3FanContestedWo => rng.random_range(1..=g),
        Shape::S5HotPool => {
            if rng.random_bool(0.8) {
                1
            } else {
                rng.random_range(2..=g)
            }
        }
        Shape::S8MixedEventMixedMethod => rng.random_range(1..=g),
        Shape::S9CausalChain => unreachable!("S9 uses per-triplet SKU layout"),
    }
}

/// Build K component triples (sku, location, qty) for a wo_complete
/// envelope. Output SKU returned separately by `pick_output_sku`.
pub fn build_components(
    shape: Shape,
    primary_sku: i64,
    iter: usize,
    rng: &mut SmallRng,
) -> Vec<(i64, i64, i64)> {
    let k = shape.k();
    let g = shape.g() as i64;
    let mut out = Vec::with_capacity(k);
    match shape {
        Shape::S1FanOutSimple | Shape::S5HotPool => {
            // K=1 path; component IS the primary.
            out.push((primary_sku, LOCATION_ID, 1));
        }
        Shape::S4FanInWo => {
            // K=5 components all on SKU 1..=5 (modulo g). g=1 means
            // single-pool fan-in, but K=5 still needs 5 *distinct*
            // pool keys for the router's disjoint-set check (a
            // wo_complete with duplicate component SKUs would be
            // rejected at parse time). Use SKU range [1..=5]
            // unconditionally for fan-in.
            for i in 0..k {
                out.push(((i as i64) + 1, LOCATION_ID, 1));
            }
        }
        Shape::S3FanContestedWo => {
            // Pick K distinct SKUs from [1..=g] randomly.
            let mut chosen: std::collections::HashSet<i64> =
                std::collections::HashSet::new();
            chosen.insert(primary_sku);
            while chosen.len() < k {
                chosen.insert(rng.random_range(1..=g));
            }
            for s in chosen {
                out.push((s, LOCATION_ID, 1));
            }
        }
        Shape::S2FanOutWo | Shape::S6LargeWo | Shape::S7VeryLargeWo => {
            // K consecutive SKUs strided by K from primary, wrapping at
            // g. Stride avoids component-set overlap between consecutive
            // envelopes from the same backend (which otherwise creates
            // pathological lock-acquisition chains across SuperBatches).
            for i in 0..k {
                let s = (((primary_sku - 1) * k as i64 + i as i64) % g) + 1;
                out.push((s, LOCATION_ID, 1));
            }
        }
        Shape::S8MixedEventMixedMethod => {
            // K distinct SKUs strided by K from primary, wrapping at g.
            for i in 0..k {
                let s = (((primary_sku - 1) * k as i64 + i as i64) % g) + 1;
                out.push((s, LOCATION_ID, 1));
            }
        }
        Shape::S9CausalChain => unreachable!("S9 has its own component layout"),
    }
    let _ = iter;
    out
}

/// Output SKU for a wo_complete envelope. Distinct from the
/// component SKUs so the lex-lock chain has K+2 nodes (K components
/// + 1 output + 1 WIP).
pub fn pick_output_sku(shape: Shape, primary_sku: i64) -> i64 {
    let g = shape.g() as i64;
    match shape {
        Shape::S4FanInWo => 100,
        Shape::S3FanContestedWo => g + primary_sku, // offset above the g range
        _ => g + 1 + (primary_sku % 1000),
    }
}

/// WIP account (wo_id, op_id). Each backend × iter gets a fresh pair
/// so the WIP pool key is unique per envelope (router treats this
/// as uncontended by construction).
pub fn pick_wip(backend_idx: usize, iter: usize) -> (i64, i64) {
    let wo_id = 1_000_000 + (backend_idx as i64) * 10_000_000 + (iter as i64);
    (wo_id, 10)
}

/// S8 method picker: rotates fifo/avg/std per envelope to dispatch
/// each path with predictable proportions.
pub fn pick_event_kind(iter: usize) -> &'static str {
    if iter % 2 == 0 {
        "inv_adjust"
    } else {
        "wo_complete"
    }
}

// ──────────────────────────────────────────────────────────────────────
// Enqueue + wait_terminal
// ──────────────────────────────────────────────────────────────────────

async fn enqueue_inv_adjust(
    pool: &PgPool,
    cid: Uuid,
    sku: i64,
    qty: i64,
    iter: i64,
) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": LOCATION_ID,
        "qty": -qty,
        "unit_cost": 0,
        "issue_id": iter,
        "business_date_jdate": 20221,
        "doc_chrono": iter,
        "document_id": 7_000_000_i64 + iter,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, LOCATION_ID]] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'inv_adjust', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await?;
    Ok(())
}

async fn enqueue_wo_complete(
    pool: &PgPool,
    cid: Uuid,
    wo_id: i64,
    op_id: i64,
    components: &[(i64, i64, i64)],
    output: (i64, i64, i64),
    iter: i64,
) -> Result<(), sqlx::Error> {
    let comps_json: Vec<serde_json::Value> = components
        .iter()
        .map(|(s, l, q)| serde_json::json!([s, l, q]))
        .collect();
    let payload = serde_json::json!({
        "wip_account": [wo_id, op_id],
        "components": comps_json,
        "output": [output.0, output.1, output.2],
        "business_date_jdate": 20221,
        "doc_chrono": iter,
        "document_id": 8_000_000_i64 + iter,
    });
    let mut sku_keys: Vec<serde_json::Value> = components
        .iter()
        .map(|(s, l, _)| serde_json::json!([s, l]))
        .collect();
    sku_keys.push(serde_json::json!([output.0, output.1]));
    let pool_keys = serde_json::json!({ "sku": sku_keys, "wip": [[wo_id, op_id]] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'wo_complete', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await?;
    Ok(())
}

async fn enqueue_po_receipt(
    pool: &PgPool,
    cid: Uuid,
    sku: i64,
    qty: i64,
    iter: i64,
) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": LOCATION_ID,
        "qty": qty,
        "unit_cost": SEED_LAYER_UNIT_COST,
        "issue_id": iter,
        "business_date_jdate": 20221,
        "doc_chrono": iter,
        "document_id": 9_000_000_i64 + iter,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, LOCATION_ID]] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'po_receipt', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await?;
    Ok(())
}

async fn enqueue_so_shipment(
    pool: &PgPool,
    cid: Uuid,
    sku: i64,
    qty: i64,
    iter: i64,
) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": LOCATION_ID,
        "qty": -qty,
        "unit_cost": 0,
        "issue_id": iter,
        "business_date_jdate": 20221,
        "doc_chrono": iter,
        "document_id": 11_000_000_i64 + iter,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, LOCATION_ID]] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'so_shipment', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await?;
    Ok(())
}

/// Poll submission_status for a single correlation_id until terminal
/// or timeout. Returns the state string ("committed" / "failed" /
/// "replayed") or None on timeout.
async fn wait_terminal_single(
    pool: &PgPool,
    cid: Uuid,
    timeout: Duration,
) -> Option<String> {
    let start = Instant::now();
    loop {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT state::text FROM poc_v21_submission_status \
              WHERE correlation_id = $1 \
                AND state IN ('committed', 'failed', 'replayed')",
        )
        .bind(cid)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        if let Some((s,)) = row {
            return Some(s);
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(POLL_TICK_MS)).await;
    }
}

// ──────────────────────────────────────────────────────────────────────
// Router stats snapshot
// ──────────────────────────────────────────────────────────────────────

async fn router_stats_snapshot(pool: &PgPool) -> (i64, i64) {
    let rows: Vec<(String, f64)> =
        sqlx::query_as("SELECT stat_name, stat_value FROM poc_v21_router_stats()")
            .fetch_all(pool)
            .await
            .unwrap_or_default();
    let mut total_env: i64 = 0;
    let mut sb_count: i64 = 0;
    for (k, v) in rows {
        if k == "total_envelopes" {
            total_env = v as i64;
        }
        if k == "superbatch_count" {
            sb_count = v as i64;
        }
    }
    (total_env, sb_count)
}

// ──────────────────────────────────────────────────────────────────────
// run_shape orchestrator
// ──────────────────────────────────────────────────────────────────────

pub async fn run_shape(pool: Arc<PgPool>, cfg: &WorkloadConfig) -> ShapeResult {
    if cfg.shape == Shape::S9CausalChain {
        return run_s9(pool, cfg).await;
    }

    let n = cfg.n_backends;
    let duration = Duration::from_secs(cfg.duration_secs);
    let barrier = Arc::new(Barrier::new(n));

    let (env_start, sb_start) = router_stats_snapshot(&pool).await;
    let wall_t0 = Instant::now();

    let mut handles = Vec::with_capacity(n);
    for backend_idx in 0..n {
        let pool = pool.clone();
        let barrier = barrier.clone();
        let shape = cfg.shape;
        handles.push(tokio::spawn(async move {
            let mut latencies: Vec<u64> = Vec::with_capacity(100_000);
            let mut committed: u64 = 0;
            let mut failed: u64 = 0;
            let mut method_counts: HashMap<String, u64> = HashMap::new();

            let seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0)
                .wrapping_add((backend_idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let mut rng = SmallRng::seed_from_u64(seed);

            barrier.wait().await;
            let start = Instant::now();
            let mut iter: usize = 0;
            while start.elapsed() < duration {
                let cid = Uuid::new_v4();
                // doc_chrono / document_id must be unique per envelope —
                // pack (backend_idx, iter) into a single i64 so they
                // never collide across backends.
                let chrono_iter: i64 =
                    (backend_idx as i64) * 100_000_000 + iter as i64 + 1;
                let primary = pick_sku(shape, backend_idx, n, iter, &mut rng);

                let t0 = Instant::now();
                let enq_res: Result<(), sqlx::Error> = match shape {
                    Shape::S1FanOutSimple | Shape::S5HotPool => {
                        enqueue_inv_adjust(&pool, cid, primary, 1, chrono_iter).await
                    }
                    Shape::S2FanOutWo
                    | Shape::S3FanContestedWo
                    | Shape::S4FanInWo
                    | Shape::S6LargeWo
                    | Shape::S7VeryLargeWo => {
                        let comps = build_components(shape, primary, iter, &mut rng);
                        let out = (
                            pick_output_sku(shape, primary),
                            LOCATION_ID,
                            1_i64,
                        );
                        let (wo_id, op_id) = pick_wip(backend_idx, iter);
                        enqueue_wo_complete(
                            &pool, cid, wo_id, op_id, &comps, out, chrono_iter,
                        )
                        .await
                    }
                    Shape::S8MixedEventMixedMethod => {
                        let kind = pick_event_kind(iter);
                        *method_counts.entry(kind.to_string()).or_insert(0) += 1;
                        if kind == "inv_adjust" {
                            enqueue_inv_adjust(&pool, cid, primary, 1, chrono_iter).await
                        } else {
                            let comps = build_components(shape, primary, iter, &mut rng);
                            let out = (
                                pick_output_sku(shape, primary),
                                LOCATION_ID,
                                1_i64,
                            );
                            let (wo_id, op_id) = pick_wip(backend_idx, iter);
                            enqueue_wo_complete(
                                &pool, cid, wo_id, op_id, &comps, out, chrono_iter,
                            )
                            .await
                        }
                    }
                    Shape::S9CausalChain => unreachable!(),
                };

                match enq_res {
                    Ok(()) => {
                        let state = wait_terminal_single(
                            &pool,
                            cid,
                            Duration::from_secs(TERMINAL_TIMEOUT_SECS),
                        )
                        .await;
                        let dt_us = (t0.elapsed().as_nanos() / 1000) as u64;
                        match state.as_deref() {
                            Some("committed") | Some("replayed") => {
                                latencies.push(dt_us);
                                committed += 1;
                            }
                            _ => {
                                failed += 1;
                            }
                        }
                    }
                    Err(_) => {
                        failed += 1;
                    }
                }
                iter += 1;
            }
            (latencies, committed, failed, method_counts)
        }));
    }

    let mut all_lat: Vec<u64> = Vec::new();
    let mut total_committed: u64 = 0;
    let mut total_failed: u64 = 0;
    let mut total_methods: HashMap<String, u64> = HashMap::new();
    for h in handles {
        let (lat, c, f, m) = h.await.expect("backend task panicked");
        all_lat.extend(lat);
        total_committed += c;
        total_failed += f;
        for (k, v) in m {
            *total_methods.entry(k).or_insert(0) += v;
        }
    }

    let (env_end, sb_end) = router_stats_snapshot(&pool).await;

    all_lat.sort_unstable();
    let throughput = (total_committed as f64) / cfg.duration_secs as f64;
    let total_envelopes = total_committed + total_failed;
    let env_delta = env_end - env_start;
    let sb_delta = sb_end - sb_start;
    let avg_eps = if sb_delta > 0 {
        env_delta as f64 / sb_delta as f64
    } else {
        0.0
    };

    let _ = wall_t0;
    ShapeResult {
        name: cfg.shape.name(),
        g: cfg.shape.g(),
        k: cfg.shape.k(),
        n_backends: cfg.n_backends,
        duration_secs: cfg.duration_secs,
        total_envelopes,
        committed: total_committed,
        failed: total_failed,
        throughput_eps: throughput,
        p50_us: percentile(&all_lat, 50.0),
        p99_us: percentile(&all_lat, 99.0),
        p999_us: percentile(&all_lat, 99.9),
        router_envelopes_delta: env_delta,
        router_superbatch_delta: sb_delta,
        avg_envelopes_per_sb: avg_eps,
        method_breakdown: if cfg.shape == Shape::S8MixedEventMixedMethod {
            Some(total_methods)
        } else {
            None
        },
        s9_margin: None,
    }
}

// ──────────────────────────────────────────────────────────────────────
// S9 causal_chain (Amendment 01)
// ──────────────────────────────────────────────────────────────────────

async fn run_s9(pool: Arc<PgPool>, cfg: &WorkloadConfig) -> ShapeResult {
    let n = cfg.n_backends;
    let duration = Duration::from_secs(cfg.duration_secs);
    let barrier = Arc::new(Barrier::new(n));
    let margin = cfg.s9_margin;

    let (env_start, sb_start) = router_stats_snapshot(&pool).await;

    // Layout: per backend b
    //   raw SKUs:      10000 + b*100 + [0..=9]   (one used per triplet)
    //   assembly SKUs: 20000 + b*100 + [0..=4]   (one used per triplet)
    let mut handles = Vec::with_capacity(n);
    for backend_idx in 0..n {
        let pool = pool.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            let mut latencies: Vec<u64> = Vec::with_capacity(50_000);
            let mut committed: u64 = 0;
            let mut failed: u64 = 0;

            let seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0)
                .wrapping_add((backend_idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let mut rng = SmallRng::seed_from_u64(seed);

            barrier.wait().await;
            let start = Instant::now();
            let mut iter: usize = 0;
            while start.elapsed() < duration {
                let raw_sku = 10000 + (backend_idx as i64) * 100 + rng.random_range(0..10);
                let asm_sku = 20000 + (backend_idx as i64) * 100 + rng.random_range(0..5);
                // R = receipt qty (10 baseline); WO consumes (R - margin).
                // margin=0 → match. margin=-1 → WO asks 11; insufficient.
                let r = 10_i64;
                let wo_consume = r.saturating_sub(margin).max(0);

                let chrono_base: i64 = (backend_idx as i64) * 100_000_000
                    + (iter as i64) * 3
                    + 1;
                let cid_e1 = Uuid::new_v4();
                let cid_e2 = Uuid::new_v4();
                let cid_e3 = Uuid::new_v4();

                let t0 = Instant::now();
                // E1: PoReceipt of raw.
                let r1 = enqueue_po_receipt(&pool, cid_e1, raw_sku, r, chrono_base).await;
                if r1.is_err() {
                    failed += 1;
                    iter += 1;
                    continue;
                }
                let s1 = wait_terminal_single(
                    &pool,
                    cid_e1,
                    Duration::from_secs(TERMINAL_TIMEOUT_SECS),
                )
                .await;
                if !matches!(s1.as_deref(), Some("committed") | Some("replayed")) {
                    failed += 1;
                    iter += 1;
                    continue;
                }

                // E2: WoComplete consuming (R-margin) raw → 1 assembly.
                let wo_id = 1_000_000 + (backend_idx as i64) * 10_000_000 + iter as i64;
                let op_id = 10;
                let comps = vec![(raw_sku, LOCATION_ID, wo_consume)];
                let out = (asm_sku, LOCATION_ID, 1_i64);
                let r2 = enqueue_wo_complete(
                    &pool, cid_e2, wo_id, op_id, &comps, out, chrono_base + 1,
                )
                .await;
                if r2.is_err() {
                    failed += 1;
                    iter += 1;
                    continue;
                }
                let s2 = wait_terminal_single(
                    &pool,
                    cid_e2,
                    Duration::from_secs(TERMINAL_TIMEOUT_SECS),
                )
                .await;
                match s2.as_deref() {
                    Some("committed") | Some("replayed") => {
                        // E3: SoShipment of assembly.
                        let r3 = enqueue_so_shipment(&pool, cid_e3, asm_sku, 1, chrono_base + 2)
                            .await;
                        if r3.is_err() {
                            failed += 1;
                            iter += 1;
                            continue;
                        }
                        let s3 = wait_terminal_single(
                            &pool,
                            cid_e3,
                            Duration::from_secs(TERMINAL_TIMEOUT_SECS),
                        )
                        .await;
                        match s3.as_deref() {
                            Some("committed") | Some("replayed") => {
                                let dt_us = (t0.elapsed().as_nanos() / 1000) as u64;
                                latencies.push(dt_us);
                                committed += 1;
                            }
                            _ => {
                                failed += 1;
                            }
                        }
                    }
                    Some("failed") => {
                        // margin=-1 cascade — E2's explicit failure is
                        // the expected outcome. Timeout (None) does NOT
                        // count as a cascade success.
                        if margin == -1 {
                            let dt_us = (t0.elapsed().as_nanos() / 1000) as u64;
                            latencies.push(dt_us);
                            committed += 1;
                        } else {
                            failed += 1;
                        }
                    }
                    _ => {
                        failed += 1;
                    }
                }
                iter += 1;
            }
            (latencies, committed, failed)
        }));
    }

    let mut all_lat: Vec<u64> = Vec::new();
    let mut total_committed: u64 = 0;
    let mut total_failed: u64 = 0;
    for h in handles {
        let (lat, c, f) = h.await.expect("s9 task panicked");
        all_lat.extend(lat);
        total_committed += c;
        total_failed += f;
    }

    let (env_end, sb_end) = router_stats_snapshot(&pool).await;

    all_lat.sort_unstable();
    let throughput = (total_committed as f64) / cfg.duration_secs as f64;
    let env_delta = env_end - env_start;
    let sb_delta = sb_end - sb_start;
    let avg_eps = if sb_delta > 0 {
        env_delta as f64 / sb_delta as f64
    } else {
        0.0
    };

    ShapeResult {
        name: cfg.shape.name(),
        g: cfg.shape.g(),
        k: cfg.shape.k(),
        n_backends: cfg.n_backends,
        duration_secs: cfg.duration_secs,
        total_envelopes: total_committed + total_failed,
        committed: total_committed,
        failed: total_failed,
        throughput_eps: throughput,
        p50_us: percentile(&all_lat, 50.0),
        p99_us: percentile(&all_lat, 99.0),
        p999_us: percentile(&all_lat, 99.9),
        router_envelopes_delta: env_delta,
        router_superbatch_delta: sb_delta,
        avg_envelopes_per_sb: avg_eps,
        method_breakdown: None,
        s9_margin: Some(margin),
    }
}

// ──────────────────────────────────────────────────────────────────────
// Formatting + percentile
// ──────────────────────────────────────────────────────────────────────

pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub fn fmt_result(r: &ShapeResult) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "shape: {:<28} g={:<5} K={:<3} N={} duration={}s\n",
        r.name, r.g, r.k, r.n_backends, r.duration_secs
    ));
    s.push_str(&format!(
        "  envelopes: total={} committed={} failed={}\n",
        r.total_envelopes, r.committed, r.failed
    ));
    s.push_str(&format!(
        "  throughput: {:.0} env/sec\n",
        r.throughput_eps
    ));
    s.push_str(&format!(
        "  latency: p50={}µs p99={}µs p99.9={}µs\n",
        r.p50_us, r.p99_us, r.p999_us
    ));
    s.push_str(&format!(
        "  router: total_envelopes Δ={} superbatch Δ={} avg_eps={:.2}\n",
        r.router_envelopes_delta, r.router_superbatch_delta, r.avg_envelopes_per_sb
    ));
    if let Some(m) = &r.method_breakdown {
        let total: u64 = m.values().sum();
        for (k, v) in m {
            let pct = (*v as f64) * 100.0 / total.max(1) as f64;
            s.push_str(&format!("  event_mix: {k}={pct:.1}%\n"));
        }
    }
    if let Some(margin) = r.s9_margin {
        s.push_str(&format!("  s9_margin: {margin}\n"));
    }
    s
}
