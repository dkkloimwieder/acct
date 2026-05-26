//! Deep-pool layer seeding (design-v3.1 §10.5, P4 acct-2ttr.8).
//!
//! Path C's hot path NEVER creates FIFO/LIFO layer rows — provisional mode
//! updates only the aggregate (`pool_state.layer_id = 0`). So the harness
//! cannot establish deep pool state by driving Path C ingestion. Instead it
//! writes layer rows directly with SQL, simulating the state a strict-mode
//! implementation would have produced, and writes a matching `trx_line` receipt
//! per layer so the trx_line stream stays consistent with pool_state (letting a
//! future recalc/close replay verify against the seeded state).
//!
//! Layer convention (schema 0003): `layer_id = trx_line.id` of the receipt that
//! created the layer; `layer_id = 0` is the aggregate. Each layer is seeded with
//! a large qty so depletion workloads (S5–S8) never exhaust the pool over a
//! measurement window — Path C raises InsufficientInventory rather than going
//! negative (negative inventory is out of scope, §3.6).

use sqlx::PgPool;

/// source_id namespace for seed trxs — high enough not to collide with the
/// run drivers' `run_prefix`-based source_ids (max ≈ 1e18).
const SEED_NS: i64 = 8_000_000_000_000_000_000;
/// Per-layer (and per-receipt) qty. Large so depletion workloads don't drain a
/// hot pool within a run: aggregate qty = depth × this.
pub const PER_LAYER_QTY: i64 = 1_000_000_000;
/// Per-layer unit_cost (1.0 at 1e-6 fixed precision). Constant across layers so
/// the seeded aggregate unit_cost is exactly this (no rounding to reconcile).
pub const UNIT_COST: i64 = 1_000_000;

const SEED_POSTED_AT: &str = "2026-01-01T00:00:00+00:00";

/// Deepen every pool in `pool_ids` to `depth` layer rows (+ matching aggregate
/// and trx_line receipts). `depth == 0` is a no-op. Idempotent: if any
/// pool_state rows already exist it assumes the universe is already seeded and
/// returns without touching it.
pub async fn deepen(pool: &PgPool, pool_ids: &[i64], depth: usize) -> Result<(), sqlx::Error> {
    if depth == 0 || pool_ids.is_empty() {
        return Ok(());
    }

    let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM pool_state")
        .fetch_one(pool)
        .await?;
    if existing > 0 {
        eprintln!(
            "seed: pool_state already has {existing} rows; assuming deep-seeded, skipping deepen"
        );
        return Ok(());
    }

    // One seed trx per pool (trx_line.trx_id is NOT NULL). Bulk-insert, then map
    // pool_id -> trx_id via the recoverable (source_id = SEED_NS + pool_id).
    let seed_source_ids: Vec<i64> = pool_ids.iter().map(|p| SEED_NS + p).collect();
    let trx_rows: Vec<(i64, i64)> = sqlx::query_as(
        "INSERT INTO trx (trx_type, source_id, posted_at) \
         SELECT 'po_receipt'::trx_type, sid, $2::timestamptz \
           FROM UNNEST($1::bigint[]) AS sid \
         RETURNING source_id, id",
    )
    .bind(&seed_source_ids)
    .bind(SEED_POSTED_AT)
    .fetch_all(pool)
    .await?;
    let trx_of = |pool_id: i64| -> i64 {
        let sid = SEED_NS + pool_id;
        trx_rows
            .iter()
            .find(|(s, _)| *s == sid)
            .map(|(_, id)| *id)
            .expect("seed trx for pool")
    };

    let depth_i = depth as i64;
    let agg_qty = depth_i * PER_LAYER_QTY;

    // Per pool: insert `depth` receipt trx_lines, mirror each into a pool_state
    // layer (layer_id = trx_line.id), then upsert the aggregate (layer_id = 0).
    for &pool_id in pool_ids {
        let trx_id = trx_of(pool_id);
        sqlx::query(
            "WITH ins AS ( \
                INSERT INTO trx_line (trx_id, pool_id, line_type, qty, unit_cost) \
                SELECT $1, $2, 'po_receipt_line'::line_type, $3, $4 \
                  FROM generate_series(1, $5::int) \
                RETURNING id \
             ) \
             INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost) \
             SELECT $2, id, $3, $4 FROM ins",
        )
        .bind(trx_id)
        .bind(pool_id)
        .bind(PER_LAYER_QTY)
        .bind(UNIT_COST)
        .bind(depth_i)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost) \
             VALUES ($1, 0, $2, $3) \
             ON CONFLICT (pool_id, layer_id) \
             DO UPDATE SET qty = EXCLUDED.qty, unit_cost = EXCLUDED.unit_cost",
        )
        .bind(pool_id)
        .bind(agg_qty)
        .bind(UNIT_COST)
        .execute(pool)
        .await?;
    }

    eprintln!(
        "seed: deepened {} pools to depth {} ({} layer rows + aggregate each, agg qty {})",
        pool_ids.len(),
        depth,
        depth,
        agg_qty
    );
    Ok(())
}
