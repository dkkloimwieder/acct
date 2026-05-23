//! Pool-universe seeder (acct-llt2).
//!
//! Idempotent: if any pool already exists in the target DB, the seeder
//! loads the existing pool_ids and returns without insert. Otherwise it
//! bulk-inserts M skus, L locations, and N pools spread across the
//! (sku, location) grid with a configurable method assignment.
//!
//! Pool universe is held as `Vec<i64>` for the runtime workload
//! generator (acct-y3v1) to pick from via Uniform / Zipf / Disjoint
//! samplers per plan §F.

use sqlx::PgPool;

use crate::cli::MethodMix;

#[derive(Debug, Clone)]
pub struct PoolUniverse {
    pub pool_ids: Vec<i64>,
    /// Inventory + AP accounts seeded alongside the pools. Workload
    /// generators reference these in their debit/credit fields.
    pub inv_account: i64,
    pub ap_account: i64,
}

/// Seed (or load) a pool universe.
///
/// `count` is the number of pools; `skus` × `locations` must be >= count
/// because the `pool (sku_id, location_id, identity_key)` UNIQUE
/// constraint pins one pool per (sku, location) with identity_key=0.
pub async fn seed(
    pool: &PgPool,
    count: usize,
    skus: usize,
    locations: usize,
    method_mix: MethodMix,
) -> Result<PoolUniverse, sqlx::Error> {
    if skus * locations < count {
        return Err(sqlx::Error::Protocol(format!(
            "seed-pools: skus*locations ({}) must be >= count ({count})",
            skus * locations
        )));
    }

    // Idempotency: reuse only when the existing count matches the
    // request. Mismatch is a configuration error — the harness scenario
    // sizing depends on the universe size (S6 stripe_size = universe /
    // callers, S5 Zipf head, etc.), so silently using a smaller fixture
    // produces meaningless measurements.
    let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM pool")
        .fetch_one(pool)
        .await?;
    if existing > 0 {
        if (existing as usize) != count {
            return Err(sqlx::Error::Protocol(format!(
                "seed-pools: existing pool count ({existing}) != requested ({count}). \
                 Drop the existing fixture (TRUNCATE pool, pool_state, pool_lock, trx, \
                 trx_line, posting_line, posting_line_dimension RESTART IDENTITY CASCADE) \
                 or re-run with --count {existing} to reuse."
            )));
        }
        let pool_ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM pool ORDER BY id")
            .fetch_all(pool)
            .await?;
        let inv: i64 = sqlx::query_scalar(
            "SELECT id FROM account WHERE code = '1000-inv' LIMIT 1",
        )
        .fetch_one(pool)
        .await?;
        let ap: i64 = sqlx::query_scalar(
            "SELECT id FROM account WHERE code = '2000-ap' LIMIT 1",
        )
        .fetch_one(pool)
        .await?;
        eprintln!("seed-pools: existing {} pools match request; reusing", pool_ids.len());
        return Ok(PoolUniverse {
            pool_ids,
            inv_account: inv,
            ap_account: ap,
        });
    }

    // ── Accounts ──
    let inv_account: i64 = sqlx::query_scalar(
        "INSERT INTO account (code, name, type) VALUES ('1000-inv', 'Inventory', 'asset') RETURNING id",
    )
    .fetch_one(pool)
    .await?;
    let ap_account: i64 = sqlx::query_scalar(
        "INSERT INTO account (code, name, type) VALUES ('2000-ap', 'AP Unsettled', 'liability') RETURNING id",
    )
    .fetch_one(pool)
    .await?;

    // ── Skus (bulk via UNNEST) ──
    let sku_codes: Vec<String> = (1..=skus).map(|i| format!("SKU-{i:05}")).collect();
    let sku_names: Vec<String> = (1..=skus).map(|i| format!("Seeded SKU {i}")).collect();
    let sku_ids: Vec<i64> = sqlx::query_scalar(
        "INSERT INTO sku (code, name) SELECT c, n FROM UNNEST($1::text[], $2::text[]) AS t(c, n) RETURNING id",
    )
    .bind(&sku_codes)
    .bind(&sku_names)
    .fetch_all(pool)
    .await?;

    // ── Locations ──
    let loc_codes: Vec<String> = (1..=locations).map(|i| format!("LOC-{i:03}")).collect();
    let loc_names: Vec<String> = (1..=locations).map(|i| format!("Seeded Loc {i}")).collect();
    let loc_ids: Vec<i64> = sqlx::query_scalar(
        "INSERT INTO location (code, name) SELECT c, n FROM UNNEST($1::text[], $2::text[]) AS t(c, n) RETURNING id",
    )
    .bind(&loc_codes)
    .bind(&loc_names)
    .fetch_all(pool)
    .await?;

    // ── Pools: walk the (sku, location) grid in row-major order
    //          until we've laid down `count` pools.
    let mut pool_sku: Vec<i64> = Vec::with_capacity(count);
    let mut pool_loc: Vec<i64> = Vec::with_capacity(count);
    let mut pool_method: Vec<String> = Vec::with_capacity(count);
    for i in 0..count {
        let sku_idx = i % skus;
        let loc_idx = (i / skus) % locations;
        pool_sku.push(sku_ids[sku_idx]);
        pool_loc.push(loc_ids[loc_idx]);
        pool_method.push(method_for_index(i, count, method_mix).to_string());
    }

    let pool_ids: Vec<i64> = sqlx::query_scalar(
        "INSERT INTO pool (sku_id, location_id, method) \
         SELECT s, l, m::pool_method FROM UNNEST($1::bigint[], $2::bigint[], $3::text[]) AS t(s, l, m) \
         RETURNING id",
    )
    .bind(&pool_sku)
    .bind(&pool_loc)
    .bind(&pool_method)
    .fetch_all(pool)
    .await?;

    eprintln!(
        "seed-pools: {} pools, {} skus, {} locations, mix={:?} → seeded",
        pool_ids.len(),
        skus,
        locations,
        method_mix
    );

    Ok(PoolUniverse {
        pool_ids,
        inv_account,
        ap_account,
    })
}

/// Map the i-th pool's method by the requested mix.
///
/// Mixed allocates 60% wac, 30% fifo, 10% std deterministically
/// (no RNG) so two seed runs at the same `count` produce identical
/// method assignments — useful when comparing equivalence runs.
fn method_for_index(i: usize, count: usize, mix: MethodMix) -> &'static str {
    match mix {
        MethodMix::AllWac => "wac",
        MethodMix::AllWacPeriodic => "wac_periodic",
        MethodMix::AllFifo => "fifo",
        MethodMix::Mixed => {
            let pct = (i * 100) / count.max(1);
            if pct < 60 {
                "wac"
            } else if pct < 90 {
                "fifo"
            } else {
                "std"
            }
        }
    }
}
