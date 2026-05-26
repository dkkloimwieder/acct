//! Pool-universe seeder (design-v3.1 §10, P4 acct-2ttr.8).
//!
//! Idempotent on the universe: if the requested pool count already exists it
//! loads and returns the existing ids. Otherwise it bulk-inserts L locations,
//! M skus, and N pools across the (sku, location) grid with a configurable
//! method assignment.
//!
//! v3.1 differences from ledger-v3: every reference/pool row carries an
//! explicit BIGINT id (no serial/identity on these tables), so the seeder
//! assigns ids deterministically — accounts inv=1000/ap=2000, skus 1..M,
//! locations 1..L, pools 1..N. Pools carry `provisional_basis = running_avg`
//! (FIFO/LIFO depletions then read the running aggregate; no standard_cost
//! rows needed). Deep-pool layer seeding lives in [`crate::seed`].

use sqlx::PgPool;

use crate::cli::MethodMix;

/// Inventory + AP account ids (fixed; the seeded chart). Workload generators
/// reference these in their debit/credit fields.
pub const INV_ACCOUNT: i64 = 1000;
pub const AP_ACCOUNT: i64 = 2000;

/// Drop all ledger data so a fresh universe can be seeded. Used by the
/// equivalence harness and the `run --method-mix` reseed path. Table list is
/// the v3.1 schema (no posting_lines_provisional — that's a v3-only table).
pub async fn reset_ledger_tables(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "TRUNCATE TABLE posting_line_dimension, posting_line, \
                       trx_line, trx, pool_state, pool_lock, pool, \
                       standard_cost, sku, location, account, accounting_period \
                       RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PoolUniverse {
    pub pool_ids: Vec<i64>,
    pub inv_account: i64,
    pub ap_account: i64,
}

/// Seed (or load) a pool universe.
///
/// `count` is the number of pools; `skus` × `locations` must be >= count
/// because the `pool (sku_id, location_id, identity_key)` UNIQUE constraint
/// pins one pool per (sku, location) at identity_key=0.
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

    // Idempotency: reuse only when the existing count matches the request.
    // A mismatch is a configuration error — scenario sizing (S6 stripe_size =
    // universe / callers, S5/S7 Zipf head) depends on the universe size, so
    // silently reusing a differently-sized fixture yields meaningless numbers.
    let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM pool")
        .fetch_one(pool)
        .await?;
    if existing > 0 {
        if (existing as usize) != count {
            return Err(sqlx::Error::Protocol(format!(
                "seed-pools: existing pool count ({existing}) != requested ({count}). \
                 Reset the fixture (run with --method-mix to TRUNCATE+reseed, or \
                 re-run with --count {existing} to reuse)."
            )));
        }
        let pool_ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM pool ORDER BY id")
            .fetch_all(pool)
            .await?;
        eprintln!("seed-pools: existing {} pools match request; reusing", pool_ids.len());
        return Ok(PoolUniverse {
            pool_ids,
            inv_account: INV_ACCOUNT,
            ap_account: AP_ACCOUNT,
        });
    }

    // ── Accounts (fixed ids) ──
    sqlx::query(
        "INSERT INTO account (id, code, name, type) VALUES \
            ($1, '1000-inv', 'Inventory', 'asset'::account_type), \
            ($2, '2000-ap',  'AP Unsettled', 'liability'::account_type)",
    )
    .bind(INV_ACCOUNT)
    .bind(AP_ACCOUNT)
    .execute(pool)
    .await?;

    // ── Skus (ids 1..=skus, bulk via UNNEST) ──
    let sku_ids: Vec<i64> = (1..=skus as i64).collect();
    let sku_codes: Vec<String> = (1..=skus).map(|i| format!("SKU-{i:05}")).collect();
    let sku_names: Vec<String> = (1..=skus).map(|i| format!("Seeded SKU {i}")).collect();
    sqlx::query(
        "INSERT INTO sku (id, code, name) \
         SELECT i, c, n FROM UNNEST($1::bigint[], $2::text[], $3::text[]) AS t(i, c, n)",
    )
    .bind(&sku_ids)
    .bind(&sku_codes)
    .bind(&sku_names)
    .execute(pool)
    .await?;

    // ── Locations (ids 1..=locations) ──
    let loc_ids: Vec<i64> = (1..=locations as i64).collect();
    let loc_codes: Vec<String> = (1..=locations).map(|i| format!("LOC-{i:03}")).collect();
    let loc_names: Vec<String> = (1..=locations).map(|i| format!("Seeded Loc {i}")).collect();
    sqlx::query(
        "INSERT INTO location (id, code, name) \
         SELECT i, c, n FROM UNNEST($1::bigint[], $2::text[], $3::text[]) AS t(i, c, n)",
    )
    .bind(&loc_ids)
    .bind(&loc_codes)
    .bind(&loc_names)
    .execute(pool)
    .await?;

    // ── Pools: ids 1..=count, walking the (sku, location) grid row-major ──
    let pool_ids_arg: Vec<i64> = (1..=count as i64).collect();
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

    sqlx::query(
        "INSERT INTO pool (id, sku_id, location_id, method, provisional_basis) \
         SELECT i, s, l, m::pool_method, 'running_avg'::pool_provisional_basis \
           FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::text[]) AS t(i, s, l, m)",
    )
    .bind(&pool_ids_arg)
    .bind(&pool_sku)
    .bind(&pool_loc)
    .bind(&pool_method)
    .execute(pool)
    .await?;

    eprintln!(
        "seed-pools: {} pools, {} skus, {} locations, mix={:?} → seeded",
        count, skus, locations, method_mix
    );

    Ok(PoolUniverse {
        pool_ids: pool_ids_arg,
        inv_account: INV_ACCOUNT,
        ap_account: AP_ACCOUNT,
    })
}

/// Load an already-seeded universe (used by the `run` drivers).
pub async fn load(pool: &PgPool) -> Result<PoolUniverse, String> {
    let pool_ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM pool ORDER BY id")
        .fetch_all(pool)
        .await
        .map_err(|e| format!("load pool universe: {e}"))?;
    if pool_ids.is_empty() {
        return Err("pool universe is empty — run `seed-pools` first".into());
    }
    Ok(PoolUniverse {
        pool_ids,
        inv_account: INV_ACCOUNT,
        ap_account: AP_ACCOUNT,
    })
}

/// Map the i-th pool's method by the requested mix. `Mixed` allocates 50% fifo,
/// 30% wac, 20% std deterministically (no RNG) so two seed runs at the same
/// `count` produce identical method assignments — needed for equivalence.
fn method_for_index(i: usize, count: usize, mix: MethodMix) -> &'static str {
    match mix {
        MethodMix::AllFifo => "fifo",
        MethodMix::AllLifo => "lifo",
        MethodMix::AllWac => "wac",
        MethodMix::AllStd => "std",
        MethodMix::AllSpecific => "specific",
        MethodMix::Mixed => {
            let pct = (i * 100) / count.max(1);
            if pct < 50 {
                "fifo"
            } else if pct < 80 {
                "wac"
            } else {
                "std"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_method_distribution_is_deterministic() {
        let count = 100;
        let methods: Vec<&str> = (0..count)
            .map(|i| method_for_index(i, count, MethodMix::Mixed))
            .collect();
        assert_eq!(methods.iter().filter(|m| **m == "fifo").count(), 50);
        assert_eq!(methods.iter().filter(|m| **m == "wac").count(), 30);
        assert_eq!(methods.iter().filter(|m| **m == "std").count(), 20);
    }

    #[test]
    fn all_methods_map_to_their_enum_text() {
        assert_eq!(method_for_index(0, 1, MethodMix::AllFifo), "fifo");
        assert_eq!(method_for_index(0, 1, MethodMix::AllLifo), "lifo");
        assert_eq!(method_for_index(0, 1, MethodMix::AllWac), "wac");
        assert_eq!(method_for_index(0, 1, MethodMix::AllStd), "std");
        assert_eq!(method_for_index(0, 1, MethodMix::AllSpecific), "specific");
    }
}
