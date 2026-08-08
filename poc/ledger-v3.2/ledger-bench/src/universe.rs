//! Bench pool universe: reference data + pools + the accounting period.
//!
//! Methods cycle fifo → lifo → wac (equal thirds; the recalc/close plane's
//! subject is fifo/lifo, wac rides along as the strict-hot-path control).
//! std/specific are covered by the acceptance/property nets and stay out of
//! the soak universe. All pools share one chart of accounts and one
//! `posting_account_map` shape per (sku, location), mirroring the test
//! fixtures so the same verify identities apply.

use sqlx::PgPool;

pub const INV_ACCT: i64 = 100;
pub const AP_ACCT: i64 = 200;
pub const VAR_ACCT: i64 = 300;
pub const ADJ_ACCT: i64 = 400;

/// The bench accounting period id (seeded as the only period, so in-order
/// close is trivially satisfied).
pub const PERIOD_ID: i64 = 1;

/// TRUNCATE every ledger / reference / staging / recalc table and drop the
/// feed slot, so a run starts from nothing (the slot is recreated by the run
/// BEFORE load starts, guaranteeing no event escapes delivery).
pub async fn reset(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "TRUNCATE TABLE posting_line_dimension, posting_line, \
                        cost_settlement, cost_layer_consumption, pool_settlement, \
                        recalc_queue, ledger_inbox, trx_line, trx, \
                        pool_state, pool, standard_cost, posting_account_map, \
                        sku, location, account, accounting_period \
                        RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "SELECT pg_drop_replication_slot(slot_name) \
           FROM pg_replication_slots WHERE slot_name = 'ledger_feed'",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Seed `n_pools` pools (with their skus, locations, account map rows) plus
/// the shared chart accounts and the open bench period covering
/// [today − period_back_days, today].
pub async fn seed(pool: &PgPool, n_pools: i64, period_back_days: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO sku (id, code, name) \
         SELECT g, 'SKU-' || g, 'Bench SKU ' || g FROM generate_series(1, $1) g \
         ON CONFLICT DO NOTHING",
    )
    .bind(n_pools)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO location (id, code, name) \
         SELECT g, 'LOC-' || g, 'Bench location ' || g FROM generate_series(1, $1) g \
         ON CONFLICT DO NOTHING",
    )
    .bind(n_pools)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO account (id, code, name, type) VALUES \
           ($1, 'INV', 'INV', 'asset'), \
           ($2, 'AP', 'AP', 'liability'), \
           ($3, 'VARIANCE', 'VARIANCE', 'expense'), \
           ($4, 'ADJ', 'ADJ', 'expense') \
         ON CONFLICT DO NOTHING",
    )
    .bind(INV_ACCT)
    .bind(AP_ACCT)
    .bind(VAR_ACCT)
    .bind(ADJ_ACCT)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO posting_account_map \
           (sku_id, location_id, receipt_debit, receipt_credit, transfer_debit, transfer_credit, \
            build_debit, build_credit, scrap_debit, scrap_credit, adjustment_debit, \
            adjustment_credit, revaluation_debit, revaluation_credit, variance_acct) \
         SELECT g, g, $2, $3, $2, $3, $2, $3, $2, $3, $2, $3, $2, $3, $4 \
           FROM generate_series(1, $1) g \
         ON CONFLICT DO NOTHING",
    )
    .bind(n_pools)
    .bind(INV_ACCT)
    .bind(AP_ACCT)
    .bind(VAR_ACCT)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO pool (id, sku_id, location_id, identity_key, method, provisional_basis) \
         SELECT g, g, g, 0, \
                (ARRAY['fifo','lifo','wac'])[(g - 1) % 3 + 1]::pool_method, \
                'running_avg'::pool_provisional_basis \
           FROM generate_series(1, $1) g \
         ON CONFLICT DO NOTHING",
    )
    .bind(n_pools)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO accounting_period (id, start_date, end_date, state) \
         VALUES ($1, ((now() AT TIME ZONE 'UTC')::date - $2::int), \
                 (now() AT TIME ZONE 'UTC')::date, 'open') \
         ON CONFLICT DO NOTHING",
    )
    .bind(PERIOD_ID)
    .bind(period_back_days as i32)
    .execute(pool)
    .await?;
    Ok(())
}
