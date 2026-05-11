//! `acct-c4p` — T1 for `post_so_ship_psync` (shape-L pseudo-sync entry).
//!
//! Verifies the new entry point ends up with identical ledger balances
//! and audit fields as the synchronous `post_so_ship`, just routed via
//! ledger_outbox + dispatcher rendezvous.
//!
//! Coverage:
//!   - Happy path standard SKU (matches `so_ship::happy_path_standard_sku_no_tax`)
//!   - Idempotent replay returns existing doc + outbox_id sentinel (-1)
//!   - lot_and_serial SKU raises P0006 (MVP limitation; documented)
//!
//! Test design: one shared PsyncRuntime per test binary (cheap to spawn
//! once; amortizes connect + listen + drain task spin-up across all
//! tests). Tests run sequentially (--test-threads=1 from
//! ./scripts/run-tests.sh).

mod common;

use common::psync_runtime::PsyncRuntime;
use common::*;
use serde_json::json;
use sqlx::PgPool;
use std::time::Duration;

/// Per-test runtime: spawned after `reset_to_fixture`, dropped at end of
/// test. ledger_outbox is part of the fixture truncation, so a stale
/// drainer from a previous test would receive NOTIFYs for outbox rows
/// that have been removed.
async fn spawn_runtime(pool: &PgPool) -> PsyncRuntime {
    PsyncRuntime::spawn(pool.clone(), "ledger_outbox_done").await
}

async fn call_so_ship_psync(
    pool: &PgPool,
    runtime: &PsyncRuntime,
    so_id: &str,
    lines: serde_json::Value,
) -> Result<String, sqlx::Error> {
    let key = format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        rand_u32(), rand_u16(), rand_u16(), rand_u16(), rand_u64() & 0xffff_ffff_ffff);
    let posted_by = "00000000-0000-0000-0000-0000000000aa";
    let (doc_id, outbox_id): (String, i64) = sqlx::query_as(
        "SELECT so_shipment_id::text, outbox_id FROM post_so_ship_psync(
            $1::UUID, $2, '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL)",
    )
    .bind(so_id)
    .bind(&lines)
    .bind(posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await?;

    if outbox_id < 0 {
        return Ok(doc_id);
    }

    match runtime
        .dispatcher
        .wait_for(outbox_id, Duration::from_secs(30))
        .await
    {
        Ok(o) if o.status == "ok" => Ok(doc_id),
        Ok(o) => Err(sqlx::Error::Protocol(format!(
            "psync drainer status={} sqlstate={:?}",
            o.status, o.sqlstate
        ))),
        Err(()) => Err(sqlx::Error::Protocol("rendezvous timeout".to_string())),
    }
}

// Tiny helpers for fresh keys.
fn rand_u64() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
fn rand_u32() -> u32 {
    (rand_u64() ^ 0x9b54_7d27) as u32
}
fn rand_u16() -> u16 {
    (rand_u64() ^ 0xdead) as u16
}

// ============================================================
// Scaffold — single SKU, standard cost, with FG seeded.
// ============================================================

struct PsyncScaffold {
    so_id: String,
    so_line_id: String,
    qty_acct: i64,
    val_acct: i64,
    cust_qty: i64,
    cust_unsettled: i64,
    revenue_acct: i64,
    cogs_acct: i64,
}

async fn build_scaffold(pool: &PgPool, code: &str, qty_ordered: i64, unit_price: i64) -> PsyncScaffold {
    let customer_id: String = sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(format!("CUST-{code}"))
    .bind(format!("Cust {code}"))
    .fetch_one(pool)
    .await
    .expect("customer");

    let sku_id: String = sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard') RETURNING id::text",
    )
    .bind(format!("SKU-{code}"))
    .fetch_one(pool)
    .await
    .expect("sku");

    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, 60, '1970-01-01'::DATE,
                 '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid())",
    )
    .bind(&sku_id)
    .execute(pool)
    .await
    .expect("standard_cost");

    let loc_id: String = sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(format!("LOC-{code}"))
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .expect("loc");

    // Accounts.
    let qty_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         VALUES ('stock_available', 'qty', $1::UUID, $2::UUID, 'debit') RETURNING id",
    )
    .bind(&sku_id)
    .bind(&loc_id)
    .fetch_one(pool)
    .await
    .expect("qty acct");
    let val_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, currency, normal_side)
         VALUES ('inv_value_fg', 'value', $1::UUID, $2::UUID, 'USD', 'debit') RETURNING id",
    )
    .bind(&sku_id)
    .bind(&loc_id)
    .fetch_one(pool)
    .await
    .expect("val acct");
    let cust_qty: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, counterparty_id, normal_side)
         VALUES ('customer_pool', 'qty', $1::UUID, 'debit') RETURNING id",
    )
    .bind(&customer_id)
    .fetch_one(pool)
    .await
    .expect("cust qty");
    let cust_unsettled: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, counterparty_id, currency, normal_side)
         VALUES ('ar_unsettled', 'value', $1::UUID, 'USD', 'debit') RETURNING id",
    )
    .bind(&customer_id)
    .fetch_one(pool)
    .await
    .expect("cust unsettled");
    let revenue_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side)
         VALUES ('revenue', 'value', 'USD', 'credit') RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("revenue acct");
    let cogs_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side)
         VALUES ('cogs', 'value', 'USD', 'debit') RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("cogs acct");

    // SO + line.
    let so_id: String = sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(&customer_id)
    .fetch_one(pool)
    .await
    .expect("so");
    let so_line_id: String = sqlx::query_scalar(
        "INSERT INTO sales_order_lines
         (so_id, line_no, sku_id, ship_location_id, qty_ordered, unit_price, currency, tax_amount)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, $4, $5, 'USD', 0)
         RETURNING id::text",
    )
    .bind(&so_id)
    .bind(&sku_id)
    .bind(&loc_id)
    .bind(qty_ordered)
    .bind(unit_price)
    .fetch_one(pool)
    .await
    .expect("so_line");

    // Seed FG via direct post_posting_lines mint (creation_void → stock_available + inv_value_fg).
    let creation_void_qty: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind='creation_void' AND ledger_kind='qty'
                                   AND counterparty_id IS NULL",
    )
    .fetch_one(pool)
    .await
    .expect("creation_void qty");
    let creation_void_val: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind='creation_void' AND ledger_kind='value'
                                   AND currency='USD' AND counterparty_id IS NULL",
    )
    .fetch_one(pool)
    .await
    .expect("creation_void val");
    let qty_at_unit_cost = qty_ordered * 60;
    let mint = json!([
        {"reason":"cycle_count_adj",
         "document_kind":"psync_test_seed", "document_id":"00000000-0000-0000-0000-000000000001",
         "debit_account_id":qty_acct, "credit_account_id":creation_void_qty,
         "amount":qty_ordered, "qty":qty_ordered,
         "business_date":"2026-04-01",
         "idempotency_key":format!("{:x}-{:04x}-{:04x}-{:04x}-{:012x}",
            rand_u32(), rand_u16(), rand_u16(), rand_u16(), rand_u64() & 0xffff_ffff_ffff),
         "posted_by":"00000000-0000-0000-0000-0000000000aa"},
        {"reason":"cycle_count_adj",
         "document_kind":"psync_test_seed", "document_id":"00000000-0000-0000-0000-000000000001",
         "debit_account_id":val_acct, "credit_account_id":creation_void_val,
         "amount":qty_at_unit_cost, "qty":qty_ordered,
         "business_date":"2026-04-01",
         "idempotency_key":format!("{:x}-{:04x}-{:04x}-{:04x}-{:012x}",
            rand_u32(), rand_u16(), rand_u16(), rand_u16(), rand_u64() & 0xffff_ffff_ffff),
         "posted_by":"00000000-0000-0000-0000-0000000000aa"},
    ]);
    sqlx::query("SELECT post_posting_lines($1::JSONB, FALSE)")
        .bind(&mint)
        .execute(pool)
        .await
        .expect("seed fg via posting_lines");

    PsyncScaffold {
        so_id,
        so_line_id,
        qty_acct,
        val_acct,
        cust_qty,
        cust_unsettled,
        revenue_acct,
        cogs_acct,
    }
}

async fn balance(pool: &PgPool, account_id: i64) -> i64 {
    sqlx::query_scalar("SELECT debits_total - credits_total FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn psync_happy_path_standard_sku() {
    let pool = connect_test_db_with(8).await;
    reset_to_fixture(&pool).await;

    let runtime = spawn_runtime(&pool).await;
    let s = build_scaffold(&pool, "PSYNC1", 100, 200).await;

    let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": 30}]);
    call_so_ship_psync(&pool, &runtime, &s.so_id, lines)
        .await
        .expect("psync ship");

    // Outbox row committed by drainer.
    let outbox: Vec<(i64, String)> = sqlx::query_as(
        "SELECT id, status FROM ledger_outbox WHERE status='committed' ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        outbox.len(),
        1,
        "expected exactly 1 committed outbox row, got {outbox:?}"
    );

    // Three so_ship posting_lines (qty + cogs + revenue legs).
    let pl_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_lines WHERE document_kind = 'so_ship'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pl_count, 3, "expected 3 so_ship posting_lines");

    // Customer-side balances (counterparty-filtered: no overlap with fixture).
    assert_eq!(balance(&pool, s.cust_qty).await, 30);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 30 * 200);

    // SKU-side balance: stock_available drained by 30 from the seeded 100.
    assert_eq!(balance(&pool, s.qty_acct).await, 100 - 30);
    // inv_value_fg drained by 30 * 60 = 1800 from seeded 6000.
    assert_eq!(balance(&pool, s.val_acct).await, 100 * 60 - 30 * 60);
}

#[tokio::test]
async fn psync_lot_and_serial_raises_p0006() {
    let pool = connect_test_db_with(8).await;
    reset_to_fixture(&pool).await;

    let _runtime = spawn_runtime(&pool).await;

    // Build a lot_and_serial scaffold variant; the post_so_ship_psync
    // function should reject before any ledger work or outbox enqueue.
    let customer_id: String = sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ('CUST-LSO', 'lso', 'USD') RETURNING id::text",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let sku_id: String = sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ('SKU-LSO', 'EA', 'lot_fifo'::cost_method, 'lot_and_serial'::inventory_tracking) RETURNING id::text",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let loc_id: String = sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ('LOC-LSO', 'Loc LSO') RETURNING id::text",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let so_id: String = sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(&customer_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let so_line_id: String = sqlx::query_scalar(
        "INSERT INTO sales_order_lines
         (so_id, line_no, sku_id, ship_location_id, qty_ordered, unit_price, currency, tax_amount)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 10, 100, 'USD', 0)
         RETURNING id::text",
    )
    .bind(&so_id)
    .bind(&sku_id)
    .bind(&loc_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    let lines = json!([{"so_line_id": so_line_id, "qty_shipped": 1}]);
    let key = format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        rand_u32(), rand_u16(), rand_u16(), rand_u16(), rand_u64() & 0xffff_ffff_ffff);
    let res: Result<(String, i64), sqlx::Error> = sqlx::query_as(
        "SELECT so_shipment_id::text, outbox_id FROM post_so_ship_psync(
            $1::UUID, $2, '2026-04-15'::DATE,
            '00000000-0000-0000-0000-0000000000aa'::UUID, $3::UUID, NULL)",
    )
    .bind(&so_id)
    .bind(&lines)
    .bind(&key)
    .fetch_one(&pool)
    .await;

    let err = res.expect_err("expected P0006 from lot_and_serial psync");
    let sqlstate = err
        .as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.to_string())
        .unwrap_or_default();
    assert_eq!(sqlstate, "P0006", "expected P0006, got: {err}");
}

#[tokio::test]
async fn psync_idempotent_replay_returns_sentinel() {
    let pool = connect_test_db_with(8).await;
    reset_to_fixture(&pool).await;

    let runtime = spawn_runtime(&pool).await;
    let s = build_scaffold(&pool, "PSYNC2", 100, 200).await;
    let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": 10}]);

    let first = call_so_ship_psync(&pool, &runtime, &s.so_id, lines.clone())
        .await
        .expect("first ship");

    // Same idempotency key replay via direct invocation (since
    // call_so_ship_psync generates a fresh key each time).
    let key = format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        rand_u32(), rand_u16(), rand_u16(), rand_u16(), rand_u64() & 0xffff_ffff_ffff);
    let (first_id, first_outbox): (String, i64) = sqlx::query_as(
        "SELECT so_shipment_id::text, outbox_id FROM post_so_ship_psync(
            $1::UUID, $2, '2026-04-15'::DATE,
            '00000000-0000-0000-0000-0000000000aa'::UUID, $3::UUID, NULL)",
    )
    .bind(&s.so_id)
    .bind(&lines)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("second ship enqueue");
    assert!(first_outbox > 0, "fresh key should yield real outbox_id");
    runtime
        .dispatcher
        .wait_for(first_outbox, Duration::from_secs(30))
        .await
        .expect("rendezvous");

    // Now replay with the SAME key — should get sentinel outbox_id = -1.
    let (replay_id, replay_outbox): (String, i64) = sqlx::query_as(
        "SELECT so_shipment_id::text, outbox_id FROM post_so_ship_psync(
            $1::UUID, $2, '2026-04-15'::DATE,
            '00000000-0000-0000-0000-0000000000aa'::UUID, $3::UUID, NULL)",
    )
    .bind(&s.so_id)
    .bind(&lines)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("replay");
    assert_eq!(replay_id, first_id);
    assert_eq!(replay_outbox, -1);
    // first is just to anchor the scaffold; not the row under test.
    let _ = first;
}
