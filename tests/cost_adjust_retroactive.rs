//! `acct-duc` / `acct-og1.2` — full matrix for cost_adjust_retroactive.
//!
//! Phase 1 Epic E. Operator-queued retroactive cost overrides that
//! flush at the target period's close. See migration 0032.

mod common;

use common::*;

// ---------- helpers ----------

async fn period_id(pool: &sqlx::PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
}

async fn sku_id(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("sku {code}: {e}"))
}

async fn loc_id(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("loc {code}: {e}"))
}

async fn balance(pool: &sqlx::PgPool, id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("balance")
}

async fn insert_sku(pool: &sqlx::PgPool, code: &str, method: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method) VALUES ($1, 'EA', $2::cost_method)
         RETURNING id::text",
    )
    .bind(code)
    .bind(method)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert sku {code}: {e}"))
}

/// Open stock_available + inv_value_fg accounts for an inserted SKU at MAIN.
async fn open_main_accounts_fg(pool: &sqlx::PgPool, sku_id: &str) {
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_available', 'qty', $1::UUID, l.id, 'debit'
           FROM locations l WHERE l.code = 'MAIN'",
    ).bind(sku_id).execute(pool).await.expect("open stock_available");
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT 'inv_value_fg', 'value', 'USD', 'debit', $1::UUID, l.id
           FROM locations l WHERE l.code = 'MAIN'",
    ).bind(sku_id).execute(pool).await.expect("open inv_value_fg");
}

#[allow(clippy::too_many_arguments)]
async fn queue_retro(
    pool: &sqlx::PgPool,
    target_period: i64,
    sku: &str,
    loc: &str,
    inv_class: &str,
    target_avg: i64,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_cost_adjustment_retroactive(
            $1::BIGINT, $2::UUID, $3::UUID, 'USD', $4,
            $5::BIGINT, $6::DATE, $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(target_period)
    .bind(sku)
    .bind(loc)
    .bind(inv_class)
    .bind(target_avg)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

/// Receive `qty` at `unit_cost` for the SKU + location in class fg.
async fn receive_fg(
    pool: &sqlx::PgPool,
    sku: &str,
    loc: &str,
    qty: i64,
    unit_cost: i64,
    business_date: &str,
) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, 'USD', 'fg',
            $5::DATE, $6::UUID, $7::UUID, NULL
         )::text",
    )
    .bind(sku).bind(loc).bind(qty).bind(unit_cost).bind(business_date)
    .bind(&posted_by).bind(&key)
    .fetch_one(pool).await.expect("receive_fg");
}

/// Deplete `qty` from the SKU + location in class fg using the running
/// avg (no asserted unit_cost).
async fn deplete_fg(pool: &sqlx::PgPool, sku: &str, loc: &str, qty: i64, business_date: &str) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, NULL::BIGINT, 'USD', 'fg',
            $4::DATE, $5::UUID, $6::UUID, NULL
         )::text",
    )
    .bind(sku).bind(loc).bind(-qty).bind(business_date)
    .bind(&posted_by).bind(&key)
    .fetch_one(pool).await.expect("deplete_fg");
}

async fn close(pool: &sqlx::PgPool, pid: i64) -> serde_json::Value {
    let actor = fresh_uuid(pool).await;
    sqlx::query_scalar("SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid)
        .bind(&actor)
        .fetch_one(pool)
        .await
        .expect("close")
}

// ============================================================
// CORE LIFECYCLE
// ============================================================

#[tokio::test]
async fn queue_then_close_posts_variance_per_depletion() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let val = account_id_for_selector(&pool, "inv_value_fg",
        Some("SKU-WAC"), Some("MAIN"), Some("USD"), None).await;
    let var = account_id_by_kind_currency(&pool, "variance_cost_adjust_retro", Some("USD")).await;

    receive_fg(&pool, &sku, &loc, 100, 6, "2026-04-05").await;
    deplete_fg(&pool, &sku, &loc, 50, "2026-04-15").await;
    // Pool: 50u, value 50*6=300 (received 600, depleted 300 at $6/u).
    assert_eq!(balance(&pool, val).await, 300);

    let pid = period_id(&pool, "2026-04").await;
    let key = fresh_uuid(&pool).await;
    let qid = queue_retro(&pool, pid, &sku, &loc, "fg", 7, "2026-04-15", &key)
        .await.expect("queue");

    let summary = close(&pool, pid).await;
    assert_eq!(summary["hook_results"]["cost_adjust_retroactive"].as_i64(), Some(1));

    // Variance: 50 × (7 - 6) = 50. Pool credited additional 50 → 300 - 50 = 250.
    assert_eq!(balance(&pool, val).await, 250);
    assert_eq!(balance(&pool, var).await, 0, "variance_cost_adjust_retro nets to zero");

    let total_var: i64 = sqlx::query_scalar(
        "SELECT total_variance FROM inventory_cost_adjustments_retroactive WHERE id = $1::UUID",
    ).bind(&qid).fetch_one(&pool).await.expect("total");
    assert_eq!(total_var, 50);
}

#[tokio::test]
async fn multiple_depletions_in_period_each_re_costed() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let val = account_id_for_selector(&pool, "inv_value_fg",
        Some("SKU-WAC"), Some("MAIN"), Some("USD"), None).await;

    receive_fg(&pool, &sku, &loc, 100, 5, "2026-04-05").await;
    deplete_fg(&pool, &sku, &loc, 30, "2026-04-10").await;  // provisional unit cost = 5
    deplete_fg(&pool, &sku, &loc, 20, "2026-04-15").await;  // still avg=5
    // Pool: 50u value 250.

    let pid = period_id(&pool, "2026-04").await;
    let key = fresh_uuid(&pool).await;
    queue_retro(&pool, pid, &sku, &loc, "fg", 7, "2026-04-15", &key)
        .await.expect("queue");

    close(&pool, pid).await;

    // Variances: 30×(7-5)=60 + 20×(7-5)=40 = 100. Pool 250 - 100 = 150.
    assert_eq!(balance(&pool, val).await, 150);

    let (total_var, dep_count): (i64, i64) = sqlx::query_as(
        "SELECT total_variance, finalized_count
           FROM inventory_cost_adjustments_retroactive WHERE target_period_id = $1",
    ).bind(pid).fetch_one(&pool).await.expect("queue");
    assert_eq!(total_var, 100);
    assert_eq!(dep_count, 2, "two depletions processed");
}

#[tokio::test]
async fn zero_variance_no_op_records_finalized() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let var = account_id_by_kind_currency(&pool, "variance_cost_adjust_retro", Some("USD")).await;

    receive_fg(&pool, &sku, &loc, 100, 5, "2026-04-05").await;
    deplete_fg(&pool, &sku, &loc, 30, "2026-04-10").await;

    let pid = period_id(&pool, "2026-04").await;
    let key = fresh_uuid(&pool).await;
    let qid = queue_retro(&pool, pid, &sku, &loc, "fg", 5, "2026-04-15", &key)
        .await.expect("queue");

    close(&pool, pid).await;

    // No variance posted (target equals provisional).
    let xfer_count: i64 = sqlx::query_scalar(
        "SELECT count(*)::BIGINT FROM transfers
          WHERE document_kind = 'cost_adjust_retroactive_close' AND document_id = $1::UUID",
    ).bind(&qid).fetch_one(&pool).await.expect("count");
    assert_eq!(xfer_count, 0);
    assert_eq!(balance(&pool, var).await, 0);

    let (finalized_set, total_var): (bool, i64) = sqlx::query_as(
        "SELECT (finalized_at IS NOT NULL), total_variance
           FROM inventory_cost_adjustments_retroactive WHERE id = $1::UUID",
    ).bind(&qid).fetch_one(&pool).await.expect("queue");
    assert!(finalized_set);
    assert_eq!(total_var, 0);
}

#[tokio::test]
async fn queue_finalized_state_recorded() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    receive_fg(&pool, &sku, &loc, 100, 5, "2026-04-05").await;
    deplete_fg(&pool, &sku, &loc, 30, "2026-04-15").await;

    let pid = period_id(&pool, "2026-04").await;
    let key = fresh_uuid(&pool).await;
    let qid = queue_retro(&pool, pid, &sku, &loc, "fg", 7, "2026-04-15", &key)
        .await.expect("queue");

    let (finalized_at_pre, _): (Option<String>, i64) =
        sqlx::query_as(
            "SELECT finalized_at::text, COALESCE(finalized_count, 0)
               FROM inventory_cost_adjustments_retroactive WHERE id = $1::UUID",
        )
        .bind(&qid).fetch_one(&pool).await.expect("pre");
    assert!(finalized_at_pre.is_none());

    close(&pool, pid).await;

    let (finalized_at_post, count, total): (Option<String>, i64, i64) =
        sqlx::query_as(
            "SELECT finalized_at::text, finalized_count, total_variance
               FROM inventory_cost_adjustments_retroactive WHERE id = $1::UUID",
        )
        .bind(&qid).fetch_one(&pool).await.expect("post");
    assert!(finalized_at_post.is_some());
    assert_eq!(count, 1);
    assert_eq!(total, 60);
}

// ============================================================
// VALIDATION
// ============================================================

#[tokio::test]
async fn target_period_closed_raises_p0021() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    close(&pool, pid).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("P0021", || async {
        queue_retro(&pool, pid, &sku, &loc, "fg", 7, "2026-04-15", &key)
            .await.map(|_| ())
    }).await;
}

#[tokio::test]
async fn business_date_outside_target_period_raises() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid_apr = period_id(&pool, "2026-04").await;
    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    // 2026-05-15 is in the May period, not April.
    expect_sqlstate("P0004", || async {
        queue_retro(&pool, pid_apr, &sku, &loc, "fg", 7, "2026-05-15", &key)
            .await.map(|_| ())
    }).await;
}

#[tokio::test]
async fn negative_target_avg_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("23514", || async {
        queue_retro(&pool, pid, &sku, &loc, "fg", -1, "2026-04-15", &key)
            .await.map(|_| ())
    }).await;
}

#[tokio::test]
async fn wip_class_raises_p0006_referencing_epic_p7v() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("P0006", || async {
        queue_retro(&pool, pid, &sku, &loc, "wip", 7, "2026-04-15", &key)
            .await.map(|_| ())
    }).await;
}

#[tokio::test]
async fn unknown_sku_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;
    let bogus_sku = "00000000-0000-0000-0000-000000000000";

    expect_sqlstate("P0010", || async {
        queue_retro(&pool, pid, bogus_sku, &loc, "fg", 7, "2026-04-15", &key)
            .await.map(|_| ())
    }).await;
}

#[tokio::test]
async fn missing_pool_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // SKU-WAC has no inv_value_fg pool opened on the ALT location.
    let pid = period_id(&pool, "2026-04").await;
    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "ALT").await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("P0010", || async {
        queue_retro(&pool, pid, &sku, &loc, "fg", 7, "2026-04-15", &key)
            .await.map(|_| ())
    }).await;
}

// ============================================================
// IDEMPOTENCY
// ============================================================

#[tokio::test]
async fn replay_returns_same_id_no_double_queue() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    let id1 = queue_retro(&pool, pid, &sku, &loc, "fg", 7, "2026-04-15", &key)
        .await.expect("first");
    let id2 = queue_retro(&pool, pid, &sku, &loc, "fg", 7, "2026-04-15", &key)
        .await.expect("replay");
    assert_eq!(id1, id2, "same idempotency key returns same id");

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*)::BIGINT FROM inventory_cost_adjustments_retroactive
          WHERE idempotency_key = $1::UUID",
    ).bind(&key).fetch_one(&pool).await.expect("count");
    assert_eq!(count, 1, "no double queue");
}

// ============================================================
// METHOD-AGNOSTIC
// ============================================================

#[tokio::test]
async fn cost_adjust_retroactive_works_on_wac_perpetual() {
    // Same as queue_then_close_posts_variance_per_depletion — SKU-WAC is
    // wac_perpetual; included here to make the matrix explicit.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    receive_fg(&pool, &sku, &loc, 100, 5, "2026-04-05").await;
    deplete_fg(&pool, &sku, &loc, 40, "2026-04-15").await;

    let pid = period_id(&pool, "2026-04").await;
    let key = fresh_uuid(&pool).await;
    queue_retro(&pool, pid, &sku, &loc, "fg", 8, "2026-04-15", &key)
        .await.expect("queue");
    let summary = close(&pool, pid).await;
    assert_eq!(summary["hook_results"]["cost_adjust_retroactive"].as_i64(), Some(1));
}

#[tokio::test]
async fn cost_adjust_retroactive_works_on_standard_sku() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;  // standard, std cost = 100
    let loc = loc_id(&pool, "MAIN").await;
    let val = account_id_for_selector(&pool, "inv_value_fg",
        Some("SKU-A"), Some("MAIN"), Some("USD"), None).await;
    let var = account_id_by_kind_currency(&pool, "variance_cost_adjust_retro", Some("USD")).await;

    // post_inventory_adjustment for standard SKUs uses resolve_standard_cost_at;
    // do NOT pass unit_cost.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 100::BIGINT, NULL::BIGINT, 'USD', 'fg',
            '2026-04-05'::DATE, $3::UUID, $4::UUID, NULL
         )::text",
    ).bind(&sku).bind(&loc).bind(&posted_by).bind(&key)
     .fetch_one(&pool).await.expect("rcv std");

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, -30::BIGINT, NULL::BIGINT, 'USD', 'fg',
            '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL
         )::text",
    ).bind(&sku).bind(&loc).bind(&posted_by).bind(&key)
     .fetch_one(&pool).await.expect("dep std");

    // Pool: 70u, value 70*100=7000.
    assert_eq!(balance(&pool, val).await, 7000);

    // Operator overrides: target=120. Variance per depletion = 30×(120-100)=600.
    let pid = period_id(&pool, "2026-04").await;
    let qkey = fresh_uuid(&pool).await;
    queue_retro(&pool, pid, &sku, &loc, "fg", 120, "2026-04-15", &qkey)
        .await.expect("queue");

    close(&pool, pid).await;

    assert_eq!(balance(&pool, val).await, 6400, "7000 - 600 (additional credit from variance)");
    assert_eq!(balance(&pool, var).await, 0, "variance nets to zero");
}

#[tokio::test]
async fn cost_adjust_retroactive_works_on_wac_periodic_double_correction() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_sku(&pool, "SKU-WACP-DC", "wac_periodic").await;
    open_main_accounts_fg(&pool, &sku).await;
    let loc = loc_id(&pool, "MAIN").await;

    // Build state forcing wac_periodic to post a variance: receive
    // 100@5, deplete 30 mid-period (provisional avg=5), then receive
    // 100@9. wac_periodic close avg = (500+900)/(100+100)=7. wac_periodic
    // variance for the depletion = 30×(7-5)=60.
    receive_fg(&pool, &sku, &loc, 100, 5, "2026-04-05").await;
    deplete_fg(&pool, &sku, &loc, 30, "2026-04-10").await;
    receive_fg(&pool, &sku, &loc, 100, 9, "2026-04-15").await;

    // Operator additionally queues cost_adjust_retroactive at target=10.
    // Variance walks the original depletion (qty=30, amount=150 →
    // provisional_unit=5): 30×(10-5)=150.
    let pid = period_id(&pool, "2026-04").await;
    let key = fresh_uuid(&pool).await;
    queue_retro(&pool, pid, &sku, &loc, "fg", 10, "2026-04-15", &key)
        .await.expect("queue");

    let summary = close(&pool, pid).await;
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(1),
               "wac_periodic finalized 1");
    assert_eq!(summary["hook_results"]["cost_adjust_retroactive"].as_i64(), Some(1),
               "cost_adjust_retroactive finalized 1");

    // Both variance accumulators received activity.
    let var_wp = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    let var_cr = account_id_by_kind_currency(&pool, "variance_cost_adjust_retro", Some("USD")).await;
    let wp_activity: i64 = sqlx::query_scalar(
        "SELECT (debits_total + credits_total)::BIGINT FROM accounts WHERE id = $1",
    ).bind(var_wp).fetch_one(&pool).await.expect("wp");
    let cr_activity: i64 = sqlx::query_scalar(
        "SELECT (debits_total + credits_total)::BIGINT FROM accounts WHERE id = $1",
    ).bind(var_cr).fetch_one(&pool).await.expect("cr");
    assert!(wp_activity > 0, "variance_wac_period saw activity");
    assert!(cr_activity > 0, "variance_cost_adjust_retro saw activity");
    assert_eq!(balance(&pool, var_wp).await, 0, "variance_wac_period nets to 0");
    assert_eq!(balance(&pool, var_cr).await, 0, "variance_cost_adjust_retro nets to 0");

    // Both document_kinds present in transfers.
    let kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT document_kind::text FROM transfers
          WHERE document_kind IN ('wac_periodic_close', 'cost_adjust_retroactive_close')
          ORDER BY document_kind::text",
    ).fetch_all(&pool).await.expect("kinds");
    assert_eq!(kinds, vec!["cost_adjust_retroactive_close", "wac_periodic_close"]);
}

// ============================================================
// VARIANCE ROUTING
// ============================================================

#[tokio::test]
async fn variance_cost_adjust_retro_nets_to_zero_per_close() {
    // Already asserted in queue_then_close_posts_variance_per_depletion;
    // included for explicit matrix coverage.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let var = account_id_by_kind_currency(&pool, "variance_cost_adjust_retro", Some("USD")).await;

    receive_fg(&pool, &sku, &loc, 100, 5, "2026-04-05").await;
    deplete_fg(&pool, &sku, &loc, 30, "2026-04-10").await;
    deplete_fg(&pool, &sku, &loc, 20, "2026-04-15").await;

    let pid = period_id(&pool, "2026-04").await;
    let key = fresh_uuid(&pool).await;
    queue_retro(&pool, pid, &sku, &loc, "fg", 8, "2026-04-15", &key)
        .await.expect("queue");
    close(&pool, pid).await;

    assert_eq!(balance(&pool, var).await, 0);
    let (debits, credits): (i64, i64) = sqlx::query_as(
        "SELECT debits_total, credits_total FROM accounts WHERE id = $1",
    ).bind(var).fetch_one(&pool).await.expect("var");
    assert!(debits > 0, "variance saw activity");
    assert_eq!(debits, credits, "balanced");
}

#[tokio::test]
async fn variance_transfer_reason_and_doc_kind() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    receive_fg(&pool, &sku, &loc, 100, 5, "2026-04-05").await;
    deplete_fg(&pool, &sku, &loc, 30, "2026-04-15").await;

    let pid = period_id(&pool, "2026-04").await;
    let key = fresh_uuid(&pool).await;
    let qid = queue_retro(&pool, pid, &sku, &loc, "fg", 7, "2026-04-15", &key)
        .await.expect("queue");
    close(&pool, pid).await;

    // Two variance transfers per processed depletion (2-transfer routing).
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT reason::text, document_kind::text FROM transfers
          WHERE document_id = $1::UUID
          ORDER BY id",
    ).bind(&qid).fetch_all(&pool).await.expect("rows");
    assert_eq!(rows.len(), 2);
    for (reason, kind) in rows {
        assert_eq!(reason, "cost_restate");
        assert_eq!(kind, "cost_adjust_retroactive_close");
    }
}

// ============================================================
// REGRESSION
// ============================================================

#[tokio::test]
async fn periods_without_queued_adjustments_close_normally() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid = period_id(&pool, "2026-04").await;
    let summary = close(&pool, pid).await;
    assert_eq!(summary["hook_results"]["cost_adjust_retroactive"].as_i64(), Some(0));
    assert_eq!(summary["closed_at"].is_string(), true);
}

#[tokio::test]
async fn closing_unrelated_period_doesnt_process_other_periods_queue() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pid_apr = period_id(&pool, "2026-04").await;
    let pid_may = period_id(&pool, "2026-05").await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    receive_fg(&pool, &sku, &loc, 100, 5, "2026-04-05").await;
    deplete_fg(&pool, &sku, &loc, 30, "2026-04-15").await;

    // Queue against April.
    let key = fresh_uuid(&pool).await;
    let qid = queue_retro(&pool, pid_apr, &sku, &loc, "fg", 7, "2026-04-15", &key)
        .await.expect("queue apr");

    // Close May (different period).
    let summary = close(&pool, pid_may).await;
    assert_eq!(summary["hook_results"]["cost_adjust_retroactive"].as_i64(), Some(0));

    // April queue row still un-finalized.
    let finalized_at: Option<String> = sqlx::query_scalar(
        "SELECT finalized_at::text FROM inventory_cost_adjustments_retroactive WHERE id = $1::UUID",
    ).bind(&qid).fetch_one(&pool).await.expect("queue row");
    assert!(finalized_at.is_none(), "April queue not touched by May close");
}
