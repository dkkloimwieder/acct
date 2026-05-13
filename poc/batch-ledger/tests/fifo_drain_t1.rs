//! acct-0450 — sub 4 FIFO bgworker drain tests.
//!
//! Coverage:
//!
//! - D1: after a batch with depletions, pending_drain_n > 0 in shmem;
//!   cost_layers.qty_remaining lags (still equals original receipt qty).
//! - D2: fifo_force_drain_tick() flushes pending_drain to durable
//!   cost_layers.qty_remaining. Post-drain: pending_drain_n == 0 and
//!   qty_remaining matches the cost-flow expectation.
//! - D3: drain_seq stamping — repeated force-drain on an idle cell is
//!   a no-op (no UPDATE rows, no error).
//! - D4: cumulative ticks_total + rows_total counters advance.
//! - D5: bgworker quiescence — after ~2× drain_interval_ms the
//!   background path also flushes (no force-drain needed). Validates
//!   the bgworker is actually running for FIFO.
//! - D6: overflow fallback — a single batch that touches more than
//!   PENDING_DRAIN_CAP=64 distinct layer slices falls back to inline
//!   UPDATE; cost_layers.qty_remaining is correct WITHOUT calling
//!   fifo_force_drain_tick(). (Validates the overflow safety net.)

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn unique_accounts() -> (i64, i64, i64) {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    // Distinct base from inline (1_000_000_000_000), WAC maximal
    // (1_500_000_000_000), and maximal F (2_100_000_000_000).
    let base = 2_500_000_000_000_i64
        + ((bytes[0] as i64) << 24
            | (bytes[1] as i64) << 16
            | (bytes[2] as i64) << 8
            | (bytes[3] as i64))
            .abs()
            % 100_000_000;
    (base, base + 1, base + 2)
}

async fn pool() -> sqlx::PgPool {
    let p = PgPoolOptions::new()
        .max_connections(8)
        .connect(&db_url())
        .await
        .expect("connect");
    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&p)
        .await
        .expect("ext");
    p
}

async fn seed_accounts(p: &sqlx::PgPool, ids: &[(i64, &str, &str)]) {
    for (id, code, kind) in ids {
        sqlx::query(
            "INSERT INTO accounts (id, code, kind, currency) \
             VALUES ($1::bigint, $2 || '-' || $1::TEXT, $3::account_kind, 'USD') \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(*id)
        .bind(*code)
        .bind(*kind)
        .execute(p)
        .await
        .expect("seed account");
    }
}

async fn cleanup(p: &sqlx::PgPool, ids: &[i64]) {
    let _ = sqlx::query(
        "DELETE FROM cost_layer_depletions \
         WHERE layer_id IN (SELECT id FROM cost_layers WHERE pool_account_id = ANY($1::bigint[]))",
    )
    .bind(ids)
    .execute(p)
    .await;
    let _ = sqlx::query("DELETE FROM cost_layers WHERE pool_account_id = ANY($1::bigint[])")
        .bind(ids)
        .execute(p)
        .await;
    for id in ids {
        let _ = sqlx::query(
            "DELETE FROM posting_lines WHERE debit_account_id = $1::bigint OR credit_account_id = $1::bigint",
        )
        .bind(*id)
        .execute(p)
        .await;
    }
    let _ = sqlx::query("DELETE FROM accounts WHERE id = ANY($1::bigint[])")
        .bind(ids)
        .execute(p)
        .await;
}

async fn run_maximal(
    p: &sqlx::PgPool,
    envelopes: serde_json::Value,
) -> Vec<(i32, String, Option<i64>)> {
    sqlx::query(
        "SELECT envelope_idx, status, posting_line_id \
         FROM post_batch_fifo_maximal_F($1::jsonb)",
    )
    .bind(envelopes)
    .fetch_all(p)
    .await
    .expect("run_maximal")
    .into_iter()
    .map(|r| {
        (
            r.get::<i32, _>("envelope_idx"),
            r.get::<String, _>("status"),
            r.get::<Option<i64>, _>("posting_line_id"),
        )
    })
    .collect()
}

async fn force_drain(p: &sqlx::PgPool) {
    sqlx::query("SELECT fifo_force_drain_tick()")
        .execute(p)
        .await
        .expect("force drain");
}

/// Pause the bgworker by setting drain_interval_ms to the max allowed
/// (3_600_000 = 1h). Use sparingly in tests that need to observe the
/// pre-drain (lag) state without racing the 100ms default cadence.
/// pg_reload_conf wakes the bgworker; we sleep briefly so its `sighup_received`
/// path runs before we proceed.
async fn pause_bgworker(p: &sqlx::PgPool) {
    sqlx::query("ALTER SYSTEM SET ledger.drain_interval_ms = 3600000")
        .execute(p)
        .await
        .expect("alter system pause");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(p)
        .await
        .expect("reload conf pause");
    tokio::time::sleep(Duration::from_millis(150)).await;
}

/// Restore the default drain cadence (100ms) so subsequent tests +
/// the live bgworker behave normally.
async fn resume_bgworker(p: &sqlx::PgPool) {
    sqlx::query("ALTER SYSTEM SET ledger.drain_interval_ms = 100")
        .execute(p)
        .await
        .expect("alter system resume");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(p)
        .await
        .expect("reload conf resume");
}

async fn layer_residuals(p: &sqlx::PgPool, pool_id: i64) -> Vec<i64> {
    sqlx::query(
        "SELECT qty_remaining FROM cost_layers \
         WHERE pool_account_id = $1::bigint ORDER BY receipt_date, id",
    )
    .bind(pool_id)
    .fetch_all(p)
    .await
    .expect("layers")
    .into_iter()
    .map(|r| r.get::<i64, _>("qty_remaining"))
    .collect()
}

async fn pending_count(p: &sqlx::PgPool, cell_idx: i64) -> i64 {
    let row = sqlx::query("SELECT fifo_pending_drain_n($1::bigint) AS n")
        .bind(cell_idx)
        .fetch_one(p)
        .await
        .expect("pending count");
    row.get::<i64, _>("n")
}

/// Returns the cell index for a given pool_account_id by re-implementing
/// the extension's slot_for_fifo + occupied/key probe in SQL... actually
/// no, just walk all 16384 buckets via fifo_pending_drain_n calls. The
/// caller doesn't know the cell idx ahead of time. We could expose a
/// helper pg_extern but for the test it's enough to call
/// fifo_drain_rows_total before+after to confirm the drain ran. Below
/// helper uses a brute approach: find the first cell with non-zero
/// pending. Suitable only when a single pool is active in the bench.
async fn find_dirty_cell(p: &sqlx::PgPool) -> Option<i64> {
    let row = sqlx::query(
        "WITH cells AS (\
             SELECT g::bigint AS idx, fifo_pending_drain_n(g::bigint) AS n \
             FROM generate_series(0, 16383) g \
         ) \
         SELECT idx FROM cells WHERE n > 0 ORDER BY idx LIMIT 1",
    )
    .fetch_optional(p)
    .await
    .expect("find dirty");
    row.map(|r| r.get::<i64, _>("idx"))
}

async fn drain_rows_total(p: &sqlx::PgPool) -> i64 {
    let row = sqlx::query("SELECT fifo_drain_rows_total() AS n")
        .fetch_one(p)
        .await
        .expect("rows total");
    row.get::<i64, _>("n")
}

async fn drain_ticks_total(p: &sqlx::PgPool) -> i64 {
    let row = sqlx::query("SELECT fifo_drain_ticks_total() AS n")
        .fetch_one(p)
        .await
        .expect("ticks total");
    row.get::<i64, _>("n")
}

/// D1 + D2 — pending_drain populated by apply; drain flushes it.
#[tokio::test]
async fn t1_pending_drain_populated_then_force_drain_flushes() {
    let p = pool().await;
    pause_bgworker(&p).await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Batch 1: pure receipts — no pending_drain entries expected.
    let recv = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 1000,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 1,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 50,
            "unit_cost": 1200,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    let _ = run_maximal(&p, recv).await;

    let cell = find_dirty_cell(&p).await;
    // Receipts CAN advance last_seq without staging pending_drain;
    // a force_drain stamps drained_seq up. So pending == 0 here is
    // the expected state; cell may still surface as "dirty" but only
    // because of stale last_seq — that's fine, drain handles it.
    if let Some(c) = cell {
        assert_eq!(
            pending_count(&p, c).await,
            0,
            "T1: receipt-only batch should not stage pending_drain"
        );
    }

    // Force drain to clear any last_seq > drained_seq state from receipts.
    force_drain(&p).await;

    // Batch 2: issue 80 units. This consumes 80 from the 100-unit
    // oldest layer, staging ONE PendingDrain entry.
    let issue = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 80,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    let _ = run_maximal(&p, issue).await;

    // BEFORE drain: cost_layers.qty_remaining should still be (100, 50)
    // because the UPDATE is asynchronous now.
    let layers_pre = layer_residuals(&p, pool_id).await;
    assert_eq!(
        layers_pre,
        vec![100, 50],
        "T1: cost_layers.qty_remaining lags before drain"
    );

    // pending_drain should have 1 entry now.
    let cell = find_dirty_cell(&p).await.expect("dirty cell");
    assert_eq!(
        pending_count(&p, cell).await,
        1,
        "T1: one pending drain entry after 80-unit issue from 100-unit layer"
    );

    // Force drain — flushes pending_drain to cost_layers.
    let ticks_pre = drain_ticks_total(&p).await;
    let rows_pre = drain_rows_total(&p).await;
    force_drain(&p).await;

    // AFTER drain: pending_drain empty, cost_layers updated.
    assert_eq!(
        pending_count(&p, cell).await,
        0,
        "T1: pending_drain emptied by force_drain"
    );
    let layers_post = layer_residuals(&p, pool_id).await;
    assert_eq!(
        layers_post,
        vec![20, 50],
        "T1: cost_layers.qty_remaining reconciled to shmem after drain"
    );
    assert!(
        drain_ticks_total(&p).await > ticks_pre,
        "T1: ticks counter advanced"
    );
    assert!(
        drain_rows_total(&p).await > rows_pre,
        "T1: rows counter advanced"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
    resume_bgworker(&p).await;
}

/// D3 — repeated force_drain on idle cell is a no-op (no error, no
/// duplicate UPDATEs).
#[tokio::test]
async fn t2_idempotent_repeated_drain() {
    let p = pool().await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 50,
            "unit_cost": 800,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 1,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 30,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    let _ = run_maximal(&p, envelopes).await;

    // First drain flushes the issue (1 row).
    let rows_0 = drain_rows_total(&p).await;
    force_drain(&p).await;
    let rows_1 = drain_rows_total(&p).await;
    assert!(rows_1 > rows_0, "T2: first drain advances rows");
    let layers = layer_residuals(&p, pool_id).await;
    assert_eq!(layers, vec![20], "T2: post-drain residual = 50 - 30");

    // Subsequent drains: pending_drain is empty, drained_seq is at
    // last_seq. Should be no-op — rows counter doesn't advance.
    force_drain(&p).await;
    let rows_2 = drain_rows_total(&p).await;
    assert_eq!(
        rows_1, rows_2,
        "T2: idle drain doesn't double-update cost_layers"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// D5 — bgworker quiescence: without force_drain, the background
/// worker eventually flushes within ~2× drain_interval_ms (default
/// 100ms, so 250ms is comfortably enough). Explicitly resumes the
/// bgworker at start in case a previous test left it paused.
#[tokio::test]
async fn t3_bgworker_drains_without_force() {
    let p = pool().await;
    resume_bgworker(&p).await;
    // Wait long enough for the sighup to land and the bgworker to
    // sleep with the resumed interval.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    let envelopes = serde_json::json!([
        {
            "envelope_idx": 0,
            "kind": "fifo_receipt",
            "debit_account_id": pool_id,
            "credit_account_id": ap_id,
            "qty": 100,
            "unit_cost": 500,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        },
        {
            "envelope_idx": 1,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 40,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }
    ]);
    let _ = run_maximal(&p, envelopes).await;

    // Wait for two drain intervals.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // cost_layers should now reflect the issue without any
    // explicit force_drain.
    let layers = layer_residuals(&p, pool_id).await;
    assert_eq!(
        layers,
        vec![60],
        "T3: bgworker autonomously drained (100 - 40 = 60 remaining)"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
}

/// D6 — overflow fallback. Push more than PENDING_DRAIN_CAP=64 distinct
/// layer drains in ONE batch (no chance for bgworker to drain mid-batch
/// since the apply path holds the cell EXCLUSIVE). Verifies the inline
/// UPDATE fallback kicks in: cost_layers.qty_remaining is correct
/// WITHOUT calling force_drain.
#[tokio::test]
async fn t4_overflow_fallback_inline_update() {
    let p = pool().await;
    // Bgworker paused so we can observe the post-batch ring state
    // (which would otherwise be drained by the 100ms tick).
    pause_bgworker(&p).await;
    let (pool_id, ap_id, cogs_id) = unique_accounts();
    seed_accounts(
        &p,
        &[
            (pool_id, "fifo_pool", "inv_value_raw"),
            (ap_id, "fifo_ap", "credit_normal"),
            (cogs_id, "fifo_cogs", "debit_normal"),
        ],
    )
    .await;

    // Build a batch that pushes pending_drain past 64.
    //
    // Strategy: 64 receipts of qty=1 + 64 issues of qty=1 → 64
    // separate (layer_id, qty=1) PendingDrain entries. PENDING_DRAIN_CAP
    // is 64 so the 65th would overflow. To trigger fallback, do
    // RECV 70 + ISSUE 70 single-unit envelopes. But MAX_LAYERS is also
    // 64 — pushing 70 layers would itself overflow the ring. Instead:
    //
    // Receipt of qty=80 → 1 layer with 80 units.
    // Then 80 single-unit issues → 80 PendingDrain entries pointing at
    // the SAME layer_id. 80 > 64 cap, overflow → inline fallback for
    // 16 entries.
    let mut envelopes = vec![serde_json::json!({
        "envelope_idx": 0,
        "kind": "fifo_receipt",
        "debit_account_id": pool_id,
        "credit_account_id": ap_id,
        "qty": 80,
        "unit_cost": 100,
        "idempotency_key": Uuid::new_v4().to_string(),
        "business_date": "2026-05-13",
    })];
    for i in 0..80 {
        envelopes.push(serde_json::json!({
            "envelope_idx": (i + 1) as i32,
            "kind": "fifo_issue",
            "debit_account_id": cogs_id,
            "credit_account_id": pool_id,
            "qty": 1,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-13",
        }));
    }
    let _ = run_maximal(&p, serde_json::Value::Array(envelopes)).await;

    // Cell holds 64 PendingDrain entries (full); the remaining 16 were
    // inline-UPDATEd. So cost_layers.qty_remaining = 80 - 16 = 64 BEFORE
    // drain. After drain, the 64 ring entries flush → 0 remaining.
    let layers_pre = layer_residuals(&p, pool_id).await;
    assert_eq!(
        layers_pre,
        vec![64],
        "T4: overflow inline-UPDATEd 16 units before drain"
    );

    let cell = find_dirty_cell(&p).await.expect("dirty cell");
    assert_eq!(
        pending_count(&p, cell).await,
        64,
        "T4: pending_drain pinned at CAP=64"
    );

    force_drain(&p).await;
    let layers_post = layer_residuals(&p, pool_id).await;
    assert_eq!(
        layers_post,
        vec![0],
        "T4: layer fully drained after force_drain"
    );

    cleanup(&p, &[pool_id, ap_id, cogs_id]).await;
    resume_bgworker(&p).await;
}
