//! Expiry is a single SQL UPDATE — no per-row worker, no re-entry.
//! Cron schedules the same statement every 30s (O1, acct-93b.21).
//! This test asserts the statement's correctness: every active row
//! whose `expires_at` is in the past flips to 'expired' in one go,
//! and rows that don't match the predicate are untouched.
//!
//! Wall-clock budgets for this UPDATE are explicitly out of scope —
//! see O1 notes; perf characterization waits on Part IV §14.1
//! baseline.

mod common;

use common::*;

#[tokio::test]
async fn expire_update_flips_all_overdue_active_rows_at_once() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let so_id = fresh_sales_order(&pool).await;

    // 1000 active rows with expires_at in the past. Bypasses the §3.3
    // promisable check — direct INSERT — because this test is about
    // the expiry UPDATE's behavior, not reservation creation.
    let n_seed: u64 = sqlx::query(
        "INSERT INTO inventory_reservations
           (sku_id, location_id, qty, so_id, so_line_id, expires_at, status)
         SELECT (SELECT id FROM skus      WHERE code = 'SKU-A'),
                (SELECT id FROM locations WHERE code = 'MAIN'),
                1, $1::UUID, gen_random_uuid(),
                clock_timestamp() - INTERVAL '1 minute',
                'active'
           FROM generate_series(1, 1000)",
    )
    .bind(&so_id)
    .execute(&pool)
    .await
    .expect("seed 1000 expired-active rows")
    .rows_affected();
    assert_eq!(n_seed, 1000);

    // Controls that MUST be untouched.
    sqlx::query(
        "INSERT INTO inventory_reservations
           (sku_id, location_id, qty, so_id, so_line_id, expires_at, status)
         SELECT (SELECT id FROM skus      WHERE code = 'SKU-A'),
                (SELECT id FROM locations WHERE code = 'MAIN'),
                1, $1::UUID, gen_random_uuid(),
                clock_timestamp() + INTERVAL '1 hour',
                'active'
           FROM generate_series(1, 5)",
    )
    .bind(&so_id)
    .execute(&pool)
    .await
    .expect("seed 5 future-active rows");

    sqlx::query(
        "INSERT INTO inventory_reservations
           (sku_id, location_id, qty, so_id, so_line_id, expires_at, status)
         SELECT (SELECT id FROM skus      WHERE code = 'SKU-A'),
                (SELECT id FROM locations WHERE code = 'MAIN'),
                1, $1::UUID, gen_random_uuid(),
                clock_timestamp() - INTERVAL '1 minute',
                'allocated'
           FROM generate_series(1, 3)",
    )
    .bind(&so_id)
    .execute(&pool)
    .await
    .expect("seed 3 allocated overdue rows");

    // The §3.3 / O1 expire statement, verbatim.
    let n_expired: u64 = sqlx::query(
        "UPDATE inventory_reservations
            SET status = 'expired',
                resolved_at = clock_timestamp()
          WHERE status = 'active'
            AND expires_at < clock_timestamp()",
    )
    .execute(&pool)
    .await
    .expect("expire UPDATE")
    .rows_affected();
    assert_eq!(n_expired, 1000, "exactly 1000 rows should be flipped");

    let counts: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status::text, COUNT(*)::BIGINT
           FROM inventory_reservations
          GROUP BY status
          ORDER BY status",
    )
    .fetch_all(&pool)
    .await
    .expect("count by status");
    assert_eq!(
        counts,
        vec![
            ("active".to_string(), 5),
            ("allocated".to_string(), 3),
            ("expired".to_string(), 1000),
        ]
    );

    // Every flipped row must have resolved_at set.
    let null_resolved: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM inventory_reservations
          WHERE status = 'expired' AND resolved_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("count null resolved_at");
    assert_eq!(null_resolved, 0);
}
