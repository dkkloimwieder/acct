//! T1 probes for acct-sbr2 (mig 0067): partition rollover registry +
//! horizon recon (check #15).
//!
//!   P1 — partitioned_tables_registry seeded with 6 known rows.
//!   P2 — _partition_max_upper_bound returns at least 2028-01-01 for
//!        each registered table (24-month bake from 2026-01 covers
//!        the registry's bake_end). Other tests in the suite may
//!        have extended partitions further (e.g., cost_layers_t1
//!        creates a 2028-03-01 partition); the bound must therefore
//!        be >= bake_end, not exactly bake_end.
//!   P3 — clean recon (current_date + min_horizon well below the
//!        bake window) → 0 alerts of kind 'partition_horizon_low'.
//!   P4 — synthesize low horizon by raising min_horizon_months past
//!        the bake window → check #15 fires.
//!   P5 — _extend_partition_horizon creates new partitions; the
//!        return value is the count of helper calls (all of which
//!        succeed; CREATE IF NOT EXISTS makes the operation
//!        idempotent at the SQL level). Asserts that the horizon
//!        advances by exactly 6 months from whatever the pre-call
//!        bound was, not from a fixed literal.
//!   P5b — _extend_partition_horizon returns -1 for an unknown table.
//!   P6 — _partition_max_upper_bound works on a non-registered
//!        partitioned table (walks pg_inherits directly).
//!   P7 — registry row notes column populated for each row.
//!   P8 — bake window is continuous and complete for each registered
//!        table: child partitions inside [bake_start, bake_end) cover
//!        every month with no gaps and no overlaps. Closes the gap
//!        left by P2 (which only checks the upper bound). acct-stls.

mod common;

use common::*;
use sqlx::Row;

#[tokio::test]
async fn p1_registry_seeded_with_six_rows() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM partitioned_tables_registry")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 6, "expected 6 registry rows");

    let names: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM partitioned_tables_registry ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        names,
        vec![
            "cost_layer_depletions",
            "cost_layers",
            "inventory_lot_events",
            "inventory_lots",
            "inventory_movements",
            "inventory_unit_events",
        ]
    );
}

#[tokio::test]
async fn p2_max_upper_bound_for_each_registered_table() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let rows = sqlx::query(
        "SELECT table_name,
                _partition_max_upper_bound(table_name)::TEXT AS m
           FROM partitioned_tables_registry
          ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(rows.len(), 6);
    for row in rows {
        let name: String = row.get("table_name");
        let m: Option<String> = row.get("m");
        let s = m.as_deref().expect("partition bound is not NULL");
        // Other tests in the suite may have extended partitions
        // beyond the registry's deploy-time bake_end. The invariant
        // here is that the bound is at least bake_end (2028-01-01).
        assert!(
            s >= "2028-01-01",
            "expected >= 2028-01-01 max upper bound on {name}, got {s}"
        );
    }
}

#[tokio::test]
async fn p3_clean_recon_fires_no_horizon_alerts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    sqlx::query("TRUNCATE reconciliation_alerts")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("SELECT run_daily_reconciliation()")
        .execute(&pool)
        .await
        .unwrap();

    let fired: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_alerts
          WHERE alert_kind = 'partition_horizon_low'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fired, 0, "expected 0 partition_horizon_low alerts");
}

#[tokio::test]
async fn p4_low_horizon_fires_check_15() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Run the whole test inside a transaction that we ROLLBACK at
    // the end. partitioned_tables_registry is config-grain (not
    // truncated by reset_to_fixture); other test binaries run in
    // parallel against the same DB and would observe a committed
    // min_horizon_months=600, false-firing check #15 from their
    // close_period / run_daily_reconciliation paths. Keeping the
    // mutation in an uncommitted txn isolates it to this test.
    let mut tx = pool.begin().await.unwrap();

    sqlx::query(
        "UPDATE partitioned_tables_registry
            SET min_horizon_months = 600
          WHERE table_name = 'inventory_movements'",
    )
    .execute(&mut *tx)
    .await
    .unwrap();

    sqlx::query("SELECT run_daily_reconciliation()")
        .execute(&mut *tx)
        .await
        .unwrap();

    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM reconciliation_alerts
          WHERE alert_kind = 'partition_horizon_low'
            AND payload->>'table_name' = 'inventory_movements'
          ORDER BY id DESC
          LIMIT 1",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();

    assert_eq!(
        payload["table_name"].as_str(),
        Some("inventory_movements")
    );
    assert_eq!(
        payload["min_horizon_months"].as_i64(),
        Some(600)
    );
    let lb = payload["latest_bound"]
        .as_str()
        .expect("latest_bound present");
    assert!(
        lb >= "2028-01-01",
        "latest_bound should be >= 2028-01-01, got {lb}"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn p5_extend_partition_horizon_extends_bake_window() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Wrap in a transaction we rollback at end. Other test binaries
    // running in parallel may also race on inventory_movements
    // partitions (inventory_movements_t1 explicitly creates a
    // 2028-03-01 partition); the txn isolation keeps before/after
    // bounds deterministic and ROLLBACK undoes the new child
    // partitions so we don't pollute global state.
    let mut tx = pool.begin().await.unwrap();

    let before: String = sqlx::query_scalar(
        "SELECT _partition_max_upper_bound('inventory_movements')::TEXT",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert!(
        before.as_str() >= "2028-01-01",
        "starting horizon should be at least bake_end, got {before}"
    );

    let first: i32 = sqlx::query_scalar(
        "SELECT _extend_partition_horizon('inventory_movements', 6)",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(first, 6);

    let expected_after: String = sqlx::query_scalar(
        "SELECT ($1::DATE + INTERVAL '6 months')::DATE::TEXT",
    )
    .bind(&before)
    .fetch_one(&mut *tx)
    .await
    .unwrap();

    let after: String = sqlx::query_scalar(
        "SELECT _partition_max_upper_bound('inventory_movements')::TEXT",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        after, expected_after,
        "expected horizon to advance 6 months from {before}"
    );

    let second: i32 = sqlx::query_scalar(
        "SELECT _extend_partition_horizon('inventory_movements', 6)",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        second, 6,
        "idempotent re-call still returns 6 (each PERFORM is itself \
         idempotent via CREATE IF NOT EXISTS)"
    );

    let expected_again: String = sqlx::query_scalar(
        "SELECT ($1::DATE + INTERVAL '6 months')::DATE::TEXT",
    )
    .bind(&after)
    .fetch_one(&mut *tx)
    .await
    .unwrap();

    let again: String = sqlx::query_scalar(
        "SELECT _partition_max_upper_bound('inventory_movements')::TEXT",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        again, expected_again,
        "second call advanced another 6 months from {after}"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn p5b_extend_returns_neg_one_for_unknown_table() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let n: i32 = sqlx::query_scalar(
        "SELECT _extend_partition_horizon('not_a_real_table', 3)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, -1);
}

#[tokio::test]
async fn p6_max_upper_bound_works_for_non_registered_partition() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Wrap in a tx + ROLLBACK so the sandbox table never commits
    // (test-binary parallelism means hygiene matters even though
    // no other binary references this name).
    let mut tx = pool.begin().await.unwrap();

    sqlx::query(
        "CREATE TABLE _t_sbr2_sandbox (
           id   BIGSERIAL,
           d    DATE NOT NULL,
           PRIMARY KEY (id, d)
         ) PARTITION BY RANGE (d)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE _t_sbr2_sandbox_2030_06
           PARTITION OF _t_sbr2_sandbox
           FOR VALUES FROM ('2030-06-01') TO ('2030-07-01')",
    )
    .execute(&mut *tx)
    .await
    .unwrap();

    let bound: Option<String> = sqlx::query_scalar(
        "SELECT _partition_max_upper_bound('_t_sbr2_sandbox')::TEXT",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(bound.as_deref(), Some("2030-07-01"));

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn p8_bake_window_is_continuous_and_complete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let tables = sqlx::query(
        "SELECT table_name, bake_start::TEXT AS bs, bake_end::TEXT AS be
           FROM partitioned_tables_registry
          ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    for row in tables {
        let table_name: String = row.get("table_name");
        let bake_start: String = row.get("bs");
        let bake_end: String = row.get("be");

        let expected_count: i64 = sqlx::query_scalar(
            "SELECT (
                EXTRACT(YEAR  FROM AGE($2::DATE, $1::DATE)) * 12
              + EXTRACT(MONTH FROM AGE($2::DATE, $1::DATE))
             )::BIGINT",
        )
        .bind(&bake_start)
        .bind(&bake_end)
        .fetch_one(&pool)
        .await
        .unwrap();

        let bound_rows = sqlx::query(
            "WITH parts AS (
               SELECT regexp_match(
                        pg_get_expr(c.relpartbound, c.oid),
                        'FROM \\(''([^'']+)''\\) TO \\(''([^'']+)''\\)'
                      ) AS m
                 FROM pg_inherits i
                 JOIN pg_class    c ON c.oid = i.inhrelid
                WHERE i.inhparent = $1::regclass
             )
             SELECT m[1] AS from_d, m[2] AS to_d
               FROM parts
              WHERE m IS NOT NULL
                AND m[1]::DATE >= $2::DATE
                AND m[2]::DATE <= $3::DATE
              ORDER BY m[1]::DATE",
        )
        .bind(&table_name)
        .bind(&bake_start)
        .bind(&bake_end)
        .fetch_all(&pool)
        .await
        .unwrap();

        let bounds: Vec<(String, String)> = bound_rows
            .iter()
            .map(|r| (r.get::<String, _>("from_d"), r.get::<String, _>("to_d")))
            .collect();

        assert_eq!(
            bounds.len() as i64,
            expected_count,
            "{table_name}: expected {expected_count} partitions in \
             [{bake_start}, {bake_end}), got {}",
            bounds.len()
        );

        assert_eq!(
            bounds.first().expect("at least one partition").0,
            bake_start,
            "{table_name}: first child FROM bound != bake_start"
        );
        assert_eq!(
            bounds.last().expect("at least one partition").1,
            bake_end,
            "{table_name}: last child TO bound != bake_end"
        );

        for i in 0..bounds.len() - 1 {
            assert_eq!(
                bounds[i].1,
                bounds[i + 1].0,
                "{table_name}: gap or overlap between partition {} \
                 (ends {}) and partition {} (starts {})",
                i,
                bounds[i].1,
                i + 1,
                bounds[i + 1].0
            );
        }
    }
}

#[tokio::test]
async fn p7_registry_rows_have_notes_populated() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let missing: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM partitioned_tables_registry
          WHERE notes IS NULL OR length(trim(notes)) = 0",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(missing, 0, "all registry rows should have notes populated");
}
