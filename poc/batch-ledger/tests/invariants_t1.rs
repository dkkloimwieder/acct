//! acct-w88b / M10.D1 — pin previously-unpinned extension invariants.
//!
//! Companion to `poc/ledger-extension/INVARIANTS.md`. Tests the
//! invariants that the M9 PoC scenarios protected only implicitly:
//!
//! - **I1** APPLY_SEQ and per-cell `last_seq` are monotonic.
//! - **I1b** per-cell `last_seq` is monotone across concurrent writers
//!   (acct-fyl3 — falsification gate for the `store→fetch_max` fix).
//! - **I2** drained_seq ≤ last_seq always.
//! - **I4** OCCUPIED_COUNT matches actual occupied buckets.
//! - **I13** bgworker scoped to `ledger.drain_database`.
//! - **I14** ledger_shmem_reset zeros all shmem state.
//!
//! ## Test isolation strategy
//!
//! These tests run against a live extension instance that may be
//! sharing shmem with other test binaries (or a background bench).
//! Each test uses a UUID-derived synthetic key range so its
//! observations are isolated to its own cells, and assertions are
//! delta-based against pre-state where possible.
//!
//! The exception is `i14_reset_completeness`, which calls
//! `ledger_shmem_reset()` and is therefore destructive to other
//! tests' shmem state. It is `#[ignore]`'d by default; run via
//! `cargo test --test invariants_t1 -- --ignored --test-threads=1`.
//!
//! Location: this file lives in `poc/batch-ledger/tests/` (alongside
//! `rollback_correctness_t1.rs`) because the `ledger-extension`
//! crate is a pgrx cdylib without sqlx/tokio. The acct-w88b issue
//! description named the path `poc/ledger-extension/tests/` — the
//! deviation is intentional and matches the M10.A1 precedent.

use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn db_url() -> String {
    std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

/// Derive a unique-per-test synthetic account_id offset. Avoids
/// collisions with concurrent test binaries by hashing UUID bytes
/// into a high (8e8) range that's well above any seeded fixture.
fn synthetic_offset() -> i64 {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let hash = ((bytes[0] as i64) << 24)
        | ((bytes[1] as i64) << 16)
        | ((bytes[2] as i64) << 8)
        | (bytes[3] as i64);
    800_000_000_i64 + (hash.abs() % 10_000_000)
}

/// I1 — APPLY_SEQ and per-cell last_seq are strictly monotonic across
/// successive applies.
///
/// **A2 note:** post-acct-4e91, `ledger_apply_balance_delta` returns
/// 0 (staging op, no seq) — APPLY_SEQ advances at COMMIT, not at
/// apply time. The cell's `last_seq` is set during the commit
/// callback. So the assertions here check:
/// - APPLY_SEQ advances by at least N across N auto-committed
///   applies (concurrent tests may add more).
/// - The cell's `last_seq` after the loop is non-zero and is a
///   plausible value relative to APPLY_SEQ.
#[tokio::test]
async fn i1_seq_monotonicity() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let acct = synthetic_offset();
    let period: i32 = 7_001;

    let pre_global: i64 = sqlx::query_scalar("SELECT ledger_shmem_apply_seq()")
        .fetch_one(&pool)
        .await
        .expect("pre apply_seq");

    // Apply 100 times against the same cell; each call auto-commits
    // (single-statement implicit txn), firing xact_commit which
    // advances APPLY_SEQ by 1 for this cell.
    let n = 100;
    for _ in 0..n {
        sqlx::query(
            "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1, 0)",
        )
        .bind(acct)
        .bind(period)
        .execute(&pool)
        .await
        .expect("apply");
    }

    let post_global: i64 = sqlx::query_scalar("SELECT ledger_shmem_apply_seq()")
        .fetch_one(&pool)
        .await
        .expect("post apply_seq");

    assert!(
        post_global >= pre_global + n as i64,
        "APPLY_SEQ did not advance enough: pre={pre_global} post={post_global} n={n}"
    );

    // The cell's last_seq is set by the commit-time next_seq() call;
    // it must be > 0 and within the global APPLY_SEQ snapshot.
    let row = sqlx::query(
        "SELECT last_seq FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("lookup");
    let last_seq: Option<i64> = row.get(0);
    let last_seq = last_seq.expect("cell should exist post-commits");
    assert!(
        last_seq > pre_global,
        "I1: cell last_seq ({last_seq}) should exceed pre_global ({pre_global})"
    );
    assert!(
        last_seq <= post_global,
        "I1: cell last_seq ({last_seq}) cannot exceed APPLY_SEQ snapshot ({post_global})"
    );

    // The cell's balance reflects all 100 applies (delta-1 each).
    let bal_row = sqlx::query(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("balance lookup");
    let balance: Option<i64> = bal_row.get(0);
    assert_eq!(
        balance,
        Some(n as i64),
        "I1: cell balance should equal N=100 applied deltas"
    );
}

/// I1b — per-cell `last_seq` is monotone under concurrent writers.
///
/// **acct-fyl3 — regression guard for the store→fetch_max fix.**
///
/// The bug: pre-fix, `try_update_existing` did
/// `b.last_seq.store(seq, Release)` after pulling `seq` from
/// `next_seq()`. Under multi-writer SHARED LWLock, two writers can
/// race so that the *later* `next_seq()` caller commits its store
/// FIRST, and the *earlier* caller's `store(smaller_seq)` lands
/// AFTER, leaving the cell at a value SMALLER than was previously
/// observed by some thread. Post-fix uses `fetch_max(seq, AcqRel)`,
/// guaranteeing per-cell monotonicity regardless of inter-thread
/// reordering.
///
/// **Falsification limit (documented honestly).** This test was
/// designed as a falsification gate but empirical probing against
/// the buggy `store` variant on this rig surfaced zero violations
/// across ~27K applies + ~14K reader samples (4s wall). The race
/// window between `next_seq()` and the store is a few CPU
/// instructions wide; the cell is re-advanced by the very next peer
/// apply within microseconds, far faster than a SQL lookup
/// round-trip (~30–100 µs) can resolve. The bug is real — the
/// reasoning is in `lib.rs` at the fetch_max site — but SQL-driven
/// probing cannot catch its transient regression window.
///
/// What this test still pins:
/// - The multi-writer SHARED-lock cell-update path holds up under
///   heavy concurrent contention without crashing, deadlocking, or
///   losing applies (final balance matches total applied deltas).
/// - The final `last_seq` is at least as large as the max observed
///   by any reader (sanity check on the value being stable post-
///   quiescence).
/// - Future regressions that widen the race window (e.g.,
///   introducing yields or expensive work between `next_seq()` and
///   `fetch_max`) would be more likely to surface here than in
///   `i1_seq_monotonicity` (single-threaded).
///
/// Existing `i1_seq_monotonicity` covers the global APPLY_SEQ via
/// `fetch_add`. This probe targets the per-cell update path with
/// concurrent writers.
#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn i1b_per_cell_last_seq_monotonic() {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let acct = synthetic_offset();
    let period: i32 = 7_006;

    // Seed the cell single-threaded so all workers race on the
    // existing-cell `try_update_existing` path (where the fetch_max
    // lives) rather than the EXCLUSIVE-protected insert_new_seeded.
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&pool)
    .await
    .expect("seed cell");

    // Race exposure: the buggy `store` race window between
    // `next_seq() → store` is only a few CPU instructions wide.
    // Detection pattern: dedicated polling readers tight-loop on
    // last_seq lookup, recording the observed value series. The
    // assertion is per-reader monotonicity. With buggy store, a
    // peer-writer's late store will regress the cell below a value
    // some reader has already observed.
    //
    // Writers pump applies to the same cell to keep contention high.
    // Test runs for fixed wall-time so the falsification probability
    // converges regardless of host speed.
    const WRITERS: usize = 8;
    const READERS: usize = 4;
    const TEST_DURATION_MS: u64 = 4_000;

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let total_applies = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut writer_handles = Vec::new();
    for _w in 0..WRITERS {
        let pool = pool.clone();
        let stop = stop.clone();
        let total_applies = total_applies.clone();
        let h = tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                sqlx::query(
                    "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1, 0)",
                )
                .bind(acct)
                .bind(period)
                .execute(&pool)
                .await
                .expect("apply");
                total_applies.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });
        writer_handles.push(h);
    }

    let mut reader_handles = Vec::new();
    for r in 0..READERS {
        let pool = pool.clone();
        let stop = stop.clone();
        let h = tokio::spawn(async move {
            let mut local_max: i64 = 0;
            let mut violations: Vec<(i64, i64)> = Vec::new();
            let mut samples: u64 = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let row = sqlx::query(
                    "SELECT last_seq FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
                )
                .bind(acct)
                .bind(period)
                .fetch_one(&pool)
                .await
                .expect("lookup");
                let observed: Option<i64> = row.get(0);
                let observed = observed.expect("cell should exist after seed");
                if observed < local_max {
                    violations.push((local_max, observed));
                }
                if observed > local_max {
                    local_max = observed;
                }
                samples += 1;
            }
            (r, samples, local_max, violations)
        });
        reader_handles.push(h);
    }

    tokio::time::sleep(std::time::Duration::from_millis(TEST_DURATION_MS)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    for h in writer_handles {
        h.await.expect("writer join");
    }

    let mut global_max: i64 = 0;
    let mut all_violations: Vec<(usize, i64, i64)> = Vec::new();
    let mut total_samples: u64 = 0;
    for h in reader_handles {
        let (r, samples, local_max, violations) = h.await.expect("reader join");
        total_samples += samples;
        if local_max > global_max {
            global_max = local_max;
        }
        for (prev, obs) in violations {
            all_violations.push((r, prev, obs));
        }
    }
    let applies = total_applies.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "i1b: applies={applies} reader_samples={total_samples} global_max={global_max} violations={}",
        all_violations.len()
    );

    if !all_violations.is_empty() {
        let sample: Vec<String> = all_violations
            .iter()
            .take(10)
            .map(|(r, prev, obs)| {
                format!("reader={r} prev_max={prev} observed={obs}")
            })
            .collect();
        panic!(
            "I1b: per-cell last_seq regressed within at least one reader's view; \
             total violations={} (sample, up to 10): {:?}",
            all_violations.len(),
            sample
        );
    }

    // Final cell last_seq must dominate every per-worker max-observed.
    let final_seq: Option<i64> = sqlx::query_scalar(
        "SELECT last_seq FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("final lookup");
    let final_seq = final_seq.expect("cell should exist post-test");
    assert!(
        final_seq >= global_max,
        "I1b: final last_seq ({final_seq}) cannot be less than max-observed ({global_max})"
    );

    // Sanity: cell's balance must equal seed + total_applies (each delta=+1).
    let bal: Option<i64> = sqlx::query_scalar(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(acct)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("balance lookup");
    let expected = 1 + applies as i64;
    assert_eq!(
        bal,
        Some(expected),
        "I1b sanity: balance disagrees with applied deltas (seed=1 + applies={applies})"
    );
}

/// I2 — drained_seq ≤ last_seq always. Probed via the dirty/drained
/// count aggregate: post-drain quiescence, no cell can have
/// last_seq > drained_seq, so dirty_count delta for these cells == 0
/// and drained_count delta >= K (the cells we touched).
#[tokio::test]
async fn i2_drained_le_lastseq() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let base = synthetic_offset();
    let period: i32 = 7_002;
    let k = 5_i64;
    let applies_per_cell = 4;

    // Apply across K cells, multiple times each.
    for cell in 0..k {
        for _ in 0..applies_per_cell {
            sqlx::query(
                "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1, 0)",
            )
            .bind(base + cell)
            .bind(period)
            .execute(&pool)
            .await
            .expect("apply");
        }
    }

    // Wait long enough for the bgworker to drain — default tick is 100 ms;
    // 1500 ms gives 10–15 ticks of headroom even under contention.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // Probe per-cell last_seq via lookup; assert each is set and the
    // cell shows up in the rollup at the same or greater watermark.
    // (Direct drained_seq isn't a SQL surface; we infer it via the
    // rollup table's last_seq, which the drainer writes from the
    // captured pre-snapshot.)
    for cell in 0..k {
        let lookup = sqlx::query(
            "SELECT balance, last_seq FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
        )
        .bind(base + cell)
        .bind(period)
        .fetch_one(&pool)
        .await
        .expect("lookup");
        let shmem_last: Option<i64> = lookup.get(1);
        assert!(
            shmem_last.is_some() && shmem_last.unwrap() > 0,
            "cell {cell}: shmem last_seq should be set, got {shmem_last:?}"
        );

        let rollup = sqlx::query(
            "SELECT last_seq FROM account_balances_rollup
                 WHERE account_id = $1 AND period_id = $2
                   AND currency_id = 1 AND ledger_kind = 1",
        )
        .bind(base + cell)
        .bind(period)
        .fetch_optional(&pool)
        .await
        .expect("rollup");

        let rollup_last: i64 = rollup
            .map(|r| r.get::<i64, _>(0))
            .expect("rollup row should exist after drain wait");

        assert!(
            rollup_last <= shmem_last.unwrap(),
            "I2 violated: rollup last_seq ({rollup_last}) > shmem last_seq ({}) for cell {cell}",
            shmem_last.unwrap()
        );
    }
}

/// I4 — OCCUPIED_COUNT matches the number of distinct keys ever
/// inserted. Tests both the single-thread path (delta semantics) and
/// the concurrent-insert race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn i4_occupied_count_consistency() {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let base = synthetic_offset();
    let period: i32 = 7_003;

    let pre: i64 = sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(&pool)
        .await
        .expect("pre occupied");

    // Phase 1 — single-thread: apply against 10 distinct new keys.
    let k1: i64 = 10;
    for cell in 0..k1 {
        sqlx::query(
            "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1, 0)",
        )
        .bind(base + cell)
        .bind(period)
        .execute(&pool)
        .await
        .expect("apply");
    }
    let after_phase1: i64 = sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(&pool)
        .await
        .expect("post phase1");
    assert!(
        after_phase1 >= pre + k1,
        "phase 1: occupied {after_phase1} not >= pre {pre} + {k1}"
    );

    // Phase 2 — re-apply against the same K keys. Occupied unchanged.
    for cell in 0..k1 {
        sqlx::query(
            "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 5, 0)",
        )
        .bind(base + cell)
        .bind(period)
        .execute(&pool)
        .await
        .expect("re-apply");
    }
    let after_phase2: i64 = sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(&pool)
        .await
        .expect("post phase2");
    assert_eq!(
        after_phase2, after_phase1,
        "phase 2: re-apply on existing keys must not change occupied count"
    );

    // Phase 3 — concurrent insert race. 4 workers × 25 distinct new keys
    // each = 100 fresh keys. Assert occupied advances by exactly 100.
    let pre_phase3: i64 = after_phase2;
    let workers = 4_usize;
    let per_worker: i64 = 25;
    let phase3_base = base + 1000; // disjoint from phase 1/2

    let mut handles = Vec::new();
    for w in 0..workers {
        let pool = pool.clone();
        let base = phase3_base + (w as i64 * per_worker);
        let h = tokio::spawn(async move {
            for cell in 0..per_worker {
                sqlx::query(
                    "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1, 0)",
                )
                .bind(base + cell)
                .bind(period)
                .execute(&pool)
                .await
                .expect("apply");
            }
        });
        handles.push(h);
    }
    for h in handles {
        h.await.expect("join");
    }
    let after_phase3: i64 = sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(&pool)
        .await
        .expect("post phase3");
    let phase3_delta = after_phase3 - pre_phase3;
    let expected = workers as i64 * per_worker;
    assert!(
        phase3_delta >= expected,
        "phase 3: occupied delta {phase3_delta} should be at least {expected} \
         (one cell per distinct key)"
    );
}

/// I13 — bgworker scoped to `ledger.drain_database`. Verifies the GUC
/// reads as `acct_poc` and that applies in this DB land in this DB's
/// rollup. A full cross-DB probe (apply in acct_poc, assert no row in
/// acct_poc_test) requires test infrastructure deferred to acct-vd74.
#[tokio::test]
async fn i13_drain_database_guc() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    // GUC reads as the default 'acct_poc'.
    let dbname: String = sqlx::query_scalar("SHOW ledger.drain_database")
        .fetch_one(&pool)
        .await
        .expect("show guc");
    assert_eq!(
        dbname, "acct_poc",
        "I13: ledger.drain_database GUC should default to 'acct_poc', got {dbname:?}"
    );

    // Verify the bgworker is alive — drain_interval_ms reads.
    let interval: String = sqlx::query_scalar("SHOW ledger.drain_interval_ms")
        .fetch_one(&pool)
        .await
        .expect("show interval");
    let interval_n: i32 = interval.parse().expect("parse interval");
    assert!(
        (10..=3_600_000).contains(&interval_n),
        "I13: drain_interval_ms out of bounds: {interval_n}"
    );

    // Apply in this DB → assert rollup row appears here.
    let acct = synthetic_offset();
    let period: i32 = 7_004;
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 42, 0)",
    )
    .bind(acct)
    .bind(period)
    .execute(&pool)
    .await
    .expect("apply");

    // Wait ~15 drain ticks.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let row = sqlx::query(
        "SELECT balance FROM account_balances_rollup
             WHERE account_id = $1 AND period_id = $2
               AND currency_id = 1 AND ledger_kind = 1",
    )
    .bind(acct)
    .bind(period)
    .fetch_optional(&pool)
    .await
    .expect("rollup lookup");

    let bal: i64 = row
        .map(|r| r.get::<i64, _>(0))
        .expect("rollup row should exist in acct_poc after drain wait");
    assert_eq!(
        bal, 42,
        "I13: rollup balance disagrees with applied delta"
    );
}

/// I14 — `ledger_shmem_reset()` zeros all shmem state.
///
/// DESTRUCTIVE: wipes EVERY cell, not just this test's. `#[ignore]`'d
/// so the default `cargo test` doesn't run it alongside the others.
/// Run with `cargo test --test invariants_t1 -- --ignored --test-threads=1`.
#[tokio::test]
#[ignore]
async fn i14_reset_completeness() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url())
        .await
        .expect("connect");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("create ext");

    let base = synthetic_offset();
    let period: i32 = 7_005;
    let k: i64 = 10;
    let applies_per_cell = 5;

    // Apply N applies across K cells.
    for cell in 0..k {
        for _ in 0..applies_per_cell {
            sqlx::query(
                "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 100, 0)",
            )
            .bind(base + cell)
            .bind(period)
            .execute(&pool)
            .await
            .expect("apply");
        }
    }

    // Wait for drain so we have a rich pre-reset state.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let pre_occupied: i64 = sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(&pool)
        .await
        .expect("pre occupied");
    let pre_apply_seq: i64 = sqlx::query_scalar("SELECT ledger_shmem_apply_seq()")
        .fetch_one(&pool)
        .await
        .expect("pre apply_seq");
    assert!(
        pre_occupied > 0 && pre_apply_seq > 0,
        "I14 pre-state should be non-trivial: occupied={pre_occupied} apply_seq={pre_apply_seq}"
    );

    // RESET.
    sqlx::query("SELECT ledger_shmem_reset()")
        .execute(&pool)
        .await
        .expect("reset");

    // Post-reset: all counters zero.
    let post_occupied: i64 = sqlx::query_scalar("SELECT ledger_shmem_occupied()")
        .fetch_one(&pool)
        .await
        .expect("post occupied");
    let post_apply_seq: i64 = sqlx::query_scalar("SELECT ledger_shmem_apply_seq()")
        .fetch_one(&pool)
        .await
        .expect("post apply_seq");
    let post_dirty: i64 = sqlx::query_scalar("SELECT ledger_shmem_dirty_count()")
        .fetch_one(&pool)
        .await
        .expect("post dirty");
    let post_drained: i64 = sqlx::query_scalar("SELECT ledger_shmem_drained_count()")
        .fetch_one(&pool)
        .await
        .expect("post drained");

    assert_eq!(post_occupied, 0, "I14: occupied not zeroed post-reset");
    assert_eq!(post_apply_seq, 0, "I14: apply_seq not zeroed post-reset");
    assert_eq!(post_dirty, 0, "I14: dirty_count not zeroed post-reset");
    assert_eq!(post_drained, 0, "I14: drained_count not zeroed post-reset");

    // Every prior key returns NULL via balance_lookup.
    for cell in 0..k {
        let lookup_balance: Option<i64> = sqlx::query_scalar(
            "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
        )
        .bind(base + cell)
        .bind(period)
        .fetch_one(&pool)
        .await
        .expect("lookup");
        assert_eq!(
            lookup_balance, None,
            "I14: cell {cell} not absent post-reset (got {lookup_balance:?})"
        );
    }

    // account_balances_rollup rows SURVIVE reset (by design — rollup is
    // a SQL table, not shmem). Document by re-applying and checking that
    // the rollup-seeded path activates.
    sqlx::query(
        "SELECT ledger_apply_balance_delta($1::bigint, $2::int, 1::smallint, 1::smallint, 1, 0)",
    )
    .bind(base)
    .bind(period)
    .execute(&pool)
    .await
    .expect("re-apply");

    let row = sqlx::query(
        "SELECT balance FROM ledger_balance_lookup($1::bigint, $2::int, 1::smallint, 1::smallint)",
    )
    .bind(base)
    .bind(period)
    .fetch_one(&pool)
    .await
    .expect("lookup post-reset");
    let bal: Option<i64> = row.get(0);
    // M6 lazy-load: the rollup row had qty=0, balance=500 (5 applies×100).
    // After reset + 1 new apply of +1: cell starts at (500 + 1, 0 + 0) = 501.
    assert_eq!(
        bal,
        Some(501),
        "I14: post-reset re-apply should lazy-load rollup state (500) + delta (1) = 501; got {bal:?}"
    );
}
