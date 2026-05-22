//! Cross-path equivalence (acct-t9lo, design-v3 §8.4).
//!
//! Generates a deterministic submission list (same workload generator,
//! seeded per caller), runs it through Path A's `ledger_submit_trx`,
//! snapshots the resulting trx + trx_line + posting_line + pool_state,
//! truncates + re-seeds, runs the same list through Path B's
//! `ledger_enqueue_trx`, snapshots again, diffs. Exit 0 = match.
//!
//! Tolerates path-allocated id differences (trx.id, trx_line.id,
//! posting_line.id) and per-row created_at timestamps by canonicalizing
//! everything to its natural key:
//!   trx          → (trx_type, source_id)
//!   trx_line     → (pool_id, line_type, source_id, qty, unit_cost)
//!                  sorted within its parent trx
//!   posting_line → (event_type, amount, debit_account, credit_account)
//!                  sorted within its parent trx_line
//!   pool_state   → keyed by (pool_id, layer_seq)
//!
//! `trx_seq` is deliberately excluded from line canonicalization.
//! It's a per-pool monotonic counter allocated at INSERT time; with
//! multiple Path B committers contending on the same pool's pool_lock
//! between commit_groups, the allocation order can diverge from Path
//! A's serial-submit order. Per-pool monotonicity holds within both
//! paths; cross-path absolute values do not, and need not for
//! ledger-amount equivalence. pool_state matching is the load-bearing
//! property — it confirms plan_apply produced the same mutations
//! regardless of `trx_seq` assignment order.
//!
//! Submission is single-threaded (one tokio task) — concurrent
//! submission would let the router and the per-tx ordering diverge,
//! masking equivalence properties. Equivalence isn't a load test;
//! it's a correctness check.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use chrono::Utc;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::cli::MethodMix;
use crate::pool_universe;
use crate::scenarios;
use crate::workload::LineParam;

pub struct EquivalenceOptions {
    pub dsn: String,
    pub scenario: String,
    pub submissions_per_caller: usize,
}

pub async fn run(opts: EquivalenceOptions) -> Result<(), String> {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&opts.dsn)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let started_at = Utc::now();
    let run_prefix: i64 = (started_at.timestamp() as i64 % 1_000_000) * 1_000_000_000_000;

    // Fixed small universe so diffs are tractable. 100 pools, 10 skus, 10
    // locations is enough to exercise all scenarios' overlap modes at the
    // reduced submission count.
    let universe_count = 100usize;
    let universe_skus = 10usize;
    let universe_locs = 10usize;

    eprintln!("[equivalence] resetting + seeding universe for Path A...");
    reset_ledger(&pool).await?;
    let universe = pool_universe::seed(&pool, universe_count, universe_skus, universe_locs, MethodMix::AllWac)
        .await
        .map_err(|e| format!("seed-pools (A): {e}"))?;

    let mut spec = scenarios::by_id(&opts.scenario, universe.clone())
        .ok_or_else(|| format!("unknown scenario '{}' (try s1..s6)", opts.scenario))?;
    // Cap callers below the universe size for Disjoint stripe_size > 0.
    if spec.callers > universe_count {
        spec.callers = universe_count.min(20);
        spec.workload.caller_count = spec.callers;
        eprintln!(
            "[equivalence] capping callers to {} (universe={})",
            spec.callers, universe_count
        );
    }
    let submissions = build_submissions(&spec.workload, spec.callers, opts.submissions_per_caller, run_prefix);
    eprintln!(
        "[equivalence] scenario={} callers={} submissions/caller={} total={}",
        opts.scenario,
        spec.callers,
        opts.submissions_per_caller,
        submissions.len()
    );

    eprintln!("[equivalence] running Path A (direct)...");
    let a_start = Instant::now();
    submit_direct(&pool, &submissions).await?;
    let direct_snap = take_snapshot(&pool).await?;
    let a_elapsed = a_start.elapsed();
    eprintln!(
        "[equivalence] direct: {} trx, {} trx_line, {} pool_state, {} posting_line ({:.2}s)",
        direct_snap.trx_groups.len(),
        direct_snap.total_lines(),
        direct_snap.pool_state.len(),
        direct_snap.total_postings(),
        a_elapsed.as_secs_f64()
    );

    eprintln!("[equivalence] resetting + reseeding universe for Path B...");
    reset_ledger(&pool).await?;
    let universe_b = pool_universe::seed(&pool, universe_count, universe_skus, universe_locs, MethodMix::AllWac)
        .await
        .map_err(|e| format!("seed-pools (B): {e}"))?;
    if universe.pool_ids != universe_b.pool_ids {
        return Err(format!(
            "universe pool_ids drifted between resets: {} → {} (TRUNCATE RESTART IDENTITY should have produced identical ids)",
            universe.pool_ids.len(),
            universe_b.pool_ids.len()
        ));
    }

    eprintln!("[equivalence] running Path B (routed)...");
    let b_start = Instant::now();
    submit_routed(&pool, &submissions).await?;
    let routed_snap = take_snapshot(&pool).await?;
    let b_elapsed = b_start.elapsed();
    eprintln!(
        "[equivalence] routed: {} trx, {} trx_line, {} pool_state, {} posting_line ({:.2}s)",
        routed_snap.trx_groups.len(),
        routed_snap.total_lines(),
        routed_snap.pool_state.len(),
        routed_snap.total_postings(),
        b_elapsed.as_secs_f64()
    );

    let diffs = diff_snapshots(&direct_snap, &routed_snap);
    if diffs.is_empty() {
        println!(
            "equivalence OK: scenario={} submissions={} A={} B={} trx (identical)",
            opts.scenario,
            submissions.len(),
            direct_snap.trx_groups.len(),
            routed_snap.trx_groups.len()
        );
        Ok(())
    } else {
        eprintln!("EQUIVALENCE FAILED ({} diffs):", diffs.len());
        for d in diffs.iter().take(20) {
            eprintln!("  {d}");
        }
        if diffs.len() > 20 {
            eprintln!("  ... {} more diffs suppressed", diffs.len() - 20);
        }
        Err(format!("{} mismatches", diffs.len()))
    }
}

#[derive(Debug, Clone)]
struct Submission {
    source_id: i64,
    lines: Vec<LineParam>,
}

fn build_submissions(
    workload: &crate::workload::Workload,
    callers: usize,
    per_caller: usize,
    run_prefix: i64,
) -> Vec<Submission> {
    let mut rngs: Vec<StdRng> = (0..callers)
        .map(|c| StdRng::seed_from_u64(0xDEAD_BEEF_u64.wrapping_add(c as u64)))
        .collect();
    let mut subs = Vec::with_capacity(callers * per_caller);
    for tick in 0..per_caller {
        for caller_id in 0..callers {
            let lines = workload.next_lines(&mut rngs[caller_id], caller_id);
            let source_id = run_prefix + (caller_id as i64) * 1_000_000 + tick as i64;
            subs.push(Submission { source_id, lines });
        }
    }
    subs
}

async fn submit_direct(pool: &PgPool, submissions: &[Submission]) -> Result<(), String> {
    let posted_at = "2026-05-21T12:00:00+00:00";
    for s in submissions {
        let lines_json = build_lines_json(&s.lines);
        sqlx::query("SELECT ledger_submit_trx('po_receipt', $1, $2, $3::jsonb)")
            .bind(s.source_id)
            .bind(posted_at)
            .bind(&lines_json)
            .execute(pool)
            .await
            .map_err(|e| format!("submit_direct sid={}: {e}", s.source_id))?;
    }
    Ok(())
}

async fn submit_routed(pool: &PgPool, submissions: &[Submission]) -> Result<(), String> {
    let posted_at = "2026-05-21T12:00:00+00:00";
    for s in submissions {
        let lines_json = build_lines_json(&s.lines);
        sqlx::query("SELECT ledger_enqueue_trx('po_receipt', $1, $2, $3::jsonb)")
            .bind(s.source_id)
            .bind(posted_at)
            .bind(&lines_json)
            .execute(pool)
            .await
            .map_err(|e| format!("enqueue_routed sid={}: {e}", s.source_id))?;
    }
    // Wait for committer to drain everything.
    wait_for_committer_quiet(pool).await;
    // Verification poll: every submission must have materialized.
    let pending = submissions_pending_count(pool, submissions).await?;
    if pending > 0 {
        return Err(format!(
            "{pending} submissions failed to materialize within drain window — routed committer is stuck"
        ));
    }
    Ok(())
}

async fn submissions_pending_count(pool: &PgPool, submissions: &[Submission]) -> Result<i64, String> {
    let sids: Vec<i64> = submissions.iter().map(|s| s.source_id).collect();
    let materialized: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx \
          WHERE trx_type = 'po_receipt'::trx_type AND source_id = ANY($1::bigint[])",
    )
    .bind(&sids)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("count: {e}"))?;
    Ok(sids.len() as i64 - materialized)
}

async fn wait_for_committer_quiet(pool: &PgPool) {
    let poll = Duration::from_millis(200);
    let cap = Instant::now() + Duration::from_secs(20);
    let mut last: i64 = -1;
    let mut stable: u32 = 0;
    while Instant::now() < cap {
        let now: i64 = sqlx::query_scalar("SELECT ledger_routed_committer_drains_total()")
            .fetch_one(pool)
            .await
            .unwrap_or(last);
        if now == last {
            stable += 1;
            if stable >= 3 {
                return;
            }
        } else {
            stable = 0;
            last = now;
        }
        tokio::time::sleep(poll).await;
    }
}

async fn reset_ledger(pool: &PgPool) -> Result<(), String> {
    sqlx::query(
        "TRUNCATE TABLE posting_line_dimension, posting_line, \
                       trx_line, trx, pool_state, pool_lock, pool, \
                       sku, location, account \
                       RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .map_err(|e| format!("TRUNCATE: {e}"))?;
    Ok(())
}

fn build_lines_json(lines: &[LineParam]) -> Value {
    let arr: Vec<Value> = lines
        .iter()
        .map(|l| {
            json!({
                "pool_id": l.pool_id,
                "line_type": l.line_type,
                "source_id": l.source_id,
                "qty": l.qty,
                "unit_cost": l.unit_cost,
                "debit_account": l.debit_account,
                "credit_account": l.credit_account,
            })
        })
        .collect();
    Value::Array(arr)
}

// ── Snapshot + diff ─────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct PostingLineCanon {
    event_type: String,
    amount: i64,
    debit_account: i64,
    credit_account: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct TrxLineCanon {
    pool_id: i64,
    line_type: String,
    source_id: Option<i64>,
    qty: i64,
    unit_cost: i64,
    postings: Vec<PostingLineCanon>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct PoolStateRow {
    pool_id: i64,
    layer_seq: i64,
    qty: i64,
    unit_cost: i64,
}

#[derive(Debug, Default)]
struct LedgerSnapshot {
    /// (trx_type, source_id) → ordered Vec of canonical lines + their postings.
    trx_groups: BTreeMap<(String, i64), Vec<TrxLineCanon>>,
    pool_state: Vec<PoolStateRow>,
}

impl LedgerSnapshot {
    fn total_lines(&self) -> usize {
        self.trx_groups.values().map(|v| v.len()).sum()
    }
    fn total_postings(&self) -> usize {
        self.trx_groups
            .values()
            .map(|lines| lines.iter().map(|l| l.postings.len()).sum::<usize>())
            .sum()
    }
}

async fn take_snapshot(pool: &PgPool) -> Result<LedgerSnapshot, String> {
    // Pull the full trx + trx_line + posting_line join in one shot.
    // trx_seq excluded from canonical key per module-level docs.
    let rows: Vec<(
        String, i64, i64, String, Option<i64>, i64, i64,
        Option<String>, Option<i64>, Option<i64>, Option<i64>,
    )> = sqlx::query_as(
        "SELECT t.trx_type::text, t.source_id, \
                tl.pool_id, tl.line_type::text, tl.source_id, tl.qty, tl.unit_cost, \
                pl.event_type::text, pl.amount, pl.debit_account, pl.credit_account \
           FROM trx t \
           JOIN trx_line tl ON tl.trx_id = t.id \
           LEFT JOIN posting_line pl ON pl.trx_line_id = tl.id \
          ORDER BY t.trx_type, t.source_id, tl.pool_id, tl.line_type, tl.source_id, \
                   pl.event_type, pl.amount",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("snapshot trx+lines: {e}"))?;

    let mut snap = LedgerSnapshot::default();
    type LineKey = (i64, String, Option<i64>, i64, i64);
    let mut by_trx: BTreeMap<(String, i64), BTreeMap<LineKey, Vec<PostingLineCanon>>> =
        BTreeMap::new();
    for row in rows {
        let (
            trx_type, trx_source_id,
            pool_id, line_type, line_source_id, qty, unit_cost,
            pl_event, pl_amount, pl_debit, pl_credit,
        ) = row;
        let key = (pool_id, line_type.clone(), line_source_id, qty, unit_cost);
        let postings = by_trx
            .entry((trx_type, trx_source_id))
            .or_default()
            .entry(key)
            .or_default();
        if let (Some(ev), Some(amt), Some(d), Some(c)) =
            (pl_event, pl_amount, pl_debit, pl_credit)
        {
            postings.push(PostingLineCanon {
                event_type: ev,
                amount: amt,
                debit_account: d,
                credit_account: c,
            });
        }
    }
    for (trx_key, lines_map) in by_trx {
        let mut canon_lines: Vec<TrxLineCanon> = lines_map
            .into_iter()
            .map(|((pool_id, line_type, source_id, qty, unit_cost), mut postings)| {
                postings.sort();
                TrxLineCanon {
                    pool_id,
                    line_type,
                    source_id,
                    qty,
                    unit_cost,
                    postings,
                }
            })
            .collect();
        canon_lines.sort();
        snap.trx_groups.insert(trx_key, canon_lines);
    }

    let ps: Vec<(i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT pool_id, layer_seq, qty, unit_cost \
           FROM pool_state \
          ORDER BY pool_id, layer_seq",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("snapshot pool_state: {e}"))?;
    snap.pool_state = ps
        .into_iter()
        .map(|(pool_id, layer_seq, qty, unit_cost)| PoolStateRow {
            pool_id,
            layer_seq,
            qty,
            unit_cost,
        })
        .collect();
    Ok(snap)
}

fn diff_snapshots(a: &LedgerSnapshot, b: &LedgerSnapshot) -> Vec<String> {
    let mut diffs = Vec::new();
    let a_keys: std::collections::BTreeSet<_> = a.trx_groups.keys().collect();
    let b_keys: std::collections::BTreeSet<_> = b.trx_groups.keys().collect();

    for k in a_keys.difference(&b_keys) {
        diffs.push(format!("trx in A but not B: {:?}", k));
    }
    for k in b_keys.difference(&a_keys) {
        diffs.push(format!("trx in B but not A: {:?}", k));
    }
    for k in a_keys.intersection(&b_keys) {
        let av = &a.trx_groups[*k];
        let bv = &b.trx_groups[*k];
        if av != bv {
            diffs.push(format!(
                "trx {:?} content differs: A has {} lines, B has {} lines (first diff: {})",
                k,
                av.len(),
                bv.len(),
                first_line_diff(av, bv)
            ));
        }
    }

    if a.pool_state != b.pool_state {
        diffs.push(format!(
            "pool_state differs: A has {} rows, B has {} rows",
            a.pool_state.len(),
            b.pool_state.len()
        ));
        let n = a.pool_state.len().min(b.pool_state.len());
        for i in 0..n {
            if a.pool_state[i] != b.pool_state[i] {
                diffs.push(format!(
                    "pool_state[{}] A={:?} B={:?}",
                    i, a.pool_state[i], b.pool_state[i]
                ));
                if diffs.len() > 25 {
                    break;
                }
            }
        }
    }

    diffs
}

fn first_line_diff(av: &[TrxLineCanon], bv: &[TrxLineCanon]) -> String {
    let n = av.len().min(bv.len());
    for i in 0..n {
        if av[i] != bv[i] {
            return format!("[{i}] A={:?} B={:?}", av[i], bv[i]);
        }
    }
    if av.len() != bv.len() {
        format!("length diff {} vs {}", av.len(), bv.len())
    } else {
        "?".to_string()
    }
}
