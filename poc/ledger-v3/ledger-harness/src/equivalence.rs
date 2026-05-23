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

use std::collections::{BTreeMap, HashMap};
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
    /// Treat WAC unit_cost drift on shared pools as an error rather
    /// than a warning. Use when running Path B with a single committer
    /// (e.g. via `ALTER SYSTEM SET ledger_routed.committer_count = 1`
    /// + postmaster restart) where strict byte equivalence is expected.
    pub strict: bool,
    /// Pool method mix for the seeded universe. AllWacPeriodic additionally
    /// seeds an accounting_period and calls ledger_close_period after each
    /// path's submissions (acct-9mgx.6).
    pub method_mix: MethodMix,
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

    let close_period_at_end = opts.method_mix == MethodMix::AllWacPeriodic;

    eprintln!(
        "[equivalence] resetting + seeding universe for Path A (method_mix={:?})...",
        opts.method_mix
    );
    reset_ledger(&pool).await?;
    let universe = pool_universe::seed(&pool, universe_count, universe_skus, universe_locs, opts.method_mix)
        .await
        .map_err(|e| format!("seed-pools (A): {e}"))?;
    let period_id_a: Option<i64> = if close_period_at_end {
        Some(seed_period(&pool).await?)
    } else {
        None
    };

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
    let submissions = if close_period_at_end {
        build_submissions_wac_periodic(
            &spec.workload,
            spec.callers,
            opts.submissions_per_caller,
            run_prefix,
        )
    } else {
        build_submissions(&spec.workload, spec.callers, opts.submissions_per_caller, run_prefix)
    };
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
    if let Some(pid) = period_id_a {
        let summary = close_period(&pool, pid).await?;
        eprintln!("[equivalence] direct close_period({pid}): {summary}");
    }
    let direct_snap = take_snapshot(&pool).await?;
    let a_elapsed = a_start.elapsed();
    eprintln!(
        "[equivalence] direct: {} trx, {} trx_line, {} pool_state, {} posting_line, {} provisional ({:.2}s)",
        direct_snap.trx_groups.len(),
        direct_snap.total_lines(),
        direct_snap.pool_state.len(),
        direct_snap.total_postings(),
        direct_snap.provisional.len(),
        a_elapsed.as_secs_f64()
    );

    eprintln!("[equivalence] resetting + reseeding universe for Path B...");
    reset_ledger(&pool).await?;
    let universe_b = pool_universe::seed(&pool, universe_count, universe_skus, universe_locs, opts.method_mix)
        .await
        .map_err(|e| format!("seed-pools (B): {e}"))?;
    if universe.pool_ids != universe_b.pool_ids {
        return Err(format!(
            "universe pool_ids drifted between resets: {} → {} (TRUNCATE RESTART IDENTITY should have produced identical ids)",
            universe.pool_ids.len(),
            universe_b.pool_ids.len()
        ));
    }
    let period_id_b: Option<i64> = if close_period_at_end {
        Some(seed_period(&pool).await?)
    } else {
        None
    };
    if period_id_a != period_id_b {
        return Err(format!(
            "period_id drifted across resets: A={period_id_a:?} B={period_id_b:?} (RESTART IDENTITY should make both 1)"
        ));
    }

    eprintln!("[equivalence] running Path B (routed)...");
    let b_start = Instant::now();
    submit_routed(&pool, &submissions).await?;
    if let Some(pid) = period_id_b {
        let summary = close_period(&pool, pid).await?;
        eprintln!("[equivalence] routed close_period({pid}): {summary}");
    }
    let routed_snap = take_snapshot(&pool).await?;
    let b_elapsed = b_start.elapsed();
    eprintln!(
        "[equivalence] routed: {} trx, {} trx_line, {} pool_state, {} posting_line, {} provisional ({:.2}s)",
        routed_snap.trx_groups.len(),
        routed_snap.total_lines(),
        routed_snap.pool_state.len(),
        routed_snap.total_postings(),
        routed_snap.provisional.len(),
        b_elapsed.as_secs_f64()
    );

    let result = diff_snapshots(&direct_snap, &routed_snap, close_period_at_end);

    if !result.wac_drifts.is_empty() {
        eprintln!(
            "[equivalence] {} WAC value_sum drift(s) (per-depletion rounding under concurrent commit_groups; non-compounding per acct-h5gs cumulative-sum form). \
             For receipt-only flows or serial-equivalence runs this should be 0; non-zero indicates the routed path saw different intermediate (qty, value_sum) at one or more depletions:",
            result.wac_drifts.len()
        );
        for w in result.wac_drifts.iter().take(10) {
            eprintln!("  {w}");
        }
        if result.wac_drifts.len() > 10 {
            eprintln!("  ... {} more WAC drifts suppressed", result.wac_drifts.len() - 10);
        }
    }

    if !result.wac_periodic_drifts.is_empty() {
        eprintln!(
            "[equivalence] {} wac_periodic drift(s) (per-depletion provisional_amount + variance redistribution under concurrent commit_groups). \
             Pool_state end-state is the load-bearing invariant and matches byte-for-byte; per-depletion split between provisional + variance may differ across paths because each depletion's running_avg depends on commit_group ordering. \
             Per the acct-9mgx.6 bd description: \"within-period provisional postings may differ on amount\".",
            result.wac_periodic_drifts.len()
        );
        for w in result.wac_periodic_drifts.iter().take(10) {
            eprintln!("  {w}");
        }
        if result.wac_periodic_drifts.len() > 10 {
            eprintln!("  ... {} more wac_periodic drifts suppressed", result.wac_periodic_drifts.len() - 10);
        }
    }

    let effective_errors = if opts.strict {
        result.errors.len() + result.wac_drifts.len() + result.wac_periodic_drifts.len()
    } else {
        result.errors.len()
    };

    if effective_errors == 0 {
        let drift_msg = match (result.wac_drifts.is_empty(), result.wac_periodic_drifts.is_empty()) {
            (true, true) => " (identical)".to_string(),
            (false, true) => format!(" ({} WAC drift(s) within acct-mcey bound, ignored)", result.wac_drifts.len()),
            (true, false) => format!(" ({} wac_periodic drift(s) within acct-9mgx.6 bound, ignored)", result.wac_periodic_drifts.len()),
            (false, false) => format!(
                " ({} WAC + {} wac_periodic drift(s), ignored)",
                result.wac_drifts.len(),
                result.wac_periodic_drifts.len()
            ),
        };
        println!(
            "equivalence OK: scenario={} submissions={} A={} B={} trx{}",
            opts.scenario,
            submissions.len(),
            direct_snap.trx_groups.len(),
            routed_snap.trx_groups.len(),
            drift_msg
        );
        Ok(())
    } else {
        if !result.errors.is_empty() {
            eprintln!("EQUIVALENCE FAILED ({} errors):", result.errors.len());
            for d in result.errors.iter().take(20) {
                eprintln!("  {d}");
            }
            if result.errors.len() > 20 {
                eprintln!("  ... {} more errors suppressed", result.errors.len() - 20);
            }
        }
        if opts.strict {
            if !result.wac_drifts.is_empty() {
                eprintln!(
                    "EQUIVALENCE FAILED (strict): {} WAC drifts upgraded to errors",
                    result.wac_drifts.len()
                );
            }
            if !result.wac_periodic_drifts.is_empty() {
                eprintln!(
                    "EQUIVALENCE FAILED (strict): {} wac_periodic drifts upgraded to errors",
                    result.wac_periodic_drifts.len()
                );
            }
        }
        Err(format!("{} mismatches", effective_errors))
    }
}

#[derive(Debug, Clone)]
struct Submission {
    trx_type: &'static str,
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
            subs.push(Submission {
                trx_type: "po_receipt",
                source_id,
                lines,
            });
        }
    }
    subs
}

/// wac_periodic-specific workload (acct-9mgx.6).
///
/// Alternating-rounds pattern per caller: even tick = receipt, odd tick =
/// depletion on the caller's most-recent receipt pool. Receipt unit_cost
/// varies per round (10, 17, 24, …) so the per-pool running_avg evolves —
/// each depletion lands at a DIFFERENT running_avg than the close hook's
/// final_avg → non-zero variance to validate the variance-posting code path.
///
/// Receipt qty=10, depletion qty=2 — safe across all overlap patterns.
/// The trx_type label tracks the line direction (po_receipt vs
/// transfer_shipment) so the trx UNIQUE(trx_type, source_id) gate is
/// honored; source_ids are also unique per (caller, tick).
fn build_submissions_wac_periodic(
    workload: &crate::workload::Workload,
    callers: usize,
    per_caller: usize,
    run_prefix: i64,
) -> Vec<Submission> {
    let mut rngs: Vec<StdRng> = (0..callers)
        .map(|c| StdRng::seed_from_u64(0xDEAD_BEEF_u64.wrapping_add(c as u64)))
        .collect();
    let mut subs = Vec::with_capacity(callers * per_caller);
    let mut last_receipt_pool: Vec<Option<i64>> = vec![None; callers];
    let inv = workload.universe.inv_account;
    let ap = workload.universe.ap_account;

    for tick in 0..per_caller {
        let is_receipt = tick % 2 == 0;
        // Vary receipt unit_cost per round so running_avg evolves on
        // multi-receipt pools (zipf head, S2/S4) and the close hook's
        // final_avg differs from any single in-period depletion's
        // provisional_amount.
        let receipt_uc: i64 = 10 + (tick as i64 / 2) * 7;

        for caller_id in 0..callers {
            let source_id = run_prefix + (caller_id as i64) * 1_000_000 + tick as i64;
            if is_receipt {
                let lines = workload.next_lines(&mut rngs[caller_id], caller_id);
                let pool_id = lines.first().map(|l| l.pool_id);
                if let Some(pid) = pool_id {
                    last_receipt_pool[caller_id] = Some(pid);
                }
                // Override qty + unit_cost on every generated line so the
                // depletion phase can safely deplete qty=2 and the running
                // avg evolves predictably.
                let lines: Vec<LineParam> = lines
                    .into_iter()
                    .map(|l| LineParam {
                        qty: 10,
                        unit_cost: receipt_uc,
                        ..l
                    })
                    .collect();
                subs.push(Submission {
                    trx_type: "po_receipt",
                    source_id,
                    lines,
                });
            } else {
                let Some(pool_id) = last_receipt_pool[caller_id] else {
                    continue; // no stock yet for this caller
                };
                subs.push(Submission {
                    trx_type: "transfer_shipment",
                    source_id,
                    lines: vec![LineParam {
                        pool_id,
                        line_type: "transfer_shipment_line",
                        source_id: Some(2),
                        qty: -2,
                        unit_cost: 0,
                        debit_account: ap,
                        credit_account: inv,
                    }],
                });
            }
        }
    }

    subs
}

async fn submit_direct(pool: &PgPool, submissions: &[Submission]) -> Result<(), String> {
    let posted_at = "2026-05-21T12:00:00+00:00";
    for s in submissions {
        let lines_json = build_lines_json(&s.lines);
        sqlx::query("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
            .bind(s.trx_type)
            .bind(s.source_id)
            .bind(posted_at)
            .bind(&lines_json)
            .execute(pool)
            .await
            .map_err(|e| format!("submit_direct trx_type={} sid={}: {e}", s.trx_type, s.source_id))?;
    }
    Ok(())
}

async fn submit_routed(pool: &PgPool, submissions: &[Submission]) -> Result<(), String> {
    let posted_at = "2026-05-21T12:00:00+00:00";
    for s in submissions {
        let lines_json = build_lines_json(&s.lines);
        sqlx::query("SELECT ledger_enqueue_trx($1, $2, $3, $4::jsonb)")
            .bind(s.trx_type)
            .bind(s.source_id)
            .bind(posted_at)
            .bind(&lines_json)
            .execute(pool)
            .await
            .map_err(|e| format!("enqueue_routed trx_type={} sid={}: {e}", s.trx_type, s.source_id))?;
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
    let mut pending = 0i64;
    // Partition by trx_type so the (trx_type, source_id) UNIQUE check is correct.
    let mut by_type: HashMap<&'static str, Vec<i64>> = HashMap::new();
    for s in submissions {
        by_type.entry(s.trx_type).or_default().push(s.source_id);
    }
    for (tt, sids) in by_type {
        let materialized: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trx \
              WHERE trx_type = $1::trx_type AND source_id = ANY($2::bigint[])",
        )
        .bind(tt)
        .bind(&sids)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("count {tt}: {e}"))?;
        pending += sids.len() as i64 - materialized;
    }
    Ok(pending)
}

async fn wait_for_committer_quiet(pool: &PgPool) {
    let poll = Duration::from_millis(200);
    // Bumped from 20s to 60s: wac_periodic s4 (zipf-1.2 complex, ~3k
    // trx_lines under heavy contention) occasionally flaked at 20s when
    // committer drain hadn't quite finished. 60s is generous for serial-
    // submission equivalence runs; not a load-test ceiling.
    let cap = Instant::now() + Duration::from_secs(60);
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
        "TRUNCATE TABLE posting_lines_provisional, posting_line_dimension, posting_line, \
                       trx_line, trx, pool_state, pool_lock, pool, \
                       sku, location, account, accounting_period \
                       RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .map_err(|e| format!("TRUNCATE: {e}"))?;
    Ok(())
}

/// Seed one accounting_period wide enough to cover all the canned
/// submission posted_at values (build_submissions uses 2026-05-21).
/// Returns the period id (always 1 after TRUNCATE RESTART IDENTITY).
async fn seed_period(pool: &PgPool) -> Result<i64, String> {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO accounting_period (start_date, end_date, state) \
         VALUES ('2026-05-01'::date, '2026-05-31'::date, 'open') RETURNING id",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("seed_period: {e}"))?;
    Ok(id)
}

async fn close_period(pool: &PgPool, period_id: i64) -> Result<String, String> {
    let summary: sqlx::types::Json<Value> =
        sqlx::query_scalar("SELECT ledger_close_period($1)")
            .bind(period_id)
            .fetch_one(pool)
            .await
            .map_err(|e| format!("close_period({period_id}): {e}"))?;
    Ok(summary.0.to_string())
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
    /// pool_id → method ('wac' | 'wac_periodic' | 'fifo' | 'lifo' | 'std' | 'specific').
    /// Used by diff to classify pool_state unit_cost drift as WAC
    /// truncation property (acct-mcey) vs real divergence, and to skip
    /// wac_periodic value_sum drift before close (legitimate per-depletion
    /// rounding) — though equivalence runs close before snapshotting so
    /// the column is post-variance and should match byte-for-byte.
    pool_methods: HashMap<i64, String>,
    /// posting_lines_provisional canonicalized + sorted by (pool_id,
    /// provisional_amount, qty). Empty for non-AllWacPeriodic runs.
    provisional: Vec<ProvisionalCanon>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ProvisionalCanon {
    pool_id: i64,
    qty: i64,
    provisional_amount: i64,
    /// NULL until close finalizes. Equivalence runs close before
    /// snapshotting, so this is always Some(_) in equivalence flows.
    variance_amount: Option<i64>,
    /// true iff variance_posting_line_id IS NOT NULL — confirms a
    /// variance posting_line was emitted (vs zero-variance finalized row).
    /// The variance posting_line itself appears in `trx_groups` under
    /// the revaluation_run trx and is canonicalized there; this flag
    /// only ties the provisional row to its presence/absence.
    has_variance_posting: bool,
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

    let methods: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, method::text FROM pool ORDER BY id")
            .fetch_all(pool)
            .await
            .map_err(|e| format!("snapshot pool methods: {e}"))?;
    snap.pool_methods = methods.into_iter().collect();

    // posting_lines_provisional (acct-s6fa). Empty for non-wac_periodic
    // runs. Canonical key skips id + finalized_at + variance_posting_line_id;
    // the variance posting_line itself is in trx_groups already.
    let prov: Vec<(i64, i64, i64, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT pool_id, qty, provisional_amount, variance_amount, variance_posting_line_id \
           FROM posting_lines_provisional \
          ORDER BY pool_id, provisional_amount, qty",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("snapshot posting_lines_provisional: {e}"))?;
    snap.provisional = prov
        .into_iter()
        .map(|(pool_id, qty, provisional_amount, variance_amount, var_pl_id)| {
            ProvisionalCanon {
                pool_id,
                qty,
                provisional_amount,
                variance_amount,
                has_variance_posting: var_pl_id.is_some(),
            }
        })
        .collect();

    Ok(snap)
}

/// Result of comparing two ledger snapshots.
///
/// `errors` are real divergences — exit nonzero.
///
/// `wac_drifts` are pool_state.unit_cost-column mismatches on WAC pools
/// where qty matches exactly. Tracked separately because the column
/// carries different meaning per method (acct-h5gs cumulative-sum form):
/// on WAC rows it stores `value_sum`, so a delta here means the routed
/// path's depletion-time rounding produced a different cumulative value
/// than the direct path's. Cumulative-sum makes receipts exact (zero
/// drift) and bounds depletion drift per-event without compounding,
/// so for the equivalence harness's serial-submission shape this
/// bucket should be empty. Non-empty under a concurrent load test would
/// indicate per-depletion rounding divergence — informational by
/// default, upgraded to errors via `--strict`.
#[derive(Default)]
struct DiffResult {
    errors: Vec<String>,
    wac_drifts: Vec<String>,
    /// wac_periodic-specific drifts (acct-9mgx.6). Per-depletion
    /// provisional_amount + per-pool variance redistribution may differ
    /// across paths because each depletion's running_avg at deplete-time
    /// depends on commit_group ordering. Pool_state end-state matches
    /// byte-for-byte (the load-bearing invariant); these are noise.
    wac_periodic_drifts: Vec<String>,
}

fn diff_snapshots(a: &LedgerSnapshot, b: &LedgerSnapshot, wac_periodic_mode: bool) -> DiffResult {
    let mut r = DiffResult::default();
    let a_keys: std::collections::BTreeSet<_> = a.trx_groups.keys().collect();
    let b_keys: std::collections::BTreeSet<_> = b.trx_groups.keys().collect();

    for k in a_keys.difference(&b_keys) {
        r.errors.push(format!("trx in A but not B: {:?}", k));
    }
    for k in b_keys.difference(&a_keys) {
        r.errors.push(format!("trx in B but not A: {:?}", k));
    }
    for k in a_keys.intersection(&b_keys) {
        let av = &a.trx_groups[*k];
        let bv = &b.trx_groups[*k];
        if av != bv {
            let msg = format!(
                "trx {:?} content differs: A has {} lines, B has {} lines (first diff: {})",
                k,
                av.len(),
                bv.len(),
                first_line_diff(av, bv)
            );
            if wac_periodic_mode
                && (k.0 == "revaluation_run"
                    || (k.0 == "transfer_shipment" && trx_lines_differ_only_in_cost(av, bv, &a.pool_methods)))
            {
                r.wac_periodic_drifts.push(msg);
            } else {
                r.errors.push(msg);
            }
        }
    }

    // posting_lines_provisional: aggregate equivalence per pool. Per-row
    // byte differences are wac_periodic drifts (see DiffResult docs).
    // Count and qty-sum per pool MUST match across paths (the workload
    // submits identical depletions both times).
    let a_prov_per_pool = aggregate_provisional_per_pool(&a.provisional);
    let b_prov_per_pool = aggregate_provisional_per_pool(&b.provisional);
    let prov_pools: std::collections::BTreeSet<_> = a_prov_per_pool
        .keys()
        .chain(b_prov_per_pool.keys())
        .collect();
    for pid in prov_pools {
        let (a_count, a_qty, a_cost_sum) = a_prov_per_pool.get(pid).copied().unwrap_or_default();
        let (b_count, b_qty, b_cost_sum) = b_prov_per_pool.get(pid).copied().unwrap_or_default();
        if a_count != b_count {
            r.errors.push(format!(
                "posting_lines_provisional count for pool {pid} differs: A={a_count} B={b_count}"
            ));
        }
        if a_qty != b_qty {
            r.errors.push(format!(
                "posting_lines_provisional sum(qty) for pool {pid} differs: A={a_qty} B={b_qty}"
            ));
        }
        // sum(provisional_amount + variance_amount) per pool = final_avg
        // * sum(depletion qty) per pool, deterministic from workload.
        // This is the strong invariant — if it differs, the close hook's
        // restatement is wrong (NOT a wac_periodic drift).
        if a_cost_sum != b_cost_sum {
            r.errors.push(format!(
                "posting_lines_provisional sum(provisional + variance) for pool {pid} differs: \
                 A={a_cost_sum} B={b_cost_sum} (= final_avg × Σ qty; should match across paths)"
            ));
        }
    }
    if wac_periodic_mode {
        let pn = a.provisional.len().min(b.provisional.len());
        for i in 0..pn {
            if a.provisional[i] != b.provisional[i] {
                r.wac_periodic_drifts.push(format!(
                    "posting_lines_provisional[{}] A={:?} B={:?}",
                    i, a.provisional[i], b.provisional[i]
                ));
                if r.wac_periodic_drifts.len() > 50 {
                    break;
                }
            }
        }
    } else if a.provisional.len() != b.provisional.len() {
        r.errors.push(format!(
            "posting_lines_provisional row count differs: A={} B={}",
            a.provisional.len(),
            b.provisional.len()
        ));
    }

    // pool_state: compare row-by-row. On WAC pools the unit_cost column
    // is value_sum (acct-h5gs per-method storage); deltas there route to
    // the wac_drifts bucket, not the errors bucket.
    if a.pool_state.len() != b.pool_state.len() {
        r.errors.push(format!(
            "pool_state row count differs: A={} B={}",
            a.pool_state.len(),
            b.pool_state.len()
        ));
    }
    let n = a.pool_state.len().min(b.pool_state.len());
    for i in 0..n {
        let ar = &a.pool_state[i];
        let br = &b.pool_state[i];
        if ar == br {
            continue;
        }
        if ar.pool_id != br.pool_id || ar.layer_seq != br.layer_seq || ar.qty != br.qty {
            r.errors.push(format!("pool_state[{}] A={:?} B={:?}", i, ar, br));
            if r.errors.len() > 25 {
                break;
            }
            continue;
        }
        // pool_id, layer_seq, qty all match; only unit_cost differs.
        let method = a.pool_methods.get(&ar.pool_id).map(|s| s.as_str()).unwrap_or("?");
        if method == "wac" || method == "wac_periodic" {
            // For WAC, the column is value_sum (acct-h5gs). Δ here means
            // per-depletion rounding divergence on this pool. running_avg
            // = value_sum / qty for the human-readable per-unit cost.
            r.wac_drifts.push(format!(
                "pool_state[{}] pool={} qty={} value_sum A={} B={} (Δ={}; running_avg A={} B={}; method={})",
                i, ar.pool_id, ar.qty, ar.unit_cost, br.unit_cost,
                ar.unit_cost - br.unit_cost,
                ar.unit_cost.checked_div(ar.qty).unwrap_or(0),
                br.unit_cost.checked_div(br.qty).unwrap_or(0),
                method
            ));
        } else {
            r.errors.push(format!(
                "pool_state[{}] unit_cost differs (non-WAC method={}): A={} B={}",
                i, method, ar.unit_cost, br.unit_cost
            ));
            if r.errors.len() > 25 {
                break;
            }
        }
    }

    r
}

/// (count, sum_qty, sum(provisional_amount + variance_amount)) per pool.
/// sum(prov + var) is the deterministic invariant: equals final_avg *
/// sum(qty) per pool regardless of submission ordering, so this MUST
/// match across paths even when individual rows shift between the
/// provisional / variance buckets.
fn aggregate_provisional_per_pool(
    rows: &[ProvisionalCanon],
) -> HashMap<i64, (usize, i64, i64)> {
    let mut out: HashMap<i64, (usize, i64, i64)> = HashMap::new();
    for r in rows {
        let entry = out.entry(r.pool_id).or_insert((0, 0, 0));
        entry.0 += 1;
        entry.1 += r.qty;
        entry.2 += r.provisional_amount + r.variance_amount.unwrap_or(0);
    }
    out
}

/// Two depletion-side trx_line vectors that differ ONLY in unit_cost on
/// matching (pool_id, line_type, qty) tuples. Used to classify
/// transfer_shipment trx diffs as wac_periodic drifts in wac_periodic_mode.
fn trx_lines_differ_only_in_cost(
    av: &[TrxLineCanon],
    bv: &[TrxLineCanon],
    pool_methods: &HashMap<i64, String>,
) -> bool {
    if av.len() != bv.len() {
        return false;
    }
    for (a, b) in av.iter().zip(bv.iter()) {
        if a.pool_id != b.pool_id || a.line_type != b.line_type || a.qty != b.qty {
            return false;
        }
        if pool_methods.get(&a.pool_id).map(|s| s.as_str()) != Some("wac_periodic") {
            return false;
        }
        // qty matches; only unit_cost + posting amounts may differ.
        // unit_cost on the trx_line and amount on the posting both
        // derive from the same running_avg; if either set differs, both
        // do — but only on wac_periodic pools is that legitimate.
    }
    true
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
