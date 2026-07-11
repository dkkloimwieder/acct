//! `ledger_close_period` / `ledger_settle_pool` — close-time finalize
//! (design-v3.2 §5e; recalc-e).
//!
//! Under alt C the mid-period GL for FIFO/LIFO pools is qty-only; recalc's
//! authoritative valuation lands continuously and close is a consistency gate
//! plus a finalize stamp, not an adjustment storm (recalc-e §1). One call to
//! `ledger_close_period(period_id, actor, force)` is one transaction:
//!
//! 1. **Closer mutex** — advisory `(32021, period_id)` exclusive serializes
//!    concurrent close callers. Re-closing a closed period returns an
//!    `already_closed` no-op report (idempotent). Periods close in
//!    `start_date` order: an earlier still-open period fails loud — closing
//!    out of order would let a backdate into the open earlier period re-cost
//!    this period's depletions and wedge the engine against the settlement
//!    guard (0017).
//! 2. **Recalc-drain gate** (recalc-e §3, (c)'s G2a scoped to the period):
//!    over fifo/lifo pools with physical activity at or before the period
//!    end, no unsettled physical event ≤ end-of-period and no recost floor
//!    ≤ end-of-period. Gate fail without `force` marks the period `closing`
//!    (appends continue — closing means draining, not frozen) and returns the
//!    per-pool lag + the G2b gross-value magnitude so no forced close is
//!    silent (recalc-e §6); the caller re-invokes.
//! 3. **Drain + sweep** (recalc-e §5/§6). Force never bypasses the gate —
//!    there is no provisional cost leg to fall back on, so force means the
//!    close pays the remaining fold synchronously. Every gate-scoped pool is
//!    swept: claim its queue row (per-pool exclusivity, waiting out workers),
//!    lock its aggregate row (every hot-path FL statement updates the
//!    aggregate first, so no physical event can commit on a swept pool until
//!    this transaction resolves), drain to stream head, then true the
//!    aggregate: `value_sum := Σ open-layer value` in one statement whose
//!    `RETURNING old` yields the residue — banker-rounding residue,
//!    the hot-path exact-empty flush, and uncovered-depletion value — posted
//!    as a single zero-qty `cost_adjustment_line` + `posting_line` against
//!    `posting_account_map.variance_acct` (NULL ⇒ `MissingVarianceAccount`,
//!    fail loud). The residue is only defined at full settlement, which is
//!    why the sweep drains to head even when the period-scoped gate passed
//!    (an unsettled next-period tail's provisional value is not separable
//!    from the residue without replaying the hot-path fold).
//! 4. **Fence + re-check + stamp** (the gate-to-stamp race, closed by
//!    protocol with 0017's trigger): take advisory `(32022, period_id)`
//!    exclusive — this waits out every in-flight insert into the period
//!    (their triggers hold the shared side) and blocks new ones — then
//!    re-run the gate on a fresh snapshot. Stragglers that committed before
//!    the fence can only be on pools this call has not already swept (swept
//!    pools' aggregate rows are held), so one more drain+sweep round
//!    converges; nothing new can land while the fence is held. Then stamp
//!    `closed_at`/`closed_by`, `state → closed`, and commit. Finalize is
//!    stamp-only: the finalized valuation IS the max-generation
//!    `cost_settlement` row per in-period depletion, frozen by 0017
//!    (PeriodClosed, SQLSTATE 55000).
//!
//! Deadlock posture: the common case is clean — normal appends stamp
//! `posted_at = now()`, whose covering period is not the one being closed, so
//! they never touch this fence. A multi-line submission backdating into the
//! closing period while holding another pool's aggregate row can deadlock
//! against the sweep; Postgres' detector aborts one side, immutability is
//! never violated, and either retry converges (the submission finds the
//! period closed and is rejected).
//!
//! Feed-currency assumption (recalc-e §3 premise): the gate reads
//! `pool_settlement` — floors and frontiers the FEED maintains. A backdated
//! event that committed but has not yet been delivered by the slot has no
//! recost floor, sits behind the settled frontier, and is invisible to the
//! gate; the design premise is a continuously running feed (G1 healthy) at
//! close time. The close report carries `feed_lag_bytes` (the G1 gauge) so no
//! caller closes blind to a lagging slot. If the straggler's floor lands
//! after the close, the engine's re-cost attempt fails loud against the 0017
//! settlement guard and the pool alarms on G2a/G2c — mis-valuation is never
//! silent, and resolving it is the out-of-scope reopen workflow (D14).
//!
//! `ledger_settle_pool(pool_id)` is the on-demand SAP shape (recalc-e §7):
//! force-drain ONE pool to its stream head synchronously — the same
//! generation-delta engine as the workers and the close, no sweep (residue is
//! close-time). The three worker-model shapes coexist: continuous workers
//! (`ledger_recalc_step` loops), on-demand (`ledger_settle_pool`), periodic
//! (`ledger_close_period`).

use pgrx::prelude::*;
use serde_json::{json, Value};

use crate::ledger_error_map;
use crate::recalc::{claim_pool, drain_pool_to_head, resolve_accounts, DrainOutcome, DrainStats};
use crate::submit::bail;

/// Advisory keyspace (database-local, int4 pair for pg_locks legibility):
/// closer-vs-closer mutex and the stamp fence of 0017. Period ids must fit
/// int4.
const MUTEX_CLASS: i32 = 32021;
const FENCE_CLASS: i32 = 32022;

/// The fence blocks new activity into the period, so each re-check round can
/// only surface pools that committed before the fence — the loop converges in
/// practice in one round; this is the fail-loud backstop.
const MAX_RECHECK_ROUNDS: usize = 64;

struct Period {
    end: String,
    state: String,
    closed_at: Option<String>,
    closed_by: Option<String>,
}

/// One gate-scoped pool: fifo/lifo with physical activity ≤ end-of-period.
struct GatePool {
    pool_id: i64,
    unsettled_events: i64,
    unsettled_gross: i64,
    floor_pending: bool,
}

impl GatePool {
    fn lagging(&self) -> bool {
        self.unsettled_events > 0 || self.floor_pending
    }
}

struct SweepResult {
    pool_id: i64,
    residue: i64,
    posting_line_id: Option<i64>,
    drain: DrainStats,
}

/// Orchestrated close of one accounting period (recalc-e §2/§8). Returns the
/// close report; never silently forces or silently skips.
#[pg_extern]
fn ledger_close_period(period_id: i64, actor: &str, force: bool) -> pgrx::JsonB {
    let key = advisory_key(period_id);
    advisory_lock(MUTEX_CLASS, key);

    let period = load_period(period_id);
    if period.state == "closed" {
        return pgrx::JsonB(json!({
            "period_id": period_id,
            "closed": true,
            "already_closed": true,
            "closed_at": period.closed_at,
            "closed_by": period.closed_by,
        }));
    }
    reject_out_of_order(period_id);

    let gate = gate_pools(&period.end);
    let lagging: Vec<Value> = gate.iter().filter(|g| g.lagging()).map(gate_json).collect();
    let gate_passed = lagging.is_empty();
    let g2b_total: i64 =
        gate.iter().filter(|g| g.lagging()).map(|g| g.unsettled_gross).sum();
    let feed_lag = feed_lag_bytes();

    if !gate_passed && !force {
        mark_closing(period_id);
        return pgrx::JsonB(json!({
            "period_id": period_id,
            "closed": false,
            "state": "closing",
            "forced": false,
            "feed_lag_bytes": feed_lag,
            "gate": {
                "passed": false,
                "pools_in_scope": gate.len(),
                "lagging": lagging,
                "unsettled_gross_total": g2b_total,
            },
        }));
    }

    // Drain + sweep every gate-scoped pool; then fence, re-check, and absorb
    // any straggler pools until a fresh-snapshot gate is clean.
    let mut adjustment_trx: Option<i64> = None;
    let mut swept: Vec<SweepResult> = Vec::new();
    for g in &gate {
        swept.push(sweep_pool(g.pool_id, &mut adjustment_trx));
    }

    advisory_lock(FENCE_CLASS, key);
    let mut recheck_rounds = 0usize;
    loop {
        let stragglers: Vec<i64> = gate_pools(&period.end)
            .iter()
            .filter(|g| g.lagging() || !swept.iter().any(|s| s.pool_id == g.pool_id))
            .map(|g| g.pool_id)
            .collect();
        if stragglers.is_empty() {
            break;
        }
        recheck_rounds += 1;
        if recheck_rounds > MAX_RECHECK_ROUNDS {
            bail(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!(
                    "ledger_close_period: period {period_id} gate did not quiesce \
                     after {MAX_RECHECK_ROUNDS} re-check rounds"
                ),
            );
        }
        for pool_id in stragglers {
            let result = sweep_pool(pool_id, &mut adjustment_trx);
            match swept.iter_mut().find(|s| s.pool_id == pool_id) {
                Some(prior) => merge_sweep(prior, result),
                None => swept.push(result),
            }
        }
    }

    let closed_at = stamp_closed(period_id, actor);

    let residues: Vec<Value> = swept
        .iter()
        .filter(|s| s.residue != 0)
        .map(|s| {
            json!({
                "pool_id": s.pool_id,
                "residue": s.residue,
                "posting_line_id": s.posting_line_id,
            })
        })
        .collect();
    let residue_abs_total: i64 = swept.iter().map(|s| s.residue.abs()).sum();
    pgrx::JsonB(json!({
        "period_id": period_id,
        "closed": true,
        "state": "closed",
        "forced": force && !gate_passed,
        "closed_at": closed_at,
        "closed_by": actor,
        "feed_lag_bytes": feed_lag,
        "gate": {
            "passed": gate_passed,
            "pools_in_scope": gate.len(),
            "lagging": lagging,
            "unsettled_gross_total": g2b_total,
        },
        "drained": {
            "pools": swept.len(),
            "passes": swept.iter().map(|s| s.drain.passes).sum::<i64>(),
            "settlements_written": swept.iter().map(|s| s.drain.settlements).sum::<i64>(),
            "adjustments_posted": swept.iter().map(|s| s.drain.adjustments).sum::<i64>(),
        },
        "sweep": {
            "pools_swept": swept.len(),
            "residues": residues,
            "residue_abs_total": residue_abs_total,
        },
        "recheck_rounds": recheck_rounds,
    }))
}

/// Force-drain one pool to its stream head synchronously (recalc-e §7, the
/// SAP on-demand shape). Pure drain — the residue sweep is close-time only.
#[pg_extern]
fn ledger_settle_pool(pool_id: i64) -> pgrx::JsonB {
    match drain_pool_to_head(pool_id) {
        DrainOutcome::NonStrict { method } => pgrx::JsonB(json!({
            "pool_id": pool_id,
            "method": method,
            "skipped": "non_strict_method",
        })),
        DrainOutcome::Drained(s) => pgrx::JsonB(json!({
            "pool_id": pool_id,
            "settled": true,
            "passes": s.passes,
            "settlements_written": s.settlements,
            "adjustments_posted": s.adjustments,
            "generation": s.generation,
        })),
    }
}

/// The G1 gauge: slot lag in bytes (NULL when no `ledger_feed` slot exists).
/// Informational in the close report — the gate's `pool_settlement` state is
/// only as current as the feed, so no caller should close blind to a lagging
/// slot.
fn feed_lag_bytes() -> Option<i64> {
    Spi::connect_mut(|client| -> Result<Option<i64>, pgrx::spi::Error> {
        let mut t = client.update("SELECT lag_bytes FROM feed_lag", None, &[])?;
        Ok(t.next().and_then(|row| row.get::<i64>(1).ok().flatten()))
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_close_period: feed gauge failed: {e}"))
    })
}

fn advisory_key(period_id: i64) -> i32 {
    i32::try_from(period_id).unwrap_or_else(|_| {
        bail(
            PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("ledger_close_period: period id {period_id} exceeds the advisory int4 keyspace"),
        )
    })
}

/// Take an exclusive xact-scope advisory lock. Runs as a mutable SPI
/// statement deliberately: pgrx executes read-only SPI on the call-start
/// snapshot until the transaction's first mutable statement, and everything
/// after this lock (period state, gate, re-check) must read fresh snapshots —
/// a concurrent closer's committed stamp, a straggler's committed event.
fn advisory_lock(class: i32, key: i32) {
    Spi::connect_mut(|client| -> Result<(), pgrx::spi::Error> {
        client
            .update("SELECT pg_advisory_xact_lock($1, $2)", None, &[class.into(), key.into()])?;
        Ok(())
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("advisory lock failed: {e}"))
    })
}

fn load_period(period_id: i64) -> Period {
    Spi::connect_mut(|client| -> Result<Option<Period>, pgrx::spi::Error> {
        let mut t = client.update(
            "SELECT end_date::text, state, closed_at::text, closed_by \
               FROM accounting_period WHERE id = $1",
            None,
            &[period_id.into()],
        )?;
        Ok(t.next().map(|row| Period {
            end: row.get::<String>(1).ok().flatten().expect("end_date NOT NULL"),
            state: row.get::<String>(2).ok().flatten().expect("state NOT NULL"),
            closed_at: row.get::<String>(3).ok().flatten(),
            closed_by: row.get::<String>(4).ok().flatten(),
        }))
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_close_period: period load failed: {e}"))
    })
    .unwrap_or_else(|| {
        bail(
            PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            format!("ledger_close_period: accounting_period {period_id} does not exist"),
        )
    })
}

/// Periods close in start_date order (see the module doc); an earlier
/// still-open period fails loud.
fn reject_out_of_order(period_id: i64) {
    let prior = Spi::connect_mut(|client| -> Result<Option<i64>, pgrx::spi::Error> {
        let mut t = client.update(
            "SELECT p.id FROM accounting_period p \
              WHERE p.state <> 'closed' \
                AND p.start_date < (SELECT start_date FROM accounting_period WHERE id = $1) \
              ORDER BY p.start_date LIMIT 1",
            None,
            &[period_id.into()],
        )?;
        Ok(t.next().and_then(|row| row.get::<i64>(1).ok().flatten()))
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_close_period: order check failed: {e}"))
    });
    if let Some(prior) = prior {
        bail(
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            format!(
                "ledger_close_period: accounting_period {prior} is still open; \
                 periods close in start_date order"
            ),
        );
    }
}

/// The gate scope: fifo/lifo pools with any physical activity at or before
/// end-of-period, with their unsettled-tail lag (G2a) and gross value (G2b)
/// clipped to ≤ end-of-period, and whether a recost floor sits at/below the
/// period end. Pre-period activity counts: the period's valuation folds over
/// everything at or before its end.
fn gate_pools(end_date: &str) -> Vec<GatePool> {
    Spi::connect_mut(|client| -> Result<Vec<GatePool>, pgrx::spi::Error> {
        let mut t = client.update(
            "SELECT p.id, lag.cnt, lag.gross, \
                    (ps.recost_floor_posted_at IS NOT NULL \
                     AND ps.recost_floor_posted_at < ($1::date + 1)::timestamptz) \
               FROM pool p \
               LEFT JOIN pool_settlement ps ON ps.pool_id = p.id \
               LEFT JOIN LATERAL ( \
                   SELECT count(*)::bigint AS cnt, \
                          COALESCE(sum(abs(t.qty) * t.unit_cost), 0)::bigint AS gross \
                     FROM trx_line t \
                    WHERE t.pool_id = p.id \
                      AND t.line_type <> 'cost_adjustment_line' \
                      AND t.posted_at < ($1::date + 1)::timestamptz \
                      AND (ps.settled_through_posted_at IS NULL \
                           OR (t.posted_at, t.id) \
                              > (ps.settled_through_posted_at, ps.settled_through_id)) \
               ) lag ON true \
              WHERE p.method IN ('fifo', 'lifo') \
                AND EXISTS (SELECT 1 FROM trx_line t2 \
                             WHERE t2.pool_id = p.id \
                               AND t2.line_type <> 'cost_adjustment_line' \
                               AND t2.posted_at < ($1::date + 1)::timestamptz) \
              ORDER BY p.id",
            None,
            &[end_date.to_string().into()],
        )?;
        let mut out = Vec::new();
        while let Some(row) = t.next() {
            out.push(GatePool {
                pool_id: row.get::<i64>(1)?.expect("pool.id NOT NULL"),
                unsettled_events: row.get::<i64>(2)?.unwrap_or(0),
                unsettled_gross: row.get::<i64>(3)?.unwrap_or(0),
                floor_pending: row.get::<bool>(4)?.unwrap_or(false),
            });
        }
        Ok(out)
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_close_period: gate query failed: {e}"))
    })
}

fn gate_json(g: &GatePool) -> Value {
    json!({
        "pool_id": g.pool_id,
        "unsettled_events": g.unsettled_events,
        "unsettled_gross_value": g.unsettled_gross,
        "recost_floor_pending": g.floor_pending,
    })
}

fn mark_closing(period_id: i64) {
    Spi::connect_mut(|client| -> Result<(), pgrx::spi::Error> {
        client.update(
            "UPDATE accounting_period SET state = 'closing' WHERE id = $1 AND state = 'open'",
            None,
            &[period_id.into()],
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_close_period: mark closing failed: {e}"))
    })
}

fn stamp_closed(period_id: i64, actor: &str) -> String {
    Spi::connect_mut(|client| -> Result<Option<String>, pgrx::spi::Error> {
        let mut t = client.update(
            "UPDATE accounting_period \
                SET state = 'closed', closed_at = now(), closed_by = $2 \
              WHERE id = $1 AND state IN ('open', 'closing') \
              RETURNING closed_at::text",
            None,
            &[period_id.into(), actor.to_string().into()],
        )?;
        Ok(t.next().and_then(|row| row.get::<String>(1).ok().flatten()))
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_close_period: stamp failed: {e}"))
    })
    .unwrap_or_else(|| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_close_period: period {period_id} state changed underneath the closer mutex"),
        )
    })
}

/// Drain one pool to head and true its aggregate to the authoritative layer
/// value, posting the residue to the variance account. Lock order matches the
/// engine workers (queue row, then aggregate row): the queue-row claim is the
/// per-pool fold exclusivity, and the aggregate lock freezes hot-path commits
/// on this pool (every FL statement's first write is the aggregate upsert)
/// until this transaction resolves — which is what makes `value_sum := Σ
/// layer value` exact rather than racing a concurrent append's contribution.
fn sweep_pool(pool_id: i64, adjustment_trx: &mut Option<i64>) -> SweepResult {
    let claim = claim_pool(pool_id);
    if claim.method != "fifo" && claim.method != "lifo" {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_close_period: sweep reached non-strict pool {pool_id}"),
        );
    }
    let has_aggregate = lock_aggregate(pool_id);
    let drain = match drain_pool_to_head(pool_id) {
        DrainOutcome::Drained(s) => s,
        DrainOutcome::NonStrict { .. } => unreachable!("method checked above"),
    };
    if !has_aggregate {
        // No aggregate row means no hot-path activity ever reached the pool;
        // nothing to true up.
        return SweepResult { pool_id, residue: 0, posting_line_id: None, drain };
    }
    let residue = apply_residue(pool_id);
    let posting_line_id =
        if residue != 0 { Some(post_residue_gl(pool_id, residue, adjustment_trx)) } else { None };
    SweepResult { pool_id, residue, posting_line_id, drain }
}

fn merge_sweep(prior: &mut SweepResult, next: SweepResult) {
    prior.residue += next.residue;
    if next.posting_line_id.is_some() {
        prior.posting_line_id = next.posting_line_id;
    }
    prior.drain.passes += next.drain.passes;
    prior.drain.settlements += next.drain.settlements;
    prior.drain.adjustments += next.drain.adjustments;
    prior.drain.generation = next.drain.generation;
}

/// Lock the pool's aggregate row for the rest of this transaction. Returns
/// whether the row exists.
fn lock_aggregate(pool_id: i64) -> bool {
    Spi::connect_mut(|client| -> Result<bool, pgrx::spi::Error> {
        let mut t = client.update(
            "SELECT 1 FROM pool_state WHERE pool_id = $1 AND layer_id = 0 FOR UPDATE",
            None,
            &[pool_id.into()],
        )?;
        Ok(t.next().is_some())
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_close_period: aggregate lock failed: {e}"))
    })
}

/// True the aggregate to the authoritative open-layer value in one statement;
/// `RETURNING old` yields the residue being absorbed (0 when already exact —
/// the guarded WHERE skips the write entirely). Runs after the drain, under
/// the aggregate row lock taken before it.
fn apply_residue(pool_id: i64) -> i64 {
    Spi::connect_mut(|client| -> Result<i64, pgrx::spi::Error> {
        let mut t = client.update(
            "UPDATE pool_state agg \
                SET value_sum = lay.v, \
                    unit_cost = CASE WHEN agg.qty > 0 \
                                     THEN banker_div(lay.v::numeric, agg.qty) \
                                     ELSE agg.unit_cost END \
               FROM (SELECT COALESCE(sum(value_sum), 0)::bigint AS v \
                       FROM pool_state WHERE pool_id = $1 AND layer_id > 0) lay \
              WHERE agg.pool_id = $1 AND agg.layer_id = 0 \
                AND agg.value_sum IS DISTINCT FROM lay.v \
              RETURNING old.value_sum - lay.v",
            None,
            &[pool_id.into()],
        )?;
        Ok(t.next().and_then(|row| row.get::<i64>(1).ok().flatten()).unwrap_or(0))
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_close_period: residue update failed: {e}"))
    })
}

/// Post one residue as a zero-qty `cost_adjustment_line` + `posting_line`.
/// A positive residue means the aggregate book value overstated the
/// authoritative layer value — value leaves inventory into variance; a
/// negative residue reverses. All of a close's residues ride ONE
/// `cost_adjustment` trx, stamped `posted_at = now()` (never a business
/// date — the same feed-loopback rule as the engine's adjustments).
fn post_residue_gl(pool_id: i64, residue: i64, adjustment_trx: &mut Option<i64>) -> i64 {
    let accounts = resolve_accounts(pool_id);
    let Some(variance) = accounts.variance else {
        ledger_error_map::raise_ledger_error(ledger_core::LedgerError::MissingVarianceAccount {
            pool_id,
        });
        unreachable!()
    };
    let inventory = accounts.receipt.0;
    let (debit, credit) = if residue > 0 { (variance, inventory) } else { (inventory, variance) };
    let amount = residue.checked_abs().unwrap_or_else(|| {
        bail(
            PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("ledger_close_period: residue overflows bigint (pool {pool_id})"),
        )
    });

    Spi::connect_mut(|client| -> Result<i64, pgrx::spi::Error> {
        let trx_id = match *adjustment_trx {
            Some(id) => id,
            None => {
                let id = client
                    .update(
                        "INSERT INTO trx (trx_type, source_id, posted_at) \
                         VALUES ('cost_adjustment', nextval('cost_adjustment_id_seq'), now()) \
                         RETURNING id",
                        None,
                        &[],
                    )?
                    .first()
                    .get::<i64>(1)?
                    .expect("trx insert returns id");
                *adjustment_trx = Some(id);
                id
            }
        };
        let posting_line_id = client
            .update(
                "WITH tl AS ( \
                    INSERT INTO trx_line \
                           (trx_id, pool_id, line_type, source_id, qty, unit_cost, \
                            source_trx_line_id, posted_at) \
                    VALUES ($1, $2, 'cost_adjustment_line', NULL, 0, 0, NULL, now()) \
                    RETURNING id \
                 ) \
                 INSERT INTO posting_line \
                        (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
                 SELECT tl.id, 'cost_adjustment', $3, $4, $5, now() FROM tl \
                 RETURNING id",
                None,
                &[trx_id.into(), pool_id.into(), amount.into(), debit.into(), credit.into()],
            )?
            .first()
            .get::<i64>(1)?
            .expect("posting_line insert returns id");
        Ok(posting_line_id)
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_close_period: residue GL post failed: {e}"))
    })
}
