//! `ledger_recalc_step` — one claim-driven recalc pass (design-v3.2 §5;
//! recalc-a/b/c/d).
//!
//! The recalc engine is the SOLE costing plane for FIFO/LIFO pools (alt C,
//! design-v3.1 §16): their hot-path appends record an observed provisional
//! cost and post no GL. Each call to this function is one worker tick:
//!
//! 1. **Claim** ONE dirty pool from `recalc_queue` (`FOR UPDATE SKIP LOCKED`,
//!    oldest mark first — recalc-b D9). The queue-row lock is the per-pool
//!    exclusivity the serial fold requires (recalc-b §3). Non-FIFO/LIFO pools
//!    are drained as free no-ops (the feed is method-agnostic; recalc-d D6).
//! 2. **Replay** the pool's physical events in R-1 `(posted_at, id)` order
//!    through `ledger_core::{fifo,lifo}::strict_fold`. A clean pool replays
//!    incrementally from the persisted layer state at `settled_through` (the
//!    live checkpoint — recalc-b D2/D4, O(new events)); a pool with a recost
//!    floor set (a backdated event, R-2) or no frontier yet replays from
//!    opening (the oracle-equivalent correctness baseline; no historical
//!    checkpoints in v3.2 — recalc-c D11). `cost_adjustment_line` rows are the
//!    engine's own output and are never replay input.
//! 3. **Write generation N** (recalc-d §5, Model 1) in this same transaction:
//!    a `cost_settlement` + `cost_layer_consumption` row set per depletion
//!    that is newly costed or whose authoritative cost changed; ONE
//!    `cost_adjustment` trx wrapping a `cost_adjustment_line` + `posting_line`
//!    per nonzero delta `(authoritative − prior) × qty`, routed through the
//!    depletion's own operation pair from `posting_account_map` (swapped to
//!    depletion direction; a negative delta reverses). The adjustment trx is
//!    stamped `posted_at = now()` — NEVER a business date — so its own feed
//!    delivery can't lower the recost floor and loop the pool. Zero-delta
//!    passes write nothing and do not bump the generation (D6).
//! 4. **Materialize** the open layers (`pool_state.layer_id > 0`, the next
//!    pass's checkpoint) and fold the net GL delta into the aggregate row's
//!    `value_sum` (provisional net → authoritative net, recalc-d §6).
//! 5. **Settle**: advance `settled_through_*`, bump the generation iff
//!    anything was written, and clear the recost floor ONLY if it still equals
//!    the claimed value — a feed batch may have lowered it mid-pass, and that
//!    newer floor must survive for the next pass.
//! 6. **Queue decision** (final statements, freshest snapshot): drop the
//!    claim's queue row unless the floor survived, a fresh tail exists past
//!    the new frontier, or an unscanned mid-pass commit landed INSIDE the
//!    replayed range (that one lowers the floor ourselves — its feed mark hit
//!    our claim-locked row's DO NOTHING and its position is behind the new
//!    frontier, so nothing else would recover it). A kept row is re-stamped to
//!    the back of the line so a hot pool cannot starve its siblings.
//!
//! Crash safety: the whole pass is the caller's transaction. An abort leaves
//! the queue row, the floor, and generation N-1 untouched; the retry recomputes
//! the same deterministic pass (recalc-d §5).

use ledger_core::{DepletionCosting, StrictEvent, StrictLayer};
use pgrx::prelude::*;
use serde_json::json;

use crate::ledger_error_map;
use crate::submit::bail;
use ledger_spi_common::line_type::decode_line_type;

/// The claimed pool + its settlement state. Timestamps travel as Postgres text
/// (`::text` round-trips exactly through `::timestamptz`); the engine never
/// interprets them — ordering and comparison stay in SQL.
pub(crate) struct Claim {
    pub(crate) pool_id: i64,
    pub(crate) method: String,
    generation: i64,
    settled_at: Option<String>,
    settled_id: Option<i64>,
    floor_at: Option<String>,
    floor_id: Option<i64>,
}

/// What one claimed pass did — the JSON-report fields of `ledger_recalc_step`,
/// shared with the targeted-drain callers (close / settle_pool).
pub(crate) struct PassOutcome {
    pub(crate) full_replay: bool,
    pub(crate) events: usize,
    pub(crate) settlements: usize,
    pub(crate) adjustments: usize,
    pub(crate) generation: i64,
    pub(crate) floor_pending: bool,
    pub(crate) requeued: bool,
}

/// Accumulated stats of a synchronous drain-to-head.
#[derive(Default)]
pub(crate) struct DrainStats {
    pub(crate) passes: i64,
    pub(crate) settlements: i64,
    pub(crate) adjustments: i64,
    pub(crate) generation: i64,
}

/// Outcome of a targeted drain: non-strict pools have nothing to fold (their
/// costing is the synchronous hot path), everything else drains to head.
pub(crate) enum DrainOutcome {
    NonStrict { method: String },
    Drained(DrainStats),
}

/// A synchronous drain in one loop never quiesces this slowly except under a
/// livelock (e.g. sustained concurrent appends outpacing the fold); fail loud
/// rather than spin forever inside the caller's transaction.
const MAX_DRAIN_PASSES: i64 = 10_000;

/// One physical event from the R-1 ordered scan.
struct Scanned {
    id: i64,
    qty: i64,
    unit_cost: i64,
    line_type: String,
    posted_at: String,
}

/// A depletion whose generation-N rows will be written this pass.
struct Pending {
    costing: DepletionCosting,
    line_type: String,
    prior_unit_cost: i64,
    /// (authoritative − prior) × qty, i128 against overflow.
    delta: i128,
}

/// One recalc worker tick. Returns a JSONB report:
/// `{"claimed": false}` when the queue is empty; otherwise pool, method,
/// replay shape, and write counts. Loop it from N connections for N workers
/// (continuous cadence, recalc-c §3).
#[pg_extern]
fn ledger_recalc_step() -> pgrx::JsonB {
    let Some(claim) = claim_next() else {
        return pgrx::JsonB(json!({"claimed": false}));
    };

    if claim.method != "fifo" && claim.method != "lifo" {
        drop_queue_row(claim.pool_id);
        return pgrx::JsonB(json!({
            "claimed": true,
            "pool_id": claim.pool_id,
            "method": claim.method,
            "skipped": "non_strict_method",
        }));
    }

    let out = run_claimed_pass(&claim);
    pgrx::JsonB(json!({
        "claimed": true,
        "pool_id": claim.pool_id,
        "method": claim.method,
        "full_replay": out.full_replay,
        "events": out.events,
        "settlements_written": out.settlements,
        "adjustments_posted": out.adjustments,
        "generation": out.generation,
        "floor_pending": out.floor_pending,
        "requeued": out.requeued,
    }))
}

/// One serial fold + generation write for an already-claimed fifo/lifo pool.
/// The caller holds the pool's `recalc_queue` row lock (the per-pool
/// exclusivity) for the rest of its transaction.
pub(crate) fn run_claimed_pass(claim: &Claim) -> PassOutcome {
    // R-2: any floor forces full-opening replay (the one live checkpoint sits
    // at settled_through, which is always ABOVE the floor — recalc-b D4).
    let full_replay = claim.floor_at.is_some() || claim.settled_at.is_none();

    let opening = if full_replay { Vec::new() } else { hydrate_layers(claim.pool_id) };
    let scanned = scan_events(
        claim.pool_id,
        if full_replay { None } else { Some((claim.settled_at.clone(), claim.settled_id)) },
    );

    let events: Vec<StrictEvent> = scanned
        .iter()
        .map(|s| StrictEvent { trx_line_id: s.id, qty: s.qty, unit_cost: s.unit_cost })
        .collect();
    let fold = match claim.method.as_str() {
        "fifo" => ledger_core::fifo::strict_fold(opening, &events),
        _ => ledger_core::lifo::strict_fold(opening, &events),
    };

    // Generation-delta write set: a depletion writes gen-N rows iff it has no
    // authoritative record yet (prior = its observed cost) or its
    // authoritative cost moved (prior = the gen-N-1 authoritative).
    let priors = load_priors(&fold.costings);
    let mut pending: Vec<Pending> = Vec::new();
    for c in fold.costings {
        let (prior, is_new) = match priors.iter().find(|(id, _)| *id == c.depletion_trx_line_id) {
            Some((_, p)) => (*p, false),
            None => (c.observed_unit_cost, true),
        };
        if is_new || c.authoritative_unit_cost != prior {
            let delta = (c.authoritative_unit_cost - prior) as i128 * c.qty as i128;
            let line_type = scanned
                .iter()
                .find(|s| s.id == c.depletion_trx_line_id)
                .map(|s| s.line_type.clone())
                .unwrap_or_else(|| {
                    // Full replay re-costs depletions below the incremental
                    // window only when full_replay scanned them all.
                    bail(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        format!(
                            "ledger_recalc_step: costed depletion {} missing from scan",
                            c.depletion_trx_line_id
                        ),
                    )
                });
            pending.push(Pending { costing: c, line_type, prior_unit_cost: prior, delta });
        }
    }

    let wrote = !pending.is_empty();
    let generation = claim.generation + i64::from(wrote);
    let mut adjustments = 0usize;
    if wrote {
        adjustments = write_generation(claim.pool_id, generation, &pending);
    }

    // The persisted layers ARE the next pass's checkpoint; rewrite them
    // whenever the replay advanced state. Only this engine writes FIFO/LIFO
    // layer rows, and the claim lock excludes sibling workers.
    if !scanned.is_empty() {
        materialize_layers(claim.pool_id, &fold.layers);
    }
    let net_delta: i128 = pending.iter().map(|p| p.delta).sum();
    if net_delta != 0 {
        reconcile_aggregate_value(claim.pool_id, net_delta);
    }

    // New business-time frontier: the last scanned event, else the claimed one.
    let frontier: Option<(String, i64)> = scanned
        .last()
        .map(|s| (s.posted_at.clone(), s.id))
        .or_else(|| claim.settled_at.clone().map(|at| (at, claim.settled_id.unwrap_or(0))));

    let floor_pending = match &frontier {
        Some((at, id)) => settle(&claim, generation, at, *id),
        None => false, // nothing ever scanned and no frontier: pure no-op
    };

    // Final statements, freshest snapshot: events past the new frontier keep
    // the pool dirty, and an unscanned event INSIDE the replayed range (one
    // that committed mid-pass — the feed compared it against the OLD frontier,
    // found it non-backdated, and its dirty mark hit our claim-locked row's
    // DO NOTHING) lowers the recost floor ourselves. Events committing after
    // this snapshot are the feed's: its floor UPDATE blocks on our settle-row
    // lock and re-evaluates against the committed new frontier. Together with
    // the feed's floors-before-marks apply order this closes every
    // stranded-pool interleaving.
    let scan_from = if full_replay { None } else { claim.settled_at.clone().map(|at| (at, claim.settled_id.unwrap_or(0))) };
    let scanned_ids: Vec<i64> = scanned.iter().map(|s| s.id).collect();
    let requeued = decide_queue(claim.pool_id, &scan_from, &frontier, &scanned_ids, floor_pending);

    PassOutcome {
        full_replay,
        events: scanned.len(),
        settlements: pending.len(),
        adjustments,
        generation,
        floor_pending,
        requeued,
    }
}

/// Targeted, BLOCKING claim of one pool (the close / settle_pool path):
/// ensures the `recalc_queue` row exists, then locks it `FOR UPDATE` without
/// `SKIP LOCKED` — waiting out any engine worker that holds the claim. The
/// caller's transaction then owns the pool until commit; `SKIP LOCKED`
/// workers pass by. A queue row created here for an already-clean pool is
/// consumed by the drain's queue decision (a free no-op pass). Re-claiming a
/// row this transaction already locked is free, so drain loops re-read the
/// claim state each iteration.
pub(crate) fn claim_pool(pool_id: i64) -> Claim {
    Spi::connect_mut(|client| -> Result<Option<Claim>, pgrx::spi::Error> {
        let known = client
            .select("SELECT EXISTS (SELECT 1 FROM pool WHERE id = $1)", None, &[pool_id.into()])?
            .first()
            .get::<bool>(1)?
            .unwrap_or(false);
        if !known {
            return Ok(None);
        }
        client.update(
            "INSERT INTO recalc_queue (pool_id) VALUES ($1) ON CONFLICT (pool_id) DO NOTHING",
            None,
            &[pool_id.into()],
        )?;
        let mut t = client.update(
            "SELECT rq.pool_id, p.method::text, \
                    COALESCE(ps.recalc_generation, 0), \
                    ps.settled_through_posted_at::text, ps.settled_through_id, \
                    ps.recost_floor_posted_at::text, ps.recost_floor_id \
               FROM recalc_queue rq \
               JOIN pool p ON p.id = rq.pool_id \
               LEFT JOIN pool_settlement ps ON ps.pool_id = rq.pool_id \
              WHERE rq.pool_id = $1 \
                FOR UPDATE OF rq",
            None,
            &[pool_id.into()],
        )?;
        Ok(t.next().map(|row| Claim {
            pool_id: row.get::<i64>(1).ok().flatten().expect("recalc_queue.pool_id NOT NULL"),
            method: row.get::<String>(2).ok().flatten().expect("pool.method NOT NULL"),
            generation: row.get::<i64>(3).ok().flatten().unwrap_or(0),
            settled_at: row.get::<String>(4).ok().flatten(),
            settled_id: row.get::<i64>(5).ok().flatten(),
            floor_at: row.get::<String>(6).ok().flatten(),
            floor_id: row.get::<i64>(7).ok().flatten(),
        }))
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("claim_pool: claim failed: {e}"))
    })
    .unwrap_or_else(|| {
        crate::ledger_error_map::raise_ledger_error(ledger_core::LedgerError::UnknownPool(pool_id));
        unreachable!()
    })
}

/// Drain one pool to its stream head synchronously (recalc-e §6/§7: the
/// `settle_pool` on-demand shape and the forced-close burst). Loops claimed
/// passes until the queue decision reports the pool clean — no surviving
/// floor, no tail past the frontier, no missed mid-pass commit.
pub(crate) fn drain_pool_to_head(pool_id: i64) -> DrainOutcome {
    let mut stats = DrainStats::default();
    loop {
        let claim = claim_pool(pool_id);
        if claim.method != "fifo" && claim.method != "lifo" {
            drop_queue_row(pool_id);
            return DrainOutcome::NonStrict { method: claim.method };
        }
        stats.generation = claim.generation;
        let out = run_claimed_pass(&claim);
        stats.passes += 1;
        stats.settlements += out.settlements as i64;
        stats.adjustments += out.adjustments as i64;
        stats.generation = out.generation;
        if !out.requeued {
            return DrainOutcome::Drained(stats);
        }
        if stats.passes >= MAX_DRAIN_PASSES {
            bail(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("drain_pool_to_head: pool {pool_id} did not quiesce after {MAX_DRAIN_PASSES} passes"),
            );
        }
    }
}

/// Claim the oldest-marked dirty pool. The `FOR UPDATE OF rq SKIP LOCKED` row
/// lock is the per-pool ownership for the rest of the pass; `pool` and
/// `pool_settlement` rows stay unlocked here (the hot path locks pool rows for
/// core-method submissions, and the feed updates floors — neither may wait on
/// a fold).
fn claim_next() -> Option<Claim> {
    Spi::connect_mut(|client| -> Result<Option<Claim>, pgrx::spi::Error> {
        let mut t = client.update(
            "SELECT rq.pool_id, p.method::text, \
                    COALESCE(ps.recalc_generation, 0), \
                    ps.settled_through_posted_at::text, ps.settled_through_id, \
                    ps.recost_floor_posted_at::text, ps.recost_floor_id \
               FROM recalc_queue rq \
               JOIN pool p ON p.id = rq.pool_id \
               LEFT JOIN pool_settlement ps ON ps.pool_id = rq.pool_id \
              ORDER BY rq.enqueued_at, rq.pool_id \
                FOR UPDATE OF rq SKIP LOCKED \
              LIMIT 1",
            None,
            &[],
        )?;
        Ok(match t.next() {
            Some(row) => Some(Claim {
                pool_id: row.get::<i64>(1)?.expect("recalc_queue.pool_id NOT NULL"),
                method: row.get::<String>(2)?.expect("pool.method NOT NULL"),
                generation: row.get::<i64>(3)?.unwrap_or(0),
                settled_at: row.get::<String>(4)?,
                settled_id: row.get::<i64>(5)?,
                floor_at: row.get::<String>(6)?,
                floor_id: row.get::<i64>(7)?,
            }),
            None => None,
        })
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_recalc_step: claim failed: {e}"))
    })
}

pub(crate) fn drop_queue_row(pool_id: i64) {
    Spi::connect_mut(|client| -> Result<(), pgrx::spi::Error> {
        client.update("DELETE FROM recalc_queue WHERE pool_id = $1", None, &[pool_id.into()])?;
        Ok(())
    })
    .unwrap_or_else(|e| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_recalc_step: queue delete failed: {e}"),
        )
    })
}

/// The persisted checkpoint: open layers ordered by the business chronology of
/// the receipts that created them (`layer_id` = the receipt trx_line.id, so
/// the join recovers `(posted_at, id)` — a backdated receipt's layer sorts
/// where business time puts it, not where its id does).
fn hydrate_layers(pool_id: i64) -> Vec<StrictLayer> {
    Spi::connect(|client| -> Result<Vec<StrictLayer>, pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT s.layer_id, s.qty, s.unit_cost \
               FROM pool_state s \
               JOIN trx_line t ON t.id = s.layer_id \
              WHERE s.pool_id = $1 AND s.layer_id > 0 \
              ORDER BY t.posted_at, t.id",
            None,
            &[pool_id.into()],
        )?;
        let mut out = Vec::new();
        while let Some(row) = t.next() {
            out.push(StrictLayer {
                layer_id: row.get::<i64>(1)?.expect("pool_state.layer_id NOT NULL"),
                qty: row.get::<i64>(2)?.expect("pool_state.qty NOT NULL"),
                unit_cost: row.get::<i64>(3)?.expect("pool_state.unit_cost NOT NULL"),
            });
        }
        Ok(out)
    })
    .unwrap_or_else(|e| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_recalc_step: layer hydration failed: {e}"),
        )
    })
}

/// R-1 ordered scan of the pool's physical events — index range scan on
/// `(pool_id, posted_at, id)`. `after` = the exclusive frontier for an
/// incremental pass; `None` scans from opening.
fn scan_events(pool_id: i64, after: Option<(Option<String>, Option<i64>)>) -> Vec<Scanned> {
    let (after_at, after_id) = match after {
        Some((at, id)) => (at, id),
        None => (None, None),
    };
    Spi::connect(|client| -> Result<Vec<Scanned>, pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT t.id, t.qty, t.unit_cost, t.line_type::text, t.posted_at::text \
               FROM trx_line t \
              WHERE t.pool_id = $1 \
                AND t.line_type <> 'cost_adjustment_line' \
                AND ($2::timestamptz IS NULL OR (t.posted_at, t.id) > ($2::timestamptz, $3)) \
              ORDER BY t.posted_at, t.id",
            None,
            &[pool_id.into(), after_at.into(), after_id.into()],
        )?;
        let mut out = Vec::new();
        while let Some(row) = t.next() {
            out.push(Scanned {
                id: row.get::<i64>(1)?.expect("trx_line.id NOT NULL"),
                qty: row.get::<i64>(2)?.expect("trx_line.qty NOT NULL"),
                unit_cost: row.get::<i64>(3)?.expect("trx_line.unit_cost NOT NULL"),
                line_type: row.get::<String>(4)?.expect("trx_line.line_type NOT NULL"),
                posted_at: row.get::<String>(5)?.expect("trx_line.posted_at NOT NULL"),
            });
        }
        Ok(out)
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_recalc_step: scan failed: {e}"))
    })
}

/// Current authoritative cost per depletion = its max-generation
/// `cost_settlement` row.
fn load_priors(costings: &[DepletionCosting]) -> Vec<(i64, i64)> {
    if costings.is_empty() {
        return Vec::new();
    }
    let ids: Vec<i64> = costings.iter().map(|c| c.depletion_trx_line_id).collect();
    Spi::connect(|client| -> Result<Vec<(i64, i64)>, pgrx::spi::Error> {
        let mut t = client.select(
            "SELECT DISTINCT ON (depletion_trx_line_id) \
                    depletion_trx_line_id, authoritative_unit_cost \
               FROM cost_settlement \
              WHERE depletion_trx_line_id = ANY($1::bigint[]) \
              ORDER BY depletion_trx_line_id, recalc_generation DESC",
            None,
            &[ids.into()],
        )?;
        let mut out = Vec::new();
        while let Some(row) = t.next() {
            out.push((
                row.get::<i64>(1)?.expect("cost_settlement key NOT NULL"),
                row.get::<i64>(2)?.expect("authoritative_unit_cost NOT NULL"),
            ));
        }
        Ok(out)
    })
    .unwrap_or_else(|e| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_recalc_step: prior lookup failed: {e}"),
        )
    })
}

/// Write generation N: consumption linkage for every pending depletion, one
/// `cost_adjustment` trx + per-depletion `cost_adjustment_line`/`posting_line`
/// for the nonzero deltas, and the `cost_settlement` records. Returns the
/// number of GL adjustments posted.
fn write_generation(pool_id: i64, generation: i64, pending: &[Pending]) -> usize {
    // Consumption linkage (batch): only real layer draws — a fully-uncovered
    // depletion has none, and its settlement row stands alone (the observed
    // fallback is not a layer).
    let mut dep_ids = Vec::new();
    let mut layer_ids = Vec::new();
    let mut qtys = Vec::new();
    let mut costs = Vec::new();
    for p in pending {
        for d in &p.costing.draws {
            dep_ids.push(p.costing.depletion_trx_line_id);
            layer_ids.push(d.layer_trx_line_id);
            qtys.push(d.qty);
            costs.push(d.unit_cost);
        }
    }

    let adjusted: Vec<&Pending> = pending.iter().filter(|p| p.delta != 0).collect();
    let accounts = if adjusted.is_empty() { None } else { Some(resolve_accounts(pool_id)) };

    Spi::connect_mut(|client| -> Result<usize, pgrx::spi::Error> {
        if !dep_ids.is_empty() {
            client.update(
                "INSERT INTO cost_layer_consumption \
                        (depletion_trx_line_id, layer_trx_line_id, recalc_generation, qty, unit_cost) \
                 SELECT u.d, u.l, $5, u.q, u.c \
                   FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[]) AS u(d, l, q, c)",
                None,
                &[
                    dep_ids.into(),
                    layer_ids.into(),
                    qtys.into(),
                    costs.into(),
                    generation.into(),
                ],
            )?;
        }

        let mut adjustments = 0usize;
        if let Some(accounts) = &accounts {
            // One system trx wraps this pass's corrections. posted_at = now(),
            // never a business date: its feed delivery must sort AHEAD of every
            // settled frontier so it can only trigger a free no-op pass, not a
            // floor-lowering loop.
            let trx_id: i64 = client
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

            for p in &adjusted {
                let amount: i64 = i64::try_from(p.delta.abs()).unwrap_or_else(|_| {
                    bail(
                        PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                        format!(
                            "ledger_recalc_step: adjustment amount overflows bigint \
                             (depletion {})",
                            p.costing.depletion_trx_line_id
                        ),
                    )
                });
                // The depletion's own operation pair (stored in receipt
                // direction): a positive delta posts MORE of the depletion
                // (swap), a negative delta hands value back (as stored).
                let (rcv_debit, rcv_credit) = pair_for(accounts, &p.line_type);
                let (debit, credit) = if p.delta > 0 {
                    (rcv_credit, rcv_debit)
                } else {
                    (rcv_debit, rcv_credit)
                };
                let posting_line_id: i64 = client
                    .update(
                        "WITH tl AS ( \
                            INSERT INTO trx_line \
                                   (trx_id, pool_id, line_type, source_id, qty, unit_cost, \
                                    source_trx_line_id, posted_at) \
                            VALUES ($1, $2, 'cost_adjustment_line', NULL, $3, $4, $5, now()) \
                            RETURNING id \
                         ) \
                         INSERT INTO posting_line \
                                (trx_line_id, event_type, amount, debit_account, credit_account, posted_at) \
                         SELECT tl.id, 'cost_adjustment', $6, $7, $8, now() FROM tl \
                         RETURNING id",
                        None,
                        &[
                            trx_id.into(),
                            pool_id.into(),
                            (-p.costing.qty).into(),
                            p.costing.authoritative_unit_cost.into(),
                            p.costing.depletion_trx_line_id.into(),
                            amount.into(),
                            debit.into(),
                            credit.into(),
                        ],
                    )?
                    .first()
                    .get::<i64>(1)?
                    .expect("posting_line insert returns id");

                client.update(
                    "INSERT INTO cost_settlement \
                            (depletion_trx_line_id, recalc_generation, authoritative_unit_cost, \
                             prior_unit_cost, adjustment_posting_line_id) \
                     VALUES ($1, $2, $3, $4, $5)",
                    None,
                    &[
                        p.costing.depletion_trx_line_id.into(),
                        generation.into(),
                        p.costing.authoritative_unit_cost.into(),
                        p.prior_unit_cost.into(),
                        posting_line_id.into(),
                    ],
                )?;
                adjustments += 1;
            }
        }

        // Zero-delta settlements (first costing at exactly the observed cost):
        // the authoritative record with nothing to post.
        let zero: Vec<&Pending> = pending.iter().filter(|p| p.delta == 0).collect();
        if !zero.is_empty() {
            let d: Vec<i64> = zero.iter().map(|p| p.costing.depletion_trx_line_id).collect();
            let a: Vec<i64> = zero.iter().map(|p| p.costing.authoritative_unit_cost).collect();
            let pr: Vec<i64> = zero.iter().map(|p| p.prior_unit_cost).collect();
            client.update(
                "INSERT INTO cost_settlement \
                        (depletion_trx_line_id, recalc_generation, authoritative_unit_cost, \
                         prior_unit_cost, adjustment_posting_line_id) \
                 SELECT u.d, $4, u.a, u.p, NULL \
                   FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[]) AS u(d, a, p)",
                None,
                &[d.into(), a.into(), pr.into(), generation.into()],
            )?;
        }

        Ok(adjustments)
    })
    .unwrap_or_else(|e| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_recalc_step: generation write failed: {e}"),
        )
    })
}

/// The pool's posting-account pairs (receipt direction per operation), from
/// `posting_account_map` on the pool's `(sku_id, location_id)`. Fail-loud on a
/// missing row — same posture as the hot path (§3.7).
pub(crate) fn resolve_accounts(pool_id: i64) -> ledger_core::PostingAccounts {
    let resolved = Spi::connect(
        |client| -> Result<Option<(i64, i64, Option<ledger_core::PostingAccounts>)>, pgrx::spi::Error> {
            let mut t = client.select(
                "SELECT p.sku_id, p.location_id, \
                        m.receipt_debit, m.receipt_credit, \
                        m.transfer_debit, m.transfer_credit, \
                        m.build_debit, m.build_credit, \
                        m.scrap_debit, m.scrap_credit, \
                        m.adjustment_debit, m.adjustment_credit, \
                        m.revaluation_debit, m.revaluation_credit, \
                        m.variance_acct \
                   FROM pool p \
                   LEFT JOIN posting_account_map m \
                     ON m.sku_id = p.sku_id AND m.location_id = p.location_id \
                  WHERE p.id = $1",
                None,
                &[pool_id.into()],
            )?;
            Ok(match t.next() {
                Some(row) => {
                    let sku_id = row.get::<i64>(1)?.unwrap_or(0);
                    let location_id = row.get::<i64>(2)?.unwrap_or(0);
                    let accounts = match row.get::<i64>(3)? {
                        Some(receipt_debit) => {
                            let g = |c: usize| -> i64 { row.get::<i64>(c).ok().flatten().unwrap_or(0) };
                            Some(ledger_core::PostingAccounts {
                                receipt: (receipt_debit, g(4)),
                                transfer: (g(5), g(6)),
                                build: (g(7), g(8)),
                                scrap: (g(9), g(10)),
                                adjustment: (g(11), g(12)),
                                revaluation: (g(13), g(14)),
                                variance: row.get::<i64>(15)?,
                            })
                        }
                        None => None,
                    };
                    Some((sku_id, location_id, accounts))
                }
                None => None,
            })
        },
    )
    .unwrap_or_else(|e| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_recalc_step: account resolution failed: {e}"),
        )
    });

    match resolved {
        Some((_, _, Some(accounts))) => accounts,
        Some((sku_id, location_id, None)) => {
            ledger_error_map::raise_ledger_error(
                ledger_core::LedgerError::MissingPostingAccounts { sku_id, location_id },
            );
            unreachable!()
        }
        None => {
            ledger_error_map::raise_ledger_error(ledger_core::LedgerError::UnknownPool(pool_id));
            unreachable!()
        }
    }
}

/// The receipt-direction pair for a depletion's line_type text.
fn pair_for(accounts: &ledger_core::PostingAccounts, line_type: &str) -> (i64, i64) {
    match decode_line_type(line_type) {
        Some(lt) => accounts.pair_for(lt),
        None => bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_recalc_step: no account pair for line_type '{line_type}'"),
        ),
    }
}

/// Rewrite the pool's open-layer materialization wholesale. Correct for both
/// replay shapes; only this engine writes FIFO/LIFO layer rows, under the
/// claim lock.
fn materialize_layers(pool_id: i64, layers: &[StrictLayer]) {
    let layer_ids: Vec<i64> = layers.iter().map(|l| l.layer_id).collect();
    let qtys: Vec<i64> = layers.iter().map(|l| l.qty).collect();
    let costs: Vec<i64> = layers.iter().map(|l| l.unit_cost).collect();
    Spi::connect_mut(|client| -> Result<(), pgrx::spi::Error> {
        client.update(
            "DELETE FROM pool_state WHERE pool_id = $1 AND layer_id > 0",
            None,
            &[pool_id.into()],
        )?;
        if !layer_ids.is_empty() {
            client.update(
                "INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum) \
                 SELECT $1, u.l, u.q, u.c, (u.q::numeric * u.c::numeric)::bigint \
                   FROM UNNEST($2::bigint[], $3::bigint[], $4::bigint[]) AS u(l, q, c)",
                None,
                &[pool_id.into(), layer_ids.into(), qtys.into(), costs.into()],
            )?;
        }
        Ok(())
    })
    .unwrap_or_else(|e| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_recalc_step: layer materialization failed: {e}"),
        )
    })
}

/// Fold this pass's net GL delta into the aggregate's `value_sum`
/// (provisional net → authoritative net, recalc-d §6). Single-statement
/// read-modify-write on the row's own lock — commutative with concurrent
/// hot-path appends. Mirrors the FL CTE conventions: numeric mid-expression,
/// derived average preserved while qty <= 0.
fn reconcile_aggregate_value(pool_id: i64, net_delta: i128) {
    let net: i64 = i64::try_from(net_delta).unwrap_or_else(|_| {
        bail(
            PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("ledger_recalc_step: net value delta overflows bigint (pool {pool_id})"),
        )
    });
    Spi::connect_mut(|client| -> Result<(), pgrx::spi::Error> {
        client.update(
            "UPDATE pool_state \
                SET value_sum = (value_sum::numeric - $2::numeric)::bigint, \
                    unit_cost = CASE WHEN qty > 0 \
                                     THEN banker_div(value_sum::numeric - $2::numeric, qty) \
                                     ELSE unit_cost END \
              WHERE pool_id = $1 AND layer_id = 0",
            None,
            &[pool_id.into(), net.into()],
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_recalc_step: aggregate reconciliation failed: {e}"),
        )
    })
}

/// Advance the settlement row: generation (bumped iff the pass wrote),
/// `settled_through_*` to the new frontier, and the recost floor cleared ONLY
/// if it still equals the claimed value — a feed batch lowering it mid-pass
/// wins, and the surviving floor forces the next pass to re-cost. Returns
/// whether a floor is still pending after this settle.
fn settle(claim: &Claim, generation: i64, frontier_at: &str, frontier_id: i64) -> bool {
    Spi::connect_mut(|client| -> Result<bool, pgrx::spi::Error> {
        let mut t = client.update(
            "INSERT INTO pool_settlement \
                    (pool_id, recalc_generation, settled_through_posted_at, settled_through_id, \
                     last_recalc_at) \
             VALUES ($1, $2, $3::timestamptz, $4, now()) \
             ON CONFLICT (pool_id) DO UPDATE SET \
                 recalc_generation = EXCLUDED.recalc_generation, \
                 settled_through_posted_at = EXCLUDED.settled_through_posted_at, \
                 settled_through_id = EXCLUDED.settled_through_id, \
                 recost_floor_posted_at = CASE \
                     WHEN pool_settlement.recost_floor_posted_at IS NOT DISTINCT FROM $5::timestamptz \
                      AND pool_settlement.recost_floor_id IS NOT DISTINCT FROM $6 \
                     THEN NULL ELSE pool_settlement.recost_floor_posted_at END, \
                 recost_floor_id = CASE \
                     WHEN pool_settlement.recost_floor_posted_at IS NOT DISTINCT FROM $5::timestamptz \
                      AND pool_settlement.recost_floor_id IS NOT DISTINCT FROM $6 \
                     THEN NULL ELSE pool_settlement.recost_floor_id END, \
                 last_recalc_at = now() \
             RETURNING recost_floor_posted_at IS NOT NULL",
            None,
            &[
                claim.pool_id.into(),
                generation.into(),
                frontier_at.to_string().into(),
                frontier_id.into(),
                claim.floor_at.clone().into(),
                claim.floor_id.into(),
            ],
        )?;
        Ok(t.next().and_then(|row| row.get::<bool>(1).ok().flatten()).unwrap_or(false))
    })
    .unwrap_or_else(|e| {
        bail(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, format!("ledger_recalc_step: settle failed: {e}"))
    })
}

/// Keep the queue row iff work remains: a surviving floor, physical events
/// past the new frontier (on the freshest snapshot), or an unscanned event
/// inside the replayed range `(scan_from, frontier]` — a mid-pass commit whose
/// feed mark our claim lock swallowed; that one also lowers the recost floor
/// (guarded min) so the next pass replays past it. A kept row is re-stamped to
/// the back of the line; otherwise the mark is consumed.
fn decide_queue(
    pool_id: i64,
    scan_from: &Option<(String, i64)>,
    frontier: &Option<(String, i64)>,
    scanned_ids: &[i64],
    floor_pending: bool,
) -> bool {
    let (from_at, from_id): (Option<String>, Option<i64>) = match scan_from {
        Some((at, id)) => (Some(at.clone()), Some(*id)),
        None => (None, None),
    };
    let (f_at, f_id): (Option<String>, Option<i64>) = match frontier {
        Some((at, id)) => (Some(at.clone()), Some(*id)),
        None => (None, None),
    };
    let ids: Vec<i64> = scanned_ids.to_vec();
    Spi::connect_mut(|client| -> Result<bool, pgrx::spi::Error> {
        let mut missed = false;
        if frontier.is_some() {
            let mut t = client.select(
                "SELECT t.posted_at::text, t.id \
                   FROM trx_line t \
                  WHERE t.pool_id = $1 \
                    AND t.line_type <> 'cost_adjustment_line' \
                    AND ($2::timestamptz IS NULL \
                         OR (t.posted_at, t.id) > ($2::timestamptz, $3)) \
                    AND (t.posted_at, t.id) <= ($4::timestamptz, $5) \
                    AND NOT (t.id = ANY($6::bigint[])) \
                  ORDER BY t.posted_at, t.id \
                  LIMIT 1",
                None,
                &[
                    pool_id.into(),
                    from_at.clone().into(),
                    from_id.into(),
                    f_at.clone().into(),
                    f_id.into(),
                    ids.into(),
                ],
            )?;
            if let Some(row) = t.next() {
                let at: String = row.get::<String>(1)?.expect("posted_at NOT NULL");
                let id: i64 = row.get::<i64>(2)?.expect("id NOT NULL");
                client.update(
                    "UPDATE pool_settlement \
                        SET recost_floor_posted_at = $2::timestamptz, recost_floor_id = $3 \
                      WHERE pool_id = $1 \
                        AND (recost_floor_posted_at IS NULL \
                             OR ($2::timestamptz, $3) \
                                < (recost_floor_posted_at, recost_floor_id))",
                    None,
                    &[pool_id.into(), at.into(), id.into()],
                )?;
                missed = true;
            }
        }

        let tail: bool = client
            .select(
                "SELECT EXISTS ( \
                    SELECT 1 FROM trx_line t \
                     WHERE t.pool_id = $1 \
                       AND t.line_type <> 'cost_adjustment_line' \
                       AND ($2::timestamptz IS NULL \
                            OR (t.posted_at, t.id) > ($2::timestamptz, $3)))",
                None,
                &[pool_id.into(), f_at.into(), f_id.into()],
            )?
            .first()
            .get::<bool>(1)?
            .unwrap_or(false);
        if floor_pending || tail || missed {
            client.update(
                "UPDATE recalc_queue SET enqueued_at = now() WHERE pool_id = $1",
                None,
                &[pool_id.into()],
            )?;
            Ok(true)
        } else {
            client.update(
                "DELETE FROM recalc_queue WHERE pool_id = $1",
                None,
                &[pool_id.into()],
            )?;
            Ok(false)
        }
    })
    .unwrap_or_else(|e| {
        bail(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("ledger_recalc_step: queue decision failed: {e}"),
        )
    })
}
