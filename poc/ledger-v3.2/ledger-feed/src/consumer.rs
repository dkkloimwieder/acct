//! The feed consumer: peek the logical slot, record delivered `trx_line`
//! events into the durable dirty-set, then advance the cursor
//! (advance-on-ingestion — recalc-c D8 option B, accepted 2026-07-10).
//!
//! Delivery contract: **at-least-once, idempotent ingestion.** The sequence is
//! peek (non-consuming) → apply the batch in one transaction (recost floors +
//! backpressure counters + dirty marks) → advance `confirmed_flush_lsn`. A
//! crash between apply and advance re-delivers the batch; re-marking a pool
//! dirty and re-taking a guarded floor minimum are both no-ops. The
//! backpressure counter bump is the one non-idempotent apply effect: a
//! re-delivered batch over-counts, in the conservative direction (an early
//! engage, never a missed one), and the engine's exact-count reset wipes the
//! overcount at the pool's next settle. The dirty-set is the crash-recovery
//! boundary — a crash re-drains the dirty-set, not the slot (recalc-c §6).
//! The slot's `confirmed_flush_lsn` is the ONLY stream cursor; no watermark
//! table exists or may be added (design-v3.1 §17 / recalc-d §1).
//!
//! The consumer is method-agnostic: every delivered event marks its pool
//! dirty. Recalc decides per-pool what a pass means, and no-op passes are free
//! (recalc-d D6), so filtering by cost method here would buy nothing and cost
//! a lookup.
//!
//! Backpressure accounting (recalc-c §5): the same apply transaction
//! accumulates the per-pool unsettled-event counter (`recalc_backlog`) for
//! delivered PHYSICAL events on fifo/lifo pools — `cost_adjustment_line` rows
//! are the recalc engine's own output and are never counted — and engages the
//! throttle (`recalc_backpressure`) for any pool whose counter reaches the
//! configured bound. The engine resets the counter to the exact committed
//! tail at every settle and releases at the low-water mark, so feed-lag skew
//! in these increments is wiped at the pool's next pass and never drifts.
//! Row-lock order shared with the engine and the close sweep:
//! pool_settlement → recalc_backlog → recalc_backpressure, with the bump's
//! per-pool locks taken in ascending pool order (the close sweep accumulates
//! the same rows ascending, so the two multi-pool writers cannot cross-lock).
//!
//! Single-consumer posture: a logical slot has one cursor, so exactly one
//! consumer process/loop owns it (matching recalc-b §5's single feed
//! consumer). Concurrency lives in the recalc workers draining the dirty-set,
//! not here.

use sqlx::PgPool;
use std::collections::HashMap;

use crate::pgoutput::{self, Message, Relation};

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Parse(#[from] pgoutput::ParseError),
    #[error("feed protocol error: {0}")]
    Protocol(String),
    /// The slot is gone or the cluster invalidated it (`wal_status = 'lost'`,
    /// which `max_slot_wal_keep_size` deliberately permits). WAL the slot
    /// never delivered has been discarded, so retrying the peek can only spin.
    #[error("feed slot '{slot}' is unusable ({reason}); retrying cannot recover events \
             whose WAL was discarded — re-create the slot and re-fold the affected pools \
             (recovery routine: acct-1vur.2b)")]
    SlotLost { slot: String, reason: String },
}

/// One `trx_line` insert delivered by the slot, projected to the columns the
/// dirty-set needs. `posted_at` stays in Postgres text form end-to-end — it is
/// only ever compared/stored by Postgres itself (cast back on the way in).
#[derive(Debug, Clone)]
pub struct TrxLineEvent {
    pub trx_line_id: i64,
    pub pool_id: i64,
    pub posted_at: String,
    /// SQL enum label; the backpressure counter excludes `cost_adjustment_line`.
    pub line_type: String,
}

/// A non-consuming read of the slot: decoded events plus the LSN frontier of
/// the batch (safe to advance to once the events are durably applied).
#[derive(Debug)]
pub struct PeekBatch {
    pub messages: usize,
    pub events: Vec<TrxLineEvent>,
    /// Max LSN over ALL returned rows (the last row is a commit boundary —
    /// `upto_nchanges` only cuts between transactions).
    pub last_lsn: Option<String>,
}

/// What one `ingest_once` did.
#[derive(Debug)]
pub struct IngestReport {
    pub messages: usize,
    pub inserts: usize,
    /// Pools newly marked dirty (already-dirty pools don't count).
    pub pools_marked: u64,
    /// pool_settlement rows whose recost floor was lowered.
    pub floors_lowered: u64,
    /// Pools newly engaged by the backpressure bound (recalc-c §5).
    pub pools_throttled: u64,
    /// Where the cursor advanced to; None when it was already at the frontier.
    pub advanced_to: Option<String>,
}

pub struct FeedConsumer {
    pool: PgPool,
    slot: String,
    publication: String,
}

impl FeedConsumer {
    pub fn new(pool: PgPool, slot: impl Into<String>, publication: impl Into<String>) -> Self {
        FeedConsumer { pool, slot: slot.into(), publication: publication.into() }
    }

    pub fn slot(&self) -> &str {
        &self.slot
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Create the slot if it does not exist. Returns true when this call
    /// created it. Slots are cluster runtime state, not schema — this is the
    /// consumer's startup responsibility, not a migration's.
    ///
    /// Creating a slot puts its cursor at the CURRENT WAL position, so every
    /// event decodable before this moment is skipped. That is harmless on a
    /// virgin database and silently destructive on one that already holds
    /// data — which is exactly what a lost slot looks like. Callers that care
    /// callers must therefore treat a `true` return over a non-empty database
    /// as a slot-loss event needing recovery (acct-1vur.2b).
    pub async fn ensure_slot(&self) -> Result<bool, FeedError> {
        let created: Option<String> = sqlx::query_scalar(
            "SELECT (pg_create_logical_replication_slot($1, 'pgoutput')).lsn::text \
             WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
        )
        .bind(&self.slot)
        .fetch_optional(&self.pool)
        .await?;
        Ok(created.is_some())
    }

    /// Fail loud when the slot is absent or the cluster has invalidated it.
    /// Checked at the top of every tick: an invalidated slot returns an error
    /// from `peek` forever, and a bare retry loop would spin on it while the
    /// dirty set silently stopped growing.
    pub async fn check_slot_usable(&self) -> Result<(), FeedError> {
        let row: Option<(bool, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT present, wal_status, invalidation_reason FROM feed_slot_health",
        )
        .fetch_optional(&self.pool)
        .await?;
        let Some((present, wal_status, invalidation)) = row else {
            return Ok(());
        };
        if !present {
            return Err(FeedError::SlotLost {
                slot: self.slot.clone(),
                reason: "slot absent".to_string(),
            });
        }
        if let Some(reason) = invalidation {
            return Err(FeedError::SlotLost { slot: self.slot.clone(), reason });
        }
        if wal_status.as_deref() == Some("lost") {
            return Err(FeedError::SlotLost {
                slot: self.slot.clone(),
                reason: "wal_status=lost".to_string(),
            });
        }
        Ok(())
    }

    /// One feed tick: peek → apply → advance. Returns what happened so the
    /// caller's loop can pace itself (messages == 0 → queue empty, sleep).
    pub async fn ingest_once(&self, limit: i32) -> Result<IngestReport, FeedError> {
        self.check_slot_usable().await?;

        // Anchor BEFORE the peek: everything decodable before this point is
        // covered by the peek below, so on an empty batch the cursor can move
        // here without skipping anything. Keeps G1 honest (and WAL released)
        // when the WAL that advanced holds nothing published — other tables,
        // other databases on the cluster.
        let pre_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
            .fetch_one(&self.pool)
            .await?;

        let batch = self.peek(limit).await?;

        let (pools_marked, floors_lowered, pools_throttled) = if batch.events.is_empty() {
            (0, 0, 0)
        } else {
            self.apply(&batch.events).await?
        };

        // A returned batch always ends on a commit boundary, so its max LSN is
        // a safe target; with no rows returned, fall back to the pre-peek
        // anchor.
        let target = batch.last_lsn.clone().unwrap_or(pre_lsn);
        let advanced_to = self.advance(&target).await?;

        Ok(IngestReport {
            messages: batch.messages,
            inserts: batch.events.len(),
            pools_marked,
            floors_lowered,
            pools_throttled,
            advanced_to,
        })
    }

    /// Non-consuming read of up to ~`limit` messages (pgoutput only cuts
    /// between transactions, so slightly more can arrive).
    pub async fn peek(&self, limit: i32) -> Result<PeekBatch, FeedError> {
        let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT lsn::text, data \
             FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, \
                  'proto_version', '1', 'publication_names', $3)",
        )
        .bind(&self.slot)
        .bind(limit)
        .bind(&self.publication)
        .fetch_all(&self.pool)
        .await?;

        let messages = rows.len();
        let mut relations: HashMap<u32, Relation> = HashMap::new();
        let mut events = Vec::new();
        let mut last_lsn: Option<String> = None;

        for (lsn, data) in rows {
            last_lsn = Some(lsn);
            match pgoutput::parse(&data)? {
                Message::Relation(rel) => {
                    relations.insert(rel.id, rel);
                }
                Message::Insert { rel_id, columns } => {
                    let rel = relations.get(&rel_id).ok_or_else(|| {
                        FeedError::Protocol(format!("insert for unknown relation {rel_id}"))
                    })?;
                    if rel.name == "trx_line" {
                        events.push(decode_trx_line(rel, &columns)?);
                    }
                }
                Message::Begin | Message::Commit | Message::Other(_) => {}
            }
        }

        Ok(PeekBatch { messages, events, last_lsn })
    }

    /// Durably record a delivered batch: for pools already settled past an
    /// event, lower the recost floor (guarded min in R-1 `(posted_at, id)`
    /// order), then bump the backpressure counters, then mark every touched
    /// pool dirty. One transaction — the commit here is what makes the
    /// subsequent cursor advance safe.
    ///
    /// The statement order is load-bearing. Marks run LAST because the mark
    /// is what guarantees a future engine pass, so it must be evaluated after
    /// every statement whose effects need that pass to reconcile:
    ///
    /// - Floors before marks: the floor UPDATE can block on a recalc worker's
    ///   in-flight settle (both write the `pool_settlement` row), while the
    ///   mark's `ON CONFLICT DO NOTHING` never blocks — it silently skips
    ///   when the queue row exists, even one a worker holds claimed and is
    ///   about to delete. Evaluating the mark after the floor means any such
    ///   block has already resolved, so the mark sees the post-settle queue
    ///   state and re-inserts the row the worker just deleted. Invariant: a
    ///   pool with a recost floor set always has a `recalc_queue` row.
    /// - Bumps before marks: a pass can scan and settle this batch's events
    ///   before their delivery lands here (the scan reads committed
    ///   `trx_line` rows directly). Its counter reset then precedes this
    ///   bump, which blocks on the pass's counter-row lock and lands on top
    ///   of the reset — counting events that are already settled. Because
    ///   the mark is evaluated after the bump resolves, it sees the queue
    ///   row that pass deleted and re-inserts it, and the guaranteed next
    ///   pass resets the counter to the true tail (releasing any engage the
    ///   stale bump produced). Invariant: after any apply commits, every
    ///   bumped pool either has a queue row or is claim-held by a pass that
    ///   will reset its counter.
    ///
    /// Returns (pools_marked, floors_lowered, pools_throttled).
    pub async fn apply(&self, events: &[TrxLineEvent]) -> Result<(u64, u64, u64), FeedError> {
        let pool_ids: Vec<i64> = events.iter().map(|e| e.pool_id).collect();
        let posted_ats: Vec<String> = events.iter().map(|e| e.posted_at.clone()).collect();
        let ids: Vec<i64> = events.iter().map(|e| e.trx_line_id).collect();
        let line_types: Vec<String> = events.iter().map(|e| e.line_type.clone()).collect();

        let mut tx = self.pool.begin().await?;

        // The floor only moves for events strictly behind the settlement
        // frontier, and only downward. Both guards live in the UPDATE itself
        // (single-statement read-modify-write under the row lock), and the
        // per-pool minimum over the batch means within-batch delivery order is
        // irrelevant.
        let floors_lowered = sqlx::query(
            "WITH ev AS ( \
                 SELECT e.pool_id, e.posted_at::timestamptz AS posted_at, e.id \
                 FROM UNNEST($1::bigint[], $2::text[], $3::bigint[]) \
                      AS e(pool_id, posted_at, id) \
             ), mins AS ( \
                 SELECT DISTINCT ON (pool_id) pool_id, posted_at, id \
                 FROM ev ORDER BY pool_id, posted_at, id \
             ) \
             UPDATE pool_settlement ps \
                SET recost_floor_posted_at = m.posted_at, \
                    recost_floor_id        = m.id \
               FROM mins m \
              WHERE ps.pool_id = m.pool_id \
                AND ps.settled_through_posted_at IS NOT NULL \
                AND (m.posted_at, m.id) \
                    < (ps.settled_through_posted_at, ps.settled_through_id) \
                AND (ps.recost_floor_posted_at IS NULL \
                     OR (m.posted_at, m.id) \
                        < (ps.recost_floor_posted_at, ps.recost_floor_id))",
        )
        .bind(&pool_ids)
        .bind(&posted_ats)
        .bind(&ids)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        // Backpressure accounting: bump each fifo/lifo pool's unsettled-event
        // counter by this batch's physical events, then engage any pool whose
        // counter reached the bound. One statement — the engage reads the
        // post-bump counters through the `bumped` CTE, so a bound crossing
        // and its throttle row commit atomically. Per-pool locks are taken in
        // ascending pool order (the ORDER BY drives insertion order), the
        // same order the close sweep accumulates these rows.
        let pools_throttled = sqlx::query(
            "WITH counted AS ( \
                 SELECT e.pool_id, count(*) AS n \
                   FROM UNNEST($1::bigint[], $2::text[]) AS e(pool_id, line_type) \
                   JOIN pool p ON p.id = e.pool_id AND p.method IN ('fifo', 'lifo') \
                  WHERE e.line_type <> 'cost_adjustment_line' \
                  GROUP BY e.pool_id \
                  ORDER BY e.pool_id \
             ), bumped AS ( \
                 INSERT INTO recalc_backlog (pool_id, pending_events) \
                 SELECT pool_id, n FROM counted \
                 ON CONFLICT (pool_id) DO UPDATE \
                     SET pending_events = recalc_backlog.pending_events \
                                          + EXCLUDED.pending_events \
                 RETURNING pool_id, pending_events \
             ) \
             INSERT INTO recalc_backpressure (pool_id, engage_events) \
             SELECT b.pool_id, b.pending_events \
               FROM bumped b, recalc_backpressure_config c \
              WHERE b.pending_events >= c.bound_events \
              ORDER BY b.pool_id \
             ON CONFLICT (pool_id) DO NOTHING",
        )
        .bind(&pool_ids)
        .bind(&line_types)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        let pools_marked = sqlx::query(
            "INSERT INTO recalc_queue (pool_id) \
             SELECT DISTINCT u.pool_id FROM UNNEST($1::bigint[]) AS u(pool_id) \
             ON CONFLICT (pool_id) DO NOTHING",
        )
        .bind(&pool_ids)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        tx.commit().await?;
        Ok((pools_marked, floors_lowered, pools_throttled))
    }

    /// Advance the slot cursor to `target` if that is forward progress.
    /// Returns the position advanced to, or None when already at/past it.
    pub async fn advance(&self, target: &str) -> Result<Option<String>, FeedError> {
        let advanced: Option<String> = sqlx::query_scalar(
            "SELECT (pg_replication_slot_advance(slot_name, $2::pg_lsn)).end_lsn::text \
             FROM pg_replication_slots \
             WHERE slot_name = $1 AND confirmed_flush_lsn < $2::pg_lsn",
        )
        .bind(&self.slot)
        .bind(target)
        .fetch_optional(&self.pool)
        .await?;
        Ok(advanced)
    }
}

fn decode_trx_line(rel: &Relation, columns: &[Option<String>]) -> Result<TrxLineEvent, FeedError> {
    let col = |name: &str| -> Result<&str, FeedError> {
        let idx = rel
            .column_names
            .iter()
            .position(|c| c == name)
            .ok_or_else(|| FeedError::Protocol(format!("trx_line has no column {name}")))?;
        columns
            .get(idx)
            .and_then(|v| v.as_deref())
            .ok_or_else(|| FeedError::Protocol(format!("trx_line.{name} is null")))
    };
    let int = |name: &str| -> Result<i64, FeedError> {
        col(name)?
            .parse()
            .map_err(|e| FeedError::Protocol(format!("trx_line.{name} not an int: {e}")))
    };
    Ok(TrxLineEvent {
        trx_line_id: int("id")?,
        pool_id: int("pool_id")?,
        posted_at: col("posted_at")?.to_string(),
        line_type: col("line_type")?.to_string(),
    })
}
