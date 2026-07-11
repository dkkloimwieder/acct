//! The feed consumer: peek the logical slot, record delivered `trx_line`
//! events into the durable dirty-set, then advance the cursor
//! (advance-on-ingestion — recalc-c D8 option B, accepted 2026-07-10).
//!
//! Delivery contract: **at-least-once, idempotent ingestion.** The sequence is
//! peek (non-consuming) → apply the batch in one transaction (dirty marks +
//! recost floors) → advance `confirmed_flush_lsn`. A crash between apply and
//! advance re-delivers the batch; re-marking a pool dirty and re-taking a
//! guarded floor minimum are both no-ops, so re-delivery is harmless. The
//! dirty-set is therefore the crash-recovery boundary — a crash re-drains the
//! dirty-set, not the slot (recalc-c §6). The slot's `confirmed_flush_lsn` is
//! the ONLY stream cursor; no watermark table exists or may be added
//! (design-v3.1 §17 / recalc-d §1).
//!
//! The consumer is method-agnostic: every delivered event marks its pool
//! dirty. Recalc decides per-pool what a pass means, and no-op passes are free
//! (recalc-d D6), so filtering by cost method here would buy nothing and cost
//! a lookup.
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
}

/// One `trx_line` insert delivered by the slot, projected to the columns the
/// dirty-set needs. `posted_at` stays in Postgres text form end-to-end — it is
/// only ever compared/stored by Postgres itself (cast back on the way in).
#[derive(Debug, Clone)]
pub struct TrxLineEvent {
    pub trx_line_id: i64,
    pub pool_id: i64,
    pub posted_at: String,
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

    /// Create the slot if it does not exist. Returns true when this call
    /// created it. Slots are cluster runtime state, not schema — this is the
    /// consumer's startup responsibility, not a migration's.
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

    /// One feed tick: peek → apply → advance. Returns what happened so the
    /// caller's loop can pace itself (messages == 0 → queue empty, sleep).
    pub async fn ingest_once(&self, limit: i32) -> Result<IngestReport, FeedError> {
        // Anchor BEFORE the peek: everything decodable before this point is
        // covered by the peek below, so on an empty batch the cursor can move
        // here without skipping anything. Keeps G1 honest (and WAL released)
        // when the WAL that advanced holds nothing published — other tables,
        // other databases on the cluster.
        let pre_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
            .fetch_one(&self.pool)
            .await?;

        let batch = self.peek(limit).await?;

        let (pools_marked, floors_lowered) = if batch.events.is_empty() {
            (0, 0)
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
    /// order), then mark every touched pool dirty. One transaction — the
    /// commit here is what makes the subsequent cursor advance safe.
    ///
    /// Floors run BEFORE marks deliberately: the floor UPDATE can block on a
    /// recalc worker's in-flight settle (both write the `pool_settlement`
    /// row), while the mark's `ON CONFLICT DO NOTHING` never blocks — it
    /// silently skips when the queue row exists, even one a worker holds
    /// claimed and is about to delete. Evaluating the mark AFTER the floor
    /// means any such block has already resolved, so the mark sees the
    /// post-settle queue state and re-inserts the row the worker just deleted.
    /// That ordering is what upholds the engine's claim-source invariant: a
    /// pool with a recost floor set always has a `recalc_queue` row.
    pub async fn apply(&self, events: &[TrxLineEvent]) -> Result<(u64, u64), FeedError> {
        let pool_ids: Vec<i64> = events.iter().map(|e| e.pool_id).collect();
        let posted_ats: Vec<String> = events.iter().map(|e| e.posted_at.clone()).collect();
        let ids: Vec<i64> = events.iter().map(|e| e.trx_line_id).collect();

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
        Ok((pools_marked, floors_lowered))
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
    })
}
