//! ledger-feed — the recalc feed consumer (design-v3.1 §17 / design-v3.2 §6).
//!
//! Consumes the `ledger_feed` logical-decoding slot (pgoutput over the SQL
//! logical-decoding interface), records delivered `trx_line` inserts into the
//! durable dirty-set (`recalc_queue` + `pool_settlement.recost_floor_*`), and
//! advances `confirmed_flush_lsn` on ingestion (recalc-c D8 option B). The
//! binary (`src/main.rs`) is a thin poll loop over [`FeedConsumer::ingest_once`];
//! recalc workers drain the dirty-set independently (phase 4).

pub mod consumer;
pub mod pgoutput;

/// The canonical slot and publication names. Fixed literals, not
/// configuration: `ledger_close_period`'s feed-currency gate and the
/// `feed_slot_health` gauge both match the slot by this exact name, so a
/// renamed slot would silently evade both while the feed appeared to work
/// (acct-1vur.2). One name is the only coherent posture.
pub const SLOT_NAME: &str = "ledger_feed";
pub const PUBLICATION_NAME: &str = "ledger_feed";

pub use consumer::{FeedConsumer, FeedError, IngestReport, PeekBatch, TrxLineEvent};
