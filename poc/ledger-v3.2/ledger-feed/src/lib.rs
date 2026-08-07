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

pub use consumer::{FeedConsumer, FeedError, IngestReport, PeekBatch, TrxLineEvent};
