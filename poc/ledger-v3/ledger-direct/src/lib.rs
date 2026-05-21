//! ledger-direct: pgrx 0.18 extension for ledger-v3 Path A.
//!
//! Authoritative spec: `poc/design_research/design-v3.md` §4.
//!
//! Exposes one SPI function — `ledger_submit_trx(trx_type, source_id, posted_at, lines)`
//! — which executes the 8-step pipeline (sorted pool_lock acquisition, bulk pool_state
//! snapshot read, ledger-core plan_apply, ordered bulk writes) inside the caller's
//! user-tx. One PG transaction per submission, one fsync, caller-visible failures.
//!
//! Modules added by follow-up beads issues:
//! - `submit.rs`           — acct-v4xz (8-step orchestration)
//! - `pool_lock.rs`        — acct-bvps (singleton-loop FOR UPDATE)
//! - `hydration.rs`        — acct-1ucl (snapshot read)
//! - `bulk_write.rs`       — acct-iir7 (UNNEST INSERT/UPSERT/UPDATE/DELETE helpers)
//! - `ledger_error_map.rs` — acct-d74b (LedgerError → ereport!)
//!
//! Per the plan, `pgrx::pg_module_magic!()` and the `_PG_init` hook are added in
//! acct-bnhr (c14: pgrx scaffolding + hello smoke pg_extern); this file is the
//! workspace stub.
