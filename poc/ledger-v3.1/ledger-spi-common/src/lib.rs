//! Shared SPI primitives for the ledger-v3.1 Path C extensions.
//!
//! `ledger-direct-c` (synchronous SPI flavor) and `ledger-routed-c` (BGWorker
//! committer flavor) both reach Postgres through the same four primitives:
//! per-pool `FOR UPDATE` acquisition, snapshot hydration, the bulk-write helpers
//! (trx / trx_line / pool_state / posting_line), and the `line_type` text →
//! `ledger_core::LineType` decoder. Keeping one copy here means a lock-ordering,
//! hydration-query, or pool_state-column change lands in both flavors at once
//! instead of silently skipping one.
//!
//! These are plain library functions over pgrx's runtime `Spi` API — no
//! `#[pg_extern]`, no `pg_module_magic!`, no PostgresEnum/Type derives. SQL
//! generation + extension registration stay in each cdylib extension crate; this
//! rlib only carries the shared logic, compiled into each extension's `.so`.
//!
//! Orchestration differs by flavor: direct calls `bulk_write::apply_plan_result`
//! (the per-submission convenience wrapper); the routed committer drives the
//! individual `insert_*` / `apply_pool_state_mutations` helpers itself so a whole
//! commit_group's depletions collapse into one aggregate UPSERT per pool (§6.7).

pub mod bulk_write;
pub mod hydration;
pub mod line_type;
pub mod pool_lock;
