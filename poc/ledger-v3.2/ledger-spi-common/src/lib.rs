//! Shared SPI primitives for the ledger-v3.2 extensions.
//!
//! `ledger-direct` (and any future SPI crate — the close-phase `settle_pool`
//! surface) reaches Postgres through the same primitives: per-pool row locking
//! for the ledger-core paths, snapshot hydration, the bulk-write helpers
//! (trx / trx_line / pool_state / posting_line), and the `line_type` text →
//! `ledger_core::LineType` decoder. Keeping one copy here means a lock-ordering,
//! hydration-query, or pool_state-column change lands in every consumer at once.
//!
//! These are plain library functions over pgrx's runtime `Spi` API — no
//! `#[pg_extern]`, no `pg_module_magic!`, no PostgresEnum/Type derives. SQL
//! generation + extension registration stay in each cdylib extension crate; this
//! rlib only carries the shared logic, compiled into each extension's `.so`.
//!
//! These primitives serve the ledger-core dispatch path (STD / specific — the
//! stateful, multi-leg methods). The commutative aggregate paths (WAC and the
//! alt-C FIFO/LIFO appends) are single CTE statements owned by `ledger-direct`
//! and never come through here (design-v3.2 §3).

pub mod bulk_write;
pub mod hydration;
pub mod line_type;
pub mod pool_row_lock;
