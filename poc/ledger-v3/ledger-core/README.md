# ledger-core

Pure-Rust cost-ledger transformation core for ledger-v3. **No pgrx, no DB** — unit-testable in isolation. Shared by both execution paths (`ledger-direct`, `ledger-routed`).

## API surface

Single public entry point:

```rust
pub fn plan_apply(
    snapshot: &mut Snapshot,
    lines: &[TrxLineRequest],
    posted_at: chrono::DateTime<chrono::Utc>,
) -> Result<PlanResult, LedgerError>;
```

Walks `lines`, dispatches per-pool via `snapshot.method_of` (static match on the closed `PoolMethod` enum — Fifo, Lifo, Wac, Std, Specific), and returns a `PlanResult` with three parallel vectors: `trx_lines`, `pool_state_mutations` (Insert/Upsert/Update/Delete), `posting_lines`. Caller bulk-writes in FK order per design-v3 §4.2 step 7 / §5.4 step 9.

Pristine-snapshot replay (design-v3 §7) is **caller's responsibility**, not `plan_apply`'s — see `src/method.rs` docstring for rationale. `PlanResult::merge()` is provided for Path B's committer to concatenate per-submission results with index translation.

Other public types: `Snapshot { pools, method_of, max_trx_seq_of, std_cost_of }`, `PoolStateRow`, `TrxLineRequest`/`TrxLineOutput`, `PoolStateMutation`, `PostingLineRequest`, `LineType` (9 variants from §2.1), `PostingEventType` (7 variants), `LedgerError` (thiserror-derived: InsufficientInventory, MethodMismatch, UnknownPool, MissingStandardCost, Overflow).

## How exercised

`cargo test -p ledger-core` runs all unit and integration tests. Per-method tests live in `tests/method_<method>.rs`:

```
cargo test -p ledger-core --test method_std       # passing
cargo test -p ledger-core --test method_fifo      # acct-ddnu
cargo test -p ledger-core --test method_lifo      # acct-stvf
cargo test -p ledger-core --test method_wac       # acct-evpw (load-bearing §3.1 guard)
cargo test -p ledger-core --test method_specific  # acct-qnpq
cargo test -p ledger-core --test snapshot_roundtrip  # acct-nmlc
```

Tests are pure `#[test]` functions — no `#[tokio::test]`, no `sqlx`, no DB. The cluster-per-binary convention used by `ledger-direct` / `ledger-routed` does NOT apply here.

## Source layout

- `src/lib.rs` — pub re-exports
- `src/method.rs` — `PoolMethod` + `plan_apply` dispatcher
- `src/snapshot.rs` — `Snapshot`, `PoolStateRow`
- `src/plan.rs` — `PlanResult` + element types + `merge()`
- `src/error.rs` — `LedgerError`
- `src/fifo.rs` / `lifo.rs` / `wac.rs` / `standard.rs` / `specific.rs` — per-method bodies
- `src/seq.rs` — per-pool `trx_seq` allocator helper

`standard.rs` not `std.rs` — avoids `mod std;` shadowing the Rust stdlib inside the crate.
