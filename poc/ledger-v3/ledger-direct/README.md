# ledger-direct

pgrx 0.18 extension implementing **Path A** of ledger-v3 (design-v3 §4). Synchronous SPI function executes the full ledger work inside the caller's user-tx: one PG transaction per submission, one fsync, caller-visible failures via `ereport!(ERROR, ...)`.

## API surface

One SPI function:

```sql
SELECT ledger_submit_trx(
    p_trx_type   trx_type,
    p_source_id  bigint,
    p_posted_at  timestamptz,
    p_lines      <line array>   -- (line_type, source_id, pool_id, qty, unit_cost, debit_account, credit_account)
) RETURNS bigint;  -- the new trx.id
```

Internally orchestrates the 8 steps from design-v3 §4.2: compute touched pools → acquire `pool_lock FOR UPDATE` in sorted order (deadlock-free) → bulk-read snapshot (`pool_state` + `pool.method` + `MAX(trx_seq)` per pool) → call `ledger_core::plan_apply` → bulk-write `trx` + `trx_line` + `pool_state` (Insert/Upsert/Update/Delete) + `posting_line` in FK order → return `trx.id`. ~9 SPI calls for a single-line PO receipt.

## How exercised

Build + install into the running `acct-postgres` container:

```bash
bash scripts/install-direct.sh                 # acct-2npt
```

Then run the regression suite via the cluster-per-binary harness:

```bash
bash scripts/run-tests.sh --path direct        # acct-21yx
```

The harness restarts the Postgres container between each test binary — required because pgrx test state (extension load, fixture rows, sequence values) leaks across binaries within one Postgres lifetime. See [bd memory `v21-test-harness-isolation-via-cluster-per-binary`] and the v21 precedent. Property tests are gated by `--ignored` and run via `cargo test --release -p ledger-direct property -- --ignored`.

## Source layout (filled in by follow-up issues)

- `src/lib.rs` — `pg_module_magic!()`, `_PG_init`, hello smoke pg_extern (acct-bnhr)
- `src/submit.rs` — 8-step orchestration (acct-v4xz)
- `src/pool_lock.rs` — sorted singleton-loop FOR UPDATE (acct-bvps)
- `src/hydration.rs` — snapshot read (acct-1ucl)
- `src/bulk_write.rs` — UNNEST INSERT/UPSERT/UPDATE/DELETE helpers (acct-iir7)
- `src/ledger_error_map.rs` — LedgerError → SQL exception with proper ERRCODEs (acct-d74b)

Features: `pg18` (default), `pg_test`, `test_hooks` (gates test-only `ledger_direct_test_*` pg_externs OUT of production .so builds).
