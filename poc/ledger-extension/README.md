# ledger-extension (acct-sw4i)

pgrx-based Postgres 18 extension. Shared-memory ledger balance rollup with
bgworker drain. Replaces `UPDATE accounts SET balance ... WHERE id = X`
+ `FOR UPDATE` with a per-bucket atomic CAS on a shmem hash.

**Status:** Milestone 1 scaffolding. `CREATE EXTENSION ledger_extension`
will register a single SQL function `ledger_extension_version()` once the
toolchain is available and the crate builds.

## Layout

```
poc/ledger-extension/
├── Cargo.toml          # pgrx 0.16 cdylib + pg18 feature
├── src/lib.rs          # extension entry, version() stub, pg_test scaffold
└── sql/                # post-init SQL deferred until Milestone 4+
```

## Build & install (host → docker container)

Path A (host build, container load) — what this PoC uses:

1. Build the `.so` on the host against PG18 dev headers.
2. Copy `.so` + `.control` + `*.sql` into the running container's extension
   dir (typically `/usr/lib/postgresql/18/lib/` + `/usr/share/postgresql/18/extension/`).
3. `CREATE EXTENSION ledger_extension;` from a `psql` session.

Both `acct` (port 5111 main DB) and `acct_poc` (PoC DB) are in the same
container, so a single install reaches both.

## Milestones

1. ✅ scaffolding (this commit)
2. shmem hash + LWLock tranche
3. `ledger_apply_balance_delta(...)` C function
4. `balance(account_id)` SQL reader (shmem-first, durable fallback)
5. bgworker drain to `account_balances_rollup`
6. Custom WAL RM + redo
7. Recon hook (shmem vs `SUM(posting_lines)` at quiescence)
8. Integration with PoC `post_batch` apply path
9. Bench validation vs `bench_fan_in` / `bench_fan_out` / `bench_wac_fan`

Stop at each milestone, surface, wait for direction (per
`treat-proceed-as-scoped-to-the-specific-item`).
