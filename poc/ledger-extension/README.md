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

```
cargo build --release --features pg18 --no-default-features \
    --manifest-path poc/ledger-extension/Cargo.toml
cargo pgrx schema pg18 \
    --manifest-path poc/ledger-extension/Cargo.toml \
    --out poc/ledger-extension/sql/ledger_extension--0.0.1.sql
bash poc/ledger-extension/scripts/install-into-container.sh
psql 'postgres://acct:acct_dev@localhost:5111/acct_poc' \
    -c 'CREATE EXTENSION ledger_extension;'
```

Both `acct` (port 5111 main DB) and `acct_poc` (PoC DB) live in the same
container (`acct-postgres`), so a single `docker cp` reaches both.

ABI compatibility: host glibc 2.42, container glibc 2.41. The .so requires
at most GLIBC_2.34 (verified via `objdump -T | grep GLIBC`), so the
container can load the host-built binary.

## Milestones

1. ✅ scaffolding + host→container install validated end-to-end
2. ✅ shmem hash (4096 slots, open addressing) + `PgLwLock` +
       `PgAtomic<u64>` occupied counter + apply_seq counter; SQL surface:
       `ledger_shmem_capacity()`, `ledger_shmem_occupied()`,
       `ledger_shmem_apply_seq()`, `ledger_balance_set(key,bal,qty)`,
       `ledger_balance_get(key)`, `ledger_shmem_reset()`.
       Cross-backend + cross-DB visibility verified. PG restart wipes shmem
       (M6 adds WAL recovery).
3. `ledger_apply_balance_delta(...)` C function
4. `balance(account_id)` SQL reader (shmem-first, durable fallback)
5. bgworker drain to `account_balances_rollup`
6. Custom WAL RM + redo
7. Recon hook (shmem vs `SUM(posting_lines)` at quiescence)
8. Integration with PoC `post_batch` apply path
9. Bench validation vs `bench_fan_in` / `bench_fan_out` / `bench_wac_fan`

Stop at each milestone, surface, wait for direction (per
`treat-proceed-as-scoped-to-the-specific-item`).
