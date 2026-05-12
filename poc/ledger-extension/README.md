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
       `PgAtomic<u64>` occupied counter + apply_seq counter.
       Cross-backend + cross-DB visibility verified.
3. ✅ Per-bucket atomics (AtomicU8/U64/I64) + packed u128 key
       (account_id<<64 | period_id<<32 | currency_id<<16 | ledger_kind<<8)
       + dual-lock hot path (SHARED for updates, EXCLUSIVE for inserts
       with re-probe). SQL surface:
       `ledger_apply_balance_delta(account_id, period_id, currency_id,
       ledger_kind, amount_delta, qty_delta)` and `ledger_balance_lookup`.
       Concurrency verified: 8 workers × 100 updates on shared cell →
       balance=8000 no lost updates; 8 × 30 distinct inserts → 240 cells
       no duplicates; 8 × 1 same-key insert race → 1 cell.
4. ✅ `account_balances_rollup` durable projection table + `balance(...)`
       SQL reader implementing shmem-first / rollup-fallback / none.
       6 scenarios validated: both empty (none), rollup-only (rollup),
       both-present (shmem wins), shmem-only (shmem), key-not-in-either-
       dimension (none), per-dimension mixing across cells. Post-restart
       verified: shmem-only cells lost; rollup-backed cells survive
       (M5 + M6 close that loss profile).
5. ✅ `ledger_drain` bgworker — connects via SPI to `ledger.drain_database`
       (default `acct_poc`), wakes every `ledger.drain_interval_ms`
       (default 100ms), walks shmem under SHARED lock to gather cells
       where `last_seq > drained_seq`, UPSERTs each into the rollup
       table, then CAS-max bumps `drained_seq` per success. Three new
       SQL functions: `ledger_shmem_dirty_count()`,
       `ledger_shmem_drained_count()`, plus `drained_seq` field per
       bucket. End-to-end verified: applies → dirty=3 → after 100ms
       wait drained=3 + rollup has rows; new apply re-dirties only the
       affected cell; post-restart cells now serve from rollup with
       correct values (the M4 loss profile is closed for drained cells).
7. ✅ `ledger_shmem_recon()` — at quiescence, returns one row per
       occupied shmem cell at the PoC convention
       `(period, currency, ledger_kind) = (1, 1, 1)` showing
       `(account_id, shmem_balance, shmem_qty, ledger_balance, drift)`.
       Ledger balance computed from `posting_lines` (signed by
       `accounts.kind`). NULL ledger_balance + NULL drift for orphan
       shmem cells (no matching `accounts` row). Other-dimension cells
       filtered out — M8's integration step parameterizes the filter.
       6 scenarios verified: synchronized (drift=0), shmem-ahead,
       posting_lines-only, re-sync, orphan, multi-dimension filtering.

6. ✅ Lazy-load from rollup at insert. The cold-path `insert_new` now
       SPI-queries `account_balances_rollup` (before acquiring the
       exclusive lock) for the cell's prior durable state; if found,
       seeds the new shmem bucket with `(rollup_balance + delta,
       rollup_qty + qty_delta)` and sets `drained_seq = rollup.last_seq`
       so the bgworker correctly sees the new state as dirty.
       `APPLY_SEQ.fetch_max(rollup.last_seq)` ensures the new cell's
       `last_seq` is strictly greater than its `drained_seq`.
       End-to-end verified: apply (1000) → drain → restart → apply (+50)
       → shmem cell is 1050 not 50 → next drain writes 1050 to rollup.
       Closes the only loss profile from M5.
3. `ledger_apply_balance_delta(...)` C function
4. `balance(account_id)` SQL reader (shmem-first, durable fallback)
5. bgworker drain to `account_balances_rollup`
6. Custom WAL RM + redo
7. Recon hook (shmem vs `SUM(posting_lines)` at quiescence)
8. Integration with PoC `post_batch` apply path
9. Bench validation vs `bench_fan_in` / `bench_fan_out` / `bench_wac_fan`

Stop at each milestone, surface, wait for direction (per
`treat-proceed-as-scoped-to-the-specific-item`).
