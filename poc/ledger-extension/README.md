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
       occupied shmem cell at PoC convention `(1, 1, 1)`. Computes
       ledger truth via debit-positive convention
       (`SUM(debits) - SUM(credits)`), matching `post_batch` semantics.
       Returns `(account_id, shmem_balance, shmem_qty, ledger_balance,
       drift)`. NULL ledger_balance for orphan cells (no `accounts`
       row). Multi-dimension cells filtered out (M8 parameterization
       deferred to acct integration).
8. ✅ `post_batch_shmem` PoC migration (0013) — drop-in replacement for
       `post_batch`'s `UPDATE accounts SET balance` path. Inserts
       posting_lines via the same CTE chain as `post_batch_append_only`,
       captures fresh vs replay rows in a TEMP TABLE, then iterates
       fresh rows applying `+amount` on the debit leg and `-amount`
       on the credit leg via `ledger_apply_balance_delta`. End-to-end
       verified: 4 envelopes across 4 accounts → recon drift=0;
       bgworker drains within 1s; replay returns `idempotent_replay`
       without double-applying.
9. ✅ Bench validation (`bench/results-shmem-apply.md`). 3×60s
       replicates at fan-in (50 accts) and fan-out (5000 accts):
       fan-in 31K → **67K tps (2.16×)** over mutable `post_batch`;
       fan-out 7.8K → **43.5K tps (5.55×)**. Latency p99 at fan-out
       drops 9.6s → 708ms (13×). Zero deadlocks across 18 runs.
       N_BUCKETS bumped 4096→16384 to fit the 5K fan-out workload
       (production needs GUC sizing — future hardening).

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

## M10 hardening (epic acct-tpqw)

M10.A1 ✅ (acct-2733, 2026-05-12) — confirmed the rollback correctness
gap. Test `poc/batch-ledger/tests/rollback_correctness_t1.rs` runs two
probes: V1 minimal `BEGIN; apply; ROLLBACK;` retains the delta in shmem;
V2 INSERT-posting-line + apply + ROLLBACK leaves drift=+1000 against an
empty posting_lines table. M10 Track A (A2 XactCallback + SubXactCallback,
B4-prep seqlock, B4 WAC integration) is correctly scoped to fix the
divergence. The test's assertion polarity flips after A2 ships — drift=0
becomes the regression-net assertion for "rollback unwinds shmem
correctly."

M10.D1 ✅ (acct-w88b, 2026-05-12) — invariant catalog landed at
[`INVARIANTS.md`](INVARIANTS.md). Five previously-unpinned invariants
(I1 seq monotonicity, I2 drained ≤ last_seq, I4 OCCUPIED_COUNT
consistency, I13 bgworker per-DB scope, I14 reset completeness) now
have explicit pinning tests in
`poc/batch-ledger/tests/invariants_t1.rs`.

M10.A2 ✅ (acct-4e91, 2026-05-13) — deferred-apply via XactCallback +
SubXactCallback. `ledger_apply_balance_delta` STAGES into a
per-backend PENDING_STACK; commit applies, rollback discards;
SAVEPOINT supported via SubXactCallback. Closes the rollback
correctness gap M10.A1 confirmed: rollback_correctness_t1.rs V1/V2
assertions FLIPPED to drift=0 (regression net). Plus
transactional_t1.rs adds 5 tests covering savepoint nesting,
cross-backend isolation, multi-cell collapse, drain isolation,
RYW-limitation pinning. INVARIANTS.md gains I19 (same-key collapse)
+ I20 (savepoint discard) pinned by the new tests; I8 statement
flipped from "KNOWN GAP" to enforced form.

A2 perf delta vs M9 documented in
[`bench/results-shmem-apply-A2.md`](../../batch-ledger/bench/results-shmem-apply-A2.md).

M10.B4-prep ✅ (acct-zo4t, 2026-05-13) — atomic 128-bit `(balance, qty)`
pair on `Bucket`. Replaces separate `AtomicI64` fields with a single
`portable_atomic::AtomicU128`; writers CAS-loop (lock-free), readers do
one atomic 128-bit load + unpack. Closes the I11 torn-read gap so WAC's
`unit_cost = pool_value / pool_qty` always divides coupled values.
Chosen over the textbook seqlock pattern: under M9's lock-free SHARED-
LWLock with concurrent `fetch_add` writers, the standard seqlock can be
defeated by two writers interleaving their `seq.fetch_add` enters such
that a reader's `s_pre == s_post` check spuriously passes; AtomicU128
sidesteps this by making the pair RMW one atomic op. Falsified pre-fix
by `tests/seqlock_torn_read_t1.rs::t2_torn_read_probe` (captured
torn read `(balance=38022000, qty=38021)` within 15s); post-fix passes
0 torn reads / >100K observations. INVARIANTS.md I11 flipped from
"TORN-READ GAP" to enforced; T1/T2/T3/T5 pinned. Bench delta vs M9
documented in
[`bench/results-shmem-apply-B4prep.md`](../../batch-ledger/bench/results-shmem-apply-B4prep.md).

Remaining M10 sub-issues (open):
- acct-n4mo — B4 post_batch_wac_shmem + bench (~2-3 days)
- acct-jjqc / nn31 / 713c — B1/B2/B3 multi-dimension scenarios
- acct-j0nh / jh9k — B6/B7 backpressure + load-factor curves
- acct-mii6 — B8 concurrent accounts table activity
- acct-3ee2 / 3ovt / e5gl / vd74 / 7eph / plle — C1-C6 error handling

Stop at each milestone, surface, wait for direction (per
`treat-proceed-as-scoped-to-the-specific-item`).
