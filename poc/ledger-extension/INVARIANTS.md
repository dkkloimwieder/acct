# ledger_extension invariants (acct-w88b / M10.D1)

Canonical catalog of every invariant the M9 PoC + M10 hardening must
uphold. Future refactors and new sub-issues update this file when they
pin a previously-unpinned invariant.

Format mirrors REVIEW.md / R1-R7 class-confusion checklist style. Each
entry lists statement, why-it-matters, enforcement site, and pinning
test.

The PoC scope: invariants apply to the extension's apply path
(`ledger_apply_balance_delta`), bgworker drain (`ledger_drain_main`),
read paths (`ledger_balance_lookup`, `balance(...)`), recon
(`ledger_shmem_recon`), and reset (`ledger_shmem_reset`). Cross-process
state (`account_balances_rollup` table) is scope-touching; cross-DB or
cross-cluster state is out of scope until multi-tenant work begins.

Code references are `src/lib.rs` line numbers as of commit `fcbcab8`.
Test paths are relative to the `acct/` workspace root.

---

## I1 — `APPLY_SEQ` and per-cell `last_seq` monotonic

**Statement.** The global `APPLY_SEQ` counter is monotonically
increasing for the lifetime of the postmaster (between resets). For each
occupied bucket, `last_seq` only advances; it never decreases unless
`ledger_shmem_reset` is called.

**Why.** The bgworker uses `last_seq > drained_seq` to decide which
cells are dirty. A non-monotonic `last_seq` (e.g., a stale apply
overwriting a newer one) would cause drain to skip real updates, and the
durable rollup would drift behind shmem indefinitely.

**Enforced by.** `next_seq()` (`src/lib.rs:355`) does
`APPLY_SEQ.fetch_add(1, AcqRel) + 1`. Per-cell stores happen via
`b.last_seq.store(seq, Release)` after fetch_add (`src/lib.rs:383`,
`:434`). Reset path zeroes both atomics (`src/lib.rs:705`, `:709`).

**Pinned by.** `tests/invariants_t1.rs::i1_seq_monotonicity` (this
issue) — samples `ledger_shmem_apply_seq()` across many applies; asserts
strictly increasing.

---

## I2 — `drained_seq` never exceeds `last_seq`

**Statement.** For every occupied bucket, `drained_seq ≤ last_seq` at
all times.

**Why.** The dirty-cell predicate is `last_seq > drained_seq`. If
`drained_seq` could overshoot `last_seq`, a subsequent apply with a new
`last_seq < drained_seq` would be hidden from the drainer forever — the
cell becomes invisibly dirty and the rollup row goes stale.

**Enforced by.** `stamp_drained` (`src/lib.rs:305`) only advances
`drained_seq` via a CAS-max loop guarded by `cur < last_seq`. The
caller (`do_drain_tick` `src/lib.rs:267`) only stamps cells where it
just observed `last_seq > drained_seq` and the apply hasn't moved on.

**Pinned by.** `tests/invariants_t1.rs::i2_drained_le_lastseq` — after
drain quiescence, `ledger_shmem_dirty_count() == 0` and
`ledger_shmem_drained_count() == ledger_shmem_occupied()`. Implies
`drained_seq == last_seq` for every cell; a violation would surface as
`dirty + drained < occupied`.

---

## I3 — `occupied=1` Release implies key + payload visible

**Statement.** A reader that observes `occupied.load(Acquire) == 1` for
some bucket is guaranteed to see the fully-initialized `key_hi`,
`key_lo`, `balance`, `qty`, `last_seq`, `drained_seq` of that bucket.

**Why.** The cold-path insert (`insert_new_seeded`) writes key + payload
with Relaxed ordering, then publishes via `occupied.store(1, Release)`.
Without the release/acquire pair, a SHARED-mode reader probing the same
bucket could see `occupied=1` with stale key bytes, mis-route an apply,
or read garbage.

**Enforced by.** `insert_new_seeded` (`src/lib.rs:425-436`) — Relaxed
writes then `occupied.store(1, Release)`. All readers
(`try_update_existing`, `ledger_balance_lookup`, `do_drain_tick`,
`stamp_drained`, `ledger_shmem_recon`) load `occupied` with Acquire
before reading anything else.

**Pinned by.** STRUCTURAL — Rust's atomic ordering semantics. Not
testable at the Postgres surface; correctness depends on the compiler
and CPU memory model honoring the release/acquire contract.

---

## I4 — `OCCUPIED_COUNT` matches actual occupied buckets

**Statement.** `OCCUPIED_COUNT.load()` equals the number of buckets with
`occupied.load() == 1`.

**Why.** Observability. The counter is read by tests and by future
load-shedding logic ("hash 70% full, switch to fallback"). A drifting
counter masks real hash-fullness issues.

**Enforced by.** `insert_new_seeded` fetch_adds `OCCUPIED_COUNT` after
publishing the cell (`src/lib.rs:437`). `ledger_shmem_reset` zeroes the
counter alongside zeroing the buckets (`src/lib.rs:708`). No
decrement path: cells are not individually deleted.

**Pinned by.** `tests/invariants_t1.rs::i4_occupied_count_consistency`
— apply against K distinct keys; assert occupied increased by K; re-apply
against the same K keys; assert no change.

---

## I5 — Exactly one cell per key

**Statement.** At most one bucket has `occupied=1 && key == K` for any
given packed key K, even under concurrent inserts of K from multiple
backends.

**Why.** If two backends each inserted their own bucket for K, the next
apply would land in one of them and the other would silently retain
stale state. Recon would not detect this; balance() would return
whichever was probed first.

**Enforced by.** Cold-path acquires EXCLUSIVE LWLock
(`src/lib.rs:562`), then re-runs `try_update_existing`
(`src/lib.rs:563`). If a concurrent inserter created the cell while we
waited on the lock, the re-probe finds it and we fall through to update.
Only one insert per key wins the exclusive race.

**Pinned by.** M3 sanity (8-workers × 1 same-key insert race → 1 cell).
Cross-reference: `bench/results-shmem-apply.md`. No regression test
added under D1; M3 coverage is adequate.

---

## I6 — Drain conservation

**Statement.** Every `(account, period, currency, kind)` cell that the
bgworker UPSERTs into `account_balances_rollup` reflects a `(balance,
qty)` snapshot the shmem cell HAD at the captured `last_seq` watermark.

**Why.** Without snapshot consistency, the rollup could publish a torn
`(balance_new, qty_old)` pair. Cross-process readers (balance()
fallback after restart) would see arithmetic that never existed in
shmem.

**Enforced by.** `do_drain_tick` Phase 1 (`src/lib.rs:216-251`) reads
`last_seq` before the data reads (`last_pre`), then re-reads after
(`last_post`); if `last_post != last_pre`, the cell is skipped (the
write was racing). UPSERT carries the snapshot verbatim; stamp_drained
uses the captured `last_pre`.

**Pinned by.** M5 + M7 + M8 end-to-end coverage (drift=0 after drain
quiescence). No new test under D1 — the existing M5 e2e probe is
load-bearing.

---

## I7 — At quiescence, `shmem_balance == SUM(debits) - SUM(credits)`

**Statement.** When no applies are in flight and the bgworker has
drained, `ledger_shmem_recon()` returns `drift = 0` for every cell that
also has matching `accounts` and `posting_lines` state.

**Why.** This is the integration correctness predicate — the extension
hot-path computes the same state the old `UPDATE accounts SET balance`
path would have.

**Enforced by.** `ledger_shmem_recon` (`src/lib.rs:633-692`) — SQL
math, debit-positive convention. The PoC's `post_batch_shmem` integration
(mig 0013) applies `+amount` on debit leg, `-amount` on credit leg,
matching the recon formula by construction.

**Pinned by.** `bench_fan_in.rs` / `bench_fan_out.rs` (the M9 bench
harness asserts recon at end of every run); `tests/rollback_correctness_t1.rs`
V2 currently asserts the BUGGY drift=+1000 — this assertion flips to
drift=0 after A2 (acct-4e91) ships.

---

## I8 — Rollback unwinds shmem (KNOWN GAP, A2 fixes)

**Statement.** After `BEGIN; ledger_apply_balance_delta(...); ROLLBACK;`,
the shmem cell does NOT retain the applied delta. After
`BEGIN; ledger_apply_balance_delta(...); COMMIT;`, the cell DOES.

**Why.** Without rollback unwind, every constraint-violation-aborted
transaction silently leaves shmem drifted from the ledger. Recon picks
it up but the discrepancy is structurally inevitable, not exceptional.

**Currently violated.** Confirmed 2026-05-12 by acct-2733 / M10.A1 via
V1 + V2 probes in `tests/rollback_correctness_t1.rs`.

**Fix.** acct-4e91 (M10.A2) introduces `RegisterXactCallback` for
PreCommit/Commit/Abort and `RegisterSubXactCallback` for savepoints,
plus a per-backend `PENDING_STACK` and `REGISTERED` flag. Applies
stage into the top of the stack; commit hook drains; rollback hook
discards.

**Pinned by.** Currently `tests/rollback_correctness_t1.rs` (asserts
the bug exists). Post-A2, the polarity flips and the same file becomes
the regression net for "rollback unwinds shmem correctly."

---

## I9 — Idempotent replay returns sentinel without double-apply

**Statement.** A `post_batch_shmem` call with the same `idempotency_key`
seen previously returns the `idempotent_replay` sentinel and applies
zero new deltas to shmem.

**Why.** Mainstream-ERP convention. Without this, retried/duplicate
batches (from network retries, supervisor restarts) double-apply.

**Enforced by.** `post_batch_shmem` (mig 0013) CTE chain de-dups
fresh-vs-replay using `posting_lines.idempotency_key` UNIQUE; only
fresh rows feed the `ledger_apply_balance_delta` loop.

**Pinned by.** M8 end-to-end coverage in PoC bench harness (asserts
`idempotent_replay` sentinel + balance unchanged on retry).
`tests/rollback_correctness_t1.rs` V2 also exercises idempotency keys.

---

## I10 — Dimension isolation across (account, period, currency, kind)

**Statement.** Two cells with the same `account_id` but different
`period_id` / `currency_id` / `ledger_kind` are mutually independent —
an apply to one does not affect the other.

**Why.** Multi-period operation (period-close semantics), multi-currency
ledgers, and qty-vs-value-vs-future-WAC distinctions all require strict
per-dimension state. A bug here would smear balances across periods or
currencies.

**Enforced by.** Packed `u128` key (`src/lib.rs:336-341`) combines all
four dimensions; `slot_for` (`src/lib.rs:344`) mixes both halves of the
u128 to avoid clustering. Bucket-key match in `try_update_existing` and
`ledger_balance_lookup` requires exact `(key_hi, key_lo)` equality.

**Pinned by.** Planned in acct-jjqc (B1 currency), acct-nn31 (B2
period), acct-713c (B3 ledger_kind). Not in D1 scope.

---

## I11 — Reader sees consistent `(balance, qty)` pair (TORN-READ GAP)

**Statement.** A reader observing a bucket sees a `(balance, qty)` pair
that existed at some single moment — not a torn pair where balance is
post-apply and qty is pre-apply (or vice versa).

**Why.** WAC dispatch divides `pool_value / pool_qty` at apply time. A
torn read could compute against `balance_new / qty_old`, producing an
incorrect unit cost that propagates into both ledger amount and any
audit-field snapshot. This is the AP9 / R7 class of bug.

**Currently violated.** Each atomic load is independent. A concurrent
`balance.fetch_add` + `qty.fetch_add` between the reader's two loads
is racy.

**Fix.** acct-zo4t (B4-prep) adds a seqlock — even=stable, odd=writing
— on `Bucket`. Writer bumps seq before+after the `(balance, qty)`
writes; reader retries on odd or changed seq.

**Pinned by.** acct-zo4t — not in D1 scope.

---

## I12 — Open-addressing probe terminates

**Statement.** `try_update_existing` and `ledger_balance_lookup` always
return within `N_BUCKETS` probe steps, regardless of hash distribution.

**Why.** Hung extension entry points wedge backends and trip PG's
deadlock detector unpredictably. Bounded probe depth is the structural
guarantee.

**Enforced by.** `for probe in 0..N_BUCKETS` (`src/lib.rs:371`,
`:425`, `:591`) with early-return on `occupied=0` (chain end) or key
match.

**Pinned by.** STRUCTURAL — the bounded loop is the guarantee.
`insert_new_seeded` raises `error!` if it walks the full table without
finding an empty slot (`src/lib.rs:441-444`); becomes a recoverable
Result post-acct-3ee2 (C1).

---

## I13 — Bgworker scoped to one database

**Statement.** The `ledger_drain` bgworker connects to the database
named in `ledger.drain_database` (default `acct_poc`) and writes
`account_balances_rollup` rows only there. Other databases in the same
cluster see no rollup writes.

**Why.** Multi-DB clusters are common in dev (acct + acct_poc + future
acct_test). Without a hard scoping rule, a misconfigured cron or
restart could redirect drains to the wrong DB and contaminate state.

**Enforced by.** `ledger_drain_main` (`src/lib.rs:187`) calls
`BackgroundWorker::connect_worker_to_spi(Some(&dbname), None)` with
`dbname` read from `DRAIN_DATABASE` GUC.

**Pinned by.** `tests/invariants_t1.rs::i13_drain_database_guc` —
asserts the GUC reads as `acct_poc`; observes that applies in
`acct_poc` land in `acct_poc.account_balances_rollup`. A full
second-DB probe requires test infrastructure deferred to acct-vd74 (C4
GUC reload).

---

## I14 — Reset zeros all shmem state

**Statement.** After `ledger_shmem_reset()`: `OCCUPIED_COUNT == 0`,
`APPLY_SEQ == 0`, every bucket has `occupied=0` and zeroed payload,
`ledger_balance_lookup` on any prior key returns NULL. Rollup rows in
`account_balances_rollup` are NOT touched (by design — that's a SQL
table, not shmem).

**Why.** Tests and benches rely on reset for clean baselines. A reset
that misses a field would leak state across runs and corrupt
measurement.

**Enforced by.** `ledger_shmem_reset` (`src/lib.rs:697-710`) iterates
every bucket and stores 0 in every atomic; zeroes both global
counters.

**Pinned by.** `tests/invariants_t1.rs::i14_reset_completeness` —
applies N deltas across K cells, drains, calls reset, asserts every
read surface returns zero/none.

---

## I15 — Common-path apply is lock-free beyond SHARED LWLock

**Statement.** `ledger_apply_balance_delta` against an existing cell
acquires the LWLock in SHARED mode and performs atomic `fetch_add` on
`balance`, `qty`, `last_seq`. Multiple concurrent SHARED holders update
the same OR different buckets without blocking each other.

**Why.** This is the architectural premise of the extension — the
mutable `UPDATE accounts SET balance ... FOR UPDATE` cost is replaced
by per-bucket atomic deltas. M9 bench validates the lift (2.16× fan-in,
5.55× fan-out).

**Enforced by.** `ledger_apply_balance_delta`
(`src/lib.rs:549-552`) — SHARED LWLock guard around
`try_update_existing`. The atomics inside use AcqRel ordering;
LWLock SHARED only serializes against EXCLUSIVE callers (the rare cold
insert path).

**Pinned by.** M9 bench numbers in
`poc/batch-ledger/bench/results-shmem-apply.md`. The 5.55× lift over
mutable `post_batch` at fan-out is the load-bearing measurement; any
regression below ~3× at fan-out should re-open this invariant.

---

## I16 — Apply path takes no locks on `accounts` table

**Statement.** `ledger_apply_balance_delta` (and by extension
`post_batch_shmem` for the apply legs) never acquires any row lock,
table lock, or FOR UPDATE on the `accounts` table.

**Why.** This is what removes the deadlock surface that mutable
`post_batch` exhibits. M9 measured zero deadlocks across 18 runs at
both fan-in and fan-out — direct consequence.

**Enforced by.** STRUCTURAL — no SQL in the apply path touches
`accounts`. The optional SPI lookup (`lookup_rollup_seed`,
`src/lib.rs:450`) reads `account_balances_rollup`, not `accounts`. The
PoC integration migration 0013's `post_batch_shmem` body INSERTs
`posting_lines` rows only; the UPDATE-accounts loop from `post_batch`
is removed.

**Pinned by.** acct-mii6 (B8, deferred) will add explicit "concurrent
DDL/onboarding on accounts" probes; the M9 bench already pins this
invariant indirectly via the zero-deadlocks finding.

---

## I17 — `balance()` reader: shmem-first, rollup-fallback, none

**Statement.** The SQL `balance(account, period, currency, kind)`
function returns:
- `source='shmem'` when the cell is present in shmem.
- `source='rollup'` when the cell is absent in shmem but present in
  `account_balances_rollup`.
- `source='none'` with `(0, 0, 0)` when neither.

**Why.** After a PG restart, shmem is empty until cells are touched.
The rollup is the durable backup; readers must fall through correctly.
Mislabeling source breaks observability and any future replication
logic.

**Enforced by.** `balance()` body in `extension_sql!`
(`src/lib.rs:744-788`) — plpgsql STABLE function; explicit IF/THEN
branching on `lookup` result presence and `account_balances_rollup`
lookup.

**Pinned by.** M4 end-to-end coverage (6 scenarios — both empty,
rollup-only, both-present, shmem-only, key-not-found, dimension-mix).
No regression added under D1.

---

## I18 — M6 lazy-load: new cell starts at rollup state + delta

**Statement.** When `ledger_apply_balance_delta` encounters a cell
missing from shmem but present in `account_balances_rollup`, the new
shmem cell initializes at `(rollup.balance + delta, rollup.qty +
qty_delta)` with `drained_seq = rollup.last_seq` and an advanced
`last_seq > drained_seq` (so the next bgworker tick correctly drains
the cell).

**Why.** Without lazy-load, every PG restart loses every untouched
cell's running total. Recon would flag every account as drifted. With
lazy-load, the restart loss profile is "shmem-only deltas not yet
drained" — bounded to one drain interval (default 100ms).

**Enforced by.** `insert_new_seeded(rollup_seed=Some(...))`
(`src/lib.rs:412-419`) — initialization branches on `rollup_seed`;
`APPLY_SEQ.fetch_max(last_seq)` ensures monotonicity of the global
counter; the subsequent `next_seq()` returns a strictly greater value
than `init_drained_seq`.

**Pinned by.** M6 e2e probe (apply 1000 → drain → restart → apply +50
→ shmem cell is 1050 not 50). No regression added under D1.

---

## Future invariants (placeholder slots)

Reserved IDs for invariants surfaced by M10 sub-issues:

| # | Tentative statement | Tracking issue |
|---|---|---|
| I19 | Per-cell apply via XactCallback is atomic with PG commit | acct-4e91 (A2) |
| I20 | SubXact rollback discards only the aborted savepoint's deltas | acct-4e91 (A2) |
| I21 | Hash-full returns recoverable -1 sentinel, not panic | acct-3ee2 (C1) |
| I22 | Bgworker survives SPI errors via consecutive-failure counter | acct-3ovt (C2) |
| I23 | Panic during apply releases LWLock guard cleanly | acct-plle (C6) |
| I24 | GUC `drain_interval_ms` reloads on SIGHUP without restart | acct-vd74 (C4) |
| I25 | Recon under concurrent writes returns coherent rows | acct-7eph (C5) |

Update this table as each sub-issue ships.

---

## Rules for adding/changing invariants

1. **Lead with the predicate, not the implementation.** "balance and
   qty are loaded atomically" is implementation; "reader sees a
   consistent (balance, qty) pair" is the invariant.
2. **Cite enforcement to specific lines.** `src/lib.rs:NNN` references
   may drift across refactors; re-anchor on commit hash when
   meaningfully changed.
3. **One pinning test per invariant.** If an invariant is structural,
   say so explicitly — don't leave the "Pinned by" field empty.
4. **Don't catalog "wishes."** Only invariants the code currently
   guarantees (or will, with a referenced fix-in-flight).
5. **When a fix ships, flip the invariant statement to its enforced
   form.** I8 currently says "KNOWN GAP" — when A2 lands, rewrite to
   the enforced positive form and cite the new test.
