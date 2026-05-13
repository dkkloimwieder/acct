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

**Enforced by.** `next_seq()` does `APPLY_SEQ.fetch_add(1, AcqRel) + 1`.
Per-cell updates use `b.last_seq.fetch_max(seq, AcqRel)` in
`try_update_existing` — `fetch_max` not `store`, because under SHARED
two writers can race the CAS-RMW on `balance_qty` and pull seqs from
the global counter in one order but reach the per-cell store in the
other order. A plain `store` would let `last_seq` regress relative to
`APPLY_SEQ`, briefly making a dirty cell look clean to the drain.
`fetch_max` guarantees per-cell `last_seq` only advances regardless of
inter-thread reordering. `insert_new_seeded` is called under EXCLUSIVE
(no race), so plain `store` is correct there. Reset path zeroes both
atomics.

**Pinned by.**
- `tests/invariants_t1.rs::i1_seq_monotonicity` — single-threaded;
  samples `ledger_shmem_apply_seq()` (global) across N applies and
  asserts the cell's final `last_seq` falls within the [pre, post]
  global snapshot.
- `tests/invariants_t1.rs::i1b_per_cell_last_seq_monotonic`
  (acct-fyl3) — multi-writer + dedicated readers tight-looping on
  `ledger_balance_lookup`. Each reader asserts the sequence of
  observed `last_seq` values is non-decreasing within its own view.
  Stress test rather than a falsification gate — the race window
  between `next_seq()` and the store is microseconds-wide and gets
  re-advanced by the next peer apply before a SQL lookup round-trip
  resolves; empirical probing against a buggy `store` variant
  surfaced zero violations across ~27K applies. Test serves as a
  multi-writer regression net + sanity check that final balance
  matches applied deltas.

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

## I8 — Rollback unwinds shmem, COMMIT applies

**Statement.** After `BEGIN; ledger_apply_balance_delta(...); ROLLBACK;`,
the shmem cell does NOT retain the applied delta. After
`BEGIN; ledger_apply_balance_delta(...); COMMIT;`, the cell DOES.

**Why.** Without rollback unwind, every constraint-violation-aborted
transaction silently leaves shmem drifted from the ledger. Recon picks
it up but the discrepancy is structurally inevitable, not exceptional.

**Enforced by.** A2 (acct-4e91, 2026-05-13). `ledger_apply_balance_delta`
STAGES `(amount_delta, qty_delta, captured_rollup_seed)` into a
per-backend `PENDING_STACK`. The XactCallback Commit hook applies
staged deltas; Abort hook discards. SubXactCallback handles
SAVEPOINT (`START_SUB` pushes frame; `COMMIT_SUB` merges into parent;
`ABORT_SUB` pops).

Per acct-17vr (2026-05-13), both callbacks are registered in
`_PG_init` at postmaster startup. Backends inherit the registration
across fork, so the callback path is wired from the first transaction
event in any backend's lifetime. This eliminates a latent edge case
where a backend's first `ledger_apply_balance_delta` call happened
inside an already-open subxact: the lazy registration would miss
that subxact's `SUBXACT_EVENT_START_SUB` and stage into the top-frame.
The existing defensive `is_empty()` guards in `subxact_*` callbacks
masked the symptom for simple cases, but the structural invariant is
now: every START_SUB pushes a frame, every COMMIT_SUB / ABORT_SUB
pops one. `src/lib.rs`: `_PG_init` (registration), `stage_apply`,
`ledger_xact_callback`, `ledger_subxact_callback`, `xact_commit`,
`xact_abort`, `subxact_*`.

**Pinned by.** `tests/rollback_correctness_t1.rs` V1 (minimal rollback
unwind), V2 (recon stays clean after rollback), V3 (commit applies).
Plus `tests/transactional_t1.rs` T2 (savepoint nesting), T3
(cross-backend isolation), T5 (multi-cell collapse), T6 (drain
isolation from staged), T7 (RYW limitation pinned), T8 (first apply
mid-subxact — acct-17vr regression net).

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

## I11 — Reader sees consistent `(balance, qty)` pair

**Statement.** A reader observing a bucket sees a `(balance, qty)` pair
that existed at some single moment — not a torn pair where balance is
post-apply and qty is pre-apply (or vice versa).

**Why.** WAC dispatch divides `pool_value / pool_qty` at apply time. A
torn read could compute against `balance_new / qty_old`, producing an
incorrect unit cost that propagates into both ledger amount and any
audit-field snapshot. This is the AP9 / R7 class of bug.

**Enforced by.** Single atomic 128-bit field `Bucket::balance_qty`
(acct-zo4t / M10.B4-prep). `pack_bal_qty(balance, qty)` stores
`balance` in the high 64 bits and `qty` in the low 64. Writers use a
CAS-loop (`balance_qty_fetch_add`) — lock-free, one writer makes
progress per round. Readers do a single `balance_qty.load(Acquire)` +
`unpack_bal_qty`, returning a real coupled snapshot from one
instant. Linearizable across multi-writer SHARED-LWLock concurrency
(the M9 lock-free hot path's regime).

Implementation note: x86_64 builds enable `target-feature=+cmpxchg16b`
via `.cargo/config.toml` so `portable_atomic::AtomicU128` is genuinely
lock-free (one `lock cmpxchg16b` instruction, not a spinlock fallback).

**Why this over textbook seqlock.** The standard `seq.fetch_add` →
write → `seq.fetch_add` pattern assumes writes are mutually exclusive.
Under M9's lock-free SHARED-LWLock with concurrent atomic `fetch_add`
writers, two writers can interleave such that a reader observes
`s_pre == s_post` between W1's `balance.fetch_add` and W1's
`qty.fetch_add` (W2's enter-increment having pushed seq back to even).
AtomicU128 packs the pair so the writer's RMW is one atomic operation
with no observable mid-state; the seqlock's premise is sidestepped
rather than emulated.

**Pinned by.** `tests/seqlock_torn_read_t1.rs`:
- T1 single-writer correctness
- **T2 torn-read probe** (load-bearing falsification gate;
  pre-B4-prep observable torn read in ~15s, post-B4-prep 0 torn
  reads across millions of observations).
- T3 8-writer composition (lost-update absence)
- T5 16-writer + 16-reader pathology bounded-time

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

## I19 — Same-key applies within one (sub)transaction collapse

**Statement.** Two or more `ledger_apply_balance_delta` calls against
the same packed key within a single (sub)transaction produce exactly
one shmem mutation at COMMIT, with the deltas summed.

**Why.** Without collapse, repeated applies on the same cell would
cause multiple LWLock acquisitions + atomic fetch_adds at commit
time. The PoC's `post_batch_shmem` doesn't typically same-key-apply,
but BOM rollup workloads and future intercompany matching would.

**Enforced by.** `stage_apply` (`src/lib.rs`) — checks
`PENDING_STACK[top]` for an existing entry first; if present, sums
deltas and returns. Otherwise creates a new entry. SubXact COMMIT_SUB
merges popped frame into parent via `merge_entry`, summing same-key
deltas across frames.

**Pinned by.** `tests/transactional_t1.rs::t5_multi_cell_collapse` —
three applies, two distinct keys; asserts post-commit balances reflect
summed deltas and APPLY_SEQ advances by N-distinct-keys, not N-calls.

---

## I20 — Subtransaction rollback discards only its own frame

**Statement.** `ROLLBACK TO SAVEPOINT s` discards the deltas staged
since `SAVEPOINT s` without affecting deltas staged before. `RELEASE
SAVEPOINT s` merges the released subtxn's deltas into its parent,
preserving them for the eventual COMMIT.

**Why.** Standard PG transactional semantics: clients use savepoints
for error recovery. If the extension's staging didn't respect
subxact boundaries, a `ROLLBACK TO` would either lose pre-savepoint
work or fail to discard the inner work.

**Enforced by.** `ledger_subxact_callback` dispatches `START_SUB` →
push fresh frame, `COMMIT_SUB` → pop + merge into parent,
`ABORT_SUB` → pop + discard. `src/lib.rs`: `subxact_start_sub`,
`subxact_commit_sub`, `subxact_abort_sub`.

**Pinned by.** `tests/transactional_t1.rs::t2_savepoint_nesting` —
the canonical example `(+100, savepoint s1, +50, savepoint s2, +20,
rollback to s2, release s1, commit)` asserts post-commit balance =
pre + 150.

---

## Future invariants (placeholder slots)

Reserved IDs for invariants surfaced by remaining M10 sub-issues:

| # | Tentative statement | Tracking issue |
|---|---|---|
| I21 | Hash-full returns recoverable -1 sentinel, not panic | acct-3ee2 (C1) |
| I22 | Bgworker survives SPI errors via consecutive-failure counter | acct-3ovt (C2) |
| I23 | Panic during apply releases LWLock guard cleanly | acct-plle (C6) |
| I24 | GUC `drain_interval_ms` reloads on SIGHUP without restart | acct-vd74 (C4) |
| I25 | Recon under concurrent writes returns coherent rows | acct-7eph (C5) |

**Note on I26.** The placeholder reserved for B4-prep ("seqlock
pattern") was retired when acct-zo4t shipped via `AtomicU128` (single
atomic 128-bit field, not a seqlock retry-loop). The substantive
invariant lives at I11 above. The seqlock placeholder is intentionally
not renumbered or repurposed.

Update this table as each sub-issue ships.

---

## A2 supplementary notes

**Eager rollup_seed capture.** SPI (used by `lookup_rollup_seed`) is
only safe in user-transaction context, not in commit callbacks. The
staging path captures the rollup seed eagerly when the cell is
missing from shmem at apply time, storing it alongside the delta.
At commit, if the cell now exists (another backend's commit raced
in), the captured seed is unused (we fall through to
`try_update_existing`). If the cell still doesn't exist, the seed
feeds `insert_new_seeded`. This preserves M6 lazy-load behavior.

**RYW limitation.** Within a transaction, after staging an apply,
the cell's shmem state is PRE-staging. Reads via
`ledger_balance_lookup` or `balance()` return the unmutated value
until COMMIT fires. The PoC integration (`post_batch_shmem`) doesn't
RYW within a txn; this is non-load-bearing for current workloads.
Pinned by `tests/transactional_t1.rs::t7_ryw_limitation`. A future
RYW-requiring caller would need a TX-local sidecar cache.

**`ledger_apply_balance_delta` return value.** Pre-A2 returned the
new `last_seq`. Post-A2 returns `0` — apply is staging, no seq yet.
Callers should query `ledger_balance_lookup` AFTER commit to observe
the committed `last_seq`. Existing PoC code does not rely on the
apply return.

---

## Deployment notes

### Lock-free `AtomicU128` on aarch64 (acct-dav7)

The `Bucket.balance_qty` field is `portable_atomic::AtomicU128`. Lock-
freedom is what gives readers a coupled `(balance, qty)` pair under
concurrent SHARED-LWLock writers (I11 / acct-zo4t).

| Architecture | Requirement | Lock-free? |
|---|---|---|
| x86_64 | `cmpxchg16b` (Nehalem 2008+) — already in `.cargo/config.toml` `target-features = "+cmpxchg16b"` | Yes |
| aarch64 | LSE atomics (Armv8.1-A+) — `RUSTFLAGS="-C target-feature=+lse"` or `target-cpu` that implies LSE | Yes |
| aarch64 (Armv8.0) | No LSE | **No — falls back to global lock**. Throughput regresses. |

Bench host today is x86_64; aarch64 production deployment is not
in scope but documented for the eventual move. The crate's
`rust-version = "1.87"` pin matches the bench host toolchain;
portable_atomic 1.x + edition 2024 + pgrx 0.16 all support 1.85+,
so 1.85 is the actual floor — 1.87 is a stricter dev-host check.

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
