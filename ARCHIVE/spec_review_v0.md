# Spec Review — `ledger_inventory_design_spec_v0.md` and `phased_migration_spec_v0.md`

Reviewer pass. Findings are grouped by severity. Each finding cites the spec and section it applies to and proposes a direction (not a finished fix) so the spec authors can decide.

Severity legend:
- **Blocker** — design will not work as written; must be resolved before code.
- **Major** — internally contradictory, materially under-scoped, or operationally unsafe.
- **Minor** — naming, completeness, or clarity issue; cheap to fix.
- **Note** — observation worth recording, no required action.

---

## Blocker findings

### B1. Self-pending reservation pattern is mechanically impossible

**Source:** `ledger_inventory_design_spec_v0.md` §5.3 (lines 263–283), §△-4 (line 564).

**Claim in the spec:**
> ```
> T_reserve  ledger=1 credit (sku, loc) Available, debit (sku, loc) Available
>            flags=pending, timeout=<reservation_ttl_seconds>
> ```
> Reservation is a *self-pending* on the Available account. `debits_pending` on that account is total reserved qty.

**Problem:**

A transfer with `debit_account_id == credit_account_id` is rejected by TigerBeetle (`accounts_must_be_different`, see TB result codes). The Postgres ledger spec enforces the same rule directly:

`phased_migration_spec_v0.md:118` — `CHECK (debit_account_id <> credit_account_id)`.

So the pattern as written cannot post in *either* system. The §△-4 note frames this as a tradeoff between "self-pending" and "Available/Reserved pair," but the self-pending branch is not actually a viable option — it's a non-starter.

**Secondary issue.** Even if same-account transfers were allowed, the "Promisable" formula given in §5.3 — `credits_posted − debits_posted − debits_pending` — has the wrong sign for an asset-normal account flagged `credits_must_not_exceed_debits`. For such an account, on-hand is `debits_posted − credits_posted` and promisable would subtract pending *credits* (outflows reserved), giving `debits_posted − credits_posted − credits_pending`. The formula and the supposed mechanism are both upside-down.

**Proposed direction:**

Drop the self-pending option entirely. Pick one of:

1. **Available + Reserved account pair per (sku, location).** Reservation = pending transfer `Available → Reserved`. Allocation = `post_pending`. Cancel = `void_pending`. Doubles account count for inventory but is mechanically simple, queryable, and uses standard TB primitives.
2. **Per-SO ephemeral reservation account.** Lazily create one Reserved account per (SO, sku) on first reservation; close on order completion. Avoids permanent doubling at the cost of more `create_accounts` calls.

Resolve §△-4 to option 1 in the v0.2 spec, update §5.3 to use a paired account, and remove the §△ marker.

---

### B2. Multiple linked-batch patterns require a balance read on the write path, contradicting §2

**Source:** `ledger_inventory_design_spec_v0.md` §2 ("NEVER perform a TB lookup that gates a subsequent TB write on the hot path beyond the balances TB itself returns"), versus §5.4, §5.5, §6.2.

**Patterns that violate the rule:**

| Section | Field | Source of value |
|---------|-------|-----------------|
| §5.4 `OP_MOVE T2` | `accumulated_cost_per_unit * qty` | `WIP_OpNN_Value.balance / WIP_OpNN.qty` |
| §5.4 `WO_COMPLETE T3` | `residual` | `WIP_Op30_Value.balance − qty * std_cost` |
| §5.5 `SCRAP T2` | `accumulated_cost` | `WIP_OpNN_Value.balance / WIP_OpNN.qty * scrap_qty` |
| §6.2 WAC alternative | unit cost at issue | `Raw_Inv_Value.balance / qty_on_hand` |

Each value is computed from the current balance of an account that is *also* being mutated in the same linked batch. That is exactly the lookup-then-write pattern §2 forbids.

**Why it matters:**
- Under concurrency, the read-then-write race means two op-moves on the same WIP_Op account can both read pre-update balances and produce diverging accumulated-cost figures.
- Even single-threaded, "balance at time of read" is not what posts — the actual accounting depends on the cluster-assigned timestamp ordering of the batch, which the API does not control.
- The Phase 1 Postgres implementation can mitigate via `FOR UPDATE` locks (it already does that in `post_transfer_batch`), but TB has no equivalent — once you migrate (Phase 4+), the pattern silently becomes racy.

**Proposed direction:**

Resolve the conflict explicitly. Three options, in order of architectural cleanliness:

1. **Move accumulated-cost computation out of the linked batch.** Have the projector maintain `wip_unit_cost_running` per (parent_sku, op) and read it from the projection at API-tier compose time. Accept the read-after-write window: at op-move time, you read a slightly stale unit cost. The error self-corrects at WO close via the WO_Close_V variance bucket. This is the model the spec already commits to for everything else.
2. **Use standard cost for OP_MOVE/SCRAP value transfers and absorb deltas at WO close.** The SCRAP_V and WO_CLOSE_V codes already exist for this. Accept that intra-WO value is approximate until close.
3. **Allow the read-then-write pattern for these specific cases and amend §2** to read "no read-then-write within a linked batch *for the same account*" or "read-then-write is permitted under a documented locking discipline (Phase 1) and replaced by projection-driven precompute (Phase 4+)."

Whichever is chosen, write it down. As of v0.1, two parts of the same spec disagree.

---

### B3. Phase 0 double-entry invariant test is unsound for multi-currency

**Source:** `phased_migration_spec_v0.md` §3.5 line 345, §4.7 line 484.

**Claim:**
> **Double-entry:** `SUM(debits_posted) = SUM(credits_posted)` across all accounts, always.

**Problem:**

Once ledger 1 (qty, unitless), ledger 840 (USD, minor units), ledger 978 (EUR, minor units) coexist, the global sum mixes incompatible units. Either:
- The assertion fails on every multi-currency batch (false positives that train operators to ignore it), or
- It happens to net to zero by numerical coincidence and *masks* a real per-ledger imbalance.

**Proposed direction:**

```sql
-- Per-ledger:
SELECT ledger, SUM(debits_posted) - SUM(credits_posted) AS imbalance
FROM ledger_accounts
GROUP BY ledger
HAVING SUM(debits_posted) <> SUM(credits_posted);
```

This must return zero rows. Update both the Phase 0 invariant in §3.5 and the daily reconciliation in §4.7 to use the per-ledger form. Same fix applies to `ledger_inventory_design_spec_v0.md` §11 if it is restated there.

---

## Major findings

### M1. `_apply_transfer_effect` is materially under-scoped

**Source:** `phased_migration_spec_v0.md` §3.2 lines 213–217.

**Claim:**
> ```
> -- (full implementation: ~200 LoC, branching on transfer flags)
> -- handles: posted vs pending totals, invariant checks, closing flag side-effects
> ```

**Problem:**

Faithful semantic parity with TB's transfer flag matrix is the central engineering risk of the entire roadmap. The flag combinations that must be implemented:

| Flag | Behavior |
|------|----------|
| `pending` | Increments `*_pending`, NOT `*_posted`. Records timeout. Subsequent post/void resolves. |
| `post_pending_transfer` | Decrements debit/credit `*_pending`, increments debit/credit `*_posted`. Optional partial amount (TB allows posting a smaller amount than the original pending). |
| `void_pending_transfer` | Decrements debit/credit `*_pending`. No `*_posted` change. Idempotent against expiry. |
| `balancing_debit` | Amount = `min(requested, debit_account.available_under_limit)`. Atomic read-min-write under lock. |
| `balancing_credit` | Symmetric. |
| `closing_debit` / `closing_credit` | Sets `account.flags |= closed` as a side effect. Closed accounts reject future non-voiding transfers. |
| `imported` | Permits user-supplied timestamps; requires monotonicity within a homogeneous batch; bypasses some normal validation. |
| `linked` | Chain failure rolls back the entire batch (already shown in the parent function). |

Plus the result-code enumeration TB returns: `ok`, `exists`, `linked_event_failed`, `linked_event_chain_open`, `pending_transfer_not_found`, `pending_transfer_already_posted`, `pending_transfer_already_voided`, `pending_transfer_expired`, `exceeds_credits`, `exceeds_debits`, `accounts_must_have_the_same_ledger`, `account_not_found`, `transfer_must_have_the_same_ledger_as_accounts`, `imported_event_timestamp_*` (several), and a dozen others.

Every code-path divergence between the Postgres implementation and TB's behavior is a future migration bug. The "200 LoC" estimate is wrong by 5–10× when result codes, flag interactions, and edge cases (post a pending in a closed account, void after expiry, balancing transfer hitting limit zero) are honored.

**Proposed direction:**
- Replace the `~200 LoC` estimate with a function-by-flag implementation table sized realistically (~1,500–2,500 LoC PL/pgSQL).
- Add the result-code matrix as a §3.2.1 subsection — without it, "ok/exists/error" in the parent function is hand-waved.
- Add explicit conformance tests in §3.5 invariant tests: a fixture of TB-recorded `(input, expected_output)` pairs the Postgres function must match. This is the only way to enforce parity.
- Re-baseline the Phase 0 timeline (2–3 weeks → 6–10 weeks is more realistic).

---

### M2. `NUMERIC(39)` everywhere is an unacknowledged performance compromise

**Source:** `phased_migration_spec_v0.md` §3.1 lines 65–145.

**Issue:**

NUMERIC in Postgres is variable-width, software-implemented arithmetic. Rough relative cost vs `BIGINT`:

| Operation | NUMERIC(39) | BIGINT | Ratio |
|-----------|-------------|--------|-------|
| Add/subtract | software | hardware | ~5–10× |
| Index width | 16+ bytes | 8 bytes | 2× |
| HOT update friendliness | worse | better | — |

For inventory qty (units, fits in i32 for almost everyone) and value (minor currency units, fits in i64 up to ~$92 quadrillion), `BIGINT` is more than enough. The NUMERIC(39) choice is made for **migration symmetry** with TB's u128, not for application need.

That tradeoff is defensible — it eliminates a Phase 4 conversion step — but it should be stated. Currently the spec presents `NUMERIC(39)` as if it were the only option.

**Proposed direction:**

Add to §3.1:

> **Type choice rationale.** All amount and ID columns use `NUMERIC(39)` to mirror TB's u128 exactly. This costs ~5–10× on arithmetic and 2× on index width versus BIGINT. We accept the cost in exchange for zero conversion at Phase 4 cutover. If profiling shows arithmetic to be a bottleneck before Phase 3 entry criteria are met, evaluate switching amount columns to BIGINT (sufficient for ledgers in our value range) and keeping NUMERIC(39) only for IDs.

Also: §△ this so it surfaces during Phase 1 perf work.

---

### M3. `user_data_128 UUID` is more restrictive than `u128`

**Source:** `phased_migration_spec_v0.md` §3.1 lines 69, 110.

**Issue:**

`UUID` in Postgres is a 128-bit type but enforces UUID version/variant bits in some validation paths (and in any UUID-typed library that consumes it). Not every TB `u128` is a valid UUID. If any user_data_128 value comes from somewhere other than a UUID generator (e.g., a hash, a TB-generated ULID-shaped id, an external system's u128), the schema rejects it.

The design spec (`ledger_inventory_design_spec_v0.md` §3.4) defines `user_data_128 = Document id`, and document IDs in the Postgres canonical tables are `UUID PRIMARY KEY`. So in the steady state the constraint may be fine — but it forecloses options.

**Proposed direction:**

Use `NUMERIC(39)` for `user_data_128` (matching the account/transfer ID columns). Cast to UUID at the application layer when displaying, not at the storage layer. The cost is one type alias; the benefit is that `user_data_128` keeps the same value-space as TB.

Alternatively, declare explicitly: "`user_data_128` is a UUID by convention; non-UUID u128 values are not supported." Either is fine; silence is not.

---

### M4. Phase 4 Step 2 "halt outbox worker briefly (~30s downtime)" is glossed over

**Source:** `phased_migration_spec_v0.md` §7.2, lines 660–666.

**Claim:**
> Halt outbox worker briefly (~30s downtime).

**Problem:**

The roadmap's whole *justification* for Phase 3+ is sustained 10K TPS (`§6.1`). 30 seconds at that rate = 300K queued events at the API tier. Either:

- The API tier rejects writes for 30s — visible to customers as ~30s of failed orders. Unacceptable for a goods-flow system.
- The API tier buffers in memory — risk of OOM, lost requests if a node restarts.
- The API tier writes to outbox but the outbox doesn't drain — fine, but then the post-resume drain has 300K rows to chew through and is itself a contention spike on TB during the most fragile moment of the cutover.

None of these are acceptable without explicit handling.

**Proposed direction:**

Replace the "halt" approach with a streaming cutover:

1. At T=0, snapshot all balances for the subledger atomically (pg_dump-style or `LOCK TABLE … IN SHARE MODE` for milliseconds, not seconds).
2. Submit `imported` transfers to TB establishing opening balances at T=0, while live writes continue to Postgres (and are also being shadow-written to TB at their actual timestamps T>0).
3. At T=imported_complete, flip the routing flag. New writes go to TB authoritatively. Postgres receives shadow writes during the reverse-shadow window.

If a brief halt really is required, quantify it ("~50ms while we acquire the snapshot lock") and explain how the API tier handles it (return 503 with retry-after, queue in memory bounded, etc.). The current text implies the team will simply absorb 30s of API downtime, which is implausible for the workload size that triggers the migration in the first place.

---

### M5. Phase 3 entry gates AND three correlated signals

**Source:** `phased_migration_spec_v0.md` §6.1, lines 565–571.

**Claim:** all three of `lock_waits > 0.5%`, `P99 write latency > threshold`, and `>10K TPS with hot-account skew` must hold to justify Phase 3.

**Problem:**

The signals are not independent. Lock contention causes latency. High TPS with hot-account skew causes lock contention. In practice, the system either trips all three together (and the AND is just a complicated way of saying "write throughput is the bottleneck") or none. The conjunction looks rigorous but is mostly aesthetic.

**Proposed direction:**

Either:
- Replace with a single signal: "sustained write-side contention on hot accounts that is not improved by index/sharding/tuning, demonstrated over a 4+ week peak window." Define "demonstrated" as a specific dashboard threshold.
- Keep the three-signal form but note explicitly that the signals are correlated and the AND is a *belt-and-suspenders* check intended to reject false-positive single-spike weeks. (This may be the actual intent; if so, say so.)

Either way, §△-H should be promoted from "calibrate later" to "calibrate during Phase 1 with real data and lock the threshold before Phase 2 ends."

---

### M6. Commodity §17.6 WAC default leaves permanently-wrong unit costs on consumed inventory

**Source:** `ledger_inventory_design_spec_v0.md` §17.6 lines 696–711.

**Claim:**
> Default policy: **book the delta going forward via the settlement batch; do NOT retroactively recompute WAC.**

**Problem:**

`Raw_Inv_Value`, COGS, and WIP are trued up at settlement via PRICE_TRUEUP_* transfers — fine. But the **per-unit cost recorded on each individual COGS event during the unsettled window** is permanently wrong. The §11 reconciliation can't catch it because the *sums* tie. An auditor reviewing per-shipment margin will see the discrepancy.

The §17.5 settlement batch books the delta at the *aggregate* level (debit COGS, amount = Δ_consumed). It does not (and cannot, without per-event rewriting) restate the per-shipment cost.

For most businesses this is immaterial. For commodity-driven margin businesses (grain, metals trading, livestock) where a single price swing can shift unit costs by 10%+, it can be material.

**Proposed direction:**

Add an explicit materiality clause:

> Default policy is forward-only true-up. If aggregate Δ for a settlement exceeds X% of the settling cohort's value at provisional pricing, additionally book a `COST_RESTATE` reversal-and-rebook against the affected COGS events (one per consumed shipment of the cohort). X is set by accounting policy; suggested starting threshold 5%.

Or, accept the limitation and document it in §17.7 as an explicit non-decision tied to materiality: "Per-shipment unit cost in COGS reflects provisional price for shipments closed before settlement. This is GAAP-acceptable on materiality grounds and is not corrected. Margin reports involving provisionally-priced commodities should be read with this caveat."

Either choice is fine. Silence is not — auditors will surface it.

---

### M7. `expire_pending_transfers` re-enters `post_transfer_batch` under load

**Source:** `phased_migration_spec_v0.md` §3.3 lines 270–304.

**Issue:**

The expiry worker calls `post_transfer_batch` once per expired pending. Each call:
- Acquires its own account locks in ID order
- Could deadlock against a concurrent live write (different acquisition path? no — same ordered locking, so deadlock-safe — but contention-heavy)
- Issues a separate Postgres transaction per expiry

Under heavy reservation churn (cart timeouts, SO reservation expiry storms after a flash sale), this becomes a serial bottleneck.

**Proposed direction:**

Batch the expiry: gather N expired pendings, build one JSONB array, call `post_transfer_batch` once with all N void events. The deterministic-ID idempotency guarantees safety if multiple workers race.

Also worth a §△ for sub-second timeout precision (already noted as §△-A) — but the batching issue is separate from precision.

---

## Minor findings

### m1. Account taxonomy gaps

`ledger_inventory_design_spec_v0.md` §3.2 table omits accounts that appear later:

- `Physical_Adj_Pool(sku)` — used in §5.8 cycle-count adjustment.
- `Inventory_Adj_Expense` — used in §5.8.
- `FX_Revaluation` — used in §9.
- `Creation_Void` — listed (good), but the convention "global singleton, fixed known id" should specify *what* the id is so it's reproducible across deployments.

**Fix:** add a row per missing account to the §3.2 tables with grain and id source.

---

### m2. Naming inconsistencies

- "Quarantine pool" is `(sku, hold_pool)` in §3.2 but `(sku, Quarantine_Pool)` in §5.6.
- "Scrap" is `(sku, scrap_pool)` in §3.2 but `(parent, Scrap_Pool)` in §5.5.
- `tb_account_map` in `ledger_inventory_design_spec_v0.md` §3.3 vs the same name carried into Phase 1 in `phased_migration_spec_v0.md` §4.5 with a note that it's an "abstraction, not the system." Confusing for new readers.

**Fix:** pick `snake_lower` or `Title_Case` and apply consistently. Rename `tb_account_map` to `ledger_account_map` and add a one-line aliasing note for those who read the design spec first.

---

### m3. `code` u16 reserve-range commentary is unnecessary

`ledger_inventory_design_spec_v0.md` §3.4. The 65,536-value space will never be approached. Reserve ranges are a nice editorial structure for *humans*, not a constraint that needs justification.

**Fix:** drop the "code is u16 (65,536 max)" sentence; keep the range table.

---

### m4. §5.7 AR/AP payment is too terse

`ledger_inventory_design_spec_v0.md` §5.7 says only "Standard double-entry, ledger=ccy only. Reference invoice via `user_data_128`." Every other §5.x section spells out the linked batch in the consistent format. §5.7 should match — show the `T1: credit AR / debit Cash` and the AP_PAYMENT counterpart explicitly. Costs four lines, removes ambiguity.

---

### m5. `flags.history` semantics across docs

Design spec §3.5 says set `flags.history` on accounts feeding period-end snapshots. Phase 0 schema (`phased_migration_spec_v0.md:84`) marks the bit "reserved; no-op in PG phase, retained for parity."

Implication: in Phase 1, period close cannot rely on TB-style historical balance queries. The migration spec should state explicitly *how* period snapshots are produced in Phase 1 (the projector populates `period_snapshots` from the changefeed at period-close time) and note that the `history` flag becomes load-bearing only in Phase 4+ for accounts resident in TB.

**Fix:** one paragraph in §5.x of the migration spec covering "history flag semantics across phases."

---

### m6. Doc 2 §4.4 perf claims need sourcing or hedging

- "10–50× speedup over per-row calls" for outbox batching — depends entirely on `_apply_transfer_effect` per-call overhead. Plausible upper bound, optimistic lower bound. Hedge to "5–20×" or measure before committing.
- "synchronous_commit = local … 2–5× on small transactions" — true on commit-bottlenecked workloads, less so on lock-bottlenecked ones. Add "depending on workload."
- "COPY … 3–5× faster" — true for *very* large batches; for a 100-event batch, multi-row INSERT is comparable. The §△-D note is right to flag this.

**Fix:** soften the numbers or back them with a benchmark reference.

---

### m7. Reverse-migration §8.2 is hand-waved

`phased_migration_spec_v0.md` §8.2: "Bulk-insert opening-balance transfers into Postgres at T=0." This requires an `imported`-equivalent path in `post_transfer_batch` — TB's `imported` flag bypasses some validation; the PG function as specified does not honor `imported` differently. If you import historical opening-balance transfers, you need:

- User-supplied timestamps allowed only when flag bit 256 is set (already in the schema).
- Monotonicity within an imported batch.
- Same-batch homogeneity (all imported or none) — currently not enforced.
- Bypass of certain invariant checks during the import? (TB allows some; spec should decide.)

**Fix:** spell out the imported-flag semantics in §3.2 of the migration spec, even if it's just "TODO — required before Phase 5 reverse migration is real."

---

### m8. Cross-system batch policy strategy B sacrifices atomicity quietly

`phased_migration_spec_v0.md` §7.3 strategy B: "Outbox two-phase with idempotent retry." This is correct as a mechanism but the consequence is not stated: there is a *visible window* where Postgres has the debit half and TB does not yet have the credit half (or vice versa). Reports run during that window will see an imbalance. Reconciliation will alert.

**Fix:** add to §7.3 a paragraph explicitly listing what an in-flight cross-system batch looks like to a reader, what window an alert can fire in, and that the projection masks this by not committing partial events to its own tables until both sides are in.

---

## Notes (no action required)

### N1. The §△ deferral mechanism is excellent

Both specs use §△ markers consistently and gather them in a single index (`§14`, `§10`). This is the right shape for an in-flight design. The only critique is volume (see N2).

### N2. Thirty open §△ items across two v0.1 specs is a smell, not a fault

Some of the §△ items are *gating* decisions that need answers before Phase 1 ships:
- §△-K (cross-system transaction policy)
- §△-15 (commodity attribution policy: FIFO vs proportional vs all-to-variance)
- §△-10 (period-lock enforcement at API)
- §△-M (reverse migration playbook)

Others are genuine "revisit with data" items that should remain open:
- §△-1 (per-SKU vs per-(SKU,location) value)
- §△-2 (qty ledger sharding)
- §△-7 (standard → WAC transition)

**Suggestion (not a finding):** split the §△ list into "gating" (must close before Phase N) and "monitoring" (close on signal). It will make the deferral list tractable instead of intimidating.

### N3. Outbox-as-shared-substrate is the strongest idea in the migration spec

Worth stating explicitly: the reason migration is "mechanical, not architectural" is that the outbox table is the seam — same rows, different sink. Phase 0 invests in that seam; Phase 4+ collects the dividend. This deserves its own section or a callout in §0 (`Guiding principles`). It is the load-bearing idea of the entire roadmap.

### N4. Per-subledger cutover with reverse-shadow is correctly identified as the right shape

Ability to roll back at every step is the property that makes the migration safe. Keep this front and center; do not let it weaken under schedule pressure.

### N5. Explicit non-decisions sections (§15, §11) are excellent

They prevent scope creep cheaply. Recommend adding to them as new "we deliberately don't do X" decisions arise during implementation, rather than letting them disperse into prose.

---

## Suggested next steps

1. **Resolve B1, B2, B3.** These are correctness bugs and must be fixed before code starts.
2. **Re-baseline Phase 0 timeline** in light of M1 (`_apply_transfer_effect` scope). 6–10 weeks is more realistic than 2–3.
3. **Promote gating §△ items to v0.2 decisions.** Specifically §△-K, §△-15, §△-10, §△-M, §△-4, §△-H. Keep monitoring §△ items as-is.
4. **Add a Type Choice Rationale subsection** addressing M2, M3 in one place.
5. **Walk the cutover (M4) with the API team** before the spec promises 30s of downtime.
6. **Add a conformance test fixture** (M1 follow-on) so Postgres/TB parity is enforced mechanically, not by inspection.
