# Database

Postgres 18 dev environment for the acct ledger project.

## Configuration

- **Image:** `acct-postgres:18`, built from `db/Dockerfile` (FROM `postgres:18`, adds `postgresql-18-cron`).
- **GUC overrides** (set via `command:` in `docker-compose.yml`):
  - `io_method=io_uring` — async I/O backend introduced in PG 18. The official `postgres:18` binary is built `--with-liburing`; verified with `ldd $(which postgres)`.
  - `shared_preload_libraries=pg_stat_statements,pg_cron`
  - `cron.database_name=acct`
- **Extensions** (created by `db/init/01-extensions.sql` on first boot):
  - `pg_stat_statements` — query-level perf observability.
  - `pg_cron` — scheduled jobs (used by reservation expiry and daily reconciliation, future Phase 0 work).
- **Database / user:** `acct` / `acct` (password `acct_dev`, dev only).
- **Port:** host `5111` → container `5432` (host port chosen to avoid clashing with other local Postgres containers).
- **Data volume:** named volume `acct-pgdata` mounted at `/var/lib/postgresql` (PG 18+ convention to enable `pg_upgrade --link`). Preserved across `dev-down.sh` unless `--wipe`.
- **seccomp:** `seccomp:unconfined` is set on the dev container because Docker's default seccomp profile blocks the `io_uring_setup` / `io_uring_enter` / `io_uring_register` syscalls. Acceptable for local dev. Production deploys use **`db/seccomp/postgres-iouring.json`** — a copy of Docker's default profile (moby v28.0.0) with exactly those three syscalls added to the default-allow list (`acct-hbp`). Wire it in by replacing `seccomp:unconfined` with `seccomp:./db/seccomp/postgres-iouring.json` in the production compose file. The profile was verified by temporarily swapping the dev container to use it: postgres started cleanly with `io_method=io_uring`, both extensions loaded, smoke test passed, and the rest of Docker's syscall restrictions remain in place by construction (diff vs upstream is exactly +3 lines).

## Usage

```bash
./scripts/dev-up.sh        # build, start, verify io_method and extensions
./scripts/dev-down.sh      # stop (data preserved)
./scripts/dev-down.sh --wipe   # stop and delete data volume
```

Connect:

```
psql 'postgres://acct:acct_dev@localhost:5111/acct'
```

## Verification (run by `dev-up.sh`)

- `pg_isready` succeeds.
- `SHOW io_method` returns `io_uring`.
- `pg_extension` contains `pg_stat_statements` and `pg_cron`.

## Migrations

Plain `.sql` migrations live under `db/migrations/`, applied via `sqlx-cli`. Reversible (`.up.sql` / `.down.sql`), sequential numbering.

Install `sqlx-cli` once:

```bash
cargo install sqlx-cli --no-default-features --features rustls,postgres
```

Run migrations against the dev DB:

```bash
./scripts/run-migrations.sh
```

The script defaults `DATABASE_URL` to `postgres://acct:acct_dev@localhost:5111/acct` if unset. `.env.example` at the repo root documents the dev URL; copy to `.env` if you'd rather have sqlx-cli pick it up automatically.

Direct invocations:

```bash
sqlx migrate run    --source db/migrations
sqlx migrate info   --source db/migrations
sqlx migrate revert --source db/migrations
```

### Adding a migration

```bash
sqlx migrate add --source db/migrations -r --sequential <name>
```

This creates `NNNN_<name>.up.sql` and `NNNN_<name>.down.sql`. Both files are mandatory — `migrate revert` requires the down file.

### Verify (S2 acceptance)

Against a clean DB:

```bash
./scripts/run-migrations.sh                       # 0001 applied
sqlx migrate info   --source db/migrations        # shows installed
sqlx migrate revert --source db/migrations        # rolls back
./scripts/run-migrations.sh                       # redeploys cleanly
```

## Write path (`post_transfers`)

The canonical write entry-point is the PL/pgSQL function `post_transfers(p_events JSONB, p_override_closed_period BOOLEAN DEFAULT FALSE) RETURNS JSONB`. Every document write (PO receipt, SO ship, work-order op_move, scrap, AR/AP, etc.) calls this function with one or more events; it locks the involved accounts in ascending id order, validates each event, posts the resulting balance changes and `transfers` rows, and returns a per-event result array.

Phase 0 calls it **synchronously inside the same Postgres transaction** as document writes — no outbox (per D3, `acct-93b.3`).

### Event input shape

```json
{
  "reason":            "po_receipt",
  "document_kind":     "purchase_order",
  "document_id":       "<uuid>",
  "document_line_id":  "<uuid>",
  "debit_account_id":  12,
  "credit_account_id": 34,
  "amount":            100,
  "routing_op":        10,
  "counterparty_id":   "<uuid>",
  "business_date":     "2026-04-15",
  "idempotency_key":   "<uuid>",
  "posted_by":         "<uuid>"
}
```

`document_line_id`, `routing_op`, `counterparty_id` are optional and may be omitted. All other fields are required — the function does **not** validate field presence upfront; missing required fields surface as Postgres cast or NOT NULL errors.

**Bifurcated input contract for cost-relevant reasons** (acct-0ig, migration 0019). For reasons in `{op_move, scrap, wo_complete, so_ship}`:

- **Value-ledger events**: caller MUST send `qty`, NOT `amount`. The function computes amount via the `_post_transfers_compute_amount` dispatcher keyed on `skus.cost_method`. Caller-supplied `amount` is ignored on these events.
- **Qty-ledger events**: caller still sends `amount` (= qty by convention). Unchanged from earlier behavior.

The dispatcher has these real branches: `'standard'` (`amount = qty × resolve_standard_cost_at(sku, business_date)` after migration 0027 / `acct-x4t`); `'wac_perpetual'` (migration 0021 / `acct-uxu`); `'wac_periodic'` (dispatcher branch added in migration 0029 / `acct-qfj`); `'wac_retroactive'` (dispatcher branch added in migration 0031 / `acct-9tw`). All three WAC branches use `amount = qty × (pool_value_balance / per_class_qty)` where `per_class_qty` is `SUM(transfers.qty signed by debit/credit on the value pool)` — refactored in migration 0030 / `acct-1vr` from the previous `stock_available.balance` divisor, which would have pooled raw and fg qty for the same SKU. The `'lot'` and `'fifo'` branches RAISE `P0006` with a TODO referencing `acct-8gg`. The qty-side gate relaxes for `'standard'`, `'wac_perpetual'`, `'wac_periodic'`, and `'wac_retroactive'`.

**Per-event qty column on `transfers`** (acct-1vr, migration 0030). `transfers.qty BIGINT NULL` carries the physical quantity for inventory-touching events. Populated at INSERT time from the event JSONB or, for qty-leg events where both sides are `ledger_kind='qty'`, inferred from `amount`. NULL for non-inventory transfers (cash, AR, AP, FX) and pre-0030 historical rows. The new column is what makes per-class WAC math work: each value pool's qty divisor is computed from its own transfer history, not the pooled `stock_available` balance.

For all other reasons (anything not in the cost-relevant set): caller sends `amount`. Unchanged.

### Per-event return

```json
{ "index": 1, "result": "ok" }     // applied
{ "index": 2, "result": "exists" } // idempotent duplicate, skipped
```

`index` is 1-based. Idempotent duplicates do **not** roll back the batch; they just skip and continue. Any other failure raises an exception with one of the codes below, which rolls back the **entire batch** (linked-batch semantics).

### Error codes

| SQLSTATE | Name | Condition |
|---|---|---|
| `P0001` | `account_closed` | debit or credit account `is_closed = TRUE` |
| `P0002` | `ledger_mismatch` | `debit.ledger_kind ≠ credit.ledger_kind` |
| `P0003` | `currency_mismatch` | both ledger_kind=`'value'`, currencies differ |
| `P0004` | `period_missing` | no period contains `business_date` |
| `P0005` | `period_closed` | period `closed_at IS NOT NULL` and `p_override_closed_period = FALSE` |
| `P0006` | `cost_method_not_implemented` / `wac_zero_qty_pool` | `reason ∈ {op_move, scrap, wo_complete, so_ship}` AND any of: (a) sku not resolvable from either account, (b) sku's `cost_method ∈ {'lot','fifo'}` on a qty-side event (the qty-side gate relaxes for `'standard'` and `'wac'`), (c) value-side event missing `qty`, (d) value-side event with sku's `cost_method ∈ {'lot','fifo'}` (raised inside the dispatcher), (e) value-side event with sku's `cost_method = 'wac'` and the qty pool is zero. Also raised by `post_inventory_adjustment` when called against a `'fifo'` or `'lot'` SKU. |
| `P0010` | `caller_bug_account_missing` | A required account does not exist for the (sku, location, currency) tuple. Raised by `reserve_inventory()` (no open `stock_available` for the sku/location pair) and by `post_inventory_adjustment` (no open `stock_available`, no open `inv_value_{class}`, no `creation_void(qty)`, or no `inv_adj_expense(currency)`). Indicates accounts must be pre-created. |
| `P0011` | `cost_assertion_invalid` | Caller's `p_unit_cost` violates the contract for the SKU's `cost_method`. Raised by `post_inventory_adjustment` when: (a) the SKU is `'standard'` and caller passed any non-NULL `p_unit_cost` (standard SKUs have a fixed cost; do not pass one), (b) the SKU is `'wac'` IN with `p_unit_cost = NULL` and the pool is empty (must seed at known cost), (c) `cost_method` is otherwise unrecognized. |
| `P0014` | `period_close_invalid` | `close_period(p_period_id, ...)` called against a missing period or one whose `closed_at IS NOT NULL`. Raised by `close_period`. Two concurrent calls on the same period both raise this — the loser re-reads `closed_at` after the `FOR UPDATE` lock is released and finds it stamped. |
| `P0015` | `period_close_provisional` | `close_period` refused: `transfers_provisional` has rows in this period with `finalized_at IS NULL`. Caller can pass `p_force_provisional := TRUE` to bypass; un-finalized rows are then left on the side table for forensics. |
| `P0016` | `period_close_reconciliation` | `close_period` refused: `run_daily_reconciliation()` raised one or more new alerts. Caller can pass `p_force_recon := TRUE` to bypass; alerts are still recorded. |
| `P0017` | `optimistic_concurrency_violation` | `post_standard_cost_roll` caller passed `p_expected_old_cost` that does not match the active standard at `p_business_date`. Surfaces stale-read bugs in callers (UI showed an old value before another roll landed). NULL/non-NULL mismatch in either direction also raises this. |
| `P0018` | `standard_cost_not_established` | A cost-relevant operation on a standard-method SKU was attempted but no `standard_costs` row is in effect at the requested `business_date`. Resolved by calling `post_standard_cost_roll()` first. Backdated transactions before the earliest `effective_at` also surface this code. Raised by `resolve_standard_cost_at` and inherited by every consumer that goes through it (currently `_post_transfers_compute_amount` standard branch, `post_inventory_adjustment` standard branch with NULL `p_unit_cost`). |
| `P0019` | `retroactive_std_cost_roll_blocked` | `post_standard_cost_roll` caller passed `p_effective_at` that is not strictly greater than every existing `standard_costs.effective_at` for the SKU. Phase 1 does not support retroactive corrections to past standard costs. |
| `P0020` | `wac_periodic_close_no_receipts` | `wac_periodic_close_hook` (composed through `close_period`) found a pool with un-finalized `wac_periodic` provisional rows but zero in-period receipts; cannot compute the period's `final_avg = Σ(receipts value) / Σ(receipts qty)`. Operator either (a) posts a receipt for the period and retries close, or (b) calls `close_period(..., p_force_provisional := TRUE)` which skips the un-processable rows and leaves them on the side table for forensics. |
| `P0021` | `target_period_closed` | `post_cost_adjustment_retroactive` caller passed `p_target_period_id` for a period whose `closed_at IS NOT NULL`. The retroactive cost-adjustment workflow only operates on currently-open periods (the queue is the period's audit trail and must be visible to `close_period`). To fix a closed period, reopen it first; reopen workflow is tracked as `acct-7h4` (Phase 2 Epic K). |
| `P0022` | `po_receipt_invalid` | `post_po_receipt` caller passed an unknown PO id, a PO with no `vendor_id`, an unknown `po_line_id`, a `po_line` belonging to a different PO, an empty lines array, or `qty_received <= 0`. Document-layer caller bug; the function rejects before posting any transfers. |
| `P0023` | `po_line_overreceived` | `post_po_receipt` would push cumulative `SUM(po_receipt_lines.qty_received)` for a `po_line` past its `qty_ordered`. Strict for Phase 1 (no over-receipt tolerance); over/under-receipt with tolerance windows is Phase 2. |
| `P0024` | `ap_bill_three_way_mismatch` | `post_ap_bill` `po_match` line failed strict three-way match: `qty` exceeds the received-not-billed remainder for the referenced `po_line`, OR `unit_cost` differs from `po_line.unit_cost`, OR `amount` ≠ `qty × unit_cost`. Caller resolves by issuing a `cost_adjustment` (§3.12) for cost discrepancies, or reversal+rebook for qty discrepancies. |
| `P0025` | `ap_bill_invalid_line` | `post_ap_bill` semantic line violation: unknown vendor, empty bill, unknown line `kind`, `po_match` line whose `po_line_id` belongs to a different vendor than the bill, currency mismatch between `po_line` and bill, `service` line missing or pointing at a closed expense account, or expense account on the wrong ledger / wrong currency. CHECK at the table layer catches table-level shape violations (NULL where required, etc.); this code surfaces what the function pre-empts. |
| `P0026` | `wo_invalid` | `post_wo_start` / `post_op_move` / `post_wo_complete` / `post_scrap` rejected the call: WO id not found, status not draft (for `post_wo_start`) or not released (for the others), `parent_sku.cost_method ≠ 'standard'` (Slice B MVP gate; lifted under `acct-p7v`), empty `wo_routings` for the WO, qty ≤ 0, scrap qty exceeds `stock_wip` pool balance at the op, or a `wo_routing_burdens.applied_account_kind` that has no `_wo_apply_reason_for` mapping (e.g. caller seeded `cogs` as a burden kind). |
| `P0027` | `wo_qty_overflow` | `post_wo_complete` or `post_scrap` would push `qty_completed + qty_scrapped + this_qty` past `qty_target`. Strict for Slice B MVP (no over-completion / over-scrap tolerance). |
| `P0028` | `routing_op_invalid` | `post_op_move` got `from_op = to_op`, or `from_op` / `to_op` (or `post_scrap`'s `routing_op`) is not in this WO's `wo_routings`. |
| `P0029` | `bom_missing` | `post_wo_start` found zero `boms` rows for `parent_sku_id`. WO parents must declare at least one component. |

The L3 append-only trigger (`P9999`) and the L2 balance-respects-normal-side CHECK (`23514`) are defenses in depth; neither should fire from inside `post_transfers` under normal use.

The numbering gaps (P0007–P0009, P0012–P0013) are intentional. Codes were considered during Phase 0 / Phase 1 design but either dropped (replaced by the broader codes that landed) or rolled into adjacent ones. The gaps are reserved; new codes claim the next un-used number rather than refilling.

## Document-layer wrappers

Phase 1 introduces document-layer functions that wrap `post_transfers` for common workflows. Callers use these instead of constructing event JSONB by hand.

### `post_inventory_adjustment` (migration 0022, acct-sb6)

Pure inventory adjustment in or out at a given (qty, unit_cost). Insert + post-transfers in one call, idempotent at the document level.

Signature:
```
post_inventory_adjustment(
  p_sku_id          UUID,
  p_location_id     UUID,
  p_qty_delta       BIGINT,   -- signed; >0 = in, <0 = out
  p_unit_cost       BIGINT,   -- NULL = use system cost (see dispatch below)
  p_currency        TEXT,
  p_inventory_class TEXT,     -- 'raw' or 'fg' (MVP; wip deferred)
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT      DEFAULT NULL
) RETURNS UUID                -- inventory_adjustments.id
```

Behavior:
- Inserts an `inventory_adjustments` row (UNIQUE on `idempotency_key`); a replay with the same key returns the existing id without re-posting.
- Resolves `stock_available(sku, location)`, `inv_value_{class}(sku, location, currency)`, `creation_void(qty)`, and `inv_adj_expense(currency)` (the bidirectional P&L counterpart); raises `P0010` if any account is missing.
- Dispatches on the SKU's `cost_method` to determine the effective unit cost:

| `cost_method` | `p_unit_cost = NULL` | `p_unit_cost = explicit` |
|---|---|---|
| `standard` | use `resolve_standard_cost_at(sku, business_date)` — raises **P0018** if no `standard_costs` row in effect | **P0011** — standard SKUs have a fixed cost; do not pass one |
| `wac_perpetual` IN | use pool average; **P0011** if pool empty (must seed) | use it; pool re-averages |
| `wac_perpetual` OUT | use pool average; **P0010** if pool empty | **P0011** — asserted cost on depletion belongs in `'lot'` cost_method (acct-8gg) |
| `wac_periodic` | use pool average; flagged in `transfers_provisional` for re-cost at close (`acct-qfj`, migration 0029); P0006 if pool empty on depletion | **P0011** (asserted-cost-on-depletion is `'lot'` territory) |
| `wac_retroactive` | use pool average; flagged in `transfers_provisional` for chronological replay at close (`acct-9tw`, migration 0031) | **P0011** (asserted-cost-on-depletion is `'lot'` territory) |
| `fifo` / `lot` | always **P0006** (acct-8gg) | always **P0006** |

  Pool reads happen under `FOR UPDATE` on the qty + value accounts, locked in ascending id order to match `post_transfers`' lock-order invariant.

- Builds a 2-event batch with reason `inventory_adjustment` (qty leg + value leg), sign-flipped on negative `qty_delta`. Skips the value leg when the effective unit cost is 0 (only possible when the SKU's resolved standard is 0).
- Calls `post_transfers(batch, FALSE)` — closed-period override is not exposed here.
- Records the **effective** unit cost (what was actually applied) in the audit row's `unit_cost` column, not the caller's input.

The `inventory_adjustment` transfer_reason is added by migration 0022 alongside the table and function. It is distinct from `cycle_count_adj`, which is reserved for cycle-count document workflows.

See consolidated doc §3.11 for the full design notes.

### `post_cost_adjustment` (migration 0024, acct-14m)

Value-only revaluation of a WAC pool's per-unit average cost. Distinct from `post_inventory_adjustment` — that one moves qty + value together; this one moves only value. Use cases: lower-of-cost-or-market write-down, quality-revealed cost overstatement, late vendor credit, audit correction.

Signature:
```
post_cost_adjustment(
  p_sku_id           UUID,
  p_location_id      UUID,
  p_currency         TEXT,
  p_inventory_class  TEXT,        -- 'raw' or 'fg'
  p_target_unit_cost BIGINT,      -- the new pool avg the user wants
  p_business_date    DATE,
  p_posted_by        UUID,
  p_idempotency_key  UUID,
  p_notes            TEXT      DEFAULT NULL
) RETURNS UUID                    -- inventory_cost_adjustments.id
```

Behavior:
- Reads `current_qty`, `current_value` of the pool under `FOR UPDATE` (locks both stock_available and inv_value_* in ascending id order).
- Computes `delta = target_unit_cost * current_qty - current_value`.
- `delta > 0` (write-up): debit `inv_value_*`, credit `variance_cost_adjustment` (revaluation gain).
- `delta < 0` (write-down): debit `variance_cost_adjustment`, credit `inv_value_*` (revaluation loss).
- `delta = 0`: audit row recorded, no transfer posted (no-op).
- Pool qty ≤ 0: **P0010** (cannot revalue an empty pool).
- Cost-method dispatch:

| `cost_method` | Behavior |
|---|---|
| `standard` | **P0011** — standard SKUs have a fixed cost; update `skus.standard_cost` separately |
| `wac_perpetual` | computes delta against live pool avg; posts immediately |
| `wac_periodic` / `wac_retroactive` | **P0006** (depends on period-close machinery, acct-s6n) |
| `fifo` / `lot` | **P0006** (acct-8gg) |

The audit row (`inventory_cost_adjustments`) records `prior_unit_cost`, `target_unit_cost`, `delta_value`, `pool_qty` for self-explanatory reporting. Idempotent on `idempotency_key`.

The `cost_adjustment` transfer_reason is distinct from `cost_restate` (which is reserved for §10 commodity provisional-to-actual settlement).

See consolidated doc §3.12 for the full design notes.

### `close_period` (migration 0026, acct-s6n)

Period close is an orchestrated operation, not a manual `UPDATE periods SET closed_at`. Migration 0025 (`acct-4mt`) lays the schema (`transfers_provisional` + three new variance `account_kind`s); migration 0026 (`acct-v51`) wires `close_period()` and the three close-hook stubs.

Signature:
```
close_period(
  p_period_id         BIGINT,
  p_actor             UUID,
  p_force_provisional BOOLEAN DEFAULT FALSE,
  p_force_recon       BOOLEAN DEFAULT FALSE
) RETURNS JSONB
```

Steps inside `close_period`:

1. `SELECT ... FROM periods WHERE id = $1 FOR UPDATE` — serializes concurrent close calls; raises **P0014** if the period is missing or already closed.
2. Calls `wac_periodic_close_hook`, `wac_retroactive_close_hook`, `cost_adjust_retroactive_hook` in that order. As of `acct-og1` (migration 0032) **all three have real bodies**: wac_periodic re-costs each provisional depletion at the period's final avg `Σ(in-period receipts)/Σ(in-period qty)`; wac_retroactive does chronological replay re-costing each depletion against the running avg it should have had given full-period data; cost_adjust_retroactive walks operator-queued `inventory_cost_adjustments_retroactive` rows and posts variance through `variance_cost_adjust_retro` per non-zero-variance in-period depletion (method-agnostic — see `post_cost_adjustment_retroactive` below). Hook variance transfers post **before** `closed_at` is stamped so they don't trip P0005.
3. **Provisional gate**: counts `transfers_provisional` rows in this period with `finalized_at IS NULL`. Raises **P0015** unless `p_force_provisional = TRUE`.
4. **Reconciliation gate**: calls `run_daily_reconciliation()`. Raises **P0016** unless `p_force_recon = TRUE`. Force still records the alerts.
5. Stamps `closed_at = clock_timestamp()`, `closed_by = p_actor`.
6. Returns a JSONB summary: `{ period_id, period_code, closed_at, closed_by, finalized_count, hook_results, unfinalized_remaining, alerts, forced }` — caller persists or logs as audit.

Force flags are independent — `p_force_provisional` does NOT bypass recon, `p_force_recon` does NOT bypass provisional. Operators can force one gate while keeping the other in effect.

Hook contract: `<name>(p_period_id BIGINT, p_force_provisional BOOLEAN DEFAULT FALSE) RETURNS BIGINT`. The 2-arg signature stabilized after `acct-og1` (migration 0032) when the last stub got its real body — all three hooks now receive `p_force_provisional` so they can skip un-processable rows when forced rather than raising `P0006` / `P0020`.

**Known limitation.** `p_actor` is unvalidated — `close_period` accepts any UUID and stores it as `closed_by`. RBAC is Part VII Q6, still open.

See consolidated doc §6 for the full design notes.

### `post_cost_adjustment_retroactive` (migration 0032, acct-og1)

Operator-queued retroactive cost override that flushes at the target period's close. Distinct from `post_cost_adjustment` (§3.12, migration 0024), which revalues the **live pool** instantaneously on `wac_perpetual` only. This one is **method-agnostic** and processes every credit-side qty-bearing depletion in the period.

Signature:
```
post_cost_adjustment_retroactive(
  p_target_period_id BIGINT,
  p_sku_id           UUID,
  p_location_id      UUID,
  p_currency         TEXT,
  p_inventory_class  TEXT,        -- 'raw' or 'fg' (wip → P0006 ref acct-p7v)
  p_target_avg       BIGINT,      -- the unit cost the operator wants
  p_business_date    DATE,        -- must fall in target period bounds
  p_posted_by        UUID,
  p_idempotency_key  UUID,
  p_notes            TEXT      DEFAULT NULL
) RETURNS UUID                    -- inventory_cost_adjustments_retroactive.id
```

Behavior:
- **Queue-time** (synchronous): replay check via idempotency_key; validate `target_avg >= 0`, target period exists and is open (closed → **P0021** ref acct-7h4), business_date in period bounds (else P0004), SKU exists, pool exists. INSERT queue row. **No transfers posted yet.**
- **Close-time** (`cost_adjust_retroactive_hook` walks the queue): for each un-finalized queue row in the closing period, walk every transfer where `credit_account_id = pool AND business_date IN period AND qty IS NOT NULL AND qty > 0`. For each such depletion: `provisional_unit = amount / qty`, `variance = qty × (target_avg − provisional_unit)`. If non-zero, post a 2-transfer batch routed through `variance_cost_adjust_retro`. UPDATE the queue row's `finalized_at`, `finalized_count`, `total_variance`.

The `qty IS NOT NULL` filter naturally excludes prior-hook variance transfers (`wac_periodic_close_hook` and `wac_retroactive_close_hook` post their variances with `qty=NULL`), so each depletion contributes to this hook's variance exactly once.

Method-agnostic. Works for any `cost_method`. With `wac_periodic` / `wac_retroactive` the corresponding hooks run first and post their own variance; this hook then layers an additional variance on top — "double-correction is acceptable" per documented design (the operator's `target_avg` is computed against the original depletion's `amount/qty`, not the wac-corrected amount).

Variance routing: `cost_restate` reason, `cost_adjust_retroactive_close` document_kind, `variance_cost_adjust_retro` P&L account (one per currency, seeded for USD + EUR in the small fixture). Two transfers per processed depletion (write-up: dr orig_debit / cr variance, dr variance / cr pool; write-down: reverse). Variance accumulator nets to zero per close.

Idempotent at the queue table (`UNIQUE(idempotency_key)`); replay returns the existing row's id without re-inserting.

See consolidated doc §3.14 for the full design notes.

### `post_standard_cost_roll` (migrations 0027 + 0028, acct-hlr)

Establishes or rolls a standard-method SKU's standard cost. INSERTs a new row into `standard_costs`, revalues existing on-hand inventory at the new standard, posts variance to `variance_std_cost_roll`. The standard cost itself lives in `standard_costs` (an append-only stream); `skus.standard_cost` does not exist as a column.

Signature:
```
post_standard_cost_roll(
  p_sku_id            UUID,
  p_new_cost          BIGINT,
  p_effective_at      DATE,
  p_business_date     DATE,
  p_posted_by         UUID,
  p_idempotency_key   UUID,
  p_notes             TEXT   DEFAULT NULL,
  p_expected_old_cost BIGINT DEFAULT NULL    -- optimistic concurrency
) RETURNS UUID                                -- inventory_standard_cost_rolls.id
```

Behavior:
- Replay check via `idempotency_key` returns the existing audit row's id without re-posting.
- Cost-method dispatch:

| `cost_method` | Behavior |
|---|---|
| `standard` | proceed |
| `wac_perpetual` / `wac_periodic` / `wac_retroactive` | **P0011** — use `post_cost_adjustment` (Epic D) for WAC pools |
| `fifo` / `lot` | **P0006** (acct-8gg) |

- **Retroactive guard**: `p_effective_at` must be strictly greater than every existing `standard_costs.effective_at` for the SKU. Otherwise **P0019**. Phase 1 does not support retroactive corrections.
- **Optimistic concurrency**: if `p_expected_old_cost` is non-NULL, must equal `resolve_standard_cost_at(sku, business_date)` (or both NULL for the first roll). Mismatch raises **P0017**.
- **WIP guard**: if any open `inv_value_wip` pool exists for the SKU with non-zero balance, raises **P0006** with reference to `acct-bru` (Epic G — WIP material revaluation companion). Phase 1 blocks; the companion workflow is deferred.
- **First roll** (no prior standard at `business_date`): INSERT only; audit row records `prior_standard_cost = NULL`, `target`, `delta = 0`, `pool_qty = 0`. No transfers posted.
- **Future-dated** (`p_effective_at > p_business_date`): INSERT only, no revaluation. New cost takes effect for transactions whose `business_date >= effective_at` via `resolve_standard_cost_at`.
- **No-op** (`p_new_cost = prior`): audit row recorded with `delta = 0`; no transfers.
- **Revaluation pass**: walks `inv_value_raw` + `inv_value_fg` pools for the SKU (WIP excluded by the guard above), locks each in ascending id order under `FOR UPDATE`, computes `delta = on_hand_qty × (target − prior)` per pool. Builds one variance event per non-zero pool with `reason='standard_cost_roll'`; direction by sign (write-up: dr inventory, cr variance). Calls `post_transfers(batch, FALSE)` — closed-period override not exposed.
- **Audit table** `inventory_standard_cost_rolls` (`prior_standard_cost` is **NULLABLE**, see first-roll case): records `prior`, `target`, `total_delta_value`, `pool_qty`, `effective_at`, `business_date`.

**Known limitation.** `p_posted_by` is unvalidated — same convention as the other document-layer functions. RBAC is Part VII Q6, still open.

See consolidated doc §3.13 for the full design notes.

### `post_po_receipt` (migration 0035, acct-7mg)

Slice A inflow workflow. Receives goods against an open PO; emits the qty + value (+ optional PPV) event batch documented in consolidated doc §3.1.

Signature:
```
post_po_receipt(
  p_po_id           UUID,
  p_lines           JSONB,   -- [{po_line_id: UUID, qty_received: BIGINT}, ...]
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT     DEFAULT NULL
) RETURNS UUID                -- po_receipts.id
```

Per line, looks up `purchase_order_lines` for SKU/location/unit_cost/currency, validates the PO/line ownership and over-receipt gate (P0023 if cumulative `qty_received` would exceed `qty_ordered`), resolves accounts (qty: stock_available + vendor_pool; value: inv_value_raw + ap_unsettled), and emits 2 events (qty leg + value leg). For standard SKUs with `po_unit_cost ≠ resolve_standard_cost_at(sku, business_date)`, emits a third PPV event routing the variance through `variance_ppv` — `inv_value_raw` lands at standard cost regardless. WAC SKUs (perpetual / periodic / retroactive) post `inv_value_raw` at `po_unit_cost`; the pool re-averages organically.

**GRNI semantics (D1, 2026-05-01).** Credits `ap_unsettled` (goods received not invoiced), not `ap`. Clearance happens when `post_ap_bill` is called against the PO line (see below). Earlier draft of the design (§3.1 pre-Slice-A) credited `ap` directly at receipt — revised when Slice A landed; see consolidated doc §3.1 for rationale.

**Errors.** P0022 on PO/line/empty/qty validation; P0023 on over-receipt; P0006 for fifo/lot SKUs; P0010 if any required account isn't open; standard P0001-P0005 inherit from `post_transfers`.

**Idempotency.** Replay on `p_idempotency_key` returns the existing `po_receipts.id` without re-posting.

### `post_ap_bill` (migration 0035, acct-7mg)

Slice A inflow workflow companion. Vendor bill — clears GRNI accruals from PO receipts and/or posts standalone service expenses.

Signature:
```
post_ap_bill(
  p_vendor_id     UUID,
  p_currency        CHAR(3),
  p_lines           JSONB,
    -- [{kind:'po_match', po_line_id, qty, unit_cost, amount}]
    -- [{kind:'service',  expense_account_id, amount}]
  p_business_date   DATE,
  p_posted_by       UUID,
  p_idempotency_key UUID,
  p_notes           TEXT     DEFAULT NULL
) RETURNS UUID                -- vendor_bills.id
```

Two line modes co-existable in one bill:

- **`po_match`** — strict three-way match (D3, 2026-05-01) against the referenced `po_line`. Validates `qty ≤ received_not_billed_remainder` (cumulative across receipts and prior bills, where `remainder = SUM(po_receipt_lines.qty_received) − SUM(prior vendor_bill_lines.qty WHERE kind='po_match')`), `unit_cost = po_line.unit_cost`, `amount = qty × unit_cost`. Posts `ap_unsettled DR / ap CR`. Cumulative billed is recomputed per line within the same batch (later lines see earlier inserts via `READ COMMITTED`).
- **`service`** — caller supplies an arbitrary value-side `expense_account_id`. Function validates the account is open, `ledger_kind='value'`, currency = bill currency. Posts `expense_account DR / ap CR`. No PO reference; covers utilities, professional services, etc. Stable expense taxonomy (operating_expense kinds) is filed as `acct-063` follow-up.

**Errors.** P0024 for any three-way mismatch; P0025 for invalid line kind, vendor mismatch, currency mismatch on po_line, missing/closed expense account; P0010 if no open `ap` for `(vendor, currency)`; P0001-P0005 inherit from `post_transfers`.

**Idempotency.** Replay on `p_idempotency_key` returns the existing `vendor_bills.id` without re-posting.

### Three-way match and GRNI flow

Inflow cycle pieces fit together as: `post_po_receipt` accrues to `ap_unsettled`, `post_ap_bill` (po_match) clears `ap_unsettled → ap`, `post_transfers(reason='ap_payment')` (per consolidated doc §3.7) settles `ap → cash`. Each step is its own ledger event with its own document trail — no implicit linkage beyond the `po_line_id` and `vendor_bill_lines.po_line_id` columns used by the three-way match query.

Standard ERP convention; Slice A's choice (D1) over the simpler "post directly to ap at receipt" pattern. The three-way match is **strict** for Phase 1: tolerance windows, over-receipt, and partial-line matching with weighted-cost averaging are all Phase 2 concerns.

### `post_wo_start` / `post_op_move` / `post_wo_complete` / `post_scrap` (migrations 0037 + 0038 + 0039, acct-b82)

Slice B conversion cycle. Document-layer wrappers for the work-order lifecycle.

**Schema (migration 0037)**:
- `work_orders(id, wo_no, parent_sku_id, fg_location_id, qty_target, qty_completed, qty_scrapped, status, currency, posted_by, posted_at)` — header. status ∈ {`draft`, `released`, `closed`}.
- `wo_routings(wo_id, routing_op, op_name)` — per-WO operation list (no shared template table at MVP). PK `(wo_id, routing_op)`.
- `wo_routing_burdens(wo_id, routing_op, applied_account_kind, std_amount)` — per-op standard absorption rates. PK `(wo_id, routing_op, applied_account_kind)`. FK to `wo_routings`. The open extension point: adding a new burden type (`outside_processing_applied`, `setup_applied`, `tooling_applied`, energy, …) is `ALTER TYPE account_kind ADD VALUE` + `ALTER TYPE transfer_reason ADD VALUE` + extending `_wo_apply_reason_for(account_kind)` + scaffolding the per-currency account. Schema is unchanged.
- `boms(parent_sku_id, component_sku_id, component_loc_id, qty_per_parent)` — formal BOM reference data. Single-level; sub-assembly composition is a separate WO chained by parent consumption.
- `wo_events(id, wo_id, event_kind, routing_op_from, routing_op_to, qty, business_date, posted_by, idempotency_key, notes)` — lifecycle audit log. event_kind ∈ {`start`, `op_move`, `wo_complete`, `scrap`}. Composite CHECK ensures the (event_kind, routing_op_*, qty) combination is internally consistent.

**Cost rollup model.** `parent_std_cost = Σ (bom.qty_per_parent × component_std_cost) + Σ_op Σ_kind wo_routing_burdens.std_amount`. BOM components and per-op burdens are the same idea — per-unit costs that apply as a unit moves through the routing — differing only in *what* (RM substance vs absorption: labor / OH / outside-proc / …) and *when* (RM at WO start; burdens at the op they're declared on).

**Functions (migration 0038, idempotency-fix 0039)**:

- `post_wo_start(p_wo_id, p_business_date, p_posted_by, p_idempotency_key, p_notes)` — releases a draft WO. Charges WIP@first_op with RM (per BOM component, valued at component standard cost via `resolve_standard_cost_at`) plus first-op burdens. Emits `wo_start` qty leg + N × `rm_issue_to_wo` (qty + value) + M × burden-apply (e.g. `labor_apply`, `oh_apply`). Flips status `draft → released`.
- `post_op_move(p_wo_id, p_from_op, p_to_op, p_qty, …)` — moves qty units between ops, then applies destination-op burdens. Value-leg amount = `qty × std_cum_at_from_op` (RM + burdens for ops ≤ from_op), passed through reason `op_move_v` (NOT in the dispatcher's cost-event list, so caller-supplied amount stands — the dispatcher's standard branch returns parent's full std cost which would be wrong at intermediate ops). Rework moves (to_op < from_op) re-apply destination burdens — realistic ERP semantics for rework labor.
- `post_wo_complete(p_wo_id, p_qty, …)` — completes qty units from the highest routing_op into FG. Value-leg uses reason `wo_complete` and rides the dispatcher's standard branch (correct at last op because `std_cum_at_last_op = parent_std_cost`). On final completion (qty_completed + qty_scrapped reaches qty_target), reads `inv_value_wip@last_op` residual under `FOR UPDATE` and emits a `wo_close_v` leg for any nonzero balance, then sets status='closed'.
- `post_scrap(p_wo_id, p_routing_op, p_qty, …)` — reads `inv_value_wip` + `stock_wip@op` pools `FOR UPDATE` to compute accumulated unit cost. Emits `scrap` qty leg (`stock_scrap DR / stock_wip CR`) + `scrap_v` value leg (`variance_scrap DR / inv_value_wip CR`).

**MVP gate.** WO `parent_sku.cost_method = 'standard'` (P0006 otherwise; lifted under acct-p7v which adds wac to WIP). Components may use any cost method but **must** have a `standard_costs` row in effect at `business_date` (resolve_standard_cost_at raises P0018 otherwise) — the MVP values RM at standard regardless of comp.cost_method to keep BOM expansion deterministic.

**Idempotency.** Each function checks `wo_events.idempotency_key` on entry (fast path) and again after the `FOR UPDATE` on `work_orders` (race-safe — fixes the wo_start / wo_complete status-transition window described in the migration 0039 header). Replays return `p_wo_id` without re-emitting events.

**WIP locking + read-then-write.** `post_op_move` does not lock — it reads BOM and `wo_routing_burdens` snapshots and trusts that those are stable for the WO's lifetime. `post_scrap` and the wo_complete residual path acquire `FOR UPDATE` on `inv_value_wip` (and the matching `stock_wip` for scrap) before reading `(debits_total - credits_total)` — the read-then-write under lock pattern, allowed in v0.2 (CLAUDE.md "Load-bearing design decisions"). Since these locks are acquired in the same transaction as the subsequent `post_transfers` call (which itself acquires `FOR UPDATE` on the same accounts in id order), there is no cross-transaction deadlock window — same-tx FOR UPDATE re-acquisition is a no-op upgrade.

**Errors.** P0026 (wo_invalid: not found, wrong status, parent ≠ standard, no routing, scrap > pool); P0027 (qty_overflow on wo_complete / scrap); P0028 (routing_op_invalid: from=to, op not in routing); P0029 (bom_missing); P0010 if any required account isn't open; P0018 if a component lacks a standard cost row; P0001–P0005 inherit from `post_transfers`.

## Schema integrity check (`scripts/ci-check.sh`)

Pre-push guard for schema changes. There is no remote CI; this script is the check. Runs entirely against an ephemeral `acct_ci` DB inside the existing dev Postgres instance — the dev `acct` DB is untouched.

```bash
./scripts/ci-check.sh
```

Five steps:

1. `sqlx migrate run` on a freshly-created `acct_ci` — every migration applies cleanly.
2. `sqlx migrate info` — assert every migration shows installed (no `pending` lines).
3. `sqlx migrate revert` in a loop until empty — every migration reverts cleanly.
4. `sqlx migrate run` again — redeploys cleanly on the empty DB.
5. `pg_dump -s | sha256sum` — print the schema digest. Useful for reviewing what changed across branches; identical schemas produce identical digests.

The throwaway DB is dropped on exit via `trap`, even on failure.

## Tests

Cargo integration tests run against an **ephemeral `acct_test` database** inside the existing dev Postgres instance. The dev `acct` DB is untouched.

```bash
./scripts/run-tests.sh                    # full run
./scripts/run-tests.sh smoke              # filter to a single test binary
./scripts/run-tests.sh -- --test-threads=1  # serialize
```

The script `DROP`s `acct_test` if present, `CREATE`s it fresh, runs all migrations against it, seeds `db/fixtures/small/seed.sql`, runs `cargo test`, and drops `acct_test` again on exit (success or failure).

`pg_cron` does not get installed in `acct_test` — its install script enforces a hard `current_database() = cron.database_name` check that `cron.use_background_workers` does not relax. Migration 0001 tolerates this with a `DO/EXCEPTION` block; the test DB simply doesn't get pg_cron. O1 / O2 (which schedule cron jobs) run only against `acct`.

Test helpers live in `tests/common/mod.rs` (loaded via `mod common;` in each integration test file): `connect_test_db`, `reset_to_fixture`, `expect_sqlstate`, `call_post_transfers`. Set `TEST_DATABASE_URL` if you want to run `cargo test` outside of `scripts/run-tests.sh`.

### Load tests (opt-in)

Long-running load tests are gated behind `#[ignore]` so they don't run with the default `cargo test`. Triggered explicitly:

```bash
./scripts/run-tests.sh --test load_deadlock_freedom -- --ignored --nocapture
```

Available env knobs (defaults shown):

- `T4_DURATION_SECS=30` — wall-clock duration of the load phase.
- `T4_WRITERS=32` — number of concurrent tokio writers.

The current single test, `deadlock_freedom_under_concurrent_post_transfers`, posts random valid batches against `post_transfers` and asserts (a) `pg_stat_database.deadlocks` delta is 0, (b) every batch succeeded (no non-deadlock errors), (c) per-ledger double-entry holds at the end. The original Part IV §13 target is 100 writers × 30 minutes; defaults are smaller for tractable sanity runs. Spec-target run:

```bash
T4_DURATION_SECS=1800 T4_WRITERS=100 ./scripts/run-tests.sh --test load_deadlock_freedom -- --ignored --nocapture
```

(Bumping `T4_WRITERS` above the dev container's `max_connections` will fail; scaling work is tracked separately.)

## Scheduled jobs (pg_cron)

`reservation_expiry` runs every 30 seconds in the `acct` database (set via `cron.database_name` GUC) and flips `inventory_reservations` rows from `'active'` to `'expired'` when `expires_at` has passed. Migration `0015_cron_reservation_expiry` registers the job; the migration tolerates non-`acct` databases (acct_test, acct_ci) where pg_cron isn't installed.

Inspect the job:

```sql
SELECT jobid, schedule, command FROM cron.job WHERE jobname = 'reservation_expiry';
SELECT runid, status, return_message, start_time, end_time
  FROM cron.job_run_details
 WHERE jobid = (SELECT jobid FROM cron.job WHERE jobname = 'reservation_expiry')
 ORDER BY runid DESC LIMIT 5;
```

Smoke-check the end-to-end flow (inserts a test row, waits 35s, asserts expiry, cleans up):

```bash
./scripts/verify-o1-expiry.sh
```

`daily_reconciliation` runs every day at 00:00 UTC in `acct` and calls the PL/pgSQL function `run_daily_reconciliation()`. Two checks today (Part IV §7):

- **Per-ledger double-entry**: `SUM(debits_total) ≠ SUM(credits_total)` per `(ledger_kind, currency)` writes a `'double_entry_imbalance'` row to `reconciliation_alerts` with `payload = {ledger_kind, currency, debits, credits, imbalance}`.
- **Reservation over-promise**: any `(sku, location)` where on-hand `<` SUM(active reservations.qty) writes a `'reservation_over_promise'` alert with `payload = {sku_id, location_id, on_hand, reserved, deficit}`.

Phase 0 is log-only; Phase 1 will tee `reconciliation_alerts` into PagerDuty / Slack / email. The function is correctness-tested in `tests/reconciliation.rs`; against the live `acct` DB you can smoke-check via:

```bash
./scripts/verify-o2-recon.sh
```

Inspect alerts directly:

```sql
SELECT alert_type, payload, created_at
  FROM reconciliation_alerts
 ORDER BY id DESC LIMIT 20;
```

## Reference data — Phase 0 scope

Phase 0 ships **stub** reference tables only (`skus`, `locations`, `sales_orders`, `purchase_orders`) — just enough columns to serve as FK targets for the ledger schema and to drive cost dispatch.

Internal document IDs use **`uuidv7()`** (PG 18 builtin) so PKs are time-ordered for B-tree locality. The `idempotency_key` on `transfers` stays random (`gen_random_uuid()` / UUIDv4) because it comes from clients.

**Out of scope until Phase 1:** `customers`, `users`, `routings`, `boms`, `work_orders`, `facilities`. `vendors` shipped in Slice A (acct-7mg, migration 0034) — `purchase_orders.vendor_id` and `vendor_bills.vendor_id` now have real FKs. `sales_orders.customer_id` is still a nullable UUID with no FK — the `customers` table doesn't exist yet.
