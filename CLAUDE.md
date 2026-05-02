# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository status

**Phase 0 + Phase 1 cost-method matrix + Slice A inflow are functionally complete.** Phase 0 (consolidated doc Part IV §13) shipped the `post_transfers` foundation, reservations, period orchestration, daily reconciliation, and the standard-cost dispatcher. Phase 1 layered on document-level workflows (inventory_adjustment, cost_adjustment, cost_adjustment_retroactive), period-close orchestration with three real close hooks, the standard-cost-as-separate-entity refactor, and the wac_perpetual / wac_periodic / wac_retroactive cost-method trio. Slice A (acct-7mg, 2026-05-01/02) shipped the inflow cycle: `vendors` table, `purchase_order_lines`, `po_receipts`/`po_receipt_lines`, `vendor_bills`/`vendor_bill_lines`, `post_po_receipt`, `post_ap_bill`, plus a `post_transfers` consolidation pass that extracted `_post_transfers_lock_pre_scan` and `_post_transfers_apply_event` helpers before adding the new dispatcher branches. Original Slice A used `suppliers` terminology; renamed to `vendors` end-to-end (acct-397, migration 0036) before more code accreted on the older naming. The remaining open bd issues are all P3 — Phase 2 epics and infrastructure deferrals.

What exists now:
- Postgres 18 dev environment in Docker (`docker-compose.yml`, `db/Dockerfile`, `db/init/`).
- Helper scripts (`scripts/dev-up.sh`, `scripts/dev-down.sh`, `scripts/run-migrations.sh`, `scripts/run-tests.sh`, `scripts/ci-check.sh`, `scripts/run-perf-baseline.sh`).
- Rust crate root (`Cargo.toml`, `src/lib.rs`) — library only, sqlx + tokio.
- 36 sequential reversible migrations under `db/migrations/` (`0001_enable_extensions` through `0036_rename_supplier_to_vendor`). Schema, `post_transfers` (extracted helpers), `reserve_inventory`, `run_daily_reconciliation`, the `_post_transfers_compute_amount` cost dispatcher with real `'standard'` / `'wac_perpetual'` / `'wac_periodic'` / `'wac_retroactive'` branches (`'fifo'`/`'lot'` raise `P0006`); document-layer wrappers (`post_inventory_adjustment`, `post_cost_adjustment`, `post_standard_cost_roll`, `post_cost_adjustment_retroactive`, `post_po_receipt`, `post_ap_bill`); period-close orchestration (`close_period` + three real hook bodies); `pg_cron` reservation expiry; the optional `ledger_outbox` table.
- 44 integration test binaries under `tests/`: per-invariant probes (T1 — including 4 new files for Phase 1 tables), workflow matrices (T2 — wac_periodic, wac_retroactive, cost_adjust, cost_adjust_retroactive, standard_cost, period_close, po_receipt, ap_bill), the conformance harness (T5, 121 cases), the WAC integration suite, and the perf load matrix (shapes A-M, 8 `#[ignore]`'d).
- `db/fixtures/small/seed.sql` — minimal-but-realistic seed for `cargo test`. Includes un-partitioned `ap_unsettled` / `vendor_pool` / `variance_ppv` for conformance harness; per-vendor scaffold (specific `vendors` rows, partitioned ap accounts) is created inline by Slice A matrix tests.
- `perf_baseline_v0.md` — 13-shape baseline matrix (acct-1ia/yjn/ezm follow-ups, set on the 21-migration schema).
- The design spec (`ledger_design_consolidated_v0.md`) and `ARCHIVE/` of predecessor docs.

When adding new categories of code, update this file.

## Implementation stack

- **Language:** Rust.
- **Postgres driver:** `sqlx` (raw SQL, compile-time query checking against the live schema).
- **Migrations:** `sqlx-cli` — plain `.sql` files under `db/migrations/`, run via `sqlx migrate run`.
- **Tests:** `cargo test` — Rust integration tests using `tokio` + `sqlx`. **No pgTAP, no Python harnesses.** A small `tests/common/` module will provide helpers (reset to fixture, expect SQLSTATE).
- **Database:** Postgres 18 with `io_method=io_uring`, `pg_stat_statements`, `pg_cron`. Runs in Docker (host port `5111`, container port `5432`). Volume mounted at `/var/lib/postgresql` (PG 18+ convention). `seccomp:unconfined` on the dev container to allow io_uring syscalls; production hardening tracked as `acct-hbp`.

## Commands

```bash
./scripts/dev-up.sh        # build + start postgres, verify io_method and extensions
./scripts/dev-down.sh      # stop (data preserved)
./scripts/dev-down.sh --wipe   # stop and remove data volume

psql 'postgres://acct:acct_dev@localhost:5111/acct'
```

See `db/README.md` for full dev DB details.

## Issue tracking

This repo uses **`bd` (beads)** for issue tracking. See `AGENTS.md` for the full integration block; key points:

- Use `bd` for all task tracking. Do **not** use TodoWrite, markdown TODO lists, or memory files for work items.
- `bd ready` shows unblocked issues; `bd show <id>` for details; `bd update <id> --claim` to take one; `bd close <id>` when done.
- Issue prefix in this repo: `acct-` (e.g., `acct-a3f2dd`).
- Storage is embedded Dolt under `.beads/` (committed to git). Sync via `bd dolt push` / `bd dolt pull` when a remote is configured.
- Run `bd prime` for full command reference and the session-close protocol.

## Source of truth

`ledger_design_consolidated_v0.md` is the single working reference. It is the document to read first and to update when design decisions change. Everything else is either superseded or historical.

`ARCHIVE/` holds four predecessor documents that were folded into the consolidated doc:
- `ledger_inventory_design_spec_v0.md` — the original v0.1 design (TigerBeetle-parity Postgres ledger).
- `phased_migration_spec_v0.md` — the original v0.1 Postgres → TigerBeetle migration roadmap (Phase 0–5).
- `spec_review_v0.md` — critical review of v0.1.
- `postgres_native_design_v0.md` — the redesign argument.

Treat `ARCHIVE/` as historical record. Do not propose changes there. If a question can be answered from the consolidated doc, do not reach into the archive.

## What the project is

An ERP ledger and inventory system: SKU × location quantity tracking, per-routing-step WIP, document lifecycle (WO/SO/TO/PO), double-entry GL, multi-currency, reservations, commodity provisional pricing, period close. The design has gone through one full review cycle and has converged on a **Postgres-native v0.2** target (consolidated doc Part IV) — explicitly not a TigerBeetle drop-in or hybrid system.

## Load-bearing design decisions (do not re-litigate without cause)

These are decisions the consolidated doc commits to. If a task touches one of them, treat it as a design change requiring deliberate justification, not an incidental edit.

- **Postgres-native, no TigerBeetle parity.** The TB-parity tax (Part III) is the reason v0.2 exists. Reintroducing `NUMERIC(39)` IDs, `user_data_*` polymorphism, bit-field `flags`, the pending/post/void primitive, lazy account materialization, or AMQP CDC is a regression.
- **Reservations are first-class rows, not pending transfers.** `inventory_reservations` table with a status enum (Part IV §1.3, §3.3). The self-pending pattern from v0.1 is a documented blocker (B1).
- **Transfers are append-only.** Trigger-blocked UPDATE/DELETE on `transfers` (Part IV §1.9). Reversals are new transfers, not edits.
- **Period lock is a schema invariant, not API discipline.** `post_transfers` consults `periods.closed_at` (Part IV §2.1, §6).
- **Period close is orchestrated, not a manual `UPDATE`.** `close_period(p_period_id, p_actor, p_force_provisional, p_force_recon)` (migration 0026, `acct-s6n`) runs hooks, gates, then stamps `closed_at`. Two gates: P0015 (un-finalized `transfers_provisional` rows) and P0016 (recon alerts), each force-flag-bypassable independently. P0014 covers missing/already-closed (and concurrent-caller serialization via `FOR UPDATE` on the period row). As of `acct-og1` (migration 0032) **all three close hooks have real bodies**: `wac_periodic_close_hook` (from `acct-qfj`, migration 0029), `wac_retroactive_close_hook` (from `acct-9tw`, migration 0031), `cost_adjust_retroactive_hook` (from `acct-og1`, migration 0032). All three share the signature `(p_period_id BIGINT, p_force_provisional BOOLEAN DEFAULT FALSE) RETURNS BIGINT`. wac_retroactive uses chronological replay ordered by `(business_date, posted_at, id)` per pool; WIP class deferred under `acct-p7v` (Phase 2 Epic J). `p_actor` is unvalidated — RBAC = Part VII Q6, still open.
- **`wac_periodic` depletions post at the running pool average and are flagged in `transfers_provisional`** (acct-qfj, migration 0029). Mid-period dispatcher math is identical to wac_perpetual (`amount = qty × pool_value/per_class_qty`); the difference is the value-leg transfer is also INSERTed into `transfers_provisional` with `cost_method='wac_periodic'`. At period close, `wac_periodic_close_hook` recomputes `final_avg = Σ(in-period receipts value) / Σ(in-period receipts qty)` per `(sku, location, currency)` pool (Oracle PAC convention) and posts variance routed through `variance_wac_period`. Receipts on wac_periodic SKUs do **not** flag — they post at their actual asserted cost (po_unit_price, etc.). Empty-pool-on-depletion = P0006 (no negative inventory; tracked as Epic H `acct-9ij`). Zero-receipts-in-period at close = **P0020** (`wac_periodic_close_no_receipts`); bypassable with `p_force_provisional=TRUE`. Alternate provisional cost sources (last period close avg, last purchase price, configured value, zero, standard) deferred to Phase 2 Epic I (`acct-cms`).
- **Retroactive cost adjustment is queue-then-flush-at-close, method-agnostic** (acct-og1, migration 0032). `post_cost_adjustment_retroactive(p_target_period_id, ...)` queues a row in `inventory_cost_adjustments_retroactive`; `cost_adjust_retroactive_hook` flushes at close, posting variance through `variance_cost_adjust_retro` per non-zero-variance in-period depletion. Distinct from §3.12 `post_cost_adjustment` which is live-pool, wac_perpetual-only. Restricted to currently-open target periods (closed → **P0021** referencing `acct-7h4` Phase 2 Epic K period reopen workflow). With `wac_periodic`/`wac_retroactive` the documented behavior is double-correction (their hooks run first; cost_adjust_retroactive layers on top against the original depletion's `amount/qty`). WIP class deferred (P0006 ref `acct-p7v`).
- **Read-then-write under `FOR UPDATE` is allowed and used for cost computation.** WIP unit cost, WAC. (Part IV §3.4, §4.2.) The TB-era prohibition is dropped deliberately.
- **Per-ledger double-entry invariant.** `GROUP BY ledger_kind, currency` — never a single global sum (B3 fix; Part IV §7).
- **`BIGINT` for amounts and balances; `BIGSERIAL` for account/transfer IDs; `UUID` only for document IDs and `idempotency_key`.** Type choices are deliberate; Part IV §1.
- **Tiered read model.** Tier 1 (base tables) → Tier 2 (trigger-maintained mat views, only when measured) → Tier 3 (logical replication for OLAP/search, only when justified). Do not propose an async projector service for MVP.
- **Sync `post_transfers` for now; pivot to pseudo-sync (shape L) deferred to Phase 1+** (Part VII Q3 originally resolved 2026-04-27 as `acct-93b.3`; re-examined and re-resolved 2026-04-30 as `acct-0oy`). `post_transfers` is called synchronously inside the same Postgres transaction as document writes. The five outbox variants (G/J/K/L/M, perf_baseline_v0.md) characterize the alternatives; shape **L** (pseudo-sync via LISTEN/NOTIFY, `acct-yjn`) is the documented escape hatch. **L's latency-and-throughput-under-contention advantages are real and wanted** — caller p99 547 ms vs F's 8.25 s (15× better) is robust under noise; throughput median is ~1.8× F (the original "16 % above shape B" claim was at the high end of L's noise distribution and didn't survive `acct-ezm`'s short-run re-measurement). The deferral is **operational, not architectural**: every Phase 0 test and every Phase 1 fixture would need rewriting around an async-listener-rendezvous call site, and realistic Phase 1 workflows (per-document transfers, naturally spread across SKUs/locations) don't hit the high-contention regime where L's advantages are load-bearing. We commit to revisiting once Phase 1 produces measured contention — tracked as `acct-c4p`. The infrastructure for L (DrainConfig.notify_channel, single-listener dispatcher, drain-tx pg_notify with SQLSTATE payload) is already built and benched; pivoting later is additive, not a foundation rewrite.
- **`standard` cost only in Phase 0** (Part VII Q4 resolved, bd `acct-93b.4`). Schema includes a `cost_method` enum and `skus.cost_method` column (default `'standard'`) so `post_transfers` dispatches on it; non-`standard` branches raise `P0006`. WAC / FIFO / lot tracked as `acct-8gg`.
- **Per-event qty is persisted on `transfers.qty`** (acct-75z, migration 0030; forward-only). Populated at INSERT time for inventory-touching events; NULL for cash/AR/AP/FX. WAC math (`wac_perpetual` and `wac_periodic`) reads its qty divisor as `SUM(transfers.qty signed by debit/credit on the value pool)` — class-isolated. Pre-0030 used `stock_available.balance` which pooled raw and fg qty for the same `(sku, location)`, breaking per-class avgs when a SKU had both pools active. `_post_transfers_lookup_qty_account` is retained for `post_transfers`'s lock pre-scan only. The "single inventory class per SKU" assumption is retired.
- **Standard cost is a separate transactional entity, not a column on `skus`** (acct-hlr, migrations 0027-0028). `skus.standard_cost` does not exist. Standard cost lives in the append-only `standard_costs` table (`sku_id`, `cost`, `effective_at`, `posted_by`, ...). The single canonical lookup is `resolve_standard_cost_at(p_sku_id, p_business_date) RETURNS BIGINT`, which raises **P0018** if no standard is in effect at `business_date`. Cost-relevant operations on standard SKUs go through this helper — the gate composes (currently catches `_post_transfers_compute_amount` standard branch + `post_inventory_adjustment` standard branch; future PO receipt / SO ship / op_move / scrap / wo_complete inherit it). Establishing or rolling the cost goes through `post_standard_cost_roll()` (INSERT into `standard_costs` + revalue raw + fg pools + audit row). WIP pools are excluded from revaluation by design — companion workflow tracked as `acct-bru` (Phase 2 Epic G). Retroactive rolls blocked (P0019); optimistic concurrency via P0017.
- **TigerBeetle is a reference model, not a parity target** (Part VII Q1 resolved, 2026-04-29). TB informs behavioral correctness (atomicity, lock semantics, idempotency, append-only) but the implementation does not have to shape itself to TB's primitives. Postgres-native ergonomics win where they conflict.
- **No fixed TPS target; correctness > performance; baseline before complexity** (Part VII Q2 resolved, 2026-04-29). The project is an exploration of what's possible Postgres-native vs the v0.1 hybrid Postgres/TB design. The §14.1 perf baseline is established on the *simplest* schema first (`acct-1ia` produces `perf_baseline_v0.md`), then every Phase 1 complexity addition is diff'd against it. Phase 1 schema expansion (customers, work_orders, routings, BOMs, alternate cost methods) is gated behind the baseline so regressions are detectable.
- **PO receipt accrues to `ap_unsettled`, not `ap`** — GRNI semantics (Slice A D1, 2026-05-01, acct-7mg, migrations 0034/0035). `post_po_receipt` credits `ap_unsettled` (goods received not invoiced) at receipt; `post_ap_bill` clears `ap_unsettled → ap` at bill posting. Mainstream-ERP convention; revises the design doc's original §3.1 (which was written speculatively to credit `ap` directly). Three close-out reasons: (a) compliance — AP shouldn't be credited before invoice approval, (b) makes the receipt-to-bill match auditable as a discrete transition, (c) `ap_unsettled` already existed for the §3.13 commodity provisional case so this is a natural reuse.
- **Three-way match is strict on (sku, location, qty, unit_cost)** — Slice A D3, 2026-05-01, `post_ap_bill` (migration 0035). `qty` ≤ received-not-yet-billed remainder per `po_line` (cumulative across receipts and prior bills); `unit_cost = po_line.unit_cost`; `amount = qty × unit_cost`. Mismatches raise **P0024**. Tolerance windows, over/under-receipt, and weighted-cost averaging are deferred to Phase 2 — caller-side workflows (`post_cost_adjustment`, reversal+rebook) handle discrepancies for now.
- **`post_transfers` apply step is centralized in `_post_transfers_apply_event`** — Slice A.1 Part A consolidation pass (acct-7mg, migration 0033, fold-in of acct-q43). Validation (P0001/P0002/P0003), period resolution + gate (P0004/P0005), qty resolution, balance update, transfer insert, and `transfers_provisional` flag for wac_periodic/wac_retroactive depletions all live in one helper. `post_transfers` itself is now an orchestrator: pre-scan (cost-event detection, lock pre-scan via `_post_transfers_lock_pre_scan`), single-pass for non-WAC batches, two-pass for WAC. Future dispatcher branches (`post_so_ship`, etc.) call into the same primitives.

## Open questions that gate work

Part VII of the consolidated doc lists 10 gating questions. **Q1 (TB optionality), Q2 (TPS projection), Q3 (outbox), Q4 (cost method) are resolved** (see "Load-bearing design decisions" above). The remaining open ones are scope-shaping rather than framing:

5. Reservation lifetime — sub-second timeouts ever needed? (Drives `pg_cron` vs `LISTEN/NOTIFY`.)
6. Append-only enforcement model — trigger + RBAC + both?
7. CDC sinks at MVP — none / search / OLAP?
8. Commodity materiality threshold — 5% is a placeholder.
9. Tier-2 mat view scope at MVP — default none.
10. Per-WO per-op account opt-in — default none.

These don't gate further engineering work the way Q1/Q2 did. Surface them when a task forces a choice; default behaviors above otherwise.

## Testing methodology

Part IV §14 specifies a three-layer progression. Status as of Phase 1 cost-method completion:

1. **Exploratory** — DONE for the 21-migration schema (`perf_baseline_v0.md`, 13 shapes A-M, acct-1ia/yjn/ezm). Re-measurement against the current 32-migration schema is tracked separately and produces `perf_baseline_v1.md` when run.
2. **Structured** — NOT YET. Five reference workloads (ecommerce / manufacturing / distribution / month-end close / backfill); SLOs filled in from exploratory output; perf-regression CI gates. Pre-Slice-A baseline regression is the trigger to formalize this.
3. **Integrated** — NOT YET. Full path through API tier, optional outbox, pgBouncer, DB; OpenTelemetry traces per layer. No API tier currently in scope; this stays deferred until one exists.

SLO numbers in §14.2 are deliberately TBD — they get filled in after Structured lands, not before.

## Working with the consolidated doc

- It is large (~85 KB / ~1,750 lines). When the user references "the spec" or "the design," they mean this file unless they explicitly say otherwise.
- The structure is fixed: Part 0 (exec summary), I (v0.1 baseline), II (review), III (parity-tax root cause), IV (v0.2 design — the implementable spec), V (§△ scoreboard), VI (tradeoffs), VII (open questions), Appendices.
- Edits should preserve cross-references. The doc has internal pointers like "Part IV §3.4" and "B2" — keep them consistent.
- The "Open questions" list is a question list, not a decision log. Do not convert it to an owners/dates table without being asked.

## Style conventions in the doc

- Severity labels in Part II: **Blocker / Major / Minor / Note** with IDs (B1–B3, M1–M7, m1–m8, N1–N5).
- §△ markers carry forward from v0.1 for items that remain genuine open questions or future-revisit items. The Part V scoreboard tracks each by ID.
- SQL examples use lowercase `snake_case` consistently. Account kinds are PG enums; transfer reasons are PG enums.
- "v0.1" refers to the archived TB-parity design; "v0.2" refers to the Postgres-native design in Part IV.
