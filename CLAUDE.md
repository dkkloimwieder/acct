# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository status

**Phase 0 is in progress** (consolidated doc Part IV §13). The design phase is complete and committed to the consolidated doc; foundational schema/tooling work is being broken out into beads issues under epic `acct-93b`.

What exists now:
- Postgres 18 dev environment in Docker (`docker-compose.yml`, `db/Dockerfile`, `db/init/`).
- Helper scripts (`scripts/dev-up.sh`, `scripts/dev-down.sh`, `scripts/run-migrations.sh`).
- Rust crate root (`Cargo.toml`, `src/lib.rs`) — minimal; sqlx + tokio deps only.
- Migration scaffold via `sqlx-cli` under `db/migrations/`. First migration: `0001_enable_extensions` (reversible, sequential).
- The design spec (`ledger_design_consolidated_v0.md`) and `ARCHIVE/` of predecessor docs.

What does NOT yet exist:
- Schema tables (enums, periods, fx_rates, accounts, transfers, …) — see `acct-93b.9` onward.
- `post_transfers` function — see `acct-93b.14`/`.15`.
- Tests, fixtures, CI.

When adding new categories of code, update this file with real commands. Until each category exists, do not invent commands for it.

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
- **Read-then-write under `FOR UPDATE` is allowed and used for cost computation.** WIP unit cost, WAC. (Part IV §3.4, §4.2.) The TB-era prohibition is dropped deliberately.
- **Per-ledger double-entry invariant.** `GROUP BY ledger_kind, currency` — never a single global sum (B3 fix; Part IV §7).
- **`BIGINT` for amounts and balances; `BIGSERIAL` for account/transfer IDs; `UUID` only for document IDs and `idempotency_key`.** Type choices are deliberate; Part IV §1.
- **Tiered read model.** Tier 1 (base tables) → Tier 2 (trigger-maintained mat views, only when measured) → Tier 3 (logical replication for OLAP/search, only when justified). Do not propose an async projector service for MVP.
- **No outbox in Phase 0** (Part VII Q3 resolved, bd `acct-93b.3`). `post_transfers` is called synchronously inside the same Postgres transaction as document writes. Outbox-vs-sync (`acct-tyq`) was characterized as shape G in `perf_baseline_v0.md` on 2026-04-29: naive single-drainer outbox is **9× slower** than direct sync (140 evps committed vs F's 1 274) because per-call `post_transfers` overhead doesn't amortize the way packed-events-in-one-call does (shape B). Caller p99 is 62× lower (writer doesn't queue on ledger lock), but end-to-end caller latency including queue residency is ~186 s p50. **D3 stands.** The super-batched drainer variant (multi-row events merged into one `post_transfers` call) is filed as `acct-hbg` — gate any reopen of D3 on its results.
- **`standard` cost only in Phase 0** (Part VII Q4 resolved, bd `acct-93b.4`). Schema includes a `cost_method` enum and `skus.cost_method` column (default `'standard'`) so `post_transfers` dispatches on it; non-`standard` branches raise `P0006`. WAC / FIFO / lot tracked as `acct-8gg`.
- **TigerBeetle is a reference model, not a parity target** (Part VII Q1 resolved, 2026-04-29). TB informs behavioral correctness (atomicity, lock semantics, idempotency, append-only) but the implementation does not have to shape itself to TB's primitives. Postgres-native ergonomics win where they conflict.
- **No fixed TPS target; correctness > performance; baseline before complexity** (Part VII Q2 resolved, 2026-04-29). The project is an exploration of what's possible Postgres-native vs the v0.1 hybrid Postgres/TB design. The §14.1 perf baseline is established on the *simplest* schema first (`acct-1ia` produces `perf_baseline_v0.md`), then every Phase 1 complexity addition is diff'd against it. Phase 1 schema expansion (customers, work_orders, routings, BOMs, alternate cost methods) is gated behind the baseline so regressions are detectable.

## Open questions that gate work

Part VII of the consolidated doc lists 10 gating questions. **Q1 (TB optionality), Q2 (TPS projection), Q3 (outbox), Q4 (cost method) are resolved** (see "Load-bearing design decisions" above). The remaining open ones are scope-shaping rather than framing:

5. Reservation lifetime — sub-second timeouts ever needed? (Drives `pg_cron` vs `LISTEN/NOTIFY`.)
6. Append-only enforcement model — trigger + RBAC + both?
7. CDC sinks at MVP — none / search / OLAP?
8. Commodity materiality threshold — 5% is a placeholder.
9. Tier-2 mat view scope at MVP — default none.
10. Per-WO per-op account opt-in — default none.

These don't gate further engineering work the way Q1/Q2 did. Surface them when a task forces a choice; default behaviors above otherwise.

## Testing methodology (when implementation starts)

Part IV §14 specifies a three-layer progression:

1. **Exploratory** — measure on real hardware, populate `perf_baseline_vN.md`, replace the envelope estimates in §12.3 with real numbers.
2. **Structured** — five reference workloads (ecommerce / manufacturing / distribution / month-end close / backfill); SLOs filled in from exploratory output; perf-regression CI gates.
3. **Integrated** — full path through API tier, optional outbox, pgBouncer, DB; OpenTelemetry traces per layer.

SLO numbers in §14.2 are deliberately TBD — they get filled in after the exploratory pass, not before.

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
