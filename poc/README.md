# PoC research streams

Four research streams live under `poc/`. Each is a **separate Cargo crate / separate Postgres database / separate scope**. None are on the production critical path; their purpose is to characterize architectural alternatives ahead of integration decisions.

Nothing under `poc/` imports from the main acct crate, and the main acct codebase doesn't import from `poc/`. PoC databases live on the existing dev container (`localhost:5111`) under distinct DB names. Migrations are applied via per-PoC `sqlx-cli` setups.

## Streams

| Dir | Epic | Status | One-line summary |
|---|---|---|---|
| `batch-ledger/` | `acct-togd` / `acct-qdp5` | bench phase complete | Pure-SQL batch-ledger PoC measuring per-row hot-path costs across cost methods at fan-in/fan-out shapes. |
| `ledger-extension/` | `acct-sw4i` | closed; M10 hardening in `acct-tpqw` | Shmem rollup + bgworker drain pgrx extension; replaces UPDATE accounts with cache-line-aligned bucket CAS. |
| `queue-extension/` | `acct-4d4n` | CONDITIONAL PASS (2026-05-16) | Queue + per-shard committer pattern (v2 of the queue costing primitive). |
| `queue-extension-v21/` | `acct-gx1z` | M0.1 scaffolding shipped; M1.1 next | Two-queue + router pattern (v2.1) — staging + committer queues with Greedy Window Router middleware. |

## Specs

`design_research/` holds the architectural specs and validation gates:

- `poc-validation-spec.md` — v2 PoC validation gate (now closed).
- `design-v2.md` — v2 reference architecture (partly deferred).
- `poc-v2.1.md` — v2.1 PoC validation gate (active; epic `acct-gx1z`).
- `design-v2.1.md` — v2.1 reference architecture (deferred until M9.1 verdict).

## Per-stream details

Each stream has its own `README.md` with build/setup/bench instructions and phase tables:

- `batch-ledger/README.md` — phases P1–P8 + HC1–HC12 hard cases.
- `ledger-extension/README.md` — M0–M10 milestones for the shmem extension.
- `queue-extension/BENCHMARK_RESULTS.md` — pinned v2 PoC verdict + numbers.
- `queue-extension-v21/` — pgrx 0.18 scaffold (M0.1 acct-jux2); `bash poc/queue-extension-v21/scripts/install-into-container.sh` then `bash scripts/create-poc-v21-db.sh`.

## Integration posture

The main acct schema and any production decision are unaffected by PoC work until an explicit integration epic is filed. The currently filed integration epic is `acct-zkb6` (v2.1 → acct posting_lines workflow), which is **blocked-by** `acct-gx1z` (the v2.1 PoC verdict).
