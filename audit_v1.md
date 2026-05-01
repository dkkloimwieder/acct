# Foundation audit v1 — pre-Slice-A health check

**Date:** 2026-05-01
**Scope:** repo state after Phase 1 cost-method matrix completion (32 migrations, 173 passing tests, all three close-period hooks have real bodies); before pivot to Slice A (PO + AP / vendor bill).
**bd issue:** acct-ok2
**Plan:** `~/.claude/plans/formulate-a-detailed-plan-glistening-treasure.md`

## Methodology

Three exploration agents ran in parallel against the current state:
1. Test surface — file inventory, migration→test mapping, §14-layer status, smoke vs matrix classification.
2. Schema/migration health — sequence completeness, dead code, lock-order invariant, idempotency pattern, error code consistency, refactor opportunities.
3. Doc/code consistency — design doc vs shipped code, CLAUDE.md vs reality, db/README vs migration source, cross-reference integrity.

After agent reports came back, the most material claims were verified directly by reading source (agents miscategorize sometimes — the test agent claimed `inventory_adjust.rs` and `cost_adjust.rs` were "smoke only" when both have full-matrix coverage; same agent also reported `perf_baseline_v0.md` doesn't exist when it's a 95KB file at repo root).

## Section 1 — Fixed in this audit pass (commit `34fc68d`)

Doc/script drift items verified directly and fixed:

| File | Before | After |
|---|---|---|
| `CLAUDE.md` repository-status section | "21 sequential reversible migrations", "26 integration test binaries", "Phase 0 is functionally complete" | 32 migrations, 38 test binaries, Phase 0 + Phase 1 cost-method matrix functionally complete |
| `CLAUDE.md` §14 testing-methodology section | "when implementation starts" | Status per layer: Exploratory done for 21-mig schema; Structured + Integrated still ahead |
| `db/README.md:117` dispatch-table attribution | conflated wac_periodic/wac_retroactive dispatcher introduction with the per-class qty divisor refactor | Split: wac_periodic = 0029 (`acct-qfj`); wac_retroactive = 0031 (`acct-9tw`); per-class divisor = 0030 (`acct-1vr`) refactored across all WAC branches |
| `db/README.md` error-codes table footnote | (no explanation) | Added one-liner explaining the P0007–P0009, P0012–P0013 numbering gaps are intentional |
| `tests/common/mod.rs:117` `seed_standard_cost` docstring | "doesn't exist until acct-8rv lands" (acct-8rv shipped months ago in 0027/0028) | Describes why a test would bypass `post_standard_cost_roll` (skip OCC + WIP + revaluation gates for ad-hoc test SKUs) |
| `scripts/ci-check.sh` schema digest | Different digest every run (PG 18 `\restrict <random>` header) | Digest stable: stripped `\restrict`/`\unrestrict` lines before sha256sum |

**Attempted but reverted:** added a `HISTORICAL NOTE` comment block to `db/migrations/0026_close_period.up.sql` describing the post-shipped 2-arg hook signature evolution. sqlx-cli rejected it (`error: migration 26 was previously applied but has been modified`). Migration files are content-addressed and immutable after first apply on any database. Reverted. The post-shipped state is already documented in `db/README.md` (close_period section, 2-arg hook contract paragraph). Lesson saved as bd memory `migration-files-are-immutable-content-addressed`.

## Section 2 — Verified, no change needed

Agent claims that turned out to be false-positives or already-correct:

- **Test agent: "no `perf_baseline_v0.md` exists"** — false. File is 95KB at repo root, dated 2026-04-30. Caveats section honest about rig noise.
- **Test agent: "`inventory_adjust.rs` and `cost_adjust.rs` are smoke-only"** — false. `inventory_adjust.rs` has 16 test fns covering standard / WAC IN / WAC OUT / multi-class / fifo rejection. `cost_adjust.rs` has 10 test fns covering write-up / write-down / no-op / multi-method dispatch / idempotency / class routing. Both are full matrix.
- **Test agent: "conformance harness has ~91 cases"** — undercount. Actual: 107 cases across 33 transfer reasons.
- **Doc agent: "§3.14 cross-reference inconsistency at lines 1278/1315"** — false-positive. The line-1278 narrative reference back to the section heading is normal prose, not a broken anchor.
- **Schema agent: "`_post_transfers_lookup_qty_account` may be unused"** — still used in `post_transfers` lock pre-scan (`db/migrations/0030_transfers_qty.up.sql:283`, `db/migrations/0031_wac_retroactive.up.sql:271`). Narrowed from divisor-computation use in 0030, retained for lock pre-scan only as documented in `0030_transfers_qty.up.sql:39`.
- **Schema agent: "lock-order invariant adherence" + "idempotency pattern adherence"** — both confirmed consistent across all `FOR UPDATE` sites and document-layer wrapper functions.

## Section 3 — Queued as bd issues (out of audit scope, real but not blocking)

| Issue | Title | Why deferred |
|---|---|---|
| `acct-ool` | T1 invariant probes for Phase 1 tables | Matrix tests exercise the workflows; per-table boundary probes (CHECK violations, FK integrity, concurrent UNIQUE collisions) would harden the surface but constraints are in the schema. Hardening, not blocking. |
| `acct-ss8` | Extend conformance harness with Phase 1 transfer reasons | T5 currently has 1 case for `cost_restate` and 0 for `inventory_adjustment` / `cost_adjustment` as transfer_reason values. Slice A will introduce more reasons; better to land them all in one pass. |
| `acct-q43` | `post_transfers` consolidation pass | Function is now ~1078 lines (0030) with dual-pass logic. Reads acceptably and is correct; the next signature-touching epic should consider extracting common helpers FIRST to avoid making duplication worse. Defer until Slice A (or whichever slice forces the question). |

## Section 4 — Already filed, surface state vs Slice A

These bd issues were already open. Audit confirms each is appropriately sequenced:

| Issue | Title | Slice A relevance |
|---|---|---|
| `acct-9ij` | Phase 2 Epic H: negative inventory support | Low. PO receipts increase qty; oversold-then-receive is a Phase 2 concern. |
| `acct-7h4` | Phase 2 Epic K: period reopen workflow | Low. Only matters if Slice A wants to fix-up closed periods, which it shouldn't out of the gate. |
| `acct-c4p` | Pivot post_transfers transport to pseudo-sync (shape L) | Watch. Slice A adds transfers per document; if measured contention emerges this gets pulled forward. |
| `acct-bru` | Phase 2 Epic G: WIP material revaluation | Out of inflow scope. |
| `acct-cms` | Phase 2 Epic I: alternate provisional cost sources | Out of inflow scope. |
| `acct-p7v` | Phase 2 Epic J: wac_periodic / wac_retroactive across WIP | Out of inflow scope. |
| `acct-8gg` | FIFO / lot cost methods | Phase 2 cost work, not inflow-driven. |
| `acct-e8g` | Convert transfers to time partitioning | Scale concern; not blocking. |

## Section 5 — Slice A foundation gap (handled at Slice A kickoff)

`db/fixtures/small/seed.sql` has zero PO/AP scaffolding. Specifically missing:

- No supplier / vendor counterparty data domain.
- No `goods_received_not_invoiced` (GRNI) account kind for accrual on PO receipt.
- No PO-receipt-side accounts (`po_receipt`, `po_receipt_value`, freight accruals).
- No AP-payment-discount or AP-rounding accounts.
- No three-way-match infrastructure (PO line ↔ receipt ↔ vendor bill).

Per the audit decision (D2), this gap is **deferred to Slice A's own kickoff** rather than filled speculatively in this pass. The workflow design will determine the right account shapes; guessing now would risk re-doing the fixture once Slice A begins.

## Section 6 — Performance re-bench

Per audit decision (D3), running the full 13-shape baseline against the current 32-migration schema using the same methodology as v0 (3 runs × 5 minutes per shape, vmstat sidecar). Total wall time ~3.5 hours.

Driver: `scripts/run-perf-baseline-v1.sh` (new, committed in audit pass).

Output goes to `/tmp/perf_v1_run/` per shape; aggregated into `perf_baseline_v1.md` with a "diff vs v0" column for each shape after the bench completes. Any shape with a regression beyond the documented v0 noise band (~15–20%) becomes a separate bd issue and gates Slice A pending root-cause.

Status at this writing: bench is running. Update this section when complete.

## Section 7 — Phase D items not raised by agents

While reviewing the agents' output, two adjacent items surfaced that aren't in the original plan but are worth recording:

- **`acct-z2x` mistake taxonomy.** Earlier this session we discovered that `acct-z2x` (qfj.3 wac_periodic doc) was claimed complete in bd memory but actually wasn't shipped — the design doc still said "filed as acct-qfj" and CLAUDE.md had no load-bearing bullet. We fixed it in commit `c255dca`. The doc-consistency agent specifically searched for similar drift and found no other instances. So the drift was localized; not a systemic issue with how completions are recorded, but worth logging that we looked.
- **PG 18 `\restrict` semantics.** While debugging the schema digest non-determinism, learned that PG 18's `pg_dump` emits a randomized `\restrict <key>` / `\unrestrict <key>` header pair as a security feature (prevents psql-restriction replay attacks). This was breaking `ci-check.sh`'s "schema integrity check" tagline because every dump had a different header. Fixed by stripping those lines before `sha256sum`. This is a `pg_dump` quirk worth knowing about for any future tooling that hashes dumps.

## Verification

After all phases:

- `./scripts/run-tests.sh` — 173 passing / 0 failing / 8 ignored. ✓ (2026-05-01)
- `./scripts/ci-check.sh` — round-trip clean, schema digest **now stable** at `842113a34338fdcf04d63c4e899d034b2d480132a2fe5903a3cf2e2948092af9` on the current 32-migration schema. ✓ (2026-05-01)
- `git diff` since audit start — only doc/comment/script changes; no schema mutation. ✓
- `audit_v1.md` (this file) — durable artifact at repo root.
- `perf_baseline_v1.md` — pending bench completion.
- bd issues for deferred items — `acct-ool`, `acct-ss8`, `acct-q43` all filed. ✓

## Out of scope (deliberate)

- Expanding `db/fixtures/small/seed.sql` — Slice A's job (D2).
- Filing the Slice A epic itself — separate planning session.
- Any schema change. The audit confirms foundation correctness; it doesn't reshape it.
- Backfilling T1 probes for the new tables (`acct-ool` filed instead).
- Extending the conformance harness for Phase 1 reasons (`acct-ss8` filed instead).
- `post_transfers` consolidation pass (`acct-q43` filed instead).
