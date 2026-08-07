# PoC research streams

Seven research streams lived under `poc/` between 2026-05 and 2026-07, each a separate
Cargo crate / separate scope, characterizing architectural alternatives for the costing
ledger. **The program converged on 2026-08-07**: the `ledger-v3.2` line is the surviving
architecture (staging-table + single-statement + alt-C + logical-decoding feed, per the
`acct-0at4.11.5` gate verdict), merged to `main` (commit `8378a7d`). The full decision
record is
[`design_research/convergence-decisions-2026-08-07.md`](design_research/convergence-decisions-2026-08-07.md)
— read it before re-litigating any stream's fate.

Nothing under `poc/` imports from the main acct crate and vice versa. PoC databases live
on the shared dev container (`localhost:5111`) under distinct DB names — except
`batch-ledger` and `ledger-extension`, which share `acct_poc` (the extension was built
against the batch-ledger schema, and its tests + bench results live under
`poc/batch-ledger/`).

## Streams

In lineage order. "Verdict" is the tracker-recorded end state as of 2026-08-07.

| Dir | Tracker root | Verdict / state | One-line summary |
|---|---|---|---|
| `batch-ledger/` | `acct-qdp5` | closed **unverdicted** 2026-08-07 (paused mid-program) | Pure-SQL batch-API PoC toward 10K TPS. P2–P5 results in-tree (`bench/results-p2.md`…`results-p5.md`); P6 (state machine/GRNI) and P7 (HC1–HC12 hard cases) never ran — open as `acct-ha7g` / `acct-yneu` behind the PAUSE gate. The backport / separate-target / no-go question was never answered inside the epic; the later gate verdict named a different surviving architecture instead. |
| `ledger-extension/` | `acct-sw4i`, `acct-tpqw` | closed **complete** (M1–M9 + M10 hardening); Track B items remain open P3 | Shmem rollup + bgworker drain pgrx extension; validated its premise (fan-in 2.16×, fan-out 5.55×) but the lineage moved on. Tests and bench artifacts live under `poc/batch-ledger/`. |
| `queue-extension/` | `acct-4d4n` | closed **CONDITIONAL PASS** 2026-05-16; condition never consumed (annotated 2026-08-07) | Queue + per-shard committer primitive (v2); design-v2 construction never started. |
| `queue-extension-v21/` | `acct-gx1z` | closed **unverdicted** 2026-08-07 | Two-queue + affinity-router pattern with two-domain lexicographical locking (SKU + WIP) — v2.1's differentiator from v2's per-item shards. Validation spec `design_research/poc-v2.1.md` (in-tree); reference architecture archived at `../ARCHIVE/design-v2.1.md`; benchmark evidence through M8 in `bench/`. |
| `ledger-v3/` | `acct-dipt` (Phase 6) | characterization **complete**; stream paused (`acct-ytd9`) | Paths A (direct, strict) vs B (routed) measured across s1–s6; produced the crossover map and the Path A hot-pool-collapse evidence that motivated Path C. |
| `ledger-v3.1/` | `acct-2ttr` (closed), `acct-0at4` (**still open**: residual follow-up `acct-0at4.14`; stream unpaused — its PAUSE gate `acct-1wyk` closed 2026-05-25) | **gate verdict** `acct-0at4.11.5`: machinery not justified | Path C (provisional hot path) PoC + SPIKE-A/B. The staging-table spike beat the shmem routed stack; `ledger-routed-c` was physically deleted 2026-08-07 (`acct-uena`, commit `6ddbf47`). `ledger-direct-c`, the harness, and the replay oracle remain as reference. |
| `ledger-v3.2/` | `acct-qm7o` | closed **complete with positive verdict** 2026-08-07; hardening rides six open audit epics (below) | **The survivor.** Full implementation: alt-C hot path, logical-decoding feed, recalc engine, close gate. Phase-6 soak PASSED oracle-exact. Merged to `main` 2026-08-07. |

## Specs (`design_research/`)

- `convergence-decisions-2026-08-07.md` — **the decision record** (Q1–Q14, ratified).
- `design-v3.2.md` + `design-v3.2-recalc-{a..e}.md` — the surviving line's spec skeleton +
  five recalc design notes. Completion into a spec-of-record is `acct-476a.1`.
- `design-v3.1.md` — Path C spec, including §16 posture / §17 feed / §18 gate verdict /
  §19 recalc risk / §20 alternatives. Decided inputs — do not re-litigate.
- `design-v3.md` — the Paths A/B PoC design for `ledger-v3/`. The A/B/C revision that
  first specified Path C — the origin of the surviving lineage — is archived at
  `../ARCHIVE/design-v3-abc.md`.
- `design-v2.md`, `poc-v2.1.md`, `poc-validation-spec.md`, `ext_design_spec.md` — retired
  lines (v2, v2.1, ledger-extension). The v2.1 reference architecture is archived at
  `../ARCHIVE/design-v2.1.md`.

## Open work on the surviving line

Six v3.2 audit epics are open (all `stream:ledger-v3.2`): `acct-476a` (spec-of-record +
graduation/backport dossier), `acct-1vur` (**P1** — close & backdate correctness
hardening), `acct-zrju` (hot-path correctness), `acct-m0ab` (write-amplification bounds),
`acct-63qs` (test/bench coverage; its `63qs.6` baseline is gated on `m0ab.1/.2` per
convergence Q12), `acct-gtp7` (operational readiness). Hardening order per Q12:
`1vur` → `m0ab`.

## PAUSE gates

Six P0 PAUSE gates remain open, covering the retired/deferred streams: `acct-1cis`
(batch-ledger), `acct-23kd` (acct main), `acct-hby7` (queue-extension-v21), `acct-s21z`
(queue-extension), `acct-ui7w` (ledger-extension), `acct-ytd9` (ledger-v3). Each gate has
a filed disposition child (`acct-1cis.1`, `acct-23kd.1`, `acct-hby7.1`, `acct-s21z.1`,
`acct-ui7w.1`, `acct-ytd9.1`): write the stream's one-line disposition, decide its gated
remnants, then close the gate. ledger-v3.1 and ledger-v3.2 are unpaused.

## Integration posture

`ledger-v3.2` is the **designated** starting architecture. It lives on `main` at
`poc/ledger-v3.2/` as a standalone Cargo workspace on its own database (`poc_v3_2`) — it
is not yet wired into the acct tree, which still contains its own plpgsql costing plane
(migrations `0013`–`0015` + the WAC family) alongside the document layer (70 migrations,
29 `post_*` wrappers, frozen 2026-05-11). Both halves of the acct tree are deferred
pending the Q2 deciding experiments `acct-476a.2` / `acct-476a.4` (asset-vs-rebuild).
