# Convergence decisions — 2026-08-07

**Status:** Decision record, ratified 2026-08-07. All 14 open convergence questions
answered by dkk after a full-repo analysis (main + poc-31 worktree + ledger-v3.2 branch +
bd tracker). This is the durable, versioned record of those answers. The CLAUDE.md
"load-bearing design decisions" discipline applies: re-litigating any decision below is a
design change requiring deliberate justification, not an incidental edit.

**Context.** The PoC program produced seven architecture generations (acct main plpgsql
tree, batch-ledger, ledger-extension, queue-extension v2, queue-extension-v21, ledger-v3
Paths A/B, ledger-v3.1 Path C, ledger-v3.2). The `acct-0at4.11.5` gate verdict — verbatim:
"Machinery not justified: staging-table + single-statement + alt-C + logical-decoding-feed
is the surviving architecture" — named the survivor. The questions below decided how to
collapse the streams onto it and what to do with everything else.

---

## Q1 — Unification plane: costing plane first

Unification targets the ledger/costing foundation (the ledger-v3.2 line). The document
layer (acct main tree: 70 plpgsql migrations, 29 `post_*` wrappers, 983 tests, frozen
2026-05-11) is not being unified in this pass; its fate is explicitly deferred to Q2's
deciding experiments.

## Q2 — Document layer: defer, with deciding experiments

`acct-476a.2` and `acct-476a.4` are designated the experiments that decide
asset-vs-rebuild for the document layer. Until they report, the acct tree and its paused
issues (~48) stay intact — no deletions, no premature port.

## Q3a — Valuation posture: ratify alt-C, with bounds

Alt-C is ratified: the hot path records physical events only (no cost leg for FIFO/LIFO);
recalc is the sole costing engine for layer-tracked methods (WAC/STD/specific remain final
on the hot path); close = consistency gate + finalize stamp; force = drain-synchronously,
never bypass. Ratification is **with bounds**, to be specified during hardening (natural
home `acct-63qs.6`):

- a recalc-lag SLO expressed on the G2 gauges,
- a close-cadence policy,
- a sized forced-close cost (D12).

The bounds bound *wrongness-exposure* (how stale/provisional mid-period costs may get),
not throughput — see Q13.

## Q3b — Quantity gate: flag-not-gate

The ledger flags negative inventory; it never rejects on quantity. Gating is a future
document/seam concern, not a ledger concern. Follow-through: direct the wac qty-gate
reconciliation toward removal — the v3.2 soak showed 84 wac qty-gate rejects, i.e. the
implementation is not yet at the designed posture.

## Q4 — Substrate: pgrx now, named revisit triggers

Harden on the built pgrx artifact (ledger_direct is shmem-free; no
shared_preload_libraries requirement). The substrate decision reopens only on named
triggers: error-identity failure at the seam, PG-major-version friction, or upgrade-path
cost. The CLAUDE.md stack section remains formally open until the spec-of-record rewrite.

## Q5 — Rescue docs: commit to ARCHIVE/

`design-v2.1.md` and `design-v3-abc.md` — previously uncommitted single working copies —
are committed to `ARCHIVE/` with SUPERSEDED banners (executed same day, commit
`a0ebb1e`). `acct-mpjz` references design-v2.1 §14 (alternatives catalog); its section
numbering is preserved verbatim. The stale untracked `design-v3.1.md` working copy was
deleted (byte-identical to an old committed blob; the ledger-v3.2 branch copy strictly
supersedes it).

## Q6 — Reservations: fold acct-cz1v into acct-476a.2

`acct-cz1v` (reservation semantics) folds into `acct-476a.2`. The two deciding dossiers
(`acct-476a.2`, `acct-476a.4`) run **early** — immediately after spec-of-record
(`acct-476a.1`).

## Q7 — Currency: functional-currency valuation

The costing plane stays single-currency as built. Documents convert FX **before**
submitting inventory facts to the ledger; `acct-476a.3`'s currency half becomes a seam
conversion contract. Do not re-import acct's per-currency pools — that shape was a
TB-parity artifact.

## Q8 — Period reopen: conditional on acct-1vur

If close-gate hardening (`acct-1vur`) proves the gate airtight, ratify reopen-out (no
reopen primitive). If an irreducible hole remains, reopen becomes a hardening
requirement. D11's no-reopen premise is provisional until that verdict. Follow-through:
wire the missing dependency edge `acct-476a.5` ↔ `acct-m0ab.3` — the checkpoint design
must not hard-assume no-reopen prematurely.

## Q9 — Legacy stream verdicts

- **Q9a (queue-extension-v21):** close `acct-gx1z` and `acct-bhfe` *unverdicted* with a
  stated reason (the stream was superseded before its verdict milestone ran; SPIKE-A +
  the `acct-0at4.11.5` gate verdict answered the question it was built to ask). Annotate
  `acct-zkb6` supersession separately.
- **Q9b (batch-ledger):** close `acct-qdp5` and `acct-qb0q` unverdicted with a stated
  reason. Respect qdp5's do-not-close instruction for its 8 related issues: re-point each
  individually, never bulk-close. The he2w 28-wrapper matrix lives only in bd — covered
  by the JSONL export now tracked in git.
- **Q9c (v2 conditional):** annotate `acct-4d4n` (its condition was never consumed; the
  line is superseded); close `acct-hjoq` moot.

## Q10 — ledger-routed-c: delete + prune + close moot

One coordinated change (`acct-uena`): remove ledger-routed-c from tip, prune its
`shared_preload_libraries` entry (ALTER SYSTEM overlay in the data volume — verify live
at next dev-up), close `acct-uena` and `acct-mvq4.43` moot with pointers.

## Q11 — Reference topology: merge to main

Merge ledger-v3.2 → main with a convergence merge commit recording these decisions;
retire the poc-31 worktree (done same day); delete the merged branch labels
(`ledger-v3.2`, `worktree-poc-31`) after the merge.

## Q12 — Write-amplification bounds (acct-m0ab): declare now, gate transitions

Adoption of bounded-write-amplification requirements is declared now; hardening order is
`acct-1vur` → `acct-m0ab`. The perf baseline (`acct-63qs.6`) and any seam-integration
epic **refuse to ship** until `acct-m0ab.1`/`.2` land. Paper dossiers still run early.

## Q13 — Posture: correctness-first stands, amended

No-TPS-target / correctness-first / baseline-before-complexity is reaffirmed, with one
amendment: the rewritten posture must explicitly name the Q3a drift-exposure bounds as
in-scope product bounds. They bound wrongness-exposure, not throughput; their numbers are
filled from the gated `acct-63qs.6` baseline, not chosen up front.

## Q14 — Backup: git + bd, via github.com/dkkloimwieder/acct

Executed 2026-08-07: all three branches pushed (SSH remote); `bd dolt push` (Dolt remote
auto-configured from the git origin); full tracker prose exported via `bd export --all`
and tracked as `.beads/issues-export.jsonl`. Cadence: git commit + git push + `bd dolt
push` after each work item; refresh the JSONL export at session close.

---

## Same-day repository surgery (read this before chasing old hashes)

- **History was rewritten** with git-filter-repo: nine bench payload `.jsonl` corpora
  (regenerable derived artifacts; two exceeded GitHub's 100 MB limit) were stripped from
  all refs. Repo store went 112 MiB → ~6 MiB packed. **Every commit hash after
  2026-05-19 changed**, and the pre-rewrite objects were gc'd — commit-hash citations
  from that era in bd issue bodies and result docs now dangle. Resolve historical
  references via bd IDs, not hashes; note-and-move-on when a dangling hash appears.
- The payload corpora remain on disk and regenerate deterministically
  (`bench_m8_ceiling.rs`, UUIDv5, byte-stable); the tracked `.meta.json` sha256 files pin
  expected content.
- The poc-31 worktree was removed; the `worktree-poc-31` branch label survives
  (rewritten).
- **Working-tree triage (wave-1 item 3):** the long-lived uncommitted drift was
  discarded rather than committed, because all of it belonged to retired streams: the
  `poc-v2.1.md` spec amendment (+173/−54) and six modified pl3b-sweep bench JSONs
  (queue-extension-v21, retired by the gate verdict) were reverted to their committed
  state; 33 stale phase6 equivalence logs + one stray result JSON under
  `poc/ledger-v3/results/` (2026-05-23 runs, superseded by the v3.2 line) were deleted;
  `.gitignore.tmp` (a leftover one-line intent to ignore `poc/ledger-v3/ledger-direct/sql/`)
  was discarded — the ledger-v3 stream is superseded, and its generated SQL stays
  tracked per the stream convention.

---

## Q2 RESOLVED (same day, wave 2): asset, not rebuild

Ratified by dkk 2026-08-07 after both deciding dossiers reported (drafted, adversarially
fact-checked, and finalized the same day — commits `e82d479` → `18761a5`):

- **`acct-476a.2` (reservations):** no rebuild forced. Shape = **S3 with the pinned path
  split out** — quantity gating stays at the document/seam (`inventory_reservations` +
  `reserve_inventory` port as an asset with their hard-in-effect semantics; the v3.2
  ledger gains no reservation concept and no per-pool serial-fold ceiling). Lot-specific
  pins are cost-layer selectors and become a **seam→costing-plane input contract**
  specified alongside `acct-476a.3` — new seam work, not a rebuild trigger.
- **`acct-476a.4` (document-cost):** asset. The `document_cost` view (max-generation
  `cost_settlement`, method-aware) carries `provisional → settled (revisable) → final`;
  R7 restates from simultaneity to convergence; invoices float until close then pin;
  close never rewrites document lines. The FIFO/LIFO hot path gains a provisional cost
  leg at observed cost (**decided**, `wac_periodic`-style; implementation + measurement
  = `acct-zrju.7`). Migration surface ≈ five pool-derived columns + a re-resolved return
  path + R7 property tests restated to convergence form. The real cost is behavioural:
  mid-period COGS for layer-tracked SKUs is decision-support (~22–30% band, directional
  bias) until settlement.

**Consequences:** `acct-23kd`'s reopen trigger (both dossiers reported + verdict
recorded) is **met** — the gate stays closed until a port/integration plan exists as its
own planned program; lifting the gate is that program's first act, not a side effect of
this ratification. The acct document layer is confirmed as a **port target, not a
teardown**: its 70 migrations / 29 wrappers port against the v3.2 costing plane behind
the Q7 functional-currency seam, with `acct-476a.3` (seam contract) and `acct-476a.5`
(close-hook mapping) as the remaining spec inputs.
