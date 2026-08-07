# Committer apply hot-path — phase split (acct-q6sx STEP 0, motivates acct-sczx)

The committer at cc=1 healthy (171 trx/group, ~4900 trx/s, ~160 µs/trx) is **SQL-execution-bound**.
acct-q6sx earlier established the DSO rollup (postgres 89.4% / our .so 2.5%) but could not split the
89% — the binary is stripped. This artifact splits it with symbol-resolved profiling.

## Method (reproducible)

- Capture: `/tmp/committer.perf.data` — `perf record -F 999 -g --call-graph dwarf` on the cc=1
  committer BGWorker during steady load (29001 samples). Setup via
  `ledger-harness/bench/setup-cc1-for-perf.sh`.
- Symbols: postgres `18.3-1.pgdg13+1`, build-id `a4b52da966144ffe6de3fb0b8ed5c38dbf15efc6`
  (verified == the build-id with hits in the capture). The matching `postgresql-18-dbgsym` was
  pulled from the PGDG archive (the live repo had moved to 18.4) and resolved via `--symfs` /
  build-id cache.
- Resolved flamegraph: `results/committer_resolved.svg` (supersedes the DSO-only
  `results/committer_apply.svg`).

## Inclusive per-phase split (% of all committer CPU)

Everything runs through SPI: `_SPI_execute_plan` = **71.8%** inclusive.

| Phase                                   | Inclusive % | Eliminable by    |
|-----------------------------------------|------------:|------------------|
| raw parse (`raw_parser`: lex + yacc)    |       8.5%  | cached plans     |
| analyze + rewrite (`pg_analyze_and_rewrite_fixedparams`) | 21.9% | cached plans     |
| plan (`pg_plan_queries` → `standard_planner`)            | 16.9% | cached plans     |
| execute (`standard_ExecutorRun` → `ExecModifyTable`: heap+index+WAL) | 16.7% | nothing (per-row floor) |
| rest                                    |  ~rest     | SPI dispatch / snapshot / memory teardown (batching) |

**Parse + analyze + plan = ~47% of all committer CPU** — work that is identical for every call and
fully eliminable. The actual INSERT execution (heap/index/WAL) is only ~16.7%.

`GetCachedPlan` shows 17.6% inclusive but is **not** caching: one-shot SPI builds an *unsaved* plan
source and re-plans on every call.

pg_stat_statements is **~1%** here (`_jumbleNode` / `AppendJumble` in the parse path), not the
"~33% in-path" the black-box sample-count had suggested.

## Flat self-time (top, % of all samples), postgres DSO total 88.6%

```
 6.26% palloc0              4.18% base_yyparse        3.78% SearchCatCacheInternal
 3.53% AllocSetAlloc        1.79% expression_tree_walker_impl   1.75% hash_search_with_hash_value
 1.70% check_stack_depth    1.32% ResourceOwnerForget 1.30% core_yylex
 1.01% CatalogCacheComputeHashValue   0.97% hash_bytes  0.93% _bt_compare
 0.90% palloc               0.77% LWLockAttemptLock   0.75% copyObjectImpl ...
```

Bucketed self-time: memory/palloc 15.6%, analyze+plan tree-walk 11.5%, execute (heap/idx) 6.9%,
catalog cache 6.9%, parse (lex+yacc) 6.7%, hash/util 3.9%, lock 3.1%, WAL 2.0%.
(Much of the ~29% "unbucketed" — `lappend`, `CheckExprStillValid`, `can_coerce_type`,
`finalize_plan`, `ScanKeywordLookup`, `ExecTypeFromTLInternal`, … — is further parse/analyze/plan.)

## Decision (acct-sczx)

Bottleneck is per-statement **planning**, not per-row execution. Two composing levers:

- **Lever B — prepared/cached plans** (chosen first): keep the SPI plans in the long-lived
  committer BGWorker so parse+analyze+plan happen once, not per call. Removes the ~47%. Low risk
  (no FK id-chain / WAC-ordering restructure). Target ~2× ceiling.
- **Lever A — cross-submission batching**: collapse the ~700 one-shot statements/group into ~6
  multi-row INSERTs. Removes the 47% AND the per-statement SPI/executor dispatch framing. Higher
  ceiling (~5×) but correctness-heavy (preserve `trx → trx_line → posting_line` id-chain + WAC
  ordering). Layered after B.
