# Committer fsync vs commit-group size (acct-e95d item 2)

The prep refold + dedup seek (acct-e95d) left `fsync` as the largest non-apply
committer span. `fsync` is the group COMMIT (`txn_ns − pipeline_ns`): **one fsync
per commit_group**, so its per-trx cost is `per-group-fsync / commit_group_size`.
This characterizes how it amortizes as the group grows.

## Method

`ledger-harness/bench/measure-apply-spans.sh` (cc=1, single committer, single
pool, all-fifo, single-push flood, `batch_window_us=20000`, 3 reps) swept over
`ledger_routed_c.batch_size_max` ∈ {50,100,200,400,800}. The router caps each
commit_group at `batch_size_max`; the span counters give `fsync` µs/trx and the
achieved `trx_per_group`. Per-size CSVs: `results/apply_spans_fsync_rb*.csv`.
Host load ~1.4 across rb100–800 (rb50 partly lower) — the structural span ratios
are load-robust; the throughput trend holds because rb100–800 ran at like load.

## Result

| batch_size_max | achieved cg | fsync µs/trx | fsync % | per-group fsync | apply µs/trx | tput (median) |
|---------------:|------------:|-------------:|--------:|----------------:|-------------:|--------------:|
| 50                |  48.5 | 39.07 | 41.4% | ~1893 µs | 44.4 | ~11.2k |
| 100               |  94.6 | 21.18 | 27.6% | ~2004 µs | 47.7 | ~13.7k |
| 200 (prod default)| 181.8 | 11.47 | 18.4% | ~2086 µs | 45.5 | ~16.7k |
| 400               | 322.8 |  6.68 | 12.2% | ~2158 µs | 43.7 | ~18.9k |
| 800               | 480.2 |  4.55 |  8.7% | ~2186 µs | 43.4 | ~20.0k |

## Findings

1. **fsync/trx falls as 1/cg, exactly the amortization model.** The per-group
   fsync cost is ~flat (~1.9–2.2 ms/commit; it rises only slightly with cg as the
   WAL flush grows), so spreading one fsync over more trx drops fsync/trx from
   39 µs (cg≈48) to 4.6 µs (cg≈480) — fsync% 41.4% → 8.7%.

2. **`batch_size_max` is the fsync lever, but cg is ingress-capped at cc=1.** cg
   tracks the cap up to rb≈400 (cg 323), then **rb800 yields only cg≈480, not
   800** — a single flooding caller can't keep the staging ring full enough to
   fill 800-trx groups inside the 20 ms window. Past rb≈400, raising the cap gives
   diminishing cg (hence fsync) returns at this ingress. To push cg higher needs
   more ingress: caller-side batching (`run-1sku-batched.sh`, CBATCH=8192,
   acct-ruex) or more callers/committers.

3. **The production default `batch_size_max=50` sits at the worst point** — cg≈48,
   fsync 41%, ~11.2k trx/s. Raising it to ~400 cuts fsync to ~12% and lifts
   throughput ~1.7× (to ~18.9k) at the same host load, purely by amortizing the
   commit. apply µs/trx is flat across the sweep (~44 µs) — it's per-trx and does
   not amortize, confirming it as the irreducible floor; the entire throughput
   gain is fsync amortization.

4. **Tradeoff: latency.** Bigger groups raise per-submission ack latency (a
   submission waits for its group to fill + the commit), bounded by
   `batch_window_us`. This sweep did not measure ack latency (the span instrument
   doesn't); the latency/throughput knee is the open tuning question. No fixed TPS
   target (Part VII Q2), so the default is a latency-conservative choice, not a
   throughput-optimal one.

## Takeaway

`fsync` is not a committer code problem — it's a commit-group-size tuning knob.
The amortization is clean and predictable (1/cg). The actionable lever is
`batch_size_max` (default 50 is fsync-heavy); the ceiling at cc=1 single-push is
ingress (~cg 480), liftable only with caller-batching or more concurrency. A
production default in the 200–400 range trades a bounded latency increase for a
~1.5–1.7× throughput gain. Picking the exact value belongs with a latency SLO
(Structured-testing phase), not characterization.
