# acct-hh7b Phase 1 — time-window-only commit_group formation

**Question.** acct-p1al found commit_group size (cg) is co-limited by two entangled
knobs — `batch_size_max` (a hard *document*-count cap) and `batch_window_us` (the time
coalesce gate) — and left disentangling them as the open follow-up. The idea to test:
make `batch_window_us` the **sole** gate (pin `batch_size_max` non-binding) so a
commit_group is "everything that accumulated during the window," with that window
*matched* to committer count / workload / pool distribution / machine IO so formation is
*balanced*. Three things to settle: (1) does pure-time gating beat the cap+window combo on
throughput **and** latency? (2) what relates the balanced window to committer_count ×
pool-overlap × arrival × IO? (3) does an uncapped window form an **unbounded** group — is
a safety ceiling on group size still needed?

**Bottom line — the premise is wrong, and that is the finding.**
- **Dropping `batch_size_max` does not make the window the sole gate.** `router_window_size`
  (the per-tick scan budget, default **1000**) becomes the binding group-size ceiling. The
  escalation probe shows `max_group` tracks `router_window_size` *exactly* (1000→2000→4000→8000).
- **The time window has near-zero throughput leverage on every high-arrival workload tested.**
  Widening it 100× (0→50 ms) grows cg 5–10× but moves throughput **< ±10%** on s2/s5/s6/s7.
  These workloads are lock-/row-lock-/ack-latency-bound, not commit-bound: their staging
  ring stays *backlogged*, so the router packs ~`router_window_size` per tick regardless of
  the window. The window only has cg-leverage at **low arrival** (s10 at cc=1).
- **The real throughput lever is `committer_count`, and its effectiveness is
  pool-distribution-dependent** — exactly hh7b's thesis, now measured: it scales on disjoint
  and on batched single-hot-pool work, and is **flat-to-declining on intertwined work**.
- **No runaway; a natural safety ceiling already exists.** `max_group` bounded at
  `router_window_size`, arena 5–46 MB of 128, **zero drops** — even at 50 ms windows /
  8000-submission groups. Raising the ceiling past 1000 is strictly worse (latency ↑,
  throughput flat-↓), so `router_window_size=1000` already sits at the knee.

**So time-window-only formation is *viable and safe*, but it is *not a throughput win* — the
window is a latency/coalescing knob, not the balance dial. The balance axis that matters is
`committer_count` matched to pool-overlap.** Recommendation (Phase 2 framing) in §6.

---

## Methodology / host caveat

Routed runs through pgbouncer (:6432); each cell is `--mode routed`, `DUR=20s`, **2 reps**,
median shown. `batch_size_max` pinned non-binding (100000), `router_pack_disjoint=on` (the
p1al production default) for every cell. `committer_count` is restart-only, so it is the
outer loop: set it, then `clean_seed` (which `docker restart`s — respawning the committer
pool and zeroing the cluster-lifetime stat counters), then the window sweep runs ascending.
New observability: `router_max_group_size()` (high-water) and `router_submission_histogram()`
(log2 group-size distribution) getters added to read shmem counters that already existed.

**The host is a noisy daily-driver workstation (Chrome up, load 1.4–4.0 throughout).** Per the
agreed run posture, absolutes are contended and flagged; **the conclusions below are
within-session structural patterns and ratios** (max_group ceiling, window-vs-throughput
orthogonality, committer-scaling-by-overlap, the rwsize cost curve) — not absolute trx/s,
which is load-sensitive on this box. cg, max_group, and the ratios are robust to load noise.

---

## 1. The window is not the sole gate — `router_window_size` is the real ceiling

`max_group = 1000` in **every cell of the entire sweep**, across all scenarios and committer
counts, because with `batch_size_max` non-binding the router can only pack what it scans in
one tick — `router_window_size = 1000`. The escalation probe (s6 cc4, window held wide at
50 ms so it never gates) raises `router_window_size` and watches the consequence:

| router_window_size | tput | cg | **max_group** | ack-p99 (ms) | locks/trx | arena (MB) | dropped |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 1000 | 8,631 | 618 | **1000** | 6,833 | 0.38 | 6.1 | 0 |
| 2000 | 8,594 | 779 | **2000** | 7,172 | 0.35 | 6.9 | 0 |
| 4000 | 7,908 | 709 | **4000** | 7,353 | 0.35 | 7.4 | 0 |
| 8000 | 7,963 | 992 | **8000** | 7,829 | 0.31 | 8.1 | 0 |

`max_group` tracks `router_window_size` one-for-one ⇒ **it is the binding group-size
ceiling**, not `batch_size_max`. Raising it is **strictly worse**: throughput flat-to-down
(~8% off at 4–8k), ack-p99 climbs (+14%), groups just get larger. There is **no catastrophic
runaway** — arena stays tiny (6→8 MB of 128), zero drops, even at 8000-submission groups
(`router_window_size ≤ staging_queue_size = 16384`, and the spillover arena absorbs the
overflow). So a ceiling is valuable not for crash-safety (the system is robust) but for
**latency + throughput**, and `router_window_size = 1000` already sits at the knee.

## 2. The window has near-zero throughput leverage (high-arrival workloads)

Window sweep at the production committer_count, `batch_size_max` non-binding. cg climbs
steeply with the window; throughput barely moves:

| scenario (callers) | cc | tput w=0 → w=50ms | cg w=0 → w=50ms | bound (top wait) |
|---|--:|--:|--:|---|
| s2 spread (16) | 4 | 11,384 → 12,374 (+9%) | 136 → 753 | staging LWLock |
| s5 hot pool (1000) | 4 | 9,345 → 9,180 (−2%) | 60 → 540 | row lock (`Lock/tuple`) |
| s6 disjoint (1000) | 8 | 10,753 → 10,894 (+1%) | 810 → 958 | `Lock/transactionid` |
| s7 deep zipf (1000) | 4 | 7,940 → 8,274 (+4%) | 77 → 569 | row lock (`Lock/tuple`) |

The mechanism: open-loop callers keep the staging ring backlogged, so every tick already has
≥`router_window_size` candidates and the router packs ~full groups *regardless of the window*.
Group size is set by **backlog depth + `router_window_size`**, and the window is the least
binding of the three. (vs p1al: window-only on s2 reaches cg≈750 / ~12.5k — the same knee
p1al hit at `batch_size_max=800`; removing the cap ≈ setting it high, no new behavior.)

The **only** place the window had cg-leverage was the low-arrival s10 at cc=1 (shallow
backlog): cg ranged 213→631 with the window. At cc≥4 even s10's backlog builds (committers
can't drain it — §3), pegging cg near the ceiling regardless of window.

## 3. `committer_count` × pool-overlap is the real balance axis

Median throughput vs committer_count, by pool-distribution (the hh7b thesis):

| scenario | overlap | locks/trx | cc=1 | cc=2 | cc=4 | cc=8 | scales? |
|---|---|--:|--:|--:|--:|--:|---|
| s5 single hot pool | max (batched→0) | ~0.01 | 3,018 | 6,034 | 9,345 | — | **yes, ~linear** |
| s6 disjoint stripes | none | ~0.45 | — | 5,524 | 9,192 | 10,753 | **yes** |
| s10 Pareto intertwined | high | ~4.6–8 | ~1,100 | — | ~1,000 | ~900 | **no — flat/↓** |

- **Disjoint (s6)** and **batched single-hot-pool (s5)** scale with committers: s5 batches the
  hot pool so hard (locks/trx → ~0.00) that it becomes commit-/row-lock-bound and parallelizes;
  s6 is disjoint so committers never contend.
- **Intertwined (s10)** does **not** scale — throughput is pool-lock-capped (~1.0k, locks/trx
  ≈ 4.6 even after batching: ~13.5 pools per complex receipt). cc=1 ≈ cc=4 ≥ cc=8: extra
  committers add only `FOR UPDATE` contention for **zero gain (slight loss at cc=8)**. This
  reproduces the acct-235v "flat across cc" result on the current build.

**Hypothesis verdict (user, 2026-06-03):** "fewer committers on small-pool/intertwined work"
is **confirmed** — more committers are pure waste there. The companion clause "larger batches
win" is **not** supported as a *throughput* lever: bigger windows grow cg without moving the
lock-bound ceiling. Larger batches help only where work is **commit/fsync-bound** (the batched
hot pool), not lock-bound (intertwined). So the right pairing is: *intertwined → few
committers* (and the window is then free to coalesce at low arrival); *disjoint → many
committers* (where 235v already showed 958→1234→1791 cc2/4/8).

## 4. No runaway / failure mode

Across the entire sweep (incl. 50 ms windows, cg up to ~1000) and the escalation to
8000-submission groups: `arena_outstanding = 0` (no leak), `arena_bump` peaked at 46 MB of
128 (intertwined s10's multi-line payloads — the largest), `dropped_submissions = 0` (no
staging backpressure). Document atomicity is preserved by construction (packing chunks whole
submissions; the p1al atomicity tests still pin this). The feared "unbounded group under an
arrival spike" **does not occur** — `router_window_size` caps it and the arena absorbs the
spill. A safety ceiling is *worth keeping* (for latency, per §1), and we already have one.

## 5. A balance model

Group size with the doc cap removed:  `cg ≈ min( router_window_size , backlog_depth_at_tick )`,
where `backlog_depth ≈ arrival_rate × max(window, drain_interval)`. Two regimes:

- **Backlog-bound (high arrival: s2/s5/s6/s7):** backlog ≥ `router_window_size`, so
  `cg → router_window_size` and the window is **irrelevant** to formation. Throughput is set
  by `committer_count × achievable_parallelism(pool-overlap)`, not by the window.
- **Window-bound (low arrival: s10 cc=1):** backlog < `router_window_size`, so the window
  trades **ack-latency for coalescing**. Here it is a real (latency) dial, not a throughput one.

The "balanced window matched to committers / workload / IO" framing resolves to: **the window
is a latency knob; the throughput balance is `committer_count` matched to pool-overlap.**
Provision committers *up to the point the bottleneck moves to per-pool lock serialization* —
that point is high for disjoint, ~1 for intertwined. `router_window_size` is the protective
group-size ceiling and sits at its knee (1000).

## 6. Recommendation → Phase 1/2 go-no-go (design choices, NOT pre-decided)

Time-window-only formation is **safe to adopt** — `router_window_size` is the natural ceiling,
no runaway — but it is **not the throughput win** the issue premised. That reframes Phase 2.
Three choices to resolve before any implementation:

1. **`batch_size_max`: drop, or keep as a high non-binding safety bound?** It is *already*
   redundant as a hard cap (`router_window_size` is the effective ceiling). My read: keep it
   only as a documented high safety bound (or fold the concept into `router_window_size`); the
   real ceiling knob is `router_window_size`, which should stay ~1000.
2. **Is a closed-loop *window* controller still worth building?** My read: **no** for
   throughput — the window is not the throughput lever; on high-arrival workloads it is inert,
   and on low-arrival it is a latency dial a static value handles. A window controller would
   tune the wrong variable.
3. **Pivot the "balance" objective to committer provisioning vs pool-overlap?** The lever that
   *does* move throughput is `committer_count` matched to workload overlap (few for intertwined,
   many for disjoint). But `committer_count` is **restart-only** today. The candidate Phase 2
   directions become: (a) a per-deployment committer_count calibration from measured pool-overlap;
   (b) making `committer_count` live-tunable so it can track workload; (c) leave both as-is and
   simply *document* the tuning rule. This is a different (and arguably more valuable) project
   than window-sizing — needs an explicit go/no-go.

Caller-ack tail latency (5–25 s on 1000-caller open-loop scenarios; ~200 ms on 16-caller s2)
is unchanged by any formation knob — it is the synchronous-ack backlog, the **shape-L
pseudo-sync** territory (acct-yjn / acct-c4p), orthogonal to this issue.

## 7. Coverage / caveats

- Windows {0, 500, 2000, 10000, 50000} µs (fine on s2/s7; coarse {0,2000,10000,50000} on the
  committer-axis scenarios s5/s6/s10 to bound runtime). committer_count {1,2,4} (s5), {2,4,8}
  (s6), {1,4,8} (s10), 4 (s2/s7). rwsize escalation {1000,2000,4000,8000} on s6 cc4.
- All absolutes host-noise-contended (Chrome up); conclusions are within-session structural
  ratios. s10 cc=1 throughput is the noisiest (one load-spike outlier of 45 trx/s discarded by
  the median). The s10 committer axis was re-run after a `DROP DATABASE WITH (FORCE)` vs
  committer-reconnect race truncated the first attempt (harness `clean_seed` hardened with a
  terminate+retry loop).
- s8/s9 (deep complex), method-mix variation, and `staging_queue_size`/`committer_queue_size`
  escalation not swept — `router_window_size` is the demonstrated binding ceiling, the others
  sit above it.

## Artifacts

- Sweep CSVs: `hh7b_window_{s2,s5,s6,s7,s10}.csv`; escalation: `hh7b_rwsize_probe_s6.csv`;
  log `hh7b_window.log`.
- Runners: `bench/sweep-hh7b.sh` (driver), `bench/sweep-hh7b-window.sh` (engine),
  `bench/probe-hh7b-rwsize.sh`.
- Instrumentation: `ledger_routed_c_router_max_group_size()` +
  `ledger_routed_c_router_submission_histogram()` getters (`ledger-routed-c/src/lib.rs`).
