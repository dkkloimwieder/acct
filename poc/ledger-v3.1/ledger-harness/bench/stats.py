#!/usr/bin/env python3
# acct-0at4.10.4 (D) — hand-rolled statistics for the multi-rep crossover bake.
#
# No numpy/scipy/matplotlib (the project avoids heavy deps; matches the q()
# quantile at profile-aggregate.py / batch-aggregate.py and linreg() at
# drift-analyze.py). Provides three primitives + a CSV aggregator:
#
#   bootstrap_ci(samples, stat, iters, alpha, seed) -> (point, lo, hi)
#       resample-with-replacement CI; FIXED PRNG seed for reproducibility.
#   mann_whitney_u(a, b) -> (U, p_two_sided)
#       rank-sum U + tie-corrected normal-approx two-sided p (math.erf CDF).
#   steady_state_window(series, cov_thresh, win) -> discard-until index | None
#       first index whose remainder's rolling coefficient-of-variation stays
#       below thresh — the principled replacement for the crude fixed trim at
#       run-sustained-5min.sh:187 (rates[3:] + drop tail <10% median).
#
# CLI:
#   stats.py selftest              run inline fixtures, assert, exit nonzero on fail
#   stats.py aggregate <csv> <md>  median +/- bootstrap CI per (scenario,mode) +
#                                  Mann-Whitney routed-vs-direct p per scenario
#
# The aggregate CSV (written by crossover-stats.sh) has header:
#   scenario,mode,seed,rep,throughput_trx_s,errors,duration_s
import math
import random
import sys

# ── quantile (same linear-interpolated, no-numpy form as the sibling scripts) ──
def q(xs, frac):
    if not xs:
        return float("nan")
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    pos = frac * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def median(xs):
    return q(xs, 0.5)


def mean(xs):
    return sum(xs) / len(xs) if xs else float("nan")


def stdev(xs):
    n = len(xs)
    if n < 2:
        return 0.0
    m = mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (n - 1)) ** 0.5


# ── bootstrap confidence interval ──────────────────────────────────────────────
def bootstrap_ci(samples, stat=median, iters=2000, alpha=0.05, seed=0xB0075):
    """Percentile bootstrap CI for `stat` over `samples`.

    Resamples WITH REPLACEMENT `iters` times under a FIXED PRNG (so the band is
    reproducible run-to-run), returns (point, lo, hi) where point = stat(samples)
    and [lo, hi] is the central (1-alpha) percentile interval of the bootstrap
    distribution. With <2 samples the CI collapses to the point (degenerate).
    """
    xs = list(samples)
    point = stat(xs)
    if len(xs) < 2:
        return point, point, point
    rng = random.Random(seed)
    n = len(xs)
    boots = []
    for _ in range(iters):
        resample = [xs[rng.randrange(n)] for _ in range(n)]
        boots.append(stat(resample))
    boots.sort()
    lo = q(boots, alpha / 2.0)
    hi = q(boots, 1.0 - alpha / 2.0)
    return point, lo, hi


# ── Mann-Whitney U (rank-sum), tie-corrected normal-approx two-sided p ─────────
def _normal_cdf(z):
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def mann_whitney_u(a, b, continuity=True):
    """Two-sided Mann-Whitney U test via the tie-corrected normal approximation.

    Returns (U, p). U is min(U_a, U_b). p is the two-sided normal-approx tail
    with a tie correction on the variance and an optional continuity correction.
    NOTE: normal-approx — for the small n (~5/group) of the crossover bake it is
    a rough guide, not an exact tail; treat p as directional evidence alongside
    the non-overlap of the bootstrap CIs, which is the primary crossover signal.
    """
    na, nb = len(a), len(b)
    if na == 0 or nb == 0:
        return float("nan"), float("nan")
    combined = [(v, 0) for v in a] + [(v, 1) for v in b]
    combined.sort(key=lambda t: t[0])
    n = na + nb
    # average ranks for ties
    ranks = [0.0] * n
    tie_terms = 0.0
    i = 0
    while i < n:
        j = i
        while j + 1 < n and combined[j + 1][0] == combined[i][0]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1.0  # ranks are 1-based
        for k in range(i, j + 1):
            ranks[k] = avg_rank
        t = j - i + 1
        if t > 1:
            tie_terms += t ** 3 - t
        i = j + 1
    r_a = sum(ranks[k] for k in range(n) if combined[k][1] == 0)
    u_a = r_a - na * (na + 1) / 2.0
    u_b = na * nb - u_a
    u = min(u_a, u_b)
    mu = na * nb / 2.0
    # tie-corrected variance
    var = (na * nb / 12.0) * ((n + 1) - tie_terms / (n * (n - 1))) if n > 1 else 0.0
    if var <= 0:
        return u, 1.0
    sigma = math.sqrt(var)
    cc = 0.5 if continuity else 0.0
    z = (abs(u - mu) - cc) / sigma
    if z < 0:
        z = 0.0
    p = 2.0 * (1.0 - _normal_cdf(z))
    return u, max(0.0, min(1.0, p))


# ── steady-state window (rolling-CoV discard-until) ────────────────────────────
def _cov(sl):
    m = mean(sl)
    if m == 0:
        return float("inf")
    return stdev(sl) / abs(m)


def steady_state_window(series, cov_thresh=0.05, win=None):
    """First index from which the series is 'steady', or None if it never is.

    Slides a window of `win` samples; the run is steady from index i when EVERY
    window starting at i or later has coefficient-of-variation (std/mean) below
    `cov_thresh`. Returns that i (discard series[:i] as warmup/ramp). Returns 0
    for an already-flat series, None if it never settles. Principled replacement
    for the fixed rates[3:] + tail-drop heuristic — the discard point is derived
    from the data's own variability instead of a hard-coded offset.
    """
    n = len(series)
    if n == 0:
        return None
    if win is None:
        win = max(3, n // 10)
    win = min(win, n)
    last_start = n - win
    for i in range(0, last_start + 1):
        if all(_cov(series[j:j + win]) < cov_thresh for j in range(i, last_start + 1)):
            return i
    return None


# ── CSV aggregator ─────────────────────────────────────────────────────────────
def _fnum(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def aggregate(csv_path, md_path, meta=""):
    rows = []
    with open(csv_path) as f:
        header = f.readline().strip().split(",")
        idx = {name: k for k, name in enumerate(header)}
        need = ("scenario", "mode", "throughput_trx_s")
        for miss in need:
            if miss not in idx:
                raise SystemExit(f"[stats] CSV missing required column {miss!r}: {header}")
        for line in f:
            parts = line.rstrip("\n").split(",")
            if len(parts) < len(header):
                continue
            thr = _fnum(parts[idx["throughput_trx_s"]])
            if thr is None:
                continue
            rows.append({
                "scenario": parts[idx["scenario"]],
                "mode": parts[idx["mode"]],
                "seed": parts[idx["seed"]] if "seed" in idx else "",
                "thr": thr,
                "errors": _fnum(parts[idx["errors"]]) if "errors" in idx else None,
            })

    # group scenario -> mode -> [thr]
    scen_order, cells = [], {}
    for r in rows:
        s, m = r["scenario"], r["mode"]
        if s not in cells:
            cells[s] = {}
            scen_order.append(s)
        cells[s].setdefault(m, []).append(r["thr"])

    out = ["# ledger-v3.1 crossover re-measurement — statistics discipline (acct-0at4.10.4)\n"]
    if meta:
        out.append(f"_`{meta}`_\n")
    out.append(
        "Each cell is **N independent reps** at distinct `--seed` values (so the "
        "workload streams differ rep-to-rep), run in **shuffled order** under the "
        "`wait_for_quiet_host` gate. Point = median of the reps; band = **percentile "
        "bootstrap 95% CI** (2000 resamples, fixed PRNG). The production decision "
        "consumes the band, not a lone number.\n")

    # per-cell table
    out.append("\n## Per-cell throughput (median ± bootstrap 95% CI)\n")
    out.append("| scenario | mode | n | median trx/s | 95% CI | min | max |")
    out.append("|---|---|--:|--:|--:|--:|--:|")
    for s in scen_order:
        for m in sorted(cells[s]):
            xs = cells[s][m]
            pt, lo, hi = bootstrap_ci(xs)
            out.append(f"| {s} | {m} | {len(xs)} | {pt:.1f} | "
                       f"[{lo:.1f}, {hi:.1f}] | {min(xs):.1f} | {max(xs):.1f} |")

    # per-scenario crossover verdict (routed vs direct-per-call)
    out.append("\n## Crossover verdict — routed vs direct-per-call\n")
    out.append("| scenario | direct med | routed med | ratio r/d | MWU p (2-sided) | CIs disjoint? | verdict |")
    out.append("|---|--:|--:|--:|--:|:--:|---|")
    summary = []
    for s in scen_order:
        d = cells[s].get("direct-per-call")
        r = cells[s].get("routed")
        if not d or not r:
            continue
        dm, dlo, dhi = bootstrap_ci(d)
        rm, rlo, rhi = bootstrap_ci(r)
        _, p = mann_whitney_u(d, r)
        ratio = rm / dm if dm else float("nan")
        disjoint = (rlo > dhi) or (dlo > rhi)
        if not disjoint:
            # Overlapping bootstrap CIs — the reps cannot distinguish the modes;
            # report the tie rather than over-claim a winner off the median gap.
            winner = "indistinguishable"
            verdict = "**indistinguishable** (CIs overlap)"
        else:
            winner = "routed" if rm > dm else "direct"
            verdict = f"**{winner}** (CIs separated)"
        out.append(f"| {s} | {dm:.1f} | {rm:.1f} | {ratio:.2f}× | {p:.4f} | "
                   f"{'yes' if disjoint else 'no'} | {verdict} |")
        summary.append((s, winner, ratio, disjoint))

    out.append("\n## Stated steady-state rule\n")
    out.append(
        "- **Per-rep throughput** (`throughput_trx_per_sec`) is measured over the "
        "harness's post-barrier window: every caller rendezvous at a start barrier "
        "before the timer starts, so intra-run connection/warmup ramp is excluded "
        "by construction. Each rep is therefore one steady-state sample; the sample "
        "unit for the CI is the **rep**, not a sub-run interval.\n"
        "- **Time-series consumers** (sustained/drift per-interval rate series) use "
        "`steady_state_window(cov_thresh=0.05)`: discard the leading samples until "
        "the rolling coefficient-of-variation stays below 5%, deriving the warmup "
        "cut from the data instead of the fixed `rates[3:]` + tail-drop heuristic.\n")

    with open(md_path, "w") as f:
        f.write("\n".join(out) + "\n")

    # one-line stdout summary
    verdicts = ", ".join(
        f"{s}:{w}{'*' if dj else ''}({rt:.2f}×)" for (s, w, rt, dj) in summary)
    print(f"[stats] {len(rows)} reps, {len(scen_order)} scenarios -> {md_path}")
    print(f"  crossover: {verdicts}   (* = bootstrap CIs disjoint)")


# ── inline self-test ───────────────────────────────────────────────────────────
def _selftest():
    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("  ok  " if cond else " FAIL ") + msg)
        ok = ok and cond

    # bootstrap: point == median, band brackets it, tighter for low-variance data
    xs = [100, 102, 98, 101, 99, 103, 97]
    pt, lo, hi = bootstrap_ci(xs, iters=3000)
    check(abs(pt - median(xs)) < 1e-9, f"bootstrap point == median ({pt:.1f})")
    check(lo <= pt <= hi, f"bootstrap CI brackets point [{lo:.1f},{hi:.1f}]")
    # reproducibility: same seed -> identical band
    a1 = bootstrap_ci(xs, seed=42)
    a2 = bootstrap_ci(xs, seed=42)
    check(a1 == a2, "bootstrap fixed-seed reproducible")

    # MWU: well-separated -> tiny p; identical -> large p
    _, p_sep = mann_whitney_u([1, 2, 3, 4, 5], [100, 101, 102, 103, 104])
    check(p_sep < 0.05, f"MWU separated samples p<0.05 (p={p_sep:.4f})")
    _, p_same = mann_whitney_u([10, 11, 12, 13, 14], [10, 11, 12, 13, 14])
    check(p_same > 0.5, f"MWU identical samples p>0.5 (p={p_same:.4f})")
    # directional: overlapping-but-shifted lands between
    _, p_mid = mann_whitney_u([10, 12, 14, 16, 18], [11, 13, 15, 17, 40])
    check(0.0 <= p_mid <= 1.0, f"MWU overlapping in [0,1] (p={p_mid:.4f})")

    # steady-state: flat -> 0, ramp-then-flat -> >0, pure noise -> None
    check(steady_state_window([50.0] * 20) == 0, "steady flat series -> 0")
    ramp = [10, 20, 30, 40] + [100.0 + (i % 2) for i in range(20)]
    idx = steady_state_window(ramp, cov_thresh=0.05)
    check(idx is not None and idx >= 4, f"steady ramp-then-flat discards ramp (idx={idx})")
    noisy = [10, 90, 15, 80, 20, 70, 5, 95, 30, 60]
    check(steady_state_window(noisy, cov_thresh=0.02) is None, "steady pure-noise -> None")

    print("\nSELFTEST: " + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


def main(argv):
    if len(argv) < 2 or argv[1] == "selftest":
        return _selftest()
    if argv[1] == "aggregate":
        if len(argv) < 4:
            raise SystemExit("usage: stats.py aggregate <csv> <out.md> [meta]")
        aggregate(argv[2], argv[3], argv[4] if len(argv) > 4 else "")
        return 0
    raise SystemExit(f"unknown subcommand {argv[1]!r} (selftest | aggregate)")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
