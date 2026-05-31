#!/usr/bin/env python3
"""Parse a `<output>.sampler.txt` (written by the harness when
LEDGER_V3_1_PRINT_SAMPLER=1) into the named committer wait-event diagnostic
(acct-czz4 batch-diag STEP 1).

The harness's SamplerReport::format() emits three sections; we only read the
COMMITTER-ONLY one:

    --- committer-only wait_event histogram (no state filter; NULL→Running) ---
    wait_event_type        wait_event                       sum_samples
    LWLock                 WALWrite                                 1234
    Extension              Extension                                5678   <- idle
    Running                Running                                  9012   <- on-CPU
    ...
    committer summary: total=.. idle=.. busy=.. (.. % util) | of busy: lock=..% running=..% lwlock=..% io=..%

Output (one space-separated line, for shell `read`):

    busy_frac lock_ob running_ob lwlock_ob io_ob top_type top_event top_samples

where the of-busy fractions come from the `committer summary:` line and
top_{type,event,samples} is the single dominant *wait* event — the histogram's
largest bucket EXCLUDING the ('Extension','Extension') idle bucket and the
('Running','Running') on-CPU bucket. That named bucket (e.g. LWLock/WALWrite,
Lock/transactionid) is the mechanism the batch-size sweep is trying to identify:
WHAT the committer blocks on, by name, not just by wait_event_type.

If the committer section is absent or has no non-idle/non-running wait, top_*
fields are emitted as `none none 0` and fractions as best-effort (or 0.0).

Usage: parse-committer-sampler.py <path/to/file.sampler.txt>
"""
import re
import sys

ANCHOR = "--- committer-only wait_event histogram"
SUMMARY = "committer summary:"
HEADER_FIRST_TOKEN = "wait_event_type"
IDLE = ("Extension", "Extension")
RUNNING = ("Running", "Running")


def parse(path):
    busy = lock_ob = run_ob = lw_ob = io_ob = 0.0
    hist = []  # (wait_event_type, wait_event, samples)
    in_section = False
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError:
        return None

    for ln in lines:
        if ANCHOR in ln:
            in_section = True
            continue
        if ln.startswith(SUMMARY):
            # of-busy fractions are percentages; convert to 0..1 fractions.
            m = re.search(
                r"\(([\d.]+)% util\).*lock=([\d.]+)% running=([\d.]+)% "
                r"lwlock=([\d.]+)% io=([\d.]+)%",
                ln,
            )
            if m:
                busy = float(m.group(1)) / 100.0
                lock_ob = float(m.group(2)) / 100.0
                run_ob = float(m.group(3)) / 100.0
                lw_ob = float(m.group(4)) / 100.0
                io_ob = float(m.group(5)) / 100.0
            in_section = False
            continue
        if not in_section:
            continue
        toks = ln.split()
        if len(toks) != 3:
            continue
        if toks[0] == HEADER_FIRST_TOKEN:  # the column header row
            continue
        try:
            n = int(toks[2])
        except ValueError:
            continue
        hist.append((toks[0], toks[1], n))

    # Dominant named WAIT event: largest bucket excluding idle + on-CPU.
    waits = [h for h in hist if (h[0], h[1]) not in (IDLE, RUNNING)]
    if waits:
        waits.sort(key=lambda h: h[2], reverse=True)
        top_type, top_event, top_n = waits[0]
    else:
        top_type, top_event, top_n = "none", "none", 0

    return (busy, lock_ob, run_ob, lw_ob, io_ob, top_type, top_event, top_n)


def main():
    if len(sys.argv) < 2:
        print("0.0 0.0 0.0 0.0 0.0 none none 0")
        return
    res = parse(sys.argv[1])
    if res is None:
        print("0.0 0.0 0.0 0.0 0.0 none none 0")
        return
    busy, lock_ob, run_ob, lw_ob, io_ob, tt, te, tn = res
    print(f"{busy:.4f} {lock_ob:.4f} {run_ob:.4f} {lw_ob:.4f} {io_ob:.4f} {tt} {te} {tn}")


if __name__ == "__main__":
    main()
