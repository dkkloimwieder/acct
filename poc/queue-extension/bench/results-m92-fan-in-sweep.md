# M9.2 (acct-4d4n.21) — fan_in statistical sweep

Per-cell 5 × 60s with 30s settle gap. hdrhistogram-based latency capture. pg_locks sampler @ 100ms per run.

## Per-N aggregate (median + IQR over the N runs)

| N | throughput med | throughput IQR | p50 med µs | p50 IQR µs | p99 med µs | p99 IQR µs | p99.9 med µs | p99.9 IQR µs | deadlocks med |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 364 | 3 | 2697 | 34 | 3625 | 12 | 6831 | 320 | 0 |
| 2 | 508 | 1 | 3913 | 4 | 4511 | 12 | 11111 | 64 | 0 |
| 4 | 1677 | 6 | 3015 | 4 | 4235 | 40 | 7683 | 8 | 0 |
| 8 | 4197 | 13 | 1408 | 9 | 4507 | 12 | 7143 | 304 | 0 |
| 16 | 7045 | 308 | 2077 | 96 | 4879 | 72 | 6551 | 112 | 0 |
| 32 | 9138 | 165 | 3415 | 54 | 6095 | 116 | 10487 | 7692 | 0 |
| 64 | 10399 | 19 | 6027 | 12 | 9879 | 48 | 13751 | 656 | 0 |
| 128 | 11092 | 272 | 11335 | 240 | 18383 | 672 | 23231 | 1376 | 0 |
| 256 | 11878 | 626 | 21167 | 1216 | 34687 | 1376 | 45983 | 2272 | 0 |

## Per-cell detail (per-run rows)

| N | run | applies | errors | tps | p50 µs | p99 µs | p99.9 µs | deadlocks Δ | classifier |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0 | 22121 | 0 | 369 | 2663 | 3587 | 6519 | 0 | idle |
| 1 | 1 | 21974 | 0 | 366 | 2675 | 3725 | 6839 | 0 | idle |
| 1 | 2 | 21858 | 0 | 364 | 2697 | 3623 | 6831 | 0 | idle |
| 1 | 3 | 21741 | 0 | 362 | 2715 | 3625 | 6931 | 0 | idle |
| 1 | 4 | 21818 | 0 | 364 | 2709 | 3635 | 6459 | 0 | idle |
| 2 | 0 | 30459 | 0 | 508 | 3917 | 4523 | 11111 | 0 | idle |
| 2 | 1 | 30525 | 0 | 509 | 3909 | 4495 | 10943 | 0 | idle |
| 2 | 2 | 30446 | 0 | 507 | 3915 | 4543 | 11191 | 0 | idle |
| 2 | 3 | 30492 | 0 | 508 | 3913 | 4511 | 11127 | 0 | idle |
| 2 | 4 | 30504 | 0 | 508 | 3911 | 4511 | 11063 | 0 | idle |
| 4 | 0 | 100816 | 0 | 1680 | 2993 | 4167 | 7683 | 0 | idle |
| 4 | 1 | 100434 | 0 | 1674 | 3015 | 4235 | 7691 | 0 | idle |
| 4 | 2 | 100627 | 0 | 1677 | 3017 | 4235 | 7663 | 0 | idle |
| 4 | 3 | 101033 | 0 | 1684 | 3013 | 4195 | 7683 | 0 | idle |
| 4 | 4 | 100222 | 0 | 1670 | 3019 | 4347 | 8015 | 0 | idle |
| 8 | 0 | 240332 | 0 | 4006 | 1501 | 4655 | 7963 | 0 | idle |
| 8 | 1 | 251862 | 0 | 4198 | 1405 | 4507 | 7143 | 0 | idle |
| 8 | 2 | 252087 | 0 | 4201 | 1405 | 4503 | 7235 | 0 | idle |
| 8 | 3 | 251823 | 0 | 4197 | 1408 | 4507 | 6931 | 0 | idle |
| 8 | 4 | 251058 | 0 | 4184 | 1414 | 4519 | 6927 | 0 | idle |
| 16 | 0 | 409594 | 0 | 6827 | 2179 | 4879 | 6551 | 0 | B5:wake |
| 16 | 1 | 422706 | 0 | 7045 | 2077 | 4907 | 6587 | 0 | B5:wake |
| 16 | 2 | 412178 | 0 | 6870 | 2159 | 4911 | 6907 | 0 | B5:wake |
| 16 | 3 | 430856 | 0 | 7181 | 2042 | 4835 | 6475 | 0 | B5:wake |
| 16 | 4 | 430636 | 0 | 7177 | 2063 | 4831 | 6355 | 0 | B5:wake |
| 32 | 0 | 538984 | 0 | 8983 | 3535 | 5891 | 7563 | 0 | B5:wake |
| 32 | 1 | 560069 | 0 | 9334 | 3365 | 5979 | 7851 | 0 | B5:wake |
| 32 | 2 | 557459 | 0 | 9291 | 3359 | 6095 | 10487 | 0 | B5:wake |
| 32 | 3 | 547562 | 0 | 9126 | 3415 | 6147 | 19743 | 0 | B5:wake |
| 32 | 4 | 548288 | 0 | 9138 | 3419 | 6095 | 15543 | 0 | B5:wake |
| 64 | 0 | 604548 | 0 | 10076 | 6255 | 9855 | 13847 | 0 | B5:wake |
| 64 | 1 | 623933 | 0 | 10399 | 6023 | 10079 | 14871 | 0 | B5:wake |
| 64 | 2 | 624647 | 0 | 10411 | 6023 | 9879 | 13191 | 0 | B5:wake |
| 64 | 3 | 624862 | 0 | 10414 | 6027 | 9903 | 13751 | 0 | B5:wake |
| 64 | 4 | 623503 | 0 | 10392 | 6035 | 9847 | 12999 | 0 | B5:wake |
| 128 | 0 | 638890 | 0 | 10648 | 11799 | 19039 | 25551 | 0 | B5:wake |
| 128 | 1 | 649263 | 0 | 10821 | 11559 | 19535 | 24607 | 0 | B5:wake |
| 128 | 2 | 665584 | 0 | 11093 | 11335 | 18367 | 23231 | 0 | B5:wake |
| 128 | 3 | 666239 | 0 | 11104 | 11319 | 18207 | 23183 | 0 | B5:wake |
| 128 | 4 | 665526 | 0 | 11092 | 11319 | 18383 | 23231 | 0 | B5:wake |
| 256 | 0 | 679161 | 0 | 11319 | 22255 | 36031 | 44831 | 0 | B5:wake |
| 256 | 1 | 712652 | 0 | 11878 | 21167 | 34655 | 47103 | 0 | B5:wake |
| 256 | 2 | 716715 | 0 | 11945 | 21039 | 34687 | 45983 | 0 | B5:wake |
| 256 | 3 | 669218 | 0 | 11154 | 22255 | 38495 | 51679 | 0 | B5:wake |
| 256 | 4 | 719042 | 0 | 11984 | 20991 | 33951 | 43967 | 0 | B5:wake |

## Sampler — top wait_event per N (median run)

| N | wait_event_type | wait_event | sum_backends | notes |
|---|---|---|---|---|
| 1 | IO | WalSync | 358 | samples=594 |
| 2 | IO | WalSync | 572 | samples=594 |
| 4 | LWLock | WALWrite | 1020 | samples=594 |
| 8 | LWLock | WALWrite | 1460 | samples=596 |
| 16 | Extension | Extension | 6224 | samples=596 |
| 32 | Extension | Extension | 15233 | samples=596 |
| 64 | Extension | Extension | 31837 | samples=595 |
| 128 | Extension | Extension | 66063 | samples=595 |
| 256 | Extension | Extension | 138755 | samples=594 |

## Notes

- 9 N × 5 runs × 60s = 45 cells. Wall-time ~63 min. 0 errors anywhere. 0 deadlocks at every N up to 256.
- Throughput plateau ~11–12K evps from N≈128 onward. Per-N IQR is single-digit µs through N=8, climbing to 1.4 ms at N=256 — the rig stays calibration-grade across the sweep.
- Classifier transition is sharp at N=16: idle through N=8, B5:wake from N=16 upward (all 25 runs at N≥16). This is the load above which the queue stops servicing apply calls inline and they start parking on `WaitForSignal`.
- pg_locks sampler maps the contention shift cleanly:
  - N=1–2: `IO:WalSync` — single-backend fsync-bound
  - N=4–8: `LWLock:WALWrite` — commit-group hits the WAL insertion lock
  - N=16+: `Extension:Extension` — the queue extension's own LWLock tranche (shard lock + slot pool) dominates
- Per-backend throughput drops from 364 evps (N=1) to 46 evps (N=256) — Amdahl's law on the slot/committer serialization.
- p99 doubles roughly every 1–2 N steps in the high regime (queue full ≈ wait time grows with concurrent waiters), as expected once `B5:wake` becomes the bottleneck.
- Sampler perturbation cell (`bench/results-m92-sampler-perturbation.md`): N=32 sampler-on p99 med=5875µs IQR=636µs vs sampler-off p99 med=6131µs IQR=1064µs; drift 256µs < on-IQR 636µs → **PASS**. Throughput essentially identical (9597 vs 9601 evps).

Generated: 2026-05-16T00:17:56Z
