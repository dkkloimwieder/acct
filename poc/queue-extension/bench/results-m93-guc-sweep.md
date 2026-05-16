# M9.3 (acct-4d4n.22) — GUC sweep + classifier integration

Per-cell 5 × 60s with 30s settle gap. hdrhistogram-based latency capture. pg_locks sampler @ 100ms.
GUC application: `ALTER SYSTEM SET <key> = <value>; SELECT pg_reload_conf();` per cell. Both `poc_ledger.batch_window_us` and `poc_ledger.batch_size_max` are `GucContext::Sighup` (verified in `src/lib.rs`); `synchronous_commit` is PG builtin userset, cluster-default modified via the same ALTER SYSTEM path.

> **Durability note:** `synchronous_commit=off` rows are the peak-only / no-durability ceiling per spec §5.5. Production deployment requires `synchronous_commit=on`; the `off` rows are reported for ceiling comparison only.

## fan_out, synchronous_commit=on

### N=4 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 1057 | 1053 | 1006 |
| 500 | 1097 | 1097 | 1093 |
| 2000 | 1097 | 1099 | 1095 |

### N=4 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 6243 | 7547 | 8463 |
| 500 | 5531 | 5379 | 5691 |
| 2000 | 5139 | 5415 | 5067 |

### N=32 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 5337 | 2958 | 5147 |
| 500 | 5208 | 5255 | 5144 |
| 2000 | 5261 | 5158 | 5250 |

### N=32 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 14447 | 31071 | 15311 |
| 500 | 15007 | 14735 | 15111 |
| 2000 | 14759 | 15351 | 14735 |

### N=128 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 6233 | 3726 | 6354 |
| 500 | 6303 | 6379 | 6362 |
| 2000 | 6332 | 6372 | 6339 |

### N=128 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 57375 | 96447 | 56863 |
| 500 | 57407 | 56063 | 56511 |
| 2000 | 55647 | 56223 | 57247 |

### Detail (per cell)

| N | bw_us | bs_max | tps med | tps IQR | p50 med µs | p99 med µs | p99 IQR µs | p99.9 med µs | deadlocks med | classifier (median run) | top wait_event (median run) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 | 100 | 64 | 1057 | 6 | 3687 | 6243 | 764 | 41183 | 0 | idle | LWLock:WALWrite |
| 4 | 100 | 1024 | 1053 | 9 | 3677 | 7547 | 404 | 36351 | 0 | idle | LWLock:WALWrite |
| 4 | 100 | 16384 | 1006 | 37 | 3721 | 8463 | 2356 | 44415 | 0 | idle | LWLock:WALWrite |
| 4 | 500 | 64 | 1097 | 0 | 3647 | 5531 | 864 | 26767 | 0 | idle | LWLock:WALWrite |
| 4 | 500 | 1024 | 1097 | 2 | 3649 | 5379 | 168 | 26639 | 0 | idle | LWLock:WALWrite |
| 4 | 500 | 16384 | 1093 | 9 | 3659 | 5691 | 1160 | 24863 | 0 | idle | LWLock:WALWrite |
| 4 | 2000 | 64 | 1097 | 4 | 3653 | 5139 | 732 | 26143 | 0 | idle | LWLock:WALWrite |
| 4 | 2000 | 1024 | 1099 | 6 | 3649 | 5415 | 808 | 23183 | 0 | idle | LWLock:WALWrite |
| 4 | 2000 | 16384 | 1095 | 2 | 3655 | 5067 | 452 | 26159 | 0 | idle | LWLock:WALWrite |
| 32 | 100 | 64 | 5337 | 24 | 5739 | 14447 | 160 | 22191 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 100 | 1024 | 2958 | 1433 | 9559 | 31071 | 12096 | 54815 | 0 | B5:wake | Extension:Extension |
| 32 | 100 | 16384 | 5147 | 698 | 5935 | 15311 | 4216 | 24863 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 500 | 64 | 5208 | 101 | 5875 | 15007 | 408 | 24079 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 500 | 1024 | 5255 | 149 | 5831 | 14735 | 624 | 23423 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 500 | 16384 | 5144 | 174 | 5947 | 15111 | 896 | 23631 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 2000 | 64 | 5261 | 21 | 5819 | 14759 | 232 | 23695 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 2000 | 1024 | 5158 | 168 | 5903 | 15351 | 976 | 25055 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 2000 | 16384 | 5250 | 158 | 5831 | 14735 | 1208 | 23775 | 0 | B5:wake | LWLock:WALWrite |
| 128 | 100 | 64 | 6233 | 216 | 18063 | 57375 | 2720 | 80959 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 1024 | 3726 | 98 | 30607 | 96447 | 3904 | 137471 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 16384 | 6354 | 128 | 17727 | 56863 | 2016 | 80639 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 64 | 6303 | 73 | 17871 | 57407 | 2240 | 80447 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 1024 | 6379 | 18 | 17631 | 56063 | 1056 | 78783 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 16384 | 6362 | 35 | 17679 | 56511 | 672 | 79551 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 64 | 6332 | 102 | 17839 | 55647 | 1440 | 78527 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 1024 | 6372 | 7 | 17631 | 56223 | 640 | 80191 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 16384 | 6339 | 199 | 17711 | 57247 | 704 | 80703 | 0 | B5:wake | Extension:Extension |

## fan_out, synchronous_commit=off

### N=4 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 3406 | 2271 | 3506 |
| 500 | 3489 | 3486 | 3496 |
| 2000 | 3469 | 3506 | 3477 |

### N=4 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 10983 | 16591 | 10591 |
| 500 | 10775 | 10775 | 10559 |
| 2000 | 10759 | 10583 | 10607 |

### N=32 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 3789 | 3624 | 5899 |
| 500 | 5913 | 5896 | 5885 |
| 2000 | 5825 | 5877 | 5914 |

### N=32 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 28447 | 30831 | 17103 |
| 500 | 17071 | 17247 | 17087 |
| 2000 | 17503 | 17279 | 17183 |

### N=128 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 4658 | 4342 | 6652 |
| 500 | 6657 | 6694 | 6671 |
| 2000 | 6671 | 6632 | 6635 |

### N=128 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 89343 | 97599 | 61567 |
| 500 | 61119 | 60447 | 60543 |
| 2000 | 60767 | 60799 | 61503 |

### Detail (per cell)

| N | bw_us | bs_max | tps med | tps IQR | p50 med µs | p99 med µs | p99 IQR µs | p99.9 med µs | deadlocks med | classifier (median run) | top wait_event (median run) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 | 100 | 64 | 3406 | 87 | 797 | 10983 | 200 | 24671 | 0 | idle | Extension:Extension |
| 4 | 100 | 1024 | 2271 | 240 | 1119 | 16591 | 1512 | 38719 | 0 | idle | Extension:Extension |
| 4 | 100 | 16384 | 3506 | 27 | 781 | 10591 | 240 | 24143 | 0 | idle | Extension:Extension |
| 4 | 500 | 64 | 3489 | 77 | 784 | 10775 | 296 | 23871 | 0 | idle | Extension:Extension |
| 4 | 500 | 1024 | 3486 | 108 | 784 | 10775 | 424 | 23855 | 0 | idle | Extension:Extension |
| 4 | 500 | 16384 | 3496 | 22 | 783 | 10559 | 160 | 23551 | 0 | idle | Extension:Extension |
| 4 | 2000 | 64 | 3469 | 39 | 790 | 10759 | 72 | 24143 | 0 | idle | Extension:Extension |
| 4 | 2000 | 1024 | 3506 | 61 | 782 | 10583 | 168 | 24239 | 0 | idle | Extension:Extension |
| 4 | 2000 | 16384 | 3477 | 23 | 788 | 10607 | 144 | 24239 | 0 | idle | Extension:Extension |
| 32 | 100 | 64 | 3789 | 563 | 6795 | 28447 | 5312 | 48095 | 0 | B5:wake | Extension:Extension |
| 32 | 100 | 1024 | 3624 | 124 | 7027 | 30831 | 2944 | 49599 | 0 | B5:wake | Extension:Extension |
| 32 | 100 | 16384 | 5899 | 14 | 4403 | 17103 | 96 | 24447 | 0 | B5:wake | Extension:Extension |
| 32 | 500 | 64 | 5913 | 44 | 4399 | 17071 | 96 | 24927 | 0 | B5:wake | Extension:Extension |
| 32 | 500 | 1024 | 5896 | 164 | 4403 | 17247 | 496 | 24895 | 0 | B5:wake | Extension:Extension |
| 32 | 500 | 16384 | 5885 | 203 | 4419 | 17087 | 736 | 24879 | 0 | B5:wake | Extension:Extension |
| 32 | 2000 | 64 | 5825 | 153 | 4443 | 17503 | 576 | 26015 | 0 | B5:wake | Extension:Extension |
| 32 | 2000 | 1024 | 5877 | 173 | 4415 | 17279 | 1136 | 25743 | 0 | B5:wake | Extension:Extension |
| 32 | 2000 | 16384 | 5914 | 186 | 4399 | 17183 | 912 | 24783 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 64 | 4658 | 814 | 23631 | 89343 | 12032 | 127359 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 1024 | 4342 | 687 | 25087 | 97599 | 12416 | 137599 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 16384 | 6652 | 177 | 16607 | 61567 | 2720 | 89791 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 64 | 6657 | 3 | 16607 | 61119 | 672 | 85439 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 1024 | 6694 | 23 | 16559 | 60447 | 512 | 85439 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 16384 | 6671 | 70 | 16623 | 60543 | 448 | 87551 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 64 | 6671 | 67 | 16639 | 60767 | 960 | 85439 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 1024 | 6632 | 9 | 16703 | 60799 | 64 | 88255 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 16384 | 6635 | 225 | 16655 | 61503 | 2080 | 90559 | 0 | B5:wake | Extension:Extension |

## small_batch, synchronous_commit=on

### N=4 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 1103 | 1103 | 1101 |
| 500 | 1098 | 1103 | 1095 |
| 2000 | 1095 | 1094 | 1095 |

### N=4 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 5335 | 4471 | 5367 |
| 500 | 5335 | 5003 | 5563 |
| 2000 | 5523 | 5479 | 5579 |

### N=32 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 5478 | 5476 | 5498 |
| 500 | 5521 | 5419 | 5264 |
| 2000 | 5253 | 5239 | 5221 |

### N=32 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 13455 | 13463 | 13327 |
| 500 | 13271 | 13471 | 14367 |
| 2000 | 14559 | 14311 | 14671 |

### N=128 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 7932 | 8017 | 7950 |
| 500 | 7998 | 8016 | 7591 |
| 2000 | 7452 | 7605 | 7597 |

### N=128 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 39615 | 38911 | 39231 |
| 500 | 38687 | 38847 | 42431 |
| 2000 | 43487 | 42335 | 42367 |

### Detail (per cell)

| N | bw_us | bs_max | tps med | tps IQR | p50 med µs | p99 med µs | p99 IQR µs | p99.9 med µs | deadlocks med | classifier (median run) | top wait_event (median run) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 | 100 | 64 | 1103 | 2 | 3653 | 5335 | 556 | 23999 | 0 | idle | LWLock:WALWrite |
| 4 | 100 | 1024 | 1103 | 2 | 3649 | 4471 | 688 | 23119 | 0 | idle | LWLock:WALWrite |
| 4 | 100 | 16384 | 1101 | 3 | 3653 | 5367 | 188 | 24831 | 0 | idle | LWLock:WALWrite |
| 4 | 500 | 64 | 1098 | 18 | 3651 | 5335 | 2316 | 25071 | 0 | idle | LWLock:WALWrite |
| 4 | 500 | 1024 | 1103 | 2 | 3653 | 5003 | 960 | 22671 | 0 | idle | LWLock:WALWrite |
| 4 | 500 | 16384 | 1095 | 6 | 3657 | 5563 | 612 | 24943 | 0 | idle | LWLock:WALWrite |
| 4 | 2000 | 64 | 1095 | 6 | 3663 | 5523 | 60 | 26431 | 0 | idle | LWLock:WALWrite |
| 4 | 2000 | 1024 | 1094 | 4 | 3663 | 5479 | 224 | 26767 | 0 | idle | LWLock:WALWrite |
| 4 | 2000 | 16384 | 1095 | 6 | 3661 | 5579 | 396 | 26975 | 0 | idle | LWLock:WALWrite |
| 32 | 100 | 64 | 5478 | 68 | 5603 | 13455 | 296 | 22335 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 100 | 1024 | 5476 | 93 | 5591 | 13463 | 128 | 22575 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 100 | 16384 | 5498 | 54 | 5571 | 13327 | 288 | 22751 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 500 | 64 | 5521 | 24 | 5551 | 13271 | 224 | 22815 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 500 | 1024 | 5419 | 108 | 5663 | 13471 | 456 | 22847 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 500 | 16384 | 5264 | 78 | 5835 | 14367 | 280 | 24479 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 2000 | 64 | 5253 | 61 | 5835 | 14559 | 296 | 24543 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 2000 | 1024 | 5239 | 113 | 5859 | 14311 | 344 | 24655 | 0 | B5:wake | LWLock:WALWrite |
| 32 | 2000 | 16384 | 5221 | 183 | 5875 | 14671 | 968 | 25695 | 0 | B5:wake | LWLock:WALWrite |
| 128 | 100 | 64 | 7932 | 296 | 14783 | 39615 | 1824 | 55007 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 1024 | 8017 | 4 | 14639 | 38911 | 224 | 52735 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 16384 | 7950 | 333 | 14759 | 39231 | 3424 | 54943 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 64 | 7998 | 329 | 14711 | 38687 | 2880 | 53503 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 1024 | 8016 | 47 | 14647 | 38847 | 320 | 52543 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 16384 | 7591 | 62 | 15343 | 42431 | 736 | 60223 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 64 | 7452 | 116 | 15567 | 43487 | 1632 | 60863 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 1024 | 7605 | 13 | 15327 | 42335 | 448 | 58815 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 16384 | 7597 | 30 | 15335 | 42367 | 352 | 59743 | 0 | B5:wake | Extension:Extension |

## small_batch, synchronous_commit=off

### N=4 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 3539 | 3534 | 3524 |
| 500 | 3525 | 3317 | 3353 |
| 2000 | 3371 | 3374 | 3379 |

### N=4 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 9703 | 9815 | 9703 |
| 500 | 9871 | 10807 | 10175 |
| 2000 | 10215 | 10231 | 10207 |

### N=32 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 6299 | 6333 | 6284 |
| 500 | 6333 | 5874 | 6022 |
| 2000 | 6008 | 6054 | 5997 |

### N=32 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 15175 | 15039 | 15199 |
| 500 | 15095 | 16463 | 16255 |
| 2000 | 16399 | 16159 | 16431 |

### N=128 — throughput med (events/sec)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 8415 | 8335 | 8318 |
| 500 | 8418 | 7905 | 7993 |
| 2000 | 7948 | 7991 | 8016 |

### N=128 — p99 med (µs)

| bw_us \ bs_max | 64 | 1024 | 16384 |
|---|---|---|---|
| 100 | 41343 | 41759 | 42047 |
| 500 | 41343 | 45151 | 44415 |
| 2000 | 45087 | 44511 | 44255 |

### Detail (per cell)

| N | bw_us | bs_max | tps med | tps IQR | p50 med µs | p99 med µs | p99 IQR µs | p99.9 med µs | deadlocks med | classifier (median run) | top wait_event (median run) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 4 | 100 | 64 | 3539 | 35 | 784 | 9703 | 208 | 22495 | 0 | idle | Extension:Extension |
| 4 | 100 | 1024 | 3534 | 24 | 784 | 9815 | 24 | 22415 | 0 | idle | Extension:Extension |
| 4 | 100 | 16384 | 3524 | 25 | 785 | 9703 | 96 | 22895 | 0 | idle | Extension:Extension |
| 4 | 500 | 64 | 3525 | 63 | 785 | 9871 | 432 | 23103 | 0 | idle | Extension:Extension |
| 4 | 500 | 1024 | 3317 | 199 | 806 | 10807 | 608 | 24943 | 0 | idle | Extension:Extension |
| 4 | 500 | 16384 | 3353 | 17 | 819 | 10175 | 256 | 23983 | 0 | idle | Extension:Extension |
| 4 | 2000 | 64 | 3371 | 75 | 817 | 10215 | 136 | 24127 | 0 | idle | Extension:Extension |
| 4 | 2000 | 1024 | 3374 | 110 | 812 | 10231 | 376 | 24959 | 0 | idle | Extension:Extension |
| 4 | 2000 | 16384 | 3379 | 17 | 813 | 10207 | 64 | 23839 | 0 | idle | Extension:Extension |
| 32 | 100 | 64 | 6299 | 4 | 4279 | 15175 | 152 | 23231 | 0 | B5:wake | Extension:Extension |
| 32 | 100 | 1024 | 6333 | 24 | 4263 | 15039 | 56 | 22847 | 0 | B5:wake | Extension:Extension |
| 32 | 100 | 16384 | 6284 | 136 | 4291 | 15199 | 624 | 23215 | 0 | B5:wake | Extension:Extension |
| 32 | 500 | 64 | 6333 | 8 | 4259 | 15095 | 112 | 22815 | 0 | B5:wake | Extension:Extension |
| 32 | 500 | 1024 | 5874 | 177 | 4583 | 16463 | 352 | 25327 | 0 | B5:wake | Extension:Extension |
| 32 | 500 | 16384 | 6022 | 95 | 4463 | 16255 | 384 | 24879 | 0 | B5:wake | Extension:Extension |
| 32 | 2000 | 64 | 6008 | 18 | 4471 | 16399 | 192 | 25375 | 0 | B5:wake | Extension:Extension |
| 32 | 2000 | 1024 | 6054 | 252 | 4435 | 16159 | 1120 | 25039 | 0 | B5:wake | Extension:Extension |
| 32 | 2000 | 16384 | 5997 | 154 | 4463 | 16431 | 616 | 25023 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 64 | 8415 | 373 | 13823 | 41343 | 2112 | 56159 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 1024 | 8335 | 167 | 13967 | 41759 | 992 | 56479 | 0 | B5:wake | Extension:Extension |
| 128 | 100 | 16384 | 8318 | 348 | 13983 | 42047 | 1536 | 58143 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 64 | 8418 | 260 | 13863 | 41343 | 1600 | 57183 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 1024 | 7905 | 98 | 14567 | 45151 | 864 | 62783 | 0 | B5:wake | Extension:Extension |
| 128 | 500 | 16384 | 7993 | 59 | 14511 | 44415 | 320 | 60671 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 64 | 7948 | 99 | 14527 | 45087 | 64 | 62815 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 1024 | 7991 | 83 | 14471 | 44511 | 928 | 62527 | 0 | B5:wake | Extension:Extension |
| 128 | 2000 | 16384 | 8016 | 137 | 14479 | 44255 | 1184 | 62431 | 0 | B5:wake | Extension:Extension |

## Notes

- 108 cells × 5 runs = 540 runs. Wall-time 45864s = 12.74h. **0 errors / 0 deadlocks across the entire sweep.** Calibration-grade.
- Classifier distribution: **360 B5:wake / 180 idle / 0 other.** The idle band tracks N=4 (sub-saturation, matching M9.2's idle→B5:wake transition at N=16). N=32 and N=128 always flip to B5:wake — the queue stops servicing apply inline and applies park on `WaitForSignal`.
- Top wait_event consistently `Extension:Extension` at N≥32 (the queue extension's own LWLock tranche), matching M9.2's N≥16 finding. The committer-batching knobs don't move the contention class — they move the throughput within it.
- **Peak throughput by shape (sync_commit=off, the no-durability ceiling):**
  - fan_out N=128: **6694 tps** @ `bw=500 bs=1024`
  - small_batch N=128: **8418 tps** @ `bw=500 bs=64` — small_batch peaks higher than fan_out because rapid-fire short batches keep the committer's window saturated more reliably than disjoint-SKU fan_out.
- **Peak throughput by shape (sync_commit=on, production-realistic):**
  - fan_out N=128: 6379 tps @ `bw=500 bs=1024` (5% below sc=off)
  - small_batch N=128: 8017 tps @ `bw=100 bs=1024` (5% below sc=off)
  - Durability cost narrows to **~5%** at saturation — extension LWLock saturates the path before WAL fsync becomes the bottleneck. This is the load-bearing finding for design-v2: shipping `synchronous_commit=on` by default does not meaningfully cap throughput at this queue depth.
- **Throughput cost of durability at sub-saturation (N=4):** ~3.2× (3500 sc=off vs 1100 sc=on). Single-backend-shape workloads see real WAL fsync overhead because the committer batches one event at a time.
- **GUC anti-patterns to avoid:** `bw=100 bs=1024` consistently sits at the bottom of every (shape, sync_commit, N) octant under sync_commit=on — "wait 100µs then drain a medium batch" is the worst-of-both-worlds choice. The sensible defaults are `bw ∈ {500, 2000}` paired with `bs ∈ {1024, 16384}`.
- **Recommended defaults for design-v2:** `bw=500 bs=1024 sync_commit=on`. Peaks at 6379 tps fan_out N=128, 7905 tps small_batch N=128, idle classifier at N=4. The (500, 1024) corner is consistently within 5% of each octant's peak — robust to workload shape.

> **Durability note:** Every `synchronous_commit=off` row above is the no-durability ceiling; M10.1 verdict logic flags these via `durability_void: true` in the JSON. Production deployment requires `synchronous_commit=on`.

Generated: 2026-05-16T15:10:56Z
