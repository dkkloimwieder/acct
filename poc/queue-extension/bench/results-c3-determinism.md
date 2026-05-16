# M10.1 C3 determinism (acct-4d4n.23)

Two identical sequences of 200 FIFO applies (issue_id 1..=200, sku rotates 1..=5, qty deterministic 1..=50, method='fifo') driven against reset+pre-seeded state. Per-(issue_id, method_used) SUM(qty) and SUM(qty*unit_cost) compared row-by-row. Row counts compared per cost table.

| metric | run 1 | run 2 | match |
|---|---|---|---|
| cost_layers rows | 25 | 25 | ✓ |
| cost_depletions rows | 200 | 200 | ✓ |
| cost_consumptions rows | 0 | 0 | ✓ |
| distinct depletion (issue_id, method) keys | 200 | 200 | ✓ |
| SUM(qty) per key (compared row-by-row) | — | — | ✓ |
| SUM(qty*unit_cost) per key (compared row-by-row) | — | — | ✓ |

**C3 verdict: PASS** — every measured aggregate identical across runs.

What was NOT compared: committer_tx_id, layer_id, depletion_id, posted_at, *_seq. These reset per cluster epoch and may shift under non-deterministic shard-election timing — incidental to the C3 contract.
