-- ledger-v3 migration 0005: explicit indexes per design-v3 §2.2 / §2.3.
--
-- Depends on 0003 ledger tables (trx_line) and 0004 posting tables
-- (posting_line, posting_line_dimension).
--
-- PK / UNIQUE auto-indexes from 0003 / 0004 already cover:
--   pool_state (pool_id, layer_seq) — used by FIFO ASC + LIFO DESC bulk scans
--   trx (trx_type, source_id)       — duplicate-submission probe
--   trx_line (pool_id, trx_seq)     — §13.1(a) MAX-scan strategy
--   pool (sku_id, location_id, identity_key)
--
-- The six indexes below cover join paths and lookup patterns NOT served by a
-- PK / UNIQUE:
--   trx_line_trx                  — fetch all lines for a trx
--   trx_line_pool                 — fetch all lines for a pool (snapshot warm-up,
--                                   ad-hoc audit). Distinct from the
--                                   (pool_id, trx_seq) UNIQUE because that index
--                                   is consumed by the MAX-scan; a leading-pool_id
--                                   second index keeps that scan plan stable when
--                                   pool-only filters compete.
--   trx_line_source               — partial index on source_trx_line_id for
--                                   reverse-lookup of depletion → receipt layer.
--                                   Partial WHERE keeps the index small (only
--                                   depletion rows reference upstream layers;
--                                   receipts are NULL).
--   posting_line_trx_line         — fetch posting lines for a trx_line.
--   posting_line_posted_at        — period-bound queries, daily roll-ups.
--   posting_line_dimension_lookup — reverse lookup posting_lines by dimension
--                                   value (sku/location/customer/etc.).
--
-- DEFERRED: trx_line_pool_seq_desc (pool_id, trx_seq DESC).
--   §13.1 lists this as a mitigation for MAX-scan plan flips. Gated behind
--   Phase 6 measurement evidence (acct-s7da). Adding it pre-measurement would
--   double-write every trx_line for no proven gain.

CREATE INDEX trx_line_trx
    ON trx_line (trx_id);

CREATE INDEX trx_line_pool
    ON trx_line (pool_id);

CREATE INDEX trx_line_source
    ON trx_line (source_trx_line_id)
    WHERE source_trx_line_id IS NOT NULL;

CREATE INDEX posting_line_trx_line
    ON posting_line (trx_line_id);

CREATE INDEX posting_line_posted_at
    ON posting_line (posted_at);

CREATE INDEX posting_line_dimension_lookup
    ON posting_line_dimension (dimension_type, dimension_id);
