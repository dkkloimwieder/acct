-- recalc-d §3 (D1 resolved) — denormalize trx_line.posted_at and index the
-- recalc walk order.
--
-- The recalc engine scans each pool's whole stream in (posted_at, id) business
-- chronology (R-1), repeatedly (R-2 replays it). The denormalized column turns
-- that walk into an index scan with no trx JOIN in the hot inner loop.
--
-- Write-path contract: populated from trx.posted_at at INSERT time (the hot
-- path already holds it — one extra constant-time column write). The value MUST
-- be the real business/effective time, not a wall-clock stamp; NOT NULL is
-- schema-enforceable, truthfulness is an SPI-contract obligation on the caller.
ALTER TABLE trx_line
    ADD COLUMN posted_at TIMESTAMPTZ NOT NULL;

-- D5 resolved: no separate (pool_id, id) index. Recalc's canonical per-pool
-- walk is business chronology (R-1), which this composite serves; pool_id-only
-- lookups use its leading prefix. No consumer needs pure id-order within a
-- pool; if one appears, adding that index back is a one-line migration.
CREATE INDEX trx_line_pool_posted ON trx_line (pool_id, posted_at, id);
