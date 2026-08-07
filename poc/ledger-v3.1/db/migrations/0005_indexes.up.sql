-- design-v3.1 §2.2 / §2.3 — indexes (collected here; tables created in 0002-0004).

CREATE INDEX account_parent ON account (parent_id) WHERE parent_id IS NOT NULL;

CREATE INDEX trx_line_trx ON trx_line (trx_id);
CREATE INDEX trx_line_pool ON trx_line (pool_id);
CREATE INDEX trx_line_source ON trx_line (source_trx_line_id) WHERE source_trx_line_id IS NOT NULL;

CREATE INDEX posting_line_trx_line ON posting_line (trx_line_id);
CREATE INDEX posting_line_posted_at ON posting_line (posted_at);

CREATE INDEX posting_line_dimension_lookup ON posting_line_dimension (dimension_type, dimension_id);
