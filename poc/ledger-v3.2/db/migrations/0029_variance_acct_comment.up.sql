-- The variance account's real requirement, stated at the schema
-- (acct-476a.1; design-v3.2 §4.1 is the authoritative statement).
--
-- 0006's header comment says "variance_acct is nullable: pools that are never
-- STD never need it". That was true when it was written and is not true now:
-- the close sweep (recalc-e §5, shipped with phase 5) posts each swept pool's
-- settlement residue as a zero-qty cost_adjustment_line against
-- posting_account_map.variance_acct, and the sweep covers every gate-scoped
-- FIFO/LIFO pool. A NULL there raises MissingVarianceAccount and fails the
-- close.
--
-- 0006 is applied and sqlx migrations are content-addressed, so its header
-- cannot be edited in place — the correction lands here, on the column
-- itself, where \d+ and any schema browser will show it. The file comment in
-- 0006 stays wrong by necessity; this is the authority.
--
-- The failure is late and load-dependent rather than immediate, which is why
-- stating it matters. post_residue_gl is only reached when the residue is
-- nonzero, and apply_residue returns 0 whenever the aggregate already equals
-- the sum of open-layer value (its UPDATE carries
-- `AND agg.value_sum IS DISTINCT FROM lay.v`); a swept pool with no aggregate
-- row short-circuits earlier still. So a misconfigured pool can close cleanly
-- several times before the first banker-rounding remainder, exact-empty
-- flush, or uncovered-depletion residue exposes it.
--
-- Numbering: 0025 was reserved for this pass, but the acct-1vur lane landed
-- 0026-0028 first and sqlx refuses a version below the highest applied one,
-- so this is 0029 and the 0025 gap is permanent and harmless.

COMMENT ON COLUMN posting_account_map.variance_acct IS
    'The account that absorbs cost that has nowhere else to go. Required for '
    'EVERY fifo/lifo pool, not only STD ones: the close sweep posts each '
    'swept pool''s settlement residue (banker-rounding remainder, exact-empty '
    'flush, uncovered-depletion value) against this account, and only '
    'fifo/lifo pools are swept. NULL raises MissingVarianceAccount at the '
    'first close that produces a nonzero residue - late and load-dependent, '
    'so populate it at pool-configuration time rather than relying on an '
    'early close to surface the gap. STD receipts whose actual cost differs '
    'from standard need it too, at submit time. See design-v3.2 §4.1.';
