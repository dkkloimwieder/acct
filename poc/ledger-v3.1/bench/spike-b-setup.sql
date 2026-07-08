-- SPIKE-B (acct-0at4.11.2) setup: banker_div in SQL.
--
-- FEEDBACK-ARCH.md problem #4 / alt B asks whether the aggregate hot path needs
-- to round-trip into ledger-core (Rust) at all, or whether one commutative SQL
-- statement under PostgreSQL's own row lock suffices. The blocker to answering
-- "yes" is that the running-average arithmetic (banker's rounding, round-half-to-
-- even) lived only in Rust (ledger_core::numeric::banker_div). This ports it to
-- SQL so `ledger_submit_trx_single_c`'s CTE can compute the derived unit_cost
-- in-statement, with NO pool_lock table and NO Rust round-trip.
--
-- Byte-faithful to ledger_core::numeric::banker_div:
--   q = trunc(numerator / denominator)          (toward zero)
--   r = numerator - q*denominator               (sign of numerator)
--   |r|*2 < |den|  -> q
--   |r|*2 > |den|  -> q + sign          sign = (num<0) xor (den<0) ? -1 : +1
--   |r|*2 = |den|  -> q if q even, else q + sign      (round-half-to-even)
-- Numerator is `numeric` (the Rust caller casts i64 -> i128 before multiplying;
-- numeric is the SQL analogue that cannot overflow mid-expression). The final
-- ::bigint cast raises `bigint out of range` when the quotient escapes i64 —
-- mirroring the Rust `expect("...overflows i64...")`.

CREATE OR REPLACE FUNCTION banker_div(numerator numeric, denominator bigint)
RETURNS bigint
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
    SELECT (
      SELECT CASE
               WHEN r = 0            THEN q
               WHEN abs(r) * 2 < abs_d THEN q
               WHEN abs(r) * 2 > abs_d THEN q + sgn
               WHEN (q % 2) = 0     THEN q          -- exact half, q even
               ELSE q + sgn                          -- exact half, q odd -> even
             END
      FROM (
        SELECT trunc(numerator / denominator::numeric)                                    AS q,
               numerator - trunc(numerator / denominator::numeric) * denominator::numeric AS r,
               abs(denominator::numeric)                                                   AS abs_d,
               CASE WHEN (numerator < 0) <> (denominator < 0) THEN -1 ELSE 1 END           AS sgn
      ) v
    )::bigint
$fn$;
