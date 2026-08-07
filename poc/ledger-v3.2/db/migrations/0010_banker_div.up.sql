-- banker_div in SQL — the enabler for the single-statement direct hot path
-- (SPIKE-B shape, design-v3.2 §3). The commutative aggregate CTEs compute the
-- derived running-average unit_cost in-statement, so the round-half-to-even
-- division must exist SQL-side.
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

CREATE FUNCTION banker_div(numerator numeric, denominator bigint)
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
