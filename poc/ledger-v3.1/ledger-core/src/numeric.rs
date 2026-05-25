//! Numeric helpers for ledger-core (design-v3.1 §3.0).
//!
//! All monetary values and quantities are BIGINT at 1e-6 fixed precision. The
//! one division site is the WAC weighted-average formula on aggregate receipts:
//! `new_unit_cost = (old_qty*old_unit_cost + Q*C) / new_qty`. It uses banker's
//! rounding (round-half-to-even) so that, over many fractional remainders, the
//! rounding bias cancels to net-zero drift instead of accumulating.

/// Banker's rounding (round-half-to-even) for integer division.
///
/// Takes an `i128` numerator — callers MUST cast their `i64` operands to `i128`
/// BEFORE multiplying, because `i64 * i64` can overflow at this ledger's
/// precision well before the values are unreasonable (§3.0). The denominator is
/// `i64`. Returns the quotient as `i64`, panicking on overflow if the true
/// quotient cannot fit in `i64` (a real application-level numeric error — a
/// pool's value exceeding representable range).
///
/// Exact-half cases round to the nearest even integer. Panics if
/// `denominator == 0`; the caller must guard (the WAC formula's `new_qty == 0`
/// branch in `wac.rs` does).
pub fn banker_div(numerator: i128, denominator: i64) -> i64 {
    debug_assert!(denominator != 0, "banker_div: denominator must be non-zero");
    let denom = denominator as i128;
    let q = numerator / denom;
    let r = numerator % denom;

    let rounded: i128 = if r == 0 {
        q
    } else {
        let abs_r = r.abs();
        let abs_d = denom.abs();
        let sign: i128 = if (numerator < 0) ^ (denominator < 0) { -1 } else { 1 };
        let twice_r = abs_r * 2;
        if twice_r < abs_d {
            q
        } else if twice_r > abs_d {
            q + sign
        } else {
            // Exactly half: round to nearest even.
            if q % 2 == 0 { q } else { q + sign }
        }
    };

    i64::try_from(rounded).expect("banker_div: result overflows i64; pool value out of range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_division_returns_quotient() {
        assert_eq!(banker_div(100, 4), 25);
        assert_eq!(banker_div(-100, 4), -25);
        assert_eq!(banker_div(0, 7), 0);
    }

    #[test]
    fn below_half_rounds_toward_zero() {
        // 10/4 = 2.5 is exactly half (covered below); 9/4 = 2.25 -> 2.
        assert_eq!(banker_div(9, 4), 2);
        // -9/4 = -2.25 -> -2 (toward zero).
        assert_eq!(banker_div(-9, 4), -2);
    }

    #[test]
    fn above_half_rounds_away_from_zero() {
        // 11/4 = 2.75 -> 3 (away, toward +inf for positive).
        assert_eq!(banker_div(11, 4), 3);
        // -11/4 = -2.75 -> -3 (away, toward -inf for negative).
        assert_eq!(banker_div(-11, 4), -3);
    }

    #[test]
    fn exactly_half_rounds_to_even_q_even() {
        // 10/4 = 2.5; q=2 is even -> stays 2.
        assert_eq!(banker_div(10, 4), 2);
        // -10/4 = -2.5; q=-2 even -> stays -2.
        assert_eq!(banker_div(-10, 4), -2);
        // 5/2 = 2.5 -> 2 (even).
        assert_eq!(banker_div(5, 2), 2);
    }

    #[test]
    fn exactly_half_rounds_to_even_q_odd() {
        // 6/4 = 1.5; q=1 is odd -> rounds to 2.
        assert_eq!(banker_div(6, 4), 2);
        // 14/4 = 3.5; q=3 odd -> 4.
        assert_eq!(banker_div(14, 4), 4);
        // -6/4 = -1.5; q=-1 odd -> -2.
        assert_eq!(banker_div(-6, 4), -2);
        // 3/2 = 1.5 -> 2 (odd q rounds up).
        assert_eq!(banker_div(3, 2), 2);
    }

    #[test]
    fn negative_numerator_positive_denominator() {
        assert_eq!(banker_div(-7, 2), -4); // -3.5; q=-3 odd -> -4 (even)
        assert_eq!(banker_div(-1, 2), 0); // -0.5; q=0 even -> 0
    }

    #[test]
    fn both_signs_negative() {
        // -7 / -2 = 3.5; q=3 odd -> 4 (even).
        assert_eq!(banker_div(-7, -2), 4);
        // -9 / -4 = 2.25 -> 2.
        assert_eq!(banker_div(-9, -4), 2);
    }

    #[test]
    fn bias_cancellation_vs_truncation() {
        // Over a run of divide-by-2, banker's rounding stays within O(1) of the
        // mathematical sum, whereas always-truncate accumulates a systematic
        // negative drift. Demonstrates the rationale in §3.0.
        let mut banker_sum: i128 = 0;
        let mut trunc_sum: i128 = 0;
        let mut exact_sum_times2: i128 = 0;
        for n in 0..1000i128 {
            banker_sum += banker_div(n, 2) as i128;
            trunc_sum += n / 2;
            exact_sum_times2 += n;
        }
        let exact_half = exact_sum_times2 / 2; // = sum(n/2 real) since 0.5s pair up
        // Banker stays within 1 of exact; truncation drifts by ~250.
        assert!((banker_sum - exact_half).abs() <= 1, "banker drift {}", banker_sum - exact_half);
        assert!(exact_half - trunc_sum >= 200, "truncation should drift down");
    }

    #[test]
    fn numerator_i128_scale_with_quotient_in_i64_range() {
        // The numerator overflows i64 (i128-scale) but the quotient fits i64 —
        // the realistic shape of the WAC formula at high pool value (§3.0).
        // (i64::MAX * 1000) / 1000 == i64::MAX exactly (no remainder).
        let num: i128 = (i64::MAX as i128) * 1000;
        assert!(i64::try_from(num).is_err(), "numerator must exceed i64 range");
        assert_eq!(banker_div(num, 1000), i64::MAX);

        // Below-half remainder at the i64 ceiling: (i64::MAX*3 + 1)/3 -> i64::MAX.
        let num2: i128 = (i64::MAX as i128) * 3 + 1;
        assert_eq!(banker_div(num2, 3), i64::MAX);

        // Negative full-range floor.
        let num3: i128 = (i64::MIN as i128) * 4;
        assert_eq!(banker_div(num3, 4), i64::MIN);
    }

    #[test]
    #[should_panic(expected = "overflows i64")]
    fn overflow_on_downcast_panics() {
        // True quotient = i64::MAX * 2, which does not fit in i64.
        banker_div((i64::MAX as i128) * 2, 1);
    }

    #[test]
    fn wac_formula_overflow_regression_needs_i128() {
        // qty = 10^10 units, unit_cost = 10^9 (= $1000 at 1e-6). Each product is
        // 10^19, which overflows i64 (~9.2e18) — the cast to i128 before
        // multiplying is what keeps this correct.
        let old_qty: i64 = 10_000_000_000;
        let old_unit_cost: i64 = 1_000_000_000;
        let q: i64 = 10_000_000_000;
        let c: i64 = 2_000_000_000;
        let new_qty = old_qty + q;
        let numerator: i128 =
            (old_qty as i128) * (old_unit_cost as i128) + (q as i128) * (c as i128);
        // Average of 1e9 and 2e9 weighted equally = 1.5e9.
        assert_eq!(banker_div(numerator, new_qty), 1_500_000_000);
        // The i64-level computation would have overflowed:
        assert!(old_qty.checked_mul(old_unit_cost).is_none());
    }
}
