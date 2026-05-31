//! [acct-0usf affinity — EXPERIMENTAL/REMOVABLE] committer→pool affinity (STEP 3).
//!
//! Clean re-implementation of the reverted acct-xdwk lever-2 V0 (commit
//! e9a77d8), rebuilt behind a default-off GUC so STEP 3 can measure it with the
//! STEP 1 instruments (committer pipeline spans + committer-segmented wait
//! sampler). EVERYTHING in this module and at every call site is tagged
//! `[acct-0usf affinity — EXPERIMENTAL/REMOVABLE]`; grep that string to find and
//! remove the whole lever. The production path is inert when
//! `ledger_routed_c.affinity_scheme = 0` (the default): the committer claim loop
//! and the router emit path each take a single cheap branch and behave exactly
//! as before.
//!
//! V0 key: a commit_group is owned by exactly one committer ordinal,
//! `owner = mix(min_pool_id) % committer_count`. On the committer claim scan a
//! non-owner skips the entry unless it has aged past `affinity_steal_ms` — an
//! age-gated steal so a backed-up or dead owner cannot starve the queue.
//!
//! A committer's ordinal is its identity-slot index (`identity.rs`). At a fresh
//! start the N committers grab the N lowest slots (0..committer_count-1), which
//! is exactly the modulo range, so ownership matches. A committer that took over
//! a slot ≥ committer_count after a peer death owns nothing and only steals —
//! acceptable for a default-off experimental lever; liveness is preserved by the
//! steal path. (The bench always restarts the cluster, so slots are 0..N-1.)

/// `affinity_scheme` GUC values.
pub(crate) const SCHEME_OFF: i32 = 0;
#[allow(dead_code)]
pub(crate) const SCHEME_MIN_POOL: i32 = 1;

/// Stable integer mix (splitmix64 finalizer) so owner assignment does not
/// cluster on the low bits of `min_pool_id`. Deterministic within and across
/// runs (no global state, no RNG).
#[inline]
fn mix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Owner ordinal for a commit_group keyed on its minimum pool_id (V0 key).
/// `committer_count` is clamped to ≥1.
#[inline]
pub(crate) fn owner_for_min_pool(min_pool_id: i64, committer_count: i32) -> u32 {
    let n = committer_count.max(1) as u64;
    (mix64(min_pool_id as u64) % n) as u32
}
