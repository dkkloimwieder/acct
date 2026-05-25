//! ledger-harness: multi-session measurement binary for ledger-v3.1 (Path C).
//!
//! Authoritative spec: `poc/design_research/design-v3.1.md` §10.
//!
//! Subcommands (P4, acct-2ttr.8): seed-pools, run (--mode direct-per-call |
//! direct-batched | routed; --scenario s1..s8), equivalence. Deep-pool seeding via
//! direct SQL into pool_state + matching trx_line receipts (§10.5).
//!
//! Scaffold stub (P1.1, acct-2ttr.1).

fn main() {
    eprintln!("ledger-harness (ledger-v3.1) scaffold — implemented in P4 (acct-2ttr.8)");
}
