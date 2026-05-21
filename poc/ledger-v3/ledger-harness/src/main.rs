//! ledger-harness: multi-session measurement binary for ledger-v3.
//!
//! Authoritative spec: `poc/design_research/design-v3.md` §8.5 / §9.
//!
//! Subcommands (added by follow-up beads issues):
//! - `seed-pools` — acct-llt2 (N pools × M skus × L locations, configurable method assignment)
//! - `run`        — acct-ykyl / acct-qiaz (driver_direct / driver_routed)
//! - `equivalence`— acct-t9lo (cross-path §8.4 diff)
//!
//! Scaffolded in acct-bitp; this file is the workspace stub.

fn main() {
    eprintln!("ledger-harness: scaffolding stub. Subcommands added in acct-bitp.");
    std::process::exit(2);
}
