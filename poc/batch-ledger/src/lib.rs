//! Batch ledger PoC — acct-qdp5 epic / P1 acct-c0ko.
//!
//! See README.md for posture (clean Postgres, throwaway after P8 decision)
//! and `bd show acct-qdp5` for the goal hierarchy (target: 10K TPS).
//!
//! P1 is schema-only. Subsequent phases add:
//!   - P2 (acct-zdrm) — pgledger-equivalent `post_transfer` baseline.
//!   - P3 (acct-k7c6) — `post_batch` for pure double-entry.
//!   - P4-P5 — cost dispatch on top of the batch API.
//!   - P6 — state-machine + GRNI.
//!   - P7-P8 — hard-cases + synthesis.
