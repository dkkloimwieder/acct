-- Close-gate feed-currency policy (acct-1vur.1).
--
-- Before this migration `ledger_close_period` ASSUMED a running feed: the
-- recalc-drain gate measures lag as physical events strictly above
-- `(settled_through_posted_at, settled_through_id)` and reads recost floors
-- that only the feed writes. An event backdated BELOW the settled frontier,
-- committed but not yet delivered by the slot, contributes to neither signal —
-- zero lag, no floor — so the gate passed and the close stamped an immutably
-- wrong valuation. Worse, the residue sweep then booked the straggler's entire
-- value to the variance account, because the aggregate carried it (the hot
-- path wrote it) while the authoritative layers did not (the engine never
-- replayed it). When the slot finally delivered, the floor dropped below the
-- closed boundary and every subsequent engine pass died on 0017's settlement
-- guard — a permanently wedged pool whose next period could never close.
--
-- `feed_required` turns that premise into an enforced gate leg. With it TRUE
-- (the production default) `ledger_close_period` requires, at gate time, a
-- present, non-invalidated `ledger_feed` slot whose `confirmed_flush_lsn` has
-- reached the WAL position captured at gate entry — the D8 contract makes
-- `confirmed_flush_lsn` the delivered-and-applied cursor, so reaching that
-- position means every event committed before this close has been delivered.
--
-- FALSE waives the leg for feedless contexts (test fixtures that never create
-- a slot). It is deliberately a stored policy rather than a function argument:
-- the waiver should be a visible, auditable property of the database, not a
-- per-call flag a caller can pass by habit.
--
-- Force does NOT bypass this leg — the only non-bypassable arm of the gate.
-- Force means "pay the remaining fold synchronously", which is coherent only
-- when the fold has all its inputs; on a lagging slot a forced close does not
-- merely close early, it actively corrupts the variance account with the
-- sweep misbooking described above. The other gate arms stay force-bypassable
-- exactly as before.
--
-- Note on liveness: `pg_replication_slots.active` is NOT a health signal here.
-- The feed consumes over the SQL logical-decoding interface (peek + advance,
-- recalc-c D8), which holds the slot only for the duration of each call, so a
-- perfectly healthy feed reads `active = false` between ticks. Currency
-- subsumes liveness — a stopped consumer fails the LSN check.
CREATE TABLE close_gate_config (
    id            BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    feed_required BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO close_gate_config DEFAULT VALUES;

-- The gate joins this row; with it missing the leg would silently vanish and
-- the blind close would be reachable again. The BOOLEAN PK prevents
-- duplicates; this guard prevents emptiness (same shape as
-- recalc_backpressure_config).
CREATE FUNCTION close_gate_config_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'close_gate_config must keep its single row (UPDATE it instead)';
END;
$$;

CREATE TRIGGER close_gate_config_keep
    BEFORE DELETE OR TRUNCATE ON close_gate_config
    FOR EACH STATEMENT EXECUTE FUNCTION close_gate_config_guard();
