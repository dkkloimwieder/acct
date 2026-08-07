-- Append-only enforcement on the replay prefix (acct-1vur.4, design-v3.2 §7).
--
-- 0017 shipped BEFORE INSERT guards only. But the engine's closed-period
-- correctness argument is literally "an unchanged physical prefix means the
-- engine can never recompute a different authoritative cost" — a single stray
-- UPDATE to a historical `trx_line` breaks replay determinism and closed-
-- period valuation with no fail-loud anywhere. The parent project's doctrine
-- is that append-only is a schema invariant, not API discipline; v3.2 had it
-- half-implemented (inserts guarded, mutations not).
--
-- Four tables carry the replay prefix and the audit trail derived from it:
--   trx_line               — replay INPUT; the prefix the argument rests on
--   posting_line           — the GL journal
--   cost_settlement        — the authoritative valuation, per generation
--   cost_layer_consumption — the per-draw derivation of that valuation
-- The engine only ever INSERTs into these (verified across ledger-direct,
-- ledger-feed, ledger-spi-common, ledger-core, tests and bench), so the
-- guards are inert on every code path that exists today. They are here for
-- the paths that do not exist yet — which is exactly when a silent mutation
-- would be most expensive to detect.
--
-- Deliberately EXEMPT: `pool_settlement` (upserted every pass — it is the
-- moving frontier, not history) and `pool_state` (the materialized aggregate
-- and layer rows, rebuilt wholesale by each replay). Both are derived state
-- that the engine is supposed to rewrite; freezing them would freeze the
-- engine.
--
-- TRUNCATE is deliberately NOT blocked. Row-level triggers do not fire on
-- TRUNCATE, and a statement-level TRUNCATE guard would break `reset_state`
-- and the bench universe seeding for no invariant gain: the invariant is
-- determinism of a LIVE replay prefix, not the durability of fixture data. A
-- wholesale table wipe is not a silent mutation — nothing survives to be
-- silently wrong about.
--
-- `accounting_period` gets a different, narrower guard. It is not append-only
-- (a close legitimately moves state), but 0022's monotonic frontier is
-- `max(end_date) FILTER (WHERE state = 'closed')`, which is monotonic ONLY
-- under the close API. Two raw UPDATEs would defeat it: reverting a closed
-- period to 'open' lowers the frontier wholesale and re-opens the
-- uncovered-range side door; extending a closed period's end_date swallows
-- events that were legally admitted above the frontier and are still
-- unsettled. 0017's period_guard_trx_line also re-reads
-- `accounting_period.state` LIVE on every insert, so an unfreeze takes effect
-- immediately and silently.
--
-- The legal transition set is exactly what close.rs performs:
--   open -> closing   (gate failed; draining)
--   open -> closed    (gate passed first time)
--   closing -> closed (drained, then stamped)
-- There is no closing -> open revert path anywhere — a forced close goes
-- straight to the stamp — so the guard can be strict. Rows in state 'closed'
-- reject EVERY update, including a well-meaning `closed_by` correction: the
-- stamp is part of the audit record, and permitting "harmless" edits to a
-- closed row is exactly the discipline-not-invariant posture this migration
-- exists to remove. Deleting a closed period is the same unfreeze hole as
-- updating one, so it is refused too.
--
-- Because the legal set is exactly those three transitions, a
-- non-state-changing UPDATE (open -> open) is refused as well, which makes an
-- open period's `start_date`/`end_date` immutable too. That falls out of the
-- rule rather than being aimed at, and it is wanted: moving `end_date` under
-- an open period silently changes which events a pending close will sweep.
-- An open period may still be DELETED outright — fixtures need that, and a
-- whole-row removal is not a silent mutation. Reopening a closed period
-- remains out of scope (D14); this guard is where such a workflow would
-- attach.

CREATE FUNCTION reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
    RAISE EXCEPTION 'AppendOnly: % on % is not permitted (append-only replay prefix)',
        TG_OP, TG_TABLE_NAME
        USING ERRCODE = '55006';
END
$fn$;

CREATE TRIGGER trx_line_append_only
    BEFORE UPDATE OR DELETE ON trx_line
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER posting_line_append_only
    BEFORE UPDATE OR DELETE ON posting_line
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER cost_settlement_append_only
    BEFORE UPDATE OR DELETE ON cost_settlement
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER cost_layer_consumption_append_only
    BEFORE UPDATE OR DELETE ON cost_layer_consumption
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

CREATE FUNCTION accounting_period_transition_guard() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state = 'closed' THEN
            RAISE EXCEPTION
                'PeriodClosed: accounting_period % is closed and cannot be deleted', OLD.id
                USING ERRCODE = '55000';
        END IF;
        RETURN OLD;
    END IF;

    IF OLD.state = 'closed' THEN
        RAISE EXCEPTION
            'PeriodClosed: accounting_period % is closed and cannot be modified', OLD.id
            USING ERRCODE = '55000';
    END IF;

    IF NOT (OLD.state = 'open'    AND NEW.state = 'closing'
         OR OLD.state = 'open'    AND NEW.state = 'closed'
         OR OLD.state = 'closing' AND NEW.state = 'closed') THEN
        RAISE EXCEPTION
            'PeriodClosed: accounting_period % may not transition % -> %',
            OLD.id, OLD.state, NEW.state
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END
$fn$;

CREATE TRIGGER accounting_period_transitions
    BEFORE UPDATE OR DELETE ON accounting_period
    FOR EACH ROW EXECUTE FUNCTION accounting_period_transition_guard();
