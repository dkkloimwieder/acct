-- The posted_at write contract, mechanically enforced where it can be
-- (acct-1vur.6; design-v3.2 §3a states the contract itself).
--
-- The recalc engine replays each pool in (pool_id, posted_at, id) order, so
-- R-1 correctness rests on `posted_at` carrying the TRUE business/effective
-- time of the event at write time. No constraint can enforce that: the
-- database cannot know what actually happened when. The v3.1 harness shows
-- the failure mode — it stamped a compile-time constant on every submission,
-- which silently degrades R-1 replay to id order (i.e. commit order), the one
-- ordering the whole design exists to not depend on. And because backdating
-- is admitted BY DESIGN (that is R-2), "close to now()" is not a valid guard
-- either. The contract is about truthfulness, not recency.
--
-- What IS mechanically enforceable, and is enforced here:
--
--   NOT NULL       — already held on trx.posted_at, trx_line.posted_at,
--                    posting_line.posted_at and ledger_inbox.posted_at since
--                    0001/0011. Audited, no change needed.
--
--   NO DEFAULT     — the load-bearing one, and the reason this migration is
--                    mostly a lock rather than an addition. None of those four
--                    columns has a column default, so a writer that forgets
--                    posted_at gets a NOT NULL violation instead of silently
--                    inheriting now(). A `DEFAULT now()` added later would
--                    convert every such bug into exactly the v3.1 failure
--                    mode, invisibly. The trigger below makes that regression
--                    impossible to introduce by accident on the staging path,
--                    where envelopes arrive as raw INSERTs from outside the
--                    SPI entry point.
--
--   STAGING SHAPE  — `ledger_inbox` is the one path where a producer writes
--                    the ledger without going through `ledger_submit_trx`, so
--                    it is the one place worth validating the envelope's
--                    business time rather than trusting it.
--
-- DECLINED: a future-side sanity bound (reject posted_at beyond now() + N).
-- The past side is already bounded — 0017's closed-period guards and 0022's
-- monotonic frontier reject backdating into frozen history — but the future
-- side is deliberately left open. Business events legitimately carry future
-- effective dates (a scheduled receipt, a dated transfer), and a horizon
-- would be an arbitrary policy that fails closed on a legitimate document
-- while doing nothing about the actual risk, which is a WRONG date rather
-- than a far one.
--
-- The one concrete hazard future-dating did create was real and is fixed at
-- its root instead: the feed's floor-lowering statement did not exclude the
-- engine's own `cost_adjustment_line` output. Those rows are stamped
-- posted_at = now(), which normally sorts above every frontier — but a pool
-- whose head event is future-dated has a frontier ABOVE now(), so the
-- engine's adjustment would deliver below it, lower a recost floor, and
-- trigger a re-fold that emits another adjustment: a self-sustaining loop.
-- The feed now filters by line_type there, matching what the backpressure
-- bump already did. That removes the premise rather than restricting which
-- business dates callers may use.

CREATE FUNCTION ledger_inbox_posted_at_guard() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    -- Redundant with NOT NULL by design: this trigger is the guard that
    -- survives a future schema edit adding a default.
    IF NEW.posted_at IS NULL THEN
        RAISE EXCEPTION
            'PostedAtContract: ledger_inbox.posted_at must be the event''s true business time, not omitted'
            USING ERRCODE = '22004';
    END IF;
    RETURN NEW;
END
$fn$;

CREATE TRIGGER ledger_inbox_posted_at
    BEFORE INSERT ON ledger_inbox
    FOR EACH ROW EXECUTE FUNCTION ledger_inbox_posted_at_guard();

COMMENT ON COLUMN trx_line.posted_at IS
    'The event''s TRUE business/effective time, supplied by the writer. The '
    'recalc engine replays in (pool_id, posted_at, id) order, so a wall-clock '
    'or constant stamp silently degrades R-1 replay to commit order. '
    'Backdating is admitted by design (R-2); no recency bound applies. '
    'See design-v3.2 §3a.';

COMMENT ON COLUMN ledger_inbox.posted_at IS
    'The event''s TRUE business/effective time. Staging envelopes bypass '
    'ledger_submit_trx, so this is validated on insert. See design-v3.2 §3a.';
