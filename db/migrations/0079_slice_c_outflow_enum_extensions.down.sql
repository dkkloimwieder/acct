-- Down: PG does not support ALTER TYPE DROP VALUE. The added enum
-- values 'ar_unsettled' (account_kind) and 'shipped' (reservation_
-- status) remain in place per project convention (mig 0020 down
-- comment: "Phase 0/1 has no production data; down is best-effort").
-- A subsequent migration that adds new functionality can rely on
-- these values existing.

-- (Intentionally empty.)
