-- design-v3.1 §2.3 (account) + §2.4 (accounting_period, sku, location).
-- Reference data: application-managed BIGINT ids (assigned by upstream systems / the
-- PoC harness), not auto-allocated. pool.sku_id / pool.location_id are plain BIGINTs
-- with no FK to sku/location (§2.2), so these tables stand alone for the ledger's purposes.

CREATE TABLE sku (
    id          BIGINT PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE location (
    id          BIGINT PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE account (
    id          BIGINT PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    type        account_type NOT NULL,
    parent_id   BIGINT REFERENCES account(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE accounting_period (
    id          BIGINT PRIMARY KEY,
    start_date  DATE NOT NULL,
    end_date    DATE NOT NULL,
    state       TEXT NOT NULL CHECK (state IN ('open', 'closing', 'closed')),
    closed_at   TIMESTAMPTZ,
    closed_by   TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (start_date, end_date)
);
