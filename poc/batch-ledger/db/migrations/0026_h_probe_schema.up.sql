-- acct-h7c4 / zm69.h0 — Candidate H SERIALIZABLE + deferred-constraint probe.
--
-- Pure-SQL ceiling probe for the append-only layers + deferred-constraint
-- design proposed as Candidate H. Tables suffixed `_h` to keep them
-- distinct from the FIFO arena's cost_layers / cost_layer_depletions
-- (which are owned by the ledger_extension at the PoC scale).
--
-- See poc/ledger-extension/docs/fifo-arena-correctness-audit-2026-05-14.md
-- §Candidate H + §Phase 2 entry — H probe.

CREATE TABLE cost_layers_h (
    layer_id BIGSERIAL PRIMARY KEY,
    layer_group_id BIGINT NOT NULL,
    qty BIGINT NOT NULL,            -- signed: positive receipts, negative reversals/adjustments
    unit_cost BIGINT NOT NULL,
    born_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    source_kind TEXT NOT NULL
);

CREATE INDEX cost_layers_h_group_idx ON cost_layers_h (layer_group_id);

CREATE TABLE cost_consumptions_h (
    consumption_id BIGSERIAL PRIMARY KEY,
    layer_group_id BIGINT NOT NULL,   -- denorm: trigger sums per group
    qty BIGINT NOT NULL CHECK (qty > 0),
    unit_cost BIGINT NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX cost_consumptions_h_group_idx ON cost_consumptions_h (layer_group_id);

-- The load-bearing invariant: per-group SUM(layers.qty) - SUM(consumptions.qty) >= 0.
-- Trigger raises SQLSTATE 40001 (serialization_failure) on violation so client
-- catch + retry follows the standard PG serialization-failure retry pattern.
CREATE OR REPLACE FUNCTION check_no_overconsume_h() RETURNS trigger AS $$
DECLARE
    v_effective BIGINT;
BEGIN
    SELECT
      COALESCE((SELECT SUM(qty) FROM cost_layers_h WHERE layer_group_id = NEW.layer_group_id), 0)
      - COALESCE((SELECT SUM(qty) FROM cost_consumptions_h WHERE layer_group_id = NEW.layer_group_id), 0)
      INTO v_effective;
    IF v_effective < 0 THEN
        RAISE EXCEPTION 'over-consumption: layer_group=% effective=%',
            NEW.layer_group_id, v_effective
            USING ERRCODE = '40001';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER check_no_overconsume_h
    AFTER INSERT ON cost_consumptions_h
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE PROCEDURE check_no_overconsume_h();
