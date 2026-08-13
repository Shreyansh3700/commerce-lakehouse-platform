-- updated_at maintenance + CDC readiness.
--
-- Logical replication (wal_level=logical) is set via the postgres container's
-- command-line flags in docker-compose.yml, since it requires a restart --
-- baking it in now means Phase 2 never has to bounce Postgres.
--
-- REPLICA IDENTITY FULL and the PUBLICATION are created now (cheap, no WAL
-- growth). The replication SLOT is deliberately NOT created here -- an
-- unconsumed slot causes Postgres to retain WAL indefinitely, so Phase 2's
-- Debezium connector creates/consumes the slot itself on first startup.
-- See docs/decisions/0001-logical-replication-in-phase1.md.

-- ---------------------------------------------------------------------------
-- updated_at trigger, shared across all 7 core tables
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION commerce.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON commerce.customers
    FOR EACH ROW EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON commerce.products
    FOR EACH ROW EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON commerce.orders
    FOR EACH ROW EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER trg_order_items_updated_at
    BEFORE UPDATE ON commerce.order_items
    FOR EACH ROW EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON commerce.payments
    FOR EACH ROW EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER trg_inventory_updated_at
    BEFORE UPDATE ON commerce.inventory
    FOR EACH ROW EXECUTE FUNCTION commerce.set_updated_at();

CREATE TRIGGER trg_shipments_updated_at
    BEFORE UPDATE ON commerce.shipments
    FOR EACH ROW EXECUTE FUNCTION commerce.set_updated_at();

-- ---------------------------------------------------------------------------
-- CDC readiness
-- ---------------------------------------------------------------------------
ALTER TABLE commerce.customers   REPLICA IDENTITY FULL;
ALTER TABLE commerce.products    REPLICA IDENTITY FULL;
ALTER TABLE commerce.orders      REPLICA IDENTITY FULL;
ALTER TABLE commerce.order_items REPLICA IDENTITY FULL;
ALTER TABLE commerce.payments    REPLICA IDENTITY FULL;
ALTER TABLE commerce.inventory   REPLICA IDENTITY FULL;
ALTER TABLE commerce.shipments   REPLICA IDENTITY FULL;

CREATE PUBLICATION commerce_cdc FOR TABLE
    commerce.customers,
    commerce.products,
    commerce.orders,
    commerce.order_items,
    commerce.payments,
    commerce.inventory,
    commerce.shipments;

-- NOTE: no `SELECT pg_create_logical_replication_slot(...)` here on purpose --
-- see the header comment. Phase 2's Debezium connector config creates it.
