CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
    id SERIAL PRIMARY KEY,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    object_id VARCHAR NOT NULL UNIQUE,
    object_value JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
    id SERIAL PRIMARY KEY,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    object_id VARCHAR NOT NULL UNIQUE,
    object_value JSONB NOT NULL
);

DROP TABLE IF EXISTS dds.dm_couriers CASCADE;
CREATE TABLE dds.dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL UNIQUE,
    courier_name TEXT NOT NULL
);

DROP TABLE IF EXISTS dds.fct_deliveries CASCADE;
CREATE TABLE dds.fct_deliveries (
    id SERIAL PRIMARY KEY,
    delivery_id VARCHAR NOT NULL UNIQUE,
    order_id INT NOT NULL REFERENCES dds.dm_orders(id),
    courier_id INT NOT NULL REFERENCES dds.dm_couriers(id),
    delivery_ts TIMESTAMPTZ,
    rate INT CHECK (rate BETWEEN 1 AND 5),
    tip_sum NUMERIC(19,2) NOT NULL DEFAULT 0 CHECK (tip_sum >= 0)
);
CREATE INDEX IF NOT EXISTS ix_fct_deliveries_courier ON dds.fct_deliveries(courier_id);
CREATE INDEX IF NOT EXISTS ix_fct_deliveries_order ON dds.fct_deliveries(order_id);

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year  SMALLINT NOT NULL CHECK (settlement_year BETWEEN 2022 AND 2500),
    settlement_month SMALLINT NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
    orders_count INT NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    rate_avg NUMERIC(3,2) NOT NULL DEFAULT 0 CHECK (rate_avg BETWEEN 1 AND 5),
    order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    courier_order_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
    courier_tips_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
    courier_reward_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0),
    UNIQUE (courier_id, settlement_year, settlement_month)
);
