import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval="15 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dds", "delivery", "project"],
    is_paused_upon_creation=False,
)
def dds_delivery_dag():
    pg = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def ensure_dds_tables():
        sql = """
        CREATE TABLE IF NOT EXISTS dds.dm_couriers (
            id SERIAL PRIMARY KEY,
            courier_id VARCHAR NOT NULL UNIQUE,
            courier_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
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
        """
        with pg.connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    @task()
    def load_dm_couriers():
        sql = """
        INSERT INTO dds.dm_couriers (courier_id, courier_name)
        SELECT
            (c.object_value->>'_id')::varchar AS courier_id,
            COALESCE(c.object_value->>'name','')::text AS courier_name
        FROM stg.deliverysystem_couriers c
        ON CONFLICT (courier_id) DO UPDATE
            SET courier_name = EXCLUDED.courier_name;
        """
        with pg.connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
            log.info(f"dm_couriers upserted: {cur.rowcount}")
            conn.commit()

    @task()
    def load_fct_deliveries():
        sql = """
        INSERT INTO dds.fct_deliveries (delivery_id, order_id, courier_id, delivery_ts, rate, tip_sum)
        SELECT
            (d.object_value->>'delivery_id')::varchar AS delivery_id,
            o.id AS order_id,
            cr.id AS courier_id,
            (d.object_value->>'delivery_ts')::timestamp AS delivery_ts,
            NULLIF(d.object_value->>'rate','')::int AS rate,
            COALESCE(NULLIF(d.object_value->>'tip_sum',''), '0')::numeric(19,2) AS tip_sum
        FROM stg.deliverysystem_deliveries d
        JOIN dds.dm_orders o
             ON o.order_key = (d.object_value->>'order_id')::varchar
        JOIN dds.dm_couriers cr
             ON cr.courier_id = (d.object_value->>'courier_id')::varchar
        ON CONFLICT (delivery_id) DO UPDATE SET
            order_id = EXCLUDED.order_id,
            courier_id = EXCLUDED.courier_id,
            delivery_ts = EXCLUDED.delivery_ts,
            rate = EXCLUDED.rate,
            tip_sum = EXCLUDED.tip_sum;
        """
        with pg.connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
            log.info(f"fct_deliveries upserted: {cur.rowcount}")
            conn.commit()

    ensure_dds_tables() >> load_dm_couriers() >> load_fct_deliveries()

dds_delivery_dag = dds_delivery_dag()
