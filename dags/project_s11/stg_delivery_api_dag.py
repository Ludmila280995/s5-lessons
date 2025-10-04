import os
import math
import json
import pendulum
import logging
import requests
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

API_BASE = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
NICKNAME = os.getenv("DELIVERY_API_NICK", "lussia2809")
COHORT   = os.getenv("DELIVERY_API_COHORT", "8")
API_KEY  = os.getenv("DELIVERY_API_KEY", "25c27781-8fde-4b30-a22e-524044a7580f")

HEADERS = {
    "X-Nickname": NICKNAME,
    "X-Cohort": COHORT,
    "X-API-KEY": API_KEY
}

LIMIT = 50

@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["stg", "delivery_api", "project"],
    is_paused_upon_creation=False,
)
def stg_delivery_api_dag():
    pg = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def ensure_stg_tables():
        sql = """
        CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers(
            id SERIAL PRIMARY KEY,
            load_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            object_id VARCHAR NOT NULL UNIQUE,
            object_value JSONB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries(
            id SERIAL PRIMARY KEY,
            load_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            object_id VARCHAR NOT NULL UNIQUE,
            object_value JSONB NOT NULL
        );
        """
        with pg.connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    def _paged_get(url, params):
        offset = 0
        total = 0
        while True:
            qp = dict(params)
            qp["limit"] = LIMIT
            qp["offset"] = offset
            r = requests.get(url, headers=HEADERS, params=qp, timeout=60)
            r.raise_for_status()
            data = r.json() or []
            if not data:
                break
            for row in data:
                yield row
                total += 1
            if len(data) < LIMIT:
                break
            offset += LIMIT
        log.info(f"Fetched {total} rows from {url}")

    @task()
    def load_couriers_stg():
        url = f"{API_BASE}/couriers"
        params = {"sort_field": "id", "sort_direction": "asc"}
        with pg.connection() as conn, conn.cursor() as cur:
            for c in _paged_get(url, params):
                obj_id = str(c.get("_id"))
                cur.execute(
                    """
                    INSERT INTO stg.deliverysystem_couriers(object_id, object_value)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value, load_ts = now()
                    """,
                    (obj_id, json.dumps(c))
                )
            conn.commit()

    @task()
    def load_deliveries_stg():
        now_utc = pendulum.now("UTC")
        frm = (now_utc - pendulum.duration(days=7)).format("YYYY-MM-DD HH:mm:ss")
        to  = now_utc.format("YYYY-MM-DD HH:mm:ss")

        url = f"{API_BASE}/deliveries"
        params = {
            "sort_field": "_id",
            "sort_direction": "asc",
            "from": frm,
            "to": to
        }
        with pg.connection() as conn, conn.cursor() as cur:
            for d in _paged_get(url, params):
                obj_id = str(d.get("delivery_id"))
                cur.execute(
                    """
                    INSERT INTO stg.deliverysystem_deliveries(object_id, object_value)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value,
                        load_ts = now()
                    """,
                    (obj_id, json.dumps(d))
                )
            conn.commit()

    ensure_stg_tables() >> [load_couriers_stg(), load_deliveries_stg()]

stg_delivery_api_dag = stg_delivery_api_dag()
