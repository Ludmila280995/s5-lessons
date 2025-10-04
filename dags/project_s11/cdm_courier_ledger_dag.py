import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval="30 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["cdm", "courier_ledger", "project"],
    is_paused_upon_creation=False,
)
def cdm_courier_ledger_dag():
    pg = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def ensure_unique_idx():
        with pg.connection() as conn, conn.cursor() as cur:
            cur.execute("""
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname='cdm' AND indexname='uq_dm_courier_ledger_rest_month'
              ) THEN
                EXECUTE 'CREATE UNIQUE INDEX uq_dm_courier_ledger_rest_month
                         ON cdm.dm_courier_ledger(courier_id, settlement_year, settlement_month)';
              END IF;
            END$$;
            """)
            conn.commit()

    @task()
    def build_prev_month_ledger():
        now = pendulum.now("UTC")
        month_start = now.start_of("month").subtract(months=1)
        next_month  = now.start_of("month")

        sql = """
        WITH bounds AS (
          SELECT
            %(from_ts)s::timestamp AS d1,
            %(to_ts)s::timestamp   AS d2,
            EXTRACT(YEAR FROM %(from_ts)s::date)::smallint AS y,
            EXTRACT(MONTH FROM %(from_ts)s::date)::smallint AS m
        ),
        orders_in_month AS (
          SELECT
            o.id AS order_id,
            o.order_status,
            t.ts::date AS order_date
          FROM dds.dm_orders o
          JOIN dds.dm_timestamps t ON t.id = o.timestamp_id
          JOIN bounds b ON t.ts >= b.d1 AND t.ts < b.d2
          WHERE o.order_status = 'CLOSED'
        ),
        base AS (
          SELECT
            fd.courier_id,
            fd.order_id,
            fd.rate,
            COALESCE(fd.tip_sum,0)::numeric(14,2) AS tip_sum
          FROM dds.fct_deliveries fd
          JOIN orders_in_month om ON om.order_id = fd.order_id
        ),
        order_totals AS (
          SELECT fps.order_id, SUM(fps.total_sum)::numeric(14,2) AS order_total
          FROM dds.fct_product_sales fps
          GROUP BY fps.order_id
        ),
        joined AS (
          SELECT
            b.courier_id,
            b.order_id,
            b.tip_sum,
            COALESCE(ot.order_total,0)::numeric(14,2) AS order_total
          FROM base b
          LEFT JOIN order_totals ot ON ot.order_id = b.order_id
        ),
        rate_avg AS (
          SELECT courier_id, AVG(NULLIF(rate,0))::numeric(3,2) AS r
          FROM base
          GROUP BY courier_id
        ),
        rules AS (
          SELECT
            ra.courier_id,
            ra.r,
            CASE
              WHEN ra.r < 4     THEN 0.05
              WHEN ra.r < 4.5   THEN 0.07
              WHEN ra.r < 4.9   THEN 0.08
                                 ELSE 0.10
            END AS pct,
            CASE
              WHEN ra.r < 4     THEN 100
              WHEN ra.r < 4.5   THEN 150
              WHEN ra.r < 4.9   THEN 175
                                 ELSE 200
            END AS min_pay
          FROM rate_avg ra
        ),
        per_order AS (
          SELECT
            j.courier_id,
            j.order_id,
            j.order_total,
            j.tip_sum,
            r.pct,
            r.min_pay,
            GREATEST( (j.order_total * r.pct), r.min_pay )::numeric(14,2) AS pay_for_order
          FROM joined j
          JOIN rules r ON r.courier_id = j.courier_id
        ),
        agg AS (
          SELECT
            courier_id,
            COUNT(DISTINCT order_id) AS orders_count,
            COALESCE(SUM(order_total),0)::numeric(14,2) AS orders_total_sum,
            COALESCE(AVG(pct),0)::numeric(3,2) AS pct_dbg,
            COALESCE(SUM(pay_for_order),0)::numeric(14,2) AS courier_order_sum,
            COALESCE(SUM(tip_sum),0)::numeric(14,2) AS courier_tips_sum
          FROM per_order
          GROUP BY courier_id
        ),
        final AS (
          SELECT
            c.courier_id AS courier_nk,
            c.courier_name,
            b.y AS settlement_year,
            b.m AS settlement_month,
            a.orders_count,
            a.orders_total_sum,
            (SELECT r.r FROM rate_avg r WHERE r.courier_id = a.courier_id)::numeric(3,2) AS rate_avg,
            (a.orders_total_sum * 0.25)::numeric(14,2)  AS order_processing_fee,
            a.courier_order_sum,
            a.courier_tips_sum,
            ROUND( a.courier_order_sum + a.courier_tips_sum * 0.95 , 2) AS courier_reward_sum
          FROM agg a
          JOIN dds.dm_couriers c ON c.id = a.courier_id
          CROSS JOIN bounds b
        ),
        keys AS (
          SELECT courier_nk AS courier_id, settlement_year, settlement_month FROM final
        ),
        del AS (
          DELETE FROM cdm.dm_courier_ledger l
          USING keys k
          WHERE l.courier_id = k.courier_id
            AND l.settlement_year = k.settlement_year
            AND l.settlement_month = k.settlement_month
          RETURNING 1
        )
        INSERT INTO cdm.dm_courier_ledger(
          courier_id, courier_name, settlement_year, settlement_month,
          orders_count, orders_total_sum, rate_avg, order_processing_fee,
          courier_order_sum, courier_tips_sum, courier_reward_sum
        )
        SELECT
          courier_nk, courier_name, settlement_year, settlement_month,
          orders_count, orders_total_sum, rate_avg, order_processing_fee,
          courier_order_sum, courier_tips_sum, courier_reward_sum
        FROM final;
        """
        params = {
            "from_ts": month_start.to_datetime_string(),
            "to_ts": next_month.to_datetime_string()
        }
        with pg.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            ins = cur.rowcount
            conn.commit()
            log.info(f"cdm.dm_courier_ledger: inserted rows for {month_start.format('YYYY-MM')}: {ins}")

    ensure_unique_idx() >> build_prev_month_ledger()

cdm_courier_ledger_dag = cdm_courier_ledger_dag()
