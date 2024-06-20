from logging import Logger
from lib import PgConnect 
  
class LegerLoader:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg

    def update_mart(self) -> None:
        with self.pg.client().cursor() as cur:
            cur.execute(
                """
                    WITH rate AS(
                    SELECT dd.courier_id, fd.order_id, AVG(fd.rate) over(PARTITION BY dd.courier_id) avg_rate
                    FROM dds.fct_deliveries fd 
                    JOIN dds.dm_deliveries dd on fd.delivery_id = dd.id
                    ),
                    tmp AS (SELECT dd.courier_id,
                        dc.courier_name,
                        EXTRACT('YEAR' FROM dt.date)::int AS settlement_year,
                        EXTRACT('MONTH' FROM dt.date)::int AS settlement_month,
                        COUNT(do2.id) orders_count,
                        SUM(fd.sum) orders_total_sum,
                        AVG(fd.rate)::numeric(14,2) rate_avg,
                        SUM(fd.sum) * 0.25 order_processing_fee,
                        SUM(CASE			    
                                WHEN rate.avg_rate < 4 THEN greatest(rate.avg_rate * 0.05, 100)
                                WHEN rate.avg_rate >= 4 AND rate.avg_rate < 4.5 THEN greatest(fd.sum * 0.07, 150)
                                WHEN rate.avg_rate >= 4.5 AND rate.avg_rate < 4.9 THEN greatest(fd.sum * 0.07, 175)
                                WHEN rate.avg_rate >= 4.9 THEN greatest (fd.sum, 200)
                            END) AS courier_order_sum,
                        SUM(fd.tip_sum) courier_tips_sum
                    FROM dds.fct_deliveries fd 
                    JOIN dds.dm_deliveries dd on fd.delivery_id = dd.id 
                    JOIN dds.dm_couriers dc on dd.courier_id = dc.id 
                    JOIN dds.dm_orders do2 on fd.order_id = do2.id 
                    JOIN dds.dm_timestamps dt on do2.timestamp_id = dt.id
                    JOIN rate on do2.id = rate.order_id
                    WHERE do2.order_status = 'CLOSED'
                    GROUP BY 1,2,3,4
                    ORDER BY 3,4,1)
                    INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                    SELECT courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_order_sum + courier_tips_sum * 0.95 courier_reward_sum
                    FROM tmp
                    WHERE settlement_year = EXTRACT('year' from current_date) AND (settlement_month = EXTRACT('month' from current_date) OR settlement_month = EXTRACT('month' from current_date) - 1)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET courier_name = EXCLUDED.courier_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
            )