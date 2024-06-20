CREATE OR REPLACE VIEW analysis.orders AS
WITH last_status AS (
SELECT ld.order_id, osl.status_id
		FROM (SELECT order_id, max(dttm) dttm
				FROM production.OrderStatusLog
				GROUP BY order_id) as ld
LEFT JOIN production.OrderStatusLog osl ON ld.order_id=osl.order_id AND ld.dttm=osl.dttm)
SELECT 
	o.order_id,
    o.order_ts,
    o.user_id,
    o.bonus_payment,
    o.payment,
    o.cost,
    o.bonus_grant,
    ls.status_id status
FROM production.orders o
JOIN last_status ls ON o.order_id = ls.order_id