CREATE TABLE analysis.tmp_rfm_recency (
 user_id INT NOT NULL PRIMARY KEY,
 recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);

WITH closed_orders AS (SELECT *
						FROM analysis.orders
						WHERE EXTRACT ('year' FROM order_ts) = 2022 AND status = 4),
orders_count AS (SELECT u.id,
					CASE WHEN c.order_count IS NULL THEN 0 ELSE c.order_count END
				FROM analysis.users u
				LEFT JOIN (SELECT user_id,
								COUNT(user_id) AS order_count
							FROM closed_orders
							GROUP BY user_id) c ON u.id=c.user_id)
INSERT INTO analysis.tmp_rfm_recency 
SELECT
	u.id user_id,
	NTILE(5) OVER(ORDER BY oc.order_count ASC) recency
FROM analysis.users u
JOIN orders_count oc ON u.id=oc.id;