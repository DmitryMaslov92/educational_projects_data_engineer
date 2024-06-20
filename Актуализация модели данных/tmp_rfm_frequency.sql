CREATE TABLE analysis.tmp_rfm_frequency (
 user_id INT NOT NULL PRIMARY KEY,
 frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);

WITH closed_orders AS (SELECT *
						FROM analysis.orders
						WHERE EXTRACT ('year' FROM order_ts) = 2022 AND status = 4),
user_last_order AS (SELECT u.id,
						CASE WHEN l.last_order IS NULL THEN '2021-12-31' ELSE l.last_order END
					FROM analysis.users u
					LEFT JOIN (SELECT user_id,
									MAX(order_ts) AS last_order
								FROM closed_orders
								GROUP BY user_id) l ON u.id=l.user_id)
INSERT INTO analysis.tmp_rfm_frequency 
SELECT
	u.id user_id,
	NTILE(5) OVER(ORDER BY ulo.last_order ASC) frequency
FROM analysis.users u
JOIN user_last_order ulo ON u.id=ulo.id;