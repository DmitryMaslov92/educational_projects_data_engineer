CREATE TABLE analysis.tmp_rfm_monetary_value (
 user_id INT NOT NULL PRIMARY KEY,
 monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);

WITH closed_orders AS (SELECT *
						FROM analysis.orders
						WHERE EXTRACT ('year' FROM order_ts) = 2022 AND status = 4),
orders_sum AS (SELECT u.id,
						CASE WHEN p.payment_sum IS NULL THEN 0 ELSE p.payment_sum END
					FROM analysis.users u
					LEFT JOIN (SELECT user_id,
								SUM(payment) AS payment_sum
								FROM closed_orders
								GROUP BY user_id) p ON u.id=p.user_id)
INSERT INTO analysis.tmp_rfm_monetary_value 
SELECT
	u.id user_id,
	NTILE(5) OVER(ORDER BY os.payment_sum ASC) monetary_value
FROM analysis.users u
JOIN orders_sum os ON u.id=os.id;