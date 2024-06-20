CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    id serial4 PRIMARY KEY,
    new_customers_count INT4,
    returning_customers_count INT4,
    refunded_customer_count INT4,
    period_name VARCHAR(20) DEFAULT 'weekly',
    period_id INT4 NOT NULL,
    item_id INT4 NOT NULL,
    new_customers_revenue NUMERIC(14, 2),
    returning_customers_revenue NUMERIC(14, 2),
    customers_refunded INT4,
    CONSTRAINT f_customer_retention_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id));


DELETE
FROM mart.f_customer_retention
WHERE period_id =
    (SELECT week_of_year
     FROM mart.d_calendar
     WHERE date_actual::Date ='{{ds}}');

WITH shipped_orders AS
  (SELECT dc.week_of_year,
          uol.item_id,
          uol.customer_id,
          payment_amount,
          count(uol.payment_amount) OVER (PARTITION BY (dc.week_of_year,
                                                        uol.item_id,
                                                        uol.customer_id)
                                          ORDER BY dc.week_of_year) AS w
   FROM staging.user_order_log uol
   LEFT JOIN mart.d_calendar dc ON uol.date_time = dc.date_actual
   WHERE status = 'shipped'
     AND dc.week_of_year =
       (SELECT week_of_year
        FROM mart.d_calendar
        WHERE date_actual::Date ='{{ds}}')
   ORDER BY dc.week_of_year,
            uol.item_id,
            uol.customer_id),
     new_cl AS
  (SELECT week_of_year,
          item_id,
          count(customer_id) new_customers_count,
          sum(payment_amount) new_customers_revenue
   FROM shipped_orders
   WHERE w = 1
   GROUP BY week_of_year,
            item_id),
     ret_cl AS
  (SELECT week_of_year,
          item_id,
          count(customer_id) returning_customers_count,
          sum(payment_amount) returning_customers_revenue
   FROM shipped_orders
   WHERE w > 1
   GROUP BY week_of_year,
            item_id),
     ref_cl AS
  (SELECT dc.week_of_year,
          uol.item_id,
          count(DISTINCT uol.customer_id) refunded_customer_count,
          count(uol.customer_id) customers_refunded
   FROM staging.user_order_log uol
   LEFT JOIN mart.d_calendar dc ON uol.date_time = dc.date_actual
   WHERE status = 'refunded'
     AND dc.week_of_year =
       (SELECT week_of_year
        FROM mart.d_calendar
        WHERE date_actual::Date ='{{ds}}')
   GROUP BY dc.week_of_year,
            uol.item_id
   ORDER BY dc.week_of_year,
            uol.item_id)
INSERT INTO mart.f_customer_retention (new_customers_count, returning_customers_count, refunded_customer_count, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
SELECT new_cl.new_customers_count,
       ret_cl.returning_customers_count,
       ref_cl.refunded_customer_count,
       new_cl.week_of_year period_id,
       new_cl.item_id,
       new_cl.new_customers_revenue,
       ret_cl.returning_customers_revenue,
       ref_cl.customers_refunded
FROM new_cl
FULL JOIN ret_cl ON (new_cl.week_of_year,
                     new_cl.item_id) = (ret_cl.week_of_year,
                                        ret_cl.item_id)
FULL JOIN ref_cl ON (new_cl.week_of_year,
                     new_cl.item_id) = (ref_cl.week_of_year,
                                        ref_cl.item_id)
ORDER BY new_cl.week_of_year,
         new_cl.item_id;