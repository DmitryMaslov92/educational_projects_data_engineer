DELETE FROM mart.f_sales
WHERE date_id = (select date_id from mart.d_calendar where date_actual::Date = '{{ds}}');

insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select dc.date_id, item_id, customer_id, city_id, quantity, payment_amount
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}'
UNION ALL
select dc.date_id, item_id, customer_id, city_id, quantity, -payment_amount from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
WHERE uol.status = 'refunded' AND uol.date_time::Date = '{{ds}}';