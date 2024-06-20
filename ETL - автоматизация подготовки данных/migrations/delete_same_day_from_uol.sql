DELETE FROM staging.user_order_log uol
WHERE uol.date_time::Date = '{{ds}}';