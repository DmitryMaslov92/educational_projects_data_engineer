CREATE TABLE IF NOT EXISTS STV230533__STAGING.currencies (
	date_update timestamp, 
    currency_code int,
    currency_code_with int,
    currency_with_div numeric(5,3)
)
ORDER BY date_update
SEGMENTED BY hash(currency_code,date_update) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2)
;

CREATE TABLE IF NOT EXISTS STV230533__STAGING.transactions (
	operation_id varchar(60),
	account_number_from int,
	account_number_to int,
	currency_code int,
	country varchar(30),
	status varchar(30),
	transaction_type varchar(30),
	amount int,
	transaction_dt timestamp
)
ORDER BY transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt) ALL NODES
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2)
;