MERGE INTO STV230533__DWH.global_metrics gm
USING 
	(WITH t AS (
		SELECT *,
		   ROW_NUMBER() OVER (PARTITION BY operation_id ORDER BY transaction_dt DESC) as rn
		FROM STV230533__STAGING.transactions t
		WHERE account_number_to >=0 AND account_number_from >=0
		  	  AND transaction_dt::date = '{{ ds }}'
	), c AS (	  
		SELECT *,
		   ROW_NUMBER() OVER (PARTITION BY currency_code,  currency_code_with ORDER BY date_update DESC) as rn
		FROM STV230533__STAGING.currencies c 
		WHERE currency_code_with = 420
			 AND date_update::date = '{{ ds }}'
	)
	SELECT t.transaction_dt::date AS date_update,
		   t.currency_code AS currency_from,
		   ROUND(SUM(t.amount * (CASE t.currency_code WHEN 420 THEN 1 ELSE c.currency_with_div END)),3) AS amount_total,
		   COUNT(DISTINCT t.operation_id) AS cnt_transactions,
		   ROUND(COUNT(DISTINCT t.operation_id)/COUNT(DISTINCT t.account_number_from), 3) AS avg_transactions_per_account,
		   ROUND(COUNT(DISTINCT t.account_number_from),3) AS cnt_accounts_make_transactions
		FROM t LEFT JOIN c
			ON t.currency_code = c.currency_code AND t.transaction_dt::date = c.date_update::date
			WHERE t.rn=1 
		GROUP BY  1, 2
		ORDER BY 1) cte
ON gm.date_update = cte.date_update AND gm.currency_from=cte.currency_from
WHEN MATCHED THEN UPDATE
SET amount_total = cte.amount_total,
	cnt_transactions = cte.cnt_transactions,
	avg_transactions_per_account = cte.avg_transactions_per_account,
	cnt_accounts_make_transactions = cte.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN
INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
VALUES (cte.date_update, cte.currency_from, cte.amount_total, cte.cnt_transactions, cte.avg_transactions_per_account, cte.cnt_accounts_make_transactions);