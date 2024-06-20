DROP SCHEMA IF EXISTS cdm CASCADE;
CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE cdm.dm_courier_ledger(
	id serial PRIMARY KEY,
	courier_id INTEGER NOT NULL,
	courier_name varchar(100) NOT NULL,
	settlement_year INTEGER NOT NULL CHECK(settlement_year >= 2022 and  settlement_year < 2500),
	settlement_month INTEGER NOT NULL CHECK(settlement_month >= 1 and settlement_month <= 12),
	orders_count INTEGER NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
	orders_total_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
	rate_avg NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (rate_avg >=0),
	order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
	courier_order_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
	courier_tips_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
	courier_reward_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0),
	CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month)
);