DROP SCHEMA IF EXISTS dds CASCADE;
CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dm_users (
	id serial PRIMARY KEY,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
	id serial PRIMARY KEY,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_products (
	id serial PRIMARY KEY,
	restaurant_id integer NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT dm_products_product_price_check CHECK ((product_price >= 0) AND (product_price < 999000000000.99)),
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants (id)
);

CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id serial PRIMARY KEY,
	ts timestamp UNIQUE NOT NULL,
	year smallint NOT NULL CONSTRAINT dm_timestamps_year_check CHECK ((year >= 2022) AND (year < 2500)),
	month smallint NOT NULL CONSTRAINT dm_timestamps_month_check CHECK ((month >= 1) AND (month <= 12)),
	day smallint NOT NULL CONSTRAINT dm_timestamps_day_check CHECK ((day >= 1) AND (day <= 31)),
	time time NOT NULL,
	date date NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id serial PRIMARY KEY,
	user_id integer NOT NULL,
	restaurant_id integer NOT NULL,
	timestamp_id integer NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users (id),
	CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants (id),
	CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps (id)
);

CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
	id serial PRIMARY KEY,
	product_id integer NOT NULL,
	order_id integer NOT NULL,
	count integer DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_count_check CHECK (count >= 0),
	price numeric(14,2) DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_price_check CHECK (price >= 0),
	total_sum numeric(14,2) DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_total_sum_check CHECK (total_sum >= 0),
	bonus_payment numeric(14,2) DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_bonus_payment_check CHECK (bonus_payment >= 0),
	bonus_grant numeric(14,2) DEFAULT 0 NOT NULL CONSTRAINT fct_product_sales_bonus_grant_check CHECK (bonus_grant >= 0),
	CONSTRAINT fct_product_sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES dds.dm_products (id),
	CONSTRAINT fct_product_sales_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders (id)
);


CREATE TABLE IF NOT EXISTS dds.dm_delivery_timestamps
(
	id serial PRIMARY KEY,
	ts timestamp UNIQUE NOT NULL,
	year smallint NOT NULL CONSTRAINT dm_timestamps_year_check CHECK ((year >= 2022) and (year < 2500)),
	month smallint NOT NULL CONSTRAINT dm_timestamps_month_check CHECK ((month >= 1) and (month <= 12)),
	day smallint NOT NULL CONSTRAINT dm_timestamps_day_check CHECK ((day >= 1) and (day <= 31)),
	time time NOT NULL,
	date date NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_couriers(
	id serial PRIMARY KEY,
	courier_id varchar(100) UNIQUE NOT NULL,
	courier_name varchar(100) NOT NULL
);


CREATE TABLE IF NOT EXISTS dds.dm_deliveries(
	id serial PRIMARY KEY,
	delivery_id varchar(100) UNIQUE NOT NULL,
	courier_id int NOT NULL,
	address varchar (255) NOT NULL,
	delivery_timestamp_id int NOT NULL,
	CONSTRAINT dm_deliveries_delivery_timestamp_id_fkey FOREIGN KEY (delivery_timestamp_id) REFERENCES dds.dm_delivery_timestamps(id),
	CONSTRAINT dm_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id)
	);

CREATE TABLE IF NOT EXISTS dds.fct_deliveries(
	id serial PRIMARY KEY,
	delivery_id int NOT NULL,
	order_id int NOT NULL,
	rate int4 NOT NULL CHECK (rate >= 1 and rate <= 5),
	"sum" numeric(14,2) NOT NULL DEFAULT 0 CHECK ("sum" >= 0),
	tip_sum numeric(14,2) NOT NULL DEFAULT 0 CHECK (tip_sum >= 0),
    CONSTRAINT fct_deliveries_delivery_id_fkey FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id),
    CONSTRAINT fct_deliveries_orders_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders (id)
);


CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

