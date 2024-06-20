DROP SCHEMA IF EXISTS stg CASCADE;
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL,
	min_payment_threshold numeric(19, 5) NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox__event_ts ON stg.bonussystem_events USING btree (event_ts);

CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
	id serial PRIMARY KEY,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts TIMESTAMP NOT NULL,
	CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id)
);

CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
	id serial PRIMARY KEY,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts TIMESTAMP NOT NULL,
	CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id)
);

CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
	id serial PRIMARY KEY,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts TIMESTAMP NOT NULL,
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id)
);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_restaurants(
    id serial,
    restaurant_id varchar NOT NULL,
    name varchar NOT NULL,
    CONSTRAINT deliverysystem_restaurants_restaurant_id_unique UNIQUE (restaurant_id)
);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers(
    id serial,
    courier_id varchar NOT NULL,
    name varchar NOT NULL,
    CONSTRAINT deliverysystem_couriers_courier_id_unique UNIQUE (courier_id)
);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries(
    id serial,
    order_id varchar NOT NULL,
    order_ts TIMESTAMP NOT NULL,
    delivery_id varchar NOT NULL,
    courier_id varchar NOT NULL,
    address varchar NOT NULL,
    delivery_ts TIMESTAMP NOT NULL,
    rate int, 
    sum numeric(14,2) NOT NULL, 
    tip_sum numeric(14,2) NOT NULL,
    CONSTRAINT deliverysystem_deliveries_order_id_unique UNIQUE (order_id)
);

CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);
