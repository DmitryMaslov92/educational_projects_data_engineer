DROP TABLE IF EXISTS public.shipping_status;
DROP TABLE IF EXISTS public.shipping_info;
DROP TABLE IF EXISTS public.shipping_country_rates;
DROP TABLE IF EXISTS public.shipping_agreement;
DROP TABLE IF EXISTS public.shipping_transfer;

-- Создание таблиц
-- shipping_country_rates
CREATE TABLE public.shipping_country_rates(
	id SERIAL,
	shipping_country VARCHAR(20),
	shipping_country_base_rate NUMERIC(14,3),
	PRIMARY KEY (id)
);
--shipping_agreement
CREATE TABLE public.shipping_agreement(
	agreement_id INT8,
	agreement_number VARCHAR(20),
	agreement_rate NUMERIC(14,2),
	agreement_commission NUMERIC(14,2),
	PRIMARY KEY (agreement_id)
	
);
-- shipping_transfer
CREATE TABLE public.shipping_transfer(
	id SERIAL,
	transfer_type VARCHAR(10),
	transfer_model VARCHAR(20),
	shipping_transfer_rate NUMERIC(14,3),
	PRIMARY KEY (id)
);
--shipping_info
CREATE TABLE public.shipping_info(
	shipping_id INT8,
	vendor_id INT8,
	payment_amount NUMERIC(14, 2),
	shipping_plan_datetime TIMESTAMP,
	shipping_transfer_id INT8,
	shipping_agreement_id INT8,
	shipping_country_rate_id INT8,
	PRIMARY KEY (shipping_id),
	FOREIGN KEY (shipping_transfer_id) REFERENCES public.shipping_transfer(id) ON UPDATE cascade,
	FOREIGN KEY (shipping_agreement_id) REFERENCES public.shipping_agreement(agreement_id) ON UPDATE cascade,
	FOREIGN KEY (shipping_country_rate_id) REFERENCES public.shipping_country_rates(id) ON UPDATE cascade
);
-- shipping_status
CREATE TABLE public.shipping_status(
	shipping_id INT8,
	status VARCHAR(20),
	state VARCHAR(20),
	shipping_start_fact_datetime TIMESTAMP,
	shipping_end_fact_datetime TIMESTAMP,
	PRIMARY KEY (shipping_id)
);


-- Заполнение таблиц
-- shipping_country_rates
INSERT INTO public.shipping_country_rates(shipping_country, shipping_country_base_rate)
SELECT DISTINCT shipping_country, shipping_country_base_rate
FROM public.shipping s
ORDER BY shipping_country;

-- shipping_agreement
WITH distinct_vad AS (
	SELECT distinct vendor_agreement_description
	FROM public.shipping s
)
INSERT INTO public.shipping_agreement
SELECT descr[1]::INT as agreement_id,
	descr[2] as agreement_number,
	descr[3]::NUMERIC(14,2) AS agreement_rate,
	descr[4]::NUMERIC(14,2) AS agreement_commission
FROM (
	SELECT regexp_split_to_array(vendor_agreement_description, ':+') AS descr
	FROM distinct_vad) AS vad
ORDER BY agreement_id;

-- shipping_transfer
WITH distinct_tdr AS (
	SELECT distinct shipping_transfer_description, shipping_transfer_rate
	FROM public.shipping s
)
INSERT INTO public.shipping_transfer(transfer_type, transfer_model, shipping_transfer_rate)
SELECT descr[1] AS transfer_type,
	descr[2] AS transfer_model,
	shipping_transfer_rate
FROM (
	SELECT regexp_split_to_array(shipping_transfer_description, ':+') AS descr,
	shipping_transfer_rate
	FROM distinct_tdr) AS tdr
ORDER BY transfer_type, transfer_model;

-- shipping_info
INSERT INTO shipping_info
SELECT DISTINCT s.shippingid,
	s.vendorid,
	s.payment_amount,
	s.shipping_plan_datetime,
	st.id as transfer_id,
	(regexp_split_to_array(s.vendor_agreement_description, ':'))[1]::INT8 as agreement_id,
	scr.id as shipping_country_rate_id
FROM public.shipping s
LEFT JOIN shipping_transfer st ON s.shipping_transfer_description = (st.transfer_type || ':' || st.transfer_model)
LEFT JOIN shipping_country_rates scr ON s.shipping_country = scr.shipping_country 
ORDER BY shippingid;

-- shipping_status
WITH shipping_time AS (
	SELECT shippingid, 
		min(CASE WHEN state = 'booked' THEN state_datetime END) AS shipping_start_fact_datetime,
		max(CASE WHEN state = 'recieved' THEN state_datetime END) AS shipping_end_fact_datetime 
	FROM shipping
	GROUP BY shippingid 
)
INSERT INTO public.shipping_status 
SELECT distinct s.shippingid, 
	FIRST_VALUE (s.status) OVER (PARTITION BY s.shippingid ORDER BY s.state_datetime DESC),
	FIRST_VALUE (s.state) OVER (PARTITION BY s.shippingid ORDER BY s.state_datetime DESC),
	st.shipping_start_fact_datetime, 
	st.shipping_end_fact_datetime
FROM shipping s
LEFT JOIN shipping_time st ON s.shippingid = st.shippingid
ORDER BY shippingid ASC;
