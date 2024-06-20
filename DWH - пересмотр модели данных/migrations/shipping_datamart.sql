CREATE OR REPLACE VIEW public.shipping_datamart AS
SELECT si.shipping_id,
	si.vendor_id,
	st.transfer_type,
	DATE_PART('day', ss.shipping_end_fact_datetime-ss.shipping_start_fact_datetime)::INT full_day_at_shipping,
	CASE
		WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN 1 ELSE 0
	END is_delay,
	CASE
		WHEN ss.status = 'finished' THEN 1 ELSE 0
	END is_shipping_finish,
	CASE
		WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN DATE_PART('day', ss.shipping_end_fact_datetime-si.shipping_plan_datetime)::INT ELSE 0
	END delay_day_at_shipping,
	si.payment_amount,
	si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) vat,
	si.payment_amount * sa.agreement_commission profit
FROM shipping_info si
LEFT JOIN shipping_transfer st ON si.shipping_transfer_id = st.id 
LEFT JOIN shipping_status ss  ON si.shipping_id=ss.shipping_id
LEFT JOIN shipping_country_rates scr ON si.shipping_country_rate_id = scr.id
LEFT JOIN shipping_agreement sa ON si.shipping_agreement_id = sa.agreement_id;
