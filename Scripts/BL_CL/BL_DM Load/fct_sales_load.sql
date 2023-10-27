
-- BL_DM Fact table loading procedure

CREATE OR REPLACE PROCEDURE bl_cl.fct_sales_load()
AS $load_customers_dim$
DECLARE 
-- Logging vars
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
halfyear TEXT;
BEGIN
	-- Partition Management
	FOR halfyear IN (SELECT DISTINCT TO_CHAR(event_dt, 'YYYY_MM') FROM BL_3NF.ce_sales WHERE EXTRACT(month FROM event_dt) IN (1, 7)) LOOP
		RAISE NOTICE 'Building Partitions... %', halfyear;
		EXECUTE 
			'CREATE TABLE IF NOT EXISTS BL_DM.FCT_SALES_MINUTES_'|| halfyear ||' 
			PARTITION OF BL_DM.FCT_SALES_MINUTES
			FOR VALUES FROM ('||quote_literal(CONCAT(halfyear,'_01'))||'::DATE) TO 
			('||quote_literal(CONCAT(halfyear,'_01'))||'::DATE + INTERVAL '|| quote_literal('6 Months') ||');';
	END LOOP;
	RAISE NOTICE 'Inserting Fresh data from 3NF ../..';
	INSERT INTO bl_dm.fct_sales_minutes (
		event_dt,
		event_minutes,
		sale_surr_id,
		customer_surr_id,
		store_surr_id,
		coupon_surr_id,
		category_surr_id,
		payment_method_surr_id,
		quantity_cnt,
		fct_price_liras, fct_discount_liras, fct_cost_liras, fct_revenue_liras, fct_payment_amount_liras,
		insert_dt, sale_src_id, source_system, source_entity
	)
	SELECT  	event_dt,
				event_minutes,
				nextval('BL_DM.SEQ_FCT_SALES_MINUTES'),
				customer_surr_id,
				store_surr_id,
				coupon_surr_id,
				category_surr_id,
				payment_method_surr_id,
				quantity_cnt,
				fct_price_liras, fct_discount_liras, fct_cost_liras, fct_revenue_liras, fct_payment_amount_liras,
				NOW(), sale_src_id, source_system, source_entity 
	FROM (
		SELECT	event_dt::DATE AS event_dt,
					EXTRACT(HOUR FROM event_dt)*60 + EXTRACT(MINUTE FROM event_dt) AS event_minutes,
					COALESCE(cust.customer_surr_id, -1) 									AS customer_surr_id,
					COALESCE(str.store_surr_id, -1) 											AS store_surr_id,
					COALESCE(coup.coupon_surr_id, -1) 										AS coupon_surr_id,
					COALESCE(cat.category_surr_id, -1) 										AS category_surr_id,
					COALESCE(pm.payment_method_surr_id, -1) 								AS payment_method_surr_id,
					quantity_cnt 				AS quantity_cnt,
					price_liras 				AS fct_price_liras,
					discount_liras 			AS fct_discount_liras,
					cost_liras 					AS fct_cost_liras,
					revenue_liras 				AS fct_revenue_liras,
					payment_amount_liras 	AS fct_payment_amount_liras,
					sale_id 						AS sale_src_id,
					'BL_3NF' 					AS source_system,
					'CE_SALES'					AS source_entity
		FROM bl_3nf.ce_sales sls
			LEFT JOIN bl_dm.dim_customers_scd cust ON sls.customer_id::VARCHAR = cust.customer_src_id
			LEFT JOIN bl_dm.dim_stores str 			ON sls.store_id::VARCHAR = str.store_src_id
			LEFT JOIN bl_dm.dim_coupons coup 		ON sls.coupon_id::VARCHAR = coup.coupon_src_id
			LEFT JOIN bl_dm.dim_categories cat 		ON sls.category_id::VARCHAR = cat.category_src_id
			LEFT JOIN bl_dm.dim_payment_methods pm ON sls.payment_method_id::VARCHAR = pm.payment_method_src_id
		WHERE	cust.is_active = 'Y'
		EXCEPT
		SELECT	event_dt, event_minutes, customer_surr_id, store_surr_id, coupon_surr_id, category_surr_id,
					payment_method_surr_id, quantity_cnt, fct_price_liras, fct_discount_liras, fct_cost_liras,
					fct_revenue_liras, fct_payment_amount_liras, sale_src_id::INT, source_system, source_entity
		FROM bl_dm.fct_sales_minutes) AS src	
	ORDER BY event_dt;
-- Logging
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		row_count := ROW_COUNT;
		context_short := SUBSTRING(context FROM 'function (.*?) line');
	-- call logger on successful insert
	CALL bl_cl.load_logger(context_short, row_count);
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS
			err_context := PG_EXCEPTION_CONTEXT,
			err_detail 	:= PG_EXCEPTION_DETAIL;
			err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');
	-- call logger in case of exception
		CALL bl_cl.load_logger(err_context_short, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
		RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END; 
$load_customers_dim$ LANGUAGE plpgsql;


-- Vacuuming tables is a must, in case of wipe+load logic.

CALL bl_cl.fct_sales_load();
SELECT COUNT(*) FROM bl_dm.dim_customers_scd dcs;
SELECT COUNT(*) FROM bl_dm.fct_sales_minutes;
SELECT * FROM bl_dm.dim_stores;
SELECT * FROM bl_3nf.ce_stores;
SELECT * FROM bl_cl.mta_load_logs mll;
VACUUM bl_dm.fct_sales_minutes;

SELECT * FROM sa_sales_invoices.src_invoices WHERE customer_id = 'C593101' ORDER BY timestamp;
SELECT * FROM bl_3nf.ce_sales WHERE customer_id = 104408;
SELECT * FROM bl_dm.fct_sales_minutes WHERE customer_surr_id = 104408;
SELECT * FROM bl_dm.dim_customers_scd dcs WHERE customer_surr_id = 104408;
SELECT * FROM bl_dm.dim_customers_scd dcs WHERE customer_src_id::INT = 104408;
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_id = 104408;
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_src_id = 'C593101';

	SELECT  	COUNT(*)
	FROM (
		SELECT	event_dt::DATE AS event_dt,
					EXTRACT(HOUR FROM event_dt)*60 + EXTRACT(MINUTE FROM event_dt) AS event_minutes,
					COALESCE(cust.customer_surr_id, -1) 									AS customer_surr_id,
					COALESCE(str.store_surr_id, -1) 											AS store_surr_id,
					COALESCE(coup.coupon_surr_id, -1) 										AS coupon_surr_id,
					COALESCE(cat.category_surr_id, -1) 										AS category_surr_id,
					COALESCE(pm.payment_method_surr_id, -1) 								AS payment_method_surr_id,
					quantity_cnt 				AS quantity_cnt,
					price_liras 				AS fct_price_liras,
					discount_liras 			AS fct_discount_liras,
					cost_liras 					AS fct_cost_liras,
					revenue_liras 				AS fct_revenue_liras,
					payment_amount_liras 	AS fct_payment_amount_liras,
					sale_id 						AS sale_src_id,
					'BL_3NF' 					AS source_system,
					'CE_SALES'					AS source_entity
		FROM bl_3nf.ce_sales sls
			LEFT JOIN bl_dm.dim_customers_scd cust ON sls.customer_id::VARCHAR = cust.customer_src_id
			LEFT JOIN bl_dm.dim_stores str 			ON sls.store_id::VARCHAR = str.store_src_id
			LEFT JOIN bl_dm.dim_coupons coup 		ON sls.coupon_id::VARCHAR = coup.coupon_src_id
			LEFT JOIN bl_dm.dim_categories cat 		ON sls.category_id::VARCHAR = cat.category_src_id
			LEFT JOIN bl_dm.dim_payment_methods pm ON sls.payment_method_id::VARCHAR = pm.payment_method_src_id
		WHERE is_active = 'Y'
		EXCEPT
		SELECT	event_dt, event_minutes, customer_surr_id, store_surr_id, coupon_surr_id, category_surr_id,
					payment_method_surr_id, quantity_cnt, fct_price_liras, fct_discount_liras, fct_cost_liras,
					fct_revenue_liras, fct_payment_amount_liras, sale_src_id::INT, source_system, source_entity
		FROM bl_dm.fct_sales_minutes) AS src;
	
	
	
			SELECT COUNT(*) FROM bl_3nf.ce_sales sls
			LEFT JOIN bl_dm.dim_customers_scd cust ON sls.customer_id::VARCHAR = cust.customer_src_id
			LEFT JOIN bl_dm.dim_stores str 			ON sls.store_id::VARCHAR = str.store_src_id
			LEFT JOIN bl_dm.dim_coupons coup 		ON sls.coupon_id::VARCHAR = coup.coupon_src_id
			LEFT JOIN bl_dm.dim_categories cat 		ON sls.category_id::VARCHAR = cat.category_src_id
			LEFT JOIN bl_dm.dim_payment_methods pm ON sls.payment_method_id::VARCHAR = pm.payment_method_src_id
					WHERE is_active = 'Y';
	