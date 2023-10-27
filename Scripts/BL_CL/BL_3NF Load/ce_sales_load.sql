
-- BL_CL DML Wrappers:

-- CE_SALES INSERT Procedure: 

CREATE OR REPLACE PROCEDURE bl_cl.ce_sales_load()
AS $load_sales$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
src_inv_last_load_ts TIMESTAMPTZ := (
	SELECT latest_load_ts FROM bl_cl.prm_mta_incremental_load WHERE target_table_name = 'CE_SALES' AND 
		src_table_name = 'SRC_INVOICES'
	);
src_ccc_last_load_ts TIMESTAMPTZ := (
	SELECT latest_load_ts FROM bl_cl.prm_mta_incremental_load WHERE target_table_name = 'CE_SALES' AND 
		src_table_name = 'SRC_CUSTOMERS_CC'
	);
BEGIN
	-- Original INSERT operation
	INSERT INTO bl_3nf.ce_sales(
		sale_id,
		sale_src_id,
		event_dt,
		store_id,
		customer_id,
		coupon_id,
		category_id,
		payment_method_id,
		quantity_cnt,
		price_liras,
		discount_liras,
		cost_liras,
		revenue_liras,
		payment_amount_liras,
		insert_dt,
		source_system,
		source_entity
	)
		SELECT 	nextval('BL_3NF.SEQ_CE_SALES'),
					COALESCE(invoice_no, 'n.a.'),
					COALESCE("timestamp", '1-1-1900')::TIMESTAMP,
					stores.store_id, cust.customer_id, coup.coupon_id, cat.category_id, pm.payment_method_id, -- FKs
					COALESCE(quantity, -1),
					price, discount, costs, revenue, payment_amount, -- metrics
					NOW(),
					src.source_system,
					src.source_entity
		FROM (
			SELECT 	invoice_no, "timestamp"::TIMESTAMP,
						shopping_mall, customer_id, coupon_id, category, payment_method, -- FKs
						quantity::INT, price::NUMERIC, discount::NUMERIC, costs::NUMERIC, revenue::NUMERIC,
						price::NUMERIC*quantity::INT AS payment_amount, -- metrics
						'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
			FROM sa_sales_invoices.src_invoices
			WHERE refresh_dt > src_inv_last_load_ts
		UNION ALL
			SELECT 	'n.a.' AS invoice_no, "timestamp"::TIMESTAMP,
						shopping_mall, customer_id, 'n.a.' AS coupon_id, 'n.a.' AS category, payment_method, -- FKs
						-1 AS quantity, -1 AS price, -1 AS discount, -1 AS costs, -1 AS revenue, payment_amount::NUMERIC, -- metrics
						'SA_SALES_CUSTOMERS_CC' AS source_system, 'SRC_CUSTOMERS_CC' AS source_entity
			FROM sa_sales_customers_cc.src_customers_cc
			WHERE refresh_dt > src_ccc_last_load_ts
		) AS src
				LEFT JOIN bl_3nf.ce_stores stores 			ON src.shopping_mall = stores.store_src_id
				LEFT JOIN bl_3nf.ce_customers_scd cust 	ON src.customer_id = cust.customer_src_id
				LEFT JOIN bl_3nf.ce_coupons coup 			ON src.coupon_id = coup.coupon_src_id
				LEFT JOIN bl_3nf.ce_categories cat 			ON src.category = cat.category_src_id
				LEFT JOIN bl_3nf.ce_payment_methods pm 	ON src.payment_method = pm.payment_method_src_id
		WHERE (stores.source_system 	= src.source_system OR stores.source_system = 'MANUAL') 	AND
				(pm.source_system 		= src.source_system OR pm.source_system = 'MANUAL')		AND
				(cat.source_system 		= src.source_system OR cat.source_system = 'MANUAL') 		AND
				(coup.source_system 		= src.source_system OR coup.source_system = 'MANUAL')		AND
				(
				(cust.source_system = 'BL_CL' AND src.source_system = 'SA_SALES_CUSTOMERS_CC') OR
				(cust.source_system = 'SA_SALES_INVOICES' AND src.source_system = 'SA_SALES_INVOICES')
				) AND
				cust.is_active = 'Y';
-- Logging
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		row_count := ROW_COUNT;
		context_short := SUBSTRING(context FROM 'function (.*?) line');
	-- call logger on successful insert
	CALL bl_cl.load_logger(context_short, row_count);
	CALL bl_cl.iload_data('SRC_CUSTOMERS_CC'::TEXT, 'CE_SALES'::TEXT, 'ce_sales_load()', NOW());
	CALL bl_cl.iload_data('SRC_INVOICES'::TEXT, 'CE_SALES'::TEXT, 'ce_sales_load()', NOW());
EXCEPTION WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS
		err_context := PG_EXCEPTION_CONTEXT,
		err_detail 	:= PG_EXCEPTION_DETAIL;
		err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');
	-- call logger in case of exception
	CALL bl_cl.load_logger(err_context_short, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
	RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END; 
$load_sales$ LANGUAGE plpgsql;

-- Debug / Testing
CALL bl_cl.ce_sales_load();

CALL BL_CL.SRC_INVOICES_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/EXT_INVOICES_900K.csv');
CALL BL_CL.SRC_CUSTOMERS_CC_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/EXT_CUSTOMERS_CC_450K.csv');

CALL BL_CL.SRC_INVOICES_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/Increments/EXT_INVOICES_100K.csv');
CALL BL_CL.SRC_CUSTOMERS_CC_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/Increments/EXT_CUSTOMERS_CC_50K.csv');

TRUNCATE BL_3NF.CE_SALES RESTART IDENTITY;
SELECT setval('BL_3NF.SEQ_CE_SALES', 1, FALSE);
SET join_collapse_limit TO 1;
SHOW join_collapse_limit;
TRUNCATE BL_CL.mta_load_logs;

SELECT COUNT(*) FROM sa_sales_customers_cc.src_customers_cc scc;
SELECT COUNT(*),refresh_dt FROM sa_sales_customers_cc.src_customers_cc scc GROUP BY refresh_dt;
SELECT COUNT(*),refresh_dt FROM sa_sales_invoices.src_invoices GROUP BY refresh_dt;
SELECT COUNT(*) FROM sa_sales_invoices.src_invoices si;
SELECT * FROM bl_3nf.ce_sales;
SELECT * FROM bl_cl.mta_load_logs;
SELECT * FROM bl_cl.prm_mta_incremental_load;








