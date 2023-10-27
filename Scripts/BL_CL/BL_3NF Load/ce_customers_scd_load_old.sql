-- CE_CUSTOMERS_SCD INSERT Procedure: 
CREATE OR REPLACE PROCEDURE bl_cl.ce_customers_scd_load_old()
AS $load_customers$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
	-- refresh WRK table:
	CALL bl_cl.wrk_customers_refresh();
	-- Original INSERT+UPDATE operation
	INSERT INTO bl_3nf.ce_customers_scd (
	customer_id,
	customer_src_id,
	customer_name,
	customer_gender,
	customer_age,
	start_dt,
	end_dt,
	is_active,
	insert_dt,
	source_system,
	source_entity
	)
	SELECT 	COALESCE(ce_cust.customer_id, nextval('BL_3NF.SEQ_CE_CUSTOMERS_SCD')),
				COALESCE(src.customer_id, 'n.a.'),
				COALESCE(src.customer_name, 'n.a.'),
				COALESCE(src.gender, 'n.a.'),
				COALESCE(src.age, '-1')::SMALLINT,
				CURRENT_DATE - INTERVAL '3 days',
				'31-12-9999'::DATE,
				'Y',
				CURRENT_DATE - INTERVAL '3 days',
				src.source_system,
				src.source_entity
	FROM (
		SELECT DISTINCT customer_id, customer_name, gender, "age"::SMALLINT, 'BL_CL' AS source_system, 'WRK_CUSTOMERS' AS source_entity
		FROM bl_cl.wrk_customers
		UNION ALL
		SELECT DISTINCT customer_id, 'n.a.', 'n.a.', -1, 'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
		FROM sa_sales_invoices.src_invoices
	) AS src
		LEFT JOIN bl_3nf.ce_customers_scd ce_cust ON src.customer_id = ce_cust.customer_src_id
	WHERE ce_cust.customer_id IS NULL OR -- newly added rows will have a NULL customer_id
			ce_cust.source_system = src.source_system AND ( -- sniff for any changes in older rows
				(ce_cust.customer_name 	!= src.customer_name AND src.customer_name IS NOT NULL) 	OR
				ce_cust.customer_gender != src.gender	OR
				ce_cust.customer_age 	!= src."age");
	-- Get row_count & context from insert
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		row_count := ROW_COUNT;
		context_short := SUBSTRING(context FROM 'function (.*?) line');
	-- Updating older rows to have a False `is_active` flag and an actual `end_dt`.
	UPDATE bl_3nf.ce_customers_scd
	SET 	end_dt = CURRENT_DATE,
			is_active = 'N'
	WHERE start_dt < CURRENT_DATE		AND -- this logic will not work with start dates later than the current date
			is_active = 'Y' 				AND
			customer_id 					IN (SELECT customer_id FROM bl_3nf.ce_customers_scd WHERE start_dt >= CURRENT_DATE);
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
$load_customers$ LANGUAGE plpgsql;


CALL bl_cl.wrk_customers_refresh();
CALL bl_cl.ce_customers_scd_load_old();
SELECT * FROM bl_cl.mta_load_logs mll;
SELECT COUNT(DISTINCT customer_id) FROM bl_cl.wrk_customers wc;
SELECT COUNT(*) FROM sa_sales_invoices.src_invoices si;
SELECT COUNT(*) FROM sa_sales_customers_cc.src_customers_cc scc;
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_src_id IN (SELECT customer_src_id FROM bl_3nf.ce_customers_scd WHERE is_active = 'N');
SELECT * FROM bl_3nf.ce_customers_scd WHERE is_active = 'N';
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_src_id = 'C144924';
-- deduplication checks
SELECT COUNT(*), customer_src_id, customer_name, customer_gender, customer_age,
		start_dt, end_dt, source_system, source_entity 
	FROM bl_3nf.ce_customers_scd WHERE is_active = 'Y'
	GROUP BY customer_src_id, customer_name, customer_gender, customer_age,
		start_dt, end_dt, source_system, source_entity
	HAVING COUNT(*) > 1;

SELECT COUNT(*), customer_src_id, source_system, source_entity 
	FROM bl_3nf.ce_customers_scd WHERE is_active = 'Y'
	GROUP BY customer_src_id, source_system, source_entity
	HAVING COUNT(*) > 1;

SELECT 	COUNT(DISTINCT customer_src_id) FILTER (WHERE source_system = 'BL_CL'),
			COUNT(DISTINCT customer_src_id) FILTER (WHERE source_system = 'SA_SALES_INVOICES'),
			COUNT(DISTINCT customer_src_id)
FROM bl_3nf.ce_customers_scd;
			
SELECT COUNT(DISTINCT customer_id) FROM sa_sales_customers_cc.src_customers_cc scc;
SELECT COUNT(DISTINCT customer_id) FROM sa_sales_invoices.src_invoices si;

			
SELECT COUNT(*) FROM (
		SELECT 	DISTINCT customer_id AS cust_id, COALESCE(customer_name, 'n.a.') AS cust_name, COALESCE(gender, 'n.a.') AS cust_gender,
					COALESCE("age"::SMALLINT, -1) AS cust_age, 'BL_CL' AS source_system, 'WRK_CUSTOMERS' AS source_entity
		FROM bl_cl.wrk_customers
	UNION ALL
		SELECT DISTINCT customer_id, 'n.a.', 'n.a.', -1, 'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
		FROM sa_sales_invoices.src_invoices
	EXCEPT
		SELECT customer_src_id, customer_name, customer_gender, customer_age, source_system, source_entity
		FROM bl_3nf.ce_customers_scd ccs) T1;
			
			