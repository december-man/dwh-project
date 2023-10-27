-- BL_CL DML Wrappers:

-- CE_CUSTOMERS_SCD INSERT Procedure: 
CREATE OR REPLACE PROCEDURE bl_cl.ce_customers_scd_load()
AS $load_customers$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
	-- refresh WRK table:
	CALL bl_cl.wrk_customers_refresh();
	-- Original INSERT+UPDATE operation
	WITH src AS (
		SELECT 	DISTINCT customer_id AS cust_id, COALESCE(customer_name, 'n.a.') AS cust_name, COALESCE(gender, 'n.a.') AS cust_gender,
					COALESCE("age"::SMALLINT, -1) AS cust_age, 'BL_CL' AS source_system, 'WRK_CUSTOMERS' AS source_entity
		FROM bl_cl.wrk_customers
	UNION ALL
		SELECT DISTINCT customer_id, 'n.a.', 'n.a.', -1, 'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
		FROM sa_sales_invoices.src_invoices
	EXCEPT 
		SELECT customer_src_id, customer_name, customer_gender, customer_age, source_system, source_entity
		FROM bl_3nf.ce_customers_scd ccs
	), upd AS (
		UPDATE bl_3nf.ce_customers_scd AS cust
		SET 	end_dt = CURRENT_DATE,
				is_active = 'N'
		FROM src
		WHERE start_dt < CURRENT_DATE		AND -- this logic will not work with start dates later than the current date
				is_active = 'Y' 				AND
				src.cust_id = cust.customer_src_id AND src.source_system = cust.source_system
		RETURNING *
	)
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
	SELECT 	COALESCE(upd.customer_id, nextval('BL_3NF.SEQ_CE_CUSTOMERS_SCD')),
				COALESCE(src.cust_id, 'n.a.'),
				COALESCE(src.cust_name, 'n.a.'),
				COALESCE(src.cust_gender, 'n.a.'),
				COALESCE(src.cust_age, '-1')::SMALLINT,
				CURRENT_DATE,
				'31-12-9999'::DATE,
				'Y',
				CURRENT_DATE,  
				src.source_system,
				src.source_entity
	FROM src LEFT JOIN upd ON upd.customer_src_id = src.cust_id;
	-- Get row_count & context from insert
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
$load_customers$ LANGUAGE plpgsql;

-- Debug / Testing

CALL bl_cl.wrk_customers_refresh();
CALL bl_cl.ce_customers_scd_load();

SELECT COUNT(*) FROM bl_3nf.ce_customers_scd;
SELECT * FROM bl_cl.wrk_customers WHERE customer_id = 'C100006';
SELECT * FROM bl_cl.mta_load_logs;
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_src_id IN (SELECT customer_src_id FROM bl_3nf.ce_customers_scd WHERE is_active = 'N');
SELECT * FROM bl_3nf.ce_customers_scd WHERE is_active = 'N';
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_src_id = 'C112595';
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_id = '100625';
SELECT * FROM bl_dm.dim_customers_scd WHERE customer_src_id = '100625';
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_id = 4886;
SELECT * FROM bl_3nf.ce_sales WHERE customer_id = 4886;
SELECT * FROM bl_3nf.ce_sales WHERE customer_id = 7;
SELECT COUNT(*) FROM bl_3nf.ce_sales;
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
		