-- BL_CL Dimension loading procedures

-- DIM_CUSTOMERS_SCD Loading procedure
-- Simple Anti-join that additionally pulls old versions of rows.
CREATE OR REPLACE PROCEDURE bl_cl.dim_customers_scd_load()
AS $load_customers_dim$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
WITH src AS( 
	SELECT customer_id::VARCHAR, customer_name, customer_gender, customer_age, start_dt, end_dt, is_active 
	FROM bl_3nf.ce_customers_scd ccs WHERE customer_id != -1
	EXCEPT 
	SELECT customer_src_id, customer_name, customer_gender, customer_age, start_dt, end_dt, is_active
	FROM bl_dm.dim_customers_scd
), upd AS (
	UPDATE bl_dm.dim_customers_scd AS cust
	SET 	end_dt = CURRENT_DATE,
			is_active = 'N'
	FROM src
	WHERE src.start_dt < CURRENT_DATE	AND -- this logic will not work with start dates later than the current date
			src.is_active = 'N' 				AND
			src.customer_id::VARCHAR = cust.customer_src_id
)
INSERT INTO bl_dm.dim_customers_scd (
	customer_surr_id,
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
SELECT 	nextval('BL_DM.SEQ_DIM_CUSTOMERS_SCD'),
			COALESCE(src.customer_id, 'n.a.'),
			COALESCE(src.customer_name, 'n.a.'),
			COALESCE(src.customer_gender, 'n.a.'),
			COALESCE(src.customer_age, '-1')::SMALLINT,
			src.start_dt,
			src.end_dt,
			src.is_active,
			CURRENT_DATE,  
			'BL_3NF',
			'CE_CUSTOMERS_SCD'
FROM src
WHERE is_active = 'Y';
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

-- gotta think of vacuuming tables in case of wipe+load logic.
VACUUM bl_dm.dim_customers_scd;

-- testing / debug:

CALL bl_cl.dim_customers_scd_load();

SELECT * FROM bl_cl.mta_load_logs;
TRUNCATE bl_dm.dim_customers_scd RESTART IDENTITY CASCADE;

SELECT COUNT(*) FROM bl_3nf.ce_customers_scd;
SELECT COUNT(*) FROM bl_dm.dim_customers_scd dcs;
SELECT * FROM bl_3nf.ce_customers_scd cust WHERE customer_src_id = 'C101173';
SELECT * FROM bl_dm.dim_customers_scd cust WHERE customer_src_id = '4886';
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_src_id IN (SELECT customer_src_id FROM bl_3nf.ce_customers_scd WHERE is_active = 'N');
SELECT * FROM bl_3nf.ce_customers_scd WHERE is_active = 'N';
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_src_id = 'C112595';
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_id = 73230;
SELECT * FROM bl_dm.dim_customers_scd WHERE customer_src_id = '73230';
SELECT * FROM bl_dm.dim_customers_scd WHERE customer_src_id IN (
SELECT customer_src_id FROM bl_dm.dim_customers_scd WHERE is_active = 'N');
SELECT * FROM bl_3nf.ce_customers_scd WHERE start_dt::TEXT ~~ '2023-08-28';
SELECT * FROM bl_3nf.ce_customers_scd WHERE customer_id = 4886;
SELECT * FROM bl_3nf.ce_sales WHERE customer_id = 4886;
SELECT * FROM bl_3nf.ce_sales WHERE customer_id = 7;
