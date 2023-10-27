

-- BL_CL DML Wrappers:

-- DIM_PAYMENT_METHODS SCD-0 Loading Procedure:

CREATE OR REPLACE PROCEDURE bl_cl.dim_payment_methods_load()
AS $load_pm_dim$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
	INSERT INTO bl_dm.dim_payment_methods (
		payment_method_surr_id, payment_method_src_id, payment_method_name,
		insert_dt, update_dt, source_system, source_entity
		)
		SELECT 	nextval('BL_DM.SEQ_DIM_PAYMENT_METHODS'),
					payment_method_id, payment_method_name, NOW(), NOW(), 'BL_3NF', 'CE_PAYMENT_METHODS' 
		FROM bl_3nf.ce_payment_methods ce_pm
		WHERE payment_method_id != -1 AND NOT EXISTS (
			SELECT 1 FROM bl_dm.dim_payment_methods 
			WHERE payment_method_src_id = ce_pm.payment_method_id::TEXT
		);
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
$load_pm_dim$ LANGUAGE plpgsql;


-- testing / debug
CALL bl_cl.ce_payment_methods_load();
CALL bl_cl.dim_payment_methods_load();

SELECT * FROM bl_3nf.ce_payment_methods;
SELECT * FROM bl_dm.dim_payment_methods;
SELECT * FROM bl_cl.mta_load_logs mll;

TRUNCATE bl_dm.dim_payment_methods RESTART IDENTITY CASCADE;



