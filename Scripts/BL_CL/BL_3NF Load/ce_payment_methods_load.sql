-- BL_CL DML Wrappers:

-- CE_PAYMENT_METHODS INSERT Procedure: 
CREATE OR REPLACE PROCEDURE bl_cl.ce_payment_methods_load()
AS $load_payment_methods$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
-- Original INSERT operation
INSERT INTO bl_3nf.ce_payment_methods (
	payment_method_id,
	payment_method_src_id,
	payment_method_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT 	nextval('BL_3NF.SEQ_CE_PAYMENT_METHODS'),
			COALESCE(src.payment_method, 'n.a.'),
			COALESCE(src.payment_method, 'n.a.'),
			NOW(),
			NOW(),
			src.source_system,
			src.source_entity
FROM (
	SELECT DISTINCT payment_method, 'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
	FROM sa_sales_invoices.src_invoices
	UNION ALL
	SELECT  DISTINCT payment_method, 'SA_SALES_CUSTOMERS_CC' AS source_system, 'SRC_CUSTOMERS_CC' AS source_entity
	FROM sa_sales_customers_cc.src_customers_cc
) AS src
WHERE NOT EXISTS (
	SELECT 1 FROM bl_3nf.ce_payment_methods WHERE payment_method_src_id = src.payment_method AND source_system = src.source_system			
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
$load_payment_methods$ LANGUAGE plpgsql;

-- Debug / Testing
CALL bl_cl.ce_payment_methods_load();

TRUNCATE BL_3NF.CE_PAYMENT_METHODS RESTART IDENTITY CASCADE;
TRUNCATE BL_CL.mta_load_logs RESTART IDENTITY;
DELETE FROM BL_3NF.CE_PAYMENT_METHODS WHERE payment_method_name = 'Cash'; 

SELECT * FROM bl_3nf.ce_payment_methods;
SELECT * FROM bl_cl.mta_load_logs;

