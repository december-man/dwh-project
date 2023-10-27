-- BL_CL DML Wrappers:

-- CE_DISTRICTS INSERT Procedure: 
CREATE OR REPLACE PROCEDURE bl_cl.ce_districts_load()
AS $load_districts$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
-- Original INSERT operation
INSERT INTO bl_3nf.ce_districts (
	district_id,
	district_src_id,
	district_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT 	nextval('BL_3NF.SEQ_CE_DISTRICTS'),
			COALESCE(src_inv.district, 'n.a.'),
			COALESCE(src_inv.district, 'n.a.'),
			NOW(),
			NOW(),
			'SA_SALES_INVOICES',
			'SRC_INVOICES'
FROM sa_sales_invoices.src_invoices AS src_inv
WHERE NOT EXISTS (SELECT district_src_id FROM bl_3nf.ce_districts WHERE district_src_id = src_inv.district)
GROUP BY src_inv.district; -- add unique districts 
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
$load_districts$ LANGUAGE plpgsql;

-- Debug / Testing
CALL bl_cl.ce_districts_load();

TRUNCATE BL_3NF.CE_DISTRICTS RESTART IDENTITY CASCADE;
TRUNCATE BL_CL.mta_load_logs RESTART IDENTITY;
DELETE FROM BL_3NF.CE_DISTRICTS WHERE district_name = 'Fatih'; 

SELECT * FROM bl_3nf.ce_districts;
SELECT * FROM bl_cl.mta_load_logs;

