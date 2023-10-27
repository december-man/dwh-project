-- BL_CL DML Wrappers:

-- CE_STORES INSERT Procedure: 
CREATE OR REPLACE PROCEDURE bl_cl.ce_stores_load()
AS $load_stores$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
-- Original INSERT operation
INSERT INTO bl_3nf.ce_stores (
	store_id,
	store_src_id,
	district_id,
	company_id,
	store_name,
	store_location_lat,
	store_location_long,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT 	nextval('BL_3NF.SEQ_CE_STORES'),
			COALESCE(src.shopping_mall, 'n.a.'),
			dist.district_id,
			comp.company_id,
			COALESCE(src.shopping_mall, 'n.a.'),
			COALESCE(src.lat, -1),
			COALESCE(src.long, -1),
			NOW(),
			NOW(),
			src.source_system,
			src.source_entity		
FROM ( 
	SELECT DISTINCT shopping_mall, district, company_name, lat::NUMERIC, long::NUMERIC, 'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
	FROM sa_sales_invoices.src_invoices
	UNION ALL
	SELECT DISTINCT shopping_mall, 'n.a.', 'n.a.', -1, -1, 'SA_SALES_CUSTOMERS_CC' AS source_system, 'SRC_CUSTOMERS_CC' AS source_entity
	FROM sa_sales_customers_cc.src_customers_cc
) AS src
	LEFT JOIN bl_3nf.ce_districts dist ON src.district = dist.district_src_id
	LEFT JOIN bl_3nf.ce_companies comp ON src.company_name = comp.company_src_id
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_stores WHERE store_src_id = src.shopping_mall AND source_system = src.source_system);
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
$load_stores$ LANGUAGE plpgsql;

-- Debug / Testing
CALL bl_cl.ce_stores_load();

TRUNCATE BL_3NF.CE_STORES RESTART IDENTITY CASCADE;
TRUNCATE BL_CL.mta_load_logs RESTART IDENTITY;

SELECT * FROM bl_3nf.ce_stores;
SELECT * FROM bl_cl.mta_load_logs;

