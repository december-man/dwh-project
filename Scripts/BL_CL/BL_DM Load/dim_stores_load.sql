

-- BL_CL DML Wrappers:

-- DIM_STORES SCD-0 Loading Procedure:

CREATE OR REPLACE PROCEDURE bl_cl.dim_stores_load() AS 
$load_stores_dim$
DECLARE 
-- Logging vars
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
	-- SCD-0 Insert logic
	INSERT INTO bl_dm.dim_stores (
		store_surr_id, store_src_id, store_name, store_location_lat, store_location_long,
		store_company_id, store_company_name, store_district_id, store_district_name,
		insert_dt, update_dt, source_system, source_entity 
	)
	SELECT 	nextval('BL_DM.SEQ_DIM_STORES'), store_id, store_name, store_location_lat, store_location_long,
				str.company_id, comp.company_name, str.district_id, dist.district_name, NOW(), NOW(), 'BL_3NF', 'CE_STORES'
	FROM bl_3nf.ce_stores str
		LEFT JOIN bl_3nf.ce_districts dist ON str.district_id = dist.district_id
		LEFT JOIN bl_3nf.ce_companies comp ON str.company_id = comp.company_id 
	WHERE str.store_id != -1 AND NOT EXISTS ( -- omit default row and already existing rows
		SELECT 1 FROM bl_dm.dim_stores WHERE store_src_id = str.store_id::VARCHAR);
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
$load_stores_dim$ LANGUAGE plpgsql;


CALL bl_cl.dim_stores_load();
SELECT setval('BL_DM.SEQ_DIM_STORES', 1, FALSE);
SELECT * FROM bl_3nf.ce_stores;
SELECT * FROM bl_dm.dim_stores;
SELECT * FROM bl_cl.mta_load_logs mll;

-- Deduplication on BL_3NF can be performed, but it has to be a business-driven decision.

