-- BL_CL DML Wrappers:

-- CE_COMPANIES INSERT Function: 
CREATE OR REPLACE FUNCTION bl_cl.ce_companies_load() RETURNS SETOF bl_3nf.ce_companies
AS $load_companies$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
-- Original INSERT operation
	RETURN QUERY
		INSERT INTO bl_3nf.ce_companies (
			company_id,
			company_src_id,
			company_name,
			insert_dt,
			update_dt,
			source_system,
			source_entity
		)
		SELECT 	nextval('BL_3NF.SEQ_CE_COMPANIES'),
					COALESCE(src_inv.company_name, 'n.a.'),
					COALESCE(src_inv.company_name, 'n.a.'),
					NOW(),
					NOW(),
					'SA_SALES_INVOICES',
					'SRC_INVOICES'
		FROM sa_sales_invoices.src_invoices AS src_inv
		WHERE NOT EXISTS (SELECT company_src_id FROM bl_3nf.ce_companies WHERE company_src_id = src_inv.company_name)
		GROUP BY src_inv.company_name
		ORDER BY src_inv.company_name
		RETURNING *;
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
		RAISE WARNING 'STATE: %, ERRM: %, DET: %', SQLSTATE, SQLERRM, err_detail;
END; 
$load_companies$ LANGUAGE plpgsql;

-- Debug / Testing
SELECT bl_cl.ce_companies_load();

TRUNCATE BL_3NF.CE_COMPANIES RESTART IDENTITY CASCADE;
TRUNCATE BL_CL.mta_load_logs RESTART IDENTITY;

SELECT * FROM bl_3nf.ce_companies;
SELECT * FROM bl_cl.mta_load_logs;


-- CE_COMPANIES_LOAD() PROCEDURE
CREATE OR REPLACE PROCEDURE bl_cl.ce_companies_load() AS
$load_companies$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
-- Original INSERT operation
	INSERT INTO bl_3nf.ce_companies (
		company_id,
		company_src_id,
		company_name,
		insert_dt,
		update_dt,
		source_system,
		source_entity
	)
	SELECT 	nextval('BL_3NF.SEQ_CE_COMPANIES'),
				COALESCE(src_inv.company_name, 'n.a.'),
				COALESCE(src_inv.company_name, 'n.a.'),
				NOW(),
				NOW(),
				'SA_SALES_INVOICES',
				'SRC_INVOICES'
	FROM sa_sales_invoices.src_invoices AS src_inv
	WHERE NOT EXISTS (SELECT company_src_id FROM bl_3nf.ce_companies WHERE company_src_id = src_inv.company_name)
	GROUP BY src_inv.company_name
	ORDER BY src_inv.company_name;
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
		RAISE WARNING 'STATE: %, ERRM: %, DET: %', SQLSTATE, SQLERRM, err_detail;
END; 
$load_companies$ LANGUAGE plpgsql;


-- Debug / Testing
CALL bl_cl.ce_companies_load();

TRUNCATE BL_3NF.CE_COMPANIES RESTART IDENTITY CASCADE;
TRUNCATE BL_CL.mta_load_logs RESTART IDENTITY;

SELECT * FROM bl_3nf.ce_companies;
SELECT * FROM bl_cl.mta_load_logs;

