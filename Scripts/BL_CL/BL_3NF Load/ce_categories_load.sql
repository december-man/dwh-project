

-- BL_CL DML Wrappers:

-- CE_CATEGORIES INSERT Procedure: 
CREATE OR REPLACE PROCEDURE bl_cl.ce_categories_load()
AS $load_categories$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
-- Original INSERT operation
	INSERT INTO bl_3nf.ce_categories (
		category_id,
		category_src_id,
		category_name,
		insert_dt,
		update_dt,
		source_system,
		source_entity
	)
	SELECT 	nextval('BL_3NF.SEQ_CE_CATEGORIES'),
				COALESCE(src_inv.category, 'n.a.'),
				COALESCE(src_inv.category, 'n.a.'),
				NOW(),
				NOW(),
				'SA_SALES_INVOICES',
				'SRC_INVOICES'
	FROM sa_sales_invoices.src_invoices AS src_inv
	WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_categories WHERE category_src_id = src_inv.category)
	GROUP BY src_inv.category
	ORDER BY src_inv.category;
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
$load_categories$ LANGUAGE plpgsql;

-- Debug / Testing

CALL bl_cl.ce_categories_load();

TRUNCATE BL_3NF.CE_CATEGORIES RESTART IDENTITY;
TRUNCATE BL_CL.mta_load_logs;

SELECT * FROM bl_3nf.ce_categories;
SELECT * FROM bl_cl.mta_load_logs;

