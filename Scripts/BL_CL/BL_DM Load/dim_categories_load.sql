-- BL_CL Dimension loading procedures

-- DIM_CATEGORIES Load
-- This script also covers the required usage of FOR LOOP cycle-through cursor, dynamic sql and custom composite type
DROP TYPE IF EXISTS bl_cl.category_3nf CASCADE;
CREATE TYPE bl_cl.category_3nf AS (
	cat_dup_id INTEGER,
	cat_dup_name VARCHAR
);
-- DROP FUNCTION IF EXISTS bl_cl.dim_categories_load();
CREATE OR REPLACE FUNCTION bl_cl.dim_categories_load() RETURNS SETOF category_3nf -- custom data type!
AS $load_categories_dim$
DECLARE 
context TEXT; context_short TEXT; err_code TEXT; err_msg TEXT;
err_context TEXT; err_context_short TEXT; err_detail TEXT;
-- declare dynamic sql query
query TEXT := FORMAT( 
	'INSERT INTO bl_dm.dim_categories SELECT nextval(%L), $1, $2, NOW(), NOW(), %L, %L;',
		'BL_DM.SEQ_DIM_CATEGORIES', 'BL_3NF', 'CE_CATEGORIES');
-- declare loop-through cursor
cat_3nf_cursor CURSOR FOR SELECT * FROM bl_3nf.ce_categories WHERE category_id != -1;
-- declare output
res category_3nf; -- custom DATA TYPE!
-- declare artificial row-counter
row_count INT := 0;
BEGIN
	-- loop through cursor output
	FOR recordvar IN cat_3nf_cursor LOOP
		IF NOT EXISTS (SELECT 1 FROM bl_dm.dim_categories WHERE category_name = recordvar.category_name) THEN
		-- execute dynamic sql query if the category is not in DM category dimension
			EXECUTE query USING recordvar.category_id, recordvar.category_name;
			RAISE NOTICE 'Loading New category type from 3NF schema to DM: %', recordvar.category_name;
		-- i++
			row_count = row_count +1;
		ELSE
		-- else output the category name & into the list of duplicates
			SELECT recordvar.category_id, recordvar.category_name INTO res;
			RETURN NEXT res;
		END IF;
	END LOOP;
	-- Logging
	GET DIAGNOSTICS 
		context := PG_CONTEXT;
		context_short := SUBSTRING(context FROM 'function (.*?) line');
	-- call logger on successful insert
	CALL bl_cl.load_logger(context_short, row_count); -- return artificial row_counter
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS
			err_context := PG_EXCEPTION_CONTEXT,
			err_detail 	:= PG_EXCEPTION_DETAIL;
			err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');
	-- call logger in case of exception
		CALL bl_cl.load_logger(err_context_short, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
		RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
	RETURN;
END; 
$load_categories_dim$ LANGUAGE plpgsql;

-- Calling such function is a bit unorthodox, because of RECORD composite type.
SELECT * FROM bl_cl.dim_categories_load();
SELECT bl_cl.dim_categories_load();
SELECT * FROM BL_3NF.ce_categories cc;
SELECT * FROM BL_DM.dim_categories dc;
SELECT * FROM BL_CL.mta_load_logs mll;
--TRUNCATE bl_dm.dim_categories RESTART IDENTITY CASCADE;

-- Create a procedure that will not rely on useless user-defined composite data type:
CREATE OR REPLACE PROCEDURE bl_cl.dim_categories_load_proc()
AS $load_cat_dim$
DECLARE 
context TEXT; context_short TEXT; err_code TEXT; err_msg TEXT;
err_context TEXT; err_context_short TEXT; err_detail TEXT;
-- declare dynamic sql query
query TEXT := FORMAT( 
	'INSERT INTO bl_dm.dim_categories SELECT nextval(%L), $1, $2, NOW(), NOW(), %L, %L;',
		'BL_DM.SEQ_DIM_CATEGORIES', 'BL_3NF', 'CE_CATEGORIES');
-- declare loop-through cursor
cat_3nf_cursor CURSOR FOR SELECT * FROM bl_3nf.ce_categories WHERE category_id != -1;
-- declare artificial row-counter
row_count INT := 0;
BEGIN
	-- loop through cursor output
	FOR recordvar IN cat_3nf_cursor LOOP
		IF NOT EXISTS (SELECT 1 FROM bl_dm.dim_categories WHERE category_name = recordvar.category_name) THEN
		-- execute dynamic sql query if the category is not in DM category dimension
			EXECUTE query USING recordvar.category_id, recordvar.category_name;
			RAISE NOTICE 'Loading New category type from 3NF schema to DM: %', recordvar.category_name;
		-- i++
			row_count = row_count +1;
		END IF;
	END LOOP;
	-- Logging
	GET DIAGNOSTICS 
		context := PG_CONTEXT;
		context_short := SUBSTRING(context FROM 'function (.*?) line');
	-- call logger on successful insert
	CALL bl_cl.load_logger(context_short, row_count); -- return artificial row_counter
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS
			err_context := PG_EXCEPTION_CONTEXT,
			err_detail 	:= PG_EXCEPTION_DETAIL;
			err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');
	-- call logger in case of exception
		CALL bl_cl.load_logger(err_context_short, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
		RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
	RETURN;
END; 
$load_cat_dim$ LANGUAGE plpgsql;

CALL bl_cl.dim_categories_load_proc();
