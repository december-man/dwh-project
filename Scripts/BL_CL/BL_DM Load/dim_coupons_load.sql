

-- BL_CL DML Wrappers:

-- DIM_COUPONS SCD-1 Loading Procedure:

-- In order for Upsert (INSERT ON CONFLICT DO UPDATE) to work,
-- We need to create an index on the src_id column or a constraint:
DROP INDEX IF EXISTS idx_dim_coupons_src_id;
CREATE INDEX IF NOT EXISTS idx_dim_coupons_src_id ON bl_dm.dim_coupons USING btree (coupon_src_id);
-- hmmm seems like in cases when we start with an empty table indexes are not going to cut it:
-- CLUSTER VERBOSE bl_dm.dim_coupons USING idx_dim_coupons_src_id;
-- CLUSTER VERBOSE bl_dm.dim_coupons;
-- Lets stick with unique constraint then (which under the hood also creates an index...)
ALTER TABLE bl_dm.dim_coupons DROP CONSTRAINT UNIQUE_DIM_COUPON_SRC_ID;
ALTER TABLE bl_dm.dim_coupons ADD CONSTRAINT UNIQUE_DIM_COUPON_SRC_ID UNIQUE (coupon_src_id);

CREATE OR REPLACE PROCEDURE bl_cl.dim_coupons_load()
AS $load_coupons_dim$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
	INSERT INTO bl_dm.dim_coupons (
		coupon_surr_id, coupon_src_id, discount_size,
		insert_dt, update_dt, source_system, source_entity
		)
		SELECT 	nextval('BL_DM.SEQ_DIM_COUPONS'),
					coupon_id, discount_size, NOW(), NOW(), 'BL_3NF', 'CE_COUPONS' 
		FROM bl_3nf.ce_coupons
		WHERE coupon_id != -1
	ON CONFLICT (coupon_src_id) DO UPDATE
		SET 	discount_size = EXCLUDED.discount_size,
				update_dt = NOW()
		WHERE bl_dm.dim_coupons.discount_size != EXCLUDED.discount_size;
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
$load_coupons_dim$ LANGUAGE plpgsql;


-- testing / debug
CALL bl_cl.ce_coupons_load();
CALL bl_cl.dim_coupons_load();

SELECT * FROM bl_3nf.ce_coupons cc;
SELECT * FROM bl_dm.dim_coupons dc;
SELECT * FROM bl_cl.mta_load_logs mll;

TRUNCATE bl_dm.dim_coupons RESTART IDENTITY CASCADE;

UPDATE bl_3nf.ce_coupons cc
SET discount_size = '10%' WHERE coupon_src_id = '10000';


-- indexes
SELECT    CONCAT(n.nspname,'.', c.relname) AS table,
          i.relname AS index_name, pg_size_pretty(pg_relation_size(x.indrelid)) AS table_size,
          pg_size_pretty(pg_relation_size(x.indexrelid)) AS index_size,
          pg_size_pretty(pg_total_relation_size(x.indrelid)) AS total_size FROM pg_class c 
JOIN      pg_index x ON c.oid = x.indrelid
JOIN      pg_class i ON i.oid = x.indexrelid
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE     c.relkind = ANY (ARRAY['r', 't'])
AND       n.oid NOT IN (99, 11, 12375);
