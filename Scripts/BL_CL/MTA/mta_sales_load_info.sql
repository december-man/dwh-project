CREATE TABLE IF NOT EXISTS bl_cl.prm_mta_incremental_load (
	src_table_name		VARCHAR		NOT NULL,
	target_table_name VARCHAR		NOT NULL,
	procedure_name		VARCHAR		NOT NULL,
	latest_load_ts		TIMESTAMPTZ NOT NULL
);

CREATE OR REPLACE PROCEDURE bl_cl.iload_data (
	src_tbl_name	VARCHAR,
	tgt_tbl_name	VARCHAR,
	proc_name		VARCHAR,
	load_ts 			TIMESTAMPTZ
) AS $sales_load_info$
BEGIN
	IF EXISTS (
		SELECT 1 FROM bl_cl.prm_mta_incremental_load WHERE src_table_name = src_tbl_name AND
			target_table_name = tgt_tbl_name	AND procedure_name = proc_name) THEN
		UPDATE bl_cl.prm_mta_incremental_load
		SET		latest_load_ts = load_ts
		WHERE 	src_table_name 	= src_tbl_name 	AND
					target_table_name = tgt_tbl_name		AND
					procedure_name = proc_name;
	ELSE
		INSERT INTO bl_cl.prm_mta_incremental_load
			SELECT src_tbl_name, tgt_tbl_name, proc_name, load_ts;
	END IF;
END;
$sales_load_info$ LANGUAGE plpgsql;



SELECT * FROM pg_proc WHERE proname ~~ '%data%';
TRUNCATE bl_cl.prm_mta_incremental_load;
SELECT * FROM pg_constraint;
SELECT * FROM bl_cl.prm_mta_incremental_load;

-- Load default rows
CALL bl_cl.iload_data('SRC_CUSTOMERS_CC'::TEXT, 'CE_SALES'::TEXT, 'ce_sales_load()', '1900-01-01'::TIMESTAMPTZ);
CALL bl_cl.iload_data('SRC_INVOICES'::TEXT, 'CE_SALES'::TEXT, 'ce_sales_load()', '1900-01-01'::TIMESTAMPTZ);


-- Create a view to check the data in the increment:
CREATE VIEW sales_increment AS
	SELECT 	invoice_no, "timestamp"::TIMESTAMP,
				shopping_mall, customer_id, coupon_id, category, payment_method, -- FKs
				quantity::INT, price::NUMERIC, discount::NUMERIC, costs::NUMERIC, revenue::NUMERIC,
				price::NUMERIC*quantity::INT AS payment_amount, -- metrics
				'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity, refresh_dt
	FROM sa_sales_invoices.src_invoices
	WHERE refresh_dt > (
		SELECT latest_load_ts FROM bl_cl.prm_mta_incremental_load 
		WHERE target_table_name = 'CE_SALES' AND src_table_name = 'SRC_INVOICES')
UNION ALL
	SELECT 	'n.a.' AS invoice_no, "timestamp"::TIMESTAMP,
				shopping_mall, customer_id, 'n.a.' AS coupon_id, 'n.a.' AS category, payment_method, -- FKs
				-1 AS quantity, -1 AS price, -1 AS discount, -1 AS costs, -1 AS revenue, payment_amount::NUMERIC, -- metrics
				'SA_SALES_CUSTOMERS_CC' AS source_system, 'SRC_CUSTOMERS_CC' AS source_entity, refresh_dt
	FROM sa_sales_customers_cc.src_customers_cc
	WHERE refresh_dt > (
		SELECT latest_load_ts FROM bl_cl.prm_mta_incremental_load 
		WHERE target_table_name = 'CE_SALES' AND src_table_name = 'SRC_CUSTOMERS_CC');



