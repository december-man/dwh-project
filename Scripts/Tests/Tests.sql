-- This is the .sql for testing/monitoring SupremeStores Data Warehouse

-- DDL

-- Master Tests table
CREATE TABLE IF NOT EXISTS bl_cl.mta_tests (
	"Test Name" 			VARCHAR(30)	 NOT NULL UNIQUE,
	"Test Description" 	VARCHAR(500) NOT NULL,
	"Script"					VARCHAR(500) NOT NULL,
	"Results Table"		VARCHAR(100) NOT NULL,
	"Latest Run"			TIMESTAMPTZ	 NOT NULL DEFAULT NOW(),
	"Status"					VARCHAR(20)	 NOT NULL
);
-- Add existing tests
INSERT INTO bl_cl.mta_tests
SELECT 	'Duplicates scan', 
			'Checks all the non-generated tables from BL_3NF & BL_DM layers for duplicate rows',
			'CALL bl_cl.unq_tests();',
			'mta_tests_unq',
			NOW(),
			'N/A'
UNION ALL
SELECT 	'Data Loss scan',
			'Checks all the non-generated tables from BL_3NF & BL_DM layers for missing data from sources',
			'CALL bl_cl.lost_data_tests();',
			'mta_tests_lost_data',
			NOW(),
			'N/A';

SELECT * FROM bl_cl.mta_tests;


-- Create a procedure that will execute all the tests from the bl_cl.mta_tests table
CREATE OR REPLACE PROCEDURE bl_cl.run_dwh_tests() AS 
$run_dwh_tests$
DECLARE
mta_tests_cursor CURSOR FOR SELECT * FROM bl_cl.mta_tests;
BEGIN
	RAISE INFO 'Preparing DWH tests .../...';
	FOR recordvar IN mta_tests_cursor LOOP
		RAISE INFO 'Executing %', recordvar."Test Name";
		RAISE INFO '% %', recordvar."Test Name", recordvar."Test Description";
			EXECUTE recordvar."Script";
		RAISE INFO '% is completed, see % for details', recordvar."Test Name", recordvar."Results Table";
	END LOOP;
END;
$run_dwh_tests$ LANGUAGE plpgsql; VOLATILE;

CALL bl_cl.run_dwh_tests();
SELECT * FROM bl_cl.mta_tests;

-- Uniqueness/Duplicates tests table
CREATE TABLE IF NOT EXISTS bl_cl.mta_tests_unq (
	"Table Name" 			VARCHAR(30) NOT NULL UNIQUE,
	"Duplicate Rows, #"	BIGINT		NOT NULL DEFAULT 0,
	"Last Update"			TIMESTAMP	DEFAULT NOW()
);

-- Add Default Rows:
INSERT INTO bl_cl.mta_tests_unq ("Table Name")
VALUES 
('ce_categories'),
('ce_coupons'),
('ce_companies'),
('ce_districts'),
('ce_payment_methods'),
('ce_stores'),
('ce_customers_scd'),
('ce_sales'),
('dim_categories'),
('dim_coupons'),
('dim_payment_methods'),
('dim_stores'),
('dim_customers_scd'),
('fct_sales_minutes');

CALL bl_cl.unq_tests();
SELECT * FROM bl_cl.mta_tests_unq;

-- Lost Data from sources Table:
CREATE TABLE IF NOT EXISTS bl_cl.mta_tests_lost_data (
	"Table Name" 			VARCHAR(30) NOT NULL UNIQUE,
	"Lost Rows, #"			BIGINT		NOT NULL DEFAULT 0,
	"Last Update"			TIMESTAMP	DEFAULT NOW()
);

INSERT INTO bl_cl.mta_tests_lost_data ("Table Name")
VALUES 
('ce_categories'),
('ce_coupons'),
('ce_companies'),
('ce_districts'),
('ce_payment_methods'),
('ce_stores'),
('ce_customers_scd'),
('ce_sales'),
('dim_categories'),
('dim_coupons'),
('dim_payment_methods'),
('dim_stores'),
('dim_customers_scd'),
('fct_sales_minutes');

CALL bl_cl.lost_data_tests();
SELECT * FROM bl_cl.mta_tests_lost_data;

-- Duplication/Uniqueness tests

-- CE tables
CREATE OR REPLACE PROCEDURE bl_cl.unq_tests() AS 
$ce_unq_tests$
BEGIN
UPDATE bl_cl.mta_tests_unq SET "Last Update" = NOW();
RAISE INFO 'Checking for duplicates in BL_3NF Layer tables .../...';
IF EXISTS (SELECT 1 FROM bl_3nf.ce_categories GROUP BY category_src_id, source_system HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'ce_categories has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_3nf.ce_categories GROUP BY category_src_id, source_system HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'ce_categories';
END IF;
IF EXISTS (SELECT 1 FROM bl_3nf.ce_coupons GROUP BY coupon_src_id, source_system HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'ce_coupons has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_3nf.ce_coupons GROUP BY coupon_src_id, source_system HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'ce_coupons';
END IF;
IF EXISTS (SELECT 1 FROM bl_3nf.ce_companies GROUP BY company_src_id, source_system HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'ce_companies has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_3nf.ce_companies GROUP BY company_src_id, source_system HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'ce_companies';
END IF;
IF EXISTS (SELECT 1 FROM bl_3nf.ce_districts GROUP BY district_src_id, source_system HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'ce_districts has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_3nf.ce_districts GROUP BY district_src_id, source_system HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'ce_districts';
END IF;
IF EXISTS (SELECT 1 FROM bl_3nf.ce_payment_methods GROUP BY payment_method_src_id, source_system HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'ce_payment_methods has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_3nf.ce_payment_methods GROUP BY payment_method_src_id, source_system HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'ce_payment_methods';
END IF;
IF EXISTS (SELECT 1 FROM bl_3nf.ce_stores GROUP BY store_src_id, source_system HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'ce_stores has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_3nf.ce_stores GROUP BY store_src_id, source_system HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'ce_stores';
END IF;
IF EXISTS (SELECT 1 FROM bl_3nf.ce_customers_scd GROUP BY customer_src_id, source_system, end_dt HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'ce_customers_scd has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_3nf.ce_customers_scd GROUP BY customer_src_id, source_system, end_dt HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'ce_customers_scd';
END IF;
IF EXISTS (SELECT 1 FROM bl_3nf.ce_sales GROUP BY sale_src_id, event_dt, customer_id, store_id, source_system HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'ce_sales has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_3nf.ce_sales GROUP BY sale_src_id, event_dt, customer_id, store_id, source_system HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'ce_sales';
END IF;
RAISE INFO 'Checking for duplicates in BL_DM Layer tables .../...';
IF EXISTS (SELECT 1 FROM bl_dm.dim_categories GROUP BY category_src_id, category_name HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'dim_categories has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_dm.dim_categories GROUP BY category_src_id, category_name HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'dim_categories';
END IF;
IF EXISTS (SELECT 1 FROM bl_dm.dim_customers_scd GROUP BY customer_src_id, end_dt HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'dim_customers_scd has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_dm.dim_customers_scd GROUP BY customer_src_id, end_dt HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'dim_customers_scd';
END IF;
IF EXISTS (SELECT 1 FROM bl_dm.dim_coupons GROUP BY coupon_src_id HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'dim_coupons has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_dm.dim_coupons GROUP BY coupon_src_id HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'dim_coupons';
END IF;
IF EXISTS (SELECT 1 FROM bl_dm.dim_payment_methods GROUP BY payment_method_src_id HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'dim_payment_methods has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_dm.dim_payment_methods GROUP BY payment_method_src_id HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'dim_payment_methods';
END IF;
IF EXISTS (SELECT 1 FROM bl_dm.dim_stores GROUP BY store_src_id, store_name HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'dim_stores has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_dm.dim_stores GROUP BY store_src_id, store_name HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'dim_stores';
END IF;
IF EXISTS (SELECT 1 FROM bl_dm.fct_sales_minutes GROUP BY sale_src_id HAVING COUNT(*) > 1) THEN
	RAISE WARNING 'fct_sales_minutes has duplicates! Refer to mta_tests_unq table for more information';
	UPDATE bl_cl.mta_tests_unq
	SET "Duplicate Rows, #" = (
		SELECT COUNT(*) OVER() FROM bl_dm.fct_sales_minutes GROUP BY sale_src_id HAVING COUNT(*) > 1 LIMIT 1)
	WHERE "Table Name" = 'fct_sales_minutes';
END IF;
IF (SELECT SUM("Duplicate Rows, #") = 0 FROM bl_cl.mta_tests_unq) THEN
	UPDATE bl_cl.mta_tests
	SET 	"Latest Run" = NOW(),
			"Status" = 'Green'
	WHERE "Results Table" = 'mta_tests_unq';
ELSE	
	UPDATE bl_cl.mta_tests
	SET 	"Latest Run" = NOW(),
			"Status" = 'Red'
	WHERE "Results Table" = 'mta_tests_unq';
END IF;
END;
$ce_unq_tests$ LANGUAGE plpgsql; VOLATILE;




CREATE OR REPLACE PROCEDURE bl_cl.lost_data_tests() AS 
$lost_data_tests$
BEGIN
-- Testing SA level SRC tables
UPDATE bl_cl.mta_tests_lost_data SET "Last Update" = NOW();
RAISE INFO 'Data Loss check-scan on BL_3NF tables .../...';
IF (SELECT (SELECT COUNT(*) FROM sa_sales_customers_cc.src_customers_cc) + 
	(SELECT COUNT(*) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_3nf.ce_sales)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO CE_SALES: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(*) FROM sa_sales_customers_cc.src_customers_cc) + 
								(SELECT COUNT(*) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_3nf.ce_sales))
	WHERE "Table Name" = 'ce_sales';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT customer_id) FROM bl_cl.wrk_customers) + 
	(SELECT COUNT(DISTINCT customer_id) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_3nf.ce_customers_scd WHERE is_active = 'Y' AND customer_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO CE_CUSTOMERS_SCD: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT customer_id) FROM bl_cl.wrk_customers) + 
								(SELECT COUNT(DISTINCT customer_id) FROM sa_sales_invoices.src_invoices) - 
		(SELECT COUNT(*) FROM bl_3nf.ce_customers_scd WHERE is_active = 'Y' AND customer_id != -1))
	WHERE "Table Name" = 'ce_customers_scd';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT category) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_3nf.ce_categories WHERE category_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO CE_CATEGORIES: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT category) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_3nf.ce_categories WHERE category_id != -1))
	WHERE "Table Name" = 'ce_categories';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT coupon_id) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_3nf.ce_coupons WHERE coupon_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO CE_COUPONS: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT coupon_id) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_3nf.ce_coupons WHERE coupon_id != -1))
	WHERE "Table Name" = 'ce_coupons';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT company_name) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_3nf.ce_companies WHERE company_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO CE_COMPANIES: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT company_name) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_3nf.ce_companies WHERE company_id != -1))
	WHERE "Table Name" = 'ce_companies';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT district) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_3nf.ce_districts WHERE district_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO CE_DISTRICTS: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT district) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_3nf.ce_districts WHERE district_id != -1))
	WHERE "Table Name" = 'ce_districts';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT shopping_mall) FROM sa_sales_invoices.src_invoices) + 
	(SELECT COUNT(DISTINCT shopping_mall) FROM sa_sales_customers_cc.src_customers_cc) > 
	(SELECT COUNT(*) FROM bl_3nf.ce_stores WHERE store_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO CE_STORES: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT shopping_mall) FROM sa_sales_invoices.src_invoices) + 
								(SELECT COUNT(DISTINCT shopping_mall) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_3nf.ce_stores WHERE store_id != -1))
	WHERE "Table Name" = 'ce_stores';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT payment_method) FROM sa_sales_invoices.src_invoices) + 
	(SELECT COUNT(DISTINCT payment_method) FROM sa_sales_customers_cc.src_customers_cc) > 
	(SELECT COUNT(*) FROM bl_3nf.ce_payment_methods WHERE payment_method_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO CE_PAYMENT_METHODS: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT payment_method) FROM sa_sales_invoices.src_invoices) + 
								(SELECT COUNT(DISTINCT payment_method) FROM sa_sales_customers_cc.src_customers_cc) - 
								(SELECT COUNT(*) FROM bl_3nf.ce_payment_methods WHERE payment_method_id != -1))
	WHERE "Table Name" = 'ce_payment_methods';
END IF;
RAISE INFO 'Data Loss check-scan on BL_DM tables .../...';
IF (SELECT (SELECT COUNT(*) FROM sa_sales_customers_cc.src_customers_cc) + (SELECT COUNT(*) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_dm.fct_sales_minutes)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO FCT_SALES_MINUTES: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(*) FROM sa_sales_customers_cc.src_customers_cc) + 
								(SELECT COUNT(*) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_dm.fct_sales_minutes))
	WHERE "Table Name" = 'fct_sales_minutes';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT customer_id) FROM bl_cl.wrk_customers) + 
	(SELECT COUNT(DISTINCT customer_id) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_dm.dim_customers_scd WHERE is_active = 'Y' AND customer_surr_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO DIM_CUSTOMERS_SCD: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT customer_id) FROM bl_cl.wrk_customers) + 
								(SELECT COUNT(DISTINCT customer_id) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_dm.dim_customers_scd WHERE is_active = 'Y' AND customer_surr_id != -1))
	WHERE "Table Name" = 'dim_customers_scd';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT category) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_dm.dim_categories WHERE category_surr_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO DIM_CATEGORIES: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT category) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_dm.dim_categories WHERE category_surr_id != -1))
	WHERE "Table Name" = 'dim_categories';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT coupon_id) FROM sa_sales_invoices.src_invoices) > 
	(SELECT COUNT(*) FROM bl_dm.dim_coupons WHERE coupon_surr_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO DIM_COUPONS: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT coupon_id) FROM sa_sales_invoices.src_invoices) - 
								(SELECT COUNT(*) FROM bl_dm.dim_coupons WHERE coupon_surr_id != -1))
	WHERE "Table Name" = 'dim_coupons';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT shopping_mall) FROM sa_sales_invoices.src_invoices) + 
	(SELECT COUNT(DISTINCT shopping_mall) FROM sa_sales_customers_cc.src_customers_cc) > 
	(SELECT COUNT(*) FROM bl_dm.dim_stores WHERE store_surr_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO DIM_STORES: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT shopping_mall) FROM sa_sales_invoices.src_invoices) + 
								(SELECT COUNT(DISTINCT shopping_mall) FROM sa_sales_customers_cc.src_customers_cc) - 
								(SELECT COUNT(*) FROM bl_dm.dim_stores WHERE store_surr_id != -1))
	WHERE "Table Name" = 'dim_stores';
END IF;
IF (SELECT (SELECT COUNT(DISTINCT payment_method) FROM sa_sales_invoices.src_invoices) + 
	(SELECT COUNT(DISTINCT payment_method) FROM sa_sales_customers_cc.src_customers_cc) > 
	(SELECT COUNT(*) FROM bl_dm.dim_payment_methods WHERE payment_method_surr_id != -1)) THEN
	RAISE WARNING 'ERROR LOADING DATA TO DIM_PAYMENT_METHODS: MISSING DATA FROM SOURCES';
	UPDATE bl_cl.mta_tests_lost_data
	SET "Lost Rows, #" = (SELECT (SELECT COUNT(DISTINCT payment_method) FROM sa_sales_invoices.src_invoices) + 
								(SELECT COUNT(DISTINCT payment_method) FROM sa_sales_customers_cc.src_customers_cc) - 
								(SELECT COUNT(*) FROM bl_dm.dim_payment_methods WHERE payment_method_surr_id != -1)) 
	WHERE "Table Name" = 'dim_payment_methods';
END IF;
IF (SELECT SUM("Lost Rows, #") = 0 FROM bl_cl.mta_tests_lost_data) THEN
	UPDATE bl_cl.mta_tests
	SET 	"Latest Run" = NOW(),
			"Status" = 'Green'
	WHERE "Results Table" = 'mta_tests_lost_data';
ELSE	
	UPDATE bl_cl.mta_tests
	SET 	"Latest Run" = NOW(),
			"Status" = 'Red'
	WHERE "Results Table" = 'mta_tests_lost_data';
END IF;
END;
$lost_data_tests$ LANGUAGE plpgsql; VOLATILE;

SELECT * FROM bl_cl.mta_tests_lost_data;
