CREATE OR REPLACE PROCEDURE BL_CL.SRC_CUSTOMERS_CC_LOAD_CSV(IN file_abspath TEXT) AS 
$src_ccc_loader$
DECLARE
context TEXT; context_short TEXT; row_count INT; err_code TEXT;  
err_context TEXT; err_context_short TEXT; err_detail TEXT; err_msg TEXT;
ext_customers_cc_qs TEXT := FORMAT(
	'CREATE FOREIGN TABLE IF NOT EXISTS sa_sales_customers_cc.ext_customers_cc (
	customer_id 		VARCHAR,
	gender 				VARCHAR,
	age 					VARCHAR,
	payment_method 	VARCHAR,
	payment_amount 	VARCHAR,
	shopping_mall 		VARCHAR,
	"timestamp"			VARCHAR,
	customer_name 		VARCHAR
	) SERVER SA_SUPREMESTORES_DWH_SERVER
	OPTIONS (
		FILENAME %L,
		FORMAT 	%L,
		HEADER 	%L
	);', $1, 'csv', 'true');
BEGIN
	RAISE INFO 'Preparing Tables .../...';
	DROP FOREIGN TABLE IF EXISTS SA_SALES_CUSTOMERS_CC.EXT_CUSTOMERS_CC; -- dump & reload logic
	EXECUTE ext_customers_cc_qs;
	CREATE TABLE IF NOT EXISTS SA_SALES_CUSTOMERS_CC.SRC_CUSTOMERS_CC (LIKE SA_SALES_CUSTOMERS_CC.EXT_CUSTOMERS_CC);
	-- Add incremental load date column:
	ALTER TABLE SA_SALES_CUSTOMERS_CC.SRC_CUSTOMERS_CC ADD COLUMN IF NOT EXISTS refresh_dt TIMESTAMPTZ DEFAULT NOW();
	RAISE INFO 'Loading to Source Table, %', NOW();
	INSERT INTO sa_sales_customers_cc.src_customers_cc
		SELECT 	customer_id, gender, age, payment_method, payment_amount, shopping_mall, "timestamp", customer_name
		FROM sa_sales_customers_cc.ext_customers_cc
	EXCEPT
		SELECT 	customer_id, gender, age, payment_method, payment_amount, shopping_mall, "timestamp", customer_name
		FROM sa_sales_customers_cc.src_customers_cc;
	-- Logging
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		row_count := ROW_COUNT;
		context_short := SUBSTRING(context FROM 'function (.*?) line');
	-- call logger on successful insert
	CALL bl_cl.load_logger(context_short, row_count);
	RAISE INFO 'Successfully loaded data to SRC_CUSTOMERS_CC table from the specified csv file';
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS
		   err_code := RETURNED_SQLSTATE,
        	err_msg := MESSAGE_TEXT,
			err_context := PG_EXCEPTION_CONTEXT,
			err_detail 	:= PG_EXCEPTION_DETAIL;
			err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');
	-- call logger in case of exception
		CALL bl_cl.load_logger(err_context_short, 0, FORMAT('ERROR %s: %s. Details: %s', err_code, err_msg, err_detail));
		RAISE EXCEPTION 'STATE: %, ERRM: %, DETAIL: %', err_code, err_msg, err_detail;
END;
$src_ccc_loader$ LANGUAGE plpgsql; VOLATILE;


CALL BL_CL.SRC_CUSTOMERS_CC_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/Increments/EXT_CUSTOMERS_CC_50K.csv');
DROP TABLE sa_sales_customers_cc.src_customers_cc;
CREATE TABLE IF NOT EXISTS SA_SALES_CUSTOMERS_CC.SRC_CUSTOMERS_CC (LIKE SA_SALES_CUSTOMERS_CC.EXT_CUSTOMERS_CC);
SELECT * FROM BL_CL.mta_load_logs mll;
SELECT COUNT(*) FROM sa_sales_customers_cc.src_customers_cc ecc;
SELECT * FROM sa_sales_customers_cc.src_customers_cc scc ORDER BY refresh_dt DESC;
SELECT * FROM sa_sales_customers_cc.src_customers_cc FETCH FIRST 100 ROWS ONLY;
SELECT * FROM pg_tables;

-- deduplication
SELECT COUNT(*) FROM (
		SELECT 	customer_id, gender, age, payment_method, payment_amount, shopping_mall, "timestamp", customer_name
		FROM sa_sales_customers_cc.ext_customers_cc
	EXCEPT
		SELECT 	customer_id, gender, age, payment_method, payment_amount, shopping_mall, "timestamp", customer_name
		FROM sa_sales_customers_cc.src_customers_cc) AS T1;
