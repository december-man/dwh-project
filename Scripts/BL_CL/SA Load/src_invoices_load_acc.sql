CREATE OR REPLACE PROCEDURE BL_CL.SRC_INVOICES_LOAD_CSV(IN file_abspath TEXT) AS 
$src_invoices_loader$
DECLARE
context TEXT; context_short TEXT; row_count INT; err_code TEXT;  
err_context TEXT; err_context_short TEXT; err_detail TEXT; err_msg TEXT;
ext_invoices_qs TEXT := FORMAT(
	'CREATE FOREIGN TABLE IF NOT EXISTS sa_sales_invoices.ext_invoices (
	invoice_no 			VARCHAR,
	customer_id 		VARCHAR,
	category 			VARCHAR,
	payment_method 	VARCHAR,
	coupon_id 			VARCHAR,
	discount 			VARCHAR,
	quantity 		 	VARCHAR,
	price 				VARCHAR,
	costs 				VARCHAR,
	revenue 				VARCHAR,
	timestamp 			VARCHAR,
	shopping_mall 		VARCHAR,
	district 			VARCHAR,
	lat 					VARCHAR,
	long 					VARCHAR,
	company_name 		VARCHAR,
	customer_rating 	VARCHAR,
	discount_size 		VARCHAR
	) SERVER SA_SUPREMESTORES_DWH_SERVER
	OPTIONS (
		FILENAME %L,
		FORMAT 	%L,
		HEADER 	%L
	);', $1, 'csv', 'true');
BEGIN
	RAISE INFO 'Preparing Tables .../...';
	DROP FOREIGN TABLE IF EXISTS SA_SALES_INVOICES.EXT_INVOICES;
	EXECUTE ext_invoices_qs;
	CREATE TABLE IF NOT EXISTS SA_SALES_INVOICES.SRC_INVOICES (LIKE SA_SALES_INVOICES.EXT_INVOICES);
	-- add incremental load monitoring column
	ALTER TABLE SA_SALES_INVOICES.SRC_INVOICES ADD COLUMN IF NOT EXISTS refresh_dt TIMESTAMPTZ DEFAULT NOW();
	RAISE INFO 'Loading to Source Table, %', NOW();
	INSERT INTO sa_sales_invoices.SRC_INVOICES
		SELECT 	invoice_no, customer_id, category, payment_method, coupon_id,	discount, quantity, price, costs,
					revenue, "timestamp", shopping_mall, district, lat, long, company_name, customer_rating, discount_size 
		FROM sa_sales_invoices.ext_invoices
	EXCEPT
		SELECT 	invoice_no, customer_id, category, payment_method, coupon_id,	discount, quantity, price, costs,
					revenue, "timestamp", shopping_mall, district, lat, long, company_name, customer_rating, discount_size
		FROM sa_sales_invoices.src_invoices;
	-- Logging
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		row_count := ROW_COUNT;
		context_short := SUBSTRING(context FROM 'function (.*?) line');
	-- call logger on successful insert
	CALL bl_cl.load_logger(context_short, row_count);
	RAISE INFO 'Successfully loaded data to SRC_INVOICES table from the specified csv file';
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
$src_invoices_loader$ LANGUAGE plpgsql; VOLATILE;


CALL BL_CL.SRC_INVOICES_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/Increments/EXT_INVOICES_100K.csv');
DROP TABLE sa_sales_invoices.src_invoices;
SELECT COUNT(*) FROM sa_sales_invoices.src_invoices;
SELECT * FROM BL_CL.mta_load_logs mll;
SELECT * FROM sa_sales_invoices.src_invoices ORDER BY time DESC FETCH FIRST 3 ROWS ONLY;



