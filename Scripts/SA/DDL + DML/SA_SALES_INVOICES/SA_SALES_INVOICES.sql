-- Adding foreign tables extension
CREATE EXTENSION IF NOT EXISTS file_fdw;

-- Creating schema
CREATE SCHEMA IF NOT EXISTS SA_SALES_INVOICES;
SET SEARCH_PATH TO SA_SALES_INVOICES;
SHOW SEARCH_PATH;

-- Creating server
CREATE SERVER IF NOT EXISTS SA_SUPREMESTORES_DWH_SERVER FOREIGN DATA WRAPPER file_fdw;

-- Creating foreign table EXT_INVOICES
DROP FOREIGN TABLE IF EXISTS EXT_INVOICES; -- debugging

CREATE FOREIGN TABLE IF NOT EXISTS sa_sales_invoices.ext_invoices (
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
)	
SERVER SA_SUPREMESTORES_DWH_SERVER
OPTIONS (
	FILENAME '/home/goetie/EPAM DAE/S2/DWH/DWH & ETL part 1/Topic_05/Data Sources/EXT_INVOICES.csv',
	FORMAT 'csv',
	HEADER 'true'
);

--Creating SRC_INVOICES Snapshot Table
DROP TABLE IF EXISTS SRC_INVOICES; -- debugging / reusability

-- CREATE TABLE IF NOT EXISTS SRC_INVOICES AS TABLE EXT_INVOICES;
-- Or using DML + DDL scripts with satisfactory levels of explicitness:

CREATE TABLE IF NOT EXISTS sa_sales_invoices.src_invoices (
	invoice_no 			varchar NULL,
	customer_id 		varchar NULL,
	category 			varchar NULL,
	payment_method 	varchar NULL,
	coupon_id 			varchar NULL,
	discount 			varchar NULL,
	quantity 			varchar NULL,
	price 				varchar NULL,
	costs 				varchar NULL,
	revenue 				varchar NULL,
	"timestamp" 		varchar NULL,
	shopping_mall 		varchar NULL,
	district 			varchar NULL,
	lat 					varchar NULL,
	long 					varchar NULL,
	company_name 		varchar NULL,
	customer_rating 	varchar NULL,
	discount_size 		varchar NULL
);

-- DML
INSERT INTO sa_sales_invoices.src_invoices
SELECT 	invoice_no,
			customer_id,
			category,
			payment_method,
			coupon_id,
			discount,
			quantity,
			price,
			costs,
			revenue,
			"timestamp",
			shopping_mall,
			district,
			lat,
			long,
			company_name,
			customer_rating,
			discount_size
FROM sa_sales_invoices.ext_invoices
WHERE NOT EXISTS (
	SELECT 1 FROM sa_sales_invoices.src_invoices -- double check
);


COMMIT; -- ensure closing of a transaction
