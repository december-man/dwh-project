-- Adding foreign tables extension
CREATE EXTENSION IF NOT EXISTS file_fdw;

-- Creating schema
CREATE SCHEMA IF NOT EXISTS SA_SALES_CUSTOMERS_CC;
SET SEARCH_PATH TO SA_SALES_CUSTOMERS_CC;
SHOW SEARCH_PATH;

-- Creating server
CREATE SERVER IF NOT EXISTS SA_SALES_SUPERSTORES_EXTERNAL_SERVER FOREIGN DATA WRAPPER file_fdw;

-- Create foreign (external) table EXT_CUSTOMERS_CC
DROP FOREIGN TABLE IF EXISTS EXT_CUSTOMERS_CC; -- debugging

CREATE FOREIGN TABLE IF NOT EXISTS EXT_CUSTOMERS_CC (
	customer_id 		VARCHAR,
	gender 				VARCHAR,
	age 					VARCHAR,
	payment_method 	VARCHAR,
	payment_amount 	VARCHAR,
	shopping_mall 		VARCHAR,
	"timestamp"			VARCHAR,
	customer_name 		VARCHAR
)	
SERVER SA_SALES_SUPERSTORES_EXTERNAL_SERVER
OPTIONS (
	FILENAME '/home/goetie/EPAM DAE/S2/DWH/DWH & ETL part 2/Topic_09/Data Sources/EXT_CUSTOMERS_CC.csv',
	FORMAT 'csv',
	HEADER 'true'
);

-- Creating SRC_CUSTOMERS_CC Snapshot Table
DROP TABLE IF EXISTS sa_sales_customers_cc.src_customers_cc CASCADE; -- debugging / reusability

-- CREATE TABLE IF NOT EXISTS SRC_CUSTOMERS_CC AS TABLE EXT_CUSTOMERS_CC;
-- Or using DML + DDL scripts with satisfactory levels of explicitness:

CREATE TABLE IF NOT EXISTS sa_sales_customers_cc.src_customers_cc (
	customer_id varchar NULL,
	gender varchar NULL,
	age varchar NULL,
	payment_method varchar NULL,
	payment_amount varchar NULL,
	shopping_mall varchar NULL,
	"timestamp" varchar NULL,
	customer_name varchar NULL,
	refresh_dt TIMESTAMPTZ
);

-- DML
INSERT INTO sa_sales_customers_cc.src_customers_cc
SELECT 	customer_id,
			gender,
			age,
			payment_method,
			payment_amount,
			shopping_mall,
			"timestamp",
			customer_name,
			NOW()
FROM sa_sales_customers_cc.ext_customers_cc;
 

DROP TABLE IF EXISTS BL_CL.WRK_CUSTOMERS; -- reusability & updates

CREATE TABLE IF NOT EXISTS BL_CL.WRK_CUSTOMERS AS
SELECT 	DISTINCT customer_id,
			gender,
			age,
			customer_name
FROM sa_sales_customers_cc.src_customers_cc
WHERE customer_name IS NOT NULL OR 
		(customer_name IS NULL AND customer_id NOT IN (
			SELECT DISTINCT customer_id FROM sa_sales_customers_cc.src_customers_cc WHERE customer_name IS NOT NULL
			)
		); 


COMMIT; -- ens ure closing of a transaction
