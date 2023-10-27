
-- DDL Scripts & Default Row Insertion for the BL_DM Schema

-- Create schema
DROP SCHEMA IF EXISTS BL_DM CASCADE;
CREATE SCHEMA IF NOT EXISTS BL_DM;
SET SEARCH_PATH TO BL_DM, BL_CL;
SHOW SEARCH_PATH;

-- CREATING DIMENSION TABLES

-- Create Time Dimension (taken from the Topic_04 HW):
-- Note: it does not require a default row
-- There is no surrogate key.

CREATE TABLE IF NOT EXISTS DIM_TIME_DAY (
  EVENT_DT 					DATE 				PRIMARY KEY,
  EPOCH 						BIGINT 			NOT NULL,
  DAY_NAME 					VARCHAR(20) 	NOT NULL,
  DAY_OF_WEEK 				INT 				NOT NULL,
  IS_WEEKEND 				BOOLEAN 			NOT NULL,
  DAY_OF_MONTH 			INT 				NOT NULL,
  DAY_OF_QUARTER 			INT 				NOT NULL,
  DAY_OF_YEAR 				INT 				NOT NULL,
  WEEK_OF_MONTH 			INT 				NOT NULL,
  WEEK_OF_YEAR 			INT 				NOT NULL,
  WEEK_OF_YEAR_ISO 		VARCHAR(10) 	NOT NULL,
  "MONTH" 					INT 				NOT NULL,
  MONTH_NAME 				VARCHAR(9) 		NOT NULL,
  MONTH_NAME_CUT 			VARCHAR(3) 		NOT NULL,
  "QUARTER" 				INT 				NOT NULL,
  QUARTER_NAME 			VARCHAR(9) 		NOT NULL,
  YEAR_ACTUAL 				INT 				NOT NULL,
  FIRST_DAY_OF_WEEK 		DATE 				NOT NULL,
  LAST_DAY_OF_WEEK 		DATE 				NOT NULL,
  FIRST_DAY_OF_MONTH 	DATE 				NOT NULL,
  LAST_DAY_OF_MONTH 		DATE 				NOT NULL,
  FIRST_DAY_OF_QUARTER 	DATE 				NOT NULL,
  LAST_DAY_OF_QUARTER 	DATE 				NOT NULL,
  FIRST_DAY_OF_YEAR 		DATE 				NOT NULL,
  LAST_DAY_OF_YEAR 		DATE 				NOT NULL,
  MMYYYY 					VARCHAR(6) 		NOT NULL,
  MMDDYYYY 					VARCHAR(10) 	NOT NULL
);


-- CREATE Categories dimension:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_CATEGORIES;

-- Create Table
CREATE TABLE IF NOT EXISTS DIM_CATEGORIES (
	CATEGORY_SURR_ID 	INT 				PRIMARY KEY,
	CATEGORY_SRC_ID 	VARCHAR(100)	NOT NULL,
	CATEGORY_NAME 		VARCHAR(100)	NOT NULL,
	INSERT_DT 			DATE				NOT NULL,
	UPDATE_DT 			DATE				NOT NULL,
	SOURCE_SYSTEM 		VARCHAR(100)	NOT NULL,
	SOURCE_ENTITY 		VARCHAR(100)	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_DIM_CATEGORIES OWNED BY DIM_CATEGORIES.CATEGORY_SURR_ID;

-- Default Row:
INSERT INTO DIM_CATEGORIES
SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM dim_categories WHERE category_surr_id = -1);


-- CREATE Coupons dimension:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_COUPONS;

-- Create Table
CREATE TABLE IF NOT EXISTS DIM_COUPONS (
	COUPON_SURR_ID 	INT 				PRIMARY KEY,
	COUPON_SRC_ID 		VARCHAR(100)	NOT NULL,
	DISCOUNT_SIZE 		VARCHAR(5)		NOT NULL,
	INSERT_DT 			DATE				NOT NULL,
	UPDATE_DT 			DATE				NOT NULL,
	SOURCE_SYSTEM 		VARCHAR(100)	NOT NULL,
	SOURCE_ENTITY 		VARCHAR(100)	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_DIM_COUPONS OWNED BY DIM_COUPONS.COUPON_SURR_ID;

-- Constraint for UPSERT SCD-1 Logic:
ALTER TABLE bl_dm.dim_coupons ADD CONSTRAINT UNIQUE_DIM_COUPON_SRC_ID UNIQUE (coupon_src_id);

-- Default Row:
INSERT INTO DIM_COUPONS
SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM dim_coupons WHERE coupon_surr_id = -1);


-- CREATE Payment Methods dimension:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_PAYMENT_METHODS;

-- Create Table
CREATE TABLE IF NOT EXISTS DIM_PAYMENT_METHODS (
	PAYMENT_METHOD_SURR_ID 		INT 				PRIMARY KEY,
	PAYMENT_METHOD_SRC_ID 		VARCHAR(100)	NOT NULL,
	PAYMENT_METHOD_NAME	 		VARCHAR(100)	NOT NULL,
	INSERT_DT 						DATE				NOT NULL,
	UPDATE_DT 						DATE				NOT NULL,
	SOURCE_SYSTEM 					VARCHAR(100)	NOT NULL,
	SOURCE_ENTITY 					VARCHAR(100)	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_DIM_PAYMENT_METHODS OWNED BY DIM_PAYMENT_METHODS.PAYMENT_METHOD_SURR_ID;

-- Default Row:
INSERT INTO DIM_PAYMENT_METHODS
SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM dim_payment_methods WHERE payment_method_surr_id = -1);


-- CREATE Stores dimension:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_STORES;

-- Create Table
CREATE TABLE IF NOT EXISTS DIM_STORES (
	STORE_SURR_ID 			INT 				PRIMARY KEY,
	STORE_SRC_ID 			VARCHAR(100)	NOT NULL,
	STORE_NAME	 			VARCHAR(100)	NOT NULL,
	STORE_LOCATION_LAT	NUMERIC			NOT NULL,
	STORE_LOCATION_LONG	NUMERIC			NOT NULL,
	STORE_COMPANY_ID		INT				NOT NULL,
	STORE_COMPANY_NAME	VARCHAR(100)	NOT NULL,
	STORE_DISTRICT_ID 	INT				NOT NULL,
	STORE_DISTRICT_NAME	VARCHAR(100)	NOT NULL,
	INSERT_DT 				DATE				NOT NULL,
	UPDATE_DT 				DATE				NOT NULL,
	SOURCE_SYSTEM 			VARCHAR(100)	NOT NULL,
	SOURCE_ENTITY 			VARCHAR(100)	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_DIM_STORES OWNED BY DIM_STORES.STORE_SURR_ID;

-- Default Row:
INSERT INTO DIM_STORES
SELECT -1, 'n.a.', 'n.a.', -1, -1, -1, 'n.a.', -1, 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM dim_stores WHERE store_surr_id = -1);


-- CREATE Customers dimension:
-- Note: SCD TYPE 2

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_DIM_CUSTOMERS_SCD;
-- Create Table
CREATE TABLE IF NOT EXISTS DIM_CUSTOMERS_SCD (
	CUSTOMER_SURR_ID 	INT 					PRIMARY KEY,
	CUSTOMER_SRC_ID 	VARCHAR(100)		NOT NULL,
	CUSTOMER_NAME 		VARCHAR(100)		NOT NULL,
	CUSTOMER_GENDER	VARCHAR(10)			NOT NULL,
	CUSTOMER_AGE		SMALLINT				NOT NULL,
	START_DT				DATE					NOT NULL,
	END_DT				DATE					NOT NULL,
	IS_ACTIVE			VARCHAR(1)			NOT NULL,
	INSERT_DT 			DATE					NOT NULL,
	SOURCE_SYSTEM 		VARCHAR(100)		NOT NULL,
	SOURCE_ENTITY 		VARCHAR(100)		NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_DIM_CUSTOMERS_SCD OWNED BY DIM_CUSTOMERS_SCD.CUSTOMER_SURR_ID;

-- Default Row:
INSERT INTO DIM_CUSTOMERS_SCD
SELECT -1, 'n.a', 'n.a.', 'n.a.', -1, '31-12-9999'::DATE, '1-1-1900'::DATE, 'Y', '1-1-1900'::DATE,  'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM dim_customers_scd WHERE customer_surr_id = -1);


-- Create Sales fact table:
-- Notes: 
-- Fact table does not require default rows
-- EVENT_MINUTES can be either a minute-precision timestamp or just an integer representing number of minutes within a day
-- Foreign keys are optional for the BL_DM layer - Dropped.
-- Partitioning strategy is a range by date with each partition being half-year period.

-- DROP TABLE IF EXISTS BL_DM.FCT_SALES_MINUTES;

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_FCT_SALES_MINUTES;

-- Create Partitioned Fact Table, remove foreign keys
CREATE TABLE IF NOT EXISTS BL_DM.FCT_SALES_MINUTES (
	EVENT_DT 						DATE				NOT NULL,
	EVENT_MINUTES 					INT				NOT NULL, 
	SALE_SURR_ID 					INT 				NOT NULL,
	CUSTOMER_SURR_ID 				INT 				NOT NULL,
	STORE_SURR_ID 					INT 				NOT NULL,
	COUPON_SURR_ID					INT 				NOT NULL,
	CATEGORY_SURR_ID 				INT 				NOT NULL,
	PAYMENT_METHOD_SURR_ID 		INT 				NOT NULL,
	QUANTITY_CNT 					SMALLINT 		NOT NULL,
	FCT_PRICE_LIRAS 				NUMERIC,
	FCT_DISCOUNT_LIRAS 			NUMERIC,
	FCT_COST_LIRAS 				NUMERIC,
	FCT_REVENUE_LIRAS 			NUMERIC,
	FCT_PAYMENT_AMOUNT_LIRAS 	NUMERIC,
	INSERT_DT 						DATE 				NOT NULL,
	SALE_SRC_ID 					VARCHAR(100) 	NOT NULL,
	SOURCE_SYSTEM 					VARCHAR(100) 	NOT NULL,
	SOURCE_ENTITY 					VARCHAR(100) 	NOT NULL
) PARTITION BY RANGE (EVENT_DT);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE BL_DM.SEQ_FCT_SALES_MINUTES OWNED BY BL_DM.FCT_SALES_MINUTES.SALE_SURR_ID;

---- Create partition tables:
--CREATE TABLE BL_DM.FCT_SALES_MINUTES_2021_01
--PARTITION OF BL_DM.FCT_SALES_MINUTES
--FOR VALUES FROM ('2021-01-01'::TIMESTAMPTZ) TO ('2021-07-01'::TIMESTAMPTZ);
--
--CREATE TABLE BL_DM.FCT_SALES_MINUTES_2021_06
--PARTITION OF BL_DM.FCT_SALES_MINUTES
--FOR VALUES FROM ('2021-07-01'::TIMESTAMPTZ) TO ('2022-01-01'::TIMESTAMPTZ);
--
--CREATE TABLE BL_DM.FCT_SALES_MINUTES_2022_01
--PARTITION OF BL_DM.FCT_SALES_MINUTES
--FOR VALUES FROM ('2022-01-01'::TIMESTAMPTZ) TO ('2022-07-01'::TIMESTAMPTZ);
--
--CREATE TABLE BL_DM.FCT_SALES_MINUTES_2022_06
--PARTITION OF BL_DM.FCT_SALES_MINUTES
--FOR VALUES FROM ('2022-07-01'::TIMESTAMPTZ) TO ('2023-01-01'::TIMESTAMPTZ);
--
--CREATE TABLE BL_DM.FCT_SALES_MINUTES_2023_01
--PARTITION OF BL_DM.FCT_SALES_MINUTES
--FOR VALUES FROM ('2023-01-01'::TIMESTAMPTZ) TO ('2023-07-01'::TIMESTAMPTZ);
--
--CREATE TABLE BL_DM.FCT_SALES_MINUTES_2023_06
--PARTITION OF BL_DM.FCT_SALES_MINUTES
--FOR VALUES FROM ('2023-07-01'::TIMESTAMPTZ) TO ('2024-01-01'::TIMESTAMPTZ);



COMMIT; -- for the DML pieces of script
