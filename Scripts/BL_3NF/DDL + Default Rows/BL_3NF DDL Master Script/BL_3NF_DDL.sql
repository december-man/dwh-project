-- BL_3NF SCHEMA & TABLE STRUCTURE (DDL)

-- REQUIREMENTS:
	-- Reusability
	-- Sequences can not be generated using `GENERATED ALWAYS AS IDENTITY` syntax
	-- Sequences can not be generated automatically using `SERIAL` pseudo-data type
	-- Sequences can not be generated automatically using `DEFAULT nextval('NAME_SEQ')` constraint
	-- All attributes must have a default value that follows a default values convention

-- Creating BL_3NF schema:
DROP SCHEMA IF EXISTS BL_3NF CASCADE; -- debugging

CREATE SCHEMA IF NOT EXISTS BL_3NF;
SET SEARCH_PATH TO BL_3NF;
SHOW SEARCH_PATH;


-- Customers Entity build:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_CUSTOMERS_SCD;

-- Create CE_CUSTOMERS table
CREATE TABLE IF NOT EXISTS BL_3NF.CE_CUSTOMERS_SCD (
	CUSTOMER_ID 		INT				NOT NULL,
	CUSTOMER_SRC_ID 	VARCHAR(1000)	NOT NULL,
	CUSTOMER_NAME 		VARCHAR(100)	NOT NULL,
	CUSTOMER_GENDER 	VARCHAR(10) 	NOT NULL,
	CUSTOMER_AGE 		SMALLINT 		NOT NULL,
	START_DT				DATE				NOT NULL,
	END_DT				DATE				NOT NULL,
	IS_ACTIVE			VARCHAR(1)		NOT NULL,
	INSERT_DT			DATE 				NOT NULL,
	SOURCE_SYSTEM		VARCHAR(1000)	NOT NULL,
	SOURCE_ENTITY		VARCHAR(1000)	NOT NULL,
	PRIMARY KEY(CUSTOMER_ID, START_DT)
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE BL_3NF.SEQ_CE_CUSTOMERS_SCD OWNED BY BL_3NF.CE_CUSTOMERS_SCD.CUSTOMER_ID;

-- Add default row:
INSERT INTO bl_3nf.ce_customers_scd (
	customer_id,
	customer_src_id,
	customer_name,
	customer_gender,
	customer_age,
	start_dt,
	end_dt,
	is_active,
	insert_dt,
	source_system,
	source_entity
)
SELECT -1, 'n.a.', 'n.a.', 'n.a.', -1, '1-1-1900'::DATE, '31-12-9999'::DATE, 'Y', '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_customers_scd WHERE customer_id = -1);


-- Categories Entity build:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_CATEGORIES;

-- Create CE_CATEGORIES table
CREATE TABLE IF NOT EXISTS BL_3NF.CE_CATEGORIES (
	CATEGORY_ID 		INT 				PRIMARY KEY,
	CATEGORY_SRC_ID 	VARCHAR(1000) 	NOT NULL,
	CATEGORY_NAME 		VARCHAR(100) 	NOT NULL,
	INSERT_DT			DATE 				NOT NULL,
	UPDATE_DT			DATE 				NOT NULL,
	SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
	SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE BL_3NF.SEQ_CE_CATEGORIES OWNED BY BL_3NF.CE_CATEGORIES.CATEGORY_ID;

-- Add default Row:
INSERT INTO bl_3nf.ce_categories (
	category_id,
	category_src_id,
	category_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_categories WHERE category_id = -1);


-- Coupons Entity build:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_COUPONS;

-- Create CE_COUPONS table
CREATE TABLE IF NOT EXISTS BL_3NF.CE_COUPONS (
	COUPON_ID 			INT 				PRIMARY KEY,
	COUPON_SRC_ID 		VARCHAR(1000) 	NOT NULL,
	DISCOUNT_SIZE		VARCHAR(5)		NOT NULL,
	INSERT_DT			DATE 				NOT NULL,
	UPDATE_DT			DATE 				NOT NULL,
	SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
	SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE BL_3NF.SEQ_CE_COUPONS OWNED BY BL_3NF.CE_COUPONS.COUPON_ID;

-- Add default Row:
INSERT INTO bl_3nf.ce_coupons (
	coupon_id,
	coupon_src_id,
	discount_size,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_coupons WHERE coupon_id = -1);


-- Companies Entity build:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_COMPANIES;

-- Create CE_COMPANIES table
CREATE TABLE IF NOT EXISTS BL_3NF.CE_COMPANIES (
	COMPANY_ID 			INT 				PRIMARY KEY,
	COMPANY_SRC_ID 	VARCHAR(1000) 	NOT NULL,
	COMPANY_NAME		VARCHAR(100) 	NOT NULL,
	INSERT_DT			DATE 				NOT NULL,
	UPDATE_DT			DATE 				NOT NULL,
	SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
	SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE BL_3NF.SEQ_CE_COMPANIES OWNED BY BL_3NF.CE_COMPANIES.COMPANY_ID;

-- Add default Row:
INSERT INTO bl_3nf.ce_companies (
	company_id,
	company_src_id,
	company_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_companies WHERE company_id = -1); 


-- Districts Entity build:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_DISTRICTS;

-- Create CE_DISTRICTS table
CREATE TABLE IF NOT EXISTS BL_3NF.CE_DISTRICTS (
	DISTRICT_ID 		INT 				PRIMARY KEY,
	DISTRICT_SRC_ID 	VARCHAR(1000) 	NOT NULL,
	DISTRICT_NAME		VARCHAR(100) 	NOT NULL,
	INSERT_DT			DATE 				NOT NULL,
	UPDATE_DT			DATE 				NOT NULL,
	SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
	SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE BL_3NF.SEQ_CE_DISTRICTS OWNED BY BL_3NF.CE_DISTRICTS.DISTRICT_ID;

-- Add default Row:
INSERT INTO bl_3nf.ce_districts (
	district_id,
	district_src_id,
	district_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_districts WHERE district_id = -1);


-- Stores Entity build:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_STORES;

-- Create CE_STORES table
CREATE TABLE IF NOT EXISTS BL_3NF.CE_STORES (
	STORE_ID 				INT 				PRIMARY KEY,
	STORE_SRC_ID 			VARCHAR(1000) 	NOT NULL,
	DISTRICT_ID				INT				NOT NULL,
	COMPANY_ID				INT				NOT NULL,
	STORE_NAME				VARCHAR(100) 	NOT NULL,
	STORE_LOCATION_LAT	NUMERIC			NOT NULL,
	STORE_LOCATION_LONG	NUMERIC			NOT NULL,
	INSERT_DT				DATE 				NOT NULL,
	UPDATE_DT				DATE 				NOT NULL,
	SOURCE_SYSTEM			VARCHAR(1000) 	NOT NULL,
	SOURCE_ENTITY			VARCHAR(1000) 	NOT NULL,
	CONSTRAINT FK_CE_STORES_DISTRICT_ID FOREIGN KEY (DISTRICT_ID) 	REFERENCES BL_3NF.CE_DISTRICTS,
	CONSTRAINT FK_CE_STORES_COMPANY_ID 	FOREIGN KEY (COMPANY_ID)	REFERENCES BL_3NF.CE_COMPANIES
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE BL_3NF.SEQ_CE_STORES OWNED BY BL_3NF.CE_STORES.STORE_ID;


-- Add default Row:
INSERT INTO bl_3nf.ce_stores (
	store_id,
	store_src_id,
	district_id,
	company_id,
	store_name,
	store_location_lat,
	store_location_long,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT -1, 'n.a.', -1, -1, 'n.a.', -1, -1, '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_stores WHERE store_id = -1);


-- Payment Methods Entity build:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_PAYMENT_METHODS;

-- Create CE_PAYMENT_METHODS table
CREATE TABLE IF NOT EXISTS BL_3NF.CE_PAYMENT_METHODS (
	PAYMENT_METHOD_ID 		INT 					PRIMARY KEY,
	PAYMENT_METHOD_SRC_ID 	VARCHAR(1000) 		NOT NULL,
	PAYMENT_METHOD_NAME	 	VARCHAR(50)			NOT NULL,
	INSERT_DT					DATE 					NOT NULL,
	UPDATE_DT					DATE					NOT NULL,
	SOURCE_SYSTEM				VARCHAR(1000) 		NOT NULL,
	SOURCE_ENTITY				VARCHAR(1000) 		NOT NULL
);

-- Associate sequence with the table's surrogate primary attribute SURVEY_ID
ALTER SEQUENCE BL_3NF.SEQ_CE_PAYMENT_METHODS OWNED BY BL_3NF.CE_PAYMENT_METHODS.PAYMENT_METHOD_ID;

-- Add default row
INSERT INTO bl_3nf.ce_payment_methods (
	payment_method_id,
	payment_method_src_id,
	payment_method_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_payment_methods WHERE payment_method_id = -1);


-- Sales entity (transaction table) build:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_SALES;

-- Create CE_SALES table
CREATE TABLE IF NOT EXISTS BL_3NF.CE_SALES (
	SALE_ID 					INT	 				PRIMARY KEY,
	SALE_SRC_ID				VARCHAR(1000)		NOT NULL,
	EVENT_DT					TIMESTAMP 			NOT NULL,
	STORE_ID					INT					NOT NULL,
	CUSTOMER_ID				INT					NOT NULL, -- Logical FOREIGN KEY 
	COUPON_ID				INT					NOT NULL,
	CATEGORY_ID				INT					NOT NULL,
	PAYMENT_METHOD_ID		INT					NOT NULL,
	QUANTITY_CNT			SMALLINT				NOT NULL,
	PRICE_LIRAS				NUMERIC 				NOT NULL,
	DISCOUNT_LIRAS			NUMERIC				NOT NULL,
	COST_LIRAS				NUMERIC				NOT NULL,
	REVENUE_LIRAS			NUMERIC				NOT NULL,
	PAYMENT_AMOUNT_LIRAS NUMERIC				NOT NULL,
	INSERT_DT				DATE 					NOT NULL,
	SOURCE_SYSTEM			VARCHAR(1000) 		NOT NULL,
	SOURCE_ENTITY			VARCHAR(1000) 		NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE BL_3NF.SEQ_CE_SALES OWNED BY BL_3NF.CE_SALES.SALE_ID;


COMMIT;
