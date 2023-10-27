-- BL_3NF DDL master procedure

CREATE OR REPLACE PROCEDURE BL_CL.BL_3NF_DDL_LOAD(IN with_drop BOOLEAN) AS
$load_3nf_schema$
BEGIN
	IF with_drop IS TRUE THEN
		DROP SCHEMA IF EXISTS BL_3NF CASCADE;
		CREATE SCHEMA BL_3NF;
		SET SEARCH_PATH TO BL_3NF, BL_CL;
	END IF;
-- Customers Entity build:
	CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_CUSTOMERS_SCD;
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
	ALTER SEQUENCE BL_3NF.SEQ_CE_CUSTOMERS_SCD OWNED BY BL_3NF.CE_CUSTOMERS_SCD.CUSTOMER_ID;
	INSERT INTO bl_3nf.ce_customers_scd
		SELECT -1, 'n.a.', 'n.a.', 'n.a.', -1, '1-1-1900'::DATE, '31-12-9999'::DATE, 'Y', '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_customers_scd WHERE customer_id = -1);
-- Categories Entity build:
	CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_CATEGORIES;
	CREATE TABLE IF NOT EXISTS BL_3NF.CE_CATEGORIES (
		CATEGORY_ID 		INT 				PRIMARY KEY,
		CATEGORY_SRC_ID 	VARCHAR(1000) 	NOT NULL,
		CATEGORY_NAME 		VARCHAR(100) 	NOT NULL,
		INSERT_DT			DATE 				NOT NULL,
		UPDATE_DT			DATE 				NOT NULL,
		SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
		SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
	);
	ALTER SEQUENCE BL_3NF.SEQ_CE_CATEGORIES OWNED BY BL_3NF.CE_CATEGORIES.CATEGORY_ID;
	INSERT INTO bl_3nf.ce_categories
		SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_categories WHERE category_id = -1);
-- Coupons Entity build:
	CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_COUPONS;
	CREATE TABLE IF NOT EXISTS BL_3NF.CE_COUPONS (
		COUPON_ID 			INT 				PRIMARY KEY,
		COUPON_SRC_ID 		VARCHAR(1000) 	NOT NULL,
		DISCOUNT_SIZE		VARCHAR(5)		NOT NULL,
		INSERT_DT			DATE 				NOT NULL,
		UPDATE_DT			DATE 				NOT NULL,
		SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
		SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
	);
	ALTER SEQUENCE BL_3NF.SEQ_CE_COUPONS OWNED BY BL_3NF.CE_COUPONS.COUPON_ID;
	INSERT INTO bl_3nf.ce_coupons
		SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_coupons WHERE coupon_id = -1);
-- Companies Entity build:
	CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_COMPANIES;
	CREATE TABLE IF NOT EXISTS BL_3NF.CE_COMPANIES (
		COMPANY_ID 			INT 				PRIMARY KEY,
		COMPANY_SRC_ID 	VARCHAR(1000) 	NOT NULL,
		COMPANY_NAME		VARCHAR(100) 	NOT NULL,
		INSERT_DT			DATE 				NOT NULL,
		UPDATE_DT			DATE 				NOT NULL,
		SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
		SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
	);
	ALTER SEQUENCE BL_3NF.SEQ_CE_COMPANIES OWNED BY BL_3NF.CE_COMPANIES.COMPANY_ID;
	INSERT INTO bl_3nf.ce_companies
		SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_companies WHERE company_id = -1);
-- Districts Entity build:
	CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_DISTRICTS;
	CREATE TABLE IF NOT EXISTS BL_3NF.CE_DISTRICTS (
		DISTRICT_ID 		INT 				PRIMARY KEY,
		DISTRICT_SRC_ID 	VARCHAR(1000) 	NOT NULL,
		DISTRICT_NAME		VARCHAR(100) 	NOT NULL,
		INSERT_DT			DATE 				NOT NULL,
		UPDATE_DT			DATE 				NOT NULL,
		SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
		SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
	);
	ALTER SEQUENCE BL_3NF.SEQ_CE_DISTRICTS OWNED BY BL_3NF.CE_DISTRICTS.DISTRICT_ID;
	INSERT INTO bl_3nf.ce_districts
		SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_districts WHERE district_id = -1);
-- Stores Entity build:
	CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_STORES;
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
	ALTER SEQUENCE BL_3NF.SEQ_CE_STORES OWNED BY BL_3NF.CE_STORES.STORE_ID;
	INSERT INTO bl_3nf.ce_stores
		SELECT -1, 'n.a.', -1, -1, 'n.a.', -1, -1, '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_stores WHERE store_id = -1);
-- Payment Methods Entity build:
	CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_PAYMENT_METHODS;
	CREATE TABLE IF NOT EXISTS BL_3NF.CE_PAYMENT_METHODS (
		PAYMENT_METHOD_ID 		INT 					PRIMARY KEY,
		PAYMENT_METHOD_SRC_ID 	VARCHAR(1000) 		NOT NULL,
		PAYMENT_METHOD_NAME	 	VARCHAR(50)			NOT NULL,
		INSERT_DT					DATE 					NOT NULL,
		UPDATE_DT					DATE					NOT NULL,
		SOURCE_SYSTEM				VARCHAR(1000) 		NOT NULL,
		SOURCE_ENTITY				VARCHAR(1000) 		NOT NULL
	);
	ALTER SEQUENCE BL_3NF.SEQ_CE_PAYMENT_METHODS OWNED BY BL_3NF.CE_PAYMENT_METHODS.PAYMENT_METHOD_ID;
	INSERT INTO bl_3nf.ce_payment_methods
		SELECT -1, 'n.a.', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
		WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_payment_methods WHERE payment_method_id = -1);
-- Sales entity (transaction table) build:
	CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_CE_SALES;
	CREATE TABLE IF NOT EXISTS BL_3NF.CE_SALES (
		SALE_ID 					INT	 				PRIMARY KEY,
		SALE_SRC_ID				VARCHAR(1000)		NOT NULL,
		EVENT_DT					TIMESTAMP 			NOT NULL,
		STORE_ID					INT					NOT NULL,
		CUSTOMER_ID				INT					NOT NULL,
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
	ALTER SEQUENCE BL_3NF.SEQ_CE_SALES OWNED BY BL_3NF.CE_SALES.SALE_ID;
END; 
$load_3nf_schema$ LANGUAGE plpgsql; VOLATILE;


CALL BL_CL.BL_3NF_DDL_LOAD(with_drop := FALSE);
