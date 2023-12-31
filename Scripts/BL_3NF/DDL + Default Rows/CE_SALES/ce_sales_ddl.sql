-- Sales entity (transaction table) build:

SET SEARCH_PATH TO BL_3NF;
SHOW SEARCH_PATH;

-- DROP TABLE IF EXISTS BL_3NF.CE_SALES;
-- TRUNCATE bl_3nf.ce_sales RESTART IDENTITY CASCADE;

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_CE_SALES;

-- Create CE_SALES table
CREATE TABLE IF NOT EXISTS CE_SALES (
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
ALTER SEQUENCE SEQ_CE_SALES OWNED BY CE_SALES.SALE_ID;


