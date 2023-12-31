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

-- Create partition tables:
CREATE TABLE BL_DM.FCT_SALES_MINUTES_2021_01
PARTITION OF BL_DM.FCT_SALES_MINUTES
FOR VALUES FROM ('2021-01-01'::TIMESTAMPTZ) TO ('2021-06-01'::TIMESTAMPTZ);

CREATE TABLE BL_DM.FCT_SALES_MINUTES_2021_06
PARTITION OF BL_DM.FCT_SALES_MINUTES
FOR VALUES FROM ('2021-06-01'::TIMESTAMPTZ) TO ('2022-01-01'::TIMESTAMPTZ);

CREATE TABLE BL_DM.FCT_SALES_MINUTES_2022_01
PARTITION OF BL_DM.FCT_SALES_MINUTES
FOR VALUES FROM ('2022-01-01'::TIMESTAMPTZ) TO ('2022-06-01'::TIMESTAMPTZ);

CREATE TABLE BL_DM.FCT_SALES_MINUTES_2022_06
PARTITION OF BL_DM.FCT_SALES_MINUTES
FOR VALUES FROM ('2022-06-01'::TIMESTAMPTZ) TO ('2023-01-01'::TIMESTAMPTZ);

CREATE TABLE BL_DM.FCT_SALES_MINUTES_2023_01
PARTITION OF BL_DM.FCT_SALES_MINUTES
FOR VALUES FROM ('2023-01-01'::TIMESTAMPTZ) TO ('2023-06-01'::TIMESTAMPTZ);

CREATE TABLE BL_DM.FCT_SALES_MINUTES_2023_06
PARTITION OF BL_DM.FCT_SALES_MINUTES
FOR VALUES FROM ('2023-06-01'::TIMESTAMPTZ) TO ('2024-01-01'::TIMESTAMPTZ);