-- Customers Entity build:

SET SEARCH_PATH TO BL_3NF;
SHOW SEARCH_PATH;

-- DROP TABLE IF EXISTS BL_3NF.CE_CUSTOMERS_SCD;

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_CE_CUSTOMERS_SCD;

-- Create CE_CUSTOMERS table
CREATE TABLE IF NOT EXISTS CE_CUSTOMERS_SCD (
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
ALTER SEQUENCE SEQ_CE_CUSTOMERS_SCD OWNED BY CE_CUSTOMERS_SCD.CUSTOMER_ID;

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

COMMIT;


