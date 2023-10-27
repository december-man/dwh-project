-- CREATE Stores dimension:

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_DIM_STORES;

-- Create Table
CREATE TABLE IF NOT EXISTS BL_DM.DIM_STORES (
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
ALTER SEQUENCE BL_DM.SEQ_DIM_STORES OWNED BY BL_DM.DIM_STORES.STORE_SURR_ID;

-- Default Row:
INSERT INTO BL_DM.DIM_STORES
SELECT -1, 'n.a.', 'n.a.', -1, -1, -1, 'n.a.', -1, 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.dim_stores WHERE store_surr_id = -1);

COMMIT;