-- Stores Entity build:


SET SEARCH_PATH TO BL_3NF;
SHOW SEARCH_PATH;


-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_CE_STORES;

-- Create CE_STORES table
CREATE TABLE IF NOT EXISTS CE_STORES (
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
	CONSTRAINT FK_CE_STORES_DISTRICT_ID FOREIGN KEY (DISTRICT_ID) 	REFERENCES CE_DISTRICTS,
	CONSTRAINT FK_CE_STORES_COMPANY_ID 	FOREIGN KEY (COMPANY_ID)	REFERENCES CE_COMPANIES
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_CE_STORES OWNED BY CE_STORES.STORE_ID;

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


COMMIT;
