-- Districts Entity build:

SET SEARCH_PATH TO BL_3NF;
SHOW SEARCH_PATH;

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_CE_DISTRICTS;

-- Create CE_DISTRICTS table
CREATE TABLE IF NOT EXISTS CE_DISTRICTS (
	DISTRICT_ID 		INT 				PRIMARY KEY,
	DISTRICT_SRC_ID 	VARCHAR(1000) 	NOT NULL,
	DISTRICT_NAME		VARCHAR(100) 	NOT NULL,
	INSERT_DT			DATE 				NOT NULL,
	UPDATE_DT			DATE 				NOT NULL,
	SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
	SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_CE_DISTRICTS OWNED BY CE_DISTRICTS.DISTRICT_ID;

-- Add default row:
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

COMMIT;

