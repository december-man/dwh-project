-- Categories Entity build:

SET SEARCH_PATH TO BL_3NF;
SHOW SEARCH_PATH;

-- DROP TABLE IF EXISTS BL_3NF.CE_CATEGORIES;

-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_CE_CATEGORIES;

-- Create CE_CATEGORIES table
CREATE TABLE IF NOT EXISTS CE_CATEGORIES (
	CATEGORY_ID 		INT 				PRIMARY KEY,
	CATEGORY_SRC_ID 	VARCHAR(1000) 	NOT NULL,
	CATEGORY_NAME 		VARCHAR(100) 	NOT NULL,
	INSERT_DT			DATE 				NOT NULL,
	UPDATE_DT			DATE 				NOT NULL,
	SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
	SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_CE_CATEGORIES OWNED BY CE_CATEGORIES.CATEGORY_ID;


-- Add default row:
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

COMMIT;