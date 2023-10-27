-- Companies Entity build:


SET SEARCH_PATH TO BL_3NF;
SHOW SEARCH_PATH;


-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_CE_COMPANIES;

-- Create CE_COMPANIES table
CREATE TABLE IF NOT EXISTS CE_COMPANIES (
	COMPANY_ID 			INT 				PRIMARY KEY,
	COMPANY_SRC_ID 	VARCHAR(1000) 	NOT NULL,
	COMPANY_NAME		VARCHAR(100) 	NOT NULL,
	INSERT_DT			DATE 				NOT NULL,
	UPDATE_DT			DATE 				NOT NULL,
	SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
	SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_CE_COMPANIES OWNED BY CE_COMPANIES.COMPANY_ID;

-- Add default row:
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

COMMIT;
