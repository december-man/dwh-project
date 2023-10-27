-- Payment Methods Entity build:


SET SEARCH_PATH TO BL_3NF;
SHOW SEARCH_PATH;


-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_CE_PAYMENT_METHODS;

-- Create CE_PAYMENT_METHODS table
CREATE TABLE IF NOT EXISTS CE_PAYMENT_METHODS (
	PAYMENT_METHOD_ID 		INT 					PRIMARY KEY,
	PAYMENT_METHOD_SRC_ID 	VARCHAR(1000) 		NOT NULL,
	PAYMENT_METHOD_NAME	 	VARCHAR(50)			NOT NULL,
	INSERT_DT					DATE 					NOT NULL,
	UPDATE_DT					DATE					NOT NULL,
	SOURCE_SYSTEM				VARCHAR(1000) 		NOT NULL,
	SOURCE_ENTITY				VARCHAR(1000) 		NOT NULL
);

-- Associate sequence with the table's surrogate primary attribute SURVEY_ID
ALTER SEQUENCE SEQ_CE_PAYMENT_METHODS OWNED BY CE_PAYMENT_METHODS.PAYMENT_METHOD_ID;

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

COMMIT;



