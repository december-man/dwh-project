-- Coupons Entity build:


SET SEARCH_PATH TO BL_3NF;
SHOW SEARCH_PATH;


-- Generate Surrogate PK
CREATE SEQUENCE IF NOT EXISTS SEQ_CE_COUPONS;

-- Create CE_COUPONS table
CREATE TABLE IF NOT EXISTS CE_COUPONS (
	COUPON_ID 			INT 				PRIMARY KEY,
	COUPON_SRC_ID 		VARCHAR(1000) 	NOT NULL,
	DISCOUNT_SIZE		VARCHAR(5)		NOT NULL,
	INSERT_DT			DATE 				NOT NULL,
	UPDATE_DT			DATE 				NOT NULL,
	SOURCE_SYSTEM		VARCHAR(1000) 	NOT NULL,
	SOURCE_ENTITY		VARCHAR(1000) 	NOT NULL
);

-- Associate sequence with the table's surrogate PK
ALTER SEQUENCE SEQ_CE_COUPONS OWNED BY CE_COUPONS.COUPON_ID;

-- Add default row:
INSERT INTO bl_3nf.ce_coupons (
	coupon_id,
	coupon_src_id,
	discount_size,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT -1, '-1', 'n.a.', '1-1-1900'::DATE, '1-1-1900'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_coupons WHERE coupon_id = -1);

COMMIT;

