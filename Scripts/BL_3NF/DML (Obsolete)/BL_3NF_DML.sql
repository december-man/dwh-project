-- BL_3NF DATA FILL (SA -> BL_3NF) (DML)
-- The script should be reviewed together with the Task Report for additional information regarding the process

-- REQUIREMENTS:
	-- Reusability
	-- NULL handling with COALESCE that follows a default values convention
	-- Default rows in each table (except the transaction table) that follows a default values convention
	-- COMMIT at the end of the script to ensure deployment of changes

-- Create Cleansing Layer for data prep:
-- DROP SCHEMA IF EXISTS BL_CL CASCADE;
CREATE SCHEMA IF NOT EXISTS BL_CL;
SET SEARCH_PATH TO BL_3NF, BL_CL;
SHOW SEARCH_PATH;

-- Fill CE_CUSTOMERS_SCD table:

-- ADDITIONAL DATA PREP IS REQUIRED: Further filtering of customers
-- Some customer IDs populate two rows because not every transaction had their name in its contents.
-- Create another snapshot table in the staging are to transform the original SRC_CUSTOMERS_CC table to fit CE_CUSTOMERS
-- table design.
DROP TABLE IF EXISTS BL_CL.WRK_CUSTOMERS; -- reusability & updates

CREATE TABLE IF NOT EXISTS BL_CL.WRK_CUSTOMERS AS
SELECT 	DISTINCT customer_id,
			gender,
			age,
			customer_name
FROM sa_sales_customers_cc.src_customers_cc
WHERE customer_name IS NOT NULL OR 
		(customer_name IS NULL AND customer_id NOT IN (
			SELECT DISTINCT customer_id FROM sa_sales_customers_cc.src_customers_cc WHERE customer_name IS NOT NULL
			)
		); 

-- Reset the sequence if needed:
-- SELECT setval('CE_CUSTOMERS_CUSTOMER_ID_SEQ', 1, FALSE);
-- TRUNCATE bl_3nf.ce_customers_scd RESTART IDENTITY CASCADE; -- debugging with sequence reset
	
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

-- Fill with data using the union of two queries:
-- NOTE: THIS IS SCD TYPE 2 TABLE, the data flow is the following:
-- Both SELECTs from 2 data sources seek (WHERE NOT EXISTS clause) for any new records and
-- any smallest relevant change in existing ones, unchanged records are left untouched.
-- New records are INSERTed without any special logic
-- Modified records are INSERTed again as new ones with the same surrogate id as the existing record
-- Older record gets "UPDATE" treatment: boolean flag is_active changes to 'N' and end_dt is being set as NOW():

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
SELECT 	COALESCE(ce_cust.customer_id, nextval('SEQ_CE_CUSTOMERS_SCD')),
			COALESCE(src.customer_id, 'n.a.'),
			COALESCE(src.customer_name, 'n.a.'),
			COALESCE(src.gender, 'n.a.'),
			COALESCE(src.age, '-1')::SMALLINT,
			CURRENT_DATE - INTERVAL '1 day',
			'31-12-9999'::DATE,
			'Y',
			CURRENT_DATE - INTERVAL '1 day',
			src.source_system,
			src.source_entity
FROM (
	SELECT DISTINCT customer_id, customer_name, gender, "age"::SMALLINT, 'BL_CL' AS source_system, 'WRK_CUSTOMERS' AS source_entity
	FROM bl_cl.wrk_customers
	UNION ALL
	SELECT DISTINCT customer_id, 'n.a.', 'n.a.', -1, 'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
	FROM sa_sales_invoices.src_invoices
) AS src
	LEFT JOIN bl_3nf.ce_customers_scd ce_cust ON src.customer_id = ce_cust.customer_src_id
WHERE ce_cust.customer_id IS NULL OR -- newly added rows will have a NULL customer_id
		ce_cust.source_system = src.source_system AND ( -- sniff for any changes in older rows
			ce_cust.customer_name 	!= COALESCE(src.customer_name, 'n.a.') 	OR
			ce_cust.customer_gender != src.gender							 			OR
			ce_cust.customer_age 	!= src."age"
);


-- Updating older rows to have a False `is_active` flag and an actual `end_dt`.
UPDATE bl_3nf.ce_customers_scd
SET 	end_dt = CURRENT_DATE,
		is_active = 'N'
WHERE start_dt < CURRENT_DATE		AND -- this logic will not work with start dates later than the current date
		is_active = 'Y' 				AND
		customer_id 					IN (SELECT customer_id FROM bl_3nf.ce_customers_scd WHERE start_dt = CURRENT_DATE);


-- Fill CE_CATEGORIES Table:
-- Categories entity only exist in SRC_INVOICES table. Loading from there.

-- Reset the sequence if needed:
-- SELECT setval('CE_CATEGORIES_CATEGORY_ID_SEQ', 1, FALSE);
-- TRUNCATE bl_3nf.ce_categories RESTART IDENTITY CASCADE; -- debugging with sequence reset

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
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_categories WHERE category_id = -1); -- If default row is present - Do not insert anything

-- Fill with data:
INSERT INTO bl_3nf.ce_categories (
	category_id,
	category_src_id,
	category_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT 	nextval('SEQ_CE_CATEGORIES'),
			COALESCE(src_inv.category, 'n.a.'),
			COALESCE(src_inv.category, 'n.a.'),
			NOW(),
			NOW(),
			'SA_SALES_INVOICES',
			'SRC_INVOICES'
FROM sa_sales_invoices.src_invoices AS src_inv
WHERE NOT EXISTS (SELECT category_src_id FROM bl_3nf.ce_categories WHERE category_src_id = src_inv.category)
GROUP BY src_inv.category -- add unique categories 
ORDER BY src_inv.category;


-- Fill CE_COUPONS Table:
-- Coupons entity only exist in SRC_INVOICES table. Loading from there.

-- Reset the sequence if needed:
-- SELECT setval('CE_COUPONS_COUPON_ID_SEQ', 1, FALSE);
-- TRUNCATE bl_3nf.ce_coupons RESTART IDENTITY CASCADE; -- debugging with sequence reset

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
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_coupons WHERE coupon_id = -1); -- If default row is present - Do not insert anything

-- Fill with data:
INSERT INTO bl_3nf.ce_coupons (
	coupon_id,
	coupon_src_id,
	discount_size,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT 	nextval('SEQ_CE_COUPONS'),
			COALESCE(src_inv.coupon_id, '-1'),
			COALESCE(src_inv.discount_size, 'n.a.'),
			NOW(),
			NOW(),
			'SA_SALES_INVOICES',
			'SRC_INVOICES'
FROM sa_sales_invoices.src_invoices AS src_inv
WHERE NOT EXISTS (
	SELECT coupon_src_id 
	FROM bl_3nf.ce_coupons 
	WHERE coupon_src_id = src_inv.coupon_id AND 
			discount_size = src_inv.discount_size
)
GROUP BY src_inv.coupon_id, -- add only unique coupons
			src_inv.discount_size
ORDER BY src_inv.coupon_id;


-- Fill CE_COMPANIES Table:
-- Companies entity only exist in SRC_INVOICES table. Loading from there.

-- Reset the sequence if needed:
-- SELECT setval('CE_COMPANIES_COMPANY_ID_SEQ', 1, FALSE);
-- TRUNCATE bl_3nf.ce_companies RESTART IDENTITY CASCADE; -- debugging with sequence reset

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
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_companies WHERE company_id = -1); -- If default row is present - Do not insert anything

-- Fill with data:
INSERT INTO bl_3nf.ce_companies (
	company_id,
	company_src_id,
	company_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT 	nextval('SEQ_CE_COMPANIES'),
			COALESCE(src_inv.company_name, 'n.a.'),
			COALESCE(src_inv.company_name, 'n.a.'),
			NOW(),
			NOW(),
			'SA_SALES_INVOICES',
			'SRC_INVOICES'
FROM sa_sales_invoices.src_invoices AS src_inv
WHERE NOT EXISTS (SELECT company_src_id FROM bl_3nf.ce_companies WHERE company_src_id = src_inv.company_name)
GROUP BY src_inv.company_name -- add unique companies 
ORDER BY src_inv.company_name;


-- Fill CE_DISTRICTS Table:
-- District entity only exist in SRC_INVOICES table. Loading from there.

-- Reset the sequence if needed:
-- SELECT setval('CE_DISTRICTS_DISTRICT_ID_SEQ', 1, FALSE);
-- TRUNCATE bl_3nf.ce_districts RESTART IDENTITY CASCADE; -- debugging with sequence reset

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
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_districts WHERE district_id = -1); -- If default row is present - Do not insert anything

-- Fill with data:
INSERT INTO bl_3nf.ce_districts (
	district_id,
	district_src_id,
	district_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT 	nextval('SEQ_CE_DISTRICTS'),
			COALESCE(src_inv.district, 'n.a.'),
			COALESCE(src_inv.district, 'n.a.'),
			NOW(),
			NOW(),
			'SA_SALES_INVOICES',
			'SRC_INVOICES'
FROM sa_sales_invoices.src_invoices AS src_inv
WHERE NOT EXISTS (SELECT district_src_id FROM bl_3nf.ce_districts WHERE district_src_id = src_inv.district)
GROUP BY src_inv.district -- add unique districts 
ORDER BY src_inv.district;


-- Fill Payment Methods Table:

-- Reset the sequence if needed:
-- SELECT setval('CE_PAYMENT_METHODS_PAYMENT_METHOD_ID_SEQ', 1, FALSE);
-- TRUNCATE bl_3nf.ce_payment_methods RESTART IDENTITY CASCADE; -- debugging with sequence reset

-- Add default row:
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
WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_payment_methods WHERE payment_method_id = -1); -- If default row is present - Do not insert anything

-- Fill with data:
INSERT INTO bl_3nf.ce_payment_methods (
	payment_method_id,
	payment_method_src_id,
	payment_method_name,
	insert_dt,
	update_dt,
	source_system,
	source_entity
)
SELECT 	nextval('SEQ_CE_PAYMENT_METHODS'),
			COALESCE(src.payment_method, 'n.a.'),
			COALESCE(src.payment_method, 'n.a.'),
			NOW(),
			NOW(),
			src.source_system,
			src.source_entity
FROM (
	SELECT DISTINCT payment_method, 'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
	FROM sa_sales_invoices.src_invoices
	UNION ALL
	SELECT  DISTINCT payment_method, 'SA_SALES_CUSTOMERS_CC' AS source_system, 'SRC_CUSTOMERS_CC' AS source_entity
	FROM sa_sales_customers_cc.src_customers_cc
) AS src
WHERE NOT EXISTS (
	SELECT 1 
	FROM bl_3nf.ce_payment_methods 
	WHERE payment_method_src_id = src.payment_method AND
			source_system = src.source_system			
);


-- Fill CE_STORES table

-- Reset the sequence if needed:
-- SELECT setval('CE_STORES_STORE_ID_SEQ', 1, FALSE);
-- TRUNCATE bl_3nf.ce_stores RESTART IDENTITY CASCADE; -- debugging with sequence reset

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

-- Fill with data
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
SELECT 	nextval('SEQ_CE_STORES'),
			COALESCE(src.shopping_mall, 'n.a.'),
			dist.district_id,
			comp.company_id,
			COALESCE(src.shopping_mall, 'n.a.'),
			COALESCE(src.lat, -1),
			COALESCE(src.long, -1),
			NOW(),
			NOW(),
			src.source_system,
			src.source_entity		
FROM ( 
	SELECT DISTINCT shopping_mall, district, company_name, lat::NUMERIC, long::NUMERIC, 'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
	FROM sa_sales_invoices.src_invoices
	UNION ALL
	SELECT DISTINCT shopping_mall, 'n.a.', 'n.a.', -1, -1, 'SA_SALES_CUSTOMERS_CC' AS source_system, 'SRC_CUSTOMERS_CC' AS source_entity
	FROM sa_sales_customers_cc.src_customers_cc
) AS src
	LEFT JOIN bl_3nf.ce_districts dist ON src.district = dist.district_src_id
	LEFT JOIN bl_3nf.ce_companies comp ON src.company_name = comp.company_src_id
WHERE NOT EXISTS (
	SELECT 1 
	FROM bl_3nf.ce_stores 
	WHERE store_src_id = src.shopping_mall AND
			source_system = src.source_system
);


---- Fill CE_SALES Transactional table

-- Reset the sequence if needed:
-- SELECT setval('CE_SALES', 1, FALSE);
TRUNCATE bl_3nf.ce_sales RESTART IDENTITY CASCADE; -- TRUNCATE BEFORE INSERT POLICY ON FACT TABLE

-- Drop Foreign Keys to imporve INSERT speed
ALTER TABLE ce_sales DROP CONSTRAINT IF EXISTS FK_CE_PAYMENTS_STORE_ID;
ALTER TABLE ce_sales DROP CONSTRAINT IF EXISTS FK_CE_PAYMENTS_COUPON_ID;
ALTER TABLE ce_sales DROP CONSTRAINT IF EXISTS FK_CE_PAYMENTS_CATEGORY_ID ;
ALTER TABLE ce_sales DROP CONSTRAINT IF EXISTS FK_CE_PAYMENTS_PAYMENT_METHOD_ID;

-- Data Fill
INSERT INTO ce_sales (
	sale_id,
	sale_src_id,
	event_dt,
	store_id,
	customer_id,
	coupon_id,
	category_id,
	payment_method_id,
	quantity_cnt,
	price_liras,
	discount_liras,
	cost_liras,
	revenue_liras,
	payment_amount_liras,
	insert_dt,
	source_system,
	source_entity
)
SELECT 	nextval('SEQ_CE_SALES'),
			COALESCE(invoice_no, 'n.a.'),
			COALESCE("timestamp", '1-1-1900')::TIMESTAMP,
			stores.store_id, cust.customer_id, coup.coupon_id, cat.category_id, pm.payment_method_id, -- FKs
			COALESCE(quantity, -1),
			price, discount, costs, revenue, payment_amount, -- metrics
			NOW(),
			src.source_system,
			src.source_entity
FROM (
	SELECT 	invoice_no, "timestamp"::TIMESTAMP,
				shopping_mall, customer_id, coupon_id, category, payment_method, -- FKs
				quantity::INT, price::NUMERIC, discount::NUMERIC, costs::NUMERIC, revenue::NUMERIC,
				price::NUMERIC*quantity::INT AS payment_amount, -- metrics
				'SA_SALES_INVOICES' AS source_system, 'SRC_INVOICES' AS source_entity
	FROM sa_sales_invoices.src_invoices 
	UNION ALL
	SELECT 	'n.a.' AS invoice_no, "timestamp"::TIMESTAMP,
				shopping_mall, customer_id, 'n.a.' AS coupon_id, 'n.a.' AS category, payment_method, -- FKs
				-1 AS quantity, -1 AS price, -1 AS discount, -1 AS costs, -1 AS revenue, payment_amount::NUMERIC, -- metrics
				'SA_SALES_CUSTOMERS_CC' AS source_system, 'SRC_CUSTOMERS_CC' AS source_entity
	FROM sa_sales_customers_cc.src_customers_cc
) AS src
		LEFT JOIN bl_3nf.ce_stores stores 			ON src.shopping_mall = stores.store_src_id
		LEFT JOIN bl_3nf.ce_customers_scd cust 	ON src.customer_id = cust.customer_src_id
		LEFT JOIN bl_3nf.ce_coupons coup 			ON src.coupon_id = coup.coupon_src_id
		LEFT JOIN bl_3nf.ce_categories cat 			ON src.category = cat.category_src_id
		LEFT JOIN bl_3nf.ce_payment_methods pm 	ON src.payment_method = pm.payment_method_src_id
WHERE stores.source_system = src.source_system 	AND -- join by equal sources
		coup.source_system 	= src.source_system	AND
		cat.source_system  	= src.source_system	AND
		pm.source_system 		= src.source_system; -- customers are not needed because of BL_CL layer
	
-- Attach Foreign Keys back to ce_sales: 
ALTER TABLE ce_sales ADD CONSTRAINT FK_CE_PAYMENTS_STORE_ID FOREIGN KEY (store_id) REFERENCES BL_3NF.CE_STORES;
ALTER TABLE ce_sales ADD CONSTRAINT FK_CE_PAYMENTS_COUPON_ID FOREIGN KEY (coupon_id) REFERENCES BL_3NF.CE_COUPONS;
ALTER TABLE ce_sales ADD CONSTRAINT FK_CE_PAYMENTS_CATEGORY_ID FOREIGN KEY (category_id) REFERENCES BL_3NF.CE_CATEGORIES;
ALTER TABLE ce_sales ADD CONSTRAINT FK_CE_PAYMENTS_PAYMENT_METHOD_ID FOREIGN KEY (payment_method_id) REFERENCES BL_3NF.CE_PAYMENT_METHODS;

	
COMMIT; -- ensure deployment of changes


