
-- This two following procedures are separated because of some strange issues most likely coming from foreign tables usage
-- The merged procedure would hang for no reason on loading of the ce_sales table.

CREATE OR REPLACE PROCEDURE BL_CL.DWH_LOAD_MASTER() AS
$load_DWH_data$
BEGIN
	RAISE INFO 'Building Source schemas & Staging Area, %', NOW();
	RAISE INFO 'Creating SA_SALES_INVOICES Schema .../...';
	CALL BL_CL.SRC_INVOICES_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/Increments/EXT_INVOICES_100K.csv');
	COMMIT;
	RAISE INFO 'Creating SA_SALES_CUSTOMERS_CC Schema .../...';
	CALL BL_CL.SRC_CUSTOMERS_CC_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/Increments/EXT_CUSTOMERS_CC_50K.csv');
	COMMIT;
	RAISE INFO 'Building BL_3NF Layer, %', NOW();
	CALL BL_CL.BL_3NF_LOAD_MASTER();
	COMMIT;
	RAISE INFO 'Building BL_DM Layer, %', NOW();
	CALL BL_CL.BL_DM_LOAD_MASTER();
	COMMIT;
END;
$load_DWH_data$ LANGUAGE plpgsql; VOLATILE;

-- CALL
COMMIT; -- from bad omens
CALL BL_CL.DWH_LOAD_MASTER();

-- Meta tables, loading control & maintenance
SELECT * FROM bl_cl.mta_load_logs mll;
SELECT * FROM bl_cl.prm_mta_incremental_load pmil;

-- Testing Layer
CALL bl_cl.run_dwh_tests();
SELECT * FROM bl_cl.mta_tests;

-- Separate Layer-level Procedures
CALL BL_CL.SRC_INVOICES_RELOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/EXT_INVOICES_900K.csv');
CALL BL_CL.SRC_CUSTOMERS_CC_RELOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/EXT_CUSTOMERS_CC_450K.csv');

CALL BL_CL.SRC_INVOICES_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/Increments/EXT_INVOICES_100K.csv');
CALL BL_CL.SRC_CUSTOMERS_CC_LOAD_CSV('/home/goetie/EPAM DAE/S2/DWH/DWH Project/Data Sources/Increments/EXT_CUSTOMERS_CC_50K.csv');

CALL BL_CL.BL_3NF_LOAD_MASTER();
CALL BL_CL.BL_DM_LOAD_MASTER();

-- Debug / Testing
SELECT * FROM bl_3nf.ce_sales;
SELECT * FROM bl_3nf.ce_customers_scd;
SELECT COUNT(*) FROM sa_sales_customers_cc.src_customers_cc scc;
SELECT COUNT(*) FROM sa_sales_invoices.ext_invoices si;
SELECT COUNT(*) FROM sa_sales_customers_cc.ext_customers_cc ecc;
SELECT COUNT(*) FROM sa_sales_invoices.src_invoices si;
SELECT COUNT(*) FROM bl_3nf.ce_customers_scd;
SELECT COUNT(*) FROM bl_3nf.ce_categories cc;
SELECT * FROM bl_dm.dim_customers_scd;
SELECT * FROM bl_dm.dim_stores;
SELECT COUNT(*) FROM bl_dm.fct_sales_minutes;
TRUNCATE bl_cl.mta_load_logs RESTART IDENTITY CASCADE;
TRUNCATE bl_cl.prm_mta_incremental_load;
CALL bl_cl.iload_data('SRC_CUSTOMERS_CC'::TEXT, 'CE_SALES'::TEXT, 'ce_sales_load()', '1900-01-01'::TIMESTAMPTZ);
CALL bl_cl.iload_data('SRC_INVOICES'::TEXT, 'CE_SALES'::TEXT, 'ce_sales_load()', '1900-01-01'::TIMESTAMPTZ);


-- Danger Zone
CALL BL_CL.BL_3NF_DDL_LOAD(with_drop := TRUE);
CALL BL_CL.BL_DM_DDL_LOAD(with_drop := TRUE);


