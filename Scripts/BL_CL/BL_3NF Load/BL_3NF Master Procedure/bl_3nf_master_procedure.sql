-- BL_3NF Master Procedure:

CREATE OR REPLACE PROCEDURE BL_CL.BL_3NF_LOAD_MASTER() AS
$load_3nf_data$
BEGIN
	RAISE INFO 'loading ce_customers_scd, transaction id: %', txid_current() + 1;
	CALL bl_cl.ce_customers_scd_load();
	COMMIT;
	RAISE INFO 'loading ce_categories, transaction id: %', txid_current() + 1;
	CALL bl_cl.ce_categories_load();
	COMMIT;
	RAISE INFO 'loading ce_coupons, transaction id: %', txid_current() + 1;
	CALL bl_cl.ce_coupons_load();
	COMMIT;
	RAISE INFO 'loading ce_companies, transaction id: %', txid_current() + 1;
	CALL bl_cl.ce_companies_load();
	COMMIT;
	RAISE INFO 'loading ce_districts, transaction id: %', txid_current() + 1;
	CALL bl_cl.ce_districts_load();
	COMMIT;
	RAISE INFO 'loading ce_stores, transaction id: %', txid_current() + 1;
	CALL bl_cl.ce_stores_load();
	COMMIT;
	RAISE INFO 'loading ce_payment_methods, transaction id: %', txid_current() + 1;
	CALL bl_cl.ce_payment_methods_load();
	COMMIT;
	RAISE INFO 'Starting ce_sales load, %', NOW();
	RAISE INFO 'loading ce_sales, %', txid_current() + 1;
	CALL bl_cl.ce_sales_load();
	RAISE INFO 'Finished Loading BL_3NF Layer';
	COMMIT;
END;
$load_3nf_data$ LANGUAGE plpgsql; VOLATILE;


CALL BL_CL.BL_3NF_LOAD_MASTER();

CALL BL_CL.BL_3NF_DDL_LOAD(with_drop := TRUE);
SELECT * FROM bl_3nf.ce_customers_scd;
SELECT COUNT(*) FROM bl_3nf.ce_sales;
SELECT * FROM bl_cl.mta_load_logs mll;
SELECT * FROM bl_cl.prm_mta_incremental_load pmil;
TRUNCATE bl_cl.mta_load_logs RESTART IDENTITY CASCADE;

CALL bl_cl.iload_data('SRC_CUSTOMERS_CC'::TEXT, 'CE_SALES'::TEXT, 'ce_sales_load()', '1900-01-01'::TIMESTAMPTZ);
CALL bl_cl.iload_data('SRC_INVOICES'::TEXT, 'CE_SALES'::TEXT, 'ce_sales_load()', '1900-01-01'::TIMESTAMPTZ);




