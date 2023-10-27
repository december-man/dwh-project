-- BL_DM Master Procedure:

CREATE OR REPLACE PROCEDURE BL_CL.BL_DM_LOAD_MASTER() AS
$load_DM_data$
BEGIN
	RAISE INFO 'loading dim_time_day, transaction id: %', txid_current() + 1;
	CALL bl_cl.dim_time_day_load();
	COMMIT;
	RAISE INFO 'loading dim_customers_scd, transaction id: %', txid_current() + 1;
	CALL bl_cl.dim_customers_scd_load();
	COMMIT;
	RAISE INFO 'loading dim_categories, transaction id: %', txid_current() + 1;
	CALL bl_cl.dim_categories_load_proc();
	COMMIT;
	RAISE INFO 'loading dim_coupons, transaction id: %', txid_current() + 1;
	CALL bl_cl.dim_coupons_load();
	COMMIT;
	RAISE INFO 'loading dim_stores, transaction id: %', txid_current() + 1;
	CALL bl_cl.dim_stores_load();
	COMMIT;
	RAISE INFO 'loading dim_payment_methods, transaction id: %', txid_current() + 1;
	CALL bl_cl.dim_payment_methods_load();
	COMMIT;
	RAISE INFO 'Starting fct_sales load, %', NOW();
	CALL bl_cl.fct_sales_load();
	COMMIT;
END;
$load_DM_data$ LANGUAGE plpgsql; VOLATILE;

CALL BL_CL.BL_DM_LOAD_MASTER();
CALL BL_CL.BL_DM_DDL_LOAD(with_drop := TRUE);
SELECT * FROM bl_cl.mta_load_logs mll;
SELECT * FROM bl_dm.fct_sales_minutes;
SELECT txid_current();
SELECT * FROM dim_time_day;
SELECT * FROM dim_coupons;
SELECT * FROM bl_3nf.ce_coupons cc;



