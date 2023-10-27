-- WRK_CUSTOMERS UPDATE Procedure:

CREATE OR REPLACE PROCEDURE bl_cl.wrk_customers_refresh()
AS $wrk_cust_refresh$
DROP TABLE IF EXISTS BL_CL.WRK_CUSTOMERS;
CREATE TABLE BL_CL.WRK_CUSTOMERS AS
SELECT 	DISTINCT customer_id,
			gender,
			age,
			customer_name
FROM sa_sales_customers_cc.src_customers_cc
WHERE customer_name IS NOT NULL OR 
		(customer_name IS NULL AND customer_id NOT IN (
			SELECT DISTINCT customer_id FROM sa_sales_customers_cc.src_customers_cc WHERE customer_name IS NOT NULL));
$wrk_cust_refresh$ LANGUAGE SQL;


CALL bl_cl.wrk_customers_refresh();