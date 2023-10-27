-- BL_CL Loading Procedures

-- DIM_TIME_DAY

CREATE OR REPLACE PROCEDURE bl_cl.dim_time_day_load()
AS $load_time_dim$
DECLARE 
context TEXT; context_short TEXT; row_count INT; err_code TEXT; err_msg TEXT; 
err_context TEXT; err_context_short TEXT; err_detail TEXT;
BEGIN
	INSERT INTO bl_dm.dim_time_day
		SELECT 
			datum AS EVENT_DT,
			EXTRACT(EPOCH FROM datum) AS EPOCH,
			TO_CHAR(datum, 'TMDay') AS DAY_NAME,
			EXTRACT(ISODOW FROM datum) AS DAY_OF_WEEK,
			CASE
				WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
				ELSE FALSE
				END AS IS_WEEKEND,
			EXTRACT(DAY FROM datum) AS DAY_OF_MONTH,
			datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS DAY_OF_QUARTER,
			EXTRACT(DOY FROM datum) AS DAY_OF_YEAR,
			TO_CHAR(datum, 'W')::INT AS WEEK_OF_MONTH,
			EXTRACT(WEEK FROM datum) AS WEEK_OF_YEAR,
			EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS WEEK_OF_YEAR_ISO,
			EXTRACT(MONTH FROM datum) AS "MONTH",
			TO_CHAR(datum, 'TMMonth') AS MONTH_NAME,
			TO_CHAR(datum, 'Mon') AS MONTH_NAME_SUFFIX,
			EXTRACT(QUARTER FROM datum) AS "QUARTER",
			CASE
				WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
				WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
				WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
				WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
				END AS QUARTER_NAME,
			EXTRACT(YEAR FROM datum) AS "YEAR",
			datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS FIRST_DAY_OF_WEEK,
			datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS LAST_DAY_OF_WEEK,
			datum + (1 - EXTRACT(DAY FROM datum))::INT AS FIRST_DAY_OF_MONTH,
			(DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS LAST_DAY_OF_MONTH,
			DATE_TRUNC('quarter', datum)::DATE AS FIRST_DAY_OF_QUARTER,
			(DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS LAST_DAY_OF_QUARTER,
			TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS FIRST_DAY_OF_YEAR,
			TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS LAST_DAY_OF_YEAR,
			TO_CHAR(datum, 'mmyyyy') AS MMYYYY,
			TO_CHAR(datum, 'mmddyyyy') AS MMDDYYYY
		FROM (
				SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
	      	FROM GENERATE_SERIES(0, 21914) AS SEQUENCE (DAY)
	      	GROUP BY SEQUENCE.DAY) DQ
		WHERE NOT EXISTS (SELECT 1 FROM bl_dm.dim_time_day)
	ORDER BY EVENT_DT ASC;
-- Logging
	GET DIAGNOSTICS 
		context := PG_CONTEXT,
		row_count := ROW_COUNT;
		context_short := SUBSTRING(context FROM 'function (.*?) line');
	-- call logger on successful insert
	CALL bl_cl.load_logger(context_short, row_count);
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS
			err_context := PG_EXCEPTION_CONTEXT,
			err_detail 	:= PG_EXCEPTION_DETAIL;
			err_context_short := SUBSTRING(err_context FROM 'function (.*?) line');
	-- call logger in case of exception
		CALL bl_cl.load_logger(err_context_short, 0, FORMAT('ERROR %s: %s. Details: %s', SQLSTATE, SQLERRM, err_detail));
		RAISE WARNING 'STATE: %, ERRM: %', SQLSTATE, SQLERRM;
END; 
$load_time_dim$ LANGUAGE plpgsql;

CALL bl_cl.dim_time_day_load();

SELECT * FROM bl_cl.mta_load_logs mll;
SELECT * FROM bl_dm.dim_time_day;


