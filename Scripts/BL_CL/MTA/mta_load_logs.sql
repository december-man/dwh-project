-- Log Table DDL
-- It has to include a timestamp, name of the procedure, number of rows affected, error/success message

--DROP TABLE IF EXISTS bl_cl.mta_load_logs;
CREATE TABLE IF NOT EXISTS bl_cl.mta_load_logs ( -- mta is short for metadata table
	id 					INT 				GENERATED ALWAYS AS IDENTITY,
	proc_name 			VARCHAR(500)	NOT NULL,
	rows_affected 		INT 				NOT NULL,
	status				TEXT 				NOT NULL DEFAULT 'SUCCESS',
	time 					TIMESTAMPTZ 	DEFAULT NOW()
);

-- Logging procedure
-- This procedure will be called by all DML scripts to log changes DWH-wide.

CREATE OR REPLACE PROCEDURE bl_cl.load_logger (
	pg_context		VARCHAR,
	row_count	 	INT,
	op_status		TEXT DEFAULT 'SUCCESS'
)
AS $load_logger$
	INSERT INTO bl_cl.mta_load_logs (proc_name, rows_affected, status)
	SELECT pg_context, row_count, op_status
$load_logger$ LANGUAGE SQL;

-- Procedure info
SELECT * FROM pg_proc WHERE proname ~~ '%logger';


