-- SQL for create table in SNOWFLAKE

CREATE TABLE if not exists SP_UBT_CF_SUM_SUMARY_REPORT 
	(
		HostDrawDatesId INT,
		SubProd STRING,
		FLAG VARCHAR(15),
		Amount numeric(32, 12),
        PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ
	) CLUSTER BY (PROCDATE);

create table if not exists sp_ubt_check_draw_close_date
(
    HostDrawDatesId INT,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ
) cluster by (PROCDATE);

create table if not exists sp_ubt_getcfsumreport
(
    hostdrawdatesid integer, 
    actualdate date, 
    subprod STRING, 
    wager numeric, 
    secondwager numeric, 
    salesfactoramount numeric, 
    secondsalesfactoramount numeric, 
    salescommamount numeric, 
    iscencelled integer,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ    
) CLUSTER BY (PROCDATE);

create table if not exists sp_ubt_getdailyretsummarybychainreport

(
    typeid STRING, 
    id STRING, 
    productname STRING, 
    amount numeric, 
    flag STRING,
    , PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ    
) CLUSTER BY (PROCDATE);