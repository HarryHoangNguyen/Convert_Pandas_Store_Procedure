-- SQL for create table in SNOWFLAKE

CREATE TABLE if not exists SP_UBT_CF_SUM_SUMARY_REPORT 
	(
		HostDrawDatesId INT,
		SubProd STRING,
		FLAG VARCHAR(15),
		Amount DOUBLE,
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
    hostdrawdatesid INT, 
    actualdate date, 
    subprod STRING, 
    wager DOUBLE, 
    secondwager DOUBLE, 
    salesfactoramount DOUBLE, 
    secondsalesfactoramount DOUBLE, 
    salescommamount DOUBLE, 
    iscencelled INT,
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
    amount DOUBLE, 
    flag STRING,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ    
) CLUSTER BY (PROCDATE);

create table if not exists sp_ubt_getdailyretsummaryreport
(
    productname STRING, 
amount DOUBLE, 
flag STRING, 
PROCDATE STRING, 
X_ETL_NAME STRING, 
X_RECORD_INSERT_TS TIMESTAMP_NTZ, 
X_RECORD_UPDATE_TS TIMESTAMP_NTZ
) 
CLUSTER BY (PROCDATE);

create table if not exists sp_ubt_getinvbankdbtcrdreport
(
    chainname STRING, 
    locdisplayid STRING, 
    locname STRING, 
    amount DOUBLE,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ
) CLUSTER BY (PROCDATE);

create table if not exists sp_ubt_getinvoicereport
(
    locterdisplayid STRING, 
productname STRING, 
amount DOUBLE, 
flag STRING, 
transtype STRING, 
idtype STRING,
PROCDATE STRING,
X_ETL_NAME STRING,
X_RECORD_INSERT_TS TIMESTAMP_NTZ,
X_RECORD_UPDATE_TS TIMESTAMP_NTZ
) CLUSTER BY (PROCDATE);


create table if not exists sp_ubt_getinvoicesummaryreport
(description STRING,
 totalamountdue DOUBLE, 
 type STRING, 
 PROCDATE STRING, 
 X_ETL_NAME STRING, 
 X_RECORD_INSERT_TS TIMESTAMP_NTZ, 
 X_RECORD_UPDATE_TS TIMESTAMP_NTZ) CLUSTER BY (PROCDATE);


create table if not exists sp_ubt_getpaynowclaim
(
    cartid STRING, 
    transactionid STRING, 
    ticketserialnumber STRING, 
    ticketclaimstatus STRING, 
    claimmode STRING, 
    ticketcount INT, 
    creditamt DOUBLE, 
    transactionfee DOUBLE, 
    validationamt DOUBLE, 
    transactiondate date, 
    businessdate date, 
    idtype STRING, 
    nric STRING,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ
) CLUSTER BY (PROCDATE);

create table if not exists SP_UBT_GETSALESCOMMREPORT
(
    chainhead STRING, 
    locationid STRING, 
    locationname STRING, 
    productname STRING, 
    invoicedate STRING, 
    salescomamount DOUBLE, 
    gstcomamount DOUBLE, 
    adjsalescom DOUBLE, 
    adjgstcom DOUBLE,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ
) CLUSTER BY (PROCDATE);

CREATE TABLE IF NOT exists SP_UBT_PAYNOWTRANSBYCHAINREPORT
(
    flag STRING, 
    id STRING, 
    amount DOUBLE,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ
) CLUSTER BY (PROCDATE);

CREATE TABLE IF NOT exists SP_UBT_SSRSINVOICE
(
    flag STRING, 
    productname STRING, 
    transtype STRING, 
    amount DOUBLE,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ
) CLUSTER BY (PROCDATE);

create table if not exists sp_ubt_insertpaysport
(
    BusinessDate STRING,
    PROCESSTYPE STRING
    TXNDATE DATE,
    TOTALPRIZEAMOUNT DOUBLE,
    TOTALREFUNDAMOUNT DOUBLE,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ
) CLUSTER BY (PROCDATE);

CREATE TABLE IF NOT EXISTS sp_ubt_getoperatinghours
(
    LOCATIONNAME STRING,
    LOCATIONID STRING,
    TERMINALID STRING,
    OPENTIME STRING,
    SignOnDate STRING,
    SignOnTime STRING,
    FirstSaleTime STRING,
    FirstHourCount STRING,
    CLOSETIME STRING,
    SignOffDate STRING,
    SignOffTime STRING,
    LastSaleTime STRING,
    LastHourCount STRING,
    PROCDATE STRING,
    X_ETL_NAME STRING,
    X_RECORD_INSERT_TS TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS TIMESTAMP_NTZ
)