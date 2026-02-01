CREATE OR REPLACE TABLE SPPL_DEV_DWH.SPPL_PUBLIC.SP_UBT_GETTERMINALINVOICE (
    TERDISPLAYID         VARCHAR(255),
    TRANSACTIONDATE            DATE,
    PRODID                  VARCHAR(255),
    LOCDISPLAYID           VARCHAR(255),
    SALES_TYPE              VARCHAR(50),
    TOTALCOUNT            INTEGER,
    AMOUNT                NUMBER(32,11),
    PROCDATE                VARCHAR(8),
    X_ETL_NAME              VARCHAR(255),
    X_RECORD_INSERT_TS       TIMESTAMP_NTZ,
    X_RECORD_UPDATE_TS      TIMESTAMP_NTZ
)
CLUSTER BY (PROCDATE);  -- Khuyến nghị: cluster theo PROCDATE vì bạn delete/insert theo ngày