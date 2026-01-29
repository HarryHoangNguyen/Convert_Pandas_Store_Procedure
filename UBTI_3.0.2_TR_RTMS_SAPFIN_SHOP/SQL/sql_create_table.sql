CREATE OR REPLACE TABLE SPPL_DEV_DWH.SPPL_PUBLIC.SP_UBT_GETRTSHOPCLOUD (
    IDMMBUSINESSDAY         VARCHAR(255),          -- Giả định ID dài, có thể điều chỉnh nếu biết độ dài chính xác
    BUSINESSDATE            DATE,                  -- Từ pd.to_datetime().dt.date
    ITEMID                  VARCHAR(255),          -- ID dạng string
    TRANSACTIONID           VARCHAR(255),          -- ID dạng string
    DOCUMENTDATE            DATE,                  -- Từ pd.to_datetime().dt.date
    LINECODE                VARCHAR(12),           -- Cắt [:12]
    SAPDOCTYPE              VARCHAR(2),            -- Cắt [:2]
    SAPPOSTINGKEY           VARCHAR(2),            -- Cắt [:2]
    SAPCONTROLACCTCODE      VARCHAR(15),           -- Cắt [:15]
    LINETEXT                VARCHAR(50),           -- Cắt [:50]
    GLNUMBER                VARCHAR(10),           -- Cắt [:10]
    SAPTAXCODE              VARCHAR(10),           -- Cắt [:10]
    SAPTAXBASEAMOUNT        NUMBER(38,0),          -- int64 → NUMBER không scale (hoặc INTEGER)
    CCCODE                  VARCHAR(10),           -- Cắt [:10]
    SAPASSIGNMENT           VARCHAR(18),           -- Cắt [:18]
    CURRENCYCODE            VARCHAR(3),            -- Cắt [:3]
    AMOUNT                  FLOAT,                 -- float64 sau pd.to_numeric (có thể có decimal)
    PRODUCT                 VARCHAR(18),           -- Cắt [:18]
    DRAWNUMBER              VARCHAR(18),           -- Cắt [:18]
    CUSTOMER                VARCHAR(10),           -- Cắt [:10]
    PROCDATE                VARCHAR(8),            -- '20250625' dạng string length 8
    X_ETL_NAME              VARCHAR(255),          -- Tên ETL, giả định độ dài hợp lý
    X_RECORD_INSERT_TS       TIMESTAMP_NTZ,         -- String định dạng 'YYYY-MM-DD HH:MI:SS.mmmmmm'
    X_RECORD_UPDATE_TS      TIMESTAMP_NTZ          -- Giống trên
)
CLUSTER BY (PROCDATE);  -- Khuyến nghị: cluster theo PROCDATE vì bạn delete/insert theo ngày