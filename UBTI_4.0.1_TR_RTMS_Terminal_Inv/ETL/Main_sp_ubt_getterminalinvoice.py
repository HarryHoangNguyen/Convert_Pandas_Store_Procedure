import pandas as pd
import numpy as np
from sp_ubt_getcommonubtdates import *
from sp_ubt_gettransamountdetails import *
from sp_ubt_getamounttransaction import *
from sp_ubt_getsweepsalespersrterminal import *
from declare_variables import *
from Transformation import *
from write_pandas import *
from Snowflake_connection import *

connection = snowflake_connection()
import logging
import os, sys, time, warnings
warnings.filterwarnings("ignore")
start_time = time.time()

# ============================================================
# ===================== Cấu hình logging =====================
# ============================================================
# Tạo thư mục logs nếu chưa tồn tại
today = time.strftime('%Y-%m-%d')
current_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(current_dir, 'logs', today)
os.makedirs(logs_dir, exist_ok=True)

# Tạo tên file log với timestamp
log_filename = f"Main_sp_ubt_getterminalinvoice_{time.strftime('%Y%m%d')}.log"
log_filepath = os.path.join(logs_dir, log_filename)

level_logging = 'INFO'  # Chỉnh mức logging ở đây: DEBUG, INFO, WARNING, ERROR, CRITICAL

# Cấu hình logging: ghi ra file trong thư mục logs
logging.basicConfig(
    filename=log_filepath,               # File log trong thư mục logs với timestamp
    level=getattr(logging, level_logging),                 # Ghi từ mức INFO trở lên
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8'                     # Quan trọng để ghi tiếng Việt không bị lỗi font
)
ETL_name = "Main_sp_ubt_getterminalinvoice"
logger = logging.getLogger(ETL_name)

# Ghi log đầu tiên để xác nhận
logger.info(f"Logging started. Log file: {log_filepath}")
logger.info(f"Current working directory: {current_dir}")

# ============================================================
# ===================== Declare Variables ====================
# ============================================================
try:
    if cfg['MODE_CONFIG']['MODE'] == 'LOCAL':
        procdate = pd.to_datetime(cfg['USER CONFIG']['PROCDATE'], format='%Y-%m-%d')
    else:
        procdate = sys.argv[1]
        procdate = pd.to_datetime(procdate, format='%Y-%m-%d')
    logger.info(f"procdate set to: {procdate}")
except Exception as e:
    logger.error(f"Error setting procdate: {e}")
    sys.exit(1)

# procdate = '2025-06-25'  # Định nghĩa procdate ở đây cho dễ test

vbusinessdate,\
vinputdate,\
vstartperiod,\
vendperiod,\
vfromdateIGT,\
vtodateIGT,\
vfromdatetimeIGT_UTC,\
vtodatetimeIGT_UTC,\
vfromdatetimeOB_UTC,\
vtodatetimeOB_UTC,\
vfromdatetimeNonHost_UTC,\
vtodatetimeNonHost_UTC,\
vfromdatetimeBMCS_UTC,\
vtodatetimeBMCS_UTC,\
vGSTRate,\
vactualdate = declare_variables(procdate)
print(f"vbusinessdate: {vbusinessdate}")
print(f"vfromdateIGT: {vfromdateIGT}")
print(f"vtodateIGT: {vtodateIGT}")
print(f"vfromdatetimeIGT_UTC: {vfromdatetimeIGT_UTC}")
print(f"vtodatetimeIGT_UTC: {vtodatetimeIGT_UTC}")
print(f"vfromdatetimeOB_UTC: {vfromdatetimeOB_UTC}")
print(f"vtodatetimeOB_UTC: {vtodatetimeOB_UTC}")
print(f"vfromdatetimeNonHost_UTC: {vfromdatetimeNonHost_UTC}")
print(f"vtodatetimeNonHost_UTC: {vtodatetimeNonHost_UTC}")
print(f"vfromdatetimeBMCS_UTC: {vfromdatetimeBMCS_UTC}")
print(f"vtodatetimeBMCS_UTC: {vtodatetimeBMCS_UTC}")
print(f"vGSTRate: {vGSTRate}")

logger.info("Starting ETL process...")
logger.info(f"procdate: {procdate}")

    # ============================================================
    # ======================== Main ETL ==========================
    # ============================================================

# ==============================
# UBT_TEMP_TERMINAL
# ==============================
try:
    logger.info("Processing UBT_TEMP_TERMINAL...")
    print("Processing UBT_TEMP_TERMINAL...")
    df_ubt_temp_terminal = ubt_temp_terminal()
    logger.info(f"UBT_TEMP_TERMINAL processed successfully. Rows: {len(df_ubt_temp_terminal)}")
    print(f"UBT_TEMP_TERMINAL processed successfully. Rows: {len(df_ubt_temp_terminal)}")
except Exception as e:
    print(f"Error processing UBT_TEMP_TERMINAL: {e}")
    logger.error(f"Error processing UBT_TEMP_TERMINAL: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_terminal.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_terminal.csv') else None
df_ubt_temp_terminal.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_terminal.csv', index=False)

# ==============================
# UBT_TEMP_PRODUCT
# ==============================
try:
    logger.info("Processing UBT_TEMP_PRODUCT...")
    print("Processing UBT_TEMP_PRODUCT...")
    df_ubt_temp_product = ubt_temp_product()
    logger.info(f"UBT_TEMP_PRODUCT processed successfully. Rows: {len(df_ubt_temp_product)}")
    print(f"UBT_TEMP_PRODUCT processed successfully. Rows: {len(df_ubt_temp_product)}")
except Exception as e:
    logger.error(f"Error processing UBT_TEMP_PRODUCT: {e}")
    print(f"Error processing UBT_TEMP_PRODUCT: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_product.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_product.csv') else None
df_ubt_temp_product.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_product.csv', index=False)
# ==============================
# UBT_TEMP_LOCATION
# ==============================
try:
    logger.info("Processing UBT_TEMP_LOCATION...")
    print("Processing UBT_TEMP_LOCATION...")
    df_ubt_temp_location = ubt_temp_location()
    logger.info(f"UBT_TEMP_LOCATION processed successfully. Rows: {len(df_ubt_temp_location)}")
    print(f"UBT_TEMP_LOCATION processed successfully. Rows: {len(df_ubt_temp_location)}")
except Exception as e:
    logger.error(f"Error processing UBT_TEMP_LOCATION: {e}")
    print(f"Error processing UBT_TEMP_LOCATION: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_location.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_location.csv') else None
df_ubt_temp_location.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_location.csv', index=False)
# ==============================
# UBT_TEMP_TmpTicketByWageAndSales
# ==============================
try:
    logger.info("Processing UBT_TEMP_TmpTicketByWageAndSales...")
    print("Processing UBT_TEMP_TmpTicketByWageAndSales...")

    df_ubt_temp_TmpTicketByWageAndSales = ubt_temp_TmpTicketByWageAndSales(vbusinessdate)


    logger.info(f"UBT_TEMP_TmpTicketByWageAndSales processed successfully. Rows: {len(df_ubt_temp_TmpTicketByWageAndSales)}")
    print(f"UBT_TEMP_TmpTicketByWageAndSales processed successfully. Rows: {len(df_ubt_temp_TmpTicketByWageAndSales)}")
except Exception as e:
    logger.error(f"Error processing UBT_TEMP_TmpTicketByWageAndSales: {e}")
    print(f"Error processing UBT_TEMP_TmpTicketByWageAndSales: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_TmpTicketByWageAndSales.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_TmpTicketByWageAndSales.csv') else None
df_ubt_temp_TmpTicketByWageAndSales.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_TmpTicketByWageAndSales.csv', index=False)



# ==============================
# UBT_TEMP_ResultCashlessInTerminal
# ==============================
try:
    logger.info("Processing UBT_TEMP_ResultCashlessInTerminal...")
    print("Processing UBT_TEMP_ResultCashlessInTerminal...")


    df_ubt_temp_ResultCashlessInTerminal = ubt_temp_ResultCashlessInTerminal(vfromdatetimeNonHost_UTC,
                                                                                vtodatetimeNonHost_UTC)


    logger.info(f"UBT_TEMP_ResultCashlessInTerminal processed successfully. Rows: {len(df_ubt_temp_ResultCashlessInTerminal)}")
    print(f"UBT_TEMP_ResultCashlessInTerminal processed successfully. Rows: {len(df_ubt_temp_ResultCashlessInTerminal)}")
except Exception as e:
    print(f"Error processing UBT_TEMP_ResultCashlessInTerminal: {e}")
    logger.error(f"Error processing UBT_TEMP_ResultCashlessInTerminal: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_ResultCashlessInTerminal.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_ResultCashlessInTerminal.csv') else None
df_ubt_temp_ResultCashlessInTerminal.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_ResultCashlessInTerminal.csv', index=False)

# ==============================
# ubt_temp_iTOTO
# ==============================
try:
    logger.info("Processing ubt_temp_iTOTO...")
    print("Processing ubt_temp_iTOTO...")
    df_ubt_temp_iTOTO = ubt_temp_iTOTO(vfromdatetimeIGT_UTC,
                                       vtodatetimeIGT_UTC)
    logger.info(f"ubt_temp_iTOTO processed successfully. Rows: {len(df_ubt_temp_iTOTO)}")
    print(f"ubt_temp_iTOTO processed successfully. Rows: {len(df_ubt_temp_iTOTO)}")
except Exception as e:
    logger.error(f"Error processing ubt_temp_iTOTO: {e}")
    print(f"Error processing ubt_temp_iTOTO: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_iTOTO.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_iTOTO.csv') else None
df_ubt_temp_iTOTO.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_iTOTO.csv', index=False)
# ==============================
# UBT_TEMP_GROUPTOTO
# ==============================
try:
    logger.info("Processing UBT_TEMP_GROUPTOTO...")
    print("Processing UBT_TEMP_GROUPTOTO...")
    df_ubt_temp_GroupTOTO = ubt_temp_grouptoto(vfromdatetimeIGT_UTC,
                                               vtodatetimeIGT_UTC)
    logger.info(f"UBT_TEMP_GROUPTOTO processed successfully. Rows: {len(df_ubt_temp_GroupTOTO)}")
    print(f"UBT_TEMP_GROUPTOTO processed successfully. Rows: {len(df_ubt_temp_GroupTOTO)}")
except Exception as e:
    logger.error(f"Error processing UBT_TEMP_GROUPTOTO: {e}")
    print(f"Error processing UBT_TEMP_GROUPTOTO: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_GroupTOTO.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_GroupTOTO.csv') else None
df_ubt_temp_GroupTOTO.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_GroupTOTO.csv', index=False)
# ==============================
# ubt_temp_CancelledBetTicketState
# ==============================
try:
    logger.info("Processing ubt_temp_CancelledBetTicketState...")
    print("Processing ubt_temp_CancelledBetTicketState...")
    df_ubt_temp_CancelledBetTicketState = ubt_temp_CancelledBetTicketState(vfromdateIGT,
                                                                           vtodateIGT)
    logger.info(f"ubt_temp_CancelledBetTicketState processed successfully. Rows: {len(df_ubt_temp_CancelledBetTicketState)}")
    print(f"ubt_temp_CancelledBetTicketState processed successfully. Rows: {len(df_ubt_temp_CancelledBetTicketState)}")
except Exception as e:
    logger.error(f"Error processing ubt_temp_CancelledBetTicketState: {e}")
    print(f"Error processing ubt_temp_CancelledBetTicketState: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_CancelledBetTicketState.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_CancelledBetTicketState.csv') else None
df_ubt_temp_CancelledBetTicketState.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_CancelledBetTicketState.csv', index=False)
# =============================
# ubt_temp_SalesGroupToto
# ============================

try:
    logger.info("Processing ubt_temp_SalesGroupToto...")
    print("Processing ubt_temp_SalesGroupToto...")


    df_ubt_temp_SalesGroupToto = ubt_temp_SalesGroupToto(df_ubt_temp_terminal,
                                                        df_ubt_temp_location,
                                                        df_ubt_temp_iTOTO,
                                                        df_ubt_temp_TmpTicketByWageAndSales,
                                                        df_ubt_temp_CancelledBetTicketState,
                                                        vfromdatetimeIGT_UTC,
                                                        vtodatetimeIGT_UTC)


    logger.info(f"ubt_temp_SalesGroupToto processed successfully. Rows: {len(df_ubt_temp_SalesGroupToto)}")
    print(f"ubt_temp_SalesGroupToto processed successfully. Rows: {len(df_ubt_temp_SalesGroupToto)}")
except Exception as e:
    logger.error(f"Error processing ubt_temp_SalesGroupToto: {e}")
    print(f"Error processing ubt_temp_SalesGroupToto: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_SalesGroupToto.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_SalesGroupToto.csv') else None
df_ubt_temp_SalesGroupToto.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_SalesGroupToto.csv', index=False)
# =============================
# ubt_temp_salestoto
# ============================
try:
    logger.info("Processing ubt_temp_salestoto...")
    print("Processing ubt_temp_salestoto...")


    df_ubt_temp_salestoto = ubt_temp_salestoto(df_ubt_temp_terminal,
                                               df_ubt_temp_location,
                                               df_ubt_temp_iTOTO,
                                               df_ubt_temp_TmpTicketByWageAndSales,
                                               df_ubt_temp_CancelledBetTicketState,
                                               vfromdatetimeIGT_UTC,
                                               vtodatetimeIGT_UTC)


    logger.info(f"ubt_temp_salestoto processed successfully. Rows: {len(df_ubt_temp_salestoto)}")
    print(f"ubt_temp_salestoto processed successfully. Rows: {len(df_ubt_temp_salestoto)}")
except Exception as e:
    logger.error(f"Error processing ubt_temp_salestoto: {e}")
    print(f"Error processing ubt_temp_salestoto: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_salestoto.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_salestoto.csv') else None
df_ubt_temp_salestoto.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_salestoto.csv', index=False)
# =============================
# ubt_temp_transamountdetaildata
# ============================
try:
    logger.info("Processing ubt_temp_transamountdetaildata...")
    print("Processing ubt_temp_transamountdetaildata...")
    df_ubt_temp_transamountdetaildata = sp_ubt_gettransamountdetails(vbusinessdate.strftime("%Y%m%d"), vbusinessdate.strftime("%Y%m%d"))

    # select terdisplayid,	trim(prodname),(amount * 100)	,ticketcount,	flag,transtype
	# from public.sp_ubt_gettransamountdetails(vbusinessdate,vbusinessdate);
    df_ubt_temp_transamountdetaildata = df_ubt_temp_transamountdetaildata[['TERDISPLAYID',
                                                                           'PRODNAME',
                                                                           'AMOUNT',
                                                                           'TICKETCOUNT',
                                                                           'FLAG',
                                                                           'TRANSTYPE']]
    df_ubt_temp_transamountdetaildata['AMOUNT'] = df_ubt_temp_transamountdetaildata['AMOUNT'] * 100
    df_ubt_temp_transamountdetaildata['PRODNAME'] = df_ubt_temp_transamountdetaildata['PRODNAME'].str.strip()


    df_ubt_temp_transamountdetaildata = df_ubt_temp_transamountdetaildata.rename(columns={
        'TERDISPLAYID': 'TERDISPLAYID',
        'PRODNAME': 'PRODUCTNAME',
        'AMOUNT': 'AMOUNT',
        'TICKETCOUNT': 'CT',
        'FLAG': 'FLAG',
        'TRANSTYPE': 'TRANSTYPE'
    })

    print(df_ubt_temp_transamountdetaildata.columns.tolist())


    print(f"ubt_temp_transamountdetaildata processed successfully. Rows: {len(df_ubt_temp_transamountdetaildata)}")
    logger.info(f"ubt_temp_transamountdetaildata processed successfully. Rows: {len(df_ubt_temp_transamountdetaildata)}")
except Exception as e:
    logger.error(f"Error processing ubt_temp_transamountdetaildata: {e}")
    print(f"Error processing ubt_temp_transamountdetaildata: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_transamountdetaildata.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_transamountdetaildata.csv') else None
df_ubt_temp_transamountdetaildata.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_transamountdetaildata.csv', index=False)

# =============================
# ubt_temp_sales_scandsr
# ============================

try:
    logger.info("Processing ubt_temp_sales_scandsr...")
    print("Processing ubt_temp_sales_scandsr...")
    df_ubt_temp_sales_scandsr = sp_ubt_getsweepsalespersrterminal(vbusinessdate, vbusinessdate)
    logger.info(f"ubt_temp_sales_scandsr processed successfully. Rows: {len(df_ubt_temp_sales_scandsr)}")
    print(f"ubt_temp_sales_scandsr processed successfully. Rows: {len(df_ubt_temp_sales_scandsr)}")

except Exception as e:
    logger.error(f"Error processing ubt_temp_sales_scandsr: {e}")
    print(f"Error processing ubt_temp_sales_scandsr: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_sales_scandsr.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_sales_scandsr.csv') else None
df_ubt_temp_sales_scandsr.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_sales_scandsr.csv', index=False)

# =============================
# ubt_temp_salesfactorconfig
# ============================
try:
    logger.info("Processing ubt_temp_salesfactorconfig...")
    print("Processing ubt_temp_salesfactorconfig...")
    df_ubt_temp_salesfactorconfig = ubt_temp_salesfactorconfig()
    logger.info(f"ubt_temp_salesfactorconfig processed successfully. Rows: {len(df_ubt_temp_salesfactorconfig)}")
    print(f"ubt_temp_salesfactorconfig processed successfully. Rows: {len(df_ubt_temp_salesfactorconfig)}")
except Exception as e:
    logger.error(f"Error processing ubt_temp_salesfactorconfig: {e}")
    print(f"Error processing ubt_temp_salesfactorconfig: {e}")
    sys.exit(1)

os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_salesfactorconfig.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_salesfactorconfig.csv') else None
df_ubt_temp_salesfactorconfig.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_salesfactorconfig.csv', index=False)




# =============================
# ubt_temp_tmpterlocprdsalesamt
# ============================
try:
    logger.info("Processing ubt_temp_tmpterlocprdsalesamt...")
    print("Processing ubt_temp_tmpterlocprdsalesamt...")


    df_ubt_temp_tmpterlocprdsalesamt = ubt_temp_tmpterlocprdsalesamt(df_ubt_temp_transamountdetaildata,
                                  df_ubt_temp_terminal,
                                  df_ubt_temp_product,
                                  df_ubt_temp_location,
                                  df_ubt_temp_CancelledBetTicketState,
                                  df_ubt_temp_iTOTO,
                                  df_ubt_temp_SalesGroupToto,
                                  df_ubt_temp_GroupTOTO,
                                  df_ubt_temp_ResultCashlessInTerminal,
                                  df_ubt_temp_TmpTicketByWageAndSales,
                                  df_ubt_temp_salestoto,
                                  df_ubt_temp_sales_scandsr,
                                  df_ubt_temp_salesfactorconfig,




                                  vfromdateIGT,
                                  vtodateIGT,
                                  vfromdatetimeIGT_UTC,
                                  vtodatetimeIGT_UTC,
                                  vfromdatetimeOB_UTC,
                                  vtodatetimeOB_UTC,
                                  vfromdatetimeBMCS_UTC,
                                  vtodatetimeBMCS_UTC,
                                    vbusinessdate,
                                  vGSTRate)


    logger.info(f"ubt_temp_tmpterlocprdsalesamt processed successfully. Rows: {len(df_ubt_temp_tmpterlocprdsalesamt)}")
    print(f"ubt_temp_tmpterlocprdsalesamt processed successfully. Rows: {len(df_ubt_temp_tmpterlocprdsalesamt)}")

except Exception as e:
    logger.error(f"Error processing ubt_temp_tmpterlocprdsalesamt: {e}")
    print(f"Error processing ubt_temp_tmpterlocprdsalesamt: {e}")
    sys.exit(1)
os.remove('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_tmpterlocprdsalesamt.csv') if os.path.exists('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_tmpterlocprdsalesamt.csv') else None
df_ubt_temp_tmpterlocprdsalesamt.to_csv('/mnt/c/Users/minhhoang/Compare_401/ubt_temp_tmpterlocprdsalesamt.csv', index=False)

# =============================
# Kết thúc ETL
# ============================

# Add 4 new columns to df_ubt_temp_tmpterlocprdsalesamt

df_ubt_temp_tmpterlocprdsalesamt = df_ubt_temp_tmpterlocprdsalesamt.assign(
    PROCDATE = procdate.strftime('%Y%m%d'),
    X_ETL_NAME = "UBTI_4.0.1_TR_RTMS_Terminal_Inv",
    X_RECORD_INSERT_TS = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
    X_RECORD_UPDATE_TS = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
)

# Convert any datetime columns to string format to avoid Snowflake cast errors
timestamp_cols = df_ubt_temp_tmpterlocprdsalesamt.select_dtypes(include=['datetime64']).columns.tolist()
for col in timestamp_cols:
    df_ubt_temp_tmpterlocprdsalesamt[col] = df_ubt_temp_tmpterlocprdsalesamt[col].dt.strftime('%Y-%m-%d %H:%M:%S')



database = "SPPL_DEV_DWH"
schema = "SPPL_PUBLIC"
table_name = "SP_UBT_GETTERMINALINVOICE"
write_to_snowflake(
    dataframe=df_ubt_temp_tmpterlocprdsalesamt,
    connection = connection,
    database=database,
    schema=schema,
    table_name=table_name,
    procdate=procdate.strftime('%Y%m%d')

)


end_time = time.time()
elapsed_time = end_time - start_time
logger.info(f"ETL process completed in {elapsed_time:.2f} seconds.")
