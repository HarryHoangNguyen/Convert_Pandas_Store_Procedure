#! /home/harryhoangnguyen/HoangNguyen/Adnovum/Convert_Pandas_Store_Procedure/.venv/bin/python3
import pandas as pd
import sys
import os

# Add the current directory to Python path to enable relative imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from Utilities.config_reader import read_local_config
cfg = read_local_config()

# Only proceed if config is loaded successfully
if cfg is None:
    print("Failed to load configuration. Exiting...")
    sys.exit(1)

from Store_Procedure_Common.sp_ubt_getcommonubtdates import *
from Utilities.write_pandas import *
import warnings, time
from Utilities.Snowflake_connection import *

connection = snowflake_connection()
# Initialize Snowflake connection with error handling
try:
    connection = snowflake_connection()
    print("Snowflake connection initialized successfully")
except Exception as e:
    print(f"Failed to initialize Snowflake connection: {e}")
    connection = None

warnings.filterwarnings("ignore")
start_time = time.time()
import logging
# Tạo thư mục logs nếu chưa tồn tại
current_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(current_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)
# Tạo tên file log với timestamp
log_filename = f"Main_sp_ubt_gettransactiondataforinterval_{time.strftime('%Y%m%d')}.log"
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
ETL_name = "Main_sp_ubt_gettransactiondataforinterval"
logger = logging.getLogger(ETL_name)
# Ghi log đầu tiên để xác nhận
logger.info(f"Logging started. Log file: {log_filepath}")
logger.info(f"Current working directory: {current_dir}")

# %% [markdown]
# # Declare Variables

# %% [markdown]
# ## Date Variables
#

# %%
# Just for testing purposes
# ===== Variables for testing only =====
# get today's date as startdatetime and enddatetime
# 2025-09-03, 2025-09-10, 2025-09-16, 2025-09-17, 2025-09-18, 2025-09-19, 2025-09-22, 2025-09-23, 2025-09-24
# startdatetime = pd.to_datetime('2025-09-23')
# # Get tomorrow's date as enddatetime
# enddatetime = (pd.to_datetime(startdatetime) + pd.Timedelta(days=1))

# For production, uncomment the following lines to get startdatetime and enddatetime from command line arguments
try:
    if cfg['MODE_CONFIG']['MODE'] == 'LOCAL':
        startdatetime = pd.to_datetime(cfg['USER CONFIG']['PROCDATE'])
        enddatetime = startdatetime + pd.Timedelta(days=1)
        logger.info(f"Running in LOCAL mode. startdatetime: {startdatetime}, enddatetime: {enddatetime}")
    else:
        logger.info("Running in PRODUCTION mode. Getting dates from command line arguments.")
        startdatetime = pd.to_datetime(sys.argv[1])
        enddatetime = pd.to_datetime(sys.argv[2])
except Exception as e:
    logger.error(f"Error in getting dates: {e}. Switching to command line argument mode.")


print(f"Start DateTime: {startdatetime}, End DateTime: {enddatetime}")
logger.info(f"Start DateTime: {startdatetime}, End DateTime: {enddatetime}")
# # ===== End of variables for testing only =====
df_dates = sp_ubt_getcommonubtdates(startdatetime, startdatetime)

# Start assigning values
startdate = pd.to_datetime(startdatetime)
enddate = pd.to_datetime(enddatetime)
bmcs_startdate = df_dates.loc[0, 'FROMDATEBMCS']
igt_startdate = df_dates.loc[0, 'FROMDATEIGT']
ob_startdate = df_dates.loc[0, 'FROMDATEOB']
bmcs_enddate = df_dates.loc[0, 'TODATEBMCS']
igt_enddate = df_dates.loc[0, 'TODATEIGT']
ob_enddate = df_dates.loc[0, 'TODATEOB']
bmcs_currentbusinessdate = df_dates.loc[0, 'FROMDATEBMCS']
igt_currentbusinessdate = df_dates.loc[0, 'FROMDATEIGT']
ob_currentbusinessdate = df_dates.loc[0, 'FROMDATEOB']
bmcs_nextbusinessdate = df_dates.loc[0, 'FROMDATEBMCS'] + pd.Timedelta(days=1)
igt_nextbusinessdate = df_dates.loc[0, 'FROMDATEIGT'] + pd.Timedelta(days=1)
ob_nextbusinessdate = df_dates.loc[0, 'FROMDATEOB'] + pd.Timedelta(days=1)
bmcs_previousbusinessdate = df_dates.loc[0, 'FROMDATEBMCS'] - pd.Timedelta(days=1)
igt_previousbusinessdate = df_dates.loc[0, 'FROMDATEIGT'] - pd.Timedelta(days=1)
ob_previousbusinessdate = df_dates.loc[0, 'FROMDATEOB'] - pd.Timedelta(days=1)
# startdateutc is startdate - 8 hours
startdateUTC = startdate - pd.Timedelta(hours=8)
# enddateutc is enddate - 8 hours
enddateUTC = enddate - pd.Timedelta(hours=8)

# tôi muốn lưu các giá trị trên thành dataframe và write ra csv để kiểm tra
df_date_values = pd.DataFrame({
    'startdate': [startdate],
    'enddate': [enddate],
    'startdateUTC': [startdateUTC],
    'enddateUTC': [enddateUTC],
    'bmcs_startdate': [bmcs_startdate],
    'igt_startdate': [igt_startdate],
    'ob_startdate': [ob_startdate],
    'bmcs_enddate': [bmcs_enddate],
    'igt_enddate': [igt_enddate],
    'ob_enddate': [ob_enddate],
    'bmcs_currentbusinessdate': [bmcs_currentbusinessdate],
    'igt_currentbusinessdate': [igt_currentbusinessdate],
    'ob_currentbusinessdate': [ob_currentbusinessdate],
    'bmcs_nextbusinessdate': [bmcs_nextbusinessdate],
    'igt_nextbusinessdate': [igt_nextbusinessdate],
    'ob_nextbusinessdate': [ob_nextbusinessdate],
    'bmcs_previousbusinessdate': [bmcs_previousbusinessdate],
    'igt_previousbusinessdate': [igt_previousbusinessdate],
    'ob_previousbusinessdate': [ob_previousbusinessdate]
})

# ===== End of assigning values =====
logger.info(f"startdate: {startdate}, enddate: {enddate}, startdateUTC: {startdateUTC}, enddateUTC: {enddateUTC}")
print(f"startdate: {startdate}, enddate: {enddate}, startdateUTC: {startdateUTC}, enddateUTC: {enddateUTC}")
# %% [markdown]
# ## Snowflake Variables

# %%
database = "SPPL_DEV_DWH"
schema = "SPPL_PUBLIC"
table_name = "SP_UBT_GETTRANSACTIONDATAFORINTERVAL"

# %% [markdown]
# # Processing SP_UBT_GETTRANSACTIONDATAFORINTERVAL

# %% [markdown]
# ## Placebettransaction & Placebettransactiontype

# %%
from ETL.Transformation_gettransactiondataforinterval import *
# Using the function to get the placebettransaction dataframe
print("Start date:  End Date: StartDateUTC: EnddateUTC:\n",startdate, enddate, startdateUTC, enddateUTC)
df_ubt_temp_placebettransaction = ubt_temp_placebettransaction(startdate, enddate, startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_PLACEBETTRANSACTION rows: {len(df_ubt_temp_placebettransaction)}")
print("UBT_TEMP_PLACEBETTRANSACTION rows: ", len(df_ubt_temp_placebettransaction))


# Select TicketSerialNumber, TranHeaderID, EntryMethodID, DeviceID, ProdID,
#    IsBetRejectedByTrader,IsExchangeTicket, TerDisplayID
#   ,SUM(TransactionType) TransactionTypeTotal
#   from ubt_temp_placebettransaction
#   Group By TicketSerialNumber, TranHeaderID, DeviceID, EntryMethodID, ProdID,
#   IsExchangeTicket, TerDisplayID, IsBetRejectedByTrader

df_ubt_temp_placebettransactiontype = df_ubt_temp_placebettransaction.groupby(['TICKETSERIALNUMBER', 'TRANHEADERID', 'DEVICEID', 'ENTRYMETHODID', 'PRODID',
                                                    'ISEXCHANGETICKET', 'TERDISPLAYID', 'ISBETREJECTEDBYTRADER'],as_index=False,dropna=False)\
    .agg({'TRANSACTIONTYPE': 'sum'})\
    .rename(columns={'TRANSACTIONTYPE': 'TRANSACTIONTYPETOTAL'})\

logger.info(f"UBT_TEMP_PLACEBETTRANSACTIONTYPE rows: {len(df_ubt_temp_placebettransactiontype)}")
print("UBT_TEMP_PLACEBETTRANSACTIONTYPE rows: ", len(df_ubt_temp_placebettransactiontype))
# %% [markdown]
# ## UBT_TEMP_BETTYPE

# %%
logger.info("Processing UBT_TEMP_BETTYPE")
df_ubt_temp_bettype = ubt_temp_bettype(df_ubt_temp_placebettransactiontype, startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_BETTYPE rows: {len(df_ubt_temp_bettype)}")

# %% [markdown]
# ## UBT_TEMP_HORSEDETAIL

# %%


logger.info("Processing UBT_TEMP_HORSEDETAIL")
df_ubt_temp_horsedetail = ubt_temp_horsedetail(df_ubt_temp_placebettransactiontype, startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_HORSEDETAIL rows: {len(df_ubt_temp_horsedetail)}")

# %% [markdown]
# ## UBT_TEMP_NUMBOARD

# %%
logger.info("Processing UBT_TEMP_NUMBOARD")
df_ubt_temp_numboard = ubt_temp_numboard(df_ubt_temp_placebettransactiontype, startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_NUMBOARD rows: {len(df_ubt_temp_numboard)}")

# %% [markdown]
# ## ubt_temp_LotteryDraw

# %%
logger.info("Processing UBT_TEMP_LOTTERYDRAW")
df_ubt_temp_lotterydraw = ubt_temp_lotterydraw(df_ubt_temp_placebettransactiontype)
logger.info(f"UBT_TEMP_LOTTERYDRAW rows: {len(df_ubt_temp_lotterydraw)}")

# %% [markdown]
# ## UBT_TEMP_NUMDRAW

# %%
logger.info("Processing UBT_TEMP_NUMDRAW")
df_ubt_temp_numdraw = ubt_temp_numdraw(df_ubt_temp_lotterydraw)
logger.info(f"UBT_TEMP_NUMDRAW rows: {len(df_ubt_temp_numdraw)}")

# %% [markdown]
# ## ubt_temp_TotoSeq

# %%
logger.info("Processing UBT_TEMP_TOTOSEQ")
df_ubt_temp_TotoSeq = ubt_temp_TotoSeq(df_ubt_temp_placebettransactiontype,startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_TOTOSEQ rows: {len(df_ubt_temp_TotoSeq)}")
# %% [markdown]
# ## ubt_Temp_TotoGroup

# %%
logger.info("Processing UBT_TEMP_TOTOGROUP")
df_ubt_temp_totogroup = ubt_temp_totogroup(df_ubt_temp_placebettransactiontype)
logger.info(f"UBT_TEMP_TOTOGROUP rows: {len(df_ubt_temp_totogroup)}")
# %% [markdown]
# ## ubt_Temp_LotteryDetail

# %%
logger.info("Processing UBT_TEMP_LOTTERYDETAILS")
df_ubt_temp_lotterydetails = ubt_temp_lotterydetails(df_ubt_temp_placebettransactiontype,df_ubt_temp_numboard, df_ubt_temp_numdraw,df_ubt_temp_TotoSeq,df_ubt_temp_totogroup , startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_LOTTERYDETAILS rows: {len(df_ubt_temp_lotterydetails)}")

# %% [markdown]
# ## ubt_temp_Liveindicator
#

# %%
logger.info("Processing UBT_TEMP_LIVEINDICATOR")
df_ubt_temp_liveindicator = ubt_temp_liveindicator(df_ubt_temp_placebettransactiontype)
logger.info(f"UBT_TEMP_LIVEINDICATOR rows: {len(df_ubt_temp_liveindicator)}")

# %% [markdown]
# # ubt_temp_sportsdetail

# %%
logger.info("Processing UBT_TEMP_SPORT_DETAILS")
df_ubt_temp_sport_details = ubt_temp_sport_details(df_ubt_temp_placebettransactiontype, df_ubt_temp_liveindicator,df_ubt_temp_bettype)
logger.info(f"UBT_TEMP_SPORT_DETAILS rows: {len(df_ubt_temp_sport_details)}")

# %%

# %%
# Fix duplicate EVENTID columns
df_ubt_temp_sport_details = df_ubt_temp_sport_details.loc[:, ~df_ubt_temp_sport_details.columns.duplicated()]

# %% [markdown]
# # UBT_TEMP_TRANS
#

# %% [markdown]
#

# %%
logger.info("Processing UBT_TEMP_TRANS")
df_ubt_temp_trans = ubt_temp_trans(df_ubt_temp_placebettransaction, startdate, enddate, startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_TRANS rows: {len(df_ubt_temp_trans)}")

# %% [markdown]
# ## UBT_TEMP_RESULTWAGER

# %%
logger.info("Processing UBT_TEMP_RESULTWAGER")
df_ubt_temp_resultwager = ubt_temp_resultwager(df_ubt_temp_placebettransactiontype, startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_RESULTWAGER rows: {len(df_ubt_temp_resultwager)}")

# %% [markdown]
# ## ubt_temp_resultsalescomwithdate

# %%
logger.info("Processing UBT_TEMP_RESULTSALESCOMWITHDATE")
df_ubt_temp_resultsalescomwithdate = ubt_temp_resultsalescomwithdate(df_ubt_temp_placebettransactiontype, startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_RESULTSALESCOMWITHDATE rows: {len(df_ubt_temp_resultsalescomwithdate)}")
# %% [markdown]
# ## UBT_Temp_resultsalessandcomm

# %%
logger.info("Processing UBT_TEMP_RESULTSALESSANDCOMM")
df_ubt_temp_resultsalessandcomm = ubt_temp_resultsalessandcomm(df_ubt_temp_resultsalescomwithdate)
logger.info(f"UBT_TEMP_RESULTSALESSANDCOMM rows: {len(df_ubt_temp_resultsalessandcomm)}")

# %% [markdown]
# ## UBT_TEMP_ResultGST

# %%
logger.info("Processing UBT_TEMP_RESULTGST")
df_ubt_temp_resultgst = ubt_temp_resultgst(df_ubt_temp_resultwager, df_ubt_temp_placebettransactiontype, df_ubt_temp_resultsalessandcomm)
logger.info(f"UBT_TEMP_RESULTGST rows: {len(df_ubt_temp_resultgst)}")

# %% [markdown]
# ## UBT_TEMP_RESULTVALIDATION

# %%
logger.info("Processing UBT_TEMP_RESULTVALIDATION")
df_ubt_temp_resultvalidation = ubt_temp_resultvalidation(df_ubt_temp_resultwager)
logger.info(f"UBT_TEMP_RESULTVALIDATION rows: {len(df_ubt_temp_resultvalidation)}")
# %% [markdown]
# ## ubt_temp_resultvalidationexchange

# %%
logger.info("Processing UBT_TEMP_RESULTVALIDATIONEXCHANGE")
df_ubt_temp_resultvalidationexchange = ubt_temp_resultvalidationexchange(df_ubt_temp_placebettransactiontype)
logger.info(f"UBT_TEMP_RESULTVALIDATIONEXCHANGE rows: {len(df_ubt_temp_resultvalidationexchange)}")
# %% [markdown]
# ## ubt_temp_transactionamount

# %%
logger.info("Processing UBT_TEMP_TRANSACTIONAMOUNT")
df_ubt_temp_transactionamount = ubt_temp_transactionamount(df_ubt_temp_resultwager, df_ubt_temp_resultsalessandcomm,
                                                           df_ubt_temp_resultgst, df_ubt_temp_resultvalidation, df_ubt_temp_resultvalidationexchange)
logger.info(f"UBT_TEMP_TRANSACTIONAMOUNT rows: {len(df_ubt_temp_transactionamount)}")

# %% [markdown]
# ## ubt_temp_final_lotteryselect

# %%
logger.info("Processing UBT_TEMP_FINAL_LOTTERYSELECT")
df_ubt_temp_final_lotteryselect = ubt_temp_final_lotteryselect(df_ubt_temp_placebettransactiontype,startdateUTC, enddateUTC)
logger.info(f"UBT_TEMP_FINAL_LOTTERYSELECT rows: {len(df_ubt_temp_final_lotteryselect)}")

# %% [markdown]
# ## ubt_temp_transactiondata

# %%
logger.info("Processing UBT_TEMP_TRANSACTIONDATA")
df_ubt_temp_transactiondata = ubt_temp_transactiondata(df_ubt_temp_trans, df_ubt_temp_bettype, df_ubt_temp_lotterydetails,
                             df_ubt_temp_final_lotteryselect, df_ubt_temp_transactionamount, df_ubt_temp_horsedetail,
                             df_ubt_temp_placebettransactiontype, df_ubt_temp_sport_details, startdate, enddate,
                             igt_startdate, igt_enddate, igt_previousbusinessdate, igt_nextbusinessdate, igt_currentbusinessdate,
                             bmcs_startdate, bmcs_enddate, bmcs_previousbusinessdate, bmcs_nextbusinessdate, bmcs_currentbusinessdate,
                             ob_startdate, ob_enddate, ob_previousbusinessdate, ob_nextbusinessdate, ob_currentbusinessdate)
logger.info(f"UBT_TEMP_TRANSACTIONDATA rows: {len(df_ubt_temp_transactiondata)}")


# Adding 4 columns:

df_ubt_temp_transactiondata = df_ubt_temp_transactiondata.assign(
    X_ETL_NAME = "UBTI_1.0.2_TR_RTMS_Bet_Transactions",
    X_RECORD_INSERT_TS = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
    X_RECORD_UPDATE_TS = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
    PROCDATE = startdatetime.strftime('%Y-%m-%d')
)

# %%
# Write dataframe to Snowflake
logger.info("Writing UBT_TEMP_TRANSACTIONDATA to Snowflake")
write_to_snowflake(
    df_ubt_temp_transactiondata,
    connection,
    database,
    schema,
    table_name,
    procdate=startdatetime.strftime('%Y-%m-%d')
)

# %% [markdown]
# # TEST
#

# %%
end_time = time.time()
print(f"Execution time: {end_time - start_time} seconds")


