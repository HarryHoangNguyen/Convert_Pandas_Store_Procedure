#! /home/minhhoang/pyspark/.venv/bin/python
import pandas as pd
import os, sys, time, warnings
import numpy as np
# Suppress all warnings
warnings.filterwarnings('ignore')

# Suppress specific pandas warnings
pd.options.mode.chained_assignment = None
pd.set_option('future.no_silent_downcasting', True)

start_time = time.time()

# ================= Import Modules =================
# Import local config reader
from config_reader import read_local_config
cfg = read_local_config()
# Import required modules
from Snowflake_connection import snowflake_connection
from sp_ubt_gettransamountdetails import sp_ubt_gettransamountdetails
from write_pandas import *
from logger_util import get_logger
sp_name = "SP_UBT_GETRTSHOPCLOUD"
etl_name = "UBTI_3.0.2_TR_RTMS_SAPFIN_SHOP"
logger, log_folder, parquet_folder = get_logger(sp_name)
connection = snowflake_connection()
# ================= End Import Modules =================


print(f"ETL Job: {etl_name} - Stored Procedure: {sp_name} started.")



# ================= Start Stored Procedure: SP_UBT_GETRTSHOPCLOUD =================
logger.info("Starting Stored Pocedure: SP_UBT_GETRTSHOPCLOUD")
# ================= Process Variables =================
    # ================= Process Procdate =================
# Determine process date based on config or default to yesterday
try:
    if cfg['MODE_CONFIG']['MODE'].upper() == 'LOCAL':
        procdate = cfg['USER CONFIG']['PROCDATE']
    else:
        procdate = sys.argv[1]
except Exception as e:
    logger.error("Error determining procdate", exc_info=e)
    raise
# print(f"Initial procdate from config: {procdate}")
# ==========================================
# Get procdate from command line argument if provided
# procdate = sys.argv[1]
# If no command line argument, get procdate from yesterday's date
# ==========================================
# Because procdate from config may not be set correctly, we check if it starts with '2' (indicating a year like 2023)
try:
    if not procdate.startswith('2'):
        raise ValueError("Invalid procdate format")
except Exception as e:
    logger.warning("Invalid procdate format detected, defaulting to yesterday's date.", exc_info=e)
#print(f"Using procdate: {procdate}")
#print(f"Processing data for procdate: {pd.to_datetime(procdate, format='%Y%m%d')}")
logger.info(f"Runtime procdate: {procdate}")
# ================= End Process Variables =================

# ================= GST Config =================
# GST CONFIG
logger.info("Loading GST Config...")
query = "SELECT * FROM SPPL_DEV_DWH.SPPL_PUBLIC.ztubt_gstconfig"
df_ztubt_gstconfig = pd.read_sql(query, connection)

# Convert date columns to datetime for proper comparison
df_ztubt_gstconfig['EFFECTIVEFROM'] = pd.to_datetime(df_ztubt_gstconfig['EFFECTIVEFROM'])
df_ztubt_gstconfig['ENDDATE'] = pd.to_datetime(df_ztubt_gstconfig['ENDDATE'])

# Convert procdate to datetime for comparison
procdate_dt = pd.to_datetime(procdate, format='%Y%m%d')

df_ztubt_gstconfig = df_ztubt_gstconfig[(df_ztubt_gstconfig['EFFECTIVEFROM'] <= procdate_dt) &
                ((df_ztubt_gstconfig['ENDDATE'] >= procdate_dt))]
try:
    vgstrate = df_ztubt_gstconfig.iloc[0]['GSTRATE']
    #print(f"GST Rate used: {vgstrate}")
    logger.info(f"GST Rate used: {vgstrate}")
except IndexError:
    vgstrate = 0
    #print("No GST Rate found for the given procdate. Defaulting to 0.")
    logger.warning("No GST Rate found for the given procdate. Defaulting to 0.")


# ================= End GST Config =================

# ================= Declare vinputtax =================
# select case when procdate <= date '2022-10-31' then 'ZA'
# 				when procdate >= date '2022-11-01' and procdate <= date '2023-12-31' then 'YA'
# 				when procdate >= date '2024-01-01' then 'XA'

# 	select case when procdate <= date '2022-10-31' then 'P1'
# 				when procdate >= date '2022-11-01' and procdate <= date '2023-12-31' then 'P2'
# 				when procdate >= date '2024-01-01' then 'P3'
# 			end into vInputTax;

# vgst_branch
try:
    procdate_dt = pd.to_datetime(procdate, format='%Y%m%d')
    logger.info("Determining vInputTax values based on procdate...")
    if procdate_dt <= pd.to_datetime('2022-10-31'):
        vGST_Branch = 'ZA'
    elif pd.to_datetime('2022-11-01') <= procdate_dt <= pd.to_datetime('2023-12-31'):
        vGST_Branch = 'YA'
    else:  # procdate_dt >= '2024-01-01'
        vGST_Branch = 'XA'
# vInputTax
    if procdate_dt <= pd.to_datetime('2022-10-31'):
        vInputTax = 'P1'
    elif pd.to_datetime('2022-11-01') <= procdate_dt <= pd.to_datetime('2023-12-31'):
        vInputTax = 'P2'
    else:  # procdate_dt >= '2024-01-01'
        vInputTax = 'P3'
    logger.info(f"vInputTax determined: {vInputTax}")
except Exception as e:
    logger.error("Error determining vInputTax", exc_info=e)
    raise
# ================= End vInputTax =================

# ================= Declare Other Variables =================
# vPeriodStartDate
vPeriodStartDate  = None
# vPeriodEndDate
vPeriodEndDate = procdate
# vTaxCode - initialized here
vTaxCode = None
# vOutputTax
vOutputTax = 'S3'
# vARCode_Q_Retailer
vARCode_Q_Retailer = '`24100060'
# vARCode_Q_Branch
vARCode_Q_Branch = '24100061'
# vARCode_P_Retailer
vARCode_P_Retailer = '24100062'
# vARCode_P_Branch
vARCode_P_Branch = '24100063'
# vSeq
vSeq = 1
# v1_docdate = procdate + 1 days
v1_docdate = (pd.to_datetime(procdate, format='%Y%m%d') + pd.Timedelta(days=1))
# vNowDayName
vnowDayName = pd.to_datetime(procdate, format='%Y%m%d').dayofweek  # Monday=0, Sunday=6




# ================= End Other Variables =================

# ================= Reading all the related Data =================
# Load ztubt_terminal data
query = "SELECT * FROM SPPL_DEV_DWH.SPPL_PUBLIC.ztubt_terminal"
df_ztubt_terminal = pd.read_sql(query, connection)
logger.debug(f"ztubt_terminal rows={len(df_ztubt_terminal)}")
# load ztubt_location data
query = "SELECT * FROM SPPL_DEV_DWH.SPPL_PUBLIC.ztubt_location"
df_ztubt_location = pd.read_sql(query, connection)
logger.debug(f"ztubt_location rows={len(df_ztubt_location)}")
#================= End Reading all the related Data =================

#================= df_V2_Tad_firstdate =================
#print("Processing df_V2_Tad_firstdate... (sp_ubt_gettransamountdetails)")
logger.info("Calling SP_UBT_GETTRANSAMOUNTDETAILS ...")

df_V2_TAD_Firstdate = sp_ubt_gettransamountdetails(procdate, procdate, logger)
logger.debug(f"sp_ubt_gettransamountdetails Columns names {df_V2_TAD_Firstdate.columns.tolist()}, rows={len(df_V2_TAD_Firstdate)}")
if df_V2_TAD_Firstdate.empty:
    logger.warning("No sp_ubt_gettransamountdetails data returned for this procdate = {procdate}.")
else:
    logger.info(f"sp_ubt_gettransamountdetails returned {len(df_V2_TAD_Firstdate)} records for procdate = {procdate}.")
    df_V2_TAD_Firstdate.rename(columns={'PRODNAME': 'PRODUCTNAME'}, inplace=True)
    df_V2_TAD_Firstdate.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_V2_TAD_FirstDate(sp_ubt_gettransamountdetails).parquet'), engine='pyarrow', index=False)

#================= End df_V2_Tad_firstdate =================

# ================= UBT_TMP_Paynow_Firstdate =================
logger.info("Processing PayNow & PayNowQR...")
# Part 1: PaynowQR processing
paynowqr_df = df_V2_TAD_Firstdate[
    (df_V2_TAD_Firstdate['FLAG'] == 'CAS') &
    (df_V2_TAD_Firstdate['PRODUCTNAME'].isin(['PaynowQR [+trans fee]', 'PaynowQR'])) &
    (df_V2_TAD_Firstdate['AMOUNT'] != 0)
].groupby('TERDISPLAYID').agg(
    Amt=('AMOUNT', lambda x: x[df_V2_TAD_Firstdate['PRODUCTNAME'] == 'PaynowQR [+trans fee]'].sum()),
    Fee=('AMOUNT', lambda x: x[(df_V2_TAD_Firstdate['PRODUCTNAME'] == 'PaynowQR') & (df_V2_TAD_Firstdate['TRANSTYPE'] == 'TF')].sum())
).reset_index()
paynowqr_df['PRODUCTNAME'] = 'PaynowQR'
# Part 2: Paynow processing
paynow_df = df_V2_TAD_Firstdate[
    (df_V2_TAD_Firstdate['FLAG'] == 'CAS') &
    (df_V2_TAD_Firstdate['PRODUCTNAME'].isin(['Paynow [+trans fee]', 'Paynow'])) &
    (df_V2_TAD_Firstdate['AMOUNT'] != 0)
].groupby('TERDISPLAYID').agg(
    Amt=('AMOUNT', lambda x: x[df_V2_TAD_Firstdate['PRODUCTNAME'] == 'Paynow [+trans fee]'].sum()),
    Fee=('AMOUNT', lambda x: x[(df_V2_TAD_Firstdate['PRODUCTNAME'] == 'Paynow') & (df_V2_TAD_Firstdate['TRANSTYPE'] == 'TF')].sum())
).reset_index()
paynow_df['PRODUCTNAME'] = 'Paynow'
# Combine results (equivalent to SQL UNION)
if not paynowqr_df.empty or not paynow_df.empty:
    df_paynow_firstdate = pd.concat([paynowqr_df, paynow_df], ignore_index=True)
    # Reorder columns to match SQL output
    df_paynow_firstdate = df_paynow_firstdate[['Amt', 'Fee', 'PRODUCTNAME', 'TERDISPLAYID']]

    df_paynow_firstdate['AMOUNT'] = df_paynow_firstdate['Amt'] - df_paynow_firstdate['Fee']
    df_paynow_firstdate['CT'] = 0
    df_paynow_firstdate['FLAG'] = 'CAS'
    df_paynow_firstdate['TRANSTYPE'] = 'CL'
    df_paynow_firstdate['FROMDATE'] = procdate
    df_paynow_firstdate['TODATE'] = procdate
    df_paynow_firstdate = df_paynow_firstdate[['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'CT', 'FLAG', 'TRANSTYPE', 'FROMDATE', 'TODATE']]
else:
    df_paynow_firstdate = pd.DataFrame(columns=['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'CT', 'FLAG', 'TRANSTYPE', 'FROMDATE', 'TODATE'])
df_paynow_firstdate.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_Paynow_Firstdate.parquet'), engine='pyarrow', index=False)
logger.debug(f"Total PayNow records: rows={len(df_paynow_firstdate)}")
# Output the result
# ================= End UBT_TMP_Paynow_Firstdate =================

# ================= Concat df_V2_Tad_firstdate =================
df_V2_TAD_Firstdate = pd.concat([df_V2_TAD_Firstdate, df_paynow_firstdate], ignore_index=True)
df_V2_TAD_Firstdate.reset_index(drop=True, inplace=True)
# filter df_V2_TAD_Firstdate is ubt_tmp_V2_TAD_FirstDate

# Output the result
# ================= End Concat df_V2_Tad_firstdate =================

#================= df_V2_TerminalTempData =================
logger.info(f"Filtering TerminalTempData...")
df_V2_TerminalTempData = pd.DataFrame(columns=['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TYPE'])
df_V2_TerminalTempData = df_V2_TAD_Firstdate[
                        ~df_V2_TAD_Firstdate['FLAG']\
                        .isin(['CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL'])
                        ][['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TRANSTYPE']]\
                        .rename(columns={'TRANSTYPE': 'TYPE'})
df_V2_TerminalTempData.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_V2_TerminalTempData.parquet'), engine='pyarrow', index=False)
logger.debug(f"TerminalTempData rows={len(df_V2_TerminalTempData)}")
#================= End df_V2_TerminalTempData =================

#================= ubt_tmp_V2_LocationTempData =================
logger.info("Enriching transaction data with terminal & location info...")
merged_df = df_V2_TerminalTempData.merge(
    df_ztubt_terminal,
    left_on='TERDISPLAYID', right_on='TERDISPLAYID',
    how='inner'
).merge(
    df_ztubt_location,
    left_on='LOCID', right_on='LOCID',
    how='inner'
)
logger.info(f"Enriched dataset size: rows={len(merged_df)}")
if merged_df.empty:
    logger.warning("Enriched dataset is empty.")

# Part 1: Flag in FUN, REC
df_FUN_REC = merged_df[merged_df['FLAG'].isin(['FUN', 'REC'])]\
    .groupby(['LOCID', 'LOCDISPLAYID', 'LOCNAME', 'LOCTYPEID', 'ISIBG', 'ISGST', 'PRODUCTNAME', 'FLAG', 'TYPE', 'ISHQ'])\
    .agg(
        AMOUNT=('AMOUNT', 'max')
    ).reset_index()
# Part 2: Other Flags
df_OtherFlags = merged_df[~merged_df['FLAG'].isin(['FUN', 'REC'])]\
    .groupby(['LOCID', 'LOCDISPLAYID', 'LOCNAME', 'LOCTYPEID', 'ISIBG', 'ISGST', 'PRODUCTNAME', 'FLAG', 'TYPE', 'ISHQ'])\
    .agg(
        AMOUNT=('AMOUNT', 'sum')
    ).reset_index()
# Combine both parts
df_V2_LocationTempData = pd.concat([df_FUN_REC, df_OtherFlags], ignore_index=True)

# Convert Amount to cent by multiplying by 100
df_V2_LocationTempData['AMOUNT'] = (df_V2_LocationTempData['AMOUNT'] * 100)

# Re-order columns to match SQL output
df_V2_LocationTempData = df_V2_LocationTempData[
    ['LOCID', 'LOCDISPLAYID', 'LOCNAME', 'LOCTYPEID', 'ISIBG', 'ISGST', 'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TYPE', 'ISHQ']
]

# Rename columns to match SQL output
df_V2_LocationTempData.rename(columns={
    'LOCTYPEID': 'LOCTYPE'
}, inplace=True)
df_V2_LocationTempData.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_V2_LocationTempData.parquet'), engine='pyarrow', index=False)

#================= End ubt_tmp_V2_LocationTempData =================

# ================== Define df_V2_TB_RTMS_RTShopCloud =================
df_V2_TB_RTMS_RTShopCloud = pd.DataFrame({
    "IDMMBUSINESSDAY": pd.Series(dtype="str"),
    "BUSINESSDATE": pd.Series(dtype="datetime64[ns]"),
    "ITEMID": pd.Series(dtype="str"),
    "TRANSACTIONID": pd.Series(dtype="str"),
    "DOCUMENTDATE": pd.Series(dtype="datetime64[ns]"),
    "LINECODE": pd.Series(dtype="str"),
    "SAPDOCTYPE": pd.Series(dtype="str"),
    "SAPPOSTINGKEY": pd.Series(dtype="str"),
    "SAPCONTROLACCTCODE": pd.Series(dtype="str"),
    "LINETEXT": pd.Series(dtype="str"),
    "GLNUMBER": pd.Series(dtype="str"),
    "SAPTAXCODE": pd.Series(dtype="str"),
    "SAPTAXBASEAMOUNT": pd.Series(dtype="int"),
    "CCCODE": pd.Series(dtype="str"),
    "SAPASSIGNMENT": pd.Series(dtype="str"),
    "CURRENCYCODE": pd.Series(dtype="str"),
    "AMOUNT": pd.Series(dtype="float"),
    "PRODUCT": pd.Series(dtype="str"),
    "DRAWNUMBER": pd.Series(dtype="str"),
    "CUSTOMER": pd.Series(dtype="str"),
    "X_RECORD_INSERT_TS": pd.Series(dtype="str"),
    "X_RECORD_UPDATE_TS": pd.Series(dtype="str"),
    "X_ETL_NAME": pd.Series(dtype="str"),
    "PROCDATE": pd.Series(dtype="str")
})

# ================== Process df_V2_TB_RTMS_RTShopCloud =================
from Paynow_and_PaynowQR import *

try:
    # Check if LocationTempData has required columns
    if df_V2_LocationTempData.empty:
        logger.warning("LocationTempData is empty, initializing empty result")
        df_V2_TB_RTMS_RTShopCloud = pd.DataFrame({
            "IDMMBUSINESSDAY": pd.Series(dtype="str"),
            "BUSINESSDATE": pd.Series(dtype="datetime64[ns]"),
            "ITEMID": pd.Series(dtype="str"),
            "TRANSACTIONID": pd.Series(dtype="str"),
            "DOCUMENTDATE": pd.Series(dtype="datetime64[ns]"),
            "LINECODE": pd.Series(dtype="str"),
            "SAPDOCTYPE": pd.Series(dtype="str"),
            "SAPPOSTINGKEY": pd.Series(dtype="str"),
            "SAPCONTROLACCTCODE": pd.Series(dtype="str"),
            "LINETEXT": pd.Series(dtype="str"),
            "GLNUMBER": pd.Series(dtype="str"),
            "SAPTAXCODE": pd.Series(dtype="str"),
            "SAPTAXBASEAMOUNT": pd.Series(dtype="int"),
            "CCCODE": pd.Series(dtype="str"),
            "SAPASSIGNMENT": pd.Series(dtype="str"),
            "CURRENCYCODE": pd.Series(dtype="str"),
            "AMOUNT": pd.Series(dtype="float"),
            "PRODUCT": pd.Series(dtype="str"),
            "DRAWNUMBER": pd.Series(dtype="str"),
            "CUSTOMER": pd.Series(dtype="str")
        })
    else:
        df_V2_TB_RTMS_RTShopCloud = PayNow_and_PaynowQR(df_V2_LocationTempData, procdate, vSeq, v1_docdate, vARCode_Q_Retailer, vgstrate, vOutputTax, vARCode_P_Branch, vARCode_P_Retailer, vARCode_Q_Branch)

    logger.info(f"[PayNow_and_PaynowQR] TB_RTMS_RTShopCloud generated successfully, rows={len(df_V2_TB_RTMS_RTShopCloud)}")

except Exception as e:
    logger.error("[PayNow_and_PaynowQR] Failed to generate TB_RTMS_RTShopCloud", exc_info=e)
    raise

try:
    # if vNowDayName in(0,4) then
	# select case when vNowDayName=0 then inbusinessdate+ interval '-2 DAY'
	# when vNowDayName=4 then inbusinessdate+ interval '-3 DAY'
	# end as PeriodStartDate into vPeriodStartDate;
    if vnowDayName in (0, 4):
        if vnowDayName == 0:
            vPeriodStartDate = (pd.to_datetime(procdate, format='%Y%m%d') - pd.Timedelta(days=2)).strftime('%Y%m%d')
        elif vnowDayName == 4:
            vPeriodStartDate = (pd.to_datetime(procdate, format='%Y%m%d') - pd.Timedelta(days=3)).strftime('%Y%m%d')
        logger.debug(f"PeriodStartDate determined based on vnowDayName: {vPeriodStartDate}")
        df_ubt_tmp_V2_TAD_SecondDate = sp_ubt_gettransamountdetails(vPeriodStartDate, vPeriodEndDate, logger)
        # Ensure column naming consistency
        df_ubt_tmp_V2_TAD_SecondDate.rename(columns={'PRODNAME': 'PRODUCTNAME'}, inplace=True)
        df_ubt_tmp_V2_TAD_SecondDate['FROMDATE'] = vPeriodStartDate
        df_ubt_tmp_V2_TAD_SecondDate['TODATE'] = vPeriodEndDate
        # Use consistent column names
        required_cols = ['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'TICKETCOUNT', 'FLAG', 'TRANSTYPE', 'FROMDATE', 'TODATE']
        available_cols = [col for col in required_cols if col in df_ubt_tmp_V2_TAD_SecondDate.columns]
        df_ubt_tmp_V2_TAD_SecondDate = df_ubt_tmp_V2_TAD_SecondDate[available_cols]
        df_ubt_tmp_V2_TAD_SecondDate.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_V2_TAD_SecondDate.parquet'), engine='pyarrow', index=False)
        logger.debug(f"sp_ubt_gettransamountdetails for SecondDate Columns names {df_ubt_tmp_V2_TAD_SecondDate.columns.tolist()}, rows={len(df_ubt_tmp_V2_TAD_SecondDate)}")
    else:
        logger.info("vnowDayName is not 0 or 4, skipping second date processing.")
        df_ubt_tmp_V2_TAD_SecondDate = pd.DataFrame()  # Empty DataFrame if condition not met
except Exception as e:
    logger.error("Error determining PeriodStartDate", exc_info=e)
    raise
df_ubt_tmp_V2_TAD_SecondDate.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_V2_TAD_SecondDate-(sp_ubt_gettransamountdetails).parquet'), engine='pyarrow', index=False)
# Load more data using from sp_ubt_getamounttransaction and sp_ubt_getrtshopcloud_sport
try:
    from sp_ubt_getrtshopcloud_sport import *
    from sp_ubt_getamounttransaction import *
    df_V2_GAT = sp_ubt_getamounttransaction(procdate, procdate, logger)
    df_V2_GAT.rename(columns={'TKTSERIALNUMBER': 'TICKETSERIALNUMBER'}, inplace=True)
    logger.debug(f"sp_ubt_getamounttransaction rows={len(df_V2_GAT)}, columns={df_V2_GAT.columns.tolist()}")
    df_V2_GAT.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_V2_GAT-(sp_ubt_getamounttransaction).parquet'), engine='pyarrow', index=False)
    df_ubt_getrtshopcloud_sport, vSeq = sp_ubt_getrtshopcloud_sport(procdate, vSeq, vGST_Branch, vInputTax, df_V2_TAD_Firstdate, df_V2_GAT, df_ubt_tmp_V2_TAD_SecondDate,logger)
    df_ubt_getrtshopcloud_sport.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_ubt_getrtshopcloud_sport-(sp_ubt_getrtshopcloud_sport).parquet'), engine='pyarrow', index=False)
    logger.debug(f"sp_ubt_getrtshopcloud_sport rows={len(df_ubt_getrtshopcloud_sport)}")
    df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, df_ubt_getrtshopcloud_sport], ignore_index=True)
    logger.info(f"[sp_ubt_getrtshopcloud_sport] TB_RTMS_RTShopCloud updated successfully, rows={len(df_V2_TB_RTMS_RTShopCloud)}")
except Exception as e:
    logger.error("[sp_ubt_getrtshopcloud_sport] Failed to update TB_RTMS_RTShopCloud", exc_info=e)
    raise

# Load more data using from sp_ubt_getrtshopcloud_hr
try:
    from sp_ubt_getrtshopcloud_hr import *
    df_ubt_getrtshopcloud_hr = sp_ubt_getrtshopcloud_hr(procdate, vSeq, logger, df_V2_TAD_Firstdate, df_V2_GAT, df_ubt_tmp_V2_TAD_SecondDate, vInputTax)
    df_ubt_getrtshopcloud_hr.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_ubt_getrtshopcloud_hr.parquet'), engine='pyarrow', index=False)
    # Check if function returned None or empty DataFrame
    if df_ubt_getrtshopcloud_hr is None:
        logger.warning("sp_ubt_getrtshopcloud_hr returned None, initializing empty DataFrame")
        df_ubt_getrtshopcloud_hr = pd.DataFrame()

    logger.debug(f"sp_ubt_getrtshopcloud_hr rows={len(df_ubt_getrtshopcloud_hr)}")

    if not df_ubt_getrtshopcloud_hr.empty:
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, df_ubt_getrtshopcloud_hr], ignore_index=True)

    logger.info(f"[sp_ubt_getrtshopcloud_hr] TB_RTMS_RTShopCloud updated successfully, rows={len(df_V2_TB_RTMS_RTShopCloud)}")
except Exception as e:
    logger.error("[sp_ubt_getrtshopcloud_hr] Failed to update TB_RTMS_RTShopCloud", exc_info=e)
    raise
# ================== End Process df_V2_TB_RTMS_RTShopCloud =================

# ================= Final Query =================
# Function to left pad with zeros to length 12
def lpad_12(value):
    if pd.isna(value):
        return '000000000000'
    return str(value).zfill(12)

# Apply left padding to 'IDMMBusinessDay' column
df_V2_TB_RTMS_RTShopCloud['IDMMBUSINESSDAY'] = df_V2_TB_RTMS_RTShopCloud['IDMMBUSINESSDAY'].apply(lpad_12)
# Apply left padding to 'ItemID' column
df_V2_TB_RTMS_RTShopCloud['ITEMID'] = df_V2_TB_RTMS_RTShopCloud['ITEMID'].apply(lpad_12)
# # apply left padding to 'TRANSACTIONID' column
df_V2_TB_RTMS_RTShopCloud['TRANSACTIONID'] = df_V2_TB_RTMS_RTShopCloud['TRANSACTIONID'].apply(lpad_12)
# Reorder columns to match SQL output

df_V2_TB_RTMS_RTShopCloud = df_V2_TB_RTMS_RTShopCloud[[
    'IDMMBUSINESSDAY', 'BUSINESSDATE', 'ITEMID', 'TRANSACTIONID',
        'DOCUMENTDATE', 'LINECODE', 'SAPDOCTYPE', 'SAPPOSTINGKEY',
        'SAPCONTROLACCTCODE', 'LINETEXT', 'GLNUMBER', 'SAPTAXCODE',
        'SAPTAXBASEAMOUNT', 'CCCODE', 'SAPASSIGNMENT', 'CURRENCYCODE',
        'AMOUNT', 'PRODUCT', 'DRAWNUMBER', 'CUSTOMER'
]].reset_index(drop=True)
df_V2_TB_RTMS_RTShopCloud.to_parquet(os.path.join(parquet_folder, 'ubt_tmp_V2_TB_RTMS_RTShopCloud.parquet'), engine='pyarrow', index=False)
# Apply data type conversions to match Snowflake schema
logger.info("Converting data types to match Snowflake schema...")

# Handle date columns first - convert to date only (not datetime)
df_V2_TB_RTMS_RTShopCloud['BUSINESSDATE'] = pd.to_datetime(df_V2_TB_RTMS_RTShopCloud['BUSINESSDATE']).dt.date
df_V2_TB_RTMS_RTShopCloud['DOCUMENTDATE'] = pd.to_datetime(df_V2_TB_RTMS_RTShopCloud['DOCUMENTDATE']).dt.date

# Handle numeric columns with proper precision
df_V2_TB_RTMS_RTShopCloud['SAPTAXBASEAMOUNT'] = pd.to_numeric(df_V2_TB_RTMS_RTShopCloud['SAPTAXBASEAMOUNT'], errors='coerce').fillna(0).astype('int64')
df_V2_TB_RTMS_RTShopCloud['AMOUNT'] = pd.to_numeric(df_V2_TB_RTMS_RTShopCloud['AMOUNT'], errors='coerce').fillna(0)

# Replace infinite values with 0 to avoid database errors
df_V2_TB_RTMS_RTShopCloud = df_V2_TB_RTMS_RTShopCloud.replace([float('inf'), float('-inf')], 0)



# Convert to appropriate pandas types that map well to Snowflake
# df_V2_TB_RTMS_RTShopCloud = df_V2_TB_RTMS_RTShopCloud.astype({
#     'IDMMBUSINESSDAY': 'string',      # varchar
#     'ITEMID': 'string',               # string
#     'TRANSACTIONID': 'string',        # string
#     'LINECODE': 'string',             # varchar(12)
#     'SAPDOCTYPE': 'string',           # varchar(2)
#     'SAPPOSTINGKEY': 'string',        # varchar(2)
#     'SAPCONTROLACCTCODE': 'string',   # varchar(15)
#     'LINETEXT': 'string',             # varchar(50)
#     'GLNUMBER': 'string',             # varchar(10)
#     'SAPTAXCODE': 'string',           # varchar(10)
#     'CCCODE': 'string',               # varchar(10)
#     'SAPASSIGNMENT': 'string',        # varchar(18)
#     'CURRENCYCODE': 'string',         # varchar(3)
#     'PRODUCT': 'string',              # varchar(18)
#     'DRAWNUMBER': 'string',           # varchar(18)
#     'CUSTOMER': 'string',             # varchar(10)
#     'PROCDATE': 'string'              # varchar(8)
# })

# Validate string lengths according to Snowflake schema
df_V2_TB_RTMS_RTShopCloud['LINECODE'] = df_V2_TB_RTMS_RTShopCloud['LINECODE'].astype(str).str[:12]
df_V2_TB_RTMS_RTShopCloud['SAPDOCTYPE'] = df_V2_TB_RTMS_RTShopCloud['SAPDOCTYPE'].astype(str).str[:2]
df_V2_TB_RTMS_RTShopCloud['SAPPOSTINGKEY'] = df_V2_TB_RTMS_RTShopCloud['SAPPOSTINGKEY'].astype(str).str[:2]
df_V2_TB_RTMS_RTShopCloud['SAPCONTROLACCTCODE'] = df_V2_TB_RTMS_RTShopCloud['SAPCONTROLACCTCODE'].astype(str).str[:15]
df_V2_TB_RTMS_RTShopCloud['LINETEXT'] = df_V2_TB_RTMS_RTShopCloud['LINETEXT'].astype(str).str[:50]
df_V2_TB_RTMS_RTShopCloud['GLNUMBER'] = df_V2_TB_RTMS_RTShopCloud['GLNUMBER'].astype(str).str[:10]
df_V2_TB_RTMS_RTShopCloud['SAPTAXCODE'] = df_V2_TB_RTMS_RTShopCloud['SAPTAXCODE'].astype(str).str[:10]
df_V2_TB_RTMS_RTShopCloud['CCCODE'] = df_V2_TB_RTMS_RTShopCloud['CCCODE'].astype(str).str[:10]
df_V2_TB_RTMS_RTShopCloud['SAPASSIGNMENT'] = df_V2_TB_RTMS_RTShopCloud['SAPASSIGNMENT'].astype(str).str[:18]
df_V2_TB_RTMS_RTShopCloud['CURRENCYCODE'] = df_V2_TB_RTMS_RTShopCloud['CURRENCYCODE'].astype(str).str[:3]
df_V2_TB_RTMS_RTShopCloud['PRODUCT'] = df_V2_TB_RTMS_RTShopCloud['PRODUCT'].astype(str).str[:18]
df_V2_TB_RTMS_RTShopCloud['DRAWNUMBER'] = df_V2_TB_RTMS_RTShopCloud['DRAWNUMBER'].astype(str).str[:18]
df_V2_TB_RTMS_RTShopCloud['CUSTOMER'] = df_V2_TB_RTMS_RTShopCloud['CUSTOMER'].astype(str).str[:10]
# df_V2_TB_RTMS_RTShopCloud['PROCDATE'] = df_V2_TB_RTMS_RTShopCloud['PROCDATE'].astype(str).str[:8]

#print("Data type conversion completed.")
#print(f"Data prepared for TB_RTMS_RTShopCloud with {len(df_V2_TB_RTMS_RTShopCloud)} records.")
logger.info(f"Final dataset rows={len(df_V2_TB_RTMS_RTShopCloud)}.")

# Add PROCDATE, X_ETL_NAME, X_RECORD_INSERT_TS, X_RECORD_UPDATE_TS column
current_ts = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S.%f')
df_V2_TB_RTMS_RTShopCloud['X_RECORD_INSERT_TS'] = current_ts
df_V2_TB_RTMS_RTShopCloud['X_RECORD_UPDATE_TS'] = current_ts
df_V2_TB_RTMS_RTShopCloud['X_ETL_NAME'] = etl_name
df_V2_TB_RTMS_RTShopCloud['PROCDATE'] = procdate
# Write to output into Snowflake
try:
    write_to_snowflake(df_V2_TB_RTMS_RTShopCloud, connection, database = 'SPPL_DEV_DWH', schema = 'SPPL_PUBLIC', table_name = 'SP_UBT_GETRTSHOPCLOUD', procdate=procdate)
    logger.info("Insert final data into Snowflake completed successfully")
except Exception:
    logger.error("Failed to insert final dataset into Snowflake", exc_info=True)
    raise

end_time = time.time()
duration = end_time - start_time
logger.info(f"SP_UBT_GETRTSHOPCLOUD completed successfully in {duration:.2f} seconds")

#print(f"ETL process completed in {end_time - start_time:.2f} seconds.")
# ================= Final Query =================