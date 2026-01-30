#! /home/harryhoangnguyen/HoangNguyen/Adnovum/Convert_Pandas_Store_Procedure/.venv/bin/python3

# =====================================================
import os, sys, time, warnings
# Add the current directory to Python path to enable relative imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)




# =====================================================
# Import Libraries
# =====================================================
import pandas as pd
import numpy as np
from Store_Procedure_Common.sp_ubt_getcommonubtdates import *
from Store_Procedure_Common.sp_ubt_gettransamountdetails import *
from Store_Procedure_Common.sp_ubt_getamounttransaction import *
from Store_Procedure_Common.sp_ubt_getsweepsalespersrterminal import *
from ETL.declare_Variables_sp_ubt_getlocationinvoice import *
from ETL.Transformation_sp_ubt_getlocationinvoice import *
from Utilities.write_pandas import *
from Utilities.config_reader import *
import logging

# ===================================================== 
# Set up logging
# =====================================================



# ====================================================
# Read configuration
# =====================================================
cfg = read_local_config()


# =====================================================
# Suppress Warnings
# =====================================================
# Suppress all warnings
warnings.filterwarnings("ignore")
start_time = time.time()

# Tạo thư mục logs nếu chưa tồn tại
current_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(current_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Tạo tên file log với timestamp
log_filename = f"Main_sp_ubt_getlocationinvoice_{time.strftime('%Y%m%d')}.log"
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
ETL_name = "Main_sp_ubt_getlocationinvoice"
logger = logging.getLogger(ETL_name)

# Ghi log đầu tiên để xác nhận
logger.info(f"Logging started. Log file: {log_filepath}")
logger.info(f"Current working directory: {current_dir}")



# =====================================================
# Determine procdate based on mode
"""
    Get the processing date based on the mode specified in the configuration.
    In LOCAL mode, use the date from the configuration file.
    In PROD mode, use the date passed as a command-line argument.
"""
# =====================================================
try:
    if cfg['MODE_CONFIG']['MODE'] == 'LOCAL':
        procdate = pd.to_datetime(cfg['USER CONFIG']['PROCDATE']).strftime("%Y%m%d")

        logger.info(f"Running in LOCAL mode. procdate: {procdate}")
    else:
        logger.info("Running in PRODUCTION mode. Getting dates from command line arguments.")
        procdate = pd.to_datetime(sys.argv[1])
        logger.info(f"procdate from command line argument: {procdate}")
except Exception as e:
    logger.error(f"Error in getting dates: {e}. Switching to command line argument mode.")

# procdate = '2025-11-27'
logger.info(f"Process date: {procdate}")

# =====================================================
# Declare Variables
# =====================================================
try:
    vbusinessdate,vinput_date, vactual_date,\
    vstartperiod, vendperiod, \
    vfromdateigt, vtodateigt, vfromdatetimeigtUTC, vtodatetimeigtUTC, \
    vfromdatetimeOB_UTC, vtodatetimeOB_UTC, vfromdatetimeBMCS_UTC, vtodatetimeBMCS_UTC,\
    vinvoiceperiodid = declare_variables(procdate)

except Exception as e:
    logger.error(f"Error declaring variables: {e}")
    raise

# =====================================================
# Process ubt_temp_table
# =====================================================
try:
    logger.info("Starting UBT_TEMP_TERMINAL data extraction...")
    df_ubt_temp_terminal = ubt_temp_terminal()
    logger.info(f"UBT_TEMP_TERMINAL data extraction completed. Records: {len(df_ubt_temp_terminal)}")
except Exception as e:
    logger.error("Error processing ubt_temp_terminal", exc_info=e)
    raise

# =====================================================
# Process ubt_temp_location
# =====================================================
try:
    logger.info("Starting UBT_TEMP_LOCATION data extraction...")
    df_ubt_temp_location = ubt_temp_location()
    logger.info(f"UBT_TEMP_LOCATION data extraction completed. Records: {len(df_ubt_temp_location)}")
except Exception as e:
    logger.error("Error processing ubt_temp_location", exc_info=e)
    raise

# =====================================================
# Process ubt_temp_product
# =====================================================
try:
    logger.info("Starting UBT_TEMP_PRODUCT data extraction...")
    df_ubt_temp_product = ubt_temp_product()
    logger.info(f"UBT_TEMP_PRODUCT data extraction completed. Records: {len(df_ubt_temp_product)}")
except Exception as e:
    logger.error("Error processing ubt_temp_product", exc_info=e)
    raise

# =====================================================
# Process ubt_temp_chain
# =====================================================
try:
    logger.info("Starting UBT_TEMP_CHAIN data extraction...")
    df_ubt_temp_chain = ubt_temp_chain()
    logger.info(f"UBT_TEMP_CHAIN data extraction completed. Records: {len(df_ubt_temp_chain)}")

except Exception as e:
    logger.error("Error processing ubt_temp_chain", exc_info=e)
    raise

# =====================================================
# Process ubt_temp_cancelledbeticket
# =====================================================
try:
    logger.info("Starting ubt_temp_cancelledbeticket data extraction...")
    df_ubt_temp_cancelledbeticket = ubt_temp_cancelledbeticket(vfromdateigt, vtodateigt)
    logger.info(f"ubt_temp_cancelledbeticket data extraction completed. Records: {len(df_ubt_temp_cancelledbeticket)}")
except Exception as e:
    logger.error("Error processing ubt_temp_cancelledbeticket", exc_info=e)
    raise

# =====================================================
# Process sp_ubt_getamounttransaction
# =====================================================
try:
    logger.info("Starting sp_ubt_getamounttransaction processing...")
    df_ubt_temp_getamounttrans = sp_ubt_getamounttransaction(vstartperiod, vendperiod)
    df_ubt_temp_getamounttrans = df_ubt_temp_getamounttrans.rename(columns={
    'TKTSERIALNUMBER': 'TICKETSERIALNUMBER',
        })
    logger.info(f"sp_ubt_getamounttransaction completed. Records: {len(df_ubt_temp_getamounttrans)}")
except Exception as e:
    logger.error("Error processing sp_ubt_getamounttransaction", exc_info=e)
    raise
# Rename columns if needed


# =====================================================
# Process ubt_temp_resultcashlesslocation
# =====================================================
try:
    logger.info("Starting UBT_TEMP_RESUTLCASHLESSLOCATION data extraction...")
    df_ubt_temp_resultcashlesslocation = ubt_temp_resultcashlesslocation(vfromdatetimeigtUTC, vtodatetimeigtUTC)
    logger.info(f"UBT_TEMP_RESUTLCASHLESSLOCATION data extraction completed. Records: {len(df_ubt_temp_resultcashlesslocation)}")
except Exception as e:
    logger.error("Error processing UBT_TEMP_RESUTLCASHLESSLOCATION", exc_info=e)
    raise


# =====================================================
# Process UBT_TEMP_itotolocation
# =====================================================
try:
    logger.info("Starting UBT_TEMP_itotolocation data extraction...")
    df_ubt_temp_itotolocation = ubt_temp_iTotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC)
    logger.info(f"UBT_TEMP_itotolocation data extraction completed. Records: {len(df_ubt_temp_itotolocation)}")
except Exception as e:
    logger.error("Error processing UBT_TEMP_itotolocation", exc_info=e)
    raise

# =====================================================
# Process UBT_TEMP_GroupToToLocation
# =====================================================
try:
    logger.info("Starting UBT_TEMP_GroupToToLocation data extraction...")
    df_ubt_temp_grouptotolocation = ubt_temp_grouptotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC)
    logger.info(f"UBT_TEMP_GroupToToLocation data extraction completed. Records: {len(df_ubt_temp_grouptotolocation)}")
except Exception as e:
    logger.error("Error processing UBT_TEMP_GroupToToLocation", exc_info=e)
    raise


# =====================================================
# Process UBT_TEMP_SalesgroupToToLocation
# =====================================================
try:
    logger.info("Starting UBT_TEMP_SalesgroupTotoLocation data extraction...")
    df_ubt_temp_salesgrouptotolocation = ubt_temp_salesgrouptotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC
                                        ,df_ubt_temp_itotolocation
                                        ,df_ubt_temp_getamounttrans
                                        ,df_ubt_temp_cancelledbeticket
                                        ,df_ubt_temp_terminal
                                        ,df_ubt_temp_location)
    logger.info(f"UBT_TEMP_SalesgroupTotoLocation data extraction completed. Records: {len(df_ubt_temp_salesgrouptotolocation)}")
except Exception as e:
    logger.error("Error processing UBT_TEMP_SalesgroupTotoLocation", exc_info=e)
    raise


# =====================================================
# Process UBT_TEMP_SALESTOTOLOCATION
# =====================================================
try:    
    logger.info("Starting UBT_TEMP_SALESTOTOLOCATION data extraction...")
    df_ubt_temp_salestotolocation = ubt_temp_salestotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC,
                                df_ubt_temp_itotolocation,
                                df_ubt_temp_getamounttrans
                                ,df_ubt_temp_cancelledbeticket
                                ,df_ubt_temp_terminal
                                ,df_ubt_temp_location)
    logger.info(f"UBT_TEMP_SALESTOTOLOCATION data extraction completed. Records: {len(df_ubt_temp_salestotolocation)}")
except Exception as e:
    logger.error("Error processing UBT_TEMP_SALESTOTOLOCATION", exc_info=e)
    raise


# =====================================================
# Process sp_ubt_gettransamountdetails
# =====================================================
try:
    logger.info("Starting sp_ubt_gettransamountdetails data extraction...")
    df_ubt_temp_transamountdetaildata = sp_ubt_gettransamountdetails(vstartperiod.strftime("%Y%m%d"), vendperiod.strftime("%Y%m%d"))
    df_ubt_temp_transamountdetaildata['PRODNAME'] = df_ubt_temp_transamountdetaildata['PRODNAME'].str.strip()
    df_ubt_temp_transamountdetaildata['AMOUNT'] = df_ubt_temp_transamountdetaildata['AMOUNT'] * 100
    df_ubt_temp_transamountdetaildata = df_ubt_temp_transamountdetaildata\
        .assign(
            FROMDATE = np.nan,
            TODATE = np.nan,
        )\
        .rename(columns={
        'TERDISPLAYID': 'TERDISPLAYID',
        'PRODNAME' : "PRODUCTNAME",
        'AMOUNT' : "AMOUNT",
        'TICKETCOUNT' : 'CT',
        'FLAG' : 'FLAG',
        'TRANSTYPE' : 'TRANSTYPE'
    })[['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'CT', 'FLAG', 'TRANSTYPE', 'FROMDATE', 'TODATE']]
    logger.info(f"sp_ubt_gettransamountdetails data extraction completed. Records: {len(df_ubt_temp_transamountdetaildata)}")
except Exception as e:
    logger.error("Error processing sp_ubt_gettransamountdetails", exc_info=e)
    raise

# =====================================================
# Process ubt_temp_datalocationinvoice
# =====================================================
try:
    logger.info("Starting ubt_temp_datalocationinvoice processing...")
    df_ubt_temp_datalocationinvoice = ubt_temp_datalocationinvoice(df_ubt_temp_transamountdetaildata, df_ubt_temp_terminal, df_ubt_temp_location,
                                                                    df_ubt_temp_product, df_ubt_temp_chain, df_ubt_temp_salestotolocation,
                                                                    df_ubt_temp_cancelledbeticket, df_ubt_temp_itotolocation,
                                                                    df_ubt_temp_grouptotolocation, df_ubt_temp_salesgrouptotolocation, df_ubt_temp_resultcashlesslocation,
                                                                df_ubt_temp_getamounttrans,
                                                               vinvoiceperiodid, vfromdatetimeigtUTC, vtodatetimeigtUTC, vfromdatetimeOB_UTC, vtodatetimeOB_UTC,
                                                                vfromdatetimeBMCS_UTC, vtodatetimeBMCS_UTC,
                                                               vactual_date,vfromdateigt, vtodateigt)
    logger.info(f"ubt_temp_datalocationinvoice processing completed. Records: {len(df_ubt_temp_datalocationinvoice)}")
except Exception as e:
    logger.error("Error processing ubt_temp_datalocationinvoice", exc_info=e)
    raise

# =====================================================
# Process sp_ubt_getsweepsalespersrterminal
# =====================================================
try:
    logger.info("Starting sp_ubt_getsweepsalespersrterminal data extraction...")
    df_ubt_temp_sales_scandsr = sp_ubt_getsweepsalespersrterminal(vstartperiod, vendperiod)
    logger.info(f"sp_ubt_getsweepsalespersrterminal data extraction completed. Records: {len(df_ubt_temp_sales_scandsr)}")
except Exception as e:
    logger.error("Error processing sp_ubt_getsweepsalespersrterminal", exc_info=e)
    raise


# =====================================================
# Process ubt_temp_sales_scandsr_loc
# =====================================================
try:
    logger.info("Starting ubt_temp_sales_scandsr_loc processing...")
    df_ubt_temp_sales_scandsr_loc = ubt_temp_sales_scandsr_loc(
            df_ubt_temp_sales_scandsr,
            df_ubt_temp_terminal,
            df_ubt_temp_location,
            df_ubt_temp_chain,
            vinvoiceperiodid
    )
    logger.info(f"ubt_temp_sales_scandsr_loc processing completed. Records: {len(df_ubt_temp_sales_scandsr_loc)}")
except Exception as e:
    logger.error("Error processing ubt_temp_sales_scandsr_loc", exc_info=e)
    raise

# =====================================================
# Update ubt_temp_datalocationinvoice using df_ubt_temp_sales_scandsr_loc
# =====================================================
# Following the logic of the SQL DELETE and INSERT operations
# DELETE FROM ubt_temp_datalocationinvoice SAL  USING ubt_temp_sales_scandsr_loc RSLoc
# WHERE RSLoc.LocDisplayID = SAL.LocDisplayID AND RSLoc.AgentInvoice_productId = SAL.AgentInvoice_productId
# AND SAL.Sales_type = '116';
mask_delete =(
    (df_ubt_temp_datalocationinvoice['SALES_TYPE'] == '116') &
    (df_ubt_temp_datalocationinvoice.set_index(['LOCDISPLAYID', 'AGENTINVOICE_PRODUCTID']).index.isin(
        df_ubt_temp_sales_scandsr_loc.set_index(['LOCDISPLAYID', 'AGENTINVOICE_PRODUCTID']).index
    ))
)
df_ubt_temp_datalocationinvoice = df_ubt_temp_datalocationinvoice[~mask_delete]

# UNION ALL INSERT
df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_ubt_temp_sales_scandsr_loc], ignore_index=True)

# =====================================================
# Process ubt_temp_salesfactorconfig
# =====================================================
try:
    logger.info("Starting ubt_temp_salesfactorconfig data extraction...")
    df_ubt_temp_salesfactorconfig = ubt_temp_salesfactorconfig()
    logger.info(f"ubt_temp_salesfactorconfig data extraction completed. Records: {len(df_ubt_temp_salesfactorconfig)}")
except Exception as e:
    logger.error("Error processing ubt_temp_salesfactorconfig", exc_info=e)
    raise

# =====================================================
# Process ubt_temp_datalocationinvoice_2nd
# =====================================================
try:
    logger.info("Starting ubt_temp_datalocationinvoice_2nd processing...")
    df_ubt_temp_datalocationinvoice_2nd = ubt_temp_datalocationinvoice_2nd(
                                                                            df_ubt_temp_datalocationinvoice,
                                                                            df_ubt_temp_location,
                                                                            df_ubt_temp_chain,
                                                                            df_ubt_temp_salesfactorconfig,
                                                                            df_ubt_temp_transamountdetaildata,
                                                                            df_ubt_temp_terminal,
                                                                            vinvoiceperiodid
                                                                    )
    logger.info(f"ubt_temp_datalocationinvoice_2nd processing completed. Records: {len(df_ubt_temp_datalocationinvoice_2nd)}")
    logger.info(f"READY TO WRITE TO SNOWFLAKE WITH {len(df_ubt_temp_datalocationinvoice_2nd)} rows")
except Exception as e:
    logger.error("Error processing ubt_temp_datalocationinvoice_2nd", exc_info=e)
    raise

# %%
# 	create temp table if not exists ubt_temp_datalocationinvoice
# 	(
# locdisplayid varchar(200) ,
# agentinvoice_invid varchar(100) ,
# agentinvoice_finidn varchar(200) ,
# agentinvoice_productid int4 ,
# sales_type varchar(100) ,
# totalcount int4 ,
# amount numeric(32, 11),
# sub_prod varchar(100)
# 	)

# =====================================================
# Write to Snowflake
# =====================================================

## Adding 4 new columns to match Snowflake table structure X_ETL_NAME, X_RECORD_INSERT_TS, X_RECORD_UPDATE_TS, PROCDATE

df_ubt_temp_datalocationinvoice_2nd = df_ubt_temp_datalocationinvoice_2nd.assign(
    X_ETL_NAME = "UBTI_2.0.2_TR_RTMS_LOCATION_INV",
    X_RECORD_INSERT_TS = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
    X_RECORD_UPDATE_TS = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
    PROCDATE = procdate
)

database = 'SPPL_DEV_DWH'
schema = 'SPPL_PUBLIC'
connection = snowflake_connection()
table_name = "SP_UBT_GETLOCATIONINVOICE"
logger.info(f"Writing data to Snowflake table {database}.{schema}.{table_name}...")
write_to_snowflake(
    dataframe=df_ubt_temp_datalocationinvoice_2nd,
    connection=connection,
    database=database,
    schema=schema,
    table_name=table_name,
    procdate=procdate
)

# =====================================================
# End time tracking and log execution time
# =====================================================
end_time = time.time()
execution_time = end_time - start_time

logger.info(f"ETL process completed successfully in {execution_time:.2f} seconds.")
print(f"ETL process completed in {execution_time:.2f} seconds.")