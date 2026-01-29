#! /home/minhhoang/pyspark/test_older_liv_env/bin/python
# %%
import pandas as pd
import numpy as np
from sp_ubt_getcommonubtdates import *
from sp_ubt_gettransamountdetails import *
from sp_ubt_getamounttransaction import *
from sp_ubt_getsweepsalespersrterminal import *
from declare_Variables import *
from Transformation import *
from write_pandas import *
import os, sys, time, warnings
warnings.filterwarnings("ignore")
start_time = time.time()
import logging

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
# %% [markdown]
# # Declare Variables

# %% [markdown]
# ## Date Variables

# %%
# Just for testing purpose


try:
    if cfg['MODE_CONFIG']['MODE'] == 'LOCAL':
        procdate = pd.to_datetime(cfg['USER CONFIG']['PROCDATE']).strftime("%Y%m%d")

        logger.info(f"Running in LOCAL mode. procdate: {procdate}")
    else:
        logger.info("Running in PRODUCTION mode. Getting dates from command line arguments.")
        procdate = pd.to_datetime(sys.argv[1])
except Exception as e:
    logger.error(f"Error in getting dates: {e}. Switching to command line argument mode.")

# procdate = '2025-11-27'
logger.info(f"Process date: {procdate}")
# When running in production, comment the above line and uncomment the below line
# procdate = sys.argv[1]
vbusinessdate,vinput_date, vactual_date,\
vstartperiod, vendperiod, \
vfromdateigt, vtodateigt, vfromdatetimeigtUTC, vtodatetimeigtUTC, \
vfromdatetimeOB_UTC, vtodatetimeOB_UTC, vfromdatetimeBMCS_UTC, vtodatetimeBMCS_UTC,\
vinvoiceperiodid = declare_variables(procdate)

# %% [markdown]
#

# %% [markdown]
# # Transformation
#

# %% [markdown]
# ## UBT_TEMP_TERMINAL

# %%
logger.info("Starting UBT_TEMP_TERMINAL data extraction...")
df_ubt_temp_terminal = ubt_temp_terminal()
logger.info(f"UBT_TEMP_TERMINAL data extraction completed. Records: {len(df_ubt_temp_terminal)}")

# %% [markdown]
# ## UBT_TEMP_LOCATION

# %%
logger.info("Starting UBT_TEMP_LOCATION data extraction...")
df_ubt_temp_location = ubt_temp_location()
logger.info(f"UBT_TEMP_LOCATION data extraction completed. Records: {len(df_ubt_temp_location)}")

# %% [markdown]
# ## UBT_TEMP_PRODUCT

# %%
logger.info("Starting UBT_TEMP_PRODUCT data extraction...")
df_ubt_temp_product = ubt_temp_product()
logger.info(f"UBT_TEMP_PRODUCT data extraction completed. Records: {len(df_ubt_temp_product)}")

# %% [markdown]
#

# %% [markdown]
# ## UBT_TEMP_CHAIN

# %%
logger.info("Starting UBT_TEMP_CHAIN data extraction...")
df_ubt_temp_chain = ubt_temp_chain()
logger.info(f"UBT_TEMP_CHAIN data extraction completed. Records: {len(df_ubt_temp_chain)}")

# %% [markdown]
# ## ubt_temp_cancelledbeticket

# %%
logger.info("Starting ubt_temp_cancelledbeticket data extraction...")
df_ubt_temp_cancelledbeticket = ubt_temp_cancelledbeticket(vfromdateigt, vtodateigt)
logger.info(f"ubt_temp_cancelledbeticket data extraction completed. Records: {len(df_ubt_temp_cancelledbeticket)}")

# %% [markdown]
# ## UBT_TEMP_GETAMOUNTTRANS -> Nhi
#

# %%
# Get data from function
logger.info("Starting sp_ubt_getamounttransaction processing...")
df_ubt_temp_getamounttrans = sp_ubt_getamounttransaction(vstartperiod, vendperiod)
logger.info(f"sp_ubt_getamounttransaction completed. Records: {len(df_ubt_temp_getamounttrans)}")

# Rename columns if needed
df_ubt_temp_getamounttrans = df_ubt_temp_getamounttrans.rename(columns={
    'TKTSERIALNUMBER': 'TICKETSERIALNUMBER',
})

# %% [markdown]
# ## UBT_TEMP_RESUTLCASHLESSLOCATION

# %%
logger.info("Starting UBT_TEMP_RESUTLCASHLESSLOCATION data extraction...")
df_ubt_temp_resultcashlesslocation = ubt_temp_resultcashlesslocation(vfromdatetimeigtUTC, vtodatetimeigtUTC)
logger.info(f"UBT_TEMP_RESUTLCASHLESSLOCATION data extraction completed. Records: {len(df_ubt_temp_resultcashlesslocation)}")

# %% [markdown]
# ## UBT_TEMP_itotolocaiion
#

# %%
logger.info("Starting UBT_TEMP_itotolocation data extraction...")
df_ubt_temp_itotolocation = ubt_temp_iTotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC)
logger.info(f"UBT_TEMP_itotolocation data extraction completed. Records: {len(df_ubt_temp_itotolocation)}")

# %% [markdown]
# ## UBT_TEMP_GroupToToLocation

# %%
logger.info("Starting UBT_TEMP_GroupToToLocation data extraction...")
df_ubt_temp_grouptotolocation = ubt_temp_grouptotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC)
logger.info(f"UBT_TEMP_GroupToToLocation data extraction completed. Records: {len(df_ubt_temp_grouptotolocation)}")

# %% [markdown]
# ## UBT_TEMP_SalesgroupTotoLocation

# %%
logger.info("Starting UBT_TEMP_SalesgroupTotoLocation data extraction...")
df_ubt_temp_salesgrouptotolocation = ubt_temp_salesgrouptotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC
                                    ,df_ubt_temp_itotolocation
                                    ,df_ubt_temp_getamounttrans
                                    ,df_ubt_temp_cancelledbeticket
                                    ,df_ubt_temp_terminal
                                    ,df_ubt_temp_location)
logger.info(f"UBT_TEMP_SalesgroupTotoLocation data extraction completed. Records: {len(df_ubt_temp_salesgrouptotolocation)}")

# %% [markdown]
# ## UBT_TEMP_SALESTOTOLOCATION

# %%
logger.info("Starting UBT_TEMP_SALESTOTOLOCATION data extraction...")
df_ubt_temp_salestotolocation = ubt_temp_salestotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC,
                               df_ubt_temp_itotolocation,
                               df_ubt_temp_getamounttrans
                               ,df_ubt_temp_cancelledbeticket
                               ,df_ubt_temp_terminal
                               ,df_ubt_temp_location)
logger.info(f"UBT_TEMP_SALESTOTOLOCATION data extraction completed. Records: {len(df_ubt_temp_salestotolocation)}")

# %% [markdown]
# # ubt_temp_transamountdetaildata

# %%
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


# %% [markdown]
# ## ubt_temp_datalocationinvoice

# %%
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


# %% [markdown]
#

# %% [markdown]
# ## ubt_temp_sales_scandsr

# %%
logger.info("Starting sp_ubt_getsweepsalespersrterminal data extraction...")
df_ubt_temp_sales_scandsr = sp_ubt_getsweepsalespersrterminal(vstartperiod, vendperiod)
logger.info(f"sp_ubt_getsweepsalespersrterminal data extraction completed. Records: {len(df_ubt_temp_sales_scandsr)}")

# %%


# %% [markdown]
# ## ubt_temp_sales_scandsr_loc

# %%
logger.info("Starting ubt_temp_sales_scandsr_loc processing...")
df_ubt_temp_sales_scandsr_loc = ubt_temp_sales_scandsr_loc(
        df_ubt_temp_sales_scandsr,
        df_ubt_temp_terminal,
        df_ubt_temp_location,
        df_ubt_temp_chain,
        vinvoiceperiodid
)
logger.info(f"ubt_temp_sales_scandsr_loc processing completed. Records: {len(df_ubt_temp_sales_scandsr_loc)}")

# %% [markdown]
# ### Update ubt_temp_datalocationinvoice using df_ubt_temp_sales_scandsr_loc

# %%
# DELETE FROM ubt_temp_datalocationinvoice SAL  USING ubt_temp_sales_scandsr_loc RSLoc
# WHERE RSLoc.LocDisplayID = SAL.LocDisplayID AND RSLoc.AgentInvoice_productId = SAL.AgentInvoice_productId
# AND SAL.Sales_type = '116';
os.remove("df_ubt_temp_datalocationinvoice_before_delete.csv") if os.path.exists("df_ubt_temp_datalocationinvoice_before_delete.csv") else None
df_ubt_temp_datalocationinvoice.to_csv("df_ubt_temp_datalocationinvoice_before_delete.csv", index=False)
mask_delete =(
    (df_ubt_temp_datalocationinvoice['SALES_TYPE'] == '116') &
    (df_ubt_temp_datalocationinvoice.set_index(['LOCDISPLAYID', 'AGENTINVOICE_PRODUCTID']).index.isin(
        df_ubt_temp_sales_scandsr_loc.set_index(['LOCDISPLAYID', 'AGENTINVOICE_PRODUCTID']).index
    ))
)

df_ubt_temp_datalocationinvoice = df_ubt_temp_datalocationinvoice[~mask_delete]
os.remove("df_ubt_temp_datalocationinvoice_after_delete.csv") if os.path.exists("df_ubt_temp_datalocationinvoice_after_delete.csv") else None
df_ubt_temp_datalocationinvoice.to_csv("df_ubt_temp_datalocationinvoice_after_delete.csv", index=False)




df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_ubt_temp_sales_scandsr_loc], ignore_index=True)

# %% [markdown]
# ## ubt_temp_salesfactorconfig

# %%
logger.info("Starting ubt_temp_salesfactorconfig data extraction...")
df_ubt_temp_salesfactorconfig = ubt_temp_salesfactorconfig()
logger.info(f"ubt_temp_salesfactorconfig data extraction completed. Records: {len(df_ubt_temp_salesfactorconfig)}")

# %% [markdown]
# ## Process df_ubt_temp_datalocationinvoice 2nd

# %%
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


# %%
end_time = time.time()
execution_time = end_time - start_time

logger.info(f"ETL process completed successfully in {execution_time:.2f} seconds.")
print(f"ETL process completed in {execution_time:.2f} seconds.")


