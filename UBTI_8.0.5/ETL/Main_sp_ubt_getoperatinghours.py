import pandas as pd
import numpy as np
from write_pandas import *
from Snowflake_connection import *
from declare_variables import *
from log_files import *
from Transformation import *
import logging, sys, os
logger = logging.getLogger(__name__)

init_logfiles()


# =====================================================
# Determine mode and processing date
# =====================================================
try:
    if cfg['MODE_CONFIG']['MODE'] == 'LOCAL':
        transactiondate = pd.to_datetime(cfg['USER CONFIG']['PROCDATE']).strftime('%Y-%m-%d')
        logger.info("Running in LOCAL mode")
        logger.info(f"Processing date: {transactiondate}")
    else:
        logger.info("Running in PROD mode")
        transactiondate = pd.to_datetime(sys.argv[1]).strftime('%Y-%m-%d')
        logger.info(f"Processing date from argument: {transactiondate}")
except Exception as e:
    logger.error(f"Error determining mode or processing date: {e}")
    sys.exit(1)

# =====================================================
# Declare variables
# =====================================================
try:
    FromDate, ToDate, FromDateUTC, ToDateUTC, ConvertToUTC, ConvertToSGT = declare_variables(transactiondate)
    print(f"FromDate: {FromDate}, ToDate: {ToDate}, FromDateUTC: {FromDateUTC}, ToDateUTC: {ToDateUTC}, ConvertToUTC: {ConvertToUTC}, ConvertToSGT: {ConvertToSGT}")
    logger.info(f"FromDate: {FromDate}, ToDate: {ToDate}, FromDateUTC: {FromDateUTC}, ToDateUTC: {ToDateUTC}, ConvertToUTC: {ConvertToUTC}, ConvertToSGT: {ConvertToSGT}")
except Exception as e:
    logger.error(f"Error declaring variables: {e}")
    sys.exit(1)

# =====================================================
# Execute Transformation
# =====================================================

# df_ubt_temp_t
try:
    df_ubt_temp_t = ubt_temp_t(FromDate, ToDate, ConvertToUTC)
    print("ubt_temp_t executed successfully.")
    print("ubt_temp_t preview: ", df_ubt_temp_t.shape[0])
    logger.info("ubt_temp_t executed successfully.")
except Exception as e:
    logger.error(f"Error executing ubt_temp_t: {e}")
    sys.exit(1)

# df_ubt_temp_count
try:
    df_ubt_temp_count = ubt_temp_count(df_ubt_temp_t, FromDate, ToDate, FromDateUTC, ToDateUTC)
    print("ubt_temp_count executed successfully.")
    print("ubt_temp_count preview: ", df_ubt_temp_count.shape[0])
    logger.info("ubt_temp_count executed successfully.")
except Exception as e:
    logger.error(f"Error executing ubt_temp_count: {e}")
    sys.exit(1)
# =====================================================

# df_final_select
try:
    df_final_select = final_select(df_ubt_temp_count, FromDate, ToDate, FromDateUTC, ToDateUTC)
    print("final_select executed successfully.")
    print("final_select preview: ", df_final_select.shape[0])
    logger.info("final_select executed successfully.")
except Exception as e:
    logger.error(f"Error executing final_select: {e}")
    sys.exit(1)

