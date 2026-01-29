#! /home/harryhoangnguyen/HoangNguyen/Adnovum/Convert_Pandas_Store_Procedure/.venv/bin/python3


# =====================================================
# Import Libraries
# =====================================================
import pandas as pd
import os, sys, time, warnings
import numpy as np
from Transformation import *
import logging
from log_files import *
from config_reader import read_local_config
from Snowflake_connection import *
from write_pandas import *

# ===================================================== 
# Set up logging
# =====================================================
init_logfiles()

logger = logging.getLogger(__name__)

# =====================================================
# Suppress Warnings
# =====================================================
# Suppress all warnings
warnings.filterwarnings('ignore')

# Suppress specific pandas warnings
pd.options.mode.chained_assignment = None
pd.set_option('future.no_silent_downcasting', True)

# =====================================================
# Start time tracking
# =====================================================
start_time = time.time()

# ====================================================
# Read configuration
# ====================================================
cfg = read_local_config()



# =====================================================
# Determine procdate based on mode
"""
    Get the processing date based on the mode specified in the configuration.
    In LOCAL mode, use the date from the configuration file.
    In PROD mode, use the date passed as a command-line argument.
"""
# =====================================================

try:
    if cfg['MODE_CONFIG']['MODE'].upper() == 'LOCAL':
        procdate = cfg['USER CONFIG']['PROCDATE']
        print(f"Running in LOCAL mode with procdate: {procdate}")
        logger.info(f"Running in LOCAL mode with procdate: {procdate}")
    else:
        procdate = sys.argv[1]
        logger.info(f"Running in PRODUCTION mode with procdate: {procdate}")
except Exception as e:
    logger.error("Error determining procdate", exc_info=e)
    raise

# =====================================================
# Declare variables
# =====================================================
HQLocation, PreviousDateTime, PeriodDateTime, ActualDate = declare_variables(procdate)

# =====================================================
# Process ubt_temp_table
# =====================================================
try:
    ubt_temp_df = ubt_temp_table(HQLocation, PreviousDateTime, PeriodDateTime, ActualDate)
    print("ubt_temp_table processed successfully.")
    logger.info("ubt_temp_table processed successfully.")
except Exception as e:
    logger.error("Error processing ubt_temp_table", exc_info=e)
    raise
