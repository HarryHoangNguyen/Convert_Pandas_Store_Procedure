#! /home/harryhoangnguyen/HoangNguyen/Adnovum/Convert_Pandas_Store_Procedure/.venv/bin/python3
import pandas as pd
import os, sys, time, warnings
import numpy as np
from Transformation import *
import logging
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Suppress all warnings
warnings.filterwarnings('ignore')

# Suppress specific pandas warnings
pd.options.mode.chained_assignment = None
pd.set_option('future.no_silent_downcasting', True)

start_time = time.time()
from config_reader import read_local_config
cfg = read_local_config()
from Snowflake_connection import *
from write_pandas import *


# Determine procdate based on mode

try:
    if cfg['MODE_CONFIG']['MODE'].upper() == 'LOCAL':
        procdate = cfg['USER CONFIG']['PROCDATE']
        print(f"Running in LOCAL mode with procdate: {procdate}")
    else:
        procdate = sys.argv[1]
except Exception as e:
    logger.error("Error determining procdate", exc_info=e)
    raise

# Declare variables
HQLocation, PreviousDateTime, PeriodDateTime, ActualDate = declare_variables(procdate)

# process ubt_temp_table
try:
    ubt_temp_df = ubt_temp_table(HQLocation, PreviousDateTime, PeriodDateTime, ActualDate)
    print("ubt_temp_table processed successfully.")
except Exception as e:
    logger.error("Error processing ubt_temp_table", exc_info=e)
    raise
