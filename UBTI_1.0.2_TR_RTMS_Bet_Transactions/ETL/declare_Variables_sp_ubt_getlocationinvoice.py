from Store_Procedure_Common.sp_ubt_getcommonubtdates import *
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
from Utilities.Snowflake_connection import *
connection = snowflake_connection()
schema = "SPPL_DEV_DWH.SPPL_PUBLIC"
import logging
logger = logging.getLogger(__name__)

def declare_variables(vbusinessdate):
    # Initial logging
    print("Starting to declare variables... (From Line 6)")
    logger.info("Starting to declare variables... (From Line 6)")
    # Convert vbusinessdate to date object
    vbusinessdate = pd.Timestamp(vbusinessdate).date()
    vinput_date = vbusinessdate + pd.Timedelta(days=1)
    vactual_date = vbusinessdate + pd.Timedelta(days=1)
    print("Converted vbusinessdate to date object and calculated vinput_date & vactual_date. (Line 26-29)")
    logger.info("Converted vbusinessdate to date object and calculated vinput_date & vactual_date. (Line 26-29)")
    # Calculate financial week boundaries
    def get_financial_week_bound(vinput_date):
        week_start = vinput_date - pd.Timedelta(days=vinput_date.weekday())
        day_of_week = vinput_date.strftime('%a').lower()
        if day_of_week in ['mon', 'tue', 'wed', 'thu']:
            vstartperiod = week_start - pd.Timedelta(days=3)
            vendperiod = week_start - pd.Timedelta(days=1)
        else:
            vstartperiod = vinput_date - pd.Timedelta(days= vinput_date.weekday())
            vendperiod = week_start + pd.Timedelta(days=3)
        return vstartperiod, vendperiod
    vstartperiod, vendperiod = get_financial_week_bound(vinput_date)
    print("Completed calculating financial week boundaries. (Line 32-40)")
    logger.info("Completed calculating financial week boundaries. (Line 32-40)")

    # Call stored procedure to get common UBT dates
    df_dates = sp_ubt_getcommonubtdates(vstartperiod, vendperiod)
    vfromdateigt = pd.to_datetime(df_dates['FROMDATEIGT'].values[0])
    vtodateigt = pd.to_datetime(df_dates['TODATEIGT'].values[0])
    vfromdatetimeigtUTC = pd.to_datetime(df_dates['UTCFROMDATEIGT'].values[0])
    vtodatetimeigtUTC = pd.to_datetime(df_dates['UTCTODATEIGT'].values[0])
    vfromdatetimeOB_UTC = pd.to_datetime(df_dates['UTCFROMDATEOB'].values[0])
    vtodatetimeOB_UTC = pd.to_datetime(df_dates['UTCTODATEOB'].values[0])
    vfromdatetimeBMCS_UTC = pd.to_datetime(df_dates['UTCFROMDATEBMCS'].values[0])
    vtodatetimeBMCS_UTC = pd.to_datetime(df_dates['UTCTODATEBMCS'].values[0])
    print("Completed calling sp_ubt_getcommonubtdates stored procedure & assigning variables. (Line 42-54)")
    logger.info("Completed calling sp_ubt_getcommonubtdates stored procedure & assigning variables. (Line 42-54)")
    # Starting declare vinvoiceperiodid (Line 56)
    query = f"""
    SELECT cast(INVOICEPERIODID as VARCHAR) as INVOICEPERIODID
    FROM {schema}.ZTUBT_INVOICEPERIOD
    WHERE STARTDATE = '{vstartperiod}' AND ENDDATE = '{vendperiod}'
    """
    df_invoiceperiod = pd.read_sql(query, connection)
    if df_invoiceperiod.empty:
        vinvoiceperiodid = None
    else:
        vinvoiceperiodid = df_invoiceperiod['INVOICEPERIODID'].values[0]
    print(f"Declared vinvoiceperiodid variable VALUE {vinvoiceperiodid}. (Line 56-48)")
    logger.info(f"Declared vinvoiceperiodid variable VALUE {vinvoiceperiodid}. (Line 56-48)")
    return vbusinessdate, vinput_date, vactual_date, vstartperiod, vendperiod, \
        vfromdateigt, vtodateigt, vfromdatetimeigtUTC, vtodatetimeigtUTC, \
        vfromdatetimeOB_UTC, vtodatetimeOB_UTC, vfromdatetimeBMCS_UTC, vtodatetimeBMCS_UTC, vinvoiceperiodid