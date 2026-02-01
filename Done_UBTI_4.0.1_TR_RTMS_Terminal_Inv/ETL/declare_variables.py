from sp_ubt_getcommonubtdates import *
import logging
logger = logging.getLogger(__name__)
import pandas as pd
from Snowflake_connection import *
connection = snowflake_connection()
schema = "SPPL_DEV_DWH.SPPL_PUBLIC"

def declare_variables(procdate):
    vbusinessdate = pd.to_datetime(procdate)
    vinputdate = None
    vactualdate = None

    vstartperiod = None
    vendperiod = None
    df_dates = sp_ubt_getcommonubtdates(vbusinessdate, vbusinessdate)
    vfromdateIGT = pd.to_datetime(df_dates['FROMDATEIGT'].values[0])
    vtodateIGT = pd.to_datetime(df_dates['TODATEIGT'].values[0])
    vfromdatetimeIGT_UTC = pd.to_datetime(df_dates['UTCFROMDATEIGT'].values[0])
    vtodatetimeIGT_UTC = pd.to_datetime(df_dates['UTCTODATEIGT'].values[0])
    vfromdatetimeOB_UTC = pd.to_datetime(df_dates['UTCFROMDATEOB'].values[0])
    vtodatetimeOB_UTC = pd.to_datetime(df_dates['UTCTODATEOB'].values[0])
    vfromdatetimeNonHost_UTC = pd.to_datetime(df_dates['UTCFROMDATENONHOST'].values[0])
    vtodatetimeNonHost_UTC = pd.to_datetime(df_dates['UTCTODATENONHOST'].values[0])
    vfromdatetimeBMCS_UTC = pd.to_datetime(df_dates['UTCFROMDATEBMCS'].values[0])
    vtodatetimeBMCS_UTC = pd.to_datetime(df_dates['UTCTODATEBMCS'].values[0])
    # Get GST Rate
    # query_gst = f"""
    # SELECT COALESCE(gstrate , 0)/100 as gstrate
    # FROM ztubt_gstconfig zg
    # WHERE '{vbusinessdate}' BETWEEN zg.effectivefrom AND COALESCE(zg.enddate , 'infinity'::date)
    # ORDER BY zg.effectivefrom
    # LIMIT 1
    # """

    query_gst = f"""
    select gstrate from ztubt_gstconfig
    WHERE '{vbusinessdate}' BETWEEN effectivefrom AND enddate
    ORDER BY effectivefrom DESC
    LIMIT 1
    """

    df_gst = pd.read_sql(query_gst, connection)
    if df_gst.empty:
        vGSTRate = 0.0
        print("GST Rate not found for the given business date. Setting vGSTRate to 0.0")
    else:
        vGSTRate = float(df_gst['gstrate'].values[0] / 100)
        print(f"GST Rate found: {vGSTRate}")

    # Set vactualdate to None
    vactualdate = None




    return vbusinessdate,\
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
            vactualdate