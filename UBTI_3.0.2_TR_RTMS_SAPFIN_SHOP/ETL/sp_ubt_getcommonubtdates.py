import pandas as pd
import warnings, time
from logger_util import get_logger
from datetime import datetime, timedelta
from Snowflake_connection import snowflake_connection

# Suppress warnings
warnings.filterwarnings('ignore')
pd.options.mode.chained_assignment = None
start_time = time.time()

def sp_ubt_getcommonubtdates(vfromdate, vtodate, logger):
    logger.info(f"[SP_GETCOMMONUBTDATES] "
                f"Initializing SP_UBT_GETCOMMONUBTDATES with input range "
                f"from={vfromdate}, to={vtodate}")
    try:
        # fromdate = datetime.strptime(vfromdate, "%Y%m%d")
        # todate = datetime.strptime(vtodate, "%Y%m%d")

        fromdate = pd.Timestamp(vfromdate)
        todate = pd.Timestamp(vtodate)
        connection = snowflake_connection()
        schema = "SPPL_DEV_DWH.SPPL_PUBLIC"

        def get_perioddatetime(fld2value, date_col):
            logger.debug(f"[SP_GETCOMMONUBTDATES] Querying perioddatetime for {fld2value}, date={date_col}")
            query = f"""
                    SELECT *
                    FROM {schema}.ZTUBT_ADHOCTIMEHISTORY za, {schema}.ZTUBT_LOOKUPVALUECONFIG zl
                    WHERE za.HOST = CAST(zl.FLD1VALUE AS INT)
                    AND zl.CONFIGNAME = 'RTMS_Host'
                """
            df_filtered = pd.read_sql(query, connection)
            df_filtered["ACTUALDATE"] = pd.to_datetime(df_filtered["ACTUALDATE"])
            df_filtered = df_filtered[(df_filtered["FLD2VALUE"] == fld2value)
                                    & (df_filtered["ACTUALDATE"] == pd.Timestamp(date_col))]
            if df_filtered.empty:
                logger.warning(f"[SP_GETCOMMONUBTDATES] No perioddatetime found for {fld2value} on {date_col}")
                return None
            return df_filtered["PERIODDATETIME"].iloc[0]

        # Get fromdate & todate for each BMCS, IGT, and OB.
        logger.info(f"[SP_GETCOMMONUBTDATES] Retrieving BMCS/IGT/OB host ...")
        fromdatebmcs = get_perioddatetime("BMCS", fromdate - timedelta(days=1))
        todatebmcs = get_perioddatetime("BMCS", todate)
        fromdateigt = get_perioddatetime("IGT", fromdate - timedelta(days=1))
        todateigt = get_perioddatetime("IGT", todate)
        fromdateob = get_perioddatetime("OB", fromdate - timedelta(days=1))
        todateob = get_perioddatetime("OB", todate)

    # Get fromdate & todate for NonHost.
        logger.info(f"[SP_GETCOMMONUBTDATES] Retrieving NonHost window...")
        query_adhoctimehistory = f"""
                        SELECT * FROM {schema}.ZTUBT_ADHOCTIMEHISTORY
                        """
        df_adhoctimehistory = pd.read_sql(query_adhoctimehistory, connection)
        df_adhoctimehistory["ACTUALDATE"] = pd.to_datetime(df_adhoctimehistory["ACTUALDATE"])
        fromdatenonhost = df_adhoctimehistory[(df_adhoctimehistory["ACTUALDATE"] == fromdate - timedelta(days=1))]\
                                ["PERIODDATETIME"].max()
        todatenonhost = df_adhoctimehistory[(df_adhoctimehistory["ACTUALDATE"] == todate)]\
                                ["PERIODDATETIME"].max()
        logger.debug(f"[SP_GETCOMMONUBTDATES] NonHost: from {fromdatenonhost} to {todatenonhost}")

        # Calculate fundfromdate & fundtodate based on fromdate's day of week.
        if fromdate.weekday() == 0:
            fundfromdate = fromdate + timedelta(days=4)
            fundtodate = fromdate - timedelta(days=6)
        elif fromdate.weekday() == 4:
            fundfromdate = fromdate + timedelta(days=3)
            fundtodate = fromdate - timedelta(days=6)
        else:
            fundfromdate = None
            fundtodate = None
        logger.debug(f"[SP_GETCOMMONUBTDATES] Fund: from {fundfromdate} to {fundtodate}")

        # Read data from ZTUBT_ADHOCTIMECONFIG
        query_adhoctimeconfig = f"""
                    SELECT * FROM {schema}.ZTUBT_ADHOCTIMECONFIG
                    """
        df_adhoctimeconfig = pd.read_sql(query_adhoctimeconfig, connection)

        # Calculate ADHOC_FROMDATE and ADHOC_TODATE
        def calc_adhoc_dates(row, date_col):

            if pd.isna(row["ADHOCTIME"]) and pd.isna(row["DEFAULTTIME"]):
                return pd.Series({"adhoc_fromdate": None, "adhoc_todate": None})


            adhoc_time = row["ADHOCTIME"] if pd.notna(row["ADHOCTIME"]) else row["DEFAULTTIME"]
            raw_hour = int(adhoc_time[:2])
            time_suffix = adhoc_time[2:]
            if raw_hour >= 24:
                adj_hour = raw_hour - 24
                adhoc_fromdate = datetime.combine(date_col + timedelta(days=1), datetime.min.time()) + timedelta(hours=adj_hour)
            else:
                adhoc_fromdate = datetime.combine(date_col, datetime.min.time()) + timedelta(hours=raw_hour)
            adhoc_todate = adhoc_fromdate + timedelta(days=1)
            return pd.Series({"ADHOC_FROMDATE": adhoc_fromdate, "ADHOC_TODATE": adhoc_todate})

        temp = df_adhoctimeconfig.apply(calc_adhoc_dates, axis=1, date_col=todate.date())
        df_adhoctimeconfig = pd.concat([df_adhoctimeconfig, temp], axis=1)

        # Get adhoc_fromdate & adhoc_todate for each host type.
        igt_adhoc_fromdate = df_adhoctimeconfig.loc[df_adhoctimeconfig["HOST"] == 1, "ADHOC_FROMDATE"].max()
        igt_adhoc_todate = df_adhoctimeconfig.loc[df_adhoctimeconfig["HOST"] == 1, "ADHOC_TODATE"].max()
        bmcs_adhoc_fromdate = df_adhoctimeconfig.loc[df_adhoctimeconfig["HOST"] == 2, "ADHOC_FROMDATE"].max()
        bmcs_adhoc_todate = df_adhoctimeconfig.loc[df_adhoctimeconfig["HOST"] == 2, "ADHOC_TODATE"].max()
        ob_adhoc_fromdate = df_adhoctimeconfig.loc[df_adhoctimeconfig["HOST"] == 3, "ADHOC_FROMDATE"].max()
        ob_adhoc_todate = df_adhoctimeconfig.loc[df_adhoctimeconfig["HOST"] == 3, "ADHOC_TODATE"].max()
        nh_adhoc_fromdate = df_adhoctimeconfig["ADHOC_FROMDATE"].min()
        nh_adhoc_todate = df_adhoctimeconfig["ADHOC_TODATE"].max()

        def coalesce(*args):
            for a in args:
                if pd.notna(a):
                    return a
            return None
        df_result = pd.DataFrame([{
            "fromdatetimebmcs": coalesce(fromdatebmcs, bmcs_adhoc_fromdate),
            "todatetimebmcs": coalesce(todatebmcs, bmcs_adhoc_todate),
            "utcfromdatetimebmcs": coalesce(fromdatebmcs, bmcs_adhoc_fromdate) - timedelta(hours=8),
            "utctodatetimebmcs": coalesce(todatebmcs, bmcs_adhoc_todate) - timedelta(hours=8),
            "fromdatetimeigt": coalesce(fromdateigt, igt_adhoc_fromdate),
            "todatetimeigt": coalesce(todateigt, igt_adhoc_todate),
            "utcfromdatetimeigt": coalesce(fromdateigt, igt_adhoc_fromdate) - timedelta(hours=8),
            "utctodatetimeigt": coalesce(todateigt, igt_adhoc_todate) - timedelta(hours=8),
            "fromdatetimeob": coalesce(fromdateob, ob_adhoc_fromdate),
            "todatetimeob": coalesce(todateob, ob_adhoc_todate),
            "utcfromdatetimeob": coalesce(fromdateob, ob_adhoc_fromdate) - timedelta(hours=8),
            "utctodatetimeob": coalesce(todateob, ob_adhoc_todate) - timedelta(hours=8),
            "fromdatetimenonhost": coalesce(fromdatenonhost, nh_adhoc_fromdate),
            "todatetimenonhost": coalesce(todatenonhost, nh_adhoc_todate),
            "utcfromdatetimenonhost": coalesce(fromdatenonhost, nh_adhoc_fromdate) - timedelta(hours=8),
            "utctodatetimenonhost": coalesce(todatenonhost, nh_adhoc_todate) - timedelta(hours=8),
            "fromdatefund": fundfromdate,
            "todatefund": fundtodate,
            "inputfromdate": fromdate,
            "inputtodate": todate
        }])
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"[SP_GETCOMMONUBTDATES] completed successfully in {duration:.2f} seconds and fetched: rows={len(df_result)}")
    except Exception:
        logger.error("[SP_GETCOMMONUBTDATES] SP_UBT_GETCOMMONUBTDATES encountered an error", exc_info=True)
        raise
    return df_result
