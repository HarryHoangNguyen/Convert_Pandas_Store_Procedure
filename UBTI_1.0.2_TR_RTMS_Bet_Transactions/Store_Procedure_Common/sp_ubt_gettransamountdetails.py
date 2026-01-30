import pandas as pd
import numpy as np
import warnings
from datetime import datetime, timedelta
from sp_ubt_getcommonubtdates import sp_ubt_getcommonubtdates
from Snowflake_connection import snowflake_connection
import os, time, logging
logger = logging.getLogger(__name__)
# Suppress all warnings
warnings.filterwarnings('ignore')
pd.options.mode.chained_assignment = None

start_time = time.time()

def sp_ubt_gettransamountdetails(in_fromdatetime, in_todatetime):
    logger.info(f"[SP_GETTRANSAMOUNTDETAILS] "
                f"Initializing SP_UBT_GETTRANSAMOUNTDETAILS with input range "
                f"from={in_fromdatetime}, to={in_todatetime}")
    fromdate = datetime.strptime(in_fromdatetime, "%Y%m%d")
    todate = datetime.strptime(in_todatetime, "%Y%m%d")
    connection = snowflake_connection()
    schema = "SPPL_DEV_DWH.SPPL_PUBLIC"

    def sp_ubt_getpaynowsuccessstatus(paymenttype: str):
        paymenttype = paymenttype.upper()
        if paymenttype == "PNQR":
            return pd.Series(["01"])
        elif paymenttype == "PNRIC":
            return pd.Series(["01", "03", "04"])
        else:
            return pd.Series([], dtype="string")

    # Declare variables by calling sp_ubt_getcommonubtdates & sp_ubt_getpaynowsuccessstatus
    logger.info(f"[SP_GETTRANSAMOUNTDETAILS] Calling SP_UBT_GETCOMMONUBTDATES ...")
    df_getcommonubtdates = sp_ubt_getcommonubtdates(in_fromdatetime, in_todatetime)
    if df_getcommonubtdates.empty:
        logger.warning(f"[SP_GETTRANSAMOUNTDETAILS] "
                    f"SP_UBT_GETCOMMONUBTDATES returned no records.")
    vfromdatetimebmcs = pd.to_datetime(df_getcommonubtdates["FROMDATEBMCS"].iloc[0])
    vtodatetimebmcs = pd.to_datetime(df_getcommonubtdates["TODATEBMCS"].iloc[0])
    vfromdatetimeigt = pd.to_datetime(df_getcommonubtdates["FROMDATEIGT"].iloc[0])
    vtodatetimeigt = pd.to_datetime(df_getcommonubtdates["TODATEIGT"].iloc[0])
    vfromdatetimeob = pd.to_datetime(df_getcommonubtdates["FROMDATEOB"].iloc[0])
    vtodatetimeob = pd.to_datetime(df_getcommonubtdates["TODATEOB"].iloc[0])
    vfromdatetimenh = pd.to_datetime(df_getcommonubtdates["FROMDATENONHOST"].iloc[0])
    vtodatetimenh = pd.to_datetime(df_getcommonubtdates["TODATENONHOST"].iloc[0])
    vfromdatefund = pd.to_datetime(df_getcommonubtdates["FUNDFROMDATE"].iloc[0])
    vtodatefund = pd.to_datetime(df_getcommonubtdates["FUNDTODATE"].iloc[0])
    vutcfromdatetimebmcs = pd.to_datetime(df_getcommonubtdates["UTCFROMDATEBMCS"].iloc[0])
    vutctodatetimebmcs = pd.to_datetime(df_getcommonubtdates["UTCTODATEBMCS"].iloc[0])
    vutcfromdatetimeigt = pd.to_datetime(df_getcommonubtdates["UTCFROMDATEIGT"].iloc[0])
    vutctodatetimeigt = pd.to_datetime(df_getcommonubtdates["UTCTODATEIGT"].iloc[0])
    vutcfromdatetimeob = pd.to_datetime(df_getcommonubtdates["UTCFROMDATEOB"].iloc[0])
    vutctodatetimeob = pd.to_datetime(df_getcommonubtdates["UTCTODATEOB"].iloc[0])
    vutcfromdatetimenh = pd.to_datetime(df_getcommonubtdates["UTCFROMDATENONHOST"].iloc[0])
    vutctodatetimenh = pd.to_datetime(df_getcommonubtdates["UTCTODATENONHOST"].iloc[0])
    v_totomatchbettypes = "'M AN', 'M 2', 'M 3', 'M 4'"
    status_pnqr = sp_ubt_getpaynowsuccessstatus("PNQR")
    status_pnric = sp_ubt_getpaynowsuccessstatus("PNRIC")

    try:
        # Query ztubt_terminal
        query = f"""
            select * from {schema}.ZTUBT_TERMINAL
        """
        df_ztubt_terminal = pd.read_sql(query, connection)

        # Query ztubt_location
        query = f"""
            select * from {schema}.ZTUBT_LOCATION
        """
        df_ztubt_location = pd.read_sql(query, connection)

        # Query ztubt_funding
        query = f"""
            select * from {schema}.ZTUBT_FUNDING
            where FUNDPERIODSTART = '{vfromdatefund}'
            and FUNDPERIODEND = '{vtodatefund}'
        """
        df_ztubt_funding = pd.read_sql(query, connection)

        # Query ztubt_recovery
        query = f"""
            select * from {schema}.ZTUBT_RECOVERY
            where RECPERIODSTART = '{vfromdatefund}'
            and RECPERIODEND = '{vtodatefund}'
        """
        df_ztubt_recovery = pd.read_sql(query, connection)

        # Query ztubt_adjustinvoice
        query = f"""
            select * from {schema}.ZTUBT_ADJUSTINVOICE
            where CREATEDDATETIME >= '{vfromdatetimenh}'
            and CREATEDDATETIME <= '{vtodatetimenh}'
        """
        df_ztubt_adjustinvoice = pd.read_sql(query, connection)

        # Query ztubt_lookupvalueconfig
        query = f"""
            select * from {schema}.ZTUBT_LOOKUPVALUECONFIG
        """
        df_ztubt_lookupvalueconfig = pd.read_sql(query, connection)

        # Query ztubt_gstconfig
        query = f"""
            select * from {schema}.ZTUBT_GSTCONFIG
        """
        df_ztubt_gstconfig = pd.read_sql(query, connection)

        # Query ztubt_locationgsthistory
        query = f"""
            select * from {schema}.ZTUBT_LOCATIONGSTHISTORY
        """
        df_ztubt_locationgsthistory = pd.read_sql(query, connection)

        ## Extract from ZTUBT_PAYMENTDETAIL
        query = f"""
            SELECT *
            FROM {schema}.ZTUBT_PAYMENTDETAIL
            WHERE CREATEDDATE >= '{vutcfromdatetimenh}'
            AND CREATEDDATE < '{vutctodatetimenh}'
        """
        df_ztubt_paymentdetail = pd.read_sql(query, connection)

        ## Extract from ZTUBT_CART
        query = f"""
            SELECT *
            FROM {schema}.ZTUBT_CART
        """
        df_ztubt_cart = pd.read_sql(query, connection)

        ## Extract from ZTUBT_PAYNOWTRANSACTION
        query = f"""
            SELECT *
            FROM {schema}.ZTUBT_PAYNOWTRANSACTION
        """
        df_ztubt_paynowtransaction = pd.read_sql(query, connection)

        # Create temp DataFrame df_ter_loc
        df_ter_loc = pd.merge(df_ztubt_terminal, df_ztubt_location, on="LOCID", how="inner")


        # Create temp DataFrame df_ter_prod_val
        query_prod = f"""
                SELECT CASE WHEN PRODID = 5 THEN 'SPORTS' ELSE TRIM(PRODNAME) END AS PRODNAME
                FROM {schema}.ZTUBT_PRODUCT
                WHERE PRODID IN (1, 2, 3, 4, 5)
                UNION ALL
                SELECT 'TOTO MATCH' AS PRODNAME
                """
        query_ter_val = f"""
                SELECT zt.TERDISPLAYID, zv.VALIDATIONNAME
                FROM {schema}.ZTUBT_TERMINAL zt
                CROSS JOIN {schema}.ZTUBT_VALIDATIONTYPE zv
                """
        df_prod_filtered = pd.read_sql(query_prod, connection)
        df_ter_val_filtered = pd.read_sql(query_ter_val, connection)
        df_ter_prod_val = pd.merge(df_ter_val_filtered, df_prod_filtered, how="cross")
        df_ter_prod_val["PRODNAME"] = df_ter_prod_val["VALIDATIONNAME"] + "-" + df_ter_prod_val["PRODNAME"]
        df_ter_prod_val = df_ter_prod_val[["TERDISPLAYID", "PRODNAME"]]

        # Create temp DataFrame df_ter_prod
        df_ter_prod = pd.merge(df_ztubt_terminal, df_prod_filtered, how="cross")

    except Exception as e:
        logger.error(f"[SP_GETTRANSAMOUNTDETAILS] "
                     f"Unexpected error occurred: {str(e)}", exc_info=True)
        raise


    logger.info("[SP_GETTRANSAMOUNTDETAILS] Processing df_trans_amt ...")

    # HorseRacing PRODID=1 SAL
    try:
        query = f"""
        SELECT
            PBTH.TERDISPLAYID
            , ZGP.ACTUALDATE
            , PBTLIN.BETTYPEID
            , CASE WHEN PBTLIN.BETTYPEID = 'W-P' THEN COALESCE(PBTLIN.SALESCOMMAMOUNT, 0) / 2
                ELSE COALESCE(PBTLIN.SALESCOMMAMOUNT, 0)
            END AS SALESCOMMAMOUNT
            , PBTH.TICKETSERIALNUMBER
        FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
        INNER JOIN {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
            ON (PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID)
        INNER JOIN {schema}.ZTUBT_HORSE_PLACEDBETTRANSACTIONLINEITEM PBTLIN
            ON (PBTH.TRANHEADERID = PBTLIN.TRANHEADERID)
        LEFT OUTER JOIN (
            SELECT DISTINCT *
            FROM {schema}.ZTUBT_GETBUSINESSDATE_PERHOST
            WHERE PREVIOUSPERIODDATETIME IS NOT NULL
            AND HOST = 1
        ) AS ZGP
        ON (PBTLIN.CREATEDDATE BETWEEN ZGP.PREVIOUSPERIODDATETIMEUTC AND ZGP.PERIODDATETIMEUTC)
        WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimebmcs}')
        AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimebmcs}')
        AND PBTH.PRODID = 1
        AND PBTHLCS.BETSTATETYPEID = 'PB06'
        AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
        AND PBTH.ISCANCELLED = FALSE
        AND PBTH.ISEXCHANGETICKET = FALSE
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = df_temp.groupby(["TERDISPLAYID", "BETTYPEID", "ACTUALDATE"], as_index=False)\
            .agg(
                SALESCOMMAMOUNT=("SALESCOMMAMOUNT", sum),
                TICKETCOUNT=("TICKETSERIALNUMBER", pd.Series.nunique)
            ).reset_index(drop=True)
        df_temp = pd.merge(df_ter_loc, df_temp, on="TERDISPLAYID", how="left")
        df_temp = df_temp.groupby(["TERDISPLAYID", "ACTUALDATE"], as_index=False)\
            .agg(
                AMOUNT=("SALESCOMMAMOUNT", sum),
                TICKETCOUNT=("TICKETCOUNT", pd.Series.nunique)
            ).reset_index(drop=True)
        df_temp = (
            df_temp
            .assign(PRODNAME="HORSE RACING", FLAG="SAL")
            [ ["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"] ]
        )
        df_trans_amt = df_temp

    # 4D ProdID=2 SAL
        query = f"""
            SELECT
                PBTH.TERDISPLAYID
                , COALESCE(PBTLIN.SALESCOMMAMOUNTBIG, 0) + COALESCE(PBTLIN.SALESCOMMAMOUNTSMALL, 0) AS SALESCOMAMT
                , PBTH.TICKETSERIALNUMBER
                , ZGP.ACTUALDATE
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID
            INNER JOIN {schema}.ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEMNUMBER PBTLIN
                ON PBTH.TRANHEADERID = PBTLIN.TRANHEADERID
            INNER JOIN {schema}.ZTUBT_DRAWDATES DD
                ON DD.TRANHEADERID = PBTLIN.TRANHEADERID
            LEFT OUTER JOIN (
                SELECT DISTINCT *
                FROM {schema}.ZTUBT_GETBUSINESSDATE_PERHOST
                WHERE PREVIOUSPERIODDATETIME IS NOT NULL
                AND HOST = 2
            ) ZGP
                ON PBTLIN.CREATEDDATE BETWEEN ZGP.PREVIOUSPERIODDATETIMEUTC AND ZGP.PERIODDATETIMEUTC
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
            AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
            AND PBTH.PRODID = 2
            AND PBTHLCS.BETSTATETYPEID = 'PB06'
            AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
            AND PBTH.ISCANCELLED = FALSE
            AND PBTH.ISEXCHANGETICKET = FALSE
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = df_temp.groupby(["TERDISPLAYID", "ACTUALDATE"], as_index=False)\
            .agg(
                AMOUNT=("SALESCOMAMT", sum),
                TICKETCOUNT=("TICKETSERIALNUMBER", pd.Series.nunique)
            ).reset_index(drop=True)
        df_temp = pd.merge(df_ter_loc, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="4D Lottery", FLAG="SAL")
            [["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] '4D ProdID=2 SAL' completed successfully, rows={len(df_temp)}")

    # TOTO ProdID=3 SAL
        query = f"""
            SELECT
                PBTH.TERDISPLAYID
                , ZGP.ACTUALDATE
                , PBTLIN.SALESCOMMAMOUNT
                , PBTH.TICKETSERIALNUMBER
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON (PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID)
            INNER JOIN {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                ON (PBTH.TRANHEADERID = PBTLIN.TRANHEADERID)
            LEFT OUTER JOIN (
                SELECT DISTINCT *
                FROM {schema}.ZTUBT_GETBUSINESSDATE_PERHOST
                WHERE PREVIOUSPERIODDATETIME IS NOT NULL
                AND HOST = 2
            ) ZGP
                ON (PBTLIN.CREATEDDATE BETWEEN ZGP.PREVIOUSPERIODDATETIMEUTC AND ZGP.PERIODDATETIMEUTC)
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
                AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
                AND PBTH.PRODID = 3
                AND PBTLIN.BETTYPEID NOT IN ({v_totomatchbettypes})
                AND PBTHLCS.BETSTATETYPEID = 'PB06'
                AND COALESCE(PBTLIN.GROUPUNITSEQUENCE, 1) = 1
                AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
                AND PBTH.ISCANCELLED = FALSE
                AND PBTH.ISEXCHANGETICKET = FALSE
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = df_temp.copy()
        df_temp = df_temp.groupby(["TERDISPLAYID", "ACTUALDATE"], as_index=False)\
            .agg(
                AMOUNT=("SALESCOMMAMOUNT", sum),
                TICKETCOUNT=("TICKETSERIALNUMBER", pd.Series.nunique)
            ).reset_index(drop=True)
        df_temp = pd.merge(df_ter_loc, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="TOTO", FLAG="SAL")
            [["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'TOTO ProdID=3 SAL' completed successfully, rows={len(df_temp)}")

# TOTO MATCH ProdID=3 SAL
        query = f"""
            SELECT
                PBTH.TERDISPLAYID
                , ZGP.ACTUALDATE
                , (CASE
                    WHEN PBTLIN.BETTYPEID NOT IN ({v_totomatchbettypes})
                    THEN PBTLIN.SALESCOMMAMOUNT
                    ELSE 0
                END) SALESCOMAMT
                , PBTLIN.SALESCOMMAMOUNT
                , PBTH.TICKETSERIALNUMBER
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON (PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID)
            INNER JOIN {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                ON (PBTH.TRANHEADERID = PBTLIN.TRANHEADERID)
            LEFT OUTER JOIN (
                SELECT DISTINCT *
                FROM {schema}.ZTUBT_GETBUSINESSDATE_PERHOST
                WHERE PREVIOUSPERIODDATETIME IS NOT NULL
                AND HOST = 2
            ) ZGP
                ON (PBTLIN.CREATEDDATE BETWEEN ZGP.PREVIOUSPERIODDATETIMEUTC AND ZGP.PERIODDATETIMEUTC)
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
                AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
                AND PBTH.PRODID = 3
                AND PBTHLCS.BETSTATETYPEID = 'PB06'
                AND COALESCE(PBTLIN.GROUPUNITSEQUENCE, 1) = 1
                AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
                AND PBTH.ISCANCELLED = FALSE
                AND PBTH.ISEXCHANGETICKET = FALSE
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = df_temp.groupby(["TERDISPLAYID","ACTUALDATE"], as_index=False)\
            .agg(
                SALESCOMAMT=("SALESCOMAMT","sum"),
                TICKETCOUNT=("TICKETSERIALNUMBER", pd.Series.nunique),
                SALESCOMTOTAL=("SALESCOMMAMOUNT","sum")
            ).reset_index(drop=True)
        df_temp = pd.merge(df_ter_loc, df_temp, on="TERDISPLAYID", how="left")
        df_temp["AMOUNT"] = (
            (df_temp["SALESCOMTOTAL"].fillna(0) / 100).apply(lambda x: int(x * 100) // 100) -
            (df_temp["SALESCOMAMT"].fillna(0) / 100).apply(lambda x: int(x * 100) // 100)
        ) * 100
        df_temp = (
            df_temp
            .assign(PRODNAME="TOTO MATCH", FLAG="SAL")
            [["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'TOTO MATCH ProdID=3 SAL' completed successfully, rows={len(df_temp)}")


    # SWEEP ProdID=4 SAL
        query = f"""
            SELECT
                PBTH.TERDISPLAYID
                , ZGP.ACTUALDATE
                , COALESCE(PBTLIN.SALESCOMMAMOUNT, 0) AS SALESCOMMAMOUNT
                , PBTH.TICKETSERIALNUMBER
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON (PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID)
            INNER JOIN {schema}.ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER PBTLIN
                ON (PBTH.TRANHEADERID = PBTLIN.TRANHEADERID AND PBTLIN.ISSOLDOUT = FALSE)
            LEFT OUTER JOIN (
                SELECT DISTINCT *
                FROM {schema}.ZTUBT_GETBUSINESSDATE_PERHOST
                WHERE PREVIOUSPERIODDATETIME IS NOT NULL
                AND HOST = 2
            ) ZGP
                ON (PBTLIN.CREATEDDATE BETWEEN ZGP.PREVIOUSPERIODDATETIMEUTC AND ZGP.PERIODDATETIMEUTC)
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
                AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
                AND PBTH.PRODID = 4
                AND PBTHLCS.BETSTATETYPEID = 'PB06'
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = df_temp.groupby(["TERDISPLAYID","ACTUALDATE"], as_index=False)\
            .agg(
                AMOUNT=("SALESCOMMAMOUNT", sum),
                TICKETCOUNT=("TICKETSERIALNUMBER", pd.Series.nunique)
            ).reset_index(drop=True)
        df_temp = pd.merge(df_ter_loc, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="SINGAPORE SWEEP", FLAG="SAL")
            [["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'SWEEP ProdID=4 SAL' completed successfully, rows={len(df_temp)}")

    # SWEEP ProdID=4 SAL - Hira added
        query = f"""
            SELECT
                PBTH.TERDISPLAYID
                , GB.ACTUALDATE
                , COALESCE(PBTLN.SALESCOMMAMOUNT,0) AS SALESCOMMAMOUNT
                , PBTH.TICKETSERIALNUMBER
            FROM (
                SELECT
                    CBT.TRANHEADERID
                    , CBT.TICKETSERIALNUMBER
                    , CBT.PRODID
                    , CBT.TERDISPLAYID
                    , CBT.CANCELLEDAMOUT
                FROM {schema}.ZTUBT_CANCELLEDBETTICKET CBT
                INNER JOIN {schema}.ZTUBT_CANCELLEDBETTICKETLIFECYCLESTATE CBLC
                    ON CBLC.TRANHEADERID=CBT.TRANHEADERID AND CBLC.BETSTATETYPEID='CB06'
                WHERE CBT.PRODID=4
                AND CBT.CANCELLEDDATE BETWEEN '{vfromdatetimeigt}' AND '{vtodatetimeigt}'
            ) PBTH
            INNER JOIN {schema}.ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER PBTLN
                ON PBTH.TRANHEADERID=PBTLN.TRANHEADERID
                AND PBTLN.ISSOLDOUT=FALSE
                AND PBTH.TERDISPLAYID=PBTLN.TERDISPLAYID
            LEFT JOIN (
                SELECT DISTINCT *
                FROM {schema}.ZTUBT_GETBUSINESSDATE_PERHOST
                WHERE PREVIOUSPERIODDATETIME IS NOT NULL AND HOST=2
            ) GB
                ON PBTLN.CREATEDDATE BETWEEN GB.PREVIOUSPERIODDATETIMEUTC AND GB.PERIODDATETIMEUTC
            WHERE PBTLN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}') - INTERVAL '45 DAYS'
            AND PBTLN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
            AND PBTH.PRODID=4
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = (
            df_temp.groupby(["TERDISPLAYID","ACTUALDATE"], as_index=False)
            .agg(
                AMOUNT=("SALESCOMMAMOUNT", lambda x: 0 - x.sum())
                , TICKETCOUNT=("TICKETSERIALNUMBER", lambda x: 0 - x.nunique())
            ).reset_index(drop=True)
        )
        df_temp = pd.merge(df_ter_loc, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="SINGAPORE SWEEP", FLAG="SAL")
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'SWEEP ProdID=4 SAL - added' completed successfully, rows={len(df_temp)}")

    # SPORTS ProdID=5,6 SAL

        query = f"""
            SELECT
                PBTLIN.TERDISPLAYID
                , COALESCE(PBTLIN.SALESCOMMAMOUNT,0) AS SALESCOMMAMOUNT
                , PBTLIN.BETAMOUNT
                , PBTLIN.ACCUMULATORID
                , PBTLIN.TRANHEADERID
                , ZGP.ACTUALDATE
                , COUNT(DISTINCT PBTH.TICKETSERIALNUMBER) AS TICKETCOUNT
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON PBTH.TRANHEADERID=PBTHLCS.TRANHEADERID
            INNER JOIN {schema}.ZTUBT_SPORTS_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                ON PBTH.TRANHEADERID=PBTLIN.TRANHEADERID
            LEFT OUTER JOIN (
                SELECT DISTINCT *
                FROM {schema}.ZTUBT_GETBUSINESSDATE_PERHOST
                WHERE PREVIOUSPERIODDATETIME IS NOT NULL
                AND HOST=3
            ) ZGP
                ON PBTLIN.CREATEDDATE BETWEEN ZGP.PREVIOUSPERIODDATETIMEUTC AND ZGP.PERIODDATETIMEUTC
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeob}')
            AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeob}')
            AND PBTH.PRODID IN (5,6)
            AND PBTHLCS.BETSTATETYPEID='PB06'
            AND COALESCE(PBTH.ISBETREJECTEDBYTRADER,FALSE)=FALSE
            AND PBTH.ISCANCELLED=FALSE
            AND PBTH.ISEXCHANGETICKET=FALSE
            GROUP BY
                PBTLIN.TERDISPLAYID
                , PBTLIN.SALESCOMMAMOUNT
                , PBTLIN.BETAMOUNT
                , PBTLIN.ACCUMULATORID
                , PBTLIN.TRANHEADERID
                , ZGP.ACTUALDATE
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = (
            df_temp.groupby(["TERDISPLAYID","ACTUALDATE"], as_index=False)
            .agg(
                AMOUNT=("SALESCOMMAMOUNT", sum)
                , TICKETCOUNT=("TICKETCOUNT", sum)
            ).reset_index(drop=True)
        )
        df_temp = pd.merge(df_ter_loc, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="SPORTS", FLAG="SAL")
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        df_temp
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'SPORTS ProdID=5,6 SAL' completed successfully, rows={len(df_temp)}")

    # HORSE RACING ProdID=1 COL
        query = f"""
            SELECT
                CASE
                    WHEN PBTLIN.BETTYPEID='W-P' THEN COALESCE(PBTLIN.BETPRICEAMOUNT,0)/2
                    ELSE COALESCE(PBTLIN.BETPRICEAMOUNT,0)
                END AS COLAMOUNT
                , PBTLIN.TERDISPLAYID
                , PBTLIN.BETTYPEID
                , PBTLIN.TRANHEADERID
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
            INNER JOIN {schema}.ZTUBT_HORSE_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                ON PBTHLCS.TRANHEADERID=PBTLIN.TRANHEADERID
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimebmcs}')
            AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimebmcs}')
            AND PBTHLCS.BETSTATETYPEID='PB06'
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = (
            df_temp.groupby(["TERDISPLAYID","BETTYPEID"], as_index=False)
            .agg(
                COLAMOUNT=("COLAMOUNT", sum)
                , TICKETCOUNT=("TRANHEADERID", pd.Series.nunique)
            ).reset_index(drop=True)
        )
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("COLAMOUNT", sum)
                , TICKETCOUNT=("TICKETCOUNT", sum)
            ).reset_index(drop=True)
        )
        df_temp = (
            df_temp
            .assign(PRODNAME="HORSE RACING", FLAG="COL", ACTUALDATE=None)
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'HORSE RACING ProdID=1 COL' completed successfully, rows={len(df_temp)}")

    # 4D ProdID=2 COL
        query = f"""
            SELECT
                PBTH.TERDISPLAYID
                , COALESCE(PBTLIN.BIGBETACCEPTEDWAGER,0) + COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER,0) AS COLAMOUNT
                , PBTH.TICKETSERIALNUMBER
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID
            INNER JOIN {schema}.ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEMNUMBER PBTLIN
                ON PBTH.TRANHEADERID = PBTLIN.TRANHEADERID
            INNER JOIN {schema}.ZTUBT_DRAWDATES DD
                ON DD.TRANHEADERID = PBTLIN.TRANHEADERID
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
            AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
            AND PBTH.PRODID = 2
            AND PBTHLCS.BETSTATETYPEID = 'PB06'
            AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
            AND PBTH.ISEXCHANGETICKET = FALSE
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("COLAMOUNT", sum)
                , TICKETCOUNT=("TICKETSERIALNUMBER", pd.Series.nunique)
            ).reset_index(drop=True)
        )
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="4D LOTTERY", FLAG="COL", ACTUALDATE=None)
            [["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] '4D ProdID=2 COL' completed successfully, rows={len(df_temp)}")

    # TOTO ProdID=3 COL
        query = f"""
            SELECT PBTH.TERDISPLAYID
                , ROUND(
                    CASE
                        WHEN PBTLIN.GROUPHOSTID = '00000000-0000-0000-0000-000000000000'
                            THEN COALESCE(PBTLIN.BETPRICEAMOUNT, 0) * COUNT(PBTLIN.TRANHEADERID)
                        ELSE SUM(PBTLIN.BETPRICEAMOUNT)
                    END, 2) AS COLAMOUNT
                , COUNT(DISTINCT PBTH.TICKETSERIALNUMBER) AS TICKETCOUNT
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON (PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID)
            INNER JOIN {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                ON (PBTH.TRANHEADERID = PBTLIN.TRANHEADERID)
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
            AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
            AND PBTH.PRODID = 3
            AND PBTLIN.BETTYPEID NOT IN ({v_totomatchbettypes})
            AND PBTHLCS.BETSTATETYPEID = 'PB06'
            AND COALESCE(PBTLIN.GROUPUNITSEQUENCE, 1) = 1
            AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
            AND PBTH.ISEXCHANGETICKET = FALSE
            GROUP BY PBTH.TERDISPLAYID
                , PBTLIN.GROUPHOSTID
                , PBTLIN.BETPRICEAMOUNT
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("COLAMOUNT", sum),
                TICKETCOUNT=("TICKETCOUNT", sum)
            )
            .reset_index(drop=True)
        )
        df_temp = (
            df_temp
            .assign(PRODNAME="TOTO", FLAG="COL", ACTUALDATE=None)
            [["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'TOTO ProdID=3 COL' completed successfully, rows={len(df_temp)}")

    # TOTO MATCH ProdID=3 COL
        query = f"""
            SELECT PBTH.TERDISPLAYID
                , ROUND(
                    CASE
                        WHEN PBTLIN.GROUPHOSTID = '00000000-0000-0000-0000-000000000000'
                            THEN COALESCE(PBTLIN.BETPRICEAMOUNT, 0) * COUNT(PBTLIN.TRANHEADERID)
                        ELSE SUM(PBTLIN.BETPRICEAMOUNT)
                    END, 2) AS COLAMOUNT
                , COUNT(DISTINCT PBTH.TICKETSERIALNUMBER) AS TICKETCOUNT
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON (PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID)
            INNER JOIN {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                ON (PBTH.TRANHEADERID = PBTLIN.TRANHEADERID)
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
            AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
            AND PBTH.PRODID = 3
            AND PBTLIN.BETTYPEID IN ({v_totomatchbettypes})
            AND PBTHLCS.BETSTATETYPEID = 'PB06'
            AND COALESCE(PBTLIN.GROUPUNITSEQUENCE, 1) = 1
            AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
            AND PBTH.ISEXCHANGETICKET = FALSE
            GROUP BY PBTH.TERDISPLAYID
                , PBTLIN.GROUPHOSTID
                , PBTLIN.BETPRICEAMOUNT
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("COLAMOUNT", sum),
                TICKETCOUNT=("TICKETCOUNT", sum)
            )
            .reset_index(drop=True)
        )
        df_temp = (
            df_temp
            .assign(PRODNAME="TOTO MATCH", FLAG="COL", ACTUALDATE=None)
            [["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'TOTO MATCH ProdID=3 COL' completed successfully, rows={len(df_temp)}")

    # SWEEP ProdID=4 COL PB06 PB03
        query = f"""
            SELECT PBTH.TERDISPLAYID
                , COALESCE(PBTLIN.BETPRICEAMOUNT, 0) AS COLAMOUNT
                , PBTH.TICKETTRANSACTIONID AS TICKETTRANSACTIONID
            FROM ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID
            INNER JOIN ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEM PBTL
                ON PBTH.TRANHEADERID = PBTL.TRANHEADERID
            INNER JOIN ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER PBTLIN
                ON PBTL.TRANLINEITEMID = PBTLIN.TRANLINEITEMID
            AND PBTLIN.ISSOLDOUT = FALSE
            INNER JOIN ZTUBT_TERMINAL ZT
                ON PBTH.TERDISPLAYID = ZT.TERDISPLAYID
            INNER JOIN ZTUBT_LOCATION ZL
                ON ZT.LOCID = ZL.LOCID
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
            AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
            AND PBTH.PRODID = 4
            AND (
                    (PBTHLCS.BETSTATETYPEID = 'PB06'
                    AND (PBTL.ISPRINTED IS NOT NULL OR ZL.SWEEPINDICATOR IS NULL))
                OR (PBTHLCS.BETSTATETYPEID = 'PB03'
                    AND (PBTL.ISPRINTED IS NULL AND ZL.SWEEPINDICATOR IS NOT NULL))
            )
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("COLAMOUNT", lambda x: round(float(x.sum()), 2)),
                TICKETCOUNT=("TICKETTRANSACTIONID", pd.Series.nunique)
            )
            .reset_index(drop=True)
        )
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="SINGAPORE SWEEP", FLAG="COL", ACTUALDATE=None)
            [["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'SWEEP ProdID=4 COL PB06 PB03' completed successfully, rows={len(df_temp)}")

    # SPORTS ProdID=5,6 COL
        query = f"""
            SELECT DISTINCT PBTLIN.TERDISPLAYID
                , PBTLIN.BETAMOUNT
                , PBTLIN.ACCUMULATORID
                , PBTLIN.TRANHEADERID
            FROM ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
                ON PBTH.TRANHEADERID = PBTHLCS.TRANHEADERID
            INNER JOIN ZTUBT_SPORTS_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                ON PBTH.TRANHEADERID = PBTLIN.TRANHEADERID
            WHERE PBTLIN.CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeob}')
            AND PBTLIN.CREATEDDATE < TO_TIMESTAMP('{vutctodatetimeob}')
            AND PBTH.PRODID IN (5,6)
            AND PBTHLCS.BETSTATETYPEID = 'PB06'
            AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
            AND PBTH.ISEXCHANGETICKET = FALSE
        """

        df_temp = pd.read_sql(query, connection)
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("BETAMOUNT", lambda x: round(float(x.sum()), 2)),
                TICKETCOUNT=("TRANHEADERID", pd.Series.nunique)
            )
            .reset_index(drop=True)
        )
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="SPORTS", FLAG="COL", ACTUALDATE=None)
            [["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'SPORTS ProdID=5,6 COL' completed successfully, rows={len(df_temp)}")

    # PAY
        query = f"""
            SELECT
                ZV.TERDISPLAYID
                , CASE WHEN ZP.PRODID IN (5,6) THEN 'SPORTS'
                        WHEN ZP.PRODID = 3 AND EXISTS (
                            SELECT 1
                            FROM {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                            WHERE ZV.TRANHEADERID = PBTLIN.TRANHEADERID
                            AND PBTLIN.BETTYPEID IN ({v_totomatchbettypes})) THEN 'TOTO MATCH'
                        ELSE TRIM(ZP.PRODNAME) END AS PRODNAME
                , ZP.PRODID
                , ZV.WINNINGAMOUNT
                , ZV.REBATEAMOUNT
                , ZV.REFUNDAMOUNT
            FROM {schema}.ZTUBT_VALIDATEDBETTICKET ZV
            INNER JOIN {schema}.ZTUBT_PRODUCT ZP ON (ZV.PRODID=ZP.PRODID)
            INNER JOIN {schema}.ZTUBT_VALIDATEDBETTICKETLIFECYCLESTATE ZVLC
                ON (ZV.TRANHEADERID = ZVLC.TRANHEADERID AND ZVLC.BETSTATETYPEID = 'VB06')
            WHERE 1=1
            AND (
                -- bmcs
                (
                    ZV.CREATEDVALIDATIONDATE >= TO_TIMESTAMP('{vutcfromdatetimebmcs}')
                    AND ZV.CREATEDVALIDATIONDATE < TO_TIMESTAMP('{vutctodatetimebmcs}')
                    AND ZVLC.CAPTUREDDATE BETWEEN TO_TIMESTAMP('{vutcfromdatetimebmcs}') - INTERVAL '1 DAY'
                        AND TO_TIMESTAMP('{vutctodatetimebmcs}') + INTERVAL '1 DAY'
                    AND ZV.PRODID IN (1)
                )
                OR
                -- igt
                (
                    ZV.CREATEDVALIDATIONDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
                    AND ZV.CREATEDVALIDATIONDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
                    AND ZVLC.CAPTUREDDATE BETWEEN TO_TIMESTAMP('{vutcfromdatetimeigt}') - INTERVAL '1 DAY'
                        AND TO_TIMESTAMP('{vutctodatetimeigt}') + INTERVAL '1 DAY'
                    AND ZV.PRODID IN (2,3,4)
                )
                OR
                -- ob
                (
                    ZV.CREATEDVALIDATIONDATE >= TO_TIMESTAMP('{vutcfromdatetimeob}')
                    AND ZV.CREATEDVALIDATIONDATE < TO_TIMESTAMP('{vutctodatetimeob}')
                    AND ZVLC.CAPTUREDDATE BETWEEN TO_TIMESTAMP('{vutcfromdatetimeob}') - INTERVAL '1 DAY'
                        AND TO_TIMESTAMP('{vutctodatetimeob}') + INTERVAL '1 DAY'
                    AND ZV.PRODID IN (5,6)
                )
            )
            """
        df_validated_base = pd.read_sql(query, connection)
        # Validated
        df_join = df_validated_base.loc[
            df_validated_base['WINNINGAMOUNT'] != 0, ['TERDISPLAYID', 'PRODNAME', 'WINNINGAMOUNT']
        ]
        df_join = df_join.rename(columns={'WINNINGAMOUNT': 'TOTAL_AMOUNT'})\
        .assign(TYPES='VALIDATED')
        df_unpivoted = pd.DataFrame()
        df_unpivoted = pd.concat([df_unpivoted, df_join], ignore_index=True)
        # Losing bet rebate
        df_join = df_validated_base.loc[
            df_validated_base['REBATEAMOUNT'] != 0, ['TERDISPLAYID', 'PRODNAME', 'REBATEAMOUNT']
        ]
        df_join = df_join.rename(columns={'REBATEAMOUNT': 'TOTAL_AMOUNT'})\
        .assign(TYPES='LOSING BET REBATE')

        df_unpivoted = pd.concat([df_unpivoted, df_join], ignore_index=True)
        # Refund
        df_join = df_validated_base.loc[
            df_validated_base['REFUNDAMOUNT'] != 0, ['TERDISPLAYID', 'PRODNAME', 'REFUNDAMOUNT']
        ]
        df_join = df_join.rename(columns={'REFUNDAMOUNT': 'TOTAL_AMOUNT'})\
        .assign(TYPES='REFUNDED')

        df_unpivoted = pd.concat([df_unpivoted, df_join], ignore_index=True)

        df_unpivoted['PRODNAME'] = df_unpivoted['TYPES'] + '-' + df_unpivoted['PRODNAME']
        df_temp = df_unpivoted.groupby(
            ['TERDISPLAYID', 'PRODNAME'], as_index=False
        ).agg(
            TOTAL_AMOUNT=('TOTAL_AMOUNT', 'sum'),
            TICKETCOUNT=('TOTAL_AMOUNT', 'count')
        )
        df_pay = df_ter_prod_val.merge(
            df_temp, on=['TERDISPLAYID', 'PRODNAME'], how='left'
        ).assign(
            FLAG='PAY', ACTUALDATE=None
        ).rename(columns={
            'TOTAL_AMOUNT': 'AMOUNT'
        })\
        [['TERDISPLAYID', 'PRODNAME', 'FLAG', 'AMOUNT', 'TICKETCOUNT', 'ACTUALDATE']]


        df_trans_amt = pd.concat([df_trans_amt, df_pay], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'PAY' completed successfully, rows={len(df_temp)}")
    # -- HorseRacing PRODID=1 PAY 'LOSING BET REBATE-HORSE RACING'
        query = f"""
            SELECT
                PBTLIN.TERDISPLAYID AS TERDISPLAYID
                , SUM(CASE
                        WHEN PBTLIN.BETTYPEID = 'W-P'
                        THEN COALESCE(PBTLIN.BOARDREBATEAMOUNT,0)/2
                        ELSE COALESCE(PBTLIN.BOARDREBATEAMOUNT,0)
                    END) AS COLAMOUNT
                , COUNT(DISTINCT PBTLIN.TRANHEADERID) AS TICKETCOUNT
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE PBTHLCS
            INNER JOIN {schema}.ZTUBT_HORSE_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                ON PBTHLCS.TRANHEADERID = PBTLIN.TRANHEADERID
            WHERE 1=1
                AND PBTLIN.CREATEDDATE >= '{vutcfromdatetimebmcs}'
                AND PBTLIN.CREATEDDATE < '{vutctodatetimebmcs}'
                AND PBTHLCS.BETSTATETYPEID = 'PB06'
            GROUP BY PBTLIN.TERDISPLAYID, PBTLIN.BETTYPEID
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ter_loc, df_temp, on=["TERDISPLAYID"], how="left")
        df_temp = df_temp.groupby(["TERDISPLAYID"], as_index=False).agg(
            AMOUNT=("COLAMOUNT", lambda x: round(float(x.fillna(0).sum()), 2)),
            TICKETCOUNT=("TICKETCOUNT", sum)
        ).reset_index(drop=True)

        df_temp = (
            df_temp
            .assign(FLAG="PAY", ACTUALDATE=None, PRODNAME="LOSING BET REBATE-HORSE RACING")
            [ ["TERDISPLAYID","PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'HorseRacing PRODID=1 PAY LOSING BET REBATE-HORSE RACING' completed successfully, rows={len(df_temp)}")
    # CANCELLED BETTICKETS
    # HorseRacing PRODID=1 CANCELLED
        query = f"""
            SELECT
                CBT.TERDISPLAYID AS TERDISPLAYID
                , ROUND(SUM(CBT.CANCELLEDAMOUT), 2) AS AMOUNT
                , COUNT(CBT.TRANHEADERID) AS TICKETCOUNT
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_CANCELLEDBETTICKET CBT
                ON PBTH.TRANHEADERID = CBT.TRANHEADERID
            INNER JOIN {schema}.ZTUBT_CANCELLEDBETTICKETLIFECYCLESTATE CBLCS
                ON PBTH.TRANHEADERID = CBLCS.TRANHEADERID
                AND CBLCS.BETSTATETYPEID = 'CB06'
            WHERE PBTH.PRODID = 1
                AND CBT.CANCELLEDDATE >= '{vutcfromdatetimebmcs}'
                AND CBT.CANCELLEDDATE < '{vutctodatetimebmcs}'
            GROUP BY CBT.TERDISPLAYID
        """
        df_temp = pd.read_sql(query, connection)
        #filter only horse racing on df_ter_prod
        df_ter_prod_filter = df_ter_prod[df_ter_prod["PRODNAME"] == "HORSE RACING"].reset_index(drop=True)

        df_temp = pd.merge(df_ter_prod_filter, df_temp, on=["TERDISPLAYID"], how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="HORSE RACING", FLAG="CAN", ACTUALDATE=None)
            [ ["TERDISPLAYID","PRODNAME" , "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'HorseRacing PRODID=1 CANCELLED' completed successfully, rows={len(df_temp)}")

    # HorseRacing PRODID=2,3 CANCELLED
        query = f"""
            SELECT
                CBT.TERDISPLAYID AS TERDISPLAYID
            , CASE
                    WHEN ZP.PRODID = 3 AND EXISTS (
                        SELECT 1
                        FROM {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                        WHERE PBTH.TRANHEADERID = PBTLIN.TRANHEADERID
                        AND PBTLIN.BETTYPEID IN ({v_totomatchbettypes})
                    ) THEN 'TOTO MATCH'
                    ELSE ZP.PRODNAME
                END AS PRODNAME
            , ROUND(SUM(CBT.CANCELLEDAMOUT), 2) AS AMOUNT
            , COUNT(CBT.TRANHEADERID) AS TICKETCOUNT
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_PRODUCT ZP
                ON PBTH.PRODID = ZP.PRODID
            INNER JOIN {schema}.ZTUBT_CANCELLEDBETTICKET CBT
                ON PBTH.TRANHEADERID = CBT.TRANHEADERID
            WHERE PBTH.PRODID IN (2,3)
            AND CBT.CANCELLEDDATE >= '{vutcfromdatetimeigt}'
            AND CBT.CANCELLEDDATE < '{vutctodatetimeigt}'
            AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
            AND PBTH.ISCANCELLED = TRUE
            AND PBTH.ISEXCHANGETICKET = FALSE
            GROUP BY CBT.TERDISPLAYID
            , CASE
                    WHEN ZP.PRODID = 3 AND EXISTS (
                        SELECT 1
                        FROM {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM PBTLIN
                        WHERE PBTH.TRANHEADERID = PBTLIN.TRANHEADERID
                        AND PBTLIN.BETTYPEID IN ({v_totomatchbettypes})
                    ) THEN 'TOTO MATCH'
                    ELSE ZP.PRODNAME
                END
        """
        df_temp = pd.read_sql(query, connection)
        df_ter_prod_copy = df_ter_prod.copy()
        df_ter_prod_copy['PRODNAME'] = df_ter_prod_copy['PRODNAME'].str.strip()
        df_temp['PRODNAME'] = df_temp['PRODNAME'].str.strip()
        df_temp = pd.merge(df_ter_prod_copy, df_temp, on=["TERDISPLAYID", "PRODNAME"], how="left")
        df_temp = df_temp[
            df_temp["PRODNAME"].isin(["4D Lottery", "TOTO", "TOTO MATCH"])
        ].reset_index(drop=True)
        df_temp = (
            df_temp
            .assign(FLAG="CAN", ACTUALDATE=None)
            [ ["TERDISPLAYID","PRODNAME" , "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'HorseRacing PRODID=2,3 CANCELLED' completed successfully, rows={len(df_temp)}")

    # CANCELLED BETTICKETS SWEEP PRODID=4 CANCELLED
        query = f"""
            SELECT
            CBT.TERDISPLAYID AS TERDISPLAYID
            , PD.PRODNAME AS PRODNAME
            , SUM(COALESCE(CBT.CANCELLEDAMOUT, 0)) AS AMOUNT
            , COUNT(CBT.TRANHEADERID) AS TICKETCOUNT
            FROM {schema}.ZTUBT_CANCELLEDBETTICKET CBT
            INNER JOIN {schema}.ZTUBT_CANCELLEDBETTICKETLIFECYCLESTATE CBLC
                ON CBLC.TRANHEADERID = CBT.TRANHEADERID AND CBLC.BETSTATETYPEID = 'CB06'
            INNER JOIN {schema}.ZTUBT_PRODUCT PD
                ON CBT.PRODID = PD.PRODID
            WHERE CBT.PRODID = 4
            AND CBT.CANCELLEDDATE BETWEEN '{vfromdatetimeigt}' AND '{vtodatetimeigt}'
            GROUP BY CBT.TERDISPLAYID, PD.PRODNAME
        """
        df_temp = pd.read_sql(query, connection)
        df_temp['PRODNAME'] = df_temp['PRODNAME'].str.strip()
        df_ter_prod['PRODNAME'] = df_ter_prod['PRODNAME'].str.strip()
        df_temp = pd.merge(df_ter_prod, df_temp, on=["TERDISPLAYID", "PRODNAME"], how="left")
        df_temp = df_temp[df_temp["PRODNAME"] == "SINGAPORE SWEEP"].reset_index(drop=True)
        df_temp = (
            df_temp
            .assign(FLAG="CAN", ACTUALDATE=None)
            [ ["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'CANCELLED BETTICKETS SWEEP PRODID=4 CANCELLED' completed successfully, rows={len(df_temp)}")

    # CANCELLED BETTICKETS SPORTS PRODID=5,6 CANCELLED
        query = f"""
            SELECT
            CBT.TERDISPLAYID AS TERDISPLAYID
            , ROUND(SUM(CBT.CANCELLEDAMOUT), 2) AS AMOUNT
            , COUNT(CBT.TRANHEADERID) AS TICKETCOUNT
            FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER PBTH
            INNER JOIN {schema}.ZTUBT_CANCELLEDBETTICKET CBT
                ON PBTH.TRANHEADERID = CBT.TRANHEADERID
            INNER JOIN {schema}.ZTUBT_CANCELLEDBETTICKETLIFECYCLESTATE CBLC
                ON PBTH.TRANHEADERID = CBLC.TRANHEADERID AND CBLC.BETSTATETYPEID = 'CB06'
            WHERE PBTH.PRODID IN (5,6)
            AND CBT.CANCELLEDDATE >= '{vutcfromdatetimeob}'
            AND CBT.CANCELLEDDATE < '{vutctodatetimeob}'
            AND COALESCE(PBTH.ISBETREJECTEDBYTRADER, FALSE) = FALSE
            AND PBTH.ISEXCHANGETICKET = FALSE
            GROUP BY CBT.TERDISPLAYID
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ter_prod, df_temp, on=["TERDISPLAYID"], how="left")
        df_temp = df_temp[df_temp["PRODNAME"] == "SPORTS"].reset_index(drop=True)
        df_temp = (
            df_temp
            .assign(FLAG="CAN", ACTUALDATE=None)
            [ ["TERDISPLAYID","PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'CANCELLED BETTICKETS SPORTS PRODID=5,6 CANCELLED' completed successfully, rows={len(df_temp)}")

# CAS
        ### Payment Type 'NC','NN'
        df_temp = df_ztubt_paymentdetail[
            df_ztubt_paymentdetail["PAYMENTTYPEID"].isin(['NC','NN'])]
        df_temp = df_temp.groupby(["TERDISPLAYID"], as_index=False).agg(
            AMOUNT=("PAIDAMOUNT", sum),
            TICKETCOUNT=("PAYMENTDETAILID", pd.Series.nunique)
        ).reset_index(drop=True)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on=["TERDISPLAYID"], how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="NETS", FLAG="CAS", ACTUALDATE=None)
            [ ["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'Payment Type 'NC','NN'' completed successfully, rows={len(df_temp)}")

        ### Pay Type 'NCC','NFP'
        df_temp = df_ztubt_paymentdetail[
            df_ztubt_paymentdetail["PAYMENTTYPEID"].isin(['NCC','NFP'])]
        df_temp = df_temp.groupby(["TERDISPLAYID"], as_index=False).agg(
            AMOUNT=("PAIDAMOUNT", sum),
            TICKETCOUNT=("PAYMENTDETAILID", pd.Series.nunique)
        ).reset_index(drop=True)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on=["TERDISPLAYID"], how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="CASHCARD/Flashpay", FLAG="CAS", ACTUALDATE=None)
            [ ["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] ' Pay Type 'NCC','NFP'' completed successfully, rows={len(df_temp)}")

        ### Merge paymentdetailid with cart and paynowtransaction
        df_merge = pd.merge(
            df_ztubt_terminal,
            df_ztubt_cart,
            on="TERDISPLAYID",
            how="left"
            ).merge(
                df_ztubt_paynowtransaction,
                on="CARTID",
                how="left"
            )
        df_merge = df_merge[
            (df_merge["UPDATEDDATETIME"] >= vutcfromdatetimeigt) &
            (df_merge["UPDATEDDATETIME"] < vutctodatetimeigt)
        ]

        ### PayNow 'PNRIC'
        df_temp = df_merge[
            (df_merge["PAYMENTTYPEID"] == 'PNRIC') &
            (df_merge["TRANSACTIONSTATUS"].isin(status_pnric))]
        df_temp['CASAMOUNT'] = -df_temp['NETAMOUNT'] + df_temp['TRANSACTIONFEE']
        df_temp = df_temp.groupby(["TERDISPLAYID"], as_index=False).agg(
            AMOUNT=("CASAMOUNT", sum),
            TICKETCOUNT=("PAYMENTDETAILID", pd.Series.nunique)
        ).reset_index(drop=True)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on=["TERDISPLAYID"], how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="Paynow [+trans fee]", FLAG="CAS", ACTUALDATE=None)
            [ ["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'PayNow 'PNRIC'' completed successfully, rows={len(df_temp)}")

        ### PaynowQR 'PNQR'
        df_temp = df_merge[
            (df_merge["PAYMENTTYPEID"] == 'PNQR') &
            (df_merge["TRANSACTIONSTATUS"].isin(status_pnqr))
        ]
        df_temp['CASAMOUNT'] = df_temp['NETAMOUNT'] + df_temp['TRANSACTIONFEE']
        df_temp = df_temp.groupby(["TERDISPLAYID"], as_index=False).agg(
            AMOUNT=("CASAMOUNT", sum),
            TICKETCOUNT=("PAYMENTDETAILID", pd.Series.nunique)
        ).reset_index(drop=True)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on=["TERDISPLAYID"], how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="PaynowQR [+trans fee]", FLAG="CAS", ACTUALDATE=None)
        )
        df_temp = df_temp[
            ["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]
        ]
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'PaynowQR 'PNQR'' completed successfully, rows={len(df_temp)}")

        ### PayNow Transaction Fee 'PNRIC'
        df_temp = df_merge[
            (df_merge["PAYMENTTYPEID"] == 'PNRIC') &
            (df_merge["TRANSACTIONSTATUS"].isin(status_pnric))]
        df_temp = df_temp.groupby(["TERDISPLAYID"], as_index=False).agg(
            AMOUNT=("TRANSACTIONFEE", sum),
            TICKETCOUNT=("PAYMENTDETAILID", pd.Series.nunique)
        ).reset_index(drop=True)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on=["TERDISPLAYID"], how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="Paynow", FLAG="CAS", ACTUALDATE=None)
            [ ["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'PayNow Transaction Fee 'PNRIC'' completed successfully, rows={len(df_temp)}")

        ### PayNow Transaction Fee 'PNQR'
        df_temp = df_merge[
            (df_merge["PAYMENTTYPEID"] == 'PNQR') &
            (df_merge["TRANSACTIONSTATUS"].isin(status_pnqr))]
        df_temp = df_temp.groupby(["TERDISPLAYID"], as_index=False).agg(
            AMOUNT=("TRANSACTIONFEE", sum),
            TICKETCOUNT=("PAYMENTDETAILID", pd.Series.nunique)
        ).reset_index(drop=True)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on=["TERDISPLAYID"], how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="PaynowQR", FLAG="CAS", ACTUALDATE=None)
            [ ["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'PayNow Transaction Fee 'PNQR'' completed successfully, rows={len(df_temp)}")

    # COL OFFLine ORDERS
        query = f"""
            SELECT PB.TERDISPLAYID
            , SUM(PBT.AMOUNT) AS OO_COL_AMOUNT
            , COUNT(1) AS TICKETCOUNT
            FROM {schema}.ZTUBT_OFFLINEPRODUCTHEADER PB
            INNER JOIN {schema}.ZTUBT_ADMISSIONVOUCHERTRANSACTION PBT
                ON PB.TRANSACTIONHEADERID = PBT.HEADERID
            WHERE 1 = 1
            AND PBT.ISACTIVE = TRUE
            AND PBT.TRANSACTIONTYPE = 1
            AND PB.TRANSACTIONTIME >= '{vfromdatetimenh}'
            AND PB.TRANSACTIONTIME < '{vtodatetimenh}'
            GROUP BY PB.TERDISPLAYID

            UNION ALL

            SELECT PB.TERDISPLAYID
            , SUM(PBT.TOTALAMOUNT * PBT.REPEATEDTICKET) AS OO_COL_AMOUNT
            , SUM(PBT.REPEATEDTICKET) AS TICKETCOUNT
            FROM {schema}.ZTUBT_OFFLINEPRODUCTHEADER PB
            INNER JOIN {schema}.ZTUBT_CHARITYTICKETTRANSACTION PBT
                ON PB.TRANSACTIONHEADERID = PBT.HEADERID
            WHERE 1 = 1
            AND PB.TRANSACTIONTIME >= '{vfromdatetimenh}'
            AND PB.TRANSACTIONTIME < '{vtodatetimenh}'
            GROUP BY PB.TERDISPLAYID

            UNION ALL

            SELECT PB.TERDISPLAYID
            , SUM(PBT.TOTALAMOUNT * PBT.REPEATEDTICKET) AS OO_COL_AMOUNT
            , SUM(PBT.REPEATEDTICKET) AS TICKETCOUNT
            FROM {schema}.ZTUBT_OFFLINEPRODUCTHEADER PB
            INNER JOIN {schema}.ZTUBT_SPATRANSACTION PBT
                ON PB.TRANSACTIONHEADERID = PBT.HEADERID
            WHERE 1 = 1
            AND PBT.TRANSACTIONTYPE = 1
            AND PB.TRANSACTIONTIME >= '{vfromdatetimenh}'
            AND PB.TRANSACTIONTIME < '{vtodatetimenh}'
            GROUP BY PB.TERDISPLAYID

            UNION ALL

            SELECT PB.TERDISPLAYID
            , SUM(PBT.TOTALAMOUNT * PBT.REPEATEDTICKET) AS OO_COL_AMOUNT
            , SUM(PBT.REPEATEDTICKET) AS TICKETCOUNT
            FROM {schema}.ZTUBT_OFFLINEPRODUCTHEADER PB
            INNER JOIN {schema}.ZTUBT_TOPUPCARDTRANSACTION PBT
                ON PB.TRANSACTIONHEADERID = PBT.HEADERID
            WHERE 1 = 1
            AND PBT.TRANSACTIONTYPE = 1
            AND PB.TRANSACTIONTIME >= '{vfromdatetimenh}'
            AND PB.TRANSACTIONTIME < '{vtodatetimenh}'
            GROUP BY PB.TERDISPLAYID

            UNION ALL

            SELECT PB.TERDISPLAYID
            , SUM(PBT.TOTALAMOUNT * PBT.REPEATEDTICKET) AS OO_COL_AMOUNT
            , SUM(PBT.REPEATEDTICKET) AS TICKETCOUNT
            FROM {schema}.ZTUBT_OFFLINEPRODUCTHEADER PB
            INNER JOIN {schema}.ZTUBT_TOTOHONGBAOTRANSACTION PBT
                ON PB.TRANSACTIONHEADERID = PBT.HEADERID
            WHERE 1 = 1
            AND PB.TRANSACTIONTIME >= '{vfromdatetimenh}'
            AND PB.TRANSACTIONTIME < '{vtodatetimenh}'
            GROUP BY PB.TERDISPLAYID
        """
        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="Offline Orders", FLAG="COL", ACTUALDATE=None)
            .rename(columns={"OO_COL_AMOUNT": "AMOUNT"})
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'COL OFFLine ORDERS' completed successfully, rows={len(df_temp)}")

    # RFD Offlines  ORDERS
        query = f"""
            SELECT PB.TERDISPLAYID,
                SUM(PBT.AMOUNT) OO_COL_AMOUNT,
                COUNT(1) TICKETCOUNT
            FROM {schema}.ZTUBT_OFFLINEPRODUCTHEADER PB
            INNER JOIN {schema}.ZTUBT_ADMISSIONVOUCHERTRANSACTION PBT
                ON (PB.TRANSACTIONHEADERID = PBT.HEADERID)
            WHERE PBT.ISACTIVE = TRUE
            AND PBT.TRANSACTIONTYPE IN (2, 3)
            AND PB.TRANSACTIONTIME >= '{vfromdatetimenh}'
            AND PB.TRANSACTIONTIME < '{vtodatetimenh}'
            GROUP BY PB.TERDISPLAYID

            UNION ALL

            SELECT PB.TERDISPLAYID,
                SUM(PBT.TOTALAMOUNT * PBT.REPEATEDTICKET) OO_COL_AMOUNT,
                SUM(PBT.REPEATEDTICKET) TICKETCOUNT
            FROM {schema}.ZTUBT_OFFLINEPRODUCTHEADER PB
            INNER JOIN {schema}.ZTUBT_SPATRANSACTION PBT
                ON (PB.TRANSACTIONHEADERID = PBT.HEADERID)
            WHERE PBT.TRANSACTIONTYPE = 2
            AND PB.TRANSACTIONTIME >= '{vfromdatetimenh}'
            AND PB.TRANSACTIONTIME < '{vtodatetimenh}'
            GROUP BY PB.TERDISPLAYID

            UNION ALL

            SELECT PB.TERDISPLAYID,
                SUM(PBT.TOTALAMOUNT * PBT.REPEATEDTICKET) OO_COL_AMOUNT,
                SUM(PBT.REPEATEDTICKET) TICKETCOUNT
            FROM {schema}.ZTUBT_OFFLINEPRODUCTHEADER PB
            INNER JOIN {schema}.ZTUBT_TOPUPCARDTRANSACTION PBT
                ON (PB.TRANSACTIONHEADERID = PBT.HEADERID)
            WHERE PBT.TRANSACTIONTYPE = 2
            AND PB.TRANSACTIONTIME >= '{vfromdatetimenh}'
            AND PB.TRANSACTIONTIME < '{vtodatetimenh}'
            GROUP BY PB.TERDISPLAYID
        """

        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="Offline Orders", FLAG="RFD", ACTUALDATE=None)
            .rename(columns={"OO_COL_AMOUNT": "AMOUNT"})
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'RFD Offlines ORDERS' completed successfully, rows={len(df_temp)}")

        # COL GATE ADMISSION
        query = f"""
            SELECT PB.TERDISPLAYID,
                SUM(PBT.TOTALAMOUNT * PBT.REPEATEDTICKET) OO_COL_AMOUNT,
                SUM(PBT.REPEATEDTICKET) TICKETCOUNT
            FROM {schema}.ZTUBT_OFFLINEPRODUCTHEADER PB
            INNER JOIN {schema}.ZTUBT_TOPUPCARDTRANSACTION PBT
                ON (PB.TRANSACTIONHEADERID = PBT.HEADERID)
            WHERE PBT.TRANSACTIONTYPE = 1
            AND PB.TRANSACTIONTIME >= '{vfromdatetimenh}'
            AND PB.TRANSACTIONTIME < '{vtodatetimenh}'
            GROUP BY PB.TERDISPLAYID
        """

        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="TERDISPLAYID", how="left")
        df_temp = (
            df_temp
            .assign(PRODNAME="Gate Admission", FLAG="COL", ACTUALDATE=None)
            .rename(columns={"OO_COL_AMOUNT": "AMOUNT"})
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'COL GATE ADMISSION' completed successfully, rows={len(df_temp)}")

        # FUNDING AND RECOVERY - FUN
        query = f"""
            SELECT ZL.LOCID,
                SUM(COALESCE(ZF.FUNDINGAMOUNT, 0)) FUNDINGAMOUNT
            FROM {schema}.ZTUBT_LOCATION ZL
            INNER JOIN {schema}.ZTUBT_FUNDING ZF
                ON (ZF.LOCID = ZL.LOCID)
            WHERE ZL.LOCTYPEID IN (2,4)
            AND CAST(ZF.FUNDPERIODSTART AS DATE) = '{vfromdatefund}'
            AND CAST(ZF.FUNDPERIODEND AS DATE) = '{vtodatefund}'
            GROUP BY ZL.LOCID
        """

        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="LOCID", how="left")
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("FUNDINGAMOUNT", sum)
            ).reset_index(drop=True)
        )
        df_temp = (
            df_temp
            .assign(PRODNAME="Funding and Recovery", FLAG="FUN", TICKETCOUNT=0, ACTUALDATE=None)
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'FUNDING AND RECOVERY - FUN' completed successfully, rows={len(df_temp)}")

        # FUNDING AND RECOVERY - REC
        query = f"""
            SELECT ZL.LOCID,
                SUM(COALESCE(ZF.FUNDINGAMOUNT,0)) FUNDINGAMOUNT
            FROM {schema}.ZTUBT_LOCATION ZL
            INNER JOIN {schema}.ZTUBT_FUNDING ZF
                ON (ZF.LOCID = ZL.LOCID)
            INNER JOIN {schema}.ZTUBT_RECOVERY ZR
                ON (ZR.FUNDINGID = ZF.FUNDINGID)
            WHERE ZL.LOCTYPEID IN (2,4)
            AND CAST(ZR.RECPERIODSTART AS DATE) = '{vfromdatefund}'
            AND CAST(ZR.RECPERIODEND AS DATE) = '{vtodatefund}'
            GROUP BY ZL.LOCID
        """

        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="LOCID", how="left")
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("FUNDINGAMOUNT", sum)
            ).reset_index(drop=True)
        )
        df_temp = (
            df_temp
            .assign(PRODNAME="Funding and Recovery", FLAG="REC", TICKETCOUNT=0, ACTUALDATE=None)
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'FUNDING AND RECOVERY - REC' completed successfully, rows={len(df_temp)}")

        # ONLINE ADJUSTMENT - FUN
        query = f"""
            SELECT ZL.LOCID,
                SUM(COALESCE(ZA.ADJUSTMENTAMOUNT,0)) ADJUSTMENTAMOUNT,
                COUNT(ZA.ADJUSTINVOICEID) TICKETCOUNT
            FROM {schema}.ZTUBT_LOCATION ZL
            INNER JOIN {schema}.ZTUBT_ADJUSTINVOICE ZA
                ON (ZA.LOCID = ZL.LOCID)
            INNER JOIN {schema}.ZTUBT_LOOKUPVALUECONFIG LKP
                ON (ZA.ADJUSTMENTCODE::VARCHAR = LKP.FLD1VALUE
                    AND LKP.CONFIGNAME = 'RTMS_AdjustmentCode'
                    AND LEFT(LKP.FLD2VALUE,1) = 'C')
            WHERE ZA.CREATEDDATETIME >= '{vfromdatetimenh}'
            AND ZA.CREATEDDATETIME < '{vtodatetimenh}'
            AND ZA.APPROVALSTATUS = 1
            GROUP BY ZL.LOCID
        """

        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="LOCID", how="left")
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("ADJUSTMENTAMOUNT", sum),
                TICKETCOUNT=("TICKETCOUNT", sum)
            ).reset_index(drop=True)
        )
        df_temp = (
            df_temp
            .assign(PRODNAME="Online Adjustment", FLAG="FUN", ACTUALDATE=None)
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'ONLINE ADJUSTMENT - FUN' completed successfully, rows={len(df_temp)}")

        # ONLINE ADJUSTMENT - REC
        query = f"""
            SELECT ZL.LOCID,
                SUM(COALESCE(ZA.ADJUSTMENTAMOUNT,0)) ADJUSTMENTAMOUNT,
                COUNT(ZA.ADJUSTINVOICEID) TICKETCOUNT
            FROM {schema}.ZTUBT_LOCATION ZL
            INNER JOIN {schema}.ZTUBT_ADJUSTINVOICE ZA
                ON (ZA.LOCID = ZL.LOCID)
            INNER JOIN {schema}.ZTUBT_LOOKUPVALUECONFIG LKP
                ON (ZA.ADJUSTMENTCODE::VARCHAR = LKP.FLD1VALUE
                    AND LKP.CONFIGNAME = 'RTMS_AdjustmentCode'
                    AND LEFT(LKP.FLD2VALUE,1) = 'D')
            WHERE ZA.CREATEDDATETIME >= '{vfromdatetimenh}'
            AND ZA.CREATEDDATETIME < '{vtodatetimenh}'
            AND ZA.APPROVALSTATUS = 1
            GROUP BY ZL.LOCID
        """

        df_temp = pd.read_sql(query, connection)
        df_temp = pd.merge(df_ztubt_terminal, df_temp, on="LOCID", how="left")
        df_temp = (
            df_temp.groupby(["TERDISPLAYID"], as_index=False)
            .agg(
                AMOUNT=("ADJUSTMENTAMOUNT", sum),
                TICKETCOUNT=("TICKETCOUNT", sum)
            ).reset_index(drop=True)
        )
        df_temp = (
            df_temp
            .assign(PRODNAME="Online Adjustment", FLAG="REC", ACTUALDATE=None)
            [["TERDISPLAYID","PRODNAME","FLAG","AMOUNT","TICKETCOUNT","ACTUALDATE"]]
        )
        df_trans_amt = pd.concat([df_trans_amt, df_temp], ignore_index=True)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] 'ONLINE ADJUSTMENT - REC' completed successfully, rows={len(df_temp)}")

        # Processing df_trans_amt final columns
        ## Standardize PRODNAME, AMOUNT, TICKETCOUNT
        df_trans_amt['PRODNAME'] = df_trans_amt['PRODNAME'].astype(str).str.strip()
        df_trans_amt['FLAG'] = df_trans_amt['FLAG'].astype(str).str.upper().str.strip()
        df_trans_amt['AMOUNT'] = pd.to_numeric(df_trans_amt['AMOUNT'], errors='coerce').fillna(0) / 100

        df_trans_amt['AMOUNT'] = np.where(
            df_trans_amt['FLAG'].isin(['CAN','RBT','PAY','GST','FUN','RFD']),
            -df_trans_amt['AMOUNT'].round(2),
            np.where(
                df_trans_amt['FLAG'] == 'SAL',
                -(np.floor(df_trans_amt['AMOUNT']*100)/100).round(2),
                df_trans_amt['AMOUNT'].round(2)
            )
        )
        df_trans_amt['TICKETCOUNT'] = df_trans_amt['TICKETCOUNT'].fillna(0).astype(int)
        df_trans_amt = df_trans_amt[["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
    except Exception:
        logger.error("[SP_GETTRANSAMOUNTDETAILS] Generating df_trans_amt encountered an error", exc_info=True)
        raise

    # ==========================================================================================
    # df_ubt_temp gst
    # ==========================================================================================

    ## ==========================================================================================
    # Convert date columns to datetime format
    try:
        df_trans_amt['ACTUALDATE'] = pd.to_datetime(df_trans_amt['ACTUALDATE'])
        df_ztubt_gstconfig['EFFECTIVEFROM'] = pd.to_datetime(df_ztubt_gstconfig['EFFECTIVEFROM'])
        df_ztubt_gstconfig['ENDDATE'] = pd.to_datetime(df_ztubt_gstconfig['ENDDATE'])
        df_ztubt_locationgsthistory['STARTDATE'] = pd.to_datetime(df_ztubt_locationgsthistory['STARTDATE'])
        df_ztubt_locationgsthistory['ENDDATE'] = pd.to_datetime(df_ztubt_locationgsthistory['ENDDATE'])

        logger.info("[SP_GETTRANSAMOUNTDETAILS] Date conversion completed")
    except Exception:
        logger.error("[SP_GETTRANSAMOUNTDETAILS] Date conversion failed", exc_info=True)
        raise

    try:
    ## ==========================================================================================
        ## Merge df_trans_amt with gst config inner join

        df_trans_amt_copy = df_trans_amt.copy()
        df_trans_amt_copy['_key'] = 1
        df_ztubt_gstconfig['_key'] = 1
        df_temp = pd.merge(df_trans_amt_copy, df_ztubt_gstconfig, on='_key').drop('_key', axis=1)

        ## Filer GST config date range
        df_temp = df_temp[
            ((df_temp['ACTUALDATE'] >= df_temp['EFFECTIVEFROM']) &
                ((df_temp['ACTUALDATE'] <= df_temp['ENDDATE']) | (df_temp['ENDDATE'].isna())))]
        df_temp = df_temp[df_temp['FLAG'].isin(['SAL'])]

        ## ==========================================================================================
        ## NOT TOTO MATCH
        part1 = (
            df_temp[df_temp['PRODNAME'] != 'TOTO MATCH']
            .groupby(['TERDISPLAYID', 'PRODNAME', 'FLAG', 'ACTUALDATE', 'GSTRATE'], as_index=False)
            .agg(
                AMOUNT=('AMOUNT', 'sum'),
                TICKETCOUNT=('TICKETCOUNT', 'sum')
            )
        )
        part1['AMOUNT'] = (part1['AMOUNT'] * part1['GSTRATE'] * 0.01).round(2)
        part1 = part1[['TERDISPLAYID','PRODNAME','FLAG','ACTUALDATE','AMOUNT','TICKETCOUNT']]
        ## TOTO MATCH
        part2 = (
            df_temp[df_temp['PRODNAME'].isin(['TOTO MATCH','TOTO'])]
            .groupby(
                ['TERDISPLAYID', 'FLAG', 'ACTUALDATE', 'GSTRATE'], as_index=False
            ).agg(
            AMOUNT_TOTAL=('AMOUNT', 'sum'),
            AMOUNT_TOTO=('AMOUNT', lambda x: x[df_temp.loc[x.index,'PRODNAME']=='TOTO'].sum()),
            TICKETCOUNT=('TICKETCOUNT', lambda x: x[df_temp.loc[x.index,'PRODNAME']=='TOTO MATCH'].sum())
            )
        )
        part2['AMOUNT'] = ((part2['AMOUNT_TOTAL'] - part2['AMOUNT_TOTO'])
                            * part2['GSTRATE'] * 0.01).round(2)
        part2 = (
            part2
            .assign(PRODNAME="TOTO MATCH")
            [ ["TERDISPLAYID", "PRODNAME", "FLAG", "AMOUNT", "TICKETCOUNT", "ACTUALDATE"]]
        )
        df_temp = pd.concat([part1, part2], ignore_index=True)
        # ==========================================================================================
        # 1. Join df_temp with df_ztubt_terminal (INNER JOIN)
        df_temp = pd.merge(df_temp, df_ztubt_terminal[['TERDISPLAYID', 'LOCID']], on='TERDISPLAYID', how='inner')

        # 2. Join with df_ztubt_locationgsthistory (LEFT JOIN)
        df_temp = pd.merge(
            df_temp,
            df_ztubt_locationgsthistory[['LOCID', 'STARTDATE', 'ENDDATE', 'ISGST', 'ISDELETED']],
            on='LOCID',
            how='left'
        )
        # 3. Filter
        df_temp = df_temp[
            (df_temp['FLAG'] == 'SAL') &
            (df_temp['ISDELETED'] == False) &
            (df_temp['ACTUALDATE'] >= df_temp['STARTDATE']) &
            (df_temp['ACTUALDATE'] <= df_temp['ENDDATE'])
        ]
        # 4. Case WHEN for AMOUNT col
        df_temp['AMOUNT'] = df_temp.apply(lambda row: row['AMOUNT'] if row['ISGST'] else 0, axis=1)
        # 5. Select final cols
        df_temp = df_temp[['TERDISPLAYID', 'PRODNAME', 'FLAG', 'AMOUNT', 'TICKETCOUNT', 'ACTUALDATE']].reset_index(drop=True)

        # ==========================================================================================
        # Trim PRODNAME before join
        df_temp['PRODNAME'] = df_temp['PRODNAME'].str.strip()
        df_ter_prod['PRODNAME'] = df_ter_prod['PRODNAME'].str.strip()

        # Join with df_ter_prod
        df_temp = df_ter_prod.merge(
            df_temp,
            on=['TERDISPLAYID','PRODNAME'],
            how='left'
        )

        # Fill NaN and cast types
        df_temp['AMOUNT'] = df_temp['AMOUNT'].fillna(0).astype(float)
        df_temp['TICKETCOUNT'] = df_temp['TICKETCOUNT'].fillna(0).astype(int)
        df_temp['FLAG'] = 'GST'

        # Select final columns
        df_temp = df_temp[['TERDISPLAYID', 'PRODNAME', 'FLAG', 'AMOUNT', 'TICKETCOUNT', 'ACTUALDATE']]

        # UNION with df_trans_amt where FLAG == 'SAL'
        df_trans_amt_sal = df_trans_amt[df_trans_amt['FLAG'] == 'SAL']
        df_temp = pd.concat([df_temp, df_trans_amt_sal], ignore_index=True)

        # Group by TERDISPLAYID, PRODNAME, FLAG and aggregate
        df_temp_gst_sal = df_temp.groupby(
            ['TERDISPLAYID','PRODNAME','FLAG'], as_index=False
        ).agg(
            AMOUNT=('AMOUNT', 'sum'),
            TICKETCOUNT=('TICKETCOUNT', 'sum')
        )

        # Add ACTUALDATE column
        df_temp_gst_sal['ACTUALDATE'] = None

        # Remove old 'SAL' rows from df_trans_amt
        df_trans_amt = df_trans_amt[df_trans_amt['FLAG'] != 'SAL']

        # Append grouped GST + SAL
        df_trans_amt = pd.concat([df_trans_amt, df_temp_gst_sal], ignore_index=True)

        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] df_temp_gst_sal update completed in ubt_temp_trans_amt, rows={len(df_trans_amt)}")
    except Exception:
        logger.error("[SP_GETTRANSAMOUNTDETAILS] df_temp_gst_sal update in ubt_temp_trans_amt encountered an error", exc_info=True)
        raise
    # ==========================================================================================
    # Line 1371-1415: Process ubt_temp_toto_can_tktcount
    # ==========================================================================================
    try:
        query = f"""
            SELECT
            ZC.TRANHEADERID ,ZC.TERDISPLAYID FROM ZTUBT_CANCELLEDBETTICKET ZC
            INNER JOIN ZTUBT_CANCELLEDBETTICKETLIFECYCLESTATE ZC2
            ON ZC.TRANHEADERID=ZC2.TRANHEADERID AND ZC2.BETSTATETYPEID='CB06'
            WHERE ZC.CANCELLEDDATE >= '{vfromdatetimeigt}'
                AND ZC.CANCELLEDDATE < '{vtodatetimeigt}'
        """
        df_fin = pd.read_sql(query, connection)

        query = f"""
            SELECT * FROM {schema}.ztubt_placedbettransactionheader
        """
        df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)

        query = f"""
            SELECT * FROM {schema}.ztubt_toto_placedbettransactionlineitem
            where bettypeid NOT IN ('M AN', 'M 2', 'M 3', 'M 4')
        """
        df_ztubt_toto_placedbettransactionlineitem = pd.read_sql(query, connection)
        # ==========================================================================================
        df_temp = df_ztubt_placedbettransactionheader.merge(
            df_ztubt_toto_placedbettransactionlineitem,
            on='TRANHEADERID',
            how='inner'
        ).merge(
            df_fin,
            on='TRANHEADERID',
            how='inner'
        )
        part1 = (
            df_temp[df_temp['GROUPUNITSEQUENCE'].isna()]
            .groupby(['TERDISPLAYID'], as_index=False)
            .agg(TOTO_TICKETCOUNT=('TICKETSERIALNUMBER', 'nunique')
        ))

        part2 = (
            df_temp[df_temp['GROUPUNITSEQUENCE'].notna()]
            .groupby(['TERDISPLAYID'], as_index=False)
            .agg(TOTO_TICKETCOUNT=('GROUPHOSTID', 'nunique'))
        )
        df_temp = pd.concat([part1, part2], ignore_index=True)
        df_toto_can_tktcount = df_temp.groupby(['TERDISPLAYID'], as_index=False).agg(
            TOTO_TICKETCOUNT=('TOTO_TICKETCOUNT', 'sum')
        ).reset_index(drop=True)
        # ==========================================================================================
        # UPDATE df_trans_amt column

            # Bước 1: Tạo mask chỉ lấy dòng TOTO + CAN
        mask = (df_trans_amt['PRODNAME'] == 'TOTO') & (df_trans_amt['FLAG'] == 'CAN')

        # Bước 2: Merge với bảng hủy, dùng suffixes để tránh conflict cột TICKETCOUNT
        df_trans_amt = df_trans_amt.merge(
            df_toto_can_tktcount.rename(columns={'TOTO_TICKETCOUNT': 'TICKETCOUNT_NEW'}),
            on='TERDISPLAYID',
            how='left'
        )

        # Bước 3: Update TICKETCOUNT cho dòng TOTO + CAN, giữ nguyên các dòng khác
        df_trans_amt.loc[mask, 'TICKETCOUNT'] = df_trans_amt.loc[mask, 'TICKETCOUNT_NEW'].fillna(0).astype('int64')

        # Bước 4: Xóa cột tạm TICKETCOUNT_NEW (không cần thiết)
        df_trans_amt = df_trans_amt.drop('TICKETCOUNT_NEW', axis=1)
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] ubt_temp_toto_can_tktcount update completed in ubt_temp_trans_amt, rows={len(df_trans_amt)}")
    except Exception:
        logger.error("[SP_GETTRANSAMOUNTDETAILS] ubt_temp_toto_can_tktcount update in ubt_temp_trans_amt encountered an error", exc_info=True)
        raise

    # ==========================================================================================
    # Line 1417-1453: Process ubt_temp_totofo_can_tktcount
    # ==========================================================================================
    try:
        query = f"""
            SELECT FIN.TERDISPLAYID, ZP.TICKETSERIALNUMBER, ZTP.GROUPHOSTID, ZTP.GROUPUNITSEQUENCE
            FROM ZTUBT_PLACEDBETTRANSACTIONHEADER ZP
            INNER JOIN ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM ZTP
                ON (ZP.TRANHEADERID = ZTP.TRANHEADERID)
            INNER JOIN (
                SELECT ZC.TRANHEADERID, ZC.TERDISPLAYID
                FROM ZTUBT_CANCELLEDBETTICKET ZC
                INNER JOIN ZTUBT_CANCELLEDBETTICKETLIFECYCLESTATE ZC2
                    ON (ZC.TRANHEADERID = ZC2.TRANHEADERID AND ZC2.BETSTATETYPEID = 'CB06')
                WHERE 1 = 1
                    AND ZC.CANCELLEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}')
                    AND ZC.CANCELLEDDATE < TO_TIMESTAMP('{vutctodatetimeigt}')
            ) FIN
                ON (ZP.TRANHEADERID = FIN.TRANHEADERID)
            WHERE ZTP.BETTYPEID IN ({v_totomatchbettypes})
        """
        df_temp = pd.read_sql(query, connection)
        part1 = df_temp[df_temp["GROUPUNITSEQUENCE"].isnull()]
        part1 = df_temp.groupby(["TERDISPLAYID"], as_index=False)\
            .agg(
                TOTOFO_TICKETCOUNT=("TICKETSERIALNUMBER", pd.Series.nunique)
            ).reset_index(drop=True)
        part1 = part1[["TERDISPLAYID", "TOTOFO_TICKETCOUNT"]]

        part2 = df_temp[df_temp["GROUPUNITSEQUENCE"].notnull()]
        part2 = df_temp.groupby(["TERDISPLAYID"], as_index=False)\
        .agg(
            TOTOFO_TICKETCOUNT=("GROUPHOSTID", pd.Series.nunique)
        ).reset_index(drop=True)
        part2 = part2[["TERDISPLAYID", "TOTOFO_TICKETCOUNT"]]

        df_temp = pd.concat([part1, part2], ignore_index=True)

        df_totofo_can_tktcount = df_temp.groupby(["TERDISPLAYID"], as_index=False)\
            .agg(
                TOTOFO_TICKETCOUNT=("TOTOFO_TICKETCOUNT", sum)
            ).reset_index(drop=True)


        # ==========================================================================================
        # UPDATE df_trans_amt column
        mask = (df_trans_amt['PRODNAME'] == 'TOTO MATCH') & (df_trans_amt['FLAG'] == 'CAN')
        df_trans_amt = pd.merge(
                        df_trans_amt,
                        df_totofo_can_tktcount,
                        on='TERDISPLAYID',
                        how='left')
        df_trans_amt.loc[mask, 'TICKETCOUNT'] = (
                        df_trans_amt.loc[mask, 'TOTOFO_TICKETCOUNT']
                        .fillna(0)
                        .astype('int64'))
        df_trans_amt = df_trans_amt.drop(columns=['TOTOFO_TICKETCOUNT'])

        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] ubt_temp_totofo_can_tktcount update completed in ubt_temp_trans_amt, rows={len(df_trans_amt)}")
    except Exception:
        logger.error("[SP_GETTRANSAMOUNTDETAILS] ubt_temp_totofo_can_tktcount update in ubt_temp_trans_amt encountered an error", exc_info=True)
        raise

    # ==========================================================================================
    # Line 1461-1468: Process ubt_temp_toto_sal_count
    # ==========================================================================================
    try:
        df_temp = df_trans_amt[(df_trans_amt['PRODNAME'] == 'TOTO') & (df_trans_amt['FLAG'].isin(['COL','CAN']))]
        df_temp['TICKET_COL'] = df_temp.apply(lambda row: row['TICKETCOUNT'] if row['FLAG']=='COL' else 0, axis=1)
        df_temp['TICKET_CAN'] = df_temp.apply(lambda row: row['TICKETCOUNT'] if row['FLAG']=='CAN' else 0, axis=1)
        df_temp = df_temp.groupby(["TERDISPLAYID", "PRODNAME"], as_index=False).agg(
                TICKET_COL=("TICKET_COL", sum),
                TICKET_CAN=("TICKET_CAN", sum)
            ).reset_index(drop=True)
        df_temp['TOTO_SAL_COUNT'] = df_temp['TICKET_COL'] - df_temp['TICKET_CAN']
        df_toto_sal_count = df_temp[['TERDISPLAYID', 'PRODNAME', 'TOTO_SAL_COUNT']]


        # ==========================================================================================
        # UPDATE df_trans_amt column
        mask = df_trans_amt['FLAG'] == 'SAL'
        df_trans_amt = pd.merge(
                    df_trans_amt,
                    df_toto_sal_count,
                    on=['TERDISPLAYID', 'PRODNAME'],
                    how='left')
        df_trans_amt.loc[mask, 'TICKETCOUNT'] = (
                    df_trans_amt.loc[mask, 'TOTO_SAL_COUNT']
                    .fillna(0)
                    .astype('int64'))
        df_trans_amt = df_trans_amt.drop(columns=['TOTO_SAL_COUNT'])

        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] ubt_temp_toto_sal_count update completed in ubt_temp_trans_amt, rows={len(df_trans_amt)}")
    except Exception:
        logger.error("[SP_GETTRANSAMOUNTDETAILS] ubt_temp_toto_sal_count update in ubt_temp_trans_amt encountered an error", exc_info=True)
        raise
    # ==========================================================================================
    # Line 1476-1483: Process ubt_temp_totofo_sal_count
    # ==========================================================================================
    try:
        df_temp = df_trans_amt[(df_trans_amt['PRODNAME'] == 'TOTO MATCH') & (df_trans_amt['FLAG'].isin(['COL','CAN']))]
        df_temp['TICKET_COL'] = df_temp.apply(lambda row: row['TICKETCOUNT'] if row['FLAG']=='COL' else 0, axis=1)
        df_temp['TICKET_CAN'] = df_temp.apply(lambda row: row['TICKETCOUNT'] if row['FLAG']=='CAN' else 0, axis=1)
        df_temp = df_temp.groupby(["TERDISPLAYID", "PRODNAME"], as_index=False).agg(
                TICKET_COL=("TICKET_COL", sum),
                TICKET_CAN=("TICKET_CAN", sum)
            ).reset_index(drop=True)
        df_temp['TOTOFO_SAL_COUNT'] = df_temp['TICKET_COL'] - df_temp['TICKET_CAN']
        df_totofo_sal_count = df_temp[['TERDISPLAYID', 'PRODNAME', 'TOTOFO_SAL_COUNT']]

        # ==========================================================================================
        # UPDATE df_trans_amt column
        mask = df_trans_amt['FLAG'] == 'SAL'
        df_trans_amt = pd.merge(
                    df_trans_amt,
                    df_totofo_sal_count,
                    on=['TERDISPLAYID', 'PRODNAME'],
                    how='left')
        df_trans_amt.loc[mask, 'TICKETCOUNT'] = (
                    df_trans_amt.loc[mask, 'TOTOFO_SAL_COUNT']
                    .fillna(0)
                    .astype('int64'))
        df_trans_amt = df_trans_amt.drop(columns=['TOTOFO_SAL_COUNT'])

        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] ubt_temp_totofo_sal_count update completed in ubt_temp_trans_amt, rows={len(df_trans_amt)}")
    except Exception:
        logger.error("[SP_GETTRANSAMOUNTDETAILS] ubt_temp_totofo_sal_count update in ubt_temp_trans_amt encountered an error", exc_info=True)
        raise
    # ==========================================================================================
    # Line 1492-1517: Process ubt_temp_trans_final
    # ==========================================================================================
    try:
        df_trans_final = pd.merge(
                    df_trans_amt,
                    df_ter_loc[['TERDISPLAYID', 'LOCTYPEID']],
                    on=['TERDISPLAYID'],
                    how='left')
        df_trans_final = df_trans_final[~df_trans_final['PRODNAME'].str.startswith('EXPIRED-')]
        df_trans_final['FLAG'] = np.where(
                    df_trans_final['PRODNAME'].str.startswith('REFUNDED-'),
                    'RFD',
                    np.where(
                        df_trans_final['PRODNAME'].str.startswith('LOSING BET REBATE-'),
                        'RBT',
                        df_trans_final['FLAG']))

        df_trans_final['TICKETCOUNT'] = np.where(
                    (df_trans_final['FLAG'] == 'SAL') & (df_trans_final['LOCTYPEID'] == 1),
                    0,
                    df_trans_final['TICKETCOUNT'])

        df_trans_final['TRANSTYPE'] = np.where(
                    df_trans_final['PRODNAME'].isin(['Offline Orders', 'Gate Admission']),
                    'OO',
                    np.where(
                        df_trans_final['PRODNAME'].isin(['NETS','CASHCARD/Flashpay', 'Paynow [+trans fee]', 'PaynowQR [+trans fee]']),
                        'CL',
                        np.where(
                            df_trans_final['PRODNAME'].isin(['Paynow','PaynowQR']),
                            'TF',
                            'CH')))
        df_trans_final['PRODNAME'] = np.where(
                    df_trans_final['PRODNAME'].str.startswith('REFUNDED-'),
                    df_trans_final['PRODNAME'].str.replace('REFUNDED-', '', regex=False),
                    np.where(
                        df_trans_final['PRODNAME'].str.startswith('LOSING BET REBATE-'),
                        df_trans_final['PRODNAME'].str.replace('LOSING BET REBATE-', '', regex=False),
                        np.where(
                            df_trans_final['PRODNAME'].str.startswith('VALIDATED-'),
                            df_trans_final['PRODNAME'].str.replace('VALIDATED-', '', regex=False),
                            df_trans_final['PRODNAME'])))

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"[SP_GETTRANSAMOUNTDETAILS] completed successfully in {duration:.2f} seconds and fetched: rows={len(df_trans_final)}")
    except Exception:
        logger.error("[SP_GETTRANSAMOUNTDETAILS] ubt_temp_trans_final update in ubt_temp_trans_amt encountered an error", exc_info=True)
        raise
    return df_trans_final.drop_duplicates().reset_index(drop=True)
