import pandas as pd
import numpy as np
from sp_ubt_getcommonubtdates import *
from Snowflake_connection import snowflake_connection
import logging
logger = logging.getLogger(__name__)

connection = snowflake_connection()
schema = 'SPPL_DEV_DWH.SPPL_PUBLIC'

def sp_ubt_getsweepsalespersrterminal(infromdate, intodate):
    df_final_result = pd.DataFrame()
    # infromdate = pd.to_datetime(infromdate)
    # intodate = pd.to_datetime(intodate)

    df_getcommonubtdates = sp_ubt_getcommonubtdates(infromdate, intodate)
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


    # Extract data from ztubt_terminal & ztubt_location
    query = f"""
        SELECT
            T.TERDISPLAYID,
            T.LOCID,
            LOC.SWEEPINDICATOR
        FROM {schema}.ZTUBT_LOCATION LOC
        INNER JOIN {schema}.ZTUBT_TERMINAL T
            ON LOC.LOCID = T.LOCID
        WHERE (LOC.SWEEPINDICATOR IS NOT NULL OR LOC.SWEEPINDICATOR <> 0)
    """
    df_ter = pd.read_sql(query, connection)


    # Extract data from ztubt_sweep_placedbettransactionlineitem
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEM
    """
    df_ztubt_sweep_placedbettransactionlineitem = pd.read_sql(query, connection)

    # Extract data from ztubt_sweep_placedbettransactionlineitemnumber
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER
        WHERE ISSOLDOUT = FALSE
    """
    df_ztubt_sweep_placedbettransactionlineitemnumber = pd.read_sql(query, connection)

    # Extract data from ztubt_placedbettransactionheader

    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER
        WHERE
            CREATEDDATE >= '{vutcfromdatetimeigt}' AND CREATEDDATE < '{vutctodatetimeigt}'
            """
    df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)



    # Extract data from ztubt_placedbettransactionheaderlifecyclestate
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE
        WHERE BETSTATETYPEID = 'PB06'
    """
    df_ztubt_placedbettransactionheaderlifecyclestate = pd.read_sql(query, connection)


    # Extract data from ZTUBT_GETBUSINESSDATE_PERHOST
    query = f"""
        SELECT *
        FROM ZTUBT_GETBUSINESSDATE_PERHOST ZGP
        WHERE
            PREVIOUSPERIODDATETIME >= '{vfromdatetimeigt}' AND PREVIOUSPERIODDATETIME < '{vtodatetimeigt}'

        AND HOST = 1
    """
    df_ubt_temp_businessdatefunction = pd.read_sql(query, connection)

    # Extract data from ZTUBT_CANCELLEDBETTICKET
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_CANCELLEDBETTICKET
        WHERE PRODID = 4
            AND (CANCELLEDDATE >= '{vfromdatetimeigt}' AND CANCELLEDDATE < '{vtodatetimeigt}')
    """
    df_ztubt_cancelledbetticket = pd.read_sql(query, connection)

    # Extract data from ZTUBT_CANCELLEDBETTICKETLIFECYCLESTATE
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_CANCELLEDBETTICKETLIFECYCLESTATE
        WHERE BETSTATETYPEID = 'CB06'
    """
    df_ztubt_cancelledbetticketlifecyclestate = pd.read_sql(query, connection)


    # Extract data from ztubt_sweep_placedbettransactionlineitemnumber
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER
        WHERE ISSOLDOUT = FALSE
        AND CREATEDDATE >= TO_TIMESTAMP('{vutcfromdatetimeigt}') - INTERVAL '45 DAYS'
        AND CREATEDDATE <  TO_TIMESTAMP('{vutctodatetimeigt}')

    """
    df_ztubt_sweep_placedbettransactionlineitemnumber = pd.read_sql(query, connection)


    # Extract data from ztubt_product
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_PRODUCT
    """
    df_ztubt_product = pd.read_sql(query, connection)




    def df_ubt_temp_ResultSalesComWithDate():

        df_ubt_temp_ResultSalesComWithDate = (
            df_ztubt_placedbettransactionheader.assign(key=1).add_suffix('_PBTH')
            .merge(df_ter.add_suffix('_TER'), left_on="TERDISPLAYID_PBTH", right_on="TERDISPLAYID_TER")
            .merge(df_ztubt_sweep_placedbettransactionlineitem.add_suffix('_PBTL'), left_on="TRANHEADERID_PBTH", right_on="TRANHEADERID_PBTL")
            .merge(df_ztubt_sweep_placedbettransactionlineitemnumber.add_suffix('_PBTLN'), left_on="TRANLINEITEMID_PBTL", right_on="TRANLINEITEMID_PBTLN")
            .merge(df_ztubt_placedbettransactionheaderlifecyclestate.add_suffix('_PBLC'), left_on="TRANHEADERID_PBTH", right_on="TRANHEADERID_PBLC")
            .merge(df_ubt_temp_businessdatefunction.assign(key=1).add_suffix('_GB'), how="left", left_on="key_PBTH", right_on="key_GB")
        )

        df_ubt_temp_ResultSalesComWithDate = df_ubt_temp_ResultSalesComWithDate[
            df_ubt_temp_ResultSalesComWithDate["CREATEDDATE_PBTH"].between(df_ubt_temp_ResultSalesComWithDate["PREVIOUSPERIODDATETIMEUTC_GB"], df_ubt_temp_ResultSalesComWithDate["PERIODDATETIMEUTC_GB"])
        ]

        df_ubt_temp_ResultSalesComWithDate = (
            df_ubt_temp_ResultSalesComWithDate.groupby(
                ["TICKETSERIALNUMBER_PBTH", "TERDISPLAYID_TER", "PRODID_PBTH", "ACTUALDATE_GB"],
                as_index=False
            )
            .agg({
                "SALESCOMMAMOUNT_PBTLN": lambda x: x.fillna(0).sum(),
                "SALESFACTORAMOUNT_PBTLN": lambda x: x.fillna(0).sum()
            })
            .assign(
                SECONDSALESCOMMAMOUNT=0,
                SECONDSALESFACTORAMOUNT=0
            )
            .rename(columns={
                "TICKETSERIALNUMBER_PBTH": "TICKETSERIALNUMBER",
                "TERDISPLAYID_TER": "TERDISPLAYID",
                "PRODID_PBTH": "PRODID",
                "ACTUALDATE_GB": "DT",
                "SALESCOMMAMOUNT_PBTLN": "SALESCOMMAMOUNT",
                "SALESFACTORAMOUNT_PBTLN": "SALESFACTORAMOUNT"
            })
        )[[
            "TICKETSERIALNUMBER", "TERDISPLAYID", "PRODID", "DT",
            "SALESCOMMAMOUNT", "SALESFACTORAMOUNT", "SECONDSALESCOMMAMOUNT", "SECONDSALESFACTORAMOUNT"
        ]]

        print(f"df_ubt_temp_ResultSalesComWithDate : COMPLETED (Line 39 - 53) with {df_ubt_temp_ResultSalesComWithDate.shape[0]} rows")
        return df_ubt_temp_ResultSalesComWithDate


    def df_ubt_temp_CancelledSalesComWithDate():
        df_ubt_temp_CancelledSalesComWithDate = (
            df_ztubt_cancelledbetticket.assign(key=1).add_suffix('_CBT')
            .merge(df_ter.add_suffix('_TER'), left_on="TERDISPLAYID_CBT", right_on="TERDISPLAYID_TER")
            .merge(df_ztubt_sweep_placedbettransactionlineitem.add_suffix('_PBTL'), left_on="TRANHEADERID_CBT", right_on="TRANHEADERID_PBTL")
            .merge(df_ztubt_sweep_placedbettransactionlineitemnumber.add_suffix('_PBTLN'), left_on="TRANLINEITEMID_PBTL", right_on="TRANLINEITEMID_PBTLN")
            .merge(df_ztubt_cancelledbetticketlifecyclestate.add_suffix('_CBLC'), left_on="TRANHEADERID_CBT", right_on="TRANHEADERID_CBLC")
            .merge(df_ubt_temp_businessdatefunction.assign(key=1).add_suffix('_GB'), how="left", left_on="key_CBT", right_on="key_GB")
        )

        df_ubt_temp_CancelledSalesComWithDate = df_ubt_temp_CancelledSalesComWithDate[
            df_ubt_temp_CancelledSalesComWithDate["CANCELLEDDATE_CBT"].between(df_ubt_temp_CancelledSalesComWithDate["PREVIOUSPERIODDATETIME_GB"], df_ubt_temp_CancelledSalesComWithDate["PERIODDATETIME_GB"])
        ]


        df_ubt_temp_CancelledSalesComWithDate = (
            df_ubt_temp_CancelledSalesComWithDate.groupby(
                ["TICKETSERIALNUMBER_CBT", "TERDISPLAYID_CBT", "PRODID_CBT", "ACTUALDATE_GB"],
                as_index=False
            )
            .agg({
                "SALESCOMMAMOUNT_PBTLN": lambda x: x.fillna(0).sum(),
                "SALESFACTORAMOUNT_PBTLN": lambda x: x.fillna(0).sum()
            })
            .assign(
                SECONDSALESCOMMAMOUNT=0,
                SECONDSALESFACTORAMOUNT=0
            )
            .rename(columns={
                "TICKETSERIALNUMBER_CBT": "TICKETSERIALNUMBER",
                "TERDISPLAYID_CBT": "TERDISPLAYID",
                "PRODID_CBT": "PRODID",
                "ACTUALDATE_GB": "DT",
                "SALESCOMMAMOUNT_PBTLN": "SALESCOMMAMOUNT",
                "SALESFACTORAMOUNT_PBTLN": "SALESFACTORAMOUNT"
            })
        )[[
            "TICKETSERIALNUMBER", "TERDISPLAYID", "PRODID", "DT",
            "SALESCOMMAMOUNT", "SALESFACTORAMOUNT", "SECONDSALESCOMMAMOUNT", "SECONDSALESFACTORAMOUNT"
        ]]

        print(f"df_ubt_temp_CancelledSalesComWithDate : COMPLETED (Line 72 - 86) with {df_ubt_temp_CancelledSalesComWithDate.shape[0]} rows")
        return df_ubt_temp_CancelledSalesComWithDate

    # ========================================
    # 1. (ResultSalesComWithDate)
    # ========================================
    df_final_ResultSales = (
        df_ubt_temp_ResultSalesComWithDate()
        .groupby(["TERDISPLAYID", "PRODID", "DT", "TICKETSERIALNUMBER"], as_index=False)
        .agg({
            "SALESFACTORAMOUNT": "sum",
            "SECONDSALESFACTORAMOUNT": "sum"
        })
    )
    df_final_ResultSales["SALES"] = (
        df_final_ResultSales["SALESFACTORAMOUNT"].fillna(0)
        + df_final_ResultSales["SECONDSALESFACTORAMOUNT"].fillna(0)
    )
    df_final_ResultSales = (
        df_final_ResultSales.groupby(["TERDISPLAYID", "PRODID", "DT"], as_index=False)
        .agg(
            TOTALCOUNT=("TICKETSERIALNUMBER", lambda x: x.nunique()),
            AMOUNT=("SALES", "sum"))
        .rename(columns={"DT": "ACTUALDATE"})
    )

    # ========================================
    # 2. (CancelledSalesComWithDate)
    # ========================================
    df_final_CancelledSales = (
        df_ubt_temp_CancelledSalesComWithDate()
        .groupby(["TERDISPLAYID", "PRODID", "DT", "TICKETSERIALNUMBER"], as_index=False)
        .agg({
            "SALESFACTORAMOUNT": "sum",
            "SECONDSALESFACTORAMOUNT": "sum"
        })
    )

    df_final_CancelledSales["SALES"] = (
        df_final_CancelledSales["SALESFACTORAMOUNT"].fillna(0)
        + df_final_CancelledSales["SECONDSALESFACTORAMOUNT"].fillna(0)
    )

    df_final_CancelledSales = (
        df_final_CancelledSales.groupby(["TERDISPLAYID", "PRODID", "DT"], as_index=False)
        .agg(
            TOTALCOUNT=("TICKETSERIALNUMBER", lambda x: -x.nunique()),
            AMOUNT=("SALES", lambda x: -x.sum()))
            .rename(columns={"DT": "ACTUALDATE"})
    )

    # ========================================
    # 3. UNION ALL
    # ========================================
    df_union = pd.concat([df_final_ResultSales, df_final_CancelledSales], ignore_index=True)
    df_join = (
        df_union.merge(
            df_ztubt_product[["PRODID", "PRODNAME"]],
            on="PRODID",
            how="left"
        )
    )
    df_final = (
        df_join.groupby(["TERDISPLAYID", "PRODID", "PRODNAME", "ACTUALDATE"], as_index=False)
        .agg(
            TOTALCOUNT=("TOTALCOUNT", lambda x: x.fillna(0).sum()),
            AMOUNT=("AMOUNT", lambda x: x.fillna(0).sum())
        )
    )
    df_final["AMOUNT"] = (
        df_final["AMOUNT"].astype(float)
        .pipe(lambda s: np.trunc((s / 100) * 100) / 100)
        .round(2)
    )
    print(f"df_final : COMPLETED (Line 91 - 116) with {df_final.shape[0]} rows")
    return df_final

