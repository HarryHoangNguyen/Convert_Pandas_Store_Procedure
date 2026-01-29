import pandas as pd
import numpy as np
from sp_ubt_getcommonubtdates import *
from Snowflake_connection import snowflake_connection
import logging
logger = logging.getLogger(__name__)
connection = snowflake_connection()
schema = 'SPPL_DEV_DWH.SPPL_PUBLIC'

def sp_ubt_getamounttransaction(infromdate, intodate):
    df_final_result = pd.DataFrame()
    infromdate = pd.to_datetime(infromdate)
    intodate = pd.to_datetime(intodate)

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
    # =============================================================================
    # Extract data from all necessary tables
    # =============================================================================

    # ====== Extract data from ztubt_placedbettransactionheader ======
    # Select all data from all time needs to be extracted base on columns CREATEDDATE
    # Including BMCS, IGT, and OpenBet time
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER
        WHERE
        -- BMCS Time UTC
        (CREATEDDATE >= '{vutcfromdatetimebmcs}' AND CREATEDDATE < '{vutctodatetimebmcs}')
        OR
        -- IGT Time UTC
        (CREATEDDATE >= '{vutcfromdatetimeigt}' AND CREATEDDATE < '{vutctodatetimeigt}')
        OR
        -- OpenBet Time UTC
        (CREATEDDATE >= '{vutcfromdatetimeob}' AND CREATEDDATE < '{vutctodatetimeob}')
    """
    df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)
    # Convert CREATEDDATE to datetime
    df_ztubt_placedbettransactionheader['CREATEDDATE'] = pd.to_datetime(df_ztubt_placedbettransactionheader['CREATEDDATE'])


    # ======== Extract data from ztubt_horse_placedbettransactionlineitem ========
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_HORSE_PLACEDBETTRANSACTIONLINEITEM
    """
    df_ztubt_horse_placedbettransactionlineitem = pd.read_sql(query, connection)

    # ======== Extract data from ztubt_placedbettransactionheaderlifecyclestate ========
    # Filter base on BETSTATETYPEID in ('PB03','PB06')
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE
        WHERE BETSTATETYPEID IN ('PB03','PB06')
    """
    df_ztubt_placedbettransactionheaderlifecyclestate = pd.read_sql(query, connection)

    # ======== Extract data from ztubt_4d_placedbettransactionlineitemnumber ========
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEMNUMBER
    """
    df_ztubt_4d_placedbettransactionlineitemnumber = pd.read_sql(query, connection)

    # Extract data from ztubt_drawdates
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_DRAWDATES
    """
    df_ztubt_drawdates = pd.read_sql(query, connection)

    # Extract data from ztubt_toto_placedbettransactionlineitem
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM
    """
    df_ztubt_toto_placedbettransactionlineitem = pd.read_sql(query, connection)

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
    """
    df_ztubt_sweep_placedbettransactionlineitemnumber = pd.read_sql(query, connection)

    # Extract data from ztubt_sports_placedbettransactionlineitem
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_SPORTS_PLACEDBETTRANSACTIONLINEITEM
    """
    df_ztubt_sports_placedbettransactionlineitem = pd.read_sql(query, connection)

    # Extract ztubt_sports_placedbettransactionlineitemnumber
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_SPORTS_PLACEDBETTRANSACTIONLINEITEMNUMBER
    """
    df_ztubt_sports_placedbettransactionlineitemnumber = pd.read_sql(query, connection)

    # extract data from ztubt_validatedbetticket
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_VALIDATEDBETTICKET
    """
    df_ztubt_validatedbetticket = pd.read_sql(query, connection)

    # Extract data from ztubt_validatedbetticketlifecyclestate
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_VALIDATEDBETTICKETLIFECYCLESTATE
    """
    df_ztubt_validatedbetticketlifecyclestate = pd.read_sql(query, connection)

      # Extract data from ztubt_terminal
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_TERMINAL
    """
    df_ztubt_terminal = pd.read_sql(query, connection)

    # Extract data from ztubt_location
    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_LOCATION
    """
    df_ztubt_location = pd.read_sql(query, connection)


    # =============================================================================
    # Transformations
    # =============================================================================
    # TODO: df_ubt_temp_resultwager, temp_ubt_ResultSales

    def df_wager():
        # ======================================
        # TEMP_UBT_RESULTWAGER
        # ======================================
        df_wager = pd.DataFrame(columns=[
            "TKTSERIALNUMBER", "WAGER", "SECONDWAGER"])


        # -----------------------------
        # HR
        # -----------------------------
        df_hr = (
            df_ztubt_placedbettransactionheader[
                (df_ztubt_placedbettransactionheader["PRODID"] == 1) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimebmcs) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimebmcs)
            ]
            .merge(df_ztubt_horse_placedbettransactionlineitem, on="TRANHEADERID")
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate[
                    df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB06"
                ],
                on="TRANHEADERID"
            )
            .assign(
                BETPRICEAMOUNT=lambda d: d["BETPRICEAMOUNT"].fillna(0),
                WAGER=lambda d: np.where(d["BETTYPEID"] == "W-P",
                                        d["BETPRICEAMOUNT"] / 2,
                                        d["BETPRICEAMOUNT"]),
                SECONDWAGER=0
            )
            .groupby("TICKETSERIALNUMBER", as_index=False)
            .agg(
                WAGER=("WAGER", "sum"),
                SECONDWAGER=("SECONDWAGER", "sum")
            ).rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
        )[["TKTSERIALNUMBER","WAGER","SECONDWAGER"]]

        # -----------------------------
        # 4D
        # -----------------------------

        audit_cols = [col for col in df_ztubt_drawdates.columns
              if col.startswith('X_') or col in ['X_RECORD_INSERT_TS', 'X_RECORD_UPDATE_TS', 'X_ETL_NAME']]
        df_4d = (
            df_ztubt_placedbettransactionheader[
                (df_ztubt_placedbettransactionheader["PRODID"] == 2) &
                (df_ztubt_placedbettransactionheader["ISEXCHANGETICKET"] == False) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimeigt) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimeigt)
            ]
            .merge(df_ztubt_4d_placedbettransactionlineitemnumber, on="TRANHEADERID")
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate[
                    df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB06"
                ],
                on="TRANHEADERID"
            )
            .merge(df_ztubt_drawdates.drop(columns=audit_cols), on="TRANHEADERID")
            .assign(
                bigbetacceptedwager=lambda d: d["BIGBETACCEPTEDWAGER"].fillna(0),
                smallbetacceptedwager=lambda d: d["SMALLBETACCEPTEDWAGER"].fillna(0)
            )
            .groupby("TICKETSERIALNUMBER", as_index=False)
            .agg(
                WAGER=("BIGBETACCEPTEDWAGER", "sum"),
                SECONDWAGER=("SMALLBETACCEPTEDWAGER", "sum")
            ).rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
        )[["TKTSERIALNUMBER","WAGER","SECONDWAGER"]]

        # -----------------------------
        # TOTO
        # -----------------------------
        df_toto = (
            df_ztubt_placedbettransactionheader[
                (df_ztubt_placedbettransactionheader["PRODID"] == 3) &
                (df_ztubt_placedbettransactionheader["ISEXCHANGETICKET"] == False) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimeigt) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimeigt)
            ]
            .merge(df_ztubt_toto_placedbettransactionlineitem, on="TRANHEADERID")
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate[
                    df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB06"
                ],
                on="TRANHEADERID"
            )
            .assign(
                BETPRICEAMOUNT=lambda d: d["BETPRICEAMOUNT"].fillna(0),
                GROUPUNITSEQUENCE=lambda d: d["GROUPUNITSEQUENCE"].fillna(1)
            )
        )
        df_toto = df_toto[df_toto["GROUPUNITSEQUENCE"] == 1]
        df_toto = (
            df_toto.groupby("TICKETSERIALNUMBER", as_index=False)
            .agg(
                WAGER=("BETPRICEAMOUNT", "sum"),
                SECONDWAGER=("BETPRICEAMOUNT", lambda x: 0)
            ).rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
        )[["TKTSERIALNUMBER", "WAGER", "SECONDWAGER"]]


        # -----------------------------
        # SWEEP PB06
        # -----------------------------
        df_sweep_pb06 = (
            df_ztubt_placedbettransactionheader[
                (df_ztubt_placedbettransactionheader["PRODID"] == 4) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimeigt) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimeigt)
            ]
            .merge(df_ztubt_sweep_placedbettransactionlineitem, on="TRANHEADERID", how="inner", suffixes=('_PBTH','_PBTL'))
            .merge(df_ztubt_sweep_placedbettransactionlineitemnumber[df_ztubt_sweep_placedbettransactionlineitemnumber["ISSOLDOUT"] == False],
                on="TRANLINEITEMID", how="inner", suffixes=('','_PBTLN'))
            .merge(df_ztubt_placedbettransactionheaderlifecyclestate[df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"]=="PB06"],
                on="TRANHEADERID", how="inner", suffixes=('','_PBLC'))
        )

        df_temp = (df_ztubt_placedbettransactionheaderlifecyclestate[df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"]=="PB06"]
                .merge(df_ztubt_placedbettransactionheader[["TRANHEADERID","TERDISPLAYID"]], on="TRANHEADERID")
                .merge(df_ztubt_terminal[["TERDISPLAYID","LOCID"]], on="TERDISPLAYID")
                .merge(df_ztubt_location, on="LOCID")
        )
        df_temp = df_temp[(df_temp["SWEEPINDICATOR"].notna()) &
                        (df_temp["CAPTUREDDATE"] >= vutcfromdatetimeigt) &
                        (df_temp["CAPTUREDDATE"] < vutctodatetimeigt)]["TRANHEADERID"]

        df_sweep_pb06 = (df_sweep_pb06[~df_sweep_pb06["TRANHEADERID"].isin(df_temp) | df_sweep_pb06["ISPRINTED"].notna()]
                    .groupby("TICKETSERIALNUMBER", as_index=False)
                    .agg(WAGER=("BETPRICEAMOUNT", lambda x: x.fillna(0).sum()),
                        SECONDWAGER=("BETPRICEAMOUNT", lambda _: 0))
                    .rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
                    )[["TKTSERIALNUMBER", "WAGER", "SECONDWAGER"]]

        # -----------------------------
        # SWEEP PB03
        # -----------------------------
        df_sweep_pb03 = (
            df_ztubt_placedbettransactionheader[
                (df_ztubt_placedbettransactionheader["PRODID"] == 4) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimeigt) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimeigt)
            ]
            .merge(df_ztubt_sweep_placedbettransactionlineitem, on="TRANHEADERID", how="inner", suffixes=("", "_PBTL"))
            .merge(df_ztubt_sweep_placedbettransactionlineitemnumber[df_ztubt_sweep_placedbettransactionlineitemnumber["ISSOLDOUT"] == False], on="TRANLINEITEMID", how="inner", suffixes=("", "_PBTLN"))
            .merge(df_ztubt_placedbettransactionheaderlifecyclestate[df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB03"], on="TRANHEADERID", how="inner", suffixes=("", "_PBLC"))
            .merge(df_ztubt_terminal, on="TERDISPLAYID", how="inner", suffixes=("", "_ZT"))
            .merge(df_ztubt_location[df_ztubt_location["SWEEPINDICATOR"].notnull()], on="LOCID", how="inner", suffixes=("", "_ZL"))
        )

        df_sweep_pb03 = (
            df_sweep_pb03[df_sweep_pb03["ISPRINTED"].isnull()]
            .groupby("TICKETSERIALNUMBER", as_index=False)
            .agg(WAGER=("BETPRICEAMOUNT", lambda x: x.fillna(0).sum()))
            .assign(SECONDWAGER=0)
            .rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
        )[["TKTSERIALNUMBER", "WAGER", "SECONDWAGER"]]


        # -----------------------------
        # SPORTS
        # -----------------------------
        df_sports = (
            df_ztubt_placedbettransactionheader[
                df_ztubt_placedbettransactionheader["PRODID"].isin([5, 6]) &
                (df_ztubt_placedbettransactionheader["ISBETREJECTEDBYTRADER"] == False) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimeob) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimeob)
            ]
            .merge(df_ztubt_sports_placedbettransactionlineitem, on="TRANHEADERID", how="inner", suffixes=("", "_SPTL"))
            .merge(df_ztubt_sports_placedbettransactionlineitemnumber, on="TRANLINEITEMID", how="inner", suffixes=("", "_SPTLN"))
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate[df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB06"],
                on="TRANHEADERID",
                how="inner",
                suffixes=("", "_PBLC")
            )[["TICKETSERIALNUMBER", "BETAMOUNT"]]
            .drop_duplicates()
            .groupby("TICKETSERIALNUMBER", as_index=False)
            .agg(WAGER=("BETAMOUNT", lambda x: x.fillna(0).sum()))
            .assign(SECONDWAGER=0)
            .rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
        )[["TKTSERIALNUMBER","WAGER","SECONDWAGER"]]

        df_wager = pd.concat([df_wager, df_hr, df_4d, df_toto, df_sweep_pb06, df_sweep_pb03, df_sports], ignore_index=True)
        return df_wager


    def df_resultsales():
        # ======================================
        # temp_ubt_ResultSales
        # ======================================
        df_resultsales = pd.DataFrame({
            "TKTSERIALNUMBER": pd.Series(dtype="string"),
            "SALESCOMMAMOUNT": pd.Series(dtype="float"),
            "SALESFACTORAMOUNT": pd.Series(dtype="float"),
            "SECONDSALESCOMMAMOUNT": pd.Series(dtype="float"),
            "SECONDSALESFACTORAMOUNT": pd.Series(dtype="float")
        })

        # -----------------------------
        # HR
        # -----------------------------
        df_hr = (
            df_ztubt_placedbettransactionheader[
                (df_ztubt_placedbettransactionheader["PRODID"] == 1) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimebmcs) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimebmcs)
            ]
            .merge(df_ztubt_horse_placedbettransactionlineitem, on="TRANHEADERID", how="inner")
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate[df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB06"],
                on="TRANHEADERID",
                how="inner"
            )
        )
        # Convert CREATEDDATE to datetime before using .dt accessor
        df_hr["CREATEDDATE"] = pd.to_datetime(df_hr["CREATEDDATE"])
        df_hr["CREATEDDATE"] = (df_hr["CREATEDDATE"] + pd.Timedelta(hours=8)).dt.date
        df_hr["SC"] = np.where(df_hr["BETTYPEID"] == "W-P", df_hr["SALESCOMMAMOUNT"].fillna(0)/2, df_hr["SALESCOMMAMOUNT"].fillna(0))
        df_hr["SF"] = np.where(df_hr["BETTYPEID"] == "W-P", df_hr["SALESFACTORAMOUNT"].fillna(0)/2, df_hr["SALESFACTORAMOUNT"].fillna(0))
        df_hr = (
            df_hr.groupby(["TICKETSERIALNUMBER", "TRANHEADERID", "CREATEDDATE"], as_index=False)[["SC", "SF"]].sum()
                .groupby("TICKETSERIALNUMBER", as_index=False)[["SC", "SF"]].sum()
                .rename(columns={"SC": "SALESCOMMAMOUNT", "SF": "SALESFACTORAMOUNT", "TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
                .assign(SECONDSALESCOMMAMOUNT=0, SECONDSALESFACTORAMOUNT=0)
        )[["TKTSERIALNUMBER", "SALESCOMMAMOUNT", "SECONDSALESCOMMAMOUNT", "SALESFACTORAMOUNT", "SECONDSALESFACTORAMOUNT"]]


        # -----------------------------
        # 4D
        # -----------------------------
        # drop audit col from df_ztubt_drawdates to avoid duplication issue
        audit_cols = [col for col in df_ztubt_drawdates.columns
              if col.startswith('X_') or col in ['X_RECORD_INSERT_TS', 'X_RECORD_UPDATE_TS', 'X_ETL_NAME']]
        df_4d = (
            df_ztubt_placedbettransactionheader[
                (df_ztubt_placedbettransactionheader["PRODID"] == 2) &
                (df_ztubt_placedbettransactionheader["ISEXCHANGETICKET"] == False) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimeigt) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimeigt)
            ]
            .merge(df_ztubt_4d_placedbettransactionlineitemnumber, on="TRANHEADERID", how="inner")
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate[
                    df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB06"
                ],
                on="TRANHEADERID",
                how="inner"
            )
            .merge(df_ztubt_drawdates.drop(columns=audit_cols), on="TRANHEADERID", how="inner")
        )
        # Convert CREATEDDATE to datetime before using .dt accessor
        df_4d["CREATEDDATE"] = pd.to_datetime(df_4d["CREATEDDATE"])
        df_4d["CREATEDDATE"] = (df_4d["CREATEDDATE"] + pd.Timedelta(hours=8)).dt.date
        df_4d = (
            df_4d.groupby(["TICKETSERIALNUMBER", "CREATEDDATE"], as_index=False)
                .agg(
                    SALESCOMMAMOUNT=("SALESCOMMAMOUNTBIG", lambda x: x.fillna(0).sum()),
                    SALESFACTORAMOUNT=("SALESFACTORAMOUNTBIG", lambda x: x.fillna(0).sum()),
                    SECONDSALESCOMMAMOUNT=("SALESCOMMAMOUNTSMALL", lambda x: x.fillna(0).sum()),
                    SECONDSALESFACTORAMOUNT=("SALESFACTORAMOUNTSMALL", lambda x: x.fillna(0).sum())
                )
        )
        df_4d = (
            df_4d.groupby("TICKETSERIALNUMBER", as_index=False)
                    .agg(
                        SALESCOMMAMOUNT=("SALESCOMMAMOUNT", "sum"),
                        SALESFACTORAMOUNT=("SALESFACTORAMOUNT", "sum"),
                        SECONDSALESCOMMAMOUNT=("SECONDSALESCOMMAMOUNT", "sum"),
                        SECONDSALESFACTORAMOUNT=("SECONDSALESFACTORAMOUNT", "sum"))
                    .rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
        )[["TKTSERIALNUMBER", "SALESCOMMAMOUNT", "SECONDSALESCOMMAMOUNT", "SALESFACTORAMOUNT", "SECONDSALESFACTORAMOUNT"]]


        # -----------------------------
        # TOTO
        # -----------------------------
        df_toto = df_ztubt_placedbettransactionheader[
            (df_ztubt_placedbettransactionheader["PRODID"] == 3) &
            (df_ztubt_placedbettransactionheader["ISEXCHANGETICKET"] == False) &
            (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimeigt) &
            (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimeigt)
        ]
        df_toto_line = df_ztubt_toto_placedbettransactionlineitem.copy()
        df_toto_line["GROUPUNITSEQUENCE"] = df_toto_line["GROUPUNITSEQUENCE"].fillna(1)
        df_toto_line = df_toto_line[df_toto_line["GROUPUNITSEQUENCE"] == 1]
        df_toto = (
            df_toto
            .merge(df_toto_line, on="TRANHEADERID", how="inner", suffixes=("", "_PBTL"))
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate[
                    df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB06"
                ],
                on="TRANHEADERID",
                how="inner",
                suffixes=("", "_PBLC")
            )
        )
        # Convert CREATEDDATE to datetime before using .dt accessor
        df_toto["CREATEDDATE"] = pd.to_datetime(df_toto["CREATEDDATE"])
        df_toto["CREATEDDATE_LOCAL"] = (df_toto["CREATEDDATE"] + pd.Timedelta(hours=8)).dt.date

        df_toto = (
            df_toto.groupby(["TICKETSERIALNUMBER", "CREATEDDATE_LOCAL"], as_index=False)
            .agg(
                SALESCOMMAMOUNT=("SALESCOMMAMOUNT", lambda x: x.fillna(0).sum()),
                SALESFACTORAMOUNT=("SALESFACTORAMOUNT", lambda x: x.fillna(0).sum())
            )
        )
        df_toto = (
            df_toto.groupby("TICKETSERIALNUMBER", as_index=False)
            .agg(
                SALESCOMMAMOUNT=("SALESCOMMAMOUNT", "sum"),
                SALESFACTORAMOUNT=("SALESFACTORAMOUNT", "sum")
            )
            .assign(SECONDSALESCOMMAMOUNT=0, SECONDSALESFACTORAMOUNT=0)
            .rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
        )[["TKTSERIALNUMBER", "SALESCOMMAMOUNT", "SECONDSALESCOMMAMOUNT", "SALESFACTORAMOUNT", "SECONDSALESFACTORAMOUNT"]]

        # -----------------------------
        # SWEEP
        # -----------------------------

        df_sweep = (
            df_ztubt_placedbettransactionheader[
                (df_ztubt_placedbettransactionheader["PRODID"] == 4) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimeigt) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimeigt)
            ]
            .merge(df_ztubt_sweep_placedbettransactionlineitem, on="TRANHEADERID", how="inner", suffixes=("", "_PBTL"))
            .merge(
                df_ztubt_sweep_placedbettransactionlineitemnumber[
                    df_ztubt_sweep_placedbettransactionlineitemnumber["ISSOLDOUT"] == False
                ],
                on="TRANLINEITEMID",
                how="inner",
                suffixes=("", "_PBTLN")
            )
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate[
                    df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB06"
                ],
                on="TRANHEADERID",
                how="inner",
                suffixes=("", "_PBLC")
            )
        )
        # Convert CREATEDDATE to datetime before using .dt accessor
        df_sweep["CREATEDDATE"] = pd.to_datetime(df_sweep["CREATEDDATE"])
        df_sweep["CREATEDDATE_LOCAL"] = (df_sweep["CREATEDDATE"] + pd.Timedelta(hours=8)).dt.date

        df_sweep = (
            df_sweep.groupby(["TICKETSERIALNUMBER", "CREATEDDATE_LOCAL"], as_index=False)
            .agg(
                SALESCOMMAMOUNT=("SALESCOMMAMOUNT", lambda x: x.fillna(0).sum()),
                SALESFACTORAMOUNT=("SALESFACTORAMOUNT", lambda x: x.fillna(0).sum())
            )
        )
        df_sweep = (
            df_sweep.groupby("TICKETSERIALNUMBER", as_index=False)
            .agg(
                SALESCOMMAMOUNT=("SALESCOMMAMOUNT", "sum"),
                SALESFACTORAMOUNT=("SALESFACTORAMOUNT", "sum"))
            .assign(SECONDSALESCOMMAMOUNT=0, SECONDSALESFACTORAMOUNT=0)
            .rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
        )[["TKTSERIALNUMBER", "SALESCOMMAMOUNT", "SECONDSALESCOMMAMOUNT", "SALESFACTORAMOUNT", "SECONDSALESFACTORAMOUNT"]]

        # -----------------------------
        # SPORTS
        # -----------------------------

        df_sports = (
            df_ztubt_placedbettransactionheader[
                df_ztubt_placedbettransactionheader["PRODID"].isin([5,6]) &
                (df_ztubt_placedbettransactionheader["ISBETREJECTEDBYTRADER"] == False) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] >= vutcfromdatetimeob) &
                (df_ztubt_placedbettransactionheader["CREATEDDATE"] < vutctodatetimeob)
            ]
            .merge(df_ztubt_sports_placedbettransactionlineitem, on="TRANHEADERID", how="inner", suffixes=("", "_SPL"))
            .merge(df_ztubt_sports_placedbettransactionlineitemnumber, on="TRANLINEITEMID", how="inner", suffixes=("", "_SPLN"))
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate[
                    df_ztubt_placedbettransactionheaderlifecyclestate["BETSTATETYPEID"] == "PB06"
                ],
                on="TRANHEADERID",
                how="inner",
                suffixes=("", "_LC")
            )
        )
        df_sports["CREATEDDATE_LOCAL"] = (df_sports["CREATEDDATE"] + pd.Timedelta(hours=8)).dt.date
        df_sports = df_sports[[
            "TICKETSERIALNUMBER","PRODID","TRANHEADERID","PLACEDBETLIFECYCLEID",
            "SOURCESYSTEMTRANSACTIONID","BETSTATETYPEID","ISSINGLEBET",
            "CREATEDDATE_LOCAL","SALESCOMMAMOUNT","SALESFACTORAMOUNT"
        ]].drop_duplicates()
        df_sports = (
            df_sports.groupby("TICKETSERIALNUMBER", as_index=False)
            .agg(
                SALESCOMMAMOUNT=("SALESCOMMAMOUNT", lambda x: x.fillna(0).sum()),
                SALESFACTORAMOUNT=("SALESFACTORAMOUNT", lambda x: x.fillna(0).sum()))
            .assign(SECONDSALESCOMMAMOUNT=0, SECONDSALESFACTORAMOUNT=0)
            .rename(columns={"TICKETSERIALNUMBER": "TKTSERIALNUMBER"})
        )[["TKTSERIALNUMBER", "SALESCOMMAMOUNT", "SECONDSALESCOMMAMOUNT", "SALESFACTORAMOUNT", "SECONDSALESFACTORAMOUNT"]]

        df_resultsales = pd.concat([df_resultsales, df_hr, df_4d, df_toto, df_sweep, df_sports], ignore_index=True)
        return df_resultsales


    df_final_result = df_wager().merge(
        df_resultsales(),
        on="TKTSERIALNUMBER",
        how="left",
        # suffixes=("", "_S")
    ).merge(
        df_ztubt_validatedbetticket[df_ztubt_validatedbetticket["WINNINGAMOUNT"].notna()],
        left_on="TKTSERIALNUMBER",
        right_on="TICKETSERIALNUMBER",
        how="left",
        # suffixes=("", "_ZV")
        ).merge(
            df_ztubt_validatedbetticketlifecyclestate[
                df_ztubt_validatedbetticketlifecyclestate["BETSTATETYPEID"] == "VB06"
            ],
            on="TRANHEADERID",
            how="left",
        )

    df_final_result = df_final_result.groupby("TKTSERIALNUMBER", as_index=False).agg(
        WAGER=("WAGER", "sum"),
        SECONDWAGER=("SECONDWAGER", "sum"),
        SALES=("SALESFACTORAMOUNT", "sum"),
        SECONDSALES=("SECONDSALESFACTORAMOUNT", "sum"),
        SALESCOMM=("SALESCOMMAMOUNT", "sum"),
        SECONDSALESCOMM=("SECONDSALESCOMMAMOUNT", "sum"),
        WINNINGAMOUNT=("WINNINGAMOUNT", lambda x: x.fillna(0).sum())
    )

    df_final_result["GST"] = df_final_result["WAGER"] - df_final_result["SALES"]
    df_final_result["SECONDGST"] = df_final_result["SECONDWAGER"] - df_final_result["SECONDSALES"]
    df_final_result["RETURNAMOUNT"] = df_final_result["WAGER"] + df_final_result["SECONDWAGER"] + df_final_result["WINNINGAMOUNT"]

    df_final_result = df_final_result[[
        "TKTSERIALNUMBER", "WAGER", "SECONDWAGER", "SALES", "SECONDSALES",
        "SALESCOMM", "SECONDSALESCOMM", "GST", "SECONDGST",
        "RETURNAMOUNT", "WINNINGAMOUNT"
    ]]

    return df_final_result