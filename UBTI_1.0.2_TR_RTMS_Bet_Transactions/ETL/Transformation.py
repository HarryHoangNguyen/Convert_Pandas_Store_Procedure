import os
import pandas as pd
import numpy as np
from Utilities.Snowflake_connection import snowflake_connection
connection = snowflake_connection()
schema = "SPPL_DEV_DWH.SPPL_PUBLIC"
import logging

logger = logging.getLogger(__name__)

# ====================== Extract Data & Transformation from common table ======================
# Get ZTUBT_TERMINAL and ZTUBT_LOCATION data and merge them
query = f"""
    select * from {schema}.ZTUBT_TERMINAL
"""
df_ztubt_terminal = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_ztubt_terminal.columns:
    df_ztubt_terminal['TERDISPLAYID'] = df_ztubt_terminal['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
query = f"""
    select * from {schema}.ZTUBT_LOCATION
"""
df_ztubt_location = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_ztubt_location.columns:
    df_ztubt_location['TERDISPLAYID'] = df_ztubt_location['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
df_ztubt_terminal_location = pd.merge(df_ztubt_terminal, df_ztubt_location,
                                      on='LOCID', how='left')
if 'TERDISPLAYID' in df_ztubt_terminal_location.columns:
    df_ztubt_terminal_location['TERDISPLAYID'] = df_ztubt_terminal_location['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
# Get ZTUBT_DRAWDATES data
query = f"""
    select * from {schema}.ZTUBT_DRAWDATES
"""
df_ztubt_drawdates = pd.read_sql(query, connection)


## Extract from ZTUBT_HORSE_PLACEDBETTRANSACTIONLINEITEM
query = f"""
SELECT *
FROM {schema}.ZTUBT_HORSE_PLACEDBETTRANSACTIONLINEITEM
"""
df_horse_placedbettransactionlineitem = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_horse_placedbettransactionlineitem.columns:
    df_horse_placedbettransactionlineitem['TERDISPLAYID'] = df_horse_placedbettransactionlineitem['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
## Extract from ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEMNUMBER
query = f"""
SELECT *
FROM {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEMNUMBER
"""
df_toto_placedbettransactionlineitemnumber = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_toto_placedbettransactionlineitemnumber.columns:
    df_toto_placedbettransactionlineitemnumber['TERDISPLAYID'] = df_toto_placedbettransactionlineitemnumber['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
## Extract from ZTUBT_SPORTS_PLACEDBETTRANSACTIONLINEITEMNUMBER
query = f"""
SELECT *
FROM {schema}.ZTUBT_SPORTS_PLACEDBETTRANSACTIONLINEITEMNUMBER
"""
df_sports_placedbettransactionlineitemnumber = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_sports_placedbettransactionlineitemnumber.columns:
    df_sports_placedbettransactionlineitemnumber['TERDISPLAYID'] = df_sports_placedbettransactionlineitemnumber['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
## Extract from ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEM
query = f"""
SELECT *
FROM {schema}.ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEM
"""
df_sweep_placedbettransactionlineitem = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_sweep_placedbettransactionlineitem.columns:
    df_sweep_placedbettransactionlineitem['TERDISPLAYID'] = df_sweep_placedbettransactionlineitem['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
## Extract from ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER
query = f"""
SELECT *
FROM {schema}.ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER
   """
df_sweep_placedbettransactionlineitemnumber = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_sweep_placedbettransactionlineitemnumber.columns:
    df_sweep_placedbettransactionlineitemnumber['TERDISPLAYID'] = df_sweep_placedbettransactionlineitemnumber['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))

## Extract from ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEM
query = f"""
SELECT *
FROM {schema}.ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEM
"""
df_4d_placedbettransactionlineitem = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_4d_placedbettransactionlineitem.columns:
    df_4d_placedbettransactionlineitem['TERDISPLAYID'] = df_4d_placedbettransactionlineitem['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
## Extract from ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEMNUMBER
query = f"""
SELECT *
FROM {schema}.ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEMNUMBER
   """
df_4d_placedbettransactionlineitemnumber = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_4d_placedbettransactionlineitemnumber.columns:
    df_4d_placedbettransactionlineitemnumber['TERDISPLAYID'] = df_4d_placedbettransactionlineitemnumber['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))

## Extract from ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER
query = f"""
SELECT *
FROM {schema}.ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER
"""
df_sweep_placedbettransactionlineitemnumber = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_sweep_placedbettransactionlineitemnumber.columns:
    df_sweep_placedbettransactionlineitemnumber['TERDISPLAYID'] = df_sweep_placedbettransactionlineitemnumber['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
## Extract from ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM
query = f"""
SELECT *
FROM {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM
"""
df_toto_placedbettransactionlineitem = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_toto_placedbettransactionlineitem.columns:
    df_toto_placedbettransactionlineitem['TERDISPLAYID'] = df_toto_placedbettransactionlineitem['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))

query = f"""
SELECT *
FROM {schema}.ZTUBT_SPORTS_PLACEDBETTRANSACTIONLINEITEM
"""
df_sports_placedbettransactionlineitem = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_sports_placedbettransactionlineitem.columns:
    df_sports_placedbettransactionlineitem['TERDISPLAYID'] = df_sports_placedbettransactionlineitem['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
query = f"""
SELECT *
FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE
"""
df_placedbettransactionheaderlifecyclestate = pd.read_sql(query, connection)
if 'TERDISPLAYID' in df_placedbettransactionheaderlifecyclestate.columns:
    df_placedbettransactionheaderlifecyclestate['TERDISPLAYID'] = df_placedbettransactionheaderlifecyclestate['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))
def lpad_12(value):
    if pd.isna(value):
        return '000000000000'
    return str(value).zfill(12)


def ubt_temp_placebettransaction(stardate, enddate, startdateutc, enddateUTC):

#     create temporary table if not exists ubt_temp_placebettransaction(
#   TicketSerialNumber VARCHAR(200),
#   TranHeaderID varchar(50),
#   EntryMethodID VARCHAR(10),
#   DeviceID VARCHAR(100),
#   ProdID INT4,
#   IsBetRejectedByTrader bool,
#   IsExchangeTicket bool,
#   TerDisplayID  varchar(100),
#   CreatedDate timestamp,
#   RequestID varchar(40),
#   UserDisplayID varchar(40),
#   CartID varchar(50),
#   TransactionType int4
#  );A

    df_ubt_temp_placebettransaction = pd.DataFrame({
        'TICKETSERIALNUMBER': pd.Series(dtype='str'),
        'TRANHEADERID': pd.Series(dtype='str'),
        'ENTRYMETHODID': pd.Series(dtype='str'),
        'DEVICEID': pd.Series(dtype='str'),
        'PRODID': pd.Series(dtype='int'),
        'ISBETREJECTEDBYTRADER': pd.Series(dtype='bool'),
        'ISEXCHANGETICKET': pd.Series(dtype='bool'),
        'TERDISPLAYID': pd.Series(dtype='str'),
        'CREATEDDATE': pd.Series(dtype='datetime64[ns]'),
        'REQUESTID': pd.Series(dtype='str'),
        'USERDISPLAYID': pd.Series(dtype='str'),
        'CARTID': pd.Series(dtype='str'),
        'TRANSACTIONTYPE': pd.Series(dtype='int')
    })

    startdate = pd.to_datetime(stardate)
    enddate = pd.to_datetime(enddate)
    startdateutc = pd.to_datetime(startdateutc)
    enddateUTC = pd.to_datetime(enddateUTC)
    query= f"""
        SELECT TicketSerialNumber, PB.TranHeaderID, EntryMethodID, DeviceID, ProdID, IsBetRejectedByTrader,IsExchangeTicket
            ,TerDisplayID,PB.CreatedDate,RequestID,UserDisplayID,CartID, 1  TransactionType
        FROM {schema}.ztubt_placedbettransactionheader PB
            INNER JOIN {schema}.ztubt_placedbettransactionheaderlifecyclestate  PBLC ON PB.TranHeaderID = PBLC.TranHeaderID AND PBLC.BetStateTypeID = 'PB06'
        WHERE
	        (PB.CreatedDate >= '{startdateutc}' And PB.CreatedDate <= '{enddateUTC}')

    UNION
        SELECT VB.TicketSerialNumber, VB.TranHeaderID, PB.EntryMethodID, PB.DeviceID, PB.ProdID, PB.IsBetRejectedByTrader
            ,PB.IsExchangeTicket,PB.TerDisplayID,PB.CreatedDate,PB.RequestID,VB.UserDisplayID,VB.CartID, 3 TransactionType
        FROM {schema}.ztubt_validatedbetticket  VB
            INNER JOIN {schema}.ztubt_placedbettransactionheader PB ON VB.TicketSerialNumber = PB.TicketSerialNumber
            INNER JOIN {schema}.ztubt_validatedbetticketlifecyclestate VBLC ON VB.TranHeaderID = VBLC.TranHeaderID AND VBLC.BetStateTypeID = 'VB06'
        WHERE VB.ValidationTypeID IN ('VALD', 'RFND')
        AND   (VB.CreatedValidationDate >= '{startdateutc}' And VB.CreatedValidationDate <= '{enddateUTC}')
    UNION
        SELECT CB.TicketSerialNumber, CB.TranHeaderID, PB.EntryMethodID, PB.DeviceID, PB.ProdID, PB.IsBetRejectedByTrader,
            PB.IsExchangeTicket,PB.TerDisplayID,PB.CreatedDate,PB.RequestID,CB.UserDisplayID,CB.CartID,5 TransactionType
        FROM {schema}.ztubt_CancelledBetTicket CB
            INNER JOIN {schema}.ztubt_placedbettransactionheader PB ON CB.TicketSerialNumber = PB.TicketSerialNumber
            INNER JOIN {schema}.ztubt_cancelledbetticketlifecyclestate  CBLC ON CBLC.TranHeaderID = PB.TranHeaderID AND CBLC.BetStateTypeID = 'CB06'
        WHERE (PB.IsBetRejectedByTrader ) = FALSE AND (CB.CancelledDate >= '{startdate}' And CB.CancelledDate <= '{enddate}')
    """
    df_ubt_temp_placebettransaction = pd.read_sql(query, connection)
    df_ubt_temp_placebettransaction['TERDISPLAYID'] = df_ubt_temp_placebettransaction['TERDISPLAYID'].apply(lambda x: str(x).zfill(9))

    # format CREATEDDATE to string '2025-09-23 06:07:20.281' lấy 3 số phía sau giây nếu 3 số phía sau giây là .000 thì bỏ đi
    df_ubt_temp_placebettransaction['CREATEDDATE'] = pd.to_datetime(df_ubt_temp_placebettransaction['CREATEDDATE'])
    df_ubt_temp_placebettransaction['CREATEDDATE'] = df_ubt_temp_placebettransaction['CREATEDDATE'].apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if x.microsecond != 0 else x.strftime('%Y-%m-%d %H:%M:%S')
    )
    return df_ubt_temp_placebettransaction

def ubt_temp_trans(df_ubt_temp_placebettransaction, startdate, enddate, startdateutc, enddateUTC):
    # Format date inputs
    startdate = pd.to_datetime(startdate)
    enddate = pd.to_datetime(enddate)
    startdateutc = pd.to_datetime(startdateutc)
    enddateUTC = pd.to_datetime(enddateUTC)
    df_ubt_temp_trans = pd.DataFrame()
    # ================================= EXTRACT DATA FROM TABLES =================================

    # GET ZTUBT_VALIDATEDBETTICKET DATA
    query = f"""
    select * from {schema}.ZTUBT_VALIDATEDBETTICKET
    WHERE (CreatedValidationDate >= '{startdateutc}' And CreatedValidationDate <= '{enddateUTC}')
    """
    df_ztubt_validatedbetticket = pd.read_sql(query, connection)

    # Get ZTUBT_CANCELLEDBETTICKET DATA
    query = f"""
    select * from {schema}.ZTUBT_CANCELLEDBETTICKET
    WHERE (CANCELLEDDATE >= '{startdate}' And CANCELLEDDATE <= '{enddate}')
    """
    df_ztubt_cancelledbetticket = pd.read_sql(query, connection)


    # ================================= TRANSFORMATION LOGIC =================================

    # --------------- 1.1: Horse Racing  from UBT_TEMP_PLACEBETTRANSACTION ONLY ---------------

    df_temp = pd.merge(
        df_ubt_temp_placebettransaction,
        df_ztubt_terminal_location[['TERDISPLAYID', 'LOCID', 'LOCDISPLAYID']],
        how='inner',
        on='TERDISPLAYID'
    )
    df_temp['CREATEDDATE'] = pd.to_datetime(df_temp['CREATEDDATE'], format='mixed')
    df_temp = df_temp[
        (df_temp['PRODID'].isin([1,2,3,4,5,6])) &
        (df_temp['TRANSACTIONTYPE'] == 1) &
        ((df_temp['CREATEDDATE'] >= startdateutc) &
        (df_temp['CREATEDDATE'] <= enddateUTC))
    ]

    df_temp['CREATEDDATE'] = df_temp['CREATEDDATE'] + pd.Timedelta(hours=8)
    df_temp = df_temp.rename(columns={
        'ENTRYMETHODID': 'ENTRYMETHODID',
        'PRODID': 'PRODID',
        'ISBETREJECTEDBYTRADER': 'ISBETREJECTEDBYTRADER',
        'LOCID': 'LOCID',
        'TICKETSERIALNUMBER': 'TICKETSERIALNUMBER',
        'REQUESTID': 'UUID',
        'CREATEDDATE': 'TRANSACTIONDATE',
        'TERDISPLAYID': 'TERMINALID',
        'USERDISPLAYID': 'USERID',
        'LOCDISPLAYID': 'LOCATIONID',
        'CARTID': 'CARTID',
        'TRANHEADERID': 'TRANHEADERID'
    })
    df_temp = df_temp.assign(TRANSACTIONTYPE = 1)[
        ['ENTRYMETHODID',
       'PRODID',
       'ISBETREJECTEDBYTRADER',
       'TICKETSERIALNUMBER',
       'UUID',
       'TRANSACTIONTYPE',
       'TRANSACTIONDATE',
       'TERMINALID',
       'USERID',
       'LOCATIONID',
       'CARTID',
       'TRANHEADERID']
    ]
    df_ubt_temp_trans = pd.concat([df_ubt_temp_trans, df_temp], ignore_index=True)
    logger.info(f"UBT_TEMP_TRANS : COMPLETED 1.1 - Line 918 - 934 with {df_temp.shape[0]} rows")
    print(f"UBT_TEMP_TRANS : COMPLETED 1.1 - Line 918 - 934 with {df_temp.shape[0]} rows")


    # ================================== 1.2: HORSE RACING===================================
    df_temp = pd.DataFrame()
    df_temp = df_ztubt_validatedbetticket\
            .merge(
                df_ubt_temp_placebettransaction,
                on='TICKETSERIALNUMBER',
                how='inner',
                suffixes=('_zv', '_PB'))\
            .merge(
                df_ztubt_terminal_location[['TERDISPLAYID', 'LOCID', 'LOCDISPLAYID']],
                left_on='TERDISPLAYID_PB',
                right_on='TERDISPLAYID',
                how='left')
    condittion = (
        (df_temp['PRODID_PB'] == 1) &
        (df_temp['TRANSACTIONTYPE'] == 3) &
        (df_temp['CREATEDVALIDATIONDATE'] >= startdateutc) &
        (df_temp['CREATEDVALIDATIONDATE'] <= enddateUTC) &
        ((df_temp['WINNINGAMOUNT'].fillna(0) > 0) | (df_temp['REBATERECLAIM'].fillna(0) > 0))
    )
    # Rename columns to standardize naming
    column_mapping = {
        'PRODID_PB': 'PRODID',
        'REQUESTID_PB': 'UUID',
        'USERDISPLAYID_PB': 'USERID',
        'CARTID_PB': 'CARTID',
        'TRANHEADERID_PB': 'TRANHEADERID',
        'CREATEDVALIDATIONDATE': 'TRANSACTIONDATE',
        'TERDISPLAYID': 'TERMINALID',
        'LOCDISPLAYID': 'LOCATIONID'
    }
    df_temp = df_temp[condittion].assign(TRANSACTIONTYPE = 3).rename(columns=column_mapping).reset_index(drop=True)[[
        'ENTRYMETHODID',
       'PRODID',
       'ISBETREJECTEDBYTRADER',
       'TICKETSERIALNUMBER',
       'UUID',
       'TRANSACTIONTYPE',
       'TRANSACTIONDATE',
       'TERMINALID',
       'USERID',
       'LOCATIONID',
       'CARTID',
       'TRANHEADERID'
    ]]
    df_ubt_temp_trans = pd.concat([df_temp, df_ubt_temp_trans], ignore_index=True)
    print(f"UBT_TEMP_TRANS : COMPLETED 1.2 - Line 947 - 967 with {df_temp.shape[0]} rows")
    logger.info(f"UBT_TEMP_TRANS : COMPLETED 1.2 - Line 947 - 967 with {df_temp.shape[0]} rows")

    # ================================== 1.3 LOTTERY ID & SPORTS BETTING===================================
    df_temp = pd.DataFrame()
    df_temp = df_ztubt_validatedbetticket\
            .merge( df_ubt_temp_placebettransaction,
                    on='TICKETSERIALNUMBER',
                    how='inner',
                    suffixes=('_zv', '_PB'))\
            .merge(
                df_ztubt_terminal_location[['TERDISPLAYID', 'LOCID', 'LOCDISPLAYID']],
                left_on='TERDISPLAYID_PB',
                right_on='TERDISPLAYID',
                how='left')
    condittion = (
        (df_temp['PRODID_PB'].isin([2,3,4,5,6])) &
        (df_temp['TRANSACTIONTYPE'] == 3) &
        (df_temp['CREATEDVALIDATIONDATE'] >= startdateutc) &
        (df_temp['CREATEDVALIDATIONDATE'] <= enddateUTC) &
        ((df_temp['WINNINGAMOUNT'].fillna(0) > 0)) &
        (df_temp['VALIDATIONTYPEID'] == 'VALD')
    )
    # Rename columns to standardize naming
    column_mapping = {
        'PRODID_PB': 'PRODID',
        'REQUESTID_PB': 'UUID',
        'USERDISPLAYID_PB': 'USERID',
        'CARTID_PB': 'CARTID',
        'TRANHEADERID_PB': 'TRANHEADERID',
        'CREATEDVALIDATIONDATE': 'TRANSACTIONDATE',
        'TERDISPLAYID': 'TERMINALID',
        'LOCDISPLAYID': 'LOCATIONID'
    }
    df_temp = df_temp[condittion].assign(TRANSACTIONTYPE = 3).rename(columns=column_mapping).reset_index(drop=True)[[
        'ENTRYMETHODID',
       'PRODID',
       'ISBETREJECTEDBYTRADER',
       'TICKETSERIALNUMBER',
       'UUID',
       'TRANSACTIONTYPE',
       'TRANSACTIONDATE',
       'TERMINALID',
       'USERID',
       'LOCATIONID',
       'CARTID',
       'TRANHEADERID'
    ]]
    df_ubt_temp_trans = pd.concat([df_ubt_temp_trans, df_temp], ignore_index=True)
    print(f"UBT_TEMP_TRANS : COMPLETED 1.3 (Line 973- 1017) with {df_temp.shape[0]} rows")
    logger.info(f"UBT_TEMP_TRANS : COMPLETED 1.3 (Line 973- 1017) with {df_temp.shape[0]} rows")


    # ================================== 1.4 REFUND===================================
    df_temp = pd.DataFrame()
    df_temp = df_ztubt_validatedbetticket\
            .merge( df_ubt_temp_placebettransaction,
                    on='TICKETSERIALNUMBER',
                    how='inner',
                    suffixes=('_zv', '_PB'))\
            .merge(
                df_ztubt_terminal_location[['TERDISPLAYID', 'LOCID', 'LOCDISPLAYID']],
                left_on='TERDISPLAYID_PB',
                right_on='TERDISPLAYID',
                how='left')
    condittion = (
        (df_temp['PRODID_PB'].isin([5,6])) &
        (df_temp['TRANSACTIONTYPE'] == 3) &
        (df_temp['CREATEDVALIDATIONDATE'] >= startdateutc) &
        (df_temp['CREATEDVALIDATIONDATE'] <= enddateUTC) &
        (df_temp['VALIDATIONTYPEID'] == 'RFND')
    )
    # Rename columns to standardize naming
    column_mapping = {
        'PRODID_PB': 'PRODID',
        'REQUESTID_PB': 'UUID',
        'USERDISPLAYID_PB': 'USERID',
        'CARTID_PB': 'CARTID',
        'TRANHEADERID_PB': 'TRANHEADERID',
        'CREATEDVALIDATIONDATE': 'TRANSACTIONDATE',
        'TERDISPLAYID': 'TERMINALID',
        'LOCDISPLAYID': 'LOCATIONID'
    }
    df_temp = df_temp[condittion].assign(TRANSACTIONTYPE = 61).rename(columns=column_mapping).reset_index(drop=True)[[
        'ENTRYMETHODID',
       'PRODID',
       'ISBETREJECTEDBYTRADER',
       'TICKETSERIALNUMBER',
       'UUID',
       'TRANSACTIONTYPE',
       'TRANSACTIONDATE',
       'TERMINALID',
       'USERID',
       'LOCATIONID',
       'CARTID',
       'TRANHEADERID'
    ]]
    df_ubt_temp_trans = pd.concat([df_ubt_temp_trans, df_temp], ignore_index=True)
    print(f"UBT_TEMP_TRANS : COMPLETED 1.4 (Line 1025 - 1044) with {df_temp.shape[0]} rows")
    logger.info(f"UBT_TEMP_TRANS : COMPLETED 1.4 (Line 1025 - 1044) with {df_temp.shape[0]} rows")


    # ================================== 1.5 CANCELLED BET TICKET===================================
    # 1.5 CANCELLED HORSE RACING
    df_temp = pd.DataFrame()
    df_temp = df_ztubt_cancelledbetticket\
            .merge(
                df_ubt_temp_placebettransaction,
                on='TICKETSERIALNUMBER',
                how='inner',
                suffixes=('_CB', '_PB'))\
            .merge(
                df_ztubt_terminal_location[['TERDISPLAYID', 'LOCID', 'LOCDISPLAYID']],
                left_on='TERDISPLAYID_PB',
                right_on='TERDISPLAYID',
                how='inner')
    condittion = (
        (df_temp['PRODID_PB'] == 1) &
        (df_temp['TRANSACTIONTYPE'] == 5) &
        ((df_temp['CANCELLEDDATE'] >= startdate) &
        (df_temp['CANCELLEDDATE'] <= enddate)) )
    # Rename columns to standardize naming
    column_mapping = {
        'PRODID_PB': 'PRODID',
        'REQUESTID_PB': 'UUID',
        'USERDISPLAYID_PB': 'USERID',
        'CARTID_PB': 'CARTID',
        'TRANHEADERID_PB': 'TRANHEADERID',
        'CANCELLEDDATE': 'TRANSACTIONDATE',
        'TERDISPLAYID': 'TERMINALID',
        'LOCDISPLAYID': 'LOCATIONID'
    }
    df_temp = df_temp[condittion].assign(TRANSACTIONTYPE = 2).rename(columns=column_mapping).reset_index(drop=True)[[
        'ENTRYMETHODID',
       'PRODID',
       'ISBETREJECTEDBYTRADER',
       'TICKETSERIALNUMBER',
       'UUID',
       'TRANSACTIONTYPE',
       'TRANSACTIONDATE',
       'TERMINALID',
       'USERID',
       'LOCATIONID',
       'CARTID',
       'TRANHEADERID'
    ]]
    df_ubt_temp_trans = pd.concat([df_ubt_temp_trans, df_temp], ignore_index=True)
    print(f"UBT_TEMP_TRANS : COMPLETED 1.5 (Line 1049-1068) with {df_temp.shape[0]} rows")
    logger.info(f"UBT_TEMP_TRANS : COMPLETED 1.5 (Line 1049-1068) with {df_temp.shape[0]} rows")
    # ================================== 1.6 CANCELLED LOTTERY===================================
    df_temp = pd.DataFrame()
    df_temp = df_ztubt_cancelledbetticket\
            .merge(
                df_ubt_temp_placebettransaction,
                on='TICKETSERIALNUMBER',
                how='inner',
                suffixes=('_CB', '_PB'))\
            .merge(
                df_ztubt_terminal_location[['TERDISPLAYID', 'LOCID', 'LOCDISPLAYID']],
                left_on='TERDISPLAYID_PB',
                right_on='TERDISPLAYID',
                how='inner')
    condittion = (
        (df_temp['PRODID_PB'].isin([2,3,4])) &
        (df_temp['TRANSACTIONTYPE'] == 5) &
        ((df_temp['CANCELLEDDATE'] >= startdate) &
        (df_temp['CANCELLEDDATE'] <= enddate)) )
    # Rename columns to standardize naming
    column_mapping = {
        'PRODID_PB': 'PRODID',
        'REQUESTID_CB': 'UUID',
        'USERDISPLAYID_PB': 'USERID',
        'CARTID_PB': 'CARTID',
        'TRANHEADERID_PB': 'TRANHEADERID',
        'CANCELLEDDATE': 'TRANSACTIONDATE',
        'TERDISPLAYID': 'TERMINALID',
        'LOCDISPLAYID': 'LOCATIONID'
    }
    df_temp = df_temp[condittion].assign(TRANSACTIONTYPE = 2).rename(columns=column_mapping).reset_index(drop=True)[[
        'ENTRYMETHODID',
       'PRODID',
       'ISBETREJECTEDBYTRADER',
       'TICKETSERIALNUMBER',
       'UUID',
       'TRANSACTIONTYPE',
       'TRANSACTIONDATE',
       'TERMINALID',
       'USERID',
       'LOCATIONID',
       'CARTID',
       'TRANHEADERID'
    ]]
    df_ubt_temp_trans = pd.concat([df_ubt_temp_trans, df_temp], ignore_index=True)
    print(f"UBT_TEMP_TRANS : COMPLETED 1.6 (Line 1071 - 1090) with {df_temp.shape[0]} rows")
    logger.info(f"UBT_TEMP_TRANS : COMPLETED 1.6 (Line 1071 - 1090) with {df_temp.shape[0]} rows")
    # ================================== 1.7 CANCELLED SPORTS BETTING===================================
    df_temp = pd.DataFrame()
    df_temp = df_ztubt_cancelledbetticket\
            .merge(
                df_ubt_temp_placebettransaction,
                on='TICKETSERIALNUMBER',
                how='inner',
                suffixes=('_CB', '_PB'))\
            .merge(
                df_ztubt_terminal_location[['TERDISPLAYID', 'LOCID', 'LOCDISPLAYID']],
                left_on='TERDISPLAYID_PB',
                right_on='TERDISPLAYID',
                how='inner')
    condittion = (
        (df_temp['PRODID_PB'].isin([5,6])) &
        (df_temp['TRANSACTIONTYPE'] == 5) &
        ((df_temp['CANCELLEDDATE'] >= startdate) &
        (df_temp['CANCELLEDDATE'] <= enddate)) &
        (df_temp['ISBETREJECTEDBYTRADER'] == False))
    # Rename columns to standardize naming
    column_mapping = {
        'PRODID_PB': 'PRODID',
        'REQUESTID_PB': 'UUID',
        'USERDISPLAYID_PB': 'USERID',
        'CARTID_PB': 'CARTID',
        'TRANHEADERID_PB': 'TRANHEADERID',
        'CANCELLEDDATE': 'TRANSACTIONDATE',
        'TERDISPLAYID': 'TERMINALID',
        'LOCDISPLAYID': 'LOCATIONID'
    }
    df_temp = df_temp[condittion].assign(TRANSACTIONTYPE = 2).rename(columns=column_mapping).reset_index(drop=True)[[
        'ENTRYMETHODID',
       'PRODID',
       'ISBETREJECTEDBYTRADER',
       'TICKETSERIALNUMBER',
       'UUID',
       'TRANSACTIONTYPE',
       'TRANSACTIONDATE',
       'TERMINALID',
       'USERID',
       'LOCATIONID',
       'CARTID',
       'TRANHEADERID'
    ]]
    df_ubt_temp_trans = pd.concat([df_ubt_temp_trans, df_temp], ignore_index=True)

    df_ubt_temp_trans['TRANSACTIONDATE'] = pd.to_datetime(df_ubt_temp_trans['TRANSACTIONDATE'])
    df_ubt_temp_trans['TRANSACTIONDATE'] = df_ubt_temp_trans['TRANSACTIONDATE'].apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if x.microsecond != 0 else x.strftime('%Y-%m-%d %H:%M:%S')
    )
    print(f"UBT_TEMP_TRANS : COMPLETED 1.7 (Line 1095 - 1115) with {df_temp.shape[0]} rows")
    logger.info(f"UBT_TEMP_TRANS : COMPLETED 1.7 (Line 1095 - 1115) with {df_temp.shape[0]} rows")
    #================================ END TRANSFORMATION LOGIC =================================
    return df_ubt_temp_trans

def ubt_temp_sport_details(df_ubt_temp_placebetransactiontype, df_ubt_temp_liveindicator, ubt_temp_bettype):

    df_ubt_temp_sport_details = pd.DataFrame()

    # ================================== EXTRACT DATA FROM TABLES =================================

    # ========== Extract data from ztubt_sports_placebetransactionlineitemnumber ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_sports_placedbettransactionlineitemnumber
    """
    df_ztubt_sports_placedbettransactionlineitemnumber = pd.read_sql(query, connection)

    # ========== Extract data from ztubt_sportevent ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_sportevent
    """
    df_ztubt_sportevent = pd.read_sql(query, connection)

    # ========== Extract data from ztubt_sporttype ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_sporttype
    """
    df_ztubt_sporttype = pd.read_sql(query, connection)

    # ========== Extract data from ztubt_sportleagueinfo ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_sportleagueinfo
    """
    df_ztubt_sportleagueinfo = pd.read_sql(query, connection)

    # ---------- Extract data from ztubt_entrymethod ----------
    query = f"""
    SELECT * FROM {schema}.ztubt_entrymethod
    """
    df_ztubt_entrymethod = pd.read_sql(query, connection)

    df_ztubt_sporttype['NAME_JOIN'] = df_ztubt_sporttype['NAME'].str.replace('|','')
    df_ztubt_sportleagueinfo['NAME_JOIN'] = df_ztubt_sportleagueinfo['NAME'].str.replace('|','')

    df_ubt_temp_placebetransactiontype = df_ubt_temp_placebetransactiontype.add_suffix('_PBVBCBT')
    df_ztubt_sports_placedbettransactionlineitemnumber = df_ztubt_sports_placedbettransactionlineitemnumber.add_suffix('_PLN')
    ubt_temp_bettype = ubt_temp_bettype.add_prefix('b.')
    # ================================== TRANSFORMATION LOGIC =================================
    df_ubt_temp_sport_details = (df_ubt_temp_placebetransactiontype\
        .merge( drop_audit_cols(df_ztubt_sports_placedbettransactionlineitemnumber), left_on='TRANHEADERID_PBVBCBT', right_on='TRANHEADERID_PLN')\
        .merge( drop_audit_cols(df_ztubt_sportevent), left_on='EVENTID_PLN', right_on='EVENTID').drop(columns='EVENTID', errors='ignore')\
        .merge( drop_audit_cols(df_ztubt_sporttype), left_on='TYPEID', right_on='TYPEID')\
        .merge( drop_audit_cols(df_ztubt_sportleagueinfo), left_on=['NAME_JOIN'], right_on=['NAME_JOIN'])\
        .merge( drop_audit_cols(df_ubt_temp_liveindicator), left_on='TICKETSERIALNUMBER_PBVBCBT', right_on='TICKETSERIALNUMBER').drop(columns='TICKETSERIALNUMBER',errors='ignore')\
        .merge( drop_audit_cols(ubt_temp_bettype), left_on='TRANLINEITEMID_PLN', right_on='b.TRANLINEITEMNUMBERID')\
        .merge( drop_audit_cols(df_ztubt_entrymethod), left_on='ENTRYMETHODID_PBVBCBT', right_on='ENTRYMETHODID', how='left')\
        .query('PRODID_PBVBCBT in [5,6]')\
        .assign(
            ENTRYMETHOD=lambda x: x['ENTRYMETHODID_PBVBCBT'].map({'MANUAL':0, 'BETSLIP':2, 'EDIT':10, 'EBETSLIP':16}),
            STARTTIME=lambda x: pd.to_datetime(x['STARTTIME']).dt.strftime('%Y%m%d %H%M%S'),
            LIVEINDICATOR=lambda x: x['LIVEINDICATOR'].astype(str))\
            .rename(columns={
                'TICKETSERIALNUMBER_PBVBCBT':'TICKETSERIALNUMBER',
                'TRANLINEITEMID_PLN':'TRANLINEITEMID',
                'ENTRYMEDTHOD' : 'ENTRYMETHOD',
                'EVENTNAME_PLN':'EVENTNAME',
                'BETTYPENAME_PLN':'BETTYPENAME',
                'SELECTIONNAME_PLN':'SELECTIONNAME',
                'SELECTIONODDS_PLN':'SELECTIONODDS',
                'LEAGUECODE':'LEAGUECODE',
                'EVENTID_PLN':'EVENTID',
                'b.BETTYPEID':'BETTYPEID',
            })\
        [['TICKETSERIALNUMBER','TRANLINEITEMID','ENTRYMETHOD','EVENTID','LEAGUECODE',
          'LIVEINDICATOR','EVENTNAME','BETTYPENAME','SELECTIONNAME','SELECTIONODDS',
          'STARTTIME','BETTYPEID']]\
        .reset_index(drop=True))
    return df_ubt_temp_sport_details

def ubt_temp_liveindicator(df_ubt_temp_placebettransactiontype):

    df_ubt_temp_liveindicator = pd.DataFrame()
    # ================================== EXTRACT DATA FROM TABLES =================================

    # ----------- Extract data from ztubt_sports_placedbettransactionlineitemnumber ----------
    query = f"""
    SELECT * FROM {schema}.ztubt_sports_placedbettransactionlineitemnumber
    """
    df_ztubt_sports_placedbettransactionlineitemnumber = pd.read_sql(query, connection)

    # ---------- Extract data from ztubt_sportevent ----------
    query = f"""
    SELECT * FROM {schema}.ztubt_sportevent
    """
    df_ztubt_sportevent = pd.read_sql(query, connection)

    # ================================== TRANSFORMATION LOGIC =================================
    df_ubt_temp_liveindicator = (df_ubt_temp_placebettransactiontype\
        .merge( df_ztubt_sports_placedbettransactionlineitemnumber, on='TRANHEADERID',suffixes=('_PBVBCBT', '_PLN'))\
        .merge( df_ztubt_sportevent, left_on='EVENTID', right_on='EVENTID'))\
        .query('PRODID in [5,6]')

    df_ubt_temp_liveindicator['ROWNUM'] = df_ubt_temp_liveindicator.groupby('TRANHEADERID',as_index=False,dropna=False).cumcount() + 1
    df_ubt_temp_liveindicator = df_ubt_temp_liveindicator[df_ubt_temp_liveindicator['ROWNUM'] == 1].reset_index(drop=True)
    df_ubt_temp_liveindicator['LIVEINDICATOR'] = df_ubt_temp_liveindicator.apply(
        lambda x: 'Y' if (x['HASBETINRUN'] == True)
        and (x['CREATEDDATE'] >= x['STARTTIME'])
        and (x['CREATEDDATE'] <= x['SUSPENDTIME']) else 'N',
        axis=1
    )
    df_ubt_temp_liveindicator = df_ubt_temp_liveindicator[['TICKETSERIALNUMBER', 'LIVEINDICATOR']]

    return df_ubt_temp_liveindicator

def ubt_temp_horsedetail(df_ubt_temp_placebettransactiontype, startdateutc, enddateUTC):

    df_ubt_temp_horsedetail = pd.DataFrame()

    # ================================== EXTRACT DATA FROM TABLES =================================

    # ========== Extract data from ztubt_horse_placedbettransactionlineitem ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_horse_placedbettransactionlineitem

    """
    ztubt_horse_placedbettransactionlineitem = pd.read_sql(query, connection)

    # ================================= TRANSFORMATION LOGIC =================================

    df_ubt_temp_horsedetail = df_ubt_temp_placebettransactiontype\
        .merge( ztubt_horse_placedbettransactionlineitem, on='TRANHEADERID',suffixes=('_PBVBCBT', '_PBTL'))\
        .query(f"PRODID == 1 and ((CREATEDDATE >= '{startdateutc}' and CREATEDDATE <= '{enddateUTC}' and TRANSACTIONTYPETOTAL in [1,4,6]) or TRANSACTIONTYPETOTAL in [3,5])")\
        .assign(
            BOARDREBATEAMOUNT=lambda x: x['BOARDREBATEAMOUNT'].fillna(0))\
        .groupby(['TICKETSERIALNUMBER', 'TRANHEADERID','BETTYPEID', 'ENTRYMETHODID'], as_index=False, dropna=False)\
        .agg(AMOUNT=('BOARDREBATEAMOUNT', lambda x: x.iloc[0] if x.name[2]=='W-P' else x.sum()))\
        .reset_index(drop=True)\
        .groupby(['TICKETSERIALNUMBER', 'ENTRYMETHODID'], as_index=False)\
        .agg(BETLINEREBATEAMOUNT = ('AMOUNT', 'sum'))\
        .assign(ENTRYMETHOD=lambda x: x['ENTRYMETHODID'].map({'MANUAL':0, 'BETSLIP':2, 'EDIT':10, 'EBETSLIP':16}))\
        [['TICKETSERIALNUMBER','ENTRYMETHOD','BETLINEREBATEAMOUNT']]

    return df_ubt_temp_horsedetail


def ubt_temp_transactiondata(df_ubt_temp_trans, df_ubt_temp_bettype, df_ubt_temp_lotterydetails,
                             df_ubt_temp_final_lotteryselect, df_ubt_temp_transactionamount, df_ubt_temp_horsedetail,
                             df_ubt_temp_placebettransactiontype, df_ubt_temp_sport_details, startdate, enddate,
                             igt_startdate, igt_enddate, igt_previousbusinessdate, igt_nextbusinessdate, igt_currentbusinessdate,
                             bmcs_startdate, bmcs_enddate, bmcs_previousbusinessdate, bmcs_nextbusinessdate, bmcs_currentbusinessdate,
                             ob_startdate, ob_enddate, ob_previousbusinessdate, ob_nextbusinessdate, ob_currentbusinessdate):


    df_ubt_temp_transactiondata = pd.DataFrame()
    # ================================= EXTRACT DATA =================================
    # ========== Extract data from ztubt_sports_placedbettransactionlineitem ==========
    query = f"""
        SELECT
            TRANHEADERID,
            SALESFACTORAMOUNT,
            TRANLINEITEMID
        FROM {schema}.ztubt_sports_placedbettransactionlineitem
        """
    df_ztubt_sports_placedbettransactionlineitem = pd.read_sql(query, connection)
    # ================================= END OF EXTRACT DATA =================================

    # ================================= TRANSFORMATION LOGIC =================================

    # ============================= UBT_TEMP_TRANSACTION 1.1 =================================:
    df_ubt_temp_trans['TRANSACTIONDATE'] = pd.to_datetime(df_ubt_temp_trans['TRANSACTIONDATE'], format='mixed')
    df_merge = df_ubt_temp_trans\
                .merge( df_ubt_temp_bettype,on='TICKETSERIALNUMBER', how='left')\
                .merge( df_ubt_temp_lotterydetails,left_on=['TICKETSERIALNUMBER','TRANLINEITEMNUMBERID'],right_on=['TICKETSERIALNUMBER','TRANLINEITEMID'], how='left')\
                .merge( df_ubt_temp_final_lotteryselect,on=['TICKETSERIALNUMBER','TRANLINEITEMID'], how='left')\
                .merge( df_ubt_temp_transactionamount,on=['TICKETSERIALNUMBER','TRANLINEITEMID'], how='left')

    ## Filter ProdID in (2,3,4) and bettypeid is not null and isbetrejectedbytrader = false
    df_transformation = df_merge.loc[
        (df_merge['PRODID'].isin([2,3,4])) &
        (df_merge['BETTYPEID'].notnull()) &
        (df_merge['ISBETREJECTEDBYTRADER'] == False) &
        (df_merge['TRANSACTIONDATE'] >= startdate) &
        (df_merge['TRANSACTIONDATE'] <= enddate)
    ].reset_index(drop=True)


    ## Calculate columns
    df_transformation = df_transformation.assign(
        PRODUCTID = lambda x: x['PRODID'].map({1:7, 2:9, 4:11, 3:23, 5:20, 6:20}),
        BUSINESSDATE = lambda x: np.where(
                    x['TRANSACTIONDATE'] < igt_startdate,
                    igt_previousbusinessdate,
                    np.where(
                        x['TRANSACTIONDATE'] > igt_enddate,
                        igt_nextbusinessdate,
                        igt_currentbusinessdate
                    )),
        BOARDSEQNUMBER = lambda x: np.where(
            x['BOARDSEQNUMBER'].isnull(),
            x['UUID'].astype(str) + ' - 00' ,
            x['UUID'].astype(str)
        ),
        WAGER1 = lambda x: x['WAGER'].fillna(0),
        WAGER2 = lambda x: x['SECONDWAGER'].fillna(0),
        SALES1 = lambda x: x['SALES'].fillna(0),
        SALES2 = lambda x: x['SECONDSALES'].fillna(0),
        SALESCOMM1 = lambda x: x['SALESCOMM'].fillna(0),
        SALESCOMM2 = lambda x: x['SECONDSALESCOMM'].fillna(0),
        GST1 = lambda x: x['GST'].fillna(0),
        GST2 = lambda x: x['SECONDGST'].fillna(0),

        RETURNAMOUNT = lambda x: np.where(
            x['TRANSACTIONTYPE'] == 2,
            x['RETURNAMOUNT'].fillna(0),
            0
        ),
        WINNINGAMOUNT = lambda x: np.where(
            x['TRANSACTIONTYPE'] == 3,
            x['WINNINGAMOUNT'].fillna(0),
            0
        ),
        PAYMENTMODE = lambda x: np.where(
            (x['TRANSACTIONTYPE'] == 3) & (x['RETURNAMOUNT'].fillna(0) > 0),
            'CA',
            ''
        ),
        EVENTID = None,
        LEAGUECODE = None,
        LIVEINDICATOR = None,
        EVENTNAME = None,
        MARKET = None,
        ODDS = None,
        MATCHKICKOFF = None,
        BETLINEREBATEAMOUNT = None
        ).sort_values(by=['UUID'])\
        .reset_index(drop=True)\
        .rename(columns={
            'TRANSACTIONDATE':'TRANSACTIONTIMESTAMP'
        })\
        .reset_index(drop=True)\
        [['UUID',
        'TRANSACTIONTYPE',
        'PRODUCTID',
        'BETTYPEID',
        'SELECTION',
        'BUSINESSDATE',
        'TRANSACTIONTIMESTAMP',
        'TERMINALID',
        'USERID',
        'LOCATIONID',
        'CARTID',
        'TRANHEADERID',
        'BOARDSEQNUMBER',
        'ENTRYMETHOD',
        'NUMBOARDS',
        'NUMDRAWS',
        'DRAWID',
        'EBETSLIPINFO_MOBNBR',
        'NUMSIMPLEBETS',
        'QPFLAG',
        'NUMMARKS',
        'BULKID',
        'ITOTO_NUMPARTS',
        'ITOTO_TOTALPARTS',
        'GRPTOTO_NUMPARTS',
        'EVENTID',
        'LEAGUECODE',
        'LIVEINDICATOR',
        'EVENTNAME',
        'MARKET',
        'ODDS',
        'MATCHKICKOFF',
        'WAGER1',
        'WAGER2',
        'SALES1',
        'SALES2',
        'SALESCOMM1',
        'SALESCOMM2',
        'GST1',
        'GST2',
        'RETURNAMOUNT',
        'WINNINGAMOUNT',
        'BETLINEREBATEAMOUNT',
        'PAYMENTMODE'
       ]]
    df_ubt_temp_transactiondata = pd.concat([df_ubt_temp_transactiondata, df_transformation], ignore_index=True)
    print(f"UBT_TEMP_TRANSACTIONDATA : COMPLETED 1.1 with {df_transformation.shape[0]} rows")
    # ============================= END UBT_TEMP_TRANSACTIONDATA 1.1 =================================:

    # ============================ UBT_TEMP_TRANSACTIONDATA 1.2 =================================:
    df_merge = pd.DataFrame()
    df_merge = df_ubt_temp_trans\
                                    .merge(df_ubt_temp_bettype,on='TICKETSERIALNUMBER', how='left')\
                                    .merge(df_ubt_temp_horsedetail,on=['TICKETSERIALNUMBER'], how='left')\
                                    .merge(df_ubt_temp_final_lotteryselect,on=['TICKETSERIALNUMBER'], how='left')\
                                    .merge(df_ubt_temp_transactionamount,on=['TICKETSERIALNUMBER'], how='left')
    ## Filter ProdID = 1 and bettypeid is not null and isbetrejectedbytrader = false
    df_transformation = df_merge.loc[
        (df_merge['PRODID'] == 1) &
        (df_merge['ISBETREJECTEDBYTRADER'] == False) &
        (pd.to_datetime(df_merge['TRANSACTIONDATE']) >= startdate) &
        (pd.to_datetime(df_merge['TRANSACTIONDATE']) <= enddate)
    ].reset_index(drop=True)
    df_transformation['TRANSACTIONDATE'] = pd.to_datetime(df_transformation['TRANSACTIONDATE'])
    df_transformation = df_transformation.assign(
        UUID = lambda x: x['UUID'],
        TRANSACTIONTYPE = lambda x: x['TRANSACTIONTYPE'],
        PRODUCTID = lambda x: x['PRODID'].map({1:7, 2:9, 4:11, 3:23, 5:20, 6:20}),
        BETTYPEID = lambda x: x['BETTYPEID'],
        SELECTION = lambda x: x['SELECTION'],
        BUSINESSDATE=lambda x: np.where(
        x['TRANSACTIONDATE'] < bmcs_startdate,
        bmcs_previousbusinessdate,
        np.where(
            x['TRANSACTIONDATE'] > bmcs_enddate,
            bmcs_nextbusinessdate,
            bmcs_currentbusinessdate
        )
        ),
        TRANSACTIONTIMESTAMP = lambda x: x['TRANSACTIONDATE'],
        TERMINALID = lambda x: x['TERMINALID'],
        USERID = lambda x: x['USERID'],
        LOCATIONID = lambda x: x['LOCATIONID'],
        CARTID = lambda x: x['CARTID'],
        TRANHEADERID = lambda x: x['TRANHEADERID'],
        BOARDSEQNUMBER = lambda x: x['UUID'].astype(str),
        ENTRYMETHOD=lambda x: x['ENTRYMETHODID'].map({'MANUAL':0, 'BETSLIP':2, 'EDIT':10, 'EBETSLIP':16}),
        NUMBOARDS = None,
        NUMDRAWS = None,
        DRAWID = None,
        EBETSLIPINFO_MOBNBR = None,
        NUMSIMPLEBETS = None,
        QPFLAG = None,
        NUMMARKS = None,
        BULKID = None,
        ITOTO_NUMPARTS = None,
        ITOTO_TOTALPARTS = None,
        GRPTOTO_NUMPARTS = None,
        EVENTID = None,
        LEAGUECODE = None,
        LIVEINDICATOR = None,
        EVENTNAME = None,
        MARKET = None,
        ODDS = None,
        MATCHKICKOFF = None,
        WAGER1 = lambda x: x['WAGER'].fillna(0),
        WAGER2 = lambda x: x['SECONDWAGER'].fillna(0),
        SALES1 = lambda x: x['SALES'].fillna(0),
        SALES2 = lambda x: x['SECONDSALES'].fillna(0),
        SALESCOMM1 = lambda x: x['SALESCOMM'].fillna(0),
        SALESCOMM2 = lambda x: x['SECONDSALESCOMM'].fillna(0),
        GST1 = lambda x: x['GST'].fillna(0),
        GST2 = lambda x: x['SECONDGST'].fillna(0),
        RETURNAMOUNT = lambda x: np.where(
            x['TRANSACTIONTYPE'] == 3,
            x['RETURNAMOUNT'].fillna(0),
            0
        ),
        WINNINGAMOUNT = lambda x: np.where(
            x['TRANSACTIONTYPE'] == 3,
            x['WINNINGAMOUNT'].fillna(0),
            0
        ),
        BETLINEREBATEAMOUNT = lambda x: np.where(
            x['TRANSACTIONTYPE'] == 3,
            x['REBATERECLAIM'].fillna(0),
            0
        ),
        PAYMENTMODE = lambda x: np.where(
            (x['TRANSACTIONTYPE'] == 3) & (x['RETURNAMOUNT'].fillna(0) > 0),
            'CA',
            ''
        )
        ).sort_values(by=['UUID'])\
        .reset_index(drop=True)\
        [['UUID',
        'TRANSACTIONTYPE',
        'PRODUCTID',
        'BETTYPEID',
        'SELECTION',
        'BUSINESSDATE',
        'TRANSACTIONTIMESTAMP',
        'TERMINALID',
        'USERID',
        'LOCATIONID',
        'CARTID',
        'TRANHEADERID',
        'BOARDSEQNUMBER',
        'ENTRYMETHOD',
        'NUMBOARDS',
        'NUMDRAWS',
        'DRAWID',
        'EBETSLIPINFO_MOBNBR',
        'NUMSIMPLEBETS',
        'QPFLAG',
        'NUMMARKS',
        'BULKID',
        'ITOTO_NUMPARTS',
        'ITOTO_TOTALPARTS',
        'GRPTOTO_NUMPARTS',
        'EVENTID',
        'LEAGUECODE',
        'LIVEINDICATOR',
        'EVENTNAME',
        'MARKET',
        'ODDS',
        'MATCHKICKOFF',
        'WAGER1',
        'WAGER2',
        'SALES1',
        'SALES2',
        'SALESCOMM1',
        'SALESCOMM2',
        'GST1',
        'GST2',
        'RETURNAMOUNT',
        'WINNINGAMOUNT',
        'BETLINEREBATEAMOUNT',
        'PAYMENTMODE'
       ]]

    df_ubt_temp_transactiondata = pd.concat([df_ubt_temp_transactiondata, df_transformation], ignore_index=True)
    print(f"UBT_TEMP_TRANSACTIONDATA : COMPLETED 1.2 with {df_transformation.shape[0]} rows")
    # ============================ END UBT_TEMP_TRANSACTIONDATA 1.2 =================================:

    # ============================ UBT_TEMP_TRANSACTIONDATA 1.3 (IGT OB DATA) ============================
    # Add suffix to avoid column name conflicts during merge
    df_ubt_temp_trans = df_ubt_temp_trans.add_suffix('_t')
    df_ztubt_sports_placedbettransactionlineitem = df_ztubt_sports_placedbettransactionlineitem.add_suffix('_pbtli')
    df_ubt_temp_placebettransactiontype = df_ubt_temp_placebettransactiontype.add_suffix('_pbvbcbtdis')
    df_ubt_temp_sport_details = df_ubt_temp_sport_details.add_suffix('_sd')
    df_ubt_temp_transactionamount = df_ubt_temp_transactionamount.add_suffix('_amt')
    df_merge = df_ubt_temp_trans\
        .merge( df_ztubt_sports_placedbettransactionlineitem,left_on='TRANHEADERID_t', right_on='TRANHEADERID_pbtli', how='inner')\
        .merge( df_ubt_temp_placebettransactiontype,left_on='TRANHEADERID_t', right_on='TRANHEADERID_pbvbcbtdis', how='inner')\
        .merge(drop_audit_cols(df_ubt_temp_sport_details),left_on=['TICKETSERIALNUMBER_t','TRANLINEITEMID_pbtli'],right_on=['TICKETSERIALNUMBER_sd','TRANLINEITEMID_sd'], how='left')\
        .merge(drop_audit_cols(df_ubt_temp_transactionamount),left_on=['TICKETSERIALNUMBER_t'], right_on=['TICKETSERIALNUMBER_amt'], how='left')

    ## Filter ProdID in (5,6) and bettypeid is not null and isbetrejectedbytrader = false
    df_transformation = df_merge.loc[
        (df_merge['PRODID_t'].isin([5,6])) &
        (df_merge['TRANSACTIONTYPETOTAL_pbvbcbtdis'].isin([1,3,4,5,6])) &
        (df_merge['ISBETREJECTEDBYTRADER_t'] == False) &
        (pd.to_datetime(df_merge['TRANSACTIONDATE_t']) >= startdate) &
        (pd.to_datetime(df_merge['TRANSACTIONDATE_t']) <= enddate)
    ].reset_index(drop=True)

    df_transformation = df_transformation.assign(
        UUID = lambda x: x['UUID_t'],
        TRANSACTIONTYPE = lambda x: x['TRANSACTIONTYPE_t'],
        PRODUCTID = lambda x: x['PRODID_t'].map({1:7, 2:9, 4:11, 3:23, 5:20, 6:20}),
        BETTYPEID = lambda x: x['BETTYPEID_sd'],
        SELECTION = lambda x: x['SELECTIONODDS_sd'],
        BUSINESSDATE = lambda x: np.where(
        x['TRANSACTIONDATE_t'] < ob_startdate,
        ob_previousbusinessdate,
        np.where(
            x['TRANSACTIONDATE_t'] > ob_enddate,
            ob_nextbusinessdate,
            ob_currentbusinessdate
        )
        ),
        TRANSACTIONTIMESTAMP = lambda x: x['TRANSACTIONDATE_t'],
        TERMINALID = lambda x: x['TERMINALID_t'],
        USERID = lambda x: x['USERID_t'],
        LOCATIONID = lambda x: x['LOCATIONID_t'],
        CARTID = lambda x: x['CARTID_t'],
        TRANHEADERID = lambda x: x['TRANHEADERID_t'],
        BOARDSEQNUMBER = lambda x: x['UUID_t'].astype(str),
        ENTRYMETHOD= lambda x: x['ENTRYMETHOD_sd'],
        NUMBOARDS = None,
        NUMDRAWS = None,
        DRAWID = None,
        EBETSLIPINFO_MOBNBR = None,
        NUMSIMPLEBETS = None,
        QPFLAG = None,
        NUMMARKS = None,
        BULKID = None,
        ITOTO_NUMPARTS = None,
        ITOTO_TOTALPARTS = None,
        GRPTOTO_NUMPARTS = None,
        EVENTID = lambda x: x['EVENTID_sd'],
        LEAGUECODE = lambda x: x['LEAGUECODE_sd'],
        LIVEINDICATOR = lambda x: x['LIVEINDICATOR_sd'],
        EVENTNAME = lambda x: x['EVENTNAME_sd'],
        MARKET = lambda x: x['BETTYPENAME_sd'],
        ODDS = lambda x: x['SELECTIONODDS_sd'],
        MATCHKICKOFF = lambda x: x['STARTTIME_sd'],
        WAGER1 = lambda x: x['WAGER_amt'].fillna(0),
        WAGER2 = lambda x: x['SECONDWAGER_amt'].fillna(0),
        SALES1 = lambda x: x['SALES_amt'].fillna(0),
        SALES2 = lambda x: x['SECONDSALES_amt'].fillna(0),
        SALESCOMM1 = lambda x: x['SALESCOMM_amt'].fillna(0),
        SALESCOMM2 = lambda x: x['SECONDSALESCOMM_amt'].fillna(0),
        GST1 = lambda x: x['GST_amt'].fillna(0),
        GST2 = lambda x: x['SECONDGST_amt'].fillna(0),
        RETURNAMOUNT = lambda x: np.where(
            x['TRANSACTIONTYPE_t'] == 2,
            x['RETURNAMOUNT_amt'].fillna(0),
            np.where(
                x['TRANSACTIONTYPE_t'] == 3,
                x['REFUNDAMOUNT_amt'].fillna(0),
                0
            )
        ),
        WINNINGAMOUNT = lambda x: np.where(
            x['TRANSACTIONTYPE_t'] == 3,
            x['WINNINGAMOUNT_amt'].fillna(0),
            0
        ),
        BETLINEREBATEAMOUNT = None,
        PAYMENTMODE = lambda x: np.where(
            (x['TRANSACTIONTYPE_t'] == 3) & (x['RETURNAMOUNT_amt'].fillna(0) > 0),
            'CA',
            ''
        )
        ).sort_values(by=['UUID'])\
        .reset_index(drop=True)\
            .rename(columns={
            'TRANSACTIONDATE':'TRANSACTIONTIMESTAMP'
        })\
        [['UUID',
        'TRANSACTIONTYPE',
        'PRODUCTID',
        'BETTYPEID',
        'SELECTION',
        'BUSINESSDATE',
        'TRANSACTIONTIMESTAMP',
        'TERMINALID',
        'USERID',
        'LOCATIONID',
        'CARTID',
        'TRANHEADERID',
        'BOARDSEQNUMBER',
        'ENTRYMETHOD',
        'NUMBOARDS',
        'NUMDRAWS',
        'DRAWID',
        'EBETSLIPINFO_MOBNBR',
        'NUMSIMPLEBETS',
        'QPFLAG',
        'NUMMARKS',
        'BULKID',
        'ITOTO_NUMPARTS',
        'ITOTO_TOTALPARTS',
        'GRPTOTO_NUMPARTS',
        'EVENTID',
        'LEAGUECODE',
        'LIVEINDICATOR',
        'EVENTNAME',
        'MARKET',
        'ODDS',
        'MATCHKICKOFF',
        'WAGER1',
        'WAGER2',
        'SALES1',
        'SALES2',
        'SALESCOMM1',
        'SALESCOMM2',
        'GST1',
        'GST2',
        'RETURNAMOUNT',
        'WINNINGAMOUNT',
        'BETLINEREBATEAMOUNT',
        'PAYMENTMODE'
       ]]
    df_ubt_temp_transactiondata = pd.concat([df_ubt_temp_transactiondata, df_transformation], ignore_index=True)
    print(f"UBT_TEMP_TRANSACTIONDATA : COMPLETED 1.3 (IGT OB DATA) with {df_transformation.shape[0]} rows")
    # ============================ END UBT_TEMP_TRANSACTIONDATA 1.3 (IGT OB DATA) =======

    # # fill NA with 0 for numeric columns
    # col_fill_0 = ('ENTRYMETHOD', 'NUMBOARDS', 'NUMDRAWS', 'NUMSIMPLEBETS', 'QPFLAG', 'NUMMARKS', 'GST2','BETLINEREBATEAMOUNT')

    # for column in col_fill_0:
    #     if column in df_ubt_temp_transactiondata.columns:
    #         df_ubt_temp_transactiondata[column] = df_ubt_temp_transactiondata[column].fillna(0)

    # # fill NA with empty string for object columns

    # col_fill_empty = ('DRAWID', 'EBETSLIPINFO_MOBNBR', 'BULKID', 'ITOTO_NUMPARTS')

    # for column in col_fill_empty:
    #     if column in df_ubt_temp_transactiondata.columns:
    #         df_ubt_temp_transactiondata[column] = df_ubt_temp_transactiondata[column].fillna('')


    conditions = [
        (df_ubt_temp_transactiondata['TRANSACTIONTIMESTAMP'] >= startdate) &
        (df_ubt_temp_transactiondata['TRANSACTIONTIMESTAMP'] <= enddate)
    ]

    df_ubt_temp_transactiondata['BUSINESSDATE'] = pd.to_datetime(df_ubt_temp_transactiondata['BUSINESSDATE']).dt.date
    # Convert to string format to avoid pandas timestamp conversion issues with Snowflake
    df_ubt_temp_transactiondata['TRANSACTIONTIMESTAMP'] = pd.to_datetime(df_ubt_temp_transactiondata['TRANSACTIONTIMESTAMP'])
    df_ubt_temp_transactiondata['TRANSACTIONTIMESTAMP'] = df_ubt_temp_transactiondata['TRANSACTIONTIMESTAMP'].apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if x.microsecond != 0 else x.strftime('%Y-%m-%d %H:%M:%S')
    )
    df_ubt_temp_transactiondata['TRANSACTIONTIMESTAMP'] = pd.to_datetime(df_ubt_temp_transactiondata['TRANSACTIONTIMESTAMP'],format = 'mixed').dt.strftime('%Y-%m-%d %H:%M:%S')
    # ================================= END TRANSFORMATION LOGIC =================================

    return df_ubt_temp_transactiondata

def ubt_temp_lotterydraw(df_ubt_temp_placebettransactiontype):

    df_ubt_temp_lotterydraw = pd.DataFrame()
    condition = (
        df_ubt_temp_placebettransactiontype['PRODID'].isin([2,3,4])
    )
    print(f"UBT_TEMP_LOTTERYDRAW : FILTERED PLACEBETTRANSACTION with {df_ubt_temp_placebettransactiontype.shape[0]} rows")
    df_ubt_temp_placebettransactiontype = df_ubt_temp_placebettransactiontype.loc[condition].reset_index(drop=True)
    print(f"UBT_TEMP_LOTTERYDRAW : AFTER FILTER PLACEBETTRANSACTION with {df_ubt_temp_placebettransactiontype.shape[0]} rows")
    df_ubt_temp_lotterydraw = df_ubt_temp_placebettransactiontype\
                                .merge(df_ztubt_drawdates,
                                    on='TRANHEADERID',
                                    how='inner')\
                                .assign(
                            HOSTDRAWDATESID = lambda x: x['HOSTDRAWDATESID'].astype(str))
    return df_ubt_temp_lotterydraw[['TICKETSERIALNUMBER', 'HOSTDRAWDATESID']]

def ubt_temp_resultsalescomwithdate(df_ubt_temp_placebettransactiontype, startdateutc, enddateUTC):
    df_ubt_temp_resultsalescomwithdate = pd.DataFrame({
        'TICKETSERIALNUMBER': [],
        'TRANLINEITEMID': [],
        'SALESCOMMAMOUNT': [],
        'SALESFACTORAMOUNT': [],
        'SECONDSALESCOMMAMOUNT': [],
        'SECONDSALESFACTORAMOUNT': [],
        'GSTRATE': []})

    # ================================== EXTRACT DATA FROM TABLES =================================
    # ========== Extract data from ztubt_toto_placedbettransactionlineitem ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_toto_placedbettransactionlineitem
    """
    df_ztubt_toto_placedbettransactionlineitem = pd.read_sql(query, connection)


    # ========== Extract data from ztubt_horse_placedbettransactionlineitem ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_horse_placedbettransactionlineitem
    """
    df_ztubt_horse_placedbettransactionlineitem = pd.read_sql(query, connection)


    # ========== Extract data from ztubt_4d_placedbettransactionlineitemnumber ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_4d_placedbettransactionlineitemnumber
    """
    df_ztubt_4d_placedbettransactionlineitemnumber = pd.read_sql(query, connection)

    # ========== Extract data from ztubt_sweep_placedbettransactionlineitem ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_sweep_placedbettransactionlineitem
    """
    df_ztubt_sweep_placedbettransactionlineitem = pd.read_sql(query, connection)

    # ========== Extract data from ztubt_sweep_placedbettransactionlineitemnumber ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_sweep_placedbettransactionlineitemnumber
    """
    df_ztubt_sweep_placedbettransactionlineitemnumber = pd.read_sql(query, connection)

    # ========== Extract data from ztubt_placedbettransactionheaderlifecyclestate ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_placedbettransactionheaderlifecyclestate
    """
    df_ztubt_placedbettransactionheaderlifecyclestate = pd.read_sql(query, connection)

    # ========== Extract data from ztubt_sports_placedbettransactionlineitem ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_sports_placedbettransactionlineitem
    """
    df_ztubt_sports_placedbettransactionlineitem = pd.read_sql(query, connection)

    # add prefix to avoid column name conflicts during merge
    df_ubt_temp_placebettransactiontype = df_ubt_temp_placebettransactiontype.add_prefix('PBT.')
    df_ztubt_toto_placedbettransactionlineitem = df_ztubt_toto_placedbettransactionlineitem.add_prefix('ZDP.')
    df_ztubt_horse_placedbettransactionlineitem = df_ztubt_horse_placedbettransactionlineitem.add_prefix('HORSE.')
    df_ztubt_4d_placedbettransactionlineitemnumber = df_ztubt_4d_placedbettransactionlineitemnumber.add_prefix('4D.')
    df_ztubt_sweep_placedbettransactionlineitem = df_ztubt_sweep_placedbettransactionlineitem.add_prefix('SWEEP.')
    df_ztubt_sweep_placedbettransactionlineitemnumber = df_ztubt_sweep_placedbettransactionlineitemnumber.add_prefix('SWEEP_NUM.')
    df_ztubt_placedbettransactionheaderlifecyclestate = df_ztubt_placedbettransactionheaderlifecyclestate.add_prefix('PBTHLCS.')
    df_ztubt_sports_placedbettransactionlineitem = df_ztubt_sports_placedbettransactionlineitem.add_prefix('SPORTS.')
    # ================================== TRANSFORMATION LOGIC =================================


    # ================================== 1.1 TOTO SALES COMM WITH DATE ====================================
    df_temp = df_ubt_temp_placebettransactiontype\
            .merge(
                df_ztubt_toto_placedbettransactionlineitem,
                left_on='PBT.TRANHEADERID',
                right_on='ZDP.TRANHEADERID',
                how='inner',
            )
    # Apply filter conditions on the merged dataframe
    print(df_temp.dtypes)
    df_temp = df_temp[
        (df_temp['PBT.PRODID'] == 3)
        &
        (df_temp['PBT.ISEXCHANGETICKET'] == False)
        &
        (
            (
            (df_temp['ZDP.CREATEDDATE'] >= startdateutc)
            &
            (df_temp['ZDP.CREATEDDATE'] <= enddateUTC)
            &
            (df_temp['PBT.TRANSACTIONTYPETOTAL'].isin([1,4,6]))
            )
        )
        |
        ( df_temp['PBT.TRANSACTIONTYPETOTAL'].isin([3,5]))
    ]

    df_temp = df_temp\
            .groupby(
                ['PBT.TICKETSERIALNUMBER', 'ZDP.TRANLINEITEMID'],
                as_index=False,dropna=False
            )\
            .agg({
                'ZDP.SALESCOMMAMOUNT': 'sum',
                'ZDP.SALESFACTORAMOUNT': 'sum'
            })\
            .rename(columns={
                'PBT.TICKETSERIALNUMBER':'TICKETSERIALNUMBER',
                'ZDP.TRANLINEITEMID':'TRANLINEITEMID',
                'ZDP.SALESCOMMAMOUNT':'SALESCOMMAMOUNT',
                'ZDP.SALESFACTORAMOUNT':'SALESFACTORAMOUNT'
            })\
            .assign(
                SECONDSALESCOMMAMOUNT=0,
                SECONDSALESFACTORAMOUNT=0,
                GSTRATE=None
            )[['TICKETSERIALNUMBER', 'TRANLINEITEMID', 'SALESCOMMAMOUNT', 'SALESFACTORAMOUNT',
               'SECONDSALESCOMMAMOUNT', 'SECONDSALESFACTORAMOUNT', 'GSTRATE']]
    df_ubt_temp_resultsalescomwithdate = pd.concat([df_ubt_temp_resultsalescomwithdate, df_temp], ignore_index=True)
    print(f"UBT_TEMP_RESULTSALESCOMWITHDATE : COMPLETED 1.1 (Line 1268 - 1276) with {df_temp.shape[0]} rows")


    # ================================== 1.2 HORSE SALES COMM WITH DATE ====================================
    df_temp = df_ubt_temp_placebettransactiontype\
            .merge(
                df_ztubt_horse_placedbettransactionlineitem,
                left_on='PBT.TRANHEADERID',
                right_on='HORSE.TRANHEADERID',
                how='inner',
            )\
            .merge(
                df_ztubt_terminal[['TERDISPLAYID', 'LOCID']],
                left_on='PBT.TERDISPLAYID',
                right_on='TERDISPLAYID',
                how='inner')
    # Apply filter conditions on the merged dataframe
    condittion_filtered = (
        (df_temp['PBT.PRODID'].isin([1])) &
        (
            (
                (df_temp['HORSE.CREATEDDATE'] >= startdateutc) &
                (df_temp['HORSE.CREATEDDATE'] <= enddateUTC) &
                df_temp['PBT.TRANSACTIONTYPETOTAL'].isin([1,4,6])
            ) |
            df_temp['PBT.TRANSACTIONTYPETOTAL'].isin([3,5])
        )
    )
    df_temp['HORSE.CREATEDDATE'] = pd.to_datetime(df_temp['HORSE.CREATEDDATE']).dt.date

    # ====== Split 2 parts (W-P and Non W-P) ======
    # Part 1: W-P
    df_temp_wp = df_temp[df_temp['HORSE.BETTYPEID'] == 'W-P']
    df_temp_wp = df_temp_wp\
        .groupby(
            ['PBT.TICKETSERIALNUMBER', 'HORSE.TRANHEADERID', 'HORSE.CREATEDDATE','HORSE.BETTYPEID'],
            as_index=False,dropna=False
        )\
        .agg({
            'HORSE.SALESCOMMAMOUNT': lambda x: x.iloc[0].fillna(0),
            'HORSE.SALESFACTORAMOUNT': lambda x: x.iloc[0].fillna(0)
        })\
        .reset_index(drop=True)
    # Part 2: Non W-P
    df_temp_non_wp = df_temp[df_temp['HORSE.BETTYPEID'] != 'W-P']
    df_temp_non_wp = df_temp_non_wp\
        .groupby(
            ['PBT.TICKETSERIALNUMBER', 'HORSE.TRANHEADERID', 'HORSE.CREATEDDATE','HORSE.BETTYPEID'],
            as_index=False,dropna=False
        )\
        .agg({
            'HORSE.SALESCOMMAMOUNT': 'sum',
            'HORSE.SALESFACTORAMOUNT': 'sum'
        })\
        .reset_index(drop=True)
    # Combine both parts
    df_temp = pd.concat([df_temp_wp, df_temp_non_wp], ignore_index=True)
    df_temp = df_temp\
        .groupby(
            ['PBT.TICKETSERIALNUMBER'],
            as_index=False,dropna=False
        )\
        .agg({
            'HORSE.SALESCOMMAMOUNT': 'sum',
            'HORSE.SALESFACTORAMOUNT': 'sum'
        })\
        .rename(columns={
            'PBT.TICKETSERIALNUMBER':'TICKETSERIALNUMBER',
            'HORSE.SALESCOMMAMOUNT':'SALESCOMMAMOUNT',
            'HORSE.SALESFACTORAMOUNT':'SALESFACTORAMOUNT'
        })\
        .assign(
            TRANLINEITEMID=None,
            SECONDSALESCOMMAMOUNT=0,
            SECONDSALESFACTORAMOUNT=0,
            GSTRATE=None
        )[['TICKETSERIALNUMBER', 'TRANLINEITEMID', 'SALESCOMMAMOUNT', 'SALESFACTORAMOUNT',
           'SECONDSALESCOMMAMOUNT', 'SECONDSALESFACTORAMOUNT', 'GSTRATE']]
    df_ubt_temp_resultsalescomwithdate = pd.concat([df_ubt_temp_resultsalescomwithdate, df_temp], ignore_index=True)
    print(f"UBT_TEMP_RESULTSALESCOMWITHDATE : COMPLETED 1.2 (Line 1283 - 1312) with {df_temp.shape[0]} rows")



    # ================================== 1.3 4D SALES COMM WITH DATE ====================================
    df_temp = df_ubt_temp_placebettransactiontype\
            .merge(
                df_ztubt_4d_placedbettransactionlineitemnumber,
                left_on='PBT.TRANHEADERID',
                right_on='4D.TRANHEADERID',
                how='inner',
            )\
            .merge(
                df_ztubt_drawdates,
                left_on='PBT.TRANHEADERID',
                right_on='TRANHEADERID',
                how='inner'
            )
    # Apply filter conditions on the merged dataframe
    condittion_filtered = (
        (df_temp['PBT.PRODID'].isin([2])) &
        (
            ((df_temp['4D.CREATEDDATE'] >= startdateutc) &
            (df_temp['4D.CREATEDDATE'] <= enddateUTC) &
            df_temp['PBT.TRANSACTIONTYPETOTAL'].isin([1,4,6])) |
            df_temp['PBT.TRANSACTIONTYPETOTAL'].isin([3,5])
        )
        & (df_temp['PBT.ISEXCHANGETICKET'] == False)
    )

    df_temp = df_temp\
            .loc[condittion_filtered]\
            .reset_index(drop=True)\
            .groupby(
                ['PBT.TICKETSERIALNUMBER', '4D.TRANLINEITEMID'],
                as_index=False,dropna=False)\
            .agg({
                '4D.SALESCOMMAMOUNTBIG': 'sum',
                '4D.SALESFACTORAMOUNTBIG': 'sum',
                '4D.SALESCOMMAMOUNTSMALL': 'sum',
                '4D.SALESFACTORAMOUNTSMALL': 'sum'
            })\
            .reset_index(drop=True)\
            .rename(columns={
                'PBT.TICKETSERIALNUMBER':'TICKETSERIALNUMBER',
                '4D.TRANLINEITEMID':'TRANLINEITEMID',
                '4D.SALESCOMMAMOUNTBIG': 'SALESCOMMAMOUNT',
                '4D.SALESFACTORAMOUNTBIG': 'SALESFACTORAMOUNT',
                '4D.SALESCOMMAMOUNTSMALL': 'SECONDSALESCOMMAMOUNT',
                '4D.SALESFACTORAMOUNTSMALL': 'SECONDSALESFACTORAMOUNT'
            })\
            .assign(
                GSTRATE=None
            )[['TICKETSERIALNUMBER', 'TRANLINEITEMID', 'SALESCOMMAMOUNT', 'SALESFACTORAMOUNT',
               'SECONDSALESCOMMAMOUNT', 'SECONDSALESFACTORAMOUNT', 'GSTRATE']]
    df_ubt_temp_resultsalescomwithdate = pd.concat([df_ubt_temp_resultsalescomwithdate, df_temp], ignore_index=True)
    print(f"UBT_TEMP_RESULTSALESCOMWITHDATE : COMPLETED 1.3 (Line 1319 - 1332) with {df_temp.shape[0]} rows")


    # ================================== 1.4 SWEEP SALES COMM WITH DATE ====================================
    df_temp = df_ubt_temp_placebettransactiontype\
            .merge(
                df_ztubt_sweep_placedbettransactionlineitem,
                left_on='PBT.TRANHEADERID',
                right_on='SWEEP.TRANHEADERID',
                how='inner',
            )\
            .merge(
                df_ztubt_sweep_placedbettransactionlineitemnumber,
                left_on='SWEEP.TRANLINEITEMID',
                right_on='SWEEP_NUM.TRANLINEITEMID',
                how='inner',
            )\
            .merge(
                df_ztubt_placedbettransactionheaderlifecyclestate,
                left_on='PBT.TRANHEADERID',
                right_on='PBTHLCS.TRANHEADERID',
                how='inner',
            )\
            .merge(
                df_ztubt_terminal_location,
                left_on='PBT.TERDISPLAYID',
                right_on='TERDISPLAYID',
                how='inner'
            )

    # Apply filter conditions on the merged dataframe
    condittion_filtered = (
        (df_temp['PBT.PRODID'].isin([4])) &
        (
			(
				(df_temp['SWEEP.ISPRINTED'].notnull() | df_temp['SWEEPINDICATOR'].isnull()) & (df_temp['PBTHLCS.BETSTATETYPEID'] == 'PB06')
			) |
			(
				(df_temp['SWEEPINDICATOR'].notnull() & df_temp['SWEEP.ISPRINTED'].isnull() & (df_temp['PBTHLCS.BETSTATETYPEID'] == 'PB03'))
			)
		) &
		(
			(
				(df_temp['SWEEP_NUM.CREATEDDATE'] >= startdateutc) & (df_temp['SWEEP_NUM.CREATEDDATE'] <= enddateUTC) & (df_temp['PBT.TRANSACTIONTYPETOTAL'].isin([1, 4, 6]))
			) |
			(df_temp['PBT.TRANSACTIONTYPETOTAL'].isin([3, 5]))
		)
    )
    df_temp = df_temp\
            .loc[condittion_filtered]\
            .reset_index(drop=True)\
            .groupby(
                ['PBT.TICKETSERIALNUMBER', 'SWEEP_NUM.TRANLINEITEMID'],
                as_index=False,dropna=False
            )\
            .agg({
                'SWEEP_NUM.SALESCOMMAMOUNT': 'sum',
                'SWEEP_NUM.SALESFACTORAMOUNT': 'sum'
            })\
            .rename(columns={
                'PBT.TICKETSERIALNUMBER':'TICKETSERIALNUMBER',
                'SWEEP_NUM.TRANLINEITEMID':'TRANLINEITEMID',
                'SWEEP_NUM.SALESCOMMAMOUNT':'SALESCOMMAMOUNT',
                'SWEEP_NUM.SALESFACTORAMOUNT':'SALESFACTORAMOUNT'
            })\
            .assign(
                SECONDSALESCOMMAMOUNT=0,
                SECONDSALESFACTORAMOUNT=0,
                GSTRATE=None
            )[['TICKETSERIALNUMBER', 'TRANLINEITEMID', 'SALESCOMMAMOUNT', 'SALESFACTORAMOUNT',
               'SECONDSALESCOMMAMOUNT', 'SECONDSALESFACTORAMOUNT', 'GSTRATE']]
    df_ubt_temp_resultsalescomwithdate = pd.concat([df_ubt_temp_resultsalescomwithdate, df_temp], ignore_index=True)
    print(f"UBT_TEMP_RESULTSALESCOMWITHDATE : COMPLETED 1.4 (Line 1338 - 1356) with {df_temp.shape[0]} rows")
    # ================================== 1.4 SPORT SALES COMM WITH DATE ====================================

    df_temp = df_ubt_temp_placebettransactiontype\
            .merge(
                df_ztubt_sports_placedbettransactionlineitem,
                left_on='PBT.TRANHEADERID',
                right_on='SPORTS.TRANHEADERID',
                how='inner',
            )
    # Apply filter conditions on the merged dataframe
    condittion_filtered = (
        (df_temp['PBT.PRODID'].isin([5,6])) &
        (df_temp['PBT.ISBETREJECTEDBYTRADER'] == False)
    )
    df_temp = df_temp\
            .loc[condittion_filtered]\
            .reset_index(drop=True)\
            .groupby(
                ['PBT.TICKETSERIALNUMBER'],
                as_index=False,dropna=False
            )\
            .agg({
                'SPORTS.SALESCOMMAMOUNT': 'sum',
                'SPORTS.SALESFACTORAMOUNT': 'sum'
            })\
            .rename(columns={
                'PBT.TICKETSERIALNUMBER':'TICKETSERIALNUMBER',
                'SPORTS.SALESCOMMAMOUNT':'SALESCOMMAMOUNT',
                'SPORTS.SALESFACTORAMOUNT':'SALESFACTORAMOUNT'
            })\
            .assign(
                TRANLINEITEMID=None,
                SECONDSALESCOMMAMOUNT=0,
                SECONDSALESFACTORAMOUNT=0,
                GSTRATE=None
            )[['TICKETSERIALNUMBER', 'TRANLINEITEMID', 'SALESCOMMAMOUNT', 'SALESFACTORAMOUNT',
               'SECONDSALESCOMMAMOUNT', 'SECONDSALESFACTORAMOUNT', 'GSTRATE']]
    df_ubt_temp_resultsalescomwithdate = pd.concat([df_ubt_temp_resultsalescomwithdate, df_temp], ignore_index=True)
    print(f"UBT_TEMP_RESULTSALESCOMWITHDATE : COMPLETED 1.5 (Line 1361 - 1377) with {df_temp.shape[0]} rows")


    #================================= END TRANSFORMATION LOGIC =================================
    return df_ubt_temp_resultsalescomwithdate

def ubt_temp_resultvalidationexchange(df_ubt_temp_placebettransactiontype):
    df_ubt_temp_resultvalidationexchange = pd.DataFrame(
        {
            'TICKETSERIALNUMBER': [],
            'TRANLINEITEMID': [],
            'WINNINGAMOUNT': [],
            'REFUNDAMOUNT': [],
            'REBATERECLAIM': []

        }
    )

    # ================================== EXTRACT DATA FROM TABLES =================================
    # ========== Extract data from ztubt_validatedbetticket ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_validatedbetticket where PRODID IN (2,3,4)
    """
    df_ztubt_validatedbetticket = pd.read_sql(query, connection)

    # ========== Extract data from ztubt_4d_placedbettransactionlineitem ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_4d_placedbettransactionlineitem
    """
    df_ztubt_4d_placedbettransactionlineitem = pd.read_sql(query, connection)

    # ========== Extract data from ztubt_validatedbetticketlifecyclestate ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_validatedbetticketlifecyclestate where BETSTATETYPEID IN ('VB06')
    """
    df_ztubt_validatedbetticketlifecyclestate = pd.read_sql(query, connection)


    # ========== Extract data from ztubt_toto_placedbettransactionlineitem ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_toto_placedbettransactionlineitem
    """
    df_ztubt_toto_placedbettransactionlineitem = pd.read_sql(query, connection)


    # ========== Extract data from ztubt_sweep_placedbettransactionlineitem ==========
    query = f"""
    SELECT * FROM {schema}.ztubt_sweep_placedbettransactionlineitem
    """
    df_ztubt_sweep_placedbettransactionlineitem = pd.read_sql(query, connection)
    # ================================== TRANSFORMATION LOGIC =================================
    # ================================== 1.1 4D RESULT VALIDATION EXCHANGE ====================================
    df_temp = df_ztubt_validatedbetticket\
                .merge(df_ztubt_4d_placedbettransactionlineitem,on='TRANHEADERID', how='inner')\
                .merge(df_ztubt_validatedbetticketlifecyclestate,on='TRANHEADERID', how='inner')\
                .merge(df_ubt_temp_placebettransactiontype,on='TRANHEADERID', how='inner',suffixes=('_VB', ''))
    ## Filter ProdID in (2) and isexchangeticket = TRUE
    df_temp = df_temp[
        (df_temp['PRODID_VB'].isin([2])) &
        (df_temp['ISEXCHANGETICKET'] == True)
    ].reset_index(drop=True)\
    .drop(columns=['TICKETSERIALNUMBER'], axis=1)\
    .rename(columns={
        'TICKETSERIALNUMBER_VB': 'TICKETSERIALNUMBER'})\
    .reset_index(drop=True)\
    [['TICKETSERIALNUMBER', 'TRANLINEITEMID', 'WINNINGAMOUNT', 'REFUNDAMOUNT', 'REBATERECLAIM']]
    df_ubt_temp_resultvalidationexchange = pd.concat([df_ubt_temp_resultvalidationexchange, df_temp], ignore_index=True)
    print(f"UBT_TEMP_RESULTVALIDATIONEXCHANGE : COMPLETED 1.1 (Line 1484 - 1489) with {df_temp.shape[0]} rows")

    # ================================== 1.2 TOTO RESULT VALIDATION EXCHANGE ====================================
    df_temp = df_ztubt_validatedbetticket\
                .merge(df_ztubt_toto_placedbettransactionlineitem,on='TRANHEADERID', how='inner')\
                .merge(df_ztubt_validatedbetticketlifecyclestate,on='TRANHEADERID', how='inner')\
                .merge(df_ubt_temp_placebettransactiontype,on='TRANHEADERID', how='inner',suffixes=('_VB', ''))
    ## Filter ProdID in (3) and isexchangeticket = TRUE
    df_temp = df_temp[
        (df_temp['PRODID_VB'].isin([3])) &
        (df_temp['ISEXCHANGETICKET'] == True)
    ].reset_index(drop=True)\
    .drop(columns=['TICKETSERIALNUMBER'], axis=1)\
    .rename(columns={
        'TICKETSERIALNUMBER_VB': 'TICKETSERIALNUMBER'})\
    .reset_index(drop=True)\
    [['TICKETSERIALNUMBER', 'TRANLINEITEMID', 'WINNINGAMOUNT', 'REFUNDAMOUNT', 'REBATERECLAIM']]
    df_ubt_temp_resultvalidationexchange = pd.concat([df_ubt_temp_resultvalidationexchange, df_temp], ignore_index=True)
    print(f"UBT_TEMP_RESULTVALIDATIONEXCHANGE : COMPLETED 1.2 (Line 1493 - 1498) with {df_temp.shape[0]} rows")

    # ================================== 1.3 SWEEP RESULT VALIDATION EXCHANGE ====================================
    df_temp = df_ztubt_validatedbetticket\
                .merge(df_ztubt_sweep_placedbettransactionlineitem,on='TRANHEADERID', how='inner')\
                .merge(df_ztubt_validatedbetticketlifecyclestate,on='TRANHEADERID', how='inner')\
                .merge(df_ubt_temp_placebettransactiontype,on='TRANHEADERID', how='inner',suffixes=('_VB', ''))
    ## Filter ProdID in (4) and isexchangeticket = TRUE
    df_temp = df_temp[
        (df_temp['PRODID_VB'].isin([4])) &
        (df_temp['ISEXCHANGETICKET'] == True)
    ].reset_index(drop=True)\
    .drop(columns=['TICKETSERIALNUMBER'], axis=1)\
    .rename(columns={
        'TICKETSERIALNUMBER_VB': 'TICKETSERIALNUMBER'})\
    .reset_index(drop=True)\
    [['TICKETSERIALNUMBER', 'TRANLINEITEMID', 'WINNINGAMOUNT', 'REFUNDAMOUNT', 'REBATERECLAIM']]
    df_ubt_temp_resultvalidationexchange = pd.concat([df_ubt_temp_resultvalidationexchange, df_temp], ignore_index=True)
    print(f"UBT_TEMP_RESULTVALIDATIONEXCHANGE : COMPLETED 1.3 (Line 1502 - 1507) with {df_temp.shape[0]} rows")
    #================================= END TRANSFORMATION LOGIC =================================
    return df_ubt_temp_resultvalidationexchange

def ubt_temp_bettype(df_ubt_temp_placebettransactiontype, startdateUTC, enddateUTC):
    df_ubt_temp_bettype = pd.DataFrame()

    # ================================== EXTRACT DATA FROM TABLES =================================
    ## Extract from ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEM & ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEMNUMBER
    query = f"""
    SELECT
        PBTLI.TRANLINEITEMID,
        CASE
            WHEN PBTLI.BETTYPEID = 'ORD' THEN
                CASE
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) > 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) = 0 THEN '0'
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) = 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) > 0 THEN '5'
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) > 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) > 0 THEN '10'
                END
            WHEN PBTLI.BETTYPEID = 'SYS' THEN
                CASE
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) > 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) = 0 THEN
                        CASE
                            WHEN PBTLI.PERMUTATION = 24 THEN '1'
                            WHEN PBTLI.PERMUTATION = 12 THEN '2'
                            WHEN PBTLI.PERMUTATION = 4  THEN '3'
                            WHEN PBTLI.PERMUTATION = 6  THEN '4'
                        END
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) = 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) > 0 THEN
                        CASE
                            WHEN PBTLI.PERMUTATION = 24 THEN '6'
                            WHEN PBTLI.PERMUTATION = 12 THEN '7'
                            WHEN PBTLI.PERMUTATION = 4  THEN '8'
                            WHEN PBTLI.PERMUTATION = 6  THEN '9'
                        END
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) > 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) > 0 THEN
                        CASE
                            WHEN PBTLI.PERMUTATION = 24 THEN '11'
                            WHEN PBTLI.PERMUTATION = 12 THEN '12'
                            WHEN PBTLI.PERMUTATION = 4  THEN '13'
                            WHEN PBTLI.PERMUTATION = 6  THEN '14'
                        END
                END
            WHEN PBTLI.BETTYPEID = 'IBET' THEN
                CASE
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) > 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) = 0 THEN
                        CASE
                            WHEN PBTLI.PERMUTATION = 24 THEN '27'
                            WHEN PBTLI.PERMUTATION = 12 THEN '28'
                            WHEN PBTLI.PERMUTATION = 4  THEN '29'
                            WHEN PBTLI.PERMUTATION = 6  THEN '30'
                        END
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) = 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) > 0 THEN
                        CASE
                            WHEN PBTLI.PERMUTATION = 24 THEN '31'
                            WHEN PBTLI.PERMUTATION = 12 THEN '32'
                            WHEN PBTLI.PERMUTATION = 4  THEN '33'
                            WHEN PBTLI.PERMUTATION = 6  THEN '34'
                        END
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) > 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) > 0 THEN
                        CASE
                            WHEN PBTLI.PERMUTATION = 24 THEN '35'
                            WHEN PBTLI.PERMUTATION = 12 THEN '36'
                            WHEN PBTLI.PERMUTATION = 4  THEN '37'
                            WHEN PBTLI.PERMUTATION = 6  THEN '38'
                        END
                END
            WHEN PBTLI.BETTYPEID = 'ROLL' THEN
                CASE
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) > 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) = 0 THEN
                        CASE
                            WHEN POSITION('R' IN PBTLIN.SELECTEDBETNUMBER) = 1 THEN '15'
                            WHEN POSITION('R' IN PBTLIN.SELECTEDBETNUMBER) = 2 THEN '16'
                            WHEN POSITION('R' IN PBTLIN.SELECTEDBETNUMBER) = 3 THEN '17'
                            ELSE '18'
                        END
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) = 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) > 0 THEN
                        CASE
                            WHEN POSITION('R' IN PBTLIN.SELECTEDBETNUMBER) = 1 THEN '19'
                            WHEN POSITION('R' IN PBTLIN.SELECTEDBETNUMBER) = 2 THEN '20'
                            WHEN POSITION('R' IN PBTLIN.SELECTEDBETNUMBER) = 3 THEN '21'
                            ELSE '22'
                        END
                    WHEN COALESCE(PBTLIN.BIGBETACCEPTEDWAGER, 0) > 0
                    AND COALESCE(PBTLIN.SMALLBETACCEPTEDWAGER, 0) > 0 THEN
                        CASE
                            WHEN POSITION('R' IN PBTLIN.SELECTEDBETNUMBER) = 1 THEN '23'
                            WHEN POSITION('R' IN PBTLIN.SELECTEDBETNUMBER) = 2 THEN '24'
                            WHEN POSITION('R' IN PBTLIN.SELECTEDBETNUMBER) = 3 THEN '25'
                            ELSE '26'
                        END
                END
        END AS BETTYPEID,
        PBTLIN.BIGBETACCEPTEDWAGER,
        PBTLIN.SMALLBETACCEPTEDWAGER,
        PBTLIN.CREATEDDATE,
        PBTLI.TRANHEADERID
    FROM {schema}.ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEM PBTLI
    INNER JOIN {schema}.ZTUBT_4D_PLACEDBETTRANSACTIONLINEITEMNUMBER PBTLIN
        ON PBTLI.TRANLINEITEMID = PBTLIN.TRANLINEITEMID
    """
    df_4D = pd.read_sql(query, connection)
    # ===============================================================================================
    query = f"""
    SELECT *
    FROM {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM
    """
    df_toto_placedbettransactionlineitem = pd.read_sql(query, connection)
    # ===============================================================================================

    query = f"""
    SELECT *
    FROM {schema}.ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER
    """
    df_sweep_placedbettransactionlineitemnumber = pd.read_sql(query, connection)


    query = f"""
    SELECT
        CASE SCAT.CATEGORYID
            WHEN 1 THEN
                CASE PBTLI.ACCUMULATORID
                    WHEN 'ACC4' THEN '4F'
                    WHEN 'DBL' THEN 'D'
                    WHEN 'SGL' THEN 'S'
                    WHEN 'TBL' THEN 'T'
                END
            WHEN 2 THEN 'S'
        END AS BETTYPEID
        , PBTLIN.TRANLINEITEMID
        , PBTLI.TRANHEADERID
        , SCAT.CATEGORYID

    FROM {schema}.ZTUBT_SPORTS_PLACEDBETTRANSACTIONLINEITEM PBTLI
    INNER JOIN {schema}.ZTUBT_SPORTS_PLACEDBETTRANSACTIONLINEITEMNUMBER PBTLIN
        ON PBTLI.TRANLINEITEMID = PBTLIN.TRANLINEITEMID
    INNER JOIN {schema}.ZTUBT_SPORTEVENT SE
        ON PBTLIN.EVENTID = SE.EVENTID
    INNER JOIN {schema}.ZTUBT_SPORTTYPE ST
        ON SE.TYPEID = ST.TYPEID
    INNER JOIN {schema}.ZTUBT_SPORTCLASS SC
        ON ST.CLASSID = SC.CLASSID
    INNER JOIN {schema}.ZTUBT_SPORTCATEGORY SCAT
        ON SC.CATEGORYID = SCAT.CATEGORYID
    """
    df_sport = pd.read_sql(query, connection)
    # ================================== TRANSFORMATION =================================
    #
    df_temp = pd.merge(df_ubt_temp_placebettransactiontype, df_4D, on="TRANHEADERID", how="inner")
    df_temp = df_temp[
        (df_temp["PRODID"] == 2)
        &
        (
            ((df_temp["BIGBETACCEPTEDWAGER"].fillna(0))
            + (df_temp["SMALLBETACCEPTEDWAGER"].fillna(0))
            > 0)
        )
        &
        (
            (
                (df_temp["CREATEDDATE"] >= startdateUTC)
                & (df_temp["CREATEDDATE"] <= enddateUTC)
                & (df_temp["TRANSACTIONTYPETOTAL"].isin([1, 4, 6]))
            )
            | (df_temp["TRANSACTIONTYPETOTAL"].isin([3, 5]))
        )
    ]
    df_temp = df_temp[["TICKETSERIALNUMBER", "TRANLINEITEMID", "BETTYPEID"]]
    df_ubt_temp_bettype = df_temp
    print(f"UBT_TEMP_BETTYPE : COMPLETED 4D with {df_temp.shape[0]} rows")


    ## TOTO
    df_temp = pd.merge(df_ubt_temp_placebettransactiontype, df_toto_placedbettransactionlineitem, on="TRANHEADERID", how="inner")
    df_temp = df_temp.loc[:, ~df_temp.columns.duplicated()]
    df_temp = df_temp[
        (df_temp['PRODID'] == 3) &
        (
            ((
                df_temp['CREATEDDATE'] >= startdateUTC)
                & (df_temp['CREATEDDATE'] <= enddateUTC)
                & (df_temp['TRANSACTIONTYPETOTAL'].isin([1, 4, 6])
                   )) |
            (df_temp['TRANSACTIONTYPETOTAL'].isin([3, 5]))
        )
    ]
    bettype_map = {
        'ORD': '0',
        'SYSR': '1',
        'SYS7': '2',
        'SYS8': '3',
        'SYS9': '4',
        'SYS10': '5',
        'SYS11': '6',
        'SYS12': '7',
        'M 4': '16',
        'M 3': '17',
        'M 2': '18',
        'M AN': '19'
    }

    df_temp['BETTYPEID'] = df_temp['BETTYPEID'].map(bettype_map)
    df_temp = df_temp[["TICKETSERIALNUMBER", "TRANLINEITEMID", "BETTYPEID"]]
    df_ubt_temp_bettype = pd.concat([df_ubt_temp_bettype, df_temp], ignore_index=True)
    print(f"UBT_TEMP_BETTYPE : COMPLETED TOTO with {df_temp.shape[0]} rows")

    ## SWEEP
    df_temp = pd.merge(df_ubt_temp_placebettransactiontype, df_sweep_placedbettransactionlineitemnumber, on="TRANHEADERID", how="inner")
    df_temp = df_temp[
        (df_temp['PRODID'] == 4) &
        (df_temp['ISSOLDOUT'] != True) &
        (
            ((df_temp['CREATEDDATE'] >= startdateUTC) & (df_temp['CREATEDDATE'] <= enddateUTC) &
            (df_temp['TRANSACTIONTYPETOTAL'].isin([1, 4, 6]))) |
            (df_temp['TRANSACTIONTYPETOTAL'].isin([3, 5]))
        )
    ]
    df_temp = (
            df_temp
            .assign(BETTYPEID="0")
            [["TICKETSERIALNUMBER", "TRANLINEITEMID", "BETTYPEID"]]
        )
    df_ubt_temp_bettype = pd.concat([df_ubt_temp_bettype, df_temp], ignore_index=True)
    print(f"UBT_TEMP_BETTYPE : COMPLETED SWEEP with {df_temp.shape[0]} rows")
    ## SPORT


    df_temp = pd.merge(df_ubt_temp_placebettransactiontype, df_sport, on="TRANHEADERID", how="inner")
    df_temp = df_temp[
        (df_temp['CATEGORYID'] == 1) |
        ((df_temp['CATEGORYID'] == 2) & df_temp['PRODID'].isin([5, 6]))
    ]
    df_temp = df_temp[["TICKETSERIALNUMBER", "TRANLINEITEMID", "BETTYPEID"]]
    df_ubt_temp_bettype = pd.concat([df_ubt_temp_bettype, df_temp], ignore_index=True)
    df_ubt_temp_bettype = df_ubt_temp_bettype.rename(columns={
        'TRANLINEITEMID' : 'TRANLINEITEMNUMBERID'
    })
    print(f"UBT_TEMP_BETTYPE : COMPLETED SPORT with {df_temp.shape[0]} rows")
    return df_ubt_temp_bettype






def ubt_temp_numboard(df_ubt_temp_placebettransactiontype, startdateUTC, enddateUTC):
    df_ubt_temp_numboard = pd.DataFrame(
    {
        "TICKETSERIALNUMBER": pd.Series(dtype="string"),
        "NUMBOARDS": pd.Series(dtype="int32"),
        "BULKID": pd.Series(dtype="string")
    }
    )
    # --- 1. 4D ---
    df_4d = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype['PRODID'] == 2]
        .merge(df_4d_placedbettransactionlineitemnumber, on='TRANHEADERID', how='inner')
    )
    df_4d = df_4d[(
        (df_4d['BIGBETACCEPTEDWAGER'].fillna(0) + df_4d['SMALLBETACCEPTEDWAGER'].fillna(0) > 0)
        & (
            ((df_4d['CREATEDDATE'] >= startdateUTC) & (df_4d['CREATEDDATE'] <= enddateUTC) &
            df_4d['TRANSACTIONTYPETOTAL'].isin([1,4,6]))
            | df_4d['TRANSACTIONTYPETOTAL'].isin([3,5])
        )
    )]

    df_4d = df_4d.groupby('TICKETSERIALNUMBER', as_index=False,dropna=False)\
        .agg({
        'TRANLINEITEMID': 'count'
    })
    df_4d = (
        df_4d
        .rename(columns={'TRANLINEITEMID': 'NUMBOARDS'})
        .assign(BULKID=None)
        [ ["TICKETSERIALNUMBER", "NUMBOARDS", "BULKID"]]
    )

    # --- 2. Toto ---
    df_toto = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype['PRODID'] == 3]
        .merge(df_toto_placedbettransactionlineitem, on='TRANHEADERID', how='inner')
    )
    df_toto = df_toto[(
        ((df_toto['CREATEDDATE'] >= startdateUTC) & (df_toto['CREATEDDATE'] <= enddateUTC) &
        df_toto['TRANSACTIONTYPETOTAL'].isin([1,4,6]))
        | df_toto['TRANSACTIONTYPETOTAL'].isin([3,5])
    )]
    df_toto = df_toto.groupby('TICKETSERIALNUMBER', as_index=False,dropna=False)\
        .agg({
        'TRANLINEITEMID':'count'
    })
    df_toto = (
        df_toto
        .rename(columns={'TRANLINEITEMID': 'NUMBOARDS'})
        .assign(BULKID=None)
        [ ["TICKETSERIALNUMBER", "NUMBOARDS", "BULKID"]]
    )

    # --- 3. Sweep ---
    df_sweep = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype['PRODID'] == 4]
        .merge(df_sweep_placedbettransactionlineitem, on='TRANHEADERID', how='inner', suffixes=('_PBT', '_PL'))
        .merge(df_sweep_placedbettransactionlineitemnumber, on='TRANLINEITEMID', how='inner', suffixes=('_PL', '_PLN'))
    )
    df_sweep = df_sweep[(
        (df_sweep['ISSOLDOUT'] != True)
        & (
            ((df_sweep['CREATEDDATE_PLN'] >= startdateUTC) & (df_sweep['CREATEDDATE_PLN'] <= enddateUTC) &
            df_sweep['TRANSACTIONTYPETOTAL'].isin([1,4,6]))
            | df_sweep['TRANSACTIONTYPETOTAL'].isin([3,5])
        )
    )]
    df_sweep = df_sweep.groupby(['TICKETSERIALNUMBER','BULKID'], as_index=False,dropna=False)\
        .agg({
        'TRANLINEITEMID':'count'
    })
    df_sweep = (
        df_sweep
        .rename(columns={'TRANLINEITEMID': 'NUMBOARDS'})
        [ ["TICKETSERIALNUMBER", "NUMBOARDS", "BULKID"]]
    )

    # --- 4. UNION ALL ---
    df_ubt_temp_numboard = pd.concat([df_4d, df_toto, df_sweep], ignore_index=True)


    return df_ubt_temp_numboard

def  ubt_temp_numdraw(df_ubt_temp_lotterydraw):
    df_ubt_temp_numdraw = pd.DataFrame()

    df_ubt_temp_numdraw = (
    df_ubt_temp_lotterydraw
    .groupby("TICKETSERIALNUMBER", as_index=False,dropna=False)
    .agg(
        NUMDRAWS=("HOSTDRAWDATESID", "count"),
        DRAWID=("HOSTDRAWDATESID", lambda x: ",".join(x.astype(str)))
    )
    .astype({
        "TICKETSERIALNUMBER": "string",
        "NUMDRAWS": "int",
        "DRAWID": "string"
    })
)
    return df_ubt_temp_numdraw

def ubt_temp_TotoSeq(df_ubt_temp_placebettransactiontype,startdateUTC, enddateUTC):
    df_ubt_temp_totoseq = (
    df_ubt_temp_placebettransactiontype.merge(df_toto_placedbettransactionlineitem, on='TRANHEADERID', how='inner')
            .merge(df_toto_placedbettransactionlineitemnumber, on='TRANLINEITEMID', how='inner')
    )
    df_ubt_temp_totoseq = df_ubt_temp_totoseq[(
        (df_ubt_temp_totoseq['PRODID'] == 3) &
        (
            (
                (df_ubt_temp_totoseq['CREATEDDATE'] >= startdateUTC) &
                (df_ubt_temp_totoseq['CREATEDDATE'] <= enddateUTC) &
                (df_ubt_temp_totoseq['TRANSACTIONTYPETOTAL'].isin([1, 4, 6]))
            ) |
            (df_ubt_temp_totoseq['TRANSACTIONTYPETOTAL'].isin([3, 5]))
        )
    )]
    df_ubt_temp_totoseq = (
        df_ubt_temp_totoseq
        .groupby('TRANLINEITEMID', as_index=False,dropna=False)
        .agg(SEQ=('SEQUENCE', 'max'))
    )

    return df_ubt_temp_totoseq

def ubt_temp_totogroup(df_ubt_temp_placebettransactiontype):
    df_ubt_temp_totogroup = pd.DataFrame()

    df_ubt_temp_totogroup = (
    df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype['PRODID'] == 3]
    .merge(
        df_toto_placedbettransactionlineitem[
            (df_toto_placedbettransactionlineitem['GROUPHOSTID'] != '00000000-0000-0000-0000-000000000000') &
            (df_toto_placedbettransactionlineitem['GROUPUNITSEQUENCE'].notna())
        ],
        on='TRANHEADERID',
        how='inner',
        suffixes=('_PBT', '_LI')
    )
    .merge(df_toto_placedbettransactionlineitem, on='GROUPHOSTID', how='inner', suffixes=('_LI', '_LI2'))
    .groupby('GROUPHOSTID', as_index=False,dropna=False)['GROUPUNITSEQUENCE_LI'].max()
    .rename(columns={'GROUPUNITSEQUENCE_LI': 'GROUPTOTO'})
)
    return df_ubt_temp_totogroup

def ubt_temp_lotterydetails(df_ubt_temp_placebettransactiontype,df_ubt_temp_numboard, df_ubt_temp_numdraw,df_ubt_temp_TotoSeq,df_ubt_temp_totogroup , startdateUTC, enddateUTC):

    df_ubt_temp_lotterydetails = pd.DataFrame({
    'TICKETSERIALNUMBER': pd.Series(dtype='string'),
    'TRANLINEITEMID': pd.Series(dtype='string'),
    'BOARDSEQNUMBER': pd.Series(dtype='int'),
    'ENTRYMETHOD': pd.Series(dtype='int'),
    'NUMBOARDS': pd.Series(dtype='int'),
    'NUMDRAWS': pd.Series(dtype='int'),
    'DRAWID': pd.Series(dtype='string'),
    'EBETSLIPINFO_MOBNBR': pd.Series(dtype='string'),
    'NUMSIMPLEBETS': pd.Series(dtype='int'),
    'QPFLAG': pd.Series(dtype='int'),
    'NUMMARKS': pd.Series(dtype='int'),
    'BULKID': pd.Series(dtype='string'),
    'ITOTO_NUMPARTS': pd.Series(dtype='string'),
    'ITOTO_TOTALPARTS': pd.Series(dtype='string'),
    'GRPTOTO_NUMPARTS': pd.Series(dtype='string')
})
    df_temp = df_ubt_temp_placebettransactiontype.merge(
        df_4d_placedbettransactionlineitem,
        on='TRANHEADERID',
        how='inner'
    ).merge(
        df_4d_placedbettransactionlineitemnumber,
        on='TRANLINEITEMID',
        how='inner'
    ).merge(
        df_ubt_temp_numboard,
        on='TICKETSERIALNUMBER',
        how='inner'
    ).merge(
        df_ubt_temp_numdraw,
        on='TICKETSERIALNUMBER',
        how='inner'
    )
    df_temp_1 = df_temp[
        (df_temp['PRODID'] == 2) &
        ((df_temp['BIGBETACCEPTEDWAGER'].fillna(0) + df_temp['SMALLBETACCEPTEDWAGER'].fillna(0)) > 0) &
        (df_temp['CREATEDDATE'] >= startdateUTC) &
        (df_temp['CREATEDDATE'] <= enddateUTC) &
        (df_temp['TRANSACTIONTYPETOTAL'].isin([1, 4, 6]))
    ]
    df_temp_2 = df_temp[
        (df_temp["PRODID"] == 2)
        & ((df_temp["BIGBETACCEPTEDWAGER"].fillna(0)
            + df_temp["SMALLBETACCEPTEDWAGER"].fillna(0)) > 0)
        & (df_temp["TRANSACTIONTYPETOTAL"].isin([3, 5]))
    ]
    def select_fields_4D(df):
        df["BOARDSEQNUMBER"] = (
            df.sort_values("TRANLINEITEMID")
            .groupby("TICKETSERIALNUMBER", as_index=False, dropna=False)
            .cumcount() + 1
        )
        df["ENTRYMETHOD"] = df["ENTRYMETHODID"].map({
            "Manual": 0,
            "BetSlip": 2,
            "Edit": 10,
            "EBetSlip": 16
        })
        df["NUMSIMPLEBETS"] = df["PERMUTATION"].fillna(1).replace(0, 1)
        df["QPFLAG"] = df["QUICKPICKINDICATOR"].astype(int)
        df["EBETSLIPINFO_MOBNBR"] = df["DEVICEID"]
        df = (
            df
            .assign(ITOTO_NUMPARTS=None, ITOTO_TOTALPARTS=None, GRPTOTO_NUMPARTS=None, NUMMARKS=4)
            [['TICKETSERIALNUMBER','TRANLINEITEMID','BOARDSEQNUMBER','ENTRYMETHOD','NUMBOARDS','NUMDRAWS','DRAWID',
            'EBETSLIPINFO_MOBNBR','NUMSIMPLEBETS','QPFLAG','NUMMARKS','BULKID','ITOTO_NUMPARTS','ITOTO_TOTALPARTS','GRPTOTO_NUMPARTS']]
        )
        return df
    df_ubt_temp_lotterydetails = pd.concat(
        [df_ubt_temp_lotterydetails, select_fields_4D(df_temp_1), select_fields_4D(df_temp_2)], ignore_index=True)


    #--------------------------------------------------------------------TOTO----------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------------------------------------------------------
    df_temp = df_ubt_temp_placebettransactiontype.merge(
        df_toto_placedbettransactionlineitem,
        on='TRANHEADERID',
        how='inner'
    ).merge(
        df_ubt_temp_TotoSeq,
        on='TRANLINEITEMID',
        how='inner'
    ).merge(
        df_ubt_temp_totogroup,
        on='GROUPHOSTID',
        how='left'
    ).merge(
        df_ubt_temp_numboard,
        on='TICKETSERIALNUMBER',
        how='inner'
    ).merge(
        df_ubt_temp_numdraw,
        on='TICKETSERIALNUMBER',
        how='inner'
    )
    df_temp_1 = df_temp[
        (df_temp['PRODID'] == 3) &
        (df_temp['CREATEDDATE'] >= startdateUTC) &
        (df_temp['CREATEDDATE'] <= enddateUTC) &
        (df_temp['TRANSACTIONTYPETOTAL'].isin([1, 4, 6]))
    ]
    df_temp_2 = df_temp[
        (df_temp['PRODID'] == 3) &
        (df_temp['TRANSACTIONTYPETOTAL'].isin([3, 5]))
    ]
    def select_fields_TOTO(df):
        df["BOARDSEQNUMBER"] = (
            df.sort_values("TRANLINEITEMID")
            .groupby("TICKETSERIALNUMBER", as_index=False, dropna=False)
            .cumcount() + 1
        )
        df["ENTRYMETHOD"] = df["ENTRYMETHODID"].map({
            "Manual": 0,
            "BetSlip": 2,
            "Edit": 10,
            "EBetSlip": 16
        })
        v_totomatchbettypes = "'M AN', 'M 2', 'M 3', 'M 4'"
        df['NUMSIMPLEBETS'] = (
            df['BETTYPEID'].apply(
                lambda x: 1   if x in v_totomatchbettypes else
                        1   if x == 'ORD'  else
                        44  if x == 'SYSR' else
                        7   if x == 'SYS7' else
                        28  if x == 'SYS8' else
                        84  if x == 'SYS9' else
                        210 if x == 'SYS10' else
                        462 if x == 'SYS11' else
                        924 if x == 'SYS12' else
                        None
            )
        )

        df["QPFLAG"] = df["QUICKPICKINDICATOR"].astype(int)
        df['ITOTO_NUMPARTS']   = df['UNITS'].astype(str)
        df['ITOTO_TOTALPARTS'] = df['SYNDICATEPARTSALLOWED'].astype(str)
        df['GRPTOTO_NUMPARTS'] = df['GROUPTOTO'].astype(str)
        df["NUMMARKS"] = df["SEQ"]
        df["EBETSLIPINFO_MOBNBR"] = df["DEVICEID"]
        df = (
            df
            [['TICKETSERIALNUMBER','TRANLINEITEMID','BOARDSEQNUMBER','ENTRYMETHOD','NUMBOARDS','NUMDRAWS','DRAWID',
            'EBETSLIPINFO_MOBNBR','NUMSIMPLEBETS','QPFLAG','NUMMARKS','BULKID','ITOTO_NUMPARTS','ITOTO_TOTALPARTS','GRPTOTO_NUMPARTS']]
        )
        return df
    df_ubt_temp_lotterydetails = pd.concat(
        [df_ubt_temp_lotterydetails, select_fields_TOTO(df_temp_1), select_fields_TOTO(df_temp_2)], ignore_index=True)


    #--------------------------------------------------------------------SWEEP----------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------------------------------------------------------
    df_temp = df_ubt_temp_placebettransactiontype.merge(
        df_sweep_placedbettransactionlineitem,
        on='TRANHEADERID',
        how='inner',
        suffixes=('_PBVBCBT', '_SWEEP')
    ).merge(
        df_sweep_placedbettransactionlineitemnumber,
        on='TRANLINEITEMID',
        how='inner',
        suffixes=('_SWEEP', '_PBTLIN')
    ).merge(
        df_ubt_temp_numboard,
        on='TICKETSERIALNUMBER',
        how='inner',
        suffixes=('_PBVBCBT', '_NB')
    ).merge(
        df_ubt_temp_numdraw,
        on='TICKETSERIALNUMBER',
        how='inner',
        suffixes=('_PBVBCBT', '_ND')
    )

    df_temp_1 = df_temp[
        (df_temp['PRODID'] == 4) &
        (df_temp['ISSOLDOUT'] != True) &
        (df_temp['CREATEDDATE_PBTLIN'] >= startdateUTC) &
        (df_temp['CREATEDDATE_PBTLIN'] <= enddateUTC) &
        (df_temp['TRANSACTIONTYPETOTAL'].isin([1, 4, 6]))
    ]
    df_temp_2 = df_temp[
        (df_temp['PRODID'] == 4) &
        (df_temp['ISSOLDOUT'] != True) &
        (df_temp['TRANSACTIONTYPETOTAL'].isin([3, 5]))
    ]
    def select_fields_SWEEP(df):
        df["BOARDSEQNUMBER"] = (
            df.sort_values("TRANLINEITEMID")
            .groupby("TICKETSERIALNUMBER", as_index=False, dropna=False)
            .cumcount() + 1
        )
        df["ENTRYMETHOD"] = df["ENTRYMETHODID"].map({
            "Manual": 0,
            "BetSlip": 2,
            "Edit": 10,
            "EBetSlip": 16
        })

        df["QPFLAG"] = df["QUICKPICKINDICATOR"].astype(int)
        df['NUMMARKS'] = df['USERSELECTEDNUMBER'].astype(str).str.len()
        df["EBETSLIPINFO_MOBNBR"] = df["DEVICEID"]
        df = (
            df
            .assign(ITOTO_NUMPARTS=None, ITOTO_TOTALPARTS=None, GRPTOTO_NUMPARTS=None, NUMSIMPLEBETS=1)
            .rename(columns={"BULKID_NB": "BULKID"})
            [['TICKETSERIALNUMBER','TRANLINEITEMID','BOARDSEQNUMBER','ENTRYMETHOD','NUMBOARDS','NUMDRAWS','DRAWID',
            'EBETSLIPINFO_MOBNBR','NUMSIMPLEBETS','QPFLAG','NUMMARKS','BULKID','ITOTO_NUMPARTS','ITOTO_TOTALPARTS','GRPTOTO_NUMPARTS']]
        )
        return df
    df_ubt_temp_lotterydetails = pd.concat(
        [df_ubt_temp_lotterydetails, select_fields_SWEEP(df_temp_1), select_fields_SWEEP(df_temp_2)], ignore_index=True)

    return df_ubt_temp_lotterydetails

def ubt_temp_resultwager(df_ubt_temp_placebettransactiontype, startdateUTC, enddateUTC):
    df_resultwager = pd.DataFrame({
        "PRODID": pd.Series(dtype="int32"),
        "TICKETSERIALNUMBER": pd.Series(dtype="string"),
        "TRANLINEITEMID": pd.Series(dtype="string"),
        "WAGER": pd.Series(dtype="float64"),
        "SECONDWAGER": pd.Series(dtype="float64")
    })
    # ==============================================
    # 1. HORSE (PRODID = 1)
    # ==============================================

    df_horse = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype["PRODID"] == 1]
        .merge(df_horse_placedbettransactionlineitem, on="TRANHEADERID", how="inner")
        .pipe(
            lambda df: df[
                (
                    (df["CREATEDDATE"] >= startdateUTC)
                    & (df["CREATEDDATE"] <= enddateUTC)
                    & (df["TRANSACTIONTYPETOTAL"].isin([1, 4, 6]))
                )
                | (df["TRANSACTIONTYPETOTAL"].isin([3, 5]))
            ]
        )
        .assign(
            AMOUNT=lambda df: df["BETPRICEAMOUNT"].fillna(0),
        )
        .groupby("TICKETSERIALNUMBER", as_index=False,dropna=False)
        .agg(WAGER=("AMOUNT", "sum"))
        .assign(PRODID=1, TRANLINEITEMID=None, SECONDWAGER=0)
        [["PRODID", "TICKETSERIALNUMBER", "TRANLINEITEMID", "WAGER", "SECONDWAGER"]]
    )


    # ==============================================
    # 2. 4D (PRODID = 2)
    # ==============================================

    df_4d = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype["PRODID"] == 2]
        .merge(df_4d_placedbettransactionlineitemnumber, on="TRANHEADERID", how="inner")
        .merge(df_ztubt_drawdates, on="TRANHEADERID", how="inner")
        .pipe(
            lambda df: df[
                (
                    (df["CREATEDDATE"] >= startdateUTC)
                    & (df["CREATEDDATE"] <= enddateUTC)
                    & (df["TRANSACTIONTYPETOTAL"].isin([1,4,6]))
                )
                | (df["TRANSACTIONTYPETOTAL"].isin([3,5]))
            ]
        )
        .groupby(["TICKETSERIALNUMBER", "TRANLINEITEMID"], as_index=False,dropna=False)
        .agg(
            WAGER=("BIGBETACCEPTEDWAGER", "sum"),
            SECONDWAGER=("SMALLBETACCEPTEDWAGER", "sum")
        )
        .assign(PRODID=2)[["PRODID", "TICKETSERIALNUMBER", "TRANLINEITEMID", "WAGER", "SECONDWAGER"]]
    )


    # ==============================================
    # 3. TOTO (PRODID = 3)
    # ==============================================

    df_toto = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype["PRODID"] == 3]
        .merge(df_toto_placedbettransactionlineitem, on="TRANHEADERID", how="inner")
        .pipe(
            lambda df: df[
                (
                    (df["CREATEDDATE"] >= startdateUTC)
                    & (df["CREATEDDATE"] <= enddateUTC)
                    & (df["TRANSACTIONTYPETOTAL"].isin([1,4,6]))
                )
                | (df["TRANSACTIONTYPETOTAL"].isin([3,5]))
            ]
        )
        .groupby(["TICKETSERIALNUMBER", "TRANLINEITEMID"], as_index=False,dropna=False)
        .agg(WAGER=("BETPRICEAMOUNT","sum"))
        .assign(PRODID=3, SECONDWAGER=0)[["PRODID","TICKETSERIALNUMBER","TRANLINEITEMID","WAGER","SECONDWAGER"]]
    )


    # ==============================================
    # 4. SWEEP (PRODID = 4)
    # ==============================================

    df_sweep = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype["PRODID"] == 4]
        .merge(df_sweep_placedbettransactionlineitem, on="TRANHEADERID", how="inner", suffixes=('_PBT', '_ZSP'))
        .merge(
            df_sweep_placedbettransactionlineitemnumber[df_sweep_placedbettransactionlineitemnumber["ISSOLDOUT"] == False],
            on="TRANLINEITEMID", how="inner", suffixes=('_ZSP', '_ZSPN')
        )
        .merge(df_placedbettransactionheaderlifecyclestate, left_on="TRANHEADERID_ZSP", right_on="TRANHEADERID", how="inner")
        .merge(df_ztubt_terminal, left_on="TERDISPLAYID_ZSP", right_on="TERDISPLAYID", how="inner")
        .merge(df_ztubt_location, on="LOCID", how="inner")
        .pipe(
            lambda df: df[
                (
                    (df["CREATEDDATE"] >= startdateUTC)
                    & (df["CREATEDDATE"] <= enddateUTC)
                    & (df["TRANSACTIONTYPETOTAL"].isin([1,4,6]))
                )
                | (df["TRANSACTIONTYPETOTAL"].isin([3,5]))
            ]
        )
        .groupby(["TICKETSERIALNUMBER","TRANLINEITEMID"], as_index=False,dropna=False)
        .agg(WAGER=("BETPRICEAMOUNT","sum"))
        .assign(PRODID=4, SECONDWAGER=0)[["PRODID","TICKETSERIALNUMBER","TRANLINEITEMID","WAGER","SECONDWAGER"]]
    )


    # ==============================================
    # 5. SPORTS (PRODID 5, 6)
    # ==============================================

    df_sport = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype["PRODID"].isin([5,6])]
        .merge(df_sports_placedbettransactionlineitem, on="TRANHEADERID", how="inner")
        .loc[lambda df: df["ISBETREJECTEDBYTRADER"] != True]
        .groupby(["TICKETSERIALNUMBER","PRODID"], as_index=False,dropna=False)
        .agg(WAGER=("BETAMOUNT","sum"))
        .assign(TRANLINEITEMID=None, SECONDWAGER=0)[["PRODID","TICKETSERIALNUMBER","TRANLINEITEMID","WAGER","SECONDWAGER"]]
    )


    # ==============================================
    # UNION ALL
    # ==============================================

    df_resultwager = pd.concat([df_resultwager, df_horse, df_4d, df_toto, df_sweep, df_sport], ignore_index=True)

    return df_resultwager

def ubt_temp_resultsalessandcomm(df_ubt_temp_resultsalescomwithdate):

    df_resultsalesandcomm = (
        df_ubt_temp_resultsalescomwithdate
        .groupby(["TICKETSERIALNUMBER", "TRANLINEITEMID"], as_index=False,dropna=False)
        .agg(
            SALES=("SALESFACTORAMOUNT", lambda x: x.fillna(0).sum()),
            SECONDSALES=("SECONDSALESFACTORAMOUNT", lambda x: x.fillna(0).sum()),
            SALESCOMM=("SALESCOMMAMOUNT", lambda x: x.fillna(0).sum()),
            SECONDSALESCOMM=("SECONDSALESCOMMAMOUNT", lambda x: x.fillna(0).sum())
        )
    )[["TICKETSERIALNUMBER", "TRANLINEITEMID", "SALES", "SECONDSALES", "SALESCOMM", "SECONDSALESCOMM"]]


    return df_resultsalesandcomm


def ubt_temp_resultgst(df_ubt_temp_resultwager, df_ubt_temp_placebettransactiontype, df_ubt_temp_resultsalessandcomm):

    df_ubt_temp_resultgst = pd.DataFrame({
        "TICKETSERIALNUMBER": pd.Series(dtype="str"),
        "TRANLINEITEMID": pd.Series(dtype="str"),
        "GST": pd.Series(dtype="float"),
        "SECONDGST": pd.Series(dtype="float")
    })
    #1: ProdID in (2,3,4)
    df_1 = (
        df_ubt_temp_resultwager
        .merge(df_ubt_temp_placebettransactiontype[["TICKETSERIALNUMBER","PRODID"]], on="TICKETSERIALNUMBER", suffixes=('_W', '_PBT'))
        .merge(df_ubt_temp_resultsalessandcomm, on=["TICKETSERIALNUMBER","TRANLINEITEMID"], suffixes=('_W', '_S'))
        .loc[lambda df: df["PRODID_W"].isin([2,3,4])]
        .assign(
            GST=lambda df: np.round(df["WAGER"].fillna(0) - df["SALES"].fillna(0), 2),
            SECONDGST=lambda df: np.round(df["SECONDWAGER"].fillna(0) - df["SECONDSALES"].fillna(0), 2)
        )[["TICKETSERIALNUMBER","TRANLINEITEMID","GST","SECONDGST"]]
    )

    #2: ProdID in (1,5,6)
    df_2 = (
        df_ubt_temp_resultwager
        .merge(df_ubt_temp_placebettransactiontype[["TICKETSERIALNUMBER","PRODID"]], on="TICKETSERIALNUMBER", suffixes=('_W', '_PBT'))
        .merge(df_ubt_temp_resultsalessandcomm, on="TICKETSERIALNUMBER", suffixes=('_W', '_S'))
        .loc[lambda df: df["PRODID_W"].isin([1,5,6])]
        .assign(
            GST=lambda df: np.round(df["WAGER"].fillna(0) - df["SALES"].fillna(0), 2),
            SECONDGST=lambda df: np.round(df["SECONDWAGER"].fillna(0) - df["SECONDSALES"].fillna(0), 2)
        )
        .rename(columns={'TRANLINEITEMID_W': 'TRANLINEITEMID'})
        [["TICKETSERIALNUMBER","TRANLINEITEMID","GST","SECONDGST"]]
    )

    # UNION ALL
    df_ubt_temp_resultgst = pd.concat([df_ubt_temp_resultgst, df_1, df_2], ignore_index=True)
    return df_ubt_temp_resultgst

def ubt_temp_resultvalidation(df_ubt_temp_resultwager):


    query = f"""
    SELECT *
    FROM {schema}.ZTUBT_VALIDATEDBETTICKET
    """
    df_ztubt_validatedbetticket = pd.read_sql(query, connection)

    query = f"""
    SELECT *
    FROM {schema}.ZTUBT_VALIDATEDBETTICKETLIFECYCLESTATE
    """
    df_ztubt_validatedbetticketlifecyclestate = pd.read_sql(query, connection)


    df_ubt_temp_resultvalidation = (
        df_ubt_temp_resultwager
        .merge(df_ztubt_validatedbetticket, on="TICKETSERIALNUMBER", how="inner")
        .merge(
            df_ztubt_validatedbetticketlifecyclestate.loc[lambda df: df["BETSTATETYPEID"]=="VB06"],
            on="TRANHEADERID", how="inner"
        )
        .loc[lambda df: df["VALIDATIONTYPEID"].isin(["VALD","RFND"])]
        .groupby(["TICKETSERIALNUMBER","WINNINGAMOUNT","REFUNDAMOUNT","REBATERECLAIM"], as_index=False,dropna=False)
        .agg(
            TOTAL_WAGER=("WAGER","sum"),
            TOTAL_SECONDWAGER=("SECONDWAGER","sum"),
            COUNT_TICKET=("TICKETSERIALNUMBER","count")
        )
        .assign(
            RETURNAMOUNT=lambda df: df["WINNINGAMOUNT"],
            WINNINGAMOUNT=lambda df: df.apply(
                lambda row: row["WINNINGAMOUNT"] - row["TOTAL_WAGER"]/max(row["COUNT_TICKET"],1)
                            - row["TOTAL_SECONDWAGER"]/max(row["COUNT_TICKET"],1)
                            if row["WINNINGAMOUNT"]>0 else 0,
                axis=1
            )
        )[["TICKETSERIALNUMBER","RETURNAMOUNT","WINNINGAMOUNT","REFUNDAMOUNT","REBATERECLAIM"]]
    )
    return df_ubt_temp_resultvalidation

def ubt_temp_transactionamount(df_ubt_temp_resultwager, df_ubt_temp_resultsalessandcomm,
                                                           df_ubt_temp_resultgst, df_ubt_temp_resultvalidation, df_ubt_temp_resultvalidationexchange):

    df_ubt_temp_transactionamount = pd.DataFrame({
    'TICKETSERIALNUMBER': pd.Series(dtype='object'),
    'TRANLINEITEMID': pd.Series(dtype='object'),
    'WAGER': pd.Series(dtype='float'),
    'SECONDWAGER': pd.Series(dtype='float'),
    'SALES': pd.Series(dtype='float'),
    'SECONDSALES': pd.Series(dtype='float'),
    'SALESCOMM': pd.Series(dtype='float'),
    'SECONDSALESCOMM': pd.Series(dtype='float'),
    'GST': pd.Series(dtype='float'),
    'SECONDGST': pd.Series(dtype='float'),
    'RETURNAMOUNT': pd.Series(dtype='float'),
    'WINNINGAMOUNT': pd.Series(dtype='float'),
    'REFUNDAMOUNT': pd.Series(dtype='float'),
    'REBATERECLAIM': pd.Series(dtype='float')
})

    df_temp_1 = (
        df_ubt_temp_resultwager[df_ubt_temp_resultwager['PRODID'].isin([2,3,4])]
        .merge(df_ubt_temp_resultsalessandcomm, on=['TICKETSERIALNUMBER','TRANLINEITEMID'], how='left')
        .merge(df_ubt_temp_resultgst, on=['TICKETSERIALNUMBER','TRANLINEITEMID'], how='left')
        .merge(df_ubt_temp_resultvalidation[['TICKETSERIALNUMBER','RETURNAMOUNT','WINNINGAMOUNT','REFUNDAMOUNT','REBATERECLAIM']],
            on='TICKETSERIALNUMBER', how='left')
        .groupby(['TICKETSERIALNUMBER','TRANLINEITEMID'], as_index=False,dropna=False)
        .agg({
            'WAGER':'sum',
            'SECONDWAGER':'sum',
            'SALES':'sum',
            'SECONDSALES':'sum',
            'SALESCOMM':'sum',
            'SECONDSALESCOMM':'sum',
            'GST':'sum',
            'SECONDGST':'sum',
            'RETURNAMOUNT':'max',
            'WINNINGAMOUNT':'max',
            'REFUNDAMOUNT':'max',
            'REBATERECLAIM':'max'
        })
    )

    df_temp_2 = (
        df_ubt_temp_resultwager[df_ubt_temp_resultwager['PRODID'].isin([1,5,6])]
        .merge(df_ubt_temp_resultsalessandcomm, on='TICKETSERIALNUMBER', how='left')
        .merge(df_ubt_temp_resultgst, on='TICKETSERIALNUMBER', how='left')
        .merge(df_ubt_temp_resultvalidation[['TICKETSERIALNUMBER','RETURNAMOUNT','WINNINGAMOUNT','REFUNDAMOUNT','REBATERECLAIM']],
            on='TICKETSERIALNUMBER', how='left')
        .groupby(['TICKETSERIALNUMBER'], as_index=False,dropna=False)
        .agg({
            'WAGER':'sum',
            'SECONDWAGER':'sum',
            'SALES':'sum',
            'SECONDSALES':'sum',
            'SALESCOMM':'sum',
            'SECONDSALESCOMM':'sum',
            'GST':'sum',
            'SECONDGST':'sum',
            'RETURNAMOUNT':'max',
            'WINNINGAMOUNT':'max',
            'REFUNDAMOUNT':'max',
            'REBATERECLAIM':'max'
        })
    )
    df_temp_2['TRANLINEITEMID'] = None


    df_temp_3 = df_ubt_temp_resultvalidationexchange.assign(
        WAGER=0, SECONDWAGER=0, SALES=0, SECONDSALES=0,
        SALESCOMM=0, SECONDSALESCOMM=0, GST=0, SECONDGST=0,
        RETURNAMOUNT=lambda x: x['WINNINGAMOUNT'], REBATERECLAIM=lambda x: x['REBATERECLAIM']
    )[[
        'TICKETSERIALNUMBER','TRANLINEITEMID','WAGER','SECONDWAGER','SALES','SECONDSALES',
        'SALESCOMM','SECONDSALESCOMM','GST','SECONDGST','WINNINGAMOUNT','RETURNAMOUNT',
        'REFUNDAMOUNT','REBATERECLAIM'
    ]]

    df_ubt_temp_transactionamount = pd.concat([df_temp_1, df_temp_2, df_temp_3], ignore_index=True)
    return df_ubt_temp_transactionamount

def ubt_temp_final_lotteryselect(df_ubt_temp_placebettransactiontype,startdateUTC, enddateUTC):

    df_ubt_temp_final_lotteryselect = pd.DataFrame({
    'TICKETSERIALNUMBER': pd.Series(dtype='str'),
    'TRANLINEITEMID': pd.Series(dtype='str'),
    'SELECTION': pd.Series(dtype='str')
    })

    # --- 1. Horse ---
    df_horse = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype['PRODID']==1]
        .merge(df_horse_placedbettransactionlineitem, on='TRANHEADERID', how='inner')
    )
    df_horse = df_horse[
        ((df_horse['CREATEDDATE'] >= startdateUTC) & (df_horse['CREATEDDATE'] <= enddateUTC) &
        df_horse['TRANSACTIONTYPETOTAL'].isin([1,4,6])) |
        (df_horse['TRANSACTIONTYPETOTAL'].isin([3,5]))
    ]
    df_horse = df_horse.groupby(['TICKETSERIALNUMBER', 'TRANHEADERID'], as_index=False,dropna=False)\
        .agg({
        'BETLINEANNOTATION': lambda x: ','.join(x.sort_values(ascending=False).astype(str))
    })
    df_horse = (
        df_horse
        .rename(columns={'BETLINEANNOTATION': 'SELECTION'})
        .assign(TRANLINEITEMID=None)
        [ ["TICKETSERIALNUMBER", "TRANLINEITEMID", "SELECTION"]]
    )
    # --- 2. 4D ---
    df_4d = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype['PRODID']==2]
        .merge(df_4d_placedbettransactionlineitemnumber, on='TRANHEADERID', how='inner')
    )
    df_4d = df_4d[
        ((df_4d['CREATEDDATE'] >= startdateUTC) & (df_4d['CREATEDDATE'] <= enddateUTC) &
        df_4d['TRANSACTIONTYPETOTAL'].isin([1,4,6])) |
        (df_4d['TRANSACTIONTYPETOTAL'].isin([3,5]))
    ]
    df_4d = (
        df_4d
        .rename(columns={'SELECTEDBETNUMBER': 'SELECTION'})
        [ ["TICKETSERIALNUMBER", "TRANLINEITEMID", "SELECTION"]]
    )

    # --- 3. Toto ---
    df_toto = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype['PRODID']==3]
        .merge(df_toto_placedbettransactionlineitem, on='TRANHEADERID', how='inner')
        .merge(df_toto_placedbettransactionlineitemnumber, on='TRANLINEITEMID', how='inner')
    )
    df_toto = df_toto[
        ((df_toto['CREATEDDATE'] >= startdateUTC) & (df_toto['CREATEDDATE'] <= enddateUTC) &
        df_toto['TRANSACTIONTYPETOTAL'].isin([1,4,6])) |
        (df_toto['TRANSACTIONTYPETOTAL'].isin([3,5]))
    ]
    df_toto = df_toto.groupby(['TICKETSERIALNUMBER','TRANLINEITEMID'], as_index=False,dropna=False)\
    .agg({
        'SINGLEBETNUMBER': lambda x: ' '.join(x.sort_values(ascending=False).astype(str))
    })
    df_toto = (
        df_toto
        .rename(columns={'SINGLEBETNUMBER': 'SELECTION'})
        [ ["TICKETSERIALNUMBER", "TRANLINEITEMID", "SELECTION"]]
    )

    # --- 4. Sweep ---
    df_sweep = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype['PRODID']==4]
        .merge(df_sweep_placedbettransactionlineitemnumber, on='TRANHEADERID', how='inner')
    )
    df_sweep = df_sweep[
        ((df_sweep['CREATEDDATE'] >= startdateUTC) & (df_sweep['CREATEDDATE'] <= enddateUTC) &
        df_sweep['TRANSACTIONTYPETOTAL'].isin([1,4,6])) |
        (df_sweep['TRANSACTIONTYPETOTAL'].isin([3,5]))
    ]
    df_sweep = df_sweep.groupby(['TICKETSERIALNUMBER','TRANLINEITEMID'], as_index=False,dropna=False)\
        .agg({
        'SELECTEDNUMBER': lambda x: ' '.join(x.sort_values(ascending=False).astype(str))
    })
    df_sweep = (
        df_sweep
        .rename(columns={'SELECTEDNUMBER': 'SELECTION'})
        [ ["TICKETSERIALNUMBER", "TRANLINEITEMID", "SELECTION"]]
    )

    # --- 5. Sports ---
    df_sports = (
        df_ubt_temp_placebettransactiontype[df_ubt_temp_placebettransactiontype['PRODID'].isin([5,6])]
        .merge(df_sports_placedbettransactionlineitemnumber, on='TRANHEADERID', how='inner')
        .merge(df_ubt_temp_placebettransactiontype[['TICKETSERIALNUMBER']], on='TICKETSERIALNUMBER', how='inner')
    )
    df_sports = df_sports.drop_duplicates(subset=['TICKETSERIALNUMBER','TRANLINEITEMID','SELECTIONNAME'])
    df_sports = (
        df_sports
        .rename(columns={'SELECTIONNAME': 'SELECTION'})
        [ ["TICKETSERIALNUMBER", "TRANLINEITEMID", "SELECTION"]]
    )

    df_ubt_temp_final_lotteryselect = pd.concat([df_horse, df_4d, df_toto, df_sweep, df_sports], ignore_index=True)
    return df_ubt_temp_final_lotteryselect

def drop_audit_cols(df):
    # List các tên chính xác hay gặp nhất
    known_audit = {
        'X_RECORD_INSERT_TS', 'X_RECORD_UPDATE_TS', 'X_ETL_NAME',
        'X_SOURCE_SYSTEM', 'X_BATCH_ID', 'X_PROCESS_ID', 'X_FILE_NAME',
        'X_ROW_NUMBER', 'X_IS_DELETED', 'X_HASH_KEY'
    }

    cols_to_drop = [
        col for col in df.columns
        if col.startswith('X_') or col.upper() in known_audit
    ]

    return df.drop(columns=cols_to_drop, errors='ignore')