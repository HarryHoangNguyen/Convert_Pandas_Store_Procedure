import pandas as pd
import numpy as np
from Snowflake_connection import *

connection = snowflake_connection()


def declare_variables(procdate):
    HQLocation = '00888'
    
    query = """
        SELECT 
            PREVIOUSPERIODDATETIME, PERIODDATETIME, ACTUALDATE
            
        FROM ZTUBT_GETBUSINESSDATE_PERHOST
        WHERE ACTUALDATE = TO_DATE('{procdate}')
        AND HOST = 3
    
    """
    df = pd.read_sql(query, connection)
    
    PreviousDateTime = pd.to_datetime(df['PREVIOUSPERIODDATETIME'].values[0])
    PeriodDateTime = pd.to_datetime(df['PERIODDATETIME'].values[0])
    ActualDate = pd.to_datetime(df['ACTUALDATE'].values[0])
    
    return HQLocation, PreviousDateTime, PeriodDateTime, ActualDate



def ubt_temp_table(HQLocation, PreviousDateTime, PeriodDateTime, ActualDate):
    
    query = """
        with ubt_temp_table as (
            SELECT
                SUM(coalesce (VBT.WinningAmount, 0)) AS PrizeAmount, --(1)
                SUM(coalesce (VBT.RefundAmount, 0)) AS RefundAmount, --(1)
                (C.CartCreatedDate + interval'8 hour'):: date As TxnDate
                FROM ztubt_cart C
                INNER JOIN ztubt_validatedbetticket VBT ON VBT.CartID = C.CartID
                INNER JOIN ztubt_placedbettransactionheader PBTH ON PBTH.TicketSerialNumber = VBT.TicketSerialNumber
                INNER JOIN ztubt_product P ON PBTH.ProdID = P.ProdID
                INNER JOIN ztubt_validationtype VT ON VBT.ValidationTypeID = VT.ValidationTypeID
                INNER JOIN ztubt_validatedbetticketlifecyclestate VBLC ON VBLC.TranHeaderID = PBTH.TranHeaderID AND VBLC.BetStateTypeID = 'VB06'
                INNER JOIN ztubt_terminal TER ON C.TerDisplayID = TER.TerDisplayID
                INNER JOIN ztubt_location L ON TER.LocID = L.LocID
            WHERE
                L.LocDisplayID = '{HQLocation}'
                AND (C.CartCreatedDate + interval'8 hour')::timestamp BETWEEN ('{PreviousDateTime}'::timestamp) AND ('{PeriodDateTime}'::timestamp)
                AND P.ProdID IN (5, 6)
            GROUP BY
                (C.CartCreatedDate + interval'8 hour'):: date
        )
        SELECT 
            '{ActualDate}'::date AS BusinessDate,
            '2' as ProcessType,
            TxnDate,
            SUM(PrizeAmount) AS TotalPrizeAmount,
            SUM(RefundAmount) AS TotalRefundAmount
        FROM ubt_temp_table
        GROUP BY TxnDate
    """
    df_ubt_temp_table = pd.read_sql(query, connection)
    
    return df_ubt_temp_table