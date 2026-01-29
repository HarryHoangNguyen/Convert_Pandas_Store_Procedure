import pandas as pd
import numpy as np
from Snowflake_connection import *
import logging
import os


current_dir = os.path.dirname(os.path.abspath(__file__))
parquet_dir = os.path.join(current_dir, "Parquets", pd.to_datetime("today").strftime("%Y%m%d%H%M"))
os.makedirs(parquet_dir, exist_ok=True)

logger = logging.getLogger(__name__)

# Establish Snowflake connection
connection = snowflake_connection()


def declare_variables(procdate):
    """
        Declare and retrieve necessary variables from the database based on the procdate.
        Returns:
            HQLocation (str): Hardcoded location value.
            PreviousDateTime (datetime): Previous period date time from the database.
            PeriodDateTime (datetime): Period date time from the database.
            ActualDate (datetime): Actual date from the database.
    """
    
    # Declare HQLocation and fetch date variables from the database
    HQLocation = '00888'
    
    # Change procdate to date format for query
    procdate = pd.to_datetime(procdate).date()
    
    # Define and execute the query to get date variables
    query = f"""
        SELECT 
            PREVIOUSPERIODDATETIME, PERIODDATETIME, ACTUALDATE
            
        FROM ZTUBT_GETBUSINESSDATE_PERHOST
        WHERE ACTUALDATE = TO_DATE('{procdate}', 'YYYY-MM-DD')
        AND HOST = 3
    """
    df = pd.read_sql(query, connection)
    
    PreviousDateTime = pd.to_datetime(df['PREVIOUSPERIODDATETIME'].values[0])
    PeriodDateTime = pd.to_datetime(df['PERIODDATETIME'].values[0])
    ActualDate = pd.to_datetime(df['ACTUALDATE'].values[0])
    
    # log the query
    logger.info(f"Executed declare_variables query: {query}")
    
    # Log the declared variables
    logger.info(f"Declared Variables - HQLocation: {HQLocation}, PreviousDateTime: {PreviousDateTime}, PeriodDateTime: {PeriodDateTime}, ActualDate: {ActualDate}")
    
    return HQLocation, PreviousDateTime, PeriodDateTime, ActualDate



def ubt_temp_table(HQLocation, PreviousDateTime, PeriodDateTime, ActualDate):
    
    
    """
        Process the ubt_temp_table based on the provided parameters.
        Returns:
            DataFrame: Processed ubt_temp_table data.
    """
    
    # Define and execute the query to process ubt_temp_table
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
    
    # ============================================================================
    # Write the resulting DataFrame to a Parquet file for debugging
    # ============================================================================
    parquet_path = os.path.join(parquet_dir, "ubt_temp_table.parquet")
    df_ubt_temp_table.to_parquet(parquet_path, engine='pyarrow', index=False)
    logger.info(f"ubt_temp_table data written to Parquet file at: {parquet_path}")
    # ============================================================================
    
    return df_ubt_temp_table