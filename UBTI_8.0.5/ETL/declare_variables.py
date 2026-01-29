import pandas as pd
from Snowflake_connection import *
import logging

logger = logging.getLogger(__name__)

connection = snowflake_connection()

def declare_variables(transactiondate):

    # Select(TransactionDate - interval '0 Day') into FromDate;

	# 	select (((date_part('day', current_timestamp at time zone 'utc')- date_part('day', current_timestamp)) *24) + (date_part('hour',current_timestamp at time zone 'utc'  )- date_part('hour',current_timestamp )))
	# 	into ConvertToUTC;

	#     select (((date_part('day',current_timestamp)- date_part('day', current_timestamp at time zone 'utc')) *24) + (date_part('hour',current_timestamp  )- date_part('hour',current_timestamp at time zone 'utc' )))
	#     into ConvertToSGT;

	# 	SELECT
	# 	   PreviousPeriodDateTime  ,
	# 	   PeriodDateTime  into FromDate, ToDate
	#     FROM ztubt_getbusinessdate_perhost zgp
	#     WHERE ActualDate = FromDate
	#     ORDER BY PreviousPeriodDateTime DESC, PeriodDateTime desc
	#     limit 1 ;

	#     select(FromDate + ( ConvertToUTC * interval'1 Hour' ) ) into  FromDateUTC;
	#     select( ToDate + ( ConvertToUTC * interval'1 Hour' )) into  ToDateUTC;
    FromDate = transactiondate

    ConvertToUTC = ((pd.Timestamp.utcnow().day - pd.Timestamp.now().day) * 24) + (pd.Timestamp.utcnow().hour - pd.Timestamp.now().hour)
    ConvertToSGT = ((pd.Timestamp.now().day - pd.Timestamp.utcnow().day) * 24) + (pd.Timestamp.now().hour - pd.Timestamp.utcnow().hour)
    query = f"""
    SELECT
       PreviousPeriodDateTime  ,
       PeriodDateTime
    FROM ZTUBT_GETBUSINESSDATE_PERHOST zgp
    WHERE ActualDate = '{FromDate}'
    ORDER BY PreviousPeriodDateTime DESC, PeriodDateTime desc
    limit 1 ;
    """
    df_dates = pd.read_sql(query, connection)
    FromDate = pd.to_datetime(df_dates['PREVIOUSPERIODDATETIME'].values[0])
    ToDate = pd.to_datetime(df_dates['PERIODDATETIME'].values[0])
    FromDateUTC = FromDate + pd.Timedelta(hours=ConvertToUTC)
    ToDateUTC = ToDate + pd.Timedelta(hours=ConvertToUTC)
    
    return FromDate, ToDate, FromDateUTC, ToDateUTC, ConvertToUTC, ConvertToSGT