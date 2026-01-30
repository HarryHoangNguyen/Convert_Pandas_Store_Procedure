import pandas as pd
import numpy as np
from Snowflake_connection import *
import logging, sys, os, warnings
logger = logging.getLogger(__name__)
connection = snowflake_connection()

# =====================================================
# Suppress Warnings
# =====================================================
# Suppress all warnings
warnings.filterwarnings('ignore')

# Suppress specific pandas warnings
pd.options.mode.chained_assignment = None
pd.set_option('future.no_silent_downcasting', True)



# =====================================================
# Gathering the data
# =====================================================

query = f"""
    SELECT * FROM ZTUBT_SESSION
"""

df_ztubt_session = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_TERMINAL
"""
df_ztubt_terminal = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_LOCATION
"""
df_ztubt_location = pd.read_sql(query, connection)


query = f"""
    SELECT * FROM ZTUBT_PLACEDBETTRANSACTIONHEADER
"""
df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_PLACEDBETTRANSACTIONHEADERLIFECYCLESTATE
"""
df_ztubt_placedbettransactionheaderlifecyclestate = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM
"""
df_ztubt_toto_placedbettransactionlineitem = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEMNUMBER
"""
df_ztubt_sweep_placedbettransactionlineitemnumber = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_SWEEP_PLACEDBETTRANSACTIONLINEITEM
"""
df_ztubt_sweep_placedbettransactionlineitem = pd.read_sql(query, connection)


query = f"""
    SELECT * FROM ZTUBT_CANCELLEDBETTICKET
"""
df_ztubt_cancelledbetticket = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_CANCELLEDBETTICKETLIFECYCLESTATE
"""
df_ztubt_cancelledbetticketlifecyclestate = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_VALIDATEDBETTICKET
"""
df_ztubt_validatedbetticket = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_VALIDATEDBETTICKETLIFECYCLESTATE
"""
df_ztubt_validatedbetticketlifecyclestate = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_HORSE_PLACEDBETTRANSACTIONLINEITEM
"""
df_ztubt_horse_placedbettransactionlineitem = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_ADMISSIONVOUCHERTRANSACTION
"""
df_ztubt_admissionvouchertransaction = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_OFFLINEPRODUCTHEADER
"""
df_ztubt_offlineproductheader = pd.read_sql(query, connection)


query = f"""
    SELECT * FROM ZTUBT_CHARITYTICKETTRANSACTION
"""
df_ztubt_charitytickettransaction = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_SPATRANSACTION
"""
df_ztubt_spatransaction = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_TOPUPCARDTRANSACTION
"""
df_ztubt_topupcardtransaction = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_TOTOHONGBAOTRANSACTION
"""
df_ztubt_totohongbaotransaction = pd.read_sql(query, connection)

query = f"""
    SELECT * FROM ZTUBT_OPERATINGHOURS
"""
df_ztubt_operatinghours = pd.read_sql(query, connection)
# =====================================================
# ADD PREFIX TO AVOIDSING CONFLICT IN MERGE
# =====================================================
def func_add_prefix(df, prefix):
    df = df.add_prefix(prefix)
    return df


df_ztubt_session = func_add_prefix(df_ztubt_session, 'SESSION.')
df_ztubt_terminal = func_add_prefix(df_ztubt_terminal, 'TERMINAL.')
df_ztubt_location = func_add_prefix(df_ztubt_location, 'LOCATION.')
df_ztubt_placedbettransactionheader = func_add_prefix(df_ztubt_placedbettransactionheader, 'PBTH.')
df_ztubt_placedbettransactionheaderlifecyclestate = func_add_prefix(df_ztubt_placedbettransactionheaderlifecyclestate, 'PBTHLCS.')
df_ztubt_toto_placedbettransactionlineitem = func_add_prefix(df_ztubt_toto_placedbettransactionlineitem, 'TOTO_PBTL.')
df_ztubt_sweep_placedbettransactionlineitemnumber = func_add_prefix(df_ztubt_sweep_placedbettransactionlineitemnumber, 'SWEEP_PBTLN.')
df_ztubt_sweep_placedbettransactionlineitem = func_add_prefix(df_ztubt_sweep_placedbettransactionlineitem, 'SWEEP_PBTL.')
df_ztubt_cancelledbetticket = func_add_prefix(df_ztubt_cancelledbetticket, 'CBT.')
df_ztubt_cancelledbetticketlifecyclestate = func_add_prefix(df_ztubt_cancelledbetticketlifecyclestate, 'CBTLCS.')
df_ztubt_validatedbetticket = func_add_prefix(df_ztubt_validatedbetticket, 'V.')
df_ztubt_validatedbetticketlifecyclestate = func_add_prefix(df_ztubt_validatedbetticketlifecyclestate, 'VLCS.')
df_ztubt_horse_placedbettransactionlineitem = func_add_prefix(df_ztubt_horse_placedbettransactionlineitem, 'HORSE_PBTL.')
df_ztubt_admissionvouchertransaction = func_add_prefix(df_ztubt_admissionvouchertransaction, 'AVT.')
df_ztubt_offlineproductheader = func_add_prefix(df_ztubt_offlineproductheader, 'OPH.')
df_ztubt_charitytickettransaction = func_add_prefix(df_ztubt_charitytickettransaction, 'CTT.')
df_ztubt_spatransaction = func_add_prefix(df_ztubt_spatransaction, 'SPAT.')
df_ztubt_topupcardtransaction = func_add_prefix(df_ztubt_topupcardtransaction, 'TUCT.')
df_ztubt_totohongbaotransaction = func_add_prefix(df_ztubt_totohongbaotransaction, 'THT.')
df_ztubt_operatinghours = func_add_prefix(df_ztubt_operatinghours, 'OH.')

# =====================================================
# CREATE TEMP TABLE UBT_TEMP_T
# =====================================================
def ubt_temp_t(FromDate, ToDate, ConvertToUTC):

    # create temp table if not exists ubt_temp_t
	#      (
	# 	    TerDisplayID VARCHAR(200),
	# 	    SessionStartDateTime timestamp,
	# 	    SessionEndDateTime timestamp,
	# 	    SessionStartDateTimeUTC timestamp,
	# 	    SessionEndDateTimeUTC timestamp,
	# 	    SweepIndicator int4
	#     );
    df_ubt_temp_t = pd.DataFrame({
        'TERDISPLAYID': pd.Series(dtype='str'),
        'SESSIONSTARTDATETIME': pd.Series(dtype='datetime64[ns]'),
        'SESSIONENDDATETIME': pd.Series(dtype='datetime64[ns]'),
        'SESSIONSTARTDATETIMEUTC': pd.Series(dtype='datetime64[ns]'),
        'SESSIONENDDATETIMEUTC': pd.Series(dtype='datetime64[ns]'),
        'SWEEPINDICATOR': pd.Series(dtype='int32')
    })

    # select SESS.TerDisplayID, MIN(SessionStartDateTime) AS SessionStartDateTime, MAX(SessionEndDateTime) AS SessionEndDateTime,
	#     MIN(SessionStartDateTime + (convertToUTC * interval'1 Hour')) AS SessionStartDateTime, MAX( SessionEndDateTime + (convertToUTC * interval'1 Hour')) AS SessionEndDateTime,--convert to UTC--(3)
	#     LOC.SweepIndicator
	#     FROM ztubt_session SESS
	#     inner join ztubt_terminal ter on SESS.TerDisplayID=ter.TerDisplayID
	#     inner join ztubt_location LOC on TER.LocID=LOC.LocID
	#     WHERE SessionStartDateTime >= FromDate
	#     AND SessionStartDateTime <= ToDate
	#     GROUP BY SESS.TerDisplayID, LOC.SweepIndicator ;

    df_merged = df_ztubt_session.merge(
        df_ztubt_terminal,
        left_on='SESSION.TERDISPLAYID',
        right_on='TERMINAL.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ztubt_location,
        left_on='TERMINAL.LOCID',
        right_on='LOCATION.LOCID',
        how='inner'
    ).loc[
        (df_ztubt_session['SESSION.SESSIONSTARTDATETIME'] >= FromDate) &
        (df_ztubt_session['SESSION.SESSIONSTARTDATETIME'] <= ToDate)
    ].groupby(['SESSION.TERDISPLAYID', 'LOCATION.SWEEPINDICATOR'], as_index=False
    ).assign(
        TERDISPLAYID=lambda x: x['SESSION.TERDISPLAYID'],
        SESSIONSTARTDATETIME=lambda x: x['SESSION.SESSIONSTARTDATETIME'].min(),
        SESSIONENDDATETIME=lambda x: x['SESSION.SESSIONENDDATETIME'].max(),
        SESSIONSTARTDATETIMEUTC=lambda x: x['SESSION.SESSIONSTARTDATETIME'] + pd.to_timedelta(ConvertToUTC, unit='H'),
        SESSIONENDDATETIMEUTC=lambda x: x['SESSION.SESSIONENDDATETIME'] + pd.to_timedelta(ConvertToUTC, unit='H'),
        SWEEPINDICATOR=lambda x: x['LOCATION.SWEEPINDICATOR']
    )[['TERDISPLAYID', 'SESSIONSTARTDATETIME', 'SESSIONENDDATETIME',
       'SESSIONSTARTDATETIMEUTC', 'SESSIONENDDATETIMEUTC', 'SWEEPINDICATOR']]

    return df_ubt_temp_t


def ubt_temp_count(df_ubt_temp_t, FromDate, ToDate, FromDateUTC, ToDateUTC):
    # create temp table if not exists ubt_temp_count
	#      (
	# 	   TerDisplayID VARCHAR(200),
	# 	   FirstDate timestamp,
	# 	   FirstCount int4,
	# 	   LastCount int4,
	# 	   TranHeaderLineItemId VARCHAR(500)
	#      );

    df_ubt_temp_count = pd.DataFrame({
        'TERDISPLAYID': pd.Series(dtype='str'),
        'FIRSTDATE': pd.Series(dtype='datetime64[ns]'),
        'FIRSTCOUNT': pd.Series(dtype='int32'),
        'LASTCOUNT': pd.Series(dtype='int32'),
        'TRANHEADERLINEITEMID': pd.Series(dtype='str')
    })

    df_ubt_temp_t = func_add_prefix(df_ubt_temp_t, 'TT.')

    # SELECT F.TerDisplayID, F.FirstDate, F.FirstCount,  F.LastCount, F.TranHeaderID
	# 	FROM
	# 	(
	# 		SELECT DISTINCT
	# 		tha.TerDisplayID,
	# 		tha.CreatedDate
	# 		AS FirstDate,
	# 		CASE
	# 			WHEN (
	# 			 tha.CreatedDate >= tt.SessionStartDateTimeUTC
	# 			AND tha.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
	# 			) then 1
	# 			ELSE NULL
	# 		END AS FirstCount,
	# 		CASE
	# 			WHEN (
	# 			tha.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
	# 			AND tha.CreatedDate <= tt.SessionEndDateTimeUTC
	# 			) then 1
	# 			ELSE NULL
	# 		END AS LastCount
	# 		,tha.TranHeaderID
	# 		FROM ubt_temp_t tt
	# 		INNER JOIN ztubt_placedbettransactionheader tha ON tt.TerDisplayID = tha.TerDisplayID
	# 		INNER JOIN ztubt_placedbettransactionheaderlifecyclestate LCS ON tha.TranHeaderID = LCS.TranHeaderID AND BetStateTypeID = 'PB06'
	# 		LEFT JOIN ztubt_toto_placedbettransactionlineitem PBTL ON tha.TranHeaderID = PBTL.TranHeaderID --TOTO
	# 		LEFT JOIN ztubt_sweep_placedbettransactionlineitemnumber PBTLN ON tha.TranHeaderID = PBTLN.TranHeaderID
	# 		LEFT JOIN ztubt_sweep_placedbettransactionlineitem SPPBTL on SPPBTL.TranLineItemID=PBTLN.TranLineItemID
	# 		WHERE
	# 		(tha.CreatedDate >= FromDateUTC
	# 		AND tha.CreatedDate <= ToDateUTC)
	# 		and tha.isExchangeTicket=False and IsBetRejectedByTrader=False
	# 		AND (PBTL.GroupUnitSequence = 1 OR PBTL.GroupUnitSequence IS NULL) --for group toto
	# 		AND (PBTLN.IsSoldOut IS NULL OR (PBTLN.IsSoldOut IS NOT NULL AND PBTLN.IsSoldOut = False)) --for sweep
	# 		AND (tt.SweepIndicator IS NULL OR (tt.SweepIndicator IS NOT NULL AND SPPBTL.IsPrinted IS NOT NULL))
	# 	)F

    df_merged = df_ubt_temp_t.merge(
        df_ztubt_placedbettransactionheader,
        left_on='TT.TERDISPLAYID',
        right_on='PBTH.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ztubt_placedbettransactionheaderlifecyclestate,
        left_on='PBTH.TRANHEADERID',
        right_on='PBTHLCS.TRANHEADERID',
        how='inner'
    ).merge(
        df_ztubt_toto_placedbettransactionlineitem,
        left_on='PBTH.TRANHEADERID',
        right_on='TOTO_PBTL.TRANHEADERID',
        how='left'
    ).merge(
        df_ztubt_sweep_placedbettransactionlineitemnumber,
        left_on='PBTH.TRANHEADERID',
        right_on='SWEEP_PBTLN.TRANHEADERID',
        how='left'
    ).merge(
        df_ztubt_sweep_placedbettransactionlineitem,
        left_on='SWEEP_PBTLN.TRANLINEITEMID',
        right_on='SWEEP_PBTL.TRANLINEITEMID',
        how='left'
    ).loc[
        (
            (df_ztubt_placedbettransactionheader['PBTH.CREATEDDATE'] >= FromDateUTC) &
            (df_ztubt_placedbettransactionheader['PBTH.CREATEDDATE'] <= ToDateUTC)
        ) &
        (df_ztubt_placedbettransactionheader['PBTH.ISEXCHANGETICKET'] == False) &
        (df_ztubt_placedbettransactionheader['PBTH.ISBETREJECTEDBYTRADER'] == False) &
        (
            (df_ztubt_toto_placedbettransactionlineitem['TOTO_PBTL.GROUPUNITSEQUENCE'] == 1) |
            (df_ztubt_toto_placedbettransactionlineitem['TOTO_PBTL.GROUPUNITSEQUENCE'].isnull())
        ) &
        (
            (df_ztubt_sweep_placedbettransactionlineitemnumber['SWEEP_PBTLN.ISSOLDOUT'].isnull()) |
            (
                (df_ztubt_sweep_placedbettransactionlineitemnumber['SWEEP_PBTLN.ISSOLDOUT'].notnull()) &
                (df_ztubt_sweep_placedbettransactionlineitemnumber['SWEEP_PBTLN.ISSOLDOUT'] == False)
            )
        ) &
        (
            (df_ubt_temp_t['TT.SWEEPINDICATOR'].isnull()) |
            (
                (df_ubt_temp_t['TT.SWEEPINDICATOR'].notnull()) &
                (df_ztubt_sweep_placedbettransactionlineitem['SWEEP_PBTL.ISPRINTED'].notnull())
            )
        ) &
        (df_ztubt_placedbettransactionheaderlifecyclestate['PBTHLCS.BETSTATETYPEID'] == 'PB06')
    ][['TT.TERDISPLAYID', 'PBTH.CREATEDDATE', 'PBTH.TRANHEADERID']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        FIRSTDATE=lambda x: x['PBTH.CREATEDDATE'],
        FIRSTCOUNT=lambda x: np.where(
            (x['PBTH.CREATEDDATE'] >= x['TT.SESSIONSTARTDATETIMEUTC']) &
            (x['PBTH.CREATEDDATE'] <= (x['TT.SESSIONSTARTDATETIMEUTC'] + pd.Timedelta(hours=1))),
            1,
            None
        )
    ).assign(
        LASTCOUNT=lambda x: np.where(
            (x['PBTH.CREATEDDATE'] >= (x['TT.SESSIONENDDATETIMEUTC'] - pd.Timedelta(hours=1))) &
            (x['PBTH.CREATEDDATE'] <= x['TT.SESSIONENDDATETIMEUTC']),
            1,
            None
        ),
        TRANHEADERLINEITEMID=lambda x: x['PBTH.TRANHEADERID']
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]

    # Apply select distinct
    df_merged = df_merged.drop_duplicates()

    # Append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)

# ================================================================================================================================================================
    # SELECT F.TerDisplayID, F.FirstDate, F.FirstCount,  F.LastCount, F.TranHeaderID
	# 	FROM
	# 	(
	# 		SELECT DISTINCT
	# 		tt.TerDisplayID,
	# 		tha.CreatedDate
	# 		AS FirstDate,
	# 		CASE
	# 			WHEN (
	# 			tha.CreatedDate >= tt.SessionStartDateTimeUTC
	# 			AND tha.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
	# 			) THEN 1
	# 			ELSE NULL
	# 		END AS FirstCount,
	# 		CASE
	# 			WHEN (
	# 			tha.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
	# 			AND tha.CreatedDate <= tt.SessionEndDateTimeUTC
	# 			) THEN 1
	# 			ELSE NULL
	# 		END AS LastCount
	# 		,tha.TranHeaderID
	# 		FROM ubt_temp_t tt
	# 		INNER JOIN ztubt_placedbettransactionheader tha ON tt.TerDisplayID = tha.TerDisplayID
	# 		INNER JOIN ztubt_placedbettransactionheaderlifecyclestate LCS ON tha.TranHeaderID = LCS.TranHeaderID AND BetStateTypeID  = 'PB03'
	# 		INNER JOIN ztubt_sweep_placedbettransactionlineitem PBTL ON tha.TranHeaderID = PBTL.TranHeaderID
	# 		INNER JOIN ztubt_sweep_placedbettransactionlineitemnumber PBTLN ON PBTL.TranLineItemID = PBTLN.TranLineItemID AND PBTLN.IsSoldOut = False
	# 		WHERE
	# 		(tha.CreatedDate >= FromDateUTC
	# 		AND tha.CreatedDate <= ToDateUTC)
	# 		AND IsBetRejectedByTrader = False
	# 		AND (tt.SweepIndicator IS NOT NULL AND IsPrinted IS NULL)
	# 	)F

    df_merged = df_ubt_temp_t.merge(
        df_ztubt_placedbettransactionheader,
        left_on='TT.TERDISPLAYID',
        right_on='PBTH.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ztubt_placedbettransactionheaderlifecyclestate,
        left_on='PBTH.TRANHEADERID',
        right_on='PBTHLCS.TRANHEADERID',
        how='inner'
    ).merge(
        df_ztubt_sweep_placedbettransactionlineitem,
        left_on='PBTH.TRANHEADERID',
        right_on='SWEEP_PBTL.TRANHEADERID',
        how='inner'
    ).merge(
        df_ztubt_sweep_placedbettransactionlineitemnumber,
        left_on='SWEEP_PBTL.TRANLINEITEMID',
        right_on='SWEEP_PBTLN.TRANLINEITEMID',
        how='inner'
    ).loc[
        (
            (df_ztubt_placedbettransactionheader['PBTH.CREATEDDATE'] >= FromDateUTC) &
            (df_ztubt_placedbettransactionheader['PBTH.CREATEDDATE'] <= ToDateUTC)
        ) &
        (df_ztubt_placedbettransactionheader['PBTH.ISBETREJECTEDBYTRADER'] == False) &
        (df_ztubt_placedbettransactionheaderlifecyclestate['PBTHLCS.BETSTATETYPEID'] == 'PB03') &
        (df_ztubt_sweep_placedbettransactionlineitemnumber['SWEEP_PBTLN.ISSOLDOUT'] == False) &
        (
            (df_ubt_temp_t['TT.SWEEPINDICATOR'].notnull()) &
            (df_ztubt_sweep_placedbettransactionlineitem['SWEEP_PBTL.ISPRINTED'].isnull())
        )
    ][['TT.TERDISPLAYID', 'PBTH.CREATEDDATE', 'PBTH.TRANHEADERID']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        FIRSTDATE=lambda x: x['PBTH.CREATEDDATE'],
        FIRSTCOUNT=lambda x: np.where(
            (x['PBTH.CREATEDDATE'] >= x['TT.SESSIONSTARTDATETIMEUTC']) &
            (x['PBTH.CREATEDDATE'] <= (x['TT.SESSIONSTARTDATETIMEUTC'] + pd.Timedelta(hours=1))),
            1,
            None
        ),
        LASTCOUNT=lambda x: np.where(
            (x['PBTH.CREATEDDATE'] >= (x['TT.SESSIONENDDATETIMEUTC'] - pd.Timedelta(hours=1))) &
            (x['PBTH.CREATEDDATE'] <= x['TT.SESSIONENDDATETIMEUTC']),
            1,
            None
        ),
        TRANHEADERLINEITEMID=lambda x: x['PBTH.TRANHEADERID']
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]

    # Apply select distinct
    df_merged = df_merged.drop_duplicates()

    # Append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)
# ================================================================================================================================================================
# SELECT F.TerDisplayID, NULL, F.FirstCount, F.LastCount, F.TranHeaderID
# 		FROM
# 		(
# 			SELECT DISTINCT
# 			tt.TerDisplayID,
# 			C.CancelledDate
# 			AS FirstDate,
# 			CASE
# 				WHEN (
# 				C.CancelledDate >= tt.SessionStartDateTime
# 				AND C.CancelledDate <= ( tt.SessionStartDateTime + INTERVAL'1 Hour') :: timestamp
# 				) THEN 1
# 				ELSE NULL
# 			END AS FirstCount,
# 						CASE
# 				WHEN (
# 				C.CancelledDate >= ( tt.SessionEndDateTime - INTERVAL'1 Hour') :: timestamp
# 				AND C.CancelledDate <= tt.SessionEndDateTime
# 				) THEN 1
# 				ELSE NULL
# 			END AS LastCount
# 			,tha.TranHeaderID
# 			FROM ubt_temp_t tt
# 			INNER JOIN ztubt_cancelledbetticket C ON  tt.TerDisplayID = C.TerDisplayID
# 			INNER JOIN ztubt_cancelledbetticketlifecyclestate LCS ON C.TranHeaderID = LCS.TranHeaderID AND BetStateTypeID = 'CB06'
# 			INNER JOIN ztubt_placedbettransactionheader tha ON C.TranHeaderID = tha.TranHeaderID
# 			LEFT JOIN ztubt_toto_placedbettransactionlineitem PBTL ON C.TranHeaderID = PBTL.TranHeaderID
# 			WHERE
# 			(C.CancelledDate >= FromDate
# 			AND C.CancelledDate <= ToDate)
# 			AND ((PBTL.GroupUnitSequence IS NOT NULL AND C.CancelledAmout != 0) OR PBTL.GroupUnitSequence IS NULL) --for group toto
# 			AND tha.IsBetRejectedByTrader = False
# 		)F

    df_merged = df_ubt_temp_t.merge(
        df_ztubt_cancelledbetticket,
        left_on='TT.TERDISPLAYID',
        right_on='CBT.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ztubt_cancelledbetticketlifecyclestate,
        left_on='CBT.TRANHEADERID',
        right_on='CBTLCS.TRANHEADERID',
        how='inner'
    ).merge(
        df_ztubt_placedbettransactionheader,
        left_on='CBT.TRANHEADERID',
        right_on='PBTH.TRANHEADERID',
        how='inner'
    ).merge(
        df_ztubt_toto_placedbettransactionlineitem,
        left_on='CBT.TRANHEADERID',
        right_on='TOTO_PBTL.TRANHEADERID',
        how='left'
    ).loc[
        (
            (df_ztubt_cancelledbetticket['CBT.CANCELLEDDATE'] >= FromDate) &
            (df_ztubt_cancelledbetticket['CBT.CANCELLEDDATE'] <= ToDate)
        ) &
        (  # Condition for group toto
            (
                (df_ztubt_toto_placedbettransactionlineitem['TOTO_PBTL.GROUPUNITSEQUENCE'].notnull()) &
                (df_ztubt_cancelledbetticket['CBT.CANCELLEDAMOUT'] != 0)
            ) |
            (df_ztubt_toto_placedbettransactionlineitem['TOTO_PBTL.GROUPUNITSEQUENCE'].isnull())
        ) &
        (df_ztubt_placedbettransactionheader['PBTH.ISBETREJECTEDBYTRADER'] == False) &
        (df_ztubt_cancelledbetticketlifecyclestate['CBTLCS.BETSTATETYPEID'] == 'CB06')
    ][['TT.TERDISPLAYID', 'CBT.CANCELLEDDATE', 'PBTH.TRANHEADERID']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        CANCELLEDDATE= pd.NA,
        FIRSTDATE=lambda x: None,
        FIRSTCOUNT=lambda x: np.where(
            (x['CBT.CANCELLEDDATE'] >= x['TT.SESSIONSTARTDATETIME']) &
            (x['CBT.CANCELLEDDATE'] <= (x['TT.SESSIONSTARTDATETIME'] + pd.Timedelta(hours=1))),
            1,
            None
        ),
        LASTCOUNT=lambda x: np.where(
            (x['CBT.CANCELLEDDATE'] >= (x['TT.SESSIONENDDATETIME'] - pd.Timedelta(hours=1))) &
            (x['CBT.CANCELLEDDATE'] <= x['TT.SESSIONENDDATETIME']),
            1,
            None
        ),
        TRANHEADERLINEITEMID=lambda x: x['PBTH.TRANHEADERID']
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]

    # Apply select distinct
    df_merged = df_merged.drop_duplicates()
    # Append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)

# ================================================================================================================================================================

    # SELECT TerDisplayID, NULL, FirstCount AS FirstCount,LastCount AS LastCount, NULL
	# 	FROM
	# 	(

	# 		SELECT TerDisplayID, Types, TotalAmount,FirstDate, FirstCount, LastCount
	# 		FROM
	# 			(
	# 				SELECT tt.TerDisplayID, V.TranHeaderID AS HeaderID,
	# 				coalesce (WinningAmount,0) AS VALIDATED,
	# 				coalesce (RebateAmount,0) AS LOSING_BET_REBATE,
	# 				coalesce (RefundAmount,0) AS REFUNDED,
	# 				V.ValidationDate AS FirstDate,
	# 				CASE
	# 					WHEN (
	# 					V.ValidationDate >= tt.SessionStartDateTime
	# 					AND V.ValidationDate <= ( tt.SessionStartDateTime + INTERVAL'1 Hour') :: timestamp
	# 					)  then 1
	# 					ELSE NULL
	# 				END AS FirstCount,
	# 				CASE
	# 					WHEN (
	# 					V.ValidationDate >=  ( tt.SessionEndDateTime - INTERVAL'1 Hour') :: timestamp
	# 					AND V.ValidationDate <= tt.SessionEndDateTime
	# 					)  THEN 1
	# 					ELSE NULL
	# 				END AS LastCount
	# 				FROM ubt_temp_t tt
	# 				INNER JOIN ztubt_validatedbetticket V ON tt.TerDisplayID = V.TerDisplayID
	# 				INNER JOIN ztubt_validatedbetticketlifecyclestate  LCS ON V.TranHeaderID = LCS.TranHeaderID AND BetStateTypeID = 'VB06'
	# 				WHERE
	# 				V.ValidationDate >= FromDate
	# 				and V.ValidationDate <= ToDate
	# 			) p
	# 			cross join lateral (
	# 			values (p.VALIDATED, 'VALIDATED'),
	# 			       (p.LOSING_BET_REBATE, 'LOSING_BET_REBATE'),
	# 			       (p.REFUNDED, 'REFUNDED')
	# 			)b(TotalAmount, Types)
	# 		) a
	# 		WHERE TotalAmount != 0

    df_merged = df_ubt_temp_t.merge(
        df_ztubt_validatedbetticket,
        left_on='TT.TERDISPLAYID',
        right_on='V.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ztubt_validatedbetticketlifecyclestate,
        left_on='V.TRANHEADERID',
        right_on='VLCS.TRANHEADERID',
        how='inner'
    ).loc[
        (
            (df_ztubt_validatedbetticket['V.VALIDATIONDATE'] >= FromDate) &
            (df_ztubt_validatedbetticket['V.VALIDATIONDATE'] <= ToDate)
        ) &
        (df_ztubt_validatedbetticketlifecyclestate['VLCS.BETSTATETYPEID'] == 'VB06')
    ][['TT.TERDISPLAYID', 'V.WINNINGAMOUNT', 'V.REBATEAMOUNT', 'V.REFUNDAMOUNT', 'V.VALIDATIONDATE', 'V.TRANHEADERID']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        FIRSTDATE= None,
        FIRSTCOUNT=lambda x: np.where(
            (x['V.VALIDATIONDATE'] >= x['TT.SESSIONSTARTDATETIME']) &
            (x['V.VALIDATIONDATE'] <= (x['TT.SESSIONSTARTDATETIME'] + pd.Timedelta(hours=1))),
            1,
            None
        ),
        LASTCOUNT=lambda x: np.where(
            (x['V.VALIDATIONDATE'] >= (x['TT.SESSIONENDDATETIME'] - pd.Timedelta(hours=1))) &
            (x['V.VALIDATIONDATE'] <= x['TT.SESSIONENDDATETIME']),
            1,
            None
        ),
        TRANHEADERLINEITEMID= None,
        VALIDATED=lambda x: x['V.WINNINGAMOUNT'].fillna(0),
        LOSING_BET_REBATE=lambda x: x['V.REBATEAMOUNT'].fillna(0),
        REFUNDED=lambda x: x['V.REFUNDAMOUNT'].fillna(0)
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID','VALIDATED','LOSING_BET_REBATE','REFUNDED']
    ].melt(
        id_vars=['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID'],
        value_vars=['VALIDATED', 'LOSING_BET_REBATE', 'REFUNDED'],
        var_name='TYPES',
        value_name='TOTALAMOUNT'
    )
    df_merged = df_merged.loc[df_merged['TOTALAMOUNT'] != 0]

    df_merged = df_merged[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]

    # Append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)

# ================================================================================================================================================================
    # SELECT H.TerDisplayID, NULL, FirstCount,LastCount, TranHeaderID
	# 	FROM
	# 	(
	# 		SELECT DISTINCT
	# 		tt.TerDisplayID, PBTL.TranHeaderID,
	# 		PBTL.CreatedDate
	# 		AS FirstDate,
	# 		CASE
	# 			WHEN (
	# 			PBTL.CreatedDate >= tt.SessionStartDateTimeUTC
	# 			AND PBTL.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
	# 			) THEN 1
	# 			ELSE NULL
	# 		END AS FirstCount,
	# 		CASE
	# 			WHEN (
	# 			PBTL.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
	# 			AND PBTL.CreatedDate <= tt.SessionEndDateTimeUTC
	# 			) THEN 1
	# 			ELSE NULL
	# 		END AS LastCount
	# 		FROM ubt_temp_t tt
	# 		INNER JOIN ztubt_horse_placedbettransactionlineitem PBTL on PBTL.TerDisplayID=tt.TerDisplayID
	# 		INNER JOIN ztubt_placedbettransactionheaderlifecyclestate PBLC ON PBTL.TranHeaderID = PBLC.TranHeaderID AND PBLC.BetStateTypeID = 'PB06'
	# 		WHERE PBTL.BoardRebateAmount > 0 AND PBTL.BoardRebateAmount IS NOT NULL
	# 		AND (PBTL.CreatedDate >= FromDateUTC
	# 		AND PBTL.CreatedDate <= ToDateUTC)
	# 	) H

    df_merged = df_ubt_temp_t.merge(
        df_ztubt_horse_placedbettransactionlineitem,
        left_on='TT.TERDISPLAYID',
        right_on='HORSE_PBTL.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ztubt_placedbettransactionheaderlifecyclestate,
        left_on='HORSE_PBTL.TRANHEADERID',
        right_on='PBTHLCS.TRANHEADERID',
        how='inner'
    ).loc[
        (df_ztubt_horse_placedbettransactionlineitem['HORSE_PBTL.BOARDREBATEAMOUNT'] > 0) &
        (df_ztubt_horse_placedbettransactionlineitem['HORSE_PBTL.BOARDREBATEAMOUNT'].notnull()) &
        (
            (df_ztubt_horse_placedbettransactionlineitem['HORSE_PBTL.CREATEDDATE'] >= FromDateUTC) &
            (df_ztubt_horse_placedbettransactionlineitem['HORSE_PBTL.CREATEDDATE'] <= ToDateUTC)
        ) &
        (df_ztubt_placedbettransactionheaderlifecyclestate['PBTHLCS.BETSTATETYPEID'] == 'PB06')
    ][['TT.TERDISPLAYID', 'HORSE_PBTL.CREATEDDATE', 'HORSE_PBTL.TRANHEADERID']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        FIRSTDATE = None,
        FIRSTCOUNT=lambda x: np.where(
            (x['HORSE_PBTL.CREATEDDATE'] >= x['TT.SESSIONSTARTDATETIMEUTC']) &
            (x['HORSE_PBTL.CREATEDDATE'] <= (x['TT.SESSIONSTARTDATETIMEUTC'] + pd.Timedelta(hours=1))),
            1,
            None
        ),
        LASTCOUNT=lambda x: np.where(
            (x['HORSE_PBTL.CREATEDDATE'] >= (x['TT.SESSIONENDDATETIMEUTC'] - pd.Timedelta(hours=1))) &
            (x['HORSE_PBTL.CREATEDDATE'] <= x['TT.SESSIONENDDATETIMEUTC']),
            1,
            None
        ),
        TRANHEADERLINEITEMID=lambda x: x['HORSE_PBTL.TRANHEADERID']
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]
    # Apply select distinct
    df_merged = df_merged.drop_duplicates()
    # Append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)
 #===============================================================================================================================================================

    # SELECT F.TerDisplayID, NULL, FirstCount, LastCount, TransactionHeaderID
	# 	FROM
	# 	(
	# 		SELECT tt.TerDisplayID,
	# 		 PBT.CreatedDate
	# 		AS FirstDate,
	# 		CASE
	# 			WHEN (
	# 			PB.TransactionTime >= tt.SessionStartDateTime
	# 			AND PB.TransactionTime <= ( tt.SessionStartDateTime + INTERVAL'1 Hour') :: timestamp
	# 			) THEN 1
	# 			ELSE NULL
	# 		END AS FirstCount,
	# 		CASE
	# 			WHEN (
	# 			PB.TransactionTime >= ( tt.SessionEndDateTime - INTERVAL'1 Hour') :: timestamp
	# 			AND PB.TransactionTime <= tt.SessionEndDateTime
	# 			) THEN 1
	# 			ELSE NULL
	# 		END AS LastCount
	# 		, PB.TransactionHeaderID
	# 		FROM ubt_temp_t tt
	# 		INNER JOIN ztubt_admissionvouchertransaction PBT ON tt.TerDisplayID = PBT.TerDisplayID
	# 		INNER JOIN ztubt_offlineproductheader PB ON PBT.HeaderID = PB.TransactionHeaderID
	# 		WHERE PBT.IsActive = True
	# 		AND  (PB.TransactionTime >= FromDate
	# 		AND  PB.TransactionTime <= ToDate)
	# 	)F
    df_merged = df_ubt_temp_t.merge(
        df_ztubt_admissionvouchertransaction,
        left_on='TT.TERDISPLAYID',
        right_on='AVT.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ztubt_offlineproductheader,
        left_on='AVT.HEADERID',
        right_on='OPH.TRANSACTIONHEADERID',
        how='inner'
    ).loc[
        (df_ztubt_admissionvouchertransaction['AVT.ISACTIVE'] == True) &
        (
            (df_ztubt_offlineproductheader['OPH.TRANSACTIONTIME'] >= FromDate) &
            (df_ztubt_offlineproductheader['OPH.TRANSACTIONTIME'] <= ToDate)
        )
    ][['TT.TERDISPLAYID', 'AVT.CREATEDDATE', 'OPH.TRANSACTIONHEADERID']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        FIRSTDATE = None,
        FIRSTCOUNT=lambda x: np.where(
            (x['OPH.TRANSACTIONTIME'] >= x['TT.SESSIONSTARTDATETIME']) &
            (x['OPH.TRANSACTIONTIME'] <= (x['TT.SESSIONSTARTDATETIME'] + pd.Timedelta(hours=1))),
            1,
            None
        ),
        LASTCOUNT=lambda x: np.where(
            (x['OPH.TRANSACTIONTIME'] >= (x['TT.SESSIONENDDATETIME'] - pd.Timedelta(hours=1))) &
            (x['OPH.TRANSACTIONTIME'] <= x['TT.SESSIONENDDATETIME']),
            1,
            None
        ),
        TRANHEADERLINEITEMID=lambda x: x['OPH.TRANSACTIONHEADERID']
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]

    # append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)
#===============================================================================================================================================================

    # SELECT F.TerDisplayID, NULL, F.FirstCount, F.LastCount, CharityTransactionID
	# 	FROM
	# 	(
	# 		SELECT tt.TerDisplayID,
	# 		CT.CreatedDate
	# 		AS FirstDate,
	# 		CASE
	# 			WHEN (
	# 			CT.CreatedDate >= tt.SessionStartDateTimeUTC
	# 			AND CT.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
	# 			) THEN CT.RepeatedTicket
	# 			ELSE 0
	# 		END AS FirstCount,
	# 		CASE
	# 			WHEN (
	# 			CT.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
	# 			AND CT.CreatedDate <=  tt.SessionEndDateTimeUTC
	# 			) THEN CT.RepeatedTicket
	# 			ELSE 0
	# 		END AS LastCount
	# 		,CT.CharityTransactionID
	# 		FROM ubt_temp_t tt
	# 		INNER JOIN ztubt_charitytickettransaction CT ON CT.TerDisplayID = tt.TerDisplayID
	# 		WHERE
	# 		CT.CreatedDate >= FromDateUTC
	# 		AND  CT.CreatedDate <= ToDateUTC
	# 	)F
    df_merged = df_ubt_temp_t.merge(
        df_ztubt_charitytickettransaction,
        left_on='TT.TERDISPLAYID',
        right_on='CT.TERDISPLAYID',
        how='inner'
    ).loc[
        (
            (df_ztubt_charitytickettransaction['CT.CREATEDDATE'] >= FromDateUTC) &
            (df_ztubt_charitytickettransaction['CT.CREATEDDATE'] <= ToDateUTC)
        )
    ][['TT.TERDISPLAYID', 'CT.CREATEDDATE', 'CT.CHARITYTRANSACTIONID', 'CT.REPEATEDTICKET']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        FIRSTDATE=None,
        FIRSTCOUNT=lambda x: np.where(
            (x['CT.CREATEDDATE'] >= x['TT.SESSIONSTARTDATETIMEUTC']) &
            (x['CT.CREATEDDATE'] <= (x['TT.SESSIONSTARTDATETIMEUTC'] + pd.Timedelta(hours=1))),
            x['CT.REPEATEDTICKET'],
            0
        ),
        LASTCOUNT=lambda x: np.where(
            (x['CT.CREATEDDATE'] >= (x['TT.SESSIONENDDATETIMEUTC'] - pd.Timedelta(hours=1))) &
            (x['CT.CREATEDDATE'] <= x['TT.SESSIONENDDATETIMEUTC']),
            x['CT.REPEATEDTICKET'],
            0
        ),
        TRANHEADERLINEITEMID=lambda x: x['CT.CHARITYTRANSACTIONID']
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]

    # append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)

#===============================================================================================================================================================

    # SELECT F.TerDisplayID, NULL, F.FirstCount, F.LastCount,HeaderID
	# 	FROM
	# 	(
	# 		select tt.TerDisplayID,
	# 		ST.CreatedDate
	# 		AS FirstDate,
	# 		CASE
	# 			WHEN (
	# 			ST.CreatedDate >=  tt.SessionStartDateTimeUTC
	# 			AND ST.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
	# 			) THEN ST.RepeatedTicket
	# 			ELSE 0
	# 		END AS FirstCount,
	# 		CASE
	# 			WHEN (
	# 			ST.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
	# 			AND ST.CreatedDate <=  tt.SessionEndDateTimeUTC
	# 			) THEN ST.RepeatedTicket
	# 			ELSE 0
	# 		END AS LastCount
	# 		,ST.HeaderID
	# 		from ubt_temp_t tt
	# 		INNER JOIN ztubt_spatransaction ST ON ST.TerDisplayID = tt.TerDisplayID
	# 		WHERE
	# 		ST.CreatedDate >= FromDateUTC
	# 		AND  ST.CreatedDate <= ToDateUTC
	# 	)F
    df_merged = df_ubt_temp_t.merge(
        df_ztubt_spatransaction,
        left_on='TT.TERDISPLAYID',
        right_on='ST.TERDISPLAYID',
        how='inner'
    ).loc[
        (
            (df_ztubt_spatransaction['ST.CREATEDDATE'] >= FromDateUTC) &
            (df_ztubt_spatransaction['ST.CREATEDDATE'] <= ToDateUTC)
        )
    ][['TT.TERDISPLAYID', 'ST.CREATEDDATE', 'ST.HEADERID', 'ST.REPEATEDTICKET']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        FIRSTDATE=lambda x: x['ST.CREATEDDATE'],
        FIRSTCOUNT=lambda x: np.where(
            (x['ST.CREATEDDATE'] >= x['TT.SESSIONSTARTDATETIMEUTC']) &
            (x['ST.CREATEDDATE'] <= (x['TT.SESSIONSTARTDATETIMEUTC'] + pd.Timedelta(hours=1))),
            x['ST.REPEATEDTICKET'],
            0
        ),
        LASTCOUNT=lambda x: np.where(
            (x['ST.CREATEDDATE'] >= (x['TT.SESSIONENDDATETIMEUTC'] - pd.Timedelta(hours=1))) &
            (x['ST.CREATEDDATE'] <= x['TT.SESSIONENDDATETIMEUTC']),
            x['ST.REPEATEDTICKET'],
            0
        ),
        TRANHEADERLINEITEMID=lambda x: x['ST.HEADERID']
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]

    #append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)
# ================================================================================================================================================================

    # SELECT F.TerDisplayID, NULL, F.FirstCount, F.LastCount, HeaderID
	# 	FROM
	# 	(
	# 		SELECT tt.TerDisplayID,
	# 		TCT.CreatedDate
	# 		AS FirstDate,
	# 		CASE
	# 			WHEN (
	# 			TCT.CreatedDate >= tt.SessionStartDateTimeUTC
	# 			AND TCT.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
	# 			) THEN TCT.RepeatedTicket
	# 			ELSE 0
	# 		END AS FirstCount,
	# 		CASE
	# 			WHEN (
	# 			TCT.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
	# 			AND TCT.CreatedDate <=  tt.SessionEndDateTimeUTC
	# 			) THEN TCT.RepeatedTicket
	# 			else 0
	# 		END AS LastCount
	# 		,TCT.HeaderID
	# 		FROM ubt_temp_t tt
	# 		INNER JOIN ztubt_topupcardtransaction TCT ON TCT.TerDisplayID = tt.TerDisplayID
	# 		WHERE
	# 		TCT.CreatedDate >= FromDateUTC
	# 		AND  TCT.CreatedDate <= ToDateUTC
	# 	)F
    df_merged = df_ubt_temp_t.merge(
        df_ztubt_topupcardtransaction,
        left_on='TT.TERDISPLAYID',
        right_on='TCT.TERDISPLAYID',
        how='inner'
    ).loc[
        (
            (df_ztubt_topupcardtransaction['TCT.CREATEDDATE'] >= FromDateUTC) &
            (df_ztubt_topupcardtransaction['TCT.CREATEDDATE'] <= ToDateUTC)
        )
    ][['TT.TERDISPLAYID', 'TCT.CREATEDDATE', 'TCT.HEADERID', 'TCT.REPEATEDTICKET']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        FIRSTDATE=None,
        FIRSTCOUNT=lambda x: np.where(
            (x['TCT.CREATEDDATE'] >= x['TT.SESSIONSTARTDATETIMEUTC']) &
            (x['TCT.CREATEDDATE'] <= (x['TT.SESSIONSTARTDATETIMEUTC'] + pd.Timedelta(hours=1))),
            x['TCT.REPEATEDTICKET'],
            0
        ),
        LASTCOUNT=lambda x: np.where(
            (x['TCT.CREATEDDATE'] >= (x['TT.SESSIONENDDATETIMEUTC'] - pd.Timedelta(hours=1))) &
            (x['TCT.CREATEDDATE'] <= x['TT.SESSIONENDDATETIMEUTC']),
            x['TCT.REPEATEDTICKET'],
            0
        ),
        TRANHEADERLINEITEMID=lambda x: x['TCT.HEADERID']
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]

    # append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)
# ================================================================================================================================================================

    # SELECT F.TerDisplayID, NULL, F.FirstCount,F.LastCount, HeaderID
	# 	FROM
	# 	(
	# 		SELECT tt.TerDisplayID,
	# 		 THT.CreatedDate
	# 		AS FirstDate,
	# 		CASE
	# 			WHEN (
	# 			 THT.CreatedDate >= tt.SessionStartDateTimeUTC
	# 			AND  THT.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
	# 			) THEN  THT.RepeatedTicket
	# 			ELSE 0
	# 		END AS FirstCount,
	# 		CASE
	# 			WHEN (
	# 			 THT.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
	# 			 AND THT.CreatedDate <= tt.SessionEndDateTimeUTC
	# 			) THEN  THT.RepeatedTicket
	# 			ELSE 0
	# 		END AS LastCount
	# 		, THT.HeaderID
	# 		FROM ubt_temp_t tt
	# 		INNER JOIN ztubt_totohongbaotransaction THT ON THT.TerDisplayID = tt.TerDisplayID
	# 		WHERE
	# 		THT.CreatedDate >= FromDateUTC
	# 		AND  THT.CreatedDate <= ToDateUTC
	# 	)F

    df_merged = df_ubt_temp_t.merge(
        df_ztubt_totohongbaotransaction,
        left_on='TT.TERDISPLAYID',
        right_on='THT.TERDISPLAYID',
        how='inner'
    ).loc[
        (
            (df_ztubt_totohongbaotransaction['THT.CREATEDDATE'] >= FromDateUTC) &
            (df_ztubt_totohongbaotransaction['THT.CREATEDDATE'] <= ToDateUTC)
        )
    ][['TT.TERDISPLAYID', 'THT.CREATEDDATE', 'THT.HEADERID', 'THT.REPEATEDTICKET']
    ].assign(
        TERDISPLAYID=lambda x: x['TT.TERDISPLAYID'],
        FIRSTDATE=None,
        FIRSTCOUNT=lambda x: np.where(
            (x['THT.CREATEDDATE'] >= x['TT.SESSIONSTARTDATETIMEUTC']) &
            (x['THT.CREATEDDATE'] <= (x['TT.SESSIONSTARTDATETIMEUTC'] + pd.Timedelta(hours=1))),
            x['THT.REPEATEDTICKET'],
            0
        ),
        LASTCOUNT=lambda x: np.where(
            (x['THT.CREATEDDATE'] >= (x['TT.SESSIONENDDATETIMEUTC'] - pd.Timedelta(hours=1))) &
            (x['THT.CREATEDDATE'] <= x['TT.SESSIONENDDATETIMEUTC']),
            x['THT.REPEATEDTICKET'],
            0
        ),
        TRANHEADERLINEITEMID=lambda x: x['THT.HEADERID']
    )[['TERDISPLAYID', 'FIRSTDATE', 'FIRSTCOUNT', 'LASTCOUNT', 'TRANHEADERLINEITEMID']]
    # append to df_ubt_temp_count
    df_ubt_temp_count = pd.concat([df_ubt_temp_count, df_merged], ignore_index=True)

    return df_ubt_temp_count


def final_select(FromDate, ToDate, df_ubt_temp_t, df_ubt_temp_count):

    # SELECT loc.LocName AS LocationName,
	# 	loc.LocDisplayID  AS LocationID,
    #     ter.TerDisplayID AS TerminalID,
	#     to_char(oh.OpenTime::time , 'HH24:MI:ss' ) AS OpenTime,
	#     to_char(tt.SessionStartDateTime::date, 'dd.MM.yyyy') AS SignOnDate,
	#     to_char(tt.SessionStartDateTime::time,'HH24:MI:ss' ) AS SignOnTime,
	#     to_char( TCount.FirstSaleDateTime +( 8 * interval'1 hour'), 'HH24:MI:ss') AS FirstSaleTime, --convert to SGT--(2)
	#     Coalesce(TCount.FirstHourCount, 0):: int AS FirstHourCount,
	#     to_char(oh.CloseTime::time , 'HH24:MI:ss' ) AS CloseTime,
	#     to_char(tt.SessionEndDateTime::date, 'dd.MM.yyyy') AS SignOffDate,
	#     to_char(tt.SessionEndDateTime::time,'HH24:MI:ss' ) AS SignOffTime,
	#     to_char( TCount.LastSaleDateTime +( 8 * interval'1 hour'), 'HH24:MI:ss') AS LastSaleTime,
	#     Coalesce(TCount.LastHourCount, 0):: int AS LastHourCount
	# 	FROM ztubt_terminal ter
	#     LEFT JOIN ztubt_operatinghours oh
	# 	ON oh.LocID = ter.LocID
	# 	AND oh.IsDeleted = False
	# 	AND oh.DayNo = (case  extract(dow from  date (FromDate - interval '1 Day')) when 7 then 1 else
	# 		extract(dow from  date (FromDate - interval '1 Day'))+1 end)
	#     LEFT JOIN ztubt_location loc
	# 		ON loc.LocID = ter.LocID
	# 		AND loc.IsDeleted = False
	#     LEFT JOIN ubt_temp_t tt ON ter.TerDisplayID = tt.TerDisplayID
	#     LEFT JOIN
	#     (
	# 	SELECT TerDisplayID, min(FirstDate) as FirstSaleDateTime, SUM(FirstCount) AS FirstHourCount, max(FirstDate) as LastSaleDateTime, sum(LastCount) as LastHourCount
	# 	from ubt_temp_count
	# 	GROUP BY TerDisplayID
	#     ) TCount on ter.TerDisplayID = TCount.TerDisplayID
	#     WHERE ter.IsDeleted = False
	#     ORDER BY loc.LocName,  ter.TerDisplayID;

    query = """
    select * from ZTUBT_OPERATINGHOURS OH
    WHERE

    OH.DAYNO = (case  extract(dow from  date ('{FromDate}'::timestamp - interval '1 Day')) when 7 then 1 else
    extract(dow from  date ('{FromDate}'::timestamp - interval '1 Day'))+1 end)
    and OH.ISDELETED = False
    """
    final_select_data_frame = df_ztubt_terminal.merge(
        df_ztubt_operatinghours,
        left_on='TER.LOCID',
        right_on='OH.LOCID',
        how='left'
    ).merge(
        df_ztubt_location,
        left_on='TER.LOCID',
        right_on='LOC.LOCID',
        how='left'
    ).merge(
        df_ubt_temp_t,
        left_on='TER.TERDISPLAYID',
        right_on='TT.TERDISPLAYID',
        how='left'
    ).merge(
        df_ubt_temp_count.groupby('TERDISPLAYID').agg(
            FIRSTSALEDATETIME=('FIRSTDATE', 'min'),
            FIRSTHOURCOUNT=('FIRSTCOUNT', 'sum'),
            LASTSALEDATETIME=('FIRSTDATE', 'max'),
            LASTHOURCOUNT=('LASTCOUNT', 'sum')
        ).reset_index(),
        left_on='TER.TERDISPLAYID',
        right_on='TERDISPLAYID',
        how='left'
    ).loc[
        (df_ztubt_operatinghours['OH.ISDELETED'] == False) &
        (df_ztubt_terminal['TER.ISDELETED'] == False) &
        (df_ztubt_location['LOC.ISDELETED'] == False)
    ][['LOC.LOCNAME', 'LOC.LOCDISPLAYID', 'TER.TERDISPLAYID', 'OH.OPENTIME', 'TT.SESSIONSTARTDATETIME', 'TT.SESSIONSTARTDATETIMEUTC', 'TCOUNT.FIRSTSALEDATETIME', 'TCOUNT.FIRSTHOURCOUNT', 'OH.CLOSETIME', 'TT.SESSIONENDDATETIME', 'TT.SESSIONENDDATETIMEUTC', 'TCOUNT.LASTSALEDATETIME', 'TCOUNT.LASTHOURCOUNT']]


    final_select_data_frame = final_select_data_frame.assign(
        LOCATIONNAME=lambda x: x['LOC.LOCNAME'],
        LOCATIONID=lambda x: x['LOC.LOCDISPLAYID'],
        TERMINALID=lambda x: x['TER.TERDISPLAYID'],
        OPENTIME=lambda x: x['OH.OPENTIME'].dt.strftime('%H:%M:%S'),
        SIGNONDATE=lambda x: x['TT.SESSIONSTARTDATETIME'].dt.strftime('%d.%m.%Y'),
        SIGNONTIME=lambda x: x['TT.SESSIONSTARTDATETIME'].dt.strftime('%H:%M:%S'),
        FIRSTSALETIME=lambda x: (x['TCOUNT.FIRSTSALEDATETIME'] + pd.Timedelta(hours=8)).dt.strftime('%H:%M:%S'),
        FIRSTHOURCOUNT=lambda x: x['TCOUNT.FIRSTHOURCOUNT'].fillna(0).astype(int),
        CLOSETIME=lambda x: x['OH.CLOSETIME'].dt.strftime('%H:%M:%S'),
        SIGNOFFDATE=lambda x: x['TT.SESSIONENDDATETIME'].dt.strftime('%d.%m.%Y'),
        SIGNOFFTIME=lambda x: x['TT.SESSIONENDDATETIME'].dt.strftime('%H:%M:%S'),
        LASTSALETIME=lambda x: (x['TCOUNT.LASTSALEDATETIME'] + pd.Timedelta(hours=8)).dt.strftime('%H:%M:%S'),
        LASTHOURCOUNT=lambda x: x['TCOUNT.LASTHOURCOUNT'].fillna(0).astype(int)
    )[['LOCATIONNAME', 'LOCATIONID', 'TERMINALID', 'OPENTIME', 'SIGNONDATE', 'SIGNONTIME', 'FIRSTSALETIME', 'FIRSTHOURCOUNT', 'CLOSETIME', 'SIGNOFFDATE', 'SIGNOFFTIME', 'LASTSALETIME', 'LASTHOURCOUNT']
    ].sort_values(by=['LOCATIONNAME', 'TERMINALID']).reset_index(drop=True)


    return final_select_data_frame