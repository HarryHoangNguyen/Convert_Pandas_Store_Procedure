
CREATE FUNCTION public.sp_ubt_getoperatinghours(transactiondate date) RETURNS
TABLE(locationname character varying, locationid character varying, terminalid character varying, opentime text, signondate text, signontime text, firstsaletime text, firsthourcount integer, closetime text, signoffdate text, signofftime text, lastsaletime text, lasthourcount integer)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column

	declare
	FromDate timestamp ;
	ToDate timestamp;
	ConvertToSGT int4;
	ConvertToUTC int4;
	FromDateUTC timestamp ;
	ToDateUTC timestamp ;

	BEGIN
		Select(TransactionDate - interval '0 Day') into FromDate;

		select (((date_part('day', current_timestamp at time zone 'utc')- date_part('day', current_timestamp)) *24) + (date_part('hour',current_timestamp at time zone 'utc'  )- date_part('hour',current_timestamp )))
		into ConvertToUTC;

	    select (((date_part('day',current_timestamp)- date_part('day', current_timestamp at time zone 'utc')) *24) + (date_part('hour',current_timestamp  )- date_part('hour',current_timestamp at time zone 'utc' )))
	    into ConvertToSGT;

		SELECT
		   PreviousPeriodDateTime  ,
		   PeriodDateTime  into FromDate, ToDate
	    FROM ztubt_getbusinessdate_perhost zgp
	    WHERE ActualDate = FromDate
	    ORDER BY PreviousPeriodDateTime DESC, PeriodDateTime desc
	    limit 1 ;

	    select(FromDate + ( ConvertToUTC * interval'1 Hour' ) ) into  FromDateUTC;
	    select( ToDate + ( ConvertToUTC * interval'1 Hour' )) into  ToDateUTC;


	    create temp table if not exists ubt_temp_t
	     (
		    TerDisplayID VARCHAR(200),
		    SessionStartDateTime timestamp,
		    SessionEndDateTime timestamp,
		    SessionStartDateTimeUTC timestamp,
		    SessionEndDateTimeUTC timestamp,
		    SweepIndicator int4
	    );

	    create temp table if not exists ubt_temp_count
	     (
		   TerDisplayID VARCHAR(200),
		   FirstDate timestamp,
		   FirstCount int4,
		   LastCount int4,
		   TranHeaderLineItemId VARCHAR(500)
	     );



	    insert into ubt_temp_t
	    select SESS.TerDisplayID, MIN(SessionStartDateTime) AS SessionStartDateTime, MAX(SessionEndDateTime) AS SessionEndDateTime,
	    MIN(SessionStartDateTime + (convertToUTC * interval'1 Hour')) AS SessionStartDateTime, MAX( SessionEndDateTime + (convertToUTC * interval'1 Hour')) AS SessionEndDateTime,--convert to UTC--(3)
	    LOC.SweepIndicator
	    FROM ztubt_session SESS
	    inner join ztubt_terminal ter on SESS.TerDisplayID=ter.TerDisplayID
	    inner join ztubt_location LOC on TER.LocID=LOC.LocID
	    WHERE SessionStartDateTime >= FromDate
	    AND SessionStartDateTime <= ToDate
	    GROUP BY SESS.TerDisplayID, LOC.SweepIndicator ;



	--collection
		--all product
		insert into ubt_temp_count
		SELECT F.TerDisplayID, F.FirstDate, F.FirstCount,  F.LastCount, F.TranHeaderID
		FROM
		(
			SELECT DISTINCT
			tha.TerDisplayID,
			tha.CreatedDate
			AS FirstDate,
			CASE
				WHEN (
				 tha.CreatedDate >= tt.SessionStartDateTimeUTC
				AND tha.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
				) then 1
				ELSE NULL
			END AS FirstCount,
			CASE
				WHEN (
				tha.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
				AND tha.CreatedDate <= tt.SessionEndDateTimeUTC
				) then 1
				ELSE NULL
			END AS LastCount
			,tha.TranHeaderID
			FROM ubt_temp_t tt
			INNER JOIN ztubt_placedbettransactionheader tha ON tt.TerDisplayID = tha.TerDisplayID
			INNER JOIN ztubt_placedbettransactionheaderlifecyclestate LCS ON tha.TranHeaderID = LCS.TranHeaderID AND BetStateTypeID = 'PB06'
			LEFT JOIN ztubt_toto_placedbettransactionlineitem PBTL ON tha.TranHeaderID = PBTL.TranHeaderID --TOTO
			LEFT JOIN ztubt_sweep_placedbettransactionlineitemnumber PBTLN ON tha.TranHeaderID = PBTLN.TranHeaderID
			LEFT JOIN ztubt_sweep_placedbettransactionlineitem SPPBTL on SPPBTL.TranLineItemID=PBTLN.TranLineItemID
			WHERE
			(tha.CreatedDate >= FromDateUTC
			AND tha.CreatedDate <= ToDateUTC)
			and tha.isExchangeTicket=False
			and IsBetRejectedByTrader=False
			AND (PBTL.GroupUnitSequence = 1
			OR PBTL.GroupUnitSequence IS NULL) --for group toto
			AND (PBTLN.IsSoldOut IS NULL OR (PBTLN.IsSoldOut IS NOT NULL AND PBTLN.IsSoldOut = False)) --for sweep
			AND (tt.SweepIndicator IS NULL OR (tt.SweepIndicator IS NOT NULL AND SPPBTL.IsPrinted IS NOT NULL))
		)F

		UNION ALL
		SELECT F.TerDisplayID, F.FirstDate, F.FirstCount,  F.LastCount, F.TranHeaderID
		FROM
		(
			SELECT DISTINCT
			tt.TerDisplayID,
			tha.CreatedDate
			AS FirstDate,
			CASE
				WHEN (
				tha.CreatedDate >= tt.SessionStartDateTimeUTC
				AND tha.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
				) THEN 1
				ELSE NULL
			END AS FirstCount,
			CASE
				WHEN (
				tha.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
				AND tha.CreatedDate <= tt.SessionEndDateTimeUTC
				) THEN 1
				ELSE NULL
			END AS LastCount
			,tha.TranHeaderID
			FROM ubt_temp_t tt
			INNER JOIN ztubt_placedbettransactionheader tha ON tt.TerDisplayID = tha.TerDisplayID
			INNER JOIN ztubt_placedbettransactionheaderlifecyclestate LCS ON tha.TranHeaderID = LCS.TranHeaderID AND BetStateTypeID  = 'PB03'
			INNER JOIN ztubt_sweep_placedbettransactionlineitem PBTL ON tha.TranHeaderID = PBTL.TranHeaderID
			INNER JOIN ztubt_sweep_placedbettransactionlineitemnumber PBTLN ON PBTL.TranLineItemID = PBTLN.TranLineItemID AND PBTLN.IsSoldOut = False
			WHERE
			(tha.CreatedDate >= FromDateUTC
			AND tha.CreatedDate <= ToDateUTC)
			AND IsBetRejectedByTrader = False
			AND (tt.SweepIndicator IS NOT NULL AND IsPrinted IS NULL)
		)F

	    UNION ALL
		SELECT F.TerDisplayID, NULL, F.FirstCount, F.LastCount, F.TranHeaderID
		FROM
		(
			SELECT DISTINCT
			tt.TerDisplayID,
			C.CancelledDate
			AS FirstDate,
			CASE
				WHEN (
				C.CancelledDate >= tt.SessionStartDateTime
				AND C.CancelledDate <= ( tt.SessionStartDateTime + INTERVAL'1 Hour') :: timestamp
				) THEN 1
				ELSE NULL
			END AS FirstCount,
						CASE
				WHEN (
				C.CancelledDate >= ( tt.SessionEndDateTime - INTERVAL'1 Hour') :: timestamp
				AND C.CancelledDate <= tt.SessionEndDateTime
				) THEN 1
				ELSE NULL
			END AS LastCount
			,tha.TranHeaderID
			FROM ubt_temp_t tt
			INNER JOIN ztubt_cancelledbetticket C ON  tt.TerDisplayID = C.TerDisplayID
			INNER JOIN ztubt_cancelledbetticketlifecyclestate LCS ON C.TranHeaderID = LCS.TranHeaderID AND BetStateTypeID = 'CB06'
			INNER JOIN ztubt_placedbettransactionheader tha ON C.TranHeaderID = tha.TranHeaderID
			LEFT JOIN ztubt_toto_placedbettransactionlineitem PBTL ON C.TranHeaderID = PBTL.TranHeaderID
			WHERE
			(C.CancelledDate >= FromDate
			AND C.CancelledDate <= ToDate)
			AND ((PBTL.GroupUnitSequence IS NOT NULL AND C.CancelledAmout != 0) OR PBTL.GroupUnitSequence IS NULL) --for group toto
			AND tha.IsBetRejectedByTrader = False
		)F

		union ALL (4)
		SELECT TerDisplayID, NULL, FirstCount AS FirstCount,LastCount AS LastCount, NULL
		FROM
		(

			SELECT TerDisplayID, Types, TotalAmount,FirstDate, FirstCount, LastCount
			FROM
				(
					SELECT tt.TerDisplayID, V.TranHeaderID AS HeaderID,
					coalesce (WinningAmount,0) AS VALIDATED,
					coalesce (RebateAmount,0) AS LOSING_BET_REBATE,
					coalesce (RefundAmount,0) AS REFUNDED,
					V.ValidationDate AS FirstDate,
					CASE
						WHEN (
						V.ValidationDate >= tt.SessionStartDateTime
						AND V.ValidationDate <= ( tt.SessionStartDateTime + INTERVAL'1 Hour') :: timestamp
						)  then 1
						ELSE NULL
					END AS FirstCount,
					CASE
						WHEN (
						V.ValidationDate >=  ( tt.SessionEndDateTime - INTERVAL'1 Hour') :: timestamp
						AND V.ValidationDate <= tt.SessionEndDateTime
						)  THEN 1
						ELSE NULL
					END AS LastCount
					FROM ubt_temp_t tt
					INNER JOIN ztubt_validatedbetticket V ON tt.TerDisplayID = V.TerDisplayID
					INNER JOIN ztubt_validatedbetticketlifecyclestate  LCS ON V.TranHeaderID = LCS.TranHeaderID AND BetStateTypeID = 'VB06'
					WHERE
					V.ValidationDate >= FromDate
					and V.ValidationDate <= ToDate
				) p
				cross join lateral (
				values (p.VALIDATED, 'VALIDATED'),
				       (p.LOSING_BET_REBATE, 'LOSING_BET_REBATE'),
				       (p.REFUNDED, 'REFUNDED')
				)b(TotalAmount, Types)
			) a
			WHERE TotalAmount != 0

				/*UNPIVOT
				(
					TotalAmount For Types IN
						([VALIDATED],  [LOSING BET REBATE], [REFUNDED])
				) AS unpvt*/


		union ALL
		SELECT H.TerDisplayID, NULL, FirstCount,LastCount, TranHeaderID
		FROM
		(
			SELECT DISTINCT
			tt.TerDisplayID, PBTL.TranHeaderID,
			PBTL.CreatedDate
			AS FirstDate,
			CASE
				WHEN (
				PBTL.CreatedDate >= tt.SessionStartDateTimeUTC
				AND PBTL.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
				) THEN 1
				ELSE NULL
			END AS FirstCount,
			CASE
				WHEN (
				PBTL.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
				AND PBTL.CreatedDate <= tt.SessionEndDateTimeUTC
				) THEN 1
				ELSE NULL
			END AS LastCount
			FROM ubt_temp_t tt
			INNER JOIN ztubt_horse_placedbettransactionlineitem PBTL on PBTL.TerDisplayID=tt.TerDisplayID
			INNER JOIN ztubt_placedbettransactionheaderlifecyclestate PBLC ON PBTL.TranHeaderID = PBLC.TranHeaderID AND PBLC.BetStateTypeID = 'PB06'
			WHERE PBTL.BoardRebateAmount > 0 AND PBTL.BoardRebateAmount IS NOT NULL
			AND (PBTL.CreatedDate >= FromDateUTC
			AND PBTL.CreatedDate <= ToDateUTC)
		) H

		union ALL
		SELECT F.TerDisplayID, NULL, FirstCount, LastCount, TransactionHeaderID
		FROM
		(
			SELECT tt.TerDisplayID,
			 PBT.CreatedDate
			AS FirstDate,
			CASE
				WHEN (
				PB.TransactionTime >= tt.SessionStartDateTime
				AND PB.TransactionTime <= ( tt.SessionStartDateTime + INTERVAL'1 Hour') :: timestamp
				) THEN 1
				ELSE NULL
			END AS FirstCount,
			CASE
				WHEN (
				PB.TransactionTime >= ( tt.SessionEndDateTime - INTERVAL'1 Hour') :: timestamp
				AND PB.TransactionTime <= tt.SessionEndDateTime
				) THEN 1
				ELSE NULL
			END AS LastCount
			, PB.TransactionHeaderID
			FROM ubt_temp_t tt
			INNER JOIN ztubt_admissionvouchertransaction PBT ON tt.TerDisplayID = PBT.TerDisplayID
			INNER JOIN ztubt_offlineproductheader PB ON PBT.HeaderID = PB.TransactionHeaderID
			WHERE PBT.IsActive = True
			AND  (PB.TransactionTime >= FromDate
			AND  PB.TransactionTime <= ToDate)
		)F


		union ALL
		SELECT F.TerDisplayID, NULL, F.FirstCount, F.LastCount, CharityTransactionID
		FROM
		(
			SELECT tt.TerDisplayID,
			CT.CreatedDate
			AS FirstDate,
			CASE
				WHEN (
				CT.CreatedDate >= tt.SessionStartDateTimeUTC
				AND CT.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
				) THEN CT.RepeatedTicket
				ELSE 0
			END AS FirstCount,
			CASE
				WHEN (
				CT.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
				AND CT.CreatedDate <=  tt.SessionEndDateTimeUTC
				) THEN CT.RepeatedTicket
				ELSE 0
			END AS LastCount
			,CT.CharityTransactionID
			FROM ubt_temp_t tt
			INNER JOIN ztubt_charitytickettransaction CT ON CT.TerDisplayID = tt.TerDisplayID
			WHERE
			CT.CreatedDate >= FromDateUTC
			AND  CT.CreatedDate <= ToDateUTC
		)F

		union ALL
		SELECT F.TerDisplayID, NULL, F.FirstCount, F.LastCount,HeaderID
		FROM
		(
			select tt.TerDisplayID,
			ST.CreatedDate
			AS FirstDate,
			CASE
				WHEN (
				ST.CreatedDate >=  tt.SessionStartDateTimeUTC
				AND ST.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
				) THEN ST.RepeatedTicket
				ELSE 0
			END AS FirstCount,
			CASE
				WHEN (
				ST.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
				AND ST.CreatedDate <=  tt.SessionEndDateTimeUTC
				) THEN ST.RepeatedTicket
				ELSE 0
			END AS LastCount
			,ST.HeaderID
			from ubt_temp_t tt
			INNER JOIN ztubt_spatransaction ST ON ST.TerDisplayID = tt.TerDisplayID
			WHERE
			ST.CreatedDate >= FromDateUTC
			AND  ST.CreatedDate <= ToDateUTC
		)F


		union ALL
		SELECT F.TerDisplayID, NULL, F.FirstCount, F.LastCount, HeaderID
		FROM
		(
			SELECT tt.TerDisplayID,
			TCT.CreatedDate
			AS FirstDate,
			CASE
				WHEN (
				TCT.CreatedDate >= tt.SessionStartDateTimeUTC
				AND TCT.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
				) THEN TCT.RepeatedTicket
				ELSE 0
			END AS FirstCount,
			CASE
				WHEN (
				TCT.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
				AND TCT.CreatedDate <=  tt.SessionEndDateTimeUTC
				) THEN TCT.RepeatedTicket
				else 0
			END AS LastCount
			,TCT.HeaderID
			FROM ubt_temp_t tt
			INNER JOIN ztubt_topupcardtransaction TCT ON TCT.TerDisplayID = tt.TerDisplayID
			WHERE
			TCT.CreatedDate >= FromDateUTC
			AND  TCT.CreatedDate <= ToDateUTC
		)F

		union ALL
		SELECT F.TerDisplayID, NULL, F.FirstCount,F.LastCount, HeaderID
		FROM
		(
			SELECT tt.TerDisplayID,
			 THT.CreatedDate
			AS FirstDate,
			CASE
				WHEN (
				 THT.CreatedDate >= tt.SessionStartDateTimeUTC
				AND  THT.CreatedDate <= ( tt.SessionStartDateTimeUTC + INTERVAL'1 Hour') :: timestamp
				) THEN  THT.RepeatedTicket
				ELSE 0
			END AS FirstCount,
			CASE
				WHEN (
				 THT.CreatedDate >= ( tt.SessionEndDateTimeUTC - INTERVAL'1 Hour') :: timestamp
				 AND THT.CreatedDate <= tt.SessionEndDateTimeUTC
				) THEN  THT.RepeatedTicket
				ELSE 0
			END AS LastCount
			, THT.HeaderID
			FROM ubt_temp_t tt
			INNER JOIN ztubt_totohongbaotransaction THT ON THT.TerDisplayID = tt.TerDisplayID
			WHERE
			THT.CreatedDate >= FromDateUTC
			AND  THT.CreatedDate <= ToDateUTC
		)F ;

	 return QUERY
		SELECT loc.LocName AS LocationName,
		loc.LocDisplayID  AS LocationID,
        ter.TerDisplayID AS TerminalID,
	    to_char(oh.OpenTime::time , 'HH24:MI:ss' ) AS OpenTime,
	    to_char(tt.SessionStartDateTime::date, 'dd.MM.yyyy') AS SignOnDate,
	    to_char(tt.SessionStartDateTime::time,'HH24:MI:ss' ) AS SignOnTime,
	    to_char( TCount.FirstSaleDateTime +( 8 * interval'1 hour'), 'HH24:MI:ss') AS FirstSaleTime, --convert to SGT--(2)
	    Coalesce(TCount.FirstHourCount, 0):: int AS FirstHourCount,
	    to_char(oh.CloseTime::time , 'HH24:MI:ss' ) AS CloseTime,
	    to_char(tt.SessionEndDateTime::date, 'dd.MM.yyyy') AS SignOffDate,
	    to_char(tt.SessionEndDateTime::time,'HH24:MI:ss' ) AS SignOffTime,
	    to_char( TCount.LastSaleDateTime +( 8 * interval'1 hour'), 'HH24:MI:ss') AS LastSaleTime,
	    Coalesce(TCount.LastHourCount, 0):: int AS LastHourCount

		FROM ztubt_terminal ter
	    LEFT JOIN ztubt_operatinghours oh
		ON oh.LocID = ter.LocID
		AND oh.IsDeleted = False
		AND oh.DayNo = (case  extract(dow from  date (FromDate - interval '1 Day')) when 7 then 1 else
			extract(dow from  date (FromDate - interval '1 Day'))+1 end)


		LEFT JOIN ztubt_location loc
			ON loc.LocID = ter.LocID
			AND loc.IsDeleted = False

			
	    LEFT JOIN ubt_temp_t tt ON ter.TerDisplayID = tt.TerDisplayID
	    LEFT JOIN
	    (
		SELECT TerDisplayID, min(FirstDate) as FirstSaleDateTime, SUM(FirstCount) AS FirstHourCount, max(FirstDate) as LastSaleDateTime, sum(LastCount) as LastHourCount
		from ubt_temp_count
		GROUP BY TerDisplayID
	    ) TCount on ter.TerDisplayID = TCount.TerDisplayID
	    WHERE ter.IsDeleted = False
	    ORDER BY loc.LocName,  ter.TerDisplayID;



	   drop table if exists ubt_temp_t;
	   drop table if exists ubt_temp_count;

end;
$$;


ALTER FUNCTION public.sp_ubt_getoperatinghours(transactiondate date) OWNER TO sp_postgres;
