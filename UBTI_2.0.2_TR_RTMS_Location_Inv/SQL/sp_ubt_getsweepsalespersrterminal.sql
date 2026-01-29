CREATE FUNCTION public.sp_ubt_getsweepsalespersrterminal(in_fromdate date, in_todate date) RETURNS TABLE(terdisplayid character varying, prodid integer, prodname character varying, actualdate date, totalcount numeric, amount numeric)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
declare

	vFromDateTimeIGT		timestamp;
	vToDateTimeIGT			timestamp;
	vUTCFromDateTimeIGT 	timestamp;
	vUTCToDateTimeIGT		timestamp;

begin

	select fromdatetimeigt,todatetimeigt,utcfromdatetimeigt,utctodatetimeigt
	into vFromDateTimeIGT, vToDateTimeIGT, vUTCFromDateTimeIGT, vUTCToDateTimeIGT
	from public.sp_ubt_getcommonubtdates(in_fromdate,in_todate);



	create temp table ubt_temp_businessDateFunction as
	select * from ztubt_getbusinessdate_perhost zgp
	where cast(previousperioddatetime as date)
		between cast(vFromDateTimeIGT as date) and cast(vToDateTimeIGT as date)
	and host =1;


	CREATE temp TABLE ubt_temp_ResultSalesComWithDate
	(
		TicketSerialNumber varchar(200),
		TerDisplayID varchar(100),
		ProdID int,
		Dt DATE,
		SalesCommAmount DECIMAL(32, 12),
		SalesFactorAmount DECIMAL(32, 12),
		SecondSalesCommAmount DECIMAL(32, 12),
		SecondSalesFactorAmount DECIMAL(32, 12)
	);

	INSERT INTO   ubt_temp_ResultSalesComWithDate(TicketSerialNumber, TerDisplayID, ProdID, Dt, SalesCommAmount, SalesFactorAmount, SecondSalesCommAmount, SecondSalesFactorAmount)
	SELECT        PBTH.TicketSerialNumber, ter.TerDisplayID, PBTH.ProdID, GB.ActualDate, SUM(coalesce(PBTLN.SalesCommAmount,0)) SalesCommAmount, SUM(coalesce(PBTLN.SalesFactorAmount,0)) SalesFactorAmount, 0, 0
	FROM          public.ztubt_placedbettransactionheader PBTH
				  INNER JOIN (SELECT  T.TerDisplayID, T.LocID, Loc.SweepIndicator
							FROM public.ztubt_location LOC
							INNER JOIN public.ztubt_terminal T ON LOC.LocID = T.LocID
							WHERE (LOC.SweepIndicator IS NOT NULL OR LOC.SweepIndicator <> 0)) ter ON ter.TerDisplayID = PBTH.TerDisplayID
				  INNER JOIN public.ztubt_sweep_placedbettransactionlineitem PBTL ON PBTH.TranHeaderID = PBTL.TranHeaderID
				  INNER JOIN public.ztubt_sweep_placedbettransactionlineitemnumber PBTLN ON PBTL.TranLineItemID = PBTLN.TranLineItemID  AND PBTLN.IsSoldOut = false AND PBTH.TranHeaderID = PBTL.TranHeaderID
				  INNER JOIN public.ztubt_placedbettransactionheaderlifecyclestate PBLC ON PBLC.TranHeaderID = PBTH.TranHeaderID AND PBLC.BetStateTypeID = 'PB06'
--				  LEFT JOIN ubt_temp_businessDateFunction GB ON DATEADD(HOUR,8,PBTH.CreatedDate) BETWEEN GB.PreviousPeriodDateTime AND GB.PeriodDateTime
				  LEFT JOIN ubt_temp_businessDateFunction GB ON PBTH.CreatedDate BETWEEN gb.previousperioddatetimeutc and gb.perioddatetimeutc
	WHERE 1=1
	and PBTH.CreatedDate BETWEEN vUTCFromDateTimeIGT AND vUTCToDateTimeIGT
	GROUP BY PBTH.TicketSerialNumber, ter.TerDisplayID, PBTH.ProdID, GB.ActualDate;



	CREATE temp TABLE ubt_temp_CancelledSalesComWithDate
	(
		TicketSerialNumber varchar(200),
		TerDisplayID varchar(100),
		ProdID int,
		Dt DATE,
		SalesCommAmount DECIMAL(32, 12),
		SalesFactorAmount DECIMAL(32, 12),
		SecondSalesCommAmount DECIMAL(32, 12),
		SecondSalesFactorAmount DECIMAL(32, 12)
	);

	---------------------------------------------------------
	--Cancellation including cross day
	---------------------------------------------------------
	INSERT INTO   ubt_temp_CancelledSalesComWithDate(TicketSerialNumber, TerDisplayID, ProdID, Dt, SalesCommAmount, SalesFactorAmount, SecondSalesCommAmount, SecondSalesFactorAmount)
	SELECT CBT.TicketSerialNumber, CBT.TerDisplayID, CBT.ProdID, GB.ActualDate, SUM(coalesce(PBTLN.SalesCommAmount,0)) SalesCommAmount, SUM(coalesce(PBTLN.SalesFactorAmount,0)) SalesFactorAmount, 0, 0
	FROM public.ztubt_cancelledbetticket CBT
	INNER JOIN (SELECT  T.TerDisplayID, T.LocID, Loc.SweepIndicator
				FROM public.ztubt_location LOC
				INNER JOIN public.ztubt_terminal T ON LOC.LocID = T.LocID
				WHERE (LOC.SweepIndicator IS NOT NULL OR LOC.SweepIndicator <> 0)) ter ON ter.TerDisplayID = CBT.TerDisplayID
	INNER JOIN public.ztubt_sweep_placedbettransactionlineitem PBTL ON CBT.TranHeaderID = PBTL.TranHeaderID
	INNER JOIN public.ztubt_sweep_placedbettransactionlineitemNumber PBTLN ON PBTL.TranLineItemID = PBTLN.TranLineItemID  AND PBTLN.IsSoldOut = false AND CBT.TranHeaderID = PBTL.TranHeaderID
	INNER JOIN public.ztubt_cancelledbetticketlifecyclestate CBLC  ON CBLC.TranHeaderID = CBT.TranHeaderID AND CBLC.BetStateTypeID = 'CB06'
	LEFT JOIN ubt_temp_businessDateFunction GB ON CBT.CancelledDate BETWEEN GB.PreviousPeriodDateTime AND GB.PeriodDateTime
	WHERE	CBT.ProdID = 4 AND CBT.CancelledDate BETWEEN vFromDateTimeIGT AND vToDateTimeIGT
	AND PBTLN.CreatedDate >= vUTCFromDateTimeIGT - interval '45 days'
	AND PBTLN.CreatedDate < vUTCToDateTimeIGT
	GROUP BY CBT.TicketSerialNumber, CBT.TerDisplayID, CBT.ProdID, GB.ActualDate;



	return query
	SELECT PB.TerDisplayID
		 , PB.ProdID::int
		 , PR.ProdName
		 , PB.ActualDate
		 , SUM(coalesce(PB.TotalCount,0))as TotalCount
		 , ROUND(trunc(SUM(coalesce(PB.Amount,0)) / 100, 2),2) as Amount --(1) Convert from cents to dollar then truncate the sales amount with 2 dp
	FROM (SELECT  GATR.TerDisplayID AS TerDisplayID, GATR.ProdID AS ProdID, GATR.Dt as ActualDate,COUNT(DISTINCT GATR.TicketSerialNumber) AS TotalCount, SUM(coalesce(GATR.Sales,0)) as Amount
			FROM
			(
				SELECT RSC.TerDisplayID, RSC.ProdID, RSC.Dt, RSC.TicketSerialNumber, (coalesce(SUM(RSC.SalesFactorAmount),0)+ coalesce(SUM(RSC.SecondSalesFactorAmount),0)) AS Sales
				FROM ubt_temp_ResultSalesComWithDate RSC
				GROUP BY RSC.TerDisplayID, RSC.ProdID, RSC.Dt, RSC.TicketSerialNumber
			) GATR
			GROUP BY GATR.TerDisplayID, GATR.ProdID, GATR.Dt
			union all
			SELECT  GATC.TerDisplayID AS TerDisplayID, GATC.ProdID AS ProdID, GATC.Dt as ActualDate,	(0-COUNT(DISTINCT GATC.TicketSerialNumber)) AS TotalCount, (0-SUM(coalesce(GATC.Sales,0))) as Amount
			FROM
			(
				SELECT CSC.TerDisplayID, CSC.ProdID, CSC.Dt, CSC.TicketSerialNumber,  (coalesce(SUM(CSC.SalesFactorAmount),0)+ coalesce(SUM(CSC.SecondSalesFactorAmount),0)) AS Sales
				FROM ubt_temp_CancelledSalesComWithDate CSC
				GROUP BY CSC.TerDisplayID, CSC.ProdID, CSC.Dt, CSC.TicketSerialNumber
			) GATC
			GROUP BY GATC.TerDisplayID, GATC.ProdID, GATC.Dt
			) PB
	LEFT JOIN public.ztubt_product PR ON PB.ProdID = PR.ProdID
	GROUP BY PB.TerDisplayID, PB.ProdID, PR.ProdName, PB.ActualDate;


	drop table ubt_temp_businessDateFunction;
	drop table ubt_temp_ResultSalesComWithDate;
	drop table ubt_temp_CancelledSalesComWithDate;

end;
$$;


ALTER FUNCTION public.sp_ubt_getsweepsalespersrterminal(in_fromdate date, in_todate date) OWNER TO consultant01;
