CREATE FUNCTION public.sp_ubt_gettransamountdetails(in_fromdatetime timestamp without time zone, in_todatetime timestamp without time zone)
RETURNS TABLE(terdisplayid character varying, prodname character varying, flag character, amount numeric, ticketcount integer, actualdate date, transtype character varying)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
declare

	vFromDateTimeBMCS		timestamp;
	vToDateTimeBMCS			timestamp;
	vFromDateTimeIGT		timestamp;
	vToDateTimeIGT			timestamp;
	vFromDateTimeOB			timestamp;
	vToDateTimeOB 			timestamp;
	vFromDateTimeNH			timestamp;
	vToDateTimeNH 			timestamp;
	vFromDateFUND			date;
	vToDateFUND				date;

	vUTCFromDateTimeBMCS 	timestamp;
	vUTCToDateTimeBMCS		timestamp;
	vUTCFromDateTimeIGT 	timestamp;
	vUTCToDateTimeIGT		timestamp;
	vUTCFromDateTimeOB		timestamp;
	vUTCToDateTimeOB		timestamp;
	vUTCFromDateTimeNH		timestamp;
	vUTCToDateTimeNH		timestamp;

	status_pnqr text[];
	status_pnric text[];
	v_totomatchBettypes text[] := (SELECT array(SELECT bettypeid FROM public.sp_ubt_gettotomatchbettypes()));
begin

	-- exceute proc sp_ubt_get_businessdate_per_host()
--	perform sp_ubt_get_businessdate_per_host();

	select
		fromdatetimebmcs,
		todatetimebmcs,
		fromdatetimeigt,
		todatetimeigt,
		fromdatetimeob,
		todatetimeob,
		fromdatetimenonhost,
		todatetimenonhost,
		fromDateFund,
		ToDateFund,
		utcfromdatetimebmcs,
		utctodatetimebmcs,
		utcfromdatetimeigt,
		utctodatetimeigt,
		utcfromdatetimeob,
		utctodatetimeob,
		utcfromdatetimenonhost,
		utctodatetimenonhost
	into
		vFromDateTimeBMCS,
		vToDateTimeBMCS,
		vFromDateTimeIGT,
		vToDateTimeIGT,
		vFromDateTimeOB,
		vToDateTimeOB,
		vFromDateTimeNH,
		vToDateTimeNH,
		vFromDateFUND,
		vToDateFUND,
		vUTCFromDateTimeBMCS,
		vUTCToDateTimeBMCS,
		vUTCFromDateTimeIGT,
		vUTCToDateTimeIGT,
		vUTCFromDateTimeOB,
		vUTCToDateTimeOB,
		vUTCFromDateTimeNH,
		vUTCToDateTimeNH
	from
	public.sp_ubt_getcommonubtdates
	(in_fromDateTime,in_toDateTime)
	;

	-- Get Success/Complete status
	select public.sp_ubt_getpaynowsuccessstatus('PNQR') into status_pnqr;
	select public.sp_ubt_getpaynowsuccessstatus('PNRIC') into status_pnric;

--	drop table public.ubt_temp_trans_amt;

	create temporary table if not exists ubt_temp_trans_amt
		(
		 terdisplayid varchar(50)
	 	,prodname varchar(50)
	 	,flag char(3)
	 	,amount numeric
	 	,ticketcount int4
	 	,actualDate Date
		);

	create temporary table if not exists ubt_temp_terminal_loc
		(
		 terDisplayID varchar(50)
		,locid int4
		);

	insert into ubt_temp_terminal_loc
	select zt.terdisplayid , zl.locid from public.ztubt_location zl
	inner join public.ztubt_terminal zt on (zl.locid=zt.locid);


	create temporary table if not exists ubt_temp_ter_prod_val
		(
		 terDisplayID varchar(50)
		,prodname  varchar(100)
		);

	insert into ubt_temp_ter_prod_val
	select terdisplayid
	, zv.validationname || '-' || zp.prodname
	from ztubt_terminal zt
	cross join (
	    select case when zp.prodid =5 then 'SPORTS' else trim(zp.prodname) end as prodname
	    from ztubt_product zp
	    where zp.prodid IN (1,2,3,4,5)
    	union all
    	select 'TOTO MATCH' AS prodname
	) zp
	cross join ztubt_validationtype zv;

	create temporary table if not exists ubt_temp_ter_prod
		(
		 terDisplayID varchar(50)
		,prodname  varchar(100)
		);

	insert into ubt_temp_ter_prod
	select terdisplayid, zp.prodname
	from ztubt_terminal zt
	cross join (
	    select case when zp.prodid =5 then 'SPORTS' else trim(zp.prodname) end as prodname
	    from ztubt_product zp
	    where zp.prodid IN (1,2,3,4,5)
    	union all
    	select 'TOTO MATCH' AS prodname
	) zp;

	create temp table ubt_temp_busdatehost1 as
	select distinct * from public.ztubt_getbusinessdate_perhost where 1=1
	and previousperioddatetime is not null
	and host =1;

	create temp table ubt_temp_busdatehost2 as
	select distinct * from public.ztubt_getbusinessdate_perhost where 1=1
	and previousperioddatetime is not null
	and host =2;

	create temp table ubt_temp_busdatehost3 as
	select distinct * from public.ztubt_getbusinessdate_perhost where 1=1
	and previousperioddatetime is not null
	and host =3;

	insert into ubt_temp_trans_amt
	select
	  terdisplayid
	 ,cast(prodname as varchar(50)) as prodname
	 ,cast(flag as char(3)) as flag
	 ,case
	 	when flag in ('CAN','RBT','PAY','GST','FUN','RFD')
	 		then round(coalesce(amount,0)/100,2) * -1
	 	when flag='SAL' then round(trunc(coalesce(amount,0)/100,2),2) * -1
	 		else round(coalesce(amount,0)/100,2) end as amount
	 ,coalesce(cast(ticketcount as int4),0) as ticketcount
	 ,actualDate
	from(
		-- HorseRacing ProdID=1 SAL
		select
		  tl.terdisplayid as TerDisplayID
		, 'HORSE RACING' ProdName
		, 'SAL' Flag
		, sum(salescommamount) Amount
		, SUM(TicketCount) TicketCount
		, actualdate
		from ubt_temp_terminal_loc tl left outer join (
			select pbth.terdisplayid, zgp.actualdate
			,  SUM(case when pbtlin.bettypeid = 'W-P' then coalesce(pbtlin.salescommamount,0)/2
					else coalesce(pbtlin.salescommamount,0) end) salescommamount
			, count(distinct pbth.ticketserialnumber) TicketCount
			from
			 public.ztubt_placedbettransactionheader pbth
			 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
			 	on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
			 inner join ztubt_horse_placedbettransactionlineitem  pbtlin
			 	on (pbth.TranHeaderID = pbtlin.TranHeaderID)
			 left outer join ubt_temp_busdatehost1 zgp
	 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
--	 			and zgp.host = 1
	 			)
	 		where 1=1
			and pbtlin.CreatedDate >= vUTCFromDateTimeBMCS
			and pbtlin.CreatedDate < vUTCToDateTimeBMCS
			and pbth.prodid = 1
			and pbthlcs.betstatetypeid = 'PB06'
			and Case
				When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
				When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
				When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
				When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
				When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
				else 5 end = 1
			group by pbth.terdisplayid,pbtlin.bettypeid, zgp.actualdate) sq
			on (tl.terdisplayid = sq.terdisplayid)
		group by tl.terdisplayid, actualdate

		union all

		-- 4D ProdID=2 SAL
		select tl.terdisplayid
		, '4D Lottery' ProdName
		, 'SAL' Flag
		, salescomamt
		, ticketcount
		, actualdate
		from
		ubt_temp_terminal_loc tl left outer join (
		select
		  pbth.terdisplayid
		, sum(COALESCE(SalesCommAmountBig,0)+COALESCE(SalesCommAmountSmall,0)) salescomamt
		, count(distinct pbth.ticketserialnumber) ticketcount
		, zgp.actualdate
		from
		 ztubt_placedbettransactionheader pbth
		 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
		 	on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
		 inner join ztubt_4d_placedbettransactionlineitemnumber pbtlin
		 	on (pbth.TranHeaderID = pbtlin.TranHeaderID)
		 inner join ztubt_drawdates dd
		 	on (dd.TranHeaderID = pbtlin.TranHeaderID)
		 left outer join ubt_temp_busdatehost2 zgp
 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
-- 			and zgp.host = 2
 			)
	 	where 1=1
		and pbtlin.CreatedDate >= vUTCFromDateTimeIGT
		and pbtlin.CreatedDate < vUTCToDateTimeIGT
		and pbth.prodid= 2
		and pbthlcs.betstatetypeid = 'PB06'
		and Case
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
			else 5 end = 1
		group by pbth.terdisplayid, zgp.actualdate)sq
		on (tl.terdisplayid = sq.terdisplayid)

		union all

		-- TOTO ProdID=3 SAL
		select tl.terdisplayid
		, 'TOTO' ProdName
		, 'SAL' Flag
		, salescomamt
		, ticketcount
		, actualdate
		from
		ubt_temp_terminal_loc tl left outer join (
		select
		  pbth.terdisplayid
		, sum(pbtlin.salescommamount) salescomamt
		, count(distinct pbth.ticketserialnumber) ticketcount
		, zgp.actualdate
		from
		 ztubt_placedbettransactionheader pbth
		 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
		 	on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
		 inner join ztubt_toto_placedbettransactionlineitem  pbtlin
		 	on (pbth.TranHeaderID = pbtlin.TranHeaderID)
		 left outer join ubt_temp_busdatehost2 zgp
 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
-- 			and zgp.host = 2
 			)
		where 1=1
		and pbtlin.CreatedDate >= vUTCFromDateTimeIGT
		and pbtlin.CreatedDate < vUTCToDateTimeIGT
		and pbth.prodid= 3
		and pbtlin.bettypeid != ALL(v_totomatchBettypes)
		and pbthlcs.betstatetypeid = 'PB06'
		and coalesce(pbtlin.groupunitsequence,1) = 1
		and Case
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
			else 5 end = 1
		group by pbth.terdisplayid, zgp.actualdate) sq
		on (tl.terdisplayid = sq.terdisplayid)

		union all
		-- TOTO MATCH ProdID=3 SAL
		select tl.terdisplayid
		, 'TOTO MATCH' ProdName
		, 'SAL' Flag
		,(trunc(coalesce(salescomtotal,0)/100,2) - trunc(coalesce(salescomamt,0)/100,2))*100 as salescomamt
		, ticketcount
		, actualdate
		from
		ubt_temp_terminal_loc tl left outer join (
		select
		  pbth.terdisplayid
		, sum(case when pbtlin.bettypeid != ALL(v_totomatchBettypes) then pbtlin.salescommamount else 0 end) salescomamt
		, count(distinct pbth.ticketserialnumber) ticketcount
		, zgp.actualdate
		, sum(pbtlin.salescommamount) as salescomtotal
		from
		 ztubt_placedbettransactionheader pbth
		 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
		 	on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
		 inner join ztubt_toto_placedbettransactionlineitem  pbtlin
		 	on (pbth.TranHeaderID = pbtlin.TranHeaderID)
		 left outer join ubt_temp_busdatehost2 zgp
 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
 			)
		where 1=1
		and pbtlin.CreatedDate >= vUTCFromDateTimeIGT
		and pbtlin.CreatedDate < vUTCToDateTimeIGT
		and pbth.prodid= 3
		-- and pbtlin.bettypeid = ANY(v_totomatchBettypes)
		and pbthlcs.betstatetypeid = 'PB06'
		and coalesce(pbtlin.groupunitsequence,1) = 1
		and Case
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
			else 5 end = 1
		group by pbth.terdisplayid, zgp.actualdate) sq
		on (tl.terdisplayid = sq.terdisplayid)

		union all

		-- SWEEP ProdID=4 SAL
		select tl.terdisplayid
		, 'SINGAPORE SWEEP' ProdName
		, 'SAL' Flag
		, salescomamt
		, ticketcount
		, actualdate
		from
		ubt_temp_terminal_loc tl left outer join (
		select
		  pbth.terdisplayid
		, sum(COALESCE(pbtlin.salescommamount,0)) salescomamt
		, count(distinct pbth.ticketserialnumber) ticketcount
		, zgp.actualdate
		from
		 ztubt_placedbettransactionheader pbth
		 --wf 20230705 check lifecycle state of PB06 begin:
		 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
	 		on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
	 		--wf 20230705 end
		 inner join ztubt_sweep_placedbettransactionlineitemnumber pbtlin
		 	on (pbth.TranHeaderID = pbtlin.TranHeaderID and pbtlin.issoldout = false)
		 left outer join ubt_temp_busdatehost2 zgp
 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
-- 			and zgp.host = 2
 			)
		where 1=1
		and pbtlin.CreatedDate >= vUTCFromDateTimeIGT
		and pbtlin.CreatedDate < vUTCToDateTimeIGT
		and pbth.prodid= 4
	     --wf 20230705 check lifecycle state of PB06 begin:
		and pbthlcs.betstatetypeid = 'PB06'
		--wf 20230705 end
--		and Case
--			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
--			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
--			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
--			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
--			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
--			else 5 end = 1
		group by pbth.terdisplayid, zgp.actualdate) sq
		on (tl.terdisplayid = sq.terdisplayid)

		union all

		-- 07-Apr-2022: Hira - union added for Sweep Consignee and Retailer Cancellation
		select tl.terdisplayid
			, 'SINGAPORE SWEEP' ProdName
			, 'SAL' Flag
			, salescomamt
			, ticketcount
			, actualdate
		from
		ubt_temp_terminal_loc tl left outer join (
		SELECT        PBTH.TerDisplayID, GB.ActualDate,
		0-SUM(coalesce(PBTLN.SalesCommAmount,0)) salescomamt,
		0-COUNT(DISTINCT PBTH.TicketSerialNumber) ticketcount
		FROM
			(SELECT CBT.TranHeaderID, CBT.TicketSerialNumber, CBT.ProdID
				, CBT.TerDisplayID, CBT.CancelledAmout
			FROM public.ztubt_cancelledbetticket CBT
			INNER JOIN public.ztubt_cancelledbetticketlifecyclestate CBLC
				ON CBLC.TranHeaderID = CBT.TranHeaderID AND CBLC.BetStateTypeID = 'CB06'
			WHERE	CBT.ProdID = 4 AND CBT.CancelledDate BETWEEN vFromDateTimeIGT AND vToDateTimeIGT
			) PBTH
			INNER JOIN ztubt_sweep_placedbettransactionlineitemnumber PBTLN
			ON PBTH.TranHeaderID = PBTLN.TranHeaderID AND PBTLN.IsSoldOut = false AND PBTH.TerDisplayID=PBTLN.TerDisplayID
			LEFT JOIN ubt_temp_busdatehost2 GB
			ON PBTLN.CreatedDate BETWEEN GB.previousperioddatetimeUTC AND GB.perioddatetimeUTC
--			and gb.host = 2
		WHERE PBTH.ProdID = 4
		And PBTLN.CreatedDate >= vUTCFromDateTimeIGT - interval '45 Days'
		AND PBTLN.CreatedDate < vUTCToDateTimeIGT
		GROUP BY PBTH.TerDisplayID, GB.ActualDate) sq
		on (tl.terdisplayid = sq.terdisplayid)

		union all

		-- SPORTS ProdID=5,6 SAL
		select tl.terdisplayid
		, 'SPORTS' ProdName
		, 'SAL' Flag
		, salescomamt
		, ticketcount
		, actualdate
		from
		ubt_temp_terminal_loc tl left outer join (
		select
		  terdisplayid
		, sum(coalesce(salescommAmount,0))  salescomamt
		, sum(ticketcount) ticketcount
		, actualdate
		from
			(select  pbtlin.terdisplayid, pbtlin.salescommAmount,pbtlin.betamount
			,accumulatorid,pbtlin.tranheaderid,zgp.actualdate, count(distinct pbth.ticketserialnumber) ticketcount
			 from ztubt_placedbettransactionheader pbth
			 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
		 	on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
		 inner join ztubt_sports_placedbettransactionlineitem pbtlin
		 	on (pbth.TranHeaderID = pbtlin.TranHeaderID)
		 left outer join ubt_temp_busdatehost3 zgp
 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
-- 			and zgp.host = 3
 			)
		where 1=1
		and pbtlin.CreatedDate >= vUTCFromDateTimeOB
		and pbtlin.CreatedDate < vUTCToDateTimeOB
		and pbth.prodid in (5,6)
		and pbthlcs.betstatetypeid = 'PB06'
		and Case
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
			else 5 end = 1
		group by  pbtlin.terdisplayid, pbtlin.salescommAmount,pbtlin.betamount
			,accumulatorid,pbtlin.tranheaderid,zgp.actualdate)a
		group by terdisplayid, actualdate) sq
		on (tl.terdisplayid = sq.terdisplayid)

		union all

--------------------------------------------------------------------------------------------------
	-- HorseRacing ProdID=1 COL
		select
		  tl.terdisplayid as TerDisplayID
		, 'HORSE RACING' ProdName
		, 'COL' Flag
		, sum(COLAmount) Amount
		, SUM(TicketCount) TicketCount
		, null actualdate
		from ztubt_terminal tl left outer join (
			select pbtlin.terdisplayid
			,  SUM(case when pbtlin.bettypeid = 'W-P' then coalesce(pbtlin.betpriceamount ,0)/2
					else coalesce(pbtlin.betpriceamount ,0) end) COLAmount
			, count(distinct pbtlin.tranheaderid) TicketCount
			from
			 ztubt_placedbettransactionheaderlifecyclestate pbthlcs
			 inner join ztubt_horse_placedbettransactionlineitem  pbtlin
			 	on (pbthlcs.TranHeaderID = pbtlin.TranHeaderID)
--			 left outer join public.ztubt_getbusinessdate_perhost zgp
--	 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
--	 			and zgp.host = 1)
	 		where 1=1
			and pbtlin.CreatedDate >= vUTCFromDateTimeBMCS
			and pbtlin.CreatedDate < vUTCToDateTimeBMCS
			and pbthlcs.betstatetypeid = 'PB06'
			group by pbtlin.terdisplayid,pbtlin.bettypeid) sq
			on (tl.terdisplayid = sq.terdisplayid)
		group by tl.terdisplayid

		union all

	-- 4D ProdID=2 COL
		select tl.terdisplayid
		, '4D Lottery' ProdName
		, 'COL' Flag
		, COLAmount
		, ticketcount
		, null actualdate
		from
		ztubt_terminal tl left outer join (
		select
		  pbth.terdisplayid
		, sum(COALESCE(pbtlin.bigbetAcceptedWager,0)+COALESCE(pbtlin.smallBetAcceptedWager,0)) COLAmount
		, count(distinct pbth.ticketserialnumber) ticketcount
		from
		 ztubt_placedbettransactionheader pbth
		 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
		 	on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
		 inner join ztubt_4d_placedbettransactionlineitemnumber pbtlin
		 	on (pbth.TranHeaderID = pbtlin.TranHeaderID)
		 inner join ztubt_drawdates dd
		 	on (dd.TranHeaderID = pbtlin.TranHeaderID)
--		 left outer join public.ztubt_getbusinessdate_perhost zgp
-- 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
-- 			and zgp.host = 2)
	 	where 1=1
		and pbtlin.CreatedDate >= vUTCFromDateTimeIGT
		and pbtlin.CreatedDate < vUTCToDateTimeIGT
		and pbth.prodid= 2
		and pbthlcs.betstatetypeid = 'PB06'
		and Case
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
			else 5 end in  (1,2)
		group by pbth.terdisplayid)sq
		on (tl.terdisplayid = sq.terdisplayid)

		union all

		-- TOTO ProdID=3 COL
		select tl.terdisplayid
		, 'TOTO' ProdName
		, 'COL' Flag
		, sum(COLAmount) COLAmount
		, sum(ticketcount) ticketcount
		, null actualdate
		from
		ztubt_terminal tl left outer join (
		select
		  pbth.terdisplayid
		, Round((case when pbtlin.GroupHostID = '00000000-0000-0000-0000-000000000000'
					then coalesce(pbtlin.betPriceAmount,0) * count(pbtlin.TranHeaderID)
					else sum(pbtlin.betPriceAmount) end
					),2) COLAmount
		, count(distinct pbth.ticketserialnumber) ticketcount
		from
		 ztubt_placedbettransactionheader pbth
		 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
		 	on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
		 inner join ztubt_toto_placedbettransactionlineitem  pbtlin
		 	on (pbth.TranHeaderID = pbtlin.TranHeaderID)
--		 left outer join public.ztubt_getbusinessdate_perhost zgp
-- 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
-- 			and zgp.host = 2)
		where 1=1
		and pbtlin.CreatedDate >= vUTCFromDateTimeIGT
		and pbtlin.CreatedDate < vUTCToDateTimeIGT
		and pbth.prodid= 3
		and pbthlcs.betstatetypeid = 'PB06'
		and coalesce(pbtlin.groupunitsequence,1) = 1
		and pbtlin.bettypeid != ALL(v_totomatchBettypes)
		and Case
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
			else 5 end in (1,2)
		group by pbth.terdisplayid, pbtlin.GroupHostID,pbtlin.betPriceAmount) sq
		on (tl.terdisplayid = sq.terdisplayid)
		group by tl.terdisplayid

		union all
		-- TOTO MATCH ProdID=3 COL
		select tl.terdisplayid
		, 'TOTO MATCH' ProdName
		, 'COL' Flag
		, sum(COLAmount) COLAmount
		, sum(ticketcount) ticketcount
		, null actualdate
		from
		ztubt_terminal tl left outer join (
		select
		  pbth.terdisplayid
		, Round((case when pbtlin.GroupHostID = '00000000-0000-0000-0000-000000000000'
					then coalesce(pbtlin.betPriceAmount,0) * count(pbtlin.TranHeaderID)
					else sum(pbtlin.betPriceAmount) end
					),2) COLAmount
		, count(distinct pbth.ticketserialnumber) ticketcount
		from
		 ztubt_placedbettransactionheader pbth
		 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
		 	on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
		 inner join ztubt_toto_placedbettransactionlineitem  pbtlin
		 	on (pbth.TranHeaderID = pbtlin.TranHeaderID)
		where 1=1
		and pbtlin.CreatedDate >= vUTCFromDateTimeIGT
		and pbtlin.CreatedDate < vUTCToDateTimeIGT
		and pbth.prodid= 3
		and pbthlcs.betstatetypeid = 'PB06'
		and coalesce(pbtlin.groupunitsequence,1) = 1
		and pbtlin.bettypeid = ANY(v_totomatchBettypes)
		and Case
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
			else 5 end in (1,2)
		group by pbth.terdisplayid, pbtlin.GroupHostID,pbtlin.betPriceAmount) sq
		on (tl.terdisplayid = sq.terdisplayid)
		group by tl.terdisplayid

		union all

	-- SWEEP ProdID=4 COL PB06 PB03
		select tl.terdisplayid
		, 'SINGAPORE SWEEP' ProdName
		, 'COL' Flag
		, COLAmount
		, ticketcount
		, null actualdate
		from
		ztubt_terminal tl left outer join (
		select
		  pbth.terdisplayid
		, Round(sum(COALESCE(pbtlin.betpriceamount,0)),2) COLAmount
		, count(distinct pbth.tickettransactionid) ticketcount
		from
		 ztubt_placedbettransactionheader pbth
		 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
	 		on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
	 	 inner join ztubt_sweep_placedbettransactionlineitem pbtl
	 	 	on (pbth.TranHeaderID = pbtl.TranHeaderID)
		 inner join ztubt_sweep_placedbettransactionlineitemnumber pbtlin
		 	on (pbtl.tranlineitemid = pbtlin.tranlineitemid and pbtlin.issoldout = false)
		 inner join ztubt_terminal zt on (pbth.terdisplayid=zt.terdisplayid)
		 inner join ztubt_location zl on (zt.locid=zl.locid)
--		 left outer join public.ztubt_getbusinessdate_perhost zgp
-- 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
-- 			and zgp.host = 2)
		where 1=1
		and pbtlin.CreatedDate >= vUTCFromDateTimeIGT
		and pbtlin.CreatedDate < vUTCToDateTimeIGT
		and pbth.prodid= 4
		and ((pbthlcs.betstatetypeid = 'PB06' and
			(pbtl.isprinted is not null or zl.sweepindicator is null))
			or
			(pbthlcs.betstatetypeid = 'PB03' and
			(pbtl.isprinted is null and zl.sweepindicator is not null)))
		group by pbth.terdisplayid) sq
		on (tl.terdisplayid = sq.terdisplayid)


		union all

		-- SPORTS ProdID=5,6 COL
		select tl.terdisplayid
		, 'SPORTS' ProdName
		, 'COL' Flag
		, COLAmount
		, ticketcount
		, null actualdate
		from
		ztubt_terminal tl left outer join (
		select
		  terdisplayid
		, Round(sum(betamount),2) COLAmount
		, count(distinct tranheaderid) ticketcount
--		, actualdate
		from
			(select distinct pbtlin.terdisplayid, pbtlin.betamount,accumulatorid,pbtlin.tranheaderid
			 from ztubt_placedbettransactionheader pbth
			 inner join ztubt_placedbettransactionheaderlifecyclestate pbthlcs
			 	on (pbth.TranHeaderID = pbthlcs.TranHeaderID)
			 inner join ztubt_sports_placedbettransactionlineitem pbtlin
			 	on (pbth.TranHeaderID = pbtlin.TranHeaderID)
	--		 left outer join public.ztubt_getbusinessdate_perhost zgp
	-- 			on (pbtlin.createdDate between zgp.previousperioddatetime and zgp.perioddatetime
	-- 			and zgp.host = 3)
			where 1=1
			and pbtlin.CreatedDate >= vUTCFromDateTimeOB
			and pbtlin.CreatedDate < vUTCToDateTimeOB
			and pbth.prodid in (5,6)
			and pbthlcs.betstatetypeid = 'PB06'
			and Case
				When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
				When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
				When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
				When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
				When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
				else 5 end in (1,2)
			)a
		group by terdisplayid
--		, zgp.actualdate
		) sq
		on (tl.terdisplayid = sq.terdisplayid)

		union all
------------------------------------------------------------------------------------------------
	-- PAY
		select
		 tpv.terDisplayID
		,tpv.prodname
		,'PAY' Flag
		, (total_amount) PAYAmount
		, ticketCount
		, null actualDate
		from ubt_temp_ter_prod_val tpv left outer join (
			select terdisplayid
			,"types" || '-' || prodname as prodname
			, sum(total_amount) total_amount
			, count(total_amount) ticketCount
			from (
			select zv.terdisplayid
			, case when zp.prodid in (5,6) then 'SPORTS'
			when zp.prodid = 3 and EXISTS (
                   select 1
                   from ztubt_toto_placedbettransactionlineitem pbtlin
                   where zv.TranHeaderID = pbtlin.TranHeaderID
                     and pbtlin.bettypeid = ANY(v_totomatchBettypes)
               ) then 'TOTO MATCH'
			else trim(zp.prodname) end as prodname
			, zp.prodid
			, zv.winningamount , zv.rebateamount , zv.refundamount
			from ztubt_validatedbetticket zv
			inner join ztubt_product zp on (zv.prodid=zp.prodid)
			inner join ztubt_validatedbetticketlifecyclestate zvlc
				on (zv.tranheaderid = zvlc.tranheaderid and zvlc.betstatetypeid = 'VB06')
			where 1=1
			and (--bmcs
				(zv.createdvalidationdate >=vUTCFromDateTimeBMCS
				and zv.createdvalidationdate < vUTCToDateTimeBMCS
				and zvlc.captureddate between vUTCFromDateTimeBMCS::date -1 and vUTCToDateTimeBMCS::date +1)
				--wf: 230517 update to segragate time range condition by product
				and zv.prodid in (1)
			  or -- igt
			  	(zv.createdvalidationdate >=vUTCFromDateTimeIGT
				and zv.createdvalidationdate < vUTCToDateTimeIGT
				and zvlc.captureddate between vUTCFromDateTimeIGT::date -1 and vUTCToDateTimeIGT::date +1)
				--wf: 230517 update to segragate time range condition by product
				and zv.prodid in (2,3,4)
			  or -- ob
			  	(zv.createdvalidationdate >=vUTCFromDateTimeOB
				and zv.createdvalidationdate < vUTCToDateTimeOB
				and zvlc.captureddate between vUTCFromDateTimeOB::date -1 and vUTCToDateTimeOB::date +1)
				--wf: 230517 update to segragate time range condition by product
				and zv.prodid in (5,6)
				)
			--wf: 230517 update to segragate time range condition by product
			--and zv.prodid in (1,2,3,4,5,6)
			)sub1
			cross join lateral ( -- Unpivoting
			values ('VALIDATED',sub1.WinningAmount),
			('LOSING BET REBATE',sub1.RebateAmount),
			('REFUNDED',sub1.RefundAmount) ) unpvot (types,	total_amount)
			where 1=1
			and total_amount<>0
			group by terdisplayid ,"types" || '-' || prodname
		) sq on (tpv.terdisplayid = sq.terdisplayid and tpv.prodname = sq.prodname)
--		where total_amount <>0
--		group by  tpv.terDisplayID,tpv.prodname

		union all

		-- HorseRacing ProdID=1 PAY 'LOSING BET REBATE-HORSE RACING'
		select
		  tl.terdisplayid as TerDisplayID
		, 'LOSING BET REBATE-HORSE RACING' ProdName
		, 'PAY' Flag
		, Round(sum(COLAmount),2) Amount
		, SUM(TicketCount) TicketCount
		, null actualdate
		from ubt_temp_terminal_loc tl left outer join (
			select pbtlin.terdisplayid
--			, zgp.actualdate
			,  SUM(case when pbtlin.bettypeid = 'W-P' then coalesce(pbtlin.boardrebateamount ,0)/2
					else coalesce(pbtlin.boardrebateamount ,0) end) COLAmount
			, count(distinct pbtlin.tranheaderid) TicketCount
			from
			 ztubt_placedbettransactionheaderlifecyclestate pbthlcs
			 inner join ztubt_horse_placedbettransactionlineitem  pbtlin
			 	on (pbthlcs.TranHeaderID = pbtlin.TranHeaderID)
--			 left outer join public.ztubt_getbusinessdate_perhost zgp
--	 			on (pbtlin.createdDate between zgp.previousperioddatetimeUTC and zgp.perioddatetimeUTC
--	 			and zgp.host = 1)
	 		where 1=1
			and pbtlin.CreatedDate >= vUTCFromDateTimeBMCS  --bmcsutc
			and pbtlin.CreatedDate < vUTCToDateTimeBMCS
			and pbthlcs.betstatetypeid = 'PB06'
			group by pbtlin.terdisplayid,pbtlin.bettypeid
--			, zgp.actualdate
			) sq
			on (tl.terdisplayid = sq.terdisplayid)
		group by tl.terdisplayid
--		, actualdate

	-- PAY

		union all
------------------------------------------------------------------------------------------------
	-- CAN
		-- CAN Prodid=1
		select tp.terdisplayid
		, tp.prodname
		, 'CAN' Flag
		, CANAmount
		, TicketCount
		, null AcutalDate
		from ubt_temp_ter_prod tp left outer join (
		select cbt.terdisplayid
		, round(sum(cbt.cancelledamout),2) CANAmount
		, count(cbt.tranheaderid) TicketCount
		from ztubt_placedbettransactionheader pbth
		inner join ztubt_cancelledbetticket cbt
			on (pbth.tranheaderid = cbt.tranheaderid)
		inner join ztubt_cancelledbetticketlifecyclestate cblc
			on (pbth.tranheaderid=cblc.tranheaderid and cblc.betstatetypeid='CB06')
		where pbth.prodid =1
		and cbt.cancelleddate >= vFromDateTimeBMCS  --bmcs
		and cbt.cancelleddate < vToDateTimeBMCS
		group by cbt.terdisplayid )sq
		on (tp.terdisplayid = sq.terdisplayid)
		where tp.prodname = 'HORSE RACING'

		union all

		-- CAN Prodid=2,3
		select tp.terdisplayid
		, tp.prodname
		, 'CAN' Flag
		, CANAmount
		, TicketCount
		, null AcutalDate
		from ubt_temp_ter_prod tp left outer join (
		select cbt.terdisplayid,
		case
			when zp.prodid = 3 and EXISTS (
                   select 1
                   from ztubt_toto_placedbettransactionlineitem pbtlin
                   where pbth.TranHeaderID = pbtlin.TranHeaderID
                     and pbtlin.bettypeid = ANY(v_totomatchBettypes)
               ) then 'TOTO MATCH'
			else zp.prodname
		end as prodname
		, round(sum(cbt.cancelledamout),2) CANAmount
		, count(cbt.tranheaderid) TicketCount
		from ztubt_placedbettransactionheader pbth
		inner join ztubt_product zp
			on (pbth.prodid = zp.prodid)
		inner join ztubt_cancelledbetticket cbt
			on (pbth.tranheaderid = cbt.tranheaderid)
		where pbth.prodid in (2,3) -- 06-04-2022: Removed prodid 4 to add seperate union foe SWEEP Cancellation
		and cbt.cancelleddate >= vFromDateTimeIGT  --igtutc
		and cbt.cancelleddate < vToDateTimeIGT
		and Case
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
			else 5 end in (2)
		group by cbt.terdisplayid,
		case
			when zp.prodid = 3 and EXISTS (
                   select 1
                   from ztubt_toto_placedbettransactionlineitem pbtlin
                   where pbth.TranHeaderID = pbtlin.TranHeaderID
                     and pbtlin.bettypeid = ANY(v_totomatchBettypes)
               ) then 'TOTO MATCH'
			else zp.prodname
		end
		)sq
		on (tp.terdisplayid = sq.terdisplayid and trim(tp.prodname) = trim(sq.prodname))
		where tp.prodname in (' 4D Lottery', '4D Lottery','TOTO', 'TOTO MATCH')

		union all

		-- 06-04-2022: Hira: Added for Sweep Crossday Cancellation
		-- CAN Prodid=4
		select tp.terdisplayid
		, tp.prodname
		, 'CAN' Flag
		, CANAmount
		, TicketCount
		, null AcutalDate
		from ubt_temp_ter_prod tp left outer join (
		SELECT CBT.TerDisplayID, PD.ProdName
		, SUM(coalesce(CBT.CancelledAmout,0)) CANAmount, COUNT(CBT.TranHeaderID) TicketCount
		FROM (SELECT    CBT.TranHeaderID, CBT.TicketSerialNumber, CBT.ProdID
			, CBT.TerDisplayID, CBT.CancelledAmout
		FROM public.ztubt_cancelledbetticket CBT
		INNER JOIN public.ztubt_cancelledbetticketlifecyclestate CBLC
			ON CBLC.TranHeaderID = CBT.TranHeaderID AND CBLC.BetStateTypeID = 'CB06'
		WHERE	CBT.ProdID = 4 AND CBT.CancelledDate BETWEEN vFromDateTimeIGT AND vToDateTimeIGT) CBT
		INNER JOIN  public.ztubt_product PD ON CBT.ProdID = PD.ProdID
		GROUP BY CBT.TerDisplayID, PD.ProdName)sq
		on (tp.terdisplayid = sq.terdisplayid and trim(tp.prodname) = trim(sq.prodname))
		where tp.prodname = 'SINGAPORE SWEEP'

		union all

		-- CAN prodid=5,6
		select tp.terdisplayid
		, tp.prodname
		, 'CAN' Flag
		, CANAmount
		, TicketCount
		, null AcutalDate
		from ubt_temp_ter_prod tp left outer join (
		select cbt.terdisplayid
		, round(sum(cbt.cancelledamout),2) CANAmount
		, count(cbt.tranheaderid) TicketCount
		from ztubt_placedbettransactionheader pbth
		inner join ztubt_cancelledbetticket cbt
			on (pbth.tranheaderid = cbt.tranheaderid)
		inner join ztubt_cancelledbetticketlifecyclestate cblc
			on (pbth.tranheaderid=cblc.tranheaderid and cblc.betstatetypeid='CB06')
		where pbth.prodid in(5,6)
		and cbt.cancelleddate >= vFromDateTimeOB  --obsutc
		and cbt.cancelleddate < vToDateTimeOB
		and Case
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 1
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 2
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = false And pbth.IsExchangeTicket = false Then 3
			When coalesce(pbth.IsBetRejectedByTrader,false) = true And pbth.IsCancelled = true And pbth.IsExchangeTicket = false Then 4
			When coalesce(pbth.IsBetRejectedByTrader,false) = false And pbth.IsCancelled = false And pbth.IsExchangeTicket = true Then 6
			else 5 end in (1,2)
		group by cbt.terdisplayid )sq
		on (tp.terdisplayid = sq.terdisplayid)
		where tp.prodname = 'SPORTS'

	-- CAN
		union all
------------------------------------------------------------------------------------------------
	-- CAS
		select zt.terdisplayid
		, 'NETS' as prodname
		, 'CAS' Flag
		, CASAmount
		, TicketCount
		, null AcutalDate
		from ztubt_terminal zt left outer join (
		select terdisplayid
		, sum(paidamount) as CASAmount
		, count(distinct paymentdetailid) as TicketCount
		from public.ztubt_paymentdetail zp where 1=1
		and paymenttypeid in ('NC','NN')
		and zp.createddate >= vUTCFromDateTimeNH  --nhutc
		and zp.createddate < vUTCToDateTimeNH
		group by terdisplayid ) sq
		 on (zt.terdisplayid=sq.terdisplayid)

		union all

		select zt.terdisplayid
		, 'CASHCARD/Flashpay'
		, 'CAS' Flag
		, CASAmount
		, TicketCount
		, null AcutalDate
		from ztubt_terminal zt left outer join (
		select terdisplayid
		, sum(paidamount) as CASAmount
		, count(distinct paymentdetailid) as TicketCount
		from public.ztubt_paymentdetail zp where 1=1
		and paymenttypeid in ('NCC','NFP')
		and zp.createddate >= vUTCFromDateTimeNH  --nonhostsutc
		and zp.createddate < vUTCToDateTimeNH
		group by terdisplayid  )sq
		on (zt.terdisplayid=sq.terdisplayid)

		------ Paynow, PaynowQR ------
		union all

		select zt.terdisplayid
		, 'Paynow [+trans fee]'
		, 'CAS' Flag
		, CASAmount
		, TicketCount
		, null AcutalDate
		from ztubt_terminal zt left outer join (
		select
		zt.terdisplayid
		, sum(-zp.netamount + zp.transactionfee) as CASAmount -- netamount + transfee
		, count(distinct paymentdetailid) as TicketCount
		from ztubt_terminal zt
		left outer join ztubt_cart zc on zt.terdisplayid = zc.terdisplayid
		left outer join ztubt_paynowtransaction zp on zc.cartid = zp.cartid
		where zp.paymenttypeid = 'PNRIC'
		and zp.transactionstatus = any(status_pnric)
		and zp.updateddatetime >= vFromDateTimeIGT --use updateddatetime instead of transactiondate
		and zp.updateddatetime < vToDateTimeIGT
		group by zt.terdisplayid  ) sq
		on (zt.terdisplayid=sq.terdisplayid)

		union all

		select zt.terdisplayid
		, 'PaynowQR [+trans fee]'
		, 'CAS' Flag
		, CASAmount
		, TicketCount
		, null AcutalDate
		from ztubt_terminal zt left outer join (
		select
		zt.terdisplayid
		, sum(zp.netamount + zp.transactionfee) as CASAmount  -- netamount + transfee
		, count(distinct paymentdetailid) as TicketCount
		from ztubt_terminal zt
		left outer join ztubt_cart zc on zt.terdisplayid = zc.terdisplayid
		left outer join ztubt_paynowtransaction zp on zc.cartid = zp.cartid
		where zp.paymenttypeid = 'PNQR'
		and zp.transactionstatus = any(status_pnqr)
		and zp.updateddatetime >= vFromDateTimeIGT --use updateddatetime instead of transactiondate
		and zp.updateddatetime < vToDateTimeIGT
		group by zt.terdisplayid  ) sq
		on (zt.terdisplayid=sq.terdisplayid)

		union all
		-- transfee PayNow
		select zt.terdisplayid
		, 'Paynow'
		, 'CAS' Flag
		, CASAmount
		, TicketCount
		, null AcutalDate
		from ztubt_terminal zt left outer join (
		select
		zt.terdisplayid
		, sum(zp.transactionfee) as CASAmount  -- transfee
		, count(distinct paymentdetailid) as TicketCount
		from ztubt_terminal zt
		left outer join ztubt_cart zc on zt.terdisplayid = zc.terdisplayid
		left outer join ztubt_paynowtransaction zp on zc.cartid = zp.cartid
		where zp.paymenttypeid = 'PNRIC'
		and zp.transactionstatus = any(status_pnric)
		and zp.updateddatetime >= vFromDateTimeIGT --use updateddatetime instead of transactiondate
		and zp.updateddatetime < vToDateTimeIGT
		group by zt.terdisplayid  ) sq
		on (zt.terdisplayid=sq.terdisplayid)

		union all
		-- transfee PayNowQR
		select zt.terdisplayid
		, 'PaynowQR'
		, 'CAS' Flag
		, CASAmount
		, TicketCount
		, null AcutalDate
		from ztubt_terminal zt left outer join (
		select
		zt.terdisplayid
		, sum(zp.transactionfee) as CASAmount  -- transfee
		, count(distinct paymentdetailid) as TicketCount
		from ztubt_terminal zt
		left outer join ztubt_cart zc on zt.terdisplayid = zc.terdisplayid
		left outer join ztubt_paynowtransaction zp on zc.cartid = zp.cartid
		where zp.paymenttypeid = 'PNQR'
		and zp.transactionstatus = any(status_pnqr)
		and zp.updateddatetime >= vFromDateTimeIGT --use updateddatetime instead of transactiondate
		and zp.updateddatetime < vToDateTimeIGT
		group by zt.terdisplayid  ) sq
		on (zt.terdisplayid=sq.terdisplayid)


	-- CAS
		union all
------------------------------------------------------------------------------------------------
	-- COL Offline Orders
		 select
		 zt.terdisplayid
		, 'Offline Orders' as prodname
		, 'COL' Flag
		, oo_col_amount
		, TicketCount
		, null AcutalDate
		 from ztubt_terminal zt left outer join (
		 select pb.terdisplayid
		 , sum(pbt.amount) oo_col_amount
		 , count(1) TicketCount
		 from ztubt_offlineproductheader pb
		 inner join ztubt_admissionvouchertransaction pbt
		 	on (pb.transactionheaderid = pbt.headerid)
		where 1=1
		and pbt.isactive=true
		and pbt.transactiontype=1
		and pb.transactiontime >= vFromDateTimeNH  --nonhostsutc
		and pb.transactiontime < vToDateTimeNH
		group by pb.terdisplayid
		union all
		 select pb.terdisplayid
		 , sum(pbt.totalAmount * pbt.repeatedTicket) oo_col_amount
		 , sum(pbt.repeatedTicket) TicketCount
		 from ztubt_offlineproductheader pb
		 inner join ztubt_charitytickettransaction pbt
			on (pb.transactionheaderid = pbt.headerid )
		where 1=1
		and pb.transactiontime >= vFromDateTimeNH  --nonhostsutc
		and pb.transactiontime < vToDateTimeNH
		group by pb.terdisplayid
		union all
		 select pb.terdisplayid
		 , sum(pbt.totalAmount * pbt.repeatedTicket) oo_col_amount
		 , sum(pbt.repeatedTicket) TicketCount
		 from ztubt_offlineproductheader pb
		 inner join ztubt_spatransaction pbt
			on (pb.transactionheaderid = pbt.headerid )
		where 1=1
		and pbt.transactiontype=1
		and pb.transactiontime >= vFromDateTimeNH  --nonhostsutc
		and pb.transactiontime < vToDateTimeNH
		group by pb.terdisplayid
		union all
		 select pb.terdisplayid
		 , sum(pbt.totalAmount * pbt.repeatedTicket) oo_col_amount
		 , sum(pbt.repeatedTicket) TicketCount
		 from ztubt_offlineproductheader pb
		 inner join ztubt_topupcardtransaction pbt
			on (pb.transactionheaderid = pbt.headerid )
		where 1=1
		and pbt.transactiontype=1
		and pb.transactiontime >= vFromDateTimeNH  --nonhostsutc
		and pb.transactiontime < vToDateTimeNH
		group by pb.terdisplayid
		union all
		 select pb.terdisplayid
		 , sum(pbt.totalAmount * pbt.repeatedTicket) oo_col_amount
		 , sum(pbt.repeatedTicket) TicketCount
		 from ztubt_offlineproductheader pb
		 inner join ztubt_totohongbaotransaction pbt
			on (pb.transactionheaderid = pbt.headerid )
		where 1=1
		and pb.transactiontime >= vFromDateTimeNH --nonhostsutc
		and pb.transactiontime < vToDateTimeNH
		group by pb.terdisplayid
		)sq
		on (zt.terdisplayid = sq.terdisplayid)

	-- COL Offline Orders
		union all
------------------------------------------------------------------------------------------------

	-- RFD Offline Orders
		 select
		 zt.terdisplayid
		, 'Offline Orders' as prodname
		, 'RFD' Flag
		, oo_col_amount
		, TicketCount
		, null AcutalDate
		 from ztubt_terminal zt left outer join (
		 select pb.terdisplayid
		 , sum(pbt.amount) oo_col_amount
		 , count(1) TicketCount
		 from ztubt_offlineproductheader pb
		 inner join ztubt_admissionvouchertransaction pbt
		 	on (pb.transactionheaderid = pbt.headerid)
		where 1=1
		and pbt.isactive=true
		and pbt.transactiontype in (2,3)
		and pb.transactiontime >= vFromDateTimeNH --nonhostsutc
		and pb.transactiontime < vToDateTimeNH
		group by pb.terdisplayid
		union all
		 select pb.terdisplayid
		 , sum(pbt.totalAmount * pbt.repeatedTicket) oo_col_amount
		 , sum(pbt.repeatedTicket) TicketCount
		 from ztubt_offlineproductheader pb
		 inner join ztubt_spatransaction pbt
			on (pb.transactionheaderid = pbt.headerid )
		where 1=1
		and pbt.transactiontype=2
		and pb.transactiontime >= vFromDateTimeNH  --nonhostsutc
		and pb.transactiontime < vToDateTimeNH
		group by pb.terdisplayid
		union all
		 select pb.terdisplayid
		 , sum(pbt.totalAmount * pbt.repeatedTicket) oo_col_amount
		 , sum(pbt.repeatedTicket) TicketCount
		 from ztubt_offlineproductheader pb
		 inner join ztubt_topupcardtransaction pbt
			on (pb.transactionheaderid = pbt.headerid )
		where 1=1
		and pbt.transactiontype=2
		and pb.transactiontime >= vFromDateTimeNH  --nonhostsutc
		and pb.transactiontime < vToDateTimeNH
		group by pb.terdisplayid
		)sq
		on (zt.terdisplayid = sq.terdisplayid)

		union all

	-- COL Gate Admission
		 select
		 zt.terdisplayid
		, 'Gate Admission' as prodname
		, 'COL' Flag
		, oo_col_amount
		, TicketCount
		, null AcutalDate
		 from ztubt_terminal zt left outer join (
		 select pb.terdisplayid
		 , sum(pbt.totalAmount * pbt.repeatedTicket) oo_col_amount
		 , sum(pbt.repeatedTicket) TicketCount
		 from ztubt_offlineproductheader pb
		 inner join ztubt_topupcardtransaction pbt
			on (pb.transactionheaderid = pbt.headerid )
		where 1=1
		and pbt.transactiontype=1
		and pb.transactiontime >= vFromDateTimeNH  --nonhostsutc
		and pb.transactiontime < vToDateTimeNH
		group by pb.terdisplayid
		)sq
		on (zt.terdisplayid = sq.terdisplayid)

		union all
------------------------------------------------------------------------------------------------

		-- FUN & REC
		select
			zt.terdisplayid,
			'Funding and Recovery' prodname,
			'FUN' flag,
			sum(fundingamount) fundingamount,
			0 ticketCount,
			null actualDate
		from ztubt_terminal zt left outer join (
		select zl.locid, sum(coalesce(zf.fundingamount,0)) fundingamount from
		ztubt_location zl inner join ztubt_funding zf on (zf.locid = zl.locid)
		where 1=1
		and zl.loctypeid in (2,4)
		and cast(zf.fundperiodstart as date) = vFromDateFUND -- fundingdt
		and cast(zf.fundperiodend as date) = vToDateFUND
		group by zl.locid) sq
		on (zt.locid=sq.locid)
		group by zt.terdisplayid
		union all
		select
			zt.terdisplayid,
			'Funding and Recovery' prodname,
			'REC' flag,
			sum(fundingamount) fundingamount,
			0 ticketCount,
			null actualDate
		from ztubt_terminal zt left outer join (
		select zl.locid, sum(coalesce(zf.fundingamount,0)) fundingamount from
		ztubt_location zl inner join ztubt_funding zf on (zf.locid = zl.locid)
		inner join ztubt_recovery zr on (zr.fundingid=zf.fundingid)
		where 1=1
		and zl.loctypeid in (2,4)
		and cast(zr.recperiodstart as date) = vFromDateFUND -- fundingdt
		and cast(zr.recperiodend as date) = vToDateFUND
		group by zl.locid) sq
		on (zt.locid=sq.locid)
		group by zt.terdisplayid
		union all
		select
			zt.terdisplayid,
			'Online Adjustment' prodname,
			'FUN' flag,
			sum(adjustmentAmount) adjustmentAmount,
			sum(ticketCount),
			null actualDate
		from ztubt_terminal zt left outer join (
		select zl.locid
		, sum(coalesce(za.adjustmentAmount,0)) adjustmentAmount
		, count(za.adjustinvoiceid) ticketCount
		from
		ztubt_location zl inner join ztubt_adjustinvoice za on (za.locid = zl.locid)
		inner join ztubt_lookupvalueconfig lkp on (za.adjustmentcode::varchar=lkp.fld1value
			and lkp.configname='RTMS_AdjustmentCode' and left(lkp.fld2value,1)='C')
		where 1=1
		and za.createddatetime >= vFromDateTimeNH  --nonhost
		and za.createddatetime < vToDateTimeNH
		and za.approvalstatus =1
		group by zl.locid) sq
		on (zt.locid=sq.locid)
		group by zt.terdisplayid
		union all
		select
			zt.terdisplayid,
			'Online Adjustment' prodname,
			'REC' flag,
			sum(adjustmentAmount) adjustmentAmount,
			sum(ticketCount),
			null actualDate
		from ztubt_terminal zt left outer join (
		select zl.locid
		, sum(coalesce(za.adjustmentAmount,0)) adjustmentAmount
		, count(za.adjustinvoiceid) ticketCount
		from
		ztubt_location zl inner join ztubt_adjustinvoice za on (za.locid = zl.locid)
		inner join ztubt_lookupvalueconfig lkp on (za.adjustmentcode::varchar=lkp.fld1value
			and lkp.configname='RTMS_AdjustmentCode' and left(lkp.fld2value,1)='D')
		where 1=1
		and za.createddatetime >= vFromDateTimeNH  --nonhost
		and za.createddatetime < vToDateTimeNH
		and za.approvalstatus =1
		group by zl.locid) sq
		on (zt.locid=sq.locid)
		group by zt.terdisplayid


------------------------------------------------------------------------------------------------
	)main;

	create temp table ubt_temp_gst_sal as
	select terdisplayid, prodname, flag
--	, case when flag= 'GST' then round(sum(amount)*0.01,2) else sum(amount) end amount
	, sum(amount) amount
	, sum(TicketCount) TicketCount
	from (
		select tp.terdisplayid, tp.prodname, 'GST' flag
			, coalesce(sq.amount,0) amount, coalesce(sq.TicketCount,0) TicketCount
			, sq.actualDate
--			, 'CH' TransType
		from ubt_temp_ter_prod tp left outer join (
			select distinct
			  a.terdisplayid, a.prodname, 'GST' flag
			, case when zlh.isgst = true then a.amount else 0 end amount
			, a.TicketCount, a.actualDate
			from (select terdisplayid, prodname,actualDate, flag
					, round(sum(amount)*zg.gstrate*0.01,2) amount
					, sum(TicketCount) TicketCount
					from ubt_temp_trans_amt tt
					inner join public.ztubt_gstconfig zg
					on ((tt.actualDate between zg.effectivefrom and zg.enddate)
					or (tt.actualDate >= zg.effectivefrom and zg.enddate is null))
					where flag='SAL' and prodname <> 'TOTO MATCH'
					group by terdisplayid, prodname,actualDate,flag,zg.gstrate

					union all

					select terdisplayid, 'TOTO MATCH' as prodname,actualDate, flag
					, round((sum(amount)*zg.gstrate*0.01),2)- round((sum(case when prodname = 'TOTO' then amount*zg.gstrate*0.01 else 0 end)),2)  amount
					, sum(case when prodname='TOTO MATCH' then TicketCount else 0 end) TicketCount
					from ubt_temp_trans_amt tt
					inner join public.ztubt_gstconfig zg
					on ((tt.actualDate between zg.effectivefrom and zg.enddate)
					or (tt.actualDate >= zg.effectivefrom and zg.enddate is null))
					where flag='SAL' and prodname in ('TOTO MATCH','TOTO')
					group by terdisplayid,actualDate,flag,zg.gstrate
					) a
			inner join public.ztubt_terminal zt on (a.terdisplayid = zt.terdisplayid)
			left outer join public.ztubt_locationgsthistory zlh
				on (zt.locid = zlh.locid and a.actualDate between zlh.startdate and zlh.enddate
				and zlh.isdeleted=false)
			where 1=1
			and a.flag='SAL'
			and zlh.isdeleted = false
			)sq
		on (tp.terdisplayid=sq.terdisplayid and trim(tp.prodname) = trim(sq.prodname))
		union all
		select * from ubt_temp_trans_amt where flag ='SAL'
	)sq
	group by terdisplayid, prodname, flag
		;


	delete from ubt_temp_trans_amt where flag = 'SAL';

	insert into ubt_temp_trans_amt
		select terdisplayid, prodname, flag, amount, TicketCount, null actualdate
		from ubt_temp_gst_sal;

	create temp table ubt_temp_toto_can_tktcount as
		select terdisplayid, sum(toto_ticketCount) toto_ticketCount from (
			select fin.terdisplayid, count(distinct zp.ticketserialnumber) toto_ticketCount
			from ztubt_placedbettransactionheader zp
			inner join ztubt_toto_placedbettransactionlineitem ztp
				on (zp.tranheaderid=ztp.tranheaderid)
			inner join
				(select zc.tranheaderid ,zc.terdisplayid from ztubt_cancelledbetticket zc
				inner join ztubt_cancelledbetticketlifecyclestate zc2
				on (zc.tranheaderid=zc2.tranheaderid and zc2.betstatetypeid='CB06')
				where 1=1
				and zc.cancelleddate >= vFromDateTimeIGT  --igtutc
				and zc.cancelleddate < vToDateTimeIGT) fin
					on  (zp.tranheaderid=fin.tranheaderid)
			where 1=1
			and ztp.bettypeid != ALL(v_totomatchBettypes)
			and ztp.groupunitsequence is null
	--		and fin.terdisplayid = '211802001'
			group by fin.terdisplayid
			union all
			select fin.terdisplayid, count(distinct ztp.grouphostid)
			from ztubt_placedbettransactionheader zp
			inner join ztubt_toto_placedbettransactionlineitem ztp
				on (zp.tranheaderid=ztp.tranheaderid)
			inner join
				(select zc.tranheaderid ,zc.terdisplayid from ztubt_cancelledbetticket zc
				inner join ztubt_cancelledbetticketlifecyclestate zc2
				on (zc.tranheaderid=zc2.tranheaderid and zc2.betstatetypeid='CB06')
				where 1=1
				and zc.cancelleddate >= vFromDateTimeIGT  --igtutc
				and zc.cancelleddate < vToDateTimeIGT) fin
					on  (zp.tranheaderid=fin.tranheaderid)
			where 1=1
			and ztp.bettypeid != ALL(v_totomatchBettypes)
			and ztp.groupunitsequence is not null
	--		and fin.terdisplayid = '211802001'
			group by fin.terdisplayid
			) sq
			group by terdisplayid;

	update ubt_temp_trans_amt a set ticketcount = tc.toto_ticketCount
	from ubt_temp_toto_can_tktcount tc
	where a.terdisplayid = tc.terdisplayid
	and a.prodname = 'TOTO'
	and a.flag = 'CAN' ;

	create temp table ubt_temp_totofo_can_tktcount as
		select terdisplayid, sum(totofo_ticketCount) totofo_ticketCount from (
			select fin.terdisplayid, count(distinct zp.ticketserialnumber) totofo_ticketCount
			from ztubt_placedbettransactionheader zp
			inner join ztubt_toto_placedbettransactionlineitem ztp
				on (zp.tranheaderid=ztp.tranheaderid)
			inner join
				(select zc.tranheaderid ,zc.terdisplayid from ztubt_cancelledbetticket zc
				inner join ztubt_cancelledbetticketlifecyclestate zc2
				on (zc.tranheaderid=zc2.tranheaderid and zc2.betstatetypeid='CB06')
				where 1=1
				and zc.cancelleddate >= vFromDateTimeIGT  --igtutc
				and zc.cancelleddate < vToDateTimeIGT) fin
					on  (zp.tranheaderid=fin.tranheaderid)
			where 1=1
			and ztp.bettypeid = ANY(v_totomatchBettypes)
			and ztp.groupunitsequence is null
			group by fin.terdisplayid
			union all
			select fin.terdisplayid, count(distinct ztp.grouphostid)
			from ztubt_placedbettransactionheader zp
			inner join ztubt_toto_placedbettransactionlineitem ztp
				on (zp.tranheaderid=ztp.tranheaderid)
			inner join
				(select zc.tranheaderid ,zc.terdisplayid from ztubt_cancelledbetticket zc
				inner join ztubt_cancelledbetticketlifecyclestate zc2
				on (zc.tranheaderid=zc2.tranheaderid and zc2.betstatetypeid='CB06')
				where 1=1
				and zc.cancelleddate >= vFromDateTimeIGT  --igtutc
				and zc.cancelleddate < vToDateTimeIGT) fin
					on  (zp.tranheaderid=fin.tranheaderid)
			where 1=1
			and ztp.bettypeid = ANY(v_totomatchBettypes)
			and ztp.groupunitsequence is not null
			group by fin.terdisplayid
			) sq
			group by terdisplayid;

	update ubt_temp_trans_amt a set ticketcount = tc.totofo_ticketCount
	from ubt_temp_totofo_can_tktcount tc
	where a.terdisplayid = tc.terdisplayid
	and a.prodname = 'TOTO MATCH'
	and a.flag = 'CAN' ;

	create temp table ubt_temp_toto_sal_count as
		select terdisplayid ,prodname
			, sum(case when flag = 'COL' then ticketcount else 0 end)
			 -sum(case when flag = 'CAN' then ticketcount else 0 end) toto_sal_count
		from ubt_temp_trans_amt where 1=1
		and prodname = 'TOTO'
		and flag in ('COL','CAN')
		group by terdisplayid,prodname;

	update ubt_temp_trans_amt a set ticketcount = tsc.toto_sal_count
	from ubt_temp_toto_sal_count tsc
	where a.terdisplayid = tsc.terdisplayid
	and a.prodname = tsc.prodname
	and a.flag = 'SAL' ;

	create temp table ubt_temp_totofo_sal_count as
		select terdisplayid ,prodname
			, sum(case when flag = 'COL' then ticketcount else 0 end)
			 -sum(case when flag = 'CAN' then ticketcount else 0 end) totofo_sal_count
		from ubt_temp_trans_amt where 1=1
		and prodname = 'TOTO MATCH'
		and flag in ('COL','CAN')
		group by terdisplayid,prodname;

	update ubt_temp_trans_amt a set ticketcount = tsc.totofo_sal_count
	from ubt_temp_totofo_sal_count tsc
	where a.terdisplayid = tsc.terdisplayid
	and a.prodname = tsc.prodname
	and a.flag = 'SAL' ;

	create temporary table ubt_temp_trans_final as
	select
		 a.terdisplayid
		,case
			when prodname like 'REFUNDED-%' then Replace(prodname,'REFUNDED-','')
			when prodname like 'LOSING BET REBATE-%' then Replace(prodname,'LOSING BET REBATE-','')
			when prodname like 'VALIDATED-%' then Replace(prodname,'VALIDATED-','')
			else prodname end as prodname
		,case
			when prodname like 'REFUNDED-%' then 'RFD'
			when prodname like 'LOSING BET REBATE-%' then 'RBT'
			else flag end as flag
		,amount
		,case when a.flag= 'SAL' and tl.loctypeid = 1 then 0
			else a.ticketcount end ticketcount
		,actualdate
		,cast(case
			when prodname in ('Offline Orders', 'Gate Admission') then 'OO'
			when prodname in ('NETS','CASHCARD/Flashpay', 'Paynow [+trans fee]', 'PaynowQR [+trans fee]') then 'CL'
			when prodname in ('Paynow','PaynowQR') then 'TF'
			else 'CH' end as varchar) TransType
	from ubt_temp_trans_amt a left outer join
		(select zt.terdisplayid, zl.loctypeid from public.ztubt_terminal zt
		inner join public.ztubt_location zl
		on zt.locid =zl.locid) tl
	on a.terdisplayid = tl.terdisplayid
	where prodname not like 'EXPIRED-%';



	return query
	select * from ubt_temp_trans_final
--	union all
--	select tp.terdisplayid, tp.prodname, 'GST' flag
--		, coalesce(sq.amount,0) amount, coalesce(sq.TicketCount,0) TicketCount
--		, sq.actualDate
--		, 'CH' TransType
--	from ubt_temp_ter_prod tp left outer join (
--		select
--		  a.terdisplayid, a.prodname, 'GST' flag
--		, case when zlh.isgst = true then round(a.amount*zg.gstrate *0.01,2) else 0 end amount
--		, a.TicketCount, a.actualDate
--		from ubt_temp_trans_final a
--		inner join public.ztubt_terminal zt on (a.terdisplayid = zt.terdisplayid)
--		inner join public.ztubt_gstconfig zg
--			on ((a.actualDate between zg.effectivefrom and zg.enddate)
--			 or (a.actualDate >= zg.effectivefrom and zg.enddate is null))
--		left outer join public.ztubt_locationgsthistory zlh
--			on (zt.locid = zlh.locid and a.actualDate between zlh.startdate and zlh.enddate)
--		where 1=1
--		and a.flag='SAL'
--		and zlh.isdeleted = false
--		)sq
--	on (tp.terdisplayid=sq.terdisplayid and trim(tp.prodname) = trim(sq.prodname))
	;

drop table ubt_temp_trans_amt;
drop table ubt_temp_terminal_loc;
drop table ubt_temp_ter_prod_val;
drop table ubt_temp_ter_prod;
drop table ubt_temp_trans_final;
drop table ubt_temp_toto_sal_count;
drop table ubt_temp_toto_can_tktcount;
drop table ubt_temp_totofo_sal_count;
drop table ubt_temp_totofo_can_tktcount;
drop table ubt_temp_gst_sal;
drop table ubt_temp_busdatehost1;
drop table ubt_temp_busdatehost2;
drop table ubt_temp_busdatehost3;

end;
$$;


ALTER FUNCTION public.sp_ubt_gettransamountdetails(in_fromdatetime timestamp without time zone, in_todatetime timestamp without time zone) OWNER TO sp_postgres;
