CREATE FUNCTION public.sp_ubt_getamounttransaction(in_fromdatetime date, in_todatetime date)
RETURNS TABLE(ticketserialnumber character varying, wager numeric, secondwager numeric, sales numeric, secondsales numeric, salescomm numeric, secondsalescomm numeric, gst numeric, secondgst numeric, returnamount numeric, winningamount numeric)
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

begin

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


	create temporary table if not exists temp_ubt_ResultWager
					(TktSerialNumber varchar(200)
					,Wager numeric(22,2)
					,SecondWager numeric(22,2)
					);

	insert into temp_ubt_ResultWager
		-- HR
		select
			pbth.ticketserialnumber
		  , sum(case when pbtl.bettypeid = 'W-P' then coalesce(pbtl.betpriceamount,0)/2
				else coalesce(pbtl.betpriceamount,0) end) as Wager
		  , 0 as SecondWager
		from
		ztubt_placedbettransactionheader pbth
		inner join ztubt_horse_placedbettransactionlineitem pbtl
		 on (pbth.tranheaderid=pbtl.tranheaderid)
		inner join ztubt_placedbettransactionheaderlifecyclestate pblc
			on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
		where 1=1
		and pbth.prodid = 1
		and pbth.createddate >= vUTCFromDateTimeBMCS  --bmcsutc
		and pbth.createddate < vUTCToDateTimeBMCS
		group by pbth.ticketserialnumber

		union all

		-- 4D
		select
			pbth.ticketserialnumber
		  , sum(coalesce(pbtl.bigbetacceptedwager,0)) as Wager
		  , sum(coalesce(pbtl.smallbetacceptedwager,0)) as SecondWager
		from
		ztubt_placedbettransactionheader pbth
		inner join ztubt_4d_placedbettransactionlineitemnumber pbtl
			on (pbth.tranheaderid=pbtl.tranheaderid)
		inner join ztubt_placedbettransactionheaderlifecyclestate pblc
			on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
		inner join ztubt_drawdates dd
			on (pbth.tranheaderid=dd.tranheaderid)
		where 1=1
		and pbth.prodid = 2
		and pbth.isexchangeticket = false
		and pbth.createddate >= vUTCFromDateTimeIGT  --igtsutc
		and pbth.createddate < vUTCToDateTimeIGT
		group by pbth.ticketserialnumber

		union all

		-- TOTO
		select
			pbth.ticketserialnumber
		  , sum(coalesce(pbtl.betpriceamount ,0)) as Wager
		  , 0 as SecondWager
		from
		ztubt_placedbettransactionheader pbth
		inner join ztubt_toto_placedbettransactionlineitem pbtl
			on (pbth.tranheaderid=pbtl.tranheaderid)
		inner join ztubt_placedbettransactionheaderlifecyclestate pblc
			on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
		where 1=1
		and pbth.prodid = 3
		and pbth.isexchangeticket = false
		and pbth.createddate >= vUTCFromDateTimeIGT  --igtutc
		and pbth.createddate < vUTCToDateTimeIGT
		and coalesce(pbtl.groupunitsequence,1) =1
		group by pbth.ticketserialnumber

		union all

		-- SWEEP PB06
		select
			pbth.ticketserialnumber
		  , sum(coalesce(pbtln.betpriceamount ,0)) as Wager
		  , 0 as SecondWager
		from
		ztubt_placedbettransactionheader pbth
		inner join ztubt_sweep_placedbettransactionlineitem pbtl
			on (pbth.tranheaderid=pbtl.tranheaderid)
		inner join ztubt_sweep_placedbettransactionlineitemnumber pbtln
			on (pbtl.tranlineitemid =pbtln.tranlineitemid and pbtln.issoldout=false)
		inner join ztubt_placedbettransactionheaderlifecyclestate pblc
			on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
		where 1=1
		and pbth.prodid = 4
		and pbth.createddate >= vUTCFromDateTimeIGT  --igtutc
		and pbth.createddate < vUTCToDateTimeIGT
		and not exists (select 1 from ztubt_placedbettransactionheaderlifecyclestate zplc
							inner join ztubt_terminal zt on (zt.terdisplayid=pbth.terdisplayid)
							inner join ztubt_location zl on (zt.locid=zl.locid)
						where 1=1
						and zplc.tranheaderid =pbth.tranheaderid
						and zplc.betstatetypeid = 'PB06'
						and zl.sweepindicator is not null
						and pbtl.isprinted is null
						and zplc.captureddate >= vUTCFromDateTimeIGT  --igtutc
						and zplc.captureddate < vUTCToDateTimeIGT)
--		and pbth.tranheaderid not in (select distinct pbth.tranheaderid from ztubt_placedbettransactionheaderlifecyclestate zplc
--					inner join ztubt_placedbettransactionheader pbth on (zplc.tranheaderid =pbth.tranheaderid)
--					inner join ztubt_terminal zt on (zt.terdisplayid=pbth.terdisplayid)
--					inner join ztubt_location zl on (zt.locid=zl.locid)
--					inner join ztubt_sweep_placedbettransactionlineitem pbtl on (pbth.tranheaderid=pbtl.tranheaderid)
--				where 1=1
--				and zplc.betstatetypeid = 'PB06'
--				and zl.sweepindicator is not null
--				and isprinted is null
--				and zplc.captureddate >= vUTCFromDateTimeIGT  --igtutc
--				and zplc.captureddate < vUTCToDateTimeIGT)
		group by pbth.ticketserialnumber

		union all

		-- SWEEP PB03
		select
			pbth.ticketserialnumber
		  , sum(coalesce(pbtln.betpriceamount ,0)) as Wager
		  , 0 as SecondWager
		from
		ztubt_placedbettransactionheader pbth
		inner join ztubt_sweep_placedbettransactionlineitem pbtl
			on (pbth.tranheaderid=pbtl.tranheaderid)
		inner join ztubt_sweep_placedbettransactionlineitemnumber pbtln
			on (pbtl.tranlineitemid =pbtln.tranlineitemid and pbtln.issoldout=false)
		inner join ztubt_placedbettransactionheaderlifecyclestate pblc
			on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB03')
		inner join ztubt_terminal zt on (pbth.terdisplayid=zt.terdisplayid)
		inner join ztubt_location zl on (zt.locid=zl.locid)
		where 1=1
		and pbth.prodid = 4
		and pbth.createddate >= vUTCFromDateTimeIGT  --igtutc
		and pbth.createddate < vUTCToDateTimeIGT
		and zl.sweepindicator is not null
		and pbtl.isprinted is null
		group by pbth.ticketserialnumber

		union all

		-- SPORTS
		select
			ticketserialnumber
		  , sum(coalesce(betamount ,0)) as Wager
		  , 0 as SecondWager
		from (select distinct pbth.ticketserialnumber,pbtl.betamount from
		ztubt_placedbettransactionheader pbth
		inner join ztubt_sports_placedbettransactionlineitem pbtl
			on (pbth.tranheaderid=pbtl.tranheaderid)
		inner join ztubt_sports_placedbettransactionlineitemnumber pbtln
			on (pbtl.tranlineitemid =pbtln.tranlineitemid)
		inner join ztubt_placedbettransactionheaderlifecyclestate pblc
			on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
		where 1=1
		and pbth.prodid in (5,6)
		and pbth.isbetrejectedbytrader = false
		and pbth.createddate >= vUTCFromDateTimeOB  --obutc
		and pbth.createddate < vUTCToDateTimeOB )sq
		group by ticketserialnumber
		;

	create temporary table if not exists temp_ubt_ResultSales
					(TktSerialNumber varchar(200)
					,SalesCommAmount numeric(22,12)
					,SalesFactorAmount numeric(22,12)
					,SecondSalesCommAmount numeric(22,12)
					,SecondSalesFactorAmount numeric(22,12));

	insert into temp_ubt_ResultSales
		-- HR
		select ticketserialnumber
		, sum(SalesCommAmount) SalesCommAmount
		, sum(SalesFactorAmount) SalesFactorAmount
		, 0 SecondSalesCommAmount
		, 0 SecondSalesFactorAmount
		from (
			select
				pbth.ticketserialnumber
			  , cast(pbth.createddate + interval '8 Hours' as date) as CreatedDate
			  , pbtl.tranheaderid
			  , sum(case when pbtl.bettypeid = 'W-P' then coalesce(pbtl.salescommamount,0)/2
					else coalesce(pbtl.salescommamount,0) end) as SalesCommAmount
			  , sum(case when pbtl.bettypeid = 'W-P' then coalesce(pbtl.salesfactoramount,0)/2
					else coalesce(pbtl.salesfactoramount,0) end) as SalesFactorAmount
			from
			ztubt_placedbettransactionheader pbth
			inner join ztubt_horse_placedbettransactionlineitem pbtl
			 on (pbth.tranheaderid=pbtl.tranheaderid)
			inner join ztubt_placedbettransactionheaderlifecyclestate pblc
				on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
			where 1=1
			and pbth.prodid = 1
			and pbth.createddate >= vUTCFromDateTimeBMCS  --bmcsutc
			and pbth.createddate < vUTCToDateTimeBMCS
			group by pbth.ticketserialnumber, pbtl.tranheaderid
			, cast(pbth.createddate + interval '8 Hours' as date)
		)sq
		group by ticketserialnumber

		union all

		-- 4D
		select ticketserialnumber
		, coalesce(sum(SalesCommAmount),0) SalesCommAmount
		, coalesce(sum(SalesFactorAmount),0) SalesFactorAmount
		, coalesce(sum(SecondSalesCommAmount),0) SecondSalesCommAmount
		, coalesce(sum(SecondSalesFactorAmount),0) SecondSalesFactorAmount
--		, round(sum(SalesCommAmount),2) SalesCommAmount
--		, round(sum(SalesFactorAmount),2) SalesFactorAmount
--		, round(sum(SecondSalesCommAmount),2) SecondSalesCommAmount
--		, round(sum(SecondSalesFactorAmount),2) SecondSalesFactorAmount
		from (
			select
				pbth.ticketserialnumber
			  , cast(pbth.createddate + interval '8 Hours' as date) as CreatedDate
			  , sum(coalesce(pbtl.salescommamountbig ,0)) as SalesCommAmount
			  , sum(coalesce(pbtl.salesfactoramountbig ,0)) as SalesFactorAmount
			  , sum(coalesce(pbtl.salescommamountsmall,0)) as SecondSalesCommAmount
			  , sum(coalesce(pbtl.salesfactoramountsmall ,0)) as SecondSalesFactorAmount
			from
			ztubt_placedbettransactionheader pbth
			inner join ztubt_4d_placedbettransactionlineitemnumber pbtl
				on (pbth.tranheaderid=pbtl.tranheaderid)
			inner join ztubt_placedbettransactionheaderlifecyclestate pblc
				on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
			inner join ztubt_drawdates dd
				on (pbth.tranheaderid=dd.tranheaderid)
			where 1=1
			and pbth.prodid = 2
			and pbth.isexchangeticket = false
			and pbth.createddate >= vUTCFromDateTimeIGT  --igtsutc
			and pbth.createddate < vUTCToDateTimeIGT
			group by pbth.ticketserialnumber, cast(pbth.createddate + interval '8 Hours' as date)
		)sq
		group by ticketserialnumber

		union all

		-- TOTO
		select ticketserialnumber
		, coalesce(sum(SalesCommAmount),0) SalesCommAmount
		, coalesce(sum(SalesFactorAmount),0) SalesFactorAmount
		, 0 SecondSalesCommAmount
		, 0 SecondSalesFactorAmount
		from (
			select
				pbth.ticketserialnumber
			  , cast(pbth.createddate + interval '8 Hours' as date) as CreatedDate
			  , sum(coalesce(pbtl.salescommamount ,0)) as SalesCommAmount
			  , sum(coalesce(pbtl.salesfactoramount ,0)) as SalesFactorAmount
			from
			ztubt_placedbettransactionheader pbth
			inner join ztubt_toto_placedbettransactionlineitem pbtl
				on (pbth.tranheaderid=pbtl.tranheaderid)
			inner join ztubt_placedbettransactionheaderlifecyclestate pblc
				on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
			where 1=1
			and pbth.prodid = 3
			and pbth.isexchangeticket = false
			and pbth.createddate >= vUTCFromDateTimeIGT  --igtutc
			and pbth.createddate < vUTCToDateTimeIGT
			and coalesce(pbtl.groupunitsequence,1) =1
			group by pbth.ticketserialnumber, cast(pbth.createddate + interval '8 Hours' as date)
		)sq
		group by ticketserialnumber

		union all

		-- SWEEP
		select ticketserialnumber
		, coalesce(sum(SalesCommAmount),0) SalesCommAmount
		, coalesce(sum(SalesFactorAmount),0) SalesFactorAmount
		, 0 SecondSalesCommAmount
		, 0 SecondSalesFactorAmount
		from (
			select
				pbth.ticketserialnumber
			  , cast(pbth.createddate + interval '8 Hours' as date) as CreatedDate
			  , sum(coalesce(pbtln.salescommamount ,0)) as SalesCommAmount
			  , sum(coalesce(pbtln.salesfactoramount ,0)) as SalesFactorAmount
			from
			ztubt_placedbettransactionheader pbth
			inner join ztubt_sweep_placedbettransactionlineitem pbtl
				on (pbth.tranheaderid=pbtl.tranheaderid)
			inner join ztubt_sweep_placedbettransactionlineitemnumber pbtln
				on (pbtl.tranlineitemid =pbtln.tranlineitemid and pbtln.issoldout=false)
			inner join ztubt_placedbettransactionheaderlifecyclestate pblc
				on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
			where 1=1
			and pbth.prodid = 4
			and pbth.createddate >= vUTCFromDateTimeIGT  --igtutc
			and pbth.createddate < vUTCToDateTimeIGT
			group by pbth.ticketserialnumber, cast(pbth.createddate + interval '8 Hours' as date)
		)sq
		group by ticketserialnumber

		union all

		-- SPORTS
		select ticketserialnumber
		, sum(coalesce(SalesCommAmount,0)) SalesCommAmount
		, sum(coalesce(SalesFactorAmount,0)) SalesFactorAmount
		, 0 SecondSalesCommAmount
		, 0 SecondSalesFactorAmount
		from (
			select distinct
				pbth.ticketserialnumber, pbth.prodid,pbth.tranheaderid
			  , pblc.placedbetlifecycleid , pbtl.sourcesystemtransactionid
			  , pblc.betstatetypeid , pbtl.issinglebet
			  , cast(pbth.createddate + interval '8 Hours' as date) as CreatedDate
			  , (coalesce(pbtl.salescommamount ,0)) as SalesCommAmount
			  , (coalesce(pbtl.salesfactoramount ,0)) as SalesFactorAmount
			from
			ztubt_placedbettransactionheader pbth
			inner join ztubt_sports_placedbettransactionlineitem pbtl
				on (pbth.tranheaderid=pbtl.tranheaderid)
			inner join ztubt_sports_placedbettransactionlineitemnumber pbtln
				on (pbtl.tranlineitemid =pbtln.tranlineitemid)
			inner join ztubt_placedbettransactionheaderlifecyclestate pblc
				on (pbth.tranheaderid=pblc.tranheaderid and pblc.betstatetypeid='PB06')
			where 1=1
			and pbth.prodid in (5,6)
			and pbth.isbetrejectedbytrader = false
			and pbth.createddate >= vUTCFromDateTimeOB  --obutc
			and pbth.createddate < vUTCToDateTimeOB
		--	group by pbth.ticketserialnumber, cast(pbth.createddate + interval '8 Hours' as date)
		--	, pbth.prodid,pbth.tranheaderid
		)sq
		group by ticketserialnumber
		;

	return query
		select
			w.TktSerialNumber
		  , sum(w.wager)
		  , sum(w.secondWager)
		  , sum(s.SalesFactorAmount)
		  , sum(s.secondSalesFactorAmount)
		  , sum(s.SalesCommAmount)
		  , sum(s.secondSalesCommAmount)
		  , sum(w.wager)-sum(s.SalesFactorAmount)
		  , sum(w.secondWager)-sum(s.secondSalesFactorAmount)
		  , sum(coalesce(zv.winningamount,0))+sum(w.wager)+sum(w.secondWager)
		  , sum(coalesce(zv.winningamount,0))
		from
		temp_ubt_ResultWager w
		left outer join temp_ubt_ResultSales s on (w.TktSerialNumber=s.TktSerialNumber)
		left outer join ztubt_validatedbetticket zv on (w.TktSerialNumber=zv.ticketserialnumber and zv.winningamount is not null)
		left outer join ztubt_validatedbetticketlifecyclestate zvlc on (zv.tranheaderid=zvlc.tranheaderid
																and zvlc.betstatetypeid='VB06')
		where 1=1
		group by w.TktSerialNumber
		;

drop table temp_ubt_ResultWager;
drop table temp_ubt_ResultSales;

end;
$$;


ALTER FUNCTION public.sp_ubt_getamounttransaction(in_fromdatetime date, in_todatetime date) OWNER TO consultant01;
