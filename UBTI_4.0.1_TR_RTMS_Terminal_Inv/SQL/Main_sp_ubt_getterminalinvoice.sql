
CREATE FUNCTION public.sp_ubt_getterminalinvoice(businessdate date) RETURNS TABLE(terdisplayid character varying, transactiondate date, prodid integer, locdisplayid character varying, sales_type character varying, totalcount integer, amount numeric)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column

declare
vbusinessdate date;
vinputdate date;
vstartperiod date;
vendperiod date;
vfromdateIGT timestamp;--@BusinessDateFrom
vtodateIGT timestamp;--@BusinessDateTo
vfromdatetimeIGT_UTC timestamp;
vtodatetimeIGT_UTC timestamp;
vfromdatetimeOB_UTC timestamp;
vtodatetimeOB_UTC timestamp;
vfromdatetimeNonHost_UTC timestamp;
vtodatetimeNonHost_UTC timestamp;
vfromdatetimeBMCS_UTC timestamp;
vtodatetimeBMCS_UTC timestamp;
vGSTRate numeric; --percentage

vactualdate date;
/*DECLARE @BusinessDateFromDATETIME;
DECLARE @BusinessDateToDATETIME;
DECLARE @l_PeriodFromDateNonHostUTCDATETIME
DECLARE @l_PeriodToDateNonHostUTCDATETIME
DECLARE @InsertedDateDATETIME;
DECLARE @l_PeriodFromDateIGTUTC DATETIME
DECLARE @l_PeriodToDateIGTUTC DATETIME
DECLARE @l_PeriodFromDateOBUTC DATETIME --(15)
DECLARE @l_PeriodToDateOBUTC DATETIME --(15)

*/

v_totomatchBettypes text[] := (SELECT array(SELECT bettypeid FROM public.sp_ubt_gettotomatchbettypes()));

BEGIN

select businessdate into vbusinessdate;

-- GST Config
SELECT COALESCE(gstrate , 0)/100 INTO vGSTRate
FROM ztubt_gstconfig zg
WHERE businessdate BETWEEN zg.effectivefrom AND COALESCE(zg.enddate , 'infinity'::date)
ORDER BY zg.effectivefrom
LIMIT 1;

create temp table if not exists ubt_temp_TmpTerLocPrdSalesAmt
	(
	terdisplayid varchar(200),
	transactiondate timestamp,
	prodid int4,
	locdisplayid varchar(200),
	sales_type varchar(100),
	totalcount int4,
	amount numeric(32, 11),
	sub_prod varchar(100)
	);


create temp table ubt_temp_terminal as
	Select Ter.TerDisplayID, Ter.LocID  FROM public.ztubt_Terminal Ter;

create temp table ubt_temp_product as
	Select PD.ProdID,
	case PD.prodid when 5 then 'SPORTS' else trim(PD.ProdName) end as ProdName  FROM public.ztubt_Product PD
	WHERE PD.ProdID != 6 ;--(15) remove Sports Live so that all Sports refer to ProductID = 5 only

insert into ubt_temp_product (prodid, prodname) values (3, 'TOTO MATCH');


create temp table ubt_temp_Location as
	Select Loc.LocID, Loc.LocDisplayID, Loc.SweepIndicator,Loc.LocTypeID,Loc.IsGST,Loc.AccountNumber,
	Loc.BranchID,Loc.Bankid,Loc.LocName,Loc.IsIBG,Loc.RetID,LOC.CHID
	from public.ztubt_location  Loc;

--GetcommondatesfroUBT

	select
	fromdatetimeigt,todatetimeigt,utcfromdatetimeigt,utctodatetimeigt,utcfromdatetimeob,utctodatetimeob,
	utcfromdatetimenonhost,utctodatetimenonhost, utcfromdatetimebmcs, utctodatetimebmcs
	into
	vfromdateIGT ,
	vtodateIGT ,
	vfromdatetimeIGT_UTC ,
	vtodatetimeIGT_UTC ,
	vfromdatetimeOB_UTC ,
	vtodatetimeOB_UTC ,
	vfromdatetimeNonHost_UTC ,
	vtodatetimeNonHost_UTC,
	vfromdatetimeBMCS_UTC,
	vtodatetimeBMCS_UTC
	from public.sp_ubt_getcommonubtdates(vbusinessdate,vbusinessdate);


create temp table if not exists ubt_temp_TmpTicketByWageAndSales
	(
	ticketserialnumber varchar(500),
	wager numeric(22,2),
	secondwager numeric(22,2),
	sales numeric(22, 11),
	secondsales numeric(22, 11),
	salescomm numeric(22, 11),
	secondsalescomm numeric(22, 11),
	gst numeric(22, 11),
	secondgst numeric(22, 11),
	returnamount numeric(22,2),
	winningamount numeric(22,2)
	);

-- from function getamounttransaction

INSERT INTO ubt_temp_TmpTicketByWageAndSales
 select * from  public.sp_ubt_getamounttransaction(vbusinessdate,vbusinessdate);




create temp table if not exists ubt_temp_ResultCashlessInTerminal
	(
	terdisplayid varchar(200),
	productname varchar(100),
	amount numeric(22, 11),
	ct int4
	);

insert into ubt_temp_ResultCashlessInTerminal

	select  pd.terdisplayid, 'NETS', coalesce (sum(coalesce (pd.paidamount,0)),0), count(distinct pd.paymentdetailid)
	from public.ztubt_paymentdetail pd
	where pd.paymenttypeid in ('NC','NN')
	and pd.createddate >= vfromdatetimeNonHost_UTC
	and pd.createddate < vtodatetimeNonHost_UTC
	group by pd.terdisplayid

	union all
	select pd.terdisplayid, 'CASHCARD', coalesce (sum(coalesce (pd.paidamount,0)),0), count(distinct pd.paymentdetailid)
	from public.ztubt_paymentdetail pd
	where pd.paymenttypeid in ('NCC') --cashcard
	and pd.createddate >= vfromdatetimeNonHost_UTC
	and pd.createddate < vtodatetimeNonHost_UTC
	group by pd.terdisplayid

	union all

	select pd.terdisplayid, 'Flashpay', coalesce (sum(coalesce (pd.paidamount,0)),0), count(distinct pd.paymentdetailid)
	from public.ztubt_paymentdetail pd
	where pd.paymenttypeid in ('NFP') --Flashpay
	and pd.createddate >= vfromdatetimeNonHost_UTC
	and pd.createddate < vtodatetimeNonHost_UTC
	group by pd.terdisplayid
	;

create temp table if not exists ubt_temp_iTOTO
	(
	ticketserialnumber varchar(200),
	itotoindicator bool,
	groupunitsequence int4,
	bettype int4
	)
;
INSERT INTO ubt_temp_iTOTO
	select distinct ticketserialnumber, itotoindicator, groupunitsequence,
	case
        when pbt.bettypeid != ALL(v_totomatchBettypes) THEN 1 --TOTO
        else 2 --totomatch
    end AS bettype
	from public.ztubt_placedbettransactionheader  pb
	inner join public.ztubt_toto_placedbettransactionlineitem  pbt on pb.tranheaderid = pbt.tranheaderid
	where ((groupunitsequence = 1) --group toto
	or (itotoindicator = true)) --itoto
	and (pb.createddate between vfromdatetimeIGT_UTC   and vtodatetimeIGT_UTC )
;


create temp table if not exists ubt_temp_GroupTOTO
	(
	ticketserialnumber varchar(200),
	grouphostid varchar(50),
	groupunitsequence int4,
	bettype int4
	);

insert into ubt_temp_grouptoto
	select distinct ticketserialnumber, grouphostid, groupunitsequence,
	case
        when pbt.bettypeid != ALL(v_totomatchBettypes) THEN 1 --TOTO
        else 2 --totomatch
    end AS bettype
	from public.ztubt_placedbettransactionheader  pb
	inner join public.ztubt_toto_placedbettransactionlineitem  pbt on pb.tranheaderid = pbt.tranheaderid
	where groupunitsequence is not null --group toto
	and (pb.createddate between vfromdatetimeIGT_UTC   and vtodatetimeIGT_UTC  )
;

create temp table ubt_temp_CancelledBetTicketState as
	select cb.cartid,cb.terdisplayid,cb.ticketserialnumber,cb.tranheaderid,cb.cancelleddate ,cb.cancelledamout,cb.prodid
	from  public.ztubt_cancelledbetticket  cb
	inner join public.ztubt_cancelledbetticketlifecyclestate cbt
   on cb.tranheaderid = cbt.tranheaderid and cbt.betstatetypeid = 'CB06'
	where cb.cancelleddate between vfromdateIGT and vtodateIGT
;


create temp table if not exists ubt_temp_SalesGroupToto
(
	terdisplayid varchar(200),
	prodid int4,
	locdisplayid varchar(200),
	bettype int4,
	wagertotal int4,--(8)
	canceltotal int4,--(8)
	wageramount numeric(32, 11), --wager of each product
	salesamount numeric(32, 11)  --Sales of each product (Wager * Sales Factor)
);

insert into ubt_temp_SalesGroupToto(terdisplayid, prodid, locdisplayid,bettype, wagertotal, canceltotal, wageramount, salesamount)
	select ter.terdisplayid,
	3 as prodid,
	loc.locdisplayid as locdisplayid,
	fin.bettype as bettype,
	count(distinct fin.ticketserialnumber)	as wagertotal,
	count(distinct can.ticketserialnumber)	as canceltotal,
	sum(coalesce(fin.wager,0))	as wageramount,
	sum(coalesce(fin.sales,0))	as salesamount
	from	ztubt_placedbettransactionheader pbth
	inner join ubt_temp_terminal ter  on pbth.terdisplayid=ter.terdisplayid
	inner join ubt_temp_location loc  on ter.locid=loc.locid
	inner join
		(
		select gat.ticketserialnumber,
		gat.wager,
		case when can.ticketserialnumber is null then gat.sales else 0 end as sales,
		itoto.bettype
		from ubt_temp_itoto itoto
		inner join ubt_temp_tmpticketbywageandsales gat on itoto.ticketserialnumber = gat.ticketserialnumber
		left join ubt_temp_cancelledbetticketstate can on gat.ticketserialnumber = can.ticketserialnumber
		where itoto.groupunitsequence = 1 --group toto
		)fin on pbth.ticketserialnumber = fin.ticketserialnumber

		left join
		(
		select gat.ticketserialnumber, gat.wager, gat.sales, itoto.bettype
		from ubt_temp_itoto itoto
		inner join ubt_temp_tmpticketbywageandsales gat on itoto.ticketserialnumber = gat.ticketserialnumber
		inner join ubt_temp_cancelledbetticketstate cb on gat.ticketserialnumber = cb.ticketserialnumber
		where itoto.groupunitsequence = 1 --group toto
		)can on pbth.ticketserialnumber = can.ticketserialnumber

	where
	pbth.prodid = 3
	and (pbth.createddate between vfromdatetimeIGT_UTC   and vtodatetimeIGT_UTC )
	group by  ter.terdisplayid,loc.locdisplayid, fin.bettype
	;

create temp table if not exists ubt_temp_salestoto
	(
	terdisplayid varchar(200),
	locdisplayid varchar(200),
	prodid int4,
	bettype int4,
	wagertotal int4,--(8)
	canceltotal int4,--(8)
	wageramount numeric(32, 11), --wager of each product
	salesamount numeric(32, 11)  --sales of each product (wager * sales factor)
	);

insert into ubt_temp_salestoto(terdisplayid, locdisplayid,prodid, bettype, wagertotal, canceltotal, wageramount, salesamount)
	select ter.terdisplayid		as terdisplayid,
	loc.locdisplayid			as locdisplayid,
	3		as prodid,
	1 as bettype,
	count(distinct fin.ticketserialnumber)	as wagertotal, --(1)count(1)		as totalcount,
	count(distinct can.ticketserialnumber)	as canceltotal,--(8)
	sum(coalesce(fin.wager,0))	as wageramount,
	sum(coalesce(fin.sales,0))	as salesamount
	from	public.ztubt_placedbettransactionheader pbth
	inner join ubt_temp_terminal ter  on pbth.terdisplayid=ter.terdisplayid
	inner join ubt_temp_location loc  on ter.locid=loc.locid
	inner join
		(
		select gat.ticketserialnumber, gat.wager, case when can.ticketserialnumber is null then gat.sales else 0 end as sales
		from ubt_temp_itoto itoto
		inner join ubt_temp_tmpticketbywageandsales gat on itoto.ticketserialnumber = gat.ticketserialnumber
		left join ubt_temp_cancelledbetticketstate can on gat.ticketserialnumber = can.ticketserialnumber
		where itoto.itotoindicator = true --itoto
		)fin on pbth.ticketserialnumber = fin.ticketserialnumber
	left join
		(
		select gat.ticketserialnumber, gat.wager, gat.sales
		from ubt_temp_itoto itoto
		inner join ubt_temp_tmpticketbywageandsales gat on itoto.ticketserialnumber = gat.ticketserialnumber
		inner join ubt_temp_cancelledbetticketstate cb on gat.ticketserialnumber = cb.ticketserialnumber
		where itoto.itotoindicator = true --itoto
		)can on pbth.ticketserialnumber = can.ticketserialnumber

	where pbth.prodid = 3
	and (pbth.createddate between vfromdatetimeIGT_UTC   and vtodatetimeIGT_UTC )
	group by ter.terdisplayid,loc.locdisplayid
	;

--FILL IN TRANS AMOUNT DETAIL DATA

create temp table if not exists ubt_temp_transamountdetaildata
	(
	terdisplayid varchar(200),
	productname varchar(100),
	amount numeric(32, 13),
	ct int4,
	flag char(3),
	transtype char(2),
	fromdate date,
	todate date
	);
Insert Into ubt_temp_transamountdetaildata
	select terdisplayid,	trim(prodname),(amount * 100)	,ticketcount,	flag,transtype
	from public.sp_ubt_gettransamountdetails(vbusinessdate,vbusinessdate);


-- insert all data

insert into ubt_temp_tmpterlocprdsalesamt(terdisplayid, transactiondate, prodid, locdisplayid, sales_type, totalcount, amount, sub_prod)

	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	prd.prodid	as prodid,
	loc.locdisplayid as locdisplayid,
	'1'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	sum(coalesce(amount,0))as amount,
	a.productname as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	inner join ubt_temp_product prd on a.productname = prd.prodname
		where flag='COL' and transtype != 'OO' and prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
		group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname

	union all

	--offline orders - wager

	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	case when a.productname <> 'Gate Admission' then 31
		 when a.productname = 'Gate Admission' then 99 end as prodid,
	loc.locdisplayid as locdisplayid,
	'1'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	sum(coalesce(amount,0))as amount,
	'' as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	where flag='COL' and transtype = 'OO'
	group by loc.locdisplayid,ter.terdisplayid,case when a.productname <> 'Gate Admission' then 31
		 											when a.productname = 'Gate Admission' then 99 end

	union ALL
	-- Gate Admission - Sales

	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	99 as prodid,
	loc.locdisplayid as locdisplayid,
	'116'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	--sum(coalesce(amount,0)*(1-vGSTRate))as amount,
	sum(coalesce(amount,0)*(100 / (100 + vGSTRate * 100)))as amount,
	'' as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	where flag='COL' and transtype = 'OO' and a.productname = 'Gate Admission'
	group by loc.locdisplayid,ter.terdisplayid

	union all

	----2	AT_CANCELAMT	Yes	cancel amount

	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	prd.prodid	as prodid,
	loc.locdisplayid as locdisplayid,
	'2'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	(sum(coalesce(amount,0)) * -1)as amount,
	a.productname as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	inner join ubt_temp_product prd on a.productname = prd.prodname
		where flag='CAN' and  prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
		group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname

	union all

	---3	AT_VALIDAMT	Yes	validation amount
	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	prd.prodid	as prodid,
	loc.locdisplayid as locdisplayid,
	'3'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	(sum(coalesce(amount,0)) * -1)as amount,
	a.productname as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	inner join ubt_temp_product prd on a.productname = prd.prodname
		where flag='PAY' and  prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
		group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname

	union all
	---4	Rebate Amount
	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	prd.prodid	as prodid,
	loc.locdisplayid as locdisplayid,
	'4'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	(sum(coalesce(amount,0)) * -1)as amount,
	a.productname as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	inner join ubt_temp_product prd on a.productname = prd.prodname
		where flag='RBT' and  prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
		group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname
	union all
	--61	Refund Amount--(4)(5)

	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	prd.prodid	as prodid,
	loc.locdisplayid as locdisplayid,
	'61'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	(sum(coalesce(amount,0)) * -1)as amount,
	a.productname as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	inner join ubt_temp_product prd on a.productname = prd.prodname
		where flag='RFD' AND TransType != 'OO' and  prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
		group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname

	union all
	--offline orders - refund
	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	31	as prodid,
	loc.locdisplayid as locdisplayid,
	'61'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	(sum(coalesce(amount,0)) * -1)as amount,
	'' as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
		where flag='RFD' AND TransType = 'OO'
		group by loc.locdisplayid,ter.terdisplayid

	union all
	--6	AT_TAXAMT 	Yes	tax amount
	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	prd.prodid	as prodid,
	loc.locdisplayid as locdisplayid,
	'6'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	(sum(coalesce(amount,0)) * -1)as amount,
	a.productname as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	inner join ubt_temp_product prd on a.productname = prd.prodname
		where flag='GST' AND prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
		group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname

	union all
	---7	AT_COMMAMT	Yes	commission amount
	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate	as transactiondate,
	prd.prodid	as prodid,
	loc.locdisplayid as locdisplayid,
	'7'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	(sum(coalesce(amount,0)) * -1)as amount,
	a.productname as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	inner join ubt_temp_product prd on a.productname = prd.prodname
		where flag='SAL' AND prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
		group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname


	------16. AT_FRACVALAMT  Show fractional validation amounts [Validation amounts for GROUP TOTO - This is a subset of Sales Type  3]
	union all

	select	ter.terdisplayid				as terdisplayid,
	vbusinessdate transactiondate,
	3		as prodid,
	loc.locdisplayid	as locdisplayid,
	'16'		as sales_type,
	count(distinct vb.tranheaderid)	as totalcount,
	sum(coalesce(vb.winningamount,0))	as totalamount,
	case when ph.bettypeid != ALL(v_totomatchBettypes) then 'TOTO'
		else 'TOTO MATCH' end as sub_prod
	from --(1)
	(
		select vb.tranheaderid,vb.winningamount,vb.cartid,vb.terdisplayid,vb.createdvalidationdate
		from public.ztubt_validatedbetticket  vb --(16)
		inner join public.ztubt_validatedbetticketlifecyclestate  vbt
		on vb.tranheaderid = vbt.tranheaderid and vbt.betstatetypeid = 'VB06'
		where vb.winningamount != 0 and vb.winningamount is not null --(8)
		and vb.createdvalidationdate >= vfromdatetimeIGT_UTC and vb.createdvalidationdate < vtodatetimeIGT_UTC --(16)
	)vb
	inner join
	(
		select distinct tranheaderid, grouphostid, groupunitsequence, bettypeid from public.ztubt_toto_placedbettransactionlineitem
	)ph on vb.tranheaderid = ph.tranheaderid --(16)
	inner join ubt_temp_terminal ter on vb.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	where ph.groupunitsequence is not null --(16)
	group by loc.locdisplayid,ter.terdisplayid,
	case when ph.bettypeid != ALL(v_totomatchBettypes) then 'TOTO'
		else 'TOTO MATCH' end

	union all
	--52. House syndicate sales amount - [iTOTO sales – this is a subset of sales type 1]
	--TOTO
	select	s.terdisplayid			as terdisplayid,
	vbusinessdate	as transactiondate,
	s.prodid	as prodid,
	s.locdisplayid as locdisplayid,
	'52' as sales_type,
	sum(coalesce(s.wagertotal,0))		as totalcount,
	sum(coalesce(s.wageramount,0))	as totalamount,
	'TOTO' as sub_prod
	from ubt_temp_salestoto s
	group by s.terdisplayid, s.prodid, s.locdisplayid

	union all
	--25. House syndicate sales amount by sales factor [iTOTO Sales by Sales Factor – This is a subset of Sales Type 116]
	--TOTO
	select	s.terdisplayid	as terdisplayid,
	vbusinessdate		as transactiondate,
	3	as prodid,
	s.locdisplayid	as locdisplayid,
	'25'	as sales_type,
	sum(coalesce(s.wagertotal,0) - coalesce(s.canceltotal,0))		as totalcount,
	trunc(sum(coalesce(s.salesamount,0)) / 100 , 2) * 100	as totalamount ,
	'TOTO' as sub_prod
	from ubt_temp_salestoto s
	group by s.locdisplayid,s.terdisplayid

	union all
	--40. GST TAX rate --------------------------------------------------------------------

	select	loc.terdisplayid as terdisplayid,
	vbusinessdate		as transactiondate,
	prd.prodid		as prodid,
	loc.locdisplayid	as locdisplayid,
	'40'		as sales_type,
	0		as totalcount ,
	gst.gstrate,
	prd.prodname as sub_prod
	from
	(
		select loc.locdisplayid, ter.terdisplayid from ubt_temp_location loc
		inner join ubt_temp_terminal ter on loc.locid = ter.locid
		where loc.isgst = true
	)loc
	cross join
	(
		select * from ubt_temp_product where prodid in (2,3,4) --lottery / es
	)prd
	cross join
	(
		select gstrate from public.ztubt_gstconfig a
		where (vbusinessdate between effectivefrom and enddate) or (vbusinessdate >=effectivefrom and enddate is null) limit 1
		--check this code
	)gst

	union all
	--59. House syndicate cancel amount - [iTOTO cancellations – This is a Sub Set of Sales Type 2]
	--TOTO
	select	cb.terdisplayid	as terdisplayid,
	vbusinessdate as transactiondate,
	3		as prodid,
	loc.locdisplayid as locdisplayid,
	'59'		as sales_type,
	count(distinct cb.ticketserialnumber)	as totalcount, --(1)count(1)as totalcount,
	sum(coalesce (cb.cancelledamout,0))as totalamount,
	'TOTO' as sub_prod
	from ubt_temp_cancelledbetticketstate cb
	inner join ubt_temp_itoto itoto on cb.ticketserialnumber = itoto.ticketserialnumber and itoto.itotoindicator = true
	inner join ubt_temp_terminal ter on cb.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	where cb.prodid = 3 --(1)and tpbtli.itotoindicator = 1
	 and cb.cancelleddate between vfromdateIGT and vtodateIGT
	group by cb.terdisplayid,loc.locdisplayid

	union all
	---60. Player syndicate sales amount by sales factor - [GROUP TOTO Sales by Sales Factor – This is a subset of Sales Type 116]
	--TOTO, totomatch
	SELECT S.TerDisplayID AS TerDisplayID ,
	 vbusinessdate	AS TransactionDate,
	S.ProdID	AS ProdID,
	S.LocDisplayID AS LocDisplayID,
	'60' AS sales_type,
	SUM(coalesce(S.WagerTotal,0)  - coalesce(S.CancelTotal,0))	AS TotalCount,--(8)
	trunc(SUM(coalesce(s.SalesAmount,0)) / 100 , 2) * 100	AS TotalAmount,
	case when S.bettype = 1 then 'TOTO'
		else 'TOTO MATCH' end as sub_prod
	FROM ubt_temp_SalesGROUPToto S
	GROUP BY  S.TerDisplayID,S.ProdID, S.LocDisplayID,
		case when S.bettype = 1 then 'TOTO'
			else 'TOTO MATCH' end

	union all

	--69. Player syndicate sales amount - [GROUP TOTO Sales – This is a subset of Sales Type 1]----------
	--TOTO, totomatch
	SELECT	S.TerDisplayID			AS TerDisplayID,
	vbusinessdate	AS TransactionDate,
	3 AS ProdID,
	S.LocDisplayID AS LocDisplayID,
	'69' AS sales_type,
	SUM(COALESCE(S.WagerTotal,0))AS TotalCount,
	SUM(COALESCE(S.WagerAmount,0))		AS TotalAmount,--(1)COALESCE(S.Amount,0)		AS TotalAmount
	case when S.bettype = 1 then 'TOTO'
		else 'TOTO MATCH' end as sub_prod
	FROM ubt_temp_SalesGROUPToto S
	GROUP BY S.TerDisplayID, S.LocDisplayID,
		case when S.bettype = 1 then 'TOTO'
			else 'TOTO MATCH' end

	union all
	--70 [	 GROUP TOTO Cancel Amount – This is a subset of Sales Type 2]-----------------------------------
	--TOTO, totomatch
	select			cbt.terdisplayid as terdisplayid,
	vbusinessdate	as transactiondate,
	3				as prodid,
	loc.locdisplayid	as locdisplayid,
	'70'				as sales_type,
	count(distinct gt.grouphostid)	as totalcount,
	sum(coalesce (cbt.cancelledamout,0))	as totalamount,
	case when gt.bettype = 1 then 'TOTO'
		else 'TOTO MATCH' end as sub_prod
	from ubt_temp_cancelledbetticketstate cbt
	inner join ubt_temp_grouptoto gt on cbt.ticketserialnumber = gt.ticketserialnumber
	inner join ubt_temp_terminal ter on cbt.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	where  cbt.prodid = 3
	and cbt.cancelleddate between vfromdateIGT and vtodateIGT
	group by cbt.terdisplayid,loc.locdisplayid,
		case when gt.bettype = 1 then 'TOTO'
			else 'TOTO MATCH' end

	union all
	--105 Sales Commision Rate
	select 	loc.terdisplayid as terdisplayid,
	vbusinessdate		as transactiondate,
	sal.prodid		as prodid,
	loc.locdisplayid	as locdisplayid,
	'105'				as sales_type,
	0				as totalcount,
	sal.salescommission,
    '' as sub_prod
		from
		(
			select loc.locdisplayid, ter.terdisplayid
			from ubt_temp_location loc
			inner join ubt_temp_terminal ter on loc.locid = ter.locid
			where loc.isgst = true
		)loc
		cross join
		(
			select distinct salescommission, prodid
			from public.ztubt_salescomconfig    a
			where commissiontype=1 and  isdeleted = false and prodid in (2,4) --lottery / es
		)sal

	union all
	--TOTO, totomatch
	select 	loc.terdisplayid as terdisplayid,
	vbusinessdate		as transactiondate,
	sal.prodid		as prodid,
	loc.locdisplayid	as locdisplayid,
	'105'				as sales_type,
	0				as totalcount,
	sal.salescommission,
	sp.sub_prod
		from
		(
			select loc.locdisplayid, ter.terdisplayid
			from ubt_temp_location loc
			inner join ubt_temp_terminal ter on loc.locid = ter.locid
			where loc.isgst = true
		)loc
		cross join
		(
			select distinct salescommission, prodid
			from public.ztubt_salescomconfig    a
			where commissiontype=1 and  isdeleted = false and prodid = 3
		)sal
		cross join (select 'TOTO' as sub_prod union all select 'TOTO MATCH' as sub_prod) as sp

	union all
	--108 AT_ON_EZLINK-----------------------------------------------------------------------

	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate as transactiondate,
	0		as prodid,
	loc.locdisplayid		as locdisplayid,
	'108'		as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	sum(coalesce(a.amount,0))as amount,
	'' as sub_prod
	from	ubt_temp_location as loc
	left join ubt_temp_terminal as ter on  ter.locid=loc.locid
	left join ubt_temp_resultcashlessinterminal a on a.terdisplayid = ter.terdisplayid
	where a.productname = 'NETS'
	group by ter.terdisplayid,loc.locdisplayid

	union all
	--110	AT_ON_ATM	Yes	 Flashpay transaction --------------------------------------------------
	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate as transactiondate,
	0		as prodid,
	loc.locdisplayid		as locdisplayid,
	'110'		as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	sum(coalesce(a.amount,0))as amount,
	'' as sub_prod
	from	ubt_temp_location as loc
	left join ubt_temp_terminal as ter on  ter.locid=loc.locid
	left join ubt_temp_resultcashlessinterminal a on a.terdisplayid = ter.terdisplayid
	where a.productname = 'Flashpay'
	group by ter.terdisplayid,loc.locdisplayid

	union all
	--112	AT_ON_CASHCARD	Yes	 Cashcard transaction------------------------------------------
	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate as transactiondate,
	0		as prodid,
	loc.locdisplayid		as locdisplayid,
	'112'		as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	sum(coalesce(a.amount,0))as amount,
	'' as sub_prod
	from	ubt_temp_location as loc
	left join ubt_temp_terminal as ter on  ter.locid=loc.locid
	left join ubt_temp_resultcashlessinterminal a on a.terdisplayid = ter.terdisplayid
	where a.productname = 'CASHCARD'
	group by ter.terdisplayid,loc.locdisplayid

	union all
	---116	AT_NOSPPL_SALAMT 	Yes	Sales multiplied by sales factor-------------------------------
	select	ter.terdisplayid	as terdisplayid,
	vbusinessdate as transactiondate,
	a.prodid as prodid,
	loc.locdisplayid		as locdisplayid,
	'116'		as sales_type,
	sum(coalesce(a.total,0)  - coalesce(can.total,0))	as totalcount, --(8)
	trunc(sum(coalesce(a.amount,0)) / 100, 2) * 100 as totalamount,
	a.sub_prod as sub_prod
  	from
		(
			select a.ticketserialnumber, sum(a.sales + a.secondsales) as amount,
			count(distinct a.ticketserialnumber) as total, pb.terdisplayid,
			case
				when pb.prodid=6 then 5 --prematch and live to be one same prodid
				else pb.prodid	end as prodid, --(15)
			case when pb.prodid <> 3 then ''
				when pb.prodid = 3 and EXISTS (
						SELECT 1
						FROM ztubt_toto_placedbettransactionlineitem pbtlin
						WHERE pb.TranHeaderID = pbtlin.TranHeaderID
							AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
					) then 'TOTO MATCH'
				else 'TOTO' end as sub_prod
			from ubt_temp_tmpticketbywageandsales a
			inner join public.ztubt_placedbettransactionheader  pb on a.ticketserialnumber=pb.ticketserialnumber
			where pb.iscancelled = false and (
					(
					pb.prodid in (2,3,4) --lottery / es
					and pb.createddate between vfromdatetimeIGT_UTC   and vtodatetimeIGT_UTC

					)
					or --(15)
					(
					pb.prodid in (5,6) --sports / ob
					and pb.createddate between vfromdatetimeOB_UTC and vtodatetimeOB_UTC
					)
					or
					(
					pb.prodid in (1) --horse racing / bmcs
					and pb.createddate between vfromdatetimeBMCS_UTC and vtodatetimeBMCS_UTC
					)
				)

				
			group by a.ticketserialnumber, pb.terdisplayid, pb.prodid,
				case when pb.prodid <> 3 then ''
					when pb.prodid = 3 and EXISTS (
							SELECT 1
							FROM ztubt_toto_placedbettransactionlineitem pbtlin
							WHERE pb.TranHeaderID = pbtlin.TranHeaderID
								AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
						) then 'TOTO MATCH'
					else 'TOTO' end
		)a
		inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
		inner join ubt_temp_location loc on ter.locid = loc.locid
		left join --(8)
		(
			select a.ticketserialnumber, sum(a.sales + a.secondsales) as amount,
			count(distinct a.ticketserialnumber) as total, cb.terdisplayid, cb.prodid,
			case when cb.prodid <> 3 then ''
				when cb.prodid = 3 and EXISTS (
						SELECT 1
						FROM ztubt_toto_placedbettransactionlineitem pbtlin
						WHERE cb.TranHeaderID = pbtlin.TranHeaderID
							AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
					) then 'TOTO MATCH'
				else 'TOTO' end as sub_prod
			from ubt_temp_tmpticketbywageandsales a
			inner join ubt_temp_cancelledbetticketstate cb on a.ticketserialnumber = cb.ticketserialnumber
			where cb.prodid in (2,3,4) --lottery / es
			group by a.ticketserialnumber, cb.terdisplayid, cb.prodid,
				case when cb.prodid <> 3 then ''
					when cb.prodid = 3 and EXISTS (
							SELECT 1
							FROM ztubt_toto_placedbettransactionlineitem pbtlin
							WHERE cb.TranHeaderID = pbtlin.TranHeaderID
								AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
						) then 'TOTO MATCH'
					else 'TOTO' end
		)can
			on a.ticketserialnumber = can.ticketserialnumber and a.terdisplayid = can.terdisplayid
			and a.prodid = can.prodid and a.sub_prod = can.sub_prod

		group by loc.locdisplayid,ter.terdisplayid,a.prodid, a.sub_prod
	;
/*	UPDATE T
		SET TotalCount = T.TotalCount
		FROM
		(
			SELECT LocDisplayID,TerDisplayId,ProdID,TotalCount FROM ubt_temp_TmpTerLocPrdSalesAmt WHERE Sales_type = 116
		)T
		INNER JOIN
		(
			SELECT LocDisplayID,TerDisplayId,ProdID,TotalCount FROM  ubt_temp_TmpTerLocPrdSalesAmt WHERE Sales_type = 7
		)FIN ON T.LocDisplayID = FIN.LocDisplayID AND T.TerDisplayId = FIN.TerDisplayId AND T.ProdID = FIN.ProdID ;
	--end modify (9)*/

/*update ubt_temp_TmpTerLocPrdSalesAmt T
set TotalCount = FIN.TotalCount
from (
		SELECT LocDisplayID,TerDisplayId,ProdID,TotalCount FROM  ubt_temp_TmpTerLocPrdSalesAmt WHERE Sales_type = '7'
		)FIN
	where
	T.sales_type='116' and
	T.LocDisplayID = FIN.LocDisplayID AND T.TerDisplayId = FIN.TerDisplayId AND T.ProdID = FIN.ProdID ;
*/

-- [2022-04-01] wf: Start handling for Sweep Consignee and Sweep Retailer Cancellation
create temp table if not exists ubt_temp_sales_scandsr
	(
	TerDisplayID varchar(200),
	ProdID int4,
	ProductName varchar(100),
	ActualDate Date,
	TotalCount int,
	Amount decimal(32, 11)
	);

Insert Into ubt_temp_sales_scandsr
--values ('001811006', 4, 'TOTO','2021-11-24'::date, 99, 100);
select *  from public.sp_ubt_getsweepsalespersrterminal(vbusinessdate, vbusinessdate);




DELETE  FROM ubt_temp_tmpterlocprdsalesamt SAL USING ubt_temp_sales_scandsr RS
where  SAL.TerDisplayID = RS.TerDisplayID
AND RS.ProdID = SAL.ProdID
AND SAL.Sales_type = '116';




INSERT INTO ubt_temp_tmpterlocprdsalesamt(TerDisplayId, TransactionDate, ProdID, LocDisplayID, Sales_type, TotalCount, Amount, sub_prod)
	SELECT	RS.TerDisplayID	as TerDisplayID,
			vbusinessdate			AS TransactionDate,
			RS.ProdID			AS ProdID,
			LOC.LocDisplayID		AS LocDisplayID,
			'116'					aS sales_type,
			RS.TotalCount,
			RS.Amount*100,
			'' as sub_prod
	FROM ubt_temp_sales_scandsr RS
	INNER JOIN ubt_temp_terminal TER ON RS.TerDisplayID = TER.TerDisplayID
	INNER JOIN ubt_temp_location LOC ON TER.LocID = LOC.LocID
	where RS.ActualDate = vbusinessdate;

-- [2022-04-01] wf: End handling for Sweep Consignee and Sweep Retailer Cancellation

--119	at_salesfactor	yes	sales factor -------------------------------
create temp table if not exists ubt_temp_salesfactorconfig
	(
		prodid int4,
		salesfactor numeric(13,12)
	)
	;
insert into ubt_temp_salesfactorconfig
	select a.prodid, a.salesfactor  from  public.ztubt_salescomconfig  a
	where commissiontype=1  and a.isdeleted = false and prodid in (2,3,4)
;
insert into ubt_temp_tmpterlocprdsalesamt(terdisplayid, transactiondate, prodid, locdisplayid, sales_type, totalcount, amount, sub_prod)
		select ter.terdisplayid, vbusinessdate, sfc.prodid, ter.locdisplayid, '119', 0, sfc.salesfactor, ''
		from
		(
			select ter.terdisplayid, loc.locdisplayid from ubt_temp_terminal ter
			inner join ubt_temp_location loc on ter.locid = loc.locid
		)ter
		cross join
		(
			select distinct sfc.prodid,sfc.salesfactor from ubt_temp_salesfactorconfig sfc  where sfc.prodid <> 3
		)sfc
;
insert into ubt_temp_tmpterlocprdsalesamt(terdisplayid, transactiondate, prodid, locdisplayid, sales_type, totalcount, amount, sub_prod)
		select ter.terdisplayid, vbusinessdate, sfc.prodid, ter.locdisplayid, '119', 0, sfc.salesfactor, sp.sub_prod
		from
		(
			select ter.terdisplayid, loc.locdisplayid from ubt_temp_terminal ter
			inner join ubt_temp_location loc on ter.locid = loc.locid
		)ter
		cross join
		(
			select distinct sfc.prodid,sfc.salesfactor from ubt_temp_salesfactorconfig sfc where sfc.prodid = 3
		)sfc
		cross join (select 'TOTO' as sub_prod union all select 'TOTO MATCH' as sub_prod) as sp
;

insert into 	ubt_temp_tmpterlocprdsalesamt
		SELECT	coalesce(DB.TerDisplayID,CR.TerDisplayID)					AS TerDisplayID,
				coalesce(DB.TransactionDate,CR.TransactionDate)			AS TransactionDate,
				0														AS ProdID,
				coalesce(DB.LocDisplayID,CR.LocDisplayID)					AS LocDisplayID,
				'727'														AS sales_type,
				0														AS TotalCount,
				SUM(coalesce(DB.Amount,0)) - SUM(coalesce(CR.Amount,0))		AS Amount,
			    '' as sub_prod
		FROM
		(
			SELECT A.TerDisplayID			AS TerDisplayID,
					A.TransactionDate		AS TransactionDate,
					A.LocDisplayID			AS LocDisplayID,
					SUM(coalesce(TotalCount,0))			AS TotalCount,
					SUM(coalesce(Amount,0))				AS Amount
			FROM  ubt_temp_tmpterlocprdsalesamt A where sales_type in
			(
				'2', --cancel amount
				'3',	--validation amount
				'7',	--commission amount
				--(7)6, -- GST tax amount
				'108', -- NETS transaction
				'110', -- Flashpay transaction
				'112', -- Cashcard transaction
				--(4)-1 -- refund and rebate
				'4', --rebate
				'61' --refund--(5)5 --refund
			 )
			--  AND (prodid :: int) != 31  --(15) exclude offline orders in the net amount due calculation
			AND (prodid :: int) not in (31,99)
			GROUP BY A.TerDisplayID, A.TransactionDate, A.LocDisplayID
		) CR
		FULL OUTER JOIN
		(
			SELECT A.TerDisplayID			AS TerDisplayID,
					A.TransactionDate		AS TransactionDate,
					A.LocDisplayID			AS LocDisplayID,
					SUM(coalesce(TotalCount,0))			AS TotalCount,
					SUM(coalesce(Amount,0))				AS Amount
			FROM  ubt_temp_tmpterlocprdsalesamt A where sales_type ='1'	--collection
			-- AND (prodid :: int)!= 31
			AND (prodid :: int) not in (31,99)
			GROUP BY A.TerDisplayID, A.TransactionDate, A.LocDisplayID
		) DB ON DB.LocDisplayID=CR.LocDisplayID AND DB.TransactionDate=CR.TransactionDate AND DB.TerDisplayID=CR.TerDisplayID
		GROUP BY DB.TerDisplayID, DB.TransactionDate, DB.LocDisplayID,CR.TerDisplayID,CR.TransactionDate, CR.LocDisplayID
;

-- for both types of toto
INSERT INTO ubt_temp_tmpterlocprdsalesamt
-- sum totomatch and t649
select terdisplayid, transactiondate, prodid, locdisplayid, sales_type, sum(totalcount), sum(amount), ''
from ubt_temp_tmpterlocprdsalesamt td
where sub_prod in ('TOTO MATCH', 'TOTO') and Sales_type not in ('40', '119', '105')
group by terdisplayid, transactiondate, prodid, locdisplayid, sales_type
union all

-- config - clone totomatch config for product Toto (t649 and totomatch)
select terdisplayid, transactiondate, prodid, locdisplayid, sales_type, totalcount, amount, ''
from ubt_temp_tmpterlocprdsalesamt td
where sub_prod in ('TOTO MATCH') and Sales_type in ('40', '119', '105');


	UPDATE ubt_temp_tmpterlocprdsalesamt td
	SET ProdID = CASE
					WHEN td.ProdID::int = 3 AND td.sub_prod = 'TOTO' THEN 71
					WHEN td.ProdID::int = 3 AND td.sub_prod = 'TOTO MATCH' THEN 66
					ELSE p.SAPProdID
				END
	FROM public.ztubt_sap_product p
	WHERE td.ProdID::int = p.prodid;

return query
SELECT DISTINCT
			coalesce(TerDisplayID, '')
			,(TransactionDate :: Date)
			,coalesce(ProdID :: int, 0)
			,coalesce(LocDisplayID, '')
			,coalesce(Sales_type, '')
			,TotalCount
			,Amount
		FROM ubt_temp_tmpterlocprdsalesamt --(12) ubt_temp_FinalTempData--(1)FROM ubt_temp_TmpTerLocPrdSalesAmt
		WHERE (TotalCount!=0 or Amount!=0)
		AND (TransactionDate :: DATE) = vbusinessdate
;
	drop table if exists ubt_temp_tmpterlocprdsalesamt;
	drop table if exists ubt_temp_tmpticketbywageandsales;
	drop table if exists ubt_temp_resultcashlessinterminal;
	drop table if exists ubt_temp_grouptoto;
	drop table if exists ubt_temp_itoto;
	drop table if exists ubt_temp_location;
	drop table if exists ubt_temp_product;
	drop table if exists ubt_temp_salesfactorconfig;
	drop table if exists ubt_temp_salesgrouptoto;
	drop table if exists ubt_temp_salestoto;
	drop table if exists ubt_temp_terminal;
	drop table if exists ubt_temp_transamountdetaildata;
	drop table if exists ubt_temp_cancelledbetticketstate;
	-- [2022-04-01] wf: drop temp table created for Sweep Consignee and Sweep Retailer Cancellation
	drop table if exists ubt_temp_sales_scandsr;


end
;
$$;


ALTER FUNCTION public.sp_ubt_getterminalinvoice(businessdate date) OWNER TO sp_postgres;
