CREATE FUNCTION public.sp_ubt_getlocationinvoice(businessdate date) RETURNS TABLE(locdisplayid character varying, agentinvoice_invid character varying, agentinvoice_finidn character varying, agentinvoice_productid integer, sales_type character varying, totalcount integer, amount numeric)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column

	declare
	vbusinessdate date;
	vinputdate date;
	vstartperiod date;
	vendperiod date;
	vfromdateIGT timestamp;
	vtodateIGT timestamp;
	vfromdatetimeIGT_UTC timestamp;
	vtodatetimeIGT_UTC timestamp;
	vfromdatetimeOB_UTC timestamp;
	vtodatetimeOB_UTC timestamp;
	vfromdatetimeBMCS_UTC timestamp;
	vtodatetimeBMCS_UTC timestamp;

	vinvoiceperiodid varchar(100);
	vactualdate date;

	v_totomatchBettypes text[] := (SELECT array(SELECT bettypeid FROM public.sp_ubt_gettotomatchbettypes()));

	BEGIN
	select businessdate into  vbusinessdate ;
	select  (vbusinessdate :: date) +  interval '1 DAY' into  vinputdate ;

	select (vbusinessdate :: date) + interval '1 DAY' into  vactualdate ;


select case when to_char(vinputdate:: date ,'dy' ) in ('mon','tue','wed','thu')
then date_trunc('week',vinputdate  ::date )- interval '3 day'
else date_trunc('week',vinputdate ::date ) end ,

case when to_char(vinputdate  ::date ,'dy' ) in ('mon','tue','wed','thu')
then date_trunc('week',vinputdate  ::date)- interval '1 day'
else date_trunc('week',vinputdate ::date )+ interval '3 day' end

into  vstartperiod,vendperiod;

select fromdatetimeigt,todatetimeigt,utcfromdatetimeigt,utctodatetimeigt,
utcfromdatetimeob,utctodatetimeob,utcfromdatetimebmcs, utctodatetimebmcs

into
	vfromdateIGT ,
	vtodateIGT ,
	vfromdatetimeIGT_UTC ,
	vtodatetimeIGT_UTC ,
	vfromdatetimeOB_UTC ,
	vtodatetimeOB_UTC ,
	vfromdatetimeBMCS_UTC,
	vtodatetimeBMCS_UTC
from public.sp_ubt_getcommonubtdates(vstartperiod,vendperiod);

select cast(invoiceperiodid as varchar)
into  vinvoiceperiodid
from public.ztubt_invoiceperiod zi  where startdate=vstartperiod and enddate=vendperiod;



create temp table if not exists ubt_temp_terminal
(
terdisplayid varchar(100),
locid int4
);

create temp table if not exists ubt_temp_location
(
locid int4,
locdisplayid varchar(100),
sweepindicator int4,
loctypeid int4,
isgst bool,
accountnumber varchar(100),
branchid varchar(100),
bankid varchar(100),
locname varchar(100),
isibg bool,
retid int4,
chid int4
);

create temp table if not exists ubt_temp_product
(
ProdID int4,
ProdName varchar(100)
);




insert into ubt_temp_terminal
	select ter.terdisplayid, ter.locid
	from public.ztubt_terminal ter;

	insert into ubt_temp_location
	select loc.locid, loc.locdisplayid, loc.sweepindicator,loc.loctypeid,loc.isgst,loc.accountnumber,
	loc.branchid,loc.bankid,loc.locname,loc.isibg,loc.retid,loc.chid
	from public.ztubt_location loc;

	insert into ubt_temp_product
	select pd.prodid, case   pd.prodid when 5 then 'SPORTS' else trim(pd.prodname) end prodname
	from public.ztubt_product pd
	where pd.prodid != 6 ;--(15) remove sports live so that all sports refer to productid = 5 only

	insert into ubt_temp_product (prodid, prodname) values (3, 'TOTO MATCH');

--	UPDATE public.ztubt_product SET prodname = 'SPORTS' WHERE ProdID = 5 ;--(15) rename "Sports %" to be "Sports" only

	create temp table if not exists ubt_temp_chain
	(
	chid int4,
	chdisplayid varchar(100),
	chname varchar(100)
	);

create temp table if not exists ubt_temp_cancelledbeticket
	(
	ticketserialnumber varchar(500),
	CancelledDate timestamp,
	CancelledAmout numeric(22,2),
	ProdID int4,
	TerDisplayID varchar(100)
	);

	insert into ubt_temp_chain

	select zc.chid, zc.chdisplayid, zc.chname	from public.ztubt_chain zc ;

	insert into ubt_temp_cancelledbeticket
	select cb.ticketserialnumber,cb.cancelleddate,
	cb.cancelledamout,cb.prodid,cb.terdisplayid
	from public.ztubt_cancelledbetticket cb
	inner join public.ztubt_cancelledbetticketlifecyclestate cbt
	on cb.tranheaderid = cbt.tranheaderid and cbt.betstatetypeid = 'CB06'
	where cb.cancelleddate between vfromdateigt and vtodateigt ;

create index  idx_ubt_cancelledbetticket_tck_prdid_candt on ubt_temp_cancelledbeticket (ticketserialnumber,Prodid, CancelledDate)
;


	create temp table if not exists ubt_temp_getamounttrans --(12)
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

	INSERT INTO ubt_temp_getamounttrans
	select * from public.sp_ubt_getamounttransaction(vstartperiod,vendperiod);

create index  idx_ubt_getamounttrans_ticket on ubt_temp_getamounttrans (ticketserialnumber)
;



create temp table if not exists ubt_temp_resultcashlesslocation
	(
terdisplayid varchar(200),
productname varchar(100),
amount numeric(22, 11),
ct int4
	);

	insert into ubt_temp_resultcashlesslocation
	select pd.terdisplayid,
	case pd.PaymentTypeID  when 'NC' then 'NETS'
	when 	'NN' then 'NETS'
	 when 'NCC' then 'CASHCARD'
	 when 'NFP' then 'Flashpay' end,
	 coalesce(sum(coalesce(pd.paidamount,0)),0),
	 count(distinct pd.paymentdetailid)
from  public.ztubt_paymentdetail  pd
where pd.paymenttypeid IN ('NC','NN','NCC','NFP')--(7)--nets
and pd.createddate >= vfromdatetimeIGT_UTC 	and pd.createddate < vtodatetimeIGT_UTC --(30)

group by pd.terdisplayid,pd.PaymentTypeID ;

create index  idx_ubt_cashless_terdsplid on ubt_temp_resultcashlesslocation (terdisplayid)
;

	create temp table if not exists ubt_temp_iTotolocation
	(
TicketSerialNumber VARCHAR(200),
iTotoIndicator bool,
GroupUnitSequence int4,
	bettype int4
	);

	insert into ubt_temp_itotolocation
select distinct ticketserialnumber, itotoindicator, groupunitsequence,
	case
        when pbt.bettypeid != ALL(v_totomatchBettypes) THEN 1 --TOTO
        else 2 --totomatch
    end AS bettype
from  public.ztubt_placedbettransactionheader  pb
inner join public.ztubt_toto_placedbettransactionlineitem  pbt on pb.tranheaderid = pbt.tranheaderid
where (groupunitsequence = 1) or --group toto
 (itotoindicator = true) --itoto
and (pb.createddate between vfromdatetimeigt_utc   and vtodatetimeigt_utc )
;
create index  idx_ubt_itotoloc_ticket on ubt_temp_iTotolocation (ticketserialnumber)
;

	create temp table if not exists ubt_temp_groupTotolocation
	(
ticketserialnumber varchar(200),
grouphostid varchar(50),
groupunitsequence int4,
	bettype int4
	);

insert into ubt_temp_grouptotolocation
select distinct ticketserialnumber, grouphostid, groupunitsequence,
	case
        when pbt.bettypeid != ALL(v_totomatchBettypes) THEN 1 --TOTO
        else 2 --totomatch
    end AS bettype
from  public.ztubt_placedbettransactionheader  pb
inner join public.ztubt_toto_placedbettransactionlineitem  pbt on pb.tranheaderid = pbt.tranheaderid
where groupunitsequence is not null --group toto
and (pb.createddate between vfromdatetimeigt_utc   and vtodatetimeigt_utc );

create index  idx_ubt_grptotoloc_ticket on ubt_temp_groupTotolocation (ticketserialnumber)
;
-----------------------------------------------------------------

create temp table ubt_temp_pbth as
select terdisplayid,ticketserialnumber,createddate
from public.ztubt_placedbettransactionheader pbt
where
	pbt.prodid = 3
and (pbt.createddate between vfromdatetimeigt_utc   and vtodatetimeigt_utc )
;
create temp table if not exists ubt_temp_salesgroupTotolocation
	(
locid int4,
chid int4,
prodid int4,
bettype int4,
wagertotal int4,--(8)
canceltotal int4,--(8)
wageramount numeric(32, 11), --wager of each product
salesamount numeric(32, 11)  --sales of each product (wager * sales factor)
	);


insert into ubt_temp_salesgroupTotolocation (locid, chid, prodid, bettype, wagertotal, canceltotal, wageramount, salesamount)
	select loc.locid, loc.chid, fin.prodid, fin.bettype,
	sum(fin.wagertotal),
	 sum(fin.canceltotal),
	 sum(fin.wageramount),
(sum(fin.salesamount) * 100 )--(10)convert back from dollars to cents
from
(
	select  (pbth.createddate :: date) actualdate, --gb.actualdate, --(10)
	pbth.terdisplayid,
	--lc.chid,
	3 as prodid,
	fin.bettype as bettype,
	count(distinct fin.ticketserialnumber)	as wagertotal, --(1)count(1) as totalcount,
	count(distinct can.ticketserialnumber)	as canceltotal,--(8)
	sum(coalesce(fin.wager,0))	as wageramount, --(1)sum(coalesce(pbtl.betpriceamount,0))	 as totalamount
	round (trunc(sum(coalesce(fin.sales,0)) / 100,2), 2)	as salesamount
--from public.ztubt_placedbettransactionheader  pbth
	from ubt_temp_pbth  pbth
	inner join
	(
select gat.ticketserialnumber, gat.wager,
case when can.ticketserialnumber is null then gat.sales else 0 end as sales,
itoto.bettype
from ubt_temp_iTotolocation itoto
inner join ubt_temp_getamounttrans gat on itoto.ticketserialnumber = gat.ticketserialnumber
left join --(9)
(

	select cb.ticketserialnumber from ubt_temp_cancelledbeticket cb
)can on gat.ticketserialnumber = can.ticketserialnumber

where itoto.groupunitsequence = 1 --group toto
	)fin on pbth.ticketserialnumber = fin.ticketserialnumber

	left join
	(
select gat.ticketserialnumber, gat.wager, gat.sales, itoto.bettype
from ubt_temp_iTotolocation itoto
inner join ubt_temp_getamounttrans gat on itoto.ticketserialnumber = gat.ticketserialnumber

inner join ubt_temp_cancelledbeticket  cb on gat.ticketserialnumber =  cb.ticketserialnumber
where itoto.groupunitsequence = 1 --group toto
	)can on pbth.ticketserialnumber = can.ticketserialnumber

/*	where
	pbth.prodid = 3
and (pbth.createddate between vfromdatetimeigt_utc   and vtodatetimeigt_utc )
*/

	group by pbth.terdisplayid, (pbth.createddate :: date), fin.bettype--gb.actualdate --(10)
)fin
inner join ubt_temp_terminal ter on fin.terdisplayid = ter.terdisplayid
inner join ubt_temp_location loc on ter.locid = loc.locid
group by loc.locid, loc.chid, fin.prodid, fin.bettype;

create index  idx_ubt_salegrptotoloc_locid_prodid on ubt_temp_salesgroupTotolocation (locid,prodid)
;

	create temp table if not exists ubt_temp_salesTotolocation
	(
locid int4,
chid int4,
prodid int4,
bettype int4,
wagertotal int4,--(8)
canceltotal int4,--(8)
wageramount numeric(32, 11), --wager of each product
salesamount numeric(32, 11)  --sales of each product (wager * sales factor)
	)
;


	INSERT INTO ubt_temp_salesTotolocation(LocID, CHID, ProdID, BetType, WagerTotal, CancelTotal, WagerAmount, SalesAmount)
	select loc.locid, loc.chid, fin.prodid, fin.bettype, sum(fin.wagertotal), sum(fin.canceltotal), sum(fin.wageramount),
(sum(fin.salesamount) * 100) --(10)convert back from dollars to cents
from
(
	select  (pbth.createddate :: date) actualdate,
	pbth.terdisplayid as terdisplayid,
	3 as prodid,
	1 as bettype,
	count(distinct fin.ticketserialnumber)	as wagertotal,
	count(distinct can.ticketserialnumber)	as canceltotal,--(8)
	sum(coalesce(fin.wager,0))	as wageramount,
	round (trunc(sum(coalesce(fin.sales,0)) / 100,2), 2)	as salesamount
	--from	public.ztubt_placedbettransactionheader pbth
	from ubt_temp_pbth pbth
	inner join
	(
select gat.ticketserialnumber, gat.wager,
case when can.ticketserialnumber is null then gat.sales else 0 end as sales
from ubt_temp_iTotolocation itoto
inner join ubt_temp_getamounttrans gat on itoto.ticketserialnumber = gat.ticketserialnumber
left join --(9)
(

	select cb.ticketserialnumber from ubt_temp_cancelledbeticket cb
)can on gat.ticketserialnumber = can.ticketserialnumber
where itoto.itotoindicator = true --itoto
	)fin on pbth.ticketserialnumber = fin.ticketserialnumber
	left join --(8)
	(
select gat.ticketserialnumber, gat.wager, gat.sales
from ubt_temp_iTotolocation itoto
inner join ubt_temp_getamounttrans gat on itoto.ticketserialnumber = gat.ticketserialnumber
inner join ubt_temp_cancelledbeticket cb on gat.ticketserialnumber = cb.ticketserialnumber
where itoto.itotoindicator = true --itoto
	)can on pbth.ticketserialnumber = can.ticketserialnumber

/*	where pbth.prodid = 3
and (pbth.createddate between vfromdatetimeigt_utc   and vtodatetimeigt_utc )
*/

	group by pbth.terdisplayid, (pbth.createddate :: date)
)fin
inner join ubt_temp_terminal ter on fin.terdisplayid = ter.terdisplayid
inner join ubt_temp_location loc on ter.locid = loc.locid
group by loc.locid, loc.chid, fin.prodid, fin.bettype
	;

create index  idx_ubt_saletotoloc_locid_prodid on ubt_temp_salesTotolocation (locid,prodid)
;

--=================================
--FILL IN TRANS AMOUNT DETAIL DATA
--=================================

create temp table if not exists ubt_temp_transamountdetaildata
	(
terdisplayid varchar(200),
productname varchar(100),
amount numeric(32,13),
ct int4,
flag char(3),
transtype char(3),
fromdate date,
todate date
	);

	Insert Into ubt_temp_transamountdetaildata
select
terdisplayid,	trim(prodname),(amount * 100)	,ticketcount,	flag,transtype
from public.sp_ubt_gettransamountdetails(vstartperiod,vendperiod);

	/*UPDATE ubt_temp_transamountdetaildata
	SET amount = amount * 100;*/

create index  idx_ubt_transamt_terdisplayid on ubt_temp_transamountdetaildata (terdisplayid)
;
create index  idx_ubt_transamt_productname on ubt_temp_transamountdetaildata (productname)
;
create index  idx_ubt_transamt_flag on ubt_temp_transamountdetaildata (flag)
;
create index  idx_ubt_transamt_transtype on ubt_temp_transamountdetaildata (transtype)
;
--------------------------------------------------------------------------------------------
	create temp table if not exists ubt_temp_datalocationinvoice
	(
locdisplayid varchar(200) ,
agentinvoice_invid varchar(100) ,
agentinvoice_finidn varchar(200) ,
agentinvoice_productid int4 ,
sales_type varchar(100) ,
totalcount int4 ,
amount numeric(32, 11),
sub_prod varchar(100)
	)
	;

insert into ubt_temp_datalocationinvoice (locdisplayid, agentinvoice_invid, agentinvoice_finidn, agentinvoice_productid, sales_type, totalcount, amount, sub_prod)

select	lc.locdisplayid as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid as agentinvoice_finidn ,
pr.prodid	as agentinvoice_productid,
'1'	as sales_type,
sum(coalesce (a.ct,0))	as totalcount,
sum(coalesce(a.amount,0)) as amount,
a.productname as sub_prod
from ubt_temp_transamountdetaildata a
inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
inner join ubt_temp_location lc on ter.locid = lc.locid
inner join ubt_temp_product pr on a.productname = pr.prodname
left join ubt_temp_chain c on lc.chid = c.chid
where flag='COL' and transtype != 'OO' and pr.prodid in (1,2,3,4,5)
group by lc.locdisplayid,c.chdisplayid,pr.prodid, a.productname

union all

select	lc.locdisplayid as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid as agentinvoice_finidn ,
case when a.productname <> 'Gate Admission' then 31
	 when a.productname = 'Gate Admission' then 99 end as agentinvoice_productid,
'1'	as sales_type,
sum(coalesce (a.ct,0))	as totalcount,
sum(coalesce (a.amount,0)) 	as amount,
'' as sub_prod
from ubt_temp_transamountdetaildata a
inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
inner join ubt_temp_location lc on ter.locid = lc.locid
left join ubt_temp_chain c on lc.chid = c.chid
where flag='COL' and transtype = 'OO'
group by lc.locdisplayid,c.chdisplayid,case when a.productname <> 'Gate Admission' then 31
											when a.productname = 'Gate Admission' then 99 end

----------2	AT_CANCELAMT
----------3 AT_VALIDAMT
----------4 AT_VALIDAMT

union all

select	lc.locdisplayid as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid as agentinvoice_finidn ,
pr.prodid	as agentinvoice_productid,
case a.Flag when 'CAN' then '2'
   when 'PAY' then '3'
   when 'RBT' then '4' end as sales_type,
sum(coalesce (a.ct,0))	as totalcount,
sum(coalesce (a.amount,0)) * -1	as amount,
a.productname as sub_prod
from ubt_temp_transamountdetaildata a
inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
inner join ubt_temp_location lc on ter.locid = lc.locid
inner join ubt_temp_product pr on a.productname = pr.prodname
left join ubt_temp_chain c on lc.chid = c.chid
where a.flag in ('CAN','PAY','RBT') and pr.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)ltrim(rtrim(productname)) in ('4d lottery','singapore sweep','toto')
group by lc.locdisplayid,c.chdisplayid,pr.prodid,a.flag, a.productname

union all

-----61	Refund Amount--(5)(6)

select	lc.locdisplayid as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid as agentinvoice_finidn ,
pr.prodid	as agentinvoice_productid,
'61'	as sales_type,
sum(coalesce (a.ct,0))	as totalcount,
sum(coalesce(a.amount,0)) * -1	as amount,
a.productname as sub_prod
from ubt_temp_transamountdetaildata a
inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
inner join ubt_temp_location lc on ter.locid = lc.locid
inner join ubt_temp_product pr on a.productname = pr.prodname
left join ubt_temp_chain c on lc.chid = c.chid
where flag='RFD' and transtype != 'OO' and pr.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)ltrim(rtrim(productname)) in ('4d lottery','singapore sweep','toto')
group by lc.locdisplayid,c.chdisplayid,pr.prodid, a.productname

--offline orders - refund

union all

select	lc.locdisplayid as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid as agentinvoice_finidn ,
31	as agentinvoice_productid,
'61'	as sales_type,
sum(coalesce(a.ct,0))	as totalcount,
(sum(coalesce(a.amount,0)) * -1) as amount,
'' as sub_prod
from ubt_temp_transamountdetaildata a
inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
inner join ubt_temp_location lc on ter.locid = lc.locid
left join ubt_temp_chain c on lc.chid = c.chid
where flag='RFD' and transtype = 'OO'
group by lc.locdisplayid,c.chdisplayid


union all
-----6	AT_TAXAMT
-----7	AT_COMMAMT

select	lc.locdisplayid as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid as agentinvoice_finidn ,
pr.prodid	as agentinvoice_productid,
case
	a.flag
		when 'GST' then '6'
		when 'SAL'then '7' end as sales_type,
sum(coalesce(a.ct,0))	as totalcount,
(sum(coalesce(a.amount,0)) * -1)as amount,
a.productname as sub_prod
from ubt_temp_transamountdetaildata a
inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
inner join ubt_temp_location lc on ter.locid = lc.locid
inner join ubt_temp_product pr on a.productname = pr.prodname
left join ubt_temp_chain c on lc.chid = c.chid
where a.flag in ('GST', 'SAL') and pr.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)ltrim(rtrim(productname)) in ('4d lottery','singapore sweep','toto')
group by lc.locdisplayid,c.chdisplayid,pr.prodid,a.flag, a.productname

union all
------16. AT_FRACVALAMT

select	lc.locdisplayid as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid	as agentinvoice_finidn,
3 as agentinvoice_productid,
'16'as sales_type,
count(distinct vb.tranheaderid)	as totalcount, --(16) --(1)count(1)as totalcount,
sum(coalesce(vb.winningamount,0))as totalamount,--(1) sum(coalesce(vb.amount,0))as totalamount
case when ph.bettypeid != ALL(v_totomatchBettypes) then 'TOTO'
	else 'TOTO MATCH' end as sub_prod
from
(
	select vb.tranheaderid,vb.winningamount,vb.terdisplayid,vb.createdvalidationdate
	from public.ztubt_validatedbetticket  vb --(16)
	inner join  public.ztubt_validatedbetticketlifecyclestate  vbt
on vb.tranheaderid = vbt.tranheaderid and vbt.betstatetypeid = 'VB06'
	where vb.winningamount != 0 and vb.winningamount is not null --(8)
	and vb.createdvalidationdate >= vfromdatetimeIGT_UTC and vb.createdvalidationdate < vtodatetimeIGT_UTC --(16)

)vb

inner join
(
	select distinct tranheaderid, grouphostid, groupunitsequence, bettypeid
	from  public.ztubt_toto_placedbettransactionlineitem ztp
) ph on vb.tranheaderid = ph.tranheaderid --(16)

inner join ubt_temp_terminal ter on vb.terdisplayid = ter.terdisplayid
inner join ubt_temp_location lc on ter.locid = lc.locid
left join ubt_temp_chain c on lc.chid = c.chid

where ph.groupunitsequence is not null --(16)
group by lc.locdisplayid,c.chdisplayid,
case when ph.bettypeid != ALL(v_totomatchBettypes) then 'TOTO'
	else 'TOTO MATCH' end

----52. House syndicate sales amount

union all
--TOTO
select	l.locdisplayid	as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid	as agentinvoice_finidn,
s.prodid as agentinvoice_productid,
'52'as sales_type,
sum(coalesce (s.wagertotal,0))	as totalcount,
sum(coalesce (s.wageramount,0))	as totalamount,--(1)(coalesce(s.amount,0))	as totalamount
'TOTO' as sub_prod
from ubt_temp_salestotolocation s
inner join ubt_temp_location l on s.locid = l.locid
left join ubt_temp_chain c on l.chid = c.chid
group by l.locdisplayid, c.chdisplayid, s.prodid


-------25. house syndicate sales amount by sales factor [itoto sales by sales factor

union all
--TOTO
select	l.locdisplayid	as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid	as agentinvoice_finidn,
3 as agentinvoice_productid,
'25' as sales_type,
sum(coalesce (s.wagertotal,0) - coalesce(s.canceltotal,0))	as totalcount,  --(8)
sum(coalesce(s.salesamount,0)) as totalamount ,
'TOTO' as sub_prod
from ubt_temp_salestotolocation s
inner join ubt_temp_location l on s.locid = l.locid
left join ubt_temp_chain c on l.chid = c.chid
group by l.locdisplayid, c.chdisplayid

-----40. GST TAX rate
union all
select lc.locdisplayid	as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
lc.chdisplayid	as agentinvoice_finidn,
prd.prodid as agentinvoice_productid,
'40'as sales_type,
0 as totalcount,
gst.gstrate,
prd.prodname as sub_prod
from
(
	select lc.locdisplayid, ch.chdisplayid
	from ubt_temp_location lc
	left join ubt_temp_chain ch on lc.chid = ch.chid
	where lc.isgst = True
)lc
cross join
(
	select prodid, prodname from ubt_temp_product where prodid in (2,3,4) --lottery / es
)prd
cross join
(
	select  gstrate from public.ztubt_gstconfig	a
	where
	(vactualdate between effectivefrom and enddate) or (vactualdate >=effectivefrom and enddate is null)
	limit 1
)gst

union all
--------59. House syndicate cancel amount - [iTOTO cancellations
--TOTO
select	lc.locdisplayid as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid	as agentinvoice_finidn,
3 as agentinvoice_productid,
'59'as sales_type,
count(distinct cb.ticketserialnumber)	as totalcount, --(1)count(1)	as totalcount,
sum(coalesce(cb.cancelledamout,0))	as totalamount,
'TOTO' as sub_prod
from ubt_temp_cancelledbeticket cb
inner join ubt_temp_iTotolocation itoto on cb.ticketserialnumber = itoto.ticketserialnumber and itoto.itotoindicator = true
inner join ubt_temp_terminal ter on cb.terdisplayid = ter.terdisplayid
inner join ubt_temp_location lc on ter.locid = lc.locid
left join ubt_temp_chain c on lc.chid = c.chid
where cb.prodid = 3
	and cb.cancelleddate >= vfromdateIGT and cb.cancelleddate < vtodateIGT --(30)

group by lc.locdisplayid,c.chdisplayid

union all
--------60. Player syndicate sales amount by sales factor
--TOTO, totomatch
select l.locdisplayid	as locdisplayid ,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid	as agentinvoice_finidn,
s.prodid	as agentinvoice_productid,
'60'	as sales_type,
sum(coalesce(s.wagertotal,0) - coalesce(s.canceltotal,0))as totalcount, --(8)
sum(coalesce(s.salesamount,0)) as totalamount,
case when s.bettype = 1 then 'TOTO'
	else 'TOTO MATCH' end as sub_prod
from ubt_temp_salesgrouptotolocation s
inner join ubt_temp_location l on s.locid = l.locid
left join ubt_temp_chain c on l.chid = c.chid
group by l.locdisplayid, c.chdisplayid,s.prodid,
	case when s.bettype = 1 then 'TOTO'
		else 'TOTO MATCH' end

union all

---------69. player syndicate sales amount

--TOTO, totomatch
select	l.locdisplayid as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid	as agentinvoice_finidn,
3 as agentinvoice_productid,
'69'as sales_type,
sum(coalesce (s.wagertotal,0))	as totalcount,
sum(coalesce (s.wageramount,0)) as totalamount,
case when s.bettype = 1 then 'TOTO'
	else 'TOTO MATCH' end as sub_prod
from ubt_temp_salesgrouptotolocation s
inner join ubt_temp_location l on s.locid = l.locid
left join ubt_temp_chain c on l.chid = c.chid
group by l.locdisplayid, c.chdisplayid,
	case when s.bettype = 1 then 'TOTO'
		else 'TOTO MATCH' end

union all

-----------70 [	 GROUP TOTO Cancel Amount
--TOTO, totomatch
select      lc.locdisplayid	as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid	as agentinvoice_finidn,
3 as agentinvoice_productid,
'70'as sales_type,
count(distinct gt.grouphostid)	as totalcount,
sum(coalesce(cbt.cancelledamout,0))	as totalamount,
case when gt.bettype = 1 then 'TOTO'
	else 'TOTO MATCH' end as sub_prod
from ubt_temp_cancelledbeticket cbt
inner join ubt_temp_grouptotolocation gt on cbt.ticketserialnumber = gt.ticketserialnumber
inner join ubt_temp_terminal ter on cbt.terdisplayid = ter.terdisplayid
inner join ubt_temp_location lc on ter.locid = lc.locid
left join ubt_temp_chain c on lc.chid = c.chid
where cbt.prodid = 3
and cbt.cancelleddate >= vfromdateIGT and cbt.cancelleddate < vtodateIGT
group by lc.locdisplayid,c.chdisplayid,
	case when gt.bettype = 1 then 'TOTO'
		else 'TOTO MATCH' end

union all
-----------------105 Sales Commision Rate
select lc.locdisplayid	as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
lc.chdisplayid	as agentinvoice_finidn,
sal.prodid as agentinvoice_productid,
'105' as sales_type,
0 as totalcount,--(1)count(1) as totalcount,
sal.salescommission,
'' as sub_prod
from
(
	select lc.locdisplayid, ch.chdisplayid from ubt_temp_location lc
	left join ubt_temp_chain ch on lc.chid = ch.chid
	where lc.isgst =true
)lc
cross join
(
	select distinct salescommission, prodid from public.ztubt_salescomconfig  a
	where commissiontype=1 and  isdeleted = false and prodid in (2,4) --lottery / es
)sal

union all
--TOTO, totomatch
select lc.locdisplayid	as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
lc.chdisplayid	as agentinvoice_finidn,
sal.prodid as agentinvoice_productid,
'105' as sales_type,
0 as totalcount,--(1)count(1) as totalcount,
sal.salescommission,
sp.sub_prod
from
(
	select lc.locdisplayid, ch.chdisplayid from ubt_temp_location lc
	left join ubt_temp_chain ch on lc.chid = ch.chid
	where lc.isgst =true
)lc
cross join
(
	select distinct salescommission, prodid from public.ztubt_salescomconfig  a
	where commissiontype=1 and  isdeleted = false and prodid in (3)
)sal
cross join (select 'TOTO' as sub_prod union all select 'TOTO MATCH' as sub_prod) as sp

union all

--------108 AT_ON_EZLINK--  NETS
----110	AT_ON_ATM--- Flashpay
---112	AT_ON_CASHCARD

select	lc.locdisplayid		as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid		as agentinvoice_finidn ,
0	as agentinvoice_productid,
case a.productname when 'NETS' then '108'
 when 'Flashpay' then '110' when 'CASHCARD' then '112' end as sales_type,
sum(coalesce(a.ct,0))	as totalcount,
sum(coalesce(a.amount,0)) as amount,
'' as sub_prod
		from	ubt_temp_location as lc
left join ubt_temp_chain c on lc.chid = c.chid
left join ubt_temp_terminal  as ter on  ter.locid=lc.locid
left join ubt_temp_resultcashlesslocation a on a.terdisplayid = ter.terdisplayid
		where a.productname in ('NETS','Flashpay','CASHCARD')
		group by lc.locdisplayid,c.chdisplayid,a.productname


union all
---------116	AT_NOSPPL_SALAMT 	Yes	Sales multiplied by sales factor

select loc.locdisplayid, vinvoiceperiodid as agentinvoice_invid, ch.chdisplayid, fin.agentinvoice_productid,
'116'	as sales_type, sum(fin.totalcount), (sum(fin.totalamount) * 100),
fin.sub_prod as sub_prod
		from
		(
			select	a.actualdate,
	a.terdisplayid	as terdisplayid,
	a.prodid	as agentinvoice_productid,
	sum(coalesce(a.total,0) - coalesce(can.total,0))	as totalcount, --(8)
	round(trunc(sum(coalesce(a.amount,0)) / 100,2), 2)	as totalamount ,
	a.sub_prod
			from
			(
select cast(pb.createddate as date) actualdate, a.ticketserialnumber,
sum(a.sales + a.secondsales) as amount, count(distinct a.ticketserialnumber) as total, pb.terdisplayid,
	case pb.prodid
		when 6 then 5 else pb.prodid
		end as prodid, --(15)
		case when pb.prodid <> 3 then ''
			when pb.prodid = 3 and EXISTS (
				SELECT 1
				FROM ztubt_toto_placedbettransactionlineitem pbtlin
				WHERE pb.TranHeaderID = pbtlin.TranHeaderID
					AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
			) then 'TOTO MATCH'
		else 'TOTO' end as sub_prod
from ubt_temp_getamounttrans a
inner join public.ztubt_placedbettransactionheader  pb on a.ticketserialnumber=pb.ticketserialnumber

where
pb.iscancelled = false and (
		(
		pb.prodid in (2,3,4) --lottery / es
		and (pb.createddate between vfromdatetimeIGT_UTC and vtodatetimeIGT_UTC )

		)
		or --(15)
		(
		pb.prodid in (5,6) --sports / ob
		and (pb.createddate between vfromdatetimeOB_UTC and vtodatetimeOB_UTC )
		)
		or
		(
		pb.prodid in (1) --horse racing / bmcs
		and pb.createddate between vfromdatetimeBMCS_UTC and vtodatetimeBMCS_UTC
		)
	)

group by pb.createddate , a.ticketserialnumber, pb.terdisplayid, pb.prodid,
	case when pb.prodid <> 3 then ''
		when pb.prodid = 3 and EXISTS (
			SELECT 1
			FROM ztubt_toto_placedbettransactionlineitem pbtlin
			WHERE pb.TranHeaderID = pbtlin.TranHeaderID
				AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
		) then 'TOTO MATCH'
		else 'TOTO' end
			)a

			left join --(8)
			(
select a.ticketserialnumber,0 as amount, count(distinct a.ticketserialnumber) as total, pb.terdisplayid,
	pb.prodid,
	case when pb.prodid <> 3 then ''
		when pb.prodid = 3 and EXISTS (
				SELECT 1
				FROM ztubt_toto_placedbettransactionlineitem pbtlin
				WHERE pb.TranHeaderID = pbtlin.TranHeaderID
					AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
			) then 'TOTO MATCH'
		else 'TOTO' end as sub_prod
from ubt_temp_getamounttrans a
inner join ubt_temp_cancelledbeticket cb on a.ticketserialnumber = cb.ticketserialnumber
inner join (select ticketserialnumber,terdisplayid,prodid, TranHeaderID from public.ztubt_placedbettransactionheader)  pb on a.ticketserialnumber = pb.ticketserialnumber
where pb.prodid in (2,3,4) --lottery / es
group by a.ticketserialnumber, pb.terdisplayid, pb.prodid,
	case when pb.prodid <> 3 then ''
		when pb.prodid = 3 and EXISTS (
				SELECT 1
				FROM ztubt_toto_placedbettransactionlineitem pbtlin
				WHERE pb.TranHeaderID = pbtlin.TranHeaderID
					AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
			) then 'TOTO MATCH'
		else 'TOTO' end
			)can

			on a.ticketserialnumber = can.ticketserialnumber and a.terdisplayid = can.terdisplayid and a.prodid = can.prodid and a.sub_prod = can.sub_prod
		group by a.actualdate, a.terdisplayid, a.prodid , a.sub_prod
		)fin

		inner join ubt_temp_terminal ter on fin.terdisplayid = ter.terdisplayid
		inner join ubt_temp_location loc on ter.locid = loc.locid
		left join ubt_temp_chain ch on loc.chid = ch.chid
		group by loc.locdisplayid, ch.chdisplayid, fin.agentinvoice_productid, fin.sub_prod;

/*	update ubt_temp_datalocationinvoice tt
		set totalcount = fin.totalcount
		from (
			select td2.locdisplayid,td2.agentinvoice_finidn,td2.agentinvoice_productid,td2.totalcount
			from  ubt_temp_datalocationinvoice td2 where td2.sales_type = '7'
		)fin
		where  tt.locdisplayid = fin.locdisplayid and tt.agentinvoice_finidn = fin.agentinvoice_finidn
		and tt.sales_type ='116'
	;*/

-- [2022-04-05] wf: Start handling for Sweep Consignee and Sweep Retailer Cancellation

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
--values ('211801001', 4, 'SWEEP','2021-11-18'::date, 99, 100);
select *  from public.sp_ubt_getsweepsalespersrterminal(vstartperiod,vendperiod);

create temp table if not exists ubt_temp_sales_scandsr_loc
	(
	LocDisplayID varchar(200),
	AgentInvoice_invId varchar(100),
	AgentInvoice_finidn varchar(200),
	AgentInvoice_productId int4,
	Sales_type varchar(100),
	TotalCount int,
	Amount decimal(32, 11)
	);


Insert into ubt_temp_sales_scandsr_loc
SELECT	LOC.LocDisplayID	as LocDisplayID,
		vinvoiceperiodid			as AgentInvoice_invId,
		CH.CHDisplayID		as AgentInvoice_finidn ,
		RS.ProdID			as AgentInvoice_productId,
		'116'					aS sales_type,
		SUM(RS.TotalCount),
		SUM(RS.Amount) * 100
FROM ubt_temp_sales_scandsr RS
INNER JOIN ubt_temp_terminal TER ON RS.TerDisplayID = TER.TerDisplayID
INNER JOIN ubt_temp_location LOC ON TER.LocID = LOC.LocID
LEFT JOIN ubt_temp_chain CH ON LOC.CHID = CH.CHID
group by LOC.LocDisplayID,
CH.CHDisplayID,
CH.CHDisplayID,
 RS.ProdID;


DELETE FROM ubt_temp_datalocationinvoice SAL  USING ubt_temp_sales_scandsr_loc RSLoc
WHERE RSLoc.LocDisplayID = SAL.LocDisplayID AND RSLoc.AgentInvoice_productId = SAL.AgentInvoice_productId
AND SAL.Sales_type = '116';

INSERT INTO ubt_temp_datalocationinvoice(LocDisplayID, AgentInvoice_invId, AgentInvoice_finidn, AgentInvoice_productId, Sales_type, TotalCount, Amount, sub_prod)
	SELECT	RSLoc.LocDisplayID	as LocDisplayID,
			vinvoiceperiodid			as AgentInvoice_invId,
			RSLoc.AgentInvoice_finidn		as AgentInvoice_finidn ,
			RSLoc.AgentInvoice_productId			as AgentInvoice_productId,
			'116'					as sales_type,
			RSLoc.TotalCount,
			RSLoc.Amount,
			'' as sub_prod
	FROM ubt_temp_sales_scandsr_loc RSLoc;

-- [2022-04-05] wf: End handling for Sweep Consignee and Sweep Retailer Cancellation

--------119	AT_SALESFACTOR
CREATE temp TABLE if not exists ubt_temp_salesfactorconfig
	(
		prodid int4,
		salesfactor numeric(13,12)
	);

	INSERT INTO ubt_temp_salesfactorconfig
	select A.ProdID, A.SalesFactor
	FROM public.ztubt_salescomconfig   A
	WHERE CommissionType=1  AND A.IsDeleted = false AND ProdID IN (2,3,4);


	INSERT INTO ubt_temp_datalocationinvoice(LocDisplayID, AgentInvoice_invId,
	AgentInvoice_finidn, AgentInvoice_productId, Sales_type, TotalCount, Amount, sub_prod)
		select loc.locdisplayid, vinvoiceperiodid	as agentinvoice_invid,
		 loc.chdisplayid, sfc.prodid, '119', 0, sfc.salesfactor, ''
		from
		(
			select loc.locdisplayid, ch.chdisplayid
			from ubt_temp_location loc
			inner join ubt_temp_chain ch on loc.chid = ch.chid
		)loc
		cross join
		(
			select sfc.prodid,sfc.salesfactor from ubt_temp_salesfactorconfig sfc where sfc.prodid <> 3
		)sfc

		union all
		--TOTO, totomatch
		select loc.locdisplayid, vinvoiceperiodid	as agentinvoice_invid, loc.chdisplayid, sfc.prodid,
		 '119', 0, sfc.salesfactor, sp.sub_prod
		from
		(
			select loc.locdisplayid, ch.chdisplayid from ubt_temp_location loc
			inner join ubt_temp_chain ch on loc.chid = ch.chid
		)loc
		cross join
		(
			select sfc.prodid,sfc.salesfactor from ubt_temp_salesfactorconfig sfc where sfc.prodid = 3
		)sfc
		cross join (select 'TOTO' as sub_prod union all select 'TOTO MATCH' as sub_prod) as sp

		union all
--------571	ACT_VALID_FUND_AMT
------572	ACT_RECOVERY_AMT

select	lc.locdisplayid		as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid		as agentinvoice_finidn ,
0	as agentinvoice_productid,
case  flag  when 'FUN' then '571'
when 'REC' then '572' end sales_type,
0	as totalcount,
(case when flag='FUN' then sum(coalesce(a.amount,0)) * -1
		when flag='REC' then sum(coalesce(a.amount,0)) end)		as amount,
		'' as sub_prod
		from
		(
			select l.locid, flag, max(a.amount) as amount
			from ubt_temp_transamountdetaildata a
			inner join ubt_temp_terminal t on a.terdisplayid = t.terdisplayid
			inner join ubt_temp_location l on t.locid = l.locid
			where flag in ('FUN','REC') AND ProductName = 'Funding and Recovery'
			and l.loctypeid in (2,4)
			group by l.locid,flag
		)a
		inner join ubt_temp_location lc on a.locid = lc.locid
		left join ubt_temp_chain c on lc.chid = c.chid
		group by lc.locdisplayid,c.chdisplayid,flag

union all

-------727	ACT_TOTAL_NETAMTDUE	Yes	Net Amt Due

select	lc.locdisplayid		as locdisplayid,
vinvoiceperiodid	as agentinvoice_invid,
c.chdisplayid		as agentinvoice_finidn ,
0	as agentinvoice_productid,
--agentinvoice_sales
case  flag  when 'REC' then '-2'
when 'FUN' then '-3' end 	as sales_type,
sum(coalesce(a.ct,0))	as totalcount,
case  flag  when 'REC' then sum(coalesce(a.amount,0))
when 'FUN' then  (sum(coalesce(a.amount,0))* -1) end as amount,
		'' as sub_prod
		from
		(
			select l.locid,flag, max(a.amount) as amount, max(a.ct) as ct
			from ubt_temp_transamountdetaildata a
			inner join ubt_temp_terminal t on a.terdisplayid = t.terdisplayid
			inner join ubt_temp_location l on t.locid = l.locid
			where flag in ('REC','FUN') and productname = 'Online Adjustment'
			group by l.locid,flag
		)a
		inner join ubt_temp_location lc on a.locid = lc.locid
		left join ubt_temp_chain c on lc.chid = c.chid
		group by lc.locdisplayid, c.chdisplayid,flag

	;

		insert into ubt_temp_datalocationinvoice
select	coalesce(db.locdisplayid,cr.locdisplayid)as locdisplayid,
coalesce(db.agentinvoice_invid,cr.agentinvoice_invid)	as agentinvoice_invid,
coalesce(db.agentinvoice_finidn,cr.agentinvoice_finidn)as agentinvoice_finidn ,
0		as agentinvoice_productid,
'727'		as sales_type,
0		as totalcount,
(sum(db.amount) - sum(cr.amount)) as amount,
'' as sub_prod
		from
		(
			select  a.locdisplayid as locdisplayid,
					a.agentinvoice_invid		as agentinvoice_invid,
					a.agentinvoice_finidn		as agentinvoice_finidn,
					sum(coalesce(a.totalcount,0))			as totalcount,
					sum(coalesce(a.amount,0))as amount
			from  ubt_temp_datalocationinvoice a where sales_type in
			(
				'2', 		--cancel amount
				'3',		--validation amount
				'7',		--commission amount
				'6', 		-- gst tax amount
				'108', 	--?nets transaction
				'110', 	--?flashpay transaction
				'112', 	--?cashcard transaction
				'571', 	--funding
				'4', 		--rebate
				'61', 	--refund--(6)5, --refund
				'-3' 		-- crd adj
			)
			-- and agentinvoice_productid != 31 --(15) exclude offline orders in the net amount due calculation
			and agentinvoice_productid not in (31,99)
			group by a.locdisplayid	, a.agentinvoice_invid,a.agentinvoice_finidn
		) cr
		full outer join
		(
			select  a.locdisplayid as locdisplayid,
				a.agentinvoice_invid		as agentinvoice_invid,
				a.agentinvoice_finidn		as agentinvoice_finidn,
				sum(coalesce(a.totalcount,0))as totalcount,
				sum(coalesce(a.amount,0))	as amount
			from  ubt_temp_datalocationinvoice a where sales_type in('1','572','-2')   --1,	--collection	572, --recovery		-2 --dbt adj

			-- and agentinvoice_productid != 31
			and agentinvoice_productid not in (31,99)
			group by a.locdisplayid	, a.agentinvoice_invid,a.agentinvoice_finidn
		) db
		on db.locdisplayid=cr.locdisplayid and db.agentinvoice_invid=cr.agentinvoice_invid
		and db.agentinvoice_finidn=cr.agentinvoice_finidn
		group by db.locdisplayid, db.agentinvoice_invid,db.agentinvoice_finidn,
		cr.locdisplayid, cr.agentinvoice_invid	,cr.agentinvoice_finidn
;

--CLEAR REFUND, REBATE, ADJ, COLLECTION DATA
	DELETE FROM ubt_temp_datalocationinvoice WHERE Sales_type IN('-2', '-3')
	;

-- for both types of toto
INSERT INTO ubt_temp_datalocationinvoice
-- sum totomatch and t649
select LocDisplayID, AgentInvoice_invId,
AgentInvoice_finidn, AgentInvoice_productId, Sales_type,

sum(TotalCount), sum(Amount), ''
from ubt_temp_datalocationinvoice td
where sub_prod in ('TOTO MATCH', 'TOTO') and Sales_type not in ('40', '119', '105')
group by LocDisplayID, AgentInvoice_invId, AgentInvoice_finidn, AgentInvoice_productId, Sales_type

union all

-- config - clone totomatch config for product Toto (t649 and totomatch)
select LocDisplayID, AgentInvoice_invId, AgentInvoice_finidn, AgentInvoice_productId, Sales_type, TotalCount, Amount, ''
from ubt_temp_datalocationinvoice td
where sub_prod in ('TOTO MATCH') and Sales_type in ('40', '119', '105');

UPDATE ubt_temp_datalocationinvoice td--(12)FinalTempDataLocationInvoice--(1)UPDATE TempDataLocationInvoice
		SET agentinvoice_productid = CASE
					WHEN td.agentinvoice_productid::int = 3 AND td.sub_prod = 'TOTO' THEN 71
					WHEN td.agentinvoice_productid::int = 3 AND td.sub_prod = 'TOTO MATCH' THEN 66
					ELSE p.SAPProdID
				END
		from public.ztubt_sap_product  p
		where td.agentinvoice_productid = p.prodid
;


		return query
		SELECT Distinct
			coalesce(LocDisplayID, '') LocDisplayID
			,coalesce(AgentInvoice_invId, '') as AgentInvoice_invId
			,coalesce(AgentInvoice_finidn, '') as AgentInvoice_finidn
			,coalesce(AgentInvoice_productId, 0) as AgentInvoice_productId
			,coalesce(Sales_type, '') as Sales_type
			,TotalCount
			,Amount

		FROM ubt_temp_datalocationinvoice
	WHERE (TotalCount!=0 or Amount!=0) AND (coalesce(AgentInvoice_finidn,'') <> '')
			AND AgentInvoice_invId in(  vinvoiceperiodid ) --(4)
			;

drop table ubt_temp_terminal;
drop table ubt_temp_location;
drop table ubt_temp_chain;
drop table ubt_temp_product;
drop table ubt_temp_cancelledbeticket;
drop table ubt_temp_getamounttrans;
drop table ubt_temp_resultcashlesslocation;
drop table ubt_temp_iTotolocation;
drop table	ubt_temp_groupTotolocation;
drop table  ubt_temp_salesgroupTotolocation;
drop table 	ubt_temp_salesTotolocation;
drop table 	ubt_temp_transamountdetaildata;
drop table	ubt_temp_datalocationinvoice;
drop table 	ubt_temp_salesfactorconfig;
drop table ubt_temp_pbth;
-- [2022-04-01] wf: drop temp table created for Sweep Consignee and Sweep Retailer Cancellation
drop table ubt_temp_sales_scandsr;
drop table ubt_temp_sales_scandsr_loc;
	end;
$$;


ALTER FUNCTION public.sp_ubt_getlocationinvoice(businessdate date) OWNER TO sp_postgres;
