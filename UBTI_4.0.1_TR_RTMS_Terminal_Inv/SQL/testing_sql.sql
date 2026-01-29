


with ubt_temp_terminal as(
    Select Ter.TerDisplayID, Ter.LocID  FROM ztubt_Terminal Ter
),

ubt_temp_product as(
    Select PD.ProdID,
	case PD.prodid when 5 then 'SPORTS' else trim(PD.ProdName) end as ProdName  FROM ztubt_Product PD
	WHERE PD.ProdID != 6
    UNION ALL
    Select 3 as ProdID, 'TOTO MATCH' as ProdName
),

ubt_temp_location as(
    Select Loc.LocID, Loc.LocDisplayID, Loc.SweepIndicator,Loc.LocTypeID,Loc.IsGST,Loc.AccountNumber,
	Loc.BranchID,Loc.Bankid,Loc.LocName,Loc.IsIBG,Loc.RetID,LOC.CHID
	from ztubt_location  Loc
),

ubt_temp_TmpTicketByWageAndSales as(
 select * from  sp_ubt_getamounttransaction('2025-06-25', '2025-06-25')
),

ubt_temp_ResultCashlessInTerminal as(
    select  pd.terdisplayid, 'NETS' as productname
    , coalesce (sum(coalesce (pd.paidamount,0)),0) as amount
    , count(distinct pd.paymentdetailid) as ct
	from ztubt_paymentdetail pd
	where pd.paymenttypeid in ('NC','NN')
	and pd.createddate >= '2025-06-24 22:00:00'
	and pd.createddate < '2025-06-25 22:00:00'
	group by pd.terdisplayid

	union all
	select pd.terdisplayid, 'CASHCARD' as productname, coalesce (sum(coalesce (pd.paidamount,0)),0) as amount, count(distinct pd.paymentdetailid) as ct
	from ztubt_paymentdetail pd
	where pd.paymenttypeid in ('NCC') --cashcard
	and pd.createddate >= '2025-06-24 22:00:00'
	and pd.createddate < '2025-06-25 22:00:00'
	group by pd.terdisplayid

	union all

	select pd.terdisplayid, 'Flashpay' as productname, coalesce (sum(coalesce (pd.paidamount,0)),0) as amount, count(distinct pd.paymentdetailid) as ct
	from ztubt_paymentdetail pd
	where pd.paymenttypeid in ('NFP') --Flashpay
    and pd.createddate >= '2025-06-24 22:00:00'
	and pd.createddate < '2025-06-25 22:00:00'
	group by pd.terdisplayid


),
ubt_temp_iTOTO as(

    select distinct ticketserialnumber, itotoindicator, groupunitsequence,
	case
        when pbt.bettypeid not in ('M AN', 'M 2', 'M 3', 'M 4') THEN 1 --TOTO
        else 2 --totomatch
    end AS bettype
	from ztubt_placedbettransactionheader  pb
	inner join ztubt_toto_placedbettransactionlineitem  pbt on pb.tranheaderid = pbt.tranheaderid
	where ((groupunitsequence = 1) --group toto
	or (itotoindicator = true)) --itoto
	and (pb.createddate between '2025-06-24 22:00:00'   and '2025-06-25 22:00:00' )
),

ubt_temp_grouptoto as(

    select distinct ticketserialnumber, grouphostid, groupunitsequence,
	case
        when pbt.bettypeid not in ('M AN', 'M 2', 'M 3', 'M 4') THEN 1 --TOTO
        else 2 --totomatch
    end AS bettype
	from ztubt_placedbettransactionheader  pb
	inner join ztubt_toto_placedbettransactionlineitem  pbt on pb.tranheaderid = pbt.tranheaderid
	where groupunitsequence is not null --group toto
	and (pb.createddate between '2025-06-24 22:00:00'   and '2025-06-25 22:00:00' )
),

ubt_temp_CancelledBetTicketState as (

    select cb.cartid,cb.terdisplayid,cb.ticketserialnumber,cb.tranheaderid,cb.cancelleddate ,cb.cancelledamout,cb.prodid
	from  ztubt_cancelledbetticket  cb
	inner join ztubt_cancelledbetticketlifecyclestate cbt
   on cb.tranheaderid = cbt.tranheaderid and cbt.betstatetypeid = 'CB06'
	where cb.cancelleddate between '2025-06-24 22:00:00' and '2025-06-25 22:00:00'


),

ubt_temp_SalesGroupToto as (

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
	and (pbth.createddate between '2025-06-24 22:00:00'   and '2025-06-25 22:00:00' )
	group by  ter.terdisplayid,loc.locdisplayid, fin.bettype
),

ubt_temp_salestoto as (

    select ter.terdisplayid		as terdisplayid,
	loc.locdisplayid			as locdisplayid,
	3		as prodid,
	1 as bettype,
	count(distinct fin.ticketserialnumber)	as wagertotal, --(1)count(1)		as totalcount,
	count(distinct can.ticketserialnumber)	as canceltotal,--(8)
	sum(coalesce(fin.wager,0))	as wageramount,
	sum(coalesce(fin.sales,0))	as salesamount
	from	ztubt_placedbettransactionheader pbth
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
	and (pbth.createddate between '2025-06-24 22:00:00'   and '2025-06-25 22:00:00' )
	group by ter.terdisplayid,loc.locdisplayid
),

ubt_temp_transamountdetaildata as(
    select terdisplayid, trim(prodname) as productname, (amount * 100) as amount, ticketcount as ct, flag, transtype
	from sp_ubt_gettransamountdetails('2025-06-25','2025-06-25')
)

,

ubt_temp_sales_scandsr as (
    select *  from sp_ubt_getsweepsalespersrterminal('2025-06-25', '2025-06-25')
)
,
ubt_temp_salesfactorconfig as (

	select a.prodid, a.salesfactor  from  ztubt_salescomconfig  a
	where commissiontype=1  and a.isdeleted = false and prodid in (2,3,4)

)

,

ubt_temp_tmpterlocprdsalesamt as (

    select	ter.terdisplayid	as terdisplayid,
	'2025-06-25'	as transactiondate,
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
	'2025-06-25'	as transactiondate,
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
	'2025-06-25'	as transactiondate,
	99 as prodid,
	loc.locdisplayid as locdisplayid,
	'116'	as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	--sum(coalesce(amount,0)*(1-0))as amount,
	sum(coalesce(amount,0)*(100 / (100 + 0 * 100)))as amount,
	'' as sub_prod
	from ubt_temp_transamountdetaildata a
	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	where flag='COL' and transtype = 'OO' and a.productname = 'Gate Admission'
	group by loc.locdisplayid,ter.terdisplayid

	union all

	----2	AT_CANCELAMT	Yes	cancel amount

	select	ter.terdisplayid	as terdisplayid,
	'2025-06-25'	as transactiondate,
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
	'2025-06-25'	as transactiondate,
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
	'2025-06-25'	as transactiondate,
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
	'2025-06-25'	as transactiondate,
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
	'2025-06-25'	as transactiondate,
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
	'2025-06-25'	as transactiondate,
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
	'2025-06-25'	as transactiondate,
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
	'2025-06-25' transactiondate,
	3		as prodid,
	loc.locdisplayid	as locdisplayid,
	'16'		as sales_type,
	count(distinct vb.tranheaderid)	as totalcount,
	sum(coalesce(vb.winningamount,0))	as totalamount,
	case when ph.bettypeid not in ('M AN', 'M 2', 'M 3', 'M 4') then 'TOTO'
		else 'TOTO MATCH' end as sub_prod
	from --(1)
	(
		select vb.tranheaderid,vb.winningamount,vb.cartid,vb.terdisplayid,vb.createdvalidationdate
		from ztubt_validatedbetticket  vb --(16)
		inner join ztubt_validatedbetticketlifecyclestate  vbt
		on vb.tranheaderid = vbt.tranheaderid and vbt.betstatetypeid = 'VB06'
		where vb.winningamount != 0 and vb.winningamount is not null --(8)
		and vb.createdvalidationdate >= '2025-06-24 22:00:00' and vb.createdvalidationdate < '2025-06-25 22:00:00' --(16)
	)vb
	inner join
	(
		select distinct tranheaderid, grouphostid, groupunitsequence, bettypeid from ztubt_toto_placedbettransactionlineitem
	)ph on vb.tranheaderid = ph.tranheaderid --(16)
	inner join ubt_temp_terminal ter on vb.terdisplayid = ter.terdisplayid
	inner join ubt_temp_location loc on ter.locid = loc.locid
	where ph.groupunitsequence is not null --(16)
	group by loc.locdisplayid,ter.terdisplayid,
	case when ph.bettypeid not in ('M AN', 'M 2', 'M 3', 'M 4') then 'TOTO'
		else 'TOTO MATCH' end

	union all
	--52. House syndicate sales amount - [iTOTO sales – this is a subset of sales type 1]
	--TOTO
	select	s.terdisplayid			as terdisplayid,
	'2025-06-25'	as transactiondate,
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
	'2025-06-25'		as transactiondate,
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
	'2025-06-25'		as transactiondate,
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
		select gstrate from ztubt_gstconfig a
		where ('2025-06-25' between effectivefrom and enddate) or ('2025-06-25' >=effectivefrom and enddate is null) limit 1
		--check this code
	)gst

	union all
	--59. House syndicate cancel amount - [iTOTO cancellations – This is a Sub Set of Sales Type 2]
	--TOTO
	select	cb.terdisplayid	as terdisplayid,
	'2025-06-25' as transactiondate,
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
	 and cb.cancelleddate between '2025-06-25 06:00:00' and '2025-06-26 06:00:00'
	group by cb.terdisplayid,loc.locdisplayid

	union all
	---60. Player syndicate sales amount by sales factor - [GROUP TOTO Sales by Sales Factor – This is a subset of Sales Type 116]
	--TOTO, totomatch
	SELECT S.TerDisplayID AS TerDisplayID ,
	 '2025-06-25'	AS TransactionDate,
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
	'2025-06-25'	AS TransactionDate,
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
	'2025-06-25'	as transactiondate,
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
	and cbt.cancelleddate between '2025-06-25 06:00:00' and '2025-06-26 06:00:00'
	group by cbt.terdisplayid,loc.locdisplayid,
		case when gt.bettype = 1 then 'TOTO'
			else 'TOTO MATCH' end

	union all
	--105 Sales Commision Rate
	select 	loc.terdisplayid as terdisplayid,
	'2025-06-25'		as transactiondate,
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
			from ztubt_salescomconfig    a
			where commissiontype=1 and  isdeleted = false and prodid in (2,4) --lottery / es
		)sal

	union all
	--TOTO, totomatch
	select 	loc.terdisplayid as terdisplayid,
	'2025-06-25'		as transactiondate,
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
			from ztubt_salescomconfig    a
			where commissiontype=1 and  isdeleted = false and prodid = 3
		)sal
		cross join (select 'TOTO' as sub_prod union all select 'TOTO MATCH' as sub_prod) as sp

	union all
	--108 AT_ON_EZLINK-----------------------------------------------------------------------

	select	ter.terdisplayid	as terdisplayid,
	'2025-06-25' as transactiondate,
	0		as prodid,
	loc.locdisplayid		as locdisplayid,
	'108'		as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	sum(coalesce(a.amount,0))as amount,
	'' as sub_prod
	from	ubt_temp_location as loc
	left join ubt_temp_terminal as ter on  ter.locid=loc.locid
	left join ubt_temp_ResultCashlessInTerminal a on a.terdisplayid = ter.terdisplayid
	where a.productname = 'NETS'
	group by ter.terdisplayid,loc.locdisplayid

	union all
	--110	AT_ON_ATM	Yes	 Flashpay transaction --------------------------------------------------
	select	ter.terdisplayid	as terdisplayid,
	'2025-06-25' as transactiondate,
	0		as prodid,
	loc.locdisplayid		as locdisplayid,
	'110'		as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	sum(coalesce(a.amount,0))as amount,
	'' as sub_prod
	from	ubt_temp_location as loc
	left join ubt_temp_terminal as ter on  ter.locid=loc.locid
	left join ubt_temp_ResultCashlessInTerminal a on a.terdisplayid = ter.terdisplayid
	where a.productname = 'Flashpay'
	group by ter.terdisplayid,loc.locdisplayid

	union all
	--112	AT_ON_CASHCARD	Yes	 Cashcard transaction------------------------------------------
	select	ter.terdisplayid	as terdisplayid,
	'2025-06-25' as transactiondate,
	0		as prodid,
	loc.locdisplayid		as locdisplayid,
	'112'		as sales_type,
	sum(coalesce(a.ct,0))	as totalcount,
	sum(coalesce(a.amount,0))as amount,
	'' as sub_prod
	from	ubt_temp_location as loc
	left join ubt_temp_terminal as ter on  ter.locid=loc.locid
	left join ubt_temp_ResultCashlessInTerminal a on a.terdisplayid = ter.terdisplayid
	where a.productname = 'CASHCARD'
	group by ter.terdisplayid,loc.locdisplayid

	union all
	---116	AT_NOSPPL_SALAMT 	Yes	Sales multiplied by sales factor-------------------------------
	select	ter.terdisplayid	as terdisplayid,
	'2025-06-25' as transactiondate,
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
							AND pbtlin.bettypeid in ('M AN', 'M 2', 'M 3', 'M 4')
					) then 'TOTO MATCH'
				else 'TOTO' end as sub_prod
			from ubt_temp_tmpticketbywageandsales a
			inner join ztubt_placedbettransactionheader  pb on a.ticketserialnumber=pb.ticketserialnumber
			where pb.iscancelled = false and (
					(
					pb.prodid in (2,3,4) --lottery / es
					and pb.createddate between '2025-06-24 22:00:00'   and '2025-06-25 22:00:00'

					)
					or --(15)
					(
					pb.prodid in (5,6) --sports / ob
					and pb.createddate between '2025-06-24 22:00:00' and '2025-06-25 22:00:00'
					)
					or
					(
					pb.prodid in (1) --horse racing / bmcs
					and pb.createddate between '2025-06-24 22:00:00' and '2025-06-25 22:00:00'
					)
				)


			group by a.ticketserialnumber, pb.terdisplayid, pb.prodid,
				case when pb.prodid <> 3 then ''
					when pb.prodid = 3 and EXISTS (
							SELECT 1
							FROM ztubt_toto_placedbettransactionlineitem pbtlin
							WHERE pb.TranHeaderID = pbtlin.TranHeaderID
								AND pbtlin.bettypeid in ('M AN', 'M 2', 'M 3', 'M 4')
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
							AND pbtlin.bettypeid in ('M AN', 'M 2', 'M 3', 'M 4')
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
								AND pbtlin.bettypeid in ('M AN', 'M 2', 'M 3', 'M 4')
						) then 'TOTO MATCH'
					else 'TOTO' end
		)can
			on a.ticketserialnumber = can.ticketserialnumber and a.terdisplayid = can.terdisplayid
			and a.prodid = can.prodid and a.sub_prod = can.sub_prod

		group by loc.locdisplayid,ter.terdisplayid,a.prodid, a.sub_prod
	union all
	select ter.terdisplayid, '2025-06-25', sfc.prodid, ter.locdisplayid, '119', 0, sfc.salesfactor, ''
		from
		(
			select ter.terdisplayid, loc.locdisplayid from ubt_temp_terminal ter
			inner join ubt_temp_location loc on ter.locid = loc.locid
		)ter
		cross join
		(
			select distinct sfc.prodid,sfc.salesfactor from ubt_temp_salesfactorconfig sfc  where sfc.prodid <> 3
		)sfc
	union all
	select ter.terdisplayid, '2025-06-25', sfc.prodid, ter.locdisplayid, '119', 0, sfc.salesfactor, sp.sub_prod
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
),

ubt_temp_tmptelocprdsalesamt_2nd as (

	select * from ubt_temp_tmpterlocprdsalesamt
	union all
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


	union all
	-- sum totomatch and t649
select terdisplayid, transactiondate, prodid, locdisplayid, sales_type, sum(totalcount), sum(amount), ''
from ubt_temp_tmpterlocprdsalesamt td
where sub_prod in ('TOTO MATCH', 'TOTO') and Sales_type not in ('40', '119', '105')
group by terdisplayid, transactiondate, prodid, locdisplayid, sales_type
union all

-- config - clone totomatch config for product Toto (t649 and totomatch)
select terdisplayid, transactiondate, prodid, locdisplayid, sales_type, totalcount, amount, ''
from ubt_temp_tmpterlocprdsalesamt td
where sub_prod in ('TOTO MATCH') and Sales_type in ('40', '119', '105')
)


select * from ubt_temp_tmptelocprdsalesamt_2nd where totalcount != 0 or amount != 0

-- select 'ubt_temp_terminal' as table_name, count(*) as record_count from ubt_temp_terminal
-- union all
-- select 'ubt_temp_product' as table_name, count(*) as record_count from ubt_temp_product
-- union all
-- select 'ubt_temp_location' as table_name, count(*) as record_count from ubt_temp_location
-- union all
-- select 'ubt_temp_TmpTicketByWageAndSales' as table_name, count(*) as record_count from ubt_temp_TmpTicketByWageAndSales
-- union all
-- select 'ubt_temp_ResultCashlessInTerminal' as table_name, count(*) as record_count from ubt_temp_ResultCashlessInTerminal
-- union all
-- select 'ubt_temp_iTOTO' as table_name, count(*) as record_count from ubt_temp_iTOTO
-- union all
-- select 'ubt_temp_grouptoto' as table_name, count(*) as record_count from ubt_temp_grouptoto
-- union all
-- select 'ubt_temp_CancelledBetTicketState' as table_name, count(*) as record_count from ubt_temp_CancelledBetTicketState
-- union all
-- select 'ubt_temp_SalesGroupToto' as table_name, count(*) as record_count from ubt_temp_SalesGroupToto
-- union all
-- select 'ubt_temp_salestoto' as table_name, count(*) as record_count from ubt_temp_salestoto
-- union all
-- select 'ubt_temp_transamountdetaildata' as table_name, count(*) as record_count
-- union all
-- select 'ubt_temp_tmpterlocprdsalesamt' as table_name, count(*) as record_count from ubt_temp_tmpterlocprdsalesamt
-- UNION ALL
-- select 'ubt_temp_sales_scandsr' as table_name, count(*) as record_count from ubt_temp_sales_scandsr
-- union all
-- select 'ubt_temp_salesfactorconfig' as table_name, count(*) as record_count from ubt_temp_salesfactorconfig
-- union all
-- select 'ubt_temp_salestoto' as table_name, count(*) as record_count from ubt_temp_salestoto
-- union all
-- select 'ubt_temp_salestotogrouptoto' as table_name, count(*) as record_count from ubt_temp_salestotogrouptoto
-- union allvvvvvvvvvvvvvvvvvv
-- select 'ubt_temp_tmptẻlocprdsalesamt_2nd' as table_name, count(*) as record_count from ubt_temp_tmptẻlocprdsalesamt_

;