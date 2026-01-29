
CREATE FUNCTION public.sp_ubt_gettransactiondataforinterval(startdatetime timestamp without time zone, enddatetime timestamp without time zone) RETURNS TABLE(uuid character varying, tranheaderid character varying, transactiontype integer, productid integer, bettypeid character varying, selection text, businessdate date, transactiontimestamp timestamp without time zone, terminalid character varying, userid character varying, locationid character varying, boardseqnumber character varying, entrymethod integer, numboards integer, numdraws integer, drawid character varying, ebetslipinfo_mobnbr character varying, numsimplebets integer, qpflag integer, nummarks integer, bulkid character varying, itoto_numparts text, itoto_totalparts text, grptoto_numparts text, eventid character varying, leaguecode character varying, liveindicator character, eventname text, market text, odds character varying, matchkickoff character varying, wager1 numeric, wager2 numeric, sales1 numeric, sales2 numeric, salescomm1 numeric, salescomm2 numeric, gst1 numeric, gst2 numeric, returnamount numeric, winningamount numeric, paymentmode character varying, cartid character varying, betlinerebateamount numeric)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
declare
startdate TIMESTAMP;
enddate timestamp;
bmcs_startdate timestamp;
igt_startdate timestamp;
ob_startdate timestamp;
bmcs_enddate timestamp;
igt_enddate timestamp;
ob_enddate timestamp;
bmcs_currentbusinessdate date;
igt_currentbusinessdate date;
ob_currentbusinessdate date;
bmcs_nextbusinessdate date;
igt_nextbusinessdate date;
ob_nextbusinessdate date;
bmcs_previousbusinessdate date;
igt_previousbusinessdate date;
ob_previousbusinessdate date;
startdateUTC timestamp;
enddateUTC timestamp;
v_totomatchBettypes text[] := (SELECT array(SELECT bettypeid FROM public.sp_ubt_gettotomatchbettypes()));

BEGIN


select
startdatetime ,
enddatetime ,

fromdatetimebmcs ,
fromdatetimeigt ,
fromdatetimeob ,

todatetimebmcs ,
todatetimeigt ,
todatetimeob ,

(fromdatetimebmcs  ::DATE) ,
(fromdatetimeigt  :: DATE) ,
(fromdatetimeob  :: DATE) ,

(fromdatetimebmcs  ::DATE) + interval '1 Day' ,
(fromdatetimeigt  :: DATE) + interval '1 Day' ,
(fromdatetimeob  :: DATE) + interval '1 Day' ,

(fromdatetimebmcs  ::DATE)- interval '1 Day'  ,
(fromdatetimeigt  :: DATE)- interval '1 Day' ,
(fromdatetimeob  :: DATE) - interval '1 Day' ,

(startdatetime - interval '8 hour')  ,
(enddatetime - interval '8 hour')

into

startdate ,
enddate ,

bmcs_startdate ,
igt_startdate ,
ob_startdate ,

bmcs_enddate ,
igt_enddate ,
ob_enddate ,

bmcs_currentbusinessdate ,
igt_currentbusinessdate ,
ob_currentbusinessdate ,

bmcs_nextbusinessdate ,
igt_nextbusinessdate ,
ob_nextbusinessdate ,

bmcs_previousbusinessdate ,
igt_previousbusinessdate ,
ob_previousbusinessdate ,

startdateUTC ,
enddateUTC
from public.sp_ubt_getcommonubtdates(StartDatetime ,StartDatetime );





--ubt_temp_placebettransaction #PBVBCBT

 create temporary table if not exists ubt_temp_placebettransaction(
  TicketSerialNumber VARCHAR(200),
  TranHeaderID varchar(50),
  EntryMethodID VARCHAR(10),
  DeviceID VARCHAR(100),
  ProdID INT4,
  IsBetRejectedByTrader bool,
  IsExchangeTicket bool,
  TerDisplayID  varchar(100),
  CreatedDate timestamp,
  RequestID varchar(40),
  UserDisplayID varchar(40),
  CartID varchar(50),
  TransactionType int4
 );




insert INTO ubt_temp_placebettransaction(TicketSerialNumber, TranHeaderID, EntryMethodID, DeviceID, ProdID, IsBetRejectedByTrader,IsExchangeTicket,TerDisplayID,CreatedDate,RequestID,UserDisplayID,CartID,TransactionType)


 SELECT TicketSerialNumber, PB.TranHeaderID, EntryMethodID, DeviceID, ProdID, IsBetRejectedByTrader,IsExchangeTicket
 ,TerDisplayID,PB.CreatedDate,RequestID,UserDisplayID,CartID, 1  TransactionType
  FROM public.ztubt_placedbettransactionheader PB
  INNER JOIN public.ztubt_placedbettransactionheaderlifecyclestate  PBLC ON PB.TranHeaderID = PBLC.TranHeaderID AND PBLC.BetStateTypeID = 'PB06'
  WHERE
	(PB.CreatedDate >= startdateUTC And PB.CreatedDate <= enddateUTC)

  Union
  SELECT VB.TicketSerialNumber, VB.TranHeaderID, PB.EntryMethodID, PB.DeviceID, PB.ProdID, PB.IsBetRejectedByTrader
  ,PB.IsExchangeTicket,PB.TerDisplayID,PB.CreatedDate,PB.RequestID,VB.UserDisplayID,VB.CartID, 3 TransactionType
  FROM public.ztubt_validatedbetticket  VB
  INNER JOIN public.ztubt_placedbettransactionheader PB ON VB.TicketSerialNumber = PB.TicketSerialNumber
  INNER JOIN public.ztubt_validatedbetticketlifecyclestate VBLC ON VB.TranHeaderID = VBLC.TranHeaderID AND VBLC.BetStateTypeID = 'VB06'
  WHERE VB.ValidationTypeID IN ('VALD', 'RFND')
  AND   (VB.CreatedValidationDate >= startdateUTC And VB.CreatedValidationDate <= enddateUTC)
  Union
  SELECT CB.TicketSerialNumber, CB.TranHeaderID, PB.EntryMethodID, PB.DeviceID, PB.ProdID, PB.IsBetRejectedByTrader,

   PB.IsExchangeTicket,PB.TerDisplayID,PB.CreatedDate,PB.RequestID,CB.UserDisplayID,CB.CartID,5 TransactionType

  FROM public.ztubt_CancelledBetTicket CB
  INNER JOIN public.ztubt_placedbettransactionheader PB ON CB.TicketSerialNumber = PB.TicketSerialNumber
  INNER JOIN public.ztubt_cancelledbetticketlifecyclestate  CBLC ON CBLC.TranHeaderID = PB.TranHeaderID AND CBLC.BetStateTypeID = 'CB06'
  WHERE (PB.IsBetRejectedByTrader ) = FALSE AND (CB.CancelledDate >= startdate And CB.CancelledDate <= enddate)
  ;

 create index idx_ubt_ticketserialnumber on ubt_temp_placebettransaction(TicketSerialNumber);
 create index idx_ubt_tranheaderid on ubt_temp_placebettransaction(TranHeaderID);
create index idx_ubt_prodid on ubt_temp_placebettransaction(prodid);
create index idx_ubt_CreatedDate on ubt_temp_placebettransaction(CreatedDate);
create index idx_ubt_TransactionType on ubt_temp_placebettransaction(TransactionType);

 create temporary table if not exists ubt_temp_placebettransaction_type
 (TicketSerialNumber VARCHAR(200),
  TranHeaderID varchar(50),
  EntryMethodID VARCHAR(50),
  DeviceID VARCHAR(100),
  ProdID INT4,
  IsBetRejectedByTrader bool,
  IsExchangeTicket bool,
  TerDisplayID  varchar(100),
  TransactionTypeTotal int4)
  ;

 insert into ubt_temp_placebettransaction_type

  Select TicketSerialNumber, TranHeaderID, EntryMethodID, DeviceID, ProdID,
   IsBetRejectedByTrader,IsExchangeTicket, TerDisplayID
  ,SUM(TransactionType) TransactionTypeTotal
  from ubt_temp_placebettransaction
  Group By TicketSerialNumber, TranHeaderID, DeviceID, EntryMethodID, ProdID,
  IsExchangeTicket, TerDisplayID, IsBetRejectedByTrader
  ;

 create index idx_ubt_transaction_type_ticketserialnumber on ubt_temp_placebettransaction_type(TicketSerialNumber);
create index idx_ubt_transaction_type_tranheaderid on ubt_temp_placebettransaction_type(TranHeaderID);
create index idx_ubt_transaction_type_prodid on ubt_temp_placebettransaction_type(ProdID);
create index idx_ubt_transaction_type_isbetrejectedbytrader on ubt_temp_placebettransaction_type(IsBetRejectedByTrader);
create index idx_ubt_transaction_type_isexchangeticket on ubt_temp_placebettransaction_type(IsExchangeTicket);
create index idx_ubt_transaction_type_terdisplayid on ubt_temp_placebettransaction_type(TerDisplayID);
create index idx_ubt_transaction_type_transactiontypetotal on ubt_temp_placebettransaction_type(TransactionTypeTotal);

  --ubt_temp_placebettransaction_type #PBDVBCBT

   -----------------------------------------------------------------
 --BET TYPE
 -----------------------------------------------------------------
 create temporary table if not exists ubt_temp_bettype
 (
  ticketserialnumber varchar(500),
  tranlineitemnumberid varchar(50),
  bettypeid varchar(2)
 )
 ;



 --4D
 INSERT INTO ubt_temp_bettype
 SELECT
 PBVBCBT.TicketSerialNumber,
 PBTLI.TranLineItemID,
 CASE
  WHEN PBTLI.BetTypeID = 'ORD' THEN
   CASE
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) > 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) = 0 THEN '0'
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) = 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) > 0 THEN '5'
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) > 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) > 0 THEN '10'
   END
  WHEN PBTLI.BetTypeID = 'SYS' THEN
   CASE
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) > 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) = 0 THEN
 CASE
  WHEN PBTLI.Permutation = 24 THEN '1'
WHEN PBTLI.Permutation = 12 THEN '2'
  WHEN PBTLI.Permutation = 4 THEN '3'
  WHEN PBTLI.Permutation = 6 THEN '4'
 END
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) = 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) > 0 THEN
 CASE
  WHEN PBTLI.Permutation = 24 THEN '6'
  WHEN PBTLI.Permutation = 12 THEN '7'
  WHEN PBTLI.Permutation = 4 THEN '8'
  WHEN PBTLI.Permutation = 6 THEN '9'
 END
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) > 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) > 0 THEN
   CASE
  WHEN PBTLI.Permutation = 24 THEN '11'
  WHEN PBTLI.Permutation = 12 THEN '12'
  WHEN PBTLI.Permutation = 4 THEN '13'
  WHEN PBTLI.Permutation = 6 THEN '14'
 END
   END
  WHEN PBTLI.BetTypeID = 'IBET' THEN
   CASE
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) > 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) = 0 THEN
 CASE
  WHEN PBTLI.Permutation = 24 THEN '27'
  WHEN PBTLI.Permutation = 12 THEN '28'
  WHEN PBTLI.Permutation = 4 THEN '29'
  WHEN PBTLI.Permutation = 6 THEN '30'
 END
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) = 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) > 0 THEN
 CASE
  WHEN PBTLI.Permutation = 24 THEN '31'
  WHEN PBTLI.Permutation = 12 THEN '32'
  WHEN PBTLI.Permutation = 4 THEN '33'
  WHEN PBTLI.Permutation = 6 THEN '34'
 END
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) > 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) > 0 THEN
 CASE
  WHEN PBTLI.Permutation = 24 THEN '35'
  WHEN PBTLI.Permutation = 12 THEN '36'
  WHEN PBTLI.Permutation = 4 THEN '37'
  WHEN PBTLI.Permutation = 6 THEN '38'
 END
   END
  WHEN PBTLI.BetTypeID = 'ROLL' THEN
   CASE
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) > 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) = 0 THEN
 CASE
  WHEN position ('R' in PBTLIN.SelectedBetNumber) = 1 THEN '15'
  WHEN position ('R' in PBTLIN.SelectedBetNumber)= 2 THEN '16'
  WHEN position ('R' in PBTLIN.SelectedBetNumber) = 3 THEN '17'
  ELSE '18'
 END
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) = 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) > 0 THEN
 CASE
  WHEN position ('R' in PBTLIN.SelectedBetNumber) = 1 THEN '19'
  WHEN position ('R' in PBTLIN.SelectedBetNumber) = 2 THEN '20'
  WHEN position ('R' in PBTLIN.SelectedBetNumber) = 3 THEN '21'
  ELSE '22'
 END
WHEN COALESCE(PBTLIN.BigBetAcceptedWager, 0) > 0  AND COALESCE(PBTLIN.SmallBetAcceptedWager, 0) > 0 THEN
 CASE
  WHEN position ('R' in PBTLIN.SelectedBetNumber) = 1 THEN '23'
  WHEN position ('R' in PBTLIN.SelectedBetNumber) = 2 THEN '24'
  WHEN position ('R' in PBTLIN.SelectedBetNumber) = 3 THEN '25'
  ELSE '26'
 END
   END
 END AS BetTypeID
 FROM ubt_temp_placebettransaction_type PBVBCBT

 INNER JOIN  public.ztubt_4d_placedbettransactionlineitem   PBTLI ON PBVBCBT.TranHeaderID = PBTLI.TranHeaderID
 INNER JOIN  public.ztubt_4d_placedbettransactionlineitemnumber  PBTLIN ON PBTLI.TranLineItemID = PBTLIN.TranLineItemID
 WHERE PBVBCBT.ProdId = 2
 AND coalesce (PBTLIN.BigBetAcceptedWager, 0) + coalesce (PBTLIN.SmallBetAcceptedWager, 0) > 0
and
 ((PBTLIN.CreatedDate >= startdateUTC And PBTLIN.CreatedDate <= enddateUTC
 and  PBVBCBT.TransactionTypeTotal In (1, 4, 6))	or  PBVBCBT.TransactionTypeTotal In (3,5))


 union all
----TOTO


 select
 pbt.ticketserialnumber,
 pbtli.tranlineitemid,
 case pbtli.bettypeid
  WHEN 'ORD'   THEN '0'
  WHEN 'SYSR'  THEN '1'
  WHEN 'SYS7'  THEN '2'
  WHEN 'SYS8'  THEN '3'
  WHEN 'SYS9'  THEN '4'
  WHEN 'SYS10' THEN '5'
  WHEN 'SYS11' THEN '6'
  WHEN 'SYS12' THEN '7'
  --totomatch
  WHEN 'M 4'  THEN '16'
  WHEN 'M 3' THEN '17'
  WHEN 'M 2' THEN '18'
  WHEN 'M AN' THEN '19'
 end as bettypeid
 from ubt_temp_placebettransaction_type pbt
 inner join public.ztubt_toto_placedbettransactionlineitem pbtli  on pbt.tranheaderid = pbtli.tranheaderid
 where pbt.prodid = 3 and
  ((pbtli.createddate >= startdateutc and pbtli.createddate <= enddateutc and
  pbt.transactiontypetotal in (1, 4, 6))
 	or  pbt.transactiontypetotal in (3,5))

 union all

 ---SWEEP

 select
 pbt.ticketserialnumber,
 pbtlin.tranlineitemid,
 '0' as bettypeid
 from ubt_temp_placebettransaction_type pbt
 inner join public.ztubt_sweep_placedbettransactionlineitemnumber pbtlin on pbt.tranheaderid = pbtlin.tranheaderid
 where   pbt.prodid = 4
 and issoldout  <> true and
 ((pbtlin.createddate >= startdateutc and pbtlin.createddate <= enddateutc
 and  pbt.transactiontypetotal in (1, 4, 6))
 	or  pbt.transactiontypetotal in (3,5))

 --SPORT
 union all
 select
 pbvbcbt.ticketserialnumber,
 pbtlin.tranlineitemid,
 case scat.categoryid when 1
 then
 case pbtli.accumulatorid
  WHEN 'ACC4' THEN '4F'
  WHEN 'DBL' THEN 'D'
  WHEN 'SGL' THEN 'S'
  WHEN 'TBL' THEN 'T'
  END
 WHEN 2 THEN 'S'

 end as bettypeid
 from ubt_temp_placebettransaction_type pbvbcbt
 inner join public.ztubt_sports_placedbettransactionlineitem  pbtli on pbvbcbt.tranheaderid = pbtli.tranheaderid
 inner  join public.ztubt_sports_placedbettransactionlineitemnumber  pbtlin on pbtli.tranlineitemid = pbtlin.tranlineitemid
 inner join public.ztubt_sportevent se on pbtlin.eventid = se.eventid
 inner join public.ztubt_sporttype st on se.typeid = st.typeid
 inner join public.ztubt_sportclass sc on st.classid = sc.classid
 inner join public.ztubt_sportcategory scat on sc.categoryid = scat.categoryid
 where (scat.categoryid= 1)  or (scat.categoryid=2 and pbvbcbt.prodid in (5,6) )

 group by pbvbcbt.ticketserialnumber,pbtlin.tranlineitemid, scat.categoryid,pbtli.accumulatorid
 ;

create index  ind_ticketsrno on ubt_temp_bettype(ticketserialnumber);
 -----------------------------------------------------------------
 --DETAIL OF HORSE
 -----------------------------------------------------------------
 create temporary table if not exists ubt_temp_horsedetail
 (
  ticketserialnumber varchar(500),
  entrymethod int4,
  betlinerebateamount numeric(22,2)
 )
 ;

 insert into ubt_temp_horsedetail


 SELECT ADP.TicketSerialNumber,
  CASE ADP.EntryMethodID
  WHEN 'Manual' THEN 0
  WHEN 'BetSlip' THEN 2
  WHEN 'Edit' THEN 10
  WHEN 'EBetSlip' THEN 16
 END AS EntryMethod,
 SUM(ADP.Amount) AS BetLineRebateAmount
 FROM
 (
  SELECT   PBVBCBT.TicketSerialNumber, PBTL.TranHeaderID, EntryMethodID,
  CASE WHEN BetTypeID = 'W-P'
THEN (coalesce(PBTL.BoardRebateAmount,0))
   ELSE SUM(coalesce(PBTL.BoardRebateAmount,0))
  END AS Amount

  FROM ubt_temp_placebettransaction_type PBVBCBT
  INNER JOIN   public.ztubt_horse_placedbettransactionlineitem  PBTL ON PBVBCBT.TranHeaderID = PBTL.TranHeaderID
	WHERE  PBVBCBT.ProdID = 1 and
	((PBTL.CreatedDate >= startdateUTC And PBTL.CreatedDate <= enddateUTC
AND PBVBCBT.TransactionTypeTotal In (1, 4, 6) ) or PBVBCBT.TransactionTypeTotal in (3,5))
  GROUP BY  PBVBCBT.TicketSerialNumber, PBTL.TranHeaderID, BetTypeID, BoardRebateAmount, EntryMethodID
 )ADP
 GROUP BY ADP.TicketSerialNumber, ADP.EntryMethodID
 ;

create index idx_ubt_horse_ticketsrno on ubt_temp_horsedetail(TicketSerialNumber);
 -----------------------------------------------------------------
 --DETAIL OF LOTTERY
 -----------------------------------------------------------------

 create temporary table if not exists ubt_temp_numboards
 (
  ticketserialnumber varchar(500),
  numboards int4,
  bulkid varchar(50)
 ) ;

 INSERT INTO ubt_temp_NumBoards
 --4D
 SELECT DISTINCT PBT.TicketSerialNumber,
 COUNT(z4dln.TranLineItemID) AS NumBoards,
 NULL AS BulkID
 FROM ubt_temp_placebettransaction_type PBT
 INNER JOIN   public.ztubt_4d_placedbettransactionlineitemnumber  z4dln ON PBT.TranHeaderID = z4dln.TranHeaderID
 WHERE PBT.ProdID = 2 and  coalesce(z4dln.BigBetAcceptedWager, 0) + coalesce(z4dln.SmallBetAcceptedWager, 0) > 0 AND
 ((z4dln.CreatedDate >= StartDateUTC And z4dln.CreatedDate <= EndDateUTC
  AND PBT.TransactionTypeTotal In (1, 4, 6)) or PBT.TransactionTypeTotal In (3,5))

 GROUP BY PBT.TicketSerialNumber


 union all
 --TOTO

 select pbvbcbt.ticketserialnumber,
 count(pbtl.tranlineitemid) as numboards,
 null as bulkid
 from ubt_temp_placebettransaction_type pbvbcbt
 inner join  public.ztubt_toto_placedbettransactionlineitem  pbtl on pbvbcbt.tranheaderid = pbtl.tranheaderid
 where   pbvbcbt.prodid = 3 and
 ((pbtl.createddate >= startdateUTC and pbtl.createddate <= enddateUTC and
  pbvbcbt.transactiontypetotal in (1, 4, 6)) or pbvbcbt.transactiontypetotal in (3,5))
 group by pbvbcbt.ticketserialnumber


 union  all

 --SWEEP
 SELECT PBT.TicketSerialNumber,
 COUNT(PLN.TranLineItemID) AS NumBoards,
 BulkID
 FROM ubt_temp_placebettransaction_type PBT
 INNER JOIN  public.ztubt_sweep_placedbettransactionlineitem  PL ON PBT.TranHeaderID = PL.TranHeaderID
 INNER JOIN  public.ztubt_sweep_placedbettransactionlineitemnumber  PLN ON PL.TranLineItemID = PLN.TranLineItemID
 WHERE  PBT.ProdID = 4 AND IsSoldOut <> true and
 ((PLN.CreatedDate >= StartDateUTC And PLN.CreatedDate <= EndDateUTC
 AND PBT.TransactionTypeTotal In (1, 4, 6)) or PBT.TransactionTypeTotal In (3,5))

 GROUP BY PBT.TicketSerialNumber, BulkID

 ;
create index idx_ubt_numboard_ticketsrno on ubt_temp_numboards (TicketSerialNumber);

create temporary table ubt_temp_LotteryDraw
( TicketSerialNumber varchar (200),
HostDrawDatesId varchar(20)
)
 ;
	insert into ubt_temp_LotteryDraw
Select PBTT.TicketSerialNumber, CAST(zdm.HostDrawDatesId AS VARCHAR) HostDrawDatesId

	FROM ubt_temp_placebettransaction_type PBTT
	INNER JOIN public.ztubt_drawdates zdm  ON PBTT.TranHeaderID = zdm.TranHeaderID
	Where  PBTT.ProdID In (2,3,4)

;
create index idx_ubt_lotterydraws_ticketsrno on ubt_temp_LotteryDraw (TicketSerialNumber,HostDrawDatesId);

create temporary table ubt_temp_numdraws
( TicketSerialNumber varchar (200),
NumDraws int4,
Drawid varchar(50)
)
 ;
	insert into ubt_temp_numdraws
	SELECT
	LD.TicketSerialNumber,
	Count(1) NumDraws,
	string_agg(CAST(LD.HostDrawDatesId AS varchar),',' ) as Drawid

	From ubt_temp_LotteryDraw LD
	Group By LD.TicketSerialNumber

;

create index idx_ubt_numdraws_ticketsrno on ubt_temp_numdraws (TicketSerialNumber);

 create temporary table if not exists ubt_temp_lotterydetail
 (
  ticketserialnumber varchar(500),
  tranlineitemid varchar(50),
  boardseqnumber int,
  entrymethod int,
  numboards int,
  numdraws int,
  drawid varchar(500),
  ebetslipinfo_mobnbr varchar(100),
  numsimplebets int4,
  qpflag int4,
  nummarks int4,
  bulkid varchar(50), --(5)
  itoto_numparts text, -- varchar(max)
  itoto_totalparts text,   --varchar(max)
  grptoto_numparts text   --varchar(max)
 )

 ;
 create temporary table if not exists ubt_temp_TotoSeq (
  TranLineItemID varchar(50),
  Seq INT4
 )
  ;
 create temporary table if not exists ubt_Temp_TotoGroup (
  GroupHostID varchar(50),
  GroupToto INT4
 ) ;

 INSERT INTO ubt_temp_TotoSeq
 SELECT LIN.TranLineItemID, MAX(LIN.Sequence) AS Seq
 FROM ubt_temp_placebettransaction_type PBT
 inner join  public.ztubt_toto_placedbettransactionlineitem  LI on PBT.TranHeaderID = LI.TranHeaderID
 inner join public.ztubt_toto_placedbettransactionlineitemnumber  LIN on LI.TranLineItemID = LIN.TranLineItemID
 WHERE PBT.ProdID = 3 and
 ((LI.CreatedDate >= StartDateUTC And LI.CreatedDate <= EndDateUTC
 AND PBT.TransactionTypeTotal In (1, 4, 6) ) or PBT.TransactionTypeTotal In (3,5))
 GROUP BY LIN.TranLineItemID
 ;

 INSERT INTO ubt_Temp_TotoGroup
 SELECT LI2.GroupHostID, MAX(LI2.GroupUnitSequence) AS GroupToto
 FROM ubt_temp_placebettransaction_type PBT
 inner join public.ztubt_toto_placedbettransactionlineitem  LI on PBT.TranHeaderID = LI.TranHeaderID
 inner join public.ztubt_toto_placedbettransactionlineitem LI2 on LI.GroupHostID = LI2.GroupHostID
 WHERE PBT.ProdID = 3
 AND LI.GroupHostID <> '00000000-0000-0000-0000-000000000000' AND LI.GroupUnitSequence IS NOT NULL
 GROUP BY LI2.GroupHostID

 ;
 --4D

 INSERT INTO ubt_Temp_LotteryDetail
 SELECT PBVBCBT.TicketSerialNumber,
 FOURD.TranLineItemID,
 ROW_NUMBER() OVER (PARTITION BY PBVBCBT.TicketSerialNumber
 					ORDER BY FOURD.TranLineItemID
-- 					ORDER BY split_part(FOURD.tranlineitemid,'-',5)
--							,split_part(FOURD.tranlineitemid,'-',4)
--							,split_part(FOURD.tranlineitemid,'-',3)
--							,split_part(FOURD.tranlineitemid,'-',2)
--							,split_part(FOURD.tranlineitemid,'-',1)
 					) AS RowNum,
 CASE PBVBCBT.EntryMethodID
  WHEN 'Manual' THEN 0
  WHEN 'BetSlip' THEN 2
  WHEN 'Edit' THEN 10
  WHEN 'EBetSlip' THEN 16
 END AS EntryMethod,
 NB.NumBoards,
 ND.NumDraws,
 ND.DrawID,
 PBVBCBT.DeviceID AS ebetslipinfo_mobnbr,
 CASE FOURD.Permutation
  WHEN NULL THEN 1
  WHEN 0   THEN 1
  ELSE FOURD.Permutation
 END AS NumSimpleBets,
 FOURD.QuickPickIndicator :: int AS QPFlag,
 4 AS NumMarks,
 NB.BulkID, --(5)
 NULL AS itoto_numparts,
 NULL AS itoto_totalparts,
 NULL AS grptoto_numparts
 FROM ubt_temp_placebettransaction_type PBVBCBT
	 INNER JOIN public.ztubt_4d_placedbettransactionlineitem  FOURD ON PBVBCBT.TranHeaderID = FOURD.TranHeaderID
	 INNER JOIN public.ztubt_4d_placedbettransactionlineitemnumber  PBTLIN ON FOURD.TranLineItemID = PBTLIN.TranLineItemID
	 INNER JOIN ubt_temp_NumBoards NB ON PBVBCBT.TicketSerialNumber = NB.TicketSerialNumber
	 INNER JOIN ubt_temp_numdraws ND ON PBVBCBT.TicketSerialNumber = ND.TicketSerialNumber
 WHERE PBVBCBT.ProdID = 2 AND coalesce (PBTLIN.BigBetAcceptedWager, 0) + coalesce (PBTLIN.SmallBetAcceptedWager, 0) > 0 and
   PBTLIN.CreatedDate >= StartDateUTC And PBTLIN.CreatedDate <= EndDateUTC AND
   PBVBCBT.TransactionTypeTotal In (1, 4, 6)

union all


 SELECT PBVBCBT.TicketSerialNumber,
 FOURD.TranLineItemID,
 ROW_NUMBER() OVER (PARTITION BY PBVBCBT.TicketSerialNumber
 					ORDER BY FOURD.TranLineItemID
-- 					ORDER BY split_part(FOURD.tranlineitemid,'-',5)
--							,split_part(FOURD.tranlineitemid,'-',4)
--							,split_part(FOURD.tranlineitemid,'-',3)
--							,split_part(FOURD.tranlineitemid,'-',2)
--							,split_part(FOURD.tranlineitemid,'-',1)

 					) AS RowNum,
 CASE PBVBCBT.EntryMethodID
  WHEN 'Manual' THEN 0
  WHEN 'BetSlip' THEN 2
  WHEN 'Edit' THEN 10
  WHEN 'EBetSlip' THEN 16
 END AS EntryMethod,
 NB.NumBoards,
 ND.NumDraws,
 ND.DrawID,
 PBVBCBT.DeviceID AS ebetslipinfo_mobnbr,
 CASE FOURD.Permutation
  WHEN NULL THEN 1
  WHEN 0   THEN 1
  ELSE FOURD.Permutation
 END AS NumSimpleBets,
 FOURD.QuickPickIndicator :: int AS QPFlag,
 4 AS NumMarks,
 NB.BulkID, --(5)
 NULL AS itoto_numparts,
 NULL AS itoto_totalparts,
 NULL AS grptoto_numparts
 FROM ubt_temp_placebettransaction_type PBVBCBT
	 INNER JOIN public.ztubt_4d_placedbettransactionlineitem  FOURD ON PBVBCBT.TranHeaderID = FOURD.TranHeaderID
	 INNER JOIN public.ztubt_4d_placedbettransactionlineitemnumber  PBTLIN ON FOURD.TranLineItemID = PBTLIN.TranLineItemID
	 INNER JOIN ubt_temp_NumBoards NB ON PBVBCBT.TicketSerialNumber = NB.TicketSerialNumber
	 INNER JOIN ubt_temp_numdraws ND ON PBVBCBT.TicketSerialNumber = ND.TicketSerialNumber
 WHERE PBVBCBT.ProdID = 2 AND coalesce (PBTLIN.BigBetAcceptedWager, 0) + coalesce (PBTLIN.SmallBetAcceptedWager, 0) > 0 and
 PBVBCBT.TransactionTypeTotal in (3,5)

union all

 --TOTO

 SELECT PBVBCBT.TicketSerialNumber,
 TOTO.TranLineItemID,
 ROW_NUMBER() OVER (PARTITION BY PBVBCBT.TicketSerialNumber
 					ORDER BY TOTO.TranLineItemID
--  					ORDER BY split_part(TOTO.tranlineitemid,'-',5)
--							,split_part(TOTO.tranlineitemid,'-',4)
--							,split_part(TOTO.tranlineitemid,'-',3)
--							,split_part(TOTO.tranlineitemid,'-',2)
--							,split_part(TOTO.tranlineitemid,'-',1)
 					) AS RowNum,
 CASE PBVBCBT.EntryMethodID
  WHEN 'Manual' THEN 0
  WHEN 'BetSlip' THEN 2
  WHEN 'Edit' THEN 10
  WHEN 'EBetSlip' THEN 16
 END AS EntryMethod,
 NB.NumBoards,
 ND.NumDraws,
 ND.DrawID,
 PBVBCBT.DeviceID AS ebetslipinfo_mobnbr,
 CASE
  WHEN TOTO.BetTypeID = ANY(v_totomatchBettypes) THEN 1
  WHEN TOTO.BetTypeID = 'ORD' THEN 1
  WHEN TOTO.BetTypeID = 'SYSR' THEN 44
  WHEN TOTO.BetTypeID = 'SYS7' THEN 7
  WHEN TOTO.BetTypeID = 'SYS8' THEN 28
  WHEN TOTO.BetTypeID = 'SYS9' THEN 84
  WHEN TOTO.BetTypeID = 'SYS10' THEN 210
  WHEN TOTO.BetTypeID = 'SYS11' THEN 462
  WHEN TOTO.BetTypeID = 'SYS12' THEN 924
 END AS NumSimpleBets,
 TOTO.QuickPickIndicator :: int AS QPFlag,
 TOTOSEQ.Seq AS NumMarks,
 NB.BulkID,
 cast(TOTO.Units as varchar) AS itoto_numparts ,
 cast(TOTO.SyndicatePartsAllowed as varchar) AS itoto_totalparts,
 cast(TOTOGROUP.GroupToto as varchar) AS grptoto_numparts

 FROM ubt_temp_placebettransaction_type PBVBCBT
 INNER JOIN public.ztubt_toto_placedbettransactionlineitem   TOTO ON PBVBCBT.TranHeaderID = TOTO.TranHeaderID
 INNER JOIN ubt_temp_TotoSeq TOTOSEQ ON TOTO.TranLineItemID = TOTOSEQ.TranLineItemID
 LEFT JOIN ubt_Temp_TotoGroup TOTOGROUP ON TOTO.GroupHostID = TOTOGROUP.GroupHostID --(3)
 INNER JOIN ubt_temp_NumBoards NB ON PBVBCBT.TicketSerialNumber = NB.TicketSerialNumber
 INNER JOIN ubt_temp_numdraws ND ON PBVBCBT.TicketSerialNumber = ND.TicketSerialNumber
 WHERE PBVBCBT.ProdID = 3 and
 TOTO.CreatedDate >= StartDateUTC And TOTO.CreatedDate <= EndDateUTC
 AND PBVBCBT.TransactionTypeTotal In (1, 4, 6)

 union all

 SELECT PBVBCBT.TicketSerialNumber,
 TOTO.TranLineItemID,
 ROW_NUMBER() OVER (PARTITION BY PBVBCBT.TicketSerialNumber
 					ORDER BY TOTO.TranLineItemID
--  					ORDER BY split_part(TOTO.tranlineitemid,'-',5)
--							,split_part(TOTO.tranlineitemid,'-',4)
--							,split_part(TOTO.tranlineitemid,'-',3)
--							,split_part(TOTO.tranlineitemid,'-',2)
--							,split_part(TOTO.tranlineitemid,'-',1)
 					) AS RowNum,
 CASE PBVBCBT.EntryMethodID
  WHEN 'Manual' THEN 0
  WHEN 'BetSlip' THEN 2
  WHEN 'Edit' THEN 10
  WHEN 'EBetSlip' THEN 16
 END AS EntryMethod,
 NB.NumBoards,
 ND.NumDraws,
 ND.DrawID,
 PBVBCBT.DeviceID AS ebetslipinfo_mobnbr,
 CASE
  WHEN TOTO.BetTypeID = ANY(v_totomatchBettypes) THEN 1
  WHEN TOTO.BetTypeID = 'ORD' THEN 1
  WHEN TOTO.BetTypeID = 'SYSR' THEN 44
  WHEN TOTO.BetTypeID = 'SYS7' THEN 7
  WHEN TOTO.BetTypeID = 'SYS8' THEN 28
  WHEN TOTO.BetTypeID = 'SYS9' THEN 84
  WHEN TOTO.BetTypeID = 'SYS10' THEN 210
  WHEN TOTO.BetTypeID = 'SYS11' THEN 462
  WHEN TOTO.BetTypeID = 'SYS12' THEN 924
 END AS NumSimpleBets,
 TOTO.QuickPickIndicator :: int AS QPFlag,
 TOTOSEQ.Seq AS NumMarks,
 NB.BulkID,
 cast(TOTO.Units as varchar) AS itoto_numparts ,
 cast(TOTO.SyndicatePartsAllowed as varchar) AS itoto_totalparts,
 cast(TOTOGROUP.GroupToto as varchar) AS grptoto_numparts

 FROM ubt_temp_placebettransaction_type PBVBCBT
 INNER JOIN public.ztubt_toto_placedbettransactionlineitem   TOTO ON PBVBCBT.TranHeaderID = TOTO.TranHeaderID
 INNER JOIN ubt_temp_TotoSeq TOTOSEQ ON TOTO.TranLineItemID = TOTOSEQ.TranLineItemID
 LEFT JOIN ubt_Temp_TotoGroup TOTOGROUP ON TOTO.GroupHostID = TOTOGROUP.GroupHostID --(3)
 INNER JOIN ubt_temp_NumBoards NB ON PBVBCBT.TicketSerialNumber = NB.TicketSerialNumber
 INNER JOIN ubt_temp_numdraws ND ON PBVBCBT.TicketSerialNumber = ND.TicketSerialNumber
 WHERE PBVBCBT.ProdID = 3 and PBVBCBT.TransactionTypeTotal in (3,5)

 union all

 --SWEEP

 SELECT PBVBCBT.TicketSerialNumber,
 SWEEP.TranLineItemID,
 ROW_NUMBER() OVER (PARTITION BY PBVBCBT.TicketSerialNumber
 					ORDER BY SWEEP.TranLineItemID
--  					ORDER BY split_part(SWEEP.tranlineitemid,'-',5)
--							,split_part(SWEEP.tranlineitemid,'-',4)
--							,split_part(SWEEP.tranlineitemid,'-',3)
--							,split_part(SWEEP.tranlineitemid,'-',2)
--							,split_part(SWEEP.tranlineitemid,'-',1)
 					) AS RowNum,
 CASE PBVBCBT.EntryMethodID
  WHEN 'Manual' THEN 0
  WHEN 'BetSlip' THEN 2
  WHEN 'Edit' THEN 10
  WHEN 'EBetSlip' THEN 16
 END AS EntryMethod,
 NB.NumBoards,
 ND.NumDraws,
 ND.DrawID,
 PBVBCBT.DeviceID AS ebetslipinfo_mobnbr,
 1 AS NumSimpleBets,
 SWEEP.QuickpickIndicator :: int AS QPFlag,
 LENgth(CAST(UserSelectedNumber AS VARCHAR)) As NumMarks,
 NB.BulkID,
 NULL AS itoto_numparts,
 NULL AS itoto_totalparts,
 NULL AS grptoto_numparts
 FROM ubt_temp_placebettransaction_type PBVBCBT
 INNER JOIN public.ztubt_sweep_placedbettransactionlineitem   SWEEP ON PBVBCBT.TranHeaderID = SWEEP.TranHeaderID
 INNER JOIN public.ztubt_sweep_placedbettransactionlineitemnumber  PBTLIN ON SWEEP.TranLineItemID = PBTLIN.TranLineItemID
 INNER JOIN ubt_temp_NumBoards NB ON PBVBCBT.TicketSerialNumber = NB.TicketSerialNumber
INNER JOIN ubt_temp_numdraws ND ON PBVBCBT.TicketSerialNumber = ND.TicketSerialNumber
 WHERE PBVBCBT.ProdID = 4  AND IsSoldOut <> true and
PBTLIN.CreatedDate >= StartDateUTC And PBTLIN.CreatedDate <= EndDateUTC AND
  PBVBCBT.TransactionTypeTotal In (1, 4, 6)

  union all

  SELECT PBVBCBT.TicketSerialNumber,
 SWEEP.TranLineItemID,
 ROW_NUMBER() OVER (PARTITION BY PBVBCBT.TicketSerialNumber
 					ORDER BY SWEEP.TranLineItemID
--  					ORDER BY split_part(SWEEP.tranlineitemid,'-',5)
--							,split_part(SWEEP.tranlineitemid,'-',4)
--							,split_part(SWEEP.tranlineitemid,'-',3)
--							,split_part(SWEEP.tranlineitemid,'-',2)
--							,split_part(SWEEP.tranlineitemid,'-',1)
 					) AS RowNum,
 CASE PBVBCBT.EntryMethodID
  WHEN 'Manual' THEN 0
  WHEN 'BetSlip' THEN 2
  WHEN 'Edit' THEN 10
  WHEN 'EBetSlip' THEN 16
 END AS EntryMethod,
 NB.NumBoards,
 ND.NumDraws,
 ND.DrawID,
 PBVBCBT.DeviceID AS ebetslipinfo_mobnbr,
 1 AS NumSimpleBets,
 SWEEP.QuickpickIndicator :: int AS QPFlag,
 LENgth(CAST(UserSelectedNumber AS VARCHAR)) As NumMarks,
 NB.BulkID,
 NULL AS itoto_numparts,
 NULL AS itoto_totalparts,
 NULL AS grptoto_numparts
 FROM ubt_temp_placebettransaction_type PBVBCBT
 INNER JOIN public.ztubt_sweep_placedbettransactionlineitem   SWEEP ON PBVBCBT.TranHeaderID = SWEEP.TranHeaderID
 INNER JOIN public.ztubt_sweep_placedbettransactionlineitemnumber  PBTLIN ON SWEEP.TranLineItemID = PBTLIN.TranLineItemID
 INNER JOIN ubt_temp_NumBoards NB ON PBVBCBT.TicketSerialNumber = NB.TicketSerialNumber
INNER JOIN ubt_temp_numdraws ND ON PBVBCBT.TicketSerialNumber = ND.TicketSerialNumber
 WHERE PBVBCBT.ProdID = 4  AND IsSoldOut <> true and PBVBCBT.TransactionTypeTotal In (3,5)

;

create index idx_ubt_lotterydetail_ticketsrno  on ubt_Temp_LotteryDetail(TicketSerialNumber);
create index idx_ubt_lotterydetail_tranlineitemid  on ubt_Temp_LotteryDetail(TranLineItemID);
 -----------------------------------------------------------------
 --DETAIL OF SPORTS
 -----------------------------------------------------------------
 create temporary table if not exists ubt_temp_Liveindidcator
 (
  TicketSerialNumber VARCHAR(500),
  LiveIndicator CHAR(1)
 )
  ;
 INSERT INTO ubt_temp_Liveindidcator
 SELECT din.TicketSerialNumber,
 CASE
  WHEN (din.HasBetInRun = TRUE) AND (din.CreatedDate BETWEEN din.StartTime AND din.SuspendTime) THEN 'Y'
  ELSE 'N'
 END AS LiveIndicator
 FROM
 (
  SELECT ROW_NUMBER() OVER (PARTITION BY SPBN.TranHeaderID
  							ORDER BY SPBN.TranLineItemID
--		  					ORDER BY split_part(SPBN.tranlineitemid,'-',5)
--									,split_part(SPBN.tranlineitemid,'-',4)
--									,split_part(SPBN.tranlineitemid,'-',3)
--									,split_part(SPBN.tranlineitemid,'-',2)
--									,split_part(SPBN.tranlineitemid,'-',1)
  							) AS ROWNUM,
  PBVBCBT.TicketSerialNumber, SE.HasBetInRun, SE.StartTime, SE.SuspendTime, SPBN.CreatedDate
  FROM ubt_temp_placebettransaction_type PBVBCBT
  INNER JOIN public.ztubt_sports_placedbettransactionlineitemnumber  SPBN ON PBVBCBT.TranHeaderID = SPBN.TranHeaderID
  INNER join public.ztubt_sportevent SE ON SPBN.EventID = SE.EventId
  WHERE PBVBCBT.ProdID IN(5,6)
 )din
 WHERE din.ROWNUM = 1
;
create index idx_ubt_liveindicator_ticketsrno  on ubt_temp_Liveindidcator(TicketSerialNumber);

 create temporary table if not exists ubt_temp_sportsdetail
 (
  TicketSerialNumber VARCHAR(500),
  TranLineItemID Varchar(50),--(1)
  EntryMethod INT4,
  EventID VARCHAR(500),
  LeagueCode VARCHAR(500),
  LiveIndicator CHAR(1),
  EventName text,
  BetTypeName text,
  Selection VARCHAR(500),
  SelectionOdds VARCHAR(500),
  StartTime VARCHAR(500),
  BetType VARCHAR(500)
 )
  ;
 INSERT INTO ubt_temp_sportsdetail
 SELECT PBVBCBT.TicketSerialNumber,
 PLN.TranLineItemID,
 CASE PBVBCBT.EntryMethodID
  WHEN 'Manual' THEN 0
  WHEN 'BetSlip' THEN 2
  WHEN 'Edit' THEN 10
  WHEN 'EBetSlip' THEN 16
 END AS EntryMethod,
 PLN.EventID AS EventID,
 SL.LeagueCode AS LeagueCode,
 LI.LiveIndicator :: char AS LiveIndicator,
 PLN.EventName AS EventName,
 PLN.BetTypeName AS BetTypeName,
 PLN.SelectionName AS Selection,
 PLN.SelectionOdds AS SelectionOdds,
 --to_char(SE.StartTime, 'yyyyMMdd HHmmss') AS StartTime, b.BetTypeID
to_char(SE.StartTime, 'yyyyMMdd HH24miss') AS StartTime, b.BetTypeID
 FROM ubt_temp_placebettransaction_type PBVBCBT
 INNER JOIN public.ztubt_sports_placedbettransactionlineitemnumber  PLN ON PBVBCBT.TranHeaderID = PLN.TranHeaderID
 INNER JOIN public.ztubt_sportevent  SE ON PLN.EventID = SE.EventId
 INNER JOIN public.ztubt_sporttype  ST ON SE.TypeId = ST.TypeId
 INNER JOIN public.ztubt_sportleagueinfo  SL ON REPLACE(ST.Name, '|', '') = REPLACE(SL.Name, '|', '')
 INNER JOIN ubt_temp_Liveindidcator LI ON PBVBCBT.TicketSerialNumber = LI.TicketSerialNumber
 inner join ubt_temp_bettype b on b.TranLineItemNumberID = PLN.TranLineItemID
 LEFT JOIN public.ztubt_entrymethod  EM ON PBVBCBT.EntryMethodID = EM.EntryMethodID
 WHERE PBVBCBT.ProdID IN (5,6)
 GROUP BY PBVBCBT.TicketSerialNumber, PLN.TranLineItemID, PBVBCBT.EntryMethodID, PLN.EventID, SL.LeagueCode,
 LI.LiveIndicator, PLN.EventName, PLN.BetTypeName, PLN.SelectionOdds, SE.StartTime,  b.BetTypeID, PLN.SelectionName
 ;

create index idx_ubt_sportdetail_ on ubt_temp_sportsdetail(TicketSerialNumber,TranLineItemID);


 -------------------------------------------------------------------------
 --GET TRANSACTION (COLLECTION, VALIDATION, CANCELLATION) PER AD HOC TIME
 -------------------------------------------------------------------------
 create temporary table if not exists ubt_temp_trans
 (
  EntryMethodID VARCHAR(10),  --(S5)
  ProdID int4,  --(S5)
  IsBetRejectedByTrader bool, --(S5)
  TicketSerialNumber VARCHAR(200),
  UUID VARCHAR(33),
  TransactionType INT4,
  TransactionDate timestamp,
  TerminalID VARCHAR(100),
  UserID VARCHAR(100),
  LocationID VARCHAR(200),
  CartID varchar(50),
  TranHeaderID varchar(50)
 )
   ;
-------COLLECTION--------------

 --HORSE RACING
  INSERT INTO ubt_temp_trans
  SELECT
  PB.EntryMethodID,
  PB.ProdID,  --(S5)
  PB.IsBetRejectedByTrader , --(S5)
  PB.TicketSerialNumber AS TSN,
  PB.RequestID AS UUID,
  1 AS TransactionType, --Wager
  ( PB.CreatedDate + interval '8 hour'),
 PB.TerDisplayID AS TerminalID,  PB.UserDisplayID AS UserID,  L.LocDisplayID AS LocationID,
  PB.CartID,
  PB.TranHeaderID
  FROM ubt_temp_placebettransaction PB
  INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.terdisplayid
  LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
  WHERE PB.ProdID  in (1, 2, 3, 4, 5, 6) and
   PB.TransactionType = 1
 AND  (PB.CreatedDate >= StartDateUTC And PB.CreatedDate <= EndDateUTC)

  --ProdID = 1 --HORSE RACING
  --ProdId=2,3,4 Lottery
  --prodid=5,6 sports

  ------------VALIDATION -----------



union all
--Horse Racing

SELECT
 PB.EntryMethodID, --(S5)
 PB.ProdID,  --(S5)
 PB.IsBetRejectedByTrader, --(S5)
 zv.TicketSerialNumber AS TSN,
 PB.RequestID AS UUID, --(5)
 3 AS TransactionType, --Validation
 zv.ValidationDate,
 zv.TerDisplayID AS TerminalID,
 zv.UserDisplayID AS UserID,
 L.LocDisplayID AS LocationID,
 zv.CartID,
 zv.TranHeaderID
 FROM  public.ztubt_validatedbetticket zv
 INNER JOIN ubt_temp_placebettransaction PB ON zv.TicketSerialNumber = PB.TicketSerialNumber
 INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
 LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
 WHERE PB.ProdID = 1
 AND (coalesce (zv.WinningAmount,0) > 0 OR coalesce (zv.RebateReclaim,0) > 0)
 AND  (zv.CreatedValidationDate >= StartDateUTC And zv.CreatedValidationDate <= EndDateUTC)
 AND PB.TransactionType = 3

 --Lottery(prod id (2,3,4)  & Sports prodid (5,6)
 union all


 SELECT
 PB.EntryMethodID, --(S5)
 PB.ProdID,  --(S5)
 PB.IsBetRejectedByTrader, --(S5)
 zv.TicketSerialNumber AS TSN,
 zv.RequestID AS UUID, --(5)
 3 AS TransactionType, --Validation
 zv.ValidationDate,
 zv.TerDisplayID AS TerminalID,
 zv.UserDisplayID AS UserID,
 L.LocDisplayID AS LocationID,
 zv.CartID,
 zv.TranHeaderID
 FROM  public.ztubt_validatedbetticket zv
 INNER JOIN ubt_temp_placebettransaction PB ON zv.TicketSerialNumber = PB.TicketSerialNumber
 INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
 LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
 WHERE PB.ProdID IN (2,3,4)
 AND coalesce (zv.WinningAmount,0) > 0 AND zv.ValidationTypeID = 'VALD'
 AND  (zv.CreatedValidationDate >= StartDateUTC And zv.CreatedValidationDate <= EndDateUTC)
 AND PB.TransactionType = 3

 union all

 SELECT
 PB.EntryMethodID, --(S5)
 PB.ProdID,  --(S5)
 PB.IsBetRejectedByTrader, --(S5)
 zv.TicketSerialNumber AS TSN,
 PB.RequestID AS UUID, --(5)
 3 AS TransactionType, --Validation
 zv.ValidationDate,
 zv.TerDisplayID AS TerminalID,
 zv.UserDisplayID AS UserID,
 L.LocDisplayID AS LocationID,
 zv.CartID,
 zv.TranHeaderID
 FROM  public.ztubt_validatedbetticket zv
 INNER JOIN ubt_temp_placebettransaction PB ON zv.TicketSerialNumber = PB.TicketSerialNumber
 INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
 LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
 WHERE PB.ProdID in (5,6)
 AND coalesce (zv.WinningAmount,0) > 0 AND zv.ValidationTypeID = 'VALD' -- Issue:Fixed on 20220221
 AND  (zv.CreatedValidationDate >= StartDateUTC And zv.CreatedValidationDate <= EndDateUTC)
 AND PB.TransactionType = 3



union all
--refund

SELECT
 PB.EntryMethodID, --(S5)
 PB.ProdID,  --(S5)
 PB.IsBetRejectedByTrader, --(S5)
 zv.TicketSerialNumber AS TSN,
 PB.RequestID AS UUID, --(5)
 61 AS TransactionType, --Validation
 zv.ValidationDate,
 zv.TerDisplayID AS TerminalID,
 zv.UserDisplayID AS UserID,
 L.LocDisplayID AS LocationID,
 zv.CartID,
 zv.TranHeaderID
 FROM  public.ztubt_validatedbetticket zv
 INNER JOIN ubt_temp_placebettransaction PB ON zv.TicketSerialNumber = PB.TicketSerialNumber
 INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
 LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
 WHERE PB.ProdID IN (5, 6)
 AND  zv.ValidationTypeID = 'RFND'
 AND  (zv.CreatedValidationDate >= StartDateUTC And zv.CreatedValidationDate <= EndDateUTC)
 AND PB.TransactionType = 3

---- ----CANCELLATION-------------------
 union all
 --Horse racing prodid (1) & Lottery prodid (2,3,4)
 SELECT
 PB.EntryMethodID, --(S5)
 PB.ProdID,  --(S5)
 PB.IsBetRejectedByTrader, --(S5)
 cbt.TicketSerialNumber AS TSN,
 PB.RequestID AS UUID, --(5)
 2 AS TransactionType, --Cancellation
 cbt.CancelledDate,
 cbt.TerDisplayID AS TerminalID,
 cbt.UserDisplayID AS UserID,
 L.LocDisplayID AS LocationID,
 cbt.CartID,
 cbt.TranHeaderID
 FROM  public.ztubt_cancelledbetticket  cbt
 INNER JOIN ubt_temp_placebettransaction PB ON cbt.TicketSerialNumber = PB.TicketSerialNumber
 INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
 LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
 WHERE PB.ProdID =1 --HORSE RACING
 AND PB.TransactionType = 5
 AND (cbt.CancelledDate >= StartDate And cbt.CancelledDate <= EndDate)

 union all
 SELECT
 PB.EntryMethodID, --(S5)
 PB.ProdID,  --(S5)
 PB.IsBetRejectedByTrader, --(S5)
 cbt.TicketSerialNumber AS TSN,
 cbt.RequestID AS UUID, --(5)
 2 AS TransactionType, --Cancellation
 cbt.CancelledDate,
 cbt.TerDisplayID AS TerminalID,
 cbt.UserDisplayID AS UserID,
 L.LocDisplayID AS LocationID,
 cbt.CartID,
 cbt.TranHeaderID
 FROM  public.ztubt_cancelledbetticket  cbt
 INNER JOIN ubt_temp_placebettransaction PB ON cbt.TicketSerialNumber = PB.TicketSerialNumber
 INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
 LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
 WHERE PB.ProdID in (2,3,4)  -- Lottery
 AND PB.TransactionType = 5
 AND (cbt.CancelledDate >= StartDate And cbt.CancelledDate <= EndDate)

union all
 --Sports

  SELECT
 PB.EntryMethodID, --(S5)
 PB.ProdID,  --(S5)
 PB.IsBetRejectedByTrader, --(S5)
 cbt.TicketSerialNumber AS TSN,
 PB.RequestID AS UUID, --(5)
 2 AS TransactionType, --Cancellation
 cbt.CancelledDate,
 cbt.TerDisplayID AS TerminalID,
 cbt.UserDisplayID AS UserID,
 L.LocDisplayID AS LocationID,
 cbt.CartID,
 cbt.TranHeaderID
 FROM  public.ztubt_cancelledbetticket  cbt
 INNER JOIN ubt_temp_placebettransaction PB ON cbt.TicketSerialNumber = PB.TicketSerialNumber
 INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
 LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
 WHERE PB.ProdID in (5,6)   --SPORTS
 AND PB.TransactionType = 5
 AND PB.IsBetRejectedByTrader = false
 AND (cbt.CancelledDate >= StartDate And cbt.CancelledDate <= EndDate)

 ;

create index idx_ubt_trans_ticketserialnumber on ubt_temp_trans(TicketSerialNumber);
create index idx_ubt_trans_tranheaderid on ubt_temp_trans(TranHeaderID);
  --FINAL RESULT OF TRANSACTIONAL VIEW
 -----------------------------------------------------------------


 create temporary table if not exists ubt_temp_transactionamount(
  TicketSerialNumber VARCHAR(500),
  TranLineItemID varchar(50),
  Wager numeric(22,2),
  SecondWager numeric(22,2),
  Sales numeric(22, 11),
  SecondSales numeric(22, 11),
  SalesComm numeric(22, 11),
  SecondSalesComm numeric(22, 11),
  GST numeric(22, 11),
  SecondGST numeric(22, 11),
  ReturnAmount  numeric(22,2),
  WinningAmount  numeric(22,2),
  RefundAmount  numeric(22,2),
  RebateReclaim  numeric(22,2)
 )

 ;
 --COLLECTION


 create temporary table if not exists ubt_temp_resultwager
(
ProdID int4,
 TicketSerialNumber VARCHAR(200),
TranLineItemID varchar(50),
 Wager numeric(22,2),
 SecondWager numeric(22,2)
)
;
 insert into ubt_temp_resultwager
 select 1::int4 ProdID, amtw.TicketSerialNumber, null , SUM(amtw.Amount) AS Wager, 0 AS SecondWager
 FROM
(
SELECT   PBVBCB.TicketSerialNumber, PBTL.TranHeaderID,
CASE WHEN BetTypeID = 'W-P'
THEN (coalesce (PBTL.BetPriceAmount,0))
ELSE SUM(coalesce (PBTL.BetPriceAmount,0))
END AS Amount,
COUNT(DISTINCT PBVBCB.TicketSerialNumber) Ct, 'COL' AS Flag,'CH' AS TransType
FROM
ubt_temp_placebettransaction_type PBVBCB
INNER JOIN  public.ztubt_horse_placedbettransactionlineitem  PBTL ON PBVBCB.TranHeaderID = PBTL.TranHeaderID
WHERE PBVBCB.ProdID = 1
and ((PBTL.CreatedDate >= StartDateUTC And PBTL.CreatedDate <= EndDateUTC
AND PBVBCB.TransactionTypeTotal In (1, 4, 6) ) or PBVBCB.TransactionTypeTotal in (3,5))

GROUP BY  PBVBCB.TicketSerialNumber, PBTL.TranHeaderID, BetTypeID, BetPriceAmount
)amtw
GROUP BY amtw.TicketSerialNumber

union all

--4D

SELECT  2::int4 ProdID, PBT.TicketSerialNumber, zdp.TranLineItemID,
  SUM(coalesce (zdp.BigBetAcceptedWager,0)) AS Wager, SUM(coalesce (zdp.SmallBetAcceptedWager,0)) AS SecondWager
FROM
ubt_temp_placebettransaction_type PBT
INNER JOIN public.ztubt_4d_placedbettransactionlineitemnumber zdp  ON zdp.TranHeaderID = PBT.TranHeaderID
INNER JOIN public.ztubt_drawdates zd   ON zd.TranHeaderID = zdp.TranHeaderID
WHERE  PBT.ProdID = 2
	AND PBT.IsExchangeTicket = FALSE
	AND ((zdp.CreatedDate >=StartDateUTC And zdp.CreatedDate <= EndDateUTC
	AND PBT.TransactionTypeTotal In (1, 4, 6) ) or PBT.TransactionTypeTotal In (3,5))

GROUP BY PBT.TicketSerialNumber, zdp.TranLineItemID


union all
--toto

SELECT  3::int4 ProdID, PBT.TicketSerialNumber, zdp.TranLineItemID,
  SUM(coalesce (zdp.BetPriceAmount,0)) AS Wager, 0 AS SecondWager
FROM
ubt_temp_placebettransaction_type PBT
INNER JOIN public.ztubt_toto_placedbettransactionlineitem  zdp  ON zdp.TranHeaderID = PBT.TranHeaderID

WHERE  PBT.ProdID = 3
	AND PBT.IsExchangeTicket = FALSE
	AND ((zdp.CreatedDate >= StartDateUTC And zdp.CreatedDate <= EndDateUTC
	AND PBT.TransactionTypeTotal In (1, 4, 6) ) or PBT.TransactionTypeTotal In (3,5))

GROUP BY PBT.TicketSerialNumber, zdp.TranLineItemID

union all

--sweep

SELECT   4::int4 ProdID, PBT.TicketSerialNumber, zspn.TranLineItemID, SUM(coalesce (zspn.BetPriceAmount,0)) AS Wager, 0 AS SecondWager
FROM
ubt_temp_placebettransaction_type PBT
INNER JOIN public.ztubt_sweep_placedbettransactionlineitem zsp   ON PBT.TranHeaderID = zsp.TranHeaderID
INNER JOIN public.ztubt_sweep_placedbettransactionlineitemnumber zspn   ON zsp.TranLineItemID = zspn.TranLineItemID AND zspn.IsSoldOut = False
INNER JOIN public.ztubt_placedbettransactionheaderlifecyclestate zplc   ON zplc.TranHeaderID = PBT.TranHeaderID
INNER JOIN (select terdisplayid,Locid from public.ztubt_terminal ) Ter  ON PBT.TerDisplayID = Ter.TerDisplayID
INNER JOIN (Select * from  public.ztubt_location) Loc  ON Ter.LocID = Loc.LocID
WHERE ( ((zsp.IsPrinted IS NOT NULL OR Loc.SweepIndicator IS NULL) AND zplc.BetStateTypeID = 'PB06' )
		OR	( Loc.SweepIndicator IS NOT NULL  And zsp.IsPrinted IS NULL AND zplc.BetStateTypeID = 'PB03' ) )
AND PBT.ProdID=4
AND ((zspn.CreatedDate >= StartDateUTC And zspn.CreatedDate <= EndDateUTC
	AND PBT.TransactionTypeTotal In (1, 4, 6) ) or PBT.TransactionTypeTotal In (3,5))

GROUP BY PBT.TicketSerialNumber, zspn.TranLineItemID

union all

--sports

SELECT   FIN.ProdID ProdID, FIN.TicketSerialNumber, NULL, SUM(coalesce (FIN.BetAmount,0)) AS Wager, 0 AS SecondWager
FROM
(
select PBT.ProdID, PBT.TranHeaderID, SourceSystemTransactionID, IsSingleBet,
	AccumulatorID, BetAmount, TicketSerialNumber
FROM
ubt_temp_placebettransaction_type PBT
INNER JOIN public.ztubt_sports_placedbettransactionlineitem zsp  ON PBT.TranHeaderID = zsp.TranHeaderID
WHERE
PBT.ProdID IN (5, 6) AND PBT.IsBetRejectedByTrader != TRUE
GROUP BY PBT.ProdID, PBT.TranHeaderID, SourceSystemTransactionID, IsSingleBet, AccumulatorID, BetAmount, TicketSerialNumber
)FIN
GROUP BY FIN.TicketSerialNumber, FIN.ProdID
;

create index idx_ubt_resultwager_TicketSerialNumber  on ubt_temp_resultwager(TicketSerialNumber);
create index idx_ubt_resultwager_tranlineitemid  on ubt_temp_resultwager(TranLineItemID);

--For Sales Comm, Sales Factor and Output GST
create temporary table if not exists ubt_temp_resultsalescomwithdate
(
 TicketSerialNumber VARCHAR(200),
 TranLineItemID varchar(50),
 SalesCommAmount numeric(32, 12),
 SalesFactorAmount numeric(32, 12),
 SecondSalesCommAmount numeric(32, 12),
 SecondSalesFactorAmount numeric(32, 12),
 GSTRate numeric(10, 2)
)  ;

--Toto

insert into ubt_temp_resultsalescomwithdate
select PBT.TicketSerialNumber, zdp.TranLineItemID, SUM(coalesce (zdp.SalesCommAmount,0)) SalesCommAmount,
SUM(coalesce (zdp.SalesFactorAmount,0)) SalesFactorAmount, 0 , 0
FROM
	ubt_temp_placebettransaction_type PBT
	Inner jOin public.ztubt_toto_placedbettransactionlineitem  zdp   ON PBT.TranHeaderID = zdp.TranHeaderID
WHERE  PBT.ProdID = 3
AND PBT.IsExchangeTicket = FALSE
AND (
  (
    zdp.CreatedDate >= StartDateUTC
  And zdp.CreatedDate <=EndDateUTC
AND  PBT.TransactionTypeTotal In (1, 4, 6)
  )
  or PBT.TransactionTypeTotal In (3,5)
  )

GROUP BY PBT.TicketSerialNumber, zdp.TranLineItemID

union all
--horse

SELECT amt.TicketSerialNumber, NULL  , SUM(amt.SalesCommAmount) , SUM(amt.SalesFactorAmount)  , 0 , 0
FROM
(
 SELECT   PBT.TicketSerialNumber, zhp.TranHeaderID,
 CASE WHEN BetTypeID = 'W-P'
  THEN (coalesce (zhp.SalesCommAmount,0))
  ELSE SUM(coalesce (zhp.SalesCommAmount,0))
 END AS SalesCommAmount,
 CASE WHEN BetTypeID = 'W-P'
  THEN (coalesce (zhp.SalesFactorAmount,0))
  ELSE SUM(coalesce (zhp.SalesFactorAmount,0))
 END AS SalesFactorAmount
 FROM
	 ubt_temp_placebettransaction_type PBT
	 INNER JOIN  public.ztubt_horse_placedbettransactionlineitem  zhp ON PBT.TranHeaderID = zhp.TranHeaderID
	 INNER JOIN (select Terdisplayid,LocID from public.ztubt_terminal zt) Ter ON PBT.TerDisplayID = Ter.TerDisplayID --AND TER.IsDeleted = 0
	 where PBT.ProdID = 1 and
	 (
	 	(zhp.CreatedDate >= StartDateUTC
	 	 And zhp.CreatedDate <= EndDateUTC
	  	 AND PBT.TransactionTypeTotal In (1, 4, 6))
	  or
	  	(PBT.TransactionTypeTotal In (3, 5))
	 )
 GROUP BY PBT.TicketSerialNumber, zhp.TranHeaderID, zhp.CreatedDate::Date
 , BetTypeID, SalesCommAmount, SalesFactorAmount
)amt
GROUP BY amt.TicketSerialNumber


union all

--4D

select PBT.TicketSerialNumber, zdp.TranLineItemID, SUM(coalesce (zdp.SalesCommAmountBig,0)) SalesCommAmount,
 SUM(coalesce (zdp.SalesFactorAmountBig,0)) SalesFactorAmount, SUM(coalesce (zdp.SalesCommAmountSmall,0)) SecondSalesCommAmount
, SUM(coalesce (zdp.SalesFactorAmountSmall,0)) SecondSalesFactorAmount
 FROM
	 ubt_temp_placebettransaction_type PBT
	 INNER JOIN  public.ztubt_4d_placedbettransactionlineitemnumber zdp   ON zdp.TranHeaderID = PBT.TranHeaderID
	 INNER JOIN  public.ztubt_drawdates zd ON zd.TranHeaderID = PBT.TranHeaderID --(7)
 where PBT.ProdID = 2 and
  ((zdp.CreatedDate >= StartDateUTC  And zdp.CreatedDate <=EndDateUTC
   AND PBT.TransactionTypeTotal In (1, 4, 6)) or PBT.TransactionTypeTotal In (3,5))
  AND PBT.IsExchangeTicket = false
 GROUP BY PBT.TicketSerialNumber, zdp.TranLineItemID

union all

--sweeep

SELECT  PBT.TicketSerialNumber, zspn.TranLineItemID, SUM(coalesce (zspn.SalesCommAmount,0)) SalesCommAmount,
SUM(coalesce (zspn.SalesFactorAmount,0)) SalesFactorAmount, 0, 0
FROM
ubt_temp_placebettransaction_type PBT
INNER JOIN public.ztubt_sweep_placedbettransactionlineitem zsp   ON PBT.TranHeaderID = zsp.TranHeaderID
INNER JOIN public.ztubt_sweep_placedbettransactionlineitemnumber zspn   ON zsp.TranLineItemID = zspn.TranLineItemID AND zspn.IsSoldOut = FALSE
INNER JOIN public.ztubt_placedbettransactionheaderlifecyclestate zpls   ON zpls.TranHeaderID = PBT.TranHeaderID
 INNER JOIN (Select terdisplayid, Locid from public.ztubt_terminal ) Ter ON PBT.TerDisplayID = Ter.TerDisplayID
 INNER JOIN (Select locid,locdisplayid,sweepindicator from public.ztubt_location ) Loc ON Ter.LocID = Loc.LocID
WHERE PBT.ProdID=4  and
( ((zsp.IsPrinted IS NOT NULL OR Loc.SweepIndicator IS NULL) AND zpls.BetStateTypeID = 'PB06' )
	OR	( Loc.SweepIndicator IS NOT NULL  And zsp.IsPrinted IS NULL AND zpls.BetStateTypeID = 'PB03' ) )
and
((zspn.CreatedDate >= StartDateUTC And zspn.CreatedDate <= EndDateUTC

	AND PBT.TransactionTypeTotal In (1, 4, 6) ) or PBT.TransactionTypeTotal In (3,5))

GROUP BY PBT.TicketSerialNumber, zspn.TranLineItemID

union all
--Sports

select amt.TicketSerialNumber, NULL , SUM(coalesce (amt.SalesCommAmount,0)) SalesCommAmount,
SUM(coalesce (amt.SalesFactorAmount,0)) SalesFactorAmount, 0 , 0
FROM
(
 SELECT PBT.ProdID ProductName, PBT.TranHeaderID, SourceSystemTransactionID, IsSingleBet,
	AccumulatorID, SalesCommAmount, TicketSerialNumber, SalesFactorAmount
 FROM
ubt_temp_placebettransaction_type PBT
INNER JOIN  public.ztubt_sports_placedbettransactionlineitem zsp   ON PBT.TranHeaderID = zsp.TranHeaderID
WHERE PBT.ProdID IN (5,6)
 AND
 PBT.IsBetRejectedByTrader=false
 GROUP BY PBT.ProdID, PBT.TranHeaderID, SourceSystemTransactionID, IsSingleBet,
 AccumulatorID, SalesCommAmount, TicketSerialNumber, (zsp.CreatedDate :: date), SalesFactorAmount
)amt
GROUP BY amt.TicketSerialNumber
;

create index idx_ubt_resulsalescomwithdate_ticket on ubt_temp_resultsalescomwithdate(TicketSerialNumber);
create index idx_ubt_resulsalescomwithdate_tranlineitemid on ubt_temp_resultsalescomwithdate(TranLineItemID);


 create temporary table if not exists ubt_temp_resultsalesandcomm
(
 TicketSerialNumber VARCHAR(200),
 TranLineItemID varchar(50),
 Sales numeric(32, 12),
 SecondSales numeric(32, 12),
 SalesComm numeric(22, 11),
 SecondSalesComm numeric(22, 11)
)
  ;
INSERT INTO ubt_temp_resultsalesandcomm
SELECT FIN.TicketSerialNumber, FIN.TranLineItemID, SUM(FIN.SalesFactorAmount),
SUM(FIN.SecondSalesFactorAmount), SUM(FIN.SalesCommAmount), SUM(FIN.SecondSalesCommAmount)
FROM ubt_temp_resultsalescomwithdate FIN
GROUP BY FIN.TicketSerialNumber, FIN.TranLineItemID

;

create index idx_ubt_resulsalescom_ticket on ubt_temp_resultsalesandcomm(TicketSerialNumber);
create index idx_ubt_resulsalescom_tranlineitemid on ubt_temp_resultsalesandcomm(TranLineItemID);

---------------------------------------------------------
--GST (OUTPUT GST = WAGER - SALES)
---------------------------------------------------------
create temporary table if not exists ubt_temp_resultGST
(
TicketSerialNumber VARCHAR(200),
TranLineItemID varchar(50),
GST numeric(22, 11),
SecondGST numeric(22, 11)
) ;

--LOTTERY(2,3,4) and Horse(1) and Sports(5,6)

INSERT INTO ubt_temp_resultGST
SELECT W.TicketSerialNumber, W.TranLineItemID, ROUND(coalesce (W.Wager,0) - coalesce (S.Sales,0) ,2),
ROUND(coalesce (W.SecondWager,0) - coalesce (S.SecondSales,0), 2)
FROM ubt_temp_resultwager W
INNER JOIN ubt_temp_placebettransaction_type PBT ON W.TicketSerialNumber = PBT.TicketSerialNumber
INNER JOIN ubt_temp_resultsalesandcomm S ON W.TicketSerialNumber = S.TicketSerialNumber AND w.TranLineItemID = S.TranLineItemID
WHERE w.ProdID IN (2, 3, 4)
union all
SELECT W.TicketSerialNumber, W.TranLineItemID, ROUND(coalesce (W.Wager,0) - coalesce (S.Sales,0) ,2),
ROUND(coalesce (W.SecondWager,0) - coalesce (S.SecondSales,0), 2)
FROM ubt_temp_resultwager W
INNER JOIN ubt_temp_placebettransaction_type PBT ON W.TicketSerialNumber = PBT.TicketSerialNumber
INNER JOIN ubt_temp_resultsalesandcomm S ON W.TicketSerialNumber = S.TicketSerialNumber
WHERE w.ProdID IN (1,5,6)

;

create index idx_ubt_resultGST_ticket on ubt_temp_resultGST(TicketSerialNumber);
create index idx_ubt_resultGST_tranlineitemid on ubt_temp_resultGST(TranLineItemID);

---------------------------------------------------------
--VALIDATION
---------------------------------------------------------
create temporary table if not exists ubt_temp_resultvalidation
(
TicketSerialNumber VARCHAR(200),
ReturnAmount numeric(22,2),
WinningAmount numeric(22,2),
RefundAmount numeric(22,2),
RebateReclaim numeric(22,2)
)

;
INSERT INTO ubt_temp_resultvalidation
SELECT zv.TicketSerialNumber,
zv.WinningAmount AS ReturnAmount,
CASE WHEN coalesce(zv.WinningAmount, 0) > 0 THEN
zv.WinningAmount - (SUM(RW.Wager)/coalesce (COUNT(RW.TicketSerialNumber),1)) - (SUM(RW.SecondWager)/coalesce(COUNT(RW.TicketSerialNumber),1)) --(6)
ELSE  0

END AS WinningAmount, --(4)
zv.RefundAmount, zv.RebateReclaim --(1)
FROM ubt_temp_resultwager RW
 INNER join public.ztubt_validatedbetticket zv   ON RW.TicketSerialNumber = zv.TicketSerialNumber
 INNER JOIN public.ztubt_validatedbetticketlifecyclestate zvlc   ON zv.TranHeaderID = zvlc.TranHeaderID AND zvlc.BetStateTypeID = 'VB06'
 WHERE zv.ValidationTypeID IN ('VALD', 'RFND') --(1)
 GROUP BY zv.TicketSerialNumber, zv.WinningAmount, zv.RefundAmount, zv.RebateReclaim

 ;

create index idx_ubt_resultvalidation_ticket on ubt_temp_resultvalidation(TicketSerialNumber);
 ---------------------------------------------------------
--VALIDATION EXCHANGE TICKET
---------------------------------------------------------

create temporary table if not exists ubt_temp_resultvalidationexchange
(
TicketSerialNumber VARCHAR(200),
TranLineItemID varchar(50),
WinningAmount numeric(22,2),
RefundAmount numeric(22,2),
RebateReclaim numeric(22,2)
)
;

INSERT INTO ubt_temp_resultvalidationexchange
select vb.TicketSerialNumber, zubt4d.TranLineItemID, vb.WinningAmount, vb.RefundAmount, vb.RebateReclaim
FROM  public.ztubt_validatedbetticket vb
INNER JOIN public.ztubt_4d_placedbettransactionlineitem zubt4d   ON vb.TranHeaderID = zubt4d.TranHeaderID
INNER JOIN public.ztubt_validatedbetticketlifecyclestate zvls   ON vb.TranHeaderID = zvls.TranHeaderID AND zvls.BetStateTypeID = 'VB06'
INNER JOIN ubt_temp_placebettransaction_type PBT ON vb.TranHeaderID = PBT.TranHeaderID
Where vb.ProdID=2 And PBT.IsExchangeTicket= true

union all

select vb.TicketSerialNumber, zubt4d.TranLineItemID, vb.WinningAmount, vb.RefundAmount, vb.RebateReclaim
FROM  public.ztubt_validatedbetticket vb
INNER JOIN public.ztubt_toto_placedbettransactionlineitem  zubt4d   ON vb.TranHeaderID = zubt4d.TranHeaderID
INNER JOIN public.ztubt_validatedbetticketlifecyclestate zvls   ON vb.TranHeaderID = zvls.TranHeaderID AND zvls.BetStateTypeID = 'VB06'
INNER JOIN ubt_temp_placebettransaction_type PBT ON vb.TranHeaderID = PBT.TranHeaderID
Where vb.ProdID=3 And PBT.IsExchangeTicket= TRUE

union all

select vb.TicketSerialNumber, zubt4d.TranLineItemID, vb.WinningAmount, vb.RefundAmount, vb.RebateReclaim
FROM  public.ztubt_validatedbetticket vb
INNER JOIN public.ztubt_sweep_placedbettransactionlineitem  zubt4d   ON vb.TranHeaderID = zubt4d.TranHeaderID
INNER JOIN public.ztubt_validatedbetticketlifecyclestate zvls   ON vb.TranHeaderID = zvls.TranHeaderID AND zvls.BetStateTypeID = 'VB06'
INNER JOIN ubt_temp_placebettransaction_type PBT ON vb.TranHeaderID = PBT.TranHeaderID
Where vb.ProdID=4 And PBT.IsExchangeTicket= TRUE

;

create index idx_ubt_resultvalidationexc_ticket on ubt_temp_resultvalidationexchange(TicketSerialNumber);
create index idx_ubt_resultvalidationexc_tranlinetiemid on ubt_temp_resultvalidationexchange(TranLineItemID);
---------------------------------------------------------
--FINAL RESULT
---------------------------------------------------------

--HORSE RACING(1), lOTEERY(2,3,4), SPORTS(5,6)
 INSERT INTO ubt_temp_transactionamount
SELECT W.TicketSerialNumber, W.TranLineItemID, SUM(W.Wager), SUM(W.SecondWager), SUM(SC.Sales), SUM(SC.SecondSales), SUM(SC.SalesComm), SUM(SC.SecondSalesComm), SUM(G.GST), SUM(G.SecondGST), MAX(V.ReturnAmount), MAX(V.WinningAmount), MAX(V.RefundAmount), MAX(V.RebateReclaim) --(1)   --(S4)
FROM ubt_temp_resultwager W
LEFT JOIN ubt_temp_resultsalesandcomm  SC ON W.TicketSerialNumber = SC.TicketSerialNumber AND w.TranLineItemID = SC.TranLineItemID  --(S4)
LEFT JOIN ubt_temp_resultGST G ON W.TicketSerialNumber = G.TicketSerialNumber AND W.TranLineItemID = G.TranLineItemID
LEFT JOIN ubt_temp_resultvalidation V ON W.TicketSerialNumber = V.TicketSerialNumber
WHERE ProdID IN (2, 3, 4)
GROUP BY W.TicketSerialNumber, W.TranLineItemID

union all
SELECT W.TicketSerialNumber, null, SUM(W.Wager), SUM(W.SecondWager), SUM(SC.Sales), SUM(SC.SecondSales), SUM(SC.SalesComm), SUM(SC.SecondSalesComm), SUM(G.GST), SUM(G.SecondGST), MAX(V.ReturnAmount), MAX(V.WinningAmount), MAX(V.RefundAmount), MAX(V.RebateReclaim) --(1)   --(S4)
FROM ubt_temp_resultwager W
LEFT JOIN ubt_temp_resultsalesandcomm  SC ON W.TicketSerialNumber = SC.TicketSerialNumber --AND w.TranLineItemID = SC.TranLineItemID  --(S4)
LEFT JOIN ubt_temp_resultGST G ON W.TicketSerialNumber = G.TicketSerialNumber --AND W.TranLineItemID = G.TranLineItemID
LEFT JOIN ubt_temp_resultvalidation V ON W.TicketSerialNumber = V.TicketSerialNumber
WHERE ProdID IN (1, 5,6)
GROUP BY W.TicketSerialNumber


union all

select TicketSerialNumber, TranLineItemID, 0 AS Wager, 0 AS SecondWager, 0 AS Sales, 0 AS SecondSales,
0 AS SalesCom, 0 AS SecondSalesCom, 0 AS GST, 0 AS SecondGST, WinningAmount, WinningAmount, RefundAmount, RebateReclaim
FROM ubt_temp_resultvalidationexchange

;

create index idx_ubt_transactionamt_ticket on ubt_temp_transactionamount(TicketSerialNumber);
create index idx_ubt_transactionamt_tranlineitemid on ubt_temp_transactionamount(TranLineItemID);

create temporary table if not exists ubt_temp_transactiondata
(
		--transid bigint  ,--primary key clustered,
		uuid varchar(33) ,
		transactiontype int4 ,
		productid int4 ,
		bettypeid varchar(2) ,
		selection text ,
		businessdate date ,
		transactiontimestamp timestamp ,
		terminalid varchar(100) ,
		userid varchar(100) ,
		locationid varchar(200) ,
		cartid varchar(50) ,
		tranheaderid varchar(50) ,
		boardseqnumber varchar(50) ,
		entrymethod int4 ,
		numboards int4 ,
		numdraws int ,
		drawid varchar(500) ,
		ebetslipinfo_mobnbr varchar(100) ,
		numsimplebets int4 ,
		qpflag int4 ,
		nummarks int4 ,
		bulkid varchar(50) ,
		itoto_numparts text ,
		itoto_totalparts text ,
		grptoto_numparts text ,
		eventid varchar(500) ,
		leaguecode varchar(500) ,
		liveindicator char(1) ,
		eventname text ,
		market text ,
		odds varchar(500) ,
		matchkickoff varchar(500) ,
		wager1 numeric(22, 2) ,
		wager2 numeric(22, 2) ,
		sales1 numeric(22, 11) ,
		sales2 numeric(22, 11) ,
		salescomm1 numeric(22, 11) ,
		salescomm2 numeric(22, 11) ,
		gst1 numeric(22, 11) ,
		gst2 numeric(22, 11) ,
		returnamount numeric(22, 2) ,
		winningamount numeric(22, 2) ,
		betlinerebateamount numeric(22, 2) ,
		paymentmode varchar(2)
	)
;
create temporary table if not exists ubt_temp_final_lotteryselect
(
	ticketserialnumber varchar(500),
	tranlineitemid varchar(50),
	selection text
)
;
 --HORSE RACING

INSERT INTO ubt_temp_final_lotteryselect
SELECT PBT.TicketSerialNumber, NULL AS TranLineItemID,
 string_agg(CAST(PL.BetLineAnnotation AS VARCHAR)  ,',') AS BetLineAnnotation
 FROM ubt_temp_placebettransaction_type PBT
 INNER JOIN public.ztubt_horse_placedbettransactionlineitem PL ON PBT.TranHeaderID = PL.TranHeaderID
 WHERE  PBT.ProdID=1 and
 ((PL.CreatedDate >= StartDateUTC And PL.CreatedDate <=EndDateUTC
 And PBT.TransactionTypeTotal In (1, 4, 6)) or PBT.TransactionTypeTotal In (3,5))
 GROUP BY PBT.TicketSerialNumber, PBT.TranHeaderID

--4D
 union all

 select TicketSerialNumber, TranLineItemID, (pbtln.SelectedBetNumber :: varchar(10))
from ubt_temp_placebettransaction_type PBT
inner join public.ztubt_4d_placedbettransactionlineitemnumber  pbtln on PBT.TranHeaderID = pbtln.TranHeaderID
WHERE PBT.ProdID = 2 and
((pbtln.CreatedDate >= StartDateUTC And pbtln.CreatedDate <=EndDateUTC
And PBT.TransactionTypeTotal In (1, 4, 6)) or  PBT.TransactionTypeTotal In (3,5))


union all
--toto
select TicketSerialNumber, pbtln.TranLineItemID, string_agg((ztp.singleBetNumber :: varchar(10)),' ')
from ubt_temp_placebettransaction_type PBT
inner join public.ztubt_toto_placedbettransactionlineitem   pbtln on PBT.TranHeaderID = pbtln.TranHeaderID
inner join public.ztubt_toto_placedbettransactionlineitemnumber ztp on pbtln.tranlineitemid =ztp.tranlineitemid
WHERE PBT.ProdID = 3 and
((pbtln.CreatedDate >= StartDateUTC And pbtln.CreatedDate <=EndDateUTC
And PBT.TransactionTypeTotal In (1, 4, 6)) or  PBT.TransactionTypeTotal In (3,5))
group by TicketSerialNumber, pbtln.TranLineItemID

--sweep
union all

select TicketSerialNumber, pbtln.TranLineItemID, string_agg((pbtln.selectednumber :: varchar(10)),' ')
from ubt_temp_placebettransaction_type PBT
inner join public.ztubt_sweep_placedbettransactionlineitemnumber  pbtln on pbtln.tranheaderid =PBT.tranheaderid
WHERE PBT.ProdID = 4 and
((pbtln.CreatedDate >= StartDateUTC And pbtln.CreatedDate <=EndDateUTC
And PBT.TransactionTypeTotal In (1, 4, 6)) or  PBT.TransactionTypeTotal In (3,5))
group by TicketSerialNumber, pbtln.TranLineItemID


--sports

union all

select pb.ticketserialnumber,
 pln.tranlineitemid as tranlineitemid,
 pln.selectionname as selection
 from ubt_temp_placebettransaction_type ph
 inner join public.ztubt_sports_placedbettransactionlineitemnumber  pln  on ph.tranheaderid = pln.tranheaderid
 inner join ubt_temp_placebettransaction_type pb on ph.ticketserialnumber = pb.ticketserialnumber --(2)
 where pb.prodid in (5, 6)
 group by pb.ticketserialnumber, pln.tranlineitemid, pln.selectionname

;

create index idx_ubt_lotteryselect_ticket on ubt_temp_final_lotteryselect(ticketserialnumber);
create index idx_ubt_lotteryselect_tranlineitemid on ubt_temp_final_lotteryselect(tranlineitemid);
 --=====================================================================================
---Transaction Data Interface table-----------------------------------------------------
 --=====================================================================================

 --LOTTERY (Per UUID and boards under one TSN)
 insert into ubt_temp_transactiondata
 select
 t.uuid,  t.transactiontype,
 case t.prodid
  when 1 then 7--horse racing
  when 2 then 9--4d
  when 4 then 11--sweep
  when 3 then 23--toto 6/49
  when 5 then 20--football/motor racing
  when 6 then 20--football/motor racing
 end as productid,
 bt.bettypeid as bettypeid,  s.selection as selection,
 case when t.transactiondate < igt_startdate then igt_previousbusinessdate
 when t.transactiondate > igt_enddate then igt_nextbusinessdate
 else igt_currentbusinessdate end as businessdate,
 t.transactiondate as transactiontimestamp,
 t.terminalid,   t.userid, t.locationid, t.cartid, t.tranheaderid,
 -----lottery section
 coalesce(concat(t.uuid , ' - ' ,right(concat('00' , cast (ld.boardseqnumber as varchar)),2)), t.uuid) as boardseqnumber,
 ld.entrymethod,   ld.numboards, ld.numdraws, ld.drawid,ld.ebetslipinfo_mobnbr, ld.numsimplebets,ld.qpflag,
 ld.nummarks,   ld.bulkid,  ld.itoto_numparts,ld.itoto_totalparts, ld.grptoto_numparts,

 -----sports section
 null, null, null, null, null, null, null,

 -----value amount section
 coalesce(amt.wager, 0) as wager,
 coalesce(amt.secondwager,0) as secondwager,
 coalesce(amt.sales, 0) as sales,
 coalesce(amt.secondsales,0) as secondsales,
 coalesce(amt.salescomm,0) as salescomm,
 coalesce(amt.secondsalescomm,0) as secondsalescomm,
 coalesce(amt.gst,0) as gst,
 coalesce(amt.secondgst,0) as secondgst,
 case when transactiontype = 3 then   amt.returnamount else 0  end as returnamount,
 case when transactiontype = 3 then   amt.winningamount else 0 end as winningamount,
  null,
 case when t.transactiontype = 3 and coalesce(amt.returnamount, 0) > 0 then 'CA' else '' end as paymentmode
 from ubt_temp_trans t
 left join ubt_temp_bettype bt on t.ticketserialnumber = bt.ticketserialnumber
 left join ubt_Temp_LotteryDetail ld on t.ticketserialnumber = ld.ticketserialnumber and bt.tranlineitemnumberid = ld.tranlineitemid
 left join ubt_temp_final_lotteryselect s on t.ticketserialnumber = s.ticketserialnumber and ld.tranlineitemid = s.tranlineitemid
 left join ubt_temp_transactionamount amt on t.ticketserialnumber = amt.ticketserialnumber and ld.tranlineitemid = amt.tranlineitemid
 where t.prodid in (2,3,4) and bettypeid is not null and t.isbetrejectedbytrader = false
 and (t.transactiondate between startdate and enddate)

 order by t.uuid
 ;
 insert into ubt_temp_transactiondata
-- union all

 --HORSE RACING (Per UUID)

 select
 t.uuid,  t.transactiontype,
 case t.prodid
  when 1 then 7--horse racing
  when 2 then 9--4d
  when 4 then 11--sweep
  when 3 then 23--toto 6/49
  when 5 then 20--football/motor racing
  when 6 then 20--football/motor racing
 end as productid,
 bt.bettypeid as bettypeid, --(19) bt.bettypeid as bettypeid,
 s.selection as selection,
  case when t.transactiondate < bmcs_startdate then bmcs_previousbusinessdate
	  when t.transactiondate > bmcs_enddate then bmcs_nextbusinessdate
	  else bmcs_currentbusinessdate end as businessdate,
 t.transactiondate as transactiontimestamp,
 t.terminalid,  t.userid,t.locationid,t.cartid, t.tranheaderid,

 -----lottery section
 t.uuid as boardseqnumber,
 case t.entrymethodid
  WHEN 'MANUAL' THEN 0
  WHEN 'BETSLIP' THEN 2
  WHEN 'EDIT' THEN 10
  WHEN 'EBETSLIP' THEN 16
 end as entrymethod,
 null,null,null,null,null,null,null,null,null,null,null,

 -----sports section
 null,null,null,null,null,null,null,

 -----value amount section
 coalesce(amt.wager, 0) as wager,
 coalesce(amt.secondwager,0) as secondwager,
 coalesce(amt.sales, 0) as sales,
 coalesce(amt.secondsales,0) as secondsales,
 coalesce(amt.salescomm,0) as salescomm,
 coalesce(amt.secondsalescomm,0) as secondsalescomm,
 coalesce(amt.gst,0) as gst,
 coalesce(amt.secondgst,0) as secondgst,
 case when transactiontype = 3 then amt.returnamount else 0 end as returnamount,
 case when transactiontype = 3 then amt.winningamount else 0 end as winningamount,
 case when t.transactiontype = 3 then amt.rebatereclaim else coalesce(hd.betlinerebateamount,0)   end as betlinerebateamount,
 case when t.transactiontype = 3 and coalesce(amt.returnamount, 0) > 0 then 'CA' else '' end as paymentmode
 from ubt_temp_trans t
 left join ubt_temp_bettype bt on t.ticketserialnumber = bt.ticketserialnumber
 left join ubt_temp_HorseDetail hd on t.ticketserialnumber = hd.ticketserialnumber
 left join ubt_temp_final_lotteryselect s on t.ticketserialnumber = s.ticketserialnumber
 left join ubt_temp_transactionamount amt on t.ticketserialnumber = amt.ticketserialnumber
 where t.prodid = 1 and t.isbetrejectedbytrader = false and (t.transactiondate::timestamp between startdate and enddate)
 order by t.uuid

 ; insert into ubt_temp_transactiondata
-- union all
  --SPORTS (Per UUID)  For Bet Placement

 select
 t.uuid,   t.transactiontype,
 case t.prodid
  when 1 then 7--horse racing
  when 2 then 9--4d
  when 4 then 11--sweep
  when 3 then 23--toto 6/49
  when 5 then 20--football/motor racing
  when 6 then 20--football/motor racing
 end as productid,
 sd.bettype as bettypeid,
 sd.selection as selection,
  case when t.transactiondate < ob_startdate then ob_previousbusinessdate
 when t.transactiondate > ob_enddate then ob_nextbusinessdate
 else ob_currentbusinessdate end as businessdate,
 t.transactiondate as transactiontimestamp,
 t.terminalid,   t.userid,   t.locationid,   t.cartid,   t.tranheaderid,
 -----lottery section
 t.uuid as boardseqnumber,   sd.entrymethod,
 null,null,null,null,null,null,null,null,null,null,null,
  -----sports section
 sd.eventid,   sd.leaguecode,   sd.liveindicator,   sd.eventname,   sd.bettypename,
 sd.selectionodds,   sd.starttime,
 -----value amount section
 amt.wager as wager,
 amt.secondwager as secondwager,
 pbtli.salesfactoramount as sales,
 amt.secondsales as secondsales,
 coalesce(amt.salescomm, 0) as salescomm,
 coalesce(amt.secondsalescomm,0) as secondsalescomm,
 coalesce(amt.gst,0) as gst,
 amt.secondgst as secondgst,
 case when transactiontype = 3 then amt.returnamount when transactiontype = 61 then amt.refundamount  else 0 end as returnamount,
 case when transactiontype = 3 then amt.winningamount   else 0 end as winningamount,
  null as betlinerebateamount,
 case when t.transactiontype = 3 and coalesce(amt.returnamount, 0) > 0 then 'CA' else '' end as paymentmode
 from ubt_temp_trans t
 inner join   public.ztubt_sports_placedbettransactionlineitem  pbtli on t.tranheaderid = pbtli.tranheaderid
 inner join ubt_temp_placebettransaction_type pbvbcbtdis on t.tranheaderid = pbvbcbtdis.tranheaderid
 left join ubt_temp_sportsdetail sd on t.ticketserialnumber = sd.ticketserialnumber and pbtli.tranlineitemid = sd.tranlineitemid
 left join ubt_temp_transactionamount amt on t.ticketserialnumber = amt.ticketserialnumber
 where
pbvbcbtdis.transactiontypetotal in (1,4,6)
 and t.prodid in (5, 6) and t.isbetrejectedbytrader = false and ( t.transactiondate between startdate and enddate)
 order by t.uuid
 ;
  insert into ubt_temp_transactiondata
-- union all

select
 t.uuid,  t.transactiontype,
 case t.prodid
  when 1 then 7--horse racing
  when 2 then 9--4d
  when 4 then 11--sweep
  when 3 then 23--toto 6/49
  when 5 then 20--football/motor racing
  when 6 then 20--football/motor racing
 end as productid,
 sd.bettype as bettypeid,   sd.selection as selection,
 case when t.transactiondate < ob_startdate then ob_previousbusinessdate
 when t.transactiondate > ob_enddate then ob_nextbusinessdate
 else ob_currentbusinessdate end as businessdate,
 t.transactiondate as transactiontimestamp,
 t.terminalid,   t.userid,   t.locationid,   t.cartid,   t.tranheaderid,
 -----lottery section
 t.uuid as boardseqnumber,   sd.entrymethod,
 null,null,null,null,null,null,null,null,null,null,null,
  -----sports section
 sd.eventid,   sd.leaguecode,   sd.liveindicator,   sd.eventname,   sd.bettypename,
 sd.selectionodds,   sd.starttime,
 -----value amount section
 amt.wager as wager,
 amt.secondwager as secondwager,
 pbtli.salesfactoramount as sales,
 amt.secondsales as secondsales,
 coalesce(amt.salescomm, 0) as salescomm,
 coalesce(amt.secondsalescomm,0) as secondsalescomm,
 coalesce(amt.gst,0) as gst,
 amt.secondgst as secondgst,
 case when transactiontype = 3 then amt.returnamount when transactiontype = 61 then amt.refundamount else 0  end as returnamount,
 case when transactiontype = 3 then amt.winningamount else 0  end as  winningamount,
 null as betlinerebateamount,
 case when t.transactiontype = 3 and coalesce(amt.returnamount, 0) > 0 then 'CA' else '' end as paymentmode
 from ubt_temp_trans t
 inner join public.ztubt_sports_placedbettransactionlineitem  pbtli on t.tranheaderid = pbtli.tranheaderid
 inner join ubt_temp_placebettransaction_type pbvbcbtdis on t.tranheaderid = pbvbcbtdis.tranheaderid
 left join ubt_temp_sportsdetail sd on t.ticketserialnumber = sd.ticketserialnumber and pbtli.tranlineitemid = sd.tranlineitemid
 left join ubt_temp_transactionamount amt on t.ticketserialnumber = amt.ticketserialnumber
 where pbvbcbtdis.transactiontypetotal in (3,5)
 and t.prodid in (5, 6) and t.isbetrejectedbytrader = false and (t.transactiondate between startdate and enddate)
 order by t.uuid

 ;
create index idx_ubt_transdata_uuid on  ubt_temp_transactiondata(uuid );
create index idx_ubt_transdata_productid  on  ubt_temp_transactiondata(productid );
create index idx_ubt_transdata_selection  on  ubt_temp_transactiondata(selection );
create index idx_ubt_transdata_businessdate  on  ubt_temp_transactiondata(businessdate );
create index idx_ubt_transdata_tranheaderid  on  ubt_temp_transactiondata(tranheaderid  );

--Interface table output-------
return query
	SELECT UUID ,
	TRANHEADERID,
	TRANSACTIONTYPE,
	PRODUCTID,
	BETTYPEID,
	SELECTION,
	BUSINESSDATE,
	--TRANSACTIONTIMESTAMP,
	to_char(TRANSACTIONTIMESTAMP,'yyyy-MM-dd HH24:mi:ss') ::timestamp as TRANSACTIONTIMESTAMP, -- For FR include second 04 Apr 2022
	TERMINALID,
	USERID,
	LOCATIONID,
	BOARDSEQNUMBER,
	coalesce(ENTRYMETHOD,0),
	coalesce(NUMBOARDS,0),
	coalesce(NUMDRAWS,0),
	coalesce(DRAWID,''),
	coalesce(EBETSLIPINFO_MOBNBR,''),
	coalesce(NUMSIMPLEBETS,0),
	coalesce(QPFLAG,0),
	coalesce(NUMMARKS,0),
	coalesce(BULKID,'') ,
	coalesce(ITOTO_NUMPARTS,''),
	ITOTO_TOTALPARTS,
	GRPTOTO_NUMPARTS,
	EVENTID,
	LEAGUECODE,
	LIVEINDICATOR,
	EVENTNAME,
	MARKET,
	ODDS,
	MATCHKICKOFF,
	WAGER1,
	WAGER2,
	SALES1,
	SALES2,
	SALESCOMM1,
	SALESCOMM2,
	GST1,
	coalesce(GST2,0),
	RETURNAMOUNT,
	WINNINGAMOUNT,
	PAYMENTMODE,
	CARTID ,
	coalesce(BETLINEREBATEAMOUNT,0)
			from ubt_temp_transactiondata
			where TransactionTimestamp >= StartDate
			and TransactionTimestamp <= EndDate
	 ;

drop table 	ubt_temp_placebettransaction_type;
drop table	ubt_temp_LotteryDraw;
drop table	ubt_temp_numdraws;
drop table	ubt_Temp_LotteryDetail;
drop table	ubt_temp_NumBoards;
drop table	ubt_temp_HorseDetail;
drop table	ubt_temp_TotoSeq;
drop table	ubt_Temp_TotoGroup;
drop table	ubt_temp_Liveindidcator;
drop table	ubt_temp_sportsdetail;
drop table	ubt_temp_placebettransaction;
drop table	ubt_temp_bettype;
drop table	ubt_temp_trans;
drop table	ubt_temp_transactionamount;
drop table	ubt_temp_resultwager;
drop table	ubt_temp_resultsalescomwithdate;
drop table	ubt_temp_resultsalesandcomm ;
drop table	ubt_temp_resultGST;
drop table	ubt_temp_resultvalidation;
drop table	ubt_temp_resultvalidationexchange;
drop table	ubt_temp_transactiondata;
drop table	ubt_temp_final_lotteryselect;

END;
$$;


ALTER FUNCTION public.sp_ubt_gettransactiondataforinterval(startdatetime timestamp without time zone, enddatetime timestamp without time zone) OWNER TO sp_postgres;
