
with ubt_temp_placebettransaction as
(
SELECT TicketSerialNumber, PB.TranHeaderID, EntryMethodID, DeviceID, ProdID, IsBetRejectedByTrader,IsExchangeTicket

 ,TerDisplayID,PB.CreatedDate,RequestID,UserDisplayID,CartID, 1  TransactionType
  FROM public.ztubt_placedbettransactionheader PB
  INNER JOIN public.ztubt_placedbettransactionheaderlifecyclestate  PBLC ON PB.TranHeaderID = PBLC.TranHeaderID AND PBLC.BetStateTypeID = 'PB06'
  WHERE
	(PB.CreatedDate >= '2025-09-22 16:00:00' And PB.CreatedDate <= '2025-09-23 16:00:00')

  Union
  SELECT VB.TicketSerialNumber, VB.TranHeaderID, PB.EntryMethodID, PB.DeviceID, PB.ProdID, PB.IsBetRejectedByTrader
  ,PB.IsExchangeTicket,PB.TerDisplayID,PB.CreatedDate,PB.RequestID,VB.UserDisplayID,VB.CartID, 3 TransactionType
  FROM public.ztubt_validatedbetticket  VB
  INNER JOIN public.ztubt_placedbettransactionheader PB ON VB.TicketSerialNumber = PB.TicketSerialNumber
  INNER JOIN public.ztubt_validatedbetticketlifecyclestate VBLC ON VB.TranHeaderID = VBLC.TranHeaderID AND VBLC.BetStateTypeID = 'VB06'
  WHERE VB.ValidationTypeID IN ('VALD', 'RFND')
  AND   (VB.CreatedValidationDate >= '2025-09-22 16:00:00' And VB.CreatedValidationDate <= '2025-09-23 16:00:00')
  Union
  SELECT CB.TicketSerialNumber, CB.TranHeaderID, PB.EntryMethodID, PB.DeviceID, PB.ProdID, PB.IsBetRejectedByTrader,

   PB.IsExchangeTicket,PB.TerDisplayID,PB.CreatedDate,PB.RequestID,CB.UserDisplayID,CB.CartID,5 TransactionType

  FROM public.ztubt_CancelledBetTicket CB
  INNER JOIN public.ztubt_placedbettransactionheader PB ON CB.TicketSerialNumber = PB.TicketSerialNumber
  INNER JOIN public.ztubt_cancelledbetticketlifecyclestate  CBLC ON CBLC.TranHeaderID = PB.TranHeaderID AND CBLC.BetStateTypeID = 'CB06'
  WHERE (PB.IsBetRejectedByTrader ) = FALSE AND (CB.CancelledDate >= '2025-09-23 00:00:00' And CB.CancelledDate <= '2025-09-24 00:00:00')
)
, ubt_temp_placebettransaction_type AS (
  Select TicketSerialNumber, TranHeaderID, EntryMethodID, DeviceID, ProdID,
   IsBetRejectedByTrader,IsExchangeTicket, TerDisplayID
  ,SUM(TransactionType) TransactionTypeTotal
  from ubt_temp_placebettransaction
  Group By TicketSerialNumber, TranHeaderID, DeviceID, EntryMethodID, ProdID,
  IsExchangeTicket, TerDisplayID, IsBetRejectedByTrader
),


 ubt_temp_NumBoards AS

(


 SELECT DISTINCT PBT.TicketSerialNumber,
 COUNT(z4dln.TranLineItemID) AS NumBoards,
 NULL AS BulkID
 FROM ubt_temp_placebettransaction_type PBT
 INNER JOIN   public.ztubt_4d_placedbettransactionlineitemnumber  z4dln ON PBT.TranHeaderID = z4dln.TranHeaderID
 WHERE PBT.ProdID = 2 and  coalesce(z4dln.BigBetAcceptedWager, 0) + coalesce(z4dln.SmallBetAcceptedWager, 0) > 0 AND
 ((z4dln.CreatedDate >= '2025-09-22 16:00:00' And z4dln.CreatedDate <= '2025-09-23 16:00:00'
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
 ((pbtl.createddate >= '2025-09-22 16:00:00' and pbtl.createddate <= '2025-09-23 16:00:00' and
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
 ((PLN.CreatedDate >= '2025-09-22 16:00:00' And PLN.CreatedDate <= '2025-09-23 16:00:00'
 AND PBT.TransactionTypeTotal In (1, 4, 6)) or PBT.TransactionTypeTotal In (3,5))

 GROUP BY PBT.TicketSerialNumber, BulkID

),

    ubt_temp_LotteryDraw AS (
        Select PBTT.TicketSerialNumber,
CAST(zdm.HostDrawDatesId AS VARCHAR) HostDrawDatesId

	FROM ubt_temp_placebettransaction_type PBTT
	INNER JOIN public.ztubt_drawdates zdm  ON PBTT.TranHeaderID = zdm.TranHeaderID
	Where  PBTT.ProdID In (2,3,4)
    )
, ubt_temp_numdraws AS (
    SELECT
	LD.TicketSerialNumber,
	Count(1) NumDraws,
	string_agg(CAST(LD.HostDrawDatesId AS varchar),',' ) as Drawid

	From ubt_temp_LotteryDraw LD
	Group By LD.TicketSerialNumber
)
,
ubt_temp_TotoSeq
        as(
            SELECT LIN.TranLineItemID, MAX(LIN.Sequence) AS Seq
 FROM ubt_temp_placebettransaction_type PBT
 inner join  public.ztubt_toto_placedbettransactionlineitem  LI on PBT.TranHeaderID = LI.TranHeaderID
 inner join public.ztubt_toto_placedbettransactionlineitemnumber  LIN on LI.TranLineItemID = LIN.TranLineItemID
 WHERE PBT.ProdID = 3 and
 ((LI.CreatedDate >= '2025-09-22 16:00:00' And LI.CreatedDate <= '2025-09-23 16:00:00'
 AND PBT.TransactionTypeTotal In (1, 4, 6)) or PBT.TransactionTypeTotal In (3,5))
 GROUP BY LIN.TranLineItemID
        )
,

ubt_Temp_TotoGroup AS(
     SELECT LI2.GroupHostID, MAX(LI2.GroupUnitSequence) AS GroupToto
 FROM ubt_temp_placebettransaction_type PBT
 inner join public.ztubt_toto_placedbettransactionlineitem  LI on PBT.TranHeaderID = LI.TranHeaderID
 inner join public.ztubt_toto_placedbettransactionlineitem LI2 on LI.GroupHostID = LI2.GroupHostID
 WHERE PBT.ProdID = 3
 AND LI.GroupHostID <> '00000000-0000-0000-0000-000000000000' AND LI.GroupUnitSequence IS NOT NULL
 GROUP BY LI2.GroupHostID
)

, ubt_Temp_LotteryDetail as (


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
   PBTLIN.CreatedDate >= '2025-09-22 16:00:00' And PBTLIN.CreatedDate <= '2025-09-23 16:00:00' AND
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
  WHEN TOTO.BetTypeID in ('M AN','M 2','M 3','M 4') THEN 1
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
 TOTO.CreatedDate >= '2025-09-22 16:00:00' And TOTO.CreatedDate <= '2025-09-23 16:00:00'
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
  WHEN TOTO.BetTypeID in ('M AN','M 2','M 3','M 4') THEN 1
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
PBTLIN.CreatedDate >= '2025-09-22 16:00:00' And PBTLIN.CreatedDate <= '2025-09-23 16:00:00' AND
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

)
, ubt_temp_Liveindidcator AS (
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

),
 ubt_temp_bettype AS (
    SELECT
 PBVBCBT.TicketSerialNumber,
 PBTLI.TranLineItemID as tranlineitemnumberid,
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
 ((PBTLIN.CreatedDate >= '2025-09-22 16:00:00' And PBTLIN.CreatedDate <= '2025-09-23 16:00:00'
 and  PBVBCBT.TransactionTypeTotal In (1, 4, 6))	or  PBVBCBT.TransactionTypeTotal In (3,5))


 union all
----TOTO


 select
 pbt.ticketserialnumber,
 pbtli.tranlineitemid tranlineitemnumberid,
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
  ((pbtli.createddate >= '2025-09-22 16:00:00' and pbtli.createddate <= '2025-09-23 16:00:00' and
  pbt.transactiontypetotal in (1, 4, 6))
 	or  pbt.transactiontypetotal in (3,5))

 union all

 ---SWEEP

 select
 pbt.ticketserialnumber,
 pbtlin.tranlineitemid as tranlineitemnumberid,
 '0' as bettypeid
 from ubt_temp_placebettransaction_type pbt
 inner join public.ztubt_sweep_placedbettransactionlineitemnumber pbtlin on pbt.tranheaderid = pbtlin.tranheaderid
 where   pbt.prodid = 4
 and issoldout  <> true and
 ((pbtlin.createddate >= '2025-09-22 16:00:00' and pbtlin.createddate <= '2025-09-23 16:00:00'
 and  pbt.transactiontypetotal in (1, 4, 6))
 	or  pbt.transactiontypetotal in (3,5))

 --SPORT
 union all
 select
 pbvbcbt.ticketserialnumber,
 pbtlin.tranlineitemid as tranlineitemnumberid,
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
)
,
ubt_temp_sportsdetail AS (
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
),

ubt_temp_trans as (

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
 AND  (PB.CreatedDate >= '2025-09-22 16:00:00' And PB.CreatedDate <= '2025-09-23 16:00:00')

  --ProdID = 1 --HORSE RACING
  --ProdId=2,3,4 Lottery
  --prodid=5,6 sports

  ------------VALIDATION -----------



-- union all
-- --Horse Racing

-- SELECT
--  PB.EntryMethodID, --(S5)
--  PB.ProdID,  --(S5)
--  PB.IsBetRejectedByTrader, --(S5)
--  zv.TicketSerialNumber AS TSN,
--  PB.RequestID AS UUID, --(5)
--  3 AS TransactionType, --Validation
--  zv.ValidationDate,
--  zv.TerDisplayID AS TerminalID,
--  zv.UserDisplayID AS UserID,
--  L.LocDisplayID AS LocationID,
--  zv.CartID,
--  zv.TranHeaderID
--  FROM  public.ztubt_validatedbetticket zv
--  INNER JOIN ubt_temp_placebettransaction PB ON zv.TicketSerialNumber = PB.TicketSerialNumber
--  INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
--  LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
--  WHERE PB.ProdID = 1
--  AND (coalesce (zv.WinningAmount,0) > 0 OR coalesce (zv.RebateReclaim,0) > 0)
--  AND  (zv.CreatedValidationDate >= '2025-09-22 16:00:00' And zv.CreatedValidationDate <= '2025-09-23 16:00:00')
--  AND PB.TransactionType = 3

--  --Lottery(prod id (2,3,4)  & Sports prodid (5,6)
--  union all


--  SELECT
--  PB.EntryMethodID, --(S5)
--  PB.ProdID,  --(S5)
--  PB.IsBetRejectedByTrader, --(S5)
--  zv.TicketSerialNumber AS TSN,
--  zv.RequestID AS UUID, --(5)
--  3 AS TransactionType, --Validation
--  zv.ValidationDate,
--  zv.TerDisplayID AS TerminalID,
--  zv.UserDisplayID AS UserID,
--  L.LocDisplayID AS LocationID,
--  zv.CartID,
--  zv.TranHeaderID
--  FROM  public.ztubt_validatedbetticket zv
--  INNER JOIN ubt_temp_placebettransaction PB ON zv.TicketSerialNumber = PB.TicketSerialNumber
--  INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
--  LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
--  WHERE PB.ProdID IN (2,3,4)
--  AND coalesce (zv.WinningAmount,0) > 0 AND zv.ValidationTypeID = 'VALD'
--  AND  (zv.CreatedValidationDate >= '2025-09-22 16:00:00' And zv.CreatedValidationDate <= '2025-09-23 16:00:00')
--  AND PB.TransactionType = 3

--  union all

--  SELECT
--  PB.EntryMethodID, --(S5)
--  PB.ProdID,  --(S5)
--  PB.IsBetRejectedByTrader, --(S5)
--  zv.TicketSerialNumber AS TSN,
--  PB.RequestID AS UUID, --(5)
--  3 AS TransactionType, --Validation
--  zv.ValidationDate,
--  zv.TerDisplayID AS TerminalID,
--  zv.UserDisplayID AS UserID,
--  L.LocDisplayID AS LocationID,
--  zv.CartID,
--  zv.TranHeaderID
--  FROM  public.ztubt_validatedbetticket zv
--  INNER JOIN ubt_temp_placebettransaction PB ON zv.TicketSerialNumber = PB.TicketSerialNumber
--  INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
--  LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
--  WHERE PB.ProdID in (5,6)
--  AND coalesce (zv.WinningAmount,0) > 0 AND zv.ValidationTypeID = 'VALD' -- Issue:Fixed on 20220221
--  AND  (zv.CreatedValidationDate >= '2025-09-22 16:00:00' And zv.CreatedValidationDate <= '2025-09-23 16:00:00')
--  AND PB.TransactionType = 3



-- union all
-- --refund

-- SELECT
--  PB.EntryMethodID, --(S5)
--  PB.ProdID,  --(S5)
--  PB.IsBetRejectedByTrader, --(S5)
--  zv.TicketSerialNumber AS TSN,
--  PB.RequestID AS UUID, --(5)
--  61 AS TransactionType, --Validation
--  zv.ValidationDate,
--  zv.TerDisplayID AS TerminalID,
--  zv.UserDisplayID AS UserID,
--  L.LocDisplayID AS LocationID,
--  zv.CartID,
--  zv.TranHeaderID
--  FROM  public.ztubt_validatedbetticket zv
--  INNER JOIN ubt_temp_placebettransaction PB ON zv.TicketSerialNumber = PB.TicketSerialNumber
--  INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
--  LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
--  WHERE PB.ProdID IN (5, 6)
--  AND  zv.ValidationTypeID = 'RFND'
--  AND  (zv.CreatedValidationDate >= '2025-09-22 16:00:00' And zv.CreatedValidationDate <= '2025-09-23 16:00:00')
--  AND PB.TransactionType = 3

-- ---- ----CANCELLATION-------------------
--  union all
--  --Horse racing prodid (1) & Lottery prodid (2,3,4)
--  SELECT
--  PB.EntryMethodID, --(S5)
--  PB.ProdID,  --(S5)
--  PB.IsBetRejectedByTrader, --(S5)
--  cbt.TicketSerialNumber AS TSN,
--  PB.RequestID AS UUID, --(5)
--  2 AS TransactionType, --Cancellation
--  cbt.CancelledDate,
--  cbt.TerDisplayID AS TerminalID,
--  cbt.UserDisplayID AS UserID,
--  L.LocDisplayID AS LocationID,
--  cbt.CartID,
--  cbt.TranHeaderID
--  FROM  public.ztubt_cancelledbetticket  cbt
--  INNER JOIN ubt_temp_placebettransaction PB ON cbt.TicketSerialNumber = PB.TicketSerialNumber
--  INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
--  LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
--  WHERE PB.ProdID =1 --HORSE RACING
--  AND PB.TransactionType = 5
--  AND (cbt.CancelledDate >= '2025-09-23 00:00:00' And cbt.CancelledDate <= '2025-09-24 00:00:00')

--  union all
--  SELECT
--  PB.EntryMethodID, --(S5)
--  PB.ProdID,  --(S5)
--  PB.IsBetRejectedByTrader, --(S5)
--  cbt.TicketSerialNumber AS TSN,
--  cbt.RequestID AS UUID, --(5)
--  2 AS TransactionType, --Cancellation
--  cbt.CancelledDate,
--  cbt.TerDisplayID AS TerminalID,
--  cbt.UserDisplayID AS UserID,
--  L.LocDisplayID AS LocationID,
--  cbt.CartID,
--  cbt.TranHeaderID
--  FROM  public.ztubt_cancelledbetticket  cbt
--  INNER JOIN ubt_temp_placebettransaction PB ON cbt.TicketSerialNumber = PB.TicketSerialNumber
--  INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
--  LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
--  WHERE PB.ProdID in (2,3,4)  -- Lottery
--  AND PB.TransactionType = 5
--  AND (cbt.CancelledDate >= '2025-09-23 00:00:00' And cbt.CancelledDate <= '2025-09-24 00:00:00')

-- union all
--  --Sports

--   SELECT
--  PB.EntryMethodID, --(S5)
--  PB.ProdID,  --(S5)
--  PB.IsBetRejectedByTrader, --(S5)
--  cbt.TicketSerialNumber AS TSN,
--  PB.RequestID AS UUID, --(5)
--  2 AS TransactionType, --Cancellation
--  cbt.CancelledDate,
--  cbt.TerDisplayID AS TerminalID,
--  cbt.UserDisplayID AS UserID,
--  L.LocDisplayID AS LocationID,
--  cbt.CartID,
--  cbt.TranHeaderID
--  FROM  public.ztubt_cancelledbetticket  cbt
--  INNER JOIN ubt_temp_placebettransaction PB ON cbt.TicketSerialNumber = PB.TicketSerialNumber
--  INNER JOIN (select terdisplayid,locid from public.ztubt_terminal) T ON PB.TerDisplayID = T.TerDisplayID
--  LEFT JOIN (select LocID, LocDisplayID from public.ztubt_location ) L ON T.LocID = L.LocID
--  WHERE PB.ProdID in (5,6)   --SPORTS
--  AND PB.TransactionType = 5
--  AND PB.IsBetRejectedByTrader = false
--  AND (cbt.CancelledDate >= '2025-09-23 00:00:00' And cbt.CancelledDate <= '2025-09-24 00:00:00')




)
select * from ubt_temp_trans