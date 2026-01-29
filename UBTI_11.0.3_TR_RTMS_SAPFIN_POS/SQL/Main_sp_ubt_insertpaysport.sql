
CREATE FUNCTION public.sp_ubt_insertpaysports(tdate date) RETURNS TABLE(businessdate date, producttype character varying, prizeamount numeric, refundamount numeric, txndate date)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column

	   declare
		ParamDate timestamp;
		PreviousDateTime timestamp;
		PeriodDateTime timestamp;
		ActualDate date;
	    HQLocation VARCHAR(200) = '008888';

	   begin

	     create temp table if not exists ubt_temp_table(
		    PrizeAmount numeric(18, 2),
		    RefundAmount numeric(18, 2),
		    TxnDate timestamp
	     );

	    ParamDate := tdate;
        SELECT
		 PreviousPeriodDateTime, PeriodDateTime, ActualDate
		into PreviousDateTime, PeriodDateTime, ActualDate
	    FROM ztubt_getbusinessdate_perhost zgp
	    WHERE ActualDate = ParamDate AND Host = 3 ;

	   INSERT into ubt_temp_table
	    SELECT
		 SUM(coalesce (VBT.WinningAmount, 0)) AS PrizeAmount, --(1)
		 SUM(coalesce (VBT.RefundAmount, 0)) AS RefundAmount, --(1)
		 (C.CartCreatedDate + interval'8 hour'):: date As TxnDate
	     FROM ztubt_cart C
		 INNER JOIN ztubt_validatedbetticket VBT ON VBT.CartID = C.CartID
		 INNER JOIN ztubt_placedbettransactionheader PBTH ON PBTH.TicketSerialNumber = VBT.TicketSerialNumber
		 INNER JOIN ztubt_product P ON PBTH.ProdID = P.ProdID
		 INNER JOIN ztubt_validationtype VT ON VBT.ValidationTypeID = VT.ValidationTypeID
		 INNER JOIN ztubt_validatedbetticketlifecyclestate VBLC ON VBLC.TranHeaderID = PBTH.TranHeaderID AND VBLC.BetStateTypeID = 'VB06'
		 INNER JOIN ztubt_terminal TER ON C.TerDisplayID = TER.TerDisplayID
		 INNER JOIN ztubt_location L ON TER.LocID = L.LocID
	     WHERE
		  L.LocDisplayID = HQLocation
		   AND (C.CartCreatedDate + interval'8 hour')::timestamp BETWEEN (PreviousDateTime::timestamp) AND (PeriodDateTime::timestamp)
		   AND P.ProdID IN (5, 6)
	     GROUP BY
         (C.CartCreatedDate + interval'8 hour'):: date ;

	   return QUERY
	    SELECT
		 ActualDate AS BusinessDate,
		 '2'::varchar AS ProductType,
		 SUM(coalesce (PrizeAmount, 0))AS PrizeAmount,
		 SUM(coalesce (RefundAmount, 0)) AS RefundAmount,
		 (TxnDate::date) As TxnDate
	     FROM ubt_temp_table
	     GROUP BY (TxnDate::date);

	    drop table ubt_temp_table;

	END;
$$;


ALTER FUNCTION public.sp_ubt_insertpaysports(tdate date) OWNER TO consultant01;
