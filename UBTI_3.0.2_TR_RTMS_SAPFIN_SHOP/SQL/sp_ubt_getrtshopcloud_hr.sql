CREATE FUNCTION public.sp_ubt_getrtshopcloud_hr(inbusinessdate date, vseq integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$

declare
vTotalAmount numeric;
vl_DocDate date:=inbusinessdate+ interval '1 DAY';
vFlagIsInsert Int4:=0;
vBaseAmount numeric;
vnowDayName int4:= extract(dow from inbusinessdate);
vTotalTempAmountPerLocation int4:=0;
vInputTax varchar := (SELECT ConfigValue FROM ubt_tmp_rtshop_config where ConfigKey = 'vInputTax');
vGSTRate numeric;
vLocDisplayID text;
vOutputTax varchar;

begin

	create temporary table if not exists ubt_tmp_V2_TerminalTempData
	(
		TerDisplayID varchar(100),
		ProductName varchar(100),
		Amount numeric(32, 3),
		FLAG char(5),
		Type char(5)
	);

	create temporary table if not exists ubt_tmp_V2_LocationTempData
	(
		LocID int4,
		LocDisplayID VARCHAR(100),
		LocName VARCHAR(100),
		LocType int4,
		IsIBG bool,
		IsGST bool,
		ProductName varchar(100),
		Amount numeric(32, 3),
		FLAG varchar(20),
		Type varchar(25),
		IsHQ bool
	);

	create temporary table if not exists ubt_tmp_V2_TSNWagerSalesAmountData
	(
		TerDisplayID varchar(100),
		LocID int4,
		LocType int4,
		Wager numeric(22, 2),
		Sales numeric(22, 11)
	);

-- GST Config
SELECT COALESCE(gstrate , 0)/100 INTO vGSTRate
FROM ztubt_gstconfig zg
WHERE inbusinessdate BETWEEN zg.effectivefrom AND COALESCE(zg.enddate , 'infinity'::date)
ORDER BY zg.effectivefrom
LIMIT 1;
-- GST config for Gate Admission
select 'S3' into vOutputTax;

insert into ubt_tmp_V2_TerminalTempData
select
	TerDisplayID,
	ProductName,
	Amount,
	FLAG,
	TransType
	from ubt_tmp_V2_TAD_FirstDate
	where
	(FLAG IN ('CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL') AND (ProductName IN ('HORSE RACING','Gate Admission')))
	OR FLAG NOT IN ('CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL');

insert into ubt_tmp_V2_LocationTempData
	SELECT Loc.LocID, Loc.LocDisplayID, Loc.LocName, Loc.LocTypeID, Loc.IsIBG, Loc.IsGST, T.ProductName, 	MAX(T.Amount), T.FLAG, T.Type, Loc.IsHQ
	FROM ubt_tmp_V2_TerminalTempData T
		INNER JOIN ztubt_terminal Ter ON T.TerDisplayID = Ter.TerDisplayID
		INNER JOIN ztubt_location Loc ON Ter.LocID = Loc.LocID
		WHERE FLAG IN ('FUN','REC')
		GROUP BY Loc.LocID, Loc.LocDisplayID, Loc.LocName, Loc.LocTypeID, Loc.IsIBG, Loc.IsGST,T.ProductName,
		T.FLAG, T.Type, Loc.IsHQ
	union all
	SELECT l.LocID, l.LocDisplayID, l.LocName, l.LocTypeID, l.IsIBG, l.IsGST, ProductName,
	SUM(Amount) as Amount, FLAG,Type, l.IsHQ
	FROM ubt_tmp_V2_TerminalTempData td
		INNER JOIN ztubt_terminal t ON td.TerDisplayID = t.TerDisplayID
		INNER JOIN ztubt_location l ON t.LocID = l.LocID
		WHERE FLAG NOT IN ('FUN','REC')
		GROUP BY l.LocID, l.LocDisplayID, l.LocName, l.LocTypeID, l.IsIBG, l.IsGST, ProductName, FLAG, Type, 		l.IsHQ;

	--CONVERT TO CENTS
	update ubt_tmp_V2_LocationTempData set Amount=Amount*100 where 1=1;
	--GET Wager, Sales amount per TSN
	--Calculate and get wager and sales amount

	INSERT INTO ubt_tmp_V2_TSNWagerSalesAmountData
		SELECT GAT.TerDisplayID, GAT.LocID, GAT.LocTypeID,
		ROUND(trunc(SUM(coalesce (Wager,0)) / 100,2), 2) * 100 AS Wager, --ROUND(SUM(coalesce (Wager,0)) / 100, 2,1)
		ROUND (trunc(SUM(coalesce (Sales,0)) / 100,2), 2) * 100 AS Sales--ROUND (SUM(coalesce (Sales,0)) / 100, 2,1)
		FROM
		(
			SELECT gat.TicketSerialNumber, T.TerDisplayID, L.LocID, L.LocTypeID,
			CASE WHEN PBTH.IsCancelled = false THEN Wager ELSE 0 END AS Wager,
			CASE WHEN PBTH.IsCancelled = false THEN Sales ELSE 0 END AS Sales
			FROM ubt_tmp_V2_GAT gat
			INNER JOIN ztubt_placedbettransactionheader pbth ON 			gat.TicketSerialNumber=pbth.TicketSerialNumber
			INNER JOIN ztubt_Terminal t ON PBTH.TerDisplayID = t.TerDisplayID
			INNER JOIN ztubt_Location l ON t.LocID = l.LocID
			WHERE pbth.ProdID IN (1)
		)GAT
		GROUP BY GAT.TerDisplayID, GAT.LocID, GAT.LocTypeID;


	--RETAILERS NORMAL
	--Calculate Invoice Amount For Retailers

	select
	coalesce (sum(case when LocType in(2,4) and flag='COL' and type<>'OO'then coalesce(amount,0)
	when LocType in(2,4) and flag='CAN'then coalesce(amount,0)
	when LocType in(2,4) and flag='RFD' and type<>'OO'then coalesce(amount,0)
	when LocType in(2,4) and flag='PAY'then coalesce(amount,0)
	when LocType in(2,4) and flag='SAL'then coalesce(amount,0)
	when LocType in(2,4) and flag='RBT'then coalesce(amount,0)
	end),0)as amount into vTotalAmount
	from ubt_tmp_V2_LocationTempData;

	if vTotalAmount<>0 then

	insert into ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, InBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL1', 'Y8', CASE WHEN vTotalAmount >= 0 THEN '01' ELSE '11' END,
			'21100091', 'INVOICE FOR RETAILERS', '12100001', '',
			NULL, '', '',  'SGD',
			CASE WHEN vTotalAmount >= 0 THEN vTotalAmount ELSE vTotalAmount * -1 END, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

	--Calculate Sales Amount For Retailers

	select coalesce(sum(coalesce(Sales,0)),0) as amount into vTotalAmount
	from ubt_tmp_V2_TSNWagerSalesAmountData where locType in(2,4)	;

	if vTotalAmount<>0 then

	INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, InBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL2', 'Y8', '50',
			'', 'SALES IN ADV FOR RETAILERS', '21814009', '',
			NULL, '', '',  'SGD',
			vTotalAmount, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

	--Calculate GST Amount For Retailers

	select coalesce(sum(coalesce( Wager,0)),0)
		  -coalesce(sum(coalesce( Sales,0)),0) as amt into vTotalAmount
	from ubt_tmp_V2_TSNWagerSalesAmountData
	WHERE LocType IN (2, 4);

	select coalesce(sum(coalesce( Sales,0)),0)
		  +vTotalAmount as amt into vBaseAmount
    from ubt_tmp_V2_TSNWagerSalesAmountData
	WHERE LocType IN (2, 4);

	if vTotalAmount<>0 then

	INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inbusinessdate, NULL, vSeq,
			vl_DocDate, 'MCOL3', 'Y8', '50',
			'', 'GOOD & SERVICES TAX FOR RETAILERS', '22000013', '',
			null, '', '',  'SGD',
			vTotalAmount, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

-- Start of change: Added on 07-Aug-2023: Added Product and Customer
--Calculate Sales Comm Amount For Retailers
	SELECT coalesce(SUM(Amount),0)*-1 as amt into vTotalAmount
	FROM ubt_tmp_V2_LocationTempData
	WHERE LocType IN (2, 4) AND FLAG = 'SAL';

	IF vTotalAmount <> 0  then

--		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
--		(
--			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
--			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
--			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
--			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
--			Amount, Product, DrawNumber, Customer)
--		VALUES
--		(
--			NULL, inBusinessDate, NULL, vSeq,
--			vl_DocDate, 'MCOL4', 'Y8', '40',
--			'', 'SALES COMMISSION FOR RETAILERS', '50200010', '',
--			NULL, '', '',  'SGD',
--			vTotalAmount, '', '', ''
--		);



			INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)

		select
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL4', 'Y8', '40',
			'', 'SALES COMMISSION FOR RETAILERS', '50200010', '',
			NULL, '', '',  'SGD',
			coalesce(SUM(Amount),0)*-1, ProductName, '', LocDisplayID
		FROM ubt_tmp_V2_LocationTempData
		WHERE LocType IN (2, 4) AND FLAG = 'SAL' and amount <>0
		group by ProductName, LocDisplayID;

		select 1 into vFlagIsInsert;
	end if;

-- End of change: Added on 07-Aug-2023: Added Product and Customer

--Calculate Prizes Paid Amount by Retailer
	SELECT coalesce(SUM(Amount),0)*-1 as amt into vTotalAmount
	FROM ubt_tmp_V2_LocationTempData
	--WHERE LocType IN (2, 4) AND FLAG = 'PAY'
    WHERE LocType IN (2, 4) AND (FLAG = 'PAY' OR (FLAG = 'RFD' AND type <> 'OO'));


	IF vTotalAmount <> 0  then

		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL5', 'Y8', '40',
			'', 'PRIZES PAID BY RETAILERS', '21815016', '',
			NULL, '', '',  'SGD',
			vTotalAmount, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

--Calculate Rebate Amount by Retailer: Added on 24-Sep-2024 by Hitachi
	SELECT coalesce(SUM(Amount),0)*-1 as amt into vTotalAmount
	FROM ubt_tmp_V2_LocationTempData
    WHERE LocType IN (2, 4) AND FLAG = 'RBT';


	IF vTotalAmount <> 0  then

		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL6', 'Y8', '40',
			'', 'REBATE FOR RETAILERS', '21100211', '',
			NULL, '', '',  'SGD',
			vTotalAmount, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

--Calculate Redemption Collection Amount by Retailer
--
--	SELECT coalesce(SUM(Amount),0)*-1 as amt into vTotalAmount
--	FROM ubt_tmp_V2_LocationTempData
--	WHERE LocType IN (2, 4) AND FLAG = 'RFD' and type<> 'OO';
--
--	IF vTotalAmount <> 0 then
--
--		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
--		(
--			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
--			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
--			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
--			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
--			Amount, Product, DrawNumber, Customer)
--		VALUES
--		(
--			NULL, inBusinessDate, NULL, vSeq,
--			vl_DocDate, 'MCOL6', 'Y8', '40',
--			'', 'REDEMPTION COLLECTION-RETAILERS', '21816001', '',
--			NULL, '', '',  'SGD',
--			vTotalAmount, 'HORSE RACING', '', ''
--		);
--			select 1 into vFlagIsInsert;
--	end if;
			if vFlagIsInsert=1 then
			select vSeq+1 into vSeq;
			end if;
		select 0 into vFlagIsInsert;
--BRANCHES NORMAL
	--Calculate Invoice Amount For Branches
	select
	coalesce (sum(case
	when LocType =1 and flag='COL' and type<>'OO'then coalesce(amount,0)
	when LocType =1 and flag='CAN'then coalesce(amount,0)
	-- 2023Jan25 - Include HQLocation 8888
	-- when LocType =1 and flag='RFD' and type<>'OO' and LocDisplayID<>vHQLocation then coalesce(amount,0)
	-- when LocType =1 and flag='PAY' and LocDisplayID<>vHQLocation then coalesce(amount,0)
	-- when LocType =1 and flag='RBT' and LocDisplayID<>vHQLocation then coalesce(amount,0)
	when LocType =1 and flag='RFD' and type<>'OO' then coalesce(amount,0)
	when LocType =1 and flag='PAY' then coalesce(amount,0)
	when LocType =1 and flag='RBT' then coalesce(amount,0)
	end),0)as amount into vTotalAmount
	from ubt_tmp_V2_LocationTempData;

	if vTotalAmount<>0 then
	INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL16', 'Y8', CASE WHEN vTotalAmount >= 0 THEN '01' ELSE '11' END,
			'23100000', 'INVOICE FOR BRANCHES', '12100003', '',
			NULL, '', '',  'SGD',
			CASE WHEN vTotalAmount >= 0 THEN vTotalAmount ELSE vTotalAmount * -1 END, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

	--Calculate Sales Amount For Branches

	SELECT coalesce(SUM(coalesce(sales,0)),0) as amt into vTotalAmount
	FROM ubt_tmp_V2_TSNWagerSalesAmountData
	WHERE LocType =1;

	IF vTotalAmount <> 0 then

		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL17', 'Y8', '50',
			'', 'SALES IN ADV FOR BRANCHES', '21814009', '',
			NULL, '', '',  'SGD',
			vTotalAmount, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

	--Calculate GST Amount For Branches

	select coalesce(sum(coalesce( Wager,0)),0)
		  -coalesce(sum(coalesce( Sales,0)),0) as amt into vTotalAmount
	from ubt_tmp_V2_TSNWagerSalesAmountData
	WHERE LocType=1;

	select coalesce(sum(coalesce( Sales,0)),0)
		  + vTotalAmount as amt into vBaseAmount
    from ubt_tmp_V2_TSNWagerSalesAmountData
	WHERE LocType=1;


	IF vTotalAmount <> 0 then

		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL18', 'Y8', '50',
			'', 'GOOD & SERVICES TAX FOR BRANCHES', '22000013', '',
			null, '', '',  'SGD',
			vTotalAmount, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;
--Calculate Prizes Paid Amount by Branch

	SELECT coalesce(SUM(Amount),0)*-1 as amt into vTotalAmount
	FROM ubt_tmp_V2_LocationTempData
	-- 2023Jan25 - Include HQLocation 8888
	-- WHERE LocType =1 AND FLAG = 'PAY' and LocDisplayID <> vHQLocation;
	--WHERE LocType =1 AND FLAG = 'PAY';
    WHERE LocType =1 AND (FLAG = 'PAY' OR (FLAG = 'RFD' AND type <> 'OO'));

	IF vTotalAmount <> 0 then

		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL19', 'Y8', '40',
			'', 'PRIZES PAID BY BRANCHES', '21815016', '',
			NULL, '', '',  'SGD',
			vTotalAmount, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

--Calculate Rebate Amount by Branches: Added on 24-Sep-2024 by Hitachi
	SELECT coalesce(SUM(Amount),0)*-1 as amt into vTotalAmount
	FROM ubt_tmp_V2_LocationTempData
    WHERE LocType = 1 AND FLAG = 'RBT';


	IF vTotalAmount <> 0  then

		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL20', 'Y8', '40',
			'', 'REBATE FOR BRANCHES', '21100211', '',
			NULL, '', '',  'SGD',
			vTotalAmount, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

--Calculate Redemption Collection Amount by Branch
--
--	SELECT coalesce(SUM(coalesce(Amount,0)),0)*-1 as amt into vTotalAmount
--	FROM ubt_tmp_V2_LocationTempData
--	WHERE LocType =1 AND FLAG = 'RFD' and type<> 'OO';
--
--	IF vTotalAmount <> 0 then
--
--		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
--		(
--			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
--			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
--			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
--			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
--			Amount, Product, DrawNumber, Customer)
--		VALUES
--		(
--			NULL, inBusinessDate, NULL, vSeq,
--			vl_DocDate, 'MCOL20', 'Y8', '40',
--			'', 'REDEMPTION COLLECTION-BRANCHES', '21816001', '',
--			NULL, '', '',  'SGD',
--			vTotalAmount, 'HORSE RACING', '', ''
--		);
--
--		select 1 into vFlagIsInsert;
--	END if;

	IF vFlagIsInsert = 1 then

		select vSeq+1 into vSeq;
	end if;

	select 0 into vFlagIsInsert;
-----------------------------------------------------------------------
--BRANCHES GATE ADMISSION
	--Calculate Invoice Amount For Branches
	create temporary table if not exists ubt_tmp_gateadmission
	(
		LocDisplayID VARCHAR(100),
		Amount numeric(32, 3)
	);

	insert into ubt_tmp_gateadmission
	select LocDisplayID,
	coalesce (sum(case
	when LocType =1 and flag='COL' and ProductName='Gate Admission' then coalesce(amount,0)
	end),0)as amount
	from ubt_tmp_V2_LocationTempData
	group by LocDisplayID;

	----Invoice for Branches
	select sum(amount) as amount into vTotalAmount
	from ubt_tmp_gateadmission;

	if vTotalAmount<>0 then
	INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MCOL21', 'Y8', CASE WHEN vTotalAmount >= 0 THEN '01' ELSE '11' END,
			'23100000', 'INVOICE FOR BRANCHES', '12100003', '',
			NULL, '', '',  'SGD',
			CASE WHEN vTotalAmount >= 0 THEN vTotalAmount ELSE vTotalAmount * -1 END, 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

	--Gate Admission is for customers who are paying to watch Horse Racing
	FOR vLocDisplayID, vTotalAmount, vBaseAmount IN
        SELECT LocDisplayID ,amount ,amount * 100 / (100 + vGSTRate * 100)
        FROM ubt_tmp_gateadmission
    LOOP
		vTotalAmount := vTotalAmount - vBaseAmount; -- GST

		IF vTotalAmount <> 0 then
			INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
			(
				IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
				DocumentDate, LineCode, SAPDocType, SAPPostingKey,
				SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
				SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
				Amount, Product, DrawNumber, Customer)
			VALUES
			(
				NULL, inBusinessDate, NULL, vSeq,
				vl_DocDate, 'MCOL23', 'Y8', '50',
				'', 'GATEADMISSION ' || to_char(inBusinessDate,'DD/MM/YYYY')||'-RET NO-' || vLocDisplayID, '22000000', vOutputTax,
				vBaseAmount, '', '',  'SGD',
				vTotalAmount, 'HORSE RACING', '', ''
			);
			select 1 into vFlagIsInsert;
		END IF;
		IF vBaseAmount <> 0 THEN
            INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
            (
                IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
                DocumentDate, LineCode, SAPDocType, SAPPostingKey,
                SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
                SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
                Amount, Product, DrawNumber, Customer
            )
            VALUES
            (
                NULL, inBusinessDate, NULL, vSeq,
                vl_DocDate, 'MCOL22', 'Y8', '50',
                '', 'GATEADMISSION ' || to_char(inBusinessDate, 'DD/MM/YYYY')||'-RET NO-' || vLocDisplayID, '41090020', '',
                NULL, '', '', 'SGD',
                vBaseAmount, 'HORSE RACING', '', vLocDisplayID
            );
            SELECT 1 INTO vFlagIsInsert;
		END IF;
	END LOOP;

    	IF vFlagIsInsert = 1 then

		select vSeq+1 into vSeq;
	end if;

	select 0 into vFlagIsInsert;
-------------------------------------------------------------------
--CASH RECEIPTS
	--Calculate Cash Receipts Amount

	SELECT coalesce(SUM(coalesce(Amount,0)),0) as amt into vTotalAmount
	FROM ubt_tmp_V2_LocationTempData
	WHERE LocType =1 AND FLAG = 'COL' and type<> 'OO'
	or (Loctype= 1 AND FLAG = 'COL' AND ProductName='Gate Admission');
	-- Inclusive of Paynow QR Branch
--	SELECT vTotalAmount - sum_paynowqr AS amount into vTotalAmount
--	FROM (SELECT
--		SUM(CASE WHEN productname = 'PaynowQR' THEN amount ELSE 0 END) AS sum_paynowqr
--	  FROM  ubt_tmp_V2_LocationTempData
--	  WHERE flag = 'CAS' and type='CL' and LocType =1
--	) tmp;

	IF vTotalAmount <> 0 then
		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MDEP1', 'YA',
			CASE WHEN vTotalAmount > 0 THEN '40' ELSE '15' END,
			'', 'HR CASH RECEIPTS', '10010001', '',
			NULL, '', '',  'SGD',
			abs(vTotalAmount), 'HORSE RACING', '', ''
		);

		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MDEP2', 'YA',
			CASE WHEN vTotalAmount > 0 THEN '15' ELSE '40' END,
			'23100000', 'HR CASH RECEIPTS', '12100003', '',
			NULL, '', '',  'SGD',
			abs(vTotalAmount), 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

	IF vFlagIsInsert = 1 then

		select vSeq+1 into vSeq;
	end if;

	select 0 into vFlagIsInsert;

	--CASH PAYMENTS
	--Calculate Cash Payout Amount

	select
	-- 2023Jan25 - Include HQLocation 8888
	-- coalesce (sum(case when LocType =1 and flag='PAY' and LocDisplayID <> vHQLocation then 	coalesce(amount,0)
	-- when LocType =1 and flag='RFD' and LocDisplayID <> vHQLocation and Type <> 'OO' then coalesce(amount,0)
	coalesce (sum(case when LocType =1 and flag='PAY' then coalesce(amount,0)
	when LocType =1 and flag='RFD' and Type <> 'OO' then coalesce(amount,0)
	when LocType =1 and flag='CAN' then coalesce(amount,0)
	when LocType =1 and flag='RBT' then coalesce(amount,0) --2024Sep24: include Rebate
	end),0)as amount into vTotalAmount
	from ubt_tmp_V2_LocationTempData;

	select vTotalAmount*-1 into vTotalAmount;

	-- Inclusive of Paynow Branch
--	SELECT vTotalAmount + sum_paynow AS amount into vTotalAmount
--	FROM (SELECT
--		SUM(CASE WHEN productname = 'Paynow' THEN amount ELSE 0 END) AS sum_paynow
--	  FROM  ubt_tmp_V2_LocationTempData
--	  WHERE flag = 'CAS' and type='CL' and LocType =1
--	) tmp;

	if vTotalAmount<>0 then

		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MDEP3', 'YA',
			CASE WHEN vTotalAmount > 0 THEN '01' ELSE '50' END,
			'23100000', 'HR CASH PAYMENTS ', '12100003', '',
			NULL, '', '',  'SGD',
			abs(vTotalAmount), 'HORSE RACING', '', ''
		);

		INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		(
			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
			Amount, Product, DrawNumber, Customer)
		VALUES
		(
			NULL, inBusinessDate, NULL, vSeq,
			vl_DocDate, 'MDEP4', 'YA',
			CASE WHEN vTotalAmount > 0 THEN '50' ELSE '01' END,
			'', 'HR CASH PAYMENTS ', '10010001', '',
			NULL, '', '',  'SGD',
			abs(vTotalAmount), 'HORSE RACING', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

		if vFlagIsInsert=1 then
			select vSeq+1 into vSeq;
			end if;
		select 0 into vFlagIsInsert;

	--INVOICE PERIOD DATA
	if vNowDayName in(0,4) then
	create temporary table if not exists ubt_tmp_V2_TerminalForInvoicePeriodData
				(
					TerDisplayID varchar(100),
					ProductName varchar(100),
					Amount decimal(32, 3),
					FLAG char(5),
					Type char(5)
				);

				create temporary table if not exists ubt_tmp_V2_LocationInvoicePeriodData (
					LocID INT,
					LocDisplayID VARCHAR(100),
					LocName VARCHAR(100),
					LocType int,
					IsIBG bool,
					IsGST bool,
					ProductName varchar(100),
					Amount numeric(32, 3),
					FLAG varchar(20),
					Type varchar(25)
				);

				INSERT INTO ubt_tmp_V2_TerminalForInvoicePeriodData
				SELECT TerDisplayID, ProductName, Amount, FLAG, TransType
				FROM ubt_tmp_V2_TAD_SecondDate
				WHERE (FLAG IN('CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL') AND (ProductName IN ('HORSE RACING')))
				OR FLAG NOT IN('CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL');

				INSERT INTO ubt_tmp_V2_LocationInvoicePeriodData
				SELECT Loc.LocID, Loc.LocDisplayID, Loc.LocName, Loc.LocTypeID, Loc.IsIBG, Loc.IsGST, T.ProductName, MAX(T.Amount), T.FLAG, T.Type
				FROM ubt_tmp_V2_TerminalForInvoicePeriodData T
				INNER JOIN public.ztubt_Terminal Ter ON T.TerDisplayID = Ter.TerDisplayID
				INNER JOIN public.ztubt_Location Loc ON Ter.LocID = Loc.LocID
				WHERE FLAG IN ('FUN','REC')
				GROUP BY Loc.LocID, Loc.LocDisplayID, Loc.LocName, Loc.LocTypeID, Loc.IsIBG, Loc.IsGST,
				T.ProductName, T.FLAG, T.type;

				INSERT INTO ubt_tmp_V2_LocationInvoicePeriodData
				SELECT l.LocID, l.LocDisplayID, l.LocName, l.LocTypeID, l.IsIBG, l.IsGST, ProductName, 				SUM(Amount) AS Amount, FLAG, Type
				FROM ubt_tmp_V2_TerminalForInvoicePeriodData td
				INNER JOIN public.ztubt_terminal t ON td.TerDisplayID = t.TerDisplayID
				INNER JOIN public.ztubt_Location l ON t.LocID = l.LocID
				WHERE --t.IsDeleted = 0 AND l.IsDeleted = 0 AND
				FLAG NOT IN ('FUN','REC')
				GROUP BY l.LocID, l.LocDisplayID, l.LocName, l.LocTypeID, l.IsIBG, l.IsGST, ProductName,
				FLAG, type;

				--CONVERT TO CENTS
				UPDATE ubt_tmp_V2_LocationInvoicePeriodData SET Amount = Amount * 100 where 1=1;

				--INPUT TAX ON SALES COMMISSION TO RETAILERS
				--Calculate GST on Sales Comms For Retailers

			select coalesce(sum(Amount),0)*-1 into vTotalAmount
			from ubt_tmp_V2_LocationInvoicePeriodData
			WHERE LocType IN (2,4) AND FLAG = 'GST';

			select coalesce(sum(Amount),0)*-1 into vBaseAmount
			from ubt_tmp_V2_LocationInvoicePeriodData
			WHERE LocType IN (2,4) AND FLAG = 'SAL'and IsGST=true;

			IF vTotalAmount <> 0 then

					INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
					(
						IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
						DocumentDate, LineCode, SAPDocType, SAPPostingKey,
						SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
						SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
						Amount, Product, DrawNumber, Customer)
					VALUES
					(
						NULL, inBusinessDate, NULL, vSeq,
						vl_DocDate, 'MTAX1', 'Y5', '11',
						'21100091', 'INPUT TAX ON SALES COMMISSION TO RETAILERS', '12100001', '',
						NULL, '', '',  'SGD',
						vTotalAmount, 'HORSE RACING', '', ''
					);

					INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
					(
						IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
						DocumentDate, LineCode, SAPDocType, SAPPostingKey,
						SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
						SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
						Amount, Product, DrawNumber, Customer)
					VALUES
					(
						NULL, inBusinessDate, NULL, vSeq,
						vl_DocDate, 'MTAX2', 'Y5', '40',
						'', 'INPUT TAX ON SALES COMMISSION TO RETAILERS', '22005000', vInputTax,
						vBaseAmount, '', '',  'SGD',
						vTotalAmount, 'HORSE RACING', '', ''
					);

					select 1 into vFlagIsInsert;
				end if;
				if vFlagIsInsert=1 then
				select vSeq+1 into vSeq;
				end if;
				select 0 into vFlagIsInsert;

			create temporary table if not exists ubt_tmp_V2_Tab
				(
					LocID INT,
					Amount numeric(32, 3)
				);

				INSERT INTO ubt_tmp_V2_Tab
				select LocID,
		 			coalesce (sum(case when flag='COL' and type<>'OO' then coalesce(amount,0)--INFLOW
		 		 when flag in('CAN','PAY','RFD','RBT') and type<>'OO' then coalesce(amount,0)--OUTFLOW
		 		 when flag='SAL' then coalesce(amount,0)--SALES COMMISION
		 		 when flag='GST' then coalesce(amount,0)--GST
				 --when productname = 'PaynowQR' and flag='CAS' and type='CL' then coalesce(-amount,0)--PaynowQR(Incoming)
				 --when productname = 'Paynow' and flag='CAS' and type='CL' then coalesce(-amount,0)--Paynow(Outcoming)
		 		 end),0) as Amount
				 from ubt_tmp_V2_LocationInvoicePeriodData
				 where isIBG=true and loctype in(2,4)
				 group by LocID;

				select coalesce(sum(Amount),0) as amt into vTotalAmount
				from ubt_tmp_V2_Tab where Amount<0;

				if vTotalAmount<0 then

				-- IBG PAYMENTS
				INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
				(
						IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
						DocumentDate, LineCode, SAPDocType, SAPPostingKey,
						SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
						SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
						Amount, Product, DrawNumber, Customer)
					VALUES
					(
						NULL, inBusinessDate, NULL, vSeq,
						vl_DocDate, 'MDEP5', 'YA',  '01',
						'21100091', 'IBG PAYMENTS', '12100001', '',
						NULL, '', '',  'SGD',
						vTotalAmount * -1, 'HORSE RACING', '', ''
					);

					INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
					(
						IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
						DocumentDate, LineCode, SAPDocType, SAPPostingKey,
						SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
						SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
						Amount, Product, DrawNumber, Customer)
					VALUES
					(
						NULL, inBusinessDate, NULL, vSeq,
						vl_DocDate, 'MDEP6', 'YA', '50',
						'', 'IBG PAYMENTS', '11003010', '',
						NULL, '', '',  'SGD',
						vTotalAmount * -1, 'HORSE RACING', '', ''
					);

					select 1 into vFlagIsInsert;
				end if;

				IF vFlagIsInsert = 1 then
				select vSeq+1 into vSeq;
				end if;

				select 1 into vFlagIsInsert;

			--IBG RECEIPTS
			--Calculate IBG Amount for IBG Retailer
			select coalesce(SUM(Amount),0) into vTotalAmount
			FROM ubt_tmp_V2_Tab WHERE Amount > 0;

			IF vTotalAmount > 0 then
				INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
					(
						IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
						DocumentDate, LineCode, SAPDocType, SAPPostingKey,
						SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
						SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
						Amount, Product, DrawNumber, Customer)
					VALUES
					(
						NULL, InBusinessDate, NULL, vSeq,
						vl_DocDate, 'MDEP7', 'YA', '40',
						'', 'IBG RECEIPTS', '11003060', '',
						NULL, '', '',  'SGD',
						vTotalAmount, 'HORSE RACING', '', ''
					);

					INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
					(
						IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
						DocumentDate, LineCode, SAPDocType, SAPPostingKey,
						SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
						SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
						Amount, Product, DrawNumber, Customer)
					VALUES
					(
						NULL, inBusinessDate, NULL, vSeq,
						vl_DocDate, 'MDEP8', 'YA', '15',
						'21100091', 'IBG RECEIPTS', '12100001', '',
						NULL, '', '',  'SGD',
						vTotalAmount, 'HORSE RACING', '', ''
					);

					select 1 into vFlagIsInsert;
				end if;

				IF vFlagIsInsert = 1 then

					select vSeq+1 into vSeq;
				end if;

				select 1 into vFlagIsInsert;


			--MANUAL PAYMENT ENDED DD/MM/YYYY-NON IBG RETAILERS

				create temporary table if not exists ubt_tmp_V2_TempAmountPerLocation
				(
					LocID INT,
					LocDisplayID VARCHAR(200),
					LocName VARCHAR(100),
					Seq INT,
					Amount numeric(32, 3)
				);

				INSERT INTO ubt_tmp_V2_TempAmountPerLocation
				SELECT A.LocID, A.LocDisplayID, A.LocName, A.Seq, A.Amount
				from (
					SELECT loc.LocID, loc.LocDisplayID, loc.LocName, ROW_NUMBER()OVER(ORDER BY loc.LocID, loc.LocName ) + vSeq - 1 AS Seq, coalesce(sum(case when flag='COL' and type<>'OO' then 				coalesce(amount,0)--INFLOW
		 		when flag in('CAN','PAY','RFD','RBT') and type<>'OO' then coalesce(amount,0)--OUTFLOW
		 		when flag='SAL' then coalesce(amount,0)--SALES COMMISION
		 		when flag='GST' then coalesce(amount,0)--GST
				--when productname = 'PaynowQR' and flag='CAS' and type='CL' then coalesce(-amount,0)--PaynowQR(Incoming)
				--when productname = 'Paynow' and flag='CAS' and type='CL' then coalesce(-amount,0)--Paynow(Outcoming)
		 		end),0) as Amount
		 		fROM ubt_tmp_V2_LocationInvoicePeriodData loc
						WHERE loc.IsIBG <> true AND loc.LocType IN (2, 4)
						GROUP BY loc.LocID, loc.LocDisplayID, loc.LocName
				)A	where A.Amount<>0;

				select count(1) into vTotalTempAmountPerLocation from ubt_tmp_V2_TempAmountPerLocation;

				IF vTotalTempAmountPerLocation > 0 then

				SELECT MAX(Seq) + 1 into vSeq
				FROM ubt_tmp_V2_TempAmountPerLocation;
				end if;

				INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
				(
					IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
					DocumentDate, LineCode, SAPDocType, SAPPostingKey,
					SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
					SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
					Amount, Product, DrawNumber, Customer
				)

				SELECT NULL, inBusinessDate,
				NULL, Loc.Seq,
				vl_DocDate,
				CASE WHEN Amount >= 0 AND constant = 1 THEN
					'MPYT1'
				WHEN Amount >= 0 AND constant = 2 THEN
					'MPYT2'
				WHEN Amount < 0 AND constant = 1 THEN
					'MPYT3'
				ELSE
					'MPYT4'
				END, 'Y6',
				CASE WHEN (Amount >= 0 AND constant = 1) OR (Amount < 0 AND constant = 2) THEN
					'11'
				ELSE
					'01'
				END,
				CASE WHEN constant = 1 THEN
					'21100091'
				ELSE
					loc.LocDisplayID
				END,
				CASE WHEN constant = 1 THEN
				'MANUAL PAYMENT ENDED ' || to_char(inBusinessDate, 'DD/MM/YYYY') || '-NON IBG RETAILERS'
				ELSE
					LEFT('MANUAL PAYMENT ENDED ' || to_char(inBusinessDate,'DD/MM/YYYY') || '-' || loc.LocName, 50)
				END, '12100001', '',
				NULL, '', '', 'SGD', CASE WHEN Amount >= 0 THEN Amount ELSE Amount * -1 END,
				'HORSE RACING', '', ''
				FROM
				(VALUES (1), (2)) AS TempTable(constant),
				ubt_tmp_V2_TempAmountPerLocation loc
				GROUP BY loc.Seq, loc.LocID, loc.LocDisplayID, loc.LocName, TempTable.constant, Amount;
			end if;

--			return query
--					select coalesce(a.IDMMBusinessDay,'000000000000') as IDMMBusinessDay
--					, a.BusinessDate, coalesce(a.ItemID,'000000000000') as ItemID,
--					coalesce(a.TransactionID,'000000000000') as TransactionID ,
--					a.DocumentDate, a.LineCode, a.SAPDocType, a.SAPPostingKey,
--					a.SAPControlAcctCode, a.LineText, a.GLNumber, a.SAPTaxCode,
--					a.SAPTaxBaseAmount, a.CCCode, a.SAPAssignment, a.CurrencyCode,
--					a.Amount, a.Product, a.DrawNumber, a.Customer
--			from ubt_tmp_V2_TB_RTMS_RTShopCloud a;



	DROP TABLE  IF EXISTS ubt_tmp_V2_TerminalTempData;
	DROP TABLE  IF EXISTS ubt_tmp_V2_LocationTempData;
	DROP TABLE  IF EXISTS ubt_tmp_V2_TSNWagerSalesAmountData;
	DROP TABLE  IF EXISTS ubt_tmp_V2_TerminalForInvoicePeriodData;
	DROP TABLE  IF EXISTS ubt_tmp_V2_LocationInvoicePeriodData;
	DROP TABLE  IF EXISTS ubt_tmp_V2_Tab;
	DROP TABLE  IF EXISTS ubt_tmp_V2_TempAmountPerLocation;
	DROP TABLE  IF EXISTS ubt_tmp_gateadmission;

    return vSeq;
end;

$$;


ALTER FUNCTION public.sp_ubt_getrtshopcloud_hr(inbusinessdate date, vseq integer) OWNER TO sp_postgres;
