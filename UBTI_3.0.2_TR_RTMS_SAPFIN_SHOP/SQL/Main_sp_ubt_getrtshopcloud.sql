CREATE FUNCTION public.sp_ubt_getrtshopcloud(inbusinessdate date)
 RETURNS TABLE(idmmbusinessday character varying, businessdate date, itemid character varying, transactionid character varying, documentdate date, linecode character varying, sapdoctype character varying, sappostingkey character varying, sapcontrolacctcode character varying, linetext character varying, glnumber character varying, saptaxcode character varying, saptaxbaseamount numeric, cccode character varying, sapassignment character varying, currencycode character varying, amt numeric, product character varying, drawnumber character varying, customer character varying)
    LANGUAGE plpgsql
    AS $$

declare
vTotalAmount numeric;
vSeq Int4:=1;
vl_DocDate date:=inbusinessdate+ interval '1 DAY';
vFlagIsInsert Int4:=0;
vBaseAmount numeric;
vnowDayName int4:= extract(dow from inbusinessdate);
vPeriodStartDate date;
vPeriodEndDate date:=inbusinessdate;
vGST_Branch varchar;
vInputTax varchar;
vOutputTax varchar;
vGSTRate numeric; --percentage
vARCode_Q_Retailer varchar; -- OCBC QR Code for PNQR
vARCode_Q_Branch varchar; -- OCBC QR Code for PNQR
vARCode_P_Retailer varchar; -- UOB QR Code for Paynow
vARCode_P_Branch varchar; -- UOB QR Code for Paynow

begin
		--01-Nov-2022: Change to replace GST taxcode with variable
	/*	PRD

	select 'S3' into vOutputTax;
	select '24100110' into vARCode_Q_Retailer; -- OCBC QR Code for PNQR
	select '24100120' into vARCode_Q_Branch; -- OCBC QR Code for PNQR
	select '24100121' into vARCode_P_Retailer; -- UOB QR Code for Paynow
	select '24100111' into vARCode_P_Branch; -- UOB QR Code for Paynow
	*/
	-- UAT
	select case when InBusinessDate <= date '2022-10-31' then 'ZA'
				when InBusinessDate >= date '2022-11-01' and InBusinessDate <= date '2023-12-31' then 'YA'
				when InBusinessDate >= date '2024-01-01' then 'XA'

	select case when InBusinessDate <= date '2022-10-31' then 'P1'
				when InBusinessDate >= date '2022-11-01' and InBusinessDate <= date '2023-12-31' then 'P2'
				when InBusinessDate >= date '2024-01-01' then 'P3'
			end into vInputTax;

	select 'S3' into vOutputTax;
	select '24100060' into vARCode_Q_Retailer; -- OCBC QR Code for PNQR
	select '24100061' into vARCode_Q_Branch; -- OCBC QR Code for PNQR
	select '24100062' into vARCode_P_Retailer; -- UOB QR Code for Paynow
	select '24100063' into vARCode_P_Branch; -- UOB QR Code for Paynow

	-- End

	-- GST Config
	SELECT COALESCE(gstrate , 0)/100 INTO vGSTRate
    FROM ztubt_gstconfig zg
    WHERE inbusinessdate BETWEEN zg.effectivefrom AND COALESCE(zg.enddate , 'infinity'::date)
    ORDER BY zg.effectivefrom
    LIMIT 1;

   	-- Store common config into temp table
	create temporary table if not exists ubt_tmp_rtshop_config
	(
		ConfigKey VARCHAR(50),
		ConfigValue VARCHAR(100)
	);
	INSERT INTO ubt_tmp_rtshop_config(ConfigKey, ConfigValue) VALUES('vGST_Branch', vGST_Branch);
	INSERT INTO ubt_tmp_rtshop_config(ConfigKey, ConfigValue) VALUES('vInputTax', vInputTax);

	create temporary table if not exists ubt_tmp_V2_TB_RTMS_RTShopCloud
	(
		--SportsMainID int4 IDENTITY(1,1) NOT  PRIMARY KEY,
		IDMMBusinessDay varchar,--int4--changed to varhcar for '000000000000' format ,
		BusinessDate date ,
		ItemID int4 ,
		TransactionID int4 ,
		DocumentDate date ,
		LineCode varchar(12) ,
		SAPDocType varchar(2) ,
		SAPPostingKey varchar(2) ,
		SAPControlAcctCode varchar(15) ,
		LineText varchar(50) ,
		GLNumber varchar(10) ,
		SAPTaxCode varchar(10) ,
		SAPTaxBaseAmount numeric(15,0) ,
		CCCode varchar(10) ,
		SAPAssignment varchar(18) ,
		CurrencyCode varchar(3) ,
		Amount numeric(32,11) ,
		Product varchar(18) ,
		DrawNumber varchar(18) ,
		Customer varchar(10)
	);

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
	create temporary table if not exists  ubt_tmp_V2_TAD_FirstDate
	(
		TerDisplayID VARCHAR(200),
		ProductName VARCHAR(100),
		Amount numeric(32,3),
		Ct int4,
		FLAG CHAR(3),
		TransType CHAR(2),
		FromDate Date,
		ToDate Date
		--INDEX TempTransAmountDetailBusiessDateIndex NONCLUSTERED(FLAG, ProductName)
	);

	create temporary table if not exists ubt_tmp_paynow_FirstDate
	(
		Amt numeric(32,3),
		Fee numeric(32,3),
		ProductName VARCHAR(100),
		TerDisplayID VARCHAR(200)
	);

	create temporary table if not exists ubt_tmp_V2_GAT
	(
		TicketSerialNumber VARCHAR(500),
		Wager numeric(22,2),
		SecondWager numeric(22,2),
		Sales numeric(22, 11),
		SecondSales numeric(22, 11),
		SalesComm numeric(22, 11),
		SecondSalesComm numeric(22, 11),
		GST numeric(22, 11),
		SecondGST numeric(22, 11),
		ReturnAmount numeric(22,2),
		WinningAmount numeric(22,2)
	);

	insert into ubt_tmp_V2_GAT
	select
	ticketserialnumber,
	wager,
	secondwager,
	sales ,
	secondsales,
	salescomm,
	secondsalescomm,
	gst,
	secondgst,
	returnamount,
	winningamount
	from
	public.sp_ubt_getamounttransaction (InBusinessDate,	InBusinessDate);

	insert into ubt_tmp_V2_TAD_FirstDate
	select
	terdisplayid,
	prodname,
	amount,
	ticketcount,
	FLAG,
	TransType,
	InBusinessDate as FromDate,
	InBusinessDate as InToDate
	from
	public.sp_ubt_gettransamountdetails(InBusinessDate,InBusinessDate);

	insert into ubt_tmp_paynow_FirstDate
	select
		SUM(CASE WHEN ProductName = 'PaynowQR [+trans fee]' THEN amount ELSE 0 END) as Amt,
		SUM(CASE WHEN ProductName = 'PaynowQR' and TransType = 'TF' THEN amount ELSE 0 END) AS Fee,
		'PaynowQR' as ProductName, terdisplayid
		from ubt_tmp_V2_TAD_FirstDate
		where flag='CAS' and ProductName in ('PaynowQR [+trans fee]', 'PaynowQR') and amount <> 0
		group by terdisplayid

	union
    select
		SUM(CASE WHEN ProductName = 'Paynow [+trans fee]' THEN amount ELSE 0 END) as Amt,
		SUM(CASE WHEN ProductName = 'Paynow' and TransType = 'TF' THEN amount ELSE 0 END) AS Fee,
		'Paynow' as ProductName, terdisplayid
		from ubt_tmp_V2_TAD_FirstDate
		where flag='CAS' and ProductName in ('Paynow [+trans fee]', 'Paynow') and amount <> 0
		group by terdisplayid;

	-- Add Paynow to ubt_tmp_V2_TAD_FirstDate
	insert into ubt_tmp_V2_TAD_FirstDate
	select
		terdisplayid, ProductName, (a.Amt - a.Fee) as Amount,
		0 as ct, 'CAS' as FLAG, 'CL' as TransType,
		InBusinessDate as FromDate, InBusinessDate as InToDate
		from ubt_tmp_paynow_FirstDate a;
	DROP TABLE IF EXISTS ubt_tmp_paynow_FirstDate;


insert into ubt_tmp_V2_TerminalTempData
select
	TerDisplayID,
	ProductName,
	Amount,
	FLAG,
	TransType
	from ubt_tmp_V2_TAD_FirstDate
	where
	FLAG NOT IN ('CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL');

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


--Paynow Charges

	-- calculate Paynow QR charges for retailer

	SELECT sum_transfee_paynow AS amount into vTotalAmount
	FROM (SELECT
		SUM(CASE WHEN productname = 'PaynowQR' and type = 'TF' THEN amount ELSE 0 END) AS sum_transfee_paynow
	  FROM  ubt_tmp_V2_LocationTempData
	  WHERE flag = 'CAS' and LocType in (2,4)
	) tmp;

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
			vl_DocDate, 'MCOL21', 'Y5', '01',
			vARCode_Q_Retailer, 'PAYNOW QR TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT E-PYT BANK FEE', '12100000', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

	vBaseAmount := vTotalAmount*100/(100 + vGSTRate*100); -- Sales
	IF vBaseAmount <> 0 then
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
			vl_DocDate, 'MCOL22', 'Y5', '50',
			'', 'PAYNOW QR TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT E-PYT BANK FEE', '41090007', '',
			NULL, '2001200000', '',  'SGD',
			vBaseAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

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
			vl_DocDate, 'MCOL23', 'Y5', '50',
			'', 'PAYNOW QR TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT E-PYT BANK FEE', '22000000', vOutputTax,
			vBaseAmount, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

	if vFlagIsInsert=1 then
			select vSeq+1 into vSeq;
	end if;
	select 0 into vFlagIsInsert;

	-- calculate Paynow charges for retailer

	SELECT sum_transfee_paynow AS amount into vTotalAmount
	FROM (SELECT
		SUM(CASE WHEN productname = 'Paynow' and type = 'TF' THEN amount ELSE 0 END) AS sum_transfee_paynow
	  FROM  ubt_tmp_V2_LocationTempData
	  WHERE flag = 'CAS' and LocType in (2,4)
	) tmp;

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
			vl_DocDate, 'MCOL24', 'Y5', '01',
			vARCode_P_Retailer, 'PAYNOW TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT PZ CLAIM BANK FEE', '12100000', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

	vBaseAmount := vTotalAmount*100/(100 + vGSTRate*100); -- Sales
	IF vBaseAmount <> 0 then
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
			vl_DocDate, 'MCOL25', 'Y5', '50',
			'', 'PAYNOW TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT PZ CLAIM BANK FEE', '41090007', '',
			NULL, '2001200000', '',  'SGD',
			vBaseAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

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
			vl_DocDate, 'MCOL26', 'Y5', '50',
			'', 'PAYNOW TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT PZ CLAIM BANK FEE', '22000000', vOutputTax,
			vBaseAmount, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

	if vFlagIsInsert=1 then
			select vSeq+1 into vSeq;
	end if;
	select 0 into vFlagIsInsert;

	-- calculate Paynow QR charges for branch

	SELECT sum_transfee_paynow AS amount into vTotalAmount
	FROM (SELECT
		SUM(CASE WHEN productname = 'PaynowQR' and type = 'TF' THEN amount ELSE 0 END) AS sum_transfee_paynow
	  FROM  ubt_tmp_V2_LocationTempData
	  WHERE flag = 'CAS' and LocType =1
	) tmp;

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
			vl_DocDate, 'MCOL27', 'Y5', '01',
			vARCode_Q_Branch, 'PAYNOW QR TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT E-PYT BANK FEE', '12100000', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

	vBaseAmount := vTotalAmount*100/(100 + vGSTRate*100); -- Sales
	IF vBaseAmount <> 0 then
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
			vl_DocDate, 'MCOL28', 'Y5', '50',
			'', 'PAYNOW QR TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT E-PYT BANK FEE', '41090007', '',
			NULL, '2001200000', '',  'SGD',
			vBaseAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

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
			vl_DocDate, 'MCOL29', 'Y5', '50',
			'', 'PAYNOW QR TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT E-PYT BANK FEE', '22000000', vOutputTax,
			vBaseAmount, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

	if vFlagIsInsert=1 then
			select vSeq+1 into vSeq;
	end if;
	select 0 into vFlagIsInsert;

	-- calculate Paynow charges for branch

	SELECT sum_transfee_paynow AS amount into vTotalAmount
	FROM (SELECT
		SUM(CASE WHEN productname = 'Paynow' and type = 'TF' THEN amount ELSE 0 END) AS sum_transfee_paynow
	  FROM  ubt_tmp_V2_LocationTempData
	  WHERE flag = 'CAS' and LocType =1
	) tmp;

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
			vl_DocDate, 'MCOL30', 'Y5', '01',
			vARCode_P_Branch, 'PAYNOW TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT PZ CLAIM BANK FEE', '12100000', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

	vBaseAmount := vTotalAmount*100/(100 + vGSTRate*100); -- Sales
	IF vBaseAmount <> 0 then
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
			vl_DocDate, 'MCOL31', 'Y5', '50',
			'', 'PAYNOW TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT PZ CLAIM BANK FEE', '41090007', '',
			NULL, '2001200000', '',  'SGD',
			vBaseAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

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
			vl_DocDate, 'MCOL32', 'Y5', '50',
			'', 'PAYNOW TXN ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' - UBT PZ CLAIM BANK FEE', '22000000', vOutputTax,
			vBaseAmount, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);
		select 1 into vFlagIsInsert;
	end if;

	if vFlagIsInsert=1 then
			select vSeq+1 into vSeq;
	end if;
	select 0 into vFlagIsInsert;

--PAYNOW, PAYNOWQR - Daily Settlement

	-- calculate PAYNOW QR for retailer
	SELECT SUM(amount) AS amount into vTotalAmount
	FROM ubt_tmp_V2_LocationTempData
	WHERE productname = 'PaynowQR' and flag = 'CAS' and type = 'CL' and LocType in (2,4);

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
			vl_DocDate, 'MCOL33', 'Y8', '11',
			'21100091', 'GAMING INVOICE ON ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' RET-PAYNOW QR', '12100001', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
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
			vl_DocDate, 'MCOL34', 'Y8', '01',
			vARCode_Q_Retailer, 'GAMING INVOICE ON ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' RET-PAYNOW QR', '12100000', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

	if vFlagIsInsert=1 then
			select vSeq+1 into vSeq;
	end if;
	select 0 into vFlagIsInsert;
	-- calculate PAYNOW for retailer

	SELECT SUM(amount) AS amount into vTotalAmount
	FROM  ubt_tmp_V2_LocationTempData
	WHERE productname = 'Paynow' and flag = 'CAS' and type = 'CL' and LocType in (2,4);

	select vTotalAmount*-1 into vTotalAmount;

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
			vl_DocDate, 'MCOL35', 'Y8', '01',
			'21100091', 'GAMING INVOICE ON ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' RET-PAYNOW', '12100001', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
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
			vl_DocDate, 'MCOL36', 'Y8', '11',
			vARCode_P_Retailer, 'GAMING INVOICE ON ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' RET-PAYNOW', '12100000', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

	if vFlagIsInsert=1 then
			select vSeq+1 into vSeq;
	end if;
	select 0 into vFlagIsInsert;

	-- To-do tomorrow
	-- calculate PAYNOW QR for branches
	SELECT SUM(amount) AS amount into vTotalAmount
	FROM ubt_tmp_V2_LocationTempData
	WHERE productname = 'PaynowQR' and flag = 'CAS' and type = 'CL' and LocType =1;

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
			vl_DocDate, 'MCOL37', 'Y8', '11',
			'23100000', 'GAMING INVOICE ON ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' BR-PAYNOW QR', '12100003', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
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
			vl_DocDate, 'MCOL38', 'Y8', '01',
			vARCode_Q_Branch, 'GAMING INVOICE ON ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' BR-PAYNOW QR', '12100000', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

	if vFlagIsInsert=1 then
			select vSeq+1 into vSeq;
	end if;
	select 0 into vFlagIsInsert;

	-- calculate PAYNOW for branch
	SELECT SUM(amount) AS amount into vTotalAmount
	FROM  ubt_tmp_V2_LocationTempData
	WHERE productname = 'Paynow' and flag = 'CAS' and type = 'CL' and LocType =1;

	select vTotalAmount*-1 into vTotalAmount;

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
			vl_DocDate, 'MCOL39', 'Y8', '01',
			'23100000', 'GAMING INVOICE ON ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' BR-PAYNOW', '12100003', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
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
			vl_DocDate, 'MCOL40', 'Y8', '11',
			vARCode_P_Branch, 'GAMING INVOICE ON ' || to_char(inBusinessDate,'DD/MM/YYYY') || ' BR-PAYNOW', '12100000', '',
			NULL, '', '',  'SGD',
			vTotalAmount, '', '', ''
		);

		select 1 into vFlagIsInsert;
	end if;

-- END

	IF vFlagIsInsert = 1 then

		select vSeq+1 into vSeq;
	end if;

	select 1 into vFlagIsInsert;

	DROP TABLE IF EXISTS ubt_tmp_V2_TerminalTempData;
	DROP TABLE IF EXISTS ubt_tmp_V2_LocationTempData;

	--INVOICE PERIOD DATA
	if vNowDayName in(0,4) then
	select case when vNowDayName=0 then inbusinessdate+ interval '-2 DAY'
	when vNowDayName=4 then inbusinessdate+ interval '-3 DAY'
	end as PeriodStartDate into vPeriodStartDate;
				create temporary table if not exists  ubt_tmp_V2_TAD_SecondDate
				(
					TerDisplayID VARCHAR(200),
					ProductName VARCHAR(100),
					Amount numeric(32, 11),
					Ct INT,
					FLAG CHAR(3),
					TransType CHAR(2),
					FromDate Date,
					ToDate Date
					--INDEX TempTransAmountDetailPeriodDateIndex NONCLUSTERED(FLAG, ProductName)
				);

				insert into ubt_tmp_V2_TAD_SecondDate
				select
					terdisplayid,
					prodname,
					amount,
					ticketcount ,
					FLAG,
					TransType,
					vPeriodStartDate as FromDate,
					vPeriodEndDate as InToDate
					from public.sp_ubt_gettransamountdetails(vPeriodStartDate,vPeriodEndDate);

				create temporary table if not exists ubt_tmp_paynow_SecondDate
				(
					Amt numeric(32,3),
					Fee numeric(32,3),
					ProductName VARCHAR(100),
					TerDisplayID VARCHAR(200)
				);
				insert into ubt_tmp_paynow_SecondDate
				select
					SUM(CASE WHEN ProductName = 'PaynowQR [+trans fee]' THEN amount ELSE 0 END) as Amt,
					SUM(CASE WHEN ProductName = 'PaynowQR' and TransType = 'TF' THEN amount ELSE 0 END) AS Fee,
					'PaynowQR' as ProductName, terdisplayid
					from ubt_tmp_V2_TAD_SecondDate
					where flag='CAS' and amount <> 0 and ProductName in ('PaynowQR [+trans fee]', 'PaynowQR')
					group by terdisplayid
				union select
					SUM(CASE WHEN ProductName = 'Paynow [+trans fee]' THEN amount ELSE 0 END) as Amt,
					SUM(CASE WHEN ProductName = 'Paynow' and TransType = 'TF' THEN amount ELSE 0 END) AS Fee,
					 'Paynow' as ProductName, terdisplayid
					from ubt_tmp_V2_TAD_SecondDate
					where flag='CAS' and amount <> 0 and ProductName in ('Paynow [+trans fee]', 'Paynow')
					group by terdisplayid;

				-- Add Paynow to ubt_tmp_V2_TAD_SecondDate
				insert into ubt_tmp_V2_TAD_SecondDate
				select
					terdisplayid, ProductName, (a.Amt - a.Fee) as amount,
					0 as ticketcount, 'CAS' as FLAG, 'CL' as TransType,
					vPeriodStartDate as FromDate, vPeriodEndDate as InToDate
					from ubt_tmp_paynow_SecondDate a;
				DROP TABLE IF EXISTS ubt_tmp_paynow_SecondDate;

				CREATE INDEX V2_NCI_FlagProdName_2 ON ubt_tmp_V2_TAD_SecondDate (FLAG, ProductName);

				end if;

	select public.sp_ubt_getrtshopcloud_sports(inbusinessdate, vSeq) into vSeq;
	select public.sp_ubt_getrtshopcloud_hr(inbusinessdate, vSeq) into vSeq;


	return query
		select coalesce(LPAD(a.IDMMBusinessDay,12,'0'),'000000000000')::varchar as IDMMBusinessDay
		, a.BusinessDate, coalesce(LPAD(a.ItemID::varchar,12,'0'),'000000000000')::varchar as ItemID,
		coalesce(LPAD(a.TransactionID::varchar,12,'0'),'000000000000')::varchar as TransactionID ,
		a.DocumentDate, a.LineCode, a.SAPDocType, a.SAPPostingKey,
		a.SAPControlAcctCode, a.LineText, a.GLNumber, a.SAPTaxCode,
		a.SAPTaxBaseAmount, a.CCCode, a.SAPAssignment, a.CurrencyCode,
		a.Amount, a.Product, a.DrawNumber, a.Customer
        from ubt_tmp_V2_TB_RTMS_RTShopCloud a;

DROP TABLE  IF EXISTS ubt_tmp_V2_GAT;
DROP TABLE  IF EXISTS ubt_tmp_V2_TAD_FirstDate;
DROP TABLE  IF EXISTS ubt_tmp_V2_TAD_SecondDate;
DROP TABLE  IF EXISTS ubt_tmp_V2_TB_RTMS_RTShopCloud;
DROP TABLE  IF EXISTS ubt_tmp_rtshop_config;
end;

$$;


ALTER FUNCTION public.sp_ubt_getrtshopcloud(inbusinessdate date) OWNER TO sp_postgres;
