import pandas as pd
import numpy as np
from Snowflake_connection import snowflake_connection
import os
connection = snowflake_connection()
schema = "SPPL_DEV_DWH.SPPL_PUBLIC"
# ztubt_terminal
query = f"""
    SELECT * FROM {schema}.ZTUBT_TERMINAL
    """
df_ztubt_terminal = pd.read_sql(query, connection)

# ztubt_location
query = f"""
    SELECT * FROM {schema}.ZTUBT_LOCATION
    """
df_ztubt_location = pd.read_sql(query, connection)

# ztubt_placedbettransactionheader
query = f"""
    SELECT * FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER
    """
df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)


def ubt_tmp_V2_LocationTempData(df_ubt_tmp_V2_TerminalTempData):
    global df_ztubt_terminal, df_ztubt_location

    # Define the schema for the temporary DataFrame
    df_ubt_tmp_V2_LocationTempData = pd.DataFrame({
        "LOCID": pd.Series(dtype='int32'),
        'LOCDISPLAYID': pd.Series(dtype='str'),
        'LOCNAME': pd.Series(dtype='str'),
        'LOCTYPE': pd.Series(dtype='int32'),
        'ISIBG': pd.Series(dtype='bool'),
        'ISGST': pd.Series(dtype='bool'),
        'PRODUCTNAME': pd.Series(dtype='str'),
        'AMOUNT': pd.Series(dtype='float'),
        'FLAG': pd.Series(dtype='str'),
        'TYPE': pd.Series(dtype='str'),
        'ISHQ': pd.Series(dtype='bool')
    })
    # Add prefixes to avoid column name conflicts during merge
    df_terminal_prefixed = df_ztubt_terminal.add_prefix('TER_')
    df_location_prefixed = df_ztubt_location.add_prefix('LOC_')
    df_ubt_tmp_V2_TerminalTempData = df_ubt_tmp_V2_TerminalTempData.add_prefix('T_')

    # 1.1 First Part
    df_temp = df_ubt_tmp_V2_TerminalTempData.merge(
        df_terminal_prefixed, how='inner',
        left_on='T_TERDISPLAYID', right_on='TER_TERDISPLAYID'
    ).merge(
        df_location_prefixed, how='inner',
        left_on='TER_LOCID', right_on='LOC_LOCID'
    )

    # Filter DF temp
    df_temp = df_temp[
        (df_temp['T_FLAG'].isin(['FUN','REC']))
    ]

    df_temp = df_temp.groupby([
        'LOC_LOCID', 'LOC_LOCDISPLAYID', 'LOC_LOCNAME', 'LOC_LOCTYPEID',
        'LOC_ISIBG', 'LOC_ISGST', 'T_PRODUCTNAME', 'T_FLAG', 'T_TYPE', 'LOC_ISHQ'
    ], as_index=False).agg({
        'T_AMOUNT': 'max'
    }).rename(columns={
        'LOC_LOCID': 'LOCID',
        'LOC_LOCDISPLAYID': 'LOCDISPLAYID',
        'LOC_LOCNAME': 'LOCNAME',
        'LOC_LOCTYPEID': 'LOCTYPE',
        'LOC_ISIBG': 'ISIBG',
        'LOC_ISGST': 'ISGST',
        'T_PRODUCTNAME': 'PRODUCTNAME',
        'T_AMOUNT': 'AMOUNT',
        'T_FLAG': 'FLAG',
        'T_TYPE': 'TYPE',
        'LOC_ISHQ': 'ISHQ'

    })[['LOCID', 'LOCDISPLAYID', 'LOCNAME', 'LOCTYPE',
        'ISIBG', 'ISGST', 'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TYPE', 'ISHQ'
    ]]
    df_ubt_tmp_V2_LocationTempData = pd.concat([df_ubt_tmp_V2_LocationTempData, df_temp], ignore_index=True)

    # 1.2 Second Part
    df_temp = df_ubt_tmp_V2_TerminalTempData.merge(
        df_terminal_prefixed, how='inner',
        left_on='T_TERDISPLAYID', right_on='TER_TERDISPLAYID'
    ).merge(
        df_location_prefixed, how='inner',
        left_on='TER_LOCID', right_on='LOC_LOCID'
    )
    # Filter DF temp
    df_temp = df_temp[
        (~df_temp['T_FLAG'].isin(['FUN','REC']))
    ]
    df_temp = df_temp.groupby([
        'LOC_LOCID', 'LOC_LOCDISPLAYID', 'LOC_LOCNAME', 'LOC_LOCTYPEID',
        'LOC_ISIBG', 'LOC_ISGST', 'T_PRODUCTNAME', 'T_FLAG', 'T_TYPE', 'LOC_ISHQ'
    ], as_index=False).agg({
        'T_AMOUNT': 'sum'
    }).rename(columns={
        'LOC_LOCID': 'LOCID',
        'LOC_LOCDISPLAYID': 'LOCDISPLAYID',
        'LOC_LOCNAME': 'LOCNAME',
        'LOC_LOCTYPEID': 'LOCTYPE',
        'LOC_ISIBG': 'ISIBG',
        'LOC_ISGST': 'ISGST',
        'T_PRODUCTNAME': 'PRODUCTNAME',
        'T_AMOUNT': 'AMOUNT',
        'T_FLAG': 'FLAG',
        'T_TYPE': 'TYPE',
        'LOC_ISHQ': 'ISHQ'
    })[['LOCID', 'LOCDISPLAYID', 'LOCNAME', 'LOCTYPE',
        'ISIBG', 'ISGST', 'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TYPE', 'ISHQ']]
    df_ubt_tmp_V2_LocationTempData = pd.concat([df_ubt_tmp_V2_LocationTempData, df_temp], ignore_index=True)

    # convert AMOUNT to cents
    df_ubt_tmp_V2_LocationTempData['AMOUNT'] = df_ubt_tmp_V2_LocationTempData['AMOUNT'] * 100
    return df_ubt_tmp_V2_LocationTempData

def ubt_tmp_V2_TSNWagerSalesAmountData(df_ubt_tmp_V2_GAT):
    global df_ztubt_terminal, df_ztubt_location, df_ztubt_placedbettransactionheader

    df_ubt_tmp_V2_TSNWagerSalesAmountData = pd.DataFrame({
        'TERDISPLAYID': pd.Series(dtype='str'),
        'LOCID': pd.Series(dtype='int32'),
        'LOCTYPE': pd.Series(dtype='int32'),
        'WAGER': pd.Series(dtype='float'),
        'SALES': pd.Series(dtype='float')
    })

    # add prefix to avoid column name conflicts during merge
    df_terminal_prefixed = df_ztubt_terminal.add_prefix('TER_')
    df_location_prefixed = df_ztubt_location.add_prefix('LOC_')
    df_pbth_prefixed = df_ztubt_placedbettransactionheader.add_prefix('PBTH_')
    df_ubt_tmp_V2_GAT = df_ubt_tmp_V2_GAT.add_prefix('GAT_')

    # SELECT GAT.TerDisplayID, GAT.LocID, GAT.LocTypeID,
	# 	ROUND(trunc(SUM(coalesce (Wager,0)) / 100,2), 2) * 100 AS Wager, --ROUND(SUM(coalesce (Wager,0)) / 100, 2,1)
	# 	ROUND (trunc(SUM(coalesce (Sales,0)) / 100,2), 2) * 100 AS Sales--ROUND (SUM(coalesce (Sales,0)) / 100, 2,1)
	# 	FROM
	# 	(
	# 		SELECT gat.TicketSerialNumber, T.TerDisplayID, L.LocID, L.LocTypeID,
	# 		CASE WHEN PBTH.IsCancelled = false THEN Wager ELSE 0 END AS Wager,
	# 		CASE WHEN PBTH.IsCancelled = false THEN Sales ELSE 0 END AS Sales
	# 		FROM ubt_tmp_V2_GAT gat
	# 		INNER JOIN ztubt_placedbettransactionheader pbth ON 			gat.TicketSerialNumber=pbth.TicketSerialNumber
	# 		INNER JOIN ztubt_Terminal t ON PBTH.TerDisplayID = t.TerDisplayID
	# 		INNER JOIN ztubt_Location l ON t.LocID = l.LocID
	# 		WHERE pbth.ProdID IN (5, 6)
	# 	)GAT
	# 	GROUP BY GAT.TerDisplayID, GAT.LocID, GAT.LocTypeID;

    df_GAT = df_ubt_tmp_V2_GAT.merge(
        df_pbth_prefixed,
        left_on='GAT_TICKETSERIALNUMBER',
        right_on='PBTH_TICKETSERIALNUMBER',
        how='inner'
    ).merge(
        df_terminal_prefixed,
        left_on='PBTH_TERDISPLAYID',
        right_on='TER_TERDISPLAYID',
        how='inner'
    ).merge(
        df_location_prefixed,
        left_on='TER_LOCID',
        right_on='LOC_LOCID',
        how='inner'
    )

    df_GAT = df_GAT[
        df_GAT['PBTH_PRODID'].isin([5,6])
    ]

    df_GAT = df_GAT.assign(
        WAGER=lambda x: x['GAT_WAGER'].where(~x['PBTH_ISCANCELLED'], 0),
        SALES=lambda x: x['GAT_SALES'].where(~x['PBTH_ISCANCELLED'], 0)
    ).groupby(
        ['TER_TERDISPLAYID', 'LOC_LOCID', 'LOC_LOCTYPEID'], as_index=False
    ).agg(
        total_wager = ('WAGER', 'sum'),
        total_sales = ('SALES', 'sum')
    ).assign(
        WAGER = lambda x: ((pd.to_numeric(x['total_wager'], errors='coerce').fillna(0) / 100).round(2)) * 100,
        SALES = lambda x: ((pd.to_numeric(x['total_sales'], errors='coerce').fillna(0) / 100).round(2)) * 100
    ).rename(columns={
        'TER_TERDISPLAYID': 'TERDISPLAYID',
        'LOC_LOCID': 'LOCID',
        'LOC_LOCTYPEID': 'LOCTYPE'
    })[['TERDISPLAYID', 'LOCID', 'LOCTYPE', 'WAGER', 'SALES']]

    df_ubt_tmp_V2_TSNWagerSalesAmountData = df_GAT.copy()
    return df_ubt_tmp_V2_TSNWagerSalesAmountData




def sp_ubt_getrtshopcloud_sport(procdate, vSeq, vGST_Branch, vInputTax, df_V2_TAD_Firstdate, df_V2_GAT, df_ubt_tmp_V2_TAD_SecondDate,logger):

    # Define variables
    vtotalamount = None
    procdate_dt = pd.to_datetime(procdate, format='%Y%m%d')
    v1_docdate = procdate_dt + pd.Timedelta(days=1)
    vFlagisinsert = 0
    vBaseAmount = None
    vnowDayName = procdate_dt.day_of_week # Monday=0, Sunday=6
    vTotalTempAmountPerLocation = 0
    vGST_Branch = vGST_Branch
    vInputTax = vInputTax

    # Define the schema for the output DataFrame
    df_sp_ubt_getrtshopcloud_sport = pd.DataFrame(columns=[
        'IDMMBUSINESSDAY', 'BUSINESSDATE', 'ITEMID', 'TRANSACTIONID',
        'DOCUMENTDATE', 'LINECODE', 'SAPDOCTYPE', 'SAPPOSTINGKEY',
        'SAPCONTROLACCTCODE', 'LINETEXT', 'GLNUMBER', 'SAPTAXCODE',
        'SAPTAXBASEAMOUNT', 'CCCODE', 'SAPASSIGNMENT', 'CURRENCYCODE',
        'AMOUNT', 'PRODUCT', 'DRAWNUMBER', 'CUSTOMER'
    ])

    # Start Transformation Logic

    # Create df_ubt_tmp_V2_TerminalTempData
    df_ubt_tmp_V2_TerminalTempData = df_V2_TAD_Firstdate[
        ((df_V2_TAD_Firstdate['FLAG'].isin(['CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL'])) &
         (df_V2_TAD_Firstdate['PRODUCTNAME'].isin(['SPORTS', 'Offline Orders']))) |
        (~df_V2_TAD_Firstdate['FLAG'].isin(['CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL']))
    ][['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TRANSTYPE']].rename(columns={'TRANSTYPE': 'TYPE'}).reset_index(drop=True)

    # Create df_ubt_tmp_V2_LocationTempData
    df_ubt_tmp_V2_LocationTempData = ubt_tmp_V2_LocationTempData(df_ubt_tmp_V2_TerminalTempData)
    logger.debug(f"[sp_ubt_getrtshopcloud_sport] df_ubt_tmp_V2_LocationTempData created with shape: rows={len(df_ubt_tmp_V2_LocationTempData)}")
    # Create ubt_tmp_V2_TSNWagerSalesAmountData
    df_V2_GAT = df_V2_GAT
    df_ubt_tmp_V2_TSNWagerSalesAmountData = ubt_tmp_V2_TSNWagerSalesAmountData(df_V2_GAT)
    logger.debug(f"[sp_ubt_getrtshopcloud_sport] df_ubt_tmp_V2_TSNWagerSalesAmountData created with shape: rows={len(df_ubt_tmp_V2_TSNWagerSalesAmountData)}")
    # Calculate vTotalAmount
    vTotalAmount = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'].isin([2,4])) &
        ((df_ubt_tmp_V2_LocationTempData['FLAG'] == 'COL') & (df_ubt_tmp_V2_LocationTempData['TYPE'] != 'OO') |
         (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'CAN') |
         (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'RFD') & (df_ubt_tmp_V2_LocationTempData['TYPE'] != 'OO') |
         (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'PAY') |
         (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'SAL') |
         (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'RBT'))]['AMOUNT'].sum()

    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MCOL1',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': np.where(vTotalAmount > 0, '01', '11'),
            'SAPCONTROLACCTCODE': '21100091',
            'LINETEXT': 'INVOICE FOR RETAILERS',
            'GLNUMBER': '12100001',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': np.where(vTotalAmount > 0, vTotalAmount, -vTotalAmount),
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1

    # 	select coalesce(sum(coalesce(Sales,0)),0) as amount into vTotalAmount
	# from ubt_tmp_V2_TSNWagerSalesAmountData where locType in(2,4)	;
    df_temp = df_ubt_tmp_V2_TSNWagerSalesAmountData[
        df_ubt_tmp_V2_TSNWagerSalesAmountData['LOCTYPE'].isin([2,4])
    ]
    vTotalAmount = df_temp['SALES'].sum()
    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MCOL2',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': 'SALES IN ADV FOR RETAILERS',
            'GLNUMBER': '21814004',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAmount,
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1
    # Calculate GST AMOUNT for Retailer
        # select coalesce(sum(coalesce( Wager,0)),0)
        # 	  -coalesce(sum(coalesce( Sales,0)),0) as amt into vTotalAmount
        # from ubt_tmp_V2_TSNWagerSalesAmountData
        # WHERE LocType IN (2, 4);

        # select coalesce(sum(coalesce( Sales,0)),0)
        # 	  +vTotalAmount as amt into vBaseAmount
        # from ubt_tmp_V2_TSNWagerSalesAmountData
        # WHERE LocType IN (2, 4);
    df_temp = df_ubt_tmp_V2_TSNWagerSalesAmountData[
        df_ubt_tmp_V2_TSNWagerSalesAmountData['LOCTYPE'].isin([2,4])
    ]
    vTotalAmount = df_temp['WAGER'].sum() - df_temp['SALES'].sum()
    vBaseAmount = df_temp['SALES'].sum() + vTotalAmount

    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MCOL3',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': 'GOOD & SERVICES TAX FOR RETAILERS',
            'GLNUMBER': '22000011',
            'SAPTAXCODE': vGST_Branch,
            'SAPTAXBASEAMOUNT': vBaseAmount,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAmount,
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1

    # Calculate Sales Comm Amount for Retailer
    # SELECT coalesce(SUM(Amount),0)*-1 as amt into vTotalAmount
	# FROM ubt_tmp_V2_LocationTempData
	# WHERE LocType IN (2, 4) AND FLAG = 'SAL';
    df_temp = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'].isin([2,4])) &
        (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'SAL')
    ]
    vTotalAmount = df_temp['AMOUNT'].sum() * -1


    # INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
	# 	(
	# 		IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
	# 		DocumentDate, LineCode, SAPDocType, SAPPostingKey,
	# 		SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
	# 		SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
	# 		Amount, Product, DrawNumber, Customer)

	# 	select
	# 		NULL, inBusinessDate, NULL, vSeq,
	# 		vl_DocDate, 'MCOL4', 'Y8', '40',
	# 		'', 'SALES COMMISSION FOR RETAILERS', '50200010', '',
	# 		NULL, '', '',  'SGD',
	# 		coalesce(SUM(Amount),0)*-1, ProductName, '', LocDisplayID
	# 	FROM ubt_tmp_V2_LocationTempData
	# 	WHERE LocType IN (2, 4) AND FLAG = 'SAL' and amount <>0
	# 	group by ProductName, LocDisplayID;
    if vTotalAmount != 0:
        df_temp = df_ubt_tmp_V2_LocationTempData[
            (df_ubt_tmp_V2_LocationTempData['LOCTYPE'].isin([2,4])) &
            (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'SAL') &
            (df_ubt_tmp_V2_LocationTempData['AMOUNT'] != 0)
        ]

        df_temp = df_temp.groupby(
            ['PRODUCTNAME', 'LOC_DISPLAYID'], as_index=False
        ).agg({
            'AMOUNT': 'sum'
        }).assign(
            IDMMBUSINESSDAY = None,
            BUSINESSDATE = procdate_dt.date(),
            ITEMID = None,
            TRANSACTIONID = vSeq,
            DOCUMENTDATE = v1_docdate.date(),
            LINECODE = 'MCOL4',
            SAPDOCTYPE = 'Y8',
            SAPPOSTINGKEY = '40',
            SAPCONTROLACCTCODE = '',
            LINETEXT = 'SALES COMMISSION FOR RETAILERS',
            GLNUMBER = '50200010',
            SAPTAXCODE = '',
            SAPTAXBASEAMOUNT = None,
            CCCODE = '',
            SAPASSIGNMENT = '',
            CURRENCYCODE = 'SGD',
            AMOUNT = lambda x: x['AMOUNT'] * -1,
            PRODUCT = lambda x: x['PRODUCTNAME'],
            DRAWNUMBER = '',
            CUSTOMER = lambda x: x['LOC_DISPLAYID']
        )[[
            'IDMMBUSINESSDAY', 'BUSINESSDATE', 'ITEMID', 'TRANSACTIONID',
            'DOCUMENTDATE', 'LINECODE', 'SAPDOCTYPE', 'SAPPOSTINGKEY',
            'SAPCONTROLACCTCODE', 'LINETEXT', 'GLNUMBER', 'SAPTAXCODE',
            'SAPTAXBASEAMOUNT', 'CCCODE', 'SAPASSIGNMENT', 'CURRENCYCODE',
            'AMOUNT', 'PRODUCT', 'DRAWNUMBER', 'CUSTOMER'
        ]]
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, df_temp], ignore_index=True)
        vFlagisinsert = 1


    # Calculate Prizes Paid Amount by Retailer
    # SELECT coalesce(SUM(Amount),0)*-1 as amt into vTotalAmount
	# FROM ubt_tmp_V2_LocationTempData
	# WHERE LocType IN (2, 4) AND FLAG = 'PAY';
    df_temp = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'].isin([2,4])) &
        (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'PAY')
    ]
    vTotalAmount = df_temp['AMOUNT'].sum() * -1

    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MCOL5',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '40',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': 'PRIZES PAID BY RETAILERS',
            'GLNUMBER': '5020218150070020',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAmount,
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1

    # Calculate Redemtion Collection Amount by Retailer
        # 	SELECT coalesce(SUM(Amount),0)*-1 as amt into vTotalAmount
        # FROM ubt_tmp_V2_LocationTempData
        # WHERE LocType IN (2, 4) AND FLAG = 'RFD' and type<> 'OO';
    df_temp = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'].isin([2,4])) &
        (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'RFD') &
        (df_ubt_tmp_V2_LocationTempData['TYPE'] != 'OO')
    ]
    vTotalAmount = df_temp['AMOUNT'].sum() * -1

    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MCOL6',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '40',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': 'REDEMPTION COLLECTION-RETAILERS',
            'GLNUMBER': '21816001',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAmount,
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1

        # Finally, increment vSeq if any insertions were made
        if vFlagisinsert == 1:
            vSeq += 1
            vFlagisinsert = 0
# Branch Normal
# ----------------------------------------
    # select
	# coalesce (sum(case
	# when LocType =1 and flag='COL' and type<>'OO'then coalesce(amount,0)
	# when LocType =1 and flag='CAN'then coalesce(amount,0)
	# -- 2023Jan25 - Include HQLocation 8888
	# -- when LocType =1 and flag='RFD' and type<>'OO' and LocDisplayID<>vHQLocation then coalesce(amount,0)
	# -- when LocType =1 and flag='PAY' and LocDisplayID<>vHQLocation then coalesce(amount,0)
	# -- when LocType =1 and flag='RBT' and LocDisplayID<>vHQLocation then coalesce(amount,0)
	# when LocType =1 and flag='RFD' and type<>'OO' then coalesce(amount,0)
	# when LocType =1 and flag='PAY' then coalesce(amount,0)
	# when LocType =1 and flag='RBT' then coalesce(amount,0)
	# end),0)as amount into vTotalAmount
	# from ubt_tmp_V2_LocationTempData;
    df_temp = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'] == 1) &
        (
            ((df_ubt_tmp_V2_LocationTempData['FLAG'] == 'COL') & (df_ubt_tmp_V2_LocationTempData['TYPE'] != 'OO')) |
            (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'CAN') |
            ((df_ubt_tmp_V2_LocationTempData['FLAG'] == 'RFD') & (df_ubt_tmp_V2_LocationTempData['TYPE'] != 'OO')) |
            (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'PAY') |
            (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'RBT')
        )
    ]
    logger.debug(f"[sp_ubt_getrtshopcloud_sport]✅ df_temp for Branch Normal calculation created with shape: rows={len(df_temp)}")
    vTotalAmount = df_temp['AMOUNT'].sum()
    logger.debug(f"[sp_ubt_getrtshopcloud_sport]✅ vTotalAmount for Branch Normal calculation: {vTotalAmount}")


    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MCOL16',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': np.where(vTotalAmount > 0, '01', '11'),
            'SAPCONTROLACCTCODE': '23100000',
            'LINETEXT': 'INVOICE FOR BRANCHES',
            'GLNUMBER': '12100001',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': np.where(vTotalAmount > 0, vTotalAmount, -vTotalAmount),
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1
    # Calculate Sales Comm Amount for Branch
    # SELECT coalesce(SUM(coalesce(sales,0)),0) as amt into vTotalAmount
	# FROM ubt_tmp_V2_TSNWagerSalesAmountData
	# WHERE LocType =1;
    df_temp = df_ubt_tmp_V2_TSNWagerSalesAmountData[
        df_ubt_tmp_V2_TSNWagerSalesAmountData['LOCTYPE'] == 1
    ]
    vTotalAmount = df_temp['SALES'].sum()

    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MCOL17',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': 'SALES IN ADV FOR BRANCHES',
            'GLNUMBER': '21814004',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAmount,
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1

    # Calculate GST Amount for branch
    # select coalesce(sum(coalesce( Wager,0)),0)
	# 	  -coalesce(sum(coalesce( Sales,0)),0) as amt into vTotalAmount
	# from ubt_tmp_V2_TSNWagerSalesAmountData
	# WHERE LocType=1;

	# select coalesce(sum(coalesce( Sales,0)),0)
	# 	  + vTotalAmount as amt into vBaseAmount
    # from ubt_tmp_V2_TSNWagerSalesAmountData
	# WHERE LocType=1;
    df_temp = df_ubt_tmp_V2_TSNWagerSalesAmountData[
        df_ubt_tmp_V2_TSNWagerSalesAmountData['LOCTYPE'] == 1
    ]
    vTotalAmount = df_temp['WAGER'].sum() - df_temp['SALES'].sum()
    vBaseAmount = df_temp['SALES'].sum() + vTotalAmount
    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MCOL18',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': 'GOOD & SERVICES TAX FOR BRANCHES',
            'GLNUMBER': '22000011',
            'SAPTAXCODE': vGST_Branch,
            'SAPTAXBASEAMOUNT': vBaseAmount,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAmount,
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1

    # Calculate Redemtion Collection Amount by Branch
    # SELECT coalesce(SUM(coalesce(Amount,0)),0)*-1 as amt into vTotalAmount
	# FROM ubt_tmp_V2_LocationTempData
	# WHERE LocType =1 AND FLAG = 'RFD' and type<> 'OO';

    df_temp = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'] == 1) &
        (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'RFD') &
        (df_ubt_tmp_V2_LocationTempData['TYPE'] != 'OO')
    ]
    vTotalAmount = df_temp['AMOUNT'].sum() * -1
    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MCOL20',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '40',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': 'REDEMPTION COLLECTION-BRANCHES',
            'GLNUMBER': '21816001',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAmount,
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1
        if vFlagisinsert == 1:
            vSeq += 1
            vFlagisinsert = 0

    # CasH RECEIPT
    # SELECT coalesce(SUM(coalesce(Amount,0)),0) as amt into vTotalAmount
	# FROM ubt_tmp_V2_LocationTempData
	# WHERE LocType =1 AND FLAG = 'COL' and type<> 'OO';

	# -- Inclusive of Paynow QR Branch
	# SELECT vTotalAmount - sum_paynowqr AS amount into vTotalAmount
	# FROM (SELECT
	# 	SUM(CASE WHEN productname = 'PaynowQR' THEN amount ELSE 0 END) AS sum_paynowqr
	#   FROM  ubt_tmp_V2_LocationTempData
	#   WHERE flag = 'CAS' and type='CL' and LocType =1
	# ) tmp;
    df_temp = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'] == 1) &
        (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'COL') &
        (df_ubt_tmp_V2_LocationTempData['TYPE'] != 'OO')
    ]
    vTotalAmount = df_temp['AMOUNT'].sum()
    df_temp_paynowqr = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'CAS') &
        (df_ubt_tmp_V2_LocationTempData['TYPE'] == 'CL') &
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'] == 1) &
        (df_ubt_tmp_V2_LocationTempData['PRODUCTNAME'] == 'PaynowQR')
    ]
    sum_paynowqr = df_temp_paynowqr['AMOUNT'].sum()
    vTotalAmount = vTotalAmount - sum_paynowqr

    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MDEP1',
            'SAPDOCTYPE': 'YA',
            'SAPPOSTINGKEY': np.where(vTotalAmount > 0, '40', '15'),
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': 'CASH RECEIPTS',
            'GLNUMBER': '10010001',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': abs(vTotalAmount),
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MDEP2',
            'SAPDOCTYPE': 'YA',
            'SAPPOSTINGKEY': np.where(vTotalAmount > 0, '15', '40'),
            'SAPCONTROLACCTCODE': '23100000',
            'LINETEXT': 'CASH RECEIPTS',
            'GLNUMBER': '12100003',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': abs(vTotalAmount),
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1
        if vFlagisinsert == 1:
            vSeq += 1
            vFlagisinsert = 0

# CASH PAYMENTS
#     select
# 	-- 2023Jan25 - Include HQLocation 8888
# 	-- coalesce (sum(case when LocType =1 and flag='PAY' and LocDisplayID <> vHQLocation then 	coalesce(amount,0)
# 	-- when LocType =1 and flag='RFD' and LocDisplayID <> vHQLocation and Type <> 'OO' then coalesce(amount,0)
# 	coalesce (sum(case when LocType =1 and flag='PAY' then coalesce(amount,0)
# 	when LocType =1 and flag='RFD' and Type <> 'OO' then coalesce(amount,0)
# 	when LocType =1 and flag='CAN' then coalesce(amount,0)
# --	when LocType =1 and productname = 'Paynow' and flag='CAS' and type='CL' then coalesce(amount,0) -- Inclusive of Paynow Branch
# 	end),0)as amount into vTotalAmount
# 	from ubt_tmp_V2_LocationTempData;
    df_temp = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'] == 1) &
        (
            (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'PAY') |
            ((df_ubt_tmp_V2_LocationTempData['FLAG'] == 'RFD') & (df_ubt_tmp_V2_LocationTempData['TYPE'] != 'OO')) |
            (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'CAN')
        )
    ]
    vTotalAmount = df_temp['AMOUNT'].sum() * -1

    # SELECT vTotalAmount + sum_paynow AS amount into vTotalAmount
	# FROM (SELECT
	# 	SUM(CASE WHEN productname = 'Paynow' THEN amount ELSE 0 END) AS sum_paynow
	#   FROM  ubt_tmp_V2_LocationTempData
	#   WHERE flag = 'CAS' and type='CL' and LocType =1
	# ) tmp;
    df_temp_paynow = df_ubt_tmp_V2_LocationTempData[
        (df_ubt_tmp_V2_LocationTempData['FLAG'] == 'CAS') &
        (df_ubt_tmp_V2_LocationTempData['TYPE'] == 'CL') &
        (df_ubt_tmp_V2_LocationTempData['LOCTYPE'] == 1) &
        (df_ubt_tmp_V2_LocationTempData['PRODUCTNAME'] == 'Paynow')
    ]
    sum_paynow = df_temp_paynow['AMOUNT'].sum()
    vTotalAmount = vTotalAmount + sum_paynow

    if vTotalAmount != 0:
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MDEP3',
            'SAPDOCTYPE': 'YA',
            'SAPPOSTINGKEY': np.where(vTotalAmount > 0, '01', '50'),
            'SAPCONTROLACCTCODE': '23100000',
            'LINETEXT': 'CASH PAYMENTS',
            'GLNUMBER': '12100003',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': abs(vTotalAmount),
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        newrow = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate_dt.date(),
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate.date(),
            'LINECODE': 'MDEP4',
            'SAPDOCTYPE': 'YA',
            'SAPPOSTINGKEY': np.where(vTotalAmount > 0, '50', '01'),
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': 'CASH PAYMENTS',
            'GLNUMBER': '10010001',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': abs(vTotalAmount),
            'PRODUCT': 'SPORTS',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
        vFlagisinsert = 1
        if vFlagisinsert == 1:
            vSeq += 1
            vFlagisinsert = 0
    if vnowDayName in [0,4]:
        # TODO: Implement logic for updating control table for sequence number
        # refer to sp_ubt_sport.sql - Start lines 588 (INVOICE PERIOD DATA)

        # df_ubt_tmp_V2_TerminalForInvoicePeriodData
        # INSERT INTO ubt_tmp_V2_TerminalForInvoicePeriodData
		# 		SELECT TerDisplayID, ProductName, Amount, FLAG, TransType
		# 		FROM ubt_tmp_V2_TAD_SecondDate
		# 		WHERE (FLAG IN('CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL') AND (ProductName IN 				('SPORTS', 'Offline Orders')))
		# 		OR FLAG NOT IN('CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL');
        df_ubt_tmp_V2_TerminalForInvoicePeriodData = df_ubt_tmp_V2_TAD_SecondDate[
            ((df_ubt_tmp_V2_TAD_SecondDate['FLAG'].isin(['CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL'])) &
             (df_ubt_tmp_V2_TAD_SecondDate['PRODUCTNAME'].isin(['SPORTS', 'Offline Orders']))) |
            (~df_ubt_tmp_V2_TAD_SecondDate['FLAG'].isin(['CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL']))
        ][['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TRANSTYPE']].rename(columns={'TRANSTYPE': 'TYPE'}).reset_index(drop=True)
        # create ubt_tmp_V2_LocationInvoicePeriodData dataframe
        # INSERT INTO ubt_tmp_V2_LocationInvoicePeriodData
		# 		SELECT Loc.LocID, Loc.LocDisplayID, Loc.LocName, Loc.LocTypeID, Loc.IsIBG, Loc.IsGST, 				T.ProductName, MAX(T.Amount), T.FLAG, T.Type
		# 		FROM ubt_tmp_V2_TerminalForInvoicePeriodData T
		# 		INNER JOIN public.ztubt_Terminal Ter ON T.TerDisplayID = Ter.TerDisplayID
		# 		INNER JOIN public.ztubt_Location Loc ON Ter.LocID = Loc.LocID
		# 		WHERE FLAG IN ('FUN','REC')
		# 		GROUP BY Loc.LocID, Loc.LocDisplayID, Loc.LocName, Loc.LocTypeID, Loc.IsIBG, Loc.IsGST,
		# 		T.ProductName, T.FLAG, T.type;

		# 		INSERT INTO ubt_tmp_V2_LocationInvoicePeriodData
		# 		SELECT l.LocID, l.LocDisplayID, l.LocName, l.LocTypeID, l.IsIBG, l.IsGST, ProductName, 				SUM(Amount) AS Amount, FLAG, Type
		# 		FROM ubt_tmp_V2_TerminalForInvoicePeriodData td
		# 		INNER JOIN public.ztubt_terminal t ON td.TerDisplayID = t.TerDisplayID
		# 		INNER JOIN public.ztubt_Location l ON t.LocID = l.LocID
		# 		WHERE --t.IsDeleted = 0 AND l.IsDeleted = 0 AND
		# 		FLAG NOT IN ('FUN','REC')
		# 		GROUP BY l.LocID, l.LocDisplayID, l.LocName, l.LocTypeID, l.IsIBG, l.IsGST, ProductName,
		# 		FLAG, type;

        # 1st part: FLAG IN ('FUN','REC')
        # add prefix
        global df_ztubt_location, df_ztubt_terminal
        df_ubt_tmp_V2_TerminalForInvoicePeriodData = df_ubt_tmp_V2_TerminalForInvoicePeriodData.add_prefix('TAD_')
        df_location_prefixed_inv = df_ztubt_location.add_prefix('LOC_')
        df_terminal_prefixed_inv = df_ztubt_terminal.add_prefix('TER_')


        df_ubt_tmp_V2_LocationInvoicePeriodData = df_ubt_tmp_V2_TerminalForInvoicePeriodData.merge(
            df_terminal_prefixed_inv,
            left_on='TAD_TERDISPLAYID',
            right_on='TER_TERDISPLAYID',
            how='inner'
        ).merge(
            df_location_prefixed_inv,
            left_on='TER_LOCID',
            right_on='LOC_LOCID',
            how='inner'
        )
        df_ubt_tmp_V2_LocationInvoicePeriodData = df_ubt_tmp_V2_LocationInvoicePeriodData[
            df_ubt_tmp_V2_LocationInvoicePeriodData['TAD_FLAG'].isin(['FUN','REC'])
        ].groupby(
            ['LOC_LOCID', 'LOC_LOCDISPLAYID', 'LOC_LOCNAME', 'LOC_LOCTYPEID', 'LOC_ISIBG', 'LOC_ISGST',
             'TAD_PRODUCTNAME', 'TAD_FLAG', 'TAD_TYPE'],
            as_index=False
        ).agg({
            'TAD_AMOUNT': 'max'
        }).rename(columns={
            'LOC_LOCID': 'LOCID',
            'LOC_LOCDISPLAYID': 'LOCDISPLAYID',
            'LOC_LOCNAME': 'LOCNAME',
            'LOC_LOCTYPEID': 'LOCTYPEID',
            'LOC_ISIBG': 'ISIBG',
            'LOC_ISGST': 'ISGST',
            'TAD_PRODUCTNAME': 'PRODUCTNAME',
            'TAD_AMOUNT': 'AMOUNT',
            'TAD_FLAG': 'FLAG',
            'TAD_TYPE': 'TYPE'
        })[[
            'LOCID', 'LOCDISPLAYID', 'LOCNAME', 'LOCTYPEID', 'ISIBG', 'ISGST',
            'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TYPE'
        ]].reset_index(drop=True)

        # 2nd part: FLAG NOT IN ('FUN','REC')
        df_ubt_tmp_V2_LocationInvoicePeriodData_2 = df_ubt_tmp_V2_TerminalForInvoicePeriodData.merge(
            df_terminal_prefixed_inv,
            left_on='TAD_TERDISPLAYID',
            right_on='TER_TERDISPLAYID',
            how='inner'
        ).merge(
            df_location_prefixed_inv,
            left_on='TER_LOCID',
            right_on='LOC_LOCID',
            how='inner'
        )
        df_ubt_tmp_V2_LocationInvoicePeriodData_2 = df_ubt_tmp_V2_LocationInvoicePeriodData_2[
            ~df_ubt_tmp_V2_LocationInvoicePeriodData_2['TAD_FLAG'].isin(['FUN','REC'])
        ].groupby(
            ['LOC_LOCID', 'LOC_LOCDISPLAYID', 'LOC_LOCNAME', 'LOC_LOCTYPEID', 'LOC_ISIBG', 'LOC_ISGST',
             'TAD_PRODUCTNAME', 'TAD_FLAG', 'TAD_TYPE'],
            as_index=False
        ).agg({
            'TAD_AMOUNT': 'sum'
        }).rename(columns={
            'LOC_LOCID': 'LOCID',
            'LOC_LOCDISPLAYID': 'LOCDISPLAYID',
            'LOC_LOCNAME': 'LOCNAME',
            'LOC_LOCTYPEID': 'LOCTYPEID',
            'LOC_ISIBG': 'ISIBG',
            'LOC_ISGST': 'ISGST',
            'TAD_PRODUCTNAME': 'PRODUCTNAME',
            'TAD_AMOUNT': 'AMOUNT',
            'TAD_FLAG': 'FLAG',
            'TAD_TYPE': 'TYPE'
        })[[
            'LOCID', 'LOCDISPLAYID', 'LOCNAME', 'LOCTYPEID', 'ISIBG', 'ISGST',
            'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TYPE'
        ]].reset_index(drop=True)
        df_ubt_tmp_V2_LocationInvoicePeriodData = pd.concat(
            [df_ubt_tmp_V2_LocationInvoicePeriodData,
             df_ubt_tmp_V2_LocationInvoicePeriodData_2],
            ignore_index=True
        )
        # Conver to cents
        df_ubt_tmp_V2_LocationInvoicePeriodData['AMOUNT'] = df_ubt_tmp_V2_LocationInvoicePeriodData['AMOUNT'].fillna(0) * 100

        # select coalesce(sum(Amount),0)*-1 into vTotalAmount
		# 	from ubt_tmp_V2_LocationInvoicePeriodData
		# 	WHERE LocType IN (2,4) AND FLAG = 'GST';

		# 	select coalesce(sum(Amount),0)*-1 into vBaseAmount
		# 	from ubt_tmp_V2_LocationInvoicePeriodData
		# 	WHERE LocType IN (2,4) AND FLAG = 'SAL'and IsGST=true;
        df_temp = df_ubt_tmp_V2_LocationInvoicePeriodData[
            (df_ubt_tmp_V2_LocationInvoicePeriodData['LOCTYPEID'].isin([2,4])) &
            (df_ubt_tmp_V2_LocationInvoicePeriodData['FLAG'] == 'GST')
        ]
        vTotalAmount = df_temp['AMOUNT'].sum() * -1

        df_temp = df_ubt_tmp_V2_LocationInvoicePeriodData[
            (df_ubt_tmp_V2_LocationInvoicePeriodData['LOCTYPEID'].isin([2,4])) &
            (df_ubt_tmp_V2_LocationInvoicePeriodData['FLAG'] == 'SAL') &
            (df_ubt_tmp_V2_LocationInvoicePeriodData['ISGST'] == True)
        ]
        vBaseAmount = df_temp['AMOUNT'].sum() * -1

        if vTotalAmount != 0:
            newrow = {
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': procdate_dt.date(),
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': v1_docdate.date(),
                'LINECODE': 'MTAX1',
                'SAPDOCTYPE': 'Y5',
                'SAPPOSTINGKEY': '11',
                'SAPCONTROLACCTCODE': '21100091',
                'LINETEXT': 'INPUT TAX ON SALES COMMISSION TO RETAILERS',
                'GLNUMBER': '12100001',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'SPORTS',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }
            df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
            newrow = {
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': procdate_dt.date(),
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': v1_docdate.date(),
                'LINECODE': 'MTAX2',
                'SAPDOCTYPE': 'Y5',
                'SAPPOSTINGKEY': '40',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'INPUT TAX ON SALES COMMISSION TO RETAILERS',
                'GLNUMBER': '22005000',
                'SAPTAXCODE': vInputTax,
                'SAPTAXBASEAMOUNT': vBaseAmount,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'SPORTS',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }
            df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
            vFlagisinsert = 1
            if vFlagisinsert == 1:
                vSeq += 1
                vFlagisinsert = 0
        # select LocID,
		#  			coalesce (sum(case when flag='COL' and type<>'OO' then coalesce(amount,0)--INFLOW
		#  		 when flag in('CAN','PAY','RFD','RBT') and type<>'OO' then coalesce(amount,0)--OUTFLOW
		#  		 when flag='SAL' then coalesce(amount,0)--SALES COMMISION
		#  		 when flag='GST' then coalesce(amount,0)--GST
		# 		 when productname = 'PaynowQR' and flag='CAS' and type='CL' then coalesce(-amount,0)--PaynowQR(Incoming)
		# 		 when productname = 'Paynow' and flag='CAS' and type='CL' then coalesce(-amount,0)--Paynow(Outcoming)
		#  		 end),0) as Amount
		# 		 from ubt_tmp_V2_LocationInvoicePeriodData
		# 		 where isIBG=true and loctype in(2,4)
		# 		 group by LocID;

		# 		select coalesce(sum(Amount),0) as amt into vTotalAmount
		# 		from ubt_tmp_V2_Tab where Amount<0;
        df_ubt_tmp_V2_Tab = df_ubt_tmp_V2_LocationInvoicePeriodData[
            (df_ubt_tmp_V2_LocationInvoicePeriodData['ISIBG'] == True) &
            (df_ubt_tmp_V2_LocationInvoicePeriodData['LOCTYPEID'].isin([2,4]))
        ]
        df_ubt_tmp_V2_Tab['AMOUNT_CALC'] = df_ubt_tmp_V2_Tab.apply(
            lambda row: (
                row['AMOUNT'] if (row['FLAG'] == 'COL' and row['TYPE'] != 'OO') else
                row['AMOUNT'] if (row['FLAG'] in ['CAN', 'PAY', 'RFD', 'RBT'] and row['TYPE'] != 'OO') * -1 else
                row['AMOUNT'] if (row['FLAG'] == 'SAL') else
                row['AMOUNT'] if (row['FLAG'] == 'GST') else
                -row['AMOUNT'] if (row['PRODUCTNAME'] == 'PaynowQR' and row['FLAG'] == 'CAS' and row['TYPE'] == 'CL') else
                -row['AMOUNT'] if (row['PRODUCTNAME'] == 'Paynow' and row['FLAG'] == 'CAS' and row['TYPE'] == 'CL') else
                0
            ),
            axis=1
        )

        df_temp = df_ubt_tmp_V2_Tab[
            df_ubt_tmp_V2_Tab['AMOUNT_CALC'] < 0
        ]

        vTotalAmount = df_temp['AMOUNT_CALC'].sum()
        if vTotalAmount < 0:
            newrow = {
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': procdate_dt.date(),
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': v1_docdate.date(),
                'LINECODE': 'MDEP5',
                'SAPDOCTYPE': 'YA',
                'SAPPOSTINGKEY': '01',
                'SAPCONTROLACCTCODE': '21100091',
                'LINETEXT': 'IBG PAYMENTS',
                'GLNUMBER': '12100001',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount * -1,
                'PRODUCT': 'SPORTS',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }
            df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
            newrow = {
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': procdate_dt.date(),
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': v1_docdate.date(),
                'LINECODE': 'MDEP6',
                'SAPDOCTYPE': 'YA',
                'SAPPOSTINGKEY': '50',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'IBG PAYMENTS',
                'GLNUMBER': '11003010',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount * -1,
                'PRODUCT': 'SPORTS',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }
            df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
            vFlagisinsert = 1
            if vFlagisinsert == 1:
                vSeq += 1
                vFlagisinsert = 0



        # IBG Receipts
        # select coalesce(SUM(Amount),0) into vTotalAmount
		# 	FROM ubt_tmp_V2_Tab WHERE Amount > 0;
        df_temp = df_ubt_tmp_V2_Tab[
            df_ubt_tmp_V2_Tab['AMOUNT_CALC'] > 0
        ]
        vTotalAmount = df_temp['AMOUNT_CALC'].sum()
        if vTotalAmount > 0:
            newrow = {
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': procdate_dt.date(),
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': v1_docdate.date(),
                'LINECODE': 'MDEP7',
                'SAPDOCTYPE': 'YA',
                'SAPPOSTINGKEY': '40',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'IBG RECEIPTS',
                'GLNUMBER': '11003060',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'SPORTS',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }
            df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
            newrow = {
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': procdate_dt.date(),
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': v1_docdate.date(),
                'LINECODE': 'MDEP8',
                'SAPDOCTYPE': 'YA',
                'SAPPOSTINGKEY': '15',
                'SAPCONTROLACCTCODE': '21100091',
                'LINETEXT': 'IBG RECEIPTS',
                'GLNUMBER': '12100001',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'SPORTS',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }
            df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)
            vFlagisinsert = 1
            if vFlagisinsert == 1:
                vSeq += 1
                vFlagisinsert = 0


        # MANUAL PAYMENT ENDED DD/MM/YYYY-NON IBG RETAILERS
        # SELECT A.LocID, A.LocDisplayID, A.LocName, A.Seq, A.Amount
		# 		from (SELECT loc.LocID, loc.LocDisplayID, loc.LocName, ROW_NUMBER()OVER(ORDER BY loc.LocID, loc.LocName ) + vSeq - 1 AS Seq, coalesce(sum(case when flag='COL' and type<>'OO' then 				coalesce(amount,0)--INFLOW
		#  		when flag in('CAN','PAY','RFD','RBT') and type<>'OO' then coalesce(amount,0)--OUTFLOW
		#  		when flag='SAL' then coalesce(amount,0)--SALES COMMISION
		#  		when flag='GST' then coalesce(amount,0)--GST
		# 		when productname = 'PaynowQR' and flag='CAS' and type='CL' then coalesce(-amount,0)--PaynowQR(Incoming)
		# 		when productname = 'Paynow' and flag='CAS' and type='CL' then coalesce(-amount,0)--Paynow(Outcoming)
		#  		end),0) as Amount
		#  		fROM ubt_tmp_V2_LocationInvoicePeriodData loc
		# 				WHERE loc.IsIBG <> true AND loc.LocType IN (2, 4)
		# 				GROUP BY loc.LocID, loc.LocDisplayID, loc.LocName
		# 		)A	where A.Amount<>0;

		# 		select count(1) into vTotalTempAmountPerLocation from ubt_tmp_V2_TempAmountPerLocation;

		# 		IF vTotalTempAmountPerLocation > 0 then

		# 		SELECT MAX(Seq) + 1 into vSeq
		# 		FROM ubt_tmp_V2_TempAmountPerLocation;
		# 		end if;
        df_temp = df_ubt_tmp_V2_LocationInvoicePeriodData[
            (df_ubt_tmp_V2_LocationInvoicePeriodData['ISIBG'] != True) &
            (df_ubt_tmp_V2_LocationInvoicePeriodData['LOCTYPEID'].isin([2,4]))
        ]
        df_temp['AMOUNT_CALC'] = df_temp.apply(
            lambda row: (
                row['AMOUNT'] if (row['FLAG'] == 'COL' and row['TYPE'] != 'OO') else
                row['AMOUNT'] if (row['FLAG'] in ['CAN', 'PAY', 'RFD', 'RBT'] and row['TYPE'] != 'OO') * -1 else
                row['AMOUNT'] if (row['FLAG'] == 'SAL') else
                row['AMOUNT'] if (row['FLAG'] == 'GST') else
                -row['AMOUNT'] if (row['PRODUCTNAME'] == 'PaynowQR' and row['FLAG'] == 'CAS' and row['TYPE'] == 'CL') else
                -row['AMOUNT'] if (row['PRODUCTNAME'] == 'Paynow' and row['FLAG'] == 'CAS' and row['TYPE'] == 'CL') else
                0
            ),
            axis=1
        )
        df_ubt_tmp_V2_TempAmountPerLocation = df_temp.groupby(
            ['LOCID', 'LOCDISPLAYID', 'LOCNAME'],
            as_index=False
        ).agg({
            'AMOUNT_CALC': 'sum'
        }).rename(columns={
            'AMOUNT_CALC': 'AMOUNT'
        })[
            ['LOCID', 'LOCDISPLAYID', 'LOCNAME', 'AMOUNT']
        ].reset_index(drop=True)
        df_ubt_tmp_V2_TempAmountPerLocation = df_ubt_tmp_V2_TempAmountPerLocation[
            df_ubt_tmp_V2_TempAmountPerLocation['AMOUNT'] != 0
        ].reset_index(drop=True)
        vTotalTempAmountPerLocation = len(df_ubt_tmp_V2_TempAmountPerLocation)
        if vTotalTempAmountPerLocation > 0:
            vSeq = df_ubt_tmp_V2_TempAmountPerLocation.index.max() + 1 + vSeq
        # INSERT INTO ubt_tmp_V2_TB_RTMS_RTShopCloud
		# 		(
		# 			IDMMBusinessDay, BusinessDate, ItemID, TransactionID,
		# 			DocumentDate, LineCode, SAPDocType, SAPPostingKey,
		# 			SAPControlAcctCode, LineText, GLNumber, SAPTaxCode,
		# 			SAPTaxBaseAmount, CCCode, SAPAssignment, CurrencyCode,
		# 			Amount, Product, DrawNumber, Customer
		# 		)

		# 		SELECT NULL, inBusinessDate,
		# 		NULL, Loc.Seq,
		# 		vl_DocDate,
		# 		CASE WHEN Amount >= 0 AND constant = 1 THEN
		# 			'MPYT1'
		# 		WHEN Amount >= 0 AND constant = 2 THEN
		# 			'MPYT2'
		# 		WHEN Amount < 0 AND constant = 1 THEN
		# 			'MPYT3'
		# 		ELSE
		# 			'MPYT4'
		# 		END, 'Y6',
		# 		CASE WHEN (Amount >= 0 AND constant = 1) OR (Amount < 0 AND constant = 2) THEN
		# 			'11'
		# 		ELSE
		# 			'01'
		# 		END,
		# 		CASE WHEN constant = 1 THEN
		# 			'21100091'
		# 		ELSE
		# 			loc.LocDisplayID
		# 		END,
		# 		CASE WHEN constant = 1 THEN
		# 		'MANUAL PAYMENT ENDED ' || to_char(inBusinessDate, 'DD/MM/YYYY') || '-NON IBG RETAILERS'
		# 		ELSE
		# 			LEFT('MANUAL PAYMENT ENDED ' || to_char(inBusinessDate,'DD/MM/YYYY') || '-' || loc.LocName, 50)
		# 		END, '12100001', '',
		# 		NULL, '', '', 'SGD', CASE WHEN Amount >= 0 THEN Amount ELSE Amount * -1 END,
		# 		'SPORTS', '', ''
		# 		FROM
		# 		(VALUES (1), (2)) AS TempTable(constant),
		# 		ubt_tmp_V2_TempAmountPerLocation loc
		# 		GROUP BY loc.Seq, loc.LocID, loc.LocDisplayID, loc.LocName, TempTable.constant, Amount;
        for index, row in df_ubt_tmp_V2_TempAmountPerLocation.iterrows():
            for constant in [1, 2]:
                amount = row['AMOUNT']
                if amount >= 0 and constant == 1:
                    linecode = 'MPYT1'
                    sappostingkey = '11'
                    sapcontrolacctcode = '21100091'
                    linetext = f'MANUAL PAYMENT ENDED {procdate.strftime("%d/%m/%Y")}-NON IBG RETAILERS'
                elif amount >= 0 and constant == 2:
                    linecode = 'MPYT2'
                    sappostingkey = '11'
                    sapcontrolacctcode = row['LOCDISPLAYID']
                    linetext = f'MANUAL PAYMENT ENDED {procdate.strftime("%d/%m/%Y")}-{row["LOCNAME"]}'[:50]
                elif amount < 0 and constant == 1:
                    linecode = 'MPYT3'
                    sappostingkey = '01'
                    sapcontrolacctcode = '21100091'
                    linetext = f'MANUAL PAYMENT ENDED {procdate.strftime("%d/%m/%Y")}-NON IBG RETAILERS'
                else:
                    linecode = 'MPYT4'
                    sappostingkey = '01'
                    sapcontrolacctcode = row['LOCDISPLAYID']
                    linetext = f'MANUAL PAYMENT ENDED {procdate.strftime("%d/%m/%Y")}-{row["LOCNAME"]}'[:50]

                newrow = {
                    'IDMMBUSINESSDAY': None,
                    'BUSINESSDATE': procdate_dt.date(),
                    'ITEMID': None,
                    'TRANSACTIONID': row.name + vSeq - len(df_ubt_tmp_V2_TempAmountPerLocation),
                    'DOCUMENTDATE': v1_docdate.date(),
                    'LINECODE': linecode,
                    'SAPDOCTYPE': 'Y6',
                    'SAPPOSTINGKEY': sappostingkey,
                    'SAPCONTROLACCTCODE': sapcontrolacctcode,
                    'LINETEXT': linetext,
                    'GLNUMBER': '12100001',
                    'SAPTAXCODE': '',
                    'SAPTAXBASEAMOUNT': None,
                    'CCCODE': '',
                    'SAPASSIGNMENT': '',
                    'CURRENCYCODE': 'SGD',
                    'AMOUNT': abs(amount),
                    'PRODUCT': 'SPORTS',
                    'DRAWNUMBER': '',
                    'CUSTOMER': ''
                }
                df_sp_ubt_getrtshopcloud_sport = pd.concat([df_sp_ubt_getrtshopcloud_sport, pd.DataFrame([newrow])], ignore_index=True)

    return df_sp_ubt_getrtshopcloud_sport, vSeq