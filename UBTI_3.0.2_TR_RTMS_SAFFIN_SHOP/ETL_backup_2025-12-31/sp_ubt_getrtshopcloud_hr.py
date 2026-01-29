# %%
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from Snowflake_connection import snowflake_connection
import os
################################
sp_name = "SP_GETRTSHOPCLOUD"
etl_name = "UBTI_3.0.2_TR_RTMS_SAPFIN_SHOP"
################################
connection = snowflake_connection()
schema = "SPPL_DEV_DWH.SPPL_PUBLIC"



def sp_ubt_getrtshopcloud_hr(inbusinessdate, vSeq, logger, df_V2_TAD_Firstdate, df_V2_GAT, df_ubt_tmp_V2_TAD_SecondDate, vInputTax):

#     # df_V2_TAD_Firstdate = sp_ubt_gettransamountdetails(inbusinessdate,inbusinessdate, logger)
#     df_V2_GAT = sp_ubt_getamounttransaction(inbusinessdate,inbusinessdate, logger).rename(columns={'TKTSERIALNUMBER':'TICKETSERIALNUMBER'})[[
#             'TICKETSERIALNUMBER','WAGER','SECONDWAGER','SALES','SECONDSALES',
#             'SALESCOMM','SECONDSALESCOMM','GST','SECONDGST',
#             'RETURNAMOUNT','WINNINGAMOUNT'
#         ]]

##############################################################################
#  Query data from Snowflake
##############################################################################
    query = f"""
    SELECT
        LOC.LOCID,
        LOC.LOCDISPLAYID,
        LOC.LOCNAME,
        LOC.LOCTYPEID,
        LOC.ISIBG,
        LOC.ISGST,
        LOC.ISHQ,
        TER.TERDISPLAYID
        FROM {schema}.ZTUBT_TERMINAL TER
        INNER JOIN ZTUBT_LOCATION LOC ON TER.LOCID = LOC.LOCID
    """
    df_ztubt_terminal_location = pd.read_sql(query, connection)



    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER
    """
    df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)




    query = f"""
        SELECT *
        FROM {schema}.ZTUBT_GSTCONFIG
    """
    df_ztubt_gstconfig = pd.read_sql(query, connection)


    ##############################################################################
    #  Define Variables
    ##############################################################################
    vFlagIsInsert = 0
    vOutputTax = 'S3'
    vNowDayName = pd.to_datetime(inbusinessdate).dayofweek
    vl_DocDate = pd.to_datetime(inbusinessdate) + pd.Timedelta(days=1)



    df_ztubt_gstconfig['EFFECTIVEFROM'] = pd.to_datetime(df_ztubt_gstconfig['EFFECTIVEFROM'])
    df_ztubt_gstconfig['ENDDATE'] = pd.to_datetime(df_ztubt_gstconfig['ENDDATE'], errors='coerce')
    inbusinessdate = pd.to_datetime(inbusinessdate)
    filtered_df = df_ztubt_gstconfig[
        (inbusinessdate >= df_ztubt_gstconfig['EFFECTIVEFROM']) &
        ((df_ztubt_gstconfig['ENDDATE'].isnull()) | (inbusinessdate <= df_ztubt_gstconfig['ENDDATE']))
    ]
    vGSTRate = filtered_df['GSTRATE'].fillna(0).iloc[0] / 100 if not filtered_df.empty else 0

    ##################################
    vInputTax = vInputTax

    ##############################################################################
    #  ubt_tmp_V2_TerminalTempData
    ##############################################################################
    df_V2_TerminalTempData = (
        df_V2_TAD_Firstdate
            .loc[
                (
                    df_V2_TAD_Firstdate['FLAG'].isin(['CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL']) &
                    df_V2_TAD_Firstdate['PRODUCTNAME'].isin(['HORSE RACING', 'Gate Admission'])
                )
                |
                (~df_V2_TAD_Firstdate['FLAG'].isin(['CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL']))
            ]
            .rename(columns={
                'PRODUCTNAME': 'PRODUCTNAME',
                'TRANSTYPE': 'TYPE'
            })
            [['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TYPE']]
    )

    ##############################################################################
    #  ubt_tmp_V2_LocationTempData
    ##############################################################################
    df_join = df_V2_TerminalTempData.merge(df_ztubt_terminal_location, on='TERDISPLAYID')

    df_V2_LocationTempData = pd.concat([
        df_join[df_join['FLAG'].isin(['FUN','REC'])]
            .groupby(['LOCID','LOCDISPLAYID','LOCNAME','LOCTYPEID','ISIBG','ISGST','PRODUCTNAME','FLAG','TYPE','ISHQ'], as_index=False)
            .agg({'AMOUNT':'max'}),
        df_join[~df_join['FLAG'].isin(['FUN','REC'])]
            .groupby(['LOCID','LOCDISPLAYID','LOCNAME','LOCTYPEID','ISIBG','ISGST','PRODUCTNAME','FLAG','TYPE','ISHQ'], as_index=False)
            .agg({'AMOUNT':'sum'})
    ], ignore_index=True)\
            .rename(columns={'LOCTYPEID':'LOCTYPE'})[
        ['LOCID','LOCDISPLAYID','LOCNAME','LOCTYPE','ISIBG','ISGST','PRODUCTNAME','AMOUNT','FLAG','TYPE','ISHQ']
    ]

    ## CONVERT TO CENTS
    df_V2_LocationTempData['AMOUNT'] = df_V2_LocationTempData['AMOUNT'] * 100
    ##############################################################################
    #  ubt_tmp_V2_TSNWagerSalesAmountData
    ##############################################################################
    df_V2_TSNWagerSalesAmountData = (
        df_V2_GAT
        .merge(df_ztubt_placedbettransactionheader, on='TICKETSERIALNUMBER', how='inner')
        .merge(df_ztubt_terminal_location, on='TERDISPLAYID', how='inner')
        .loc[lambda x: x['PRODID'].isin([1])]
        .assign(
            WAGER=lambda x: x['WAGER'].where(~x['ISCANCELLED'], 0),
            SALES=lambda x: x['SALES'].where(~x['ISCANCELLED'], 0)
        )
        .groupby(['TERDISPLAYID','LOCID','LOCTYPEID'], as_index=False)
        .agg({'WAGER':'sum','SALES':'sum'})
        .assign(
            WAGER=lambda x: (x['WAGER'].fillna(0) / 100).round(2) * 100,
            SALES=lambda x: (x['SALES'].fillna(0) / 100).round(2) * 100
        )
        .rename(columns={'LOCTYPEID':'LOCTYPE'})[[
            'TERDISPLAYID','LOCID','LOCTYPE','WAGER','SALES'
        ]]
    )

    ##############################################################################
    ## RETAILERS NORMAL
    ##############################################################################

    ## Calculate Invoice Amount For Retailers
    cond = (
        df_V2_LocationTempData['LOCTYPE'].isin([2, 4]) &
        (
            ((df_V2_LocationTempData['FLAG'] == 'COL') & (df_V2_LocationTempData['TYPE'] != 'OO')) |
            (df_V2_LocationTempData['FLAG'].isin(['CAN', 'PAY', 'SAL', 'RBT'])) |
            ((df_V2_LocationTempData['FLAG'] == 'RFD') & (df_V2_LocationTempData['TYPE'] != 'OO'))
        )
    )

    vTotalAmount = df_V2_LocationTempData.loc[cond, 'AMOUNT'].fillna(0).sum()
    df_ubt_getrtshopcloud_hr = pd.DataFrame()
    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL1',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '01' if vTotalAmount >= 0 else '11',
                'SAPCONTROLACCTCODE': '21100091',
                'LINETEXT': 'INVOICE FOR RETAILERS',
                'GLNUMBER': '12100001',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': abs(vTotalAmount),
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1

    ## Calculate Sales Amount For Retailers
    vTotalAmount = (
        df_V2_TSNWagerSalesAmountData
        .loc[df_V2_TSNWagerSalesAmountData['LOCTYPE'].isin([2, 4]), 'SALES']
        .fillna(0)
        .sum()
    )

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL2',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '50',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'SALES IN ADV FOR RETAILERS',
                'GLNUMBER': '21814009',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1


    ## Calculate GST Amount For Retailers
    df = df_V2_TSNWagerSalesAmountData.loc[
        df_V2_TSNWagerSalesAmountData['LOCTYPE'].isin([2, 4])
    ]

    wager_sum = df['WAGER'].fillna(0).sum()
    sales_sum = df['SALES'].fillna(0).sum()

    vTotalAmount = wager_sum - sales_sum
    vBaseAmount = sales_sum + vTotalAmount

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL3',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '50',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'GOOD & SERVICES TAX FOR RETAILERS',
                'GLNUMBER': '22000013',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1


    ## Calculate Sales Comm Amount For Retailers
    df = df_V2_LocationTempData.loc[
        (df_V2_LocationTempData['LOCTYPE'].isin([2, 4])) &
        (df_V2_LocationTempData['FLAG'] == 'SAL')
    ]

    vTotalAmount = -df['AMOUNT'].fillna(0).sum()

    if vTotalAmount != 0:
        grp = (
            df.loc[df['AMOUNT'] != 0]
            .groupby(['PRODUCTNAME', 'LOCDISPLAYID'], as_index=False)['AMOUNT']
            .sum()
        )

        rows = []
        for _, r in grp.iterrows():
            rows.append({
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL4',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '40',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'SALES COMMISSION FOR RETAILERS',
                'GLNUMBER': '50200010',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': -r['AMOUNT'],
                'PRODUCT': r['PRODUCTNAME'],
                'DRAWNUMBER': '',
                'CUSTOMER': r['LOCDISPLAYID']
            })

        df_ubt_getrtshopcloud_hr = pd.concat(
            [df_ubt_getrtshopcloud_hr, pd.DataFrame(rows)],
            ignore_index=True
        )

        vFlagIsInsert = 1


    ## Calculate Prizes Paid Amount by Retailer
    df = df_V2_LocationTempData.loc[
        (df_V2_LocationTempData['LOCTYPE'].isin([2, 4])) &
        (
            (df_V2_LocationTempData['FLAG'] == 'PAY') |
            (
                (df_V2_LocationTempData['FLAG'] == 'RFD') &
                (df_V2_LocationTempData['TYPE'] != 'OO')
            )
        )
    ]

    vTotalAmount = -df['AMOUNT'].fillna(0).sum()

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL5',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '40',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'PRIZES PAID BY RETAILERS',
                'GLNUMBER': '21815016',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1


    ## Calculate Rebate Amount by Retailer
    df = df_V2_LocationTempData.loc[
        (df_V2_LocationTempData['LOCTYPE'].isin([2, 4])) &
        (df_V2_LocationTempData['FLAG'] == 'RBT')
    ]

    vTotalAmount = -df['AMOUNT'].fillna(0).sum()

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL6',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '40',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'REBATE FOR RETAILERS',
                'GLNUMBER': '21100211',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1


    ####### Reset variable
    if vFlagIsInsert == 1:
        vSeq += 1

    vFlagIsInsert = 0
    #######


    ## Calculate Invoice Amount For Branches
    vTotalAmount = df_V2_LocationTempData.loc[
        df_V2_LocationTempData['LOCTYPE'] == 1,
        'AMOUNT'
    ].where(
        (df_V2_LocationTempData['FLAG'].isin(['COL', 'CAN', 'RFD', 'PAY', 'RBT'])) &
        ((df_V2_LocationTempData['FLAG'] != 'RFD') | (df_V2_LocationTempData['TYPE'] != 'OO')) &
        ((df_V2_LocationTempData['FLAG'] != 'COL') | (df_V2_LocationTempData['TYPE'] != 'OO'))
    ).sum()
    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL16',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '01' if vTotalAmount >= 0 else '11',
                'SAPCONTROLACCTCODE': '23100000',
                'LINETEXT': 'INVOICE FOR BRANCHES',
                'GLNUMBER': '12100003',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount if vTotalAmount >= 0 else vTotalAmount * -1,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1


    ## Calculate Sales Amount For Branches
    vTotalAmount = df_V2_TSNWagerSalesAmountData.loc[
        df_V2_TSNWagerSalesAmountData['LOCTYPE'] == 1,
        'SALES'
    ].fillna(0).sum()

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL17',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '50',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'SALES IN ADV FOR BRANCHES',
                'GLNUMBER': '21814009',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1


    ## Calculate GST Amount For Branches
    vTotalAmount = df_V2_TSNWagerSalesAmountData.loc[
        df_V2_TSNWagerSalesAmountData['LOCTYPE'] == 1,
        'WAGER'
    ].fillna(0).sum() - df_V2_TSNWagerSalesAmountData.loc[
        df_V2_TSNWagerSalesAmountData['LOCTYPE'] == 1,
        'SALES'
    ].fillna(0).sum()

    vBaseAmount = df_V2_TSNWagerSalesAmountData.loc[
        df_V2_TSNWagerSalesAmountData['LOCTYPE'] == 1,
        'SALES'
    ].fillna(0).sum() + vTotalAmount

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL18',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '50',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'GOOD & SERVICES TAX FOR BRANCHES',
                'GLNUMBER': '22000013',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1



    ## Calculate Prizes Paid Amount by Branch

    vTotalAmount = df_V2_LocationTempData.loc[
        (df_V2_LocationTempData['LOCTYPE'] == 1) &
        (
            (df_V2_LocationTempData['FLAG'] == 'PAY') |
            (
                (df_V2_LocationTempData['FLAG'] == 'RFD') & (df_V2_LocationTempData['TYPE'] != 'OO')
            )
        ),
        'AMOUNT'
    ].fillna(0).sum() * -1

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL19',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '40',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'PRIZES PAID BY BRANCHES',
                'GLNUMBER': '21815016',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1


    ## Calculate Rebate Amount by Branches
    vTotalAmount = df_V2_LocationTempData.loc[
        (df_V2_LocationTempData['LOCTYPE'] == 1) &
        (df_V2_LocationTempData['FLAG'] == 'RBT'),
        'AMOUNT'
    ].fillna(0).sum() * -1

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL20',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '40',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'REBATE FOR BRANCHES',
                'GLNUMBER': '21100211',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1

    ######## Reset variable
    if vFlagIsInsert == 1:
        vSeq += 1

    vFlagIsInsert = 0
    ########

    ##############################################################################
    ## BRANCHES GATE ADMISSION
    ##############################################################################

    df_gateadmission = df_V2_LocationTempData.loc[
        (df_V2_LocationTempData['LOCTYPE'] == 1) &
        (df_V2_LocationTempData['FLAG'] == 'COL') &
        (df_V2_LocationTempData['PRODUCTNAME'] == 'Gate Admission')
    ].groupby('LOCDISPLAYID')['AMOUNT'].sum().reset_index()

    df_gateadmission['AMOUNT'] = df_gateadmission['AMOUNT'].fillna(0)

    ## Invoice for Branches
    vTotalAmount = df_gateadmission['AMOUNT'].sum()
    if vTotalAmount != 0:
        df_V2_LocationTempData = pd.concat([
            df_V2_LocationTempData,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MCOL21',
                'SAPDOCTYPE': 'Y8',
                'SAPPOSTINGKEY': '01' if vTotalAmount >= 0 else '11',
                'SAPCONTROLACCTCODE': '23100000',
                'LINETEXT': 'INVOICE FOR BRANCHES',
                'GLNUMBER': '12100003',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': vTotalAmount if vTotalAmount >= 0 else vTotalAmount * -1,
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1

    ## Gate Admission is for customers who are paying to watch Horse Racing
    for index, row in df_gateadmission.iterrows():
        vLocDisplayID = row['LOCDISPLAYID']
        vTotalAmount = row['AMOUNT']
        vBaseAmount = vTotalAmount * 100 / (100 + vGSTRate * 100)

        vTotalAmount -= vBaseAmount

        if vTotalAmount != 0:
            df_ubt_getrtshopcloud_hr = pd.concat([
                df_ubt_getrtshopcloud_hr,
                pd.DataFrame([{
                    'IDMMBUSINESSDAY': None,
                    'BUSINESSDATE': inbusinessdate,
                    'ITEMID': None,
                    'TRANSACTIONID': vSeq,
                    'DOCUMENTDATE': vl_DocDate,
                    'LINECODE': 'MCOL23',
                    'SAPDOCTYPE': 'Y8',
                    'SAPPOSTINGKEY': '50',
                    'SAPCONTROLACCTCODE': '',
                    'LINETEXT': f'GATEADMISSION {inbusinessdate.strftime("%d/%m/%Y")}-RET NO-{vLocDisplayID}',
                    'GLNUMBER': '22000000',
                    'SAPTAXCODE': vOutputTax,
                    'SAPTAXBASEAMOUNT': vBaseAmount,
                    'CCCODE': '',
                    'SAPASSIGNMENT': '',
                    'CURRENCYCODE': 'SGD',
                    'AMOUNT': vTotalAmount,
                    'PRODUCT': 'HORSE RACING',
                    'DRAWNUMBER': '',
                    'CUSTOMER': ''
                }])
            ], ignore_index=True)

            vFlagIsInsert = 1

        if vBaseAmount != 0:
            df_ubt_getrtshopcloud_hr = pd.concat([
                df_ubt_getrtshopcloud_hr,
                pd.DataFrame([{
                    'IDMMBUSINESSDAY': None,
                    'BUSINESSDATE': inbusinessdate,
                    'ITEMID': None,
                    'TRANSACTIONID': vSeq,
                    'DOCUMENTDATE': vl_DocDate,
                    'LINECODE': 'MCOL22',
                    'SAPDOCTYPE': 'Y8',
                    'SAPPOSTINGKEY': '50',
                    'SAPCONTROLACCTCODE': '',
                    'LINETEXT': f'GATEADMISSION {inbusinessdate.strftime("%d/%m/%Y")}-RET NO-{vLocDisplayID}',
                    'GLNUMBER': '41090020',
                    'SAPTAXCODE': '',
                    'SAPTAXBASEAMOUNT': None,
                    'CCCODE': '',
                    'SAPASSIGNMENT': '',
                    'CURRENCYCODE': 'SGD',
                    'AMOUNT': vBaseAmount,
                    'PRODUCT': 'HORSE RACING',
                    'DRAWNUMBER': '',
                    'CUSTOMER': vLocDisplayID
                }])
            ], ignore_index=True)

            vFlagIsInsert = 1

    ###### Reset variable
    if vFlagIsInsert == 1:
        vSeq += 1

    vFlagIsInsert = 0
    #######

    ##############################################################################
    ## CASH RECEIPTS
    ##############################################################################

    ## Calculate Cash Receipts Amount
    vTotalAmount = df_V2_LocationTempData.loc[
        (df_V2_LocationTempData['LOCTYPE'] == 1) &
        (df_V2_LocationTempData['FLAG'] == 'COL') &
        (df_V2_LocationTempData['TYPE'] != 'OO') |
        ((df_V2_LocationTempData['LOCTYPE'] == 1) &
        (df_V2_LocationTempData['FLAG'] == 'COL') &
        (df_V2_LocationTempData['PRODUCTNAME'] == 'Gate Admission'))
    ]['AMOUNT'].fillna(0).sum()

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MDEP1',
                'SAPDOCTYPE': 'YA',
                'SAPPOSTINGKEY': '40' if vTotalAmount > 0 else '15',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'HR CASH RECEIPTS',
                'GLNUMBER': '10010001',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': abs(vTotalAmount),
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MDEP2',
                'SAPDOCTYPE': 'YA',
                'SAPPOSTINGKEY': '15' if vTotalAmount > 0 else '40',
                'SAPCONTROLACCTCODE': '23100000',
                'LINETEXT': 'HR CASH RECEIPTS',
                'GLNUMBER': '12100003',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': abs(vTotalAmount),
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        vFlagIsInsert = 1

    ###### Reset variable
    if vFlagIsInsert == 1:
        vSeq += 1

    vFlagIsInsert = 0
    #######

    ##############################################################################
    # ##### CASH PAYMENTS
    ###############################################################################


    # select
	# -- 2023Jan25 - Include HQLocation 8888
	# -- coalesce (sum(case when LocType =1 and flag='PAY' and LocDisplayID <> vHQLocation then 	coalesce(amount,0)
	# -- when LocType =1 and flag='RFD' and LocDisplayID <> vHQLocation and Type <> 'OO' then coalesce(amount,0)
	# coalesce (sum(case when LocType =1 and flag='PAY' then coalesce(amount,0)
	# when LocType =1 and flag='RFD' and Type <> 'OO' then coalesce(amount,0)
	# when LocType =1 and flag='CAN' then coalesce(amount,0)
	# when LocType =1 and flag='RBT' then coalesce(amount,0) --2024Sep24: include Rebate
	# end),0)as amount into vTotalAmount
	# from ubt_tmp_V2_LocationTempData;

    df_V2_LocationTempData['FLAG'] = df_V2_LocationTempData['FLAG'].astype(str)
    vTotalAmount = df_V2_LocationTempData.loc[
        df_V2_LocationTempData['LOCTYPE'] == 1,
        'AMOUNT'
    ].where(
        (df_V2_LocationTempData['FLAG'] == 'PAY') |
        ((df_V2_LocationTempData['FLAG'] == 'RFD') & (df_V2_LocationTempData['TYPE'] != 'OO')) |
        (df_V2_LocationTempData['FLAG'] == 'CAN') |
        (df_V2_LocationTempData['FLAG'] == 'RBT')
    ).fillna(0).sum() * - 1

    # ## Calculate Cash Payout Amount
    # vTotalAmount = df_V2_LocationTempData.loc[
    #     df_V2_LocationTempData['LOCTYPE'] == 1,
    #     'AMOUNT'
    # ].where(
    #     df_V2_LocationTempData['FLAG'].isin(['PAY', 'RFD', 'CAN', 'RBT'])
    # ).fillna(0).sum()

    # vTotalAmount *= -1

    if vTotalAmount != 0:
        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MDEP3',
                'SAPDOCTYPE': 'YA',
                'SAPPOSTINGKEY': '01' if vTotalAmount > 0 else '50',
                'SAPCONTROLACCTCODE': '23100000',
                'LINETEXT': 'HR CASH PAYMENTS',
                'GLNUMBER': '12100003',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': abs(vTotalAmount),
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        df_ubt_getrtshopcloud_hr = pd.concat([
            df_ubt_getrtshopcloud_hr,
            pd.DataFrame([{
                'IDMMBUSINESSDAY': None,
                'BUSINESSDATE': inbusinessdate,
                'ITEMID': None,
                'TRANSACTIONID': vSeq,
                'DOCUMENTDATE': vl_DocDate,
                'LINECODE': 'MDEP4',
                'SAPDOCTYPE': 'YA',
                'SAPPOSTINGKEY': '50' if vTotalAmount > 0 else '01',
                'SAPCONTROLACCTCODE': '',
                'LINETEXT': 'HR CASH PAYMENTS',
                'GLNUMBER': '10010001',
                'SAPTAXCODE': '',
                'SAPTAXBASEAMOUNT': None,
                'CCCODE': '',
                'SAPASSIGNMENT': '',
                'CURRENCYCODE': 'SGD',
                'AMOUNT': abs(vTotalAmount),
                'PRODUCT': 'HORSE RACING',
                'DRAWNUMBER': '',
                'CUSTOMER': ''
            }])
        ], ignore_index=True)

        ######## Reset variable
        vFlagIsInsert = 1
        if vFlagIsInsert == 1:
            vSeq += 1

        vFlagIsInsert = 0
        ########
    ##############################################################################
    ## INVOICE PERIOD DATA
    ##############################################################################

    if vNowDayName in (0, 4):

        df_V2_TerminalForInvoicePeriodData = df_ubt_tmp_V2_TAD_SecondDate.loc[
            ((df_ubt_tmp_V2_TAD_SecondDate['FLAG'].isin(['CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL'])) &
            (df_ubt_tmp_V2_TAD_SecondDate['ProductName'] == 'HORSE RACING')) |
            ~df_ubt_tmp_V2_TAD_SecondDate['FLAG'].isin(['CAN', 'COL', 'GST', 'PAY', 'RBT', 'RFD', 'SAL'])
        ][['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'FLAG', 'TRANSTYPE']]

        df_V2_LocationInvoicePeriodData = pd.DataFrame()
        df_V2_LocationInvoicePeriodData = df_V2_LocationInvoicePeriodData.append(
            df_V2_TerminalForInvoicePeriodData.merge(
                df_ztubt_terminal_location,
                left_on='TERDISPLAYID',
                right_on='TERDISPLAYID'
            ).loc[df_V2_TerminalForInvoicePeriodData['FLAG'].isin(['FUN', 'REC'])]
            .groupby(['LOCID', 'LOCDISPLAYID', 'LOCNAME', 'LOCTYPEID', 'ISIBG', 'ISGST', 'PRODUCTNAME', 'FLAG', 'TRANSTYPE'])
            .agg({'AMOUNT': 'max'}).reset_index()
        )

        df_V2_LocationInvoicePeriodData = df_V2_LocationInvoicePeriodData.append(
            df_V2_TerminalForInvoicePeriodData.merge(
                df_ztubt_terminal_location,
                left_on='TERDISPLAYID',
                right_on='TERDISPLAYID'
            ).loc[~df_V2_TerminalForInvoicePeriodData['FLAG'].isin(['FUN', 'REC'])]
            .groupby(['LOCID', 'LOCDISPLAYID', 'LOCNAME', 'LOCTYPEID', 'ISIBG', 'ISGST', 'PRODUCTNAME', 'FLAG', 'TRANSTYPE'])
            .agg({'AMOUNT': 'sum'}).reset_index()
        )

        df_V2_LocationInvoicePeriodData['AMOUNT'] *= 100

        vTotalAmount = df_V2_LocationInvoicePeriodData.loc[
            (df_V2_LocationInvoicePeriodData['LOCTYPEID'].isin([2, 4])) &
            (df_V2_LocationInvoicePeriodData['FLAG'] == 'GST'),
            'AMOUNT'
        ].sum() * -1

        vBaseAmount = df_V2_LocationInvoicePeriodData.loc[
            (df_V2_LocationInvoicePeriodData['LOCTYPEID'].isin([2, 4])) &
            (df_V2_LocationInvoicePeriodData['FLAG'] == 'SAL') &
            (df_V2_LocationInvoicePeriodData['ISGST'] == True),
            'AMOUNT'
        ].sum() * -1

        if vTotalAmount != 0:

            df_ubt_getrtshopcloud_hr = pd.concat([
                df_ubt_getrtshopcloud_hr,
                pd.DataFrame([{
                    'IDMMBUSINESSDAY': None,
                    'BUSINESSDATE': inbusinessdate,
                    'ITEMID': None,
                    'TRANSACTIONID': vSeq,
                    'DOCUMENTDATE': vl_DocDate,
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
                    'PRODUCT': 'HORSE RACING',
                    'DRAWNUMBER': '',
                    'CUSTOMER': ''
                }])
            ], ignore_index=True)

            df_ubt_getrtshopcloud_hr = pd.concat([
                df_ubt_getrtshopcloud_hr,
                pd.DataFrame([{
                    'IDMMBUSINESSDAY': None,
                    'BUSINESSDATE': inbusinessdate,
                    'ITEMID': None,
                    'TRANSACTIONID': vSeq,
                    'DOCUMENTDATE': vl_DocDate,
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
                    'PRODUCT': 'HORSE RACING',
                    'DRAWNUMBER': '',
                    'CUSTOMER': ''
                }])
            ], ignore_index=True)

            vFlagIsInsert = 1

    ################################################################################################################
        if vFlagIsInsert == 1:
            vSeq += 1

        vFlagIsInsert = 0
    ################################################################################################################

        df_V2_Tab = df_V2_LocationInvoicePeriodData.loc[
            (df_V2_LocationInvoicePeriodData['ISIBG'] == True) &
            (df_V2_LocationInvoicePeriodData['LOCTYPE'].isin([2, 4]))
        ].groupby('LOCID').agg(
            Amount=lambda x: (
                x.where((df_V2_LocationInvoicePeriodData['FLAG'] == 'COL') &
                        (df_V2_LocationInvoicePeriodData['TYPE'] != 'OO')).sum() +
                x.where((df_V2_LocationInvoicePeriodData['FLAG'].isin(['CAN', 'PAY', 'RFD', 'RBT']) &
                        (df_V2_LocationInvoicePeriodData['TYPE'] != 'OO'))).sum() +
                x.where(df_V2_LocationInvoicePeriodData['FLAG'] == 'SAL').sum() +
                x.where(df_V2_LocationInvoicePeriodData['FLAG'] == 'GST').sum()
            )
        ).reset_index()
        vTotalAmount = df_V2_Tab.loc[df_V2_Tab['AMOUNT'] < 0, 'AMOUNT'].sum()
    ## IBG PAYMENTS
        if vTotalAmount < 0:
            df_ubt_getrtshopcloud_hr = pd.concat([
                df_ubt_getrtshopcloud_hr,
                pd.DataFrame([{
                    'IDMMBUSINESSDAY': None,
                    'BUSINESSDATE': inbusinessdate,
                    'ITEMID': None,
                    'TRANSACTIONID': vSeq,
                    'DOCUMENTDATE': vl_DocDate,
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
                    'PRODUCT': 'HORSE RACING',
                    'DRAWNUMBER': '',
                    'CUSTOMER': ''
                }])
            ], ignore_index=True)

            df_ubt_getrtshopcloud_hr = pd.concat([
                df_ubt_getrtshopcloud_hr,
                pd.DataFrame([{
                    'IDMMBUSINESSDAY': None,
                    'BUSINESSDATE': inbusinessdate,
                    'ITEMID': None,
                    'TRANSACTIONID': vSeq,
                    'DOCUMENTDATE': vl_DocDate,
                    'LINECODE': 'MDEP6',
                    'SAPDOCTYPE': 'YA',
                    'SAPPOSTINGKEY': '50',
                    'SAPCONTROLACCTCODE': '11003010',
                    'LINETEXT': 'IBG PAYMENTS',
                    'GLNUMBER': '',
                    'SAPTAXCODE': '',
                    'SAPTAXBASEAMOUNT': None,
                    'CCCODE': '',
                    'SAPASSIGNMENT': '',
                    'CURRENCYCODE': 'SGD',
                    'AMOUNT': vTotalAmount * -1,
                    'PRODUCT': 'HORSE RACING',
                    'DRAWNUMBER': '',
                    'CUSTOMER': ''
                }])
            ], ignore_index=True)

            vFlagIsInsert = 1

    ################################################################################################################
        if vFlagIsInsert == 1:
            vSeq += 1

        vFlagIsInsert = 0
    ################################################################################################################
        ## IBG RECEIPTS
        ## Calculate IBG Amount for IBG Retailer

        vTotalAmount = df_V2_Tab.loc[df_V2_Tab['AMOUNT'] > 0, 'AMOUNT'].sum()

        if vTotalAmount > 0:
            df_ubt_getrtshopcloud_hr = pd.concat([
                df_ubt_getrtshopcloud_hr,
                pd.DataFrame([{
                    'IDMMBUSINESSDAY': None,
                    'BUSINESSDATE': inbusinessdate,
                    'ITEMID': None,
                    'TRANSACTIONID': vSeq,
                    'DOCUMENTDATE': vl_DocDate,
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
                    'PRODUCT': 'HORSE RACING',
                    'DRAWNUMBER': '',
                    'CUSTOMER': ''
                }])
            ], ignore_index=True)

            df_ubt_getrtshopcloud_hr = pd.concat([
                df_ubt_getrtshopcloud_hr,
                pd.DataFrame([{
                    'IDMMBUSINESSDAY': None,
                    'BUSINESSDATE': inbusinessdate,
                    'ITEMID': None,
                    'TRANSACTIONID': vSeq,
                    'DOCUMENTDATE': vl_DocDate,
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
                    'PRODUCT': 'HORSE RACING',
                    'DRAWNUMBER': '',
                    'CUSTOMER': ''
                }])
            ], ignore_index=True)

            vFlagIsInsert = 1

    ################################################################################################################
        if vFlagIsInsert == 1:
            vSeq += 1

        vFlagIsInsert = 0
    ################################################################################################################

    ## MANUAL PAYMENT ENDED DD/MM/YYYY-NON IBG RETAILERS

        df_V2_TempAmountPerLocation = df_V2_LocationInvoicePeriodData.loc[
            (df_V2_LocationInvoicePeriodData['ISIBG'] != True) &
            (df_V2_LocationInvoicePeriodData['LOCTYPE'].isin([2, 4]))
        ].groupby(['LOCID', 'LOCDISPLAYID', 'LOCNAME']).agg(
            Amount=lambda x: (
                x.where((df_V2_LocationInvoicePeriodData['FLAG'] == 'COL') &
                        (df_V2_LocationInvoicePeriodData['TYPE'] != 'OO')).sum() +
                x.where((df_V2_LocationInvoicePeriodData['FLAG'].isin(['CAN', 'PAY', 'RFD', 'RBT']) &
                        (df_V2_LocationInvoicePeriodData['TYPE'] != 'OO'))).sum() +
                x.where(df_V2_LocationInvoicePeriodData['FLAG'] == 'SAL').sum() +
                x.where(df_V2_LocationInvoicePeriodData['FLAG'] == 'GST').sum()
            )
        ).reset_index()

        df_V2_TempAmountPerLocation.columns = df_V2_TempAmountPerLocation.columns.str.upper()
        df_V2_TempAmountPerLocation['SEQ'] = range(1, len(df_V2_TempAmountPerLocation) + 1)
        df_V2_TempAmountPerLocation = df_V2_TempAmountPerLocation[df_V2_TempAmountPerLocation['AMOUNT'] != 0]
        vTotalTempAmountPerLocation = len(df_V2_TempAmountPerLocation)
        if vTotalTempAmountPerLocation > 0:
            vSeq = df_V2_TempAmountPerLocation['SEQ'].max() + 1
            for constant in [1, 2]:
                temp_records = df_V2_TempAmountPerLocation.copy()
                temp_records['LINECODE'] = temp_records.apply(lambda row: 'MPYT1' if (row['AMOUNT'] >= 0 and constant == 1) else
                                                            ('MPYT2' if (row['AMOUNT'] >= 0 and constant == 2) else
                                                            ('MPYT3' if (row['AMOUNT'] < 0 and constant == 1) else 'MPYT4')), axis=1)

                temp_records['SAPPOSTINGKEY'] = temp_records.apply(lambda row: '11' if ((row['AMOUNT'] >= 0 and constant == 1) or (row['AMOUNT'] < 0 and constant == 2)) else '01', axis=1)
                temp_records['SAPCONTROLACCTCODE'] = temp_records.apply(lambda row: '21100091' if constant == 1 else row['LOCDISPLAYID'], axis=1)

                temp_records['LINETEXT'] = temp_records.apply(lambda row: f'MANUAL PAYMENT ENDED {inbusinessdate.strftime("%d/%m/%Y")}-NON IBG RETAILERS' if constant == 1 else
                                                            (f'MANUAL PAYMENT ENDED {inbusinessdate.strftime("%d/%m/%Y")}-{row["LOCNAME"]}')[:50], axis=1)

                temp_records['AMOUNT'] = temp_records['AMOUNT'].apply(lambda x: x if x >= 0 else x * -1)

                temp_records = temp_records[['LOCID', 'SEQ', 'LINECODE', 'SAPDOCTYPE', 'SAPPOSTINGKEY', 'SAPCONTROLACCTCODE', 'LINETEXT', 'GLNUMBER', 'SAPTAXCODE', 'SAPTAXBASEAMOUNT', 'CCCODE', 'SAPASSIGNMENT', 'CURRENCYCODE', 'AMOUNT']]

                temp_records['IDMMBUSINESSDAY'] = None
                temp_records['BUSINESSDATE'] = inbusinessdate
                temp_records['ITEMID'] = None
                temp_records['TRANSACTIONID'] = vSeq
                temp_records['DOCUMENTDATE'] = vl_DocDate
                temp_records['PRODUCT'] = 'HORSE RACING'
                temp_records['DRAWNUMBER'] = ''
                temp_records['CUSTOMER'] = ''

                df_ubt_getrtshopcloud_hr = pd.concat([df_ubt_getrtshopcloud_hr, temp_records], ignore_index=True)

    return df_ubt_getrtshopcloud_hr



