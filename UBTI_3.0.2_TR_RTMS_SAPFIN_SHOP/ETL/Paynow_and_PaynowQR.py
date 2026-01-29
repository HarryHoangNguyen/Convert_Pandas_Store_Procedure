import pandas as pd

def PayNow_and_PaynowQR(df_V2_LocationTempData, procdate, vSeq, v1_docdate, vARCode_Q_Retailer,
                 vgstrate, vOutputTax,vARCode_P_Branch, vARCode_P_Retailer, vARCode_Q_Branch):
    ## calculate Paynow QR charges for retailer
    # Filtered df_V2_LocationTempData
    filtered_df = df_V2_LocationTempData[
                    (df_V2_LocationTempData['FLAG'] == 'CAS') &
                    (df_V2_LocationTempData['LOCTYPE'].isin([2,4]))
                    ]
    # sum AMOUNT have conditions
    vTotalAMOUNT = (
        filtered_df['AMOUNT']
        .where((filtered_df['PRODUCTNAME'] == 'PaynowQR') & (filtered_df['TYPE'] == 'TF'), 0)
        .sum()
    )
    # Define ubt_tmp_V2_TB_RTMS_RTShopCloud
    df_V2_TB_RTMS_RTShopCloud = pd.DataFrame(columns=[
        'IDMMBUSINESSDAY', 'BUSINESSDATE', 'ITEMID', 'TRANSACTIONID',
        'DOCUMENTDATE', 'LINECODE', 'SAPDOCTYPE', 'SAPPOSTINGKEY',
        'SAPCONTROLACCTCODE', 'LINETEXT', 'GLNUMBER', 'SAPTAXCODE',
        'SAPTAXBASEAMOUNT', 'CCCODE', 'SAPASSIGNMENT', 'CURRENCYCODE',
        'AMOUNT', 'PRODUCT', 'DRAWNUMBER', 'CUSTOMER'
    ])

    # Define vFlagIsInsert
    vFlagIsInsert = 0

    #
    if vTotalAMOUNT != 0:
        line_text = f"PAYNOW QR TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT E-PYT BANK FEE"
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL21',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '01',
            'SAPCONTROLACCTCODE': vARCode_Q_Retailer,
            'LINETEXT': line_text,
            'GLNUMBER': '12100000',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        # concat
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # Calculate vBaseAMOUNT
    vBaseAMOUNT = vTotalAMOUNT * 100 / (100 + vgstrate * 100) if (vTotalAMOUNT != 0 and vgstrate != 0) else 0

    if vBaseAMOUNT != 0:
        line_text = f"PAYNOW QR TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT E-PYT BANK FEE"
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL22',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': line_text,
            'GLNUMBER': '41090007',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '2001200000',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vBaseAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # GST
    vTotalAMOUNT = vTotalAMOUNT - vBaseAMOUNT
    if vTotalAMOUNT != 0:
        line_text = f"PAYNOW QR TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT E-PYT BANK FEE"

        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL23',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': line_text,
            'GLNUMBER': '22000000',
            'SAPTAXCODE': vOutputTax,
            'SAPTAXBASEAMOUNT': vBaseAMOUNT,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # === 5. Tăng vSeq nếu có insert ===
    if vFlagIsInsert == 1:
        vSeq += 1

    # Reset vflaginsert
    vFlagIsInsert = 0

    ############################## calculate Paynow charges for retailer ###############################
        # Filtered df_V2_LocationTempData
    filtered_df = df_V2_LocationTempData[
                    (df_V2_LocationTempData['FLAG'] == 'CAS') &
                    (df_V2_LocationTempData['LOCTYPE'].isin([2,4]))
                    ]
    # sum AMOUNT have conditions
    vTotalAMOUNT = (
        filtered_df['AMOUNT']
        .where((filtered_df['PRODUCTNAME'] == 'Paynow') & (filtered_df['TYPE'] == 'TF'), 0)
        .sum()
    )

    # Define vFlagIsInsert
    vFlagIsInsert = 0
    #
    if vTotalAMOUNT != 0:
        line_text = f"PAYNOW TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT PZ CLAIM BANK FEE"
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL24',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '01',
            'SAPCONTROLACCTCODE': vARCode_P_Retailer,
            'LINETEXT': line_text,
            'GLNUMBER': '12100000',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        # concat
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # Calculate vBaseAMOUNT
    vBaseAMOUNT = vTotalAMOUNT * 100 / (100 + vgstrate * 100) if (vTotalAMOUNT != 0 and vgstrate != 0) else 0

    if vBaseAMOUNT != 0:
        line_text = f"PAYNOW TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT PZ CLAIM BANK FEE"
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL25',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': line_text,
            'GLNUMBER': '41090007',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '2001200000',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vBaseAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # GST
    vTotalAMOUNT = vTotalAMOUNT - vBaseAMOUNT
    if vTotalAMOUNT != 0:
        line_text = f"PAYNOW TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT PZ CLAIM BANK FEE"

        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL26',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': line_text,
            'GLNUMBER': '22000000',
            'SAPTAXCODE': vOutputTax,
            'SAPTAXBASEAMOUNT': vBaseAMOUNT,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # === 5. Tăng vSeq nếu có insert ===
    if vFlagIsInsert == 1:
        vSeq += 1

    # Reset vflaginsert
    vFlagIsInsert = 0

        ################################### calculate Paynow QR charges for branch ######################
    # Filtered df_V2_LocationTempData
    filtered_df = df_V2_LocationTempData[
                    (df_V2_LocationTempData['FLAG'] == 'CAS') &
                    (df_V2_LocationTempData['LOCTYPE'].isin([1]))
                    ]
    # sum AMOUNT have conditions
    vTotalAMOUNT = (
        filtered_df['AMOUNT']
        .where((filtered_df['PRODUCTNAME'] == 'PaynowQR') & (filtered_df['TYPE'] == 'TF'), 0)
        .sum()
    )

    # Define vFlagIsInsert
    vFlagIsInsert = 0
    #
    if vTotalAMOUNT != 0:
        line_text = f"PAYNOW QR TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT E-PYT BANK FEE"
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL27',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '01',
            'SAPCONTROLACCTCODE': vARCode_P_Retailer,
            'LINETEXT': line_text,
            'GLNUMBER': '12100000',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        # concat
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # Calculate vBaseAMOUNT
    vBaseAMOUNT = vTotalAMOUNT * 100 / (100 + vgstrate * 100) if (vTotalAMOUNT != 0 and vgstrate != 0) else 0

    if vBaseAMOUNT != 0:
        line_text = f"PAYNOW QR TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT E-PYT BANK FEE"
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL28',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': line_text,
            'GLNUMBER': '41090007',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '2001200000',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vBaseAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # GST
    vTotalAMOUNT = vTotalAMOUNT - vBaseAMOUNT
    if vTotalAMOUNT != 0:
        line_text = f"PAYNOW QR TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT E-PYT BANK FEE"

        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL29',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': line_text,
            'GLNUMBER': '22000000',
            'SAPTAXCODE': vOutputTax,
            'SAPTAXBASEAMOUNT': vBaseAMOUNT,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # === 5. Tăng vSeq nếu có insert ===
    if vFlagIsInsert == 1:
        vSeq += 1

    # Reset vflaginsert
    vFlagIsInsert = 0
        ###################### calculate Paynow charges for branch ##########################
        # Filtered df_V2_LocationTempData
    filtered_df = df_V2_LocationTempData[
                    (df_V2_LocationTempData['FLAG'] == 'CAS') &
                    (df_V2_LocationTempData['LOCTYPE'].isin([1]))
                    ]
    # sum AMOUNT have conditions
    vTotalAMOUNT = (
        filtered_df['AMOUNT']
        .where((filtered_df['PRODUCTNAME'] == 'Paynow') & (filtered_df['TYPE'] == 'TF'), 0)
        .sum()
    )

    # Define vFlagIsInsert
    vFlagIsInsert = 0
    #
    if vTotalAMOUNT != 0:
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL30',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '01',
            'SAPCONTROLACCTCODE': vARCode_P_Retailer,
            'LINETEXT': f"PAYNOW TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT PZ CLAIM BANK FEE",
            'GLNUMBER': '12100000',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        # concat
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # Calculate vBaseAMOUNT
    vBaseAMOUNT = vTotalAMOUNT * 100 / (100 + vgstrate * 100) if (vTotalAMOUNT != 0 and vgstrate != 0) else 0

    if vBaseAMOUNT != 0:
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL31',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': f"PAYNOW TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT PZ CLAIM BANK FEE",
            'GLNUMBER': '41090007',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '2001200000',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vBaseAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # GST
    vTotalAMOUNT = vTotalAMOUNT - vBaseAMOUNT
    if vTotalAMOUNT != 0:

        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL32',
            'SAPDOCTYPE': 'Y5',
            'SAPPOSTINGKEY': '50',
            'SAPCONTROLACCTCODE': '',
            'LINETEXT': f"PAYNOW TXN {pd.to_datetime(procdate).strftime('%d/%m/%Y')} - UBT PZ CLAIM BANK FEE",
            'GLNUMBER': '22000000',
            'SAPTAXCODE': vOutputTax,
            'SAPTAXBASEAMOUNT': vBaseAMOUNT,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud, pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # === 5. Tăng vSeq nếu có insert ===
    if vFlagIsInsert == 1:
        vSeq += 1

    # Reset vflaginsert
    vFlagIsInsert = 0
    ############################ Paynow, Paynow QR for Daily Settlement  ##############################
    # Filtered df_V2_LocationTempData
    filtered_df = df_V2_LocationTempData[
                    (df_V2_LocationTempData['FLAG'] == 'CAS') &
                    (df_V2_LocationTempData['LOCTYPE'].isin([2,4]))
                    ]
    # sum AMOUNT have conditions
    vTotalAMOUNT = (
        filtered_df['AMOUNT']
        .where((filtered_df['PRODUCTNAME'] == 'PaynowQR') & (filtered_df['TYPE'] == 'CL'), 0)
        .sum()
    )

    # Define vFlagIsInsert
    vFlagIsInsert = 0
    #
    if vTotalAMOUNT != 0:
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL33',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '11',
            'SAPCONTROLACCTCODE': '21100091',
            'LINETEXT': f"GAMING INVOICE ON {pd.to_datetime(procdate).strftime('%d/%m/%Y')} RET-PAYNOW QR",
            'GLNUMBER': '12100001',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        # concat
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)

        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL34',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '01',
            'SAPCONTROLACCTCODE': '21100091',
            'LINETEXT': f"GAMING INVOICE ON {pd.to_datetime(procdate).strftime('%d/%m/%Y')} RET-PAYNOW QR",
            'GLNUMBER': '12100000',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)

        vFlagIsInsert = 1

    # === 5. Tăng vSeq nếu có insert ===
    if vFlagIsInsert == 1:
        vSeq += 1

    # Reset vflaginsert
    vFlagIsInsert = 0
    ################################# calculate PAYNOW for retailer ##############################
    # Filtered df_V2_LocationTempData
    filtered_df = df_V2_LocationTempData[
                    (df_V2_LocationTempData['FLAG'] == 'CAS') &
                    (df_V2_LocationTempData['LOCTYPE'].isin([2,4]))
                    ]
    # sum AMOUNT have conditions
    vTotalAMOUNT = (
        filtered_df['AMOUNT']
        .where((filtered_df['PRODUCTNAME'] == 'Paynow') & (filtered_df['TYPE'] == 'CL'), 0)
        .sum()
    )

    vTotalAMOUNT = vTotalAMOUNT * -1

    # Define vFlagIsInsert
    vFlagIsInsert = 0
    #
    if vTotalAMOUNT != 0:
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL35',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '01',
            'SAPCONTROLACCTCODE': '21100091',
            'LINETEXT': f"GAMING INVOICE ON {pd.to_datetime(procdate).strftime('%d/%m/%Y')} RET-PAYNOW",
            'GLNUMBER': '12100001',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        # concat
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)

        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL36',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '11',
            'SAPCONTROLACCTCODE': '21100091',
            'LINETEXT': f"GAMING INVOICE ON {pd.to_datetime(procdate).strftime('%d/%m/%Y')} RET-PAYNOW",
            'GLNUMBER': '12100000',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)

        vFlagIsInsert = 1

    # === 5. Tăng vSeq nếu có insert ===
    if vFlagIsInsert == 1:
        vSeq += 1

    # Reset vflaginsert
    vFlagIsInsert = 0
    ################################################# calculate PAYNOW QR for branches ##############################
    # Filtered df_V2_LocationTempData
    filtered_df = df_V2_LocationTempData[
                    (df_V2_LocationTempData['FLAG'] == 'CAS') &
                    (df_V2_LocationTempData['LOCTYPE'].isin([1]))
                    ]
    # sum AMOUNT have conditions
    vTotalAMOUNT = (
        filtered_df['AMOUNT']
        .where((filtered_df['PRODUCTNAME'] == 'PaynowQR') & (filtered_df['TYPE'] == 'CL'), 0)
        .sum()
    )


    # Define vFlagIsInsert
    vFlagIsInsert = 0
    #
    if vTotalAMOUNT != 0:
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL37',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '11',
            'SAPCONTROLACCTCODE': '23100000',
            'LINETEXT': f"GAMING INVOICE ON {pd.to_datetime(procdate).strftime('%d/%m/%Y')} BR-PAYNOW QR",
            'GLNUMBER': '12100003',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        # concat
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)

        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL38',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '01',
            'SAPCONTROLACCTCODE': vARCode_Q_Branch,
            'LINETEXT': f"GAMING INVOICE ON {pd.to_datetime(procdate).strftime('%d/%m/%Y')} BR-PAYNOW QR",
            'GLNUMBER': '12100000',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)

        vFlagIsInsert = 1

    # === 5. Tăng vSeq nếu có insert ===
    if vFlagIsInsert == 1:
        vSeq += 1

    # Reset vflaginsert
    vFlagIsInsert = 0
    ################################# calculate PAYNOW for branches ##############################
    # Filtered df_V2_LocationTempData
    filtered_df = df_V2_LocationTempData[
                    (df_V2_LocationTempData['FLAG'] == 'CAS') &
                    (df_V2_LocationTempData['LOCTYPE'].isin([1]))
                    ]
    # sum AMOUNT have conditions
    vTotalAMOUNT = (
        filtered_df['AMOUNT']
        .where((filtered_df['PRODUCTNAME'] == 'Paynow') & (filtered_df['TYPE'] == 'CL'), 0)
        .sum()
    )
    vTotalAMOUNT = vTotalAMOUNT * -1

    # Define vFlagIsInsert
    vFlagIsInsert = 0
    #
    if vTotalAMOUNT != 0:
        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL39',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '01',
            'SAPCONTROLACCTCODE': '23100000',
            'LINETEXT': f"GAMING INVOICE ON {pd.to_datetime(procdate).strftime('%d/%m/%Y')} BR-PAYNOW",
            'GLNUMBER': '12100003',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        # concat
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)

        new_row = {
            'IDMMBUSINESSDAY': None,
            'BUSINESSDATE': procdate,
            'ITEMID': None,
            'TRANSACTIONID': vSeq,
            'DOCUMENTDATE': v1_docdate,
            'LINECODE': 'MCOL40',
            'SAPDOCTYPE': 'Y8',
            'SAPPOSTINGKEY': '11',
            'SAPCONTROLACCTCODE': vARCode_P_Branch,
            'LINETEXT': f"GAMING INVOICE ON {pd.to_datetime(procdate).strftime('%d/%m/%Y')} BR-PAYNOW",
            'GLNUMBER': '12100000',
            'SAPTAXCODE': '',
            'SAPTAXBASEAMOUNT': None,
            'CCCODE': '',
            'SAPASSIGNMENT': '',
            'CURRENCYCODE': 'SGD',
            'AMOUNT': vTotalAMOUNT,
            'PRODUCT': '',
            'DRAWNUMBER': '',
            'CUSTOMER': ''
        }
        df_V2_TB_RTMS_RTShopCloud = pd.concat([df_V2_TB_RTMS_RTShopCloud,pd.DataFrame([new_row])], ignore_index=True)
        vFlagIsInsert = 1

    # === 5. Tăng vSeq nếu có insert ===
    if vFlagIsInsert == 1:
        vSeq += 1

    # Reset vflaginsert
    vFlagIsInsert = 0
    return df_V2_TB_RTMS_RTShopCloud