import pandas as pd
import numpy as np
import os, logging
logger = logging.getLogger(__name__)
from Snowflake_connection import snowflake_connection
connection = snowflake_connection()
schema = "SPPL_DEV_DWH.SPPL_PUBLIC"

v_totomatchBettypes = [
    ('M AN'),
    ('M 2'),
    ('M 3'),
    ('M 4')
]
def ubt_temp_terminal():
    df_ubt_temp_terminal = pd.DataFrame()

    query = f"""
    SELECT TERDISPLAYID, LOCID
    FROM {schema}.ZTUBT_TERMINAL
    """
    df_ubt_temp_terminal = pd.read_sql(query, connection)

    return df_ubt_temp_terminal

def ubt_temp_location():
    df_ubt_temp_location = pd.DataFrame()

    query = f"""
    SELECT *
    FROM {schema}.ZTUBT_LOCATION
    """
    df_ubt_temp_location = pd.read_sql(query, connection)

    return df_ubt_temp_location

def ubt_temp_product():
    df_ubt_temp_product = pd.DataFrame()

    query = f"""
    SELECT PRODID,
    CASE PRODID
        WHEN 5 THEN 'SPORTS'
        ELSE TRIM(PRODNAME)
    END AS PRODNAME
    FROM {schema}.ZTUBT_PRODUCT
    """
    df_ubt_temp_product = pd.read_sql(query, connection)
    df_temp = pd.DataFrame({
        'PRODID': [3],
        'PRODNAME': ['TOTO MATCH']
    })
    df_ubt_temp_product = pd.concat([df_ubt_temp_product, df_temp], ignore_index=True)
    return df_ubt_temp_product

def ubt_temp_chain():
    df_ubt_temp_chain = pd.DataFrame()

    query = f"""
    SELECT CHID, CHDISPLAYID, CHNAME
    FROM {schema}.ZTUBT_CHAIN
    """
    df_ubt_temp_chain = pd.read_sql(query, connection)

    return df_ubt_temp_chain

def ubt_temp_cancelledbeticket(vfromdateigt, vtodateigt):
    df_ubt_temp_cancelledbeticket = pd.DataFrame()

    query = f"""
    SELECT *
    FROM {schema}.ztubt_cancelledbetticket
    WHERE CANCELLEDDATE BETWEEN '{vfromdateigt}' AND '{vtodateigt}'
    """

    df_ubt_temp_cancelledbeticket = pd.read_sql(query, connection)
    # Extract data from ztubt_cancelledbetticketlifecyclestate table
    query = f"""
    SELECT TRANHEADERID
    FROM {schema}.ztubt_cancelledbetticketlifecyclestate
    where BETSTATETYPEID = 'CB06'
    """
    df_ubt_temp_cancelledbeticketlifecyclestate = pd.read_sql(query, connection)

    # Merge 2 dataframes
    df_ubt_temp_cancelledbeticket = df_ubt_temp_cancelledbeticket.merge(
        df_ubt_temp_cancelledbeticketlifecyclestate,
        left_on='TRANHEADERID',
        right_on='TRANHEADERID',
        how='inner'
    )[['TICKETSERIALNUMBER', 'CANCELLEDDATE', 'CANCELLEDAMOUT', 'PRODID', 'TERDISPLAYID']]
    logger.info(f"ubt_temp_cancelledbeticket: Retrieved {len(df_ubt_temp_cancelledbeticket)} records.")
    return df_ubt_temp_cancelledbeticket

def ubt_temp_datalocationinvoice (df_ubt_temp_transamountdetaildata, df_ubt_temp_terminal, df_ubt_temp_location,
                                                                df_ubt_temp_product, df_ubt_temp_chain, df_ubt_temp_salestotolocation,
                                                                df_ubt_temp_cancelledbeticket, df_ubt_temp_itotolocation,
                                                                df_ubt_temp_grouptotolocation, df_ubt_temp_salesgrouptotolocation, df_ubt_temp_resultcashlesslocation,
                                                                df_ubt_temp_getamounttrans,
                                                               vinvoiceperiodid, vfromdatetimeigtUTC, vtodatetimeigtUTC, vfromdatetimeOB_UTC, vtodatetimeOB_UTC,
                                                                vfromdatetimeBMCS_UTC, vtodatetimeBMCS_UTC,
                                                               vactual_date,vfromdateigt, vtodateigt):

    # =========================================================================
    query = f"""
    SELECT * from {schema}.ztubt_validatedbetticket
    WHERE CREATEDVALIDATIONDATE >= '{vfromdatetimeigtUTC}' AND CREATEDVALIDATIONDATE <= '{vtodatetimeigtUTC}'
    AND WINNINGAMOUNT IS NOT NULL AND WINNINGAMOUNT <> 0
    """
    df_ztubt_validatedbetticket = pd.read_sql(query, connection)

    query = f"""
    SELECT * from {schema}.ztubt_validatedbetticketlifecyclestate
    WHERE BETSTATETYPEID = 'VB06'
    """
    df_ztubt_validatedbetticketlifecyclestate = pd.read_sql(query, connection)

    query = f"""
    SELECT * FROM {schema}.ztubt_toto_placedbettransactionlineitem
    """
    df_ztubt_toto_placedbettransactionlineitem = pd.read_sql(query, connection)
    # Define schema
    df_ubt_temp_datalocationinvoice = pd.DataFrame({
        "LOCDISPLAYID" : pd.Series(dtype="string"),
        "AGENTINVOICE_INVID" : pd.Series(dtype="string"),
        "AGENTINVOICE_FINIDN" : pd.Series(dtype="string"),
        "AGENTINVOICE_PRODUCTID" : pd.Series(dtype="int"),
        "SALES_TYPE" : pd.Series(dtype="string"),
        "TOTALCOUNT" : pd.Series(dtype="int"),
        "AMOUNT" : pd.Series(dtype="float"),
        "SUB_PROD" : pd.Series(dtype="string")
    })
    # Add suffixes for dataframe
    df_ubt_temp_transamountdetaildata = df_ubt_temp_transamountdetaildata.add_suffix('_a')
    df_ubt_temp_terminal = df_ubt_temp_terminal.add_suffix('_ter')
    df_ubt_temp_location = df_ubt_temp_location.add_suffix('_lc')
    df_ubt_temp_product = df_ubt_temp_product.add_suffix('_pr')
    df_ubt_temp_chain = df_ubt_temp_chain.add_suffix('_c')
    df_ubt_temp_cancelledbeticket = df_ubt_temp_cancelledbeticket.add_suffix('_cb')
    df_ubt_temp_itotolocation = df_ubt_temp_itotolocation.add_suffix('_itoto')
    df_ubt_temp_salesgrouptotolocation = df_ubt_temp_salesgrouptotolocation.add_suffix('_sgtl')
    df_ubt_temp_grouptotolocation = df_ubt_temp_grouptotolocation.add_suffix('_gtl')
    df_ubt_temp_resultcashlesslocation = df_ubt_temp_resultcashlesslocation.add_suffix('_rcll')
    df_ubt_temp_getamounttrans = df_ubt_temp_getamounttrans.add_suffix('_gtat')

    # 1.1 UBT_TEMP_DATALOCATIONINVOICE
    # condition = [
    #     (df_merge['FLAG'] == "COL") &
    #     (df_merge['TRANSTYPE'] != 'OO') &
    #     (df_merge['PRODID_pr'].isin([1,2,3,4,5]))
    # ]
    df_merge = (
        df_ubt_temp_transamountdetaildata.merge(
            df_ubt_temp_terminal,
            left_on='TERDISPLAYID_a',
            right_on='TERDISPLAYID_ter',
            how='inner'
        )\
        .merge(
            df_ubt_temp_location,
            left_on='LOCID_ter',
            right_on='LOCID_lc',
            how='inner'
        )\
        .merge(
            df_ubt_temp_product,
            left_on='PRODUCTNAME_a',
            right_on='PRODNAME_pr',
            how='inner'
        )\
        .merge(
            df_ubt_temp_chain,
            left_on='CHID_lc',
            right_on='CHID_c',
            how='left'
        )
    )


    # Apply condition
    df_merge = df_merge.loc[
        (df_merge['FLAG_a'] == "COL") &
        (df_merge['TRANSTYPE_a'] != 'OO') &
        (df_merge['PRODID_pr'].isin([1,2,3,4,5]))
        ]


    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'FROMDATE_a', 'TODATE_a', 'PRODID_pr', 'CHDISPLAYID_c'],
        as_index=False
    ).agg(
        TOTALCOUNT=('CT_a', 'sum'),
        AMOUNT=('AMOUNT_a', 'sum')
    )\
    .assign(
        SALES_TYPE= '1',
        AGENTINVOICE_INVID= vinvoiceperiodid,
        SUB_PROD = ''
    )\
    .rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'PRODID_pr': 'AGENTINVOICE_PRODUCTID',
        }
    )\
    .reset_index(drop=True)\
    [['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.1 : {len(df_merge)} rows - Line (423 - 437)" )
    print(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.1 : {len(df_merge)} rows - Line (423 - 437)" )
    # Partition base on terdisplayid, agentinvoice_productid

    # 1.2 UBT_TEMP_DATALOCATIONINVOICE

    df_merge = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on='TERDISPLAYID_a',
        right_on='TERDISPLAYID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='LOCID_ter',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )

    # Apply condition
    df_merge = df_merge.loc[
        (df_merge['FLAG_a'] == "COL") &
        (df_merge['TRANSTYPE_a'] != 'OO')
        ]

    df_merge['AGENTINVOICE_PRODUCTID'] = np.where(
        df_merge['PRODUCTNAME_a'] == 'Gate Admission',
        99,
        31
    )

    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'AGENTINVOICE_PRODUCTID'],
        as_index=False
    ).agg(
        TOTALCOUNT=('CT_a', 'sum'),
        AMOUNT=('AMOUNT_a', 'sum')
    )

    df_merge = df_merge.assign(
        SALES_TYPE = '1',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        SUB_PROD = ''
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.2 : {len(df_merge)} rows - Line (441 - 455)" )

    # 1.3 UBT_TEMP_DATALOCATIONINVOICE
    df_merge = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on='TERDISPLAYID_a',
        right_on='TERDISPLAYID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='LOCID_ter',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    ).merge(
        df_ubt_temp_product,
        left_on='PRODUCTNAME_a',
        right_on='PRODNAME_pr',
        how='inner'
    )

    # Apply condition
    df_merge = df_merge.loc[
         (df_merge['FLAG_a'].isin(["CAN", "PAY","RBT"])) &
       (df_merge['PRODID_pr'].isin([1,2,3,4,5]))
    ]

    # Filter Sales Type
    df_merge['SALES_TYPE'] = np.where(
        df_merge['FLAG_a'] == 'CAN',
        '2',
        np.where(
            df_merge['FLAG_a'] == 'PAY',
            '3',
            '4' # 'RBT'
        )
    )

    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'PRODID_pr', 'FLAG_a', 'PRODUCTNAME_a', 'SALES_TYPE'],
        as_index=False
    ).agg(
        TOTALCOUNT=('CT_a', 'sum'),
        AMOUNT=('AMOUNT_a', 'sum')
    )

    df_merge['AMOUNT'] = df_merge['AMOUNT'].fillna(0) * - 1

    df_merge = df_merge.assign(
        AGENTINVOICE_INVID = vinvoiceperiodid,
        SUB_PROD = ''
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'PRODID_pr': 'AGENTINVOICE_PRODUCTID',
        }
    ).reset_index(drop=True)\
    [['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.3 : {len(df_merge)} rows - Line (464 - 480)" )

    # 1.4 : 61 Refund AMOUNT
    df_merge = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on='TERDISPLAYID_a',
        right_on='TERDISPLAYID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='LOCID_ter',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    ).merge(
        df_ubt_temp_product,
        left_on='PRODUCTNAME_a',
        right_on='PRODNAME_pr',
        how='inner'
    )
    # Apply condition
    df_merge = df_merge.loc[
         (df_merge['FLAG_a'] == "RFD") &
         (df_merge['TRANSTYPE_a'] != 'OO')
    ]
    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'PRODID_pr', 'PRODUCTNAME_a'],
        as_index=False
    ).agg(
        TOTALCOUNT=('CT_a', 'sum'),
        AMOUNT=('AMOUNT_a', 'sum')
    ).assign(
        SALES_TYPE = '61',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        SUB_PROD = ''
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'PRODID_pr': 'AGENTINVOICE_PRODUCTID',
        }

    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]

    df_merge['AMOUNT'] = df_merge['AMOUNT'].fillna(0) * - 1
    df_merge['TOTALCOUNT'] = df_merge['TOTALCOUNT'].fillna(0)

    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.4 : {len(df_merge)} rows - Line (489 - 503)" )

    # 1.5: OFFLINES ORDERS - REFUND
    df_merge = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on='TERDISPLAYID_a',
        right_on='TERDISPLAYID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='LOCID_ter',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )

    # Apply condition
    df_merge = df_merge.loc[
         (df_merge['FLAG_a'] == "RFD") &
         (df_merge['TRANSTYPE_a'] == 'OO')
    ]

    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c']
        , as_index=False
    ).agg(
        TOTALCOUNT=('CT_a', 'sum'),
        AMOUNT=('AMOUNT_a', 'sum')
    ).assign(
        SALES_TYPE = '62',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        AGENTINVOICE_PRODUCTID = 31,
        SUB_PROD = ''
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]

    df_merge['AMOUNT'] = df_merge['AMOUNT'].fillna(0) * - 1
    df_merge['TOTALCOUNT'] = df_merge['TOTALCOUNT'].fillna(0)

    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.5 : {len(df_merge)} rows - Line (505 - 519)" )

    # 1.6: AT_TAXAMT
    df_merge = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on='TERDISPLAYID_a',
        right_on='TERDISPLAYID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='LOCID_ter',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    ).merge(
        df_ubt_temp_product,
        left_on='PRODUCTNAME_a',
        right_on='PRODNAME_pr',
        how='inner'
    )
    # Apply condition
    df_merge = df_merge.loc[
         (df_merge['FLAG_a'].isin(["GST", 'SAL'])) &
         (df_merge['PRODID_pr'].isin([1,2,3,4,5])) ]

    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'PRODID_pr', 'FLAG_a', 'PRODUCTNAME_a'],
        as_index=False
    ).agg(
        TOTALCOUNT=('CT_a', 'sum'),
        AMOUNT=('AMOUNT_a', 'sum')
    ).assign(
        SALES_TYPE = lambda x: np.where(
            x['FLAG_a'] == 'GST',
            '6',
            '7' # 'SAL'
        ),
        AGENTINVOICE_INVID = vinvoiceperiodid,
        SUB_PROD = ''
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'PRODID_pr': 'AGENTINVOICE_PRODUCTID',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.6 : {len(df_merge)} rows - Line (526 - 543)" )

    # 1.7: AT_FRACVALAMT

    df_vb = df_ztubt_validatedbetticket.merge(
        df_ztubt_validatedbetticketlifecyclestate,
        left_on='TRANHEADERID',
        right_on='TRANHEADERID',
        how='inner'
    )\
    [['TRANHEADERID', 'WINNINGAMOUNT', 'TERDISPLAYID', 'CREATEDVALIDATIONDATE']]
    # Apply condition
    df_vb = df_vb.loc[
        (df_vb['WINNINGAMOUNT'] != 0) &
        (df_vb['CREATEDVALIDATIONDATE'] >= vfromdatetimeigtUTC) &
        (df_vb['CREATEDVALIDATIONDATE'] <= vtodatetimeigtUTC) &
        (df_vb['WINNINGAMOUNT'].notnull())
    ]
    # add suffix
    df_vb = df_vb.add_suffix('_vb')
    df_merge = df_vb.merge(
        df_ztubt_toto_placedbettransactionlineitem,
        left_on='TRANHEADERID_vb',
        right_on='TRANHEADERID',
        how='inner'
    ).merge(
        df_ubt_temp_terminal,
        left_on='TERDISPLAYID_vb',
        right_on='TERDISPLAYID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='LOCID_ter',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )
    # Apply condition
    df_merge = df_merge.loc[
        df_merge['GROUPUNITSEQUENCE'].notnull()
    ]

    # Create SUB_PROD column before groupby
    df_merge['SUB_PROD'] = np.where(
        ~df_merge['BETTYPEID'].isin(v_totomatchBettypes),
        'TOTO',
        'TOTO MATCH'
    )

    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'SUB_PROD'],
        as_index=False
    ).agg(
        TOTALCOUNT=('TRANHEADERID_vb', 'nunique'),
        AMOUNT=('WINNINGAMOUNT_vb', 'sum')
    ).assign(
        SALES_TYPE = '16',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        AGENTINVOICE_PRODUCTID = 3
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.7 : {len(df_merge)} rows - Line (548 - 583)" )

    # 1.8:
    # Add suffixes for dataframe
    df_ubt_temp_salestotolocation = df_ubt_temp_salestotolocation.add_suffix('_stl')
    df_merge = df_ubt_temp_salestotolocation.merge(
        df_ubt_temp_location,
        left_on='LOCID_stl',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )

    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'PRODID_stl'],
        as_index=False
    ).agg(
        TOTALCOUNT=('WAGERTOTAL_stl', 'sum'),
        AMOUNT=('WAGERAMOUNT_stl', 'sum')
    ).assign(
        SALES_TYPE = '52',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        SUB_PROD = 'TOTO'
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'PRODID_stl': 'AGENTINVOICE_PRODUCTID',
        }
    ).reset_index(drop=True)\
    [['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.8 : {len(df_merge)} rows - Line (587 - 598)" )

    # 1.9:
    df_merge = df_ubt_temp_salestotolocation.merge(
        df_ubt_temp_location,
        left_on='LOCID_stl',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )
    df_merge['TOTALCOUNT'] = df_merge['WAGERTOTAL_stl'].fillna(0) - df_merge['CANCELTOTAL_stl'].fillna(0)
    df_merge['SALESAMOUNT_stl'] = df_merge['WAGERAMOUNT_stl'].fillna(0)
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c'],
        as_index=False
    ).agg(
        TOTALCOUNT=('TOTALCOUNT', 'sum'),
        TOTALAMOUNT=('SALESAMOUNT_stl', 'sum')
    ).assign(
        SALES_TYPE = '53',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        AGENTINVOICE_PRODUCTID = 31,
        SUB_PROD = 'TOTO'
    )\
    .rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'TOTALAMOUNT': 'AMOUNT',
        }
    ).reset_index(drop=True)\
    [['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.9 : {len(df_merge)} rows - Line (605 - 616)" )

    # 1.10



# select lc.locdisplayid	as locdisplayid,
# vinvoiceperiodid	as agentinvoice_invid,
# lc.chdisplayid	as agentinvoice_finidn,
# prd.prodid as agentinvoice_productid,
# '40'as sales_type,
# 0 as totalcount,
# gst.gstrate,
# prd.prodname as sub_prod
# from
# (
# 	select lc.locdisplayid, ch.chdisplayid
# 	from ubt_temp_location lc
# 	left join ubt_temp_chain ch on lc.chid = ch.chid
# 	where lc.isgst = True
# )lc
# cross join
# (
# 	select prodid, prodname from ubt_temp_product where prodid in (2,3,4) --lottery / es
# )prd
# cross join
# (
# 	select  gstrate from public.ztubt_gstconfig	a
# 	where
# 	(vactualdate between effectivefrom and enddate) or (vactualdate >=effectivefrom and enddate is null)
# 	limit 1
# )gst



    query = f"""
    SELECT GSTRATE from {schema}.ztubt_gstconfig
    where ('{vactual_date}' between EFFECTIVEFROM and ENDDATE)
    or (ENDDATE >= '{vactual_date}' or ENDDATE is null)
    limit 1
    """
    df_ztubt_gstconfig = pd.read_sql(query, connection)

    df_merge = pd.DataFrame()


    df_merge = df_ubt_temp_location.merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    ).merge(
        df_ubt_temp_product[['PRODID_pr', 'PRODNAME_pr']],
        how='cross'
    ).merge(
        df_ztubt_gstconfig,
        how='cross'
    )
    # Filter
    df_merge = df_merge.loc[
        (df_merge['ISGST_lc'] == True) &
        (df_merge['PRODID_pr'].isin([2,3,4]))
    ]
    df_merge = df_merge.assign(
        SALES_TYPE = '40',
        TOTALCOUNT = 0,
        AGENTINVOICE_INVID = vinvoiceperiodid
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'PRODID_pr': 'AGENTINVOICE_PRODUCTID',
            'GSTRATE': 'AMOUNT',
            'PRODNAME_pr': 'SUB_PROD'
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT','AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.10 : {len(df_merge)} rows - Line (620 - 644)" )



    # 1.11: CANCELLED BET TICKET
    df_merge = df_ubt_temp_cancelledbeticket.merge(
        df_ubt_temp_itotolocation,
        left_on='TICKETSERIALNUMBER_cb',
        right_on='TICKETSERIALNUMBER_itoto',
        how='inner'
    ).merge(
        df_ubt_temp_terminal,
        left_on='TERDISPLAYID_cb',
        right_on='TERDISPLAYID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='LOCID_ter',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )
    # Filter
    df_merge = df_merge.loc[
        (df_merge['PRODID_cb'].isin([3])) &
        (df_merge['CANCELLEDDATE_cb'] >= vfromdateigt) &
        (df_merge['CANCELLEDDATE_cb'] <= vtodateigt)
    ]
    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c'],
        as_index=False
    ).agg(
        TOTALCOUNT=('TICKETSERIALNUMBER_cb', 'nunique'),
        AMOUNT=('CANCELLEDAMOUT_cb', 'sum')
    ).assign(
        SALES_TYPE = '59',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        AGENTINVOICE_PRODUCTID = 3,
        SUB_PROD = 'TOTO'
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.11 : {len(df_merge)} rows - Line (649 - 665)" )

    # 1.12:

#     select l.locdisplayid	as locdisplayid ,
# vinvoiceperiodid	as agentinvoice_invid,
# c.chdisplayid	as agentinvoice_finidn,
# s.prodid	as agentinvoice_productid,
# '60'	as sales_type,
# sum(coalesce(s.wagertotal,0) - coalesce(s.canceltotal,0))as totalcount, --(8)
# sum(coalesce(s.salesamount,0)) as totalamount,
# case when s.bettype = 1 then 'TOTO'
# 	else 'TOTO MATCH' end as sub_prod
# from ubt_temp_salesgrouptotolocation s
# inner join ubt_temp_location l on s.locid = l.locid
# left join ubt_temp_chain c on l.chid = c.chid
# group by l.locdisplayid, c.chdisplayid,s.prodid,
# 	case when s.bettype = 1 then 'TOTO'
# 		else 'TOTO MATCH' end

    df_merge = df_ubt_temp_salesgrouptotolocation.merge(
        df_ubt_temp_location,
        left_on='LOCID_sgtl',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )
    df_merge['TOTALCOUNT'] = df_merge['WAGERTOTAL_sgtl'].fillna(0) - df_merge['CANCELTOTAL_sgtl'].fillna(0)
    df_merge['SALESAMOUNT_sgtl'] = df_merge['SALESAMOUNT_sgtl'].fillna(0)
    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'PRODID_sgtl', 'BETTYPE_sgtl'],
        as_index=False
    ).agg(
        TOTALCOUNT=('TOTALCOUNT', 'sum'),
        AMOUNT=('SALESAMOUNT_sgtl', 'sum')
    ).assign(
        SALES_TYPE = '60',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        SUB_PROD = np.where(
            df_merge['BETTYPE_sgtl'] == 1,
            'TOTO',
            'TOTO MATCH'
        )
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'PRODID_sgtl': 'AGENTINVOICE_PRODUCTID',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.12 : {len(df_merge)} rows - Line (670 - 684)" )

#     # 1.13:
# ---------69. player syndicate sales amount

# --TOTO, totomatch
# select	l.locdisplayid as locdisplayid,
# vinvoiceperiodid	as agentinvoice_invid,
# c.chdisplayid	as agentinvoice_finidn,
# 3 as agentinvoice_productid,
# '69'as sales_type,
# sum(coalesce (s.wagertotal,0))	as totalcount,
# sum(coalesce (s.wageramount,0)) as totalamount,
# case when s.bettype = 1 then 'TOTO'
# 	else 'TOTO MATCH' end as sub_prod
# from ubt_temp_salesgrouptotolocation s
# inner join ubt_temp_location l on s.locid = l.locid
# left join ubt_temp_chain c on l.chid = c.chid
# group by l.locdisplayid, c.chdisplayid,
# 	case when s.bettype = 1 then 'TOTO'
# 		else 'TOTO MATCH' end
# ---------
    df_merge = df_ubt_temp_salesgrouptotolocation.merge(
        df_ubt_temp_location,
        left_on='LOCID_sgtl',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )
    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'BETTYPE_sgtl'],
        as_index=False
    ).agg(
        TOTALCOUNT=('WAGERTOTAL_sgtl', 'sum'),
        AMOUNT=('WAGERAMOUNT_sgtl', 'sum')
    ).assign(
        SALES_TYPE = '69',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        AGENTINVOICE_PRODUCTID = 3,
        SUB_PROD = np.where(
            df_merge['BETTYPE_sgtl'] == 1,
            'TOTO',
            'TOTO MATCH'
        )
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.13 : {len(df_merge)} rows - Line (691 - 705)" )


    # 1.14


# -----------70 [	 GROUP TOTO Cancel Amount
# --TOTO, totomatch
# select      lc.locdisplayid	as locdisplayid,
# vinvoiceperiodid	as agentinvoice_invid,
# c.chdisplayid	as agentinvoice_finidn,
# 3 as agentinvoice_productid,
# '70'as sales_type,
# count(distinct gt.grouphostid)	as totalcount,
# sum(coalesce(cbt.cancelledamout,0))	as totalamount,
# case when gt.bettype = 1 then 'TOTO'
# 	else 'TOTO MATCH' end as sub_prod
# from ubt_temp_cancelledbeticket cbt
# inner join ubt_temp_grouptotolocation gt on cbt.ticketserialnumber = gt.ticketserialnumber
# inner join ubt_temp_terminal ter on cbt.terdisplayid = ter.terdisplayid
# inner join ubt_temp_location lc on ter.locid = lc.locid
# left join ubt_temp_chain c on lc.chid = c.chid
# where cbt.prodid = 3
# and cbt.cancelleddate >= vfromdateIGT and cbt.cancelleddate < vtodateIGT
# group by lc.locdisplayid,c.chdisplayid,
# 	case when gt.bettype = 1 then 'TOTO'
# 		else 'TOTO MATCH' end

    df_merge = df_ubt_temp_cancelledbeticket.merge(
        df_ubt_temp_grouptotolocation,
        left_on='TICKETSERIALNUMBER_cb',
        right_on='TICKETSERIALNUMBER_gtl',
        how='inner'
    ).merge(
        df_ubt_temp_terminal,
        left_on='TERDISPLAYID_cb',
        right_on='TERDISPLAYID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='LOCID_ter',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )
    # Filter
    df_merge = df_merge.loc[
        (df_merge['PRODID_cb'].isin([3])) &
        (df_merge['CANCELLEDDATE_cb'] >= vfromdateigt) &
        (df_merge['CANCELLEDDATE_cb'] <= vtodateigt)
    ]
    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'BETTYPE_gtl'],
        as_index=False
    ).agg(
        TOTALCOUNT=('GROUPHOSTID_gtl', 'nunique'),
        AMOUNT=('CANCELLEDAMOUT_cb', 'sum')
    ).assign(
        SALES_TYPE = '70',
        AGENTINVOICE_INVID = vinvoiceperiodid,
        AGENTINVOICE_PRODUCTID = 3,
        SUB_PROD = np.where(
            df_merge['BETTYPE_gtl'] == 1,
            'TOTO',
            'TOTO MATCH'
        )
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.14 : {len(df_merge)} rows - Line (712 - 728)" )

    # 1.15:


    # -----------------105 Sales Commision Rate
    # select lc.locdisplayid	as locdisplayid,
    # vinvoiceperiodid	as agentinvoice_invid,
    # lc.chdisplayid	as agentinvoice_finidn,
    # sal.prodid as agentinvoice_productid,
    # '105' as sales_type,
    # 0 as totalcount,--(1)count(1) as totalcount,
    # sal.salescommission,
    # '' as sub_prod
    # from
    # (
    # 	select lc.locdisplayid, ch.chdisplayid from ubt_temp_location lc
    # 	left join ubt_temp_chain ch on lc.chid = ch.chid
    # 	where lc.isgst =true
    # )lc
    # cross join
    # (
    # 	select distinct salescommission, prodid from public.ztubt_salescomconfig  a
    # 	where commissiontype=1 and  isdeleted = false and prodid in (2,4) --lottery / es
    # )sal

    query = f"""
    SELECT DISTINCT salescommission, prodid FROM {schema}.ztubt_salescomconfig
    WHERE commissiontype=1 AND isdeleted = false AND prodid IN (2,4)
    """
    df_salcomconfig = pd.read_sql(query, connection)
    df_merge = df_ubt_temp_location.merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    ).merge(
        df_salcomconfig,
        how='cross'
    )

    #filter
    df_merge = df_merge.loc[
        df_merge['ISGST_lc'] == True
    ]

    df_merge = df_merge.assign(
        SALES_TYPE = '105',
        TOTALCOUNT = 0,
        AGENTINVOICE_INVID = vinvoiceperiodid,
        SUB_PROD = ''
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'PRODID': 'AGENTINVOICE_PRODUCTID',
            'SALESCOMMISSION': 'AMOUNT'
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.15 : {len(df_merge)} rows - Line (734 - 755)" )

    # 1.16:
# -----------------105 Sales Commision Rate TOTO / TOTO MATCH

# select lc.locdisplayid	as locdisplayid,
# vinvoiceperiodid	as agentinvoice_invid,
# lc.chdisplayid	as agentinvoice_finidn,
# sal.prodid as agentinvoice_productid,
# '105' as sales_type,
# 0 as totalcount,--(1)count(1) as totalcount,
# sal.salescommission,
# sp.sub_prod
# from
# (
# 	select lc.locdisplayid, ch.chdisplayid from ubt_temp_location lc
# 	left join ubt_temp_chain ch on lc.chid = ch.chid
# 	where lc.isgst =true
# )lc
# cross join
# (
# 	select distinct salescommission, prodid from public.ztubt_salescomconfig  a
# 	where commissiontype=1 and  isdeleted = false and prodid in (3)
# )sal
# cross join (select 'TOTO' as sub_prod union all select 'TOTO MATCH' as sub_prod) as sp


    query = f"""
    SELECT DISTINCT salescommission, prodid FROM {schema}.ztubt_salescomconfig
    WHERE commissiontype=1 AND isdeleted = false AND prodid IN (3)
    """
    df_salcomconfig = pd.read_sql(query, connection)
    df_merge = pd.DataFrame()
    df_merge = df_ubt_temp_location.merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    ).merge(
        df_salcomconfig,
        how='cross'
    ).merge(
        pd.DataFrame({'SUB_PROD': ['TOTO', 'TOTO MATCH']}),
        how='cross'
    )

    #filter
    df_merge = df_merge.loc[
        (df_merge['ISGST_lc'] == True)
    ]

    df_merge = df_merge.assign(
        SALES_TYPE = '105',
        TOTALCOUNT = 0,
        AGENTINVOICE_INVID = vinvoiceperiodid
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'PRODID': 'AGENTINVOICE_PRODUCTID',
            'SALESCOMMISSION': 'AMOUNT'
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.16 : {len(df_merge)} rows - Line (761 - 783)" )

    # 1.17

    # select	lc.locdisplayid		as locdisplayid,
    # vinvoiceperiodid	as agentinvoice_invid,
    # c.chdisplayid		as agentinvoice_finidn ,
    # 0	as agentinvoice_productid,
    # case a.productname when 'NETS' then '108'
    #  when 'Flashpay' then '110' when 'CASHCARD' then '112' end as sales_type,
    # sum(coalesce(a.ct,0))	as totalcount,
    # sum(coalesce(a.amount,0)) as amount,
    # '' as sub_prod
    # 		from	ubt_temp_location as lc
    # left join ubt_temp_chain c on lc.chid = c.chid
    # left join ubt_temp_terminal  as ter on  ter.locid=lc.locid
    # left join ubt_temp_resultcashlesslocation a on a.terdisplayid = ter.terdisplayid
    # 		where a.productname in ('NETS','Flashpay','CASHCARD')
    # 		group by lc.locdisplayid,c.chdisplayid,a.productname
    df_merge = df_ubt_temp_location.merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    ).merge(
        df_ubt_temp_terminal,
        left_on='LOCID_lc',
        right_on='LOCID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_resultcashlesslocation,
        left_on='TERDISPLAYID_ter',
        right_on = 'TERDISPLAYID_rcll'
    )
    # Filter
    df_merge = df_merge.loc[
        (df_merge['PRODUCTNAME_rcll'].isin(['NETS','Flashpay','CASHCARD']))
    ]
    # Group by and aggregate
    df_merge = df_merge.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'PRODUCTNAME_rcll'],
        as_index=False
    ).agg(
        TOTALCOUNT=('CT_rcll', 'sum'),
        AMOUNT=('AMOUNT_rcll', 'sum')
    ).assign(
        AGENTINVOICE_INVID = vinvoiceperiodid,
        AGENTINVOICE_PRODUCTID = 0,
        SALES_TYPE = np.where(
            df_merge['PRODUCTNAME_rcll'] == 'NETS',
            '108',
            np.where(
                df_merge['PRODUCTNAME_rcll'] == 'Flashpay',
                '110',
                '112' # CASHCARD
            )
        ),
        SUB_PROD = ''
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.17 : {len(df_merge)} rows - Line (789 - 811)" )

    # 1.18

# select loc.locdisplayid, vinvoiceperiodid as agentinvoice_invid, ch.chdisplayid, fin.agentinvoice_productid,
# '116'	as sales_type, sum(fin.totalcount), (sum(fin.totalamount) * 100),
# fin.sub_prod as sub_prod
# 		from
# 		(
# 			select	a.actualdate,
# 	a.terdisplayid	as terdisplayid,
# 	a.prodid	as agentinvoice_productid,
# 	sum(coalesce(a.total,0) - coalesce(can.total,0))	as totalcount, --(8)
# 	round(trunc(sum(coalesce(a.amount,0)) / 100,2), 2)	as totalamount ,
# 	a.sub_prod
# 			from
# 			(
# select cast(pb.createddate as date) actualdate, a.ticketserialnumber,
# sum(a.sales + a.secondsales) as amount, count(distinct a.ticketserialnumber) as total, pb.terdisplayid,
# 	case pb.prodid
# 		when 6 then 5 else pb.prodid
# 		end as prodid, --(15)
# 		case when pb.prodid <> 3 then ''
# 			when pb.prodid = 3 and EXISTS (
# 				SELECT 1
# 				FROM ztubt_toto_placedbettransactionlineitem pbtlin
# 				WHERE pb.TranHeaderID = pbtlin.TranHeaderID
# 					AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
# 			) then 'TOTO MATCH'
# 		else 'TOTO' end as sub_prod
# from ubt_temp_getamounttrans a
# inner join public.ztubt_placedbettransactionheader  pb on a.ticketserialnumber=pb.ticketserialnumber

# where
# pb.iscancelled = false and (
# 		(
# 		pb.prodid in (2,3,4) --lottery / es
# 		and (pb.createddate between vfromdatetimeIGT_UTC and vtodatetimeIGT_UTC )

# 		)
# 		or --(15)
# 		(
# 		pb.prodid in (5,6) --sports / ob
# 		and (pb.createddate between vfromdatetimeOB_UTC and vtodatetimeOB_UTC )
# 		)
# 		or
# 		(
# 		pb.prodid in (1) --horse racing / bmcs
# 		and pb.createddate between vfromdatetimeBMCS_UTC and vtodatetimeBMCS_UTC
# 		)
# 	)

# group by pb.createddate , a.ticketserialnumber, pb.terdisplayid, pb.prodid,
# 	case when pb.prodid <> 3 then ''
# 		when pb.prodid = 3 and EXISTS (
# 			SELECT 1
# 			FROM ztubt_toto_placedbettransactionlineitem pbtlin
# 			WHERE pb.TranHeaderID = pbtlin.TranHeaderID
# 				AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
# 		) then 'TOTO MATCH'
# 		else 'TOTO' end
# 			)a

# 			left join --(8)
# 			(
# select a.ticketserialnumber,0 as amount, count(distinct a.ticketserialnumber) as total, pb.terdisplayid,
# 	pb.prodid,
# 	case when pb.prodid <> 3 then ''
# 		when pb.prodid = 3 and EXISTS (
# 				SELECT 1
# 				FROM ztubt_toto_placedbettransactionlineitem pbtlin
# 				WHERE pb.TranHeaderID = pbtlin.TranHeaderID
# 					AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
# 			) then 'TOTO MATCH'
# 		else 'TOTO' end as sub_prod
# from ubt_temp_getamounttrans a
# inner join ubt_temp_cancelledbeticket cb on a.ticketserialnumber = cb.ticketserialnumber
# inner join (select ticketserialnumber,terdisplayid,prodid, TranHeaderID from public.ztubt_placedbettransactionheader)  pb on a.ticketserialnumber = pb.ticketserialnumber
# where pb.prodid in (2,3,4) --lottery / es
# group by a.ticketserialnumber, pb.terdisplayid, pb.prodid,
# 	case when pb.prodid <> 3 then ''
# 		when pb.prodid = 3 and EXISTS (
# 				SELECT 1
# 				FROM ztubt_toto_placedbettransactionlineitem pbtlin
# 				WHERE pb.TranHeaderID = pbtlin.TranHeaderID
# 					AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
# 			) then 'TOTO MATCH'
# 		else 'TOTO' end
# 			)can on a.ticketserialnumber = can.ticketserialnumber and a.terdisplayid = can.terdisplayid and a.prodid = can.prodid and a.sub_prod = can.sub_prod
# 		group by a.actualdate, a.terdisplayid, a.prodid , a.sub_prod
# 		)fin

# 		inner join ubt_temp_terminal ter on fin.terdisplayid = ter.terdisplayid
# 		inner join ubt_temp_location loc on ter.locid = loc.locid
# 		left join ubt_temp_chain ch on loc.chid = ch.chid
# 		group by loc.locdisplayid, ch.chdisplayid, fin.agentinvoice_productid, fin.sub_prod;

    query = f"""
    select * from {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER
    """
    df_ubt_temp_placedbettransactionheader = pd.read_sql(query, connection)
    df_ubt_temp_placedbettransactionheader = df_ubt_temp_placedbettransactionheader.add_suffix('_pbtth')
    query = f"""
    SELECT * FROM {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM
    """
    df_ubt_temp_toto_placedbettransactionlineitem = pd.read_sql(query, connection)
    df_ubt_temp_toto_placedbettransactionlineitem = df_ubt_temp_toto_placedbettransactionlineitem.add_suffix('_pbtli')


    # First, perform the merge and filtering
    df_merged = df_ubt_temp_getamounttrans.merge(
        df_ubt_temp_placedbettransactionheader,
        left_on='TICKETSERIALNUMBER_gtat',
        right_on='TICKETSERIALNUMBER_pbtth',
        how='inner'
    ).loc[
        lambda x: (
            (x['ISCANCELLED_pbtth'] == False)
            &((
                (x['PRODID_pbtth'].isin([2,3,4]) &
                 (x['CREATEDDATE_pbtth'] >= vfromdatetimeigtUTC) & (x['CREATEDDATE_pbtth'] <= vtodatetimeigtUTC)
                ) |
                (x['PRODID_pbtth'].isin([5,6]) &
                 (x['CREATEDDATE_pbtth'] >= vfromdatetimeOB_UTC) & (x['CREATEDDATE_pbtth'] <= vtodatetimeOB_UTC)
                ) |
                (x['PRODID_pbtth'].isin([1]) &
                 (x['CREATEDDATE_pbtth'] >= vfromdatetimeBMCS_UTC) & (x['CREATEDDATE_pbtth'] <= vtodatetimeBMCS_UTC)
                )
            ))
        )
    ]

    # Check if the DataFrame is empty before applying assign operations
    if df_merged.empty:
        # Create an empty DataFrame with the expected columns for groupby
        df_a = pd.DataFrame(columns=['ACTUALDATE','TICKETSERIALNUMBER', 'AMOUNT', 'TOTAL','TERDISPLAYID', 'PRODID', 'SUB_PROD'])
    else:
        df_a = df_merged.assign(
            PRODID_adj = lambda x: np.where(
                x['PRODID_pbtth'] == 6,
                5,
                x['PRODID_pbtth']
            ),
            SUB_PROD = lambda x: np.where(
                (x['PRODID_pbtth'] != 3),
                '',
                np.where(
                    (x['PRODID_pbtth'] == 3) & x.apply(
                        lambda row: (
                            row['TICKETSERIALNUMBER_gtat'] in df_ubt_temp_toto_placedbettransactionlineitem[
                                df_ubt_temp_toto_placedbettransactionlineitem['BETTYPEID_pbtli'].isin(v_totomatchBettypes)
                            ]['TRANHEADERID_pbtli'].values
                        ),
                        axis=1
                    ),
                    'TOTO MATCH',
                    'TOTO'
                )
            ),
            AMOUNT_gtat = lambda x: x['SALES_gtat'] + x['SECONDSALES_gtat']

        ).groupby(
            ['CREATEDDATE_pbtth','TICKETSERIALNUMBER_gtat', 'TERDISPLAYID_pbtth', 'PRODID_adj', 'SUB_PROD'],
            as_index=False
        ).agg(
            AMOUNT = ('AMOUNT_gtat', 'sum'),
            TOTAL = ('TICKETSERIALNUMBER_gtat', 'nunique')
        ).rename(columns={
            "CREATEDDATE_pbtth": "ACTUALDATE",
            "TICKETSERIALNUMBER_gtat": "TICKETSERIALNUMBER",
            "AMOUNT": "AMOUNT",
            "TERDISPLAYID_pbtth": "TERDISPLAYID",
            "PRODID_adj": "PRODID",
            "TOTAL": "TOTAL"
        })[['ACTUALDATE', 'TICKETSERIALNUMBER', 'AMOUNT', 'TOTAL', 'TERDISPLAYID', 'PRODID', 'SUB_PROD']]

    # First, perform the merge and filtering for df_can
    df_can_merge = df_ubt_temp_getamounttrans.merge(
        df_ubt_temp_cancelledbeticket,
        left_on='TICKETSERIALNUMBER_gtat',
        right_on='TICKETSERIALNUMBER_cb',
        how='inner'
    ).merge(
        df_ubt_temp_placedbettransactionheader,
        left_on='TICKETSERIALNUMBER_gtat',
        right_on='TICKETSERIALNUMBER_pbtth',
        how='inner'
    ).loc[
        lambda x: (
            (x['PRODID_pbtth'].isin([2,3,4]))
        )
    ]

    # Check if the DataFrame is empty before applying assign operations
    if df_can_merge.empty:
        # Create an empty DataFrame with the expected columns for groupby
        df_can = pd.DataFrame(columns=['TICKETSERIALNUMBER','AMOUNT','TOTAL', 'TERDISPLAYID', 'PRODID','SUB_PROD'])
    else:
        df_can = df_can_merge.assign(
            SUB_PROD = lambda x: np.where(
                (x['PRODID_pbtth'] != 3),
                '',
                np.where(
                    (x['PRODID_pbtth'] == 3) & x.apply(
                        lambda row: (
                            row['TICKETSERIALNUMBER_gtat'] in df_ubt_temp_toto_placedbettransactionlineitem[
                                df_ubt_temp_toto_placedbettransactionlineitem['BETTYPEID_pbtli'].isin(v_totomatchBettypes)
                            ]['TRANHEADERID_pbtli'].values
                        ),
                        axis=1
                    ),
                    'TOTO MATCH',
                    'TOTO'
                    )
                ),
                AMOUNT = 0
            ).groupby(
                ['TERDISPLAYID_pbtth', 'PRODID_pbtth', 'SUB_PROD','TICKETSERIALNUMBER_gtat'],
                as_index=False
            ).agg(
                TOTALCOUNT = ('TICKETSERIALNUMBER_gtat', 'nunique'),
            ).rename(columns={
                'TICKETSERIALNUMBER_gtat': 'TICKETSERIALNUMBER',
                'TERDISPLAYID_pbtth': 'TERDISPLAYID',
                'PRODID_pbtth': 'PRODID'
            })[['TICKETSERIALNUMBER','AMOUNT','TOTAL', 'TERDISPLAYID', 'PRODID','SUB_PROD']]
    df_a = df_a.add_prefix('a.')
    df_can = df_can.add_prefix('can.')

    df_merged = df_a.merge(
            df_can,
            left_on=['a.TICKETSERIALNUMBER', 'a.TERDISPLAYID', 'a.PRODID','a.SUB_PROD'],
            right_on=['can.TICKETSERIALNUMBER', 'can.TERDISPLAYID', 'can.PRODID','can.SUB_PROD'],
            how='left'
            )
    df_fin = df_merged.assign(
                TOTALCOUNT = lambda x: x['a.TOTAL'].fillna(0) - x['can.TOTAL'].fillna(0),
                TOTALAMOUNT = lambda x: (pd.to_numeric(x['a.AMOUNT'], errors='coerce').fillna(0) / 100).round(2)
            ).groupby(
                ['a.ACTUALDATE', 'a.TERDISPLAYID', 'a.PRODID','a.SUB_PROD'],
                as_index=False
            ).agg(
                TOTALCOUNT = ('TOTALCOUNT', 'sum'),
                TOTALAMOUNT = ('TOTALAMOUNT', 'sum')
            ).rename(columns={
                "a.ACTUALDATE": "ACTUALDATE",
                "a.TERDISPLAYID": "TERDISPLAYID",
                "a.PRODID": "AGENTINVOICE_PRODUCTID",
                "a.TICKETSERIALNUMBER": "TICKETSERIALNUMBER",
                "a.SUB_PROD": "SUB_PROD"
            })\
            [['ACTUALDATE', 'TERDISPLAYID', 'AGENTINVOICE_PRODUCTID', 'TOTALCOUNT', 'TOTALAMOUNT','SUB_PROD']]
    df_fin = df_fin.add_prefix('fin.')
    df_final = df_fin.merge(
        df_ubt_temp_terminal,
        left_on='fin.TERDISPLAYID',
        right_on='TERDISPLAYID_ter',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='LOCID_ter',
        right_on='LOCID_lc',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='CHID_lc',
        right_on='CHID_c',
        how='left'
    )
    # Group by and aggregate
    df_final = df_final.groupby(
        ['LOCDISPLAYID_lc', 'CHDISPLAYID_c', 'fin.AGENTINVOICE_PRODUCTID', 'fin.SUB_PROD'],
        as_index=False
    ).agg(
        TOTALCOUNT=('fin.TOTALCOUNT', 'sum'),
        AMOUNT=('fin.TOTALAMOUNT', 'sum')
    ).assign(
        SALES_TYPE = '116',
        AGENTINVOICE_INVID = vinvoiceperiodid
    ).rename(
        columns={
            'LOCDISPLAYID_lc': 'LOCDISPLAYID',
            'CHDISPLAYID_c': 'AGENTINVOICE_FINIDN',
            'fin.AGENTINVOICE_PRODUCTID': 'AGENTINVOICE_PRODUCTID',
            'fin.SUB_PROD': 'SUB_PROD'
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT','SUB_PROD']]
    df_final['AMOUNT'] = df_final['AMOUNT'] * 100
    df_ubt_temp_datalocationinvoice = pd.concat([df_ubt_temp_datalocationinvoice, df_final], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE COMPLETED 1.18 : {len(df_final)} rows - Line (802- 895)" )

    for terdisplayid, agentinvoice_productid,sales_types in df_merge.groupby(['LOCDISPLAYID', 'AGENTINVOICE_PRODUCTID','SALES_TYPE']).groups.keys():
        partition = df_merge[
            (df_merge['LOCDISPLAYID'] == terdisplayid) &
            (df_merge['AGENTINVOICE_PRODUCTID'] == agentinvoice_productid) &
            (df_merge['SALES_TYPE'] == sales_types)
        ]
        logger.info(f"Partition - LOCDISPLAYID: {terdisplayid}, AGENTINVOICE_PRODUCTID: {agentinvoice_productid},SALES_TYPE: {sales_types}, Rows: {len(partition)}")


    return df_ubt_temp_datalocationinvoice


def ubt_temp_transamountdetaildata(vstartperiod, vendperiod):
    df_ubt_temp_transamountdetaildata = pd.DataFrame(
        {
            "TERDISPLAYID": pd.Series(dtype="string"),
            "PRODUCTNAME": pd.Series(dtype="string"),
            "AMOUNT": pd.Series(dtype="float"),
            "CT": pd.Series(dtype="int"),
            "FLAG": pd.Series(dtype="string"),
            "TRANSTYPE": pd.Series(dtype="string"),
            "FROMDATE": pd.Series(dtype="datetime64[ns]"),
            "TODATE": pd.Series(dtype="datetime64[ns]"),
        }
    )
    return df_ubt_temp_transamountdetaildata

def ubt_temp_salestotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC,
                               df_ubt_temp_itotolocation,
                               df_ubt_temp_getamounttrans
                               ,df_ubt_temp_cancelledbeticket
                               ,df_ubt_temp_terminal
                               ,df_ubt_temp_location):
    df_ubt_temp_salestotolocation = pd.DataFrame({
        "LOCID": pd.Series(dtype="string"),
        "CHID": pd.Series(dtype="string"),
        "PRODID": pd.Series(dtype="int"),
        "BETTYPE": pd.Series(dtype="int"),
        "WAGERTOTAL": pd.Series(dtype="float"),
        "CANCELTOTAL": pd.Series(dtype="float"),
        "WAGERAMOUNT": pd.Series(dtype="float"),
        "SALESAMOUNT": pd.Series(dtype="float")
    })

#     select loc.locid, loc.chid, fin.prodid, fin.bettype, sum(fin.wagertotal), sum(fin.canceltotal), sum(fin.wageramount),
# (sum(fin.salesamount) * 100) --(10)convert back from dollars to cents
# from
# (
# 	select  (pbth.createddate :: date) actualdate,
# 	pbth.terdisplayid as terdisplayid,
# 	3 as prodid,
# 	1 as bettype,
# 	count(distinct fin.ticketserialnumber)	as wagertotal,
# 	count(distinct can.ticketserialnumber)	as canceltotal,--(8)
# 	sum(coalesce(fin.wager,0))	as wageramount,
# 	round (trunc(sum(coalesce(fin.sales,0)) / 100,2), 2)	as salesamount
# 	--from	public.ztubt_placedbettransactionheader pbth
# 	from ubt_temp_pbth pbth
# 	inner join
# 	(
# select gat.ticketserialnumber, gat.wager,
# case when can.ticketserialnumber is null then gat.sales else 0 end as sales
# from ubt_temp_iTotolocation itoto
# inner join ubt_temp_getamounttrans gat on itoto.ticketserialnumber = gat.ticketserialnumber
# left join --(9)
# (

# 	select cb.ticketserialnumber from ubt_temp_cancelledbeticket cb
# )can on gat.ticketserialnumber = can.ticketserialnumber
# where itoto.itotoindicator = true --itoto
# 	)fin on pbth.ticketserialnumber = fin.ticketserialnumber
# 	left join --(8)
# 	(
# select gat.ticketserialnumber, gat.wager, gat.sales
# from ubt_temp_iTotolocation itoto
# inner join ubt_temp_getamounttrans gat on itoto.ticketserialnumber = gat.ticketserialnumber
# inner join ubt_temp_cancelledbeticket cb on gat.ticketserialnumber = cb.ticketserialnumber
# where itoto.itotoindicator = true --itoto
# 	)can on pbth.ticketserialnumber = can.ticketserialnumber

# 	group by pbth.terdisplayid, (pbth.createddate :: date)
# )fin
# inner join ubt_temp_terminal ter on fin.terdisplayid = ter.terdisplayid
# inner join ubt_temp_location loc on ter.locid = loc.locid
# group by loc.locid, loc.chid, fin.prodid, fin.bettype


    query = f"""
    select TERDISPLAYID , TICKETSERIALNUMBER, CREATEDDATE
    from {schema}.ztubt_placedbettransactionheader pbt
    where
	pbt.prodid = 3
    and (pbt.createddate between '{vfromdatetimeigtUTC}'   and '{vtodatetimeigtUTC}')
    """
    df_ubt_temp_pbth = pd.read_sql(query, connection)

    df_ubt_temp_pbth = df_ubt_temp_pbth.add_prefix('pbth.')






    # Add prefix to avoid column name conflicts during merges
    df_ubt_temp_itotolocation = df_ubt_temp_itotolocation.add_prefix('itoto.')
    df_ubt_temp_getamounttrans = df_ubt_temp_getamounttrans.add_prefix('gat.')
    df_ubt_temp_cancelledbeticket = df_ubt_temp_cancelledbeticket.add_prefix('cb.')
    df_ubt_temp_terminal = df_ubt_temp_terminal.add_prefix('ter.')
    df_ubt_temp_location = df_ubt_temp_location.add_prefix('loc.')

    # df_fin
    df_fin = df_ubt_temp_itotolocation.merge(
        df_ubt_temp_getamounttrans,
        left_on='itoto.TICKETSERIALNUMBER',
        right_on='gat.TICKETSERIALNUMBER',
        how='inner'
    ).merge(
        df_ubt_temp_cancelledbeticket,
        left_on='itoto.TICKETSERIALNUMBER',
        right_on='cb.TICKETSERIALNUMBER',
        how='left'
    )
    df_fin = df_fin.loc[
        (df_fin['itoto.ITOTOINDICATOR'] == True)
    ].assign(
        SALES = lambda x: np.where(
            x['cb.TICKETSERIALNUMBER'].isna(),
            x['gat.SALES'],
            0
        )
    ).rename(
        columns={
            'itoto.TICKETSERIALNUMBER': 'TICKETSERIALNUMBER',
            'gat.WAGER': 'WAGER',
            'SALES': 'SALES'
        }
    )[['TICKETSERIALNUMBER', 'WAGER', 'SALES']]

    # df_cancel
    df_cancel = df_ubt_temp_itotolocation.merge(
        df_ubt_temp_getamounttrans,
        left_on='itoto.TICKETSERIALNUMBER',
        right_on='gat.TICKETSERIALNUMBER',
        how='inner'
    ).merge(
        df_ubt_temp_cancelledbeticket,
        left_on='itoto.TICKETSERIALNUMBER',
        right_on='cb.TICKETSERIALNUMBER',
        how='inner'
    )
    df_cancel = df_cancel.loc[
        (df_cancel['itoto.ITOTOINDICATOR'] == True)
    ].rename(
        columns={
            'itoto.TICKETSERIALNUMBER': 'TICKETSERIALNUMBER',
            'gat.WAGER': 'WAGER',
            'gat.SALES': 'SALES'
        }
    )[['TICKETSERIALNUMBER', 'WAGER', 'SALES']]

    df_fin = df_fin.add_prefix('fin.')
    df_cancel = df_cancel.add_prefix('can.')
    df_merged = df_ubt_temp_pbth.merge(
        df_fin,
        left_on='pbth.TICKETSERIALNUMBER',
        right_on='fin.TICKETSERIALNUMBER',
        how='inner'
    ).merge(
        df_cancel,
        left_on='pbth.TICKETSERIALNUMBER',
        right_on='can.TICKETSERIALNUMBER',
        how='left'
    )

    df_merged = df_merged.groupby(
        ['pbth.TERDISPLAYID', 'pbth.CREATEDDATE'],
        as_index=False
    ).agg(
        WAGERTOTAL = ('fin.TICKETSERIALNUMBER', 'nunique'),
        CANCELTOTAL = ('can.TICKETSERIALNUMBER', 'nunique'),
        WAGERAMOUNT = ('fin.WAGER', 'sum'),
        SALESAMOUNT = ('fin.SALES', 'sum')
    ).assign(
        PRODID = 3,
        BETTYPE = 1,
    ).rename(
        columns={
            'pbth.CREATEDDATE': 'ACTUALDATE',
            'pbth.TERDISPLAYID': 'TERDISPLAYID'
        }
    )[['ACTUALDATE', 'TERDISPLAYID', 'PRODID', 'BETTYPE', 'WAGERTOTAL', 'CANCELTOTAL', 'WAGERAMOUNT', 'SALESAMOUNT']]

    df_merged['SALESAMOUNT'] = (df_merged['SALESAMOUNT'] /100).round(2)

    df_merged = df_merged.add_prefix('fin.')
    df_ubt_temp_salestotolocation = df_merged.merge(
        df_ubt_temp_terminal,
        left_on='fin.TERDISPLAYID',
        right_on='ter.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='ter.LOCID',
        right_on='loc.LOCID',
        how='inner'
    ).groupby(
        ['loc.LOCID', 'loc.CHID', 'fin.PRODID', 'fin.BETTYPE'],
    ).agg(
        WAGERTOTAL = ('fin.WAGERTOTAL', 'sum'),
        CANCELTOTAL = ('fin.CANCELTOTAL', 'sum'),
        WAGERAMOUNT = ('fin.WAGERAMOUNT', 'sum'),
        SALESAMOUNT = ('fin.SALESAMOUNT', 'sum')
    ).reset_index().rename(
        columns={
            'loc.LOCID': 'LOCID',
            'loc.CHID': 'CHID',
            'fin.PRODID': 'PRODID',
            'fin.BETTYPE': 'BETTYPE',
        }
    )[['LOCID', 'CHID', 'PRODID', 'BETTYPE', 'WAGERTOTAL', 'CANCELTOTAL', 'WAGERAMOUNT', 'SALESAMOUNT']]

    df_ubt_temp_salestotolocation['SALESAMOUNT'] = df_ubt_temp_salestotolocation['SALESAMOUNT'] * 100

    return df_ubt_temp_salestotolocation

def ubt_temp_resultcashlesslocation(vfromdatetimeigtUTC, vtodatetimeigtUTC):
    df_ubt_temp_resultcashlesslocation = pd.DataFrame({
        "TERDISPLAYID": pd.Series(dtype="string"),
        "PRODUCTNAME": pd.Series(dtype="string"),
        "AMOUNT": pd.Series(dtype="float"),
        "CT": pd.Series(dtype="int")
    })

    query = f"""
    SELECT * FROM {schema}.ZTUBT_PAYMENTDETAIL
    WHERE paymenttypeid IN ('NC','NN','NCC','NFP')
    AND createddate >= '{vfromdatetimeigtUTC}' AND createddate < '{vtodatetimeigtUTC}'
    """
    df_paymentdetail = pd.read_sql(query, connection)
    df_paymentdetail = df_paymentdetail.add_prefix('pd.')
    df_ubt_temp_resultcashlesslocation = df_paymentdetail.groupby(
        ['pd.TERDISPLAYID', 'pd.PAYMENTTYPEID'],
        as_index=False
    ).agg(
        AMOUNT = ('pd.PAIDAMOUNT', 'sum'),
        CT = ('pd.PAYMENTDETAILID', 'nunique')
    ).assign(
        PRODUCTNAME = lambda x: np.where(
            (x['pd.PAYMENTTYPEID'].isin(['NC','NN'])),
            'NETS',
            np.where(
                (x['pd.PAYMENTTYPEID'] == 'NCC'),
                'CASHCARD',
                'Flashpay'  # 'NFP'
            )
        )
    ).rename(
        columns={
            'pd.TERDISPLAYID': 'TERDISPLAYID'
        }
    )[['TERDISPLAYID', 'PRODUCTNAME', 'AMOUNT', 'CT']]

    return df_ubt_temp_resultcashlesslocation

def ubt_temp_iTotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC):
    df_ubt_temp_iTotolocation = pd.DataFrame({
       "TICKETSERIALNUMBER": pd.Series(dtype="string"),
       "ITOTOINDICATOR": pd.Series(dtype="bool"),
       "GROUPUNITSEQUENCE": pd.Series(dtype="int"),
       "BETTYPE": pd.Series(dtype="int")
    })


    query = f"""
    SELECT DISTINCT pb.TICKETSERIALNUMBER, pbt.ITOTOINDICATOR, pbt.GROUPUNITSEQUENCE,
    CASE
        WHEN pbt.BETTYPEID NOT IN ('M AN','M 2','M 3','M 4') THEN 1
        ELSE 2
    END AS BETTYPE
    FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER pb
    INNER JOIN {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM pbt
    ON pb.TRANHEADERID = pbt.TRANHEADERID
    WHERE (pbt.GROUPUNITSEQUENCE = 1)
    OR (pbt.ITOTOINDICATOR = true)
    AND (pb.CREATEDDATE >= '{vfromdatetimeigtUTC}' AND pb.CREATEDDATE < '{vtodatetimeigtUTC}')
    """
    df_ubt_temp_iTotolocation = pd.read_sql(query, connection)

    return df_ubt_temp_iTotolocation

def ubt_temp_grouptotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC):
    df_ubt_temp_grouptotolocation = pd.DataFrame({
      "TICKETSERIALNUMBER": pd.Series(dtype="string"),
      "GROUPHOSTID": pd.Series(dtype="string"),
      "GROUPUNITSEQUENCE": pd.Series(dtype="int"),
      "BETTYPE": pd.Series(dtype="int")
    })

    query = f"""
    SELECT DISTINCT pb.TICKETSERIALNUMBER, pbt.GROUPHOSTID, pbt.GROUPUNITSEQUENCE,
    CASE
        WHEN pbt.BETTYPEID NOT IN ('M AN','M 2','M 3','M 4') THEN 1
        ELSE 2
    END AS BETTYPE
    FROM {schema}.ZTUBT_PLACEDBETTRANSACTIONHEADER pb
    INNER JOIN {schema}.ZTUBT_TOTO_PLACEDBETTRANSACTIONLINEITEM pbt
    ON pb.TRANHEADERID = pbt.TRANHEADERID
    WHERE pbt.GROUPUNITSEQUENCE IS NOT NULL
    AND (pb.CREATEDDATE >= '{vfromdatetimeigtUTC}' AND pb.CREATEDDATE < '{vtodatetimeigtUTC}')
    """
    df_ubt_temp_grouptotolocation = pd.read_sql(query, connection)
    logger.info(f"UBT_TEMP_GROUPTOLOCATION COMPLETED : {len(df_ubt_temp_grouptotolocation)} rows - Line (223 - 231)" )
    return df_ubt_temp_grouptotolocation

def ubt_temp_salesgrouptotolocation(vfromdatetimeigtUTC, vtodatetimeigtUTC
                                    ,df_ubt_temp_itotolocation
                                    ,df_ubt_temp_getamounttrans
                                    ,df_ubt_temp_cancelledbeticket
                                    ,df_ubt_temp_terminal
                                    ,df_ubt_temp_location):

    df_ubt_temp_salesgrouptotolocation = pd.DataFrame({
      "LOCID": pd.Series(dtype="int"),
      "CHID": pd.Series(dtype="int"),
      "PRODID": pd.Series(dtype="int"),
      "BETTYPE": pd.Series(dtype="int"),
      "WAGERTOTAL": pd.Series(dtype="int"),
      "CANCELTOTAL": pd.Series(dtype="int"),
      "WAGERAMOUNT": pd.Series(dtype="float"),
      "SALESAMOUNT": pd.Series(dtype="float")
    })
    query = f"""
    select TERDISPLAYID , TICKETSERIALNUMBER, CREATEDDATE
    from {schema}.ztubt_placedbettransactionheader pbt
    where
	pbt.prodid = 3
    and (pbt.createddate between '{vfromdatetimeigtUTC}'   and '{vtodatetimeigtUTC}')
    """
    df_ubt_temp_pbth = pd.read_sql(query, connection)

    df_ubt_temp_pbth = df_ubt_temp_pbth.add_prefix('pbth.')
    df_ubt_temp_itotolocation = df_ubt_temp_itotolocation.add_prefix('itoto.')
    df_ubt_temp_getamounttrans = df_ubt_temp_getamounttrans.add_prefix('gat.')
    df_ubt_temp_cancelledbeticket = df_ubt_temp_cancelledbeticket.add_prefix('cb.')
    df_ubt_temp_terminal = df_ubt_temp_terminal.add_prefix('ter.')
    df_ubt_temp_location = df_ubt_temp_location.add_prefix('lc.')

    df_fin = df_ubt_temp_itotolocation.merge(
        df_ubt_temp_getamounttrans,
        left_on='itoto.TICKETSERIALNUMBER',
        right_on='gat.TICKETSERIALNUMBER',
        how='inner'
    ).merge(
        df_ubt_temp_cancelledbeticket,
        left_on='itoto.TICKETSERIALNUMBER',
        right_on='cb.TICKETSERIALNUMBER',
        how='left'
    )
    df_fin = df_fin.loc[
        (df_fin['itoto.GROUPUNITSEQUENCE'] == 1)
    ].assign(
        SALES = lambda x: np.where(
            x['cb.TICKETSERIALNUMBER'].isnull(),
            x['gat.SALES'],
            0
        )
    ).rename(
        columns={
            'gat.TICKETSERIALNUMBER': 'fin.TICKETSERIALNUMBER',
            'gat.WAGER': 'fin.WAGER',
            'gat.SALES': 'fin.SALES',
            'itoto.BETTYPE': 'fin.BETTYPE',
        }
    )[['fin.TICKETSERIALNUMBER', 'fin.WAGER', 'fin.SALES', 'fin.BETTYPE']]
    logger.info(f"UBT_TEMP_FIN COMPLETED : {len(df_fin)} rows - Line (278 - 290)" )

    df_can = df_ubt_temp_itotolocation.merge(
        df_ubt_temp_getamounttrans,
        left_on='itoto.TICKETSERIALNUMBER',
        right_on='gat.TICKETSERIALNUMBER',
        how='inner'
    ).merge(
        df_ubt_temp_cancelledbeticket,
        left_on='itoto.TICKETSERIALNUMBER',
        right_on='cb.TICKETSERIALNUMBER',
        how='inner'
    )
    df_can = df_can.loc[
        (df_can['itoto.GROUPUNITSEQUENCE'] == 1)
    ].rename(
        columns={
            'gat.TICKETSERIALNUMBER': 'can.TICKETSERIALNUMBER',
            'gat.WAGER': 'can.WAGER',
            'gat.SALES': 'can.SALES',
            'itoto.BETTYPE': 'can.BETTYPE',
        }
    )[['can.TICKETSERIALNUMBER', 'can.WAGER', 'can.SALES', 'can.BETTYPE']]
    logger.info(f"UBT_TEMP_CAN COMPLETED : {len(df_can)} rows - Line (294 - 299)" )


    df_merge = df_ubt_temp_pbth.merge(
        df_fin,
        left_on='pbth.TICKETSERIALNUMBER',
        right_on='fin.TICKETSERIALNUMBER',
        how='inner'
    ).merge(
        df_can,
        left_on='pbth.TICKETSERIALNUMBER',
        right_on='can.TICKETSERIALNUMBER',
        how='left'
    ).assign(
        ACTUALDATE = lambda x: pd.to_datetime(x['pbth.CREATEDDATE']).dt.date,
        PRODID = 3
    ).groupby(
        ['pbth.TERDISPLAYID', 'ACTUALDATE', 'PRODID', 'fin.BETTYPE'],
        as_index=False
    ).agg(
        WAGERTOTAL = ('fin.TICKETSERIALNUMBER', 'nunique'),
        CANCELTOTAL = ('can.TICKETSERIALNUMBER', 'nunique'),
        WAGERAMOUNT = ('fin.WAGER', 'sum'),
        SALESAMOUNT = ('fin.SALES', 'sum')
    )[['ACTUALDATE', 'pbth.TERDISPLAYID', 'PRODID', 'fin.BETTYPE', 'WAGERTOTAL', 'CANCELTOTAL', 'WAGERAMOUNT', 'SALESAMOUNT']]

    df_merge['SALESAMOUNT'] = (df_merge['SALESAMOUNT']/100).round(2) * 100
    df_ubt_temp_salesgrouptotolocation = df_merge.merge(
        df_ubt_temp_terminal,
        left_on='pbth.TERDISPLAYID',
        right_on='ter.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='ter.LOCID',
        right_on='lc.LOCID',
        how='inner'
    ).rename(
        columns={
            'lc.LOCID': 'LOCID',
            'lc.CHID': 'CHID',
            'fin.BETTYPE': 'BETTYPE',
        }
    )[['LOCID', 'CHID', 'PRODID', 'BETTYPE', 'WAGERTOTAL', 'CANCELTOTAL', 'WAGERAMOUNT', 'SALESAMOUNT']]

    df_ubt_temp_salesgrouptotolocation = df_ubt_temp_salesgrouptotolocation.groupby(
        ['LOCID', 'CHID', 'PRODID', 'BETTYPE'],
        as_index=False
    ).agg(
        WAGERTOTAL = ('WAGERTOTAL', 'sum'),
        CANCELTOTAL = ('CANCELTOTAL', 'sum'),
        WAGERAMOUNT = ('WAGERAMOUNT', 'sum'),
        SALESAMOUNT = ('SALESAMOUNT', 'sum')
    )
    df_ubt_temp_salesgrouptotolocation['SALESAMOUNT'] = df_ubt_temp_salesgrouptotolocation['SALESAMOUNT'] * 100

    logger.info(f"UBT_TEMP_SALESGROUPTOLOCATION COMPLETED : {len(df_ubt_temp_salesgrouptotolocation)} rows - Line (258 - 311)" )
    return df_ubt_temp_salesgrouptotolocation

def ubt_temp_sales_scandsr_loc(df_ubt_temp_sales_scandsr,
                                df_ubt_temp_terminal,
                                df_ubt_temp_location,
                                df_ubt_temp_chain,
                                vinvoiceperiodid):


    df_ubt_temp_sales_scandsr_loc = pd.DataFrame({
        "LOCDISPLAYID": pd.Series(dtype="string"),
        "AGENTINVOICE_INVID": pd.Series(dtype="int"),
        "AGENTINVOICE_FINIDN": pd.Series(dtype="string"),
        "AGENTINVOICE_PRODUCTID": pd.Series(dtype="int"),
        "SALES_TYPE": pd.Series(dtype="string"),
        "TOTALCOUNT": pd.Series(dtype="int"),
        "AMOUNT": pd.Series(dtype="float")
    })

    # Add prefix to avoid column name conflicts during merges
    df_ubt_temp_sales_scandsr = df_ubt_temp_sales_scandsr.add_prefix('RS.')
    df_ubt_temp_terminal = df_ubt_temp_terminal.add_prefix('TER.')
    df_ubt_temp_location = df_ubt_temp_location.add_prefix('LOC.')
    df_ubt_temp_chain = df_ubt_temp_chain.add_prefix('CH.')

    # Merge
    df_merged = df_ubt_temp_sales_scandsr.merge(
        df_ubt_temp_terminal,
        left_on='RS.TERDISPLAYID',
        right_on='TER.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='TER.LOCID',
        right_on='LOC.LOCID',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='LOC.CHID',
        right_on='CH.CHID',
        how='left'
    )

    # Group by and aggregate
    df_ubt_temp_sales_scandsr_loc = df_merged.groupby(
        ['LOC.LOCDISPLAYID', 'CH.CHDISPLAYID', 'RS.PRODID'],
        as_index=False
    ).agg(
        TOTALCOUNT=('RS.TOTALCOUNT', 'sum'),
        AMOUNT=('RS.AMOUNT', 'sum')
    ).assign(
        SALES_TYPE='116',
        AGENTINVOICE_INVID=vinvoiceperiodid
    ).rename(
        columns={
            'LOC.LOCDISPLAYID': 'LOCDISPLAYID',
            'CH.CHDISPLAYID': 'AGENTINVOICE_FINIDN',
            'RS.PRODID': 'AGENTINVOICE_PRODUCTID'
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT']]
    logger.info(f"UBT_TEMP_SALES_SCANDSR_LOC COMPLETED : {len(df_ubt_temp_sales_scandsr_loc)} rows - Line (964 - 995)" )
    return df_ubt_temp_sales_scandsr_loc

def ubt_temp_salesfactorconfig():
    df_ubt_temp_salesfactorconfig = pd.DataFrame({
        "PRODID": pd.Series(dtype="int"),
        "SALESFACTOR": pd.Series(dtype="float")
    })

    try:
        query = f"""
        SELECT PRODID, SALESFACTOR FROM {schema}.ztubt_salescomconfig
        where COMMISSIONTYPE = 1
        AND ISDELETED = false
        AND PRODID IN (2,3,4)
        """
        df_ubt_temp_salesfactorconfig = pd.read_sql(query, connection)
        logger.info(f"ubt_temp_salesfactorconfig: Retrieved {len(df_ubt_temp_salesfactorconfig)} records.")
    except Exception as e:
        logger.warning(f"Could not load ZTUBT_SALESFACTORCONFIG table: {str(e)}")
        logger.warning("Returning empty dataframe with default schema.")
        # Return empty DataFrame with correct schema
        df_ubt_temp_salesfactorconfig = pd.DataFrame({
            "PRODID": pd.Series(dtype="int"),
            "SALESFACTOR": pd.Series(dtype="float")
        })

    return df_ubt_temp_salesfactorconfig

def ubt_temp_datalocationinvoice_2nd(df_ubt_temp_datalocationinvoice,
                                    df_ubt_temp_location,
                                    df_ubt_temp_chain,
                                    df_ubt_temp_salesfactorconfig,
                                    df_ubt_temp_transamountdetaildata,
                                    df_ubt_temp_terminal,
                                    vinvoiceperiodid):


    df_ubt_temp_datalocationinvoice_2nd = pd.DataFrame({
        "LOCDISPLAYID": pd.Series(dtype="string"),
        "AGENTINVOICE_INVID": pd.Series(dtype="int"),
        "AGENTINVOICE_FINIDN": pd.Series(dtype="string"),
        "AGENTINVOICE_PRODUCTID": pd.Series(dtype="int"),
        "SALES_TYPE": pd.Series(dtype="string"),
        "TOTALCOUNT": pd.Series(dtype="int"),
        "AMOUNT": pd.Series(dtype="float"),
        "SUB_PROD": pd.Series(dtype="string")
    })

    df_ubt_temp_datalocationinvoice_2nd = df_ubt_temp_datalocationinvoice.copy()
    # Add prefix to avoid column name conflicts during merges
    df_ubt_temp_location = df_ubt_temp_location.add_prefix('LOC.')
    df_ubt_temp_chain = df_ubt_temp_chain.add_prefix('CH.')
    df_ubt_temp_salesfactorconfig = df_ubt_temp_salesfactorconfig.add_prefix('SFC.')
    df_ubt_temp_transamountdetaildata = df_ubt_temp_transamountdetaildata.add_prefix('TAD.')
    df_ubt_temp_terminal = df_ubt_temp_terminal.add_prefix('TER.')


    df_merge = df_ubt_temp_location.merge(
        df_ubt_temp_chain,
        left_on='LOC.CHID',
        right_on='CH.CHID',
        how='inner'
    ).merge(
        df_ubt_temp_salesfactorconfig,
        how='cross'
    )

    df_merge = df_merge.loc[
        (~df_merge['SFC.PRODID'].isin([3]))
    ]

    df_merge = df_merge.assign(
        SALES_TYPE = '119',
        TOTALCOUNT = 0,
        SUB_PROD = '',
        AMOUNT = lambda x : x['SFC.SALESFACTOR'],
        AGENTINVOICE_INVID = vinvoiceperiodid
    ).rename(
        columns={
            'LOC.LOCDISPLAYID': 'LOCDISPLAYID',
            'CH.CHDISPLAYID': 'AGENTINVOICE_FINIDN',
            'SFC.PRODID': 'AGENTINVOICE_PRODUCTID',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT','AMOUNT', 'SUB_PROD']]

    df_ubt_temp_datalocationinvoice_2nd = pd.concat([df_ubt_temp_datalocationinvoice_2nd, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE_2ND 1.1 COMPLETED : {len(df_ubt_temp_datalocationinvoice_2nd)} rows - Line (990 - 1001)" )

    # 1.2 : TOTO, TOTO MATCH
    df_merge = df_ubt_temp_location.merge(
        df_ubt_temp_chain,
        left_on='LOC.CHID',
        right_on='CH.CHID',
        how='inner'
    ).merge(
        df_ubt_temp_salesfactorconfig.loc[df_ubt_temp_salesfactorconfig['SFC.PRODID'] == 3],
        how='cross'
    ).merge(
        pd.DataFrame({
            'SUB_PROD': ['TOTO', 'TOTO MATCH']
        }),
        how='cross'
    ).assign(
        SALES_TYPE = '119',
        TOTALCOUNT = 0,
        AGENTINVOICE_INVID = vinvoiceperiodid
    ).rename(
        columns={
            'LOC.LOCDISPLAYID': 'LOCDISPLAYID',
            'CH.CHDISPLAYID': 'AGENTINVOICE_FINIDN',
            'SFC.PRODID': 'AGENTINVOICE_PRODUCTID',
            'SFC.SALESFACTOR': 'AMOUNT'
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT','AMOUNT', 'SUB_PROD']]
    df_ubt_temp_datalocationinvoice_2nd = pd.concat([df_ubt_temp_datalocationinvoice_2nd, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE_2ND 1.2 COMPLETED : {len(df_merge)} rows - Line (1003 - 1016)" )



    #1.3: ACT_VALID_FUND_AMT & ACT_RECOVERY_AMT
    df_a = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on='TAD.TERDISPLAYID',
        right_on='TER.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='TER.LOCID',
        right_on='LOC.LOCID',
        how='inner'
    )

    # Apply filters after merge using the merged DataFrame columns
    df_a = df_a.loc[
        (df_a['TAD.FLAG'].isin(['FUN', 'REC'])) &
        (df_a['TAD.PRODUCTNAME'].isin(['Funding and Recovery'])) &
        (df_a['LOC.LOCTYPEID'].isin([2,4]))
    ].groupby(
        ['LOC.LOCID', 'TAD.FLAG'], as_index=False
    ).agg(
        AMOUNT = ('TAD.AMOUNT', 'max')
    )[['LOC.LOCID', 'TAD.FLAG', 'AMOUNT']]

    df_a = df_a.merge(
        df_ubt_temp_location,
        left_on='LOC.LOCID',
        right_on='LOC.LOCID',
        how='inner'
    ).merge(
        df_ubt_temp_chain,
        left_on='LOC.CHID',
        right_on='CH.CHID',
        how='left'
    ).groupby(
        ['LOC.LOCDISPLAYID','CH.CHDISPLAYID','TAD.FLAG'], as_index=False
    ).agg(
        FUN_SUM = ('AMOUNT','sum'),
        REC_SUM = ('AMOUNT','sum')
    )
    df_a['FUN_SUM'] = df_a['FUN_SUM'].fillna(0) * -1
    df_a['REC_SUM'] = df_a['REC_SUM'].fillna(0)

    df_a = df_a.assign(
        AGENTINVOICE_INVID = vinvoiceperiodid,
        AGENTINVOICE_PRODUCTID = 0,
        TOTALCOUNT = 0,
        SALES_TYPE = lambda x: np.where(
            x['TAD.FLAG'] == 'FUN',
            '571',
            np.where(
                x['TAD.FLAG'] == 'REC',
                '572',
                ''
            )
        ),
        AMOUNT = lambda x: np.where(
            x['TAD.FLAG'] == 'FUN',
            x['FUN_SUM'],
            np.where(
                x['TAD.FLAG'] == 'REC',
                x['REC_SUM'],
                ''
            )
        ),
        SUB_PROD = ''
    ).rename(
        columns={
            'LOC.LOCDISPLAYID': 'LOCDISPLAYID',
            'CH.CHDISPLAYID': 'AGENTINVOICE_FINIDN',
            'AMOUNT': 'AMOUNT',
        }
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID','SALES_TYPE', 'TOTALCOUNT', 'AMOUNT']]

    df_ubt_temp_datalocationinvoice_2nd = pd.concat([df_ubt_temp_datalocationinvoice_2nd, df_a], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE_2ND 1.3 COMPLETED : {len(df_a)} rows - Line (1022 - 1044)")

    # 1.4

    df_a = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on='TAD.TERDISPLAYID',
        right_on='TER.TERDISPLAYID',
        how='inner'
    ).merge(
        df_ubt_temp_location,
        left_on='TER.LOCID',
        right_on='LOC.LOCID',
        how='inner'
    )

    df_a = df_a.loc[
         (df_a['TAD.FLAG'].isin(['FUN', 'REC'])) &
        (df_a['TAD.PRODUCTNAME'].isin(['Online Adjustment']))
    ]

    df_a = df_a.groupby(
        ['LOC.LOCID', 'TAD.FLAG'], as_index=False
    ).agg(
        AMOUNT = ('TAD.AMOUNT', 'max'),
        CT = ('TAD.CT', 'max')
    )[['LOC.LOCID', 'TAD.FLAG', 'AMOUNT', 'CT']]

    df_final = (
    df_a
    .merge(df_ubt_temp_location, left_on='LOC.LOCID', right_on='LOC.LOCID', how='inner')
    .merge(df_ubt_temp_chain,   left_on='LOC.CHID',   right_on='CH.CHID',     how='left')

    # Pivot để tách REC và FUN thành 2 cột riêng
    .assign(
        AMOUNT_ADJ = lambda x: np.where(x['TAD.FLAG'] == 'FUN', x['AMOUNT'] * -1, x['AMOUNT']),
        SALES_TYPE = lambda x: np.where(x['TAD.FLAG'] == 'REC', '-2', '-3')
    )
    .groupby(['LOC.LOCDISPLAYID', 'CH.CHDISPLAYID', 'SALES_TYPE'], as_index=False)
    .agg(
        TOTALCOUNT=('CT', 'sum'),
        AMOUNT=('AMOUNT_ADJ', 'sum')
    )
    .assign(
        AGENTINVOICE_INVID=vinvoiceperiodid,
        AGENTINVOICE_PRODUCTID=0,
        SUB_PROD='',
        AGENTINVOICE_FINIDN=lambda x: x['CH.CHDISPLAYID'],
        LOCDISPLAYID=lambda x: x['LOC.LOCDISPLAYID']
    )
    [['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN',
      'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT', 'SUB_PROD']]
)
    df_ubt_temp_datalocationinvoice_2nd = pd.concat([df_ubt_temp_datalocationinvoice_2nd, df_final], ignore_index=True)
    print(f"UBT_TEMP_DATALOCATIONINVOICE_2ND 1.4 COMPLETED : {len(df_final)} rows - Line (1050 - 1075)")


    # 1.5 : Line 1076 - 1126:
    df_ubt_temp_datalocationinvoice_2nd['AMOUNT'] = df_ubt_temp_datalocationinvoice_2nd['AMOUNT'].astype(float)
    df_cr = df_ubt_temp_datalocationinvoice_2nd.loc[
        (df_ubt_temp_datalocationinvoice_2nd['SALES_TYPE'].isin(
            ['2', '3', '7', '6', '108', '110', '112', '571', '4', '61', '-3']
        )) &
        (~df_ubt_temp_datalocationinvoice_2nd['AGENTINVOICE_PRODUCTID'].isin([31,99]))
    ].groupby(
        ['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN'],
        as_index=False
    ).agg(
        TOTALCOUNT = ('TOTALCOUNT', 'sum'),
        AMOUNT = ('AMOUNT', 'sum')
    )

    df_db = df_ubt_temp_datalocationinvoice_2nd.loc[
        (df_ubt_temp_datalocationinvoice_2nd['SALES_TYPE'].isin(
            ['1', '572', '-2']
        )) &
        (~df_ubt_temp_datalocationinvoice_2nd['AGENTINVOICE_PRODUCTID'].isin([31,99]))
    ].groupby(
        ['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN'],
        as_index=False
    ).agg(
        TOTALCOUNT = ('TOTALCOUNT', 'sum'),
        AMOUNT = ('AMOUNT', 'sum')
    )

    df_cr = df_cr.add_prefix('cr.')
    df_db = df_db.add_prefix('db.')
    df_merge = df_db.merge(
        df_cr,
        left_on=['db.LOCDISPLAYID', 'db.AGENTINVOICE_INVID', 'db.AGENTINVOICE_FINIDN'],
        right_on=['cr.LOCDISPLAYID', 'cr.AGENTINVOICE_INVID', 'cr.AGENTINVOICE_FINIDN'],
        how='outer',
    )

    df_merge = df_merge.assign(
        LOCDISPLAYID = df_merge['cr.LOCDISPLAYID'].combine_first(df_merge['db.LOCDISPLAYID']),
        AGENTINVOICE_INVID = df_merge['cr.AGENTINVOICE_INVID'].combine_first(df_merge['db.AGENTINVOICE_INVID']),
        AGENTINVOICE_FINIDN = df_merge['cr.AGENTINVOICE_FINIDN'].combine_first(df_merge['db.AGENTINVOICE_FINIDN']),
        AGENTINVOICE_PRODUCTID = 0,
        SALES_TYPE = '727',
        TOTALCOUNT = 0,
        AMOUNT = (df_merge['db.AMOUNT'].fillna(0) - df_merge['cr.AMOUNT'].fillna(0))
    )[[
        'LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN',
        'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT'
    ]]
    df_ubt_temp_datalocationinvoice_2nd = pd.concat([df_ubt_temp_datalocationinvoice_2nd, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE_2ND 1.5 COMPLETED : {len(df_merge)} rows - Line (1077 - 1126)")


    # Line 1130 : DELETE RECORDS WHERE SALES_TYPE IN -2, -3
    df_ubt_temp_datalocationinvoice_2nd = df_ubt_temp_datalocationinvoice_2nd.loc[
        ~df_ubt_temp_datalocationinvoice_2nd['SALES_TYPE'].isin(['-2', '-3'])
    ]

    # 1.6 sum TOTO MATCH AND T649
    df_merge = pd.DataFrame()
    df_merge = df_ubt_temp_datalocationinvoice_2nd.loc[
        (df_ubt_temp_datalocationinvoice_2nd['SUB_PROD'].isin(['TOTO MATCH', 'TOTO'])) &
        (~df_ubt_temp_datalocationinvoice_2nd['SALES_TYPE'].isin(['40', '119', '105']))
    ].groupby(
        ['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE'],
        as_index=False
    ).agg(
        TOTALCOUNT = ('TOTALCOUNT', 'sum'),
        AMOUNT = ('AMOUNT', 'sum')
    ).assign(
        SUB_PROD = ''
    )
    df_ubt_temp_datalocationinvoice_2nd = pd.concat([df_ubt_temp_datalocationinvoice_2nd, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE_2ND 1.6 COMPLETED : {len(df_merge)} rows - Line (1132 - 1150)" )

    # 1.7 : config - clone totomatch config for product Toto (t649 and totomatch)
    df_merge = pd.DataFrame()
    df_merge = df_ubt_temp_datalocationinvoice_2nd.loc[
        (df_ubt_temp_datalocationinvoice_2nd['SUB_PROD'].isin(['TOTO MATCH'])) &
        (df_ubt_temp_datalocationinvoice_2nd['SALES_TYPE'].isin(['40', '119', '105']))
    ].assign(
        SUB_PROD = ''
    )
    df_ubt_temp_datalocationinvoice_2nd = pd.concat([df_ubt_temp_datalocationinvoice_2nd, df_merge], ignore_index=True)
    logger.info(f"UBT_TEMP_DATALOCATIONINVOICE_2ND 1.7 COMPLETED : {len(df_merge)} rows - Line (1152 - 1161)" )
    #===== UPDATE  =====#
    query = f"""
    SELECT PRODID, SAPPRODID FROM {schema}.ztubt_sap_product
    """
    df_ztubt_sap_product = pd.read_sql(query, connection)

    df_ztubt_sap_product = df_ztubt_sap_product.add_prefix('SP.')

    df_ubt_temp_datalocationinvoice_2nd = df_ubt_temp_datalocationinvoice_2nd.merge(
        df_ztubt_sap_product,
        left_on='AGENTINVOICE_PRODUCTID',
        right_on='SP.PRODID',
        how='left'
    ).assign(
        AGENTINVOICE_PRODUCTID = lambda x: np.where(
            (x['AGENTINVOICE_PRODUCTID'] == 3) & (x['SUB_PROD'] == 'TOTO'),
            71,
            np.where(
                (x['AGENTINVOICE_PRODUCTID'] == 3) & (x['SUB_PROD'] == 'TOTO MATCH'),
                66,
                x['SP.SAPPRODID']
            )
        )
    ).drop(
        columns=['SP.PRODID', 'SP.SAPPRODID']
    )
    [['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT', 'SUB_PROD']]

    # final return

    df_ubt_temp_datalocationinvoice_2nd = df_ubt_temp_datalocationinvoice_2nd.loc[
        ((df_ubt_temp_datalocationinvoice_2nd['TOTALCOUNT'] != 0) | (df_ubt_temp_datalocationinvoice_2nd['AMOUNT'] != 0)) &
        (df_ubt_temp_datalocationinvoice_2nd['AGENTINVOICE_FINIDN'] != '') &
        (df_ubt_temp_datalocationinvoice_2nd['AGENTINVOICE_INVID'] == vinvoiceperiodid)
    ].drop_duplicates(
        subset=['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT']
    )[['LOCDISPLAYID', 'AGENTINVOICE_INVID', 'AGENTINVOICE_FINIDN', 'AGENTINVOICE_PRODUCTID', 'SALES_TYPE', 'TOTALCOUNT', 'AMOUNT']]

    return df_ubt_temp_datalocationinvoice_2nd