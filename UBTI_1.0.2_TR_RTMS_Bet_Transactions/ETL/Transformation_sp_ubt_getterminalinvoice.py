import pandas as pd
import numpy as np
import os, logging
logger = logging.getLogger(__name__)
from Utilities.Snowflake_connection import snowflake_connection
connection = snowflake_connection()
schema = "SPPL_DEV_DWH.SPPL_PUBLIC"
from Store_Procedure_Common.sp_ubt_getamounttransaction import *
v_totomatchBettypes = [
    ('M AN'),
    ('M 2'),
    ('M 3'),
    ('M 4')
]


def ubt_temp_terminal():
    query = f"""
    Select Ter.TerDisplayID, Ter.LocID  FROM {schema}.ZTUBT_TERMINAL Ter
    """

    df_ubt_temp_terminal = pd.read_sql(query, connection)
    return df_ubt_temp_terminal

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



def ubt_temp_location():
    query = f"""
        Select Loc.LocID, Loc.LocDisplayID, Loc.SweepIndicator,Loc.LocTypeID,Loc.IsGST,Loc.AccountNumber,
	Loc.BranchID,Loc.Bankid,Loc.LocName,Loc.IsIBG,Loc.RetID,LOC.CHID
	from {schema}.ztubt_location  Loc
    """
    df_ubt_temp_location = pd.read_sql(query, connection)

    return df_ubt_temp_location

def ubt_temp_TmpTicketByWageAndSales(vbusinessdate):

    df_ubt_temp_TmpTicketByWageAndSales = sp_ubt_getamounttransaction(vbusinessdate, vbusinessdate)
    df_ubt_temp_TmpTicketByWageAndSales.rename(columns={
        "TKTSERIALNUMBER": "TICKETSERIALNUMBER"
    }, inplace=True)

    return df_ubt_temp_TmpTicketByWageAndSales

def ubt_temp_ResultCashlessInTerminal(vfromdatetimeNonHost_UTC,
                                      vtodatetimeNonHost_UTC
):
    # terdisplayid varchar(200),
	# productname varchar(100),
	# amount numeric(22, 11),
	# ct int4
    df_ubt_temp_ResultCashlessInTerminal = pd.DataFrame(
        {
            "TERDISPLAYID": pd.Series(dtype="string"),
            "PRODUCTNAME": pd.Series(dtype="string"),
            "AMOUNT": pd.Series(dtype="float"),
            "CT": pd.Series(dtype="Int64"),
        }
    )
    query = f"""
                SELECT
                    pd.terdisplayid,
                    'NETS' as PRODUCTNAME,
                    coalesce (sum(coalesce (pd.paidamount,0)),0) as AMOUNT,
                    count(distinct pd.paymentdetailid) as CT
                from {schema}.ztubt_paymentdetail pd
                where pd.paymenttypeid in ('NC','NN')
                and pd.createddate >= '{vfromdatetimeNonHost_UTC}'
                and pd.createddate < '{vtodatetimeNonHost_UTC}'
                group by pd.terdisplayid

                union all
                select pd.terdisplayid,
                'CASHCARD' as PRODUCTNAME,
                coalesce (sum(coalesce (pd.paidamount,0)),0) as AMOUNT,
                count(distinct pd.paymentdetailid) as CT
                from {schema}.ztubt_paymentdetail pd
                where pd.paymenttypeid in ('NCC') --cashcard
                and pd.createddate >= '{vfromdatetimeNonHost_UTC}'
                and pd.createddate < '{vtodatetimeNonHost_UTC}'
                group by pd.terdisplayid

                union all

                select pd.terdisplayid,
                'Flashpay' as PRODUCTNAME,
                coalesce (sum(coalesce (pd.paidamount,0)),0) as AMOUNT,
                count(distinct pd.paymentdetailid) as CT
                from {schema}.ztubt_paymentdetail pd
                where pd.paymenttypeid in ('NFP') --Flashpay
                and pd.createddate >= '{vfromdatetimeNonHost_UTC}'
                and pd.createddate < '{vtodatetimeNonHost_UTC}'
                group by pd.terdisplayid
    """

    df_ubt_temp_ResultCashlessInTerminal = pd.read_sql(query, connection)


    return df_ubt_temp_ResultCashlessInTerminal

def ubt_temp_iTOTO(vfromdatetimeIGT_UTC,
                   vtodatetimeIGT_UTC
):
    df_ubt_temp_iTOTO = pd.DataFrame({
        "TICKETSERIALNUMBER": pd.Series(dtype="string"),
        "ITOTOINDICATOR": pd.Series(dtype="bool"),
        "GROUPUNITSEQUENCE": pd.Series(dtype="int"),
        "BETTYPE": pd.Series(dtype="int"),
    })

    query = f"""
    select * from ztubt_placedbettransactionheader
    where createddate between '{vfromdatetimeIGT_UTC}'   and '{vtodatetimeIGT_UTC}'
    """
    df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)

    query = f"""
    select * from ztubt_toto_placedbettransactionlineitem
    where tranheaderid in (select tranheaderid from ztubt_placedbettransactionheader
    where createddate between '{vfromdatetimeIGT_UTC}'   and '{vtodatetimeIGT_UTC}')
    """

    df_ztubt_toto_placedbettransactionlineitem = pd.read_sql(query, connection)

    # add prefix
    df_ztubt_placedbettransactionheader = df_ztubt_placedbettransactionheader.add_prefix("PB.")
    df_ztubt_toto_placedbettransactionlineitem = df_ztubt_toto_placedbettransactionlineitem.add_prefix("PBT.")

    df_temp = df_ztubt_placedbettransactionheader.merge(
        df_ztubt_toto_placedbettransactionlineitem,
        left_on="PB.TRANHEADERID",
        right_on="PBT.TRANHEADERID",
        how="inner"
    )

    df_temp = df_temp[
        (df_temp["PBT.GROUPUNITSEQUENCE"] == 1) |
        (df_temp["PBT.ITOTOINDICATOR"] == True) &
        ((df_temp["PB.CREATEDDATE"] >= vfromdatetimeIGT_UTC) &
        (df_temp["PB.CREATEDDATE"] < vtodatetimeIGT_UTC))
    ]

    df_temp["BETTYPE"] = np.where(
        df_temp["PBT.BETTYPEID"].isin([item[0] for item in v_totomatchBettypes]),
        2,  # TOTO MATCH
        1   # TOTO
    )

    df_ubt_temp_iTOTO = df_temp[[
        "PB.TICKETSERIALNUMBER",
        "PBT.ITOTOINDICATOR",
        "PBT.GROUPUNITSEQUENCE",
        "BETTYPE"
    ]]\
    .drop_duplicates(subset=["PB.TICKETSERIALNUMBER", "BETTYPE","PBT.GROUPUNITSEQUENCE", "PBT.ITOTOINDICATOR"])

    df_ubt_temp_iTOTO.rename(columns={
        "PB.TICKETSERIALNUMBER": "TICKETSERIALNUMBER",
        "PBT.ITOTOINDICATOR": "ITOTOINDICATOR",
        "PBT.GROUPUNITSEQUENCE": "GROUPUNITSEQUENCE"
    }, inplace=True)

    df_ubt_temp_iTOTO = df_ubt_temp_iTOTO[['TICKETSERIALNUMBER', 'ITOTOINDICATOR', 'GROUPUNITSEQUENCE', 'BETTYPE']]


    return df_ubt_temp_iTOTO

def ubt_temp_grouptoto(vfromdatetimeIGT_UTC,
                        vtodatetimeIGT_UTC
):
    df_ubt_temp_grouptoto = pd.DataFrame({
        "TICKETSERIALNUMBER": pd.Series(dtype="string"),
        "GROUPHOSTID": pd.Series(dtype="string"),
        "GROUPUNITSEQUENCE": pd.Series(dtype="int"),
        "BETTYPE": pd.Series(dtype="int")
    })

    query = f"""
    select * from ztubt_placedbettransactionheader
    where createddate between '{vfromdatetimeIGT_UTC}'   and '{vtodatetimeIGT_UTC}'
    """

    df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)

    query = f"""
    select * from ztubt_toto_placedbettransactionlineitem
    where tranheaderid in (select tranheaderid from ztubt_placedbettransactionheader
    where createddate between '{vfromdatetimeIGT_UTC}'   and '{vtodatetimeIGT_UTC}')
    """
    df_ztubt_toto_placedbettransactionlineitem = pd.read_sql(query, connection)

    # add prefix
    df_ztubt_placedbettransactionheader = df_ztubt_placedbettransactionheader.add_prefix("PB.")
    df_ztubt_toto_placedbettransactionlineitem = df_ztubt_toto_placedbettransactionlineitem.add_prefix("PBT.")

    df_temp = df_ztubt_placedbettransactionheader.merge(
        df_ztubt_toto_placedbettransactionlineitem,
        left_on="PB.TRANHEADERID",
        right_on="PBT.TRANHEADERID",
        how="inner"
    )

    # Filter: GROUPUNITSEQUENCE is not null  and createddate between vfromdatetimeIGT_UTC and vtodatetimeIGT_UTC
    df_temp = df_temp[
        (df_temp["PBT.GROUPUNITSEQUENCE"].notnull()) &
        ((df_temp["PB.CREATEDDATE"] >= vfromdatetimeIGT_UTC) &
        (df_temp["PB.CREATEDDATE"] < vtodatetimeIGT_UTC))
    ]

    df_temp["BETTYPE"] = np.where(
        df_temp["PBT.BETTYPEID"].isin([item[0] for item in v_totomatchBettypes]),
        2,  # TOTO MATCH
        1   # TOTO
    )
    df_ubt_temp_grouptoto = df_temp[[
        "PB.TICKETSERIALNUMBER",
        "PBT.GROUPHOSTID",
        "PBT.GROUPUNITSEQUENCE",
        "BETTYPE"
    ]]\
    .drop_duplicates(subset=["PB.TICKETSERIALNUMBER", "PBT.GROUPHOSTID", "PBT.GROUPUNITSEQUENCE"], ignore_index=True)

    df_ubt_temp_grouptoto.rename(columns={
        "PB.TICKETSERIALNUMBER": "TICKETSERIALNUMBER",
        "PBT.GROUPHOSTID": "GROUPHOSTID",
        "PBT.GROUPUNITSEQUENCE": "GROUPUNITSEQUENCE"
    }, inplace=True)

    df_ubt_temp_grouptoto = df_ubt_temp_grouptoto[['TICKETSERIALNUMBER', 'GROUPHOSTID', 'GROUPUNITSEQUENCE', 'BETTYPE']]

    return df_ubt_temp_grouptoto

def ubt_temp_CancelledBetTicketState(vfromdateIGT,
                                     vtodateIGT
):
    query = f"""
    select cb.cartid,cb.terdisplayid,cb.ticketserialnumber,cb.tranheaderid,cb.cancelleddate ,cb.cancelledamout,cb.prodid
	from  {schema}.ztubt_cancelledbetticket  cb
	inner join {schema}.ztubt_cancelledbetticketlifecyclestate cbt
   on cb.tranheaderid = cbt.tranheaderid and cbt.betstatetypeid = 'CB06'
	where cb.cancelleddate between '{vfromdateIGT}' and '{vtodateIGT}'
    """

    df_ubt_temp_CancelledBetTicketState = pd.read_sql(query, connection)

    return df_ubt_temp_CancelledBetTicketState

def ubt_temp_SalesGroupToto(df_ubt_temp_terminal,
                            df_ubt_temp_location,
                            df_ubt_temp_iTOTO,
                            df_ubt_temp_TmpTicketByWageAndSales,
                            df_ubt_temp_CancelledBetTicketState,
                            vfromdatetimeIGT_UTC,
                            vtodatetimeIGT_UTC
):


# create temp table if not exists ubt_temp_SalesGroupToto
# (
# 	terdisplayid varchar(200),
# 	prodid int4,
# 	locdisplayid varchar(200),
# 	bettype int4,
# 	wagertotal int4,--(8)
# 	canceltotal int4,--(8)
# 	wageramount numeric(32, 11), --wager of each product
# 	salesamount numeric(32, 11)  --Sales of each product (Wager * Sales Factor)
# );



    df_ubt_temp_SalesGroupToto = pd.DataFrame({
        "TERDISPLAYID": pd.Series(dtype="string"),
        "PRODID": pd.Series(dtype="int"),
        "LOCDISPLAYID": pd.Series(dtype="string"),
        "BETTYPE": pd.Series(dtype="int"),
        "WAGERTOTAL": pd.Series(dtype="int"),
        "CANCELTOTAL": pd.Series(dtype="int"),
        "WAGERAMOUNT": pd.Series(dtype="float"),
        "SALESAMOUNT": pd.Series(dtype="float"),
    })


    # select ter.terdisplayid,
	# 3 as prodid,
	# loc.locdisplayid as locdisplayid,
	# fin.bettype as bettype,
	# count(distinct fin.ticketserialnumber)	as wagertotal,
	# count(distinct can.ticketserialnumber)	as canceltotal,
	# sum(coalesce(fin.wager,0))	as wageramount,
	# sum(coalesce(fin.sales,0))	as salesamount
	# from	ztubt_placedbettransactionheader pbth
	# inner join ubt_temp_terminal ter  on pbth.terdisplayid=ter.terdisplayid
	# inner join ubt_temp_location loc  on ter.locid=loc.locid
	# inner join
	# 	(
	# 	select gat.ticketserialnumber, gat.wager, case when can.ticketserialnumber is null then gat.sales else 0 end as sales, itoto.bettype
	# 	from ubt_temp_itoto itoto
	# 	inner join ubt_temp_tmpticketbywageandsales gat on itoto.ticketserialnumber = gat.ticketserialnumber
	# 	left join ubt_temp_cancelledbetticketstate can on gat.ticketserialnumber = can.ticketserialnumber
	# 	where itoto.groupunitsequence = 1 --group toto
	# 	)fin on pbth.ticketserialnumber = fin.ticketserialnumber

	# 	left join
	# 	(
	# 	select gat.ticketserialnumber, gat.wager, gat.sales, itoto.bettype
	# 	from ubt_temp_itoto itoto
	# 	inner join ubt_temp_tmpticketbywageandsales gat on itoto.ticketserialnumber = gat.ticketserialnumber
	# 	inner join ubt_temp_cancelledbetticketstate cb on gat.ticketserialnumber = cb.ticketserialnumber
	# 	where itoto.groupunitsequence = 1 --group toto
	# 	)can on pbth.ticketserialnumber = can.ticketserialnumber

	# where
	# pbth.prodid = 3
	# and (pbth.createddate between vfromdatetimeIGT_UTC   and vtodatetimeIGT_UTC )
	# group by  ter.terdisplayid,loc.locdisplayid, fin.bettype
    query = f"""
    select * from ztubt_placedbettransactionheader pbth
    where
    pbth.prodid = 3
    and (pbth.createddate between '{vfromdatetimeIGT_UTC}'   and '{vtodatetimeIGT_UTC}')
    """
    df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)

    # add prefix
    df_ztubt_placedbettransactionheader = df_ztubt_placedbettransactionheader.add_prefix("PBTH.")
    df_ubt_temp_terminal = df_ubt_temp_terminal.add_prefix("TER.")
    df_ubt_temp_location = df_ubt_temp_location.add_prefix("LOC.")
    df_ubt_temp_iTOTO = df_ubt_temp_iTOTO.add_prefix("ITOTO.")
    df_ubt_temp_TmpTicketByWageAndSales = df_ubt_temp_TmpTicketByWageAndSales.add_prefix("GAT.")
    df_ubt_temp_CancelledBetTicketState = df_ubt_temp_CancelledBetTicketState.add_prefix("CAN.")



    # 1st part: fin
    df_fin = df_ubt_temp_iTOTO.merge(
        df_ubt_temp_TmpTicketByWageAndSales,
        left_on="ITOTO.TICKETSERIALNUMBER",
        right_on="GAT.TICKETSERIALNUMBER",
        how="inner"
    ).merge(
        df_ubt_temp_CancelledBetTicketState,
        left_on="GAT.TICKETSERIALNUMBER",
        right_on="CAN.TICKETSERIALNUMBER",
        how="left"
    )
    df_fin = df_fin[
        (df_fin["ITOTO.GROUPUNITSEQUENCE"] == 1)
    ]
    df_fin = df_fin.assign(
        SALES =lambda x: np.where(
            x["CAN.TICKETSERIALNUMBER"].isnull(),
            x["GAT.SALES"],
            0
        )
    )
    df_fin.rename(columns={
        "GAT.TICKETSERIALNUMBER": "FIN.TICKETSERIALNUMBER",
        "GAT.WAGER": "FIN.WAGER",
        "SALES": "FIN.SALES",
        "ITOTO.BETTYPE": "FIN.BETTYPE"
    }, inplace=True)

    df_fin = df_fin[[
        "FIN.TICKETSERIALNUMBER",
        "FIN.WAGER",
        "FIN.SALES",
        "FIN.BETTYPE"
    ]]


    # 2nd part: can
    df_can = df_ubt_temp_iTOTO.merge(
        df_ubt_temp_TmpTicketByWageAndSales,
        left_on="ITOTO.TICKETSERIALNUMBER",
        right_on="GAT.TICKETSERIALNUMBER",
        how="inner"
    ).merge(
        df_ubt_temp_CancelledBetTicketState,
        left_on="GAT.TICKETSERIALNUMBER",
        right_on="CAN.TICKETSERIALNUMBER",
        how="inner"
    )
    df_can = df_can[
        (df_can["ITOTO.GROUPUNITSEQUENCE"] == 1)
    ]
    # DROP COLUMN CAN.TICKETSERIALNUMBER
    df_can = df_can.drop(columns=["CAN.TICKETSERIALNUMBER"])

    df_can.rename(columns={
        "GAT.TICKETSERIALNUMBER": "CAN.TICKETSERIALNUMBER",
        "GAT.WAGER": "CAN.WAGER",
        "GAT.SALES": "CAN.SALES",
        "ITOTO.BETTYPE": "CAN.BETTYPE"
    }, inplace=True)

    df_can = df_can[[
        "CAN.TICKETSERIALNUMBER",
        "CAN.WAGER",
        "CAN.SALES",
        "CAN.BETTYPE"
    ]]

    # Main Merge
    df_main = df_ztubt_placedbettransactionheader.merge(
        df_ubt_temp_terminal,
        left_on="PBTH.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_fin,
        left_on="PBTH.TICKETSERIALNUMBER",
        right_on="FIN.TICKETSERIALNUMBER",
        how="inner"
    ).merge(
        df_can,
        left_on="PBTH.TICKETSERIALNUMBER",
        right_on="CAN.TICKETSERIALNUMBER",
        how="left"
    )


    # where
    df_ubt_temp_SalesGroupToto = df_main.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "FIN.BETTYPE"
        ],
        as_index=False
    ).agg(
        WAGERTOTAL = ("FIN.TICKETSERIALNUMBER", "nunique"),
        CANCELTOTAL = ("CAN.TICKETSERIALNUMBER", "nunique"),
        WAGERAMOUNT = ("FIN.WAGER", "sum"),
        SALESAMOUNT = ("FIN.SALES", "sum")
    ).assign(
        CANCELTOTAL = lambda x: x["CANCELTOTAL"].fillna(0).astype(int),
        WAGERAMOUNT = lambda x: x["WAGERAMOUNT"].fillna(0.0),
        SALESAMOUNT = lambda x: x["SALESAMOUNT"].fillna(0.0),
        PRODID = 3
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "FIN.BETTYPE": "BETTYPE"
    })[[
        "TERDISPLAYID",
        "PRODID",
        "LOCDISPLAYID",
        "BETTYPE",
        "WAGERTOTAL",
        "CANCELTOTAL",
        "WAGERAMOUNT",
        "SALESAMOUNT"
    ]]


    return df_ubt_temp_SalesGroupToto

def ubt_temp_salestoto(df_ubt_temp_terminal,
                        df_ubt_temp_location,
                        df_ubt_temp_iTOTO,
                        df_ubt_temp_TmpTicketByWageAndSales,
                        df_ubt_temp_CancelledBetTicketState,
                        vfromdatetimeIGT_UTC,
                        vtodatetimeIGT_UTC):

    # create temp table if not exists ubt_temp_salestoto
	# (
	# terdisplayid varchar(200),
	# locdisplayid varchar(200),
	# prodid int4,
	# bettype int4,
	# wagertotal int4,--(8)
	# canceltotal int4,--(8)
	# wageramount numeric(32, 11), --wager of each product
	# salesamount numeric(32, 11)  --sales of each product (wager * sales factor)
	# );

    df_ubt_temp_salestoto = pd.DataFrame({
        "TERDISPLAYID": pd.Series(dtype="string"),
        "LOCDISPLAYID": pd.Series(dtype="string"),
        "PRODID": pd.Series(dtype="int"),
        "BETTYPE": pd.Series(dtype="int"),
        "WAGERTOTAL": pd.Series(dtype="int"),
        "CANCELTOTAL": pd.Series(dtype="int"),
        "WAGERAMOUNT": pd.Series(dtype="float"),
        "SALESAMOUNT": pd.Series(dtype="float"),
    })


# insert into ubt_temp_salestoto(terdisplayid, locdisplayid,prodid, bettype, wagertotal, canceltotal, wageramount, salesamount)
# 	select ter.terdisplayid		as terdisplayid,
# 	loc.locdisplayid			as locdisplayid,
# 	3		as prodid,
# 	1 as bettype,
# 	count(distinct fin.ticketserialnumber)	as wagertotal, --(1)count(1)		as totalcount,
# 	count(distinct can.ticketserialnumber)	as canceltotal,--(8)
# 	sum(coalesce(fin.wager,0))	as wageramount,
# 	sum(coalesce(fin.sales,0))	as salesamount
# 	from	{schema}.ztubt_placedbettransactionheader pbth
# 	inner join ubt_temp_terminal ter  on pbth.terdisplayid=ter.terdisplayid
# 	inner join ubt_temp_location loc  on ter.locid=loc.locid
# 	inner join
# 		(
# 		select gat.ticketserialnumber, gat.wager, case when can.ticketserialnumber is null then gat.sales else 0 end as sales
# 		from ubt_temp_itoto itoto
# 		inner join ubt_temp_tmpticketbywageandsales gat on itoto.ticketserialnumber = gat.ticketserialnumber
# 		left join ubt_temp_cancelledbetticketstate can on gat.ticketserialnumber = can.ticketserialnumber
# 		where itoto.itotoindicator = true --itoto
# 		)fin on pbth.ticketserialnumber = fin.ticketserialnumber
# 	left join
# 		(
# 		select gat.ticketserialnumber, gat.wager, gat.sales
# 		from ubt_temp_itoto itoto
# 		inner join ubt_temp_tmpticketbywageandsales gat on itoto.ticketserialnumber = gat.ticketserialnumber
# 		inner join ubt_temp_cancelledbetticketstate cb on gat.ticketserialnumber = cb.ticketserialnumber
# 		where itoto.itotoindicator = true --itoto
# 		)can on pbth.ticketserialnumber = can.ticketserialnumber

# 	where pbth.prodid = 3
# 	and (pbth.createddate between vfromdatetimeIGT_UTC   and vtodatetimeIGT_UTC )
# 	group by ter.terdisplayid,loc.locdisplayid

    query = f"""
    select * from ztubt_placedbettransactionheader pbth
    where pbth.prodid = 3
    and (pbth.createddate between '{vfromdatetimeIGT_UTC}'   and '{vtodatetimeIGT_UTC}')
    """
    df_ztubt_placedbettransactionheader = pd.read_sql(query, connection)

    # add prefix
    df_ztubt_placedbettransactionheader = df_ztubt_placedbettransactionheader.add_prefix("PBTH.")
    df_ubt_temp_terminal = df_ubt_temp_terminal.add_prefix("TER.")
    df_ubt_temp_location = df_ubt_temp_location.add_prefix("LOC.")
    df_ubt_temp_iTOTO = df_ubt_temp_iTOTO.add_prefix("ITOTO.")
    df_ubt_temp_TmpTicketByWageAndSales = df_ubt_temp_TmpTicketByWageAndSales.add_prefix("GAT.")
    df_ubt_temp_CancelledBetTicketState = df_ubt_temp_CancelledBetTicketState.add_prefix("CAN.")

    # 1st part: fin
    df_fin = df_ubt_temp_iTOTO.merge(
        df_ubt_temp_TmpTicketByWageAndSales,
        left_on="ITOTO.TICKETSERIALNUMBER",
        right_on="GAT.TICKETSERIALNUMBER",
        how="inner"
    ).merge(
        df_ubt_temp_CancelledBetTicketState,
        left_on="GAT.TICKETSERIALNUMBER",
        right_on="CAN.TICKETSERIALNUMBER",
        how="left"
    )
    df_fin = df_fin[
        (df_fin["ITOTO.ITOTOINDICATOR"] == True)
    ]
    df_fin = df_fin.assign(
        SALES =lambda x: np.where(
            x["CAN.TICKETSERIALNUMBER"].isnull(),
            x["GAT.SALES"],
            0
        )
    )
    df_fin.rename(columns={
        "GAT.TICKETSERIALNUMBER": "FIN.TICKETSERIALNUMBER",
        "GAT.WAGER": "FIN.WAGER",
        "SALES": "FIN.SALES"
    }, inplace=True)
    df_fin = df_fin[[
        "FIN.TICKETSERIALNUMBER",
        "FIN.WAGER",
        "FIN.SALES"
    ]]
    # 2nd part: can

    df_can = df_ubt_temp_iTOTO.merge(
        df_ubt_temp_TmpTicketByWageAndSales,
        left_on="ITOTO.TICKETSERIALNUMBER",
        right_on="GAT.TICKETSERIALNUMBER",
        how="inner"
    ).merge(
        df_ubt_temp_CancelledBetTicketState,
        left_on="GAT.TICKETSERIALNUMBER",
        right_on="CAN.TICKETSERIALNUMBER",
        how="inner"
    )
    df_can = df_can[
        (df_can["ITOTO.ITOTOINDICATOR"] == True)
    ]
    # DROP COLUMN CAN.TICKETSERIALNUMBER
    df_can = df_can.drop(columns=["CAN.TICKETSERIALNUMBER"])
    df_can.rename(columns={
        "GAT.TICKETSERIALNUMBER": "CAN.TICKETSERIALNUMBER",
        "GAT.WAGER": "CAN.WAGER",
        "GAT.SALES": "CAN.SALES"
    }, inplace=True)
    df_can = df_can[[
        "CAN.TICKETSERIALNUMBER",
        "CAN.WAGER",
        "CAN.SALES"
    ]]
    # Main Merge
    df_main = df_ztubt_placedbettransactionheader.merge(
        df_ubt_temp_terminal,
        left_on="PBTH.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_fin,
        left_on="PBTH.TICKETSERIALNUMBER",
        right_on="FIN.TICKETSERIALNUMBER",
        how="inner"
    ).merge(
        df_can,
        left_on="PBTH.TICKETSERIALNUMBER",
        right_on="CAN.TICKETSERIALNUMBER",
        how="left"
    )

    # where
    df_ubt_temp_salestoto = df_main.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        WAGERTOTAL = ("FIN.TICKETSERIALNUMBER", "nunique"),
        CANCELTOTAL = ("CAN.TICKETSERIALNUMBER", "nunique"),
        WAGERAMOUNT = ("FIN.WAGER", "sum"),
        SALESAMOUNT = ("FIN.SALES", "sum")
    ).assign(
        CANCELTOTAL = lambda x: x["CANCELTOTAL"].fillna(0).astype(int),
        WAGERAMOUNT = lambda x: x["WAGERAMOUNT"].fillna(0.0),
        SALESAMOUNT = lambda x: x["SALESAMOUNT"].fillna(0.0),
        PRODID = 3,
        BETTYPE = 1
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "LOCDISPLAYID",
        "PRODID",
        "BETTYPE",
        "WAGERTOTAL",
        "CANCELTOTAL",
        "WAGERAMOUNT",
        "SALESAMOUNT"
    ]]


    return df_ubt_temp_salestoto


def ubt_temp_salesfactorconfig():
    query = f"""

    select a.prodid, a.salesfactor  from  ztubt_salescomconfig  a
	where commissiontype=1  and a.isdeleted = false and prodid in (2,3,4)

    """

    df_ubt_temp_salesfactorconfig = pd.read_sql(query, connection)

    return df_ubt_temp_salesfactorconfig



def ubt_temp_tmpterlocprdsalesamt(df_ubt_temp_transamountdetaildata,
                                  df_ubt_temp_terminal,
                                  df_ubt_temp_product,
                                  df_ubt_temp_location,
                                  df_ubt_temp_cancelledbetticketstate,
                                  df_ubt_temp_itoto,
                                  df_ubt_temp_SalesGroupToto,
                                  df_ubt_temp_grouptoto,
                                  df_ubt_temp_resultcashlessinterminal,
                                  df_ubt_temp_tmpticketbywageandsales,
                                  df_ubt_temp_salestoto,
                                  df_ubt_temp_sales_scandsr,
                                  df_ubt_temp_salesfactorconfig,





                                  vfromdateIGT,
                                  vtodateIGT,
                                  vfromdatetimeIGT_UTC,
                                  vtodatetimeIGT_UTC,
                                  vfromdatetimeOB_UTC,
                                  vtodatetimeOB_UTC,
                                  vfromdatetimeBMCS_UTC,
                                  vtodatetimeBMCS_UTC,
                                    vbusinessdate,
                                  vGSTRate
):

    df_ubt_temp_tmpterlocprdsalesamt = pd.DataFrame({
        "TERDISPLAYID": pd.Series(dtype="string"),
        "TRANSACTIONDATE": pd.Series(dtype="string"),
        "PRODID": pd.Series(dtype="int"),
        "LOCDISPLAYID": pd.Series(dtype="string"),
        "SALES_TYPE": pd.Series(dtype="string"),
        "TOTALCOUNT": pd.Series(dtype="int"),
        "AMOUNT": pd.Series(dtype="float"),
        "SUB_PROD": pd.Series(dtype="string"),
    })

    ####################################################################################


    # Ađd prefix
    df_ubt_temp_transamountdetaildata = df_ubt_temp_transamountdetaildata.add_prefix("TRANS.")
    df_ubt_temp_terminal = df_ubt_temp_terminal.add_prefix("TER.")
    df_ubt_temp_location = df_ubt_temp_location.add_prefix("LOC.")
    df_ubt_temp_product = df_ubt_temp_product.add_prefix("PRD.")
    df_ubt_temp_cancelledbetticketstate = df_ubt_temp_cancelledbetticketstate.add_prefix("CB.")
    df_ubt_temp_itoto = df_ubt_temp_itoto.add_prefix("ITOTO.")
    df_ubt_temp_SalesGroupToto = df_ubt_temp_SalesGroupToto.add_prefix("SALESGROUPTOTO.")
    df_ubt_temp_grouptoto = df_ubt_temp_grouptoto.add_prefix("GROUPTOTO.")
    df_ubt_temp_resultcashlessinterminal = df_ubt_temp_resultcashlessinterminal.add_prefix("RCIT.")
    df_ubt_temp_tmpticketbywageandsales = df_ubt_temp_tmpticketbywageandsales.add_prefix("TTBWS.")
    df_ubt_temp_salestoto = df_ubt_temp_salestoto.add_prefix("SALESTOTO.")
    df_ubt_temp_sales_scandsr = df_ubt_temp_sales_scandsr.add_prefix("SCANDSR.")
    df_ubt_temp_salesfactorconfig = df_ubt_temp_salesfactorconfig.add_prefix("SFC.")


# select
# ter.terdisplayid	as terdisplayid,
# vbusinessdate	as transactiondate,
# prd.prodid	as prodid,
# loc.locdisplayid as locdisplayid,
# '1'	as sales_type,
# sum(coalesce(a.ct,0))	as totalcount,
# sum(coalesce(amount,0))as amount,
# a.productname as sub_prod
# from ubt_temp_transamountdetaildata a
# inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
# inner join ubt_temp_location loc on ter.locid = loc.locid
# inner join ubt_temp_product prd on a.productname = prd.prodname
# 	where flag='COL' and transtype != 'OO' and prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
# 	group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname


    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_ubt_temp_product,
        left_on="TRANS.PRODUCTNAME",
        right_on="PRD.PRODNAME",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'COL') &
        (df_temp["TRANS.TRANSTYPE"] != 'OO') &
        (df_temp["PRD.PRODID"].isin([1,2,3,4,5]))
    ]
    df_ubt_temp_tmpterlocprdsalesamt = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "PRD.PRODID",
            "TRANS.PRODUCTNAME"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", "sum")
    ).assign(
        SALES_TYPE = '1',
        TRANSACTIONDATE = vbusinessdate,
        SUB_PROD = lambda x: x["TRANS.PRODUCTNAME"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "PRD.PRODID": "PRODID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]

    # --offline orders - wager

	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate	as transactiondate,
	# case when a.productname <> 'Gate Admission' then 31
	# 	 when a.productname = 'Gate Admission' then 99 end as prodid,
	# loc.locdisplayid as locdisplayid,
	# '1'	as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# sum(coalesce(amount,0))as amount,
	# '' as sub_prod
	# from ubt_temp_transamountdetaildata a
	# inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# where flag='COL' and transtype = 'OO'
	# group by loc.locdisplayid,ter.terdisplayid,case when a.productname <> 'Gate Admission' then 31
	# 	 											when a.productname = 'Gate Admission' then 99 end
    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'COL') &
        (df_temp["TRANS.TRANSTYPE"] == 'OO')
    ]

    df_temp = df_temp.assign(
        PRODID = lambda x: np.where(
            x["TRANS.PRODUCTNAME"] != 'Gate Admission',
            31,
            99
        )
    )

    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "PRODID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", "sum")
    ).assign(
        SALES_TYPE = '1',
        TRANSACTIONDATE = vbusinessdate,
        SUB_PROD = ''
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # -- Gate Admission - Sales

	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate	as transactiondate,
	# 99 as prodid,
	# loc.locdisplayid as locdisplayid,
	# '116'	as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# --sum(coalesce(amount,0)*(1-vGSTRate))as amount,
	# sum(coalesce(amount,0)*(100 / (100 + vGSTRate * 100)))as amount,
	# '' as sub_prod
	# from ubt_temp_transamountdetaildata a
	# inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# where flag='COL' and transtype = 'OO' and a.productname = 'Gate Admission'
	# group by loc.locdisplayid,ter.terdisplayid

    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'COL') &
        (df_temp["TRANS.TRANSTYPE"] == 'OO') &
        (df_temp["TRANS.PRODUCTNAME"] == 'Gate Admission')
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", lambda x: (x * (100 / (100 + vGSTRate * 100))).sum())
    ).assign(
        SALES_TYPE = '116',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 99,
        SUB_PROD = ''
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]

    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # ----2	AT_CANCELAMT	Yes	cancel amount

    # select	ter.terdisplayid	as terdisplayid,
    # vbusinessdate	as transactiondate,
    # prd.prodid	as prodid,
    # loc.locdisplayid as locdisplayid,
    # '2'	as sales_type,
    # sum(coalesce(a.ct,0))	as totalcount,
    # (sum(coalesce(amount,0)) * -1)as amount,
    # a.productname as sub_prod
    # from ubt_temp_transamountdetaildata a
    # inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
    # inner join ubt_temp_location loc on ter.locid = loc.locid
    # inner join ubt_temp_product prd on a.productname = prd.prodname
    # 	where flag='CAN' and  prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
    # 	group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname
    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_ubt_temp_product,
        left_on="TRANS.PRODUCTNAME",
        right_on="PRD.PRODNAME",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'CAN') &
        (df_temp["PRD.PRODID"].isin([1,2,3,4,5]))
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "PRD.PRODID",
            "TRANS.PRODUCTNAME"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", lambda x: (x * -1).sum())
    ).assign(
        SALES_TYPE = '2',
        TRANSACTIONDATE = vbusinessdate,
        SUB_PROD = lambda x: x["TRANS.PRODUCTNAME"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "PRD.PRODID": "PRODID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # ---3	AT_VALIDAMT	Yes	validation amount
	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate	as transactiondate,
	# prd.prodid	as prodid,
	# loc.locdisplayid as locdisplayid,
	# '3'	as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# (sum(coalesce(amount,0)) * -1)as amount,
	# a.productname as sub_prod
	# from ubt_temp_transamountdetaildata a
	# inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# inner join ubt_temp_product prd on a.productname = prd.prodname
	# 	where flag='PAY' and  prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
	# 	group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname
    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_ubt_temp_product,
        left_on="TRANS.PRODUCTNAME",
        right_on="PRD.PRODNAME",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'PAY') &
        (df_temp["PRD.PRODID"].isin([1,2,3,4,5]))
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "PRD.PRODID",
            "TRANS.PRODUCTNAME"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", lambda x: (x * -1).sum())
    ).assign(
        SALES_TYPE = '3',
        TRANSACTIONDATE = vbusinessdate,
        SUB_PROD = lambda x: x["TRANS.PRODUCTNAME"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "PRD.PRODID": "PRODID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # ---4	Rebate Amount
	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate	as transactiondate,
	# prd.prodid	as prodid,
	# loc.locdisplayid as locdisplayid,
	# '4'	as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# (sum(coalesce(amount,0)) * -1)as amount,
	# a.productname as sub_prod
	# from ubt_temp_transamountdetaildata a
	# inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# inner join ubt_temp_product prd on a.productname = prd.prodname
	# 	where flag='RBT' and  prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
	# 	group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname
    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_ubt_temp_product,
        left_on="TRANS.PRODUCTNAME",
        right_on="PRD.PRODNAME",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'RBT') &
        (df_temp["PRD.PRODID"].isin([1,2,3,4,5]))
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "PRD.PRODID",
            "TRANS.PRODUCTNAME"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", lambda x: (x * -1).sum())
    ).assign(
        SALES_TYPE = '4',
        TRANSACTIONDATE = vbusinessdate,
        SUB_PROD = lambda x: x["TRANS.PRODUCTNAME"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "PRD.PRODID": "PRODID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # --61	Refund Amount--(4)(5)

	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate	as transactiondate,
	# prd.prodid	as prodid,
	# loc.locdisplayid as locdisplayid,
	# '61'	as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# (sum(coalesce(amount,0)) * -1)as amount,
	# a.productname as sub_prod
	# from ubt_temp_transamountdetaildata a
	# inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# inner join ubt_temp_product prd on a.productname = prd.prodname
	# 	where flag='RFD' AND TransType != 'OO' and  prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
	# 	group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname
    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_ubt_temp_product,
        left_on="TRANS.PRODUCTNAME",
        right_on="PRD.PRODNAME",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'RFD') &
        (df_temp["TRANS.TRANSTYPE"] != 'OO') &
        (df_temp["PRD.PRODID"].isin([1,2,3,4,5]))
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "PRD.PRODID",
            "TRANS.PRODUCTNAME"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", lambda x: (x * -1).sum())
    ).assign(
        SALES_TYPE = '61',
        TRANSACTIONDATE = vbusinessdate,
        SUB_PROD = lambda x: x["TRANS.PRODUCTNAME"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "PRD.PRODID": "PRODID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # --offline orders - refund
	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate	as transactiondate,
	# 31	as prodid,
	# loc.locdisplayid as locdisplayid,
	# '61'	as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# (sum(coalesce(amount,0)) * -1)as amount,
	# '' as sub_prod
	# from ubt_temp_transamountdetaildata a
	# inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# 	where flag='RFD' AND TransType = 'OO'
	# 	group by loc.locdisplayid,ter.terdisplayid
    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'RFD') &
        (df_temp["TRANS.TRANSTYPE"] == 'OO')
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", lambda x: (x * -1).sum())
    ).assign(
        SALES_TYPE = '61',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 31,
        SUB_PROD = ''
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )


    # --6	AT_TAXAMT 	Yes	tax amount
	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate	as transactiondate,
	# prd.prodid	as prodid,
	# loc.locdisplayid as locdisplayid,
	# '6'	as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# (sum(coalesce(amount,0)) * -1)as amount,
	# a.productname as sub_prod
	# from ubt_temp_transamountdetaildata a
	# inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# inner join ubt_temp_product prd on a.productname = prd.prodname
	# 	where flag='GST' AND prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
	# 	group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname
    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_ubt_temp_product,
        left_on="TRANS.PRODUCTNAME",
        right_on="PRD.PRODNAME",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'GST') &
        (df_temp["PRD.PRODID"].isin([1,2,3,4,5]))
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "PRD.PRODID",
            "TRANS.PRODUCTNAME"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", lambda x: (x * -1).sum())
    ).assign(
        SALES_TYPE = '6',
        TRANSACTIONDATE = vbusinessdate,
        SUB_PROD = lambda x: x["TRANS.PRODUCTNAME"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "PRD.PRODID": "PRODID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )


    # ---7	AT_COMMAMT	Yes	commission amount
	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate	as transactiondate,
	# prd.prodid	as prodid,
	# loc.locdisplayid as locdisplayid,
	# '7'	as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# (sum(coalesce(amount,0)) * -1)as amount,
	# a.productname as sub_prod
	# from ubt_temp_transamountdetaildata a
	# inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# inner join ubt_temp_product prd on a.productname = prd.prodname
	# 	where flag='SAL' AND prd.prodid in (1,2,3,4,5) --(15)add horse racing, sports --(8)productname in ('4d lottery','singapore sweep','toto')
	# 	group by loc.locdisplayid,ter.terdisplayid,prd.prodid, a.productname

    df_temp = df_ubt_temp_transamountdetaildata.merge(
        df_ubt_temp_terminal,
        left_on="TRANS.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_ubt_temp_product,
        left_on="TRANS.PRODUCTNAME",
        right_on="PRD.PRODNAME",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["TRANS.FLAG"] == 'SAL') &
        (df_temp["PRD.PRODID"].isin([1,2,3,4,5]))
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "PRD.PRODID",
            "TRANS.PRODUCTNAME"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TRANS.CT", "sum"),
        AMOUNT = ("TRANS.AMOUNT", lambda x: (x * -1).sum())
    ).assign(
        SALES_TYPE = '7',
        TRANSACTIONDATE = vbusinessdate,
        SUB_PROD = lambda x: x["TRANS.PRODUCTNAME"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "PRD.PRODID": "PRODID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

	# ------16. AT_FRACVALAMT  Show fractional validation amounts [Validation amounts for GROUP TOTO - This is a subset of Sales Type  3]

	# select	ter.terdisplayid				as terdisplayid,
	# vbusinessdate transactiondate,
	# 3		as prodid,
	# loc.locdisplayid	as locdisplayid,
	# '16'		as sales_type,
	# count(distinct vb.tranheaderid)	as totalcount,
	# sum(coalesce(vb.winningamount,0))	as totalamount,
	# case when ph.bettypeid != ALL(v_totomatchBettypes) then 'TOTO'
	# 	else 'TOTO MATCH' end as sub_prod
	# from --(1)
	# (
	# 	select vb.tranheaderid,vb.winningamount,vb.cartid,vb.terdisplayid,vb.createdvalidationdate
	# 	from {schema}.ztubt_validatedbetticket  vb --(16)
	# 	inner join {schema}.ztubt_validatedbetticketlifecyclestate  vbt
	# 	on vb.tranheaderid = vbt.tranheaderid and vbt.betstatetypeid = 'VB06'
	# 	where vb.winningamount != 0 and vb.winningamount is not null --(8)
	# 	and vb.createdvalidationdate >= vfromdatetimeIGT_UTC and vb.createdvalidationdate < vtodatetimeIGT_UTC --(16)
	# )vb
	# inner join
	# (
	# 	select distinct tranheaderid, grouphostid, groupunitsequence, bettypeid from {schema}.ztubt_toto_placedbettransactionlineitem
	# )ph on vb.tranheaderid = ph.tranheaderid --(16)
	# inner join ubt_temp_terminal ter on vb.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# where ph.groupunitsequence is not null --(16)
	# group by loc.locdisplayid,ter.terdisplayid,
	# case when ph.bettypeid != ALL(v_totomatchBettypes) then 'TOTO'
	# 	else 'TOTO MATCH' end

    query = f"""
    select vb.tranheaderid,vb.winningamount,vb.cartid,vb.terdisplayid,vb.createdvalidationdate
		from {schema}.ztubt_validatedbetticket  vb --(16)
		inner join {schema}.ztubt_validatedbetticketlifecyclestate  vbt
		on vb.tranheaderid = vbt.tranheaderid and vbt.betstatetypeid = 'VB06'
		where vb.winningamount != 0 and vb.winningamount is not null --(8)
		and vb.createdvalidationdate >= '{vfromdatetimeIGT_UTC}' and vb.createdvalidationdate < '{vtodatetimeIGT_UTC}' --(16)
    """
    df_vb = pd.read_sql(query, connection)

    query = f"""
    select distinct tranheaderid, grouphostid, groupunitsequence, bettypeid from {schema}.ztubt_toto_placedbettransactionlineitem
    """
    df_ph = pd.read_sql(query, connection)

    # add prefix
    df_vb = df_vb.add_prefix("VB.")
    df_ph = df_ph.add_prefix("PH.")

    df_temp = df_vb.merge(
        df_ph,
        left_on="VB.TRANHEADERID",
        right_on="PH.TRANHEADERID",
        how="inner"
    ).merge(
        df_ubt_temp_terminal,
        left_on="VB.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    )

    df_temp = df_temp[
        df_temp["PH.GROUPUNITSEQUENCE"].notnull()
    ]

    df_temp = df_temp.assign(
        SUB_PROD = lambda x: np.where(
            x["PH.BETTYPEID"].isin(v_totomatchBettypes),
            'TOTO MATCH',
            'TOTO'
        )
    )

    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "SUB_PROD"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("VB.TRANHEADERID", "nunique"),
        AMOUNT = ("VB.WINNINGAMOUNT", "sum")
    ).assign(
        SALES_TYPE = '16',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 3
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]

    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )


    # --52. House syndicate sales amount - [iTOTO sales – this is a subset of sales type 1]
	# --TOTO
	# select	s.terdisplayid			as terdisplayid,
	# vbusinessdate	as transactiondate,
	# s.prodid	as prodid,
	# s.locdisplayid as locdisplayid,
	# '52' as sales_type,
	# sum(coalesce(s.wagertotal,0))		as totalcount,
	# sum(coalesce(s.wageramount,0))	as totalamount,
	# 'TOTO' as sub_prod
	# from ubt_temp_salestoto s
	# group by s.terdisplayid, s.prodid, s.locdisplayid

    df_temp = df_ubt_temp_salestoto.groupby(
        [
            "SALESTOTO.TERDISPLAYID",
            "SALESTOTO.PRODID",
            "SALESTOTO.LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("SALESTOTO.WAGERTOTAL", "sum"),
        AMOUNT = ("SALESTOTO.WAGERAMOUNT", "sum")
    ).assign(
        SALES_TYPE = '52',
        TRANSACTIONDATE = vbusinessdate,
        SUB_PROD = 'TOTO',
        PRODID = lambda x: x["SALESTOTO.PRODID"],
        LOCDISPLAYID = lambda x: x["SALESTOTO.LOCDISPLAYID"]
    ).rename(columns={
        "SALESTOTO.TERDISPLAYID": "TERDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

        # --25. House syndicate sales amount by sales factor [iTOTO Sales by Sales Factor – This is a subset of Sales Type 116]
        # --TOTO
        # select	s.terdisplayid	as terdisplayid,
        # vbusinessdate		as transactiondate,
        # 3	as prodid,
        # s.locdisplayid	as locdisplayid,
        # '25'	as sales_type,
        # sum(coalesce(s.wagertotal,0) - coalesce(s.canceltotal,0))		as totalcount,
        # trunc(sum(coalesce(s.salesamount,0)) / 100 , 2) * 100	as totalamount ,
        # 'TOTO' as sub_prod
        # from ubt_temp_salestoto s
        # group by s.locdisplayid,s.terdisplayid
    df_temp = df_ubt_temp_salestoto.groupby(
        [
            "SALESTOTO.TERDISPLAYID",
            "SALESTOTO.LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("SALESTOTO.WAGERTOTAL", lambda x: (x.sum() - df_ubt_temp_salestoto.loc[x.index, "SALESTOTO.CANCELTOTAL"].sum())),
        AMOUNT = ("SALESTOTO.SALESAMOUNT", lambda x: trunc_value((x.fillna(0).sum() / 100), 2) * 100)
    ).assign(
        SALES_TYPE = '25',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 3,
        SUB_PROD = 'TOTO',
        LOCDISPLAYID = lambda x: x["SALESTOTO.LOCDISPLAYID"]
    ).rename(columns={
        "SALESTOTO.TERDISPLAYID": "TERDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # --40. GST TAX rate --------------------------------------------------------------------

    # select	loc.terdisplayid as terdisplayid,
    # vbusinessdate		as transactiondate,
    # prd.prodid		as prodid,
    # loc.locdisplayid	as locdisplayid,
    # '40'		as sales_type,
    # 0		as totalcount ,
    # gst.gstrate,
    # prd.prodname as sub_prod
    # from
    # (
    # 	select loc.locdisplayid, ter.terdisplayid from ubt_temp_location loc
    # 	inner join ubt_temp_terminal ter on loc.locid = ter.locid
    # 	where loc.isgst = true
    # )loc
    # cross join
    # (
    # 	select * from ubt_temp_product where prodid in (2,3,4) --lottery / es
    # )prd
    # cross join
    # (
    # 	select gstrate from {schema}.ztubt_gstconfig a
    # 	where (vbusinessdate between effectivefrom and enddate) or (vbusinessdate >=effectivefrom and enddate is null) limit 1
    # 	--check this code
    # )gst

    query = f"""
    select gstrate from {schema}.ztubt_gstconfig a
        where ( '{vbusinessdate}' between effectivefrom and enddate) or ('{vbusinessdate}' >=effectivefrom and enddate is null) limit 1
    """
    df_gst = pd.read_sql(query, connection).add_prefix("GST.")

    df_loc = df_ubt_temp_location.merge(
        df_ubt_temp_terminal,
        left_on="LOC.LOCID",
        right_on="TER.LOCID",
        how="inner"
    )[["LOC.LOCDISPLAYID", "TER.TERDISPLAYID", "LOC.ISGST"]]
    df_loc = df_loc[df_loc["LOC.ISGST"] == True]

    df_prd = df_ubt_temp_product[
        df_ubt_temp_product["PRD.PRODID"].isin([2,3,4])
    ]



    df_temp = df_loc.merge(
        df_prd,
        how="cross"
    ).merge(
        df_gst,
        how="cross"
    )
    df_temp = df_temp.assign(
        SALES_TYPE = '40',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = lambda x: x["PRD.PRODID"],
        TOTALCOUNT = 0,
        AMOUNT = lambda x: x["GST.GSTRATE"],
        SUB_PROD = lambda x: x["PRD.PRODNAME"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # --59. House syndicate cancel amount - [iTOTO cancellations – This is a Sub Set of Sales Type 2]
	# --TOTO
	# select	cb.terdisplayid	as terdisplayid,
	# vbusinessdate as transactiondate,
	# 3		as prodid,
	# loc.locdisplayid as locdisplayid,
	# '59'		as sales_type,
	# count(distinct cb.ticketserialnumber)	as totalcount, --(1)count(1)as totalcount,
	# sum(coalesce (cb.cancelledamout,0))as totalamount,
	# 'TOTO' as sub_prod
	# from ubt_temp_cancelledbetticketstate cb
	# inner join ubt_temp_itoto itoto on cb.ticketserialnumber = itoto.ticketserialnumber and itoto.itotoindicator = true
	# inner join ubt_temp_terminal ter on cb.terdisplayid = ter.terdisplayid
	# inner join ubt_temp_location loc on ter.locid = loc.locid
	# where cb.prodid = 3
	#  and cb.cancelleddate between vfromdateIGT and vtodateIGT
	# group by cb.terdisplayid,loc.locdisplayid
    df_temp = df_ubt_temp_cancelledbetticketstate.merge(
        df_ubt_temp_itoto,
        left_on="CB.TICKETSERIALNUMBER",
        right_on="ITOTO.TICKETSERIALNUMBER",
        how="inner"
    ).merge(
        df_ubt_temp_terminal,
        left_on="CB.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["CB.PRODID"] == 3) &
        (df_temp["CB.CANCELLEDDATE"] >= vfromdateIGT) &
        (df_temp["CB.CANCELLEDDATE"] < vtodateIGT)
    ]
    df_temp = df_temp.groupby(
        [
            "CB.TERDISPLAYID",
            "LOC.LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("CB.TICKETSERIALNUMBER", "nunique"),
        AMOUNT = ("CB.CANCELLEDAMOUT", "sum")
    ).assign(
        SALES_TYPE = '59',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 3,
        SUB_PROD = 'TOTO',
        LOCDISPLAYID = lambda x: x["LOC.LOCDISPLAYID"]
    ).rename(columns={
        "CB.TERDISPLAYID": "TERDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # ---60. Player syndicate sales amount by sales factor - [GROUP TOTO Sales by Sales Factor – This is a subset of Sales Type 116]
	# --TOTO, totomatch
	# SELECT S.TerDisplayID AS TerDisplayID ,
	#  vbusinessdate	AS TransactionDate,
	# S.ProdID	AS ProdID,
	# S.LocDisplayID AS LocDisplayID,
	# '60' AS sales_type,
	# SUM(coalesce(S.WagerTotal,0)  - coalesce(S.CancelTotal,0))	AS TotalCount,--(8)
	# trunc(SUM(coalesce(s.SalesAmount,0)) / 100 , 2) * 100	AS TotalAmount,
	# case when S.bettype = 1 then 'TOTO'
	# 	else 'TOTO MATCH' end as sub_prod
	# FROM ubt_temp_SalesGROUPToto S
	# GROUP BY  S.TerDisplayID,S.ProdID, S.LocDisplayID,
	# 	case when S.bettype = 1 then 'TOTO'
	# 		else 'TOTO MATCH' end

    df_temp = df_ubt_temp_SalesGroupToto.assign(
        SUB_PROD = lambda x: np.where(
            x["SALESGROUPTOTO.BETTYPE"] == 1,
            'TOTO',
            'TOTO MATCH'
        )
    )

    df_temp = df_temp.groupby(
        [
            "SALESGROUPTOTO.TERDISPLAYID",
            "SALESGROUPTOTO.PRODID",
            "SALESGROUPTOTO.LOCDISPLAYID",
            "SUB_PROD"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("SALESGROUPTOTO.WAGERTOTAL", lambda x: (x.sum() - df_ubt_temp_SalesGroupToto.loc[x.index, "SALESGROUPTOTO.CANCELTOTAL"].sum())),
        AMOUNT = ("SALESGROUPTOTO.SALESAMOUNT", lambda x: trunc_value((x.fillna(0).sum() / 100), 2) * 100)
    ).assign(
        SALES_TYPE = '60',
        TRANSACTIONDATE = vbusinessdate
    ).rename(columns={
        "SALESGROUPTOTO.TERDISPLAYID": "TERDISPLAYID",
        "SALESGROUPTOTO.PRODID": "PRODID",
        "SALESGROUPTOTO.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # --69. Player syndicate sales amount - [GROUP TOTO Sales – This is a subset of Sales Type 1]----------
	# --TOTO, totomatch
	# SELECT	S.TerDisplayID			AS TerDisplayID,
	# vbusinessdate	AS TransactionDate,
	# 3 AS ProdID,
	# S.LocDisplayID AS LocDisplayID,
	# '69' AS sales_type,
	# SUM(COALESCE(S.WagerTotal,0))AS TotalCount,
	# SUM(COALESCE(S.WagerAmount,0))		AS TotalAmount,--(1)COALESCE(S.Amount,0)		AS TotalAmount
	# case when S.bettype = 1 then 'TOTO'
	# 	else 'TOTO MATCH' end as sub_prod
	# FROM ubt_temp_SalesGROUPToto S
	# GROUP BY S.TerDisplayID, S.LocDisplayID,
	# 	case when S.bettype = 1 then 'TOTO'
	# 		else 'TOTO MATCH' end
    df_temp = df_ubt_temp_SalesGroupToto.assign(
        SUB_PROD = lambda x: np.where(
            x["SALESGROUPTOTO.BETTYPE"] == 1,
            'TOTO',
            'TOTO MATCH'
        )
    )
    df_temp = df_temp.groupby(
        [
            "SALESGROUPTOTO.TERDISPLAYID",
            "SALESGROUPTOTO.LOCDISPLAYID",
            "SUB_PROD"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("SALESGROUPTOTO.WAGERTOTAL", "sum"),
        AMOUNT = ("SALESGROUPTOTO.WAGERAMOUNT", "sum")
    ).assign(
        SALES_TYPE = '69',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 3
    ).rename(columns={
        "SALESGROUPTOTO.TERDISPLAYID": "TERDISPLAYID",
        "SALESGROUPTOTO.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # --70 [	 GROUP TOTO Cancel Amount – This is a subset of Sales Type 2]-----------------------------------
    # --TOTO, totomatch
    # select			cbt.terdisplayid as terdisplayid,
    # vbusinessdate	as transactiondate,
    # 3				as prodid,
    # loc.locdisplayid	as locdisplayid,
    # '70'				as sales_type,
    # count(distinct gt.grouphostid)	as totalcount,
    # sum(coalesce (cbt.cancelledamout,0))	as totalamount,
    # case when gt.bettype = 1 then 'TOTO'
    # 	else 'TOTO MATCH' end as sub_prod
    # from ubt_temp_cancelledbetticketstate cbt
    # inner join ubt_temp_grouptoto gt on cbt.ticketserialnumber = gt.ticketserialnumber
    # inner join ubt_temp_terminal ter on cbt.terdisplayid = ter.terdisplayid
    # inner join ubt_temp_location loc on ter.locid = loc.locid
    # where  cbt.prodid = 3
    # and cbt.cancelleddate between vfromdateIGT and vtodateIGT
    # group by cbt.terdisplayid,loc.locdisplayid,
    # 	case when gt.bettype = 1 then 'TOTO'
    # 		else 'TOTO MATCH' end
    df_temp = df_ubt_temp_cancelledbetticketstate.merge(
        df_ubt_temp_grouptoto,
        left_on="CB.TICKETSERIALNUMBER",
        right_on="GROUPTOTO.TICKETSERIALNUMBER",
        how="inner"
    ).merge(
        df_ubt_temp_terminal,
        left_on="CB.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    )
    df_temp = df_temp[
        (df_temp["CB.PRODID"] == 3) &
        (df_temp["CB.CANCELLEDDATE"] >= vfromdateIGT) &
        (df_temp["CB.CANCELLEDDATE"] < vtodateIGT)
    ]
    df_temp = df_temp.assign(
        SUB_PROD = lambda x: np.where(
            x["GROUPTOTO.BETTYPE"] == 1,
            'TOTO',
            'TOTO MATCH'
        )
    )
    df_temp = df_temp.groupby(
        [
            "CB.TERDISPLAYID",
            "LOC.LOCDISPLAYID",
            "SUB_PROD"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("GROUPTOTO.GROUPHOSTID", "nunique"),
        AMOUNT = ("CB.CANCELLEDAMOUT", "sum")
    ).assign(
        SALES_TYPE = '70',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 3
    ).rename(columns={
        "CB.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )


    # --105 Sales Commision Rate
	# select 	loc.terdisplayid as terdisplayid,
	# vbusinessdate		as transactiondate,
	# sal.prodid		as prodid,
	# loc.locdisplayid	as locdisplayid,
	# '105'				as sales_type,
	# 0				as totalcount,
	# sal.salescommission,
    # '' as sub_prod
	# 	from
	# 	(
	# 		select loc.locdisplayid, ter.terdisplayid
	# 		from ubt_temp_location loc
	# 		inner join ubt_temp_terminal ter on loc.locid = ter.locid
	# 		where loc.isgst = true
	# 	)loc
	# 	cross join
	# 	(
	# 		select distinct salescommission, prodid
	# 		from {schema}.ztubt_salescomconfig    a
	# 		where commissiontype=1 and  isdeleted = false and prodid in (2,4) --lottery / es
	# 	)sal
    df_loc = df_ubt_temp_location.merge(
        df_ubt_temp_terminal,
        left_on="LOC.LOCID",
        right_on="TER.LOCID",
        how="inner"
    )[["LOC.LOCDISPLAYID", "TER.TERDISPLAYID", "LOC.ISGST"]]
    df_loc = df_loc[df_loc["LOC.ISGST"] == True]
    query = f"""
    select distinct salescommission, prodid
        from {schema}.ztubt_salescomconfig    a
        where commissiontype=1 and  isdeleted = false and prodid in (2,4) --lottery / es
    """
    df_sal = pd.read_sql(query, connection).add_prefix("SAL.")
    df_temp = df_loc.merge(
        df_sal,
        how="cross"
    )
    df_temp = df_temp.assign(
        SALES_TYPE = '105',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = lambda x: x["SAL.PRODID"],
        TOTALCOUNT = 0,
        AMOUNT = lambda x: x["SAL.SALESCOMMISSION"],
        SUB_PROD = ''
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # --TOTO, totomatch
	# select 	loc.terdisplayid as terdisplayid,
	# vbusinessdate		as transactiondate,
	# sal.prodid		as prodid,
	# loc.locdisplayid	as locdisplayid,
	# '105'				as sales_type,
	# 0				as totalcount,
	# sal.salescommission,
	# sp.sub_prod
	# 	from
	# 	(
	# 		select loc.locdisplayid, ter.terdisplayid
	# 		from ubt_temp_location loc
	# 		inner join ubt_temp_terminal ter on loc.locid = ter.locid
	# 		where loc.isgst = true
	# 	)loc
	# 	cross join
	# 	(
	# 		select distinct salescommission, prodid
	# 		from {schema}.ztubt_salescomconfig    a
	# 		where commissiontype=1 and  isdeleted = false and prodid = 3
	# 	)sal
	# 	cross join (select 'TOTO' as sub_prod union all select 'TOTO MATCH' as sub_prod) as sp
    df_loc = df_ubt_temp_location.merge(
        df_ubt_temp_terminal,
        left_on="LOC.LOCID",
        right_on="TER.LOCID",
        how="inner"
    )[["LOC.LOCDISPLAYID", "TER.TERDISPLAYID", "LOC.ISGST"]]
    df_loc = df_loc[df_loc["LOC.ISGST"] == True]

    query = f"""

    select distinct salescommission, prodid
        from {schema}.ztubt_salescomconfig    a
        where commissiontype=1 and  isdeleted = false and prodid = 3
    """
    df_sal = pd.read_sql(query, connection).add_prefix("SAL.")
    df_sp = pd.DataFrame({
        "SUB_PROD": ['TOTO', 'TOTO MATCH']
    })
    df_temp = df_loc.merge(
        df_sal,
        how="cross"
    ).merge(
        df_sp,
        how="cross"
    )
    df_temp = df_temp.assign(
        SALES_TYPE = '105',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = lambda x: x["SAL.PRODID"],
        TOTALCOUNT = 0,
        AMOUNT = lambda x: x["SAL.SALESCOMMISSION"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )


    # --108 AT_ON_EZLINK-----------------------------------------------------------------------

    # select	ter.terdisplayid	as terdisplayid,
    # vbusinessdate as transactiondate,
    # 0		as prodid,
    # loc.locdisplayid		as locdisplayid,
    # '108'		as sales_type,
    # sum(coalesce(a.ct,0))	as totalcount,
    # sum(coalesce(a.amount,0))as amount,
    # '' as sub_prod
    # from	ubt_temp_location as loc
    # left join ubt_temp_terminal as ter on  ter.locid=loc.locid
    # left join ubt_temp_resultcashlessinterminal a on a.terdisplayid = ter.terdisplayid
    # where a.productname = 'NETS'
    # group by ter.terdisplayid,loc.locdisplayid
    df_temp = df_ubt_temp_location.merge(
        df_ubt_temp_terminal,
        left_on="LOC.LOCID",
        right_on="TER.LOCID",
        how="left"
    ).merge(
        df_ubt_temp_resultcashlessinterminal,
        left_on="TER.TERDISPLAYID",
        right_on="RCIT.TERDISPLAYID",
        how="left"
    )
    df_temp = df_temp[
        df_temp["RCIT.PRODUCTNAME"] == 'NETS'
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("RCIT.CT", "sum"),
        AMOUNT = ("RCIT.AMOUNT", "sum")
    ).assign(
        SALES_TYPE = '108',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 0,
        SUB_PROD = ''
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # --110	AT_ON_ATM	Yes	 Flashpay transaction --------------------------------------------------
	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate as transactiondate,
	# 0		as prodid,
	# loc.locdisplayid		as locdisplayid,
	# '110'		as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# sum(coalesce(a.amount,0))as amount,
	# '' as sub_prod
	# from	ubt_temp_location as loc
	# left join ubt_temp_terminal as ter on  ter.locid=loc.locid
	# left join ubt_temp_resultcashlessinterminal a on a.terdisplayid = ter.terdisplayid
	# where a.productname = 'Flashpay'
	# group by ter.terdisplayid,loc.locdisplayid
    df_temp = df_ubt_temp_location.merge(
        df_ubt_temp_terminal,
        left_on="LOC.LOCID",
        right_on="TER.LOCID",
        how="left"
    ).merge(
        df_ubt_temp_resultcashlessinterminal,
        left_on="TER.TERDISPLAYID",
        right_on="RCIT.TERDISPLAYID",
        how="left"
    )
    df_temp = df_temp[
        df_temp["RCIT.PRODUCTNAME"] == 'Flashpay'
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("RCIT.CT", "sum"),
        AMOUNT = ("RCIT.AMOUNT", "sum")
    ).assign(
        SALES_TYPE = '110',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 0,
        SUB_PROD = ''
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )


    # --112	AT_ON_CASHCARD	Yes	 Cashcard transaction------------------------------------------
	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate as transactiondate,
	# 0		as prodid,
	# loc.locdisplayid		as locdisplayid,
	# '112'		as sales_type,
	# sum(coalesce(a.ct,0))	as totalcount,
	# sum(coalesce(a.amount,0))as amount,
	# '' as sub_prod
	# from	ubt_temp_location as loc
	# left join ubt_temp_terminal as ter on  ter.locid=loc.locid
	# left join ubt_temp_resultcashlessinterminal a on a.terdisplayid = ter.terdisplayid
	# where a.productname = 'CASHCARD'
	# group by ter.terdisplayid,loc.locdisplayid
    df_temp = df_ubt_temp_location.merge(
        df_ubt_temp_terminal,
        left_on="LOC.LOCID",
        right_on="TER.LOCID",
        how="left"
    ).merge(
        df_ubt_temp_resultcashlessinterminal,
        left_on="TER.TERDISPLAYID",
        right_on="RCIT.TERDISPLAYID",
        how="left"
    )
    df_temp = df_temp[
        df_temp["RCIT.PRODUCTNAME"] == 'CASHCARD'
    ]
    df_temp = df_temp.groupby(
        [
            "TER.TERDISPLAYID",
            "LOC.LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("RCIT.CT", "sum"),
        AMOUNT = ("RCIT.AMOUNT", "sum")
    ).assign(
        SALES_TYPE = '112',
        TRANSACTIONDATE = vbusinessdate,
        PRODID = 0,
        SUB_PROD = ''
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )



    # ---116	AT_NOSPPL_SALAMT 	Yes	Sales multiplied by sales factor-------------------------------
	# select	ter.terdisplayid	as terdisplayid,
	# vbusinessdate as transactiondate,
	# a.prodid as prodid,
	# loc.locdisplayid		as locdisplayid,
	# '116'		as sales_type,
	# sum(coalesce(a.total,0)  - coalesce(can.total,0))	as totalcount, --(8)
	# trunc(sum(coalesce(a.amount,0)) / 100, 2) * 100 as totalamount,
	# a.sub_prod as sub_prod
  	# from
	# 	(
	# 		select a.ticketserialnumber, sum(a.sales + a.secondsales) as amount,
	# 		count(distinct a.ticketserialnumber) as total, pb.terdisplayid,
	# 		case
	# 			when pb.prodid=6 then 5 --prematch and live to be one same prodid
	# 			else pb.prodid	end as prodid, --(15)
	# 		case when pb.prodid <> 3 then ''
	# 			when pb.prodid = 3 and EXISTS (
	# 					SELECT 1
	# 					FROM ztubt_toto_placedbettransactionlineitem pbtlin
	# 					WHERE pb.TranHeaderID = pbtlin.TranHeaderID
	# 						AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
	# 				) then 'TOTO MATCH'
	# 			else 'TOTO' end as sub_prod
	# 		from ubt_temp_tmpticketbywageandsales a
	# 		inner join {schema}.ztubt_placedbettransactionheader  pb on a.ticketserialnumber=pb.ticketserialnumber
	# 		where pb.iscancelled = false and (
	# 				(
	# 				pb.prodid in (2,3,4) --lottery / es
	# 				and pb.createddate between vfromdatetimeIGT_UTC   and vtodatetimeIGT_UTC

	# 				)
	# 				or --(15)
	# 				(
	# 				pb.prodid in (5,6) --sports / ob
	# 				and pb.createddate between vfromdatetimeOB_UTC and vtodatetimeOB_UTC
	# 				)
	# 				or
	# 				(
	# 				pb.prodid in (1) --horse racing / bmcs
	# 				and pb.createddate between vfromdatetimeBMCS_UTC and vtodatetimeBMCS_UTC
	# 				)
	# 			)
	# 		group by a.ticketserialnumber, pb.terdisplayid, pb.prodid,
	# 			case when pb.prodid <> 3 then ''
	# 				when pb.prodid = 3 and EXISTS (
	# 						SELECT 1
	# 						FROM ztubt_toto_placedbettransactionlineitem pbtlin
	# 						WHERE pb.TranHeaderID = pbtlin.TranHeaderID
	# 							AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
	# 					) then 'TOTO MATCH'
	# 				else 'TOTO' end
	# 	)a
	# 	inner join ubt_temp_terminal ter on a.terdisplayid = ter.terdisplayid
	# 	inner join ubt_temp_location loc on ter.locid = loc.locid
	# 	left join --(8)
	# 	(
	# 		select a.ticketserialnumber, sum(a.sales + a.secondsales) as amount,
	# 		count(distinct a.ticketserialnumber) as total, cb.terdisplayid, cb.prodid,
	# 		case when cb.prodid <> 3 then ''
	# 			when cb.prodid = 3 and EXISTS (
	# 					SELECT 1
	# 					FROM ztubt_toto_placedbettransactionlineitem pbtlin
	# 					WHERE cb.TranHeaderID = pbtlin.TranHeaderID
	# 						AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
	# 				) then 'TOTO MATCH'
	# 			else 'TOTO' end as sub_prod
	# 		from ubt_temp_tmpticketbywageandsales a
	# 		inner join ubt_temp_cancelledbetticketstate cb on a.ticketserialnumber = cb.ticketserialnumber
	# 		where cb.prodid in (2,3,4) --lottery / es
	# 		group by a.ticketserialnumber, cb.terdisplayid, cb.prodid,
	# 			case when cb.prodid <> 3 then ''
	# 				when cb.prodid = 3 and EXISTS (
	# 						SELECT 1
	# 						FROM ztubt_toto_placedbettransactionlineitem pbtlin
	# 						WHERE cb.TranHeaderID = pbtlin.TranHeaderID
	# 							AND pbtlin.bettypeid = ANY(v_totomatchBettypes)
	# 					) then 'TOTO MATCH'
	# 				else 'TOTO' end
	# 	)can
	# 		on a.ticketserialnumber = can.ticketserialnumber and a.terdisplayid = can.terdisplayid
	# 		and a.prodid = can.prodid and a.sub_prod = can.sub_prod

	# 	group by loc.locdisplayid,ter.terdisplayid,a.prodid, a.sub_prod

    query = f"""
    select * from ztubt_placedbettransactionheader  pb
    where pb.iscancelled = false and (
            (
            pb.prodid in (2,3,4) --lottery / es
            and pb.createddate between '{vfromdatetimeIGT_UTC}' and '{vtodatetimeIGT_UTC}'
            )
            or --(15)
            (
            pb.prodid in (5,6) --sports / ob
            and pb.createddate between '{vfromdatetimeOB_UTC}' and '{vtodatetimeOB_UTC}'
            )
            or
            (
            pb.prodid in (1) --horse racing / bmcs
            and pb.createddate between '{vfromdatetimeBMCS_UTC}' and '{vtodatetimeBMCS_UTC}'
            )
        )
    """
    df_ubt_temp_placedbettransactionheader = pd.read_sql(query, connection).add_prefix("PB.")

    query = f"""
    select * from ztubt_toto_placedbettransactionlineitem pbtlin
    WHERE pbtlin.bettypeid in ({', '.join([f"'{bet}'" for bet in v_totomatchBettypes])})
    and tranheaderid in (
        select pb.tranheaderid from ztubt_placedbettransactionheader  pb
        where pb.iscancelled = false and pb.prodid = 3
        and pb.createddate between '{vfromdatetimeIGT_UTC}' and '{vtodatetimeIGT_UTC}'
    )
    """

    df_ubt_temp_toto_placedbettransactionlineitem = pd.read_sql(query, connection).add_prefix("PBTLIN.")

    df_exists_toto_match = df_ubt_temp_toto_placedbettransactionlineitem[["PBTLIN.TRANHEADERID"]].drop_duplicates()


    # 1st part with df_temp_a
    df_temp_a = df_ubt_temp_tmpticketbywageandsales.merge(
        df_ubt_temp_placedbettransactionheader,
        left_on="TTBWS.TICKETSERIALNUMBER",
        right_on="PB.TICKETSERIALNUMBER",
        how="inner"
    )
    df_temp_a = df_temp_a.assign(
        PRODID = lambda x: np.where(
            x["PB.PRODID"] == 6,
            5,
            x["PB.PRODID"]
        ),
        SUB_PROD = lambda x: np.where(
            x["PB.PRODID"] != 3,
            '',
            np.where(
                x["PB.TRANHEADERID"].isin(df_exists_toto_match["PBTLIN.TRANHEADERID"]),
                'TOTO MATCH',
                'TOTO'
            )
        )
    )
    df_temp_a = df_temp_a[
        (df_temp_a["PB.PRODID"].isin([2,3,4]) &
         ((df_temp_a["PB.CREATEDDATE"] >= vfromdatetimeIGT_UTC) &
          (df_temp_a["PB.CREATEDDATE"] < vtodatetimeIGT_UTC))
        ) |
        (df_temp_a["PB.PRODID"].isin([5,6]) &
         ((df_temp_a["PB.CREATEDDATE"] >= vfromdatetimeOB_UTC) &
          (df_temp_a["PB.CREATEDDATE"] < vtodatetimeOB_UTC))
        ) |
        (df_temp_a["PB.PRODID"].isin([1]) &
         ((df_temp_a["PB.CREATEDDATE"] >= vfromdatetimeBMCS_UTC) &
          (df_temp_a["PB.CREATEDDATE"] < vtodatetimeBMCS_UTC))
        )
    ]
    df_temp_a = df_temp_a.groupby(
        [
            "TTBWS.TICKETSERIALNUMBER",
            "PB.TERDISPLAYID",
            "PRODID",
            "SUB_PROD"
        ],
        as_index=False
    ).agg(
        #AMOUNT = (lambda x: (x["TTBWS.SALES"].sum() + x["TTBWS.SECONDSALES"].sum())),
        AMOUNT = ("TTBWS.SALES", lambda x: (x.sum() + df_temp_a.loc[x.index, "TTBWS.SECONDSALES"].sum())),
        TOTALCOUNT = ("TTBWS.TICKETSERIALNUMBER", "nunique")
    )[[
        "TTBWS.TICKETSERIALNUMBER",
        "AMOUNT",
        "TOTALCOUNT",
        "PB.TERDISPLAYID",
        "PRODID",
        "SUB_PROD"
    ]]

    # 2nd part with cancelled
    df_temp_can = df_ubt_temp_tmpticketbywageandsales.merge(
        df_ubt_temp_cancelledbetticketstate,
        left_on="TTBWS.TICKETSERIALNUMBER",
        right_on="CB.TICKETSERIALNUMBER",
        how="inner"
    )
    df_temp_can = df_temp_can.assign(
        PRODID = lambda x: np.where(
            x["CB.PRODID"] == 6,
            5,
            x["CB.PRODID"]
        ),
        SUB_PROD = lambda x: np.where(
            x["CB.PRODID"] != 3,
            '',
            np.where(
                x["CB.TRANHEADERID"].isin(df_exists_toto_match["PBTLIN.TRANHEADERID"]),
                'TOTO MATCH',
                'TOTO'
            )
        )
    )
    df_temp_can = df_temp_can[
        df_temp_can["CB.PRODID"].isin([2,3,4])
    ]
    df_temp_can = df_temp_can.groupby(
        [
            "TTBWS.TICKETSERIALNUMBER",
            "CB.TERDISPLAYID",
            "PRODID",
            "SUB_PROD"
        ],
        as_index=False
    ).agg(
        # AMOUNT = (lambda x: (x["TTBWS.SALES"] + x["TTBWS.SECONDSALES"]).sum()),
        AMOUNT = ("TTBWS.SALES", lambda x: (x.sum() + df_temp_can.loc[x.index, "TTBWS.SECONDSALES"].sum())),
        TOTALCOUNT = ("TTBWS.TICKETSERIALNUMBER", "nunique")
    )[[
        "TTBWS.TICKETSERIALNUMBER",
        "AMOUNT",
        "TOTALCOUNT",
        "CB.TERDISPLAYID",
        "PRODID",
        "SUB_PROD"
    ]]
    # remove before "." on columns name
    df_temp_a.columns = [col.split(".", 1)[-1] for col in df_temp_a.columns]
    df_temp_can.columns = [col.split(".", 1)[-1] for col in df_temp_can.columns]
    # add prefix
    df_temp_a = df_temp_a.add_prefix("A.")
    df_temp_can = df_temp_can.add_prefix("CAN.")


    df_temp = df_temp_a.merge(
        df_ubt_temp_terminal,
        left_on="A.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_temp_can,
        left_on=["A.TICKETSERIALNUMBER", "A.TERDISPLAYID"],
        right_on=["CAN.TICKETSERIALNUMBER", "CAN.TERDISPLAYID"],
        how="left"
    )

    df_temp = df_temp.groupby(
        [
        "LOC.LOCDISPLAYID",
        "TER.TERDISPLAYID",
        "A.PRODID",
        "A.SUB_PROD"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("A.TOTALCOUNT", lambda x: (x.sum() - df_temp.loc[x.index, "CAN.TOTALCOUNT"].fillna(0).sum())),
        AMOUNT = ("A.AMOUNT", lambda x: trunc_value((x.fillna(0).sum() / 100), 2) * 100)
    ).assign(
        SALES_TYPE = '116',
        TRANSACTIONDATE = vbusinessdate
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "A.PRODID": "PRODID",
        "A.SUB_PROD": "SUB_PROD"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )


    # DELETE  FROM ubt_temp_tmpterlocprdsalesamt SAL USING ubt_temp_sales_scandsr RS
    # where  SAL.TerDisplayID = RS.TerDisplayID
    # AND RS.ProdID = SAL.ProdID
    # AND SAL.Sales_type = '116';

    # Only perform delete operation if df_ubt_temp_sales_scandsr has data and TERDISPLAYID column
    if not df_ubt_temp_sales_scandsr.empty and 'TERDISPLAYID' in df_ubt_temp_sales_scandsr.columns:
        mask_delete = (
            (df_ubt_temp_tmpterlocprdsalesamt["SALES_TYPE"] == '116') &
            (df_ubt_temp_tmpterlocprdsalesamt.set_index(['TERDISPLAYID']).index.isin(
            df_ubt_temp_sales_scandsr.set_index(['TERDISPLAYID']).index))
        )
        df_ubt_temp_tmpterlocprdsalesamt = df_ubt_temp_tmpterlocprdsalesamt[~mask_delete]

    # SELECT	RS.TerDisplayID	as TerDisplayID,
	# 		vbusinessdate			AS TransactionDate,
	# 		RS.ProdID			AS ProdID,
	# 		LOC.LocDisplayID		AS LocDisplayID,
	# 		'116'					aS sales_type,
	# 		RS.TotalCount,
	# 		RS.Amount*100,
	# 		'' as sub_prod
	# FROM ubt_temp_sales_scandsr RS
	# INNER JOIN ubt_temp_terminal TER ON RS.TerDisplayID = TER.TerDisplayID
	# INNER JOIN ubt_temp_location LOC ON TER.LocID = LOC.LocID
	# where RS.ActualDate = vbusinessdate;

    df_temp = df_ubt_temp_sales_scandsr.merge(
        df_ubt_temp_terminal,
        left_on="SCANDSR.TERDISPLAYID",
        right_on="TER.TERDISPLAYID",
        how="inner"
    ).merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    )
    df_temp = df_temp[
        df_temp["SCANDSR.ACTUALDATE"] == vbusinessdate
    ]
    df_temp = df_temp.assign(
        SALES_TYPE = '116',
        TRANSACTIONDATE = vbusinessdate,
        AMOUNT = lambda x: x["SCANDSR.AMOUNT"] * 100,
        SUB_PROD = ''
    ).rename(columns={
        "SCANDSR.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "SCANDSR.PRODID": "PRODID",
        "SCANDSR.TOTALCOUNT": "TOTALCOUNT"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

    # select ter.terdisplayid, vbusinessdate, sfc.prodid, ter.locdisplayid, '119', 0, sfc.salesfactor, ''
	# 	from
	# 	(
	# 		select ter.terdisplayid, loc.locdisplayid from ubt_temp_terminal ter
	# 		inner join ubt_temp_location loc on ter.locid = loc.locid
	# 	)ter
	# 	cross join
	# 	(
	# 		select distinct sfc.prodid,sfc.salesfactor from ubt_temp_salesfactorconfig sfc  where sfc.prodid <> 3
	# 	)sfc

    df_temp = df_ubt_temp_terminal.merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_ubt_temp_salesfactorconfig[
            df_ubt_temp_salesfactorconfig['SFC.PRODID'] != 3
        ][['SFC.PRODID', 'SFC.SALESFACTOR']]
            .drop_duplicates()
            .reset_index(drop=True)
        ,
        how="cross"
    )
    df_temp = df_temp.assign(
        SALES_TYPE = '119',
        TRANSACTIONDATE = vbusinessdate,
        TOTALCOUNT = 0,
        AMOUNT = lambda x: x["SFC.SALESFACTOR"],
        SUB_PROD = ''
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "SFC.PRODID": "PRODID"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )


#     select ter.terdisplayid, vbusinessdate, sfc.prodid, ter.locdisplayid, '119', 0, sfc.salesfactor, sp.sub_prod
# 		from
# 		(
# 			select ter.terdisplayid, loc.locdisplayid from ubt_temp_terminal ter
# 			inner join ubt_temp_location loc on ter.locid = loc.locid
# 		)ter
# 		cross join
# 		(
# 			select distinct sfc.prodid,sfc.salesfactor from ubt_temp_salesfactorconfig sfc where sfc.prodid = 3
# 		)sfc
# 		cross join (select 'TOTO' as sub_prod union all select 'TOTO MATCH' as sub_prod) as sp
    df_temp = df_ubt_temp_terminal.merge(
        df_ubt_temp_location,
        left_on="TER.LOCID",
        right_on="LOC.LOCID",
        how="inner"
    ).merge(
        df_ubt_temp_salesfactorconfig[
            df_ubt_temp_salesfactorconfig['SFC.PRODID'] == 3
        ][['SFC.PRODID', 'SFC.SALESFACTOR']]
            .drop_duplicates()
            .reset_index(drop=True)
        ,
        how="cross"
    ).merge(
        pd.DataFrame({
            "SUB_PROD": ['TOTO', 'TOTO MATCH']
        }),
        how="cross"
    )
    df_temp = df_temp.assign(
        SALES_TYPE = '119',
        TRANSACTIONDATE = vbusinessdate,
        TOTALCOUNT = 0,
        AMOUNT = lambda x: x["SFC.SALESFACTOR"]
    ).rename(columns={
        "TER.TERDISPLAYID": "TERDISPLAYID",
        "LOC.LOCDISPLAYID": "LOCDISPLAYID",
        "SFC.PRODID": "PRODID",
        "SUB_PROD": "SUB_PROD"
    })[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )



# insert into 	ubt_temp_tmpterlocprdsalesamt
# 		SELECT	coalesce(DB.TerDisplayID,CR.TerDisplayID)					AS TerDisplayID,
# 				coalesce(DB.TransactionDate,CR.TransactionDate)			AS TransactionDate,
# 				0														AS ProdID,
# 				coalesce(DB.LocDisplayID,CR.LocDisplayID)					AS LocDisplayID,
# 				'727'														AS sales_type,
# 				0														AS TotalCount,
# 				SUM(coalesce(DB.Amount,0)) - SUM(coalesce(CR.Amount,0))		AS Amount,
# 			    '' as sub_prod
# 		FROM
# 		(
# 			SELECT A.TerDisplayID			AS TerDisplayID,
# 					A.TransactionDate		AS TransactionDate,
# 					A.LocDisplayID			AS LocDisplayID,
# 					SUM(coalesce(TotalCount,0))			AS TotalCount,
# 					SUM(coalesce(Amount,0))				AS Amount
# 			FROM  ubt_temp_tmpterlocprdsalesamt A where sales_type in
# 			(
# 				'2', --cancel amount
# 				'3',	--validation amount
# 				'7',	--commission amount
# 				--(7)6, -- GST tax amount
# 				'108', -- NETS transaction
# 				'110', -- Flashpay transaction
# 				'112', -- Cashcard transaction
# 				--(4)-1 -- refund and rebate
# 				'4', --rebate
# 				'61' --refund--(5)5 --refund
# 			 )
# 			--  AND (prodid :: int) != 31  --(15) exclude offline orders in the net amount due calculation
# 			AND (prodid :: int) not in (31,99)
# 			GROUP BY A.TerDisplayID, A.TransactionDate, A.LocDisplayID
# 		) CR
# 		FULL OUTER JOIN
# 		(
# 			SELECT A.TerDisplayID			AS TerDisplayID,
# 					A.TransactionDate		AS TransactionDate,
# 					A.LocDisplayID			AS LocDisplayID,
# 					SUM(coalesce(TotalCount,0))			AS TotalCount,
# 					SUM(coalesce(Amount,0))				AS Amount
# 			FROM  ubt_temp_tmpterlocprdsalesamt A where sales_type ='1'	--collection
# 			-- AND (prodid :: int)!= 31
# 			AND (prodid :: int) not in (31,99)
# 			GROUP BY A.TerDisplayID, A.TransactionDate, A.LocDisplayID
# 		) DB ON DB.LocDisplayID=CR.LocDisplayID AND DB.TransactionDate=CR.TransactionDate AND DB.TerDisplayID=CR.TerDisplayID
# 		GROUP BY DB.TerDisplayID, DB.TransactionDate, DB.LocDisplayID,CR.TerDisplayID,CR.TransactionDate, CR.LocDisplayID
    os.remove('debug_tmpterlocprdsalesamt_before_727.csv') if os.path.exists('debug_tmpterlocprdsalesamt_before_727.csv') else None
    df_ubt_temp_tmpterlocprdsalesamt.to_csv('debug_tmpterlocprdsalesamt_before_727.csv', index=False)
    df_cr = df_ubt_temp_tmpterlocprdsalesamt[
        df_ubt_temp_tmpterlocprdsalesamt["SALES_TYPE"].isin(
            ['2', '3', '7', '108', '110', '112', '4', '61']
        ) &
        (~df_ubt_temp_tmpterlocprdsalesamt["PRODID"].isin([31, 99]))
    ].groupby(
        [
            "TERDISPLAYID",
            "TRANSACTIONDATE",
            "LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TOTALCOUNT", "sum"),
        AMOUNT = ("AMOUNT", "sum")
    ).rename(columns={
        "TERDISPLAYID": "CR_TERDISPLAYID",
        "TRANSACTIONDATE": "CR_TRANSACTIONDATE",
        "LOCDISPLAYID": "CR_LOCDISPLAYID",
        "TOTALCOUNT": "CR_TOTALCOUNT",
        "AMOUNT": "CR_AMOUNT"
    })

    os.remove('debug_tmpterlocprdsalesamt_cr.csv') if os.path.exists('debug_tmpterlocprdsalesamt_cr.csv') else None
    df_cr.to_csv('debug_tmpterlocprdsalesamt_cr.csv', index=False)

    df_db = df_ubt_temp_tmpterlocprdsalesamt[
        (df_ubt_temp_tmpterlocprdsalesamt["SALES_TYPE"] == '1') &
        (~df_ubt_temp_tmpterlocprdsalesamt["PRODID"].isin([31, 99]))
    ].groupby(
        [
            "TERDISPLAYID",
            "TRANSACTIONDATE",
            "LOCDISPLAYID"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TOTALCOUNT", "sum"),
        AMOUNT = ("AMOUNT", "sum")
    ).rename(columns={
        "TERDISPLAYID": "DB_TERDISPLAYID",
        "TRANSACTIONDATE": "DB_TRANSACTIONDATE",
        "LOCDISPLAYID": "DB_LOCDISPLAYID",
        "TOTALCOUNT": "DB_TOTALCOUNT",
        "AMOUNT": "DB_AMOUNT"
    })
    os.remove('debug_tmpterlocprdsalesamt_db.csv') if os.path.exists('debug_tmpterlocprdsalesamt_db.csv') else None
    df_db.to_csv('debug_tmpterlocprdsalesamt_db.csv', index=False)

    df_temp = df_db.merge(
        df_cr,
        left_on=["DB_TERDISPLAYID", "DB_TRANSACTIONDATE", "DB_LOCDISPLAYID"],
        right_on=["CR_TERDISPLAYID", "CR_TRANSACTIONDATE", "CR_LOCDISPLAYID"],
        how="outer"
    ).assign(
        TERDISPLAYID = lambda x: np.where(x["DB_TERDISPLAYID"].notna(), x["DB_TERDISPLAYID"], x["CR_TERDISPLAYID"]),
        TRANSACTIONDATE = lambda x: np.where(x["DB_TRANSACTIONDATE"].notna(), x["DB_TRANSACTIONDATE"], x["CR_TRANSACTIONDATE"]),
        PRODID = 0,
        LOCDISPLAYID = lambda x: np.where(x["DB_LOCDISPLAYID"].notna(), x["DB_LOCDISPLAYID"], x["CR_LOCDISPLAYID"]),
        SALES_TYPE = '727',
        TOTALCOUNT = 0,
        AMOUNT = lambda x: x["DB_AMOUNT"].fillna(0) - x["CR_AMOUNT"].fillna(0),
        SUB_PROD = ''
    )[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )



# -- for both types of toto
# INSERT INTO ubt_temp_tmpterlocprdsalesamt
# -- sum totomatch and t649
# select terdisplayid, transactiondate, prodid, locdisplayid, sales_type, sum(totalcount), sum(amount), ''
# from ubt_temp_tmpterlocprdsalesamt td
# where sub_prod in ('TOTO MATCH', 'TOTO') and Sales_type not in ('40', '119', '105')
# group by terdisplayid, transactiondate, prodid, locdisplayid, sales_type
# union all

    df_temp = df_ubt_temp_tmpterlocprdsalesamt[
        (df_ubt_temp_tmpterlocprdsalesamt["SUB_PROD"].isin(['TOTO MATCH', 'TOTO'])) &
        (~df_ubt_temp_tmpterlocprdsalesamt["SALES_TYPE"].isin(['40', '119', '105']))
    ].groupby(
        [
            "TERDISPLAYID",
            "TRANSACTIONDATE",
            "PRODID",
            "LOCDISPLAYID",
            "SALES_TYPE"
        ],
        as_index=False
    ).agg(
        TOTALCOUNT = ("TOTALCOUNT", "sum"),
        AMOUNT = ("AMOUNT", "sum")
    ).assign(
        SUB_PROD = ''
    )[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
        "SUB_PROD"
    ]]

    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )



# -- config - clone totomatch config for product Toto (t649 and totomatch)
# select terdisplayid, transactiondate, prodid, locdisplayid, sales_type, totalcount, amount, ''
# from ubt_temp_tmpterlocprdsalesamt td
# where sub_prod in ('TOTO MATCH') and Sales_type in ('40', '119', '105');

    df_temp = df_ubt_temp_tmpterlocprdsalesamt[
        (df_ubt_temp_tmpterlocprdsalesamt["SUB_PROD"] == 'TOTO MATCH') &
        (df_ubt_temp_tmpterlocprdsalesamt["SALES_TYPE"].isin(['40', '119', '105']))
    ][[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
    ]].assign(
        SUB_PROD = ''
    )
    df_ubt_temp_tmpterlocprdsalesamt = pd.concat(
        [df_ubt_temp_tmpterlocprdsalesamt, df_temp],
        ignore_index=True
    )

# UPDATE ubt_temp_tmpterlocprdsalesamt td
# SET ProdID = CASE
# 				WHEN td.ProdID::int = 3 AND td.sub_prod = 'TOTO' THEN 71
# 				WHEN td.ProdID::int = 3 AND td.sub_prod = 'TOTO MATCH' THEN 66
# 				ELSE p.SAPProdID
# 			END
# FROM public.ztubt_sap_product p
# WHERE td.ProdID::int = p.prodid;

    query = f"""
        Select * from ztubt_sap_product

    """
    df_ztubt_sap_product = pd.read_sql(query, connection).add_prefix("P.")

    df_ubt_temp_tmpterlocprdsalesamt = df_ubt_temp_tmpterlocprdsalesamt.merge(
        df_ztubt_sap_product,
        left_on="PRODID",
        right_on="P.PRODID",
        how="left"
    )
    df_ubt_temp_tmpterlocprdsalesamt = df_ubt_temp_tmpterlocprdsalesamt.assign(
        PRODID = lambda x: np.where(
            (x["PRODID"] == 3) & (x["SUB_PROD"] == 'TOTO'),
            71,
            np.where(
                (x["PRODID"] == 3) & (x["SUB_PROD"] == 'TOTO MATCH'),
                66,
                x["P.SAPPRODID"]
            )
        )
    ).drop(columns=["P.PRODID", "P.SAPPRODID","P.X_RECORD_INSERT_TS","P.X_RECORD_UPDATE_TS","P.X_ETL_NAME"])
    [['TERDISPLAYID',
        'TRANSACTIONDATE',
        'PRODID',
        'LOCDISPLAYID',
        'SALES_TYPE',
        'TOTALCOUNT',
        'AMOUNT',
        'SUB_PROD'
    ]]



    # FINAL QUERY
    # SELECT DISTINCT
	# 		coalesce(TerDisplayID, '')
	# 		,(TransactionDate :: Date)
	# 		,coalesce(ProdID :: int, 0)
	# 		,coalesce(LocDisplayID, '')
	# 		,coalesce(Sales_type, '')
	# 		,TotalCount
	# 		,Amount
	# 	FROM ubt_temp_tmpterlocprdsalesamt --(12) ubt_temp_FinalTempData--(1)FROM ubt_temp_TmpTerLocPrdSalesAmt
	# 	WHERE (TotalCount!=0 or Amount!=0)
	# 	AND (TransactionDate :: DATE) = vbusinessdate

    df_ubt_temp_tmpterlocprdsalesamt = df_ubt_temp_tmpterlocprdsalesamt[
        ((df_ubt_temp_tmpterlocprdsalesamt["TOTALCOUNT"] != 0) |
        (df_ubt_temp_tmpterlocprdsalesamt["AMOUNT"] != 0))
        # & df_ubt_temp_tmpterlocprdsalesamt["TRANSACTIONDATE"] == vbusinessdate
    ]

    df_ubt_temp_tmpterlocprdsalesamt['PRODID'] = df_ubt_temp_tmpterlocprdsalesamt['PRODID'].fillna(0).astype(int)
    def padright_11_after_dot_point():
        def padright(value):
            str_value = str(value)
            if '.' in str_value:
                integer_part, decimal_part = str_value.split('.')
                decimal_part = decimal_part.ljust(11, '0')  # Pad right with zeros to ensure 11 digits
                return f"{integer_part}.{decimal_part}"
            else:
                return f"{str_value}.{'0'*11}"  # If no decimal part, add .00000000000
        return padright

    df_ubt_temp_tmpterlocprdsalesamt['AMOUNT'] = df_ubt_temp_tmpterlocprdsalesamt['AMOUNT'].apply(padright_11_after_dot_point())

    #remove duplicate rows

    df_ubt_temp_tmpterlocprdsalesamt = df_ubt_temp_tmpterlocprdsalesamt[[
        "TERDISPLAYID",
        "TRANSACTIONDATE",
        "PRODID",
        "LOCDISPLAYID",
        "SALES_TYPE",
        "TOTALCOUNT",
        "AMOUNT",
    ]]
    df_ubt_temp_tmpterlocprdsalesamt = df_ubt_temp_tmpterlocprdsalesamt.drop_duplicates()

    return df_ubt_temp_tmpterlocprdsalesamt





def trunc_value(value, decimals):
    """
    Truncates a number to a specified number of decimal places without rounding.
    """
    factor = 10 ** decimals
    return np.trunc(value * factor) / factor
