import pandas as pd
from datetime import datetime, timedelta
from Snowflake_connection import snowflake_connection
connection = snowflake_connection()
schema = "SPPL_DEV_DWH.SPPL_PUBLIC"

def sp_ubt_getcommonubtdates(vfromdate, vtodate):

    fromdate = pd.Timestamp(vfromdate)
    todate = pd.Timestamp(vtodate)

    query = f"""
        with getInputDates as
	(select
	 cast('{vfromdate}' as timestamp) as inFromDate, cast('{vtodate}' as timestamp) as inToDate
	)
	select infromdate,intodate
	,coalesce(fromdatebmcs,bmcs_adhoc_FromDate) as fromdatebmcs
	,coalesce(todatebmcs,bmcs_adhoc_ToDate) as todatebmcs
	,coalesce(fromdatebmcs,bmcs_adhoc_FromDate)-interval '8 hour' as UTCfromdatebmcs
	,coalesce(todatebmcs,bmcs_adhoc_ToDate)-interval '8 hour' as UTCtodatebmcs
	,coalesce(fromdateigt,igt_adhoc_FromDate) as fromdateigt
	,coalesce(todateigt,igt_adhoc_ToDate) as todateigt
	,coalesce(fromdateigt,igt_adhoc_FromDate)-interval '8 hour' as UTCfromdateigt
	,coalesce(todateigt,igt_adhoc_ToDate)-interval '8 hour' as UTCtodateigt
	,coalesce(fromdateob,ob_adhoc_FromDate) as fromdateob
	,coalesce(todateob,ob_adhoc_ToDate) as todateob
	,coalesce(fromdateob,ob_adhoc_FromDate)-interval '8 hour' as UTCfromdateob
	,coalesce(todateob,ob_adhoc_ToDate)-interval '8 hour' as UTCtodateob
	,coalesce(fromdatenonhost,nh_adhoc_FromDate) as fromdatenonhost
	,coalesce(todatenonhost,nh_adhoc_ToDate) as todatenonhost
	,coalesce(fromdatenonhost,nh_adhoc_FromDate)-interval '8 hour' as UTCfromdatenonhost
	,coalesce(todatenonhost,nh_adhoc_ToDate)-interval '8 hour' as UTCtodatenonhost
	,FundFromDate,FundToDate
	from (
	select  inFromDate, inToDate
		,(select za.perioddatetime from
		getInputDates, {schema}.ztubt_adhoctimehistory za , {schema}.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'BMCS'
		and za.actualdate = cast(inFromDate as date) -1
		) fromDateBMCS
		,(select za.perioddatetime from
		getInputDates, {schema}.ztubt_adhoctimehistory za , {schema}.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'BMCS'
		and za.actualdate = cast(inToDate as date)
		) toDateBMCS
		,(select za.perioddatetime from
		getInputDates, {schema}.ztubt_adhoctimehistory za , {schema}.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'IGT'
		and za.actualdate = cast(inFromDate as date) -1
		) fromDateIGT
		,(select za.perioddatetime from
		getInputDates, {schema}.ztubt_adhoctimehistory za , {schema}.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'IGT'
		and za.actualdate = cast(inToDate as date)
		) toDateIGT
		,(select za.perioddatetime from
		getInputDates, {schema}.ztubt_adhoctimehistory za , {schema}.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'OB'
		and za.actualdate = cast(inFromDate as date) -1
		) fromDateOB
		,(select za.perioddatetime from
		getInputDates, {schema}.ztubt_adhoctimehistory za , {schema}.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'OB'
		and za.actualdate = cast(inToDate as date)
		) toDateOB
		,(select max(za.perioddatetime) from
		getInputDates, {schema}.ztubt_adhoctimehistory za
		where 1=1
		and za.actualdate = cast(inFromDate as date) -1
		) fromDateNonHost
		,(select max(za.perioddatetime) from
		getInputDates, {schema}.ztubt_adhoctimehistory za
		where 1=1
		and za.actualdate = cast(inToDate as date)
		) toDateNonHost
		,case when extract(dow from inFromDate) = 1 then cast(inFromDate as date) + 4
			  when extract(dow from inFromDate) = 5 then cast(inFromDate as date) + 3 end as FundFromDate
		,case when extract(dow from inFromDate) = 1 then cast(inFromDate as date) + 6
			  when extract(dow from inFromDate) = 5 then cast(inFromDate as date) + 6 end as FundToDate
	from getInputDates
	)sub_query1
	,(select
	max(case when host =1 then adhoc_fromdate end) igt_adhoc_FromDate,
	max(case when host =1 then adhoc_todate end) igt_adhoc_ToDate,
	max(case when host =2 then adhoc_fromdate end) bmcs_adhoc_FromDate,
	max(case when host =2 then adhoc_todate end) bmcs_adhoc_ToDate,
	max(case when host =3 then adhoc_fromdate end) ob_adhoc_FromDate,
	max(case when host =3 then adhoc_todate end) ob_adhoc_ToDate,
	min(adhoc_fromdate) nh_adhoc_FromDate,
	max(adhoc_todate) nh_adhoc_ToDate
	from (
	select
	  host
	, adhoc_time
	, case when left(adhoc_time,2)::int >= 24
		then
			cast(cast(inToDate as date)::string || ' ' || lpad(cast(left(adhoc_time,2)::int - 24 as text),2,'0') || right (adhoc_time,6) as timestamp)
		else
			cast(cast(inToDate as date)::string || ' ' || adhoc_time as timestamp)
		end as adhoc_fromdate
	, case when left(adhoc_time,2)::int >= 24
		then
			cast(cast(cast(inToDate as date)+1 as string) || ' ' || lpad(cast(left(adhoc_time,2)::int - 24 as text),2,'0') || right (adhoc_time,6) as timestamp)
		else
			cast(cast(cast(inToDate as date)+1 as string) || ' ' || adhoc_time as timestamp)
		end as adhoc_todate
	from (
	select host , coalesce(adhoctime,defaulttime) adhoc_time
	from {schema}.ztubt_adhoctimeconfig za ) a, getInputDates
	)sq
	)subquery2

    """
    df_dates = pd.read_sql(query, connection)


    return df_dates

df_dates = sp_ubt_getcommonubtdates('2025-09-23 06:00:00', '2025-09-24 05:59:59')
df_dates.to_csv('test_dates_output.csv', index=False)
