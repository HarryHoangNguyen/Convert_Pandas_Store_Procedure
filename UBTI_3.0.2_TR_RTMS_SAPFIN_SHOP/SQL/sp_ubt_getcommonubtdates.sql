CREATE FUNCTION public.sp_ubt_getcommonubtdates(vfromdate timestamp without time zone, vtodate timestamp without time zone)
 RETURNS TABLE(inputfromdate timestamp without time zone, inputtodate timestamp without time zone, fromdatetimebmcs timestamp without time zone, todatetimebmcs timestamp without time zone, utcfromdatetimebmcs timestamp without time zone, utctodatetimebmcs timestamp without time zone, fromdatetimeigt timestamp without time zone, todatetimeigt timestamp without time zone, utcfromdatetimeigt timestamp without time zone, utctodatetimeigt timestamp without time zone, fromdatetimeob timestamp without time zone, todatetimeob timestamp without time zone, utcfromdatetimeob timestamp without time zone, utctodatetimeob timestamp without time zone, fromdatetimenonhost timestamp without time zone, todatetimenonhost timestamp without time zone, utcfromdatetimenonhost timestamp without time zone, utctodatetimenonhost timestamp without time zone, fromdatefund date, todatefund date)
    LANGUAGE plpgsql
    AS $$

begin

return Query
	with getInputDates as
	(select
	 vFromDate as inFromDate, vToDate as inToDate
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
		getInputDates, public.ztubt_adhoctimehistory za , public.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'BMCS'
		and za.actualdate = cast(inFromDate as date) -1
		) fromDateBMCS
		,(select za.perioddatetime from
		getInputDates, public.ztubt_adhoctimehistory za , public.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'BMCS'
		and za.actualdate = cast(inToDate as date)
		) toDateBMCS
		,(select za.perioddatetime from
		getInputDates, public.ztubt_adhoctimehistory za , public.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'IGT'
		and za.actualdate = cast(inFromDate as date) -1
		) fromDateIGT
		,(select za.perioddatetime from
		getInputDates, public.ztubt_adhoctimehistory za , public.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'IGT'
		and za.actualdate = cast(inToDate as date)
		) toDateIGT
		,(select za.perioddatetime from
		getInputDates, public.ztubt_adhoctimehistory za , public.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'OB'
		and za.actualdate = cast(inFromDate as date) -1
		) fromDateOB
		,(select za.perioddatetime from
		getInputDates, public.ztubt_adhoctimehistory za , public.ztubt_lookupvalueconfig zl
		where 1=1
		and za.host = cast(zl.fld1value as int)
		and zl.configname  ='RTMS_Host'
		and zl.fld2value = 'OB'
		and za.actualdate = cast(inToDate as date)
		) toDateOB
		,(select max(za.perioddatetime) from
		getInputDates, public.ztubt_adhoctimehistory za
		where 1=1
		and za.actualdate = cast(inFromDate as date) -1
		) fromDateNonHost
		,(select max(za.perioddatetime) from
		getInputDates, public.ztubt_adhoctimehistory za
		where 1=1
		and za.actualdate = cast(inToDate as date)
		) toDateNonHost
		,case when extract(dow from inFromDate) = 1 then infromdate::date + 4
			  when extract(dow from inFromDate) = 5 then infromdate::date + 3 end as FundFromDate
		,case when extract(dow from inFromDate) = 1 then infromdate::date + 6
			  when extract(dow from inFromDate) = 5 then infromdate::date + 6 end as FundToDate
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
			cast(inToDate as date)
			+ cast(lpad(cast(left(adhoc_time,2)::int - 24 as text),2,'0') || right (adhoc_time,6) as time)
		else
			cast(inToDate as date)+ adhoc_time::time
		end as adhoc_fromdate
	, case when left(adhoc_time,2)::int >= 24
		then
			cast(inToDate as date)+1
			+ cast(lpad(cast(left(adhoc_time,2)::int - 24 as text),2,'0') || right (adhoc_time,6) as time)
		else
			cast(inToDate as date)+1 + adhoc_time::time
		end as adhoc_todate
	from (
	select host , coalesce(adhoctime,defaulttime) adhoc_time
	from public.ztubt_adhoctimeconfig za ) a, getInputDates
	)sq
	)subquery2
	;
end;

$$;


ALTER FUNCTION public.sp_ubt_getcommonubtdates(vfromdate timestamp without time zone, vtodate timestamp without time zone) OWNER TO consultant01;
