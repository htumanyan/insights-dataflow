use insights;
drop table if exists adjusted_mmr_pre;
create table adjusted_mmr_pre as select 
v.vin,
v.lease_start_date,
v.lease_end_date, 
coalesce(vdm.vb_make, cc.division, v.make) as make,
coalesce(vdm.vb_model, cc.model_name, v.model) as model,
coalesce(vdm.vb_model_year, cc.model_year, v.model_year) as modelyear,
coalesce(vdm.ev_trim, cc.style_name, v.body_style_description) as trim,
case when y1.mid is not null then 'vdm' when y2.mid is not null then  'cc' when y3.mid is not null then 'rpm' end as source,
coalesce(y1.mid, y2.mid, y3.mid ) as mid,
coalesce(y1.mile, y2.mile, y3.mile) as mile,
coalesce(y1.mile_factor, y2.mile_factor, y3.mile_factor) as mile_factor,
coalesce(y1.wholesaleprice, y2.wholesaleprice, y3.wholesaleprice) as wholesaleprice,
coalesce(y1.weekdate, y2.weekdate, y3.weekdate) as weekdate
from 
rpm.vehicles_stg v
left join vdm.vehicles vdm on v.vin = vdm.vb_vin
left join ying_mmr.mmr y1 on
trim(lower(y1.make))=trim(lower(vdm.vb_make)) and
trim(lower(y1.model))=trim(lower(vdm.vb_model)) and
trim(lower(y1.body))=trim(lower(vdm.ev_trim)) and
trim(lower(y1.modelyear))=trim(lower(vdm.vb_model_year)) and
month(y1.weekdate) = month(v.lease_end_date) and
year(y1.weekdate) =  year(v.lease_end_date)
left join chrome.chrome_consolidated cc on v.vin = cc.vin
left join ying_mmr.style_to_mid stm on stm.chromestyleid= cc.style_id
left join ying_mmr.mmr y2 on stm.mid = y2.mid and
month(y2.weekdate) = month(v.lease_end_date) and
year(y2.weekdate) =  year(v.lease_end_date)
left join ying_mmr.mmr y3 on 
trim(lower(y3.make))=trim(lower(v.make)) and
trim(lower(y3.model))=trim(lower(v.model)) and
trim(lower(y3.body))=trim(lower(v.body_style_description)) and
trim(lower(y3.modelyear))=trim(lower(v.model_year)) and
month(y3.weekdate) = month(v.lease_end_date) and
year(y3.weekdate) =  year(v.lease_end_date);

drop  table if exists adjusted_mmr_tmp;
create table adjusted_mmr_tmp as select
y.make,
y.model,
y.modelyear,
y.trim,
y.mid,
y.weekdate, 
count(*) num_vehicles,
 (avg(datediff(y.lease_end_date, y.lease_start_date)/30.4166667*1000)-y.mile)*y.mile_factor+y.wholesaleprice as mmr from 
adjusted_mmr_pre y
group by y.make, y.model, y.modelyear, y.trim, y.mid, y.weekdate, y.mile, y.mile_factor, y.wholesaleprice;

drop  table if exists adjusted_mmr;
create table  adjusted_mmr  as select
v.vin,
y.make,
y.model,
y.modelyear,
y.trim,
y.mid,
y.weekdate,
y.mmr
from  adjusted_mmr_pre v  join adjusted_mmr_tmp y
on v.mid=y.mid and
v.make=y.make and 
v.modelyear = y.modelyear and 
v.trim = y.trim and 
v.weekdate = y.weekdate;
