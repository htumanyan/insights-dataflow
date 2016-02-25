use insights;
drop table if exists adjusted_mmr;
create table adjusted_mmr as select v.make, v.model, v.model_year, v.body_style_description, y.weekdate, 
count(*) num_vehicles,
 (avg(datediff(v.lease_start_date, y.weekdate)/30.4166667*1000)-y.mile)*y.mile_factor+y.wholesaleprice as mmr from 
rpm.vehicles_stg v join
ying_mmr.mmr y on 
lower(v.make)=lower(y.make) and 
lower(v.model)=lower(y.model) and 
lower(v.model_year)=lower(y.modelyear) and 
lower(y.body)=lower(v.body_style_description) and 
month(y.weekdate) = month(v.lease_end_date) and
 year(y.weekdate) =  year(v.lease_end_date) 
group by v.make, v.model, v.model_year, v.body_style_description, y.weekdate, y.mile, y.mile_factor, y.wholesaleprice;
