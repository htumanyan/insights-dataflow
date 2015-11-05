use insights;
set  spark.sql.shuffle.partitions=24;

drop  table if exists dso_dates_tmp;
create  table if not exists  dso_dates_tmp  as select 
t.make, t.model, t.modelyear, t.geo_dma_id, t.dt from ( 
select make, model, modelyear,geo_dma_id, to_date(from_unixtime(market_created)) as dt from retail_market_cached  
union 
select make, model, modelyear, geo_dma_id, to_date(from_unixtime(market_last_seen )) as dt from  retail_market_cached 
union 
select make, model, modelyear, geo_dma_id, to_date(from_unixtime(sales_last_seen )) as dt from  retail_market_cached 

) as t
group by t.make, t.model, t.modelyear, t.geo_dma_id, t.dt;

drop  table if exists dso_vehicles_sold_daily_tmp;
create table dso_vehicles_sold_daily_tmp as select 
   make,
   model,
   geo_dma_id,
   modelyear,  
   to_date(from_unixtime(sales_last_seen)) as sales_date,
   count(distinct  case when issold =1 then vin else null end)  as count_sold
   from  retail_market_cached 
 group by  make, model, geo_dma_id, modelyear,   to_date(from_unixtime(sales_last_seen))  order by sales_date  desc;

drop  table if exists dso_average_daily_sold_tmp;
create table  dso_average_daily_sold_tmp as select 
   make,
   model,
   modelyear,  
   geo_dma_id,
   sales_date,
   sum(coalesce(count_sold, 0 ) ) over (partition by t.make, t.model, t.modelyear, t.geo_dma_id  order by unix_timestamp(t.sales_date, 'yyyy-MM-dd') range between 3888000 preceding and current row) as count
 from (select d.dt as sales_date, d.make, d.model, d.modelyear, d.geo_dma_id, s.count_sold from  dso_dates_tmp d left  join  dso_vehicles_sold_daily_tmp  s on 
s.model = d.model and 
s.make = d.make and 
s.modelyear=d.modelyear and 
d.geo_dma_id = s.geo_dma_id and 
s.sales_date = d.dt ) t ;


drop table dso;
create table  dso as select DD.make, DD.model, DD.modelyear, DD.geo_dma_id, DD.dt, count(distinct RM.vin) as count from 
dso_dates_tmp DD join  retail_market_cached RM
on
  DD.make = RM.make and
  DD.model = RM.model and 
  DD.modelyear = RM.modelyear and 
  DD.geo_dma_id = RM.geo_dma_id
  and RM.market_created <= unix_timestamp(DD.dt, 'yyyy-MM-dd') and unix_timestamp(DD.dt, 'yyyy-MM-dd') <= RM.market_last_seen
group by  DD.make, DD.model, DD.modelyear, DD.geo_dma_id, DD.dt;


drop table dso_metrics;
create table dso_metrics as select 
dso.make, 
dso.model,
dso.modelyear,
dso.geo_dma_id,
dso.dt as date,
coalesce(dso.count, 0) as inventory,
coalesce(ads.count, 0) as daily_sold
from dso_dates_tmp dates left  join  dso 
on 
dates.make = dso.make and
dates.model = dso.model and
dates.modelyear = dso.modelyear and 
dates.geo_dma_id = dso.geo_dma_id and 
dates.dt = dso.dt
left join dso_average_daily_sold_tmp ads  on
dates.make = ads.make and
dates.model = ads.model and
dates.modelyear = ads.modelyear and 
dates.geo_dma_id = ads.geo_dma_id and 
dates.dt = ads.sales_date;

--drop  table dso;
--drop table dso_dates_tmp;
--drop table dso_vehicles_sold_daily_tmp ;
--drop table dso_average_daily_sold_tmp; 
 
