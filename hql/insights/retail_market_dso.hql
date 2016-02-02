use insights;
set  spark.sql.shuffle.partitions=24;

create table  dso_dates_tmp as select * from (
select distinct to_date(from_unixtime(market_created)) as dt from retail_market_cached union
select distinct to_date(from_unixtime(market_last_seen)) as dt from retail_market_cached union
select distinct to_date(from_unixtime(sales_last_seen)) as dt from retail_market_cached ;

drop  table if exists dso_sold_daily_tmp;
create table dso_sold_daily_tmp as select 
   make,
   model,
   geo_dma_id,
   modelyear,  
   dd.dt,
   count(distinct  case when issold =1 then vin else null end)  as count_sold
   from  dso_dates_tmp DD left join  retail_market_cached RM
   on to_date(from_unixtime(RM.sales_last_seen)) = DD.dt 
 group by  make, model, geo_dma_id, modelyear,   DD.dt  order by DD.dt  desc;

drop table dso;
create table  dso as select make, model, modelyear, geo_dma_id, DD.dt, count(distinct RM.vin) as count from
dso_dates_tmp DD join  retail_market_cached RM
on
  RM.market_created <= unix_timestamp(DD.dt, 'yyyy-MM-dd') and unix_timestamp(DD.dt, 'yyyy-MM-dd') <= RM.market_last_seen
group by  make, model, modelyear, geo_dma_id, DD.dt;

drop table dso_metrics_tmp;
create table dso_metrics_tmp as select 
dso.make, 
dso.model,
dso.modelyear,
dso.geo_dma_id,
dso.dt as date,
coalesce(dso.count, 0) as inventory,
coalesce(ds.count_sold, 0) as daily_sold
from  
dso full join dso_vehicles_sold_daily_tmp ds  on
dso.make = ds.make and
dso.model = ds.model and
dso.modelyear = ds.modelyear and 
dso.geo_dma_id = ds.geo_dma_id and 
dso.dt = ds.dt
where dso.count != 0 order by dso.dt;

drop  table if exists dso_metrics;
create table dso_metrics as select 
   make,
   model,
   modelyear,  
   geo_dma_id,
   date,
   inventory,
   sum(coalesce(daily_sold, 0 ) ) over (partition by make, model, modelyear, geo_dma_id  order by unix_timestamp(date, 'yyyy-MM-dd') range between 3888000 preceding and current row) as count
 from dso_metrics_tmp ;

--drop  table dso;
--drop table dso_dates_tmp;
--drop table dso_metrics_tmp ;
--drop table dso_sold_daily_tmp; 
 
