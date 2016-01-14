SET spark.sql.shuffle.partitions=200;
use vauto;
drop table if exists  vauto_recent_market_data_dedup;
CREATE TABLE vauto_recent_market_data_dedup  AS SELECT 
*
from  
( SELECT 
vin                             ,
postal_code                     ,
stock_number                    ,
model_year                      ,
make                            ,
model                           ,
series                          ,
series_detail                   ,
odometer                        ,
new_used                        ,
is_certified                    ,
body_description                ,
body_type                       ,
body_door_count                 ,
body_cab_style                  ,
body_bed_style                  ,
body_roof_style                 ,
engine_description              ,
engine_cylinder_count           ,
engine_displacement             ,
engine_fuel_type                ,
transmission_description        ,
transmission_type               ,
transmission_gear_count         ,
drive_train_type                ,
exterior_color                  ,
exterior_base_color             ,
interior_description            ,
interior_color                  ,
interior_material               ,
categorized_equipment_ids       ,
days_ininventory,
veh_segment                     ,
veh_type                        ,
created,
last_seen,
 case 
   when unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(last_seen, 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
   else unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') end as last_seen_ts, 
row_number() over ( partition by vin, created, is_certified order by
        case when unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(last_seen, 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
        else unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') end desc ) group_rank 
from vauto_recent_market_data where cast(model_year as int) >=2012 ) rm  where rm.group_rank=1;

drop table if exists  vauto.vauto_sold_market_vehicle_dedup;
CREATE TABLE vauto.vauto_sold_market_vehicle_dedup  AS SELECT 
*
from  
( SELECT 
vin                             ,
postal_code                     ,
stock_number                    ,
model_year                      ,
make                            ,
model                           ,
series                          ,
series_detail                   ,
odometer                        ,
new_used                        ,
is_certified                    ,
body_description                ,
body_type                       ,
body_door_count                 ,
body_cab_style                  ,
body_bed_style                  ,
body_roof_style                 ,
engine_description              ,
engine_cylinder_count           ,
engine_displacement             ,
engine_fuel_type                ,
transmission_description        ,
transmission_type               ,
transmission_gear_count         ,
drive_train_type                ,
exterior_color                  ,
exterior_base_color             ,
interior_description            ,
interior_color                  ,
interior_material               ,
categorized_equipment_ids       ,
days_ininventory,
veh_segment                     ,
veh_type                        ,
created,
last_seen,
 case 
   when unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(last_seen, 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
   else unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') end as last_seen_ts, 
row_number() over ( partition by vin, created, is_certified  order by
        case when unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') is null then unix_timestamp(last_seen, 'dd-MMM-yy hh.mm.ss.SSSSSS aa' ) 
        else unix_timestamp(last_seen, 'MM/dd/yyyy HH:mm:ss aa') end desc ) group_rank 
from vauto.vauto_sold_market_vehicle where cast(model_year as int) >=2012 ) rm  where rm.group_rank=1;
