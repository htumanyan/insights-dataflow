SET spark.sql.shuffle.partitions=200;


drop table if exists  vauto.vauto_recent_market_data_dedup;
CREATE TABLE vauto.vauto_recent_market_data_dedup  AS SELECT 
rm.vin                             ,
rm.postal_code                     ,
rm.stock_number                    ,
rm.model_year                      ,
rm.make                            ,
rm.model                           ,
rm.series                          ,
rm.series_detail                   ,
rm.odometer                        ,
rm.new_used                        ,
rm.is_certified                    ,
rm.body_description                ,
rm.body_type                       ,
rm.body_door_count                 ,
rm.body_cab_style                  ,
rm.body_bed_style                  ,
rm.body_roof_style                 ,
rm.engine_description              ,
rm.engine_cylinder_count           ,
rm.engine_displacement             ,
rm.engine_fuel_type                ,
rm.transmission_description        ,
rm.transmission_type               ,
rm.transmission_gear_count         ,
rm.drive_train_type                ,
rm.exterior_color                  ,
rm.exterior_base_color             ,
rm.interior_description            ,
rm.interior_color                  ,
rm.interior_material               ,
rm.categorized_equipment_ids       ,
rm.days_ininventory,
rm.veh_segment                     ,
rm.veh_type,                        
rm.created,
rm.last_seen
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
row_number() over ( partition by vin,created order by unix_timestamp(created, 'dd/mm/yyyy hh:mm:ss aaa')  desc ) group_rank 
from vauto.vauto_recent_market_data ) rm  where rm.group_rank=1;

