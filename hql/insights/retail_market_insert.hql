INSERT OVERWRITE TABLE insights.retail_market_cached SELECT 
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
rm.days_ininventory                ,
rm.veh_segment                     ,
rm.veh_type                        ,
unix_timestamp(rm.created, 'dd/mm/yyyy hh:mm:ss aaa')  as market_created                         ,
unix_timestamp(rm.last_seen, 'dd/mm/yyyy hh:mm:ss aaa') as market_last_seen                         ,
unix_timestamp(s.last_seen, 'dd/mm/yyyy hh:mm:ss aaa') as sales_last_seen, 
case when  s.vin is not null then 1 else 0 end as issold,
g.dma_durable_key as geo_dma_durable_key,
g.dma_code as geo_dma_code,
g.dma_desc as geo_dma_desc,
g.city as geo_city,
g.state_code as geo_state_code,
g.county as geo_county,
g.country_code as geo_country_code,
g.latitude as geo_latitude,
g.longitude as geo_longitude,
g.submarket as geo_submarket,
g.tim_zone_desc as geo_tim_zone_desc
from vauto.vauto_recent_market_data rm  
left join vauto.vauto_sold_market_vehicle s  on  rm.vin = s.vin
left join at.geo g on rm.postal_code = cast(g.zip_code as int);
