CREATE TABLE insights.retail_market_pricing as select
model_year                  ,
make                        ,
model                       ,
series                      ,
drive_train_type           ,
body_cab_style ,
engine_cylinder_count  ,
engine_displacement    ,
transmission_type      ,
sum(count)                  ,
sum(avg_odometer_adjustment)/sum(count) as avg_odometer_adjustment,
sum(avg_list_price)/sum(count) as avg_list_price,
theyear,
themonth,
theday,
unix_timestamp(concat_ws('-', theyear, themonth,theday ), 'YYYY-MM-dd') as date_ts,
from 
vauto.vauto_market_pricing where model_year>=2010 and (make='Nissan' and make='Inifinity')
group by model_year                  ,
make                        ,
model                       ,
series                      ,
drive_train_type           ,
body_cab_style ,
engine_cylinder_count  ,
engine_displacement    ,
transmission_type   ;
         


