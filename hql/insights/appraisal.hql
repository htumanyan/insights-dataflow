--
-- Hive script to populate the appraisals table from various sources
-- (vAuto, Manheim, MMR etc.)
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
-- 
SET spark.sql.shuffle.partitions=64;
INSERT INTO TABLE
    insights.appraisal_tmp
SELECT
    NULL             as mid,
    NULL as chrome_style_id,
    t.modelyear as modelyear,
    t.make as make,
    t.model as model,
    NULL             as    mileage,
    t.series as series,
    t.segment as segment,
    NULL             as    series_detail,
    NULL             as    is_certified,
    NULL             as    body_description,
    NULL             as    body_type,
    NULL             as    number_of_doors,
    t.body_cab_style as body_cab_style,
    NULL             as    body_bed_style,
    NULL             as    body_roof_style,
    NULL             as    engine_description,
    t.engine_cylinder_count as engine_cylinder_count,
    t.engine_displacement as engine_displacement,
    NULL             as    engine_fuel_type_desct,
    NULL             as    transmission_desription,
    t.transmission_type as transmission_type,
    NULL             as    transmission_gear_count,
    NULL             as    drive_train_type,
    NULL             as    exterior_color,
    NULL             as    exterior_base_color,
    NULL             as    interior_description,
    NULL             as    interior_color,
    NULL             as    interior_material,
    NULL             as    categorized_equipment_ids,
    NULL             as    veh_type,
    NULL             as    dma_durable_key,
    t.dma_code as dma_code,
    t.dma_id as dma_id,
    NULL             as    dma_desc,
    NULL             as    city,
    NULL             as    state_code,
    NULL             as    zip_code,
    NULL             as    country,
    NULL             as    country_code,
    NULL             as    longitude,
    NULL             as    latitude,
    NULL             as    submarket,
    t.sample_date as sample_date,
    t.sample_date_ts as sample_date_ts,
    t.sample_size as sample_size,
    t.value as value,
    NULL                     as algorithm,
    1                        as source,
    1                        as source_type,
    sum(value*sample_size) over (partition by t.make, t.model, t.modelyear,t.dma_code, t.sample_date ) as ymmd_sum_list_price,
    sum(sample_size) over (partition by t.make, t.model, t.modelyear,t.dma_code, t.sample_date ) as ymmd_total_count
FROM
(select
    va.model_year    as    modelyear,
    va.make          as    make,
    va.model         as    model,
    va.series        as    series,
    sm.segment       as    segment,
    va.body_cab_style        as body_cab_style,
    va.engine_cylinder_count as engine_cylinder_count,
    va.engine_displacement   as engine_displacement,
    va.transmission_type     as transmission_type,
    va.dma           as    dma_code,
    g.dma_id         as    dma_id,
    printf("%d-%d-%d", theyear, themonth, theday) as sample_date,
    unix_timestamp( printf("%d-%02d-%d", theyear, themonth, theday), "yyyy-MM-dd") as sample_date_ts,
    va.count                 as sample_size,
    va.avg_list_price        as value
from
    vauto.vauto_market_pricing va on  va.avg_list_price is not NULL
LEFT OUTER JOIN 
    insights.segment_map sm ON va.make = sm.make and va.model = sm.model
LEFT OUTER JOIN 
    at.dma_code_id_map g ON va.dma = g.dma_code
) as t
;
