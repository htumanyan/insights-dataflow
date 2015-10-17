--
-- Hive script to populate the appraisals table from various sources
-- (vAuto, Manheim, MMR etc.)
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
-- 
INSERT INTO TABLE
    insights.appraisal
SELECT
    NULL             as mid,
    NULL as chrome_style_id,
    va.model_year    as    modelyear,
    va.make          as    make,
    va.model         as    model,
    NULL             as    mileage,
    va.series        as    series,
    sm.segment       as    segment,
    NULL             as    series_detail,
    NULL             as    is_certified,
    NULL             as    body_description,
    NULL             as    body_type,
    NULL             as    number_of_doors,
    va.body_cab_style        as body_cab_style,
    NULL             as    body_bed_style,
    NULL             as    body_roof_style,
    NULL             as    engine_description,
    va.engine_cylinder_count as engine_cylinder_count,
    va.engine_displacement   as engine_displacement,
    NULL             as    engine_fuel_type_desct,
    NULL             as    transmission_desription,
    va.transmission_type     as transmission_type,
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
    va.dma           as    dma_code,
    g.dma_id         as    dma_id,
    NULL             as    dma_desc,
    NULL             as    city,
    NULL             as    state_code,
    NULL             as    zip_code,
    NULL             as    country,
    NULL             as    country_code,
    NULL             as    longitude,
    NULL             as    latitude,
    NULL             as    submarket,
    printf("%d-%d-%d", theyear, themonth, theday) as sample_date,
    unix_timestamp( printf("%d-%02d-%d", theyear, themonth, theday), "yyyy-MM-dd") as sample_date_ts,
    va.count                 as sample_size,
    va.avg_list_price        as value,
    NULL                     as algorithm,
    1                        as source,
    1                        as source_type
FROM
    vauto.vauto_market_pricing va
LEFT OUTER JOIN 
    insights.segment_map sm ON va.make = sm.make and va.model = sm.model
LEFT OUTER JOIN 
    at.dma_code_id_map g ON va.dma = g.dma_code
WHERE
    va.avg_list_price is not NULL
;
