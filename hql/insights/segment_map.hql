--
-- Hive script to populate the segment map from various sources
-- (vAuto, Manheim, MMR etc.)
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
--
use insights;
INSERT INTO TABLE
    segment_map
SELECT
    NULL             as mid,
    NULL             as chrome_style_id,
    gva.make         as make,
    gva.model        as model,
    gva.segment      as segment
FROM
    (
        SELECT
          va.veh_segment   as segment, 
          va.make          as make, 
          va.model         as model
        FROM 
          vauto.vauto_sold_market_vehicle va 
        GROUP BY 
          va.veh_segment, 
          va.make, 
          va.model
    ) gva;
