--
-- Hive script to create a table that maps Make and Model (or potentially Chrome Style ID or Manheim model id) to vehicle segment and populates
-- based on segmentation data from various sources (vAuto, Manheim, MMR etc.). 
-- ChromeStyleID is an industry standard unique identifier for Make/Model combinations
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
-- 
use insights;
drop  table if exists segment_map;
CREATE TABLE `segment_map`(
  mid               INT      COMMENT 'Manheim ID',
  chrome_style_id   INT      COMMENT 'Chrome Style ID id',
  make              STRING   COMMENT 'Vehicle Make', 
  model             STRING   COMMENT 'Vehicle Model',
  segment           STRING   COMMENT 'The segment where given Make/Model combination belongs to. Ex: Compact Sedan, Luxury SUV etc.'
)
COMMENT 'Maps Make/Model combinations to Segments. Populated from vAuto (other sources may be added later). The mapping may be also done based on Manheim model id or Chrome style id'
STORED AS PARQUET;
