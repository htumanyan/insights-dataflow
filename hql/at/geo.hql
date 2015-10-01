--
-- Hive script to import AutoTrader geography data
-- AutoTrader geography provides mapping of zip codes to city/state, DMA (Dealer Market Area), longitude/latitude etc.
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
-- 
use at;
add jar hdfs://dev-na-lxhdn01:8020/user/oozie/share/lib/hive-serde.jar ;
drop table if exists `geo_stg`;
CREATE TEMPORARY TABLE `geo_stg` USING org.apache.spark.sql.parquet OPTIONS (path "/data/database/at/geo_dma/");
drop table if exists `adwords_geo_stg`;
CREATE EXTERNAL TABLE `adwords_geo_stg` (
   `City` string,
   `Criteria ID` string,
   `State` string,
   `DMA_Region` string,
   `DMA_Region_Code` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
LOCATION '/data/database/3rd_party/adwords/geo_dma*.csv'
TBLPROPERTIES("skip.header.line.count"="1");

use at;
drop table if exists `dma_bnd_stg`;
CREATE EXTERNAL TABLE `dma_bnd_stg` (
   `dma_code` string,
   `dma_code_candidate` string,
   `dma_id` string,
   `dma_region` string,
   `latitude` string,
   `longitude` string,
   `geometry` string,
   `geometry_vertex_cnt` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/data/database/3rd_party/dma_bnd'
TBLPROPERTIES("skip.header.line.count"="1");

--
-- Here we drop and re-create the table geo. It is a product of the original AT Geo data, joined with 
-- a mapping of AutoTrader DMA Codes to DMA Codes in the shape file. THe shape file DMA resembles Nielsen 
-- DMA codes, however it is not clear whether it is an exact replica
drop table if exists `geo`;
CREATE TABLE `geo` (
  `dma_durable_key`  string, 
  `dma_code`         string     COMMENT 'AutoTrader DMA Code',
  `dma_id`           smallint   COMMENT 'DMA ID that corresponds to the Map/Shape file',
  `dma_desc`         string     COMMENT 'Human readable description of the DMA',
  `city`             string,
  `state_code`       string,
  `zip_code`         string,
  `county`           string,
  `country_code`     string,
  `latitude`         double,
  `longitude`        double,
  `submarket`        string,
  `tim_zone_desc`    string,
  `geometry`         string          COMMENT 'DMA Area geometry expressed as an XML entry for inclusion in KML files',
  `geometry_vergex_cnt` smallint     COMMENT 'Vertex count in the geometry field'
);


-- Because of the inconsistent DMA id generation scheme, we have to resort
-- to two attempts to find mappings using two different algirithms.
-- dma_code and dma_code_candidate in the staging table are generated using different
-- algorithns. Here we try the simple mapping first (by dma_code in dma_bnd_stg) and
-- if no match found, try the second algorithm by dma_code_candidate.
-- CASE WHEN (d1.dma_code is not null) THEN d1.dma_code ELSE d2.dma_code_candidate END,
-- CASE WHEN (d1.dma_code is not null) THEN d1.geometry_vertex_cnt ELSE d2.geometry_vertex_cnt END
INSERT INTO TABLE 
    at.geo
SELECT
    gs.dma_durable_key,
    gs.dma_code,
    CAST(CASE WHEN (d1.dma_code is not null) THEN d1.dma_id ELSE d2.dma_id END as SMALLINT),
    gs.dma_desc,
    gs.city,
    gs.state_code,
    gs.zip_code,
    gs.county,
    gs.country_code,
    gs.latitude,
    gs.longitude,
    gs.submarket,
    gs.tim_zone_desc,
    CASE WHEN (d1.dma_code is not null) THEN d1.geometry ELSE d2.geometry END,
    CAST(CASE WHEN (d1.dma_code is not null) THEN d1.geometry_vertex_cnt ELSE d2.geometry_vertex_cnt END as SMALLINT)
FROM
    geo_stg gs
LEFT OUTER JOIN
    at.dma_bnd_stg d1 ON gs.dma_code = d1.dma_code
LEFT OUTER JOIN
    at.dma_bnd_stg d2 ON gs.dma_code = d2.dma_code_candidate;

drop table `geo_stg`;
drop table `dma_bnd_stg`;



