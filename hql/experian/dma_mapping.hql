use experian;
drop table if exists dma_mapping_stg;
create external table `dma_mapping_stg`(
    `dma_experian` string,
    `dma_table` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/data/database/experian/dma_mapping'
TBLPROPERTIES("skip.header.line.count"="1");


drop table if exists `dma_mapping`;
create table `dma_mapping` (
  `dma_experian`     string    COMMENT 'Experian DMA Code',
  `dma_at_geo_code`  string    COMMENT 'AutoTrader DMA Code (textual)',
  `dma_at_geo_id`    smallint  COMMENT 'DMA ID - matches shape files',
  `dma_at_geo_desc`  string    COMMENT 'DMA description from AutoTrader'
);

--
-- Here we fill in the Experian to AutoTrader DMA mapping
-- with values from dma_code_id_map that establishes the correlation
-- between AutoTrader dma_code and (dma_id, dma_desc)
--
INSERT INTO TABLE
    dma_mapping
SELECT
    e.dma_experian,
    geo.dma_code,
    geo.dma_id,
    geo.dma_desc
FROM
    dma_mapping_stg e,
    at.dma_code_id_map geo
WHERE
    e.dma_table = geo.dma_code

