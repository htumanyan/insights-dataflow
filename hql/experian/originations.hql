--
-- Hive script to import Experian origination data, received as a one-time snapshot from Experian
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
-- 
-- Below is a sample excerpt from the Experian data file
-- "TitleState"|"ReportingPeriod"|"DMA"|"VehicleYear"|"VehicleMake"|"VehicleModel"|"VehicleSegment"|"LeaseTerm"|"LeaseMaturityDate"|"TotalMatchedCount"|"TotalEstimatedMarketCount"
-- "AK"|"201201"|"ANCHORAGE, AK"|"2012"|"ACURA"|"TL"|"UPSCALE - NEAR LUXURY"|"36"|"Jan2015"|"1"|"1"
-- "AK"|"201201"|"ANCHORAGE, AK"|"2012"|"BMW"|"3-SERIES"|"UPSCALE - NEAR LUXURY"|"36"|"Jan2015"|"2"|"3"
-- "AK"|"201201"|"ANCHORAGE, AK"|"2012"|"HONDA"|"PILOT"|"CUV - MID RANGE"|"36"|"Jan2015"|"1"|"1"
-- "TitleState"|"ReportingPeriod"|"GeoLevel"|"VehicleYear"|"VehicleMake"|"VehicleModel"|"VehicleSegment"|"LeaseMaturityDate"|"TotalEstimatedMarketCount"
-- "AK"|"201201 - 201510"|"ANCHORAGE, AK"|"2011"|"BMW"|"3-SERIES"|"UPSCALE - NEAR LUXURY"|"Nov 2014"|"7"

SET spark.default.parallelism=8;

use experian;
add jar hdfs://rmsus-hdn01:8020/user/oozie/share/lib/hive-serde.jar ;
drop  table if exists `originations_stg`;
CREATE EXTERNAL TABLE `originations_stg`(
`state_abbr` string,
`reporting_period` string,
`experian_dma` string,
`modelyear` string,
`make` string,
`model` string,
`segment` string,
`lease_maturity_date` string ,
`estimated_market_cnt` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "|",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
LOCATION '/data/database/experian/originations/*.dsv'
TBLPROPERTIES("skip.header.line.count"="1");

cache table `originations_stg`;

--
-- OpenCSV serde produces text columns irrespective of the table specification.
-- Here we convert columns to their ultimate types and also add new columns as necessary
--

drop  table if exists `originations`;
CREATE TABLE `originations`(
`state_abbr` string,
`reporting_period` string,
`experian_dma` string,
`modelyear` string,
`make` string,
`model` string,
`segment` string,
`lease_maturity_date` string ,
`estimated_market_cnt` int,
`reporting_period_ts` int,
`reporting_period_year` int,
`reporting_period_month` int,
`lease_maturity_date_ts` int,
`lease_maturity_date_year` int,
`lease_maturity_date_month` int,
`dma_id` int        COMMENT 'AutoTrader DMA ID that corresponds to shapes',
`dma_desc` string   COMMENT'DMA Description from At Geo - human readable'
);

INSERT INTO TABLE originations 
SELECT
  state_abbr,
  reporting_period,
  experian_dma,
  modelyear,
  case when make='UNKNOWN VIN' then NULL else make end as make,
  case when model='UNKNOWN VIN' then 'UNKNOWN MODEL' else model end as model,
  segment,
  lease_maturity_date,
  estimated_market_cnt,
  unix_timestamp(SUBSTRING(reporting_period, 10, 6), "yyyyMM"),
  year(cast(unix_timestamp(SUBSTRING(reporting_period, 10, 6), "yyyyMM") * 1000 as timestamp)),
  month(cast(unix_timestamp(SUBSTRING(reporting_period, 10, 6), "yyyyMM") * 1000 as timestamp)),
  unix_timestamp(lease_maturity_date, "MMM yyyy"),
  year(cast(unix_timestamp(lease_maturity_date, "MMM yyyy") * 1000 as timestamp)),
  month(cast(unix_timestamp(lease_maturity_date, "MMM yyyy") * 1000 as timestamp)),
  GEO.dma_at_geo_id as dma_id,
  GEO.dma_at_geo_desc as dma_desc
FROM originations_stg
left outer join experian.dma_mapping as GEO on experian_dma=GEO.dma_experian
;

cache table originations;

drop table originations_stg;
