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

use experian;
add jar hdfs://dev-na-lxhdn01:8020/user/oozie/share/lib/hive-serde.jar ;
drop  table if exists originations_stg;
CREATE EXTERNAL TABLE `originations_stg`(
`state_abbr` string,
`reporting_period` string,
`experian_dma` string,
`modelyear` string,
`make` string,
`model` string,
`segment` string,
`lease_term_mo` int,
`lease_maturity_date` string ,
`matched_cnt` int ,
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

--
-- OpenCSV serde produces text columns irrespective of the table specification.
-- Here we convert columns to their ultimate types and also add new columns as necessary
--
drop  table if exists originations;
CREATE TABLE `originations`(
`state_abbr` string,
`reporting_period` string,
`experian_dma` string,
`modelyear` string,
`make` string,
`model` string,
`segment` string,
`lease_term_mo` int,
`lease_maturity_date` string ,
`matched_cnt` int ,
`estimated_market_cnt` int,
`reporting_period_ts` int,
`reporting_period_year` int,
`reporting_period_month` int,
`lease_maturity_date_ts` int,
`lease_maturity_date_year` int,
`lease_maturity_date_month` int
);

INSERT INTO TABLE originations SELECT
  state_abbr,
  reporting_period,
  experian_dma,
  modelyear,
  make,
  model,
  segment,
  lease_term_mo,
  lease_maturity_date,
  matched_cnt,
  estimated_market_cnt,
  unix_timestamp(reporting_period, "yyyyMM"),
  year(cast(unix_timestamp(reporting_period, "yyyyMM") * 1000 as timestamp)),
  month(cast(unix_timestamp(reporting_period, "yyyyMM") * 1000 as timestamp)),
  unix_timestamp(lease_maturity_date, "MMMyyyy"),
  year(cast(unix_timestamp(lease_maturity_date, "MMMyyyy") * 1000 as timestamp)),
  month(cast(unix_timestamp(lease_maturity_date, "MMMyyyy") * 1000 as timestamp))
FROM originations_stg;

drop table originations_stg;
