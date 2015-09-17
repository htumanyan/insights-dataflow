--
-- Hive script to import AutoTrader geography data
-- AutoTrader geography provides mapping of zip codes to city/state, DMA (Dealer Market Area), longitude/latitude etc.
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
-- 
use at;
drop table if exists `geo_stg`;
CREATE TEMPORARY TABLE `geo_stg` USING org.apache.spark.sql.parquet OPTIONS (path "/data/database/at/geo_dma/");
drop table if exists `geo`;
CREATE TABLE `geo` as select * from geo_stg;
drop table `geo_stg`;
