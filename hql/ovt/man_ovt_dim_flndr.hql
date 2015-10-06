use ovt;
DROP TABLE IF EXISTS man_ovt_dim_flndr;
CREATE EXTERNAL TABLE IF NOT EXISTS man_ovt_dim_flndr
(
	flndr_key     	       INT,    
	flndr_code             STRING,  
	flndr_desc             STRING, 
	dw_created_on          TIMESTAMP,          
	dw_updated_on          TIMESTAMP,
	ingestion_timestamp    STRING
)
COMMENT 'This table is imported from Manheim Oracle table dim_flndr'
PARTITIONED BY (
  `theyear` smallint,
  `themonth` smallint,
  `theday` smallint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS  PARQUET
LOCATION  '/data/database/manheim/man_ovt_raw/man_ovt_dim_flndr';

