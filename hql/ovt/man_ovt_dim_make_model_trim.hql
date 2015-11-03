use ovt;
DROP TABLE IF EXISTS man_ovt_dim_make_model_trim;
CREATE EXTERNAL TABLE IF NOT EXISTS man_ovt_dim_make_model_trim
(
   make_model_trim_key 					INT,
    unit_type_desc 					STRING,
    unit_super_type_desc 					STRING,
    make_desc 					STRING,
    model_desc 					STRING,
    trim_desc 					STRING,
    dw_created_on 					TIMESTAMP,
    dw_updated_on 					TIMESTAMP,
    insert_batch_key 					INT,
    update_batch_key 					INT,
    ingestion_timestamp  				STRING
)
COMMENT 'This table is imported from Manheim Oracle table dim_make_model_trim'
PARTITIONED BY (
  `theyear` smallint,
  `themonth` smallint,
  `theday` smallint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS  PARQUET
LOCATION  '/data/database/manheim/man_ovt_raw/man_ovt_dim_make_model_trim/';
