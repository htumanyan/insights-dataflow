use ovt;
DROP TABLE IF EXISTS man_ovt_dim_veh_att_list2;
CREATE EXTERNAL TABLE IF NOT EXISTS man_ovt_dim_veh_att_list2 
(
veh_att_key2     	 	INT,       
audio_desc              	STRING,   
door_desc                	STRING,   
engine_desc                	STRING,
fuel_intake_desc            	STRING,
ignition_desc               	STRING,
transmission_desc           	STRING,
window_type_desc           	STRING,
dw_created_on     	     	TIMESTAMP,        
dw_updated_on     	      	TIMESTAMP,       
insert_batch_key  	       	INT,
update_batch_key  	    	INT,
ingestion_timestamp    STRING
)
COMMENT 'This table is imported from Manheim Oracle table dim_veh_att_list2'
PARTITIONED BY (
  `theyear` smallint,
  `themonth` smallint,
  `theday` smallint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS  PARQUET
LOCATION  '/data/database/manheim/man_ovt_raw/man_ovt_dim_veh_att_list2';
