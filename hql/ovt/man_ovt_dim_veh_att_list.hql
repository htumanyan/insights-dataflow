use ovt;
DROP TABLE IF EXISTS man_ovt_dim_veh_att_list;
CREATE EXTERNAL TABLE IF NOT EXISTS man_ovt_dim_veh_att_list 
(
veh_att_key         		INT,		    
body_desc            		STRING,        	 
drive_train_desc       		STRING,      	 
engine_cooling_desc     	STRING,   	 
engine_size_unit        	STRING,	
fuel_type_desc          	STRING,	 
interior_desc               	STRING,	 
roof_desc                    	STRING,	 
dw_created_on       		TIMESTAMP,         
dw_updated_on     		TIMESTAMP,        
insert_batch_key		INT,  
update_batch_key   		INT,
ingestion_timestamp    STRING
)
COMMENT 'This table is imported from Manheim Oracle table dim_veh_att_list'
PARTITIONED BY (
  `theyear` smallint,
  `themonth` smallint,
  `theday` smallint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS PARQUET
LOCATION  '/data/database/manheim/man_ovt_raw/man_ovt_dim_veh_att_list';
