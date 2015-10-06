use ovt;
DROP TABLE IF EXISTS man_ovt_dim_auction;
CREATE EXTERNAL TABLE IF NOT EXISTS man_ovt_dim_auction 
(
	auction_key               	    INT,   
	ove_auction_id                      INT,   
	finance_auction_id                  STRING,   
	vicki_auction_id                    STRING,
	auction_nm                          STRING, 
	parent_company_nm                   STRING, 
	parent_company_type_nm              STRING,  
	cur_auction_nm                      STRING, 
	cur_parent_company_nm               STRING, 
	cur_parent_company_type_nm          STRING,  
	auction_cd                          STRING,  
	hist_ops_mkt_id                     STRING,   
	hist_ops_mkt_nm                     STRING,  
	hist_ops_rgn_id                     STRING,   
	hist_ops_rgn_nm                     STRING,  
	hist_ops_div_id                     STRING,   
	hist_ops_div_nm                     STRING,  
	hist_ops_glbl_rgn_id                STRING,   
	hist_ops_glbl_rgn_nm                STRING,  
	cur_ops_mkt_id                      STRING,   
	cur_ops_mkt_nm                      STRING,  
	cur_ops_rgn_id                      STRING,   
	cur_ops_rgn_nm                      STRING,  
	cur_ops_div_id                      STRING,   
	cur_ops_div_nm                      STRING,  
	cur_ops_glbl_rgn_id                 STRING,  
	cur_ops_glbl_rgn_nm                 STRING,  
	time_zone_desc                      STRING,  
	addr_line_1_txt                     STRING, 
	addr_line_2_txt                     STRING, 
	city_nm                             STRING,  
	state_cd                            STRING,   
	state_nm                            STRING,
	zip_cd                              STRING,  
	country_cd                          STRING,   
	country_nm                          STRING,  
	ops_peer_grp                        STRING,  
	ops_peer_sub_grp                    STRING, 
	cur_ops_peer_grp                    STRING,  
	cur_ops_peer_sub_grp                STRING,  
	row_is_current                      INT,     
	row_start_dt                        TIMESTAMP,          
	row_end_dt                          TIMESTAMP,          
	row_change_reason                   INT,    
	dw_created_on                       TIMESTAMP,          
	dw_updated_on                       TIMESTAMP,          
	insert_batch_key                    INT,    
	update_batch_key                    INT,    
	prim_sale_day                       STRING,  
	ods_service_provider_id             INT,
	ingestion_timestamp                 STRING
)
COMMENT 'This table is imported from Manheim Oracle table dim_auction'
PARTITIONED BY (
  `theyear` smallint,
  `themonth` smallint,
  `theday` smallint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
STORED AS  PARQUET
LOCATION  '/data/database/manheim/man_ovt_raw/man_ovt_dim_auction';
