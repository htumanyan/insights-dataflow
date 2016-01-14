use chrome;
drop table if exists chrome.style;

CREATE EXTERNAL TABLE style
(
 input_row_num BIGINT, 
 passthru_id BIGINT, 
 style_id INT, 
 division_id INT, 
 sub_division_id INT, 
 model_id INT, 
 base_msrp DOUBLE, 
 base_invoice DOUBLE, 
 dest_chrg DOUBLE, 
 is_price_unknown BOOLEAN, 
 market_class_id INT, 
 model_year INT, 
 style_name VARCHAR(50), 
 style_name_wo_trim VARCHAR(30), 
 trim_name VARCHAR(20), 
 mfr_model_code INT, 
 is_fleet_only BOOLEAN, 
 is_model_fleet BOOLEAN, 
 pass_doors VARCHAR(20), 
 alt_model_name VARCHAR(50), 
 alt_style_name VARCHAR(50), 
 alt_body_type VARCHAR(20), 
 drivetrain VARCHAR(50)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/data/database/chrome/style/';

