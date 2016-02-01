use chrome;
drop table if exists chrome.vehicle_description_build_files;

CREATE EXTERNAL TABLE vehicle_description_build_files
(
 input_row_num BIGINT, 
 passthru_id INT, 
 vin VARCHAR(17), 
 gvwr_low VARCHAR(10), 
 gvwr_high VARCHAR(10), 
 world_manufacturer_identifier VARCHAR(30), 
 manufacturer_identification_code VARCHAR(30), 
 restraint_types VARCHAR(30), 
 market_class_id INT, 
 model_year INT, 
 division VARCHAR(40), 
 model_name VARCHAR(20), 
 style_name VARCHAR(30), 
 body_type VARCHAR(30), 
 driving_wheels VARCHAR(5), 
 built VARCHAR(20), 
 country VARCHAR(3), 
 language VARCHAR(3), 
 best_make_name VARCHAR(30), 
 best_model_name VARCHAR(20), 
 best_style_name VARCHAR(50), 
 best_trim_name VARCHAR(15), 
 base_msrp_low DOUBLE, 
 base_msrp_high DOUBLE, 
 base_inv_low DOUBLE, 
 base_inv_high DOUBLE, 
 dest_chrg_low DOUBLE, 
 dest_chrg_high DOUBLE, 
 is_price_unknown BOOLEAN
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/data/database/chrome/vehicle_description/build';


drop table if exists chrome.vehicle_description_cache;

CREATE EXTERNAL TABLE vehicle_description_cache
(
 input_row_num BIGINT,
 passthru_id INT,
 vin VARCHAR(17),
 gvwr_low VARCHAR(10),
 gvwr_high VARCHAR(10),
 world_manufacturer_identifier VARCHAR(30),
 manufacturer_identification_code VARCHAR(30),
 restraint_types VARCHAR(30),
 market_class_id INT,
 model_year INT,
 division VARCHAR(40),
 model_name VARCHAR(20),
 style_name VARCHAR(30),
 body_type VARCHAR(30),
 driving_wheels VARCHAR(5),
 built VARCHAR(20),
 country VARCHAR(3),
 language VARCHAR(3),
 best_make_name VARCHAR(30),
 best_model_name VARCHAR(20),
 best_style_name VARCHAR(50),
 best_trim_name VARCHAR(15),
 base_msrp_low DOUBLE,
 base_msrp_high DOUBLE,
 base_inv_low DOUBLE,
 base_inv_high DOUBLE,
 dest_chrg_low DOUBLE,
 dest_chrg_high DOUBLE,
 is_price_unknown BOOLEAN
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/database/chrome/cached/vehicle_description/cache';

