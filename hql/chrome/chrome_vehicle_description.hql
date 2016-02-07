drop table if exists chrome.vehicle_description;

CREATE EXTERNAL TABLE chrome.vehicle_description
(
 input_row_num INT,
 passthru_id INT,
 vin STRING,
 gvwr_low STRING,
 gvwr_high STRING,
 world_manufacturer_identifier STRING,
 manufacturer_identification_code STRING,
 restraint_types STRING,
 model_year INT,
 division STRING,
 model_name STRING,
 style_name STRING,
 body_type STRING,
 country STRING,
 language STRING,
 source_folder STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/data/database/chrome/vehicle_description/';

