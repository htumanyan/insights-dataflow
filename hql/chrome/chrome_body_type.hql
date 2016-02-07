drop table if exists chrome.body_type;

CREATE EXTERNAL TABLE chrome.body_type
(
input_row_num INT,
passthru_id INT,
style_id INT,
body_type_id INT,
body_type_name STRING,
is_primary BOOLEAN,
source_folder STRING
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE
LOCATION '/data/database/chrome/body_type/';
