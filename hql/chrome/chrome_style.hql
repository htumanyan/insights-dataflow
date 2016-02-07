drop table if exists chrome.style;

CREATE EXTERNAL TABLE chrome.style
(
 input_row_num INT, 
 passthru_id INT, 
 style_id INT, 
 division_id INT, 
 sub_division_id INT, 
 model_id INT, 
 model_year INT, 
 style_name STRING, 
 source_folder STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE
LOCATION '/data/database/chrome/style/';

