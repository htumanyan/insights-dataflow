use chrome;
drop table if exists chrome.body_types;

CREATE EXTERNAL TABLE body_type
(
input_row_num BIGINT, 
passthru_id BIGINT, 
style_id INT, body_type_id INT, 
body_type_name VARCHAR(20), 
is_primary VARCHAR(6)
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/data/database/chrome/body_type';

