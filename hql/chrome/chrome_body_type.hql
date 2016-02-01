use chrome;
drop table if exists chrome.body_types_build_files;

CREATE EXTERNAL TABLE body_type_build_files
(
input_row_num BIGINT, 
passthru_id BIGINT, 
style_id INT, body_type_id INT, 
body_type_name VARCHAR(20), 
is_primary VARCHAR(6)
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/data/database/chrome/body_type/build';

drop table if exists chrome.body_types_cache;

CREATE EXTERNAL TABLE body_type_cache
(
input_row_num BIGINT,
passthru_id BIGINT,
style_id INT, body_type_id INT,
body_type_name VARCHAR(20),
is_primary VARCHAR(6)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/database/chrome/body_type/cache';

