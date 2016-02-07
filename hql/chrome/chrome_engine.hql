drop table if exists chrome.engine;

CREATE EXTERNAL TABLE chrome.engine
(
 input_row_num INT,
 passthru_id INT,
 engine_type_id INT,
 engine_type STRING,
 fuel_type_id INT,
 fuel_type STRING,
 install_cause VARCHAR(30),
 install_cause_detail STRING,
 source_folder STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/data/database/chrome/engine/';
