drop table if exists chrome.model_definition;

CREATE EXTERNAL TABLE chrome.model_definition
(
 model_id INT , 
 model_name VARCHAR(30) , 
 country VARCHAR(3) , 
 language VARCHAR(3)
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE 
LOCATION '/data/database/chrome/model_definition';
