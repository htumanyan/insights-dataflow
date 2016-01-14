use chrome;

drop table if exists chrome.division_definition;

CREATE EXTERNAL TABLE division_definition 
(
 division_id INT , 
 division_name VARCHAR(40) , 
 country VARCHAR(3) , 
 language VARCHAR(3)
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/data/database/chrome/division_definition';

