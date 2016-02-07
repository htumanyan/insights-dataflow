drop table if exists chrome.subdivision_definition;

CREATE EXTERNAL TABLE chrome.subdivision_definition 
(
 sub_division_id INT, 
 sub_division_name VARCHAR(40), 
 country VARCHAR(3), 
 language VARCHAR(3)
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE 
LOCATION '/data/database/chrome/subdivision_definition';

