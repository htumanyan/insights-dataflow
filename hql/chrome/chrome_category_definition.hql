drop table if exists chrome.category_definition;

CREATE EXTERNAL TABLE chrome.category_definition 
(
 group_id INT , 
 group_name VARCHAR(30) , 
 header_id INT , 
 header_name VARCHAR(100) , 
 category_id INT , 
 category_name VARCHAR(100) , 
 type_id INT , 
 type_name VARCHAR(40) , 
 country VARCHAR(3) , 
 language VARCHAR(3)
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE 
LOCATION '/data/database/chrome/category_definition';

