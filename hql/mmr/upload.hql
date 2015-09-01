use insights;
DROP TABLE IF EXISTS mmr_dump;
create external table mmr_dump 
(vin String, solddate String, mileage String, modelyear string, make string, model string, derivative string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n' STORED AS TEXTFILE LOCATION '/tmp/mmr_dump';
INSERT into table mmr_dump
 SELECT vin, date_format(solddate, 'M/d/yyyy'), mileage, modelyear, make, model, derivative FROM sales_report_cached;
