use mmr;
drop table if exists sales;
CREATE EXTERNAL TABLE `sales`(
`m_vin` string,
`m_saledate` string,
`m_mileage` int,
`m_year` int,
`m_make` string,
`m_model` string, 
`m_derivative` string,
`mmr_model_year` int,
`mmr_make` string,
`mmr_model` string,
`mmr_body` string,
`mmr_edition` string,
`mmr_algorithm` string,
`mmr_national_value` string,
`mmr_national_sample_size` int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/mmr'
tblproperties ("skip.header.line.count"="1");
