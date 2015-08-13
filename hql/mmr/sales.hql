use mmr;
drop table sales;
CREATE EXTERNAL TABLE `sales`(
`m_vin` string,
`m_saledate` string,
`m_mileage` int,
`m_year` int,
`m_make` string,
`m_model` string, 
`m_derivative` string,
`m_model_year` int,
`m_make` string,
`m_model` string,
`m_body` string,
`m_edition` string,
`m_algorithm` string,
`m_national_value` string,
`m_national_sample_size` int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/mmr'
tblproperties ("skip.header.line.count"="1");
