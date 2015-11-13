use mmr;
drop table if exists popular_makes;
CREATE EXTERNAL TABLE `popular_makes`(
`make` string,
`model` string,
`body` string,
`mid` bigint,
`modelyear` int,
`weekdate` string,
`year` int,
`month` int,
`AgeMonth` int,
`mile` int,
`wholesaleprice` int,
`mile_factor` float
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/mmr/popular'
tblproperties ("skip.header.line.count"="1");