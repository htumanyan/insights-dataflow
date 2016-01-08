use ying_mmr;
drop  table mmr;
create external table mmr(
make string, 
model string,
body string,
mid string,
modelyear string,
weekdate string,
year int,
month int,
AgeMonth int,
mile int,
wholesaleprice int,
mile_factor double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/ying_mmr/mmr/'
tblproperties ("skip.header.line.count"="1");

