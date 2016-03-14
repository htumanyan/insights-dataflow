use ying_mmr;
drop  table style_to_mid;
create external table style_to_mid(
chromestyleid int,
mid string,
eyear int,
emake string,
emodel string,
ebody string,
ssubse string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/ying_mmr/style_to_mid/'
tblproperties ("skip.header.line.count"="1");

