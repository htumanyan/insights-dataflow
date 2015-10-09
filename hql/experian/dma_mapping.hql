use experian;
drop table if exists dma_mapping;
create external table `dma_mapping`(
    `dma_experian` string,
    `dma_table` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/data/database/experian/dma_mapping'
TBLPROPERTIES("skip.header.line.count"="1");

