use at;
drop table if exists `us_census`;
create external table `us_census` (
    `zip_code` string,
    `population` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/data/database/at/us_census'
TBLPROPERTIES("skip.header.line.count"="1");


drop table if exists `dma_census`;
create external table `dma_census` (
    `dma_id` smallint,
    `population` int
);

--
-- Grouping the population by dma_id from the geo table
--

INSERT INTO TABLE
    dma_census
SELECT
    g.dma_id,
    SUM(c.population)
FROM
    geo g
JOIN
    us_census c
ON
    g.zip_code=c.zip_code
GROUP BY
    g.dma_id;
