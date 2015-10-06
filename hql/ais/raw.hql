--
-- Hive script to import HomeNet origination data, received as a one-time snapshot from HomeNet/AIS
--
-- Hovhannes Tumanyan (hovhannes@nus.la)
-- 
-- The data is sent to us as zipped XML files. At the time, when this script is executed
-- zipped XML files are extracted and concatenated in /data/database/ais/ais.xml
--
-- Here we map XML files into raw staging table using Hive-XML-Serde (https://github.com/dvasilen/Hive-XML-SerDe/)
--
-- 

use ais;
add jar ${hivevar:NameNode}/user/oozie/share/lib/hivexmlserde-1.0.5.3.jar;
drop  table if exists ais_stg;
CREATE EXTERNAL TABLE `ais_stg`(
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
["xml.processor.class"="<xml_processor_class_name>",]
"column.xpath.<column_name>"="<xpath_query>",
... 
["xml.map.specification.<xml_element_name>"="<map_specification>"
...
]
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
[LOCATION '/data/database/ais/ais.xml']
TBLPROPERTIES (
  "xmlinput.start"="<AISDataSetByMake",
  "xmlinput.end"="<AISDataSetByMake"
);


drop table ais_stg;