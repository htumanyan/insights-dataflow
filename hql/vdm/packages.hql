use vdm;
CREATE EXTERNAL TABLE `packages`(
`vehicle_id` string,
`model_year` string,
`make`  string,
`model` string,
`p_package_id` int,
`build_package_code` string,
`marketing_package_code` string,
`package_name` string,
`options_list` string,
`style_id` int,
`defining_source` string,
`description_type_code` string,
`package_id` int 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/vdm/packages/*.dsv'
tblproperties ("skip.header.line.count"="1");
