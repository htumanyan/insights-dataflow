use vdm;
drop  table if exists packages;
CREATE EXTERNAL TABLE `packages`(
`vb_vehicle_id` string,
`vb_model_year` string,
`vb_make` string,
`vb_model` string,
`p_package_id` int,
`p_build_package_code` string,
`p_marketing_package_code` string,
`p_package_name` string,
`p_options_list` string,
`p_style_id` int,
`p_defining_source` string,
`p_description_type_code` string,
`po_package_id` int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/vdm/packages/*.dsv'
tblproperties ("skip.header.line.count"="1");
