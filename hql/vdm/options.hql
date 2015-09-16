use vdm;
drop  table if exists options;
CREATE EXTERNAL TABLE `options`(
`b_vehicle_id` string,
`vb_model_year` string,
`vb_make` string,
`vb_model` string,
`o_option_id` int,
`o_option_code` string,
`o_option_description` string,
`o_option_group` string,
`o_standard_feature_ind` int ,
`o_style_id` int ,
`o_defining_source` string ,
`o_description_type_code` string,
`o_categorytypefilter` string,
`po_package_id` int,
`po_option_id` int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'  lines terminated by '\n'
STORED AS TEXTFILE
LOCATION '/data/database/vdm/options/'
tblproperties ("skip.header.line.count"="1");
