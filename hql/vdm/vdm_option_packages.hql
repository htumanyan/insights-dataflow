add jar ${hivevar:NameNode}/user/oozie/share/lib/adaltas-hive-udf-0.0.1-SNAPSHOT.jar;
DROP FUNCTION IF EXISTS vdm.to_map;
CREATE FUNCTION  vdm.to_map as "com.adaltas.UDAFToMap";
SET spark.sql.shuffle.partitions=6;
DROP TABLE if exists  vdm.vdm_options_packages;
CREATE TABLE vdm.vdm_options_packages as SELECT
v.vin,
to_map(vdmo.o_option_id, vdmo.o_option_description) as vdm_options_desc_map,
to_map(vdmo.o_option_id, vdmo.o_option_group) as vdm_options_group_map
from
rpm.vehicles_stg V 
join vdm.vehicles vdmv on vdmv.vb_vin=v.vin
join vdm.options vdmo on vdmo.b_vehicle_id = vdmv.b_vehicle_id
group by vin;
