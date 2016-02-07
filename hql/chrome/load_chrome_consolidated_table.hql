SET spark.sql.autoBroadcastJoinThreshold= 104857600;
SET spark.sql.shuffle.partitions=200;

use chrome;

cache table model_definition;
cache table subdivision_definition;
cache table division_definition;
cache table category_definition;

use default;

drop table if exists chrome.chrome_consolidated_tmp;

CREATE TABLE chrome.chrome_consolidated_tmp
AS
SELECT
 vd.source_folder,
 vd.input_row_num,
 vd.vin,
 vd.style_name,
 vd.model_year,
 md.model_id,
 md.model_name,
 md.country,
 md.language,
 e.engine_type_id,
 e.engine_type,
 e.fuel_type_id,
 e.fuel_type,
 e.install_cause,
 e.install_cause_detail,
 vd.passthru_id,
 vd.gvwr_low,
 vd.gvwr_high,
 vd.world_manufacturer_identifier,
 vd.manufacturer_identification_code,
 vd.restraint_types,
 vd.division,
 vd.body_type
FROM 
 chrome.vehicle_description vd 
join chrome.style s ON (s.input_row_num = vd.input_row_num and s.source_folder = vd.source_folder)
 LEFT JOIN chrome.subdivision_definition as sdd ON ( s.sub_division_id = sdd.sub_division_id)
 LEFT JOIN chrome.division_definition as dd ON (s.division_id = dd.division_id)
 LEFT JOIN chrome.model_definition as md ON ( s.model_id = md.model_id)
 JOIN chrome.engine as e 
 ON (e.input_row_num = vd.input_row_num and e.source_folder = vd.source_folder);

drop table chrome.chrome_consolidated;
alter table chrome.chrome_consolidated_tmp rename to chrome.chrome_consolidated;
