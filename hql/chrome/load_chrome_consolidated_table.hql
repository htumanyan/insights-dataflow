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
 bt.body_type_id,
 bt.body_type_name,
 bt.is_primary,
 s.style_id,
 s.model_year,
 s.style_name,
 s.source_folder,
 md.model_id,
 md.model_name,
 md.country,
 md.language,
 sdd.sub_division_id,
 sdd.sub_division_name,
 dd.division_id,
 dd.division_name,
 s.input_row_num,
 e.engine_type_id,
 e.engine_type,
 e.fuel_type_id,
 e.fuel_type,
 e.install_cause,
 e.install_cause_detail,
 vd.passthru_id,
 vd.vin,
 vd.gvwr_low,
 vd.gvwr_high,
 vd.world_manufacturer_identifier,
 vd.manufacturer_identification_code,
 vd.restraint_types,
 vd.division,
 vd.body_type
FROM 
 chrome.style s 
 LEFT JOIN chrome.subdivision_definition as sdd ON ( s.sub_division_id = sdd.sub_division_id)
 LEFT JOIN chrome.division_definition as dd ON (s.division_id = dd.division_id)
 LEFT JOIN chrome.model_definition as md ON ( s.model_id = md.model_id)
 LEFT JOIN chrome.body_type bt 
 ON (bt.input_row_num = s.input_row_num and bt.source_folder = s.source_folder and bt.style_id = s.style_id)
 JOIN chrome.vehicle_description vd 
 ON (s.input_row_num = vd.input_row_num and s.source_folder = vd.source_folder)
 JOIN chrome.engine as e 
 ON (e.input_row_num = vd.input_row_num and e.source_folder = vd.source_folder);

drop table if exists chrome.chrome_consolidated;

create table if not exists chrome.chrome_consolidated 
as 
select *
from 
(select *, row_number() over (partition by vin) as group_rank
from chrome.chrome_consolidated_tmp
) t where group_rank=1;

