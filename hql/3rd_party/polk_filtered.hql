set spark.sql.autoBroadcastJoinThreshold=100000000;
set spark.sql.shuffle.partitions=200;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

use 3rd_party;
drop table if exists 3rd_party.vins;

create table 3rd_party.vins  as select  v.vin from
 (select vin as vin from insights.sales_report_cached  union all 
  select vin as vin from insights.inventory_report_cached
) v 

group by v.vin;


DROP  table if exists 3rd_party.polk_filtered;

CREATE TABLE IF NOT EXISTS 3rd_party.polk_filtered (
  `data_type` STRING, 
  `vin` STRING, 
  `polk_control_num` STRING, 
  `dealer_name` STRING, 
  `dealer_address` STRING, 
  `dealer_town` STRING, 
  `dealer_state` STRING, 
  `dealer_zip` STRING, 
  `dealer_dma` STRING, 
  `fran_ind` STRING, 
  `vin_year_model` STRING, 
  `segment` STRING, 
  `make` STRING, 
  `model` STRING, 
  `series` STRING, 
  `sub_series` STRING, 
  `vehicle_type` STRING, 
  `body_style` STRING, 
  `cylinders` STRING, 
  `drive_type` STRING, 
  `doors` STRING, 
  `fuel_type` STRING, 
  `origin` STRING, 
  `gvw` STRING, 
  `bed_size` STRING, 
  `cab_size` STRING, 
  `turbo` STRING, 
  `financial_inst` STRING, 
  `veh_mfg_base_list_price` STRING, 
  `corporation` STRING, 
  `reg_dma` STRING, 
  `reg_state` STRING, 
  `reg_county` STRING, 
  `reg_zip` STRING, 
  `reg_type` STRING, 
  `purchase_lease` STRING, 
  `report_year_month` STRING, 
  `transaction_date` STRING, 
  `odometer` STRING, 
  `trans_price` STRING, 
  `vehicle_count` STRING
)
STORED AS PARQUET;
set spark.sql.shuffle.partitions=24;

INSERT OVERWRITE TABLE 3rd_party.polk_filtered
 select 
p.data_type as data_type,
p.vin as vin,
p.polk_control_num as polk_control_num,
p.dealer_name as dealer_name,
p.dealer_address as dealer_address,
p.dealer_town as dealer_town,
p.dealer_state as dealer_state,
p.dealer_zip as dealer_zip,
p.dealer_dma as dealer_dma,
p.fran_ind as fran_ind,
p.vin_year_model as vin_year_model,
p.segment as segment,
p.make as make,
p.model as model,
p.series as series,
p.sub_series as sub_series,
p.vehicle_type as vehicle_type,
p.body_style as body_style,
p.cylinders as cylinders,
p.drive_type as drive_type,
p.doors as doors,
p.fuel_type as fuel_type,
p.origin as origin,
p.gvw as gvw,
p.bed_size as bed_size,
p.cab_size as cab_size,
p.turbo as turbo,
p.financial_inst as financial_inst,
p.veh_mfg_base_list_price as veh_mfg_base_list_price,
p.corporation as corporation,
p.reg_dma as reg_dma,
p.reg_state as reg_state,
p.reg_county as reg_county,
p.reg_zip as reg_zip,
p.reg_type as reg_type,
p.purchase_lease as purchase_lease,
p.report_year_month as report_year_month,
p.transaction_date as transaction_date,
p.odometer as odometer,
p.trans_price as trans_price,
p.vehicle_count as vehicle_count
from 3rd_party.polk p join 3rd_party.vins v  on p.vin=v.vin
;
