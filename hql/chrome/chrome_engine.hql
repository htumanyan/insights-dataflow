use chrome;

drop table if exists chrome.engine;

CREATE EXTERNAL TABLE engine
(
 input_row_num BIGINT, 
 passthru_id INT, 
 engine_type_id INT, 
 engine_type VARCHAR(20), 
 fuel_type_id INT, 
 fuel_type VARCHAR(20), 
 hp_value DOUBLE, 
 hp_rpm INT, 
 net_torque_value DOUBLE, 
 net_torque_rpm INT, 
 cylinders INT, 
 displ_liters DOUBLE, 
 displ_cubic_inch DOUBLE, 
 fuel_economy_city_low DOUBLE, 
 fuel_economy_city_high DOUBLE, 
 fuel_economy_hwy_low DOUBLE, 
 fuel_economy_hwy_high DOUBLE, 
 fuel_economy_unit VARCHAR(10), 
 fuel_tank_capacity_low DOUBLE, 
 fuel_tank_capacity_high DOUBLE, 
 fuel_tank_capacity_unit VARCHAR(10), 
 forced_induction_id INT, 
 forced_induction_type VARCHAR(30), 
 install_cause VARCHAR(30), 
 install_cause_detail VARCHAR(40), 
 is_high_output BOOLEAN
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/data/database/chrome/engine/';
