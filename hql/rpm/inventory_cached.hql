INSERT OVERWRITE TABLE insights.inventory_report_cached AS SELECT 
V.vehicleid,
V.vin,
V.region_code as countryid,
V.color as exteriorcolour,
unix_timestamp(V.created_at) as creation_ts,
v.region_code as countryname,
v.make,
v.make as makeref,
0 as salechannelid, 
'none' as salechannel,
'none' as commercialconceptname,
D.name as vendortradingname,
D.is as vendorid,
'n/a' as derivative,
0 as derivativeid,
0 as sourceid,
'n/a' as sourcename,
AD.estimated_repair_cost as totaldamagesnetprice,
G.mileage,
datediff(to_date(G.created_at), from_unixtime(unix_timestamp())) as stockage,
AV.fuel_type as fueltype,
V.model,
V.model,
'n/a' as code,
V.modelyear as modelyear,
V.model_serial_number as model_code,
0 as auctionprice,
V.transmission_type as transmission,
V.transmission_type as transmissionid,
'n/a' as registration,
0 as vendorstatusid,
0 as vehicleageindays,
stockage/7 as stockageweeks,
0 as vehicleageweeks,
'' as stockageweeksbandname,
0 as stockageweeksbandid,
0 as damagesbandid,
'' as mileagebandname,
0 as statusid,
'n/a' as statusdescription

FROM rpm.vehicles_stg V
 inner join on rpm.dealers_stg D on D.nna_dealer_number=V.dealer_number
 left outer join on rpm.groundings G on G.vehicle_id=V.vehicleid 
 left outer join on aim_vehicles AV on V.vehicle_id=AV.vehicle_id
 left outer join on aim_damages AD on AD.aim_vehicle_id=AV.id;
  
 


