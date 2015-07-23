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

 


