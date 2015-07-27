INSERT OVERWRITE TABLE inventory_report_cached  SELECT 
V.id,
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
 D.id as vendorid,
 'n/a' as derivative,
 0 as derivativeid,
 0 as sourceid,
 'n/a' as sourcename,
 AD.estimated_repair_cost as totaldamagesnetprice,
 G.mileage,
 datediff(to_date(G.created_at),
 from_unixtime(unix_timestamp())) as stockage,
 AV.fuel_type as fueltype,
 V.model,
 V.model as modelref,
 'n/a' as code,
 V.model_year as modelyear,
 V.model_serial_number as model_code,
 0 as auctionprice,
 V.transmission_type as transmission,
 0 as transmissionid,
 'n/a' as registration,
 0 as vendorstatusid,
 0 as vehicleageindays,
 datediff(to_date(G.created_at), from_unixtime(unix_timestamp()))/7 as stockageinweeks,
 0 as vehicleageweeks,
 '' as stockageweeksbandname,
 0 as stockageweeksbandid,
 '' as ageinweeksbandname,
 0 as ageinweeksbandid,
      CASE WHEN AD.estimated_repair_cost >=0 AND AD.estimated_repair_cost <99 THEN 'under 100'
           WHEN AD.estimated_repair_cost >=100 AND AD.estimated_repair_cost <500 THEN '100 - 500'
           WHEN AD.estimated_repair_cost >=500 AND AD.estimated_repair_cost <750 THEN '501 - 750'
           WHEN AD.estimated_repair_cost >=750 AND AD.estimated_repair_cost <100000 THEN 'over 750'
      end as damagesBandName,
      case WHEN AD.estimated_repair_cost >=0 AND AD.estimated_repair_cost <100 THEN 0
           WHEN AD.estimated_repair_cost >=100 AND AD.estimated_repair_cost <500 THEN 1
           WHEN AD.estimated_repair_cost >=500 AND AD.estimated_repair_cost <750 THEN 2
           WHEN AD.estimated_repair_cost >=750 AND AD.estimated_repair_cost <100000 THEN 3
      end as damagesBandId,
     case WHEN G.mileage >=0 AND G.mileage <10000 THEN '0-10 000'
          WHEN G.mileage >=10000 AND G.mileage <20000 THEN '10 001-20 000'
          WHEN G.mileage >=20000 AND G.mileage <30000 THEN '20 001-30 000'
          WHEN G.mileage >=30000 AND G.mileage <40000 THEN '30 001-40 000'
          WHEN G.mileage >=40000 AND G.mileage <50000 THEN '40 001-50 000'
          WHEN G.mileage >=50000 AND G.mileage <75000 THEN '50 001-75 000'
          WHEN G.mileage >=75000 AND G.mileage <100000 THEN '75001-100 000'
          WHEN G.mileage >=100000 AND G.mileage <150000 THEN '100 001-150 000'
          WHEN G.mileage >=150000 AND G.mileage <999999 THEN 'over 150 000'
      end mileageBandName,
    case
            WHEN G.mileage >=0 AND G.mileage <10000 THEN 0
            WHEN G.mileage >=10000 AND G.mileage <20000 THEN 1
            WHEN G.mileage >=20000 AND G.mileage <30000 THEN 2
            WHEN G.mileage >=30000 AND G.mileage <40000 THEN 3
            WHEN G.mileage >=40000 AND G.mileage <50000 THEN 4
            WHEN G.mileage >=50000 AND G.mileage <75000 THEN 5
            WHEN G.mileage >=75000 AND G.mileage <100000 THEN 6
            WHEN G.mileage >=100000 AND G.mileage <150000 THEN 7
            WHEN G.mileage >=150000 AND G.mileage <999999 THEN 8
end mileageBandId
 0 as statusid,
 'n/a' as statusdescription  
FROM rpm.vehicles_stg V 
 left outer join rpm.groundings_stg G on G.vehicle_id=V.id 
 inner join rpm.dealerships_stg D on D.nna_dealer_number=V.dealer_number 
 inner join rpm.aim_vehicles_stg AV on V.id=AV.vehicle_id 
 inner join rpm.aim_damages_stg AD on AD.aim_vehicle_id=AV.id;
