SET spark.sql.shuffle.partitions=6;
INSERT OVERWRITE TABLE insights.inventory_report_cached  SELECT 
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
 V.trim_level as derivative,
 abs(hash(v.trim_level)) as derivativeid,
 0 as sourceid,
 'n/a' as sourcename,
 AD.repair_cost as totaldamagesnetprice,
G.mileage,
G.stockage, 
 AV.fuel_type as fueltype,
 V.model,
 V.model as modelref,
 'n/a' as code,
 V.model_year as modelyear,
 V.model_serial_number as model_code,
 0 as auctionprice,
 V.transmission_type as transmission,
 abs(hash(V.transmission_type)) as transmissionid,
 'n/a' as registration,
 0 as vendorstatusid,
 0 as vehicleageindays,
 G.stockage/7 as stockageinweeks,
 0 as vehicleageweeks,
 '' as ageinweeksbandname,
 0 as ageinweeksbandid,
     CASE WHEN G.stockage/7 >=0 AND G.stockage/7 <=7 THEN 'under 1 '
            WHEN G.stockage/7 >=8 AND G.stockage/7 <=14 THEN '1 - 2'
            WHEN G.stockage/7 >=15 AND G.stockage/7 <=21 THEN '2 - 3'
            WHEN G.stockage/7 >=22 AND G.stockage/7 <=28 THEN '3 - 4'
            WHEN G.stockage/7 >=29 AND G.stockage/7 <=35 THEN '4 - 5'
            WHEN G.stockage/7 >=36 AND G.stockage/7 <=42 THEN '5 - 6'
            WHEN G.stockage/7 >=43 AND G.stockage/7 <=49 THEN '6 - 7'
            WHEN G.stockage/7 >=50 AND G.stockage/7 <=56 THEN '7 - 8'
            WHEN G.stockage/7 >=57 AND G.stockage/7 <=63 THEN '8 - 9'
            WHEN G.stockage/7 >=64 AND G.stockage/7 <=70 THEN '9 - 10'
            WHEN G.stockage/7 >=71 AND G.stockage/7 <=77 THEN '10 - 11'
            WHEN G.stockage/7 >=78 AND G.stockage/7 <=84 THEN '11 - 12'
            WHEN G.stockage/7 >=85 AND G.stockage/7 <=91 THEN '12 - 13'
            WHEN G.stockage/7 >=92 AND G.stockage/7 <=98 THEN '13 - 14'
            WHEN G.stockage/7 >=99 AND G.stockage/7 <=105 THEN '14 - 15'
            WHEN G.stockage/7 >=106 AND G.stockage/7 <=112 THEN '15 - 16'
            WHEN G.stockage/7 >=113 AND G.stockage/7 <=100000 THEN 'over 16'
      end as stockageWeeksBandName,
      CASE  WHEN G.stockage/7 >=0 AND G.stockage/7 <=7 THEN 0
            WHEN G.stockage/7 >=8 AND G.stockage/7 <=14 THEN 1
            WHEN G.stockage/7 >=15 AND G.stockage/7 <=21 THEN 2
            WHEN G.stockage/7 >=22 AND G.stockage/7 <=28 THEN 3
            WHEN G.stockage/7 >=29 AND G.stockage/7 <=35 THEN 4
            WHEN G.stockage/7 >=36 AND G.stockage/7 <=42 THEN 5
            WHEN G.stockage/7 >=43 AND G.stockage/7 <=49 THEN 6
            WHEN G.stockage/7 >=50 AND G.stockage/7 <=56 THEN 7
            WHEN G.stockage/7 >=57 AND G.stockage/7 <=63 THEN 8
            WHEN G.stockage/7 >=64 AND G.stockage/7 <=70 THEN 9
            WHEN G.stockage/7 >=71 AND G.stockage/7 <=77 THEN 10
            WHEN G.stockage/7 >=78 AND G.stockage/7 <=84 THEN 11
            WHEN G.stockage/7 >=85 AND G.stockage/7 <=91 THEN 12
            WHEN G.stockage/7 >=92 AND G.stockage/7 <=98 THEN 13
            WHEN G.stockage/7 >=99 AND G.stockage/7 <=105 THEN 14
            WHEN G.stockage/7 >=106 AND G.stockage/7 <=112 THEN 15
            WHEN G.stockage/7 >=113 AND G.stockage/7 <=100000 THEN 16
      end as stockageWeeksBandId,
      CASE WHEN AD.repair_cost >=0 AND AD.repair_cost <99 THEN 'under 100'
           WHEN AD.repair_cost >=100 AND AD.repair_cost <500 THEN '100 - 500'
           WHEN AD.repair_cost >=500 AND AD.repair_cost <750 THEN '501 - 750'
           WHEN AD.repair_cost >=750 AND AD.repair_cost <100000 THEN 'over 750'
      end as damagesBandName,
      case WHEN AD.repair_cost >=0 AND AD.repair_cost <100 THEN 0
           WHEN AD.repair_cost >=100 AND AD.repair_cost <500 THEN 1
           WHEN AD.repair_cost >=500 AND AD.repair_cost <750 THEN 2
           WHEN AD.repair_cost >=750 AND AD.repair_cost <100000 THEN 3
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
end mileageBandId,
 0 as statusid,
 'n/a' as statusdescription, 
1 as pl_id
FROM rpm.vehicles_stg V 
 inner join rpm.aim_vehicles_stg AV on V.id=AV.vehicle_id 
 inner join (select aim_vehicle_id, SUM(estimated_repair_cost) as repair_cost from  rpm.aim_damages_stg GROUP BY aim_vehicle_id) AD on AD.aim_vehicle_id=AV.id
 inner join rpm.dealerships_stg D on D.nna_dealer_number=V.dealer_number 
 left outer join (select * ,  datediff( from_unixtime(unix_timestamp()), to_date(created_at)) as stockage from rpm.groundings_stg) G on G.vehicle_id=V.id;
SET spark.sql.shuffle.partitions=1;
