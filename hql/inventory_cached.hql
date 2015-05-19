use psa_shark;
SET spark.sql.shuffle.partitions=6;
DROP TABLE IF EXISTS inventory_report_cached_tmp;
CREATE TABLE inventory_report_cached_tmp
 AS SELECT 
                           VI.vehicleid,
                           VI.vin,
                           VI.countryid as CountryId,
                           VI.exteriorcolour,
                           unix_timestamp(VI.createddt) as CreationTS,
                           CU.Name as CountryName,
                           VI.make,
                           VI.makeref,
                           SCD.SaleChannelId,
                           VI.SaleChannel,
                           CommercialConcept.salechanneltypename as CommercialConceptName,
                           ST.id as TacticId,
                           ST.SalesTacticName as TacticName, 
                           SS.id as SalesSessionID,
                           SS.name as SalesSessionName, 
                           V.Name as VendorTradingName,
                           VI.VendorID, 
                           VI.Derivative,
                           VI.derivativeid,
                           VI.sourceid,
                           Source.sourcename,
                           VI.totaldamagesnetprice AS totaldamagesnetprice,
                           VI.Mileage,
                           VI.Stockage, 
                           VI.fueltype AS fueltype,
                           VI.Model,
                           VI.Modelref,
                           VI.Code,
                           VI.ModelYear,
                           VI.ModelRef AS Model_Code,
                           VI.AuctionPrice,
                           VI.Transmission, 
                           VI.Transmissionid, 
                           VI.Registration,
                           VI.VendorStatusId,
                           VI.VehicleAgeInDays,
                           floor(VI.Stockage/7) as stockageweeks,
                           floor(VI.VehicleAgeInDays/7) as vehicleageweeks, 
                           VDB.ageinweeksbandname,
                           VDB.ageinweeksbandid,
                           VDB.stockageweeksbandname,
                           VDB.stockageweeksbandid,
                           VDB.damagesbandname,
                           VDB.damagesbandid, 
                           VDB.mileagebandname,
                           VDB.mileagebandid,
                           VS.BaseStatusId as StatusID,
                           VS.Description as StatusDescription
FROM
   psa.VehicleInformation_stg VI
   INNER JOIN psa_shark.vehicle_dimension_bands VDB  ON VI.VehicleInstanceID = VDB.VehicleInstanceId
   JOIN psa.Vendor_stg V on VI.Vendorid = V.id
   LEFT OUTER  JOIN psa.Country_stg CU ON  VI.countryid = CU.ID
   LEFT OUTER  JOIN psa.SaleChannelDetail_stg SCD ON SCD.VendorID = VI.VendorId and SCD.SaleChannelName=VI.SaleChannel
   LEFT OUTER JOIN psa.SalesSessionVehicles_stg SSV ON SSV.VehicleInstanceId = VI.vehicleInstanceid
   LEFT OUTER  JOIN psa.SaleChannelTypeMaster_stg  CommercialConcept ON CommercialConcept.SaleChannelTypeID = SSV.saleschanneltypeid 
   LEFT OUTER JOIN psa.SalesSessions_stg SS ON SS.id = SSV.SalesSessionID
   LEFT OUTER JOIN psa.SalesTactics_stg ST ON SS.salestacticid=ST.id
   LEFT OUTER JOIN psa.source_stg Source on Source.sourceid = VI.sourceid
   LEFT OUTER  JOIN psa.VendorStatuses_stg VS ON VI.VendorStatusId = VS.id and VI.vendorid=VS.vendorid;
uncache table inventory_report_cached;
DROP TABLE IF EXISTS inventory_report_cached;
alter table inventory_report_cached_tmp rename to inventory_report_cached;
cache table inventory_report_cached;
SET spark.sql.shuffle.partitions=1;
