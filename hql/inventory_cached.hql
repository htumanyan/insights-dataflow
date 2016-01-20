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
                           VS.Description as StatusDescription,
                           VC.id as vendorcountryid,
                           VC.name as vendorcountryname,
                           VI.isupstream as isupstram,
                           VI.sold as issold
FROM
   psa.VehicleInformation_stg VI
   INNER JOIN psa_shark.vehicle_dimension_bands VDB  ON VI.VehicleInstanceID = VDB.VehicleInstanceId
   JOIN psa.Vendor_stg V on VI.Vendorid = V.id
   LEFT OUTER  JOIN psa.VendorAddress_stg VAD ON VAD.vendorid = VI.vendorid
   LEFT OUTER  JOIN psa.Address_stg VD ON VD.ID = VAD.addressid
   LEFT OUTER  JOIN psa.Country_stg VC ON VC.id = VD.countryid
   LEFT OUTER  JOIN psa.Country_stg CU ON  VI.countryid = CU.ID
   LEFT OUTER  JOIN psa.SaleChannelDetail_stg SCD ON SCD.VendorID = VI.VendorId and SCD.SaleChannelName=VI.SaleChannel
   LEFT OUTER  JOIN psa.SaleChannelTypeMaster_stg  CommercialConcept ON CommercialConcept.SaleChannelTypeID = SCD.salechanneltypeid 
   LEFT OUTER JOIN psa.source_stg Source on Source.sourceid = VI.sourceid
   LEFT OUTER  JOIN psa.VendorStatuses_stg VS ON VI.VendorStatusId = VS.id and VI.vendorid=VS.vendorid
   where  VI.StatusID NOT IN  (32,4);
uncache table inventory_report_cached;
DROP TABLE IF EXISTS inventory_report_cached;
alter table inventory_report_cached_tmp rename to inventory_report_cached;
cache table inventory_report_cached;
SET spark.sql.shuffle.partitions=1;
