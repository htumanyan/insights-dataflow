use psa_shark;
CREATE TABLE inventory_report_cached
 AS SELECT 
                           VI.vehicleid,
                           VI.vin,
                           VI.countryid as CountryId,
                           unix_timestamp(VI.createddt) as CreationTS,
                           CU.Name as CountryName,
                           VI.make,
                           VI.makeref,
                           SCD.SaleChannelId,
                           VI.SaleChannel,
                           case when BVP.directsaleid > 0 then 'Direct Sale' else 
                           CommercialConcept.salechanneltypename end as CommercialConceptName,
                           ST.id as TacticId,
                           ST.SalesTacticName as TacticName, 
                           SS.id as SalesSessionID,
                           SS.name as SalesSessionName, 
                           VI.VendorTradingName,
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
   LEFT OUTER  JOIN psa.Country_stg CU ON  VI.countryid = CU.ID
   LEFT OUTER  JOIN psa.SaleChannelDetail_stg SCD ON SCD.VendorID = VI.VendorId and SCD.SaleChannelName=VI.SaleChannel
   LEFT OUTER  JOIN psa.SaleChannelTypeTranslation_stg CommercialConcept ON CommercialConcept.SaleChannelTypeID = SCD.SaleChannelTypeId and CommercialConcept.languageID=1 and CommercialConcept.vendorid=VI.VendorId
   LEFT OUTER JOIN psa.SalesSessionVehicles_stg SSV ON SSV.VehicleInstanceId = VI.vehicleInstanceid
   LEFT OUTER JOIN psa.SalesSessions_stg SS ON SS.id = SSV.SalesSessionID
   LEFT OUTER JOIN psa.SalesTactics_stg ST ON SS.salestacticid=ST.id
   LEFT OUTER JOIN psa.source_stg Source on Source.sourceid = VI.sourceid
   LEFT OUTER  JOIN psa.VendorStatuses_stg VS ON VI.VendorStatusId = VS.id and VI.vendorid=VS.vendorid;
