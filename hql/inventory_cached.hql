use psa_shark;
CREATE TABLE inventory_report_cached
 AS SELECT 
                           VI.vehicleid,
                           VI.vin,
                           VI.countryid as CountryId,
                           CU.Name as CountryName,
                           VI.make,
                           VI.makeref,
                           SCD.SaleChannelId,
                           VI.SaleChannel,
                           CommercialConcept.Description as CommercialConceptName,
                           CommercialConcept.SaleChannelTypeId as CommercialConceptId,
                           ST.id as TacticId,
                           ST.SalesTacticName as TacticName, 
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
                           VI.Code,
                           VI.ModelYear,
                           VI.ModelRef AS Model_Code,
                           VI.AuctionPrice,
                           VI.Transmission, 
                           VI.Transmissionid, 
                           Source.sourcename,
                           VI.Registration,
                           VI.VendorStatusId,
                           VS.BaseStatusId as StatusID,
                           VS.Description as StatusDescription
FROM
   psa.VehicleInformation_stg VI
   LEFT OUTER  JOIN psa.Country_stg CU ON  VI.countryid = CU.ID
   LEFT OUTER  JOIN psa.SaleChannelDetail_stg SCD ON SCD.VendorID = VI.VendorId and SCD.SaleChannelName=VI.SaleChannel
   LEFT OUTER  JOIN psa.SaleChannelTypeTranslation_stg CommercialConcept ON CommercialConcept.SaleChannelTypeID = SCD.SaleChannelTypeId and CommercialConcept.languageID=1 and CommercialConcept.vendorid=VI.VendorId
   LEFT OUTER JOIN psa.SalesSessionVehicles_stg SSV ON SSV.VehicleInstanceId = VI.vehicleInstanceid
   LEFT OUTER JOIN psa.SalesSessions_stg SS ON SS.id = SSV.SalesSessionID
   LEFT OUTER JOIN psa.SalesTactics_stg ST ON SS.salestacticid=ST.id
   LEFT OUTER JOIN psa.source_stg Source on Source.sourceid = VI.sourceid
   LEFT OUTER  JOIN psa.VendorStatuses_stg VS ON VI.VendorStatusId = VS.id and VI.vendorid=VS.vendorid
   

