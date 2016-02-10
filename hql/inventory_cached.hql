use psa_shark;
SET spark.sql.shuffle.partitions=6;
drop table if exists SessionInfoUpstream_tmp;
create table if not exists SessionInfoUpstream_tmp as
select  vehicleid, collect_set(SaleChannelTypeName)[0] as SaleChannelTypeName, collect_set(saleschanneltypeid)[0] as SaleChannelTypeId from (
select VI.vehicleid, S.saleschanneltypeid, SM.SaleChannelTypeName, SS.SalesSessionID
FROM
psa.vehicleInformation_Stg  VI
join  psa.SalesSessionVehicles_stg S on  VI.vehicleid = S.vehicleInstanceId
                                                        INNER JOIN psa.SalesSessionSteps_stg SS ON S.SalesSessionStepID = SS.ID
                                                        INNER JOIN psa.SalesSessions_stg SalesSessions  ON SS.SalesSessionID = SalesSessions.ID
                                                        left  join psa.SaleChannelTypeMaster_stg SM on SM.SaleChannelTypeID = S.saleschanneltypeid
                                        WHERE   S.VehicleInstanceID = VI.vehicleid AND
                                                        VI.StatusID = 47 AND
                                                        SS.isActive = 1 AND
                                                        S.IsActive = 1 AND
                                                        SS.Status NOT IN (5) AND
                                                        SalesSessions.Status NOT IN(5)
DIstribute by VI.vehicleid sort by SS.SalesSessionID desc
) t group by   vehicleid;

drop table if exists SessionInfo_tmp;
create table if not exists SessionInfo_tmp as
select  vehicleid,  collect_set(SaleChannelTypeName)[0] as SaleChannelTypeName, collect_set(saleschanneltypeid)[0] as SaleChannelTypeId from (
select VI.vehicleid, S.saleschanneltypeid , SM.SaleChannelTypeName, SS.SalesSessionID
FROM
psa.vehicleInformation_Stg  VI
join  psa.SalesSessionVehicles_stg S on  VI.vehicleid = S.vehicleInstanceId
                                                        INNER JOIN psa.SalesSessionSteps_stg SS ON S.SalesSessionStepID = SS.ID
                                                        INNER JOIN psa.SalesSessions_stg SalesSessions  ON SS.SalesSessionID = SalesSessions.ID
                                                        left  join psa.SaleChannelTypeMaster_stg SM on SM.SaleChannelTypeID = S.saleschanneltypeid
                                        WHERE   S.VehicleInstanceID = VI.vehicleid AND
                                                SS.isActive = 1 AND
                                                S.IsActive = 1 AND
                                                SS.Status NOT IN (4,5) AND
                                                SalesSessions.Status NOT IN (4,5)

DIstribute by VI.vehicleid sort by SS.SalesSessionID desc
) t group by   vehicleid;



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
                           COALESCE(ST.Description,SM.SaleChannelTypeName, STU.Description, SMU.SaleChannelTypeName) as CommercialConceptName,
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
psa.VehicleInstance_stg VIS
   join psa.VehicleInformation_stg VI on VIS.vehicleId= VI.vehicleid
   JOIN psa_shark.vehicle_dimension_bands VDB  ON VI.vehicleid = VDB.vehicleid
   JOIN psa.Vendor_stg V on VIS.Vendorid = V.id
   JOIN psa.VendorStatuses_stg VS ON VI.VendorStatusId = VS.id and VI.vendorid=VS.vendorid
   LEFT OUTER  JOIN psa.VendorAddress_stg VAD ON VAD.vendorid = VIS.vendorid
   LEFT OUTER  JOIN psa.Address_stg VD ON VD.ID = VAD.addressid
   LEFT OUTER  JOIN psa.Country_stg VC ON VC.id = VD.countryid
   LEFT OUTER  JOIN psa.Country_stg CU ON  VI.countryid = CU.ID
   LEFT OUTER  JOIN psa.SaleChannelDetail_stg SCD  ON  SCD.VendorID = VI.VendorId and SCD.SaleChannelName=VI.SaleChannel
   LEFT JOIN   SessionInfo_tmp SessionInfo on SessionInfo.vehicleinstanceid = VIS.Id
   LEFT JOIN  psa.SaleChannelTypeMaster_stg SM  ON SessionInfo.SaleChannelTypeId = SM.SaleChannelTypeID
   LEFT JOIN  psa.SaleChannelTypeTranslation_stg ST ON ST.SaleChannelTypeID = SessionInfo.SaleChannelTypeID AND ST.VendorID = VIS.vendorId AND ST.LanguageID = 1
   LEFT JOIN  SessionInfoUpstream_tmp SessionInfoUpstream on SessionInfoUpstream.vehicleinstanceID = VIS.Id
   LEFT JOIN  psa.SaleChannelTypeMaster_stg SMU ON SessionInfoUpstream.SaleChannelTypeId = SMU.SaleChannelTypeID
   LEFT JOIN  psa.SaleChannelTypeTranslation_stg STU  ON STU.SaleChannelTypeID = SessionInfoUpstream.SaleChannelTypeID AND STU.VendorID = VIS.vendorId  AND STU.LanguageID = 1
   LEFT OUTER JOIN psa.source_stg Source on Source.sourceid = VI.sourceid
   where  VI.StatusID NOT IN  (32);
uncache table inventory_report_cached;
DROP TABLE IF EXISTS inventory_report_cached;
alter table inventory_report_cached_tmp rename to inventory_report_cached;
cache table inventory_report_cached;
SET spark.sql.shuffle.partitions=1;
