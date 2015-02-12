use psa_shark;
CREATE TABLE rpt_bmw_soldreport_cached
 AS SELECT VI.Make,
                           VI.Registration,
                           VI.Chassis,
                           VI.Derivative,
                           VI.derivativeid,
                           VI.RegistrationDate,
                           VI.SaleChannel,
               SCD.SaleChannelId,
                           VI.VendorTradingName,
               VI.VendorID, 
                           BVP.VehiclePurchaseDt AS SoldDate,
                           CASE VI.VatQualified
                            WHEN true THEN 'Marginal'
                                WHEN false THEN 'VAT Qualifying'
                                WHEN NULL THEN ''
                           END AS VatQualified,
                           (BVP.NetPriceAmt + BVP.VATAmt) AS SoldPrice,
                            BPC.BuyerPremium AS BuyerPremium,
                GDC.Delivery AS Delivery,
                            B.Name AS Buyer ,
                                B.BuyerCode AS BuyerCode,
                                regexp_replace(COALESCE(AD.NameNo, ''), ',', ' ') + ' ' + regexp_replace(COALESCE(AD.Addressline1, ''), ',', ' ') + ' '
                                + regexp_replace(COALESCE(AD.Addressline2, ''), ',', ' ') + ' ' + regexp_replace(COALESCE(AD.Town, ''), ',', ' ') + ' '
                                + regexp_replace(COALESCE(AD.PostCode, ''), ',', ' ') + ' ' + regexp_replace(COALESCE(AD.County, ''), ',', ' ') + ' '
                                + regexp_replace(COALESCE(CU.Name, ''), ',', ' ') + ' ' AS DeliveryLocation,
                           CASE BVP.IsActive
                                WHEN true THEN 'N'
                                WHEN false THEN 'Y'
                                WHEN NULL THEN ''
                           END AS ActiveSale ,
                           VI.Model,
                           VI.Code,
                           VI.ModelYear,
                           VI.ModelRef AS Model_Code,
                           VI.Mileage,
                           VI.ExteriorColour,
                           VI.DaysOnSale,
                           VI.AuctionPrice,
                           VI.Transmission, 
                           VI.Transmissionid, 
                           VI.Stockage, 
                           U.Description AS Vehicle_Type,
                           SalesTacticSession.sessionname AS SalesSession,
                           SalesTacticSession.sessionid AS SalesSessionID,
                           SalesTacticSession.tacticname AS TacticName,
                           SalesTacticSession.tacticid AS TacticId,
                           CommercialConcept.Description as CommercialConceptName,
                          CommercialConcept.SaleChannelTypeId as CommercialConceptId,
                           VI.countryid as CountryId,
                           CU.Name as CountryName,
                           VI.totaldamagesnetprice AS totaldamagesnetprice
 from  
   psa.VehicleInformation_stg VI
   INNER JOIN psa.BuyerVehiclePurchase_stg BVP ON VI.VehicleInstanceID = BVP.VehicleID and year(BVP.VehiclePurchaseDt) not in(1900)
   LEFT OUTER JOIN (select t.vehicleinstanceid as VehicleInstanceID, buyerpremiumcharge as BuyerPremium  from psa.buyerpremiumcharge_stg t limit 1) BPC ON BPC.VehicleInstanceID = BVP.VehicleID
   LEFT OUTER JOIN (select t.vehicleinstanceid as VehicleInstanceID, deliverycharges as Delivery  from psa.getdeliverycharges t limit 1) GDC ON GDC.VehicleInstanceID = BVP.VehicleID
   LEFT OUTER  JOIN psa.Buyer_stg B ON B.ID = BVP.VehicleID
   LEFT OUTER  JOIN psa.Address_stg AD ON AD.ID = BVP.BuyerDeliveryLocationID
   LEFT OUTER  JOIN psa.Country_stg CU ON  AD.CountryID = CU.ID
   LEFT OUTER  JOIN psa.UnitType_stg U ON U.ID = VI.UnitType
   LEFT OUTER  JOIN psa.SaleChannelDetail_stg SCD ON SCD.VendorID = VI.VendorId and SCD.SaleChannel=SaleChannelName 
   LEFT OUTER  JOIN psa.SaleChannelTypeTranslation_stg CommercialConcept ON CommercialConcept.SaleChannelTypeID = SCD.SaleChannelTypeId and CommercialConcept.language=1 and CommercialConcept.vendorid=VI.vendorid
   LEFT OUTER  JOIN psa_shark.sales_sessions_tactic_cached SalesTacticSession ON BVP.salessessionstepid = SalesTacticSession.stepid;
