use psa_shark;
CREATE TABLE rpt_bmw_soldreport_cached
 AS SELECT VI.Make,
                           VI.Registration,
                           VI.Chassis,
                           VI.Derivative,
                           VI.RegistrationDate,
                           VI.SaleChannel,
                           VI.VendorTradingName,
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
                           U.Description AS Vehicle_Type
 from  
psa.VehicleInformation_stg VI
   INNER JOIN psa.BuyerVehiclePurchase_stg BVP ON VI.VehicleInstanceID = BVP.VehicleID and year(BVP.VehiclePurchaseDt) not in(1900)
   LEFT OUTER JOIN (select FIRST_VALUE(t.vehicleinstanceid) as VehicleInstanceID, buyerpremiumcharge as BuyerPremium  from psa.buyerpremiumcharge_stg t) BPC ON BPC.VehicleInstanceID = BVP.VehicleID
   LEFT OUTER JOIN (select FIRST_VALUE(t.vehicleinstanceid) as VehicleInstanceID, deliverycharges as Delivery  from psa.getdeliverycharges t) GDC ON GDC.VehicleInstanceID = BVP.VehicleID
   LEFT OUTER  JOIN psa.Buyer_stg B ON B.ID = BVP.VehicleID
   LEFT OUTER  JOIN psa.Address_stg AD ON AD.ID = BVP.BuyerDeliveryLocationID
   LEFT OUTER  JOIN psa.Country_stg CU ON  AD.CountryID = CU.ID
   LEFT OUTER  JOIN psa.UnitType_stg U ON U.ID = VI.UnitType;

