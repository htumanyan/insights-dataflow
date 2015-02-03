use psa_shark;
drop table if exists rpt_psa_buyer_activity_report_cached;
CREATE TABLE IF NOT EXISTS rpt_psa_buyer_activity_report_cached  
  AS SELECT  CASE WHEN BU.UserID IS NOT NULL THEN 1 ELSE 0 END AS BuyerUser,
  B.BuyerCode,
  B.Name,
  BR.BuyerRegionName,
  V.TradingName,
  BVP.*
 FROM
  Buyervehiclepurchase_stg_cached BVP
  INNER JOIN Buyer_stg_cached B ON B.ID = BVP.BUYERID and BVP.IsActive = 1
  LEFT OUTER JOIN  BuyerRegion_stg_cached BR ON B.BuyerRegionID = BR.BuyerRegionID
  LEFT OUTER JOIN  Buyerusers_stg_cached BU ON BVP.buyerid = BU.buyerid and BVP.createduserid = BU.userid
  INNER JOIN VendorBuyers_stg_cached VB ON VB.BuyerID = B.ID
  INNER JOIN Vendor_stg_cached V ON VB.VendorID = V.ID;
quit;
