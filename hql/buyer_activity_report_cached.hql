CREATE TABLE buyer_activity_report_cached AS
SELECT U.id,
        U.username,
        U.createddate,
        U.loggedindate,
        Buyer.id,
        Buyer.name,
        BuyerType.BuyerTypeId,
        BuyerType.BuyerTypeName
from psa.useraudit_stg U
join psa.BuyerUsers_stg BU ON BU.userid=U.id
join psa.Buyer_stg Buyer ON Buyer.Id = BU.buyerid
LEFT OUTER  JOIN psa.BuyerType_stg BuyerType ON Buyer.BuyerTypeId = BuyerType.BuyerTypeId;
 
