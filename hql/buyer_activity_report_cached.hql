CREATE TABLE buyer_activity_report_cached AS
SELECT User.id,
        User.username,
        User.createddate,
        User.loggedindate,
        Buyer.id,
        Buyer.name,
        BuyerType.BuyerTypeId,
        BuyerType.BuyerTypeName
from psa.useraudit_stg User
join psa.BuyerUsers_stg ON BuyerUsers.userid=Buyer.id
join psa.Buyer_stg ON Buyer.Id = BuyerUsers.buyerid
LEFT OUTER  JOIN psa.BuyerType_stg BuyerType ON Buyer.BuyerTypeId = BuyerType.BuyerTypeId;
 
