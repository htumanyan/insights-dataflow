CREATE TABLE buyer_activity_report_cached AS
SELECT UserAudit.id,
        User.username,
        User.createddate,
        User.loggedindate,
        Buyer.id,
        Buyer.name,
        BuyerType.BuyerTypeId,
        BuyerType.BuyerTypeName,
from psa.useraudit_stg
inner join BuyerUsers ON BuyerUsers.userid=Buyer.id
inner join Buyer ON Buyer.Id = BuyerUsers.buyerid
LEFT OUTER  JOIN psa.BuyerType_stg BuyerType ON Buyer.BuyerTypeId = BuyerType.BuyerTypeId;
 
