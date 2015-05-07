use psa_shark;
SET spark.sql.shuffle.partitions=6;
CREATE TABLE buyer_activity_report_cached_tmp AS
SELECT U.id,
        U.username,
        U.createddate,
        U.loggedindate,
        Buyer.id as buyerid,
        Buyer.name as buyername,
        BuyerType.BuyerTypeId,
        BuyerType.BuyerTypeName
from psa.useraudit_stg U
join psa.BuyerUsers_stg BU ON BU.userid=U.id
join psa.Buyer_stg Buyer ON Buyer.Id = BU.buyerid
LEFT OUTER  JOIN psa.BuyerType_stg BuyerType ON Buyer.BuyerTypeId = BuyerType.BuyerTypeId;
DROP TABLE IF EXISTS buyer_activity_report_cached;
alter table buyer_activity_report_cached_tmp rename to buyer_activity_report_cached;
uncache table buyer_activity_report_cached;
cache table buyer_activity_report_cached;
SET spark.sql.shuffle.partitions=1;
