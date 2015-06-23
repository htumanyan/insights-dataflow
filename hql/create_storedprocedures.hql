use psa_shark;
drop table if exists buyerpremiumcharge_stg;
CREATE TABLE  buyerpremiumcharge_stg as SELECT  VC.VehicleInstanceID as vehicleinstanceid,  VC.Price + VC.Vat AS psa.buyerpremiumcharge 
FROM  psa.VehicleCharges_stg VC
   join psa.VehicleChargeTypes_stg VT ON VC.VehicleChargeTypeID=VT.ID
 WHERE VT.ID=2;

drop table if exists getdeliverycharges;
CREATE TABLE getdeliverycharges as SELECT  VC.VehicleInstanceID as vehicleinstanceid,  VC.Price + VC.Vat AS psa.deliverycharges
 FROM  psa.VehicleCharges_stg VC
   join VehicleChargeTypes_stg VT ON VC.VehicleChargeTypeID=VT.ID
 WHERE VT.ID=1;
