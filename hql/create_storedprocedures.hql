!connect ${connectString}/psa dummy dummy org.apache.hive.jdbc.HiveDriver
use psa;
drop table buyerpremiumcharge_stg;
CREATE TABLE  buyerpremiumcharge_stg as SELECT  VC.VehicleInstanceID as vehicleinstanceid,  VC.Price + VC.Vat AS buyerpremiumcharge 
FROM  VehicleCharges_stg VC
   join VehicleChargeTypes_stg VT ON VC.VehicleChargeTypeID=VT.ID
 WHERE VT.ID=2;

drop table getdeliverycharges;
CREATE TABLE getdeliverycharges as SELECT  VC.VehicleInstanceID as vehicleinstanceid,  VC.Price + VC.Vat AS deliverycharges
 FROM  VehicleCharges_stg VC
   join VehicleChargeTypes_stg VT ON VC.VehicleChargeTypeID=VT.ID
 WHERE VT.ID=1;
!quit
