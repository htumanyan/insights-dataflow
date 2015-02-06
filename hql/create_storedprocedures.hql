use psa;
drop table buyerpremiumcharge_stg;
CREATE TABLE IF NOT EXISTS buyerpremiumcharge_stg as SELECT  VC.VehicleInstanceID as vehicleinstanceid,  VC.Price + VC.Vat AS buyerpremiumcharge
	FROM 	VehicleCharges_stg VC
			join VehicleChargeTypes_stg VT ON VC.VehicleChargeTypeID=VT.ID
	WHERE VT.ID=2;

drop table getdeliverycharges;
CREATE TABLE IF NOT EXISTS getdeliverycharges as SELECT  VC.VehicleInstanceID as vehicleinstanceid,  VC.Price + VC.Vat AS deliverycharges
	FROM 	VehicleCharges_stg VC
			join VehicleChargeTypes_stg VT ON VC.VehicleChargeTypeID=VT.ID
	WHERE VT.ID=1;
