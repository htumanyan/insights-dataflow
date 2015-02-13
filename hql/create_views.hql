use psa;
CREATE TABLE vehicleinformationwithoptions_stg AS SELECT
 VI.*,
		OPT.OptionID,
		OPT.OptionRef,
		OPT.Standard AS StandardOption
	FROM VehicleInformation_stg VI
		LEFT OUTER JOIN Options_stg OPT
		ON OPT.VehicleInstanceID = VI.VehicleInstanceID;
INSERT INTO TABLE vehicleinformationwithoptions_stg SELECT 
 VI.*,
                OPT.OptionID,
                OPT.OptionRef,
                OPT.Standard AS StandardOption
	FROM VehicleInformation_stg VI
                LEFT OUTER JOIN Options_stg OPT
		ON  OPT.DerivativeID = VI.DerivativeID; 
!quit
