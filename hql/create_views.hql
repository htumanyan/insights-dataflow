!connect ${connectString}/psa_shark dummy dummy org.apache.hive.jdbc.HiveDriver
use psa;
DROP TABLE if EXISTS vehicleinformationwithoptions_stg;
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
