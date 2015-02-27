package com.nusla.hivePlugins;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MileageBand extends UDF {
	
	 public int evaluate(int mileage)
	 {
		 int band = 0;	
		 	if(mileage > 0 && mileage < 10000)
		 		band= 0;
		 	if(mileage >= 10001 && mileage <= 20000)
		 		band=1;
		 	if(mileage >= 20001 && mileage <= 30000)
		 		band= 2;
		 	if(mileage >= 30001 && mileage <= 60000)
		 		band= 3;
		 	if(mileage >= 60001 && mileage <= 90000)
		 		band= 4;
		 	if(mileage >= 120001 && mileage <= 160000)
		 		band= 5;
		 	if(mileage >= 160001 && mileage <= 180000)
		 		band= 6;
		 	if(mileage >= 180001)
		 		band= 7;
		 	return band;
		 	
		  }

}
