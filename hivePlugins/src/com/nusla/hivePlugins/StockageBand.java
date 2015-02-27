package com.nusla.hivePlugins;


import org.apache.hadoop.hive.ql.exec.UDF;

public class StockageBand extends UDF {
	 public int evaluate(long stockage)
	 {
		 if(stockage >= 0 && stockage <= 7) return 1;
		 if(stockage >= 8 && stockage <= 14) return 2;
		 if(stockage >= 15 && stockage <= 21) return 3;
		 if(stockage >= 22 && stockage <= 28) return 4;
		 if(stockage >= 29 && stockage <= 35) return 5;
		 if(stockage >= 36 && stockage <= 42) return 6;
		 if(stockage >= 43 && stockage <= 49) return 7;
		 if(stockage >= 50 && stockage <= 56) return 8;
		 if(stockage >= 57 && stockage <= 63) return 9;
		 if(stockage >= 64 && stockage <= 70) return 10;
		 if(stockage >= 71 && stockage <= 77) return 11;
		 if(stockage >= 78 && stockage <= 84) return 12;
		 if(stockage >= 85 && stockage <= 91) return 13;
		 if(stockage >= 92 && stockage <= 98) return 14;
		 if(stockage >= 99 && stockage <= 105) return 15;
		 if(stockage >= 106 && stockage <= 112) return 16;
		 if(stockage >= 113 && stockage <= 100000) return 17;
		 return -1;
	}
}

