package com.nusla.hivePlugins;


import org.apache.hadoop.hive.ql.exec.UDF;

public class NetDamagesBand extends UDF {
	 public int evaluate(long netdamages)
	 {
		 if(netdamages >= 0 && netdamages <= 99) return 1;
		 if(netdamages >= 100 && netdamages <= 500) return 2;
		 if(netdamages >= 501 && netdamages <= 750) return 3;
		 if(netdamages >= 751 && netdamages <= 100000) return 4;
		 return -1;
	}
}

