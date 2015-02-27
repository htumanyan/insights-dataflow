package com.nusla.hivePlugins;

import static org.junit.Assert.*;

import org.junit.Test;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;;

public class MileageBandTest {

	@Test
	public void test() {
		MileageBand b = new MileageBand();
		assertTrue(UDF.class.isAssignableFrom(b.getClass()));
		System.out.println(FunctionUtils.getUDFClassType(b.getClass()));
	}

}
