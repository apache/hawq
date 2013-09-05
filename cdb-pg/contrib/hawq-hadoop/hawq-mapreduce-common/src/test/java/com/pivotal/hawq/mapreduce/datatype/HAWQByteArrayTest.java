package com.pivotal.hawq.mapreduce.datatype;

import static org.junit.Assert.assertTrue;

import org.junit.Test;


public class HAWQByteArrayTest
{
	@Test
	public void testByteArrayCtor()
	{
		byte[] bytes = {'a','b','c','d'};
		HAWQByteArray varbit1 = new HAWQByteArray(bytes);
		assertTrue(varbit1.toString().equals("abcd"));
		HAWQByteArray varbit2 = new HAWQByteArray(bytes, 1, 2);
		assertTrue(varbit2.toString().equals("bc"));
	}

}
