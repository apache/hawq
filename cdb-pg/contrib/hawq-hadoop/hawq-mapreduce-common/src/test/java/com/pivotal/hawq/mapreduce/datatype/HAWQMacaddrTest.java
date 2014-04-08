package com.pivotal.hawq.mapreduce.datatype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class HAWQMacaddrTest
{
	@Test
	public void testMacaddrCtorWithEnoughBytes()
	{
		byte[] bytes =
		{ (byte) 0xFF, (byte) 0xBB, 0x01, 0x22, (byte) 0x8C, (byte) 0x9F,
				(byte) 0x88 };
		HAWQMacaddr macaddr1 = new HAWQMacaddr(bytes);
		HAWQMacaddr macaddr2 = new HAWQMacaddr(bytes, 1);
		HAWQMacaddr macaddr3 = new HAWQMacaddr(bytes, 3);
		assertTrue(macaddr1.toString().toUpperCase().equals("FF:BB:01:22:8C:9F"));
		assertTrue(macaddr2.toString().toUpperCase().equals("BB:01:22:8C:9F:88"));
		assertTrue(macaddr3.toString().toUpperCase().equals("22:8C:9F:88:00:00"));
	}

	@Test
	public void testMacaddrEquals() throws Exception {
		HAWQMacaddr m1 = new HAWQMacaddr(new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6});
		HAWQMacaddr m2 = new HAWQMacaddr(new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6});
		HAWQMacaddr m3 = new HAWQMacaddr(new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x7});

		assertEquals(m1, m2);
		assertEquals(m2, m1);

		assertFalse(m1.equals(m3));
		assertFalse(m2.equals(m3));
	}
}
