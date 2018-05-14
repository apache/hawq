package org.apache.hawq.pxf.api;

import static org.junit.Assert.*;

import java.net.InetAddress;
import org.junit.Test;

public class FragmentTest {

	@Test
	public void testEquals() throws Exception {
		String dataSource = "/pxf-s3-devel/test-write";
		String pxfHost = InetAddress.getLocalHost().getHostAddress();
		String[] hosts = new String[] { pxfHost };
		String key = "test-write/1524496731-0000002514_5";
		Fragment f1 = new Fragment(dataSource, hosts, key.getBytes());
		Fragment f2 = new Fragment(dataSource, hosts, key.getBytes());
		assertTrue(f2.equals(f1));
	}

}
