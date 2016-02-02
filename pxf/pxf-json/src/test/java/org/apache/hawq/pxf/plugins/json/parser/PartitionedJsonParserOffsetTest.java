package org.apache.hawq.pxf.plugins.json.parser;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

public class PartitionedJsonParserOffsetTest {
	/*
	 * [{"color": "red","v": "vv"},{"color": "red","v": "vv"}]
	 */
	public static String json2 = "[{\"color\": \"red\",\"v\": \"vv\"},{\"color\": \"red\",\"v\": \"vv\"}]";

	@Test
	public void testOffset() throws IOException {
		InputStream jsonInputStream = createFromString(json2);
		PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);
		String result = parser.nextObjectContainingMember("color");
		assertNotNull(result);
		assertEquals(27, parser.getBytesRead());
		assertEquals(1, parser.getBytesRead() - result.length());
		result = parser.nextObjectContainingMember("color");
		assertNotNull(result);
		assertEquals(54, parser.getBytesRead());
		assertEquals(28, parser.getBytesRead() - result.length());
	}

	public InputStream createFromString(String s) {
		return new ByteArrayInputStream(s.getBytes());
	}
}
