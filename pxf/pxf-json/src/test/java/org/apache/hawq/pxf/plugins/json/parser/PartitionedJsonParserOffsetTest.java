package org.apache.hawq.pxf.plugins.json.parser;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
		jsonInputStream.close();
	}

	public InputStream createFromString(String s) {
		return new ByteArrayInputStream(s.getBytes());
	}
}
