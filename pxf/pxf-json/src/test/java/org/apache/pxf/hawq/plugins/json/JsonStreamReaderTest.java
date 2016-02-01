package org.apache.pxf.hawq.plugins.json;

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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hawq.pxf.plugins.json.JsonStreamReader;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Before;
import org.junit.Test;

public class JsonStreamReaderTest {

	private JsonStreamReader rdr = null;

	private static final String FILE = "src/test/resources/sample.json";

	@Before
	public void setup() throws FileNotFoundException {
		rdr = new JsonStreamReader("menuitem", new FileInputStream(FILE));
	}

	@After
	public void after() throws IOException {
		rdr.close();
	}

	@Test
	public void testReadRecords() throws IOException {
		assertEquals("{\"key1\":\"Foo\"}", rdr.getJsonRecord());
		assertEquals("{\"key1\":\"Bar\"}", rdr.getJsonRecord());
		assertEquals("{\"key1\":\"Baz\"}", rdr.getJsonRecord());
		assertEquals(null, rdr.getJsonRecord());
	}

	@Test
	public void testReadMalfomredJson() throws IOException {
		JsonStreamReader rdr2 = new JsonStreamReader("menuitem", new FileInputStream("src/test/resources/sample-malformed.json"));
		assertEquals("{\"key1\":\"Foo\"}", rdr2.getJsonRecord());
		assertNotEquals("{\"key1\":\"Bar\"}", rdr2.getJsonRecord());
		rdr2.close();
	}
}