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

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class PartitionedJsonParserNoSeekTest {

	private static final Log LOG = LogFactory.getLog(PartitionedJsonParserNoSeekTest.class);

	@Test
	public void testNoSeek() throws IOException {
		File testsDir = new File("src/test/resources/parser-tests/noseek");
		File[] jsonFiles = testsDir.listFiles(new FilenameFilter() {
			public boolean accept(File file, String s) {
				return s.endsWith(".json");
			}
		});

		for (File jsonFile : jsonFiles) {
			runTest(jsonFile);
		}
	}

	public void runTest(final File jsonFile) throws IOException {
		InputStream jsonInputStream = new FileInputStream(jsonFile);

		try {
			PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);

			File[] jsonOjbectFiles = jsonFile.getParentFile().listFiles(new FilenameFilter() {
				public boolean accept(File file, String s) {
					return s.contains(jsonFile.getName()) && s.contains("expected");
				}
			});

			for (File jsonObjectFile : jsonOjbectFiles) {
				String expected = trimWhitespaces(FileUtils.readFileToString(jsonObjectFile));
				String result = parser.nextObjectContainingMember("name");
				assertNotNull(jsonFile.getName() + "/" + jsonObjectFile.getName(), result);
				assertEquals(jsonFile.getName() + "/" + jsonObjectFile.getName(), expected, trimWhitespaces(result));
				LOG.info("File " + jsonFile.getName() + "/" + jsonObjectFile.getName() + " passed");
			}

		} finally {
			IOUtils.closeQuietly(jsonInputStream);
		}
	}

	public String trimWhitespaces(String s) {
		return s.replaceAll("[\\n\\t\\r \\t]+", " ").trim();
	}
}
