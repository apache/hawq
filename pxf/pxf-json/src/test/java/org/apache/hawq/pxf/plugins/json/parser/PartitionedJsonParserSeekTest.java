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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class PartitionedJsonParserSeekTest {

	private static final Log LOG = LogFactory.getLog(PartitionedJsonParserSeekTest.class);

	@Test
	public void testNoSeek() throws IOException {
		File testsDir = new File("src/test/resources/parser-tests/seek");
		File[] dirs = testsDir.listFiles();

		for (File jsonDir : dirs) {
			runTest(jsonDir);
		}
	}

	public void runTest(final File jsonDir) throws IOException {

		File jsonFile = new File(jsonDir, "input.json");
		InputStream jsonInputStream = new FileInputStream(jsonFile);

		try {
			seekToStart(jsonInputStream);
			PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);

			File[] jsonOjbectFiles = jsonFile.getParentFile().listFiles(new FilenameFilter() {
				public boolean accept(File file, String s) {
					return s.contains("expected");
				}
			});

			Arrays.sort(jsonOjbectFiles, new Comparator<File>() {
				public int compare(File file, File file1) {
					return file.compareTo(file1);
				}
			});

			if (jsonOjbectFiles == null || jsonOjbectFiles.length == 0) {
				String result = parser.nextObjectContainingMember("name");
				assertNull("File " + jsonFile.getAbsolutePath() + " got result '" + result + "'", result);
				LOG.info("File " + jsonFile.getAbsolutePath() + " passed");
			} else {
				for (File jsonObjectFile : jsonOjbectFiles) {
					String expected = trimWhitespaces(FileUtils.readFileToString(jsonObjectFile));
					String result = parser.nextObjectContainingMember("name");
					assertNotNull(jsonFile.getAbsolutePath() + "/" + jsonObjectFile.getName(), result);
					assertEquals(jsonFile.getAbsolutePath() + "/" + jsonObjectFile.getName(), expected,
							trimWhitespaces(result));
					LOG.info("File " + jsonFile.getAbsolutePath() + "/" + jsonObjectFile.getName() + " passed");
				}
			}

		} finally {
			IOUtils.closeQuietly(jsonInputStream);
		}
	}

	public void seekToStart(InputStream jsonInputStream) throws IOException {
		// pop off characters until we see <SEEK>
		StringBuilder sb = new StringBuilder();
		int i;
		while ((i = jsonInputStream.read()) != -1) {
			sb.append((char) i);

			if (sb.toString().endsWith("<SEEK>")) {
				return;
			}
		}
		assertTrue(false);
	}

	public String trimWhitespaces(String s) {
		return s.replaceAll("[\\n\\t\\r \\t]+", " ").trim();
	}
}
