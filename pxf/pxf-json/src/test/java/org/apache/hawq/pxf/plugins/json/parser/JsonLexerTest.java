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
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class JsonLexerTest {

	private static final Log LOG = LogFactory.getLog(JsonLexerTest.class);

	@Test
	public void testSimple() throws IOException {
		File testsDir = new File("src/test/resources/lexer-tests");
		File[] jsonFiles = testsDir.listFiles(new FilenameFilter() {
			public boolean accept(File file, String s) {
				return s.endsWith(".json");
			}
		});

		for (File jsonFile : jsonFiles) {
			File stateFile = new File(jsonFile.getAbsolutePath() + ".state");
			if (stateFile.exists()) {
				runTest(jsonFile, stateFile);
			}
		}
	}

	public static Pattern STATE_RECURRENCE = Pattern.compile("^([A-Za-z\\_0-9]+)\\{([0-9]+)\\}$");

	public void runTest(File jsonFile, File stateFile) throws IOException {
		List<String> lexerStates = FileUtils.readLines(stateFile);
		InputStream jsonInputStream = new FileInputStream(jsonFile);

		try {
			JsonLexer lexer = new JsonLexer();

			int byteOffset = 0;
			int i;
			ListIterator<String> stateIterator = lexerStates.listIterator();
			int recurrence = 0;
			JsonLexer.JsonLexerState expectedState = null;
			StringBuilder sb = new StringBuilder();
			int stateFileLineNum = 0;
			while ((i = jsonInputStream.read()) != -1) {
				byteOffset++;
				char c = (char) i;

				sb.append(c);

				lexer.lex(c);

				if (lexer.getState() == JsonLexer.JsonLexerState.WHITESPACE) {
					// optimization to skip over multiple whitespaces
					continue;
				}

				if (!stateIterator.hasNext()) {
					assertFalse(formatStateInfo(jsonFile, sb.toString(), byteOffset, stateFileLineNum)
							+ ": Input stream had character '" + c + "' but no matching state", true);
				}

				if (recurrence <= 0) {
					String state = stateIterator.next().trim();
					stateFileLineNum++;

					while (state.equals("") || state.startsWith("#")) {
						if (!stateIterator.hasNext()) {
							assertFalse(formatStateInfo(jsonFile, sb.toString(), byteOffset, stateFileLineNum)
									+ ": Input stream had character '" + c + "' but no matching state", true);
						}
						state = stateIterator.next().trim();
						stateFileLineNum++;
					}

					Matcher m = STATE_RECURRENCE.matcher(state);
					recurrence = 1;
					if (m.matches()) {
						state = m.group(1);
						recurrence = Integer.valueOf(m.group(2));
					}
					expectedState = JsonLexer.JsonLexerState.valueOf(state);
				}

				assertEquals(formatStateInfo(jsonFile, sb.toString(), byteOffset, stateFileLineNum)
						+ ": Issue for char '" + c + "'", expectedState, lexer.getState());
				recurrence--;
			}

			if (stateIterator.hasNext()) {
				assertFalse(formatStateInfo(jsonFile, sb.toString(), byteOffset, stateFileLineNum)
						+ ": Input stream has ended but more states were expected: '" + stateIterator.next() + "...'",
						true);
			}

		} finally {
			IOUtils.closeQuietly(jsonInputStream);
		}

		LOG.info("File " + jsonFile.getName() + " passed");

	}

	static String formatStateInfo(File jsonFile, String streamContents, int streamByteOffset, int stateFileLineNum) {
		return jsonFile.getName() + ": Input stream currently at byte-offset " + streamByteOffset + ", contents = '"
				+ streamContents + "'" + " state-file line = " + stateFileLineNum;
	}
}