package org.apache.hawq.pxf.plugins.json;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Reads a JSON stream and sequentially extracts all JSON objects identified by the <b>identifier</b> parameter. Returns
 * null when there are no more objects to read.
 *
 */
public class JsonStreamReader extends BufferedReader {

	private static final Log LOG = LogFactory.getLog(JsonStreamReader.class);

	private static char START_BRACE = '{';
	private static char END_BRACE = '}';
	private static int EOF = -1;

	private StringBuilder bldr = new StringBuilder();
	private String identifier = null;
	private long bytesRead = 0;

	/**
	 * @param identifier
	 *            JSON object name to extract.
	 * @param inputStream
	 *            JSON document input stream.
	 */
	public JsonStreamReader(String identifier, InputStream inputStream) {
		super(new InputStreamReader(inputStream));

		if (isEmpty(identifier)) {
			throw new java.lang.IllegalArgumentException("The X-GP-IDENTIFIER paramter is not set!");
		}

		this.identifier = identifier;
	}

	/**
	 * @return Returns next JSON object identified by the {@link JsonStreamReader#identifier} parameter or Null if no
	 *         more objects are available.
	 * @throws IOException
	 */
	public String getJsonRecord() throws IOException {
		bldr.delete(0, bldr.length());

		boolean foundRecord = false;

		int c = 0, numBraces = 1;
		while ((c = super.read()) != EOF) {
			++bytesRead;
			if (!foundRecord) {
				bldr.append((char) c);

				if (bldr.toString().contains(identifier)) {
					if (forwardToStartBrace()) {
						foundRecord = true;

						bldr.delete(0, bldr.length());
						bldr.append(START_BRACE);
					}
				}
			} else {
				bldr.append((char) c);

				if (c == START_BRACE) {
					++numBraces;
				} else if (c == END_BRACE) {
					--numBraces;
				}

				if (numBraces == 0) {
					break;
				}
			}
		}

		if (foundRecord && numBraces == 0) {
			return bldr.toString();
		} else {
			if (bldr.length() > 0) {
				LOG.error("Incomplete JSON object starting with: " + bldr.toString());
			}
			return null;
		}
	}

	/**
	 * Moves the {@link #bytesRead} cursor to the next open brace character ('{') in the stream.
	 * 
	 * @return true iff an open brace has been found and false otherwise.
	 * @throws IOException
	 */
	private boolean forwardToStartBrace() throws IOException {
		int c;
		do {
			c = super.read();
			++bytesRead; // count number of read bytes for exit condition
		} while (c != START_BRACE && c != EOF);

		return (c == START_BRACE);
	}

	/**
	 * @return number of bytes read from the inputStream so far.
	 */
	public long getBytesRead() {
		return bytesRead;
	}
}