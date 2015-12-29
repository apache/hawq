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

public class JsonStreamReader extends BufferedReader {

	private StringBuilder bldr = new StringBuilder();
	private String identifier = null;
	private long bytesRead = 0;

	public JsonStreamReader(String identifier, InputStream strm) {
		super(new InputStreamReader(strm));
		this.identifier = identifier;
	}

	public String getJsonRecord() throws IOException {
		bldr.delete(0, bldr.length());

		boolean foundRecord = false;

		int c = 0, numBraces = 1;
		while ((c = super.read()) != -1) {
			++bytesRead;
			if (!foundRecord) {
				bldr.append((char) c);

				if (bldr.toString().contains(identifier)) {
					forwardToBrace();
					foundRecord = true;

					bldr.delete(0, bldr.length());
					bldr.append('{');
				}
			} else {
				bldr.append((char) c);

				if (c == '{') {
					++numBraces;
				} else if (c == '}') {
					--numBraces;
				}

				if (numBraces == 0) {
					break;
				}
			}
		}

		if (foundRecord) {
			return bldr.toString();
		} else {
			return null;
		}
	}

	private void forwardToBrace() throws IOException {
		int c;
		do {
			c = super.read();
			++bytesRead; // count number of read bytes for exit condition
		} while (c != '{' && c != -1);
	}

	public long getBytesRead() {
		return bytesRead;
	}
}