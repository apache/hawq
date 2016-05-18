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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hawq.pxf.plugins.json.parser.JsonLexer.JsonLexerState;

/**
 * A simple parser that can support reading JSON objects from a random point in JSON text. It reads from the supplied
 * stream (which is assumed to be positioned at any arbitrary position inside some JSON text) until it finds the first
 * JSON begin-object "{". From this point on it will keep reading JSON objects until it finds one containing a member
 * string that the user supplies.
 * <p/>
 * It is not recommended to use this with JSON text where individual JSON objects that can be large (MB's or larger).
 */
public class PartitionedJsonParser {

	private static final char BACKSLASH = '\\';
	private static final char START_BRACE = '{';
	private static final int EOF = -1;
	private final InputStreamReader inputStreamReader;
	private final JsonLexer lexer;
	private long bytesRead = 0;
	private boolean endOfStream = false;

	public PartitionedJsonParser(InputStream is) {
		this.lexer = new JsonLexer();

		// You need to wrap the InputStream with an InputStreamReader, so that it can encode the incoming byte stream as
		// UTF-8 characters
		this.inputStreamReader = new InputStreamReader(is, StandardCharsets.UTF_8);
	}

	private boolean scanToFirstBeginObject() throws IOException {
		// seek until we hit the first begin-object
		char prev = ' ';
		int i;
		while ((i = inputStreamReader.read()) != EOF) {
			char c = (char) i;
			bytesRead++;
			if (c == START_BRACE && prev != BACKSLASH) {
				lexer.setState(JsonLexer.JsonLexerState.BEGIN_OBJECT);
				return true;
			}
			prev = c;
		}
		endOfStream = true;
		return false;
	}

	private enum MemberSearchState {
		FOUND_STRING_NAME,

		SEARCHING,

		IN_MATCHING_OBJECT
	}

	private static final EnumSet<JsonLexerState> inStringStates = EnumSet.of(JsonLexerState.INSIDE_STRING,
			JsonLexerState.STRING_ESCAPE);

	/**
	 * @param memberName
	 *            Indicates the member name used to determine the encapsulating object to return.
	 * @return Returns next json object that contains a member attribute with name: memberName. Returns null if no such
	 *         object is found or the end of the stream is reached.
	 * @throws IOException
	 */
	public String nextObjectContainingMember(String memberName) throws IOException {

		if (endOfStream) {
			return null;
		}

		int i;
		int objectCount = 0;
		StringBuilder currentObject = new StringBuilder();
		StringBuilder currentString = new StringBuilder();
		MemberSearchState memberState = MemberSearchState.SEARCHING;

		List<Integer> objectStack = new ArrayList<Integer>();

		if (!scanToFirstBeginObject()) {
			return null;
		}
		currentObject.append(START_BRACE);
		objectStack.add(0);

		while ((i = inputStreamReader.read()) != EOF) {
			char c = (char) i;
			bytesRead++;

			lexer.lex(c);

			currentObject.append(c);

			switch (memberState) {
			case SEARCHING:
				if (lexer.getState() == JsonLexerState.BEGIN_STRING) {
					// we found the start of a string, so reset our string buffer
					currentString.setLength(0);
				} else if (inStringStates.contains(lexer.getState())) {
					// we're still inside a string, so keep appending to our buffer
					currentString.append(c);
				} else if (lexer.getState() == JsonLexerState.END_STRING && memberName.equals(currentString.toString())) {

					if (objectStack.size() > 0) {
						// we hit the end of the string and it matched the member name (yay)
						memberState = MemberSearchState.FOUND_STRING_NAME;
						currentString.setLength(0);
					}
				} else if (lexer.getState() == JsonLexerState.BEGIN_OBJECT) {
					// we are searching and found a '{', so we reset the current object string
					if (objectStack.size() == 0) {
						currentObject.setLength(0);
						currentObject.append(START_BRACE);
					}
					objectStack.add(currentObject.length() - 1);
				} else if (lexer.getState() == JsonLexerState.END_OBJECT) {
					if (objectStack.size() > 0) {
						objectStack.remove(objectStack.size() - 1);
					}
					if (objectStack.size() == 0) {
						currentObject.setLength(0);
					}
				}
				break;
			case FOUND_STRING_NAME:
				// keep popping whitespaces until we hit a different token
				if (lexer.getState() != JsonLexerState.WHITESPACE) {
					if (lexer.getState() == JsonLexerState.NAME_SEPARATOR) {
						// found our member!
						memberState = MemberSearchState.IN_MATCHING_OBJECT;
						objectCount = 0;

						if (objectStack.size() > 1) {
							currentObject.delete(0, objectStack.get(objectStack.size() - 1));
						}
						objectStack.clear();
					} else {
						// we didn't find a value-separator (:), so our string wasn't a member string. keep searching
						memberState = MemberSearchState.SEARCHING;
					}
				}
				break;
			case IN_MATCHING_OBJECT:
				if (lexer.getState() == JsonLexerState.BEGIN_OBJECT) {
					objectCount++;
				} else if (lexer.getState() == JsonLexerState.END_OBJECT) {
					objectCount--;
					if (objectCount < 0) {
						// we're done! we reached an "}" which is at the same level as the member we found
						return currentObject.toString();
					}
				}
				break;
			}
		}
		endOfStream = true;
		return null;
	}

	/**
	 * @return Returns the number of bytes read from the stream.
	 */
	public long getBytesRead() {
		return bytesRead;
	}

	/**
	 * @return Returns true if the end of the stream has been reached and false otherwise.
	 */
	public boolean isEndOfStream() {
		return endOfStream;
	}
}