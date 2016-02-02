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

/**
 * A very loosey-goosey lexer that doesn't enforce any JSON structural rules
 */
public class JsonLexer {

	/**
	 * The current Lexer state.
	 */
	private JsonLexerState state;

	/**
	 * Create a new lexer with {@link JsonLexerState#NULL} initial state
	 */
	public JsonLexer() {
		this(JsonLexerState.NULL);
	}

	/**
	 * Create a new lexer with initial state
	 * 
	 * @param initState
	 *            Lexer initial state
	 */
	public JsonLexer(JsonLexerState initState) {
		state = initState;
	}

	/**
	 * @return current lexer state
	 */
	public JsonLexerState getState() {
		return state;
	}

	/**
	 * Change Lexer's {@link #state}
	 * 
	 * @param state
	 *            New lexer state
	 */
	public void setState(JsonLexerState state) {
		this.state = state;
	}

	/**
	 * Represents the possible states of a cursor can take in a JSON document.
	 */
	public static enum JsonLexerState {
		NULL,

		DONT_CARE,

		BEGIN_OBJECT,

		END_OBJECT,

		BEGIN_STRING,

		END_STRING,

		INSIDE_STRING,

		STRING_ESCAPE,

		VALUE_SEPARATOR,

		NAME_SEPARATOR,

		BEGIN_ARRAY,

		END_ARRAY,

		WHITESPACE
	}

	/**
	 * Given the current lexer state and the next cursor position computes the next {@link #state}.
	 * 
	 * @param c
	 *            next character the cursor is moved to
	 */
	public void lex(char c) {
		switch (state) {
		case NULL:
		case BEGIN_OBJECT:
		case END_OBJECT:
		case BEGIN_ARRAY:
		case END_ARRAY:
		case END_STRING:
		case VALUE_SEPARATOR:
		case NAME_SEPARATOR:
		case DONT_CARE:
		case WHITESPACE: {
			if (Character.isWhitespace(c)) {
				state = JsonLexerState.WHITESPACE;
				break;
			}
			switch (c) {
			// value-separator (comma)
			case ',':
				state = JsonLexerState.VALUE_SEPARATOR;
				break;
			// name-separator (colon)
			case ':':
				state = JsonLexerState.NAME_SEPARATOR;
				break;
			// string
			case '"':
				state = JsonLexerState.BEGIN_STRING;
				break;
			// start-object
			case '{':
				state = JsonLexerState.BEGIN_OBJECT;
				break;
			// end-object
			case '}':
				state = JsonLexerState.END_OBJECT;
				break;
			// begin-array
			case '[':
				state = JsonLexerState.BEGIN_ARRAY;
				break;
			// end-array
			case ']':
				state = JsonLexerState.END_ARRAY;
				break;
			default:
				state = JsonLexerState.DONT_CARE;
			}
			break;
		}
		case BEGIN_STRING:
		case INSIDE_STRING: {
			state = JsonLexerState.INSIDE_STRING;
			// we will now enter the STRING state below

			switch (c) {
			// end-string
			case '"':
				state = JsonLexerState.END_STRING;
				break;
			// escape
			case '\\':
				state = JsonLexerState.STRING_ESCAPE;
			}
			break;
		}
		case STRING_ESCAPE: {
			state = JsonLexerState.INSIDE_STRING;
			break;
		}
		}
	}
}