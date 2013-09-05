package com.pivotal.hawq.mapreduce.schema;

import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField.PrimitiveType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * User: gaod1
 * Date: 8/26/13
 */
public class HAWQSchema extends HAWQGroupField {

	public HAWQSchema(String name, HAWQField... fields) {
		this(name, Arrays.asList(fields));
	}

	public HAWQSchema(String name, List<HAWQField> fields) {
		super(false, false, name, null, fields);
	}

	@Override
	public void writeToStringBuilder(StringBuilder sb, String indent) {
		sb.append("message ").append(getName()).append(" {\n");
		writeMembersToStringBuilder(sb, indent);
		sb.append("\n}");
	}

	/***
	 * Build HAWQSchema from schema string, inverse procedure of toString.
	 * @param schemaString
	 * @return schema constructed from string
	 */
	public static HAWQSchema fromString(String schemaString) {
		SchemaParser parser = new SchemaParser(new SchemaLexer(schemaString));
		return parser.parse();
	}

	//-------------------------------------------------------------------------
	// Parser class for HAWQSchema
	//-------------------------------------------------------------------------

	private static class SchemaLexer {
		private StringTokenizer tokenizer;
		private int lineNumber;
		private StringBuilder currentLine;

		public SchemaLexer(String schemaString) {
			tokenizer = new StringTokenizer(schemaString, " \t\n:{}[]();", /* returnDelims= */true);
			lineNumber = 1;
			currentLine = new StringBuilder();
		}

		public String nextToken() {
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (token.equals("\n")) {
					lineNumber++;
					currentLine.setLength(0);
				} else {
					currentLine.append(token);
				}
				if (token.equals(" ") || token.equals("\t") || token.equals("\n"))
					continue; // ignore whitespaces
				return token;
			}
			throw new IllegalArgumentException("unexpected end of schema");
		}

		public boolean hasMoreTokens() {
			return tokenizer.hasMoreTokens();
		}

		public String getLocationString() {
			return "line " + lineNumber + ":" + currentLine;
		}
	}

	private static class SchemaParser {
		private SchemaLexer lexer;

		public SchemaParser(SchemaLexer lexer) {
			this.lexer = lexer;
		}

		public HAWQSchema parse() {
			HAWQSchema schema = readSchema(lexer.nextToken());
			if (lexer.hasMoreTokens())
				throw new IllegalArgumentException("extra data on line " + lexer.lineNumber);
			return schema;
		}

		/**
		 * schema := 'message' <schemaName> '{' fields '}'
		 * fields := (field)*
		 */
		private HAWQSchema readSchema(String token) {
			matchToken(token, "message", "start with 'message'");
			String schemaName = lexer.nextToken();
			matchToken(lexer.nextToken(), "{");

			List<HAWQField> fields = new ArrayList<HAWQField>();
			while (!(token = lexer.nextToken()).equals("}")) {
				fields.add(readField(token));
			}

			matchToken(token, "}");
			return new HAWQSchema(schemaName, fields);
		}

		/**
		 * field := ('required' | 'optional') (primitive_field | group_field)
		 */
		private HAWQField readField(String token) {
			if (!token.equals("required") && !token.equals("optional"))
				throw new IllegalArgumentException(String.format(
						"missing 'required' or 'optional' keyword for field definition, found '%s' at %s",
						token, lexer.getLocationString()));

			boolean isOptional = token.equals("optional");
			token = lexer.nextToken();

			if (token.equals("group"))
				return readGroupField(isOptional, token);
			return readPrimitiveField(isOptional, token);
		}

		/**
		 * group_field := 'group' ('[]')? <groupName> ('('<dataTypeName>')')? '{' fields '}'
		 */
		private HAWQField readGroupField(boolean isOptional, String token) {
			boolean isArray = false;
			String groupName = null;
			String dataTypeName = null;

			matchToken(token, "group");
			token = lexer.nextToken();

			if (token.equals("[")) {
				matchToken(lexer.nextToken(), "]");
				isArray = true;
				token = lexer.nextToken();
			}
			groupName = token;

			token = lexer.nextToken();
			if (token.equals("(")) {
				dataTypeName = lexer.nextToken();
				matchToken(lexer.nextToken(), ")", "datatype name for group");
				token = lexer.nextToken();
			}
			matchToken(token, "{", "start of group");

			List<HAWQField> fields = new ArrayList<HAWQField>();
			while (!(token = lexer.nextToken()).equals("}")) {
				fields.add(readField(token));
			}
			matchToken(token, "}");

			return new HAWQGroupField(isOptional, isArray, groupName, dataTypeName, fields);
		}

		/**
		 * primitive_field := <primitive_field_type> ('[]')? <fieldName> ';'
		 */
		private HAWQField readPrimitiveField(boolean isOptional, String token) {
			PrimitiveType type = null;
			boolean isArray = false;
			String fieldName = null;

			try {
				type = PrimitiveType.valueOf(token.toUpperCase());
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(String.format(
						"unsupported primitive field type '%s' at %s", token, lexer.getLocationString()
				));
			}

			token = lexer.nextToken();
			if (token.equals("[")) {
				matchToken(lexer.nextToken(), "]");
				isArray = true;
				token = lexer.nextToken();
			}
			fieldName = token;
			matchToken(lexer.nextToken(), ";", "primitive field should end with ';'");

			return new HAWQPrimitiveField(isOptional, fieldName, type, isArray);
		}

		private void matchToken(String token, String expect) {
			matchToken(token, expect, null);
		}

		private void matchToken(String token, String expect, String message) {
			if (!token.equals(expect)) {
				if (message == null || "".equals(message.trim()))
					throw new IllegalArgumentException(String.format(
							"expect '%s', but found '%s' at %s", expect, token, lexer.getLocationString()
					));
				else
					throw new IllegalArgumentException(String.format(
							"%s: expect '%s', but found '%s' at %s", message, expect, token, lexer.getLocationString()
					));
			}
		}
	}

	//-------------------------------------------------------------------------
	// Factory methods to create HAWQField
	//-------------------------------------------------------------------------

	public static HAWQField required_field(PrimitiveType type, String name) {
		return new HAWQPrimitiveField(false, name, type, false);
	}

	public static HAWQField optional_field(PrimitiveType type, String name) {
		return new HAWQPrimitiveField(true, name, type, false);
	}

	public static HAWQField required_field_array(PrimitiveType type, String name) {
		return new HAWQPrimitiveField(false, name, type, true);
	}

	public static HAWQField optional_field_array(PrimitiveType type, String name) {
		return new HAWQPrimitiveField(true, name, type, true);
	}

	public static HAWQField required_group(String name, String dataTypeName, HAWQField... fields) {
		return new HAWQGroupField(false, false, name, dataTypeName, fields);
	}

	public static HAWQField optional_group(String name, String dataTypeName, HAWQField... fields) {
		return new HAWQGroupField(true, false, name, dataTypeName, fields);
	}

	public static HAWQField required_group_array(String name, String dataTypeName, HAWQField... fields) {
		return new HAWQGroupField(false, true, name, dataTypeName, fields);
	}

	public static HAWQField optional_group_array(String name, String dataTypeName, HAWQField... fields) {
		return new HAWQGroupField(true, true, name, dataTypeName, fields);
	}
}
