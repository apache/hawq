package com.pivotal.hawq.mapreduce.schema;


import org.junit.Test;

import static com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField.PrimitiveType.*;
import static com.pivotal.hawq.mapreduce.schema.HAWQSchema.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * User: gaod1
 * Date: 8/27/13
 */
public class HAWQSchemaTest {

	private HAWQSchema personSchema = new HAWQSchema("person",
			required_field(VARCHAR, "name"),
			required_field(INT2, "age"),
			optional_group("home_addr", "address",
					required_field(VARCHAR, "street"),
					required_field(VARCHAR, "city"),
					required_field(VARCHAR, "state"),
					required_field(INT4, "zip")),
			required_field_array(VARCHAR, "tags"));

	private String personSchemaString =
			"message person {\n"
					+ "  required varchar name;\n"
					+ "  required int2 age;\n"
					+ "  optional group home_addr (address) {\n"
					+ "    required varchar street;\n"
					+ "    required varchar city;\n"
					+ "    required varchar state;\n"
					+ "    required int4 zip;\n"
					+ "  }\n"
					+ "  required varchar[] tags;\n"
					+ "}";

	@Test
	public void testSchemaToString() {
		assertThat(personSchema.toString(), is(personSchemaString));
	}

	@Test
	public void testParseSchema() {
		assertThat(HAWQSchema.fromString(personSchemaString), is(personSchema));
	}
}
