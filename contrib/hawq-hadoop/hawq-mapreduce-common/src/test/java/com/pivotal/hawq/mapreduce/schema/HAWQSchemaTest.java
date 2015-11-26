package com.pivotal.hawq.mapreduce.schema;

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



import org.junit.Test;

import static com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField.PrimitiveType.*;
import static com.pivotal.hawq.mapreduce.schema.HAWQSchema.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
