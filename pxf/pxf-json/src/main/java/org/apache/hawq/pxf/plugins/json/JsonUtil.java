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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Utility that helps to convert JSON text into {@link JsonNode} object
 *
 */
public class JsonUtil {

	private static final Log LOG = LogFactory.getLog(JsonUtil.class);

	private static JsonFactory factory = new JsonFactory();
	private static ObjectMapper mapper = new ObjectMapper(factory);

	/**
	 * Converts the input line parameter into {@link JsonNode} instance.
	 * 
	 * @param line
	 *            JSON text
	 * @return Returns a {@link JsonNode} that represents the input line or null for invalid json.
	 */
	public static synchronized JsonNode decodeLineToJsonNode(String line) {

		try {
			return mapper.readTree(line);
		} catch (Exception e) {
			LOG.warn("Failed to convert the line into JSON object", e);
			return null;
		}
	}
}