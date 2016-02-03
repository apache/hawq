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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This JSON resolver for PXF will decode a given object from the {@link JsonAccessor} into a row for HAWQ. It will
 * decode this data into a JsonNode and walk the tree for each column. It supports normal value mapping via projections
 * and JSON array indexing.
 */
public class JsonResolver extends Plugin implements ReadResolver {

	private static final Log LOG = LogFactory.getLog(JsonResolver.class);

	private ArrayList<OneField> oneFieldList;
	private ColumnDescriptorCache[] columnDescriptorCache;
	private ObjectMapper mapper;

	public JsonResolver(InputData inputData) throws Exception {
		super(inputData);
		oneFieldList = new ArrayList<OneField>();
		mapper = new ObjectMapper(new JsonFactory());

		// Pre-generate all column structure attributes concerning the JSON to column value resolution.
		columnDescriptorCache = new ColumnDescriptorCache[inputData.getColumns()];
		for (int i = 0; i < inputData.getColumns(); ++i) {
			ColumnDescriptor cd = inputData.getColumn(i);
			columnDescriptorCache[i] = new ColumnDescriptorCache(cd);
		}
	}

	@Override
	public List<OneField> getFields(OneRow row) throws Exception {
		oneFieldList.clear();

		String jsonRecordAsText = row.getData().toString();

		JsonNode root = decodeLineToJsonNode(jsonRecordAsText);

		if (root == null) {
			LOG.warn("Return null-fields row due to invalid JSON:" + jsonRecordAsText);
		}

		// Iterate through the column definition and fetch our JSON data
		for (ColumnDescriptorCache column : columnDescriptorCache) {

			// Get the current column description

			if (root == null) {
				// Return empty (e.g. null) filed in case of malformed json.
				addNullField(column.getColumnType());
			} else {

				// Move down the JSON path to the final name
				JsonNode node = getPriorJsonNode(root, column.getProjections());

				// If this column is an array index, ex. "tweet.hashtags[0]"
				if (column.isArrayName()) {

					// Move to the array node
					node = node.get(column.getArrayNodeName());

					// If this node is null or missing, add a null value here
					if (node == null || node.isMissingNode()) {
						addNullField(column.getColumnType());
					} else if (node.isArray()) {
						// If the JSON node is an array, then add it to our list
						addFieldFromJsonArray(column.getColumnType(), node, column.getArrayIndex());
					} else {
						throw new IllegalStateException(column.getArrayNodeName() + " is not an array node");
					}
				} else {
					// This column is not an array type
					// Move to the final node
					node = node.get(column.getLastProjection());

					// If this node is null or missing, add a null value here
					if (node == null || node.isMissingNode()) {
						addNullField(column.getColumnType());
					} else {
						// Else, add the value to the record
						addFieldFromJsonNode(column.getColumnType(), node);
					}
				}
			}
		}

		return oneFieldList;
	}

	/**
	 * Iterates down the root node to the child JSON node defined by the projs path.
	 * 
	 * @param root
	 *            node to to start the traversal from.
	 * @param projs
	 *            defines the path from the root to the desired child node.
	 * @return Returns the child node defined by the root and projs path.
	 */
	private JsonNode getPriorJsonNode(JsonNode root, String[] projs) {

		// Iterate through all the tokens to the desired JSON node
		JsonNode node = root;
		for (int j = 0; j < projs.length - 1; ++j) {
			node = node.path(projs[j]);
		}

		return node;
	}

	/**
	 * Iterates through the given JSON node to the proper index and adds the field of corresponding type
	 * 
	 * @param type
	 *            The {@link DataType} type
	 * @param node
	 *            The JSON array node
	 * @param index
	 *            The array index to iterate to
	 * @throws IOException
	 */
	private void addFieldFromJsonArray(DataType type, JsonNode node, int index) throws IOException {

		int count = 0;
		boolean added = false;
		for (Iterator<JsonNode> arrayNodes = node.getElements(); arrayNodes.hasNext();) {
			JsonNode arrayNode = arrayNodes.next();

			if (count == index) {
				added = true;
				addFieldFromJsonNode(type, arrayNode);
				break;
			}

			++count;
		}

		// if we reached the end of the array without adding a
		// field, add null
		if (!added) {
			addNullField(type);
		}
	}

	/**
	 * Adds a field from a given JSON node value based on the {@link DataType} type.
	 * 
	 * @param type
	 *            The DataType type
	 * @param val
	 *            The JSON node to extract the value.
	 * @throws IOException
	 */
	private void addFieldFromJsonNode(DataType type, JsonNode val) throws IOException {
		OneField oneField = new OneField();
		oneField.type = type.getOID();

		if (val.isNull()) {
			oneField.val = null;
		} else {
			switch (type) {
			case BIGINT:
				oneField.val = val.asLong();
				break;
			case BOOLEAN:
				oneField.val = val.asBoolean();
				break;
			case CHAR:
				oneField.val = val.asText().charAt(0);
				break;
			case BYTEA:
				oneField.val = val.asText().getBytes();
				break;
			case FLOAT8:
			case REAL:
				oneField.val = val.asDouble();
				break;
			case INTEGER:
			case SMALLINT:
				oneField.val = val.asInt();
				break;
			case BPCHAR:
			case TEXT:
			case VARCHAR:
				oneField.val = val.asText();
				break;
			default:
				throw new IOException("Unsupported type " + type);
			}
		}

		oneFieldList.add(oneField);
	}

	/**
	 * Adds a null field of the given type.
	 * 
	 * @param type
	 *            The {@link DataType} type
	 */
	private void addNullField(DataType type) {
		oneFieldList.add(new OneField(type.getOID(), null));
	}

	/**
	 * Converts the input line parameter into {@link JsonNode} instance.
	 * 
	 * @param line
	 *            JSON text
	 * @return Returns a {@link JsonNode} that represents the input line or null for invalid json.
	 */
	private JsonNode decodeLineToJsonNode(String line) {

		try {
			return mapper.readTree(line);
		} catch (Exception e) {
			LOG.error("Failed to pars JSON object", e);
			return null;
		}
	}
}