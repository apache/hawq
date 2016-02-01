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
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.codehaus.jackson.JsonNode;

/**
 * This JSON resolver for PXF will decode a given object from the {@link JsonAccessor} into a row for HAWQ. It will
 * decode this data into a JsonNode and walk the tree for each column. It supports normal value mapping via projections
 * and JSON array indexing.
 */
public class JsonResolver extends Plugin implements ReadResolver {

	private static Pattern ARRAY_PROJECTION_PATTERN = Pattern.compile("(.*)\\[([0-9]+)\\]");

	private ArrayList<OneField> list = new ArrayList<OneField>();

	public JsonResolver(InputData inputData) throws Exception {
		super(inputData);
	}

	@Override
	public List<OneField> getFields(OneRow row) throws Exception {
		list.clear();

		// key is a Text object
		JsonNode root = JsonUtil.decodeLineToJsonNode(row.getData().toString());

		// if we weren't given a null object
		if (root != null) {
			// Iterate through the column definition and fetch our JSON data
			for (int i = 0; i < inputData.getColumns(); ++i) {

				// Get the current column description
				ColumnDescriptor cd = inputData.getColumn(i);
				DataType columnType = DataType.get(cd.columnTypeCode());

				// Get the JSON projections from the column name
				// For example, "user.name" turns into ["user","name"]
				String[] projs = cd.columnName().split("\\.");

				// Move down the JSON path to the final name
				JsonNode node = getPriorJsonNode(root, projs);

				// If this column is an array index, ex. "tweet.hashtags[0]"
				if (isArrayIndex(projs)) {

					// Get the node name and index
					String nodeName = getArrayName(projs);
					int arrayIndex = getArrayIndex(projs);

					// Move to the array node
					node = node.get(nodeName);

					// If this node is null or missing, add a null value here
					if (node == null || node.isMissingNode()) {
						addNullField(columnType);
					} else if (node.isArray()) {
						// If the JSON node is an array, then add it to our list
						addFieldFromJsonArray(columnType, node, arrayIndex);
					} else {
						throw new InvalidParameterException(nodeName + " is not an array node");
					}
				} else {
					// This column is not an array type
					// Move to the final node
					node = node.get(projs[projs.length - 1]);

					// If this node is null or missing, add a null value here
					if (node == null || node.isMissingNode()) {
						addNullField(columnType);
					} else {
						// Else, add the value to the record
						addFieldFromJsonNode(columnType, node);
					}
				}
			}
		}

		return list;
	}

	/**
	 * Iterates down the root node to the prior JSON node. This node is used
	 * 
	 * @param root
	 * @param projs
	 * @return
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
	 * Gets a boolean value indicating if this column is an array index column
	 * 
	 * @param projs
	 *            The array of JSON projections
	 * @throws ArrayIndexOutOfBoundsException
	 */
	private boolean isArrayIndex(String[] projs) {
		return getMatchGroup(projs[projs.length - 1], 1) != null;
	}

	/**
	 * Gets the node name from the given String array of JSON projections, parsed from the ColumnDescriptor's
	 * 
	 * @param projs
	 *            The array of JSON projections
	 * @return The name
	 * @throws ArrayIndexOutOfBoundsException
	 */
	private String getArrayName(String[] projs) {
		return getMatchGroup(projs[projs.length - 1], 1);
	}

	/**
	 * Gets the array index from the given String array of JSON projections, parsed from the ColumnDescriptor's name
	 * 
	 * @param projs
	 *            The array of JSON projections
	 * @return The index
	 * @throws ArrayIndexOutOfBoundsException
	 */
	private int getArrayIndex(String[] projs) {
		return Integer.parseInt(getMatchGroup(projs[projs.length - 1], 2));
	}

	/**
	 * Extracts the array name or array index. E.g. getMatchGroup("myarray[666]", 1) returns "myarray" and
	 * getMatchGroup("myarray[666]", 2) returns "666". If the text is not an array expression function returns NULL.
	 * 
	 * @param text
	 *            expression to evaluate
	 * @param groupId
	 *            in case of array text expression, groupId=1 will refer the array name and groupId=2 refers the array
	 *            index.
	 * @return If text is an array expression, return the name or the index of the array. If text is not an array
	 *         expression return NULL.
	 */
	private String getMatchGroup(String text, int groupId) {
		Matcher matcher = ARRAY_PROJECTION_PATTERN.matcher(text);
		if (matcher.matches()) {
			return matcher.group(groupId);
		}
		return null;
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

		list.add(oneField);
	}

	/**
	 * Adds a null field of the given type.
	 * 
	 * @param type
	 *            The {@link DataType} type
	 */
	private void addNullField(DataType type) {
		list.add(new OneField(type.getOID(), null));
	}
}