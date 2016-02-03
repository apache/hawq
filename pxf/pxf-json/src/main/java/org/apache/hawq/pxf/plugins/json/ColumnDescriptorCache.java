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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;

/**
 * Helper class used to retrieve all column details relevant for the json processing.
 */
public class ColumnDescriptorCache {

	private static Pattern ARRAY_PROJECTION_PATTERN = Pattern.compile("(.*)\\[([0-9]+)\\]");
	private static int ARRAY_NAME_GROUPID = 1;
	private static int ARRAY_INDEX_GROUPID = 2;

	private final DataType columnType;
	private final String[] projections;
	private final String nodeName;
	private final int arrayIndex;
	private final boolean isArray;

	public ColumnDescriptorCache(ColumnDescriptor columnDescriptor) {

		this.columnType = DataType.get(columnDescriptor.columnTypeCode());

		this.projections = columnDescriptor.columnName().split("\\.");

		this.isArray = isArrayIndex(projections);
		if (this.isArrayName()) {
			this.nodeName = getArrayName(projections);
			this.arrayIndex = getArrayIndex(projections);
		} else {
			this.nodeName = null;
			this.arrayIndex = -1;
		}

	}

	/**
	 * @return Column's type
	 */
	public DataType getColumnType() {
		return columnType;
	}

	/**
	 * If the column name contains dots (.) then this name is interpreted as path into the target json document pointing
	 * to nested json member. The leftmost path element stands for the root in the json document.
	 * 
	 * @return If the column name contains dots (.) list of field names that represent the path from the root json node
	 *         to the target nested node.
	 */
	public String[] getProjections() {
		return projections;
	}

	/**
	 * The 'jsonName[index]' column name conventions is used to point to a particular json array element.
	 * 
	 * @return Returns the json name of the array element.
	 */
	public String getArrayNodeName() {
		return nodeName;
	}

	/**
	 * The 'jsonName[index]' column name conventions is used to point to a particular json array element.
	 * 
	 * @return Returns the json index of the refered array element.
	 */
	public int getArrayIndex() {
		return arrayIndex;
	}

	/**
	 * @return Returns true if the column name is a path to json array element and false otherwise.
	 */
	public boolean isArrayName() {
		return isArray;
	}

	/**
	 * @return Returns the json node name as defined in the column name. If the dot convention is used then returns the
	 *         child json node name.
	 */
	public String getLastProjection() {
		return projections[projections.length - 1];
	}

	/**
	 * Gets a boolean value indicating if this column is an array index column
	 * 
	 * @param projs
	 *            The array of JSON projections
	 * @throws ArrayIndexOutOfBoundsException
	 */
	private boolean isArrayIndex(String[] projs) {
		return getMatchGroup(projs[projs.length - 1], ARRAY_NAME_GROUPID) != null;
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
		return getMatchGroup(projs[projs.length - 1], ARRAY_NAME_GROUPID);
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
		return Integer.parseInt(getMatchGroup(projs[projs.length - 1], ARRAY_INDEX_GROUPID));
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
}
