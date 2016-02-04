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

	private static Pattern ARRAY_PROJECTION_PATTERN = Pattern.compile("(.+)\\[([0-9]+)\\]");
	private static int ARRAY_NAME_GROUPID = 1;
	private static int ARRAY_INDEX_GROUPID = 2;

	private final DataType columnType;
	private final String[] normalizedProjection;
	private final String arrayNodeName;
	private final int arrayNodeIndex;
	private final boolean isArray;
	private String columnName;

	public ColumnDescriptorCache(ColumnDescriptor columnDescriptor) {

		// HAWQ column type
		this.columnType = DataType.get(columnDescriptor.columnTypeCode());

		this.columnName = columnDescriptor.columnName();

		// Column name can use dot-name convention to specify a nested json node.
		// Break the path into array of path steps called projections
		String[] projection = columnDescriptor.columnName().split("\\.");

		// When the projection contains array reference (e.g. projections = foo.bar[66]) then replace the last path
		// element by the array name (e.g. normalizedProjection = foo.bar)
		normalizedProjection = new String[projection.length];

		// Check if the provided json path (projections) refers to an array element.
		Matcher matcher = ARRAY_PROJECTION_PATTERN.matcher(projection[projection.length - 1]);
		if (matcher.matches()) {
			this.isArray = true;
			// extracts the array node name from the projection path
			this.arrayNodeName = matcher.group(ARRAY_NAME_GROUPID);
			// extracts the array index from the projection path
			this.arrayNodeIndex = Integer.parseInt(matcher.group(ARRAY_INDEX_GROUPID));

			System.arraycopy(projection, 0, normalizedProjection, 0, projection.length - 1);
			normalizedProjection[projection.length - 1] = this.arrayNodeName;
		} else {
			this.isArray = false;
			this.arrayNodeName = null;
			this.arrayNodeIndex = -1;

			System.arraycopy(projection, 0, normalizedProjection, 0, projection.length);
		}
	}

	/**
	 * @return Column's type
	 */
	public DataType getColumnType() {
		return columnType;
	}

	/**
	 * @return Returns the column name as defined in the HAWQ table.
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * If the column name contains dots (.) then this name is interpreted as path into the target json document pointing
	 * to nested json member. The leftmost path element stands for the root in the json document.
	 * 
	 * @return If the column name contains dots (.) list of field names that represent the path from the root json node
	 *         to the target nested node.
	 */
	public String[] getNormalizedProjections() {
		return normalizedProjection;
	}

	/**
	 * The 'jsonName[index]' column name conventions is used to point to a particular json array element.
	 * 
	 * @return Returns the json index of the referred array element.
	 */
	public int getArrayNodeIndex() {
		return arrayNodeIndex;
	}

	/**
	 * @return Returns true if the column name is a path to json array element and false otherwise.
	 */
	public boolean isArray() {
		return isArray;
	}
}
