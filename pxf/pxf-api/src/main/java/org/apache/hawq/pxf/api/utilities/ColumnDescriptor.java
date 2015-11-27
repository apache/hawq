package org.apache.hawq.pxf.api.utilities;

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
 * ColumnDescriptor describes one column in hawq database.
 * Currently it means a name, a type id (HAWQ/GPDB OID), a type name and column index.
 */
public class ColumnDescriptor {

	int gpdbColumnTypeCode;
    String gpdbColumnName;
    String gpdbColumnTypeName;
    int gpdbColumnIndex;

    /**
     * Reserved word for a table record key.
     * A field with this name will be treated as record key.
     */
    public static final String RECORD_KEY_NAME = "recordkey";

    /**
     * Constructs a ColumnDescriptor.
     *
     * @param name column name
     * @param typecode OID
     * @param index column index
     * @param typename type name
     */
    public ColumnDescriptor(String name, int typecode, int index, String typename) {
        gpdbColumnTypeCode = typecode;
        gpdbColumnTypeName = typename;
        gpdbColumnName = name;
        gpdbColumnIndex = index;
    }

    /**
     * Constructs a copy of ColumnDescriptor.
     *
     * @param copy the ColumnDescriptor to copy
     */
    public ColumnDescriptor(ColumnDescriptor copy) {
        this.gpdbColumnTypeCode = copy.gpdbColumnTypeCode;
        this.gpdbColumnName = copy.gpdbColumnName;
        this.gpdbColumnIndex = copy.gpdbColumnIndex;
        this.gpdbColumnTypeName = copy.gpdbColumnTypeName;
    }

    public String columnName() {
        return gpdbColumnName;
    }

    public int columnTypeCode() {
        return gpdbColumnTypeCode;
    }

    public int columnIndex() {
        return gpdbColumnIndex;
    }

    public String columnTypeName() {
        return gpdbColumnTypeName;
    }

    /**
     * Returns <tt>true</tt> if {@link #gpdbColumnName} is a {@link #RECORD_KEY_NAME}.
     *
     * @return whether column is a record key column
     */
    public boolean isKeyColumn() {
        return RECORD_KEY_NAME.equalsIgnoreCase(gpdbColumnName);
    }

    @Override
	public String toString() {
		return "ColumnDescriptor [gpdbColumnTypeCode=" + gpdbColumnTypeCode
				+ ", gpdbColumnName=" + gpdbColumnName
				+ ", gpdbColumnTypeName=" + gpdbColumnTypeName
				+ ", gpdbColumnIndex=" + gpdbColumnIndex + "]";
	}
}
