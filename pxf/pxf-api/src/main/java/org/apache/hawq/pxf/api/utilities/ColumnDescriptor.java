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

    int dbColumnTypeCode;
    String dbColumnName;
    String dbColumnTypeName;
    int dbColumnIndex;
    Integer[] dbColumnTypeModifiers;
    boolean isProjected;

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
     * @param typemods type modifiers
     * @param isProj does the column need to be projected
     */
    public ColumnDescriptor(String name, int typecode, int index, String typename, Integer[] typemods, boolean isProj) {
        this(name, typecode, index, typename, typemods);
        isProjected = isProj;
    }

    public ColumnDescriptor(String name, int typecode, int index, String typename, Integer[] typemods) {
        dbColumnTypeCode = typecode;
        dbColumnTypeName = typename;
        dbColumnName = name;
        dbColumnIndex = index;
        dbColumnTypeModifiers = typemods;
        isProjected = true;
    }

    /**
     * Constructs a copy of ColumnDescriptor.
     *
     * @param copy the ColumnDescriptor to copy
     */
    public ColumnDescriptor(ColumnDescriptor copy) {
        this.dbColumnTypeCode = copy.dbColumnTypeCode;
        this.dbColumnName = copy.dbColumnName;
        this.dbColumnIndex = copy.dbColumnIndex;
        this.dbColumnTypeName = copy.dbColumnTypeName;
        if (copy.dbColumnTypeModifiers != null
                && copy.dbColumnTypeModifiers.length > 0) {
            this.dbColumnTypeModifiers = new Integer[copy.dbColumnTypeModifiers.length];
            System.arraycopy(copy.dbColumnTypeModifiers, 0,
                    this.dbColumnTypeModifiers, 0,
                    copy.dbColumnTypeModifiers.length);
        }
        this.isProjected = copy.isProjected;
    }

    public String columnName() {
        return dbColumnName;
    }

    public int columnTypeCode() {
        return dbColumnTypeCode;
    }

    public int columnIndex() {
        return dbColumnIndex;
    }

    public String columnTypeName() {
        return dbColumnTypeName;
    }

    public Integer[] columnTypeModifiers() {
        return dbColumnTypeModifiers;
    }

    /**
     * Returns <tt>true</tt> if {@link #dbColumnName} is a {@link #RECORD_KEY_NAME}.
     *
     * @return whether column is a record key column
     */
    public boolean isKeyColumn() {
        return RECORD_KEY_NAME.equalsIgnoreCase(dbColumnName);
    }

    public boolean isProjected() {
        return isProjected;
    }

    public void setProjected(boolean projected) {
        isProjected = projected;
    }

    @Override
	public String toString() {
		return "ColumnDescriptor [dbColumnTypeCode=" + dbColumnTypeCode
				+ ", dbColumnName=" + dbColumnName
				+ ", dbColumnTypeName=" + dbColumnTypeName
				+ ", dbColumnIndex=" + dbColumnIndex
				+ ", dbColumnTypeModifiers=" + dbColumnTypeModifiers
                + ", isProjected=" + isProjected + "]";
	}
}
