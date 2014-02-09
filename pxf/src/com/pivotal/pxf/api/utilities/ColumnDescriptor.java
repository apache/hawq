package com.pivotal.pxf.api.utilities;

/**
 * ColumnDescriptor describes one column in hawq database.
 * Currently it means a name, a type id (HAWQ/GPDB OID), a type name and column index.
 */
public class ColumnDescriptor {
    int gpdbColumnTypeCode;
    String gpdbColumnName;
    String gpdbColumnTypeName;
    int gpdbColumnIndex;
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

    /** Returns <tt>true</tt> if {@link #gpdbColumnName} is a {@link #RECORD_KEY_NAME}. */
    public boolean isKeyColumn() {
        return RECORD_KEY_NAME.equalsIgnoreCase(gpdbColumnName);
    }
}
