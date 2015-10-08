package com.pivotal.pxf.plugins.hbase;

import com.pivotal.pxf.api.*;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseTupleDescription;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

/**
 * Record resolver for HBase.
 *
 * The class is responsible to convert rows from HBase scans (returned as {@link Result} objects)
 * into a List of {@link OneField} objects.
 * That also includes the conversion process of each HBase column's value into its HAWQ assigned type.
 *
 * Currently, the class assumes all HBase values are stored as String object Bytes encoded.
 */
public class HBaseResolver extends Plugin implements ReadResolver {
    private HBaseTupleDescription tupleDescription;

    /**
     * Constructs a resolver and initializes the table's tuple description.
     *
     * @param input query information, contains HBase table name and filter
     */
    public HBaseResolver(InputData input) {
        super(input);
        tupleDescription = new HBaseTupleDescription(input);
    }

    /**
     * Splits an HBase {@link Result} object into a list of {@link OneField},
     * based on the table's tuple description.
     * Each field is converted from HBase bytes into its column description type.
     *
     * @return list of fields
     */
    @Override
    public List<OneField> getFields(OneRow onerow) throws Exception {
        Result result = (Result) onerow.getData();
        LinkedList<OneField> fields = new LinkedList<OneField>();

        for (int i = 0; i < tupleDescription.columns(); ++i) {
            HBaseColumnDescriptor column = tupleDescription.getColumn(i);
            byte[] value;

            if (column.isKeyColumn()) // if a row column is requested
            {
                value = result.getRow(); // just return the row key
            } else // else, return column value
            {
                value = getColumnValue(result, column);
            }

            OneField oneField = new OneField();
            oneField.type = column.columnTypeCode();
            oneField.val = convertToJavaObject(oneField.type, column.columnTypeName(), value);
            fields.add(oneField);
        }
        return fields;
    }

    /**
     * Converts given byte array value to the matching java object, according to
     * the given type code.
     *
     * @param typeCode ColumnDescriptor type id
     * @param typeName type name. Used for error messages
     * @param val value to be converted
     * @return value converted to matching object type
     * @throws Exception when conversion fails or type code is not supported
     */
    Object convertToJavaObject(int typeCode, String typeName, byte[] val) throws Exception {
        if (val == null) {
            return null;
        }
        try {
            switch (DataType.get(typeCode)) {
                case TEXT:
                case VARCHAR:
                case BPCHAR:
                    return Bytes.toString(val);

                case INTEGER:
                    return Integer.parseInt(Bytes.toString(val));

                case BIGINT:
                    return Long.parseLong(Bytes.toString(val));

                case SMALLINT:
                    return Short.parseShort(Bytes.toString(val));

                case REAL:
                    return Float.parseFloat(Bytes.toString(val));

                case FLOAT8:
                    return Double.parseDouble(Bytes.toString(val));

                case BYTEA:
                    return val;

                case BOOLEAN:
                    return Boolean.valueOf(Bytes.toString(val));

                case NUMERIC:
                    return Bytes.toString(val);

                case TIMESTAMP:
                    return Timestamp.valueOf(Bytes.toString(val));

                default:
                    throw new UnsupportedTypeException("Unsupported data type " + typeName);
            }
        } catch (NumberFormatException e) {
            throw new BadRecordException("Error converting value '" + Bytes.toString(val) + "' " +
                    "to type " + typeName + ". " +
                    "(original error: " + e.getMessage() + ")");
        }
    }

    /**
     * Returns the value of a column from a Result object.
     *
     * @param result HBase table row
     * @param column HBase column to be retrieved
     * @return HBase column value
     */
    byte[] getColumnValue(Result result, HBaseColumnDescriptor column) {
        // if column does not contain a value, return null
        if (!result.containsColumn(column.columnFamilyBytes(),
                column.qualifierBytes())) {
            return null;
        }

        // else, get the latest version of the requested column
        Cell cell = result.getColumnLatestCell(column.columnFamilyBytes(), column.qualifierBytes());
        return CellUtil.cloneValue(cell);
    }
}
