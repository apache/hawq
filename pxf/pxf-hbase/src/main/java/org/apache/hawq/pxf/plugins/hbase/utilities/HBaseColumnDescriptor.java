package org.apache.hawq.pxf.plugins.hbase.utilities;

import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * {@link ColumnDescriptor} for HBase columns.
 */
public class HBaseColumnDescriptor extends ColumnDescriptor {
    byte[] columnFamily;
    byte[] qualifier;

    /**
     * Constructs a column descriptor using the given copy's column name.
     *
     * @param copy column descriptor to be copied
     */
    public HBaseColumnDescriptor(ColumnDescriptor copy) {
        this(copy, copy.columnName().getBytes());
    }

    /**
     * Constructs an HBase column descriptor from a generic column descriptor and an HBase column name.
     * <p>
     * The column name must be in either of the following forms:
     * <ol>
     * <li>columnfamily:qualifier - standard HBase column.</li>
     * <li>recordkey - Row key column (case insensitive).</li>
     * </ol>
     * <p>
     * For recordkey, no HBase name is created.
     *
     * @param copy column descriptor
     * @param newColumnName HBase column name - can be different than the given column descriptor name.
     */
    public HBaseColumnDescriptor(ColumnDescriptor copy, byte[] newColumnName) {
        super(copy);

        if (isKeyColumn()) {
            return;
        }

        int seperatorIndex = getSeparatorIndex(newColumnName);

        columnFamily = Arrays.copyOfRange(newColumnName, 0, seperatorIndex);
        qualifier = Arrays.copyOfRange(newColumnName, seperatorIndex + 1, newColumnName.length);
    }

    /**
     * Returns the family column name.
     * (E.g. "cf1:q2" will return "cf1")
     *
     * @return family column name
     */
    public byte[] columnFamilyBytes() {
        return columnFamily;
    }

    /**
     * Returns the qualifier column name.
     * (E.g. "cf1:q2" will return "q2")
     *
     * @return qualifier column name
     */
    public byte[] qualifierBytes() {
        return qualifier;
    }

    private int getSeparatorIndex(byte[] columnName) {
        for (int i = 0; i < columnName.length; ++i) {
            if (columnName[i] == ':') {
                return i;
            }
        }

        throw new IllegalArgumentException("Illegal HBase column name " +
                Bytes.toString(columnName) +
                ", missing :");
    }
}
