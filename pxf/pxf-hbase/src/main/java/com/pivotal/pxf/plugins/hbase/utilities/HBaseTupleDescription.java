package com.pivotal.pxf.plugins.hbase.utilities;

import com.pivotal.pxf.api.utilities.ColumnDescriptor;
import com.pivotal.pxf.api.utilities.InputData;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The class extends the tuple description provided by {@link InputData}
 * for usage of {@link HBaseColumnDescriptor}.
 * <p>
 * This class also loads lookup table sent (optionally) by the
 * fragmenter.
 */
public class HBaseTupleDescription {
    private Map<String, byte[]> tableMapping;
    private List<HBaseColumnDescriptor> tupleDescription;
    private InputData conf;

    /**
     * Constructs tuple description of the HBase table.
     *
     * @param conf data containing table tuple description
     */
    public HBaseTupleDescription(InputData conf) {
        this.conf = conf;
        parseHBaseTupleDescription();
    }

    /**
     * Returns the number of fields.
     *
     * @return number of fields
     */
    public int columns() {
        return tupleDescription.size();
    }

    /**
     * Returns the column description of index column.
     *
     * @param index column index to be returned
     * @return column description
     */
    public HBaseColumnDescriptor getColumn(int index) {
        return tupleDescription.get(index);
    }

    private void parseHBaseTupleDescription() {
        tupleDescription = new ArrayList<HBaseColumnDescriptor>();
        loadUserData();
        createTupleDescription();
    }

    /**
     * Loads user information from fragmenter.
     * The data contains optional table mappings from the lookup table,
     * between field names in HAWQ table and in the HBase table.
     */
    @SuppressWarnings("unchecked")
    private void loadUserData() {
        try {
            byte[] serializedTableMappings = conf.getFragmentUserData();

            // No userdata means no mappings for our table in lookup table
            if (serializedTableMappings == null) {
                return;
            }

            ByteArrayInputStream bytesStream = new ByteArrayInputStream(serializedTableMappings);
            ObjectInputStream objectStream = new ObjectInputStream(bytesStream);
            tableMapping = (Map<String, byte[]>) objectStream.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Exception while reading expected user data from HBase's fragmenter", e);
        }
    }

    private void createTupleDescription() {
        for (int i = 0; i < conf.getColumns(); ++i) {
            ColumnDescriptor column = conf.getColumn(i);
            tupleDescription.add(getHBaseColumn(column));
        }
    }

    /**
     * Returns the {@link #HBaseColumnDescriptor} for given column.
     * If the column has a lookup table mapping, the HBase column name is used.
     *
     * @param column HAWQ column description
     * @return matching HBase column description
     */
    private HBaseColumnDescriptor getHBaseColumn(ColumnDescriptor column) {
        if (!column.isKeyColumn() && hasMapping(column)) {
            return new HBaseColumnDescriptor(column, getMapping(column));
        }
        return new HBaseColumnDescriptor(column);
    }

    /**
     * Returns true if there is a mapping for given column name.
     */
    private boolean hasMapping(ColumnDescriptor column) {
        return tableMapping != null &&
                tableMapping.containsKey(column.columnName().toLowerCase());
    }

    /**
     * Returns the HBase name mapping for the given column name.
     *
     * @param column HAWQ column description
     * @return HBase name for the column
     */
    private byte[] getMapping(ColumnDescriptor column) {
        return tableMapping.get(column.columnName().toLowerCase());
    }
}
