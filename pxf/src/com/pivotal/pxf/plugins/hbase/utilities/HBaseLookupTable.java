package com.pivotal.pxf.plugins.hbase.utilities;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/*
 * HBaseLookupTable will load a table's lookup information
 * from lookup table if exists.
 *
 * Data is returned as a map of string and byte array from GetMappings
 *
 * Once created, close() MUST be called to cleanup resources.
 */
public class HBaseLookupTable implements Closeable {
    private static final byte[] LOOKUPTABLENAME = Bytes.toBytes("pxflookup");
    private static final byte[] LOOKUPCOLUMNFAMILY = Bytes.toBytes("mapping");

    private static final Configuration hbaseConfiguration = initConfiguration();
    private static final Log LOG = LogFactory.getLog(HBaseLookupTable.class);

    private HBaseAdmin admin;
    private Map<byte[], byte[]> rawTableMapping;
    private HTableInterface lookupTable;

    public HBaseLookupTable() throws IOException {
        admin = new HBaseAdmin(hbaseConfiguration);
    }

    public Map<String, byte[]> getMappings(String tableName) throws IOException {
        if (!lookupTableValid()) {
            return null;
        }

        loadTableMappings(tableName);

        if (tableHasNoMappings()) {
            return null;
        }

        return lowerCaseMappings();
    }

    public void close() throws IOException {
        admin.close();
    }

    private boolean lookupTableValid() throws IOException {
        return (admin.isTableAvailable(LOOKUPTABLENAME) &&
                admin.isTableEnabled(LOOKUPTABLENAME) &&
                lookupHasCorrectStructure());
    }

    private boolean lookupHasCorrectStructure() throws IOException {
        HTableDescriptor htd = admin.getTableDescriptor(LOOKUPTABLENAME);
        return htd.hasFamily(LOOKUPCOLUMNFAMILY);
    }

    private void loadTableMappings(String tableName) throws IOException {
        openLookupTable();
        loadMappingMap(tableName);
        closeLookupTable();
    }

    private boolean tableHasNoMappings() {
        return MapUtils.isEmpty(rawTableMapping);
    }

    private Map<String, byte[]> lowerCaseMappings() {
        Map<String, byte[]> lowCaseKeys = new HashMap<String, byte[]>();
        for (Map.Entry<byte[], byte[]> entry : rawTableMapping.entrySet()) {
            lowCaseKeys.put(lowerCase(entry.getKey()),
                    entry.getValue());
        }

        return lowCaseKeys;
    }

    private void openLookupTable() throws IOException {
        lookupTable = new HTable(hbaseConfiguration, LOOKUPTABLENAME);
    }

    private void loadMappingMap(String tableName) throws IOException {
        Get lookupRow = new Get(Bytes.toBytes(tableName));
        lookupRow.setMaxVersions(1);
        lookupRow.addFamily(LOOKUPCOLUMNFAMILY);
        Result row;

        row = lookupTable.get(lookupRow);
        rawTableMapping = row.getFamilyMap(LOOKUPCOLUMNFAMILY);
        LOG.debug("lookup table mapping for " + tableName +
                " has " + (rawTableMapping == null ? 0 : rawTableMapping.size()) + " entries");
    }

    private void closeLookupTable() throws IOException {
        lookupTable.close();
    }

    private String lowerCase(byte[] key) {
        return Bytes.toString(key).toLowerCase();
    }

    private static Configuration initConfiguration() {
        return HBaseConfiguration.create();
    }
}
