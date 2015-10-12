package com.pivotal.pxf.plugins.hbase.utilities;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * HBaseLookupTable will load a table's lookup information
 * from HBase pxflookup table if exists.<br>
 * This table holds mappings between HAWQ column names (key) and HBase column names (value).<br>
 * E.g. for an HBase table "hbase_table", mappings between HAWQ column names and HBase column names,
 * when <code>"hawq1"</code> is mapped to <code>"cf1:hbase1"</code> and
 * <code>"hawq2"</code> is mapped to <code>"cf1:hbase2"</code>, will be:<br>
 * <pre>
 * 	ROW                     COLUMN+CELL
 *  hbase_table             column=mapping:hawq1, value=cf1:hbase1
 *  hbase_table             column=mapping:hawq2, value=cf1:hbase2
 * </pre>
 *
 * Data is returned as a map of string and byte array from {@link #getMappings(String)}.
 * <p>
 * Once created, {@link #close()} MUST be called to cleanup resources.
 */
public class HBaseLookupTable implements Closeable {
    private static final String LOOKUPTABLENAME = "pxflookup";
    private static final byte[] LOOKUPCOLUMNFAMILY = Bytes.toBytes("mapping");

    private static final Log LOG = LogFactory.getLog(HBaseLookupTable.class);

    private Connection connection;
    private Configuration hbaseConfiguration;
    private Admin admin;
    private Map<byte[], byte[]> rawTableMapping;
    private Table lookupTable;

    /**
     * Constructs a connector to HBase lookup table.
     * Requires calling {@link #close()} to close {@link HBaseAdmin} instance.
     *
     * @param conf HBase configuration
     * @throws IOException when initializing HBaseAdmin fails
     */
    public HBaseLookupTable(Configuration conf) throws Exception {
        hbaseConfiguration = conf;
        connection = ConnectionFactory.createConnection(hbaseConfiguration);
        admin = connection.getAdmin();
        ClusterStatus cs = admin.getClusterStatus();
        LOG.debug("HBase cluster has " + cs.getServersSize() + " region servers " +
                "(" + cs.getDeadServers() + " dead)");
    }

    /**
     * Returns mappings for given table name between its HAWQ column names and
     * HBase column names.
     * If lookup table doesn't exist or no mappings for the table exist, returns null.
     * <p>
     * All HAWQ column names are returns in low case.
     *
     * @param tableName HBase table name
     * @return mappings between HAWQ column names and HBase column names
     * @throws IOException when HBase operations fail
     */
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

    /**
     * Closes HBase resources. Must be called after initializing this class.
     */
    @Override
    public void close() throws IOException {
        admin.close();
    }

    /**
     * Returns true if {@link #LOOKUPTABLENAME} is available and enabled.
     *
     * @return whether lookup table is valid
     */
    private boolean lookupTableValid() throws IOException {
        return (HBaseUtilities.isTableAvailable(admin, LOOKUPTABLENAME) &&
                lookupHasCorrectStructure());
    }

    /**
     * Returns true if {@link #LOOKUPTABLENAME} has {@value #LOOKUPCOLUMNFAMILY} family.
     *
     * @return whether lookup has expected column family name
     */
    private boolean lookupHasCorrectStructure() throws IOException {
        HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(LOOKUPTABLENAME));
        return htd.hasFamily(LOOKUPCOLUMNFAMILY);
    }

    /**
     * Loads table name mappings from {@link #LOOKUPTABLENAME} lookup table.
     *
     * @param tableName table name
     */
    private void loadTableMappings(String tableName) throws IOException {
        openLookupTable();
        loadMappingMap(tableName);
        closeLookupTable();
    }

    /**
     * Returns true if lookup table has no relevant mappings.
     * Should be called after {@link #loadMappingMap(String)}.
     */
    private boolean tableHasNoMappings() {
        return MapUtils.isEmpty(rawTableMapping);
    }

    /**
     * Returns a map of mappings between HAWQ and HBase column names,
     * with the HAWQ column values in lower case.
     */
    private Map<String, byte[]> lowerCaseMappings() {
        Map<String, byte[]> lowCaseKeys = new HashMap<String, byte[]>();
        for (Map.Entry<byte[], byte[]> entry : rawTableMapping.entrySet()) {
            lowCaseKeys.put(lowerCase(entry.getKey()),
                    entry.getValue());
        }

        return lowCaseKeys;
    }

    /**
     * Load hbase table object using ConnectionFactory
     */
    private void openLookupTable() throws IOException {
        lookupTable = connection.getTable(TableName.valueOf(LOOKUPTABLENAME));
    }

    /**
     * Loads mappings for given table name from the lookup table {@link #LOOKUPTABLENAME}.
     * The table name should be in the row key, and the family name should be {@link #LOOKUPCOLUMNFAMILY}.
     *
     * @param tableName HBase table name
     * @throws IOException when HBase operations fail
     */
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
        HBaseUtilities.closeConnection(admin, connection);
    }

    private String lowerCase(byte[] key) {
        return Bytes.toString(key).toLowerCase();
    }
}
