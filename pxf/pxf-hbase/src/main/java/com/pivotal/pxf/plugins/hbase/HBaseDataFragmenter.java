package com.pivotal.pxf.plugins.hbase;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseLookupTable;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseUtilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Fragmenter class for HBase data resources.
 *
 * Extends the {@link Fragmenter} abstract class, with the purpose of transforming
 * an input data path (an HBase table name in this case) into a list of regions
 * that belong to this table.
 *
 * This class also puts HBase lookup table information for the given
 * table (if exists) in each fragment's user data field.
 */
public class HBaseDataFragmenter extends Fragmenter {

    private static final Configuration hbaseConfiguration = HBaseUtilities.initHBaseConfiguration();
    private HBaseAdmin hbaseAdmin;

    public HBaseDataFragmenter(InputData inConf) {
        super(inConf);
    }

    /**
     * Returns list of fragments containing all of the
     * HBase's table data.
     * Lookup table information with mapping between
     * field names in HAWQ table and HBase table will be
     * returned as user data.
     *
     * @return a list of fragments
     */
    @Override
    public List<Fragment> getFragments() throws Exception {

        // check that Zookeeper and HBase master are available
        HBaseAdmin.checkHBaseAvailable(hbaseConfiguration);
        hbaseAdmin = new HBaseAdmin(hbaseConfiguration);
        if (!HBaseUtilities.isTableAvailable(hbaseAdmin, inputData.getDataSource())) {
            throw new TableNotFoundException(inputData.getDataSource());
        }

        byte[] userData = prepareUserData();
        addTableFragments(userData);

        return fragments;
    }

    /**
     * Serializes lookup table mapping into byte array.
     *
     * @return serialized lookup table mapping
     * @throws IOException when connection to lookup table fails
     * or serialization fails
     */
    private byte[] prepareUserData() throws Exception {
        HBaseLookupTable lookupTable = new HBaseLookupTable(hbaseConfiguration);
        Map<String, byte[]> mappings = lookupTable.getMappings(inputData.getDataSource());
        lookupTable.close();

        if (mappings != null) {
            return serializeMap(mappings);
        }

        return null;
    }

    /**
     * Serializes fragment metadata information
     * (region start and end keys) into byte array.
     *
     * @param region region to be serialized
     * @return serialized metadata information
     * @throws IOException when serialization fails
     */
    private byte[] prepareFragmentMetadata(HRegionInfo region) throws IOException {

        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
        objectStream.writeObject(region.getStartKey());
        objectStream.writeObject(region.getEndKey());

        return byteArrayStream.toByteArray();
    }

    private void addTableFragments(byte[] userData) throws IOException {
        HTable table = new HTable(hbaseConfiguration, inputData.getDataSource());
        NavigableMap<HRegionInfo, ServerName> locations = table.getRegionLocations();

        for (Map.Entry<HRegionInfo, ServerName> entry : locations.entrySet()) {
            addFragment(entry, userData);
        }

        table.close();
    }

    private void addFragment(Map.Entry<HRegionInfo, ServerName> entry,
            byte[] userData) throws IOException {
        ServerName serverInfo = entry.getValue();
        String[] hosts = new String[] {serverInfo.getHostname()};
        HRegionInfo region = entry.getKey();
        byte[] fragmentMetadata = prepareFragmentMetadata(region);
        Fragment fragment = new Fragment(inputData.getDataSource(), hosts, fragmentMetadata, userData);
        fragments.add(fragment);
    }

    private byte[] serializeMap(Map<String, byte[]> tableMappings) throws IOException {
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
        objectStream.writeObject(tableMappings);

        return byteArrayStream.toByteArray();
    }
}
