package com.pivotal.pxf.plugins.hbase;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseLookupTable;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseUtilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

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
    private Admin hbaseAdmin;
    private Connection connection;

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
        connection = ConnectionFactory.createConnection(hbaseConfiguration);
        hbaseAdmin = connection.getAdmin();
        if (!HBaseUtilities.isTableAvailable(hbaseAdmin, inputData.getDataSource())) {
            HBaseUtilities.closeConnection(hbaseAdmin, connection);
            throw new TableNotFoundException(inputData.getDataSource());
        }

        byte[] userData = prepareUserData();
        addTableFragments(userData);

        HBaseUtilities.closeConnection(hbaseAdmin, connection);

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
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(inputData.getDataSource()));
        List <HRegionLocation> locations = regionLocator.getAllRegionLocations();

        for (HRegionLocation location : locations) {
            addFragment(location, userData);
        }

        regionLocator.close();
    }

    private void addFragment(HRegionLocation location,
            byte[] userData) throws IOException {
        ServerName serverInfo = location.getServerName();
        String[] hosts = new String[] {serverInfo.getHostname()};
        HRegionInfo region = location.getRegionInfo();
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
