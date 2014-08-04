package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.mapred.*;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

/**
 * Fragmenter class for HIVE tables
 * <p/>
 * Given a Hive table and its partitions
 * divide the data into fragments (here a data fragment is actually a HDFS file block) and return a list of them.
 * Each data fragment will contain the following information:
 * <ol>
 * <li>sourceName: full HDFS path to the data file that this data fragment is part of</li>
 * <li>hosts: a list of the datanode machines that hold a replica of this block</li>
 * <li>userData: file_input_format_name_DELIM_serde_name_DELIM_serialization_properties</li>
 * </ol>
 */
public class HiveDataFragmenter extends Fragmenter {
    private static final Log LOG = LogFactory.getLog(HiveDataFragmenter.class);
    private static final String HIVE_DEFAULT_DBNAME = "default";
    private static final short ALL_PARTS = -1;

    static final String HIVE_UD_DELIM = "!HUDD!";
    static final String HIVE_1_PART_DELIM = "!H1PD!";
    static final String HIVE_PARTITIONS_DELIM = "!HPAD!";
    static final String HIVE_NO_PART_TBL = "!HNPT!";

    private JobConf jobConf;
    private HiveMetaStoreClient client;

    /* internal class used for parsing the qualified table name received as input to getFragments() */
    private class TblDesc {
        String dbName;
        String tableName;
    }

    /*
     * A Hive table unit - means a subset of the HIVE table, where we can say that for all files in this subset,
     * they all have the same InputFormat and Serde.
     * For a partitioned table the HiveTableUnit will be one partition and for an unpartitioned table, the
     * HiveTableUnit will be the whole table
     */
    class HiveTablePartition {
        StorageDescriptor storageDesc;
        Properties properties;
        Partition partition;
        List<FieldSchema> partitionKeys;
        String tableName;

        HiveTablePartition(StorageDescriptor storageDesc,
                           Properties properties,
                           Partition partition,
                           List<FieldSchema> partitionKeys,
                           String tableName) {
            this.storageDesc = storageDesc;
            this.properties = properties;
            this.partition = partition;
            this.partitionKeys = partitionKeys;
            this.tableName = tableName;
        }


        @Override
        public String toString() {
            return "table - " + tableName +
                    ((partition == null)
                            ? ""
                            : ", partition - " + partition);
        }
    }

    /**
     * Constructs a HiveDataFragmenter object
     *
     * @param inputData all input parameters coming from the client
     */
    public HiveDataFragmenter(InputData inputData) {
        this(inputData, HiveDataFragmenter.class);
    }

    /**
     * Constructs a HiveDataFragmenter object
     *
     * @param inputData all input parameters coming from the client
     * @param clazz     Class for JobConf
     */
    public HiveDataFragmenter(InputData inputData, Class<?> clazz) {
        super(inputData);
        jobConf = new JobConf(new Configuration(), clazz);
        initHiveClient();
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        TblDesc tblDesc = parseTableQualifiedName(inputData.getDataSource());
        if (tblDesc == null) {
            throw new IllegalArgumentException(inputData.getDataSource() + " is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>");
        }

        fetchTableMetaData(tblDesc);

        return fragments;
    }

    /**
     * Creates the partition InputFormat
     *
     * @param inputFormatName input format class name
     * @param jobConf         configuration data for the Hadoop framework
     * @return a {@link org.apache.hadoop.mapred.InputFormat} derived object
     */
    public static InputFormat<?, ?> makeInputFormat(String inputFormatName, JobConf jobConf) throws Exception {
        Class<?> c = Class.forName(inputFormatName, true, JavaUtils.getClassLoader());
        InputFormat<?, ?> inputFormat = (InputFormat<?, ?>) c.newInstance();

        if ("org.apache.hadoop.mapred.TextInputFormat".equals(inputFormatName)) {
            ((TextInputFormat) inputFormat).configure(jobConf); // TextInputFormat needs a special configuration
        }

        return inputFormat;
    }

    /* 
     * Initialize the HiveMetaStoreClient
	 * Uses classpath configuration files to locate the MetaStore
	 */
    private void initHiveClient() {
        try {
            client = new HiveMetaStoreClient(new HiveConf());
        } catch (MetaException cause) {
            throw new RuntimeException("Failed connecting to Hive MetaStore service: " + cause.getMessage(), cause);
        }
    }

    /*
     * parseTableQualifiedName() extract the db_name and table_name from the qualifiedName.
     * qualifiedName is the Hive table name that the user enters in the CREATE EXTERNAL TABLE statement. It can be
     * either <table_name> or <db_name.table_name>.
     */
    TblDesc parseTableQualifiedName(String qualifiedName) {
        TblDesc tblDesc = new TblDesc();

        String[] toks = qualifiedName.split("[.]");
        if (toks.length == 1) {
            tblDesc.dbName = HIVE_DEFAULT_DBNAME;
            tblDesc.tableName = toks[0];
        } else if (toks.length == 2) {
            tblDesc.dbName = toks[0];
            tblDesc.tableName = toks[1];
        } else {
            tblDesc = null;
        }

        return tblDesc;
    }

    /* Goes over the table partitions metadata and extracts the splits and the InputFormat and Serde per split. */
    private void fetchTableMetaData(TblDesc tblDesc) throws Exception {
        Table tbl = client.getTable(tblDesc.dbName, tblDesc.tableName);
        String tblType = tbl.getTableType();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Table: " + tblDesc.dbName + "." + tblDesc.tableName + ", type: " + tblType);
        }

        if (TableType.valueOf(tblType) == TableType.VIRTUAL_VIEW) {
            throw new UnsupportedOperationException("PXF doesn't support HIVE views");
        }

        verifySchema(tbl);

        List<Partition> partitions = client.listPartitions(tblDesc.dbName, tblDesc.tableName, ALL_PARTS);
        StorageDescriptor descTable = tbl.getSd();
        Properties props;

        if (partitions.isEmpty()) {
            props = getSchema(tbl);
            fetchMetaDataForSimpleTable(descTable, props);
        } else {
            List<FieldSchema> partitionKeys = tbl.getPartitionKeys();

            for (Partition partition : partitions) {
                StorageDescriptor descPartition = partition.getSd();
                props = MetaStoreUtils.getSchema(descPartition,
                        descTable,
                        null, // Map<string, string> parameters - can be empty
                        tblDesc.dbName, tblDesc.tableName, // table name
                        partitionKeys);
                fetchMetaDataForPartitionedTable(descPartition, props, partition, partitionKeys, tblDesc.tableName);
            }
        }
    }

    void verifySchema(Table tbl) throws Exception {
        /* nothing to verify here */
    }

    private static Properties getSchema(Table table) {
        return MetaStoreUtils.getSchema(table.getSd(),
                table.getSd(),
                table.getParameters(),
                table.getDbName(),
                table.getTableName(),
                table.getPartitionKeys());
    }

    private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc, Properties props) throws Exception {
        fetchMetaDataForSimpleTable(stdsc, props, null);
    }

    private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc, Properties props, String tableName) throws Exception {
        fetchMetaData(new HiveTablePartition(stdsc, props, null, null, tableName));
    }

    private void fetchMetaDataForPartitionedTable(StorageDescriptor stdsc,
                                                  Properties props,
                                                  Partition partition,
                                                  List<FieldSchema> partitionKeys,
                                                  String tableName) throws Exception {
        fetchMetaData(new HiveTablePartition(stdsc, props, partition, partitionKeys, tableName));
    }

    /* Fills a table partition */
    private void fetchMetaData(HiveTablePartition tablePartition) throws Exception {
        InputFormat<?, ?> fformat = makeInputFormat(tablePartition.storageDesc.getInputFormat(), jobConf);
        FileInputFormat.setInputPaths(jobConf, new Path(tablePartition.storageDesc.getLocation()));
        InputSplit[] splits = fformat.getSplits(jobConf, 1);

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String[] hosts = fsp.getLocations();
            String filepath = fsp.getPath().toUri().getPath();

            byte[] locationInfo = HdfsUtilities.prepareFragmentMetadata(fsp);
            Fragment fragment = new Fragment(filepath, hosts, locationInfo, makeUserData(tablePartition));
            fragments.add(fragment);
        }
    }

    /* Turns a Properties class into a string */
    private String serializeProperties(Properties props) throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        props.store(outStream, ""/* comments */);
        return outStream.toString();
    }

    /* Turns the partition keys into a string */
    String serializePartitionKeys(HiveTablePartition partData) throws Exception {
        if (partData.partition == null) /* this is a simple hive table - there are no partitions */ {
            return HIVE_NO_PART_TBL;
        }

        StringBuilder partitionKeys = new StringBuilder();
        String prefix = "";
        ListIterator<String> valsIter = partData.partition.getValues().listIterator();
        ListIterator<FieldSchema> keysIter = partData.partitionKeys.listIterator();
        while (valsIter.hasNext() && keysIter.hasNext()) {
            FieldSchema key = keysIter.next();
            String name = key.getName();
            String type = key.getType();
            String val = valsIter.next();
            String oneLevel = prefix + name + HIVE_1_PART_DELIM + type + HIVE_1_PART_DELIM + val;
            partitionKeys.append(oneLevel);
            prefix = HIVE_PARTITIONS_DELIM;
        }

        return partitionKeys.toString();
    }

    byte[] makeUserData(HiveTablePartition partData) throws Exception {
        String inputFormatName = partData.storageDesc.getInputFormat();
        String serdeName = partData.storageDesc.getSerdeInfo().getSerializationLib();
        String propertiesString = serializeProperties(partData.properties);
        String partitionKeys = serializePartitionKeys(partData);
        String userData = inputFormatName + HIVE_UD_DELIM +
                serdeName + HIVE_UD_DELIM +
                propertiesString + HIVE_UD_DELIM +
                partitionKeys;

        return userData.getBytes();
    }
}
