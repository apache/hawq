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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.mapred.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

/*
 * Fragmenter class for a HIVE table
 *
 * Given a Hive table and its partitions
 * divide the data into fragments (here a data fragment is actually a HDFS file block) and return a list of them.
 * Each data fragment will contain the following information:
 * a. sourceName: full HDFS path to the data file that this data fragment is part of
 * b. hosts: a list of the datanode machines that hold a replica of this block
 * c. userData: file_input_format_name_DELIM_serde_name_DELIM_serialization_properties
 */
public class HiveDataFragmenter extends Fragmenter {
    private JobConf jobConf;
    HiveClient client;
    Log Log = LogFactory.getLog(HiveDataFragmenter.class);

    /* Encapsulates Metastore host and port*/
    static class Metastore {
        String host;
        int port;

        /* C'tor */
        Metastore(String host, int port) {
            this.host = host;
            this.port = port;
        }

        /* C'tor */
        Metastore(Metastore other) {
            this.host = other.host;
            this.port = other.port;
        }
    }

    private Metastore metastore;

    public static final String HIVE_DEFAULT_DBNAME = "default";
    public static final String HIVE_UD_DELIM = "!HUDD!";
    public static final String HIVE_1_PART_DELIM = "!H1PD!";
    public static final String HIVE_PARTITIONS_DELIM = "!HPAD!";
    public static final String HIVE_NO_PART_TBL = "!HNPT!";

    /* TODO: get rid of these */
    public static final int HIVE_MAX_PARTS = 1000;
    static final int METASTORE_DEFAULT_PORT = 9083; /* default metastore port */
    static final String METASTORE_DEFAULT_HOST = "localhost";

    /* internal class used for parsing the qualified table name received as input to getFragments() */
    class TblDesc {
        public String dbName;
        public String tableName;
    }

    /*
     * A Hive table unit - means a subset of the HIVE table, where we can say that for all files in this subset,
     * they all have the same InputFormat and Serde.
     * For a partitioned table the HiveTableUnit will be one partition and for an unpartitioned table, the
     * HiveTableUnit will be the whole table
     */
    class HiveTablePartition {
        public StorageDescriptor storageDesc;
        public Properties properties;
        public Partition partition;
        public List<FieldSchema> partitionKeys;

        public HiveTablePartition(StorageDescriptor inStorageDesc, Properties inProperties) {
            storageDesc = inStorageDesc;
            properties = inProperties;
            partition = null;
            partitionKeys = null;
        }

        public HiveTablePartition(StorageDescriptor inStorageDesc,
                                  Properties inProperties,
                                  Partition inPartition,
                                  List<FieldSchema> inPartitionKeys) {
            storageDesc = inStorageDesc;
            properties = inProperties;
            partition = inPartition;
            partitionKeys = inPartitionKeys;
        }
    }

    /*
     * C'tor
     */
    public HiveDataFragmenter(InputData md) {
        super(md);

        jobConf = new JobConf(new Configuration(), HiveDataFragmenter.class);
        client = null;
    }

    /*
     * path is a data source URI that can appear as a file
     * name, a directory name  or a wildcard returns the data
     * fragments in json format
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
    	
    	if (client == null) {
    		initHiveClient();
    	}
    	
        TblDesc tblDesc = parseTableQualifiedName(inputData.tableName());
        if (tblDesc == null) {
            throw new IllegalArgumentException(inputData.tableName() + " is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>");
        }

        fetchTableMetaData(tblDesc);

        return fragments;
    }

    /* Initialize the Hive client */
    private void initHiveClient() {
        loadHostAndPort();
        TSocket transport = new TSocket(metastore.host, metastore.port);
       
        try {
        	transport.open();
        } catch (TTransportException e) {
        	throw new RuntimeException("Failed to connect to Hive metastore: " + e.getMessage());
        }
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        client = new HiveClient(protocol);
    }

    /*
     * Load metastore host and port from configuration
     * In case of corrupted or unset configuration we stay with the hardcoded
     * METASTORE_DEFAULT_HOST and METASTORE_DEFAULT_PORT
     */
    private void loadHostAndPort() {
        HiveConf hiveConf = new HiveConf();
        /* example of hive.metastore.uris: thrift://localhost:9084*/
        metastore = parseMetastoreUri(hiveConf.getVar(ConfVars.METASTOREURIS), Log);
    }

    /*
     * Parse the Metastore uri
     * In case of corrupted or unset configuration we stay with the hardcoded
     * METASTORE_DEFAULT_HOST and METASTORE_DEFAULT_PORT
     */
    static Metastore parseMetastoreUri(String uri, Log log) {
        Metastore ms = new Metastore(METASTORE_DEFAULT_HOST, METASTORE_DEFAULT_PORT);
        if (uri == null) /* non existent property hive.metastore.uris */ {
            log.warn("Property [hive.metastore.uris] is missing from hive-site.xml. will use "
                    + "default values for metastore service host:port - localhost:9083");
            return ms;
        }

        String[] arr = uri.split("\\/\\/");
        if (arr.length != 2) /* the value of  property hive.metastore.uris is corrupted */ {
            log.warn("Property [hive.metastore.uris] in hive-site.xml. is invalid. "
                    + "host:port section is missing."
                    + "Will use default values for metastore service host:port - localhost:9083");
            return ms;
        }

        String hostport = arr[1];
        arr = hostport.split(":");
        if (arr.length != 2) /* the value of  property hive.metastore.uris is corrupted */ {
            log.warn("Property [hive.metastore.uris] in hive-site.xml. is invalid. "
                    + "There is no [:] between host and port."
                    + "Will use default values for metastore service host:port - localhost:9083");
            return ms;
        }

        String host = arr[0];
        String sport = arr[1];
        int nport = (sport != null) ? Integer.parseInt(sport) : 0;

        if (host != null && nport != 0) {
            ms = new Metastore(host, nport);
        }

        return ms;
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

    /*
     * Goes over the table partitions metadata and extracts the splits and the InputFormat and Serde per split.
     */
    private void fetchTableMetaData(TblDesc tblDesc) throws Exception {
        Table tbl = client.get_table(tblDesc.dbName, tblDesc.tableName);
        String tblType = tbl.getTableType();

        if (Log.isDebugEnabled()) {
            Log.debug("Table: " + tblDesc.dbName + "." + tblDesc.tableName + ", type: " + tblType);
        }

        if (TableType.valueOf(tblType) == TableType.VIRTUAL_VIEW) {
            throw new UnsupportedOperationException("PXF doesn't support HIVE views");
        }

        // guessing the max partitions - will have to further research this
        List<Partition> partitions = client.get_partitions(tblDesc.dbName, tblDesc.tableName, (short) HIVE_MAX_PARTS);
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
                fetchMetaDataForPrtitionedTable(descPartition, props, partition, partitionKeys);
            }
        }
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
        HiveTablePartition tablePartition = new HiveTablePartition(stdsc, props);
        fetchMetaData(tablePartition);
    }

    private void fetchMetaDataForPrtitionedTable(StorageDescriptor stdsc,
                                                 Properties props,
                                                 Partition partition,
                                                 List<FieldSchema> partitionKeys) throws Exception {
        HiveTablePartition tablePartition = new HiveTablePartition(stdsc, props, partition, partitionKeys);
        fetchMetaData(tablePartition);
    }

    /*
     * Fill a table partition
     */
    private void fetchMetaData(HiveTablePartition tablePartition) throws Exception {
        FileInputFormat<?, ?> fformat = makeInputFormat(tablePartition.storageDesc.getInputFormat(), jobConf);
        FileInputFormat.setInputPaths(jobConf, new Path(tablePartition.storageDesc.getLocation()));
        InputSplit[] splits = fformat.getSplits(jobConf, 1);

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String[] hosts = fsp.getLocations();
            String filepath = fsp.getPath().toUri().getPath();
            filepath = filepath.substring(1); // TODO - remove the '/' from the beginning - will deal with this next

            byte[] locationInfo = HdfsUtilities.prepareFragmentMetadata(fsp);
            Fragment fragment = new Fragment(filepath, hosts, locationInfo, makeUserData(tablePartition));
            fragments.add(fragment);
        }
    }

    /* Create the partition InputFormat  */
    static public FileInputFormat<?, ?> makeInputFormat(String inputFormatName, JobConf jobConf) throws Exception {
        Class<?> c = Class.forName(inputFormatName, true, JavaUtils.getClassLoader());
        FileInputFormat<?, ?> fformat = (FileInputFormat<?, ?>) c.newInstance();

        if ("org.apache.hadoop.mapred.TextInputFormat".equals(inputFormatName)) {
            ((TextInputFormat) fformat).configure(jobConf); // TextInputFormat needs a special configuration
        }

        return fformat;
    }

    /*
     * Turn a Properties class into a string
     */
    private String serializeProperties(Properties props) throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        props.store(outStream, ""/* comments */);
        return outStream.toString();
    }

    /*
     * Turn the partition keys into a string
     */
    private String serializePartitionKeys(HiveTablePartition partData) throws Exception {
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

    private byte[] makeUserData(HiveTablePartition partData) throws Exception {
        String inputFormatName = partData.storageDesc.getInputFormat();
        String serdeName = partData.storageDesc.getSerdeInfo().getSerializationLib();
        String propertiesString = serializeProperties(partData.properties);
        String partionKeys = serializePartitionKeys(partData);
        String userData = inputFormatName + HIVE_UD_DELIM +
                serdeName + HIVE_UD_DELIM +
                propertiesString + HIVE_UD_DELIM +
                partionKeys;

        return userData.getBytes();
    }
}




