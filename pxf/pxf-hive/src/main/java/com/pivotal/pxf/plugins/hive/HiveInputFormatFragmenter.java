package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.UnsupportedTypeException;
import com.pivotal.pxf.api.UserDataException;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.ColumnDescriptor;
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

import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

/**
 * Specialized Hive fragmenter for RC files tables.
 * Unlike the HiveDataFragmenter, this class does not send the serde properties to the accessor/resolvers.
 * This is done in order to avoid memory explosion in Hawq.
 * This fragmenter is used with HiveRCFileAccessor/HiveColumnarSerdeResolver.
 * <p/>
 * Given a Hive table and its partitions
 * divide the data into fragments (here a data fragment is actually a HDFS file block) and return a list of them.
 * Each data fragment will contain the following information:
 * a. sourceName: full HDFS path to the data file that this data fragment is part of
 * b. hosts: a list of the datanode machines that hold a replica of this block
 * c. userData: inputformat name, serde names and partition keys
 */
public class HiveInputFormatFragmenter extends Fragmenter {
    private JobConf jobConf;
    HiveMetaStoreClient client;
    Log Log = LogFactory.getLog(HiveInputFormatFragmenter.class);

    private static final String HIVE_DEFAULT_DBNAME = "default";
    static final String HIVE_UD_DELIM = "!HUDD!";
    static final String HIVE_1_PART_DELIM = "!H1PD!";
    static final String HIVE_PARTITIONS_DELIM = "!HPAD!";
    static final String HIVE_NO_PART_TBL = "!HNPT!";
    static final String STR_RC_FILE_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
    static final String STR_COLUMNAR_SERDE = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
    static final String STR_LAZY_BINARY_COLUMNAR_SERDE = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe";
    private static final int EXPECTED_NUM_OF_TOKS = 2;
    public static final int TOK_SERDE = 0;
    public static final int TOK_KEYS = 1;
    private static final short ALL_PARTS = -1;

    /* defines the Hive input formats currently supported in pxf */
    public enum PXF_HIVE_INPUT_FORMATS {
        RC_FILE_INPUT_FORMAT
    }

    /* defines the Hive serializers (serde classes) currently supported in pxf */
    public enum PXF_HIVE_SERDES {
        COLUMNAR_SERDE,
        LAZY_BINARY_COLUMNAR_SERDE
    }

    /* internal class used for parsing the qualified table name received as input to getFragments() */
    class TblDesc {
        public String dbName;
        public String tableName;
    }

    /*
     * A Hive table unit - means a subset of the HIVE table, where we can say that for all files in this subset,
     * they all have the same InputFormat and Serde.
     * For a partitioned table the HiveTablePartition will be one partition and for an unpartitioned table, the
     * HiveTablePartition will be the whole table
     */
    class HiveTablePartition {
        public StorageDescriptor storageDesc;
        public Properties properties;
        public Partition partition;
        public List<FieldSchema> partitionKeys;
        public String tableName;

        public HiveTablePartition(StorageDescriptor inStorageDesc, Properties inProperties, String tblName) {
            storageDesc = inStorageDesc;
            properties = inProperties;
            tableName = tblName;
            partition = null;
            partitionKeys = null;
        }

        public HiveTablePartition(StorageDescriptor inStorageDesc,
                                  Properties inProperties,
                                  String tblName,
                                  Partition inPartition,
                                  List<FieldSchema> inPartitionKeys) {
            storageDesc = inStorageDesc;
            properties = inProperties;
            tableName = tblName;
            partition = inPartition;
            partitionKeys = inPartitionKeys;
        }
    }

    /**
     * Constructs a HiveInputFormatFragmenter
     */
    public HiveInputFormatFragmenter(InputData md) {
        super(md);

        jobConf = new JobConf(new Configuration(), HiveInputFormatFragmenter.class);
        initHiveClient();
    }

    /**
     * Extracts the user data
     */
    static public String[] parseToks(InputData input) throws Exception {
        String userData = new String(input.getFragmentUserData());
        String[] toks = userData.split(HIVE_UD_DELIM);
        if (toks.length != (EXPECTED_NUM_OF_TOKS)) {
            throw new UserDataException("HiveInputFormatFragmenter expected " + EXPECTED_NUM_OF_TOKS + " tokens, but got " + toks.length);
        }

        return toks;
    }

    /**
     * path is a data source URI that can appear as a file
     * name, a directory name  or a wildcard returns the data
     * fragments in json format
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        TblDesc tblDesc = parseTableQualifiedName(inputData.getDataSource());
        if (tblDesc == null) {
            throw new IllegalArgumentException(inputData.getDataSource() + " is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>");
        }

        fetchTableMetaData(tblDesc);

        return fragments;
    }

    /*
     * Checks that hive fields and partitions match the HAWQ schema.
     * Throws an exception if:
     * - the number of fields (+ partitions) do not match the HAWQ table definition.
     * - the hive fields types do not match the HAWQ fields.
     */
    private void verifySchema(Table tbl) throws Exception {

        int columnsSize = inputData.getColumns();
        int hiveColumnsSize = tbl.getSd().getColsSize();
        int hivePartitionsSize = tbl.getPartitionKeysSize();

        if (Log.isDebugEnabled()) {
            Log.debug("Hive table: " + hiveColumnsSize + " fields, " + hivePartitionsSize + " partitions. " +
                    "HAWQ table: " + columnsSize + " fields.");
        }

        // check schema size
        if (columnsSize != (hiveColumnsSize + hivePartitionsSize)) {
            throw new IllegalArgumentException("Hive table schema (" + hiveColumnsSize + " fields, "
                    + hivePartitionsSize + " partitions) " + "doesn't match PXF table (" + columnsSize + " fields)");
        }

        int index = 0;
        // check hive fields
        List<FieldSchema> hiveColumns = tbl.getSd().getCols();
        for (FieldSchema hiveCol : hiveColumns) {
            ColumnDescriptor colDesc = inputData.getColumn(index);
            DataType colType = DataType.get(colDesc.columnTypeCode());
            compareTypes(colType, hiveCol.getType(), colDesc.columnName());
            index++;
        }
        // check partition fields
        List<FieldSchema> hivePartitions = tbl.getPartitionKeys();
        for (FieldSchema hivePart : hivePartitions) {
            ColumnDescriptor colDesc = inputData.getColumn(index);
            DataType colType = DataType.get(colDesc.columnTypeCode());
            compareTypes(colType, hivePart.getType(), colDesc.columnName());
            index++;
        }

    }

    private void compareTypes(DataType type, String hiveType, String fieldName) {
        String convertedHive = toHiveType(type, fieldName);
        if (!convertedHive.equals(hiveType) && !(convertedHive.equals("smallint") && hiveType.equals("tinyint"))) {
            throw new UnsupportedTypeException("Schema mismatch definition: Field " + fieldName +
                    " (Hive type " + hiveType + ", HAWQ type " + type.toString() + ")");
        }
        if (Log.isDebugEnabled()) {
            Log.debug("Field " + fieldName + ": Hive type " + hiveType + ", HAWQ type " + type.toString());
        }
    }

    /**
     * Converts HAWQ type to hive type.
     * The supported mappings are:
     * BOOLEAN -> boolean
     * SMALLINT -> smallint (tinyint is converted to smallint)
     * BIGINT -> bigint
     * TIMESTAMP, TIME -> timestamp
     * NUMERIC -> decimal
     * BYTEA -> binary
     * INTERGER -> int
     * TEXT -> string
     * REAL -> float
     * FLOAT8 -> double
     * <p/>
     * All other types (both in HAWQ and in HIVE) are not supported.
     *
     * @param type HAWQ data type
     * @param name field name
     * @return Hive type
     */
    public static String toHiveType(DataType type, String name) {
        switch (type) {
            case BOOLEAN:
            case SMALLINT:
            case BIGINT:
            case TIMESTAMP:
                return type.toString().toLowerCase();
            case NUMERIC:
                return "decimal";
            case BYTEA:
                return "binary";
            case INTEGER:
                return "int";
            case TEXT:
                return "string";
            case REAL:
                return "float";
            case FLOAT8:
                return "double";
            case TIME:
                return "timestamp";
            default:
                throw new UnsupportedTypeException(type.toString()
                        + " conversion is not supported by HiveInputFormatFragmenter (Field " + name + ")");
        }
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

    /*
     * Goes over the table partitions metadata and extracts the splits.
     */
    private void fetchTableMetaData(TblDesc tblDesc) throws Exception {
        Table tbl = client.getTable(tblDesc.dbName, tblDesc.tableName);
        String tblType = tbl.getTableType();

        if (Log.isDebugEnabled()) {
            Log.debug("Table: " + tblDesc.dbName + "." + tblDesc.tableName + ", type: " + tblType);
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
            fetchMetaDataForSimpleTable(descTable, props, tblDesc.tableName);
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

    private static Properties getSchema(Table table) {
        return MetaStoreUtils.getSchema(table.getSd(),
                table.getSd(),
                table.getParameters(),
                table.getDbName(),
                table.getTableName(),
                table.getPartitionKeys());
    }

    private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc, Properties props, String tableName) throws Exception {
        HiveTablePartition tablePartition = new HiveTablePartition(stdsc, props, tableName);
        fetchMetaData(tablePartition);
    }

    private void fetchMetaDataForPartitionedTable(StorageDescriptor stdsc,
                                                  Properties props,
                                                  Partition partition,
                                                  List<FieldSchema> partitionKeys,
                                                  String tableName) throws Exception {
        HiveTablePartition tablePartition = new HiveTablePartition(stdsc, props, tableName, partition, partitionKeys);
        fetchMetaData(tablePartition);
    }

    /* Fills a table partition */
    private void fetchMetaData(HiveTablePartition tablePartition) throws Exception {
        FileInputFormat<?, ?> fformat = makeInputFormat(tablePartition.storageDesc.getInputFormat(), jobConf);
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

    /**
     * Creates the partition InputFormat
     */
    static public FileInputFormat<?, ?> makeInputFormat(String inputFormatName, JobConf jobConf) throws Exception {
        Class<?> c = Class.forName(inputFormatName, true, JavaUtils.getClassLoader());
        FileInputFormat<?, ?> fformat = (FileInputFormat<?, ?>) c.newInstance();

        if ("org.apache.hadoop.mapred.TextInputFormat".equals(inputFormatName)) {
            ((TextInputFormat) fformat).configure(jobConf); // TextInputFormat needs a special configuration
        }

        return fformat;
    }

    /* Turn the partition keys into a string */
    private String serializePartitionKeys(HiveTablePartition partData) throws Exception {
        if (partData.partition == null) { // this is a simple hive table - there are no partitions
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

    /* Creates a full name for the partition */
    private String fullPartitionName(HiveTablePartition partData) {
        return (partData.partition == null)
                ? "table - " + partData.tableName
                : "table - " + partData.tableName + ", partition - " + partData.partition;
    }

    /*
     * Validates that partition format corresponds to PXF supported formats and transforms the class name
     * to an enumeration for writing it to the accessors on other PXF instances.
     */
    private String assertRCFile(String className, HiveTablePartition partData) throws Exception {
        if (className.equals(STR_RC_FILE_INPUT_FORMAT)) {
            return PXF_HIVE_INPUT_FORMATS.RC_FILE_INPUT_FORMAT.name();
        }

    	/* if we got here it means the partition was written to HDFS with an unsupported InputFormat */
        throw new IllegalArgumentException("HiveInputFormatFragmenter does not yet support " + className +
                " for " + fullPartitionName(partData) + ". Supported InputFormat is " + STR_RC_FILE_INPUT_FORMAT);
    }

    /*
     * Validates that partition serde corresponds to PXF supported serdes and transforms the class name
     * to an enumeration for writing it to the resolvers on other PXF instances.
     */
    private String assertSerde(String className, HiveTablePartition partData) throws Exception {
        if (className.equals(STR_COLUMNAR_SERDE)) {
            return PXF_HIVE_SERDES.COLUMNAR_SERDE.name();
        } else if (className.equals(STR_LAZY_BINARY_COLUMNAR_SERDE)) {
            return PXF_HIVE_SERDES.LAZY_BINARY_COLUMNAR_SERDE.name();
        }

    	/* if we got here it means that the Hive records were serialized with an unsupported Serde */
        throw new UnsupportedTypeException("HiveInputFormatFragmenter does not yet support  " + className +
                " for " + fullPartitionName(partData) + ". Supported serializers are: " + STR_COLUMNAR_SERDE + " or " + STR_LAZY_BINARY_COLUMNAR_SERDE);
    }

    private byte[] makeUserData(HiveTablePartition partData) throws Exception {
        String inputFormatName = partData.storageDesc.getInputFormat();
        String serdeName = partData.storageDesc.getSerdeInfo().getSerializationLib();
        String partitionKeys = serializePartitionKeys(partData);

        assertRCFile(inputFormatName, partData);
        String userData = assertSerde(serdeName, partData) + HIVE_UD_DELIM + partitionKeys;

        return userData.getBytes();
    }
}
