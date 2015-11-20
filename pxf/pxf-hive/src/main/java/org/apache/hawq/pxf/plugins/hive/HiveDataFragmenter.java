package org.apache.hawq.pxf.plugins.hive;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.api.Metadata;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;

/**
 * Fragmenter class for HIVE tables.
 * <br>
 * Given a Hive table and its partitions divide the data into fragments (here a
 * data fragment is actually a HDFS file block) and return a list of them. Each
 * data fragment will contain the following information:
 * <ol>
 * <li>sourceName: full HDFS path to the data file that this data fragment is
 * part of</li>
 * <li>hosts: a list of the datanode machines that hold a replica of this block</li>
 * <li>userData:
 * file_input_format_name_DELIM_serde_name_DELIM_serialization_properties</li>
 * </ol>
 */
public class HiveDataFragmenter extends Fragmenter {
    private static final Log LOG = LogFactory.getLog(HiveDataFragmenter.class);
    private static final short ALL_PARTS = -1;

    static final String HIVE_UD_DELIM = "!HUDD!";
    static final String HIVE_1_PART_DELIM = "!H1PD!";
    static final String HIVE_PARTITIONS_DELIM = "!HPAD!";
    static final String HIVE_NO_PART_TBL = "!HNPT!";

    static final String HIVE_API_EQ = " = ";
    static final String HIVE_API_DQUOTE = "\"";

    private JobConf jobConf;
    private HiveMetaStoreClient client;

    protected boolean filterInFragmenter = false;

    // Data structure to hold hive partition names if exist, to be used by
    // partition filtering
    private Set<String> setPartitions = new TreeSet<String>(
            String.CASE_INSENSITIVE_ORDER);

    /**
     * A Hive table unit - means a subset of the HIVE table, where we can say
     * that for all files in this subset, they all have the same InputFormat and
     * Serde. For a partitioned table the HiveTableUnit will be one partition
     * and for an unpartitioned table, the HiveTableUnit will be the whole table
     */
    class HiveTablePartition {
        StorageDescriptor storageDesc;
        Properties properties;
        Partition partition;
        List<FieldSchema> partitionKeys;
        String tableName;

        HiveTablePartition(StorageDescriptor storageDesc,
                           Properties properties, Partition partition,
                           List<FieldSchema> partitionKeys, String tableName) {
            this.storageDesc = storageDesc;
            this.properties = properties;
            this.partition = partition;
            this.partitionKeys = partitionKeys;
            this.tableName = tableName;
        }

        @Override
        public String toString() {
            return "table - " + tableName
                    + ((partition == null) ? "" : ", partition - " + partition);
        }
    }

    /**
     * Constructs a HiveDataFragmenter object.
     *
     * @param inputData all input parameters coming from the client
     */
    public HiveDataFragmenter(InputData inputData) {
        this(inputData, HiveDataFragmenter.class);
    }

    /**
     * Constructs a HiveDataFragmenter object.
     *
     * @param inputData all input parameters coming from the client
     * @param clazz Class for JobConf
     */
    public HiveDataFragmenter(InputData inputData, Class<?> clazz) {
        super(inputData);
        jobConf = new JobConf(new Configuration(), clazz);
        client = HiveUtilities.initHiveClient();
    }

    @Override
    public List<Fragment> getFragments() throws Exception {
        Metadata.Table tblDesc = HiveUtilities.parseTableQualifiedName(inputData.getDataSource());

        fetchTableMetaData(tblDesc);

        return fragments;
    }

    /**
     * Creates the partition InputFormat.
     *
     * @param inputFormatName input format class name
     * @param jobConf configuration data for the Hadoop framework
     * @return a {@link org.apache.hadoop.mapred.InputFormat} derived object
     * @throws Exception if failed to create input format
     */
    public static InputFormat<?, ?> makeInputFormat(String inputFormatName,
                                                    JobConf jobConf)
            throws Exception {
        Class<?> c = Class.forName(inputFormatName, true,
                JavaUtils.getClassLoader());
        InputFormat<?, ?> inputFormat = (InputFormat<?, ?>) c.newInstance();

        if ("org.apache.hadoop.mapred.TextInputFormat".equals(inputFormatName)) {
            // TextInputFormat needs a special configuration
            ((TextInputFormat) inputFormat).configure(jobConf);
        }

        return inputFormat;
    }

    /*
     * Goes over the table partitions metadata and extracts the splits and the
     * InputFormat and Serde per split.
     */
    private void fetchTableMetaData(Metadata.Table tblDesc) throws Exception {

        Table tbl = HiveUtilities.getHiveTable(client, tblDesc);

        verifySchema(tbl);

        List<Partition> partitions = null;
        String filterStringForHive = "";

        // If query has filter and hive table has partitions, prepare the filter
        // string for hive metastore and retrieve only the matched partitions
        if (inputData.hasFilter() && tbl.getPartitionKeysSize() > 0) {

            // Save all hive partition names in a set for later filter match
            for (FieldSchema fs : tbl.getPartitionKeys()) {
                setPartitions.add(fs.getName());
            }

            LOG.debug("setPartitions :" + setPartitions);

            // Generate filter string for retrieve match pxf filter/hive
            // partition name
            filterStringForHive = buildFilterStringForHive();
        }

        if (!filterStringForHive.isEmpty()) {

            LOG.debug("Filter String for Hive partition retrieval : "
                    + filterStringForHive);

            filterInFragmenter = true;

            // API call to Hive Metastore, will return a List of all the
            // partitions for this table, that matches the partition filters
            // Defined in filterStringForHive.
            partitions = client.listPartitionsByFilter(tblDesc.getDbName(),
                    tblDesc.getTableName(), filterStringForHive, ALL_PARTS);

            // No matched partitions for the filter, no fragments to return.
            if (partitions == null || partitions.isEmpty()) {

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Table -  " + tblDesc.getDbName() + "."
                            + tblDesc.getTableName()
                            + " Has no matched partitions for the filter : "
                            + filterStringForHive);
                }
                return;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Table -  " + tblDesc.getDbName() + "."
                        + tblDesc.getTableName()
                        + " Matched partitions list size: " + partitions.size());
            }

        } else {
            // API call to Hive Metastore, will return a List of all the
            // partitions for this table (no filtering)
            partitions = client.listPartitions(tblDesc.getDbName(),
                    tblDesc.getTableName(), ALL_PARTS);
        }

        StorageDescriptor descTable = tbl.getSd();
        Properties props;

        if (partitions.isEmpty()) {
            props = getSchema(tbl);
            fetchMetaDataForSimpleTable(descTable, props);
        } else {
            List<FieldSchema> partitionKeys = tbl.getPartitionKeys();

            for (Partition partition : partitions) {
                StorageDescriptor descPartition = partition.getSd();
                props = MetaStoreUtils.getSchema(descPartition, descTable,
                        null, // Map<string, string> parameters - can be empty
                        tblDesc.getDbName(), tblDesc.getTableName(), // table
                                                                     // name
                        partitionKeys);
                fetchMetaDataForPartitionedTable(descPartition, props,
                        partition, partitionKeys, tblDesc.getTableName());
            }
        }
    }

    void verifySchema(Table tbl) throws Exception {
        /* nothing to verify here */
    }

    private static Properties getSchema(Table table) {
        return MetaStoreUtils.getSchema(table.getSd(), table.getSd(),
                table.getParameters(), table.getDbName(), table.getTableName(),
                table.getPartitionKeys());
    }

    private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc,
                                             Properties props) throws Exception {
        fetchMetaDataForSimpleTable(stdsc, props, null);
    }

    private void fetchMetaDataForSimpleTable(StorageDescriptor stdsc,
                                             Properties props, String tableName)
            throws Exception {
        fetchMetaData(new HiveTablePartition(stdsc, props, null, null,
                tableName));
    }

    private void fetchMetaDataForPartitionedTable(StorageDescriptor stdsc,
                                                  Properties props,
                                                  Partition partition,
                                                  List<FieldSchema> partitionKeys,
                                                  String tableName)
            throws Exception {
        fetchMetaData(new HiveTablePartition(stdsc, props, partition,
                partitionKeys, tableName));
    }

    /* Fills a table partition */
    private void fetchMetaData(HiveTablePartition tablePartition)
            throws Exception {
        InputFormat<?, ?> fformat = makeInputFormat(
                tablePartition.storageDesc.getInputFormat(), jobConf);
        FileInputFormat.setInputPaths(jobConf, new Path(
                tablePartition.storageDesc.getLocation()));

        InputSplit[] splits = null;
        try {
            splits = fformat.getSplits(jobConf, 1);
        } catch (org.apache.hadoop.mapred.InvalidInputException e) {
            LOG.debug("getSplits failed on " + e.getMessage());
            return;
        }

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String[] hosts = fsp.getLocations();
            String filepath = fsp.getPath().toUri().getPath();

            byte[] locationInfo = HdfsUtilities.prepareFragmentMetadata(fsp);
            Fragment fragment = new Fragment(filepath, hosts, locationInfo,
                    makeUserData(tablePartition));
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
        if (partData.partition == null) /*
                                         * this is a simple hive table - there
                                         * are no partitions
                                         */{
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
            String oneLevel = prefix + name + HIVE_1_PART_DELIM + type
                    + HIVE_1_PART_DELIM + val;
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
        String userData = inputFormatName + HIVE_UD_DELIM + serdeName
                + HIVE_UD_DELIM + propertiesString + HIVE_UD_DELIM
                + partitionKeys + HIVE_UD_DELIM + filterInFragmenter;

        return userData.getBytes();
    }

    /*
     * Build filter string for HiveMetaStoreClient.listPartitionsByFilter API
     * method.
     *
     * The filter string parameter for
     * HiveMetaStoreClient.listPartitionsByFilter will be created from the
     * incoming getFragments filter string parameter. It will be in a format of:
     * [PARTITON1 NAME] = \"[PARTITON1 VALUE]\" AND [PARTITON2 NAME] =
     * \"[PARTITON2 VALUE]\" ... Filtering can be done only on string partition
     * keys and AND operators.
     *
     * For Example for query: SELECT * FROM TABLE1 WHERE part1 = 'AAAA' AND
     * part2 = '1111' For HIVE HiveMetaStoreClient.listPartitionsByFilter, the
     * incoming HAWQ filter string will be mapped into :
     * "part1 = \"AAAA\" and part2 = \"1111\""
     */
    private String buildFilterStringForHive() throws Exception {

        StringBuilder filtersString = new StringBuilder();
        String filterInput = inputData.getFilterString();

        if (LOG.isDebugEnabled()) {

            for (ColumnDescriptor cd : inputData.getTupleDescription()) {
                LOG.debug("ColumnDescriptor : " + cd);
            }

            LOG.debug("Filter string input : " + inputData.getFilterString());
        }

        HiveFilterBuilder eval = new HiveFilterBuilder(inputData);
        Object filter = eval.getFilterObject(filterInput);

        String prefix = "";

        if (filter instanceof List) {

            for (Object f : (List<?>) filter) {
                if (buildSingleFilter(f, filtersString, prefix)) {
                    // Set 'and' operator between each matched partition filter.
                    prefix = " and ";
                }
            }

        } else {
            buildSingleFilter(filter, filtersString, prefix);
        }

        return filtersString.toString();
    }

    /*
     * Build filter string for a single filter and append to the filters string.
     * Filter string shell be added if filter name match hive partition name
     * Single filter will be in a format of: [PARTITON NAME] = \"[PARTITON
     * VALUE]\"
     */
    private boolean buildSingleFilter(Object filter,
                                      StringBuilder filtersString, String prefix)
            throws Exception {

        // Let's look first at the filter
        FilterParser.BasicFilter bFilter = (FilterParser.BasicFilter) filter;

        // In case this is not an "equality filter", we ignore this filter (no
        // add to filter list)
        if (!(bFilter.getOperation() == FilterParser.Operation.HDOP_EQ)) {
            LOG.debug("Filter operator is not EQ, ignore this filter for hive : "
                    + filter);
            return false;
        }

        // Extract column name and value
        int filterColumnIndex = bFilter.getColumn().index();
        String filterValue = bFilter.getConstant().constant().toString();
        ColumnDescriptor filterColumn = inputData.getColumn(filterColumnIndex);
        String filterColumnName = filterColumn.columnName();

        // In case this filter is not a partition, we ignore this filter (no add
        // to filter list)
        if (!setPartitions.contains(filterColumnName)) {
            LOG.debug("Filter name is not a partition , ignore this filter for hive: "
                    + filter);
            return false;
        }

        filtersString.append(prefix);
        filtersString.append(filterColumnName);
        filtersString.append(HIVE_API_EQ);
        filtersString.append(HIVE_API_DQUOTE);
        filtersString.append(filterValue);
        filtersString.append(HIVE_API_DQUOTE);

        return true;
    }

    /**
     * Returns statistics for Hive table. Currently it's not implemented.
     */
    @Override
    public FragmentsStats getFragmentsStats() throws Exception {
        throw new UnsupportedOperationException("ANALYZE for Hive plugin is not supported");
    }
}
