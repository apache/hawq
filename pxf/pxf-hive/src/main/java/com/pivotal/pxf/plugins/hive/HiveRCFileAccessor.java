package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.FilterParser;
import com.pivotal.pxf.api.utilities.ColumnDescriptor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Specialization of HdfsSplittableDataAccessor for a Hive table that stores only RC files.
 * This class replaces the generic HiveAccessor for a case where a table is stored entirely as RC files.
 * Use together with HiveInputFormatFragmenter/HiveColumnarSerdeResolver
 */
public class HiveRCFileAccessor extends HdfsSplittableDataAccessor {
    private static Log Log = LogFactory.getLog(HiveRCFileAccessor.class);

    public class Partition {
        public String name;
        public String type;
        public String val;

        Partition(String inName, String inType, String inVal) {
            name = inName;
            type = inType;
            val = inVal;
        }
    }

    private List<Partition> partitions;

    /**
     * Constructs a HiveRCFileAccessor.
     * Creates the InputFormat
     */
    public HiveRCFileAccessor(InputData input) throws Exception {
        super(input, new RCFileInputFormat());
        initUserData(input);
    }

    /**
     * Overriding openForRead to enable partition filtering
     * If partition filter is set and the file currently opened by the accessor does not belong
     * to the partition we return false and stop processing for this file
     */
    @Override
    public boolean openForRead() throws Exception {
        return isOurDataInsideFilteredPartition() && super.openForRead();
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split) throws IOException {
        return new RCFileRecordReader(jobConf, (FileSplit) split);
    }

    /* read the data supplied by the fragmenter: inputformat name, serde name, partition keys */
    private void initUserData(InputData input) throws Exception {
        String[] toks = HiveInputFormatFragmenter.parseToks(input);
        initPartitionFields(toks[HiveInputFormatFragmenter.TOK_KEYS]);
    }

    /*
     * The partition fields are initialized one time base on userData provided by the fragmenter
     */
    private void initPartitionFields(String partitionKeys) {

        partitions = new LinkedList<>();

        if (partitionKeys.equals(HiveDataFragmenter.HIVE_NO_PART_TBL)) {
            return;
        }

        String[] partitionLevels = partitionKeys.split(HiveInputFormatFragmenter.HIVE_PARTITIONS_DELIM);
        for (String partLevel : partitionLevels) {
            String[] levelKey = partLevel.split(HiveInputFormatFragmenter.HIVE_1_PART_DELIM);
            String name = levelKey[0];
            String type = levelKey[1];
            String val = levelKey[2];
            partitions.add(new Partition(name, type, val));
        }
    }

    private boolean isOurDataInsideFilteredPartition() throws Exception {
        if (!inputData.hasFilter()) {
            return true;
        }

        String filterStr = inputData.getFilterString();
        HiveFilterBuilder eval = new HiveFilterBuilder(inputData);
        Object filter = eval.getFilterObject(filterStr);

        boolean returnData = isFiltered(partitions, filter);

        if (Log.isDebugEnabled()) {
            Log.debug("segmentId: " + inputData.getSegmentId() + " " + inputData.getDataSource() + "--" + filterStr + "returnData: " + returnData);
            if (filter instanceof List) {
                for (Object f : (List) filter) {
                    printOneBasicFilter(f);
                }
            } else {
                printOneBasicFilter(filter);
            }
        }

        return returnData;
    }

    private boolean isFiltered(List<Partition> partitionFields, Object filter) {
        if (filter instanceof List) {
            /*
             * We are going over each filter in the filters list and test it against all the partition fields
			 * since filters are connected only by AND operators, its enough for one filter to fail in order to
			 * deny this data.
			 */
            for (Object f : (List) filter) {
                if (!testOneFilter(partitionFields, f, inputData)) {
                    return false;
                }
            }
            return true;
        }

        return testOneFilter(partitionFields, filter, inputData);
    }

    /*
     * We are testing one filter against all the partition fields.
     * The filter has the form "fieldA = valueA".
     * The partitions have the form partitionOne=valueOne/partitionTwo=ValueTwo/partitionThree=valueThree
     * 1. For a filter to match one of the partitions, lets say partitionA for example, we need:
     * fieldA = partittionOne and valueA = valueOne. If this condition occurs, we return true.
     * 2. If fieldA does not match any one of the partition fields we also return true, it means we ignore this filter
     * because it is not on a partition field.
     * 3. If fieldA = partittionOne and valueA != valueOne, then we return false.
     */
    private boolean testOneFilter(List<Partition> partitionFields, Object filter, InputData input) {
        // Let's look first at the filter
        FilterParser.BasicFilter bFilter = (FilterParser.BasicFilter) filter;

        boolean isFilterOperationEqual = (bFilter.getOperation() == FilterParser.Operation.HDOP_EQ);
        if (!isFilterOperationEqual) /* in case this is not an "equality filter" we ignore it here - in partition filtering */ {
            return true;
        }

        int filterColumnIndex = bFilter.getColumn().index();
        String filterValue = bFilter.getConstant().constant().toString();
        ColumnDescriptor filterColumn = input.getColumn(filterColumnIndex);
        String filterColumnName = filterColumn.columnName();

        for (Partition partition : partitionFields) {
            if (filterColumnName.equals(partition.name)) {
                /* the filter field matches a partition field, but the values do not match */
                return filterValue.equals(partition.val);
            }
        }

		/* filter field did not match any partition field, so we ignore this filter and hence return true */
        return true;
    }

    private void printOneBasicFilter(Object filter) {
        FilterParser.BasicFilter bFilter = (FilterParser.BasicFilter) filter;
        boolean isOperationEqual = (bFilter.getOperation() == FilterParser.Operation.HDOP_EQ);
        int columnIndex = bFilter.getColumn().index();
        String value = bFilter.getConstant().constant().toString();
        Log.debug("isOperationEqual:  " + isOperationEqual + " columnIndex:  " + columnIndex + " value:  " + value);
    }
}
