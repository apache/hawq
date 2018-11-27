package org.apache.hawq.pxf.plugins.hive;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hawq.pxf.api.BasicFilter;
import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.LogicalFilter;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.HdfsSplittableDataAccessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hawq.pxf.api.io.DataType.*;
import static org.apache.hawq.pxf.api.io.DataType.BPCHAR;
import static org.apache.hawq.pxf.api.io.DataType.BYTEA;

/**
 * Accessor for Hive tables. The accessor will open and read a split belonging
 * to a Hive table. Opening a split means creating the corresponding InputFormat
 * and RecordReader required to access the split's data. The actual record
 * reading is done in the base class -
 * {@link org.apache.hawq.pxf.plugins.hdfs.HdfsSplittableDataAccessor}. <br>
 * HiveAccessor will also enforce Hive partition filtering by filtering-out a
 * split which does not belong to a partition filter. Naturally, the partition
 * filtering will be done only for Hive tables that are partitioned.
 */
public class HiveAccessor extends HdfsSplittableDataAccessor {
    private static final Log LOG = LogFactory.getLog(HiveAccessor.class);
    List<HivePartition> partitions;
    String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    class HivePartition {
        public String name;
        public String type;
        public String val;

        HivePartition(String name, String type, String val) {
            this.name = name;
            this.type = type;
            this.val = val;
        }
    }

    protected Boolean filterInFragmenter = false;

    /**
     * Constructs a HiveAccessor and creates an InputFormat (derived from
     * {@link org.apache.hadoop.mapred.InputFormat}) and the Hive partition
     * fields
     *
     * @param input contains the InputFormat class name and the partition fields
     * @throws Exception if failed to create input format
     */
    public HiveAccessor(InputData input) throws Exception {
        /*
         * Unfortunately, Java does not allow us to call a function before
         * calling the base constructor, otherwise it would have been:
         * super(input, createInputFormat(input))
         */
        super(input, null);
        inputFormat = createInputFormat(input);
    }

    /**
     * Constructs a HiveAccessor
     *
     * @param input contains the InputFormat class name and the partition fields
     * @param inputFormat Hive InputFormat
     */
    public HiveAccessor(InputData input, InputFormat<?, ?> inputFormat) {
        super(input, inputFormat);
    }

    /**
     * Opens Hive data split for read. Enables Hive partition filtering. <br>
     *
     * @return true if there are no partitions or there is no partition filter
     *         or partition filter is set and the file currently opened by the
     *         accessor belongs to the partition.
     * @throws Exception if filter could not be built, connection to Hive failed
     *             or resource failed to open
     */
    @Override
    public boolean openForRead() throws Exception {
        return isOurDataInsideFilteredPartition() && super.openForRead();
    }

    /**
     * Creates the RecordReader suitable for this given split.
     *
     * @param jobConf configuration data for the Hadoop framework
     * @param split the split that was allocated for reading to this accessor
     * @return record reader
     * @throws IOException if failed to create record reader
     */
    @Override
    protected Object getReader(JobConf jobConf, InputSplit split)
            throws IOException {
        return inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
    }

    /*
     * Parses the user-data supplied by the HiveFragmenter from InputData. Based
     * on the user-data construct the partition fields and the InputFormat for
     * current split
     */
    private InputFormat<?, ?> createInputFormat(InputData input)
            throws Exception {
        String userData = new String(input.getFragmentUserData());
        String[] toks = userData.split(HiveDataFragmenter.HIVE_UD_DELIM);
        initPartitionFields(toks[3]);
        filterInFragmenter = new Boolean(toks[4]);
        return HiveDataFragmenter.makeInputFormat(
                toks[0]/* inputFormat name */, jobConf);
    }

    /*
     * The partition fields are initialized one time base on userData provided
     * by the fragmenter
     */
    void initPartitionFields(String partitionKeys) {
        partitions = new LinkedList<HivePartition>();
        if (partitionKeys.equals(HiveDataFragmenter.HIVE_NO_PART_TBL)) {
            return;
        }

        String[] partitionLevels = partitionKeys.split(HiveDataFragmenter.HIVE_PARTITIONS_DELIM);
        for (String partLevel : partitionLevels) {
            String[] levelKey = partLevel.split(HiveDataFragmenter.HIVE_1_PART_DELIM);
            String name = levelKey[0];
            String type = levelKey[1];
            String val = levelKey[2];
            partitions.add(new HivePartition(name, type, val));
        }
    }

    private boolean isOurDataInsideFilteredPartition() throws Exception {
        if (!inputData.hasFilter()) {
            return true;
        }

        if (filterInFragmenter) {
            LOG.debug("filtering was done in fragmenter");
            return true;
        }

        String filterStr = inputData.getFilterString();
        HiveFilterBuilder eval = new HiveFilterBuilder(inputData);
        Object filter = eval.getFilterObject(filterStr);

        boolean returnData = isFiltered(partitions, filter);

        if (LOG.isDebugEnabled()) {
            LOG.debug("segmentId: " + inputData.getSegmentId() + " "
                    + inputData.getDataSource() + "--" + filterStr
                    + " returnData: " + returnData);
            if (filter instanceof LogicalFilter) {
                printLogicalFilter((LogicalFilter) filter);
            } else {
                printOneBasicFilter(filter);
            }
        }

        return returnData;
    }

    private boolean isFiltered(List<HivePartition> partitionFields,
                               Object filter) {
        if (filter instanceof List) {
            /*
             * We are going over each filter in the filters list and test it
             * against all the partition fields since filters are connected only
             * by AND operators, its enough for one filter to fail in order to
             * deny this data.
             */
            for (Object f : (List<?>) filter) {
                if (!testOneFilter(partitionFields, f, inputData)) {
                    return false;
                }
            }
            return true;
        }

        return testOneFilter(partitionFields, filter, inputData);
    }

    private boolean testForUnsupportedOperators(List<Object> filterList) {
        boolean nonAndOp = true;
        for (Object filter : filterList) {
            if (filter instanceof LogicalFilter) {
                if (((LogicalFilter) filter).getOperator() != FilterParser.LogicalOperation.HDOP_AND)
                    return false;
                if (((LogicalFilter) filter).getFilterList() != null)
                    nonAndOp = testForUnsupportedOperators(((LogicalFilter) filter).getFilterList());
            }
        }
        return nonAndOp;
    }

    private boolean testForPartitionEquality(List<HivePartition> partitionFields, List<Object> filterList, InputData input) {
        boolean partitionAllowed = true;
        for (Object filter : filterList) {
            if (filter instanceof BasicFilter) {
                BasicFilter bFilter = (BasicFilter) filter;
                boolean isFilterOperationEqual = (bFilter.getOperation() == FilterParser.Operation.HDOP_EQ);
                if (!isFilterOperationEqual) /*
                                      * in case this is not an "equality filter"
                                      * we ignore it here - in partition
                                      * filtering
                                      */{
                    return true;
                }

                int filterColumnIndex = bFilter.getColumn().index();
                String filterValue = bFilter.getConstant().constant().toString();
                ColumnDescriptor filterColumn = input.getColumn(filterColumnIndex);
                String filterColumnName = filterColumn.columnName();

                for (HivePartition partition : partitionFields) {
                    if (filterColumnName.equals(partition.name)) {

                /*
                 * the filter field matches a partition field, but the values do
                 * not match
                 */
                        boolean keepPartition = filterValue.equals(partition.val);

                        /*
                         * If the string comparison fails then we should check the comparison of
                         * the two operands as typed values
                         * If the partition value equals HIVE_DEFAULT_PARTITION just skip
                         */
                        if (!keepPartition && !partition.val.equals(HIVE_DEFAULT_PARTITION)){
                            keepPartition = testFilterByType(filterValue, partition);
                        }
                        return keepPartition;
                    }
                }

        /*
         * filter field did not match any partition field, so we ignore this
         * filter and hence return true
         */
            } else if (filter instanceof LogicalFilter) {
                partitionAllowed = testForPartitionEquality(partitionFields, ((LogicalFilter) filter).getFilterList(), input);
            }
        }
        return partitionAllowed;
    }

    /*
     * Given two values in String form and their type, convert each to the same type do an equality check
     */
    private boolean testFilterByType(String filterValue, HivePartition partition) {
        boolean result;
        switch (partition.type) {
            case serdeConstants.BOOLEAN_TYPE_NAME:
                result = Boolean.valueOf(filterValue).equals(Boolean.valueOf(partition.val));
                break;
            case serdeConstants.TINYINT_TYPE_NAME:
            case serdeConstants.SMALLINT_TYPE_NAME:
                result = (Short.parseShort(filterValue) == Short.parseShort(partition.val));
                break;
            case serdeConstants.INT_TYPE_NAME:
                result = (Integer.parseInt(filterValue) == Integer.parseInt(partition.val));
                break;
            case serdeConstants.BIGINT_TYPE_NAME:
                result = (Long.parseLong(filterValue) == Long.parseLong(partition.val));
                break;
            case serdeConstants.FLOAT_TYPE_NAME:
                result = (Float.parseFloat(filterValue) == Float.parseFloat(partition.val));
                break;
            case serdeConstants.DOUBLE_TYPE_NAME:
                result = (Double.parseDouble(filterValue) == Double.parseDouble(partition.val));
                break;
            case serdeConstants.TIMESTAMP_TYPE_NAME:
                result = Timestamp.valueOf(filterValue).equals(Timestamp.valueOf(partition.val));
                break;
            case serdeConstants.DATE_TYPE_NAME:
                result = Date.valueOf(filterValue).equals(Date.valueOf(partition.val));
                break;
            case serdeConstants.DECIMAL_TYPE_NAME:
                result = HiveDecimal.create(filterValue).bigDecimalValue().equals(HiveDecimal.create(partition.val).bigDecimalValue());
                break;
            case serdeConstants.BINARY_TYPE_NAME:
                result = filterValue.getBytes().equals(partition.val.getBytes());
                break;
            case serdeConstants.STRING_TYPE_NAME:
            case serdeConstants.VARCHAR_TYPE_NAME:
            case serdeConstants.CHAR_TYPE_NAME:
            default:
               result = false;
        }

        return result;
    }

    /*
     * We are testing one filter against all the partition fields. The filter
     * has the form "fieldA = valueA". The partitions have the form
     * partitionOne=valueOne/partitionTwo=ValueTwo/partitionThree=valueThree 1.
     * For a filter to match one of the partitions, lets say partitionA for
     * example, we need: fieldA = partittionOne and valueA = valueOne. If this
     * condition occurs, we return true. 2. If fieldA does not match any one of
     * the partition fields we also return true, it means we ignore this filter
     * because it is not on a partition field. 3. If fieldA = partittionOne and
     * valueA != valueOne, then we return false.
     */
    private boolean testOneFilter(List<HivePartition> partitionFields,
                                  Object filter, InputData input) {
        // Let's look first at the filter and escape if there are any OR or NOT ops
        if (!testForUnsupportedOperators(Arrays.asList(filter)))
            return true;

        return testForPartitionEquality(partitionFields, Arrays.asList(filter), input);
    }

    private void printLogicalFilter(LogicalFilter filter) {
        for (Object f : filter.getFilterList()) {
            if (f instanceof LogicalFilter) {
                printLogicalFilter((LogicalFilter) f);
            } else {
                printOneBasicFilter(f);
            }
        }
    }

    private void printOneBasicFilter(Object filter) {
        BasicFilter bFilter = (BasicFilter) filter;
        boolean isOperationEqual = (bFilter.getOperation() == FilterParser.Operation.HDOP_EQ);
        int columnIndex = bFilter.getColumn().index();
        String value = bFilter.getConstant() == null ? null : bFilter.getConstant().constant().toString();
        LOG.debug("isOperationEqual: " + isOperationEqual + " columnIndex: "
                + columnIndex + " value: " + value);
    }
}
