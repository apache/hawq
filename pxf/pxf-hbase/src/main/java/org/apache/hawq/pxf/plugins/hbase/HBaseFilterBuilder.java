package org.apache.hawq.pxf.plugins.hbase;

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


import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import org.apache.hawq.pxf.plugins.hbase.utilities.HBaseIntegerComparator;
import org.apache.hawq.pxf.plugins.hbase.utilities.HBaseTupleDescription;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hawq.pxf.api.io.DataType.TEXT;

/**
 * This is the implementation of {@code FilterParser.FilterBuilder} for HBase.
 * <p>
 * The class uses the filter parser code to build a filter object,
 * either simple (single {@link Filter} class) or a compound ({@link FilterList})
 * for {@link HBaseAccessor} to use for its scan.
 * <p>
 * This is done before the scan starts. It is not a scan time operation.
 * <p>
 * HBase row key column is a special case.
 * If the user defined row key column as TEXT and used {@code <,>,<=,>=,=} operators
 * the startkey ({@code >/>=}) and the endkey ({@code </<=}) are stored in addition to
 * the created filter.
 * This is an addition on top of regular filters and does not replace
 * any logic in HBase filter objects.
 */
public class HBaseFilterBuilder implements FilterParser.FilterBuilder {
    private Map<FilterParser.Operation, CompareFilter.CompareOp> operatorsMap;
    private byte[] startKey;
    private byte[] endKey;
    private HBaseTupleDescription tupleDescription;

    public HBaseFilterBuilder(HBaseTupleDescription tupleDescription) {
        initOperatorsMap();
        startKey = HConstants.EMPTY_START_ROW;
        endKey = HConstants.EMPTY_END_ROW;
        this.tupleDescription = tupleDescription;
    }

    /**
     * Translates a filterString into a HBase {@link Filter} object.
     *
     * @param filterString filter string
     * @return filter object
     * @throws Exception if parsing failed
     */
    public Filter getFilterObject(String filterString) throws Exception {
        FilterParser parser = new FilterParser(this);
        Object result = parser.parse(filterString);

        if (!(result instanceof Filter)) {
            throw new Exception("String " + filterString + " resolved to no filter");
        }

        return (Filter) result;
    }

    /**
     * Returns the startKey for scanning the HBase table.
     * If the user specified a {@code > / >=} operation
     * on a textual row key column, this value will be returned.
     * Otherwise, the start of table.
     *
     * @return start key for scanning HBase table
     */
    public byte[] startKey() {
        return startKey;
    }

    /**
     * Returns the endKey for scanning the HBase table.
     * If the user specified a {@code < / <=} operation
     * on a textual row key column, this value will be returned.
     * Otherwise, the end of table.
     *
     * @return end key for scanning HBase table
     */
    public byte[] endKey() {
        return endKey;
    }

    /**
     * Builds a filter from the input operands and operation.
     * Two kinds of operations are handled:
     * <ol>
     * <li>Simple operation between {@code FilterParser.Constant} and {@code FilterParser.ColumnIndex}.
     *    Supported operations are {@code <, >, <=, <=, >=, =, !=}. </li>
     * <li>Compound operations between {@link Filter} objects.
     *    The only supported operation is {@code AND}. </li>
     * </ol>
     * <p>
     * This function is called by {@link FilterParser},
     * each time the parser comes across an operator.
     */
    @Override
    public Object build(FilterParser.Operation opId,
                        Object leftOperand,
                        Object rightOperand) throws Exception {
        if (leftOperand instanceof Filter) {
            if (opId != FilterParser.Operation.HDOP_AND ||
                    !(rightOperand instanceof Filter)) {
                throw new Exception("Only AND is allowed between compound expressions");
            }

            return handleCompoundOperations((Filter) leftOperand, (Filter) rightOperand);
        }

        if (!(rightOperand instanceof FilterParser.Constant)) {
            throw new Exception("expressions of column-op-column are not supported");
        }

        // Assume column is on the left
        return handleSimpleOperations(opId,
                (FilterParser.ColumnIndex) leftOperand,
                (FilterParser.Constant) rightOperand);
    }

    /**
     * Initializes the {@link #operatorsMap} with appropriate values.
     */
    private void initOperatorsMap() {
        operatorsMap = new HashMap<FilterParser.Operation, CompareFilter.CompareOp>();
        operatorsMap.put(FilterParser.Operation.HDOP_LT, CompareFilter.CompareOp.LESS); // "<"
        operatorsMap.put(FilterParser.Operation.HDOP_GT, CompareFilter.CompareOp.GREATER); // ">"
        operatorsMap.put(FilterParser.Operation.HDOP_LE, CompareFilter.CompareOp.LESS_OR_EQUAL); // "<="
        operatorsMap.put(FilterParser.Operation.HDOP_GE, CompareFilter.CompareOp.GREATER_OR_EQUAL); // ">="
        operatorsMap.put(FilterParser.Operation.HDOP_EQ, CompareFilter.CompareOp.EQUAL); // "="
        operatorsMap.put(FilterParser.Operation.HDOP_NE, CompareFilter.CompareOp.NOT_EQUAL); // "!="
    }

    /**
     * Handles simple column-operator-constant expressions.
     * Creates a special filter in the case the column is the row key column.
     */
    private Filter handleSimpleOperations(FilterParser.Operation opId,
                                          FilterParser.ColumnIndex column,
                                          FilterParser.Constant constant) throws Exception {
        HBaseColumnDescriptor hbaseColumn = tupleDescription.getColumn(column.index());
        ByteArrayComparable comparator = getComparator(hbaseColumn.columnTypeCode(),
                constant.constant());

        /**
         * If row key is of type TEXT, allow filter in start/stop row key API in
         * HBaseAccessor/Scan object.
         */
        if (textualRowKey(hbaseColumn)) {
            storeStartEndKeys(opId, constant.constant());
        }

        if (hbaseColumn.isKeyColumn()) {
            return new RowFilter(operatorsMap.get(opId), comparator);
        }

        return new SingleColumnValueFilter(hbaseColumn.columnFamilyBytes(),
                hbaseColumn.qualifierBytes(),
                operatorsMap.get(opId),
                comparator);
    }

    /**
     * Resolves the column's type to a comparator class to be used.
     * Currently, supported types are TEXT and INTEGER types.
     */
    private ByteArrayComparable getComparator(int type, Object data) throws Exception {
        ByteArrayComparable result;
        switch (DataType.get(type)) {
            case TEXT:
                result = new BinaryComparator(Bytes.toBytes((String) data));
                break;
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                result = new HBaseIntegerComparator((Long) data);
                break;
            default:
                throw new Exception("unsupported column type for filtering " + type);
        }

        return result;
    }

    /**
     * Handles operation between already calculated expressions.
     * Currently only {@code AND}, in the future {@code OR} can be added.
     * <p>
     * Four cases here:
     * <ol>
     * <li>Both are simple filters.</li>
     * <li>Left is a FilterList and right is a filter.</li>
     * <li>Left is a filter and right is a FilterList.</li>
     * <li>Both are FilterLists.</li>
     * </ol>
     * <p>
     * Currently, 1, 2 can occur, since no parenthesis are used.
     */
    private Filter handleCompoundOperations(Filter left, Filter right) {
        FilterList result;

        if (left instanceof FilterList) {
            result = (FilterList) left;
            result.addFilter(right);

            return result;
        }

        result = new FilterList(FilterList.Operator.MUST_PASS_ALL, new Filter[] {left, right});

        return result;
    }

    /**
     * Returns true if column is of type TEXT and is a row key column.
     */
    private boolean textualRowKey(HBaseColumnDescriptor column) {
        return column.isKeyColumn() &&
                column.columnTypeCode() == TEXT.getOID();
    }

    /**
     * Sets startKey/endKey and their inclusiveness
     * according to the operation op.
     * <p>
     * TODO allow only one assignment to start/end key.
     * Currently, multiple calls to this function might change
     * previous assignments.
     */
    private void storeStartEndKeys(FilterParser.Operation op, Object data) {
        String key = (String) data;

        // Adding a zero byte to endkey, makes it inclusive
        // Adding a zero byte to startkey, makes it exclusive
        byte[] zeroByte = new byte[1];
        zeroByte[0] = 0;

        switch (op) {
            case HDOP_LT:
                endKey = Bytes.toBytes(key);
                break;
            case HDOP_GT:
                startKey = Bytes.add(Bytes.toBytes(key), zeroByte);
                break;
            case HDOP_LE:
                endKey = Bytes.add(Bytes.toBytes(key), zeroByte);
                break;
            case HDOP_GE:
                startKey = Bytes.toBytes(key);
                break;
            case HDOP_EQ:
                startKey = Bytes.toBytes(key);
                endKey = Bytes.add(Bytes.toBytes(key), zeroByte);
                break;
        }
    }
}
