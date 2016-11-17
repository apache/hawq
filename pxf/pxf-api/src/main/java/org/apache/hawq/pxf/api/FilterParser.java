package org.apache.hawq.pxf.api;

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


import org.apache.hawq.pxf.api.io.DataType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Stack;

/**
 * The parser code which goes over a filter string and pushes operands onto a stack.
 * Once an operation is read, the evaluate function is called for the {@link FilterBuilder}
 * interface with two pop-ed operands.
 * <br>
 * The filter string is of the pattern:
 * [attcode][attnum][constcode][constval][constsizecode][constsize][constdata][constvalue][opercode][opernum]
 * <br>
 * A sample string of filters looks like this:
 * <code>a2c23s1d5o1a1c25s3dabco2o7</code>
 * which means {@code column#2 < 5 AND column#1 > "abc"}
 * <br>
 * It is a RPN serialized representation of a filters tree in GPDB where
 * <ul>
 * <li> a means an attribute (column)</li>
 * <li>c means a constant followed by the datatype oid</li>
 * <li>s means the length of the data in bytes</li>
 * <li>d denotes the start of the constant data</li>
 * <li>o means operator</li>
 * </ul>
 * <br>
 * For constants all three portions are required in order to parse the data type, the length of the data in bytes
 * and the data itself
 * <br>
 * The parsing operation parses each element of the filter (constants, columns, operations) and adds them to a stack.
 * When the parser sees an op code 'o' or 'l' it pops off two elements from the stack assigns them as children of the op
 * and pushses itself onto the stack. After parsing is complete there should only be one element in the stack, the root
 * node of the filter's tree representation which is returned from this method
 * <br>
 * FilterParser only knows about columns and constants. The rest is up to the {@link FilterBuilder} implementer.
 * FilterParser makes sure column objects are always on the left of the expression (when relevant).
 */
public class FilterParser {
    private int index;
    private byte[] filterByteArr;
    private Stack<Object> operandsStack;
    private FilterBuilder filterBuilder;
    public static final char COL_OP = 'a';
    public static final char CONST_OP = 'c';
    public static final char CONST_LEN = 's';
    public static final char CONST_DATA = 'd';
    public static final char COMP_OP = 'o';
    public static final char LOG_OP = 'l';
    public static final String DEFAULT_CHARSET = "UTF-8";

    /** Supported operations by the parser. */
    public enum Operation {
        NOOP,
        HDOP_LT,
        HDOP_GT,
        HDOP_LE,
        HDOP_GE,
        HDOP_EQ,
        HDOP_NE,
        HDOP_LIKE,
        HDOP_IS_NULL,
        HDOP_IS_NOT_NULL
    }

    /**
     * This enum was added to support filter pushdown with the logical operators OR and NOT
     * HAWQ-964
     */
    public enum LogicalOperation {
        HDOP_AND,
        HDOP_OR,
        HDOP_NOT
    }

    /**
     * Interface a user of FilterParser should implement.
     * This is used to let the user build filter expressions in the manner she sees fit.
     * When an operator is parsed, this function is called to let the user decide what to do with its operands.
     */
    public interface FilterBuilder {
        /**
         * Builds the filter for an operation with 2 operands
         *
         * @param operation the parsed operation to perform
         * @param left the left operand
         * @param right the right operand
         * @return the built filter
         * @throws Exception if building the filter failed
         */
        public Object build(Operation operation, Object left, Object right) throws Exception;

        /**
         * Builds the filter for an operation with one operand
         *
         * @param operation the parsed operation to perform
         * @param operand the single operand
         * @return the built filter
         * @throws Exception if building the filter failed
         */
        public Object build(Operation operation, Object operand) throws Exception;

        /**
         * Builds the filter for a logical operation and two operands
         *
         * @param operation the parsed logical operation to perform
         * @param left the left operand
         * @param right the right operand
         * @return the built filter
         * @throws Exception if building the filter failed
         */
        public Object build(LogicalOperation operation, Object left, Object right) throws Exception;

        /**
         * Builds the filter for a logical operation and one operand
         *
         * @param operation the parsed unary logical operation to perform
         * @param filter the single operand
         * @return the built filter
         * @throws Exception if building the filter failed
         */
        public Object build(LogicalOperation operation, Object filter) throws Exception;
    }

    /** Represents a column index. */
    public class ColumnIndex {
        private int index;

        public ColumnIndex(int idx) {
            index = idx;
        }

        public int index() {
            return index;
        }
    }

    /** Represents a constant object (String, Long, ...). */
    public class Constant {
        private Object constant;

        public Constant(Object obj) {
            constant = obj;
        }

        public Object constant() {
            return constant;
        }
    }

    /**
     * Thrown when a filter's parsing exception occurs.
     */
    @SuppressWarnings("serial")
    class FilterStringSyntaxException extends Exception {
        FilterStringSyntaxException(String desc) {
            super(desc + " (filter string: '" + new String(filterByteArr) + "')");
        }
    }

    /**
     * Constructs a FilterParser.
     *
     * @param eval the filter builder
     */
    public FilterParser(FilterBuilder eval) {
        operandsStack = new Stack<Object>();
        filterBuilder = eval;
    }

    /**
     * Parses the string filter.
     *
     * @param filter the filter to parse
     * @return the parsed filter
     * @throws Exception if the filter string had wrong syntax
     */
    public Object parse(byte[] filter) throws Exception {
        index = 0;
        filterByteArr = filter;
        int opNumber;

        if (filter == null) {
            throw new FilterStringSyntaxException("filter parsing ended with no result");
        }

        while (index < filterByteArr.length) {
            char op = (char) filterByteArr[index++];
            switch (op) {
                case COL_OP:
                    operandsStack.push(new ColumnIndex(safeToInt(parseNumber())));
                    break;
                case CONST_OP:
                    operandsStack.push(new Constant(parseParameter()));
                    break;
                case COMP_OP:
                    opNumber = safeToInt(parseNumber());
                    Operation operation = opNumber < Operation.values().length ? Operation.values()[opNumber] : null;
                    if (operation == null) {
                        throw new FilterStringSyntaxException("unknown op ending at " + index);
                    }

                    // Pop right operand
                    if (operandsStack.empty()) {
                        throw new FilterStringSyntaxException("missing operands for op " + operation + " at " + index);
                    }
                    Object rightOperand = operandsStack.pop();

                    // all operations other than null checks require 2 operands
                    Object result;
                    if (operation == Operation.HDOP_IS_NULL || operation == Operation.HDOP_IS_NOT_NULL) {
                        result = filterBuilder.build(operation, rightOperand);
                    } else {
                        // Pop left operand
                        if (operandsStack.empty()) {
                            throw new FilterStringSyntaxException("missing operands for op " + operation + " at " + index);
                        }
                        Object leftOperand = operandsStack.pop();

                        if (leftOperand instanceof BasicFilter || rightOperand instanceof BasicFilter) {
                            throw new FilterStringSyntaxException("missing logical operator before op " + operation + " at " + index);
                        }

                        // Normalize order, evaluate
                        // Column should be on the left
                        result = (leftOperand instanceof Constant)
                                // column on the right, reverse expression
                                ? filterBuilder.build(reverseOp(operation), rightOperand, leftOperand)
                                // no swap, column on the left
                                : filterBuilder.build(operation, leftOperand, rightOperand);
                    }
                    // Store result on stack
                    operandsStack.push(result);
                    break;
                // Handle parsing logical operator (HAWQ-964)
                case LOG_OP:
                    opNumber = safeToInt(parseNumber());
                    LogicalOperation logicalOperation = opNumber < LogicalOperation.values().length ? LogicalOperation.values()[opNumber] : null;

                    if (logicalOperation == null) {
                        throw new FilterStringSyntaxException("unknown op ending at " + index);
                    }

                    if (logicalOperation == LogicalOperation.HDOP_NOT) {
                        Object exp = operandsStack.pop();
                        result = filterBuilder.build(logicalOperation, exp);
                    } else if (logicalOperation == LogicalOperation.HDOP_AND || logicalOperation == LogicalOperation.HDOP_OR){
                        rightOperand  = operandsStack.pop();
                        Object leftOperand = operandsStack.pop();

                        result = filterBuilder.build(logicalOperation, leftOperand, rightOperand);
                    } else {
                        throw new FilterStringSyntaxException("unknown logical op code " + opNumber);
                    }
                    operandsStack.push(result);
                    break;
                default:
                    index--; // move index back to operand location
                    throw new FilterStringSyntaxException("unknown opcode " + op +
                            "(" + (int) op + ") at " + index);
            }
        }

        if (operandsStack.empty()) {
            throw new FilterStringSyntaxException("filter parsing ended with no result");
        }

        Object result = operandsStack.pop();

        if (!operandsStack.empty()) {
            throw new FilterStringSyntaxException("Stack not empty, missing operators?");
        }

        if ((result instanceof Constant) || (result instanceof ColumnIndex)) {
            throw new FilterStringSyntaxException("filter parsing failed, missing operators?");
        }

        return result;
    }

    /**
     * Safely converts a long value to an int.
     *
     * @param value the long value to convert
     * @return the converted int value
     * @throws FilterStringSyntaxException if the long value is not inside an int scope
     */
    int safeToInt(Long value) throws FilterStringSyntaxException {
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new FilterStringSyntaxException("value " + value + " larger than intmax ending at " + index);
        }

        return value.intValue();
    }

    private int parseConstDataType() throws Exception {
        if (!Character.isDigit((char) filterByteArr[index])) {
            throw new FilterStringSyntaxException("datatype OID should follow at " + index);
        }

        String digits = parseDigits();

        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            throw new FilterStringSyntaxException("invalid numeric argument at " + digits);
        }
    }

    private int parseDataLength() throws Exception {
        if (((char) filterByteArr[index]) != CONST_LEN) {
            throw new FilterStringSyntaxException("data length delimiter 's' expected at " +  index);
        }

        index++;
        return parseInt();
    }

    private int parseInt() throws Exception {
        if (index == filterByteArr.length) {
            throw new FilterStringSyntaxException("numeric argument expected at " + index);
        }

        String digits = parseDigits();

        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            throw new FilterStringSyntaxException("invalid numeric argument " + digits);
        }
    }

    private Object convertDataType(byte[] byteData, int start, int end, DataType dataType) throws Exception {
        String data = new String(byteData, start, end-start, DEFAULT_CHARSET);
        try {
            switch (dataType) {
                case BIGINT:
                    return Long.parseLong(data);
                case INTEGER:
                case SMALLINT:
                    return Integer.parseInt(data);
                case REAL:
                    return Float.parseFloat(data);
                case NUMERIC:
                case FLOAT8:
                    return Double.parseDouble(data);
                case TEXT:
                case VARCHAR:
                case BPCHAR:
                    return data;
                case BOOLEAN:
                    return Boolean.parseBoolean(data);
                case DATE:
                    return Date.valueOf(data);
                case TIMESTAMP:
                    return Timestamp.valueOf(data);
                case TIME:
                    return Time.valueOf(data);
                case BYTEA:
                    return data.getBytes();
                default:
                    throw new FilterStringSyntaxException("DataType " + dataType.toString() + " unsupported");
            }
        } catch (NumberFormatException nfe) {
            throw new FilterStringSyntaxException("failed to parse number data type starting at " + index);
        }
    }
    /**
     * Parses either a number or a string.
     */
    private Object parseParameter() throws Exception {
        if (index == filterByteArr.length) {
            throw new FilterStringSyntaxException("argument should follow at " + index);
        }

        DataType dataType = DataType.get(parseConstDataType());
        if (dataType == DataType.UNSUPPORTED_TYPE) {
            throw new FilterStringSyntaxException("invalid DataType OID at " + (index - 1));
        }

        int dataLength = parseDataLength();

        if (index + dataLength > filterByteArr.length) {
            throw new FilterStringSyntaxException("data size larger than filter string starting at " + index);
        }

        if (((char) filterByteArr[index]) != CONST_DATA) {
            throw new FilterStringSyntaxException("data delimiter 'd' expected at " + index);
        }

        index++;

        Object data = convertDataType(filterByteArr, index, index+dataLength, dataType);
        index += dataLength;
        return data;
    }

    private Long parseNumber() throws Exception {
        if (index == filterByteArr.length) {
            throw new FilterStringSyntaxException("numeric argument expected at " + index);
        }

        String digits = parseDigits();

        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            throw new FilterStringSyntaxException("invalid numeric argument " + digits);
        }

    }

    /*
     * Parses the longest sequence of digits into a number
     * advances the index accordingly
     */
    private String parseDigits() throws Exception {
        String result;
        int i = index;
        int filterLength = filterByteArr.length;

        // allow sign
        if (filterLength > 0) {
            char chr = (char) filterByteArr[i];
            if (chr == '-' || chr == '+') {
                ++i;
            }
        }
        for (; i < filterLength; ++i) {
            char chr = (char) filterByteArr[i];
            if (chr < '0' || chr > '9') {
                break;
            }
        }

        if (i == index) {
            throw new FilterStringSyntaxException("numeric argument expected at " + index);
        }

        result = new String(filterByteArr, index, i - index);
        index = i;
        return result;
    }

    /*
     * The function takes an operator and reverses it
     * e.g. > turns into <
     */
    private Operation reverseOp(Operation operation) {
        switch (operation) {
            case HDOP_LT:
                operation = Operation.HDOP_GT;
                break;
            case HDOP_GT:
                operation = Operation.HDOP_LT;
                break;
            case HDOP_LE:
                operation = Operation.HDOP_GE;
                break;
            case HDOP_GE:
                operation = Operation.HDOP_LE;
                break;
            default:
                // no change o/w
        }

        return operation;
    }
}