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


import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * The parser code which goes over a filter string and pushes operands onto a stack.
 * Once an operation is read, the evaluate function is called for the {@link FilterBuilder}
 * interface with two pop-ed operands.
 * <br>
 * A string of filters looks like this:
 * <code>a2c5o1a1c"abc"o2o7</code>
 * which means {@code column#2 < 5 AND column#1 > "abc"}
 * <br>
 * It is a RPN serialized representation of a filters tree in GPDB where
 * <ul>
 * <li> a means an attribute (column)</li>
 * <li>c means a constant (either string or numeric)</li>
 * <li>o means operator</li>
 * </ul>
 *
 * Assuming all operators are binary, RPN representation allows it to be read left to right easily.
 * <br>
 * FilterParser only knows about columns and constants. The rest is up to the {@link FilterBuilder} implementer.
 * FilterParser makes sure column objects are always on the left of the expression (when relevant).
 */
public class FilterParser {
    private int index;
    private String filterString;
    private Stack<Object> operandsStack;
    private FilterBuilder filterBuilder;

    private static Map<Integer, Operation> operatorTranslationMap = initOperatorTransMap();

    /** Supported operations by the parser. */
    public enum Operation {
        HDOP_LT,
        HDOP_GT,
        HDOP_LE,
        HDOP_GE,
        HDOP_EQ,
        HDOP_NE,
        HDOP_AND
    }

    /**
     * Interface a user of FilterParser should implement.
     * This is used to let the user build filter expressions in the manner she sees fit.
     * When an operator is parsed, this function is called to let the user decide what to do with its operands.
     */
    public interface FilterBuilder {
        /**
         * Builds the filter.
         *
         * @param operation the parse operation to perform
         * @param left the left operand
         * @param right the right operand
         * @return the built filter
         * @throws Exception if building the filter failed
         */
        public Object build(Operation operation, Object left, Object right) throws Exception;
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
     * Basic filter provided for cases where the target storage system does not provide it own filter
     * For example: Hbase storage provides its own filter but for a Writable based record in a
     * SequenceFile there is no filter provided and so we need to have a default
     */
    static public class BasicFilter {
        private Operation oper;
        private ColumnIndex column;
        private Constant constant;

        /**
         * Constructs a BasicFilter.
         *
         * @param oper the parse operation to perform
         * @param column the column index
         * @param constant the constant object
         */
        public BasicFilter(Operation oper, ColumnIndex column, Constant constant) {
            this.oper = oper;
            this.column = column;
            this.constant = constant;
        }

        public Operation getOperation() {
            return oper;
        }

        public ColumnIndex getColumn() {
            return column;
        }

        public Constant getConstant() {
            return constant;
        }
    }

    /**
     * Thrown when a filter's parsing exception occurs.
     */
    @SuppressWarnings("serial")
    class FilterStringSyntaxException extends Exception {
        FilterStringSyntaxException(String desc) {
            super(desc + " (filter string: '" + filterString + "')");
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
    public Object parse(String filter) throws Exception {
        index = 0;
        filterString = filter;

        if (filter == null) {
            throw new FilterStringSyntaxException("filter parsing ended with no result");
        }

        while (index < filterString.length()) {
            char op = filterString.charAt(index);
            ++index; // skip op character
            switch (op) {
                case 'a':
                    operandsStack.push(new ColumnIndex(safeToInt(parseNumber())));
                    break;
                case 'c':
                    operandsStack.push(new Constant(parseParameter()));
                    break;
                case 'o':
                    // Parse and translate opcode
                    Operation operation = operatorTranslationMap.get(safeToInt(parseNumber()));
                    if (operation == null) {
                        throw new FilterStringSyntaxException("unknown op ending at " + index);
                    }

                    // Pop right operand
                    if (operandsStack.empty()) {
                        throw new FilterStringSyntaxException("missing operands for op " + operation + " at " + index);
                    }
                    Object rightOperand = operandsStack.pop();

                    // Pop left operand
                    if (operandsStack.empty()) {
                        throw new FilterStringSyntaxException("missing operands for op " + operation + " at " + index);
                    }
                    Object leftOperand = operandsStack.pop();

                    // Normalize order, evaluate
                    // Column should be on the left
                    Object result = (leftOperand instanceof Constant)
                            // column on the right, reverse expression
                            ? filterBuilder.build(reverseOp(operation), rightOperand, leftOperand)
                            // no swap, column on the left
                            : filterBuilder.build(operation, leftOperand, rightOperand);

                    // Store result on stack
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

    /**
     * Parses either a number or a string.
     */
    private Object parseParameter() throws Exception {
        if (index == filterString.length()) {
            throw new FilterStringSyntaxException("argument should follow at " + index);
        }

        return senseString()
                ? parseString()
                : parseNumber();
    }

    private boolean senseString() {
        return filterString.charAt(index) == '"';
    }

    private Long parseNumber() throws Exception {
        if (index == filterString.length()) {
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
        int filterLength = filterString.length();

        // allow sign
        if (filterLength > 0) {
            int chr = filterString.charAt(i);
            if (chr == '-' || chr == '+') {
                ++i;
            }
        }
        for (; i < filterLength; ++i) {
            int chr = filterString.charAt(i);
            if (chr < '0' || chr > '9') {
                break;
            }
        }

        if (i == index) {
            throw new FilterStringSyntaxException("numeric argument expected at " + index);
        }

        result = filterString.substring(index, i);
        index = i;
        return result;
    }

    /*
     * Parses a string after its beginning '"' until its ending '"'
     * advances the index accordingly
     *
     * Currently the string cannot contain '"' itself
     * TODO add support for '"' inside the string
     */
    private String parseString() throws Exception {
        StringBuilder result = new StringBuilder();
        boolean ended = false;
        int i;

        // starting from index + 1 to skip leading "
        for (i = index + 1; i < filterString.length(); ++i) {
            char chr = filterString.charAt(i);
            if (chr == '"') {
                ended = true;
                break;
            }
            result.append(chr);
        }

        if (!ended) {
            throw new FilterStringSyntaxException("string started at " + index + " not ended with \"");
        }

        index = i + 1; // +1 to skip ending "
        return result.toString();
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

    /**
     * Create a translation table of opcodes to their enum meaning.
     *
     * These codes correspond to the codes in GPDB C code
     * see gphdfilters.h in pxf protocol.
     */
    static private Map<Integer, Operation> initOperatorTransMap() {
        Map<Integer, Operation> operatorTranslationMap = new HashMap<Integer, Operation>();
        operatorTranslationMap.put(1, Operation.HDOP_LT);
        operatorTranslationMap.put(2, Operation.HDOP_GT);
        operatorTranslationMap.put(3, Operation.HDOP_LE);
        operatorTranslationMap.put(4, Operation.HDOP_GE);
        operatorTranslationMap.put(5, Operation.HDOP_EQ);
        operatorTranslationMap.put(6, Operation.HDOP_NE);
        operatorTranslationMap.put(7, Operation.HDOP_AND);
        return operatorTranslationMap;
    }
}
