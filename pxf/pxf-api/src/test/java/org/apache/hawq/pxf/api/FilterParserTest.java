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


import org.apache.hawq.pxf.api.FilterParser.FilterBuilder;
import org.apache.hawq.pxf.api.FilterParser.Operation;
import org.apache.hawq.pxf.api.FilterParser.LogicalOperation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FilterBuilder.class})
public class FilterParserTest {

    FilterBuilder filterBuilder;
    FilterParser filterParser;
    String filter, exception;

    @Before
    public void setUp() throws Exception {
        filterBuilder = mock(FilterBuilder.class);
        filterParser = new FilterParser(filterBuilder);
    }

    @Test
    public void parseNegativeEmpty() {
        filter = "";
        runParseNegative("empty string", filter, "filter parsing ended with no result");
    }

    @Test
    public void parseNegativeNotOperand() {
        filter = "g is not an operand";
        int index = 0;
        char op = filter.charAt(index);

        runParseNegative("illegal operand g", filter,
                "unknown opcode " + op + "(" + (int) op + ") at " + index);
    }

    @Test
    public void parseNegativeBadNumber() {

        filter = "a";
        int index = 1;
        exception = "numeric argument expected at " + index;

        runParseNegative("numeric operand with no number", filter, exception);

        filter = "aa";
        exception = "numeric argument expected at " + index;

        runParseNegative("numeric operand with non-number value", filter, exception);

        filter = "a12345678901234567890123456789";
        exception = "invalid numeric argument 12345678901234567890123456789";

        runParseNegative("numeric operand with too big number", filter, exception);

        filter = "a-12345678901234567890";
        exception = "invalid numeric argument -12345678901234567890";

        runParseNegative("numeric operand with too big negative number", filter, exception);

        filter = "a12345678901223456";
        exception = "value 12345678901223456 larger than intmax ending at " + filter.length();

        runParseNegative("numeric operand with long value", filter, exception);

        filter = "a-12345678901223456";
        exception = "value -12345678901223456 larger than intmax ending at " + filter.length();

        runParseNegative("numeric operand with negative long value", filter, exception);
    }

    @Test
    public void parseNegativeBadConst() {
        filter = "cs";
        int index = 1;
        exception = "datatype OID should follow at " + index;
        runParseNegative("const operand with no datatype", filter, exception);

        filter = "c5";
        exception = "invalid DataType OID at " + index;
        runParseNegative("const operand with invalid datatype oid", filter, exception);

        filter = "c20x";
        index = 3;
        exception = "data length delimiter 's' expected at " + index;
        runParseNegative("const operand with missing 's' delimiter", filter, exception);

        filter = "c20sd";
        index = 4;
        exception = "numeric argument expected at "  + index;
        runParseNegative("const operand with missing numeric data length", filter, exception);

        filter = "c20s1500";
        index = 8;
        exception = "data size larger than filter string starting at " + index;
        runParseNegative("const operand with missing 'd' delimiter", filter, exception);

        filter = "c20s1x";
        index = 5;
        exception = "data delimiter 'd' expected at " + index;
        runParseNegative("const operand with missing 'd' delimiter", filter, exception);

        filter = "c20s5d";
        index = 5;
        exception = "data size larger than filter string starting at " + index;
        runParseNegative("const operand with missing data", filter, exception);

        filter = "c20s3ds9r";
        index = 6;
        exception = "failed to parse number data type starting at " + index;
        runParseNegative("const operand with an invalid value", filter, exception);
    }

    @Test
    public void parseNegativeBadOperation() {
        filter = "o";
        int index = 1;
        exception = "numeric argument expected at " + index;
        runParseNegative("operation with no value", filter, exception);

        filter = "ohno";
        exception = "numeric argument expected at " + index;
        runParseNegative("operation with no number", filter, exception);

        filter = "o100";
        index = 4;
        exception = "unknown op ending at " + index;
        runParseNegative("operation with out of bounds number", filter, exception);
    }

    @Test
    public void parseNegativeNoOperator() {

        filter = "a1234567890";
        runParseNegative("filter with only column", filter, "filter parsing failed, missing operators?");

        filter = "c20s1d1";
        runParseNegative("filter with only numeric const", filter, "filter parsing failed, missing operators?");
    }

    @Test
    public void parseEmptyString() {
        filter = "c25s0d";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with empty string", filter, exception);
    }

    @Test
    public void parseDecimalValues() {
        filter = "c700s3d9.0";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with decimal value", filter, exception);

        filter = "c701s7d10.0001";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with decimal value", filter, exception);
    }

    @Test
    public void parseNegativeValues() {
        filter = "c700s3d-90";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with decimal value", filter, exception);

        filter = "c701s8d-10.0001";
        exception = "filter parsing failed, missing operators?";
        runParseNegative("const operand with decimal value", filter, exception);
    }

    @Test
    public void parseNegativeTwoParams() {

        filter = "c20s1d1c20s1d1";
        exception = "Stack not empty, missing operators?";
        runParseNegative("filter with two consts in a row", filter, exception);

        filter = "c20s1d1a1";
        exception = "Stack not empty, missing operators?";
        runParseNegative("filter with const and attribute", filter, exception);

        filter = "a1c700s1d1";
        exception = "Stack not empty, missing operators?";
        runParseNegative("filter with attribute and const", filter, exception);
    }

    @Test
    public void parseNegativeOperationFirst() {

        filter = "o1a3";
        int index = 2;
        FilterParser.Operation operation = FilterParser.Operation.HDOP_LT;
        exception = "missing operands for op " + operation + " at " + index;
        runParseNegative("filter with operation first", filter, exception);

        filter = "a2o1";
        index = 4;
        exception = "missing operands for op " + operation + " at " + index;
        runParseNegative("filter with only attribute before operation", filter, exception);
    }

    @Test
    public void parseColumnOnLeft() throws Exception {

        filter = "a1c20s1d1o1";
        Operation op = Operation.HDOP_LT;

        runParseOneOperation("this filter was build from HDOP_LT", filter, op);

        filter = "a1c20s1d1o2";
        op = Operation.HDOP_GT;
        runParseOneOperation("this filter was build from HDOP_GT", filter, op);

        filter = "a1c20s1d1o3";
        op = Operation.HDOP_LE;
        runParseOneOperation("this filter was build from HDOP_LE", filter, op);

        filter = "a1c20s1d1o4";
        op = Operation.HDOP_GE;
        runParseOneOperation("this filter was build from HDOP_GE", filter, op);

        filter = "a1c20s1d1o5";
        op = Operation.HDOP_EQ;
        runParseOneOperation("this filter was build from HDOP_EQ", filter, op);

        filter = "a1c20s1d1o6";
        op = Operation.HDOP_NE;
        runParseOneOperation("this filter was build from HDOP_NE", filter, op);

        filter = "a1c20s1d1o7";
        op = Operation.HDOP_LIKE;
        runParseOneOperation("this filter was built from HDOP_LIKE", filter, op);

        filter = "a1o8";
        op = Operation.HDOP_IS_NULL;
        runParseOneUnaryOperation("this filter was build from HDOP_IS_NULL", filter, op);

        filter = "a1o9";
        op = Operation.HDOP_IS_NOT_NULL;
        runParseOneUnaryOperation("this filter was build from HDOP_IS_NULL", filter, op);

    }

    @Test
    public void parseColumnOnRight() throws Exception {

        filter = "c20s1d1a1o1";
        Operation op = Operation.HDOP_GT;
        runParseOneOperation("this filter was build from HDOP_LT -> HDOP_GT using reverse!", filter, op);

        filter = "c20s1d1a1o2";
        op = Operation.HDOP_LT;
        runParseOneOperation("this filter was build from HDOP_GT -> HDOP_LT using reverse!", filter, op);

        filter = "c20s1d1a1o3";
        op = Operation.HDOP_GE;
        runParseOneOperation("this filter was build from HDOP_LE -> HDOP_GE using reverse!", filter, op);

        filter = "c20s1d1a1o4";
        op = Operation.HDOP_LE;
        runParseOneOperation("this filter was build from HDOP_GE -> HDOP_LE using reverse!", filter, op);

        filter = "c20s1d1a1o5";
        op = Operation.HDOP_EQ;
        runParseOneOperation("this filter was build from HDOP_EQ using reverse!", filter, op);

        filter = "c20s1d1a1o6";
        op = Operation.HDOP_NE;
        runParseOneOperation("this filter was build from HDOP_NE using reverse!", filter, op);

        filter = "c20s1d1a1o7";
        op = Operation.HDOP_LIKE;
        runParseOneOperation("this filter was build from HDOP_LIKE using reverse!", filter, op);
    }

    @Test
    public void parseFilterWith2Operations() throws Exception {
        filter = "a1c25s5dfirsto5a2c20s1d1o2l0";

        Object firstOp = "first operation HDOP_EQ";
        Object secondOp = "second operation HDOP_GT";
        Object lastOp = "filter with 2 operations connected by AND";

        when(filterBuilder.build(eq(Operation.HDOP_EQ),
                any(),
                any())).thenReturn(firstOp);

        when(filterBuilder.build(eq(Operation.HDOP_GT),
                any(),
                any())).thenReturn(secondOp);

        when(filterBuilder.build(eq(LogicalOperation.HDOP_AND),
                eq(firstOp),
                eq(secondOp))).thenReturn(lastOp);

        Object result = filterParser.parse(filter.getBytes());

        assertEquals(lastOp, result);
    }

    @Test
    public void parseLogicalAndOperator() throws Exception {
        filter = "a1c20s1d0o5a2c20s1d3o2l0";

        Object firstOp = "first operation HDOP_EQ";
        Object secondOp = "second operation HDOP_GT";
        Object lastOp = "filter with 2 operations connected by AND";

        when(filterBuilder.build(eq(Operation.HDOP_EQ),
                any(),
                any())).thenReturn(firstOp);

        when(filterBuilder.build(eq(Operation.HDOP_GT),
                any(),
                any())).thenReturn(secondOp);

        when(filterBuilder.build(eq(LogicalOperation.HDOP_AND),
                any(),
                any())).thenReturn(lastOp);

        Object result = filterParser.parse(filter.getBytes());

        assertEquals(lastOp, result);
    }

    @Test
    public void parseLogicalOrOperator() throws Exception {
        filter = "a1c20s1d0o5a2c20s1d3o2l1";

        Object firstOp = "first operation HDOP_EQ";
        Object secondOp = "second operation HDOP_GT";
        Object lastOp = "filter with 1 OR operator";

        when(filterBuilder.build(eq(Operation.HDOP_EQ),
                any(),
                any())).thenReturn(firstOp);

        when(filterBuilder.build(eq(Operation.HDOP_GT),
                any(),
                any())).thenReturn(secondOp);

        when(filterBuilder.build(eq(LogicalOperation.HDOP_OR),
                any(),
                any())).thenReturn(lastOp);

        Object result = filterParser.parse(filter.getBytes());
        assertEquals(lastOp, result);
    }

    @Test
    public void parseLogicalNotOperator() throws Exception {
        filter = "a1c20s1d0o5l2";

        Object firstOp = "first operation HDOP_EQ";
        Object op = "filter with NOT operator";

        when(filterBuilder.build(eq(Operation.HDOP_EQ),
                any(),
                any())).thenReturn(firstOp);

        when(filterBuilder.build(eq(LogicalOperation.HDOP_NOT),
                any())).thenReturn(op);

        Object result = filterParser.parse(filter.getBytes());
        assertEquals(op, result);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    @Test
    public void parseLogicalUnknownCodeError() throws Exception {
        thrown.expect(FilterParser.FilterStringSyntaxException.class);
        thrown.expectMessage("unknown op ending at 2");

        filter = "l7";
        when(filterBuilder.build(eq(LogicalOperation.HDOP_AND),
                any(),
                any())).thenReturn(null);

        Object result = filterParser.parse(filter.getBytes());
    }

    @Test
    public void parseLogicalOperatorNotExpression() throws Exception {
        filter = "a1c25s5dfirsto5a2c20s1d2o2l0l2";
        Object firstOp = "first operation HDOP_EQ";
        Object secondOp = "second operation HDOP_GT";
        Object thirdOp = "filter with 2 operations connected by AND";
        Object lastOp = "filter with 1 NOT operation";

        when(filterBuilder.build(eq(Operation.HDOP_EQ),
                any(),
                any())).thenReturn(firstOp);


        when(filterBuilder.build(eq(Operation.HDOP_GT),
                any(),
                any())).thenReturn(secondOp);

        when(filterBuilder.build(eq(LogicalOperation.HDOP_AND),
                any(),
                any())).thenReturn(thirdOp);

        when(filterBuilder.build(eq(LogicalOperation.HDOP_NOT),
                any())).thenReturn(lastOp);

        Object result = filterParser.parse(filter.getBytes());
        assertEquals(lastOp, result);
    }

	/*
     * Helper functions
	 */
    private void runParseNegative(String description, String filter, String exception) {
        try {
            filterParser.parse(filter.getBytes());
            fail(description + ": should have failed with FilterStringSyntaxException");
        } catch (FilterParser.FilterStringSyntaxException e) {
            assertEquals(description, exception + filterStringMsg(filter), e.getMessage());
        } catch (Exception e) {
            fail(description + ": should have failed with FilterStringSyntaxException and not " + e.getMessage());
        }
    }

    private void runParseOneOperation(String description, String filter, Operation op) throws Exception {
        when(filterBuilder.build(eq(op),
                any(),
                any())).thenReturn(description);

        Object result = filterParser.parse(filter.getBytes());

        assertEquals(description, result);
    }

    private void runParseOneUnaryOperation(String description, String filter, Operation op) throws Exception {
        when(filterBuilder.build(eq(op), any())).thenReturn(description);

        Object result = filterParser.parse(filter.getBytes());

        assertEquals(description, result);
    }

    private String filterStringMsg(String filter) {
        return " (filter string: '" + filter + "')";
    }
}
