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
import org.junit.Before;
import org.junit.Test;
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
    public void parseNegativeNull() {
        filter = null;
        runParseNegative("null string", null, "filter parsing ended with no result");
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
        filter = "c";
        int index = 1;
        exception = "argument should follow at " + index;
        runParseNegative("const operand with no value", filter, exception);

        filter = "cyan";
        exception = "numeric argument expected at " + index;
        runParseNegative("const operand with illegal value", filter, exception);

        filter = "c\"and that's it";
        exception = "string started at " + index + " not ended with \"";
        runParseNegative("string without closing \"", filter, exception);
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

        filter = "c1";
        runParseNegative("filter with only numeric const", filter, "filter parsing failed, missing operators?");

        filter = "c\"something in the way\"";
        runParseNegative("filter with only string const", filter, "filter parsing failed, missing operators?");
    }

    @Test
    public void parseNegativeTwoParams() {

        filter = "c1c2";
        exception = "Stack not empty, missing operators?";
        runParseNegative("filter with two consts in a row", filter, exception);

        filter = "c1a1";
        exception = "Stack not empty, missing operators?";
        runParseNegative("filter with const and attribute", filter, exception);

        filter = "a1c80";
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

        filter = "a1c2o1";
        Operation op = Operation.HDOP_LT;

        runParseOneOperation("this filter was build from HDOP_LT", filter, op);

        filter = "a1c2o2";
        op = Operation.HDOP_GT;
        runParseOneOperation("this filter was build from HDOP_GT", filter, op);

        filter = "a1c2o3";
        op = Operation.HDOP_LE;
        runParseOneOperation("this filter was build from HDOP_LE", filter, op);

        filter = "a1c2o4";
        op = Operation.HDOP_GE;
        runParseOneOperation("this filter was build from HDOP_GE", filter, op);

        filter = "a1c2o5";
        op = Operation.HDOP_EQ;
        runParseOneOperation("this filter was build from HDOP_EQ", filter, op);

        filter = "a1c2o6";
        op = Operation.HDOP_NE;
        runParseOneOperation("this filter was build from HDOP_NE", filter, op);

        filter = "a1c2o7";
        op = Operation.HDOP_AND;
        runParseOneOperation("this filter was build from HDOP_AND", filter, op);
    }

    @Test
    public void parseColumnOnRight() throws Exception {

        filter = "c2a1o1";
        Operation op = Operation.HDOP_GT;
        runParseOneOperation("this filter was build from HDOP_LT -> HDOP_GT using reverse!", filter, op);

        filter = "c2a1o2";
        op = Operation.HDOP_LT;
        runParseOneOperation("this filter was build from HDOP_GT -> HDOP_LT using reverse!", filter, op);

        filter = "c2a1o3";
        op = Operation.HDOP_GE;
        runParseOneOperation("this filter was build from HDOP_LE -> HDOP_GE using reverse!", filter, op);

        filter = "c2a1o4";
        op = Operation.HDOP_LE;
        runParseOneOperation("this filter was build from HDOP_GE -> HDOP_LE using reverse!", filter, op);

        filter = "c2a1o5";
        op = Operation.HDOP_EQ;
        runParseOneOperation("this filter was build from HDOP_EQ using reverse!", filter, op);

        filter = "c2a1o6";
        op = Operation.HDOP_NE;
        runParseOneOperation("this filter was build from HDOP_NE using reverse!", filter, op);

        filter = "c2a1o7";
        op = Operation.HDOP_AND;
        runParseOneOperation("this filter was build from HDOP_AND using reverse!", filter, op);
    }

    @Test
    public void parseFilterWith2Operations() throws Exception {
        filter = "a1c\"first\"o5a2c2o2o7";

        Object firstOp = "first operation HDOP_EQ";
        Object secondOp = "second operation HDOP_GT";
        Object lastOp = "filter with 2 operations connected by AND";

        when(filterBuilder.build(eq(Operation.HDOP_EQ),
                any(),
                any())).thenReturn(firstOp);

        when(filterBuilder.build(eq(Operation.HDOP_GT),
                any(),
                any())).thenReturn(secondOp);

        when(filterBuilder.build(eq(Operation.HDOP_AND),
                eq(firstOp),
                eq(secondOp))).thenReturn(lastOp);

        Object result = filterParser.parse(filter);

        assertEquals(lastOp, result);
    }

	/*
     * Helper functions
	 */
    private void runParseNegative(String description, String filter, String exception) {
        try {
            filterParser.parse(filter);
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

        Object result = filterParser.parse(filter);

        assertEquals(description, result);
    }

    private String filterStringMsg(String filter) {
        return " (filter string: '" + filter + "')";
    }
}
