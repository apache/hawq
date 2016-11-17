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


import org.apache.hawq.pxf.api.FilterParser.LogicalOperation;
import org.apache.hawq.pxf.api.LogicalFilter;
import org.junit.Test;

import org.apache.hawq.pxf.api.BasicFilter;

import static org.apache.hawq.pxf.api.FilterParser.Operation;
import static org.apache.hawq.pxf.api.FilterParser.Operation.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HiveFilterBuilderTest {
    @Test
    public void parseFilterWithThreeOperations() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder(null);
        String[] consts = new String[] {"first", "2"};
        Operation[] ops = new Operation[] {HDOP_EQ, HDOP_GT};
        int[] idx = new int[] {1, 2};

        LogicalFilter filterList = (LogicalFilter) builder.getFilterObject("a1c25s5dfirsto5a2c20s1d2o2l0");
        assertEquals(LogicalOperation.HDOP_AND, filterList.getOperator());
        BasicFilter leftOperand = (BasicFilter) filterList.getFilterList().get(0);
        assertEquals(consts[0], leftOperand.getConstant().constant());
        assertEquals(idx[0], leftOperand.getColumn().index());
        assertEquals(ops[0], leftOperand.getOperation());
    }

    @Test
    public void parseNullFilter() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder(null);
        LogicalFilter filterList = (LogicalFilter) builder.getFilterObject(null);
        assertNull(builder.getFilterObject(null));
    }

    @Test
    public void parseFilterWithLogicalOperation() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder(null);
        LogicalFilter filter = (LogicalFilter) builder.getFilterObject("a1c25s5dfirsto5a2c20s1d2o2l0");
        assertEquals(LogicalOperation.HDOP_AND, filter.getOperator());
        assertEquals(2, filter.getFilterList().size());
    }

    @Test
    public void parseNestedExpressionWithLogicalOperation() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder(null);
        LogicalFilter filter = (LogicalFilter) builder.getFilterObject("a1c25s5dfirsto5a2c20s1d2o2l0a1c20s1d1o1l1");
        assertEquals(LogicalOperation.HDOP_OR, filter.getOperator());
        assertEquals(LogicalOperation.HDOP_AND, ((LogicalFilter) filter.getFilterList().get(0)).getOperator());
        assertEquals(HDOP_LT, ((BasicFilter) filter.getFilterList().get(1)).getOperation());
    }

    @Test
    public void parseISNULLExpression() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder(null);
        BasicFilter filter = (BasicFilter) builder.getFilterObject("a1o8");
        assertEquals(Operation.HDOP_IS_NULL, filter.getOperation());
        assertEquals(1, filter.getColumn().index());
        assertNull(filter.getConstant());
    }

    @Test
    public void parseISNOTNULLExpression() throws Exception {
        HiveFilterBuilder builder = new HiveFilterBuilder(null);
        BasicFilter filter = (BasicFilter) builder.getFilterObject("a1o9");
        assertEquals(Operation.HDOP_IS_NOT_NULL, filter.getOperation());
        assertEquals(1, filter.getColumn().index());
        assertNull(filter.getConstant());
    }

}
