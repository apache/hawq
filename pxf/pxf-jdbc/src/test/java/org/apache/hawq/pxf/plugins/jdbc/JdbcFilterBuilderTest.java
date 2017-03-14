package org.apache.hawq.pxf.plugins.jdbc;

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


import org.apache.hawq.pxf.api.BasicFilter;
import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.FilterParser.LogicalOperation;
import org.apache.hawq.pxf.api.LogicalFilter;
import org.junit.Test;

import static org.apache.hawq.pxf.api.FilterParser.Operation.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JdbcFilterBuilderTest {
    @Test
    public void parseFilterWithThreeOperations() throws Exception {
        //orgin sql => col_1>'2008-02-01' and col_1<'2008-12-01' or col_2 > 1200
        String filterstr = "a1c25s10d2008-02-01o2a1c25s10d2008-12-01o1l0a2c20s4d1200o2l1";
        JdbcFilterBuilder builder = new JdbcFilterBuilder();

        LogicalFilter filterList = (LogicalFilter) builder.getFilterObject(filterstr);
        assertEquals(LogicalOperation.HDOP_OR, filterList.getOperator());
        LogicalFilter l1_left = (LogicalFilter) filterList.getFilterList().get(0);
        BasicFilter l1_right = (BasicFilter) filterList.getFilterList().get(1);

        //column_2 > 1200
        assertEquals(2, l1_right.getColumn().index());
        assertEquals(HDOP_GT, l1_right.getOperation());
        assertEquals(1200L, l1_right.getConstant().constant());

        assertEquals(LogicalOperation.HDOP_AND, l1_left.getOperator());
        BasicFilter l2_left = (BasicFilter) l1_left.getFilterList().get(0);
        BasicFilter l2_right = (BasicFilter) l1_left.getFilterList().get(1);

        //column_1 > '2008-02-01'
        assertEquals(1, l2_left.getColumn().index());
        assertEquals(HDOP_GT, l2_left.getOperation());
        assertEquals("2008-02-01", l2_left.getConstant().constant());

        //column_2 < '2008-12-01'
        assertEquals(1, l2_right.getColumn().index());
        assertEquals(HDOP_LT, l2_right.getOperation());
        assertEquals("2008-12-01", l2_right.getConstant().constant());

    }

    @Test
    public void parseFilterWithLogicalOperation() throws Exception {
        WhereSQLBuilder builder = new WhereSQLBuilder(null);
        LogicalFilter filter = (LogicalFilter) builder.getFilterObject("a1c25s5dfirsto5a2c20s1d2o2l0");
        assertEquals(LogicalOperation.HDOP_AND, filter.getOperator());
        assertEquals(2, filter.getFilterList().size());
    }

    @Test
    public void parseNestedExpressionWithLogicalOperation() throws Exception {
        WhereSQLBuilder builder = new WhereSQLBuilder(null);
        LogicalFilter filter = (LogicalFilter) builder.getFilterObject("a1c25s5dfirsto5a2c20s1d2o2l0a1c20s1d1o1l1");
        assertEquals(LogicalOperation.HDOP_OR, filter.getOperator());
        assertEquals(LogicalOperation.HDOP_AND, ((LogicalFilter) filter.getFilterList().get(0)).getOperator());
        assertEquals(HDOP_LT, ((BasicFilter) filter.getFilterList().get(1)).getOperation());
    }

    @Test
    public void parseISNULLExpression() throws Exception {
        WhereSQLBuilder builder = new WhereSQLBuilder(null);
        BasicFilter filter = (BasicFilter) builder.getFilterObject("a1o8");
        assertEquals(FilterParser.Operation.HDOP_IS_NULL, filter.getOperation());
        assertEquals(1, filter.getColumn().index());
        assertNull(filter.getConstant());
    }

    @Test
    public void parseISNOTNULLExpression() throws Exception {
        WhereSQLBuilder builder = new WhereSQLBuilder(null);
        BasicFilter filter = (BasicFilter) builder.getFilterObject("a1o9");
        assertEquals(FilterParser.Operation.HDOP_IS_NOT_NULL, filter.getOperation());
        assertEquals(1, filter.getColumn().index());
        assertNull(filter.getConstant());
    }

}
