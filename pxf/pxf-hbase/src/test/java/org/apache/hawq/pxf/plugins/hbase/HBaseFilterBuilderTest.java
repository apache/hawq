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

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hawq.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import org.apache.hawq.pxf.plugins.hbase.utilities.HBaseTupleDescription;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HBaseFilterBuilderTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void parseNOTExpressionIgnored() throws Exception {
        String filter = "a1c2o1a1c2o2l0l2";
        HBaseFilterBuilder builder = new HBaseFilterBuilder(null);
        assertNull(builder.getFilterObject(filter));

        filter = "a1c2o1a1c2o2l2l0";
        builder = new HBaseFilterBuilder(null);
        assertNull(builder.getFilterObject(filter));
    }

    @Test
    public void parseNOTOpCodeInConstant() throws Exception {
        String filter = "a1c25s2dl2o1a1c20s1d2o2l0";
        HBaseFilterBuilder builder = new HBaseFilterBuilder(null);
        //Testing that we get past the parsing stage
        //Very crude but it avoids instantiating all the necessary dependencies
        thrown.expect(NullPointerException.class);
        builder.getFilterObject(filter);
    }

    @Test
    public void parseNullFilter() throws Exception {
        HBaseFilterBuilder builder = new HBaseFilterBuilder(null);
        assertNull(builder.getFilterObject(null));
    }

    @Test
    public void parseISNULLExpression() throws Exception {
        String filter = "a1o8";
        HBaseTupleDescription desc = mock(HBaseTupleDescription.class);
        HBaseColumnDescriptor column = mock(HBaseColumnDescriptor.class);
        when(desc.getColumn(1)).thenReturn(column);

        byte[] family = new byte[]{};
        byte[] qualifier = new byte[]{};

        when(column.columnFamilyBytes()).thenReturn(family);
        when(column.qualifierBytes()).thenReturn(qualifier);

        HBaseFilterBuilder builder = new HBaseFilterBuilder(desc);
        SingleColumnValueFilter result = (SingleColumnValueFilter) builder.getFilterObject(filter);

        assertNotNull(result);
        assertSame(family, result.getFamily());
        assertSame(qualifier, result.getQualifier());
        assertEquals(CompareFilter.CompareOp.EQUAL, result.getOperator());
        assertTrue(result.getComparator() instanceof NullComparator);

    }

    @Test
    public void parseISNOTNULLExpression() throws Exception {
        String filter = "a1o9";
        HBaseTupleDescription desc = mock(HBaseTupleDescription.class);
        HBaseColumnDescriptor column = mock(HBaseColumnDescriptor.class);
        when(desc.getColumn(1)).thenReturn(column);

        byte[] family = new byte[]{};
        byte[] qualifier = new byte[]{};

        when(column.columnFamilyBytes()).thenReturn(family);
        when(column.qualifierBytes()).thenReturn(qualifier);

        HBaseFilterBuilder builder = new HBaseFilterBuilder(desc);
        SingleColumnValueFilter result = (SingleColumnValueFilter) builder.getFilterObject(filter);

        assertNotNull(result);
        assertSame(family, result.getFamily());
        assertSame(qualifier, result.getQualifier());
        assertEquals(CompareFilter.CompareOp.NOT_EQUAL, result.getOperator());
        assertTrue(result.getComparator() instanceof NullComparator);

    }

}