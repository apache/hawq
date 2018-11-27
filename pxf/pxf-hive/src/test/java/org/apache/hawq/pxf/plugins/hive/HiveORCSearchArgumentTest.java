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

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hawq.pxf.api.BasicFilter;
import org.apache.hawq.pxf.api.LogicalFilter;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HiveORCSearchArgumentTest {

    @Test
    public void buildLogicalOperationTree() throws Exception {

        /* Predicate pushdown configuration */
        String filterStr = "a2c23s1d1o2a3c23s1d3o3l0a4c23s1d5o1l1";
        HiveFilterBuilder eval = new HiveFilterBuilder(null);
        Object filter = eval.getFilterObject(filterStr);

        SearchArgument.Builder filterBuilder = SearchArgumentFactory.newBuilder();
        buildExpression(filterBuilder, Arrays.asList(filter));
        SearchArgument sarg = filterBuilder.build();
        Assert.assertEquals("and(or(lt(col1, 5), not(lteq(col1, 1))), or(lt(col1, 5), lteq(col1, 3)))", sarg.toFilterPredicate().toString());
    }

    @Test
    public void buildIn() throws Exception {
        String filterStr = "a0m1009s4drow1s4drow2o10a1m1009s3ds_6s3ds_7o10l0";
        HiveFilterBuilder eval = new HiveFilterBuilder(null);
        Object filter = eval.getFilterObject(filterStr);

        SearchArgument.Builder filterBuilder = SearchArgumentFactory.newBuilder();
        buildExpression(filterBuilder, Arrays.asList(filter));
        SearchArgument sarg = filterBuilder.build();
        Assert.assertEquals("and(or(eq(col1, Binary{\"row1\"}), eq(col1, Binary{\"row2\"})), or(eq(col1, Binary{\"s_6\"}), eq(col1, Binary{\"s_7\"})))", sarg.toFilterPredicate().toString());
    }

    private boolean buildExpression(SearchArgument.Builder builder, List<Object> filterList) {
        for (Object f : filterList) {
            if (f instanceof LogicalFilter) {
                switch(((LogicalFilter) f).getOperator()) {
                    case HDOP_OR:
                        builder.startOr();
                        break;
                    case HDOP_AND:
                        builder.startAnd();
                        break;
                    case HDOP_NOT:
                        builder.startNot();
                        break;
                }
                if (buildExpression(builder, ((LogicalFilter) f).getFilterList())) {
                    builder.end();
                } else {
                    return false;
                }
            } else {
                if (!buildArgument(builder, f)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean buildArgument(SearchArgument.Builder builder, Object filterObj) {
        /* The below functions will not be compatible and requires update  with Hive 2.0 APIs */
        BasicFilter filter = (BasicFilter) filterObj;
        int filterColumnIndex = filter.getColumn().index();
        Object filterValue = filter.getConstant().constant();
        Integer[] arr = {};
        ColumnDescriptor filterColumn = new ColumnDescriptor("col1", 1,1, "Integer", arr);
        String filterColumnName = filterColumn.columnName();

        switch(filter.getOperation()) {
            case HDOP_LT:
                builder.lessThan(filterColumnName, filterValue);
                break;
            case HDOP_GT:
                builder.startNot().lessThanEquals(filterColumnName, filterValue).end();
                break;
            case HDOP_LE:
                builder.lessThanEquals(filterColumnName, filterValue);
                break;
            case HDOP_GE:
                builder.startNot().lessThanEquals(filterColumnName, filterValue).end();
                break;
            case HDOP_EQ:
                builder.equals(filterColumnName, filterValue);
                break;
            case HDOP_NE:
                builder.startNot().equals(filterColumnName, filterValue).end();
                break;
            case HDOP_IN:
                if (filterValue instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> l = (List<Object>)filterValue;
                    builder.in(filterColumnName, l.toArray());
                } else {
                    throw new IllegalArgumentException("filterValue should be instace of List for HDOP_IN operation");
                }
                break;
            default:
                return false;
        }
        return true;
    }
}
