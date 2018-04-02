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
import org.apache.hawq.pxf.api.LogicalFilter;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.text.ParseException;

/**
 * A filter builder. Uses a single {@link BasicFilter} or a {@link List} of {@link BasicFilter} objects.
 *
 * The subclass {@link WhereSQLBuilder} will use the result to generate WHERE statement.
 */
public class JdbcFilterBuilder implements FilterParser.FilterBuilder {
    /**
     * Translates a filterString into a {@link BasicFilter} or a
     * list of such filters.
     *
     * @param filterString the string representation of the filter
     * @return a {@link BasicFilter} or a {@link List} of {@link BasicFilter}.
     * @throws ParseException if parsing the filter failed or filter is not a basic filter or list of basic filters
     */
    public Object getFilterObject(String filterString) throws ParseException {
        try {
            FilterParser parser = new FilterParser(this);
            Object result = parser.parse(filterString.getBytes(FilterParser.DEFAULT_CHARSET));

            if (
                !(result instanceof LogicalFilter) &&
                !(result instanceof BasicFilter) &&
                !(result instanceof List)
            ) {
                throw new Exception("'" + filterString + "' could not be resolved to a filter");
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage(), 0);
        }
    }

    @Override
    public Object build(FilterParser.LogicalOperation op, Object leftOperand, Object rightOperand) {
        return handleLogicalOperation(op, leftOperand, rightOperand);
    }

    @Override
    public Object build(FilterParser.LogicalOperation op, Object filter) {
        return handleLogicalOperation(op, filter);
    }

    @Override
    public Object build(FilterParser.Operation opId, Object leftOperand,
                        Object rightOperand) throws Exception {
        // Assume column is on the left
        return handleSimpleOperations(
            opId,
            (FilterParser.ColumnIndex) leftOperand,
            (FilterParser.Constant) rightOperand
        );
    }

    @Override
    public Object build(FilterParser.Operation operation, Object operand) throws UnsupportedOperationException {
        if (
            operation == FilterParser.Operation.HDOP_IS_NULL ||
            operation == FilterParser.Operation.HDOP_IS_NOT_NULL
        ) {
            // Use null for the constant value of null comparison
            return handleSimpleOperations(operation, (FilterParser.ColumnIndex) operand, null);
        }
        else {
            throw new UnsupportedOperationException("Unsupported unary operation '" + operation + "'");
        }
    }

    /*
     * Handles simple column-operator-constant expressions.
     * Creates a special filter in the case the column is the row key column
     */
    private BasicFilter handleSimpleOperations(FilterParser.Operation opId,
                                               FilterParser.ColumnIndex column,
                                               FilterParser.Constant constant) {
        return new BasicFilter(opId, column, constant);
    }

    /**
     * Handles AND of already calculated expressions.
     *
     * Four cases here:
     * <ol>
     * <li>both are simple filters</li>
     * <li>left is a FilterList and right is a filter</li>
     * <li>left is a filter and right is a FilterList</li>
     * <li>both are FilterLists</li>
     * </ol>
     * Currently, 1, 2 can occur, since no parenthesis are used
     *
     * @param left left hand filter
     * @param right right hand filter
     * @return list of filters constructing the filter tree
     */
    private List<BasicFilter> handleCompoundOperations(List<BasicFilter> left,
                                                       BasicFilter right) {
        left.add(right);
        return left;
    }

    private List<BasicFilter> handleCompoundOperations(BasicFilter left,
                                                       BasicFilter right) {
        List<BasicFilter> result = new LinkedList<>();

        result.add(left);
        result.add(right);

        return result;
    }

    private Object handleLogicalOperation(FilterParser.LogicalOperation operator, Object leftOperand, Object rightOperand) {
        List<Object> result = new LinkedList<>();

        result.add(leftOperand);
        result.add(rightOperand);
        return new LogicalFilter(operator, result);
    }

    private Object handleLogicalOperation(FilterParser.LogicalOperation operator, Object filter) {
        return new LogicalFilter(operator, Arrays.asList(filter));
    }
}
