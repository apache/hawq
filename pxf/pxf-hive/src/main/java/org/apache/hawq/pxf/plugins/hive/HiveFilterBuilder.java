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


import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.util.LinkedList;
import java.util.List;

/**
 * Uses the filter parser code to build a filter object, either simple - a
 * single {@link org.apache.hawq.pxf.api.FilterParser.BasicFilter} object or a
 * compound - a {@link java.util.List} of
 * {@link org.apache.hawq.pxf.api.FilterParser.BasicFilter} objects.
 * {@link org.apache.hawq.pxf.plugins.hive.HiveAccessor} will use the filter for
 * partition filtering.
 */
public class HiveFilterBuilder implements FilterParser.FilterBuilder {
    private InputData inputData;

    /**
     * Constructs a HiveFilterBuilder object.
     *
     * @param input input data containing filter string
     */
    public HiveFilterBuilder(InputData input) {
        inputData = input;
    }

    /**
     * Translates a filterString into a {@link org.apache.hawq.pxf.api.FilterParser.BasicFilter} or a
     * list of such filters.
     *
     * @param filterString the string representation of the filter
     * @return a single {@link org.apache.hawq.pxf.api.FilterParser.BasicFilter}
     *         object or a {@link java.util.List} of
     *         {@link org.apache.hawq.pxf.api.FilterParser.BasicFilter} objects.
     * @throws Exception if parsing the filter failed or filter is not a basic
     *             filter or list of basic filters
     */
    public Object getFilterObject(String filterString) throws Exception {
        FilterParser parser = new FilterParser(this);
        Object result = parser.parse(filterString);

        if (!(result instanceof FilterParser.BasicFilter)
                && !(result instanceof List)) {
            throw new Exception("String " + filterString
                    + " resolved to no filter");
        }

        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object build(FilterParser.Operation opId, Object leftOperand,
                        Object rightOperand) throws Exception {
        if (leftOperand instanceof FilterParser.BasicFilter
                || leftOperand instanceof List) {
            if (opId != FilterParser.Operation.HDOP_AND
                    || !(rightOperand instanceof FilterParser.BasicFilter)) {
                throw new Exception(
                        "Only AND is allowed between compound expressions");
            }

            if (leftOperand instanceof List) {
                return handleCompoundOperations(
                        (List<FilterParser.BasicFilter>) leftOperand,
                        (FilterParser.BasicFilter) rightOperand);
            } else {
                return handleCompoundOperations(
                        (FilterParser.BasicFilter) leftOperand,
                        (FilterParser.BasicFilter) rightOperand);
            }
        }

        if (!(rightOperand instanceof FilterParser.Constant)) {
            throw new Exception(
                    "expressions of column-op-column are not supported");
        }

        // Assume column is on the left
        return handleSimpleOperations(opId,
                (FilterParser.ColumnIndex) leftOperand,
                (FilterParser.Constant) rightOperand);
    }

    /*
     * Handles simple column-operator-constant expressions Creates a special
     * filter in the case the column is the row key column
     */
    private FilterParser.BasicFilter handleSimpleOperations(FilterParser.Operation opId,
                                                            FilterParser.ColumnIndex column,
                                                            FilterParser.Constant constant) {
        return new FilterParser.BasicFilter(opId, column, constant);
    }

    /**
     * Handles AND of already calculated expressions. Currently only AND, in the
     * future OR can be added
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
    private List<FilterParser.BasicFilter> handleCompoundOperations(List<FilterParser.BasicFilter> left,
                                                                    FilterParser.BasicFilter right) {
        left.add(right);
        return left;
    }

    private List<FilterParser.BasicFilter> handleCompoundOperations(FilterParser.BasicFilter left,
                                                                    FilterParser.BasicFilter right) {
        List<FilterParser.BasicFilter> result = new LinkedList<FilterParser.BasicFilter>();

        result.add(left);
        result.add(right);

        return result;
    }
}
