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

/**
 * Basic filter provided for cases where the target storage system does not provide it own filter
 * For example: Hbase storage provides its own filter but for a Writable based record in a
 * SequenceFile there is no filter provided and so we need to have a default
 */
public class BasicFilter {
    private FilterParser.Operation oper;
    private FilterParser.ColumnIndex column;
    private FilterParser.Constant constant;

    /**
     * Constructs a BasicFilter.
     *
     * @param oper the parse operation to perform
     * @param column the column index
     * @param constant the constant object
     */
    public BasicFilter(FilterParser.Operation oper, FilterParser.ColumnIndex column, FilterParser.Constant constant) {
        this.oper = oper;
        this.column = column;
        this.constant = constant;
    }

    public FilterParser.Operation getOperation() {
        return oper;
    }

    public FilterParser.ColumnIndex getColumn() {
        return column;
    }

    public FilterParser.Constant getConstant() {
        return constant;
    }
}
