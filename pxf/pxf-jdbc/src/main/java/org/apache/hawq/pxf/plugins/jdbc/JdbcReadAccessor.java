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

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;

/**
 * Accessor for Jdbc tables. The accessor will open and read a partition belonging
 * to a Jdbc table. JdbcReadAccessor generates and executes SQL from filter and
 * fragmented information, uses {@link JdbcReadResolver } to read the ResultSet, and generates
 * the data type - List {@link OneRow} that HAWQ needs.
 */
public class JdbcReadAccessor extends JdbcPlugin implements ReadAccessor {
    private static final Log LOG = LogFactory.getLog(JdbcReadAccessor.class);

    WhereSQLBuilder filterBuilder = null;
    private ColumnDescriptor keyColumn = null;

    private String querySql = null;
    private Statement statement = null;
    private ResultSet resultSet = null;

    public JdbcReadAccessor(InputData input) throws UserDataException {
        super(input);
        filterBuilder = new WhereSQLBuilder(inputData);

        //buid select statement (not contain where statement)
        ArrayList<ColumnDescriptor> columns = input.getTupleDescription();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor column = columns.get(i);
            if (column.isKeyColumn())
                keyColumn = column;
            if (i > 0) sb.append(",");
            sb.append(column.columnName());
        }
        sb.append(" FROM ").append(getTableName());
        querySql = sb.toString();
    }

    /**
     * open db connection, execute query sql
     */
    @Override
    public boolean openForRead() throws Exception {
        if (statement != null && !statement.isClosed())
            return true;
        super.openConnection();

        statement = dbConn.createStatement();

        resultSet = executeQuery(querySql);

        return true;
    }

    public ResultSet executeQuery(String sql) throws Exception {
        String query = sql;
        if (inputData.hasFilter()) {
            //parse filter string , build where statement
            String whereSql = filterBuilder.buildWhereSQL(dbProduct);

            if (whereSql != null) {
                query = query + " WHERE " + whereSql;
            }
        }

        //according to the fragment information, rewriting sql
        JdbcPartitionFragmenter fragmenter = new JdbcPartitionFragmenter(inputData);
        query = fragmenter.buildFragmenterSql(dbProduct, query);

        if (LOG.isDebugEnabled()) {
            LOG.debug("executeQuery: " + query);
        }

        return statement.executeQuery(query);
    }

    @Override
    public OneRow readNextObject() throws Exception {
        if (resultSet.next()) {
            return new OneRow(null, resultSet);
        }
        return null;
    }

    @Override
    public void closeForRead() throws Exception {
        if (statement != null && !statement.isClosed()) {
            statement.close();
            statement = null;
        }
        super.closeConnection();
    }
}