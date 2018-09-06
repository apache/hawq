package org.apache.hawq.pxf.plugins.jdbc.writercallable;

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
import org.apache.hawq.pxf.plugins.jdbc.JdbcResolver;
import org.apache.hawq.pxf.plugins.jdbc.JdbcPlugin;

import java.util.LinkedList;
import java.util.List;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * This writer makes batch INSERTs.
 *
 * A call() is required after a certain number of supply() calls
 */
class BatchWriterCallable implements WriterCallable {
    @Override
    public void supply(OneRow row) throws IllegalStateException {
        if ((batchSize > 0) && (rows.size() >= batchSize)) {
            throw new IllegalStateException("Trying to supply() a OneRow object to a full WriterCallable");
        }
        if (row == null) {
            throw new IllegalArgumentException("Trying to supply() a null OneRow object");
        }
        rows.add(row);
    }

    @Override
    public boolean isCallRequired() {
        return (batchSize > 0) && (rows.size() >= batchSize);
    }

    @Override
    public SQLException call() throws IOException, SQLException, ClassNotFoundException {
        if (rows.isEmpty()) {
            return null;
        }

        boolean statementMustBeDeleted = false;
        if (statement == null) {
            statement = plugin.getPreparedStatement(plugin.getConnection(), query);
            statementMustBeDeleted = true;
        }

        for (OneRow row : rows) {
            JdbcResolver.decodeOneRowToPreparedStatement(row, statement);
            statement.addBatch();
        }

        try {
            statement.executeBatch();
        }
        catch (SQLException e) {
            return e;
        }
        finally {
            rows.clear();
            if (statementMustBeDeleted) {
                JdbcPlugin.closeStatement(statement);
                statement = null;
            }
        }

        return null;
    }

    /**
     * Construct a new batch writer
     */
    BatchWriterCallable(JdbcPlugin plugin, String query, PreparedStatement statement, int batchSize) {
        if (plugin == null || query == null) {
            throw new IllegalArgumentException("The provided JdbcPlugin or SQL query is null");
        }

        this.plugin = plugin;
        this.query = query;
        this.statement = statement;
        this.batchSize = batchSize;

        rows = new LinkedList<>();
    }

    private final JdbcPlugin plugin;
    private final String query;
    private PreparedStatement statement;
    private List<OneRow> rows;
    private final int batchSize;
}
