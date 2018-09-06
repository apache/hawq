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
import org.apache.hawq.pxf.plugins.jdbc.JdbcPlugin;

import java.sql.PreparedStatement;

/**
 * An object that processes INSERT operation on {@link OneRow} objects
 */
public class WriterCallableFactory {
    /**
     * Create a new {@link WriterCallable} factory.
     *
     * Note that 'setPlugin' and 'setQuery' must be called before construction of a {@link WriterCallable}.
     *
     * By default, 'statement' is null
     */
    public WriterCallableFactory() {
        batchSize = JdbcPlugin.DEFAULT_BATCH_SIZE;
        plugin = null;
        query = null;
        statement = null;
    }

    /**
     * Get an instance of WriterCallable
     *
     * @return an implementation of WriterCallable, chosen based on parameters that were set for this factory
     */
    public WriterCallable get() {
        if (batchSize > 1) {
            return new BatchWriterCallable(plugin, query, statement, batchSize);
        }
        return new SimpleWriterCallable(plugin, query, statement);
    }

    /**
     * Set {@link JdbcPlugin} to use.
     * REQUIRED
     */
    public void setPlugin(JdbcPlugin plugin) {
        this.plugin = plugin;
    }

    /**
     * Set SQL query to use.
     * REQUIRED
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * Set batch size to use
     *
     * @param batchSize > 1: Use batches of specified size
     * @param batchSize < 1: Do not use batches
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Set statement to use.
     *
     * @param statement = null: Create a new connection & a new statement each time {@link WriterCallable} is called
     * @param statement not null: Use the given statement and do not close or reopen it
     */
    public void setStatement(PreparedStatement statement) {
        this.statement = statement;
    }

    private int batchSize;
    private JdbcPlugin plugin;
    private String query;
    private PreparedStatement statement;
}
