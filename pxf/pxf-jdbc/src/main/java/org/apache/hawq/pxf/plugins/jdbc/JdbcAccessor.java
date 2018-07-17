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
import org.apache.hawq.pxf.api.WriteAccessor;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.jdbc.writercallable.WriterCallable;
import org.apache.hawq.pxf.plugins.jdbc.writercallable.WriterCallableFactory;

import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.io.IOException;
import java.text.ParseException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JDBC tables accessor
 *
 * The SELECT queries are processed by {@link java.sql.Statement}
 *
 * The INSERT queries are processed by {@link java.sql.PreparedStatement} and
 * built-in JDBC batches of arbitrary size
 */
public class JdbcAccessor extends JdbcPlugin implements ReadAccessor, WriteAccessor {
    /**
     * Class constructor
     */
    public JdbcAccessor(InputData inputData) throws UserDataException {
        super(inputData);
    }

    /**
     * openForRead() implementation
     * Create query, open JDBC connection, execute query and store the result into resultSet
     *
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     * @throws ParseException if th SQL statement provided in PXF InputData is incorrect
     * @throws ClassNotFoundException if the JDBC driver was not found
     */
    @Override
    public boolean openForRead() throws SQLException, SQLTimeoutException, ParseException, ClassNotFoundException {
        if (statementRead != null && !statementRead.isClosed()) {
            return true;
        }

        Connection connection = super.getConnection();

        queryRead = buildSelectQuery(connection.getMetaData());
        statementRead = connection.createStatement();
        resultSetRead = statementRead.executeQuery(queryRead);

        return true;
    }

    /**
     * readNextObject() implementation
     * Retreive the next tuple from resultSet and return it
     *
     * @throws SQLException if a problem in resultSet occurs
     */
    @Override
    public OneRow readNextObject() throws SQLException {
        if (resultSetRead.next()) {
            return new OneRow(resultSetRead);
        }
        return null;
    }

    /**
     * closeForRead() implementation
     */
    @Override
    public void closeForRead() {
        JdbcPlugin.closeStatement(statementRead);
    }

    /**
     * openForWrite() implementation
     * Create query template and open JDBC connection
     *
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     * @throws ParseException if the SQL statement provided in PXF InputData is incorrect
     * @throws ClassNotFoundException if the JDBC driver was not found
     * @throws IllegalArgumentException if the provided or generated combination of user-defined parameters cannot be processed
     */
    @Override
    public boolean openForWrite() throws SQLException, SQLTimeoutException, ParseException, ClassNotFoundException, IllegalArgumentException {
        if (statementWrite != null && !statementWrite.isClosed()) {
            throw new SQLException("The connection to an external database is already open.");
        }

        Connection connection = super.getConnection();

        queryWrite = buildInsertQuery();
        statementWrite = super.getPreparedStatement(connection, queryWrite);

        // Process batchSize
        if ((batchSize != 0) && (!connection.getMetaData().supportsBatchUpdates())) {
            LOG.warn(
                "The database '" +
                connection.getMetaData().getDatabaseProductName() +
                "' does not support batch updates. The current request will be handled without batching"
            );
            batchSize = 0;
        }

        // Process poolSize
        if (poolSize < 1) {
            poolSize = Runtime.getRuntime().availableProcessors();
            LOG.info(
                "The POOL_SIZE is set to the number of CPUs available (" + Integer.toString(poolSize) + ")"
            );
        }
        if (poolSize > 1) {
            executorServiceWrite = Executors.newFixedThreadPool(poolSize);
            poolTasks = new LinkedList<>();
        }

        // Setup WriterCallableFactory
        writerCallableFactory = new WriterCallableFactory();
        writerCallableFactory.setPlugin(this);
        writerCallableFactory.setQuery(queryWrite);
        writerCallableFactory.setBatchSize(batchSize);
        if (poolSize == 1) {
            writerCallableFactory.setStatement(statementWrite);
        }

        writerCallable = writerCallableFactory.get();

        return true;
    }

	/**
     * writeNextObject() implementation
     *
     * If batchSize is not 0 or 1, add a tuple to the batch of statementWrite
     * Otherwise, execute an INSERT query immediately
     *
     * In both cases, a {@link java.sql.PreparedStatement} is used
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if the data provided by {@link JdbcResolver} is corrupted
     * @throws ClassNotFoundException if pooling is used and the JDBC driver was not found
     * @throws IllegalStateException if writerCallableFactory was not properly initialized
     * @throws Exception if it happens in writerCallable.call()
     */
    @Override
    public boolean writeNextObject(OneRow row) throws Exception {
        if (writerCallable == null) {
            throw new IllegalStateException("The JDBC connection was not properly initialized (writerCallable is null)");
        }

        SQLException rollbackException = null;

        writerCallable.supply(row);
        if (writerCallable.isCallRequired()) {
            if (poolSize > 1) {
                // Pooling is used. Create new writerCallable
                poolTasks.add(executorServiceWrite.submit(writerCallable));
                writerCallable = writerCallableFactory.get();
            }
            else {
                // Pooling is not used
                try {
                    rollbackException = writerCallable.call();
                }
                catch (SQLException e) {
                    rollbackException = e;
                }
                catch (Exception e) {
                    // This is not expected
                    throw e;
                }
            }
        }

        if (rollbackException != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Rollback is now required");
            }
            rollbackException = tryRollback(statementWrite.getConnection(), rollbackException);
            throw rollbackException;
        }

        return true;
    }

    /**
     * closeForWrite() implementation
     *
     * @throws SQLException if a database access error occurs
     * @throws Exception if it happens in writerCallable.call() or due to runtime errors in thread pool
     */
    @Override
    public void closeForWrite() throws Exception {
        if ((statementWrite == null) || (writerCallable == null)) {
            return;
        }

        try {
            if (poolSize > 1) {
                // Process thread pool
                Exception firstException = null;
                for (Future<SQLException> task : poolTasks) {
                    // We need this construction to ensure that we try to close all connections opened by pool threads
                    try {
                        SQLException currentSqlException = task.get();
                        if (currentSqlException != null) {
                            if (firstException == null) {
                                firstException = currentSqlException;
                            }
                            LOG.error(
                                "A SQLException in a pool thread occured: " + currentSqlException.getClass() + " " + currentSqlException.getMessage()
                            );
                        }
                    }
                    catch (Exception e) {
                        // This exception must have been caused by some thread execution error. However, there may be other exception (maybe of class SQLException) that happened in one of threads that were not examined yet. That is why we do not modify firstException
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                "A runtime exception in a thread pool occured: " + e.getClass() + " " + e.getMessage()
                            );
                        }
                    }
                }
                try {
                    executorServiceWrite.shutdown();
                    executorServiceWrite.shutdownNow();
                }
                catch (Exception e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("executorServiceWrite.shutdown() or .shutdownNow() threw an exception: " + e.getClass() + " " + e.getMessage());
                    }
                }
                if (firstException != null) {
                    // Rollback is of no use: each pool thread commits a part of the query itself
                    throw firstException;
                }
            }

            // Send the data that is left
            SQLException rollbackException;
            try {
                rollbackException = writerCallable.call();
            }
            catch (SQLException e) {
                rollbackException = e;
            }
            catch (Exception e) {
                throw e;
            }

            if (rollbackException != null) {
                if (poolSize == 1) {
                    rollbackException = tryRollback(statementWrite.getConnection(), rollbackException);
                }
                // Otherwise, rollback is of no use (again)
                throw rollbackException;
            }
        }
        finally {
            JdbcPlugin.closeStatement(statementWrite);
        }
    }


    /**
     * Build SELECT query (with "WHERE" and partition constraints)
     *
     * @return Complete SQL query
     *
     * @throws ParseException if the constraints passed in InputData are incorrect
     * @throws SQLException if the database metadata is invalid
     */
    private String buildSelectQuery(DatabaseMetaData databaseMetaData) throws ParseException, SQLException {
        if (databaseMetaData == null) {
            throw new IllegalArgumentException("The provided databaseMetaData is null");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        // Insert columns' names
        String columnDivisor = "";
        for (ColumnDescriptor column : columns) {
            sb.append(columnDivisor);
            columnDivisor = ", ";
            sb.append(column.columnName());
        }

        // Insert the table name
        sb.append(" FROM ").append(tableName);

        // Insert regular WHERE constraints
        (new WhereSQLBuilder(inputData)).buildWhereSQL(databaseMetaData.getDatabaseProductName(), sb);

        // Insert partition constraints
        JdbcPartitionFragmenter.buildFragmenterSql(inputData, databaseMetaData.getDatabaseProductName(), sb);

        return sb.toString();
    }

    /**
     * Build INSERT query template (field values are replaced by placeholders '?')
     *
     * @return SQL query with placeholders instead of actual values
     */
    private String buildInsertQuery() {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ");

        // Insert the table name
        sb.append(tableName);

        // Insert columns' names
        sb.append("(");
        String fieldDivisor = "";
        for (ColumnDescriptor column : columns) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append(column.columnName());
        }
        sb.append(")");

        sb.append(" VALUES ");

        // Insert values placeholders
        sb.append("(");
        fieldDivisor = "";
        for (int i = 0; i < columns.size(); i++) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append("?");
        }
        sb.append(")");

        return sb.toString();
    }

    /**
     * Try to rollback the current transaction
     *
     * @param rollbackException the rollback cause
     *
     * @return The resulting SQLException (it may differ from 'rollbackException')
     */
    private SQLException tryRollback(Connection connection, SQLException rollbackException) {
        // Get transactions support status
        boolean areTransactionsSupported = false;
        try {
            areTransactionsSupported = connection.getMetaData().supportsTransactions();
        }
        catch (SQLException e) {
            // This error is unexpected; the dbMeta is probably corrupted
            return rollbackException;
        }

        if (rollbackException instanceof BatchUpdateException) {
            // If we examine the BatchUpdateException, it must have the 'next' SQLException
            rollbackException = ((BatchUpdateException)rollbackException).getNextException();
        }

        if (!areTransactionsSupported) {
            // The bad case: transactions are NOT supported
            if ((batchSize < 0) || (batchSize > 1)) {
                // The worst case: batching was used
                return new SQLException(FAILED_INSERT_MESSAGE + "The exact number of tuples inserted is unknown.", rollbackException);
            }
            // Batching was NOT used
            return new SQLException(FAILED_INSERT_MESSAGE + "The exact number of tuples inserted can be found in PXF logs.", rollbackException);
        }

        // The best case: transactions are supported
        try {
            connection.rollback();
        }
        catch (SQLException e) {
            // log exception as error, but do not throw it (the actual cause of rollback will be thrown instead)
            LOG.error("An exception happened during the transaction rollback: '" + e.toString() + "'");
        }
        return rollbackException;
    }

    // Read variables
    private String queryRead = null;
    private Statement statementRead = null;
    private ResultSet resultSetRead = null;

    // Write variables
    private String queryWrite = null;
    private PreparedStatement statementWrite = null;
    private WriterCallableFactory writerCallableFactory = null;
    private WriterCallable writerCallable = null;
    private ExecutorService executorServiceWrite = null;
    private List<Future<SQLException> > poolTasks = null;

    // Static variables
    private static final Log LOG = LogFactory.getLog(JdbcAccessor.class);
    private static final String FAILED_INSERT_MESSAGE = "Insert failed due to an SQLException. SOME DATA MAY HAVE BEEN INSERTED. ";
}
