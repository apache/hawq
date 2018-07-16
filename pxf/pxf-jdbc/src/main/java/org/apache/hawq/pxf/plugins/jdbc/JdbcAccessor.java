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
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.WriteAccessor;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.IOException;
import java.text.ParseException;
import java.math.BigDecimal;
import java.sql.Types;
import java.sql.Timestamp;
import java.sql.Date;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.BatchUpdateException;
import java.sql.Connection;

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

        dbCurrentConn = super.openConnection();

        queryRead = buildSelectQuery();
        statementRead = dbCurrentConn.createStatement();
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
     *
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void closeForRead() throws SQLException {
        if (statementRead != null && !statementRead.isClosed()) {
            statementRead.close();
            statementRead = null;
        }
        JdbcPlugin.closeConnection(dbCurrentConn);
    }

    /**
     * openForWrite() implementation
     * Create query template and open JDBC connection
     *
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     * @throws ParseException if the SQL statement provided in PXF InputData is incorrect
     * @throws ClassNotFoundException if the JDBC driver was not found
     */
    @Override
    public boolean openForWrite() throws SQLException, SQLTimeoutException, ParseException, ClassNotFoundException {
        if (statementWriteCurrent != null && !statementWriteCurrent.isClosed()) {
            throw new SQLException("The connection to an external database is already open.");
        }
        queryWrite = buildInsertQuery();
        prepareVariablesForWrite();

        if ((batchSize != 0) && (!dbMeta.supportsBatchUpdates())) {
            LOG.warn(
                "The database '" +
                dbMeta.getDatabaseProductName() +
                "' does not support batch updates. The current request will be handled without batching"
            );
            batchSize = 0;
        }

        if (poolSize < 1) {
            // Set the poolSize equal to the number of CPUs available
            poolSize = Runtime.getRuntime().availableProcessors();
            LOG.info(
                "The POOL_SIZE is set to the number of CPUs available (" + Integer.toString(poolSize) + ")"
            );
        }
        // Now the poolSize is >= 1
        if (poolSize > 1) {
            executorServiceWrite = Executors.newFixedThreadPool(poolSize);
            poolTasks = new LinkedList<>();
        }

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
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean writeNextObject(OneRow row) throws SQLException, IOException, ClassNotFoundException {
        // This cast is safe because the data in the row is formed by JdbcPlugin
        List<OneField> tuple = (List<OneField>) row.getData();

        if (LOG.isDebugEnabled()) {
            LOG.debug("writeNextObject() called");
        }

        for (int i = 1; i <= tuple.size(); i++) {
            OneField field = tuple.get(i - 1);
            switch (DataType.get(field.type)) {
                case INTEGER:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.INTEGER);
                    }
                    else {
                        statementWriteCurrent.setInt(i, (int)field.val);
                    }
                    break;
                case BIGINT:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.INTEGER);
                    }
                    else {
                        statementWriteCurrent.setLong(i, (long)field.val);
                    }
                    break;
                case SMALLINT:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.INTEGER);
                    }
                    else {
                        statementWriteCurrent.setShort(i, (short)field.val);
                    }
                    break;
                case REAL:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.FLOAT);
                    }
                    else {
                        statementWriteCurrent.setFloat(i, (float)field.val);
                    }
                    break;
                case FLOAT8:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.DOUBLE);
                    }
                    else {
                        statementWriteCurrent.setDouble(i, (double)field.val);
                    }
                    break;
                case BOOLEAN:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.BOOLEAN);
                    }
                    else {
                        statementWriteCurrent.setBoolean(i, (boolean)field.val);
                    }
                    break;
                case NUMERIC:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.NUMERIC);
                    }
                    else {
                        statementWriteCurrent.setBigDecimal(i, (BigDecimal)field.val);
                    }
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.VARCHAR);
                    }
                    else {
                        statementWriteCurrent.setString(i, (String)field.val);
                    }
                    break;
                case BYTEA:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.BINARY);
                    }
                    else {
                        statementWriteCurrent.setBytes(i, (byte[])field.val);
                    }
                    break;
                case TIMESTAMP:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.TIMESTAMP);
                    }
                    else {
                        statementWriteCurrent.setTimestamp(i, (Timestamp)field.val);
                    }
                    break;
                case DATE:
                    if (field.val == null) {
                        statementWriteCurrent.setNull(i, Types.DATE);
                    }
                    else {
                        statementWriteCurrent.setDate(i, (Date)field.val);
                    }
                    break;
                default:
                    throw new IOException("The data tuple from JdbcResolver is corrupted");
            }
        }

        SQLException rollbackException = null;
        if (batchSize < 0) {
            // Batch has an infinite size
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trying to add a tuple to the batch (batch size is infinite)");
            }
            try {
                statementWriteCurrent.addBatch();
            }
            catch (SQLException e) {
                rollbackException = e;
            }
        }
        else if ((batchSize == 0) || (batchSize == 1)) {
            // Batching is not used
            StatementCallable statementCallable = new StatementCallable(false, statementWriteCurrent);
            // Check whether pooling is used and act accordingly
            if (poolSize == 1) {
                rollbackException = statementCallable.call();
            }
            else {
                statementCallable.setSelfDestructible();
                poolTasks.add(executorServiceWrite.submit(statementCallable));
                prepareVariablesForWrite();
            }
        }
        else {
            // Batch has a finite size
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Trying to add a tuple to the batch (batch size is now " + Integer.toString(batchSizeCurrent) + ")"
                );
            }
            statementWriteCurrent.addBatch();
            batchSizeCurrent += 1;
            if (batchSizeCurrent >= batchSize) {
                batchSizeCurrent = 0;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Trying to execute the batch (batch size is now equal to the required one)");
                }
                StatementCallable statementCallable = new StatementCallable(true, statementWriteCurrent);
                // Check if pooling is used and act accordingly
                if (poolSize == 1) {
                    rollbackException = statementCallable.call();
                }
                else {
                    statementCallable.setSelfDestructible();
                    poolTasks.add(executorServiceWrite.submit(statementCallable));
                    prepareVariablesForWrite();
                }
            }
        }

        if (rollbackException != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Rollback is now required");
            }
            rollbackException = tryRollback(rollbackException);
            throw rollbackException;
        }

        return true;
    }

    /**
     * closeForWrite() implementation
     *
     * @throws SQLException if a database access error occurs
     * @throws Exception if pooling is used and any pool thread encounter runtime problems
     */
    @Override
    public void closeForWrite() throws Exception {
        // We make a manual check here because if something is wrong with the statement or the connection, we should not throw an exception; instead, we only throw exception from the first unsuccessful update operation
        if ((dbCurrentConn != null) && (statementWriteCurrent != null) && (!statementWriteCurrent.isClosed())) {
            try {
                // Collect the results of pool threads
                if (poolSize > 1) {
                    Exception firstException = null;
                    for (Future<SQLException> task : poolTasks) {
                        // We need this construction to ensure that we try to close *all* connections opened by pool threads
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
                            // This exception could be caused by connection and statement cleanup problems or by thread execution errors
                            /*
                            if (firstException == null) {
                                firstException = e;
                            }
                            */
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                    "A runtime exception in a pool thread occured: " + e.getClass() + " " + e.getMessage()
                                );
                            }
                        }
                    }
                    try {
                        executorServiceWrite.shutdown();
                    }
                    catch (Exception e) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("executorServiceWrite.shutdown() threw an exception: " + e.getClass().toString());
                        }
                    }
                    if (firstException != null) {
                        // Rollback is not possible: each pool thread commits its own part of the query
                        throw firstException;
                    }
                }
                // If batching was used, execute the last batch
                if ((batchSize < 0) || ((batchSize > 1) && (batchSizeCurrent > 0))) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Trying to execute the batch (closeForWrite() was called)");
                    }
                    SQLException e = new StatementCallable(true, statementWriteCurrent).call();
                    if (e != null) {
                        e = tryRollback(e);
                        throw e;
                    }
                }
            }
            finally {
                cleanVariablesForWrite(statementWriteCurrent);
            }
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
    private String buildSelectQuery() throws ParseException, SQLException {
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
        (new WhereSQLBuilder(inputData)).buildWhereSQL(dbMeta.getDatabaseProductName(), sb);

        // Insert partition constraints
        JdbcPartitionFragmenter.buildFragmenterSql(inputData, dbMeta.getDatabaseProductName(), sb);

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
    private SQLException tryRollback(SQLException rollbackException) {
        // Get transactions support status
        boolean areTransactionsSupported = false;
        try {
            areTransactionsSupported = dbMeta.supportsTransactions();
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
            dbCurrentConn.rollback();
        }
        catch (SQLException e) {
            // log exception as error, but do not throw it (the actual cause of rollback will be thrown instead)
            LOG.error("An exception happened during the transaction rollback: '" + e.toString() + "'");
        }
        return rollbackException;
    }

    /**
     * Open the Connection, set autocommit mode if possible and prepare statement
     */
    private void prepareVariablesForWrite() throws SQLException, SQLTimeoutException, ClassNotFoundException {
        dbCurrentConn = super.openConnection();
        if (dbMeta.supportsTransactions()) {
            dbCurrentConn.setAutoCommit(false);
        }
        statementWriteCurrent = dbCurrentConn.prepareStatement(queryWrite);
    }

    /**
     * Close the given statement and a connection that it uses
     */
    private static void cleanVariablesForWrite(PreparedStatement statement) throws SQLException {
        if ((statement == null) || (statement.isClosed())) {
            return;
        }
        Connection conn = null;
        try {
            conn = statement.getConnection();
        }
        catch (Exception e) {}
        statement.close();
        JdbcPlugin.closeConnection(conn);
    }

    private static class StatementCallable implements Callable<SQLException> {
        /**
         * Wrap a PreparedStatement to implement Callable interface
         */
        public StatementCallable(boolean withBatch, PreparedStatement statement) throws SQLException {
            selfDestructible = false;
            this.withBatch = withBatch;
            if ((statement == null) || (statement.isClosed())) {
                throw new SQLException("The provided statement is null or closed");
            }
            this.statement = statement;
        }

        @Override
        public SQLException call() throws SQLException {
            SQLException result;
            try {
                if (withBatch) {
                    result = executeUpdateWithBatch(statement);
                }
                else {
                    result = executeUpdateWithoutBatch(statement);
                }
            }
            finally {
                if (isSelfDestructible()) {
                    JdbcAccessor.cleanVariablesForWrite(statement);
                }
            }
            return result;
        }

        /**
         * Make this StatementCallable close the connection after execution
         */
        public void setSelfDestructible() {
            selfDestructible = true;
        }

        /**
         * Check if this StatementCallable will close its connections automatically after execution
         */
        public boolean isSelfDestructible() {
            return selfDestructible;
        }

        /**
         * Execute INSERT query without batching.
         *
         * @param statement A PreparedStatement containing the query
         * @return null or a SQLException that occured during update
         */
        private SQLException executeUpdateWithoutBatch(PreparedStatement statement) {
            try {
                if (statement.executeUpdate() != 1) {
                    throw new SQLException("The number of rows affected by INSERT query is incorrect");
                }
            }
            catch (SQLException e) {
                return e;
            }
            return null;
        }

        /**
         * Execute INSERT query with batching.
         *
         * @param statement A PreparedStatement containing the batch of queries
         * @return null or a SQLException that occured during update
         */
        private SQLException executeUpdateWithBatch(PreparedStatement statement) {
            try {
                statement.executeBatch();
            }
            catch (SQLException e) {
                return e;
            }
            return null;
        }

        private final boolean withBatch;
        private boolean selfDestructible;
        private PreparedStatement statement;
    }

    private static final Log LOG = LogFactory.getLog(JdbcAccessor.class);

    private static final String FAILED_INSERT_MESSAGE = "Insert failed due to an SQLException. SOME DATA MAY HAVE BEEN INSERTED. ";

    // Read variables
    private String queryRead = null;
    private Statement statementRead = null;
    private ResultSet resultSetRead = null;

    // Write variables
    private String queryWrite = null;
    private PreparedStatement statementWriteCurrent = null;
    private int batchSizeCurrent = 0;

    private ExecutorService executorServiceWrite = null;
    private List<Future<SQLException> > poolTasks = null;
}
