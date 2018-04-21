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
import java.io.IOException;
import java.text.ParseException;
import java.math.BigDecimal;
import java.sql.Types;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.sql.Timestamp;

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

        super.openConnection();

        queryRead = buildSelectQuery();
        statementRead = dbConn.createStatement();
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
        super.closeConnection();
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
        if (statementWrite != null && !statementWrite.isClosed()) {
            return true;
        }

        super.openConnection();
        if (dbMeta.supportsTransactions()) {
            dbConn.setAutoCommit(false);
        }

        queryWrite = buildInsertQuery();
        statementWrite = dbConn.prepareStatement(queryWrite);

        if ((batchSize != 0) && (!dbMeta.supportsBatchUpdates())) {
            LOG.warn(
                "The database '" +
                dbMeta.getDatabaseProductName() +
                "' does not support batch updates. The current request will be handled without batching"
            );
            batchSize = 0;
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
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean writeNextObject(OneRow row) throws SQLException, IOException {
        // This cast is safe because the data in the row is formed by JdbcPlugin
        List<OneField> tuple = (List<OneField>) row.getData();

        if (LOG.isDebugEnabled()) {
            LOG.debug("writeNextObject() called");
        }

        for (int i = 1; i <= tuple.size(); i++) {
            OneField field = tuple.get(i - 1);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Field " + i + ": " + DataType.get(field.type).toString());
            }
            switch (DataType.get(field.type)) {
                case INTEGER:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.INTEGER);
                    }
                    else {
                        statementWrite.setInt(i, (int)field.val);
                    }
                    break;
                case BIGINT:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.INTEGER);
                    }
                    else {
                        statementWrite.setLong(i, (long)field.val);
                    }
                    break;
                case SMALLINT:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.INTEGER);
                    }
                    else {
                        statementWrite.setShort(i, (short)field.val);
                    }
                    break;
                case REAL:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.FLOAT);
                    }
                    else {
                        statementWrite.setFloat(i, (float)field.val);
                    }
                    break;
                case FLOAT8:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.DOUBLE);
                    }
                    else {
                        statementWrite.setDouble(i, (double)field.val);
                    }
                    break;
                case BOOLEAN:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.BOOLEAN);
                    }
                    else {
                        statementWrite.setBoolean(i, (boolean)field.val);
                    }
                    break;
                case NUMERIC:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.NUMERIC);
                    }
                    else {
                        statementWrite.setBigDecimal(i, (BigDecimal)field.val);
                    }
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.VARCHAR);
                    }
                    else {
                        statementWrite.setString(i, (String)field.val);
                    }
                    break;
                case BYTEA:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.BINARY);
                    }
                    else {
                        statementWrite.setBytes(i, (byte[])field.val);
                    }
                    break;
                case TIMESTAMP:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.TIMESTAMP);
                    }
                    else {
                        statementWrite.setTimestamp(i, (Timestamp)field.val);
                    }
                    break;
                case DATE:
                    if (field.val == null) {
                        statementWrite.setNull(i, Types.DATE);
                    }
                    else {
                        statementWrite.setDate(i, (Date)field.val);
                    }
                    break;
                default:
                    throw new IOException("The data tuple from JdbcResolver is corrupted");
            }
        }

        boolean rollbackRequired = false;
        SQLException rollbackException = null;
        if (batchSize < 0) {
            // Batch has an infinite size
            statementWrite.addBatch();
        }
        else if ((batchSize == 0) || (batchSize == 1)) {
            // Batching is not used
            try {
                rollbackRequired = (statementWrite.executeUpdate() != 1);
                if (rollbackRequired) {
                    rollbackException = new SQLException("The number of rows affected by INSERT query is incorrect");
                }
            }
            catch (SQLException e) {
                rollbackRequired = true;
                rollbackException = e;
            }
        }
        else {
            // Batch has a finite size
            statementWrite.addBatch();
            batchSizeCurrent += 1;
            if (batchSizeCurrent >= batchSize) {
                batchSizeCurrent = 0;
                try {
                    statementWrite.executeBatch();
                    statementWrite.clearBatch();
                }
                catch (SQLException e) {
                    rollbackRequired = true;
                    rollbackException = e;
                }
            }
        }

        if (rollbackRequired) {
            rollbackException = tryRollback(rollbackException);
            throw rollbackException;
        }

        return true;
    }

    /**
     * closeForWrite() implementation
     *
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void closeForWrite() throws SQLException {
        try {
            if ((dbConn != null) && (statementWrite != null) && (!statementWrite.isClosed())) {
                // If batching was used, execute the batch
                if ((batchSize < 0) || ((batchSize > 1) && (batchSizeCurrent > 0))) {
                    try {
                        statementWrite.executeBatch();
                    }
                    catch (SQLException e) {
                        e = tryRollback(e);
                        throw e;
                    }
                }
                if (dbMeta.supportsTransactions()) {
                    dbConn.commit();
                }
            }
        }
        finally {
            super.closeConnection();
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
            dbConn.rollback();
        }
        catch (SQLException e) {
            // log exception as error, but do not throw it (the actual cause of rollback will be thrown instead)
            LOG.error("An exception happened during the transaction rollback: '" + e.toString() + "'");
        }
        return rollbackException;
    }


    private static final Log LOG = LogFactory.getLog(JdbcAccessor.class);

    private static final String FAILED_INSERT_MESSAGE = "Insert failed due to an SQLException. The target database does not support transactions and SOME DATA MAY HAVE BEEN INSERTED. ";

    // Read variables
    private String queryRead = null;
    private Statement statementRead = null;
    private ResultSet resultSetRead = null;

    // Write variables
    private String queryWrite = null;
    private PreparedStatement statementWrite = null;
    private int batchSizeCurrent = 0;
}
