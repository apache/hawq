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

import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JDBC tables plugin (base class)
 *
 * Implemented subclasses: {@link JdbcAccessor}, {@link JdbcResolver}.
 */
public class JdbcPlugin extends Plugin {
    /**
     * Class constructor
     *
     * @param input {@link InputData} provided by PXF
     *
     * @throws UserDataException if one of the required request parameters is not set
     */
    public JdbcPlugin(InputData input) throws UserDataException {
        super(input);

        jdbcDriver = input.getUserProperty("JDBC_DRIVER");
        if (jdbcDriver == null) {
            throw new UserDataException("JDBC_DRIVER is a required parameter");
        }

        dbUrl = input.getUserProperty("DB_URL");
        if (dbUrl == null) {
            throw new UserDataException("DB_URL is a required parameter");
        }

        tableName = input.getDataSource();
        if (tableName == null) {
            throw new UserDataException("Data source must be provided");
        }
        /*
        At the moment, when writing into some table, the table name is
        concatenated with a special string that is necessary to write into HDFS.
        However, a raw table name is necessary in case of JDBC.
        The correct table name is extracted here.
        */
        Matcher matcher = tableNamePattern.matcher(tableName);
        if (matcher.matches()) {
            inputData.setDataSource(matcher.group(1));
            tableName = input.getDataSource();
        }

        columns = inputData.getTupleDescription();
        if (columns == null) {
            throw new UserDataException("Tuple description must be provided");
        }

        // This parameter is not required. The default value is null
        user = input.getUserProperty("USER");
        if (user != null) {
            pass = input.getUserProperty("PASS");
        }

        // This parameter is not required. The default value is 0
        String batchSizeRaw = input.getUserProperty("BATCH_SIZE");
        if (batchSizeRaw != null) {
            try {
                batchSize = Integer.parseInt(batchSizeRaw);
                if (batchSize < 1) {
                    throw new NumberFormatException();
                } else if (batchSize == 0) {
                    batchSize = 1;
                }
                batchSizeIsSetByUser = true;
            }
            catch (NumberFormatException e) {
                throw new UserDataException("BATCH_SIZE is incorrect: must be a non-negative integer");
            }
        }

        // This parameter is not required. The default value is 1
        String poolSizeRaw = input.getUserProperty("POOL_SIZE");
        if (poolSizeRaw != null) {
            try {
                poolSize = Integer.parseInt(poolSizeRaw);
            }
            catch (NumberFormatException e) {
                throw new UserDataException("POOL_SIZE is incorrect: must be an integer");
            }
        }
    }

    /**
     * Open a new JDBC connection
     *
     * @throws ClassNotFoundException if the JDBC driver was not found
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a connection problem occurs
     */
    public Connection getConnection() throws ClassNotFoundException, SQLException, SQLTimeoutException {
        Connection connection;
        if (LOG.isDebugEnabled()) {
            if (user != null) {
                LOG.debug(String.format("Open JDBC connection: driver=%s, url=%s, user=%s, pass=%s, table=%s",
                    jdbcDriver, dbUrl, user, pass, tableName));
            }
            else {
                LOG.debug(String.format("Open JDBC connection: driver=%s, url=%s, table=%s",
                    jdbcDriver, dbUrl, tableName));
            }
        }
        Class.forName(jdbcDriver);
        if (user != null) {
            connection = DriverManager.getConnection(dbUrl, user, pass);
        }
        else {
            connection = DriverManager.getConnection(dbUrl);
        }
        return connection;
    }

    /**
     * Close a JDBC connection
     */
    public static void closeConnection(Connection connection) {
        try {
            if ((connection != null) && (!connection.isClosed())) {
                if ((connection.getMetaData().supportsTransactions()) && (!connection.getAutoCommit())) {
                    connection.commit();
                }
                connection.close();
            }
        }
        catch (SQLException e) {
            LOG.error("JDBC connection close error", e);
        }
    }

    /**
     * Prepare a JDBC PreparedStatement
     *
     * @throws ClassNotFoundException if the JDBC driver was not found
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a connection problem occurs
     */
    public PreparedStatement getPreparedStatement(Connection connection, String query) throws SQLException, SQLTimeoutException, ClassNotFoundException {
        if ((connection == null) || (query == null)) {
            throw new IllegalArgumentException("The provided query or connection is null");
        }
        if (connection.getMetaData().supportsTransactions()) {
            connection.setAutoCommit(false);
        }
        return connection.prepareStatement(query);
    }

    /**
     * Close a JDBC Statement (and the connection it is based on)
     */
    public static void closeStatement(Statement statement) {
        if (statement == null) {
            return;
        }
        Connection connection = null;
        try {
            if (!statement.isClosed()) {
                connection = statement.getConnection();
                statement.close();
            }
        }
        catch (Exception e) {}
        closeConnection(connection);
    }

    // JDBC parameters
    protected String jdbcDriver = null;
    protected String dbUrl = null;
    protected String user = null;
    protected String pass = null;

    protected String tableName = null;

    // '100' is a recommended value: https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754
    public static final int DEFAULT_BATCH_SIZE = 100;
    // After argument parsing, this value is guaranteed to be >= 1
    protected int batchSize = DEFAULT_BATCH_SIZE;
    protected boolean batchSizeIsSetByUser = false;

    protected int poolSize = 1;

    // Columns description
    protected ArrayList<ColumnDescriptor> columns = null;


    private static final Log LOG = LogFactory.getLog(JdbcPlugin.class);

    // At the moment, when writing into some table, the table name is concatenated with a special string that is necessary to write into HDFS. However, a raw table name is necessary in case of JDBC. This Pattern allows to extract the correct table name from the given InputData.dataSource
    private static final Pattern tableNamePattern = Pattern.compile("/(.*)/[0-9]*-[0-9]*_[0-9]*");
}
