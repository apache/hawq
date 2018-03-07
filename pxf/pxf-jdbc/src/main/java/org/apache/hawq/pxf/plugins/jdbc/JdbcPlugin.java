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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;

/**
 * JDBC tables plugin
 *
 * Implemented subclasses: {@link JdbcAccessor}, {@link JdbcResolver}.
 */
public class JdbcPlugin extends Plugin {
    /**
     * Class constructor. Parses and checks InputData
     *
     * @param input PXF InputData
     *
     * @throws UserDataException if one of the required request parameters is not set
     */
    public JdbcPlugin(InputData input) throws UserDataException {
        super(input);

        jdbcDriver = input.getUserProperty("JDBC_DRIVER");
        if (jdbcDriver == null) {
            throw new UserDataException("JDBC_DRIVER must be set");
        }

        dbUrl = input.getUserProperty("DB_URL");
        if (dbUrl == null) {
            throw new UserDataException("DB_URL must be set");
        }

        // This parameter may be null
        user = input.getUserProperty("USER");
        if (user != null) {
            pass = input.getUserProperty("PASS");
        }

        // If this parameter is not set, the default value (0) is used instead
        String batchSizeRaw = input.getUserProperty("BATCH_SIZE");
        if (batchSizeRaw != null) {
            try {
                batchSize = Integer.parseInt(batchSizeRaw);
            }
            catch (NumberFormatException e) {
                throw new UserDataException("BATCH_SIZE is incorrect: must be an integer");
            }
        }

        tblName = input.getDataSource();
        if (tblName == null) {
            throw new UserDataException("TABLE_NAME must be set as DataSource");
        }
        // At the moment, when writing into some table, the table name is concatenated with a special string that is necessary to write into HDFS. However, a raw table name is necessary in case of JDBC. The correct table name is extracted here. This code should be removed in later versions (when the table name becomes correct)
        Matcher matcher = tableNamePattern.matcher(tblName);
        if (matcher.matches()) {
            inputData.setDataSource(matcher.group(1));
            tblName = input.getDataSource();
        }

        // This variable is used in Accessor and Resolver, thus if it is not present, no other actions can be performed. This parameter is not under user's control
        columns = inputData.getTupleDescription();
        if (columns == null) {
            throw new UserDataException("Tuple description is not provided");
        }
    }


    // JDBC connection parameters
    protected String jdbcDriver = null;
    protected String dbUrl = null;
    protected String user = null;
    protected String pass = null;
    protected String tblName = null;

    // JDBC connection object
    protected Connection dbConn = null;
    // Database metadata
    protected DatabaseMetaData dbMeta = null;
    // Batch size for INSERTs into the database
    protected int batchSize = 0;
    // Columns description
    protected ArrayList<ColumnDescriptor> columns = null;

    /**
     * Open a JDBC connection
     *
     * @throws ClassNotFoundException if the JDBC driver was not found
     * @throws SQLException if a database access error occurs
     * @throws SQLTimeoutException if a problem with the connection occurs
     */
    protected Connection openConnection() throws ClassNotFoundException, SQLException, SQLTimeoutException {
        if (LOG.isDebugEnabled()) {
            if (user != null) {
                LOG.debug(String.format("Open JDBC connection: driver=%s, url=%s, user=%s, pass=%s, table=%s",
                    jdbcDriver, dbUrl, user, pass, tblName));
            }
            else {
                LOG.debug(String.format("Open JDBC connection: driver=%s, url=%s, table=%s",
                    jdbcDriver, dbUrl, tblName));
            }
        }
        if (dbConn == null || dbConn.isClosed()) {
            Class.forName(jdbcDriver);
            if (user != null) {
                dbConn = DriverManager.getConnection(dbUrl, user, pass);
            }
            else {
                dbConn = DriverManager.getConnection(dbUrl);
            }
            dbMeta = dbConn.getMetaData();
        }
        return dbConn;
    }

    /**
     * Close a JDBC connection
     */
    protected void closeConnection() {
        try {
            if (dbConn != null) {
                dbConn.close();
                dbConn = null;
            }
        }
        catch (SQLException e) {
            LOG.error("JDBC connection close error. ", e);
        }
    }


    private static final Log LOG = LogFactory.getLog(JdbcPlugin.class);
    // At the moment, when writing into some table, the table name is concatenated with a special string that is necessary to write into HDFS. However, a raw table name is necessary in case of JDBC. This Pattern allows to extract the correct table name from the given InputData.dataSource
    private static final Pattern tableNamePattern = Pattern.compile("/(.*)/[0-9]*-[0-9]*_[0-9]*");
}
