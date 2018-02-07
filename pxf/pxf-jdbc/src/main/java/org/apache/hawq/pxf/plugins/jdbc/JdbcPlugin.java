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
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

import java.sql.*;

/**
 * This class resolves the jdbc connection parameter and manages the opening and closing of the jdbc connection.
 * Implemented subclasses: {@link JdbcReadAccessor}.
 *
 */
public class JdbcPlugin extends Plugin {
    private static final Log LOG = LogFactory.getLog(JdbcPlugin.class);

    //jdbc connection parameters
    protected String jdbcDriver = null;
    protected String dbUrl = null;
    protected String user = null;
    protected String pass = null;
    protected String tblName = null;
    protected int batchSize = 100;

    //jdbc connection
    protected Connection dbConn = null;
    //database type, from DatabaseMetaData.getDatabaseProductName()
    protected String dbProduct = null;

    /**
     * parse input data
     *
     * @param input the input data
     * @throws UserDataException if the request parameter is malformed
     */
    public JdbcPlugin(InputData input) throws UserDataException {
        super(input);
        jdbcDriver = input.getUserProperty("JDBC_DRIVER");
        dbUrl = input.getUserProperty("DB_URL");
        user = input.getUserProperty("USER");
        pass = input.getUserProperty("PASS");
        String strBatch = input.getUserProperty("BATCH_SIZE");
        if (strBatch != null) {
            batchSize = Integer.parseInt(strBatch);
        }

        if (jdbcDriver == null) {
            throw new UserDataException("JDBC_DRIVER must be set");
        }
        if (dbUrl == null) {
            throw new UserDataException("DB_URL must be set(read)");
        }

        tblName = input.getDataSource();
        if (tblName == null) {
            throw new UserDataException("TABLE_NAME must be set as DataSource.");
        }
    }

    public String getTableName() {
        return tblName;
    }

    protected Connection openConnection() throws ClassNotFoundException, SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Open JDBC: driver=%s,url=%s,user=%s,pass=%s,table=%s",
                    jdbcDriver, dbUrl, user, pass, tblName));
        }
        if (dbConn == null || dbConn.isClosed()) {
            Class.forName(jdbcDriver);
            if (user != null) {
                dbConn = DriverManager.getConnection(dbUrl, user, pass);
            } else {
                dbConn = DriverManager.getConnection(dbUrl);
            }
            DatabaseMetaData meta = dbConn.getMetaData();
            dbProduct = meta.getDatabaseProductName();
        }
        return dbConn;
    }

    protected void closeConnection() {
        try {
            if (dbConn != null) {
                dbConn.close();
                dbConn = null;
            }
        } catch (SQLException e) {
            LOG.error("Close db connection error . ", e);
        }
    }
}
