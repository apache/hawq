package org.apache.hawq.pxf.plugins.jdbc.utils;

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
 * A tool class to process data types that must have different form in different databases.
 * Such processing is required to create correct constraints (WHERE statements).
 */
public abstract class DbProduct {
    /**
     * Get an instance of some class - the database product
     *
     * @param String dbName A full name of the database
     * @return a DbProduct of the required class
     */
    public static DbProduct getDbProduct(String dbName) {
        if (dbName.toUpperCase().contains("MYSQL"))
            return new MysqlProduct();
        else if (dbName.toUpperCase().contains("ORACLE"))
            return new OracleProduct();
        else if (dbName.toUpperCase().contains("POSTGRES"))
            return new PostgresProduct();
        else if (dbName.toUpperCase().contains("MICROSOFT"))
            return new MicrosoftProduct();
        else
            return new CommonProduct();
    }

    /**
     * Wraps a given date value the way required by a target database
     *
     * @param val {@link java.sql.Date} object to wrap
     * @return a string with a properly wrapped date object
     */
    public abstract String wrapDate(Object val);

    /**
     * Wraps a given timestamp value the way required by a target database
     *
     * @param val {@link java.sql.Timestamp} object to wrap
     * @return a string with a properly wrapped timestamp object
     */
    public abstract String wrapTimestamp(Object val);
}

/**
 * Common product. Used when no other products are avalibale
 */
class CommonProduct extends DbProduct {
    @Override
    public String wrapDate(Object val) {
        return "date'" + val + "'";
    }

    @Override
    public String wrapTimestamp(Object val) {
        return "'" + val + "'";
    }
}
