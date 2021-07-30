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
 * As the syntax of different database products are not the same, such as the date type  field for processing, ORACLE use to_date () function, and mysql use Date () function.
 So we create this class to abstract public methods, the specific database products can implementation of these  methods.
 */
public abstract class DbProduct {
    //wrap date string
    public abstract String wrapDate(Object date_val);


    public static DbProduct getDbProduct(String dbName) {
        if (dbName.toUpperCase().contains("MYSQL"))
            return new MysqlProduct();
        else if (dbName.toUpperCase().contains("ORACLE"))
            return new OracleProduct();
        else if (dbName.toUpperCase().contains("POSTGRES"))
            return new PostgresProduct();
        else
            //Unsupported databases may execute errors
            return new CommonProduct();
    }
}

class CommonProduct extends DbProduct {
    @Override
    public String wrapDate(Object dateVal) {
        return "date'" + dateVal + "'";
    }
}
