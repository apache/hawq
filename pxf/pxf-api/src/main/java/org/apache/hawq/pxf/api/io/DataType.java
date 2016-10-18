package org.apache.hawq.pxf.api.io;

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


import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Supported Data Types and OIDs (HAWQ Data Type identifiers).
 * There's a one-to-one match between a Data Type and it's corresponding OID.
 */
public enum DataType {
    BOOLEAN(16),
    BYTEA(17),
    BIGINT(20),
    SMALLINT(21),
    INTEGER(23),
    TEXT(25),
    REAL(700),
    FLOAT8(701),
    /**
     * char(length), blank-padded string, fixed storage length
     */
    BPCHAR(1042),
    /**
     * varchar(length), non-blank-padded string, variable storage length
     */
    VARCHAR(1043),
    DATE(1082),
    TIME(1083),
    TIMESTAMP(1114),
    NUMERIC(1700),
    UNSUPPORTED_TYPE(-1);

    private static final Map<Integer, DataType> lookup = new HashMap<>();

    static {
        for (DataType dt : EnumSet.allOf(DataType.class)) {
            lookup.put(dt.getOID(), dt);
        }
    }

    private final int OID;

    DataType(int OID) {
        this.OID = OID;
    }

    /**
     * Utility method for converting an {@link #OID} to a {@link #DataType}.
     *
     * @param OID the oid to be converted
     * @return the corresponding DataType if exists, else returns {@link #UNSUPPORTED_TYPE}
     */
    public static DataType get(int OID) {
        DataType type = lookup.get(OID);
        return type == null
                ? UNSUPPORTED_TYPE
                : type;
    }

    public int getOID() {
        return OID;
    }
}
