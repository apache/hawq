package com.pivotal.pxf.api.io;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum DataType {
    BOOLEAN(16),
    BYTEA(17),
    CHAR(18),
    BIGINT(20),
    SMALLINT(21),
    INTEGER(23),
    TEXT(25),
    REAL(700),
    FLOAT8(701),
    BPCHAR(1042),
    VARCHAR(1043),
    DATE(1082),
    TIME(1083),
    TIMESTAMP(1114),
    NUMERIC(1700),
    UN_SUPPORTED_TYPE(-1);


    private static final Map<Integer, DataType> lookup = new HashMap<Integer, DataType>();

    static {
        for (DataType dt : EnumSet.allOf(DataType.class)) {
            lookup.put(dt.getOID(), dt);
        }
    }

    private final int OID;

    DataType(int OID) {
        this.OID = OID;
    }

    public static DataType get(int OID) {
        DataType type = lookup.get(OID);
        return type == null
                ? UN_SUPPORTED_TYPE
                : type;
    }

    public int getOID() {
        return OID;
    }
}