package org.apache.hawq.pxf.api.io;

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