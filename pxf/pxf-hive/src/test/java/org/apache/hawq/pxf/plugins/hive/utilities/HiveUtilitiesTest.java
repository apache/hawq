package org.apache.hawq.pxf.plugins.hive.utilities;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Test;

import org.apache.hawq.pxf.api.Metadata;
import org.apache.hawq.pxf.api.UnsupportedTypeException;

public class HiveUtilitiesTest {

    FieldSchema hiveColumn;
    Metadata.Table tblDesc;

    static String[][] typesMappings = {
        /* hive type -> hawq type */
        {"tinyint", "int2"},
        {"smallint", "int2"},
        {"int", "int4"},
        {"bigint", "int8"},
        {"boolean", "bool"},
        {"float", "float4"},
        {"double", "float8"},
        {"string", "text"},
        {"binary", "bytea"},
        {"timestamp", "timestamp"},
        {"date", "date"},
    };

    static String[][] typesWithModifiers = {
        {"decimal(19,84)", "numeric", "19,84"},
        {"varchar(13)", "varchar", "13"},
        {"char(40)", "bpchar", "40"},
    };

    @Test
    public void mapHiveTypeUnsupported() throws Exception {

        hiveColumn = new FieldSchema("complex", "array", null);

        try {
            HiveUtilities.mapHiveType(hiveColumn);
            fail("unsupported type");
        } catch (UnsupportedTypeException e) {
            assertEquals("HAWQ does not support type " + hiveColumn.getType() + " (Field " + hiveColumn.getName() + ")",
                    e.getMessage());
        }
    }

    @Test
    public void mapHiveTypeSimple() throws Exception {
        /*
         * tinyint -> int2
         * smallint -> int2
         * int -> int4
         * bigint -> int8
         * boolean -> bool
         * float -> float4
         * double -> float8
         * string -> text
         * binary -> bytea
         * timestamp -> timestamp
         * date -> date
         */
        for (String[] line: typesMappings) {
            String hiveType = line[0];
            String expectedType = line[1];
            hiveColumn = new FieldSchema("field" + hiveType, hiveType, null);
            Metadata.Field result = HiveUtilities.mapHiveType(hiveColumn);
            assertEquals("field" + hiveType, result.getName());
            assertEquals(expectedType, result.getType());
            assertNull(result.getModifiers());
        }
    }

    @Test
    public void mapHiveTypeWithModifiers() throws Exception {
        /*
         * decimal -> numeric
         * varchar -> varchar
         * char -> bpchar
         */
        for (String[] line: typesWithModifiers) {
            String hiveType = line[0];
            String expectedType = line[1];
            String modifiersStr = line[2];
            String[] expectedModifiers = modifiersStr.split(",");
            hiveColumn = new FieldSchema("field" + hiveType, hiveType, null);
            Metadata.Field result = HiveUtilities.mapHiveType(hiveColumn);
            assertEquals("field" + hiveType, result.getName());
            assertEquals(expectedType, result.getType());
            assertArrayEquals(expectedModifiers, result.getModifiers());
        }
    }

    @Test
    public void mapHiveTypeWithModifiersNegative() throws Exception {

        String badHiveType = "decimal(2)";
        hiveColumn = new FieldSchema("badNumeric", badHiveType, null);
        try {
            HiveUtilities.mapHiveType(hiveColumn);
            fail("should fail with bad numeric type error");
        } catch (UnsupportedTypeException e) {
            String errorMsg = "HAWQ does not support type " + badHiveType + " (Field badNumeric), " +
                "expected type of the form <type name>(<parameter>,<parameter>)";
            assertEquals(errorMsg, e.getMessage());
        }

        badHiveType = "char(1,2,3)";
        hiveColumn = new FieldSchema("badChar", badHiveType, null);
        try {
            HiveUtilities.mapHiveType(hiveColumn);
            fail("should fail with bad char type error");
        } catch (UnsupportedTypeException e) {
            String errorMsg = "HAWQ does not support type " + badHiveType + " (Field badChar), " +
                "expected type of the form <type name>(<parameter>)";
            assertEquals(errorMsg, e.getMessage());
        }

        badHiveType = "char(acter)";
        hiveColumn = new FieldSchema("badModifier", badHiveType, null);
        try {
            HiveUtilities.mapHiveType(hiveColumn);
            fail("should fail with bad modifier error");
        } catch (UnsupportedTypeException e) {
            String errorMsg = "HAWQ does not support type " + badHiveType + " (Field badModifier), " +
                "modifiers should be integers";
            assertEquals(errorMsg, e.getMessage());
        }
    }

    @Test
    public void parseTableQualifiedNameNoDbName() throws Exception {
        String name = "orphan";
        tblDesc = HiveUtilities.parseTableQualifiedName(name);

        assertEquals("default", tblDesc.getDbName());
        assertEquals(name, tblDesc.getTableName());
    }

    @Test
    public void parseTableQualifiedName() throws Exception {
        String name = "not.orphan";
        tblDesc = HiveUtilities.parseTableQualifiedName(name);

        assertEquals("not", tblDesc.getDbName());
        assertEquals("orphan", tblDesc.getTableName());
    }

    @Test
    public void parseTableQualifiedNameTooManyQualifiers() throws Exception {
        String name = "too.many.parents";
        String errorMsg = surroundByQuotes(name) + " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";

        parseTableQualifiedNameNegative(name, errorMsg, "too many qualifiers");
    }

    @Test
    public void parseTableQualifiedNameEmpty() throws Exception {
        String name = "";
        String errorMsg = "empty string is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";

        parseTableQualifiedNameNegative(name, errorMsg, "empty string");

        name = null;
        parseTableQualifiedNameNegative(name, errorMsg, "null string");

        name = ".";
        errorMsg = surroundByQuotes(name) + " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";
        parseTableQualifiedNameNegative(name, errorMsg, "empty db and table names");

        name = " . ";
        errorMsg = surroundByQuotes(name) + " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";
        parseTableQualifiedNameNegative(name, errorMsg, "only white spaces in string");
    }

    private String surroundByQuotes(String str) {
        return "\"" + str + "\"";
    }

    private void parseTableQualifiedNameNegative(String name, String errorMsg, String reason) throws Exception {
        try {
            tblDesc = HiveUtilities.parseTableQualifiedName(name);
            fail("test should fail because of " + reason);
        } catch (IllegalArgumentException e) {
            assertEquals(errorMsg, e.getMessage());
        }
    }
}
