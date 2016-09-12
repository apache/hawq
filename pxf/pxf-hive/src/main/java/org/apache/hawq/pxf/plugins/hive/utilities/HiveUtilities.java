package org.apache.hawq.pxf.plugins.hive.utilities;

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


import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hawq.pxf.api.Metadata;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.utilities.EnumHawqType;
import org.apache.hawq.pxf.api.io.DataType;

/**
 * Class containing helper functions connecting
 * and interacting with Hive.
 */
public class HiveUtilities {

    private static final Log LOG = LogFactory.getLog(HiveUtilities.class);
    private static final String WILDCARD = "*";

    /**
     * Default Hive DB (schema) name.
     */
    private static final String HIVE_DEFAULT_DBNAME = "default";

    /**
     * Initializes the HiveMetaStoreClient
     * Uses classpath configuration files to locate the MetaStore
     *
     * @return initialized client
     */
    public static HiveMetaStoreClient initHiveClient() {
        HiveMetaStoreClient client = null;
        try {
            client = new HiveMetaStoreClient(new HiveConf());
        } catch (MetaException cause) {
            throw new RuntimeException("Failed connecting to Hive MetaStore service: " + cause.getMessage(), cause);
        }
        return client;
    }

    public static Table getHiveTable(HiveMetaStoreClient client, Metadata.Item itemName)
            throws Exception {
        Table tbl = client.getTable(itemName.getPath(), itemName.getName());
        String tblType = tbl.getTableType();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Item: " + itemName.getPath() + "." + itemName.getName() + ", type: " + tblType);
        }

        if (TableType.valueOf(tblType) == TableType.VIRTUAL_VIEW) {
            throw new UnsupportedOperationException("Hive views are not supported by HAWQ");
        }

        return tbl;
    }

    /**
     * Checks if hive type is supported, and if so return its matching HAWQ
     * type. Unsupported types will result in an exception. <br>
     * The supported mappings are:
     * <ul>
     * <li>{@code tinyint -> int2}</li>
     * <li>{@code smallint -> int2}</li>
     * <li>{@code int -> int4}</li>
     * <li>{@code bigint -> int8}</li>
     * <li>{@code boolean -> bool}</li>
     * <li>{@code float -> float4}</li>
     * <li>{@code double -> float8}</li>
     * <li>{@code string -> text}</li>
     * <li>{@code binary -> bytea}</li>
     * <li>{@code timestamp -> timestamp}</li>
     * <li>{@code date -> date}</li>
     * <li>{@code decimal(precision, scale) -> numeric(precision, scale)}</li>
     * <li>{@code varchar(size) -> varchar(size)}</li>
     * <li>{@code char(size) -> bpchar(size)}</li>
     * <li>{@code array<dataType> -> text}</li>
     * <li>{@code map<keyDataType, valueDataType> -> text}</li>
     * <li>{@code struct<field1:dataType,...,fieldN:dataType> -> text}</li>
     * <li>{@code uniontype<...> -> text}</li>
     * </ul>
     *
     * @param hiveColumn
     *            hive column schema
     * @return field with mapped HAWQ type and modifiers
     * @throws UnsupportedTypeException
     *             if the column type is not supported
     * @see EnumHiveToHawqType
     */
    public static Metadata.Field mapHiveType(FieldSchema hiveColumn) throws UnsupportedTypeException {
        String fieldName = hiveColumn.getName();
        String hiveType = hiveColumn.getType(); // Type name and modifiers if any
        String hiveTypeName; // Type name
        String[] modifiers = null; // Modifiers
        EnumHiveToHawqType hiveToHawqType = EnumHiveToHawqType.getHiveToHawqType(hiveType);
        EnumHawqType hawqType = hiveToHawqType.getHawqType();

        if (hiveToHawqType.getSplitExpression() != null) {
            String[] tokens = hiveType.split(hiveToHawqType.getSplitExpression());
            hiveTypeName = tokens[0];
            if (hawqType.getModifiersNum() > 0) {
                modifiers = Arrays.copyOfRange(tokens, 1, tokens.length);
                if (modifiers.length != hawqType.getModifiersNum()) {
                    throw new UnsupportedTypeException(
                            "HAWQ does not support type " + hiveType
                                    + " (Field " + fieldName + "), "
                                    + "expected number of modifiers: "
                                    + hawqType.getModifiersNum()
                                    + ", actual number of modifiers: "
                                    + modifiers.length);
                }
                if (!verifyIntegerModifiers(modifiers)) {
                    throw new UnsupportedTypeException("HAWQ does not support type " + hiveType + " (Field " + fieldName + "), modifiers should be integers");
                }
            }
        } else
            hiveTypeName = hiveType;

        return new Metadata.Field(fieldName, hawqType, hiveTypeName, modifiers);
    }

    /**
     * Verifies modifiers are null or integers.
     * Modifier is a value assigned to a type,
     * e.g. size of a varchar - varchar(size).
     *
     * @param modifiers type modifiers to be verified
     * @return whether modifiers are null or integers
     */
    private static boolean verifyIntegerModifiers(String[] modifiers) {
        if (modifiers == null) {
            return true;
        }
        for (String modifier: modifiers) {
            if (StringUtils.isBlank(modifier) || !StringUtils.isNumeric(modifier)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Extracts the db_name and table_name from the qualifiedName.
     * qualifiedName is the Hive table name that the user enters in the CREATE EXTERNAL TABLE statement
     * or when querying HCatalog table.
     * It can be either <code>table_name</code> or <code>db_name.table_name</code>.
     *
     * @param qualifiedName Hive table name
     * @return {@link Metadata.Item} object holding the full table name
     */
    public static Metadata.Item extractTableFromName(String qualifiedName) {
        List<Metadata.Item> items = extractTablesFromPattern(null, qualifiedName);
        if(items.isEmpty()) {
            throw new IllegalArgumentException("No tables found");
        }
        return items.get(0);
    }

    /**
     * Extracts the db_name(s) and table_name(s) corresponding to the given pattern.
     * pattern is the Hive table name or pattern that the user enters in the CREATE EXTERNAL TABLE statement
     * or when querying HCatalog table.
     * It can be either <code>table_name_pattern</code> or <code>db_name_pattern.table_name_pattern</code>.
     *
     * @param client Hivemetastore client
     * @param pattern Hive table name or pattern
     * @return list of {@link Metadata.Item} objects holding the full table name
     */
    public static List<Metadata.Item> extractTablesFromPattern(HiveMetaStoreClient client, String pattern) {

        String dbPattern, tablePattern;
        String errorMsg = " is not a valid Hive table name. "
                + "Should be either <table_name> or <db_name.table_name>";

        if (StringUtils.isBlank(pattern)) {
            throw new IllegalArgumentException("empty string" + errorMsg);
        }

        String[] rawToks = pattern.split("[.]");
        ArrayList<String> toks = new ArrayList<String>();
        for (String tok: rawToks) {
            if (StringUtils.isBlank(tok)) {
                continue;
            }
            toks.add(tok.trim());
        }

        if (toks.size() == 1) {
            dbPattern = HIVE_DEFAULT_DBNAME;
            tablePattern = toks.get(0);
        } else if (toks.size() == 2) {
            dbPattern = toks.get(0);
            tablePattern = toks.get(1);
        } else {
            throw new IllegalArgumentException("\"" + pattern + "\"" + errorMsg);
        }

        return getTablesFromPattern(client, dbPattern, tablePattern);
   }

    private static List<Metadata.Item> getTablesFromPattern(HiveMetaStoreClient client, String dbPattern, String tablePattern) {

        List<String> databases = null;
        List<Metadata.Item> itemList = new ArrayList<Metadata.Item>();
        List<String> tables = new ArrayList<String>();

        if(client == null || (!dbPattern.contains(WILDCARD) && !tablePattern.contains(WILDCARD)) ) {
            /* This case occurs when the call is invoked as part of the fragmenter api or when metadata is requested for a specific table name */
            itemList.add(new Metadata.Item(dbPattern, tablePattern));
            return itemList;
        }

        try {
            databases = client.getDatabases(dbPattern);
            if(databases.isEmpty()) {
                LOG.warn("No database found for the given pattern: " + dbPattern);
                return null;
            }
            for(String dbName: databases) {
                for(String tableName: client.getTables(dbName, tablePattern)) {
                    itemList.add(new Metadata.Item(dbName, tableName));
                }
            }
            return itemList;

        } catch (MetaException cause) {
            throw new RuntimeException("Failed connecting to Hive MetaStore service: " + cause.getMessage(), cause);
        }
    }


    /**
     * Converts HAWQ type to hive type.
     * @see EnumHiveToHawqType For supported mappings
     * @param type HAWQ data type
     * @param name field name
     * @return Hive type
     * @throws UnsupportedTypeException if type is not supported
     */
    public static String toCompatibleHiveType(DataType type, Integer[] modifiers) {

        EnumHiveToHawqType hiveToHawqType = EnumHiveToHawqType.getCompatibleHiveToHawqType(type);
        return EnumHiveToHawqType.getFullHiveTypeName(hiveToHawqType, modifiers);
    }



    /**
     * Validates whether given HAWQ and Hive data types are compatible.
     * If data type could have modifiers, HAWQ data type is valid if it hasn't modifiers at all
     * or HAWQ's modifiers are greater or equal to Hive's modifiers.
     * <p>
     * For example:
     * <p>
     * Hive type - varchar(20), HAWQ type varchar - valid.
     * <p>
     * Hive type - varchar(20), HAWQ type varchar(20) - valid.
     * <p>
     * Hive type - varchar(20), HAWQ type varchar(25) - valid.
     * <p>
     * Hive type - varchar(20), HAWQ type varchar(15) - invalid.
     *
     *
     * @param hawqDataType HAWQ data type
     * @param hawqTypeMods HAWQ type modifiers
     * @param hiveType full Hive type, i.e. decimal(10,2)
     * @param hawqColumnName Hive column name
     * @throws UnsupportedTypeException if types are incompatible
     */
    public static void validateTypeCompatible(DataType hawqDataType, Integer[] hawqTypeMods, String hiveType, String hawqColumnName) {

        EnumHiveToHawqType hiveToHawqType = EnumHiveToHawqType.getHiveToHawqType(hiveType);
        EnumHawqType expectedHawqType = hiveToHawqType.getHawqType();

        if (!expectedHawqType.getDataType().equals(hawqDataType)) {
            throw new UnsupportedTypeException("Invalid definition for column " + hawqColumnName
                                    +  ": expected HAWQ type " + expectedHawqType.getDataType() +
                    ", actual HAWQ type " + hawqDataType);
        }

        switch (hawqDataType) {
            case NUMERIC:
            case VARCHAR:
            case BPCHAR:
                if (hawqTypeMods != null && hawqTypeMods.length > 0) {
                    Integer[] hiveTypeModifiers = EnumHiveToHawqType
                            .extractModifiers(hiveType);
                    for (int i = 0; i < hiveTypeModifiers.length; i++) {
                        if (hawqTypeMods[i] < hiveTypeModifiers[i])
                            throw new UnsupportedTypeException(
                                    "Invalid definition for column " + hawqColumnName
                                            + ": modifiers are not compatible, "
                                            + Arrays.toString(hiveTypeModifiers) + ", "
                                            + Arrays.toString(hawqTypeMods));
                    }
                }
                break;
        }
    }
}
