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
     * Checks if hive type is supported, and if so
     * return its matching HAWQ type.
     * Unsupported types will result in an exception.
     * <br>
     * The supported mappings are:<ul>
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
     * </ul>
     *
     * @param hiveColumn hive column schema
     * @return field with mapped HAWQ type and modifiers
     * @throws UnsupportedTypeException if the column type is not supported
     */
    public static Metadata.Field mapHiveType(FieldSchema hiveColumn) throws UnsupportedTypeException {
        String fieldName = hiveColumn.getName();
        String hiveType = hiveColumn.getType();
        String mappedType;
        String[] modifiers = null;

        // check parameterized types:
        if (hiveType.startsWith("varchar(") ||
                hiveType.startsWith("char(")) {
            String[] toks = hiveType.split("[(,)]");
            if (toks.length != 2) {
                throw new UnsupportedTypeException( "HAWQ does not support type " + hiveType + " (Field " + fieldName + "), " +
                        "expected type of the form <type name>(<parameter>)");
            }
            mappedType = toks[0];
            if (mappedType.equals("char")) {
                mappedType = "bpchar";
            }
            modifiers = new String[] {toks[1]};
        } else if (hiveType.startsWith("decimal(")) {
            String[] toks = hiveType.split("[(,)]");
            if (toks.length != 3) {
                throw new UnsupportedTypeException( "HAWQ does not support type " + hiveType + " (Field " + fieldName + "), " +
                        "expected type of the form <type name>(<parameter>,<parameter>)");
            }
            mappedType = "numeric";
            modifiers = new String[] {toks[1], toks[2]};
        } else {

            switch (hiveType) {
            case "tinyint":
            case "smallint":
            	mappedType = "int2";
            	break;
            case "int":
            	mappedType = "int4";
            	break;
            case "bigint":
            	mappedType = "int8";
            	break;
            case "boolean":
            	mappedType = "bool";
            	break;
            case "timestamp":
            case "date":
                mappedType = hiveType;
                break;
            case "float":
                mappedType = "float4";
                break;
            case "double":
                mappedType = "float8";
                break;
            case "string":
                mappedType = "text";
                break;
            case "binary":
                mappedType = "bytea";
                break;
            default:
                throw new UnsupportedTypeException(
                        "HAWQ does not support type " + hiveType + " (Field " + fieldName + ")");
            }
        }
        if (!verifyModifers(modifiers)) {
            throw new UnsupportedTypeException("HAWQ does not support type " + hiveType + " (Field " + fieldName + "), modifiers should be integers");
        }
        return new Metadata.Field(fieldName, mappedType, modifiers);
    }

    /**
     * Verifies modifiers are null or integers.
     * Modifier is a value assigned to a type,
     * e.g. size of a varchar - varchar(size).
     *
     * @param modifiers type modifiers to be verified
     * @return whether modifiers are null or integers
     */
    private static boolean verifyModifers(String[] modifiers) {
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
}
