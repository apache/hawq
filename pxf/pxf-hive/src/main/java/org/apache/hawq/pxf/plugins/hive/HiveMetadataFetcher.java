package org.apache.hawq.pxf.plugins.hive;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import org.apache.hawq.pxf.api.Metadata;
import org.apache.hawq.pxf.api.MetadataFetcher;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;

/**
 * Class for connecting to Hive's MetaStore and getting schema of Hive tables.
 */
public class HiveMetadataFetcher extends MetadataFetcher {

    private static final Log LOG = LogFactory.getLog(HiveMetadataFetcher.class);
    private HiveMetaStoreClient client;

    public HiveMetadataFetcher() {
        super();

        // init hive metastore client connection.
        client = HiveUtilities.initHiveClient();
    }

    @Override
    public Metadata getTableMetadata(String tableName) throws Exception {

        Metadata.Table tblDesc = HiveUtilities.parseTableQualifiedName(tableName);
        Metadata metadata = new Metadata(tblDesc);

        Table tbl = HiveUtilities.getHiveTable(client, tblDesc);

        getSchema(tbl, metadata);

        return metadata;
    }


    /**
     * Populates the given metadata object with the given table's fields and partitions,
     * The partition fields are added at the end of the table schema.
     * Throws an exception if the table contains unsupported field types.
     *
     * @param tbl Hive table
     * @param metadata schema of given table
     */
    private void getSchema(Table tbl, Metadata metadata) {

        int hiveColumnsSize = tbl.getSd().getColsSize();
        int hivePartitionsSize = tbl.getPartitionKeysSize();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Hive table: " + hiveColumnsSize + " fields, " + hivePartitionsSize + " partitions.");
        }

        // check hive fields
        try {
            List<FieldSchema> hiveColumns = tbl.getSd().getCols();
            for (FieldSchema hiveCol : hiveColumns) {
                metadata.addField(HiveUtilities.mapHiveType(hiveCol));
            }
            // check partition fields
            List<FieldSchema> hivePartitions = tbl.getPartitionKeys();
            for (FieldSchema hivePart : hivePartitions) {
                metadata.addField(HiveUtilities.mapHiveType(hivePart));
            }
        } catch (UnsupportedTypeException e) {
            String errorMsg = "Failed to retrieve metadata for table " + metadata.getTable() + ". " +
                    e.getMessage();
            throw new UnsupportedTypeException(errorMsg);
        }
    }
}
