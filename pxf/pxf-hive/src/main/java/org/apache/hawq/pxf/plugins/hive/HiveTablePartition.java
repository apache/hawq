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
import java.util.Properties;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

/**
 * A Hive table unit - means a subset of the HIVE table, where we can say
 * that for all files in this subset, they all have the same InputFormat and
 * Serde. For a partitioned table the HiveTableUnit will be one partition
 * and for an unpartitioned table, the HiveTableUnit will be the whole table
 */
public class HiveTablePartition {
    public StorageDescriptor storageDesc;
    public Properties properties;
    public Partition partition;
    public List<FieldSchema> partitionKeys;
    public String tableName;

    HiveTablePartition(StorageDescriptor storageDesc,
                       Properties properties, Partition partition,
                       List<FieldSchema> partitionKeys, String tableName) {
        this.storageDesc = storageDesc;
        this.properties = properties;
        this.partition = partition;
        this.partitionKeys = partitionKeys;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "table - " + tableName
                + ((partition == null) ? "" : ", partition - " + partition);
    }
}
