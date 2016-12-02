package org.apache.hawq.pxf.plugins.hive;

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
