package com.pivotal.pxf.plugins.hbase.utilities;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseUtilities {

    /**
     * Initializes HBase configuration.
     * The following parameters are edited:
     *
     * hbase.client.retries.number = 1
     *  - tries to connect to HBase only 2 times before failing.
     *
     * @return HBase configuration
     */
    public static Configuration initHBaseConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.client.retries.number", "3");
        return conf;
    }

    /**
     * Returns if given table exists and is enabled.
     *
     * @param hbaseAdmin HBase admin, must be initialized
     * @param tableName table name
     * @return true if table exists
     * @throws IOException
     */
    public static boolean isTableAvailable(HBaseAdmin hbaseAdmin, String tableName) throws IOException {
        return hbaseAdmin.isTableAvailable(tableName) &&
                hbaseAdmin.isTableEnabled(tableName);
    }
}
