package com.pivotal.pxf.plugins.hbase.utilities;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

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
     * @throws IOException if a remote or network exception occurs when connecting to HBase
     */
    public static boolean isTableAvailable(Admin hbaseAdmin, String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        return hbaseAdmin.isTableAvailable(name) &&
                hbaseAdmin.isTableEnabled(name);
    }

    /**
     * Closes HBase admin and connection if they are open.
     *
     * @param hbaseAdmin HBase admin
     * @param hbaseConnection HBase connection
     * @throws IOException if an I/O error occurs when connecting to HBase
     */
    public static void closeConnection(Admin hbaseAdmin, Connection hbaseConnection) throws IOException {
        if (hbaseAdmin != null) {
            hbaseAdmin.close();
        }
        if (hbaseConnection != null) {
            hbaseConnection.close();
        }
    }
}
