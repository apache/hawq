package com.emc.greenplum.gpdb.hdfsconnector;

/*
 * Defines one field ion a deserialized record the type is in OID values recognized by GPDBWritable
 * and val is the actual field value
 */
public class OneField
{
	public int type;
	public Object val;
}

/*
 * the current protocols implemented in the package: HDFS file types and HBASE database
 */
enum BridgeProtocols
{
    GPHDFS /* default value */,
    GPHBASE;
}