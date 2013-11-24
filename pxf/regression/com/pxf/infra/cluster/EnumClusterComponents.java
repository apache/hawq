package com.pxf.infra.cluster;

/**
 * Different Hadoop cluster components. The "cmd" is the script prefix for
 * interacting with the component (start, stop...)
 * 
 */
public enum EnumClusterComponents {

	All("gphd.sh"), HDFS("hdfs.sh"), HBASE("hbase.sh"), HIVE("hive.sh");

	private String cmd;

	private EnumClusterComponents(String cmd) {
		this.cmd = cmd;
	}

	public String getCmd() {
		return this.cmd;
	}
}
