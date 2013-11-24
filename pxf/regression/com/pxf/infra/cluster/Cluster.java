package com.pxf.infra.cluster;

/**
 * Define Cluster functionality
 */
public interface Cluster {

	public void startHiveServer() throws Exception;

	public void killHiveServer() throws Exception;

	public void start(EnumClusterComponents component) throws Exception;

	public void stop(EnumClusterComponents component) throws Exception;

	public boolean isUp(EnumClusterComponents component) throws Exception;

	public String getHost();

}
