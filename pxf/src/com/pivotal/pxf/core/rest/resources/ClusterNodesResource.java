package com.pivotal.pxf.core.rest.resources;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/*
 * Class enhances the API of the HBASE rest server.
 * Example for querying API getClusterNodesInfo from a web client
 * curl "http://localhost:50070/gpdb/v2/HadoopCluster/getNodesInfo"
 * /gpdb/ is made part of the path when this package is registered in the jetty servlet
 * in Main.java in the hbase package - hbase-x.xx.x-sc.jar
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/HadoopCluster/")
public class ClusterNodesResource
{
	private Log Log;
	public ClusterNodesResource() throws IOException
	{
		Log = LogFactory.getLog(ClusterNodesResource.class);
	}
	
	
	/*
	 * Function queries the Hadoop namenode with the getDataNodeStats API
	 * It gets the host's IP and REST port of every HDFS data node in the 
	 * cluster. Then, it packs the results in JSON format and writes to the
	 * HTTP response stream.
	 * Response Examples:
	 * a. When there are no datanodes - getDataNodeStats returns an empty array
	 *    {"regions":[]}
	 * b. When there are datanodes 
	 *    {"regions":[{"host":"1.2.3.1","port":50075},{"host":"1.2.3.2","port":50075}]}
	 */
	@GET
	@Path("getNodesInfo")
	@Produces("application/json")
	public Response read () throws Exception
	{
		Log.debug("getNodesInfo started");
		String jsonOutput;
		
		try
		{
			/* 1. Initialize the HADOOP client side API for a distributed file system */
			DatanodeInfo[] nodes      = null;
			Configuration conf        = new Configuration();
			FileSystem fs             = FileSystem.get(conf);
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			
			/* 2. Query the namenode for the datanodes info */
			nodes = dfs.getDataNodeStats();
			
			/* 3. Pack the datanodes info in a JSON text format and write it 
			 *    to the HTTP output stream.
			 */
			jsonOutput = "{\"regions\":[";
			
			for (int i = 0; i < nodes.length; i++)
			{
				verifyNode(nodes[i]);
				jsonOutput += writeNode(nodes[i]); // write one node to the HTTP stream 
				if (i < (nodes.length - 1))
					jsonOutput += ","; // Separate the nodes in the JSON array with ','
			}
			jsonOutput += "]}";
			Log.debug("getNodesCluster output: " + jsonOutput);
		}
		catch (NodeDataException e)
		{
			Log.error("Nodes verification failed", e);
			throw e;
		}
		catch (org.eclipse.jetty.io.EofException e)
		{
			Log.error("Remote connection closed by GPDB", e);
			throw e;
		}
		catch (java.io.IOException e)
		{
			Log.error("Unhandled exception thrown", e);
			throw e;
		}
		
		return Response.ok(jsonOutput, MediaType.APPLICATION_JSON_TYPE).build();
	}

	private class NodeDataException extends java.io.IOException {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public NodeDataException(String paramString)
		{
			super(paramString);
		} 
	}
	
	private void verifyNode(DatanodeInfo node) throws NodeDataException
	{
		int port = node.getInfoPort();
		String ip = node.getIpAddr();
		
		if ((ip == null) || (ip.length() == 0))
		{
			throw new NodeDataException("Invalid IP: " + ip + " (Node " + node.toString() + ")");
		}
		
		if (port <= 0)
		{
			throw new NodeDataException("Invalid port: " + port + " (Node " + node.toString() + ")");
		}
	}
	
	String writeNode(DatanodeInfo node) throws java.io.IOException
	{
		String output = "{\"host\":\"" + node.getIpAddr() +
				"\",\"port\":" + Integer.toString(node.getInfoPort()) + "}";
		return output;
	}
}
