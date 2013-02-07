package com.emc.greenplum.gpdb.rest.resources;

import java.io.IOException;
import java.io.OutputStream;
import java.io.DataOutputStream;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * Class enhances the API of the HBASE rest server.
 * Example for querying API getClusterNodesInfo from a web client
 * curl http://localhost:8080/gpdb/v2/HadoopCluster/getNodesInfo
 * /gpdb/ is made part of the path when this package is registered in the jetty servlet
 * in Main.java in the hbase package - hbase-x.xx.x-sc.jar
 */
@Path( "/v2/HadoopCluster/" )
public class ClusterNodesResource
{
	private Log Log;
	public ClusterNodesResource() throws IOException
	{
		Log = LogFactory.getLog(ClusterNodesResource.class);
	}
	
	@GET
	@Path( "getNodesInfo" )
	@Produces("application/json")
	public Response read () throws Exception
	{
		Log.debug("getNodesInfo started");
		StreamingOutput streaming = new StreamingOutput()
		{
			/*
			 * Function queries the Hadoop namenode with the getDataNodeStats API
			 * It gets the hostname and REST port of every HDFS data node in the 
			 * cluster. Then, it packs the results in JSON format and writes to the
			 * HTTP response stream.
			 * Response Examples:
			 * a. When there are no datanodes - getDataNodeStats returns an emty array
			 *    {"regions":[]}
			 * b. When there are datanodes 
			 *    {"regions":[{"host":"host_name1","port":50075},{"host":"host_name2","port":50075}]}
			 */
			@Override
			public void write(final OutputStream out ) throws IOException
			{
				try
				{
					/* 1. Initialize the HADOOP client side API for a distributed file system */
					DatanodeInfo[] nodes      = null;
					DataOutputStream dos      = null; 
					Configuration conf        = new Configuration();
					FileSystem fs             = FileSystem.get(conf);
					DistributedFileSystem dfs = (DistributedFileSystem)fs;
					
					/* 2. Query the namenode for the datanodes info */
					nodes = dfs.getDataNodeStats();
					
					/* 3. Pack the datanodes info in a JASON text format and write it 
					 *    to the HTTP output stream.
					 */
					dos = new DataOutputStream(out);
					dos.writeBytes("{\"regions\":[");
					for (int i = 0; i < nodes.length; i++)
					{
						writeNode(dos, nodes[i]); // write one node to the HTTP stream 
						if (i < (nodes.length - 1))
							dos.writeBytes(","); // Separate the nodes in the JASON array with ','
					}
					dos.writeBytes("]}");
				}
				catch (org.mortbay.jetty.EofException e)
				{
					Log.error("Remote connection closed by GPDB", e);
					throw e;
				}
				catch (java.io.IOException e)
				{
					Log.error("Unhandled exception thrown", e);
					throw e;
				}
			}
		};
		
		return Response.ok( streaming, MediaType.APPLICATION_OCTET_STREAM ).build();
	}

	void writeNode(DataOutputStream d, DatanodeInfo node) throws java.io.IOException
	{
		d.writeBytes("{\"host\":\"");
		d.writeBytes(node.getHostName());
		d.writeBytes("\",\"port\":");
		String port = Integer.toString(node.getInfoPort());
		d.writeBytes(port);
		d.writeBytes("}");		
	}
}
