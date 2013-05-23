package com.pivotal.pxf.rest.resources;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.HashMap;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.analyzers.AnalyzerFactory;
import com.pivotal.pxf.analyzers.IAnalyzer;
import com.pivotal.pxf.utilities.BaseMetaData;

/*
 * Class enhances the API of the WEBHDFS REST server.
 * Returns the data fragments that a data resource is made of, enabling parallel processing of the data resource.
 * Example for querying API ANALYZER from a web client
 * curl -i "http://localhost:50070/gpdb/v2/Analyzer/getEstimatedStats?path=/dir1/dir2/*txt"
 * /gpdb/ is made part of the path when this package is registered in the jetty servlet
 * in NameNode.java in the hadoop package - /hadoop-core-X.X.X.jar
 */
@Path("/" + Version.PXF_VERSION + "/Analyzer/")
public class AnalyzerResource
{
	org.apache.hadoop.fs.Path path = null;
	private Log Log;

	
	public AnalyzerResource() throws IOException
	{ 
		Log = LogFactory.getLog(AnalyzerResource.class);
	}

	/*
	 * Returns estimated statistics for the given path (data source).
	 * Example for querying API ANALYZER from a web client
	 * curl -i "http://localhost:50070/gpdb/v2/Analyzer/getEstimatedStats?path=/dir1/dir2/*txt"
	 * A default answer, unless an analyzer implements GetEstimatedStats, would be:
	 * {"GPXFDataSourceStats":[{"blockSize":67108864,"numberOfBlocks":1000,"numberOfTuples":1000000}]}
	 * Currently only HDFS is implemented to calculate the block size and block number, 
	 * and returns -1 for number of tuples.
	 * Example:
	 * {"GPXFDataSourceStats":[{"blockSize":67108864,"numberOfBlocks":3,"numberOfTuples":-1}]}
	 */
	@GET
	@Path("getEstimatedStats")
	@Produces("application/json")
	public Response getEstimatedStats(@Context HttpHeaders headers,
			                 @QueryParam("path") String path) throws Exception
	{
	                  
		String startmsg = new String("ANALYZER/getEstimatedStats started for path \"" + path + "\"");
				
		if (headers != null) 
		{
			for (String header : headers.getRequestHeaders().keySet()) 
				startmsg += " Header: " + header + " Value: " + headers.getRequestHeader(header);
		}
		
		Log.debug(startmsg);
				  
		/* Convert headers into a regular map */
		Map<String, String> params = convertToRegularMap(headers.getRequestHeaders());
		
		/*
		 * Here - in AnalyzerResource.getEstimatedStats() - we implement the policy that the analyzer should process a single block
		 * when calculating the tuples number. All data sources provided will have a split number 0.
		 * X-GP-DATA-FRAGMENTS is set in the same manner in BridgeResource - there the splits are provided by
		 * the GP segment on the URI string
		 */
		params.put("X-GP-DATA-FRAGMENTS", "0");
		
		final IAnalyzer analyzer = AnalyzerFactory.create(new BaseMetaData(params));
		final String datapath = new String(path);
		
		StreamingOutput streaming = new StreamingOutput()
		{
			/*
			 * Function queries the gpxf Analyzer for the data fragments of the resource
			 * The fragments are returned in a string formatted in JSON	 
			 */			
			@Override
			public void write(final OutputStream out) throws IOException
			{
				DataOutputStream dos = new DataOutputStream(out);
				
				try
				{
					dos.writeBytes(analyzer.GetEstimatedStats(datapath));
				} 
				catch (org.eclipse.jetty.io.EofException e)
				{
					Log.error("Remote connection closed by GPDB", e);
				}
				catch (Exception e)
				{
					// API does not allow throwing Exception so need to convert to something
					// I can throw without declaring...
					Log.error("Exception thrown streaming", e);
					throw new RuntimeException(e);
				}				
			}
		};
		
		return Response.ok( streaming, MediaType.APPLICATION_OCTET_STREAM ).build();
	}
	
	Map<String, String> convertToRegularMap(MultivaluedMap<String, String> multimap)
	{
		Map<String, String> result = new HashMap<String, String>();
		for (String key : multimap.keySet())
		{
			String newKey = key;
			if (key.startsWith("X-GP-"))
				newKey = key.toUpperCase();
			result.put(newKey, multimap.getFirst(key).replace("\\\"", "\""));
		}
		return result;
	}
}
