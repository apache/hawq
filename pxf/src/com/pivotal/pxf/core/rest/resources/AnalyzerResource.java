package com.pivotal.pxf.core.rest.resources;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.api.Analyzer;
import com.pivotal.pxf.api.AnalyzerStats;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.core.AnalyzerFactory;
import com.pivotal.pxf.core.utilities.SecuredHDFS;

/*
 * Class enhances the API of the WEBHDFS REST server.
 * Returns the data fragments that a data resource is made of, enabling parallel processing of the data resource.
 * Example for querying API ANALYZER from a web client
 * curl -i "http://localhost:50070/gpdb/v2/Analyzer/getEstimatedStats?path=/dir1/dir2/*txt"
 * /gpdb/ is made part of the path when this package is registered in the jetty servlet
 * in NameNode.java in the hadoop package - /hadoop-core-X.X.X.jar
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Analyzer/")
public class AnalyzerResource
{
	private Log Log;

	
	public AnalyzerResource() throws IOException
	{ 
		Log = LogFactory.getLog(AnalyzerResource.class);
	}

	/*
	 * Returns estimated statistics for the given path (data source).
	 * Example for querying API ANALYZER from a web client
	 * curl -i "http://localhost:50070/gpdb/v2/Analyzer/getEstimatedStats?path=/dir1/dir2/*txt"
	 * A default answer, unless an analyzer implements getEstimatedStats, would be:
	 * {"PXFDataSourceStats":[{"blockSize":67108864,"numberOfBlocks":1000,"numberOfTuples":1000000}]}
	 * Currently only HDFS is implemented to calculate the block size and block number, 
	 * and returns -1 for number of tuples.
	 * Example:
	 * {"PXFDataSourceStats":[{"blockSize":67108864,"numberOfBlocks":3,"numberOfTuples":-1}]}
	 *
	 * @param servletContext Servlet context contains attributes required by SecuredHDFS
	 * @param headers Holds HTTP headers from request
	 * @param path Holds URI path option used in this request
	 */
	@GET
	@Path("getEstimatedStats")
	@Produces("application/json")
	public Response getEstimatedStats(@Context ServletContext servletContext,
									  @Context final HttpHeaders headers,
			 		                  @QueryParam("path") String path) throws Exception
	{

        if (Log.isDebugEnabled()) {
            StringBuilder startmsg = new StringBuilder("ANALYZER/getEstimatedStats started for path \"" + path + "\"");
            if (headers != null) {
                for (String header : headers.getRequestHeaders().keySet())
                    startmsg.append(" Header: ").append(header).append(" Value: ").append(headers.getRequestHeader(header));
            }
            Log.debug(startmsg);
        }

		/* Convert headers into a regular map */
		Map<String, String> params = convertToRegularMap(headers.getRequestHeaders());
		
		final InputData inputData = new InputData(params);
        SecuredHDFS.verifyToken(inputData, servletContext);
		final Analyzer analyzer = AnalyzerFactory.create(inputData);

		/*
		 * Function queries the pxf Analyzer for the data fragments of the resource
		 * The fragments are returned in a string formatted in JSON	 
		 */		
		String jsonOutput = AnalyzerStats.dataToJSON(analyzer.getEstimatedStats(path));
		
		return Response.ok(jsonOutput, MediaType.APPLICATION_JSON_TYPE).build();
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
