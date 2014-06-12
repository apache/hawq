package com.pivotal.pxf.service.rest;

import com.pivotal.pxf.api.Analyzer;
import com.pivotal.pxf.api.AnalyzerStats;
import com.pivotal.pxf.service.AnalyzerFactory;
import com.pivotal.pxf.service.utilities.ProtocolData;
import com.pivotal.pxf.service.utilities.SecuredHDFS;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;

/*
 * Class enhances the API of the WEBHDFS REST server.
 * Returns the data fragments that a data resource is made of, enabling parallel processing of the data resource.
 * Example for querying API ANALYZER from a web client
 * curl -i "http://localhost:50070/pxf/v2/Analyzer/getEstimatedStats?path=/dir1/dir2/*txt"
 * /pxf/ is made part of the path when there is a webapp by that name in tcServer.
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Analyzer/")
public class AnalyzerResource extends RestResource {
    private Log Log;


    public AnalyzerResource() throws IOException {
        Log = LogFactory.getLog(AnalyzerResource.class);
    }

    /*
     * Returns estimated statistics for the given path (data source).
     * Example for querying API ANALYZER from a web client
     * curl -i "http://localhost:50070/pxf/v2/Analyzer/getEstimatedStats?path=/dir1/dir2/*txt"
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
                                      @QueryParam("path") String path) throws Exception {

        if (Log.isDebugEnabled()) {
            StringBuilder startmsg = new StringBuilder("ANALYZER/getEstimatedStats started for path \"" + path + "\"");
            if (headers != null) {
                for (String header : headers.getRequestHeaders().keySet()) {
                    startmsg.append(" Header: ").append(header).append(" Value: ").append(headers.getRequestHeader(header));
                }
            }
            Log.debug(startmsg);
        }

		/* Convert headers into a regular map */
        Map<String, String> params = convertToCaseInsensitiveMap(headers.getRequestHeaders());

        /* Store protocol level properties and verify */
        final ProtocolData protData = new ProtocolData(params);
        SecuredHDFS.verifyToken(protData, servletContext);
        
        /*
         * Analyzer is a special case in which it is hard to tell if user didn't
         * specify one, or specified a profile that doesn't include one, or it's
         * an actual protocol violation. Since we can only test protocol level
         * logic, we assume (like before) that it's a user error, which is the
         * case in most likelihood. When analyzer module is removed in the near
         * future, this assumption will go away with it.
         */
        if (protData.getAnalyzer() == null) {
			throw new IllegalArgumentException(
					"PXF 'Analyzer' class was not found. Please supply it in the LOCATION clause or use it in a PXF profile in order to run ANALYZE on this table");
        }
        
        /* Create an analyzer instance with API level parameters */
        final Analyzer analyzer = AnalyzerFactory.create(protData);

		/*
         * Function queries the pxf Analyzer for the data fragments of the resource
		 * The fragments are returned in a string formatted in JSON	 
		 */
        String jsonOutput = AnalyzerStats.dataToJSON(analyzer.getEstimatedStats(path));

        return Response.ok(jsonOutput, MediaType.APPLICATION_JSON_TYPE).build();
    }
}
