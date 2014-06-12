package com.pivotal.pxf.core.rest.resources;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.core.FragmenterFactory;
import com.pivotal.pxf.core.FragmentsResponseFormatter;
import com.pivotal.pxf.core.utilities.ProtocolData;
import com.pivotal.pxf.core.utilities.SecuredHDFS;
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
import java.util.List;
import java.util.Map;

/*
 * Class enhances the API of the WEBHDFS REST server.
 * Returns the data fragments that a data resource is made of, enabling parallel processing of the data resource.
 * Example for querying API FRAGMENTER from a web client
 * curl -i "http://localhost:50070/gpdb/v2/Fragmenter?path=/dir1/dir2/*txt"
 * /gpdb/ is made part of the path when this package is registered in the jetty servlet
 * in NameNode.java in the hadoop package - /hadoop-core-X.X.X.jar
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Fragmenter/")
public class FragmenterResource extends RestResource {
    private Log Log;

    public FragmenterResource() throws IOException {
        Log = LogFactory.getLog(FragmenterResource.class);
    }

    /*
     * The function is called when http://nn:port/gpdb/vx/Fragmenter/getFragments?path=...
     * is used
     *
     * @param servletContext Servlet context contains attributes required by SecuredHDFS
     * @param headers Holds HTTP headers from request
     * @param path Holds URI path option used in this request
     */
    @GET
    @Path("getFragments")
    @Produces("application/json")
    public Response getFragments(@Context final ServletContext servletContext,
                                 @Context final HttpHeaders headers,
                                 @QueryParam("path") final String path) throws Exception {

        if (Log.isDebugEnabled()) {
            StringBuilder startMsg = new StringBuilder("FRAGMENTER started for path \"" + path + "\"");
            for (String header : headers.getRequestHeaders().keySet()) {
                startMsg.append(" Header: ").append(header).append(" Value: ").append(headers.getRequestHeader(header));
            }
            Log.debug(startMsg);
        }

		/* Convert headers into a case-insensitive regular map */
        Map<String, String> params = convertToCaseInsensitiveMap(headers.getRequestHeaders());
        
        /* Store protocol level properties and verify */
        ProtocolData protData = new ProtocolData(params);
        if (protData.getFragmenter() == null) {
        	protData.protocolViolation("fragmenter");
        }
        SecuredHDFS.verifyToken(protData, servletContext);

        /* Create a fragmenter instance with API level parameters */
        final Fragmenter fragmenter = FragmenterFactory.create(protData);

        List<Fragment> fragments = fragmenter.getFragments();
        String jsonOutput = FragmentsResponseFormatter.formatResponseString(fragments, path);

        return Response.ok(jsonOutput, MediaType.APPLICATION_JSON_TYPE).build();
    }
}
