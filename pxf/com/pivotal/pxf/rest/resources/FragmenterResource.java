package com.pivotal.pxf.rest.resources;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

import com.pivotal.pxf.fragmenters.Fragmenter;
import com.pivotal.pxf.fragmenters.FragmenterFactory;
import com.pivotal.pxf.fragmenters.FragmentsOutput;
import com.pivotal.pxf.fragmenters.FragmentsResponseFormatter;
import com.pivotal.pxf.utilities.BaseMetaData;

/*
 * Class enhances the API of the WEBHDFS REST server.
 * Returns the data fragments that a data resource is made of, enabling parallel processing of the data resource.
 * Example for querying API FRAGMENTER from a web client
 * curl -i "http://localhost:50070/gpdb/v2/Fragmenter?path=/dir1/dir2/*txt"
 * /gpdb/ is made part of the path when this package is registered in the jetty servlet
 * in NameNode.java in the hadoop package - /hadoop-core-X.X.X.jar
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Fragmenter/")
public class FragmenterResource
{
	org.apache.hadoop.fs.Path path = null;
	private Log Log;
	
	public FragmenterResource() throws IOException
	{ 
		Log = LogFactory.getLog(FragmenterResource.class);
	}
	
	@GET
	@Path("getFragments")
	@Produces("application/json")
	public Response getFragments(@Context HttpHeaders headers,
						  		  @QueryParam("path") String path) throws Exception
	{
	
		String startmsg = new String("FRAGMENTER started for path \"" + path + "\"");
				
		if (headers != null) 
		{
			for (String header : headers.getRequestHeaders().keySet()) 
				startmsg += " Header: " + header + " Value: " + headers.getRequestHeader(header);
		}
		
		Log.debug(startmsg);
				  
		/* Convert headers into a regular map */
		Map<String, String> params = convertToRegularMap(headers.getRequestHeaders());
		final Fragmenter fragmenter = FragmenterFactory.create(new BaseMetaData(params));

		FragmentsOutput fragments = fragmenter.GetFragments(path);
		String jsonOutput = FragmentsResponseFormatter.formatResponseString(fragments, path);		
		
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
