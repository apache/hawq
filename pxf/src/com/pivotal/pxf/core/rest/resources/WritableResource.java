package com.pivotal.pxf.core.rest.resources;

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.pivotal.pxf.core.bridge.Bridge;
import com.pivotal.pxf.core.bridge.WriteBridge;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.core.utilities.SecuredHDFS;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * Running this resource manually:
 *
 * run: 
 	curl -i -X post "http://localhost:50070/gpdb/v5w/Writable/stream?path=/data/curl/curl`date \"+%h%d_%H%M%s\"`" \
 	--header "X-GP-Accessor: TextFileWAccessor" \
 	--header "X-GP-Resolver: TextWResolver" \
 	--header "Content-Type:application/octet-stream" \
 	--header "Expect: 100-continue" \
  	--header "X-GP-ALIGNMENT: 4" \
 	--header "X-GP-SEGMENT-ID: 0" \
 	--header "X-GP-SEGMENT-COUNT: 3" \
 	--header "X-GP-HAS-FILTER: 0" \
 	--header "X-GP-FORMAT: TEXT" \
 	--header "X-GP-URI: pxf://localhost:50070/data/curl/?Accessor=TextFileWAccessor&Resolver=TextWResolver" \
 	--header "X-GP-URL-HOST: localhost" \
 	--header "X-GP-URL-PORT: 50070" \
 	--header "X-GP-ATTRS: 0" \
 	--header "X-GP-DATA-DIR: data/curl/" \
 	  -d "data111" -d "data222"

 * 	result:

  	HTTP/1.1 200 OK
	Content-Type: text/plain;charset=UTF-8
	Content-Type: text/plain
	Transfer-Encoding: chunked
	Server: Jetty(7.6.10.v20130312)

	wrote 15 bytes to curlAug11_17271376231245

	file content:
	bin/hdfs dfs -cat /data/curl/*45 
	data111&data222

 */


/*
 * This class handles the subpath /<version>/Writable/ of this
 * REST component
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Writable/")
public class WritableResource
{
	private static final Log LOG = LogFactory.getLog(WritableResource.class);
	
	public WritableResource() {}

	/*
 	run: 
 	curl -i -X post "http://localhost:50070/gpdb/v5w/Writable/stream?path=/data/curl/curl`date \"+%h%d_%H%M%s\"`" \
 	--header "X-GP-Accessor: TextFileWAccessor" \
 	--header "X-GP-Resolver: TextWResolver" \
 	--header "Content-Type:application/octet-stream" \
 	--header "Expect: 100-continue" \
  	--header "X-GP-ALIGNMENT: 4" \
 	--header "X-GP-SEGMENT-ID: 0" \
 	--header "X-GP-SEGMENT-COUNT: 3" \
 	--header "X-GP-HAS-FILTER: 0" \
 	--header "X-GP-FORMAT: TEXT" \
 	--header "X-GP-URI: pxf://localhost:50070/data/curl/?Accessor=TextFileWAccessor&Resolver=TextWResolver" \
 	--header "X-GP-URL-HOST: localhost" \
 	--header "X-GP-URL-PORT: 50070" \
 	--header "X-GP-ATTRS: 0" \
 	--header "X-GP-DATA-DIR: data/curl/" \
 	  -d "data111" -d "data222"

  	result:

  	HTTP/1.1 200 OK
	Content-Type: text/plain;charset=UTF-8
	Content-Type: text/plain
	Transfer-Encoding: chunked
	Server: Jetty(7.6.10.v20130312)

	wrote 15 bytes to curlAug11_17271376231245

	file content:
	bin/hdfs dfs -cat /data/curl/*45 
	data111&data222
	 */
	@POST
	@Path("stream")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response stream(@Context final ServletContext servletContext,
						   @Context HttpHeaders headers, 
			               @QueryParam("path") String path,
			               InputStream inputStream) throws Exception {	
		
		// Convert headers into a regular map
		Map<String, String> params = convertToRegularMap(headers.getRequestHeaders());
		params.put("X-GP-DATA-PATH", path);
		LOG.debug("WritableResource started with parameters: " + params);
		
		InputData inputData = new InputData(params);
        SecuredHDFS.verifyToken(inputData, servletContext);
		Bridge bridge = new WriteBridge(inputData);

		// THREAD-SAFE parameter has precedence 
		boolean isThreadSafe = inputData.threadSafe() && bridge.isThreadSafe();
        LOG.debug("Request for " + path + " handled " +
				  (isThreadSafe ? "without" : "with") + " synchronization"); 
		
		return isThreadSafe ? 
				writeResponse(bridge, path, inputStream) : 
				synchronizedWriteResponse(bridge, path, inputStream);
	}
	
	private static synchronized Response synchronizedWriteResponse(Bridge bridge,
																   String path,
			                                                       InputStream inputStream) 
			                                                    		   throws Exception {
		return writeResponse(bridge, path, inputStream);
	}	
	
	private static Response writeResponse(Bridge bridge,
										  String path,
			                              InputStream inputStream) throws Exception {
		
		String returnMsg;
		
		// Open the output file	
		bridge.beginIteration();
		
		DataInputStream dataStream = new DataInputStream(inputStream);
		
		long totalRead = 0;

		try
		{
			while (bridge.setNext(dataStream))
			{
				++totalRead;
			}
		}
		catch (org.eclipse.jetty.io.EofException e)
		{
            LOG.debug("EofException timeout: connection closed on client side");
		}
		catch (Exception ex)
		{
            LOG.debug("totalRead so far " + totalRead + " to " + path);
			throw ex;
		}
		finally 
		{
			inputStream.close();
		}
		returnMsg = "wrote " + totalRead + " bulks to " + path;
        LOG.debug(returnMsg);
		
		return Response.ok(returnMsg).build();
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
