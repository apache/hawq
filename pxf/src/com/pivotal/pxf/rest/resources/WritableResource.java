package com.pivotal.pxf.rest.resources;

import java.io.DataInputStream;
import java.io.DataOutputStream;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.pivotal.pxf.accessors.IWriteAccessor;
import com.pivotal.pxf.bridge.IBridge;
import com.pivotal.pxf.bridge.WriteBridge;
import com.pivotal.pxf.resolvers.IWriteResolver;
import com.pivotal.pxf.utilities.InputData;

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
	IWriteAccessor fileAccessor = null;
	IWriteResolver fieldsResolver = null;
	private static Log Log = LogFactory.getLog(WritableResource.class);
	
	public WritableResource() {}

	/*
	 * run:

	 curl -i -X post "http://localhost:50070/gpdb/v5w/Writable/write?path=/data/curl/write" -d "hello" --header "Content-Type:application/octet-stream"
		HTTP/1.1 200 OK
		Content-Type: text/plain;charset=UTF-8
		Content-Length: 0
		Server: Jetty(7.6.10.v20130312)

	 * result:
 	2013-08-11 10:42:35,118 DEBUG com.pivotal.pxf.rest.resources.WritableResource: {Host=[localhost:50070], Accept=[*\/*], Content-Length=[5], Content-Type=[application/octet-stream], User-Agent=[curl/7.24.0 (x86_64-apple-darwin12.0) libcurl/7.24.0 OpenSSL/0.9.8x zlib/1.2.5]}
	2013-08-11 10:42:35,119 DEBUG com.pivotal.pxf.rest.resources.WritableResource: hello

	 */
	@POST
	@Path("write")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response write(@Context HttpHeaders headers, 
			 			  @QueryParam("path") String path,
			 			  String msg) throws Exception
	{
	
		MultivaluedMap<String, String> params = headers.getRequestHeaders();
		Log.debug(params);
		Log.debug(msg);

		// construct the output stream
		DataOutputStream dos;
		Configuration conf;
		conf        = new Configuration();
		FileSystem fs = FileSystem.get(conf);
	
		// Open a stream to write to the path. 
		org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(path);
		dos = fs.create(file, false /* do not override */);		

		// write msg it to the outputstream
		dos.writeBytes(msg);
		dos.close();

		return Response.ok().build();

	}

	/*
	 * This function is called when http://nn:port/gpdb/vx/Writable/stream?path=...
	 * is used.
	 *
	 * @param servletContext Servlet context contains attributes required by SecuredHDFS
	 * @param headers Holds HTTP headers from request
	 * @param path Holds URI path option used in this request
	 * @param inputStream stream of bytes to write from Hawq
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
		Log.debug("WritableResource started with parameters: " + params.toString());
		
		InputData inputData = new InputData(params, servletContext);
		IBridge bridge = new WriteBridge(inputData);

		// THREAD-SAFE parameter has precedence 
		boolean isThreadSafe = inputData.threadSafe() && bridge.isThreadSafe();
		Log.debug("Request for " + path + " handled " +
				  (isThreadSafe ? "without" : "with") + " synchronization"); 
		
		return isThreadSafe ? 
				writeResponse(bridge, path, inputStream) : 
				synchronizedWriteResponse(bridge, path, inputStream);
	}
	
	private static synchronized Response synchronizedWriteResponse(IBridge bridge, 
																   String path,
			                                                       InputStream inputStream) 
			                                                    		   throws Exception {
		return writeResponse(bridge, path, inputStream);
	}	
	
	private static Response writeResponse(IBridge bridge, 
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
			Log.debug("EofException timeout: connection closed on client side");
		}
		catch (Exception ex)
		{
			Log.debug("totalRead so far " + totalRead + " to " + path);
			throw ex;
		}
		finally 
		{
			inputStream.close();
		}
		returnMsg = "wrote " + totalRead + " bulks to " + path;
		Log.debug(returnMsg);
		
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
