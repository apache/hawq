package com.pivotal.pxf.rest.resources;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.eclipse.jetty.io.RuntimeIOException;

import com.pivotal.pxf.bridge.IBridge;
import com.pivotal.pxf.bridge.ReadBridge;
import com.pivotal.pxf.utilities.InputData;

/*
 * This class handles the subpath /<version>/Bridge/ of this
 * REST component
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Bridge/")
public class BridgeResource extends SecuredResource
{

	private static Log Log = LogFactory.getLog(BridgeResource.class);
	
	public BridgeResource() {}

	/*
	 * Used to be HDFSReader. Creates a bridge instance and iterates over
	 * its records, printing it out to outgoing stream.
	 * Outputs GPDBWritable.
	 *
	 * Parameters come through HTTP header other than the fragments.
	 * fragments is part of the url:
	 * /<version>/Bridge?fragment=
	 */
	@GET
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response read(@Context HttpHeaders headers,
					     @QueryParam("fragment") String fragment) throws Exception
	{
		// Convert headers into a regular map
		Map<String, String> params = convertToRegularMap(headers.getRequestHeaders());
		params.put("X-GP-DATA-FRAGMENT", fragment);

		Log.debug("started with paramters: " + params.toString());

		InputData inputData = new InputData(params);
		IBridge bridge = new ReadBridge(inputData);	
		String dataDir = inputData.path();
		// THREAD-SAFE parameter has precedence 
		boolean isThreadSafe = inputData.threadSafe() && bridge.isThreadSafe();
		Log.debug("Request for " + dataDir + " handled " +
				  (isThreadSafe ? "without" : "with") + " synchronization"); 
		
		return isThreadSafe ? readResponse(bridge, inputData) : 
			synchronizedReadResponse(bridge, inputData);
	}
	
	/*
	 * Used to handle requests for formats that do not support multithreading.
	 * E.g. - BZip2 files (*.bz2)
	 */
	private static synchronized Response synchronizedReadResponse(IBridge bridge, InputData inputData) throws Exception
	{
		return readResponse(bridge, inputData);
	}

	static Response readResponse(IBridge ibridge, InputData inputData) throws Exception
	{
		final IBridge bridge = ibridge;	

		if (!bridge.beginIteration())
			return Response.ok().build();

		final int fragment = inputData.getDataFragment();
		final String dataDir = inputData.getProperty("X-GP-DATA-DIR");

		// Creating an internal streaming class
		// which will iterate the records and put them on the
		// output stream
		final StreamingOutput streaming = new StreamingOutput()
		{
			@Override
			public void write(final OutputStream out) throws IOException, WebApplicationException
			{
				long recordCount = 0;

				try
				{
					Writable record = null;
					DataOutputStream dos = new DataOutputStream(out);
					Log.debug("Starting streaming fragment " + fragment + " of resource " + dataDir);
					while ((record = bridge.getNext()) != null)
					{
						record.write(dos);
						++recordCount;
					}
					Log.debug("Finished streaming fragment " + fragment + " of resource " + dataDir + ", " + recordCount + " records.");
				}
				catch (org.eclipse.jetty.io.EofException e)
				{
					// Occurs whenever GPDB decides the end the connection
					Log.error("Remote connection closed by GPDB", e);
				}
				catch (Exception e)
				{
					Log.error("Exception thrown streaming", e);
					// API does not allow throwing Exception so need to convert to something
					// I can throw without declaring...
					// Jetty ignores most exceptions while streaming (i.e recordCount > 0), so we need to throw a RuntimeIOException
					// (see org.eclipse.jetty.servlet.ServletHandler)
					if (recordCount > 0)
					{
						throw new RuntimeIOException(e.getMessage());
					}
					throw new IOException(e.getMessage());
				}
			}
		};

		return Response.ok(streaming, MediaType.APPLICATION_OCTET_STREAM).build();
	}

	Map<String, String> convertToRegularMap(MultivaluedMap<String, String> multimap) throws Exception
	{
		Map<String, String> result = new HashMap<String, String>();
		for (String key : multimap.keySet())
		{
			String val = multimap.getFirst(key);
			String newVal = val.replace("\\\"", "\"");
			String newKey = key;
			if (key.startsWith("X-GP-"))
				newKey = key.toUpperCase();
			if (newKey.compareTo("X-GP-FRAGMENT-USER-DATA") == 0) 
			{
				/* FRAGMENT-USER-DATA must be Base64 encoded */
				if (!Base64.isArrayByteBase64(newVal.getBytes()))
					throw new IOException("Fragment user data must be Base64 encoded. " +
							"(Bad value: " + newVal + ")");
				Log.debug("X-GP-FRAGMENT-USER-DATA: " + newVal);
			}
			result.put(newKey, newVal);
		}

		return result;
	}

}
