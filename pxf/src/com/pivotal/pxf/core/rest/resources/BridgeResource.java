package com.pivotal.pxf.core.rest.resources;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import com.pivotal.pxf.core.bridge.Bridge;
import com.pivotal.pxf.core.bridge.ReadBridge;
import com.pivotal.pxf.core.io.Writable;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.core.utilities.SecuredHDFS;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.io.RuntimeIOException;

/*
 * This class handles the subpath /<version>/Bridge/ of this
 * REST component
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Bridge/")
public class BridgeResource
{

	private static Log Log = LogFactory.getLog(BridgeResource.class);
	/**
	 * Lock is needed here in the case of a non-thread-safe plugin.
	 * Using synchronized methods is not enough because the bridge work
	 * is called by jetty ({@link StreamingOutput}), after we are getting 
	 * out of this class's context.
	 * 
	 * BRIDGE_LOCK is accessed through lock() and unlock() functions, based on the 
	 * isThreadSafe parameter that is determined by the bridge.
	 */
	private static final ReentrantLock BRIDGE_LOCK = new ReentrantLock();
	
	public BridgeResource() {}

	/*
	 * Used to be HDFSReader. Creates a bridge instance and iterates over
	 * its records, printing it out to outgoing stream.
	 * Outputs GPDBWritable.
	 *
	 * Parameters come through HTTP header.
	 *
	 * @param servletContext Servlet context contains attributes required by SecuredHDFS
	 * @param headers Holds HTTP headers from request
	 */
	@GET
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response read(@Context final ServletContext servletContext,
						 @Context HttpHeaders headers) throws Exception
	{
		// Convert headers into a regular map
		Map<String, String> params = convertToRegularMap(headers.getRequestHeaders());

		Log.debug("started with parameters: " + params);

		InputData inputData = new InputData(params);
        SecuredHDFS.verifyToken(inputData, servletContext);
		Bridge bridge = new ReadBridge(inputData);
		String dataDir = inputData.path();
		// THREAD-SAFE parameter has precedence 
		boolean isThreadSafe = inputData.threadSafe() && bridge.isThreadSafe();
		Log.debug("Request for " + dataDir + " will be handled " +
				  (isThreadSafe ? "without" : "with") + " synchronization"); 
		
		return readResponse(bridge, inputData, isThreadSafe);
	}

	Response readResponse(final Bridge bridge, InputData inputData, final boolean threadSafe) throws Exception
	{
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

				if (!threadSafe) {
					lock(dataDir);
				}
				try {
					
					if (!bridge.beginIteration()) {
						return;
					}
					
					Writable record;
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
				finally {
					if (!threadSafe) {
						unlock(dataDir);
					}
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
	
	/**
	 * Lock BRIDGE_LOCK
	 * 
	 * @param path path for the request, used for logging.
	 */
	private void lock(String path) {
		Log.trace("Locking BridgeResource for " + path);
		BRIDGE_LOCK.lock();
		Log.trace("Locked BridgeResource for " + path);
	}
	
	/**
	 * Unlock BRIDGE_LOCK
	 * 
	 * @param path path for the request, used for logging.
	 */
	private void unlock(String path) {
		Log.trace("Unlocking BridgeResource for " + path);
		BRIDGE_LOCK.unlock();
		Log.trace("Unlocked BridgeResource for " + path);
	}

}
