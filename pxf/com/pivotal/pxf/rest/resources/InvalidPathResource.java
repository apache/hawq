package com.pivotal.pxf.rest.resources;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


class Version 
{
	final static String PXF_VERSION = "v2";	
}

/*
 * Class for catching paths that are not defined by other resources.
 * For each path, the version is compared to the current version GPXF_VERSION.
 * The expected format of a path is "http://<host>:<port>/gpdb/<version>/<rest of path>
 * 
 * The returned value is always a Server Error code (500).
 * If the version is different than the current version, an appropriate error is returned with version details.
 * Otherwise, an error about unknown path is returned.
 */
@Path("/")
public class InvalidPathResource
{
	@Context
	UriInfo rootUri;
	
	private Log Log;
	
	public InvalidPathResource() throws IOException
	{ 
		Log = LogFactory.getLog(InvalidPathResource.class);
	}

	/*
	 * Catch path /gpdb/
	 */
	@GET
	@Path("/")
	public Response noPath() throws Exception
	{
		String errmsg = "Unknown path " + rootUri.getAbsolutePath();
		return sendErrorMessage(errmsg);
	}
	
	/*
	 * Catch paths of pattern /gpdb/*
	 */
	@GET
	@Path("/{path:.*}")
	public Response wrongPath(@PathParam("path") String path) throws Exception
	{
	        
		String errmsg;
		String version = parseVersion(path);
			
		Log.debug("REST request: " +  rootUri.getAbsolutePath()  + ". " + 
				  "Version " + version + ", supported version is " + Version.PXF_VERSION);
				  
		if (version.equals(Version.PXF_VERSION))
		{
			errmsg = "Unknown path " + rootUri.getAbsolutePath();
		}
		else 
		{
			errmsg = "Wrong version " + version + ", supported version is " + Version.PXF_VERSION;
		}
		
		return sendErrorMessage(errmsg);
	}

	/* 
	 * Return error message
	 */
	private Response sendErrorMessage(String message)
	{
		final String returnmsg = new String(message);

		StreamingOutput streaming = new StreamingOutput()
		{
			/*
			 * writes given message to stream
			 */			
			@Override
			public void write(final OutputStream out) throws IOException
			{
				DataOutputStream dos = new DataOutputStream(out);

				try
				{
					dos.writeBytes(returnmsg);
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

		ResponseBuilder b = Response.serverError();
		b.entity(streaming);
		b.type(MediaType.APPLICATION_OCTET_STREAM);
		return b.build();
	}

	/*
	 * Parse the version part from the path.
	 * The the absolute path is 
	 * http://<host>:<port>/gpdb/<version>/<rest of path>
	 * 
	 * path - the path part after /gpdb/ 
	 * returns the first element after /gpdb/
	 */
	private String parseVersion(String path) {

		int slash = path.indexOf('/');
		if (slash == -1)
			return path;
					
		return path.substring(0, slash);
	}
}
