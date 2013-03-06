package com.emc.greenplum.gpdb.rest.resources;

import java.io.OutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.Map;
import java.util.HashMap;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.hadoop.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.emc.greenplum.gpdb.hdfsconnector.HDMetaData;
import com.emc.greenplum.gpdb.hdfsconnector.IBridge;
import com.emc.greenplum.gpdb.hdfsconnector.BridgeFactory;
import com.emc.greenplum.gpdb.hdfsconnector.HiveDataFragmenter;

/*
 * This class handles the subpath /v2/Bridge/ of this
 * REST component
 */
@Path("/v2/Bridge/")
public class BridgeResource
{
	private Log Log;
	public BridgeResource()
	{
		Log = LogFactory.getLog(BridgeResource.class);
	}

	/*
	 * Used to be HDFSReader. Creates a bridge instance and iterates over
	 * its records, printing it out to outgoing stream.
	 * Outputs GPDBWritable.
	 *
	 * Parameters come through HTTP header other than the fragments.
	 * fragments is part of the url:
	 * /v2/Bridge?fragments=
	 */
    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response read(@Context HttpHeaders headers,
						 @QueryParam("fragments") String fragments) throws Exception
	{
		// Convert headers into a regular map
		Map<String, String> params = convertToRegularMap(headers.getRequestHeaders());
		params.put("X-GP-DATA-FRAGMENTS", fragments);		
		
		Log.debug("started with paramters: " + params.toString());

		return readResponse(params);
	}

	Response readResponse(Map<String, String> params) throws Exception
	{
		HDMetaData connectorConf = new HDMetaData(params);
		final IBridge bridge = BridgeFactory.create(connectorConf);

		if (!bridge.BeginIteration())
			return Response.ok().build();

		// Creating an internal streaming class
		// which will iterate the records and put them on the
		// output stream
		final StreamingOutput streaming = new StreamingOutput()
		{
			@Override
			public void write(final OutputStream out) throws IOException
			{
				try
				{
					Writable record = null;
					DataOutputStream dos = new DataOutputStream(out);
					while ((record = bridge.GetNext()) != null)
						record.write(dos);
				}
				catch (org.mortbay.jetty.EofException e)
				{
					// Occurs whenever GPDB decides the end the connection
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

		return Response.ok(streaming, MediaType.APPLICATION_OCTET_STREAM).build();
	}

	Map<String, String> convertToRegularMap(MultivaluedMap<String, String> multimap)
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
				newVal = newVal.replaceAll(HiveDataFragmenter.HIVE_LINEFEED_REPLACE, "\n"); /* Replacing the '\n' that were removed in HiveDataFragmenter.makeUserData */
			result.put(newKey, newVal);
		}

		return result;
	}
}
