package com.pivotal.pxf.rest.resources;

import org.apache.hadoop.security.UserGroupInformation;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class SecuredResource
{
	private Log Log;

	public SecuredResource()
	{
		Log = LogFactory.getLog(SecuredResource.class);

		if ( securityEnabled() )
			failRequest();
	}

	private boolean securityEnabled()
	{
		return UserGroupInformation.isSecurityEnabled();
	}

	private void failRequest()
	{
		String message = "PXF is not yet supported with Secured HDFS";
		Log.error(message);
		ResponseBuilder forbidden = Response.status(Response.Status.FORBIDDEN).entity(message);
		throw new WebApplicationException(forbidden.build());
	}
}
