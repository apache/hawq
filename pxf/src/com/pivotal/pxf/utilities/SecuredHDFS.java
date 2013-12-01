package com.pivotal.pxf.utilities;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import javax.servlet.ServletContext;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * The class handles security functions for handling 
 * secured HDFS
 */
class SecuredHDFS
{
	private static final Log LOG = LogFactory.getLog(SecuredHDFS.class);

	/*
	 * The function will tell whether secured HDFS is enabled 
	 * or not.
	 * The function is negative as it is mainly used in:
	 * if (SecuredHDFS.isDisabled)
	 *
	 * @returns false when enabled, true when disabled
	 */
	static boolean isDisabled()
	{
		return !UserGroupInformation.isSecurityEnabled();
	}

	/*
	 * The function will verify the token with NameNode
	 * if available and will create a UserGroupInformation.
	 *
	 * Code in this function is copied from JspHelper.getTokenUGI
	 *
	 * @param identifier Delegation token identifier
	 * @param password Delegation token password
	 * @param servletContext Jetty servlet context which contains the NN address
	 *
	 * @throws SecurityException Thrown when authentication fails
	 */
	static void verifyToken(byte[] identifier, byte[] password, 
							byte[] kind, byte[] service,
							ServletContext servletContext)
	{
		try
		{
			Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
					identifier,
					password,
					new Text(kind),
					new Text(service));

			ByteArrayInputStream buf =
				new ByteArrayInputStream(token.getIdentifier());
			DataInputStream in = new DataInputStream(buf);
			DelegationTokenIdentifier id = new DelegationTokenIdentifier();
			id.readFields(in);

			final NameNode nn = NameNodeHttpServer.getNameNodeFromContext(servletContext);
			if (nn != null) {
				nn.getNamesystem().verifyToken(id, token.getPassword());
			}

			UserGroupInformation userGroupInformation = id.getUser();
			userGroupInformation.addToken(token);
			LOG.debug("user " + userGroupInformation.getUserName() + 
					  " (" + userGroupInformation.getShortUserName() + 
					  ") authenticated");
		} catch (IOException e)
		{
			throw new SecurityException("Failed to verify delegation token " + e, e);
		}
	}
}
