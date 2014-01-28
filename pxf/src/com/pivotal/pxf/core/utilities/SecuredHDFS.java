package com.pivotal.pxf.core.utilities;

import com.pivotal.pxf.api.utilities.InputData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import javax.servlet.ServletContext;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/*
 * The class handles security functions for handling 
 * secured HDFS
 */
public class SecuredHDFS {
    private static final Log LOG = LogFactory.getLog(SecuredHDFS.class);

    /*
     * Convert a hex string to a byte array.
	 *
	 * @throws IllegalArgumentException when data is not even
	 *
	 * @param hex HEX string to deserialize
	 */
    private static byte[] hexStringToByteArray(String hex) {
        final int HEX_RADIX = 16;
        final int NIBBLE_SIZE_IN_BITS = 4;

        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException("Internal server error. String " +
                    hex + " isn't a valid hex string");
        }

        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i = i + 2) {
            result[i / 2] = (byte) ((Character.digit(hex.charAt(i), HEX_RADIX) << NIBBLE_SIZE_IN_BITS) +
                    Character.digit(hex.charAt(i + 1), HEX_RADIX));
        }

        return result;
    }

    /*
     * The function will get the token information from parameters
	 * and call SecuredHDFS to verify the token.
	 *
	 * X-GP data will be deserialied from hex string to a byte array
	 */
    public static void verifyToken(InputData inputData, ServletContext context) {
        if (UserGroupInformation.isSecurityEnabled()) {
            byte[] identifier = hexStringToByteArray(inputData.getProperty("X-GP-TOKEN-IDNT"));
            byte[] password = hexStringToByteArray(inputData.getProperty("X-GP-TOKEN-PASS"));
            byte[] kind = hexStringToByteArray(inputData.getProperty("X-GP-TOKEN-KIND"));
            byte[] service = hexStringToByteArray(inputData.getProperty("X-GP-TOKEN-SRVC"));
            verifyToken(identifier, password, kind, service, context);
        }
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
    private static void verifyToken(byte[] identifier, byte[] password,
                                    byte[] kind, byte[] service,
                                    ServletContext servletContext) {
        try {
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
        } catch (IOException e) {
            throw new SecurityException("Failed to verify delegation token " + e, e);
        }
    }
}
