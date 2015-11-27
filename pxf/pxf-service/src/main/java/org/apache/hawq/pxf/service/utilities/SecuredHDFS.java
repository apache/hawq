package org.apache.hawq.pxf.service.utilities;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
     * The function will get the token information from parameters and call
     * SecuredHDFS to verify the token.
     *
     * All token properties will be deserialized from string to a Token object
     *
     * @throws SecurityException Thrown when authentication fails
     */
    public static void verifyToken(ProtocolData protData, ServletContext context) {
        try {
            if (UserGroupInformation.isSecurityEnabled()) {
                Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
                String tokenString = protData.getToken();
                token.decodeFromUrlString(tokenString);

                verifyToken(token.getIdentifier(), token.getPassword(),
                        token.getKind(), token.getService(), context);
            }
        } catch (IOException e) {
            throw new SecurityException("Failed to verify delegation token "
                    + e, e);
        }
    }

    /*
     * The function will verify the token with NameNode if available and will
     * create a UserGroupInformation.
     *
     * Code in this function is copied from JspHelper.getTokenUGI
     *
     * @param identifier Delegation token identifier
     *
     * @param password Delegation token password
     *
     * @param servletContext Jetty servlet context which contains the NN address
     *
     * @throws SecurityException Thrown when authentication fails
     */
    private static void verifyToken(byte[] identifier, byte[] password,
                                    Text kind, Text service,
                                    ServletContext servletContext) {
        try {
            Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
                    identifier, password, kind, service);

            ByteArrayInputStream buf = new ByteArrayInputStream(
                    token.getIdentifier());
            DataInputStream in = new DataInputStream(buf);
            DelegationTokenIdentifier id = new DelegationTokenIdentifier();
            id.readFields(in);

            final NameNode nn = NameNodeHttpServer.getNameNodeFromContext(servletContext);
            if (nn != null) {
                nn.getNamesystem().verifyToken(id, token.getPassword());
            }

            UserGroupInformation userGroupInformation = id.getUser();
            userGroupInformation.addToken(token);
            LOG.debug("user " + userGroupInformation.getUserName() + " ("
                    + userGroupInformation.getShortUserName()
                    + ") authenticated");

            // re-login if necessary
            userGroupInformation.checkTGTAndReloginFromKeytab();
        } catch (IOException e) {
            throw new SecurityException("Failed to verify delegation token "
                    + e, e);
        }
    }
}
