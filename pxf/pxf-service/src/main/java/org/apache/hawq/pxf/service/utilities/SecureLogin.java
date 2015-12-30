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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;

/**
 * This class relies heavily on Hadoop API to
 * <ul>
 * <li>Check need for secure login in Hadoop</li>
 * <li>Parse and load .xml configuration file</li>
 * <li>Do a Kerberos login with a kaytab file</li>
 * <li>convert _HOST in Kerberos principal to current hostname</li>
 * </ul>
 *
 * It uses Hadoop Configuration to parse XML configuration files.<br>
 * It uses Hadoop Security to modify principal and perform the login.
 *
 * The major limitation in this class is its dependency on Hadoop. If Hadoop
 * security is off, no login will be performed regardless of connector being
 * used.
 */
public class SecureLogin {
    private static final Log LOG = LogFactory.getLog(SecureLogin.class);
    private static final String CONFIG_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";
    private static final String CONFIG_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";

    public static void login() {
        try {
            Configuration config = new Configuration();
            config.addResource("pxf-site.xml");

            SecurityUtil.login(config, CONFIG_KEY_SERVICE_KEYTAB,
                    CONFIG_KEY_SERVICE_PRINCIPAL);
        } catch (Exception e) {
            LOG.error("PXF service login failed");
            throw new RuntimeException(e);
        }
    }
}
