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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

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

    private static final String PROPERTY_KEY_USER_IMPERSONATION = "pxf.service.user.impersonation.enabled";
    private static final String CONFIG_KEY_SERVICE_PRINCIPAL = "pxf.service.kerberos.principal";
    private static final String CONFIG_KEY_SERVICE_KEYTAB = "pxf.service.kerberos.keytab";

    /**
     * Establishes Login Context for the PXF service principal using Kerberos keytab.
     */
    public static void login() {
        try {
            boolean isUserImpersonationEnabled = isUserImpersonationEnabled();
            LOG.info("User impersonation is " + (isUserImpersonationEnabled ? "enabled" : "disabled"));

            if (!UserGroupInformation.isSecurityEnabled()) {
                LOG.info("Kerberos Security is not enabled");
                return;
            }

            LOG.info("Kerberos Security is enabled");

            if (!isUserImpersonationEnabled) {
                throw new RuntimeException("User Impersonation is required when Kerberos Security is enabled. " +
                        "Set PXF_USER_IMPERSONATION=true in $PXF_HOME/conf/pxf-env.sh");
            }

            String principal = System.getProperty(CONFIG_KEY_SERVICE_PRINCIPAL);
            String keytabFilename = System.getProperty(CONFIG_KEY_SERVICE_KEYTAB);

            if (StringUtils.isEmpty(principal)) {
                throw new RuntimeException("Kerberos Security requires a valid principal.");
            }

            if (StringUtils.isEmpty(keytabFilename)) {
                throw new RuntimeException("Kerberos Security requires a valid keytab file name.");
            }

            Configuration config = new Configuration();
            config.set(CONFIG_KEY_SERVICE_PRINCIPAL, principal);
            config.set(CONFIG_KEY_SERVICE_KEYTAB, keytabFilename);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Kerberos principal: " + config.get(CONFIG_KEY_SERVICE_PRINCIPAL));
                LOG.debug("Kerberos keytab: " + config.get(CONFIG_KEY_SERVICE_KEYTAB));
            }

            SecurityUtil.login(config, CONFIG_KEY_SERVICE_KEYTAB, CONFIG_KEY_SERVICE_PRINCIPAL);

        } catch (Exception e) {
            LOG.error("PXF service login failed: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns whether user impersonation has been configured as enabled.
     *
     * @return true if user impersonation is enabled, false otherwise
     */
    public static boolean isUserImpersonationEnabled() {
        return StringUtils.equalsIgnoreCase(System.getProperty(PROPERTY_KEY_USER_IMPERSONATION, ""), "true");
    }
}
