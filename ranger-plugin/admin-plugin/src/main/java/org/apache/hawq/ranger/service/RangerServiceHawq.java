/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hawq.ranger.service;

import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.PasswordUtils;

import java.util.*;

public class RangerServiceHawq extends RangerBaseService {

    private static final Log LOG = LogFactory.getLog(RangerServiceHawq.class);

    public RangerServiceHawq() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public HashMap<String, Object> validateConfig() throws Exception {
        boolean isDebugEnabled = LOG.isDebugEnabled();

        if (isDebugEnabled) {
            LOG.debug("==> RangerServiceHawq.validateConfig Service: (hawq)");
        }

        HashMap<String, Object> result = new HashMap<>();
        if (configs != null) {
            boolean retry = false;

            // try normal password (user input in RangerUI form)
            result = checkConnection(configs);
            if (result.containsKey("connectivityStatus") && !(boolean)(result.get("connectivityStatus"))) {
                retry = true;
            }

            if (retry) {
                // try decrypt password
                decryptPassword(configs);
                result = checkConnection(configs);
            }

        }

        if (isDebugEnabled) {
            LOG.debug("<== RangerServiceHawq.validateConfig Response : (" + result + ")");
        }
        return result;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        String serviceName = getServiceName();
        String serviceType = getServiceType();

        // lookup always need decrypt password (fetch from database)
        decryptPassword(configs);

        List<String> resources = HawqResourceMgr.getHawqResources(serviceName, serviceType, configs, context);

        return resources;
    }

    private HashMap<String, Object> checkConnection(Map<String, String> configs) throws Exception {
        HashMap<String, Object> result;
        String serviceName = getServiceName();

        try {
            HawqClient hawqClient = new HawqClient(serviceName, configs);
            result = hawqClient.checkConnection(configs);
        } catch (HadoopException e) {
            LOG.error("<== RangerServiceHawq.validateConfig Error:" + e);
            throw e;
        }

        return result;
    }

    /**
     * decrypt password field of configs
     * Note:
     *  the decrypted password is set in a new password_jdbc field
     * @param configs
     * @throws Exception
     */
    private void decryptPassword(Map<String, String> configs) throws Exception {
        if (configs.containsKey("password")) {
            String normal_password = configs.get("password");
            try {
                normal_password = PasswordUtils.decryptPassword(normal_password);
            }
            catch (java.io.IOException e) {
                // when decrypt failed do nothing
                LOG.warn("decrypt_password failed: " + e);
            }
            configs.put("password_jdbc", normal_password);
        }
    }

}
