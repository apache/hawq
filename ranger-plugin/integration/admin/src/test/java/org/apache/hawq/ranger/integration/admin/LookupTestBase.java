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

package org.apache.hawq.ranger.integration.admin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.ranger.service.RangerServiceHawq;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class LookupTestBase {

    protected static final Log LOG = LogFactory.getLog(LookupTestBase.class);
    protected RangerServiceHawq service;

    @Before
    public void setup() {
        Map<String, String> configs = new HashMap<>();
        configs = new HashMap<>();
        configs.put("username", "gpadmin");
        configs.put("password", "dQSF8ViAE4/I38xmFwJfCg==");
        configs.put("hostname", "localhost");
        configs.put("port", "5432");
        configs.put("jdbc.driverClassName", "org.postgresql.Driver");

        service = new RangerServiceHawq();
        service.setServiceName("hawq");
        service.setServiceType("hawq");
        service.setConfigs(configs);
    }

    protected ResourceLookupContext getContext(String resourceName, String userInput) {
        ResourceLookupContext context = new ResourceLookupContext();
        context.setResourceName(resourceName);
        context.setUserInput(userInput);
        return context;
    }

    protected ResourceLookupContext getContext(String resourceName, String userInput, Map<String, List<String>> resources) {
        ResourceLookupContext context = getContext(resourceName, userInput);
        context.setResources(resources);
        return context;
    }
}
