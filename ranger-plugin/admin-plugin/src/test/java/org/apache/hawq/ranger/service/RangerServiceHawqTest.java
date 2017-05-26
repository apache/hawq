/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hawq.ranger.service;

import org.apache.ranger.plugin.client.BaseClient;
import org.apache.hawq.ranger.service.HawqClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.powermock.api.mockito.PowerMockito.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import static org.mockito.Matchers.*;

import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.apache.hawq.ranger.service.HawqClient.CONNECTION_SUCCESSFUL_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;


@RunWith(PowerMockRunner.class)
@PrepareForTest({HawqClient.class, RangerServiceHawq.class})
public class RangerServiceHawqTest {

    private RangerServiceHawq service;
    private Map<String, String> configs;

    @Mock
    HawqClient mockHawqClient;
    @Mock
    Connection conn;
    
    private Map<String, String> connectionProperties;

    @Before
    public void setup() {
        service = new RangerServiceHawq();
        service.setServiceName("hawq");
        service.setServiceType("hawq");

        configs = new HashMap<>();
        configs.put("username", "username");
        configs.put("password", "password");
        configs.put("hostname", "localhost");
        configs.put("port", "5432");

        service.setConfigs(configs);

        mockStatic(DriverManager.class);
    }

    @Test
    public void testValidateConfigSuccess() throws Exception {
		suppress(constructor(BaseClient.class, String.class, Map.class));
		suppress(method(HawqClient.class, "initHawq"));
    	    
        HashMap<String, Object> result = new HashMap<>();
        result.put("message", "ConnectionTest Successful");
        result.put("description", "ConnectionTest Successful");
        result.put("connectivityStatus", true);
        
        mockHawqClient = new HawqClient("hawq", connectionProperties);
        mockHawqClient.setConnection(conn);
        PowerMockito.whenNew(HawqClient.class).withArguments(anyObject(),anyObject()).thenReturn(mockHawqClient);
        
        when(conn.getCatalog()).thenReturn("catalog");

        HashMap<String, Object> response = service.validateConfig();
        assertEquals(CONNECTION_SUCCESSFUL_MESSAGE, response.get("description"));
        assertEquals(CONNECTION_SUCCESSFUL_MESSAGE, response.get("message"));
        assertTrue((Boolean) response.get("connectivityStatus"));
    }
}
