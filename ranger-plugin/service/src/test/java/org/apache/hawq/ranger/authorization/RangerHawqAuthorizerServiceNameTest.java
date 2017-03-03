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

package org.apache.hawq.ranger.authorization;

import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.Test;

import static org.apache.hawq.ranger.authorization.Utils.INSTANCE_PROPERTY_KEY_ENV;
import static org.junit.Assert.assertEquals;
public class RangerHawqAuthorizerServiceNameTest {

    @Test
    public void testServiceNameIsSet() {
        System.setProperty(INSTANCE_PROPERTY_KEY_ENV, "instance");
        assertEquals("instance", Utils.getInstanceName());
        RangerBasePlugin plugin = RangerHawqAuthorizer.getInstance().getRangerPlugin();
        assertEquals("instance", plugin.getServiceName());
        assertEquals("hawq", plugin.getServiceType());
        assertEquals("rps", plugin.getAppId());
    }
}
