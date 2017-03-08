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

package org.apache.hawq.ranger.authorization;

import org.junit.Test;

import static org.apache.hawq.ranger.authorization.Utils.INSTANCE_PROPERTY_KEY_ENV;
import static org.apache.hawq.ranger.authorization.Utils.VERSION_PROPERTY_KEY_ENV;
import static org.junit.Assert.assertEquals;

/**
 * This test class uses values from rps.properties file in test/resources directory.
 */
public class UtilsTest {

    @Test
    public void testAppId_SystemEnv() throws Exception {
        System.setProperty(INSTANCE_PROPERTY_KEY_ENV, "instance");
        assertEquals("instance", Utils.getInstanceName());
        System.clearProperty(INSTANCE_PROPERTY_KEY_ENV);
    }

    @Test
    public void testAppId_PropertyFile() throws Exception {
        assertEquals("instance-test", Utils.getInstanceName());
    }

    @Test
    public void testGetVersion_SystemEnv() throws Exception {
        System.setProperty(VERSION_PROPERTY_KEY_ENV, "1.2.3");
        assertEquals("1.2.3", Utils.getVersion());
        System.clearProperty(VERSION_PROPERTY_KEY_ENV);
    }

    @Test
    public void testGetVersion_PropertyFile() throws Exception {
        assertEquals("version-test", Utils.getVersion());
    }
}