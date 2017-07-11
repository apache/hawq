package org.apache.hawq.pxf.api;

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


import org.apache.hawq.pxf.api.examples.DemoAccessor;
import org.apache.hawq.pxf.api.examples.DemoResolver;
import org.apache.hawq.pxf.api.examples.DemoTextResolver;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DemoAccessor.class}) // Enables mocking 'new' calls

public class DemoResolverTest {

    @Mock InputData inputData;
    DemoResolver customResolver;
    DemoTextResolver textResolver;
    OneRow row;

    @Before
    public void setup() throws Exception {
        customResolver = new DemoResolver(inputData);
        textResolver = new DemoTextResolver(inputData);
        row = new OneRow("0.0","value1,value2");
    }

    @Test
    public void testCustomData() throws Exception {

        List<OneField> output = customResolver.getFields(row);
        assertEquals("value1", output.get(0).toString());
        assertEquals("value2", output.get(1).toString());
    }

    @Test
    public void testTextData() throws Exception {

        List<OneField> output = textResolver.getFields(row);
        assertEquals("value1,value2", output.get(0).toString());

    }
}
