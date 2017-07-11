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
import org.apache.hawq.pxf.api.utilities.InputData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DemoAccessor.class}) // Enables mocking 'new' calls

public class DemoAccessorTest {

    @Mock InputData inputData;
    DemoAccessor accessor;

    @Before
    public void setup() throws Exception {
        accessor = new DemoAccessor(inputData);
    }

    @Test
    public void testRowsWithSingleColumn() throws Exception {

        when(inputData.getDataFragment()).thenReturn(0);
        when(inputData.getFragmentMetadata()).thenReturn("fragment1".getBytes(), "fragment1".getBytes());
        when(inputData.getColumns()).thenReturn(1);

        int numRows = 2;
        for (int i = 0; i < numRows; i++) {
            OneRow row = accessor.readNextObject();
            assertEquals(row.toString(),  "OneRow:0." + i + "->fragment1 row" + (i+1));
        }
        assertNull(accessor.readNextObject());
    }

    @Test
    public void testRowsWithMultipleColumns() throws Exception {

        when(inputData.getDataFragment()).thenReturn(0);
        when(inputData.getFragmentMetadata()).thenReturn("fragment1".getBytes(), "fragment1".getBytes());
        when(inputData.getColumns()).thenReturn(3);

        int numRows = 2;
        for (int i = 0; i < numRows; i++) {
            OneRow row = accessor.readNextObject();
            assertEquals(row.toString(),  "OneRow:0." + i + "->fragment1 row" + (i+1) + ",value1,value2");
        }
        assertNull(accessor.readNextObject());
    }
}
