package org.apache.hawq.pxf.service;

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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OutputFormat;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class BridgeInputBuilderTest {
    ProtocolData mockProtocolData;
    BridgeInputBuilder inputBuilder;
    DataInputStream inputStream;

    @Test
    /*
     * Test makeInput method: small \n terminated input
     */
    public void makeInput() throws Exception {

        byte[] data = new byte[] {
                (int) 'a',
                (int) 'b',
                (int) 'c',
                (int) 'd',
                (int) '\n',
                (int) 'n',
                (int) 'o',
                (int) '\n' };

        prepareInput(data);

        List<OneField> record = inputBuilder.makeInput(inputStream);

        verifyRecord(record, Arrays.copyOfRange(data, 0, 5));

        record = inputBuilder.makeInput(inputStream);
        verifyRecord(record, Arrays.copyOfRange(data, 5, 8));
    }

    @Test
    /*
     * Test the makeInput method: input > buffer size, \n terminated
     */
    public void makeInputBigArray() throws Exception {

        byte[] bigArray = new byte[2000];
        for (int i = 0; i < 1999; ++i) {
            bigArray[i] = (byte) (i % 10 + 30);
        }
        bigArray[1999] = (byte) '\n';

        prepareInput(bigArray);

        List<OneField> record = inputBuilder.makeInput(inputStream);

        verifyRecord(record, bigArray);
    }

    @Test
    /*
     * Test the makeInput method: input > buffer size, no \n
     */
    public void makeInputBigArrayNoNewLine() throws Exception {

        byte[] bigArray = new byte[2000];
        for (int i = 0; i < 2000; ++i) {
            bigArray[i] = (byte) (i % 10 + 60);
        }

        prepareInput(bigArray);

        List<OneField> record = inputBuilder.makeInput(inputStream);

        verifyRecord(record, bigArray);
    }

    @Test
    /*
     * Test the makeInput method: empty stream (returns -1)
     */
    public void makeInputEmptyStream() throws Exception {

        byte[] empty = new byte[0];

        prepareInput(empty);

        List<OneField> record = inputBuilder.makeInput(inputStream);

        verifyRecord(record, empty);
    }

    /*
     * helpers functions
     */

    @After
    public void cleanUp() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    private void prepareInput(byte[] data) throws Exception {
        mockProtocolData = mock(ProtocolData.class);
        PowerMockito.when(mockProtocolData.outputFormat()).thenReturn(
                OutputFormat.TEXT);
        inputBuilder = new BridgeInputBuilder(
                mockProtocolData);
        inputStream = new DataInputStream(
                new ByteArrayInputStream(data));
    }

    private void verifyRecord(List<OneField> record, byte[] expected) {
        assertEquals(record.size(), 1);

        OneField field = record.get(0);
        assertEquals(field.type, DataType.BYTEA.getOID());

        byte[] bytes = (byte[]) field.val;
        byte[] result = Arrays.copyOfRange(bytes, 0, bytes.length);
        assertEquals(result.length, expected.length);
        assertTrue(Arrays.equals(result, expected));
    }
}
