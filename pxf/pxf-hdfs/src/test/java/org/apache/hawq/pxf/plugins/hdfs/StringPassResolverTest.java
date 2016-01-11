package org.apache.hawq.pxf.plugins.hdfs;

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.OutputFormat;
import org.apache.hawq.pxf.service.BridgeInputBuilder;
import org.apache.hawq.pxf.service.io.Text;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        Text.class,
        BridgeInputBuilder.class,
        ProtocolData.class,
        LogFactory.class })
public class StringPassResolverTest {
    ProtocolData mockProtocolData;

    @Test
    /*
     * Test the setFields method: small \n terminated input
     */
    public void testSetFields() throws Exception {
        StringPassResolver resolver = buildResolver();

        byte[] data = new byte[] {
                (int) 'a',
                (int) 'b',
                (int) 'c',
                (int) 'd',
                (int) '\n',
                (int) 'n',
                (int) 'o',
                (int) '\n' };

        DataInputStream inputStream = new DataInputStream(
                new ByteArrayInputStream(data));
        BridgeInputBuilder inputBuilder = new BridgeInputBuilder(
                mockProtocolData);
        List<OneField> record = inputBuilder.makeInput(inputStream);

        OneRow oneRow = resolver.setFields(record);
        verifyOneRow(oneRow, Arrays.copyOfRange(data, 0, 5));

        record = inputBuilder.makeInput(inputStream);
        oneRow = resolver.setFields(record);
        verifyOneRow(oneRow, Arrays.copyOfRange(data, 5, 8));
    }

    @Test
    /*
     * Test the setFields method: input > buffer size, \n terminated
     */
    public void testSetFieldsBigArray() throws Exception {

        StringPassResolver resolver = buildResolver();

        byte[] bigArray = new byte[2000];
        for (int i = 0; i < 1999; ++i) {
            bigArray[i] = (byte) (i % 10 + 30);
        }
        bigArray[1999] = (byte) '\n';

        DataInputStream inputStream = new DataInputStream(
                new ByteArrayInputStream(bigArray));
        BridgeInputBuilder inputBuilder = new BridgeInputBuilder(
                mockProtocolData);
        List<OneField> record = inputBuilder.makeInput(inputStream);

        OneRow oneRow = resolver.setFields(record);

        verifyOneRow(oneRow, bigArray);
    }

    @Test
    /*
     * Test the setFields method: input > buffer size, no \n
     */
    public void testSetFieldsBigArrayNoNewLine() throws Exception {

        StringPassResolver resolver = buildResolver();

        byte[] bigArray = new byte[2000];
        for (int i = 0; i < 2000; ++i) {
            bigArray[i] = (byte) (i % 10 + 60);
        }

        DataInputStream inputStream = new DataInputStream(
                new ByteArrayInputStream(bigArray));
        BridgeInputBuilder inputBuilder = new BridgeInputBuilder(
                mockProtocolData);
        List<OneField> record = inputBuilder.makeInput(inputStream);

        OneRow oneRow = resolver.setFields(record);

        verifyOneRow(oneRow, bigArray);
    }

    @Test
    /*
     * Test the setFields method: empty stream (returns -1)
     */
    public void testSetFieldsEmptyStream() throws Exception {

        StringPassResolver resolver = buildResolver();

        byte[] empty = new byte[0];

        DataInputStream inputStream = new DataInputStream(
                new ByteArrayInputStream(empty));
        BridgeInputBuilder inputBuilder = new BridgeInputBuilder(
                mockProtocolData);
        List<OneField> record = inputBuilder.makeInput(inputStream);

        OneRow oneRow = resolver.setFields(record);

        assertNull(oneRow);
    }

    /*
     * helpers functions
     */
    private StringPassResolver buildResolver() throws Exception {

        mockProtocolData = mock(ProtocolData.class);
        PowerMockito.when(mockProtocolData.outputFormat()).thenReturn(
                OutputFormat.TEXT);

        return new StringPassResolver(mockProtocolData);
    }

    private void verifyOneRow(OneRow oneRow, byte[] expected) {
        assertNull(oneRow.getKey());
        byte[] bytes = (byte[]) oneRow.getData();
        byte[] result = Arrays.copyOfRange(bytes, 0, bytes.length);
        assertEquals(result.length, expected.length);
        assertTrue(Arrays.equals(result, expected));
    }
}
