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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.junit.Test;

public class StringPassResolverTest {
    InputData mockInputData;

    @Test
    /*
     * Test the setFields method: small input
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

        List<OneField> record = Collections.singletonList(new OneField(DataType.BYTEA.getOID(),
                Arrays.copyOfRange(data, 0, 5)));

        OneRow oneRow = resolver.setFields(record);
        verifyOneRow(oneRow, Arrays.copyOfRange(data, 0, 5));

        record = Collections.singletonList(new OneField(DataType.BYTEA.getOID(),
                Arrays.copyOfRange(data, 5, 8)));

        oneRow = resolver.setFields(record);
        verifyOneRow(oneRow, Arrays.copyOfRange(data, 5, 8));
    }

    @Test
    /*
     * Test the setFields method: empty byte array
     */
    public void testSetFieldsEmptyByteArray() throws Exception {

        StringPassResolver resolver = buildResolver();

        byte[] empty = new byte[0];

        List<OneField> record = Collections.singletonList(new OneField(DataType.BYTEA.getOID(),
                                                          empty));

        OneRow oneRow = resolver.setFields(record);

        assertNull(oneRow);
    }

    /*
     * helpers functions
     */
    private StringPassResolver buildResolver() throws Exception {
        mockInputData = mock(InputData.class);
        return new StringPassResolver(mockInputData);
    }

    private void verifyOneRow(OneRow oneRow, byte[] expected) {
        assertNull(oneRow.getKey());
        byte[] bytes = (byte[]) oneRow.getData();
        byte[] result = Arrays.copyOfRange(bytes, 0, bytes.length);
        assertEquals(result.length, expected.length);
        assertTrue(Arrays.equals(result, expected));
    }
}
