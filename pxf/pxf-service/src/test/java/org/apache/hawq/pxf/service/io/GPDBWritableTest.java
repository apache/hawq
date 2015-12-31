package org.apache.hawq.pxf.service.io;

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


import org.apache.commons.logging.Log;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.DataInput;
import java.io.EOFException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("GPDBWritable")
@PrepareForTest({GPDBWritable.class})
public class GPDBWritableTest {

    DataInput inputStream;
    OngoingStubbing<Integer> ongoing;
    Log LOG;

    @Before
    public void setupStaticLog() {
        LOG = mock(Log.class);
        Whitebox.setInternalState(GPDBWritable.class, LOG);
    }

    /*
     * Test the readFields method: empty stream
	 */
    @Test
    public void testReadFieldsEmpty() throws Exception {

        GPDBWritable gpdbWritable = buildGPDBWritable();

        int[] empty = new int[0];
        buildStream(empty, true);

        gpdbWritable.readFields(inputStream);

        verifyLog("Reached end of stream (EOFException)");
        assertTrue(gpdbWritable.isEmpty());
    }

    /*
     * Test the readFields method: first int -1
     */
    @Test
    public void testReadFieldsFirstIntMinusOne() throws Exception {

        GPDBWritable gpdbWritable = buildGPDBWritable();

        int[] firstInt = new int[]{-1};
        buildStream(firstInt, false);

        gpdbWritable.readFields(inputStream);

        verifyLog("Reached end of stream (returned -1)");
        assertTrue(gpdbWritable.isEmpty());
    }

    /*
     * Test the readFields method: first int ok (negative and positive numbers)
     */
    @Test
    public void testReadFieldsFirstIntOK() throws Exception {
        GPDBWritable gpdbWritable = buildGPDBWritable();

        int[] firstInt = new int[]{-2};
        buildStream(firstInt, true);
        when(inputStream.readShort()).thenThrow(new EOFException());

        try {
            gpdbWritable.readFields(inputStream);
        } catch (EOFException e) {
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }

        assertFalse(gpdbWritable.isEmpty()); // len < 0

        firstInt = new int[]{8};
        buildStream(firstInt, true);
        when(inputStream.readShort()).thenThrow(new EOFException());

        try {
            gpdbWritable.readFields(inputStream);
        } catch (EOFException e) {
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }
        assertFalse(gpdbWritable.isEmpty()); // len > 0
    }


    /*
     * helpers functions
     */
    private GPDBWritable buildGPDBWritable()
            throws Exception {
        return new GPDBWritable();
    }

    // add data to stream, end with EOFException on demand.
    private DataInput buildStream(int[] data, boolean throwException) throws Exception {
        inputStream = mock(DataInput.class);
        ongoing = when(inputStream.readInt());
        for (int b : data) {
            ongoing = ongoing.thenReturn(b);
        }

        if (throwException) {
            ongoing.thenThrow(new EOFException());
        }
        return inputStream;
    }

    private void verifyLog(String msg) {
        Mockito.verify(LOG).debug(msg);
    }
}
