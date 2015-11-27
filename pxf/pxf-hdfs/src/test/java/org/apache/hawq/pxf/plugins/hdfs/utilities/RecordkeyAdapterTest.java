package org.apache.hawq.pxf.plugins.hdfs.utilities;

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
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RecordkeyAdapter.class, LogFactory.class})
public class RecordkeyAdapterTest {
    Log Log;
    RecordkeyAdapter recordkeyAdapter;

    /**
     * Test convertKeyValue for Integer type
     */
    @Test
    public void convertKeyValueInteger() {
        int key = 13;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new IntWritable(key));
    }

    /**
     * Test convertKeyValue for Boolean type
     */
    @Test
    public void convertKeyValueBoolean() {
        boolean key = true;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new BooleanWritable(key));
    }

    /**
     * Test convertKeyValue for Byte type
     */
    @Test
    public void convertKeyValueByte() {
        byte key = 1;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new ByteWritable(key));
    }

    /**
     * Test convertKeyValue for Double type
     */
    @Test
    public void convertKeyValueDouble() {
        double key = 2.3;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new DoubleWritable(key));
    }

    /**
     * Test convertKeyValue for Float type
     */
    @Test
    public void convertKeyValueFloat() {
        float key = (float) 2.3;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new FloatWritable(key));
    }

    /**
     * Test convertKeyValue for Long type
     */
    @Test
    public void convertKeyValueLong() {
        long key = 12345678901234567l;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new LongWritable(key));
    }

    /**
     * Test convertKeyValue for String type
     */
    @Test
    public void convertKeyValueString() {
        String key = "key";
        initRecordkeyAdapter();
        runConvertKeyValue(key, new Text(key));
    }

    /**
     * Test convertKeyValue for several calls of the same type
     */
    @Test
    public void convertKeyValueManyCalls() {
        Boolean key = true;
        mockLog();
        initRecordkeyAdapter();
        runConvertKeyValue(key, new BooleanWritable(key));
        verifyLog("converter initialized for type " + key.getClass() +
                " (key value: " + key + ")");

        for (int i = 0; i < 5; ++i) {
            key = (i % 2) == 0;
            runConvertKeyValue(key, new BooleanWritable(key));
        }
        verifyLogOnlyOnce();
    }

    /**
     * Test convertKeyValue for boolean type and then string type - negative
     * test
     */
    @Test
    public void convertKeyValueBadSecondValue() {
        boolean key = true;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new BooleanWritable(key));
        String badKey = "bad";
        try {
            recordkeyAdapter.convertKeyValue(badKey);
            fail("conversion of string to boolean should fail");
        } catch (ClassCastException e) {
            assertEquals(e.getMessage(),
                    "java.lang.String cannot be cast to java.lang.Boolean");
        }
    }

    private void initRecordkeyAdapter() {
        recordkeyAdapter = new RecordkeyAdapter();
    }

    private void runConvertKeyValue(Object key, Writable expected) {
        Writable writable = recordkeyAdapter.convertKeyValue(key);
        assertEquals(writable, expected);
    }

    private void mockLog() {
        PowerMockito.mockStatic(LogFactory.class);
        Log = mock(Log.class);
        when(LogFactory.getLog(RecordkeyAdapter.class)).thenReturn(Log);
    }

    private void verifyLog(String msg) {
        Mockito.verify(Log).debug(msg);
    }

    private void verifyLogOnlyOnce() {
        Mockito.verify(Log, Mockito.times(1)).debug(Mockito.any());
    }
}
