package org.apache.hawq.pxf.plugins.hive;

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


import org.apache.hawq.pxf.api.utilities.InputData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.mapred.JobConf;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveDataFragmenter.class}) // Enables mocking 'new' calls
@SuppressStaticInitializationFor({"org.apache.hadoop.mapred.JobConf", 
                                  "org.apache.hadoop.hive.metastore.api.MetaException",
                                  "org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities"}) // Prevents static inits
public class HiveDataFragmenterTest {
    InputData inputData;
    Configuration hadoopConfiguration;
    JobConf jobConf;
    HiveConf hiveConfiguration;
    HiveMetaStoreClient hiveClient;
    HiveDataFragmenter fragmenter;

    @Test
    public void construction() throws Exception {
        prepareConstruction();
        fragmenter = new HiveDataFragmenter(inputData);
        PowerMockito.verifyNew(JobConf.class).withArguments(hadoopConfiguration, HiveDataFragmenter.class);
        PowerMockito.verifyNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration);
    }

    @Test
    public void constructorCantAccessMetaStore() throws Exception {
        prepareConstruction();
        PowerMockito.whenNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration).thenThrow(new MetaException("which way to albuquerque"));

        try {
            fragmenter = new HiveDataFragmenter(inputData);
            fail("Expected a RuntimeException");
        } catch (RuntimeException ex) {
            assertEquals(ex.getMessage(), "Failed connecting to Hive MetaStore service: which way to albuquerque");
        }
    }

    @Test
    public void invalidTableName() throws Exception {
        prepareConstruction();
        fragmenter = new HiveDataFragmenter(inputData);

        when(inputData.getDataSource()).thenReturn("t.r.o.u.b.l.e.m.a.k.e.r");

        try {
            fragmenter.getFragments();
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "\"t.r.o.u.b.l.e.m.a.k.e.r\" is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>");
        }
    }

    private void prepareConstruction() throws Exception {
        inputData = mock(InputData.class);

        hadoopConfiguration = mock(Configuration.class);
        PowerMockito.whenNew(Configuration.class).withNoArguments().thenReturn(hadoopConfiguration);

        jobConf = mock(JobConf.class);
        PowerMockito.whenNew(JobConf.class).withArguments(hadoopConfiguration, HiveDataFragmenter.class).thenReturn(jobConf);

        hiveConfiguration = mock(HiveConf.class);
        PowerMockito.whenNew(HiveConf.class).withNoArguments().thenReturn(hiveConfiguration);

        hiveClient = mock(HiveMetaStoreClient.class);
        PowerMockito.whenNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration).thenReturn(hiveClient);
    }
}
