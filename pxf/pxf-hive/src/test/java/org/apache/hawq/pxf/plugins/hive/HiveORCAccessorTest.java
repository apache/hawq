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

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.mapred.*;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.EnumAggregationType;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities.PXF_HIVE_SERDES;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory.SARG_PUSHDOWN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveORCAccessor.class, HiveUtilities.class, HdfsUtilities.class, HiveDataFragmenter.class})
@SuppressStaticInitializationFor({"org.apache.hadoop.mapred.JobConf",
        "org.apache.hadoop.hive.metastore.api.MetaException",
        "org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities"}) // Prevents static inits
public class HiveORCAccessorTest {

    @Mock InputData inputData;
    @Mock OrcInputFormat orcInputFormat;
    @Mock InputFormat inputFormat;
    @Mock ColumnDescriptor columnDesc;
    @Mock Reader orcReader;
    JobConf jobConf;
    HiveORCAccessor accessor;

    @Before
    public void setup() throws Exception {
        jobConf = new JobConf();
        PowerMockito.whenNew(JobConf.class).withAnyArguments().thenReturn(jobConf);

        PowerMockito.mockStatic(HiveUtilities.class);
        PowerMockito.when(HiveUtilities.parseHiveUserData(any(InputData.class), any(PXF_HIVE_SERDES[].class))).thenReturn(new HiveUserData("", "", null, HiveDataFragmenter.HIVE_NO_PART_TBL, true, "1", ""));
        PowerMockito.when(HiveUtilities.getOrcReader(any(InputData.class))).thenReturn(orcReader);

        PowerMockito.mockStatic(HdfsUtilities.class);

        PowerMockito.mockStatic(HiveDataFragmenter.class);
        PowerMockito.when(HiveDataFragmenter.makeInputFormat(any(String.class), any(JobConf.class))).thenReturn(inputFormat);

        PowerMockito.whenNew(OrcInputFormat.class).withNoArguments().thenReturn(orcInputFormat);
        RecordReader recordReader = mock(RecordReader.class);
        PowerMockito.when(orcInputFormat.getRecordReader(any(InputSplit.class), any(JobConf.class), any(Reporter.class))).thenReturn(recordReader);

        accessor = new HiveORCAccessor(inputData);
    }

    @Test
    public void parseFilterWithISNULL() throws Exception {

        when(inputData.hasFilter()).thenReturn(true);
        when(inputData.getFilterString()).thenReturn("a1o8");
        when(columnDesc.columnName()).thenReturn("FOO");
        when(inputData.getColumn(1)).thenReturn(columnDesc);

        accessor.openForRead();

        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd().isNull("FOO").end().build();
        assertEquals(sarg.toKryo(), jobConf.get(SARG_PUSHDOWN));
    }

    @Test
    public void parseFilterWithISNOTNULL() throws Exception {

        when(inputData.hasFilter()).thenReturn(true);
        when(inputData.getFilterString()).thenReturn("a1o9");
        when(columnDesc.columnName()).thenReturn("FOO");
        when(inputData.getColumn(1)).thenReturn(columnDesc);

        accessor.openForRead();

        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd().startNot().isNull("FOO").end().end().build();
        assertEquals(sarg.toKryo(), jobConf.get(SARG_PUSHDOWN));
    }

    @Test
    public void parseFilterWithIn() throws Exception {

        when(inputData.hasFilter()).thenReturn(true);
        when(inputData.getFilterString()).thenReturn("a1m1007s1d1s1d2s1d3o10");
        when(columnDesc.columnName()).thenReturn("FOO");
        when(inputData.getColumn(1)).thenReturn(columnDesc);

        accessor.openForRead();

        SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd().in("FOO", 1, 2, 3).end().build();

        assertEquals(sarg.toKryo(), jobConf.get(SARG_PUSHDOWN));
    }

    @Test(expected=IllegalStateException.class)
    public void emitAggObjectCountStatsNotInitialized() {
        accessor.emitAggObject();
    }

}