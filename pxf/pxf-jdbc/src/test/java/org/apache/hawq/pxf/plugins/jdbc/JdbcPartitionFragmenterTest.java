package org.apache.hawq.pxf.plugins.jdbc;

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

import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.jdbc.utils.ByteUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class JdbcPartitionFragmenterTest {
    InputData inputData;

    @Before
    public void setUp() throws Exception {
        prepareConstruction();
        when(inputData.getDataSource()).thenReturn("sales");
    }

    @Test
    public void testPartionByDateOfMonth() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("1:month");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(12, fragments.size());

        //fragment - 1
        byte[] fragMeta = fragments.get(0).getMetadata();
        byte[][] newBytes = ByteUtil.splitBytes(fragMeta);
        long fragStart = ByteUtil.toLong(newBytes[0]);
        long fragEnd = ByteUtil.toLong(newBytes[1]);
        assertDateEquals(fragStart, 2008, 1, 1);
        assertDateEquals(fragEnd, 2008, 2, 1);

        //fragment - 12
        fragMeta = fragments.get(11).getMetadata();
        newBytes = ByteUtil.splitBytes(fragMeta);
        fragStart = ByteUtil.toLong(newBytes[0]);
        fragEnd = ByteUtil.toLong(newBytes[1]);
        assertDateEquals(fragStart, 2008, 12, 1);
        assertDateEquals(fragEnd, 2009, 1, 1);

        //when end_date > start_date
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2001-01-01");
        fragment = new JdbcPartitionFragmenter(inputData);
        fragments = fragment.getFragments();
        assertEquals(0, fragments.size());
    }

    @Test
    public void testPartionByDateOfYear() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2011-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("1:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(3, fragments.size());
    }

    @Test
    public void testPartionByInt() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("year:int");
        when(inputData.getUserProperty("RANGE")).thenReturn("2001:2012");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("2");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(6, fragments.size());

        //fragment - 1
        byte[] fragMeta = fragments.get(0).getMetadata();
        byte[][] newBytes = ByteUtil.splitBytes(fragMeta);
        long fragStart = ByteUtil.toLong(newBytes[0]);
        long fragEnd = ByteUtil.toLong(newBytes[1]);
        assertEquals(2001, fragStart);
        assertEquals(2003, fragEnd);

        //fragment - 6
        fragMeta = fragments.get(5).getMetadata();
        newBytes = ByteUtil.splitBytes(fragMeta);
        fragStart = ByteUtil.toLong(newBytes[0]);
        fragEnd = ByteUtil.toLong(newBytes[1]);
        assertEquals(2011, fragStart);
        assertEquals(2012, fragEnd);

        //when end > start
        when(inputData.getUserProperty("RANGE")).thenReturn("2013:2012");
        fragment = new JdbcPartitionFragmenter(inputData);
        assertEquals(0, fragment.getFragments().size());
    }

    @Test
    public void testPartionByEnum() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("level:enum");
        when(inputData.getUserProperty("RANGE")).thenReturn("excellent:good:general:bad");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(4, fragments.size());

        //fragment - 1
        byte[] fragMeta = fragments.get(0).getMetadata();
        assertEquals("excellent", new String(fragMeta));

        //fragment - 4
        fragMeta = fragments.get(3).getMetadata();
        assertEquals("bad", new String(fragMeta));
    }

    @Test(expected = UserDataException.class)
    public void testInValidPartitiontype() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("level:float");
        when(inputData.getUserProperty("RANGE")).thenReturn("100:200");

        new JdbcPartitionFragmenter(inputData);
    }

    @Test(expected = UserDataException.class)
    public void testInValidParameterFormat() throws Exception {

        //PARTITION_BY must be comma-delimited string
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("level-enum");
        when(inputData.getUserProperty("RANGE")).thenReturn("100:200");

        new JdbcPartitionFragmenter(inputData);
    }

    @Test(expected = UserDataException.class)
    public void testInValidDateFormat() throws Exception {

        //date string must be yyyy-MM-dd
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008/01/01:2009-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("1:month");

        new JdbcPartitionFragmenter(inputData).getFragments();
    }

    @Test(expected = UserDataException.class)
    public void testInValidParameterValue() throws Exception {

        //INTERVAL must be greater than 0
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("-1:month");

        new JdbcPartitionFragmenter(inputData);
    }

    @Test(expected = UserDataException.class)
    public void testInValidIntervaltype() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2011-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("6:hour");

        new JdbcPartitionFragmenter(inputData).getFragments();
    }

    @Test(expected = UserDataException.class)
    public void testIntervaltypeMissing() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2011-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("6");

        new JdbcPartitionFragmenter(inputData).getFragments();
    }

    @Test
    public void testIntervaltypeMissingValid() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("year:int");
        when(inputData.getUserProperty("RANGE")).thenReturn("2001:2012");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("1");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(11, fragments.size());
    }

    @Test
    public void testIntervalMissingEnum() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("level:enum");
        when(inputData.getUserProperty("RANGE")).thenReturn("100:200:300");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(3, fragments.size());
    }

    @Test(expected = UserDataException.class)
    public void testRangeMissingEndValue() throws Exception {
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("1:year");

        new JdbcPartitionFragmenter(inputData).getFragments();
    }

    @Test(expected = UserDataException.class)
    public void testRangeMissing() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("year:int");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("1");

        new JdbcPartitionFragmenter(inputData).getFragments();
    }

    @Test
    public void testRangeSingleValueEnum() throws Exception {

        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("level:enum");
        when(inputData.getUserProperty("RANGE")).thenReturn("100");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(1, fragments.size());
    }

    @Test
    public void testNoPartition() throws Exception {

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(1, fragments.size());
    }

    private void assertDateEquals(long date, int year, int month, int day) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(date);
        assertEquals(calendar.get(Calendar.YEAR), year);
        assertEquals(calendar.get(Calendar.MONTH), month - 1);
        assertEquals(calendar.get(Calendar.DAY_OF_MONTH), day);
    }

    private void prepareConstruction() throws Exception {
        inputData = mock(InputData.class);
    }
}
