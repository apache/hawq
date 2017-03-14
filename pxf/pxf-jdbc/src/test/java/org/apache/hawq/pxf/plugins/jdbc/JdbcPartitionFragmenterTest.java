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
import org.junit.Test;

import java.text.ParseException;
import java.util.Calendar;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class JdbcPartitionFragmenterTest {
    InputData inputData;

    @Test
    public void testPartionByDateOfMonth() throws Exception {
        prepareConstruction();
        when(inputData.getDataSource()).thenReturn("sales");
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("1:month");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(fragments.size(), 12);

        //fragment - 1
        byte[] fragMeta = fragments.get(0).getMetadata();
        byte[][] newBytes = ByteUtil.splitBytes(fragMeta, 8);
        long fragStart = ByteUtil.toLong(newBytes[0]);
        long fragEnd = ByteUtil.toLong(newBytes[1]);
        assertDateEquals(fragStart, 2008, 1, 1);
        assertDateEquals(fragEnd, 2008, 2, 1);

        //fragment - 12
        fragMeta = fragments.get(11).getMetadata();
        newBytes = ByteUtil.splitBytes(fragMeta, 8);
        fragStart = ByteUtil.toLong(newBytes[0]);
        fragEnd = ByteUtil.toLong(newBytes[1]);
        assertDateEquals(fragStart, 2008, 12, 1);
        assertDateEquals(fragEnd, 2009, 1, 1);

        //when end_date > start_date
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2001-01-01");
        fragment = new JdbcPartitionFragmenter(inputData);
        assertEquals(0, fragment.getFragments().size());
    }

    @Test
    public void testPartionByDateOfYear() throws Exception {
        prepareConstruction();
        when(inputData.getDataSource()).thenReturn("sales");
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2011-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("1:year");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(fragments.size(), 3);
    }

    @Test
    public void testPartionByInt() throws Exception {
        prepareConstruction();
        when(inputData.getDataSource()).thenReturn("sales");
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("year:int");
        when(inputData.getUserProperty("RANGE")).thenReturn("2001:2012");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("2");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(fragments.size(), 6);

        //fragment - 1
        byte[] fragMeta = fragments.get(0).getMetadata();
        byte[][] newBytes = ByteUtil.splitBytes(fragMeta, 4);
        int fragStart = ByteUtil.toInt(newBytes[0]);
        int fragEnd = ByteUtil.toInt(newBytes[1]);
        assertEquals(fragStart, 2001);
        assertEquals(fragEnd, 2003);

        //fragment - 6
        fragMeta = fragments.get(5).getMetadata();
        newBytes = ByteUtil.splitBytes(fragMeta, 4);
        fragStart = ByteUtil.toInt(newBytes[0]);
        fragEnd = ByteUtil.toInt(newBytes[1]);
        assertEquals(fragStart, 2011);
        assertEquals(fragEnd, 2012);

        //when end > start
        when(inputData.getUserProperty("RANGE")).thenReturn("2013:2012");
        fragment = new JdbcPartitionFragmenter(inputData);
        assertEquals(0, fragment.getFragments().size());

    }

    @Test
    public void testPartionByEnum() throws Exception {
        prepareConstruction();
        when(inputData.getDataSource()).thenReturn("sales");
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("level:enum");
        when(inputData.getUserProperty("RANGE")).thenReturn("excellent:good:general:bad");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(fragments.size(), 4);

        //fragment - 1
        byte[] fragMeta = fragments.get(0).getMetadata();
        assertEquals("excellent", new String(fragMeta));

        //fragment - 4
        fragMeta = fragments.get(3).getMetadata();
        assertEquals("bad", new String(fragMeta));
    }

    @Test
    public void inValidPartitiontype() throws Exception {
        prepareConstruction();
        when(inputData.getDataSource()).thenReturn("sales");
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("level:float");
        when(inputData.getUserProperty("RANGE")).thenReturn("100:200");

        try {
            new JdbcPartitionFragmenter(inputData);
            fail("Expected an IllegalArgumentException");
        } catch (UserDataException ex) {

        }
    }

    @Test
    public void inValidParameterFormat() throws Exception {
        prepareConstruction();
        when(inputData.getDataSource()).thenReturn("sales");

        //PARTITION_BY must be comma-delimited string
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("level-enum");
        when(inputData.getUserProperty("RANGE")).thenReturn("100:200");
        try {
            new JdbcPartitionFragmenter(inputData);
            fail("Expected an ArrayIndexOutOfBoundsException");
        } catch (ArrayIndexOutOfBoundsException ex) {
        }

        //date string must be yyyy-MM-dd
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008/01/01:2009-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("1:month");
        try {
            JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
            fragment.getFragments();
            fail("Expected an ParseException");
        } catch (UserDataException ex) {
        }
    }

    @Test
    public void inValidParameterValue() throws Exception {
        prepareConstruction();
        //INTERVAL must be greater than 0
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("-1:month");
        try {
            new JdbcPartitionFragmenter(inputData);
            fail("Expected an UserDataException");
        } catch (UserDataException ex) {
        }
    }

    @Test
    public void inValidIntervaltype() throws Exception {
        prepareConstruction();
        when(inputData.getDataSource()).thenReturn("sales");
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2011-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("6:hour");

        try {
            JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
            fragment.getFragments();
            fail("Expected an UserDataException");
        } catch (UserDataException ex) {
        }
    }

    @Test
    public void testNoPartition() throws Exception {
        prepareConstruction();
        when(inputData.getDataSource()).thenReturn("sales");

        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(fragments.size(), 1);
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
