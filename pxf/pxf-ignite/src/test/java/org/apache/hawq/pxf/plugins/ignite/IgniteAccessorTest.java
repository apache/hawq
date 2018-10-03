package org.apache.hawq.pxf.plugins.ignite;

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

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import static org.mockito.Matchers.anyString;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.powermock.api.support.membermodification.MemberMatcher.method;


@RunWith(PowerMockRunner.class)
@PrepareForTest({IgniteAccessor.class})
public class IgniteAccessorTest {
    @Before
    public void prepareAccessorTest() throws Exception {
        inputData = Mockito.mock(InputData.class);

        Mockito.when(inputData.getDataSource()).thenReturn("TableTest");

        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("name", DataType.TEXT.getOID(), 1, "text", null));
        columns.add(new ColumnDescriptor("birthday", DataType.DATE.getOID(), 2, "date", null));
        columns.add(new ColumnDescriptor("key", DataType.BYTEA.getOID(), 3, "bytea", null));
        Mockito.when(inputData.getTupleDescription()).thenReturn(columns);
        Mockito.when(inputData.getColumn(0)).thenReturn(columns.get(0));
        Mockito.when(inputData.getColumn(1)).thenReturn(columns.get(1));
        Mockito.when(inputData.getColumn(2)).thenReturn(columns.get(2));
        Mockito.when(inputData.getColumn(3)).thenReturn(columns.get(3));
    }

    @Test
    public void testReadAccess() throws Exception {
        IgniteAccessor acc = PowerMockito.spy(new IgniteAccessor(inputData));

        JsonObject correctAnswer = new JsonObject();
        JsonArray tempArray = new JsonArray();
        JsonArray tempArray2 = new JsonArray();
        tempArray2.add(1);
        tempArray2.add("abcd");
        tempArray2.add("'2001-01-01'");
        tempArray2.add("YWJjZA==");
        tempArray.add(tempArray2);
        correctAnswer.add("items", tempArray);
        correctAnswer.addProperty("last", false);
        correctAnswer.addProperty("queryId", 1);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        PowerMockito.doReturn(correctAnswer).when(acc, "sendRestRequest", anyString());

        acc.openForRead();
        acc.readNextObject();
        acc.closeForRead();

        PowerMockito.verifyPrivate(acc, Mockito.times(3)).invoke(method(IgniteAccessor.class, "sendRestRequest", String.class)).withArguments(captor.capture());

        List<String> allParams = captor.getAllValues();

        assertEquals(allParams.get(0), "http://127.0.0.1:8080/ignite?cmd=qryfldexe&pageSize=0&qry=SELECT+id%2C+name%2C+birthday%2C+key+FROM+TableTest");
        assertEquals(allParams.get(1), "http://127.0.0.1:8080/ignite?cmd=qryfetch&pageSize=128&qryId=1");
        assertEquals(allParams.get(2), "http://127.0.0.1:8080/ignite?cmd=qrycls&qryId=1");
    }

    @Test
    public void testWriteAccess() throws Exception {
        IgniteAccessor acc = PowerMockito.spy(new IgniteAccessor(inputData));

        OneRow insert_row_1 = new OneRow("(1, 'abcd', '2001-01-01', '61626364')");
        OneRow insert_row_2 = new OneRow("(2, 'abcd', '2001-01-01', '61626364')");

        JsonObject correctAnswer = new JsonObject();
        correctAnswer.addProperty("queryId", 1);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        PowerMockito.doReturn(correctAnswer).when(acc, "sendRestRequest", anyString());

        acc.openForWrite();
        acc.writeNextObject(insert_row_1);
        acc.writeNextObject(insert_row_2);
        acc.closeForWrite();

        PowerMockito.verifyPrivate(acc, Mockito.times(4)).invoke(method(IgniteAccessor.class, "sendRestRequest", String.class)).withArguments(captor.capture());

        List<String> allParams = captor.getAllValues();

        assertEquals(allParams.get(0), "http://127.0.0.1:8080/ignite?cmd=qryfldexe&pageSize=0&qry=INSERT+INTO+TableTest%28id%2C+name%2C+birthday%2C+key%29+VALUES+%281%2C+%27abcd%27%2C+%272001-01-01%27%2C+%2761626364%27%29");
        assertEquals(allParams.get(1), "http://127.0.0.1:8080/ignite?cmd=qrycls&qryId=1");
        assertEquals(allParams.get(2), "http://127.0.0.1:8080/ignite?cmd=qryfldexe&pageSize=0&qry=INSERT+INTO+TableTest%28id%2C+name%2C+birthday%2C+key%29+VALUES+%282%2C+%27abcd%27%2C+%272001-01-01%27%2C+%2761626364%27%29");
        assertEquals(allParams.get(3), "http://127.0.0.1:8080/ignite?cmd=qrycls&qryId=1");
    }

    private ArrayList<ColumnDescriptor> columns = new ArrayList<>();
    private InputData inputData = null;
}
