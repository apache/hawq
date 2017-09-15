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


import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.BasicFilter;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import static org.apache.hawq.pxf.api.FilterParser.Operation.*;

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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

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

    @Test
    public void testBuildSingleFilter() throws Exception {
        prepareConstruction();
        fragmenter = new HiveDataFragmenter(inputData);
        ColumnDescriptor columnDescriptor =
                new ColumnDescriptor("textColumn", 25, 3, "text", null,true);
        String filterColumnName=columnDescriptor.columnName();
        int filterColumnIndex = columnDescriptor.columnIndex();
        HiveFilterBuilder builder = new HiveFilterBuilder(null);
        when(inputData.getColumn(filterColumnIndex)).thenReturn(columnDescriptor);

        // Mock private field partitionkeyTypes
        Field partitionkeyTypes = PowerMockito.field(HiveDataFragmenter.class, "partitionkeyTypes");
        Map<String, String> localpartitionkeyTypes = new HashMap<>();
        localpartitionkeyTypes.put(filterColumnName,"string");
        partitionkeyTypes.set(fragmenter,localpartitionkeyTypes);

        //Mock private field setPartitions
        Field setPartitions = PowerMockito.field(HiveDataFragmenter.class, "setPartitions");
        Set<String> localSetPartitions = new TreeSet<String>(
                String.CASE_INSENSITIVE_ORDER);
        localSetPartitions.add(filterColumnName);
        setPartitions.set(fragmenter,localSetPartitions);

        Map<FilterParser.Operation, String> filterStrings = new HashMap<>();
        /*
         * Filter string representation in the respective order of their declaration
            testColumn != 2016-01-03
            testColumn = 2016-01-03
            testColumn >= 2016-01-03
            testColumn <= 2016-01-03
            testColumn >= 2016-01-03
            testColumn < 2016-01-03
            testColumn like '2016-01-0%'
         */
        filterStrings.put(HDOP_NE, "a3c25s10d2016-01-03o6");
        filterStrings.put(HDOP_EQ, "a3c25s10d2016-01-03o5");
        filterStrings.put(HDOP_GE, "a3c25s10d2016-01-03o4");
        filterStrings.put(HDOP_LE, "a3c25s10d2016-01-03o3");
        filterStrings.put(HDOP_GT, "a3c25s10d2016-01-03o2");
        filterStrings.put(HDOP_LT, "a3c25s10d2016-01-03o1");
        filterStrings.put(HDOP_LIKE, "a3c25s10d2016-01-0%o7");

        for (FilterParser.Operation operation : filterStrings.keySet()){
            BasicFilter bFilter = (BasicFilter) builder.getFilterObject(filterStrings.get(operation));
            checkFilters(fragmenter,bFilter, operation);
        }
    }

    @Test
    public void testIntegralPushdown() throws Exception {
        prepareConstruction();
        fragmenter = new HiveDataFragmenter(inputData);
        // Mock private field partitionkeyTypes
        Field partitionkeyTypes = PowerMockito.field(HiveDataFragmenter.class, "partitionkeyTypes");
        // Mock private method buildSingleFilter
        Method method = PowerMockito.method(HiveDataFragmenter.class, "buildSingleFilter",
                new Class[]{Object.class,StringBuilder.class,String.class});
        //Mock private field setPartitions
        Field setPartitions = PowerMockito.field(HiveDataFragmenter.class, "setPartitions");
        //Mock private field canPushDownIntegral
        Field canPushDownIntegral = PowerMockito.field(HiveDataFragmenter.class, "canPushDownIntegral");
        canPushDownIntegral.set(fragmenter,true);

        ColumnDescriptor dateColumnDescriptor =
                new ColumnDescriptor("dateColumn", 1082, 1, "date", null, true);
        ColumnDescriptor stringColumnDescriptor =
                new ColumnDescriptor("stringColumn", 25, 1, "string", null, true);
        ColumnDescriptor intColumnDescriptor =
                new ColumnDescriptor("intColumn", 23, 1, "int", null, true);
        ColumnDescriptor bigIntColumnDescriptor =
                new ColumnDescriptor("bigIntColumn", 20, 1, "bigint", null, true);
        ColumnDescriptor smallIntColumnDescriptor =
                new ColumnDescriptor("smallIntColumn", 21, 1, "smallint", null, true);
        List<ColumnDescriptor> columnDescriptors = new ArrayList<>();

        columnDescriptors.add(dateColumnDescriptor);
        columnDescriptors.add(stringColumnDescriptor);
        columnDescriptors.add(intColumnDescriptor);
        columnDescriptors.add(bigIntColumnDescriptor);
        columnDescriptors.add(smallIntColumnDescriptor);

        for (ColumnDescriptor cd : columnDescriptors){

            checkPushDownFilter(fragmenter, cd, method, partitionkeyTypes,setPartitions);
        }
    }

    private void checkPushDownFilter(HiveDataFragmenter fragmenter, ColumnDescriptor columnDescriptor, Method method,
                                     Field partitionkeyTypes, Field setPartitions) throws Exception{
        String filterColumnName=columnDescriptor.columnName();
        int filterColumnIndex = columnDescriptor.columnIndex();
        String typeName = columnDescriptor.columnTypeName();
        int typeCode = columnDescriptor.columnTypeCode();
        String data = "2016-08-11";
        String dataIntegralDataTypes= "126";
        String filterString = "a"+filterColumnIndex+"c"+typeCode+"s"+data.length()+"d"+data+"o";
        String filterIntegralDataTypes =
                "a"+filterColumnIndex+"c"+typeCode+"s"+dataIntegralDataTypes.length()+"d"+dataIntegralDataTypes+"o";
        int notEquals=6;
        int equals=5;
        int greaterEquals=4;

        when(inputData.getColumn(filterColumnIndex)).thenReturn(columnDescriptor);
        //Set partition Key type
        Map<String, String> localpartitionkeyTypes = new HashMap<>();
        localpartitionkeyTypes.put(filterColumnName,typeName);
        partitionkeyTypes.set(fragmenter,localpartitionkeyTypes);
        // Set column as partition
        Set<String> localSetPartitions = new TreeSet<String>(
                String.CASE_INSENSITIVE_ORDER);
        localSetPartitions.add(filterColumnName);
        setPartitions.set(fragmenter,localSetPartitions);

        switch(typeName){

            case "date":
                assertFalse(isColumnStringOrIntegral(method, filterString+notEquals));
                assertFalse(isColumnStringOrIntegral(method, filterString+equals));
                assertFalse(isColumnStringOrIntegral(method, filterString+greaterEquals));
                break;
            case "string":
                assertTrue(isColumnStringOrIntegral(method, filterString+notEquals));
                assertTrue(isColumnStringOrIntegral(method, filterString+equals));
                assertTrue(isColumnStringOrIntegral(method, filterString+greaterEquals));
                break;
            case "int":
                assertTrue(isColumnStringOrIntegral(method, filterIntegralDataTypes+notEquals));
                assertTrue(isColumnStringOrIntegral(method, filterIntegralDataTypes+equals));
                assertFalse(isColumnStringOrIntegral(method, filterIntegralDataTypes+greaterEquals));
                break;
            case "bigint":
                assertTrue(isColumnStringOrIntegral(method, filterIntegralDataTypes+notEquals));
                assertTrue(isColumnStringOrIntegral(method, filterIntegralDataTypes+equals));
                assertFalse(isColumnStringOrIntegral(method, filterIntegralDataTypes+greaterEquals));
                break;
            case "smallint":
                assertTrue(isColumnStringOrIntegral(method, filterIntegralDataTypes+notEquals));
                assertTrue(isColumnStringOrIntegral(method, filterIntegralDataTypes+equals));
                assertFalse(isColumnStringOrIntegral(method, filterIntegralDataTypes+greaterEquals));
                break;
        }
    }

    private boolean isColumnStringOrIntegral(Method method, String filterString) throws Exception{
        BasicFilter bFilter;
        String prefix="";
        StringBuilder localFilterString = new StringBuilder();
        boolean result;
        HiveFilterBuilder builder = new HiveFilterBuilder(null);

        bFilter = (BasicFilter) builder.getFilterObject(filterString);
        result = (Boolean)method.invoke(fragmenter, new Object[]{bFilter,localFilterString,prefix});
        return result;
    }

    private void checkFilters(HiveDataFragmenter fragmenter, BasicFilter bFilter, FilterParser.Operation operation)
            throws Exception{

        String prefix="";
        StringBuilder localFilterString = new StringBuilder();
        String expectedResult;

        // Mock private method buildSingleFilter
        Method method = PowerMockito.method(HiveDataFragmenter.class, "buildSingleFilter",
                new Class[]{Object.class,StringBuilder.class,String.class});
        boolean result = (Boolean)method.invoke(fragmenter, new Object[]{bFilter,localFilterString,prefix});

        switch (operation){
            case HDOP_NE:
                expectedResult = "textColumn != \"2016-01-03\"";
                assertTrue(result);
                assertEquals(expectedResult,localFilterString.toString());
                break;
            case HDOP_EQ:
                expectedResult = "textColumn = \"2016-01-03\"";
                assertTrue(result);
                assertEquals(expectedResult,localFilterString.toString());
                break;
            case HDOP_GE:
                expectedResult = "textColumn >= \"2016-01-03\"";
                assertTrue(result);
                assertEquals(expectedResult,localFilterString.toString());
                break;
            case HDOP_LE:
                expectedResult = "textColumn <= \"2016-01-03\"";
                assertTrue(result);
                assertEquals(expectedResult,localFilterString.toString());
                break;
            case HDOP_GT:
                expectedResult = "textColumn > \"2016-01-03\"";
                assertTrue(result);
                assertEquals(expectedResult,localFilterString.toString());
                break;
            case HDOP_LT:
                expectedResult = "textColumn < \"2016-01-03\"";
                assertTrue(result);
                assertEquals(expectedResult,localFilterString.toString());
                break;
            case HDOP_LIKE:
                expectedResult = "";
                assertFalse(result);
                assertEquals(expectedResult,localFilterString.toString());
                break;
            default:
                assertFalse(result);
                break;
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
