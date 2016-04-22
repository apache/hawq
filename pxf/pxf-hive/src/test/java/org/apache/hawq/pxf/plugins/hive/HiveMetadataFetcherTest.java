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


import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import org.apache.hawq.pxf.api.Metadata;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveMetadataFetcher.class}) // Enables mocking 'new' calls
@SuppressStaticInitializationFor({"org.apache.hadoop.hive.metastore.api.MetaException",
"org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities"}) // Prevents static inits
public class HiveMetadataFetcherTest {
    InputData inputData;
    Log LOG;
    HiveConf hiveConfiguration;
    HiveMetaStoreClient hiveClient;
    HiveMetadataFetcher fetcher;
    List<Metadata> metadataList;

    @Before
    public void SetupCompressionFactory() {
        LOG = mock(Log.class);
        Whitebox.setInternalState(HiveUtilities.class, LOG);
    }

    @Test
    public void construction() throws Exception {
        prepareConstruction();
        fetcher = new HiveMetadataFetcher(inputData);
        PowerMockito.verifyNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration);
    }

    @Test
    public void constructorCantAccessMetaStore() throws Exception {
        prepareConstruction();
        PowerMockito.whenNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration).thenThrow(new MetaException("which way to albuquerque"));

        try {
            fetcher = new HiveMetadataFetcher(inputData);
            fail("Expected a RuntimeException");
        } catch (RuntimeException ex) {
            assertEquals("Failed connecting to Hive MetaStore service: which way to albuquerque", ex.getMessage());
        }
    }

    @Test
    public void getTableMetadataInvalidTableName() throws Exception {
        prepareConstruction();
        fetcher = new HiveMetadataFetcher(inputData);
        String tableName = "t.r.o.u.b.l.e.m.a.k.e.r";

        try {
            fetcher.getMetadata(tableName);
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertEquals("\"t.r.o.u.b.l.e.m.a.k.e.r\" is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>", ex.getMessage()); 
        }
    }

    @Test
    public void getTableMetadataView() throws Exception {
        prepareConstruction();

        fetcher = new HiveMetadataFetcher(inputData);
        String tableName = "cause";

        // mock hive table returned from hive client
        Table hiveTable = new Table();
        hiveTable.setTableType("VIRTUAL_VIEW");
        when(hiveClient.getTable("default", tableName)).thenReturn(hiveTable);

        try {
            metadataList = fetcher.getMetadata(tableName);
            fail("Expected an UnsupportedOperationException because PXF doesn't support views");
        } catch (UnsupportedOperationException e) {
            assertEquals("Hive views are not supported by HAWQ", e.getMessage());
        }
    }

    @Test
    public void getTableMetadata() throws Exception {
        prepareConstruction();

        fetcher = new HiveMetadataFetcher(inputData);
        String tableName = "cause";

        // mock hive table returned from hive client
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("field1", "string", null));
        fields.add(new FieldSchema("field2", "int", null));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);
        Table hiveTable = new Table();
        hiveTable.setTableType("MANAGED_TABLE");
        hiveTable.setSd(sd);
        hiveTable.setPartitionKeys(new ArrayList<FieldSchema>());
        when(hiveClient.getTable("default", tableName)).thenReturn(hiveTable);

        // Get metadata
        metadataList = fetcher.getMetadata(tableName);
        Metadata metadata = metadataList.get(0);

        assertEquals("default.cause", metadata.getItem().toString());

        List<Metadata.Field> resultFields = metadata.getFields();
        assertNotNull(resultFields);
        assertEquals(2, resultFields.size());
        Metadata.Field field = resultFields.get(0);
        assertEquals("field1", field.getName());
        assertEquals("text", field.getType().getTypeName()); // converted type
        field = resultFields.get(1);
        assertEquals("field2", field.getName());
        assertEquals("int4", field.getType().getTypeName());
    }

    @Test
    public void getTableMetadataWithMultipleTables() throws Exception {
        prepareConstruction();

        fetcher = new HiveMetadataFetcher(inputData);

        String tablepattern = "*";
        String dbpattern = "*";
        String dbname = "default";
        String tablenamebase = "regulartable";
        String pattern = dbpattern + "." + tablepattern;

        List<String> dbNames = new ArrayList<String>(Arrays.asList(dbname));
        List<String> tableNames = new ArrayList<String>();

        // Prepare for tables
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("field1", "string", null));
        fields.add(new FieldSchema("field2", "int", null));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);

        // Mock hive tables returned from hive client
        for(int index=1;index<=2;index++) {
            String tableName = tablenamebase + index;
            tableNames.add(tableName);;
            Table hiveTable = new Table();
            hiveTable.setTableType("MANAGED_TABLE");
            hiveTable.setSd(sd);
            hiveTable.setPartitionKeys(new ArrayList<FieldSchema>());
            when(hiveClient.getTable(dbname, tableName)).thenReturn(hiveTable);
        }

        // Mock database and table names return from hive client
        when(hiveClient.getDatabases(dbpattern)).thenReturn(dbNames);
        when(hiveClient.getTables(dbname, tablepattern)).thenReturn(tableNames);

        // Get metadata
        metadataList = fetcher.getMetadata(pattern);
        assertEquals(2, metadataList.size());

        for(int index=1;index<=2;index++) {
            Metadata metadata = metadataList.get(index-1);
            assertEquals(dbname + "." + tablenamebase+index, metadata.getItem().toString());
            List<Metadata.Field> resultFields = metadata.getFields();
            assertNotNull(resultFields);
            assertEquals(2, resultFields.size());
            Metadata.Field field = resultFields.get(0);
            assertEquals("field1", field.getName());
            assertEquals("text", field.getType().getTypeName()); // converted type
            field = resultFields.get(1);
            assertEquals("field2", field.getName());
            assertEquals("int4", field.getType().getTypeName());
        }
    }

    @Test
    public void getTableMetadataWithIncompatibleTables() throws Exception {
        prepareConstruction();

        fetcher = new HiveMetadataFetcher(inputData);

        String tablepattern = "*";
        String dbpattern = "*";
        String dbname = "default";
        String pattern = dbpattern + "." + tablepattern;

        String tableName1 = "viewtable";
        // mock hive table returned from hive client
        Table hiveTable1 = new Table();
        hiveTable1.setTableType("VIRTUAL_VIEW");
        when(hiveClient.getTable(dbname, tableName1)).thenReturn(hiveTable1);

        String tableName2 = "regulartable";
        // mock hive table returned from hive client
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("field1", "string", null));
        fields.add(new FieldSchema("field2", "int", null));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(fields);
        Table hiveTable2 = new Table();
        hiveTable2.setTableType("MANAGED_TABLE");
        hiveTable2.setSd(sd);
        hiveTable2.setPartitionKeys(new ArrayList<FieldSchema>());
        when(hiveClient.getTable(dbname, tableName2)).thenReturn(hiveTable2);

        // Mock get databases and tables return from hive client
        List<String> tableNames = new ArrayList<String>(Arrays.asList(tableName1, tableName2));
        List<String> dbNames = new ArrayList<String>(Arrays.asList(dbname));
        when(hiveClient.getDatabases(dbpattern)).thenReturn(dbNames);
        when(hiveClient.getTables(dbname, tablepattern)).thenReturn(tableNames);

        // Get metadata
        metadataList = fetcher.getMetadata(pattern);
        assertEquals(1, metadataList.size());
        Metadata metadata = metadataList.get(0);
        assertEquals(dbname + "." + tableName2, metadata.getItem().toString());

        List<Metadata.Field> resultFields = metadata.getFields();
        assertNotNull(resultFields);
        assertEquals(2, resultFields.size());
        Metadata.Field field = resultFields.get(0);
        assertEquals("field1", field.getName());
        assertEquals("text", field.getType().getTypeName()); // converted type
        field = resultFields.get(1);
        assertEquals("field2", field.getName());
        assertEquals("int4", field.getType().getTypeName());
    }

    private void prepareConstruction() throws Exception {
        hiveConfiguration = mock(HiveConf.class);
        PowerMockito.whenNew(HiveConf.class).withNoArguments().thenReturn(hiveConfiguration);

        hiveClient = mock(HiveMetaStoreClient.class);
        PowerMockito.whenNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration).thenReturn(hiveClient);
    }
}
