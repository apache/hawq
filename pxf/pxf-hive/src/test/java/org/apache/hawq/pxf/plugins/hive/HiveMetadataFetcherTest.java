package org.apache.hawq.pxf.plugins.hive;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
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

    Log LOG;
    HiveConf hiveConfiguration;
    HiveMetaStoreClient hiveClient;
    HiveMetadataFetcher fetcher;
    Metadata metadata;

    @Before
    public void SetupCompressionFactory() {
        LOG = mock(Log.class);
        Whitebox.setInternalState(HiveUtilities.class, LOG);
    }

    @Test
    public void construction() throws Exception {
        prepareConstruction();
        fetcher = new HiveMetadataFetcher();
        PowerMockito.verifyNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration);
    }

    @Test
    public void constructorCantAccessMetaStore() throws Exception {
        prepareConstruction();
        PowerMockito.whenNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration).thenThrow(new MetaException("which way to albuquerque"));

        try {
            fetcher = new HiveMetadataFetcher();
            fail("Expected a RuntimeException");
        } catch (RuntimeException ex) {
            assertEquals("Failed connecting to Hive MetaStore service: which way to albuquerque", ex.getMessage());
        }
    }

    @Test
    public void getTableMetadataInvalidTableName() throws Exception {
        prepareConstruction();
        fetcher = new HiveMetadataFetcher();
        String tableName = "t.r.o.u.b.l.e.m.a.k.e.r";

        try {
            fetcher.getTableMetadata(tableName);
            fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            assertEquals("\"t.r.o.u.b.l.e.m.a.k.e.r\" is not a valid Hive table name. Should be either <table_name> or <db_name.table_name>", ex.getMessage()); 
        }
    }

    @Test
    public void getTableMetadataView() throws Exception {
        prepareConstruction();

        fetcher = new HiveMetadataFetcher();
        String tableName = "cause";

        // mock hive table returned from hive client
        Table hiveTable = new Table();
        hiveTable.setTableType("VIRTUAL_VIEW");
        when(hiveClient.getTable("default", tableName)).thenReturn(hiveTable);

        try {
            metadata = fetcher.getTableMetadata(tableName);
            fail("Expected an UnsupportedOperationException because PXF doesn't support views");
        } catch (UnsupportedOperationException e) {
            assertEquals("Hive views are not supported by HAWQ", e.getMessage());
        }
    }

    @Test
    public void getTableMetadata() throws Exception {
        prepareConstruction();

        fetcher = new HiveMetadataFetcher();
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

        // get metadata
        metadata = fetcher.getTableMetadata(tableName);

        assertEquals("default.cause", metadata.getTable().toString());

        List<Metadata.Field> resultFields = metadata.getFields();
        assertNotNull(resultFields);
        assertEquals(2, resultFields.size());
        Metadata.Field field = resultFields.get(0);
        assertEquals("field1", field.getName());
        assertEquals("text", field.getType()); // converted type
        field = resultFields.get(1);
        assertEquals("field2", field.getName());
        assertEquals("int4", field.getType());
    }

    private void prepareConstruction() throws Exception {
        hiveConfiguration = mock(HiveConf.class);
        PowerMockito.whenNew(HiveConf.class).withNoArguments().thenReturn(hiveConfiguration);

        hiveClient = mock(HiveMetaStoreClient.class);
        PowerMockito.whenNew(HiveMetaStoreClient.class).withArguments(hiveConfiguration).thenReturn(hiveClient);
    }
}
