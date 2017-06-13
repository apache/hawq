/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hawq.ranger.service;

import org.apache.ranger.plugin.client.BaseClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.hawq.ranger.service.HawqClient.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(HawqClient.class)
public class HawqClientTest {

    @Mock
    public Connection conn;
    @Mock
    public PreparedStatement preparedStatement;
    @Mock
    public ResultSet resultSet;

    public HawqClient hawqClientSpy;

    public HawqClient hawqClient;
    private Map<String, String> connectionProperties;
    private Map<String, List<String>> resources;

    @Before
    public void setUp() throws Exception {
        connectionProperties = new HashMap<>();
        connectionProperties.put("hostname", "hostname");
        connectionProperties.put("port", "5432");
        connectionProperties.put("hostname", "hostname");
        connectionProperties.put("username", "username");
        connectionProperties.put("password", "password");

        mockStatic(DriverManager.class);
        suppress(constructor(BaseClient.class, String.class, Map.class));
        hawqClient = new HawqClient("hawq", connectionProperties);

        hawqClientSpy = PowerMockito.spy(hawqClient);

        resources = new HashMap<>();
        List<String> dbs = Arrays.asList("db1", "db2");
        List<String> schemas = Arrays.asList("schema1", "schema2");
        resources.put("database", dbs);
        resources.put("schema", schemas);
    }

    @Test
    public void testCheckConnection_Failure() throws Exception {
        when(DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(conn);
        when(conn.getCatalog()).thenReturn(null);
        Map<String, Object> response = hawqClient.checkConnection(connectionProperties);
        assertEquals(CONNECTION_FAILURE_MESSAGE, response.get("message"));
        assertFalse((Boolean) response.get("connectivityStatus"));
    }

    @Test
    public void testCheckConnection_Success() throws Exception {
        when(DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(conn);
        when(conn.getCatalog()).thenReturn("catalog");
        Map<String, Object> response = hawqClient.checkConnection(connectionProperties);
        assertEquals(CONNECTION_SUCCESSFUL_MESSAGE, response.get("message"));
        assertTrue((Boolean) response.get("connectivityStatus"));
    }

    @Test
    public void testCheckConnection_ThrowsSQLException_Failure() throws Exception {
        when(DriverManager.getConnection(anyString(), any(Properties.class))).thenThrow(new SQLException("Failed to connect"));
        Map<String, Object> response = hawqClient.checkConnection(connectionProperties);
        assertEquals(CONNECTION_FAILURE_MESSAGE, response.get("message"));
        assertEquals("Failed to connect", response.get("description"));
        assertFalse((Boolean) response.get("connectivityStatus"));
    }

    @Test
    public void testDatabaseList_Success() throws Exception {
        when(conn.prepareStatement(DATABASE_LIST_QUERY)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSet.getString(DATNAME)).thenReturn("db1").thenReturn("db2");
        PowerMockito.doReturn(conn).when(hawqClientSpy, "getConnection", anyMap(), anyString());
        assertEquals(Arrays.asList("db1", "db2"), hawqClientSpy.getDatabaseList("d"));
    }

    @Test
    public void testTablespaceList_Success() throws Exception {
        doReturn(Arrays.asList("tablespace1", "tablespace2")).when(hawqClientSpy, "queryHawq", "t", SPCNAME, TABLESPACE_LIST_QUERY, null);
        assertEquals(Arrays.asList("tablespace1", "tablespace2"), hawqClientSpy.getTablespaceList("t"));
    }

    @Test
    public void testProtocolList_Success() throws Exception {
        List<String> expectedResult = Arrays.asList("protocol1", "protocol2", "pxf");
        doReturn(Arrays.asList("protocol1", "protocol2")).when(hawqClientSpy, "queryHawq", "p", PTCNAME, PROTOCOL_LIST_QUERY, null);
        List<String> result = hawqClientSpy.getProtocolList("p");
        Collections.sort(result);
        assertEquals(expectedResult, result);
    }

    @Test
    public void testSchemaList_MultipleDb_Success() throws Exception {
        doReturn(Arrays.asList("schema1")).when(hawqClientSpy, "queryHawq", "s", SCHEMA_NAME, SCHEMA_LIST_QUERY, "db1");
        doReturn(Arrays.asList("schema2")).when(hawqClientSpy, "queryHawq", "s", SCHEMA_NAME, SCHEMA_LIST_QUERY, "db2");
        List<String> result = hawqClientSpy.getSchemaList("s", resources);
        Collections.sort(result);
        assertEquals(Arrays.asList("schema1", "schema2"), result);
    }

    @Test
    public void testSchemaList_AllDb_Success() throws Exception {
        resources.put("database", Arrays.asList(WILDCARD));
        doReturn(Arrays.asList("db1", "db2", "db3")).when(hawqClientSpy, "getDatabaseList", WILDCARD);
        doReturn(Arrays.asList("schema1")).when(hawqClientSpy, "queryHawq", "s", SCHEMA_NAME, SCHEMA_LIST_QUERY, "db1");
        doReturn(Arrays.asList("schema2")).when(hawqClientSpy, "queryHawq", "s", SCHEMA_NAME, SCHEMA_LIST_QUERY, "db2");
        doReturn(Arrays.asList("schema3")).when(hawqClientSpy, "queryHawq", "s", SCHEMA_NAME, SCHEMA_LIST_QUERY, "db3");
        List<String> result = hawqClientSpy.getSchemaList("s", resources);
        Collections.sort(result);
        assertEquals(Arrays.asList("schema1", "schema2", "schema3"), result);
    }

    @Test
    public void testLanguageList_Success() throws Exception {
        doReturn(Arrays.asList("language1")).when(hawqClientSpy, "queryHawq", "l", LANGUAGE_NAME, LANGUAGE_LIST_QUERY, "db1");
        doReturn(Arrays.asList("language2")).when(hawqClientSpy, "queryHawq", "l", LANGUAGE_NAME, LANGUAGE_LIST_QUERY, "db2");
        List<String> result = hawqClientSpy.getLanguageList("l", resources);
        Collections.sort(result);
        assertEquals(Arrays.asList("language1", "language2"), result);
    }

    @Test
    public void testTableList_Success() throws Exception {
        PowerMockito.doReturn(conn).when(hawqClientSpy, "getConnection", anyMap(), anyString());
        when(conn.prepareStatement(TABLE_LIST_QUERY)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSet.getString(RELNAME)).thenReturn("table1").thenReturn("table2");
        when(resultSet.getString(NSPNAME)).thenReturn("schema1").thenReturn("schema2");
        List<String> result = hawqClientSpy.getTableList("t", resources);
        Collections.sort(result);
        assertEquals(Arrays.asList("table1", "table2"), result);
    }

    @Test
    public void testSequenceList_Success() throws Exception {
        PowerMockito.doReturn(conn).when(hawqClientSpy, "getConnection", anyMap(), anyString());
        when(conn.prepareStatement(SEQUENCE_LIST_QUERY)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSet.getString(RELNAME)).thenReturn("seq1").thenReturn("seq2");
        when(resultSet.getString(SCHEMANAME)).thenReturn("schema1").thenReturn("schema2");
        List<String> result = hawqClientSpy.getSequenceList("s", resources);
        Collections.sort(result);
        assertEquals(Arrays.asList("seq1", "seq2"), result);
    }

    @Test
    public void testSequenceList_SchemaFiltered_Success() throws Exception {
        PowerMockito.doReturn(conn).when(hawqClientSpy, "getConnection", anyMap(), anyString());
        when(conn.prepareStatement(SEQUENCE_LIST_QUERY)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSet.getString(RELNAME)).thenReturn("seq1").thenReturn("seq2");
        when(resultSet.getString(SCHEMANAME)).thenReturn("schema1").thenReturn("schema3");
        assertEquals(Arrays.asList("seq1"), hawqClientSpy.getSequenceList("s", resources));
    }

    @Test
    public void testFunctionList_Success() throws Exception {
        PowerMockito.doReturn(conn).when(hawqClientSpy, "getConnection", anyMap(), anyString());
        when(conn.prepareStatement(FUNCTION_LIST_QUERY)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSet.getString(PRONAME)).thenReturn("fxn1").thenReturn("fxn2");
        when(resultSet.getString(NSPNAME)).thenReturn("schema1").thenReturn("schema2");
        assertEquals(Arrays.asList("fxn1", "fxn2"), hawqClientSpy.getFunctionList("f", resources));
    }

}
