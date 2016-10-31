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

import com.sun.org.apache.xml.internal.utils.StringComparable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.FilterParser;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.io.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcMySqlExtensionTest {
    private static final Log LOG = LogFactory.getLog(JdbcMySqlExtensionTest.class);
    static String MYSQL_URL = "jdbc:mysql://10.110.17.21:3306/demodb";
    InputData inputData;

    //@Before
    public void setup() throws Exception {
        LOG.info("JdbcMySqlExtensionTest.setup()");
        prepareConstruction();
        JdbcWriter writer = new JdbcWriter(inputData);
        writer.openForWrite();

        //create table
        writer.executeSQL("CREATE TABLE sales (id int primary key, cdate date, amt decimal(10,2),grade varchar(30))");
        //INSERT DEMO data
        String[] inserts = {"INSERT INTO sales values (1, DATE('2008-01-01'), 1000,'general')",
                "INSERT INTO sales values (2, DATE('2008-02-01'), 900,'bad')",
                "INSERT INTO sales values (3, DATE('2008-03-10'), 1200,'good')",
                "INSERT INTO sales values (4, DATE('2008-04-10'), 1100,'good')",
                "INSERT INTO sales values (5, DATE('2008-05-01'), 1010,'general')",
                "INSERT INTO sales values (6, DATE('2008-06-01'), 850,'bad')",
                "INSERT INTO sales values (7, DATE('2008-07-01'), 1400,'excellent')",
                "INSERT INTO sales values (8, DATE('2008-08-01'), 1500,'excellent')",
                "INSERT INTO sales values (9, DATE('2008-09-01'), 1000,'good')",
                "INSERT INTO sales values (10, DATE('2008-10-01'), 800,'bad')",
                "INSERT INTO sales values (11, DATE('2008-11-01'), 1250,'good')",
                "INSERT INTO sales values (12, DATE('2008-12-01'), 1300,'excellent')",
                "INSERT INTO sales values (13, DATE('2009-03-01'), 1250,'good')",
                "INSERT INTO sales values (14, DATE('2009-04-01'), 1300,'excellent')",
                "INSERT INTO sales values (15, DATE('2009-01-01'), 1500,'excellent')",
                "INSERT INTO sales values (16, DATE('2009-02-01'), 1340,'excellent')"};
        for (String sql : inserts)
            writer.executeSQL(sql);

        writer.closeWrite();
    }

    //@After
    public void cleanup() throws Exception {
        LOG.info("JdbcMySqlExtensionTest.cleanup()");
        prepareConstruction();
        JdbcWriter writer = new JdbcWriter(inputData);
        writer.openForWrite();
        writer.executeSQL("drop table sales");
        writer.closeWrite();
    }

    @Test
    public void testIdFilter() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        when(inputData.getFilterString()).thenReturn("a0c1o5");//id=1
        JdbcReadAccessor reader = new JdbcReadAccessor(inputData);
        reader.openForRead();
        ArrayList<List<OneField>> row_list = readAllRows(reader);
        assertEquals(1, row_list.size());

        List<OneField> fields = row_list.get(0);
        assertEquals(4, fields.size());
        assertEquals(1, fields.get(0).val);
        assertEquals("2008-01-01", fields.get(1).val.toString());
        assertEquals("general", fields.get(3).val);

        reader.closeForRead();

    }

    @Test
    public void testDateAndAmtFilter() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        // cdate>'2008-02-01' and cdate<'2008-12-01' and amt > 1200
        when(inputData.getFilterString()).thenReturn("a1c\"2008-02-01\"o2a1c\"2008-12-01\"o1l0a2c1200o2l0");
        JdbcReadAccessor reader = new JdbcReadAccessor(inputData);
        reader.openForRead();

        ArrayList<List<OneField>> row_list = readAllRows(reader);

        assertEquals(3, row_list.size());

        //assert random row
        Random random = new Random();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        List<OneField> fields = row_list.get(random.nextInt(row_list.size()));
        assertTrue(((Date)(fields.get(1).val)).after(dateFormat.parse("2008-02-01")));
        assertTrue(((Date)(fields.get(1).val)).before(dateFormat.parse("2008-12-01")));
        assertTrue(((Double)(fields.get(2).val)) > 1200);

        reader.closeForRead();
    }

    @Test
    public void testUnsupportedOperationFilter() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        // grade like 'bad'
        when(inputData.getFilterString()).thenReturn("a3c\"bad\"o7");
        JdbcReadAccessor reader = new JdbcReadAccessor(inputData);
        reader.openForRead();

        ArrayList<List<OneField>> row_list = readAllRows(reader);

        //return all data
        assertEquals(16, row_list.size());

        reader.closeForRead();
    }

    @Test
    public void testUnsupportedLogicalFilter() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        // cdate>'2008-02-01' or amt < 1200
        when(inputData.getFilterString()).thenReturn("a1c\"2008-02-01\"o2a2c1200o2l1");
        JdbcReadAccessor reader = new JdbcReadAccessor(inputData);
        reader.openForRead();

        ArrayList<List<OneField>> row_list = readAllRows(reader);

        //all data
        assertEquals(16, row_list.size());

        reader.closeForRead();
    }

    @Test
    public void testDatePartition() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(false);
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("cdate:date");
        when(inputData.getUserProperty("RANGE")).thenReturn("2008-01-01:2009-01-01");
        when(inputData.getUserProperty("INTERVAL")).thenReturn("2:month");
        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(6, fragments.size());

        //partition-1 : cdate>=2008-01-01 and cdate<2008-03-01
        when(inputData.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        JdbcReadAccessor reader = new JdbcReadAccessor(inputData);
        reader.openForRead();

        ArrayList<List<OneField>> row_list = readAllRows(reader);

        //all data
        assertEquals(2, row_list.size());

        //assert random row
        Random random = new Random();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        List<OneField> fields = row_list.get(random.nextInt(row_list.size()));
        assertTrue(((Date)(fields.get(1).val)).after(dateFormat.parse("2007-12-31")));
        assertTrue(((Date)(fields.get(1).val)).before(dateFormat.parse("2008-03-01")));

        reader.closeForRead();
    }

    @Test
    public void testFilterAndPartition() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(true);
        when(inputData.getFilterString()).thenReturn("a0c5o2");//id>5
        when(inputData.getUserProperty("PARTITION_BY")).thenReturn("grade:enum");
        when(inputData.getUserProperty("RANGE")).thenReturn("excellent:good:general:bad");
        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();

        //partition-1 : id>5 and grade='excellent'
        when(inputData.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        JdbcReadAccessor reader = new JdbcReadAccessor(inputData);
        reader.openForRead();

        ArrayList<List<OneField>> row_list = readAllRows(reader);

        //all data
        assertEquals(6, row_list.size());

        //assert random row
        Random random = new Random();
        List<OneField> fields = row_list.get(random.nextInt(row_list.size()));
        assertTrue((Integer)(fields.get(0).val) > 5);
        assertEquals("excellent",fields.get(3).val);

        reader.closeForRead();
    }

    @Test
    public void testNoPartition() throws Exception {
        prepareConstruction();
        when(inputData.hasFilter()).thenReturn(false);
        JdbcPartitionFragmenter fragment = new JdbcPartitionFragmenter(inputData);
        List<Fragment> fragments = fragment.getFragments();
        assertEquals(1, fragments.size());

        when(inputData.getFragmentMetadata()).thenReturn(fragments.get(0).getMetadata());

        JdbcReadAccessor reader = new JdbcReadAccessor(inputData);
        reader.openForRead();

        ArrayList<List<OneField>> row_list = readAllRows(reader);

        //all data
        assertEquals(16, row_list.size());

        reader.closeForRead();
    }

    private ArrayList<List<OneField>> readAllRows(JdbcReadAccessor reader) throws Exception {
        JdbcReadResolver resolver = new JdbcReadResolver(inputData);
        ArrayList<List<OneField>> row_list = new ArrayList<>();
        OneRow row;
        do {
            row = reader.readNextObject();
            if (row != null)
                row_list.add(resolver.getFields(row));
        } while (row != null);

        return row_list;
    }


    private void prepareConstruction() throws Exception {
        inputData = mock(InputData.class);
        when(inputData.getUserProperty("JDBC_DRIVER")).thenReturn("com.mysql.jdbc.Driver");
        when(inputData.getUserProperty("DB_URL")).thenReturn(MYSQL_URL);
        when(inputData.getUserProperty("USER")).thenReturn("root");
        when(inputData.getUserProperty("PASS")).thenReturn("root");
        when(inputData.getDataSource()).thenReturn("sales");


        ArrayList<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
        columns.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columns.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 1, "date", null));
        columns.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 2, "float8", null));
        columns.add(new ColumnDescriptor("grade", DataType.TEXT.getOID(), 3, "text", null));
        when(inputData.getTupleDescription()).thenReturn(columns);
        when(inputData.getColumn(0)).thenReturn(columns.get(0));
        when(inputData.getColumn(1)).thenReturn(columns.get(1));
        when(inputData.getColumn(2)).thenReturn(columns.get(2));
        when(inputData.getColumn(3)).thenReturn(columns.get(3));

    }

    class JdbcWriter extends JdbcPlugin {
        Statement statement = null;

        /**
         * @param input the input data
         */
        public JdbcWriter(InputData input) {
            super(input);
        }

        public boolean openForWrite() throws SQLException, ClassNotFoundException {
            if (statement != null && !statement.isClosed())
                return true;
            super.openConnection();
            statement = dbconn.createStatement();

            return true;
        }

        public boolean executeSQL(String sql) throws SQLException {
            LOG.info("execute Update SQL : " + sql );
            return statement.execute(sql);
        }

        public void closeWrite() throws SQLException {
            if (statement != null && !statement.isClosed()) {
                statement.close();
                statement = null;
            }
            super.closeConnection();

        }
    }
}
