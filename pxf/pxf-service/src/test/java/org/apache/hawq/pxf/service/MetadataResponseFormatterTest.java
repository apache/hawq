package org.apache.hawq.pxf.service;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.directory.shared.kerberos.components.MethodData;
import org.junit.Test;

import org.apache.hawq.pxf.api.Metadata;

public class MetadataResponseFormatterTest {

    String result = null;

    @Test
    public void formatResponseString() throws Exception {
        List<Metadata> metadataList = new ArrayList<Metadata>();
        List<Metadata.Field> fields = new ArrayList<Metadata.Field>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, fields);
        fields.add(new Metadata.Field("field1", "int"));
        fields.add(new Metadata.Field("field2", "text"));
        metadataList.add(metadata);

        result = MetadataResponseFormatter.formatResponseString(metadataList);
        StringBuilder expected = new StringBuilder("{\"PXFMetadata\":[{");
        expected.append("\"item\":{\"path\":\"default\",\"name\":\"table1\"},")
                .append("\"fields\":[{\"name\":\"field1\",\"type\":\"int\"},{\"name\":\"field2\",\"type\":\"text\"}]}]}");

        assertEquals(expected.toString(), result);
    }

    @Test
    public void formatResponseStringWithNullModifier() throws Exception {
        List<Metadata> metadataList = new ArrayList<Metadata>();
        List<Metadata.Field> fields = new ArrayList<Metadata.Field>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, fields);
        fields.add(new Metadata.Field("field1", "int", null));
        fields.add(new Metadata.Field("field2", "text", new String[] {}));
        metadataList.add(metadata);

        result = MetadataResponseFormatter.formatResponseString(metadataList);
        StringBuilder expected = new StringBuilder("{\"PXFMetadata\":[{");
        expected.append("\"item\":{\"path\":\"default\",\"name\":\"table1\"},")
                .append("\"fields\":[{\"name\":\"field1\",\"type\":\"int\"},{\"name\":\"field2\",\"type\":\"text\"}]}]}");

        assertEquals(expected.toString(), result);
    }

    @Test
    public void formatResponseStringWithModifiers() throws Exception {
        List<Metadata> metadataList = new ArrayList<Metadata>();
        List<Metadata.Field> fields = new ArrayList<Metadata.Field>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, fields);
        fields.add(new Metadata.Field("field1", "int"));
        fields.add(new Metadata.Field("field2", "numeric",
                new String[] {"1349", "1789"}));
        fields.add(new Metadata.Field("field3", "char",
                new String[] {"50"}));
        metadataList.add(metadata);

        result = MetadataResponseFormatter.formatResponseString(metadataList);
        StringBuilder expected = new StringBuilder("{\"PXFMetadata\":[{");
        expected.append("\"item\":{\"path\":\"default\",\"name\":\"table1\"},")
                .append("\"fields\":[")
                .append("{\"name\":\"field1\",\"type\":\"int\"},")
                .append("{\"name\":\"field2\",\"type\":\"numeric\",\"modifiers\":[\"1349\",\"1789\"]},")
                .append("{\"name\":\"field3\",\"type\":\"char\",\"modifiers\":[\"50\"]}")
                .append("]}]}");

        assertEquals(expected.toString(), result);
    }

    @Test
    public void formatResponseStringNull() throws Exception {
        List<Metadata> metadataList = null;
        result = MetadataResponseFormatter.formatResponseString(metadataList);
        String expected = new String("{\"PXFMetadata\":[]}");
        assertEquals(expected, result);
    }

    @Test
    public void formatResponseStringNoFields() throws Exception {
        List<Metadata> metadataList = new ArrayList<Metadata>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, null);
        metadataList.add(metadata);
        try {
            result = MetadataResponseFormatter.formatResponseString(metadataList);
            fail("formatting should fail because fields field is null");
        } catch (IllegalArgumentException e) {
            assertEquals("metadata for " + metadata.getItem() + " contains no fields - cannot serialize", e.getMessage());
        }

        ArrayList<Metadata.Field> fields = new ArrayList<Metadata.Field>();
        metadataList = new ArrayList<Metadata>();
        metadata = new Metadata(itemName, fields);
        metadataList.add(metadata);
        try {
            result = MetadataResponseFormatter.formatResponseString(metadataList);
            fail("formatting should fail because there are no fields");
        } catch (IllegalArgumentException e) {
            assertEquals("metadata for " + metadata.getItem() + " contains no fields - cannot serialize", e.getMessage());
        }
    }

    @Test
    public void formatResponseStringPartialNull() throws Exception {
        List<Metadata> metadataList = new ArrayList<Metadata>();
        List<Metadata.Field> fields = new ArrayList<Metadata.Field>();
        Metadata.Item itemName = new Metadata.Item("default", "table1");
        Metadata metadata = new Metadata(itemName, fields);
        fields.add(new Metadata.Field("field1", "int"));
        metadataList.add(null);
        metadataList.add(metadata);
        try {
            result = MetadataResponseFormatter.formatResponseString(metadataList);
            fail("formatting should fail because one of the metdata object is null");
        } catch (IllegalArgumentException e) {
            assertEquals("metadata object is null - cannot serialize", e.getMessage());
        }
    }

    @Test
    public void formatResponseStringWithMultipleItems() throws Exception {
        List <Metadata> metdataList = new ArrayList<Metadata>();
        for (int i=1; i<=10; i++) {
            List<Metadata.Field> fields = new ArrayList<Metadata.Field>();
            Metadata.Item itemName = new Metadata.Item("default", "table"+i);
            Metadata metadata = new Metadata(itemName, fields);
            fields.add(new Metadata.Field("field1", "int"));
            fields.add(new Metadata.Field("field2", "text"));
            metdataList.add(metadata);
        }
        result = MetadataResponseFormatter.formatResponseString(metdataList);

        StringBuilder expected = new StringBuilder();
        for (int i=1; i<=10; i++) {
            if(i==1) {
                expected.append("{\"PXFMetadata\":[");
            } else {
                expected.append(",");
            }
            expected.append("{\"item\":{\"path\":\"default\",\"name\":\"table").append(i).append("\"},");
            expected.append("\"fields\":[{\"name\":\"field1\",\"type\":\"int\"},{\"name\":\"field2\",\"type\":\"text\"}]}");
        }
        expected.append("]}");

        assertEquals(expected.toString(), result);
    }

    @Test
    public void formatResponseStringWithMultiplePathsAndItems() throws Exception {
        List <Metadata> metdataList = new ArrayList<Metadata>();
        for (int i=1; i<=10; i++) {
            List<Metadata.Field> fields = new ArrayList<Metadata.Field>();
            Metadata.Item itemName = new Metadata.Item("default"+i, "table"+i);
            Metadata metadata = new Metadata(itemName, fields);
            fields.add(new Metadata.Field("field1", "int"));
            fields.add(new Metadata.Field("field2", "text"));
            metdataList.add(metadata);
        }
        result = MetadataResponseFormatter.formatResponseString(metdataList);
        StringBuilder expected = new StringBuilder();
        for (int i=1; i<=10; i++) {
            if(i==1) {
                expected.append("{\"PXFMetadata\":[");
            } else {
                expected.append(",");
            }
            expected.append("{\"item\":{\"path\":\"default").append(i).append("\",\"name\":\"table").append(i).append("\"},");
            expected.append("\"fields\":[{\"name\":\"field1\",\"type\":\"int\"},{\"name\":\"field2\",\"type\":\"text\"}]}");
        }
        expected.append("]}");

        assertEquals(expected.toString(), result);
    }
}

