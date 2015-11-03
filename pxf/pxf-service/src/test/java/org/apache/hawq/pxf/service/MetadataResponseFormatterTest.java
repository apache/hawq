package org.apache.hawq.pxf.service;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.hawq.pxf.api.Metadata;

public class MetadataResponseFormatterTest {

    String result = null;

    @Test
    public void formatResponseString() throws Exception {
        List<Metadata.Field> fields = new ArrayList<Metadata.Field>();
        Metadata.Table tableName = new Metadata.Table("default", "table1");
        Metadata metadata = new Metadata(tableName, fields);
        fields.add(new Metadata.Field("field1", "int"));
        fields.add(new Metadata.Field("field2", "text"));

        result = MetadataResponseFormatter.formatResponseString(metadata);
        String expected = "{\"PXFMetadata\":[{"
                + "\"table\":{\"dbName\":\"default\",\"tableName\":\"table1\"},"
                + "\"fields\":[{\"name\":\"field1\",\"type\":\"int\"},{\"name\":\"field2\",\"type\":\"text\"}]}]}";

        assertEquals(expected, result);
    }

    @Test
    public void formatResponseStringWithNullModifier() throws Exception {
        List<Metadata.Field> fields = new ArrayList<Metadata.Field>();
        Metadata.Table tableName = new Metadata.Table("default", "table1");
        Metadata metadata = new Metadata(tableName, fields);
        fields.add(new Metadata.Field("field1", "int", null));
        fields.add(new Metadata.Field("field2", "text", new String[] {}));

        result = MetadataResponseFormatter.formatResponseString(metadata);
        String expected = "{\"PXFMetadata\":[{"
                + "\"table\":{\"dbName\":\"default\",\"tableName\":\"table1\"},"
                + "\"fields\":[{\"name\":\"field1\",\"type\":\"int\"},{\"name\":\"field2\",\"type\":\"text\"}]}]}";

        assertEquals(expected, result);
    }

    @Test
    public void formatResponseStringWithModifiers() throws Exception {
        List<Metadata.Field> fields = new ArrayList<Metadata.Field>();
        Metadata.Table tableName = new Metadata.Table("default", "table1");
        Metadata metadata = new Metadata(tableName, fields);
        fields.add(new Metadata.Field("field1", "int"));
        fields.add(new Metadata.Field("field2", "numeric",
                new String[] {"1349", "1789"}));
        fields.add(new Metadata.Field("field3", "char",
                new String[] {"50"}));

        result = MetadataResponseFormatter.formatResponseString(metadata);
        String expected = "{\"PXFMetadata\":[{"
                + "\"table\":{\"dbName\":\"default\",\"tableName\":\"table1\"},"
                + "\"fields\":["
                + "{\"name\":\"field1\",\"type\":\"int\"},"
                + "{\"name\":\"field2\",\"type\":\"numeric\",\"modifiers\":[\"1349\",\"1789\"]},"
                + "{\"name\":\"field3\",\"type\":\"char\",\"modifiers\":[\"50\"]}"
                + "]}]}";

        assertEquals(expected, result);
    }

    @Test
    public void formatResponseStringNull() throws Exception {
        Metadata metadata = null;

        try {
            result = MetadataResponseFormatter.formatResponseString(metadata);
            fail("formatting should fail because metadata is null");
        } catch (IllegalArgumentException e) {
            assertEquals("metadata object is null - cannot serialize", e.getMessage());
        }
    }

    @Test
    public void formatResponseStringNoFields() throws Exception {
        Metadata.Table tableName = new Metadata.Table("default", "table1");
        Metadata metadata = new Metadata(tableName, null);

        try {
            result = MetadataResponseFormatter.formatResponseString(metadata);
            fail("formatting should fail because fields field is null");
        } catch (IllegalArgumentException e) {
            assertEquals("metadata contains no fields - cannot serialize", e.getMessage());
        }

        ArrayList<Metadata.Field> fields = new ArrayList<Metadata.Field>();
        metadata = new Metadata(tableName, fields);

        try {
            result = MetadataResponseFormatter.formatResponseString(metadata);
            fail("formatting should fail because there are no fields");
        } catch (IllegalArgumentException e) {
            assertEquals("metadata contains no fields - cannot serialize", e.getMessage());
        }
    }
}

