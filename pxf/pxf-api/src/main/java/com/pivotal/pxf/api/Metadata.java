package com.pivotal.pxf.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * Metadata holds a table's metadata information.
 * {@link MetadataFetcher#getTableMetadata} returns the table's metadata.
 */
public class Metadata {

    /**
     * Class representing table name - db (schema) name and table name.
     */
    public static class Table {
        private String dbName;
        private String tableName;

        public Table(String dbName, String tableName) {

            if (StringUtils.isBlank(dbName) || StringUtils.isBlank(tableName)) {
                throw new IllegalArgumentException("Table name cannot be empty");
            }

            this.dbName = dbName;
            this.tableName = tableName;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        /**
         * Returns full table name in the form db_name.table_name
         */
        @Override
        public String toString() {
            return dbName + "." + tableName;
        }
    }

    /**
     * Class representing table field - name and type.
     */
    public static class Field {
        private String name;
        private String type; // TODO: nhorn - 06-03-15 - change to enum
        private String[] modifiers; // type modifiers, optional field

        public Field(String name, String type) {

            if (StringUtils.isBlank(name) || StringUtils.isBlank(type)) {
                throw new IllegalArgumentException("Field name and type cannot be empty");
            }

            this.name = name;
            this.type = type;
        }

        public Field(String name, String type, String[] modifiers) {
            this(name, type);
            this.modifiers = modifiers;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public String[] getModifiers() {
            return modifiers;
        }
    }

    /**
     * Table name
     */
    private Metadata.Table table;

    /**
     * Table's fields
     */
    private List<Metadata.Field> fields;

    /**
     * Constructs a table's Metadata.
     *
     * @param tableName the table name
     * @param fields the table's fields
     */
    public Metadata(Metadata.Table tableName,
            List<Metadata.Field> fields) {
        this.table = tableName;
        this.fields = fields;
    }

    public Metadata(Metadata.Table tableName) {
        this(tableName, new ArrayList<Metadata.Field>());
    }

    public Metadata.Table getTable() {
        return table;
    }

    public List<Metadata.Field> getFields() {
        return fields;
    }

    /**
     * Adds a field to metadata fields.
     *
     * @param field field to add
     */
    public void addField(Metadata.Field field) {
        if (fields == null) {
            fields = new ArrayList<Metadata.Field>();
        }
        fields.add(field);
    }
}
