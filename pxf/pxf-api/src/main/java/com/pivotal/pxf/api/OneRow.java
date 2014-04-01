package com.pivotal.pxf.api;

/**
 * Represents one row in the external system data store.
 * Supports the general case where one row contains both a record and a
 * separate key like in the HDFS key/value model for MapReduce (Example: HDFS sequence file).
 */
public class OneRow {
    private Object key;
    private Object data;

    public OneRow(){
    }

    /**
     * Constructs a OneRow
     *
     * @param key the key for the record
     * @param data the actual record
     */
    public OneRow(Object key, Object data) {
        this.key = key;
        this.data = data;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Object getKey() {
        return key;
    }

    public Object getData() {
        return data;
    }

    @Override
    public String toString() {
        return "OneRow:" + key + "->" + data;
    }
}

