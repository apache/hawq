package org.apache.hawq.pxf.api;

/**
 * Defines a one field in a deserialized record.
 */
public class OneField {
    /** OID value recognized by GPDBWritable. */
    public int type;

    /** Field value. */
    public Object val;

    public OneField() {
    }

    /**
     * Constructs a OneField object.
     *
     * @param type the OID value recognized by GPDBWritable
     * @param val the field value
     */
    public OneField(int type, Object val) {
        this.type = type;
        this.val = val;
    }
}
